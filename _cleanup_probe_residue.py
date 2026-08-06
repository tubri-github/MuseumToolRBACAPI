#!/usr/bin/env python
"""
Remove the rows my round-trip probes left in the curator-facing history tables.

They are harmless (every merge was undone, every decision reverted, the two applies
failed on purpose) but they show up in the merge history and the genus decision list as
entries by "probe", which is confusing for the person actually using those screens. The
probe scripts remain in the repo as the reproducible tests.

Refuses to touch anything that is not probe residue: only rows authored by the probe user
AND already in a terminal state (undone / failed / reverted).

Run: PYTHONIOENCODING=utf-8 python _cleanup_probe_residue.py [--apply]
"""
import asyncio
import io
import sys

from app.db.database import get_db, execute_query

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

APPLY = "--apply" in sys.argv
PROBE_USERS = ["probe", "probe_curator"]


async def main():
    logs = await execute_query(
        "SELECT id, operation, status, old_name, new_name, applied_by, undone_by "
        "FROM taxon_recheck_apply_log "
        "WHERE (applied_by = ANY($1::text[]) OR undone_by = ANY($1::text[])) "
        "  AND status IN ('undone', 'failed') ORDER BY id", PROBE_USERS)
    decisions = await execute_query(
        "SELECT id, taxon_id, decision, field, value_before, value_after, decided_by "
        "FROM genus_manual_decision "
        "WHERE decided_by = ANY($1::text[]) AND reverted_at IS NOT NULL ORDER BY id",
        PROBE_USERS)

    print(f"apply-log rows to delete: {len(logs)}")
    for x in logs:
        print(f"   id={x['id']:<3} {x['operation']:<14} {x['status']:<7} "
              f"{x['old_name']} -> {x['new_name']}  by={x['applied_by']}")
    print(f"\ngenus decisions to delete: {len(decisions)}")
    for x in decisions:
        print(f"   id={x['id']:<3} taxon={x['taxon_id']} {x['decision']} "
              f"({x['field']}) by={x['decided_by']}")

    # anything left behind that is NOT probe residue must survive untouched
    others = await execute_query(
        "SELECT count(*) AS n FROM taxon_recheck_apply_log "
        "WHERE NOT (applied_by = ANY($1::text[]) OR undone_by = ANY($1::text[]))",
        PROBE_USERS)
    live = await execute_query(
        "SELECT count(*) AS n FROM taxon_recheck_apply_log WHERE status = 'applied'")
    print(f"\nnon-probe apply-log rows (kept): {others[0]['n']}")
    print(f"apply-log rows still 'applied' (must be 0 before deleting): {live[0]['n']}")
    if live[0]["n"]:
        print("REFUSING: an applied operation is still live; deleting its log would strip "
              "the only way to undo it.")
        return

    if not APPLY:
        print("\nDRY-RUN: nothing deleted. Re-run with --apply.")
        return

    async with get_db() as conn:
        tx = conn.transaction()
        await tx.start()
        try:
            d1 = await conn.execute(
                "DELETE FROM taxon_recheck_apply_log WHERE id = ANY($1::int[])",
                [x["id"] for x in logs]) if logs else "0"
            d2 = await conn.execute(
                "DELETE FROM genus_manual_decision WHERE id = ANY($1::int[])",
                [x["id"] for x in decisions]) if decisions else "0"
            await tx.commit()
            print(f"\ndeleted -> apply-log: {d1} | genus decisions: {d2}")
        except Exception:
            await tx.rollback()
            raise

    for t in ("taxon_recheck_apply_log", "genus_manual_decision"):
        r = await execute_query(f"SELECT count(*) AS n FROM {t}")
        print(f"   {t}: {r[0]['n']} rows left")


asyncio.run(main())