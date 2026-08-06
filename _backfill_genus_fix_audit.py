#!/usr/bin/env python
"""
Populate genus_fix_audit from the pre-fix whole-table snapshot the repair left behind.

One-time backfill: the repair (_apply_genus_fix.py) ran before the audit table existed, so
the "before" values only live in TaxonomicTable_bak_<timestamp>. This copies them into a
permanent table so the curator UI does not depend on a timestamped backup surviving.

Idempotent (ON CONFLICT DO NOTHING on taxon_id+source).

Run: PYTHONIOENCODING=utf-8 python _backfill_genus_fix_audit.py [--apply]
"""
import asyncio
import io
import sys

from app.db.database import get_db, execute_query

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

APPLY = "--apply" in sys.argv
SOURCE = "genus_fix_20260806"


async def main():
    snaps = await execute_query(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_name LIKE 'TaxonomicTable_bak%' ORDER BY table_name")
    if not snaps:
        print("no TaxonomicTable_bak_* snapshot found; nothing to backfill")
        return
    bak = snaps[0]["table_name"]
    if len(snaps) > 1:
        print(f"WARNING: {len(snaps)} snapshots exist; using the earliest ({bak}) as the "
              f"pre-fix state: {[s['table_name'] for s in snaps]}")
    print(f"snapshot: {bak}")

    rows = await execute_query(f'''
        SELECT cur."TaxonID"                AS taxon_id,
               cur."FullScientificName"     AS full_name,
               b."Genus"                    AS genus_before,
               cur."Genus"                  AS genus_after,
               f."FamilyName"               AS family_name
        FROM "TaxonomicTable" cur
        JOIN "{bak}" b ON b."TaxonID" = cur."TaxonID"
        LEFT JOIN "Family" f ON f."FamilyID" = cur."FamilyID"
        WHERE cur."Genus" IS DISTINCT FROM b."Genus"
        ORDER BY cur."TaxonID"''')
    print(f"rows whose Genus differs from the snapshot: {len(rows)}")
    for r in rows[:5]:
        print(f"   {r['taxon_id']:<6} {r['full_name']:<34} "
              f"{str(r['genus_before'])[:34]!r} -> {r['genus_after']!r}")
    if len(rows) > 5:
        print(f"   ... ({len(rows) - 5} more)")

    if not APPLY:
        print("\nDRY-RUN: nothing written. Re-run with --apply.")
        return

    async with get_db() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            await conn.executemany(
                "INSERT INTO genus_fix_audit "
                "(taxon_id, full_name, genus_before, genus_after, family_name, fixed_by, source) "
                "VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING",
                [(r["taxon_id"], r["full_name"], r["genus_before"], r["genus_after"],
                  r["family_name"], "genus_fix_script", SOURCE) for r in rows])
            n = await conn.fetchval(
                "SELECT count(*) FROM genus_fix_audit WHERE source=$1", SOURCE)
            await tr.commit()
            print(f"\nAPPLY: genus_fix_audit now holds {n} rows for source={SOURCE}")
        except Exception:
            await tr.rollback()
            raise


asyncio.run(main())