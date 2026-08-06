"""
Merge duplicate taxa: two "TaxonomicTable" rows spelling the same FullScientificName,
each carrying its own specimens.

Nothing is deleted. The losing row is tagged merged_into_taxon_id and kept; its current
determinations are retired (IsCurrent=false) and re-inserted against the surviving taxon
with Date1 / DeterminerName COPIED FORWARD, because a merge is bookkeeping, not a
re-identification -- see migrations/taxon_merge.sql for the full reasoning.

Contrast with TaxonApplyService: there the name actually changes, so the new determination
is correctly stamped with the current curator and today. Do not unify the two write-backs.

The merge is recorded in taxon_recheck_apply_log with operation='taxon_merge' so the
curator has a single undo history, and undo restores the previous state exactly.
"""
import logging
from typing import Any, Dict, List, Optional

from app.db.database import get_db, execute_query, execute_single_query
# The apply log is shared with TaxonApplyService, and so is the newest-first undo rule.
from app.services.taxon_apply_service import undo_blocker

logger = logging.getLogger("taxon_merge_service")

MERGE_MARK = "merged from taxon {}"   # only ever written into an EMPTY Remarks


class TaxonMergeService:

    # ---- what is duplicated ----------------------------------------------------------

    @staticmethod
    async def duplicate_groups() -> Dict[str, Any]:
        """One entry per duplicated FullScientificName, with each member's specimen count
        and a recommended winner (the taxon the collection is already on).

        Excludes rows already merged away, so a group disappears once it is resolved.
        """
        rows = await execute_query('''
            WITH dups AS (
              SELECT lower(btrim("FullScientificName")) AS fsn
              FROM "TaxonomicTable"
              WHERE merged_into_taxon_id IS NULL
              GROUP BY 1 HAVING count(*) > 1)
            SELECT lower(btrim(tt."FullScientificName")) AS fsn,
                   tt."TaxonID", tt."FullScientificName", tt."Genus", tt."Species",
                   tt."FamilyID", f."FamilyName", tt.created_via,
                   (SELECT count(*) FROM "Determination" d
                    WHERE d."TaxonID" = tt."TaxonID" AND d."IsCurrent" IS TRUE) AS dets_current,
                   (SELECT count(*) FROM "Determination" d
                    WHERE d."TaxonID" = tt."TaxonID") AS dets_all
            FROM "TaxonomicTable" tt
            LEFT JOIN "Family" f ON f."FamilyID" = tt."FamilyID"
            JOIN dups ON dups.fsn = lower(btrim(tt."FullScientificName"))
            WHERE tt.merged_into_taxon_id IS NULL
            ORDER BY 1, tt."TaxonID"''')

        groups: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            g = groups.setdefault(r["fsn"], {"name": r["FullScientificName"], "members": []})
            g["members"].append({
                "taxon_id": r["TaxonID"], "family": r["FamilyName"],
                "family_id": r["FamilyID"], "created_via": r["created_via"],
                "determinations_current": r["dets_current"], "determinations_all": r["dets_all"],
            })

        out = []
        for fsn, g in groups.items():
            members = g["members"]
            winner = max(members, key=lambda m: (m["determinations_current"], -m["taxon_id"]))
            used = [m for m in members if m["determinations_current"] or m["determinations_all"]]
            to_move = sum(m["determinations_current"] for m in members
                          if m["taxon_id"] != winner["taxon_id"])
            out.append({
                "name": g["name"],
                "members": members,
                "recommended_winner": winner["taxon_id"],
                # trivial  = only one side is used, nothing to move
                # merge    = both sides carry specimens
                "kind": "merge" if len(used) > 1 else "trivial",
                "determinations_to_move": to_move,
                "families_differ": len({m["family"] for m in members}) > 1,
            })
        out.sort(key=lambda x: -x["determinations_to_move"])
        return {
            "groups": out,
            "totals": {
                "groups": len(out),
                "needing_merge": sum(1 for g in out if g["kind"] == "merge"),
                "determinations_to_move": sum(g["determinations_to_move"] for g in out),
            },
        }

    # ---- preview ---------------------------------------------------------------------

    @staticmethod
    async def preview(winner_taxon_id: int, loser_taxon_id: int) -> Dict[str, Any]:
        pair = await execute_query(
            'SELECT "TaxonID", "FullScientificName", "FamilyID", merged_into_taxon_id '
            'FROM "TaxonomicTable" WHERE "TaxonID" = ANY($1::int[])',
            [winner_taxon_id, loser_taxon_id])
        by_id = {r["TaxonID"]: r for r in pair}
        if winner_taxon_id not in by_id or loser_taxon_id not in by_id:
            return {"error": "winner or loser taxon not found"}

        dets = await execute_single_query(
            'SELECT count(*) AS n, count(DISTINCT "PrimaryID") AS prim '
            'FROM "Determination" WHERE "TaxonID"=$1 AND "IsCurrent" IS TRUE', loser_taxon_id)
        sample = await execute_query(
            'SELECT "DeterminationID","PrimaryID","Date1","DeterminerName","Remarks" '
            'FROM "Determination" WHERE "TaxonID"=$1 AND "IsCurrent" IS TRUE '
            'ORDER BY "PrimaryID" LIMIT 5', loser_taxon_id)
        return {
            "winner": {"taxon_id": winner_taxon_id,
                       "name": by_id[winner_taxon_id]["FullScientificName"]},
            "loser": {"taxon_id": loser_taxon_id,
                      "name": by_id[loser_taxon_id]["FullScientificName"],
                      "already_merged_into": by_id[loser_taxon_id]["merged_into_taxon_id"]},
            "names_identical": (by_id[winner_taxon_id]["FullScientificName"] or "").strip().lower()
                               == (by_id[loser_taxon_id]["FullScientificName"] or "").strip().lower(),
            "families_differ": by_id[winner_taxon_id]["FamilyID"] != by_id[loser_taxon_id]["FamilyID"],
            "determinations_to_move": dets["n"] if dets else 0,
            "affected_primaries": dets["prim"] if dets else 0,
            "sample": sample,
        }

    # ---- merge -----------------------------------------------------------------------

    async def merge(self, winner_taxon_id: int, loser_taxon_id: int, merged_by: str,
                    notes: Optional[str] = None,
                    allow_different_names: bool = False) -> Dict[str, Any]:
        if winner_taxon_id == loser_taxon_id:
            return {"error": "winner and loser are the same taxon"}

        pre = await self.preview(winner_taxon_id, loser_taxon_id)
        if "error" in pre:
            return pre
        if pre["loser"]["already_merged_into"] is not None:
            return {"error": f"taxon {loser_taxon_id} was already merged into "
                             f"{pre['loser']['already_merged_into']}"}
        if not pre["names_identical"] and not allow_different_names:
            # A merge is for rows spelling the SAME name. Different names mean a taxonomic
            # decision, which belongs in the recheck apply path (it stamps the curator).
            return {"error": f"names differ ({pre['winner']['name']!r} vs "
                             f"{pre['loser']['name']!r}); use the recheck apply flow for a "
                             f"name change, or pass allow_different_names"}

        who = (merged_by or "")[:50]
        async with get_db() as conn:
            tx = conn.transaction()
            await tx.start()
            try:
                dets = await conn.fetch(
                    'SELECT "DeterminationID","PrimaryID","Date1","DeterminerName","Remarks",'
                    '"oldDeterminerID","Determiner","lotsnumber" '
                    'FROM "Determination" WHERE "TaxonID"=$1 AND "IsCurrent" IS TRUE',
                    loser_taxon_id)
                retired = [d["DeterminationID"] for d in dets]
                inserted: List[int] = []
                primaries = [d["PrimaryID"] for d in dets]

                if retired:
                    await conn.execute(
                        'UPDATE "Determination" SET "IsCurrent"=false '
                        'WHERE "DeterminationID"=ANY($1::int[])', retired)
                    # copy-forward: same determiner, same date, same remarks. The merge marker
                    # only fills a Remarks that was empty, so no curatorial text is lost.
                    mark = MERGE_MARK.format(loser_taxon_id)[:50]
                    rows = await conn.fetch(
                        'INSERT INTO "Determination" '
                        '("PrimaryID","TaxonID","IsCurrent","Date1","DeterminerName","Remarks",'
                        ' "oldDeterminerID","Determiner","lotsnumber", merged_from_taxon_id) '
                        'SELECT x.pid, $1, true, x.d1, x.dn, '
                        '       CASE WHEN x.rm IS NULL OR btrim(x.rm)=\'\' THEN $2 ELSE x.rm END, '
                        '       x.odi, x.det, x.lot, $10 '
                        'FROM unnest($3::int[], $4::timestamp[], $5::text[], $6::text[], '
                        '            $7::int[], $8::int[], $9::int[]) '
                        '     AS x(pid, d1, dn, rm, odi, det, lot) '
                        'RETURNING "DeterminationID"',
                        winner_taxon_id, mark,
                        primaries,
                        [d["Date1"] for d in dets],
                        [d["DeterminerName"] for d in dets],
                        [d["Remarks"] for d in dets],
                        [d["oldDeterminerID"] for d in dets],
                        [d["Determiner"] for d in dets],
                        [d["lotsnumber"] for d in dets],
                        loser_taxon_id)
                    inserted = [r["DeterminationID"] for r in rows]

                # tag, never delete
                await conn.execute(
                    'UPDATE "TaxonomicTable" SET merged_into_taxon_id=$1 WHERE "TaxonID"=$2',
                    winner_taxon_id, loser_taxon_id)

                log_id = await conn.fetchval(
                    "INSERT INTO taxon_recheck_apply_log "
                    "(operation, review_id, old_taxon_id, new_taxon_id, old_name, new_name,"
                    " new_taxon_created, determinations_changed, retired_determination_ids,"
                    " inserted_determination_ids, affected_primary_ids, applied_by, notes, status) "
                    "VALUES ('taxon_merge',NULL,$1,$2,$3,$4,false,$5,$6,$7,$8,$9,$10,'applied') "
                    "RETURNING id",
                    loser_taxon_id, winner_taxon_id, pre["loser"]["name"], pre["winner"]["name"],
                    len(inserted), retired, inserted, primaries, who, notes)

                # the log id only exists now, so stamp it onto the rows just inserted
                if inserted:
                    await conn.execute(
                        'UPDATE "Determination" SET merge_log_id=$1 '
                        'WHERE "DeterminationID"=ANY($2::int[])', log_id, inserted)

                await tx.commit()
            except Exception as e:  # noqa: BLE001
                await tx.rollback()
                logger.exception("merge %s -> %s failed", loser_taxon_id, winner_taxon_id)
                return {"error": f"merge failed: {e}"}

        return {
            "apply_log_id": log_id,
            "operation": "taxon_merge",
            "winner_taxon_id": winner_taxon_id,
            "loser_taxon_id": loser_taxon_id,
            "determinations_moved": len(inserted),
            "affected_primaries": len(set(primaries)),
            "status": "applied",
        }

    # ---- undo ------------------------------------------------------------------------

    @staticmethod
    async def undo(apply_log_id: int, undone_by: str) -> Dict[str, Any]:
        log = await execute_single_query(
            "SELECT * FROM taxon_recheck_apply_log WHERE id=$1", apply_log_id)
        if not log:
            return {"error": "apply log not found"}
        if log["operation"] != "taxon_merge":
            return {"error": f"log {apply_log_id} is a '{log['operation']}', not a merge; "
                             f"undo it from the recheck apply history"}
        if log["status"] == "undone":
            return {"error": "already undone"}

        retired = log["retired_determination_ids"] or []
        inserted = log["inserted_determination_ids"] or []

        blocker = await undo_blocker(apply_log_id, inserted)
        if blocker:
            return {"error": blocker}

        async with get_db() as conn:
            tx = conn.transaction()
            await tx.start()
            try:
                if inserted:
                    await conn.execute(
                        'DELETE FROM "Determination" WHERE "DeterminationID"=ANY($1::int[])',
                        inserted)
                if retired:
                    await conn.execute(
                        'UPDATE "Determination" SET "IsCurrent"=true '
                        'WHERE "DeterminationID"=ANY($1::int[])', retired)
                await conn.execute(
                    'UPDATE "TaxonomicTable" SET merged_into_taxon_id=NULL WHERE "TaxonID"=$1',
                    log["old_taxon_id"])
                await conn.execute(
                    "UPDATE taxon_recheck_apply_log SET status='undone', undone_by=$1, "
                    "undone_at=NOW() WHERE id=$2", (undone_by or "")[:50], apply_log_id)
                await tx.commit()
            except Exception as e:  # noqa: BLE001
                await tx.rollback()
                logger.exception("merge undo failed for log %s", apply_log_id)
                return {"error": f"undo failed: {e}"}

        return {"apply_log_id": apply_log_id, "status": "undone",
                "determinations_restored": len(retired),
                "determinations_removed": len(inserted),
                "taxon_untagged": log["old_taxon_id"]}