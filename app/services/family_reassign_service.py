"""Curator-driven family reassignment: move chosen taxa to a family the curator picks.

Why this exists. The family-disagreement tab can only answer "ours is right" (keep_local) or
"CoF is right, some day" (adopt_reference, which moves nothing). Neither helps when both
families are wrong -- and that is the residue nobody has handled: fix_family_mismatch.py
(2026-06-10) already auto-moved the 79 disagreements it could decide mechanically, but its
target was always CoF's suggestion, and it deliberately skipped the 146 same-order splits as
the curator's call. This service is the same move with the target supplied by a person.

What it touches. Exactly one column: TaxonomicTable."FamilyID". "Determination" is not read or
written -- a specimen keeps its identification, and the taxon it points at simply lands in a
different family. That is what makes the operation cheap to undo.

Provenance. Per-taxon before/after goes into family_fix_audit, the same table the 2026-06-10
script wrote, so one query answers "what was this taxon's family before" whichever route
changed it. The curator action itself is one family_reassign_op row, which is what undo
targets. See migrations/family_reassign.sql.
"""
import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional

from app.db.database import execute_query, execute_single_query, get_db
from app.db.taxon_database import execute_taxon_query, is_taxon_db_configured

logger = logging.getLogger("family_reassign_service")

CREATED_VIA = "curator_reassign"
AUDIT_CATEGORY = "CURATOR REASSIGN"


class FamilyReassignService:

    # ---- shared lookups ------------------------------------------------------------------

    @staticmethod
    async def _taxa_rows(taxon_ids: List[int]) -> List[Dict[str, Any]]:
        """The taxa being moved, with their current family and specimen load.

        Specimens come from Determination(IsCurrent), not Primary."TaxonID" -- the latter is
        almost entirely empty in this database.
        """
        return await execute_query(
            'SELECT tt."TaxonID", btrim(tt."Genus") AS genus, tt."Species" AS species, '
            '       tt."FullScientificName" AS full_name, tt."FamilyID" AS family_id, '
            '       f."FamilyName" AS family_name, '
            '  (SELECT count(*) FROM "Determination" d '
            '   WHERE d."TaxonID" = tt."TaxonID" AND d."IsCurrent" IS TRUE) AS specimens '
            'FROM "TaxonomicTable" tt LEFT JOIN "Family" f ON f."FamilyID" = tt."FamilyID" '
            'WHERE tt."TaxonID" = ANY($1::int[]) '
            'ORDER BY tt."FullScientificName"', taxon_ids)

    @staticmethod
    async def _reference_families(genera: List[str]) -> Dict[str, List[str]]:
        """genus (lowercased) -> the families CoF files it under."""
        if not genera or not is_taxon_db_configured():
            return {}
        rows = await execute_taxon_query(
            "SELECT g.scientific_name AS genus, fam.scientific_name AS fam "
            "FROM taxa g JOIN taxa fam ON g.parent_id = fam.id AND fam.rank = 'FAMILY' "
            "WHERE g.rank = 'GENUS' AND g.status = 'valid' "
            "AND lower(g.scientific_name) = ANY($1::text[])",
            sorted({g.lower() for g in genera if g}))
        out = defaultdict(set)
        for r in rows:
            out[r["genus"].lower()].add(r["fam"])
        return {k: sorted(v) for k, v in out.items()}

    @staticmethod
    async def _resolve_target(target_family_id: Optional[int],
                              target_family_name: Optional[str]) -> Dict[str, Any]:
        """Find the target family by id, or by name, or report that it would be created.

        Name lookup is case/whitespace insensitive because the legacy Family table is
        inconsistently cased (POTAMOTRYGONIDAE next to Potamotrygonidae) and a curator typing
        an existing family must not silently create a second row for it.
        """
        if target_family_id:
            row = await execute_single_query(
                'SELECT "FamilyID", "FamilyName" FROM "Family" WHERE "FamilyID" = $1',
                target_family_id)
            if not row:
                return {"error": f"family {target_family_id} not found"}
            if not (row["FamilyName"] or "").strip():
                # FamilyID 1 and 5 are name-less shells; moving taxa onto one would drop them
                # out of every family-keyed report with no visible cause.
                return {"error": f"family {target_family_id} has no name; pick another"}
            return {"family_id": row["FamilyID"], "family_name": row["FamilyName"],
                    "exists": True}

        name = (target_family_name or "").strip()
        if not name:
            return {"error": "a target family (id or name) is required"}
        row = await execute_single_query(
            'SELECT "FamilyID", "FamilyName" FROM "Family" '
            'WHERE lower(btrim("FamilyName")) = lower($1) ORDER BY "FamilyID" LIMIT 1', name)
        if row:
            return {"family_id": row["FamilyID"], "family_name": row["FamilyName"],
                    "exists": True}
        return {"family_id": None, "family_name": name, "exists": False}

    # ---- preview -------------------------------------------------------------------------

    async def preview(self, taxon_ids: List[int], target_family_id: Optional[int] = None,
                      target_family_name: Optional[str] = None) -> Dict[str, Any]:
        """What the move would do, including the warnings it would create.

        The last part matters: moving to a family CoF still disagrees with does not end the
        argument, it renames it. The curator should see that before committing, not discover it
        when the same records keep being held back.
        """
        taxon_ids = sorted({int(t) for t in (taxon_ids or [])})
        if not taxon_ids:
            return {"error": "no taxa selected"}

        target = await self._resolve_target(target_family_id, target_family_name)
        if "error" in target:
            return target

        rows = await self._taxa_rows(taxon_ids)
        found = {r["TaxonID"] for r in rows}
        missing = [t for t in taxon_ids if t not in found]

        moving, already = [], []
        for r in rows:
            (already if target["exists"] and r["family_id"] == target["family_id"]
             else moving).append(dict(r))

        ref = await self._reference_families([r["genus"] for r in moving if r["genus"]])
        # Which (target family -> CoF family) pairs would start warning after the move?
        new_pairs = set()
        for r in moving:
            fams = ref.get((r["genus"] or "").lower(), [])
            for f in fams:
                if f.lower() != target["family_name"].lower():
                    new_pairs.add(f)

        existing_rulings = set()
        if new_pairs:
            for r in await execute_query(
                    "SELECT reference_family FROM family_reference_policy "
                    "WHERE revoked_at IS NULL AND decision = 'keep_local' "
                    "AND lower(local_family) = lower($1) "
                    "AND lower(reference_family) = ANY($2::text[])",
                    target["family_name"], [p.lower() for p in new_pairs]):
                existing_rulings.add(r["reference_family"].lower())

        return {
            "target": target,
            "moving": moving,
            "already_in_target": already,
            "missing_taxon_ids": missing,
            "taxa_count": len(moving),
            "specimens_count": sum(r["specimens"] for r in moving),
            "families_left": sorted({r["family_name"] for r in moving if r["family_name"]}),
            # Pairs the move would silence on the curator's behalf. Shown, never hidden: it is
            # a second decision riding on the first one.
            "rulings_to_create": sorted(p for p in new_pairs
                                        if p.lower() not in existing_rulings),
            "rulings_already_present": sorted(p for p in new_pairs
                                              if p.lower() in existing_rulings),
        }

    # ---- apply ---------------------------------------------------------------------------

    async def reassign(self, taxon_ids: List[int], performed_by: str,
                       target_family_id: Optional[int] = None,
                       target_family_name: Optional[str] = None,
                       source_local_family: Optional[str] = None,
                       source_reference_family: Optional[str] = None,
                       note: Optional[str] = None) -> Dict[str, Any]:
        pre = await self.preview(taxon_ids, target_family_id, target_family_name)
        if "error" in pre:
            return pre
        if not pre["moving"]:
            return {"error": "nothing to move: every selected taxon is already in that family"}

        who = (performed_by or "")[:120]
        target = pre["target"]
        moving = pre["moving"]

        async with get_db() as conn:
            tx = conn.transaction()
            await tx.start()
            try:
                family_id = target["family_id"]
                created = False
                if not target["exists"]:
                    family_id = await conn.fetchval(
                        'INSERT INTO "Family" ("FamilyName", created_at, created_via) '
                        'VALUES ($1, NOW(), $2) RETURNING "FamilyID"',
                        target["family_name"], CREATED_VIA)
                    created = True

                op_id = await conn.fetchval(
                    "INSERT INTO family_reassign_op "
                    "(performed_by, target_family_id, target_family_name, target_family_created,"
                    " source_local_family, source_reference_family, taxa_count, specimens_count,"
                    " note) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id",
                    who, family_id, target["family_name"], created,
                    source_local_family, source_reference_family,
                    len(moving), pre["specimens_count"], note)

                for r in moving:
                    await conn.execute(
                        "INSERT INTO family_fix_audit "
                        "(taxon_id, genus, species, full_name, category, old_family_id,"
                        " old_family_name, new_family_id, new_family_name, created_new_family,"
                        " usage_count, reassign_op_id, performed_by) "
                        "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
                        r["TaxonID"], r["genus"], r["species"], r["full_name"], AUDIT_CATEGORY,
                        r["family_id"], r["family_name"], family_id, target["family_name"],
                        created, r["specimens"], op_id, who)

                await conn.execute(
                    'UPDATE "TaxonomicTable" SET "FamilyID" = $1 WHERE "TaxonID" = ANY($2::int[])',
                    family_id, [r["TaxonID"] for r in moving])

                # Silence the argument the move relocates rather than ends.
                ruling_ids: List[int] = []
                for ref_fam in pre["rulings_to_create"]:
                    rid = await conn.fetchval(
                        "INSERT INTO family_reference_policy "
                        "(local_family, reference_family, decision, note, created_by,"
                        " reassign_op_id) VALUES ($1,$2,'keep_local',$3,$4,$5) "
                        "ON CONFLICT DO NOTHING RETURNING id",
                        target["family_name"], ref_fam,
                        f"auto-recorded with family move #{op_id}: "
                        f"{len(moving)} taxa moved to {target['family_name']}",
                        who, op_id)
                    if rid:
                        ruling_ids.append(rid)
                if ruling_ids:
                    await conn.execute(
                        "UPDATE family_reassign_op SET created_ruling_ids = $1 WHERE id = $2",
                        ruling_ids, op_id)

                await tx.commit()
            except Exception as e:  # noqa: BLE001
                await tx.rollback()
                logger.exception("family reassign to %s failed", target["family_name"])
                return {"error": f"reassign failed: {e}"}

        logger.info("family reassign #%s by %s: %s taxa -> %s (family_id=%s, created=%s), "
                    "%s specimens, %s auto rulings",
                    op_id, who, len(moving), target["family_name"], family_id, created,
                    pre["specimens_count"], len(ruling_ids))
        return {
            "op_id": op_id,
            "target_family_id": family_id,
            "target_family_name": target["family_name"],
            "target_family_created": created,
            "taxa_moved": len(moving),
            "specimens_affected": pre["specimens_count"],
            "rulings_created": ruling_ids,
            "status": "applied",
        }

    # ---- history / undo ------------------------------------------------------------------

    @staticmethod
    async def history(limit: int = 50) -> List[Dict[str, Any]]:
        return await execute_query(
            "SELECT o.*, "
            "  (SELECT count(*) FROM family_fix_audit a WHERE a.reassign_op_id = o.id) "
            "    AS audit_rows "
            "FROM family_reassign_op o ORDER BY o.performed_at DESC LIMIT $1", limit)

    @staticmethod
    async def operation_taxa(op_id: int) -> List[Dict[str, Any]]:
        """The per-taxon detail of one move, straight from the audit table."""
        return await execute_query(
            "SELECT taxon_id, full_name, genus, species, old_family_id, old_family_name, "
            "       new_family_id, new_family_name, usage_count "
            "FROM family_fix_audit WHERE reassign_op_id = $1 ORDER BY full_name", op_id)

    async def undo(self, op_id: int, undone_by: str) -> Dict[str, Any]:
        op = await execute_single_query(
            "SELECT * FROM family_reassign_op WHERE id = $1", op_id)
        if not op:
            return {"error": "operation not found"}
        if op["status"] == "undone":
            return {"error": "already undone"}

        audit = await execute_query(
            "SELECT taxon_id, old_family_id, old_family_name, full_name "
            "FROM family_fix_audit WHERE reassign_op_id = $1", op_id)
        if not audit:
            return {"error": "no audit rows for this operation; refusing to guess"}

        # Refuse if anything moved again afterwards -- restoring would silently overwrite a
        # later decision, which is exactly the kind of change that must never happen quietly.
        current = {r["TaxonID"]: r["FamilyID"] for r in await execute_query(
            'SELECT "TaxonID", "FamilyID" FROM "TaxonomicTable" '
            'WHERE "TaxonID" = ANY($1::int[])', [a["taxon_id"] for a in audit])}
        moved_on = [a for a in audit
                    if current.get(a["taxon_id"]) != op["target_family_id"]]
        if moved_on:
            names = ", ".join(str(a["full_name"] or a["taxon_id"]) for a in moved_on[:5])
            return {"error": f"{len(moved_on)} of these taxa have been moved again since "
                             f"(e.g. {names}); undo the later change first"}

        restorable = [a for a in audit if a["old_family_id"] is not None]
        async with get_db() as conn:
            tx = conn.transaction()
            await tx.start()
            try:
                by_family: Dict[int, List[int]] = defaultdict(list)
                for a in restorable:
                    by_family[a["old_family_id"]].append(a["taxon_id"])
                for fid, ids in by_family.items():
                    await conn.execute(
                        'UPDATE "TaxonomicTable" SET "FamilyID" = $1 '
                        'WHERE "TaxonID" = ANY($2::int[])', fid, ids)

                # Revoke only the rulings this move wrote; a keep_local the curator made by
                # hand for the same pair is a separate decision and stays.
                ruling_ids = list(op["created_ruling_ids"] or [])
                if ruling_ids:
                    await conn.execute(
                        "UPDATE family_reference_policy SET revoked_at = NOW(), revoked_by = $1,"
                        " revoke_reason = $2 WHERE id = ANY($3::int[]) AND revoked_at IS NULL",
                        (undone_by or "")[:120],
                        f"family move #{op_id} was undone", ruling_ids)

                await conn.execute(
                    "UPDATE family_reassign_op SET status = 'undone', undone_at = NOW(), "
                    "undone_by = $1 WHERE id = $2", (undone_by or "")[:120], op_id)
                await tx.commit()
            except Exception as e:  # noqa: BLE001
                await tx.rollback()
                logger.exception("undo of family reassign #%s failed", op_id)
                return {"error": f"undo failed: {e}"}

        logger.info("family reassign #%s undone by %s: %s taxa restored, %s rulings revoked",
                    op_id, undone_by, len(restorable), len(op["created_ruling_ids"] or []))
        return {
            "op_id": op_id,
            "taxa_restored": len(restorable),
            "rulings_revoked": len(op["created_ruling_ids"] or []),
            # A family created for the move is kept: it is inert once empty, and deleting it
            # would orphan the audit rows that name it.
            "target_family_kept": bool(op["target_family_created"]),
            "status": "undone",
        }