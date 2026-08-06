"""
Genus-column repair report.

Read-only view of what the Genus repair changed in the curator's data, plus the handful of
rows that were deliberately NOT changed because they need a taxonomic judgement.

Background lives in migrations/genus_fix_audit.sql. In short: an old import wrote
genus+species+family markers into TaxonomicTable."Genus"; 811 rows (all Cyprinidae) were
rebuilt from FullScientificName, 9 were left for a human.
"""
from typing import Any, Dict, Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel

from app.db.database import (execute_paginated_query_with_count, execute_query,
                             execute_single_query, execute_mutation, get_db)

router = APIRouter()

# "Genus disagrees with the first token of FullScientificName" -- the live definition of a
# row that still needs a human. The repaired rows no longer match it.
UNRESOLVED = """(tt."Genus" IS NOT NULL AND btrim(tt."Genus") <> ''
                 AND lower(btrim(tt."Genus"))
                     <> lower(split_part(btrim(tt."FullScientificName"), ' ', 1)))"""

# ...and it has not already been settled by hand
UNDECIDED = """NOT EXISTS (SELECT 1 FROM genus_manual_decision md
                           WHERE md.taxon_id = tt."TaxonID" AND md.reverted_at IS NULL)"""


class ResponseModel(BaseModel):
    code: int
    data: Dict[str, Any] = {}
    message: Optional[str] = None


class RebuildGenusModel(BaseModel):
    """The NAME is right -> rebuild Genus from it."""
    decided_by: str
    note: Optional[str] = None


class RenameModel(BaseModel):
    """The GENUS is right -> correct FullScientificName to what the curator supplies."""
    new_full_name: str
    decided_by: str
    note: Optional[str] = None


class KeepAsIsModel(BaseModel):
    """Both columns are fine (placeholder, hybrid notation) -- just stop asking."""
    decided_by: str
    note: Optional[str] = None


class RevertModel(BaseModel):
    reverted_by: str


@router.get("/summary", response_model=ResponseModel)
async def summary():
    """Headline counts for the tab."""
    try:
        fixed = await execute_single_query(
            "SELECT count(*) AS total, count(DISTINCT family_name) AS families, "
            "min(fixed_at) AS fixed_at FROM genus_fix_audit")
        by_family = await execute_query(
            "SELECT coalesce(family_name,'(none)') AS family, count(*) AS n "
            "FROM genus_fix_audit GROUP BY 1 ORDER BY 2 DESC LIMIT 10")
        unresolved = await execute_single_query(
            f'SELECT count(*) AS n FROM "TaxonomicTable" tt '
            f'WHERE {UNRESOLVED} AND {UNDECIDED}')
        decided = await execute_single_query(
            "SELECT count(*) AS n FROM genus_manual_decision WHERE reverted_at IS NULL")
        return ResponseModel(code=20000, data={
            "fixed": fixed, "fixed_by_family": by_family,
            "unresolved": unresolved["n"] if unresolved else 0,
            "decided_manually": decided["n"] if decided else 0,
        })
    except Exception as e:  # noqa: BLE001
        return ResponseModel(code=50000, message=f"Failed to load summary: {e}")


@router.get("/fixed", response_model=ResponseModel)
async def fixed_rows(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    search: Optional[str] = Query(None, description="match name or genus"),
):
    """Every row whose Genus was rebuilt, with the value it had before."""
    try:
        where, params, i = [], [], 1
        if search:
            where.append(f"(full_name ILIKE ${i} OR genus_after ILIKE ${i} "
                         f"OR genus_before ILIKE ${i})")
            params.append(f"%{search}%")
            i += 1
        where_sql = (" WHERE " + " AND ".join(where)) if where else ""
        main = ("SELECT taxon_id, full_name, genus_before, genus_after, family_name, "
                f"fixed_at, fixed_by FROM genus_fix_audit{where_sql} ORDER BY taxon_id")
        count = f"SELECT COUNT(*) FROM genus_fix_audit{where_sql}"
        result = await execute_paginated_query_with_count(main, count, params, page, page_size)
        return ResponseModel(code=20000, data=result.get("data", {}))
    except Exception as e:  # noqa: BLE001
        return ResponseModel(code=50000, message=f"Failed to list repaired rows: {e}")


@router.get("/unresolved", response_model=ResponseModel)
async def unresolved_rows():
    """Rows where Genus and FullScientificName still disagree and the repair left them alone.

    These are NOT leftovers of the same corruption: in most of them the Genus column is the
    correct one and the name is the stale/misspelled one, so rebuilding Genus from the name
    would turn right into wrong. Each needs a taxonomic decision, which is what the three
    resolve endpoints below record.

    `rename_target_exists` says the corrected name is already on another taxon, i.e. renaming
    would create a duplicate -- those belong in the merge tool instead.
    """
    try:
        rows = await execute_query(f'''
            SELECT tt."TaxonID"            AS taxon_id,
                   tt."FullScientificName" AS full_name,
                   tt."Genus"              AS genus_now,
                   split_part(btrim(tt."FullScientificName"), ' ', 1) AS genus_if_rebuilt,
                   tt."Species"            AS species,
                   f."FamilyName"          AS family_name,
                   btrim(tt."Genus") || ' ' || coalesce(btrim(tt."Species"), '') AS rename_suggestion,
                   EXISTS (SELECT 1 FROM "TaxonomicTable" o
                           WHERE o."TaxonID" <> tt."TaxonID"
                             AND lower(btrim(o."FullScientificName")) =
                                 lower(btrim(tt."Genus") || ' ' || coalesce(btrim(tt."Species"), '')))
                       AS rename_target_exists,
                   (SELECT count(*) FROM "Determination" d
                    WHERE d."TaxonID" = tt."TaxonID" AND d."IsCurrent" IS TRUE) AS specimens
            FROM "TaxonomicTable" tt
            LEFT JOIN "Family" f ON f."FamilyID" = tt."FamilyID"
            WHERE {UNRESOLVED} AND {UNDECIDED}
            ORDER BY tt."TaxonID"''')
        return ResponseModel(code=20000, data={"items": rows, "total": len(rows)})
    except Exception as e:  # noqa: BLE001
        return ResponseModel(code=50000, message=f"Failed to list unresolved rows: {e}")


@router.get("/decisions", response_model=ResponseModel)
async def manual_decisions(include_reverted: bool = Query(False)):
    """What has been settled by hand, and by whom."""
    try:
        where = "" if include_reverted else " WHERE md.reverted_at IS NULL"
        rows = await execute_query(f'''
            SELECT md.*, tt."FullScientificName" AS current_name, tt."Genus" AS current_genus
            FROM genus_manual_decision md
            LEFT JOIN "TaxonomicTable" tt ON tt."TaxonID" = md.taxon_id{where}
            ORDER BY md.decided_at DESC''')
        return ResponseModel(code=20000, data={"items": rows, "total": len(rows)})
    except Exception as e:  # noqa: BLE001
        return ResponseModel(code=50000, message=f"Failed to list decisions: {e}")


async def _taxon(taxon_id: int):
    return await execute_single_query(
        'SELECT "TaxonID", "FullScientificName", "Genus", "Species" '
        'FROM "TaxonomicTable" WHERE "TaxonID" = $1', taxon_id)


async def _already_decided(taxon_id: int):
    return await execute_single_query(
        "SELECT id, decision FROM genus_manual_decision "
        "WHERE taxon_id = $1 AND reverted_at IS NULL", taxon_id)


@router.post("/unresolved/{taxon_id}/rebuild-genus", response_model=ResponseModel)
async def rebuild_genus(taxon_id: int, body: RebuildGenusModel):
    """The scientific name is the correct one: set Genus to its first word."""
    try:
        t = await _taxon(taxon_id)
        if not t:
            return ResponseModel(code=40400, message="Taxon not found")
        if await _already_decided(taxon_id):
            return ResponseModel(code=40000, message="This taxon has already been decided")
        new_genus = (t["FullScientificName"] or "").strip().split(" ")[0]
        if not new_genus:
            return ResponseModel(code=40000, message="The name has no first word to use")
        async with get_db() as conn:
            tx = conn.transaction()
            await tx.start()
            try:
                await conn.execute(
                    'UPDATE "TaxonomicTable" SET "Genus" = $1 WHERE "TaxonID" = $2',
                    new_genus, taxon_id)
                await conn.execute(
                    "INSERT INTO genus_manual_decision "
                    "(taxon_id, decision, field, value_before, value_after, note, decided_by) "
                    "VALUES ($1,'rebuild_genus','Genus',$2,$3,$4,$5)",
                    taxon_id, t["Genus"], new_genus, body.note, body.decided_by[:120])
                await tx.commit()
            except Exception:
                await tx.rollback()
                raise
        return ResponseModel(code=20000,
                             data={"taxon_id": taxon_id, "genus_before": t["Genus"],
                                   "genus_after": new_genus},
                             message=f"Genus set to '{new_genus}'")
    except Exception as e:  # noqa: BLE001
        return ResponseModel(code=50000, message=f"Rebuild failed: {e}")


@router.post("/unresolved/{taxon_id}/rename", response_model=ResponseModel)
async def rename_taxon(taxon_id: int, body: RenameModel):
    """The Genus column is the correct one: correct the scientific name to match.

    Refuses when the target name already exists on another taxon -- that is a duplicate to be
    merged (so the specimens end up together), not a rename.
    """
    try:
        t = await _taxon(taxon_id)
        if not t:
            return ResponseModel(code=40400, message="Taxon not found")
        if await _already_decided(taxon_id):
            return ResponseModel(code=40000, message="This taxon has already been decided")
        new_name = (body.new_full_name or "").strip()
        if len(new_name.split()) < 2:
            return ResponseModel(code=40000, message="Give a full name, e.g. 'Lepomis gulosus'")
        clash = await execute_single_query(
            'SELECT "TaxonID" FROM "TaxonomicTable" '
            'WHERE "TaxonID" <> $1 AND lower(btrim("FullScientificName")) = lower($2)',
            taxon_id, new_name)
        if clash:
            return ResponseModel(
                code=40000,
                message=f"'{new_name}' already exists as taxon {clash['TaxonID']}. Renaming "
                        f"would create a duplicate — merge the two in Taxon Data Quality "
                        f"instead, so the specimens end up on one taxon.")
        async with get_db() as conn:
            tx = conn.transaction()
            await tx.start()
            try:
                await conn.execute(
                    'UPDATE "TaxonomicTable" SET "FullScientificName" = $1 WHERE "TaxonID" = $2',
                    new_name, taxon_id)
                await conn.execute(
                    "INSERT INTO genus_manual_decision "
                    "(taxon_id, decision, field, value_before, value_after, note, decided_by) "
                    "VALUES ($1,'rename','FullScientificName',$2,$3,$4,$5)",
                    taxon_id, t["FullScientificName"], new_name, body.note,
                    body.decided_by[:120])
                await tx.commit()
            except Exception:
                await tx.rollback()
                raise
        return ResponseModel(code=20000,
                             data={"taxon_id": taxon_id, "name_before": t["FullScientificName"],
                                   "name_after": new_name},
                             message=f"Name corrected to '{new_name}'")
    except Exception as e:  # noqa: BLE001
        return ResponseModel(code=50000, message=f"Rename failed: {e}")


@router.post("/unresolved/{taxon_id}/keep-as-is", response_model=ResponseModel)
async def keep_as_is(taxon_id: int, body: KeepAsIsModel):
    """Both columns are fine as they are; record that and stop listing the row."""
    try:
        t = await _taxon(taxon_id)
        if not t:
            return ResponseModel(code=40400, message="Taxon not found")
        if await _already_decided(taxon_id):
            return ResponseModel(code=40000, message="This taxon has already been decided")
        await execute_mutation(
            "INSERT INTO genus_manual_decision "
            "(taxon_id, decision, field, value_before, value_after, note, decided_by) "
            "VALUES ($1,'keep_as_is',NULL,$2,$2,$3,$4)",
            taxon_id, t["Genus"], body.note, body.decided_by[:120])
        return ResponseModel(code=20000, data={"taxon_id": taxon_id},
                             message="Recorded — nothing was changed")
    except Exception as e:  # noqa: BLE001
        return ResponseModel(code=50000, message=f"Failed to record: {e}")


@router.post("/decisions/{decision_id}/revert", response_model=ResponseModel)
async def revert_decision(decision_id: int, body: RevertModel):
    """Undo a manual decision, putting the edited column back and re-listing the row."""
    try:
        d = await execute_single_query(
            "SELECT * FROM genus_manual_decision WHERE id = $1", decision_id)
        if not d:
            return ResponseModel(code=40400, message="Decision not found")
        if d["reverted_at"] is not None:
            return ResponseModel(code=40000, message="Already reverted")
        async with get_db() as conn:
            tx = conn.transaction()
            await tx.start()
            try:
                if d["field"] == "Genus":
                    await conn.execute(
                        'UPDATE "TaxonomicTable" SET "Genus" = $1 WHERE "TaxonID" = $2',
                        d["value_before"], d["taxon_id"])
                elif d["field"] == "FullScientificName":
                    await conn.execute(
                        'UPDATE "TaxonomicTable" SET "FullScientificName" = $1 '
                        'WHERE "TaxonID" = $2', d["value_before"], d["taxon_id"])
                await conn.execute(
                    "UPDATE genus_manual_decision SET reverted_at = NOW(), reverted_by = $1 "
                    "WHERE id = $2", body.reverted_by[:120], decision_id)
                await tx.commit()
            except Exception:
                await tx.rollback()
                raise
        return ResponseModel(code=20000, data={"decision_id": decision_id, "status": "reverted"},
                             message="Decision reverted")
    except Exception as e:  # noqa: BLE001
        return ResponseModel(code=50000, message=f"Revert failed: {e}")