"""Imported name -> the taxon a curator decided it means, reused across batches.

Why this exists. Anything the matcher can resolve is resolved at import and never reaches a
person. What reaches the curator is what the matcher could not resolve, and that judgement
used to be thrown away the moment the batch was finished -- the same misspelling in the next
delivery arrived as `no_match` again. See migrations/taxon_name_decision.sql for the full
reasoning and the measurements behind it.

Two rules shape everything here:

  1. What is recorded is the taxon the curator APPLIED, never the taxon the importer
     suggested. On every real cross-batch conflict measured, the suggestion was wrong and the
     applied taxon was right.

  2. A hit pre-fills, it does not verify. Reused decisions come back as `pending` with the
     answer filled in and the record marked, so a wrong old decision surfaces for review
     instead of quietly spreading through every future batch. That was the explicit condition
     for building this at all.
"""
import logging
import re
from typing import Any, Dict, List, Optional

from app.db.database import execute_query, execute_single_query, get_db

logger = logging.getLogger("name_decision_service")

_WS = re.compile(r"\s+")


def name_key(genus: Optional[str], species: Optional[str]) -> str:
    """The lookup key for an imported name.

    Must stay identical to NAME_KEY_SQL in name_group_service (lower, collapse whitespace,
    trim, genus + ' ' + species) -- a decision written by one and read by the other has to
    land on the same string. It is duplicated in SQL there because that side groups thousands
    of rows inside the query; this is the Python half of the same definition.
    """
    joined = f"{(genus or '').strip()} {(species or '').strip()}"
    return _WS.sub(" ", joined).strip().lower()


class NameDecisionService:

    # ---- writing -------------------------------------------------------------------------

    @staticmethod
    async def record(genus: Optional[str], species: Optional[str], taxon_id: int,
                     decided_by: str = "", source_batch: str = "",
                     source: str = "record_edit") -> Optional[Dict[str, Any]]:
        """Remember that this imported name means this taxon. Returns the row, or None if
        there is nothing worth remembering.

        Best-effort by design: this is called from the middle of the curator's save path, and
        failing to record a reusable decision must never fail the save itself.
        """
        key = name_key(genus, species)
        if not key or taxon_id is None:
            return None
        try:
            taxon = await execute_single_query(
                'SELECT "TaxonID", "FullScientificName" AS n FROM "TaxonomicTable" '
                'WHERE "TaxonID" = $1', taxon_id)
            if not taxon:
                return None

            existing = await execute_single_query(
                "SELECT * FROM taxon_name_decision WHERE name_key = $1 AND status = 'active'",
                key)

            if existing and existing["taxon_id"] == taxon_id:
                # same answer again -- nothing to change, and no reason to churn the audit
                # fields on every one of a thousand records carrying the name
                return dict(existing)

            if existing:
                # The curator is changing a previous answer. The old taxon is kept in
                # previous_taxon_id rather than overwritten, because this row now decides
                # what future imports get pre-filled with and a silent change to that is
                # exactly the "wrong old decision spreading" failure this design guards
                # against.
                row = await execute_single_query(
                    "UPDATE taxon_name_decision SET taxon_id = $1, taxon_name = $2, "
                    "  previous_taxon_id = taxon_id, updated_at = NOW(), updated_by = $3, "
                    "  source_batch = COALESCE(NULLIF($4, ''), source_batch), source = $5 "
                    "WHERE id = $6 RETURNING *",
                    taxon_id, taxon["n"], (decided_by or "")[:120], source_batch, source,
                    existing["id"])
                logger.info("name decision changed: '%s' %s -> %s by %s (%s)",
                            key, existing["taxon_id"], taxon_id, decided_by or "?", source)
                return dict(row) if row else None

            row = await execute_single_query(
                "INSERT INTO taxon_name_decision "
                "(name_key, verbatim_genus, verbatim_species, taxon_id, taxon_name, "
                " decided_by, source_batch, source) "
                "VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *",
                key, genus, species, taxon_id, taxon["n"], (decided_by or "")[:120],
                source_batch or None, source)
            logger.info("name decision recorded: '%s' -> %s '%s' by %s (%s)",
                        key, taxon_id, taxon["n"], decided_by or "?", source)
            return dict(row) if row else None
        except Exception as e:  # noqa: BLE001 - must never break the curator's save
            logger.warning("failed to record the name decision for '%s': %s", key, e)
            return None

    @staticmethod
    async def record_many(pairs: List[Dict[str, Any]], decided_by: str = "",
                          source_batch: str = "", source: str = "name_group") -> int:
        """Record several (genus, species, taxon_id) decisions; returns how many were stored.

        Applying a name group settles hundreds of records but is ONE decision about ONE name,
        so the caller passes one pair, not one per record.
        """
        stored = 0
        for p in pairs:
            if await NameDecisionService.record(
                    p.get("genus"), p.get("species"), p.get("taxon_id"),
                    decided_by=decided_by, source_batch=source_batch, source=source):
                stored += 1
        return stored

    # ---- reading -------------------------------------------------------------------------

    @staticmethod
    async def lookup(genus: Optional[str], species: Optional[str]) -> Optional[Dict[str, Any]]:
        """The active decision for this imported name, or None."""
        key = name_key(genus, species)
        if not key:
            return None
        try:
            row = await execute_single_query(
                "SELECT d.*, tt.\"FullScientificName\" AS current_taxon_name, "
                '       f."FamilyName" AS family '
                "FROM taxon_name_decision d "
                'LEFT JOIN "TaxonomicTable" tt ON tt."TaxonID" = d.taxon_id '
                'LEFT JOIN "Family" f ON f."FamilyID" = tt."FamilyID" '
                "WHERE d.name_key = $1 AND d.status = 'active'", key)
            return dict(row) if row else None
        except Exception as e:  # noqa: BLE001 - advisory path, never break an import
            logger.warning("name decision lookup failed for '%s': %s", key, e)
            return None

    @staticmethod
    async def lookup_many(keys: List[str]) -> Dict[str, Dict[str, Any]]:
        """Active decisions for many already-normalised keys, keyed by name_key.

        Import resolves a whole spreadsheet at once; one query for the batch, not one per row.
        """
        keys = [k for k in {k for k in keys if k}]
        if not keys:
            return {}
        try:
            rows = await execute_query(
                "SELECT d.*, tt.\"FullScientificName\" AS current_taxon_name "
                "FROM taxon_name_decision d "
                'LEFT JOIN "TaxonomicTable" tt ON tt."TaxonID" = d.taxon_id '
                "WHERE d.status = 'active' AND d.name_key = ANY($1::text[])", keys)
            return {r["name_key"]: dict(r) for r in rows}
        except Exception as e:  # noqa: BLE001
            logger.warning("bulk name decision lookup failed: %s", e)
            return {}

    @staticmethod
    async def mark_reused(decision_id: int, records: int) -> None:
        """Count records pre-filled from a decision, so the curator can see which entries
        are actually doing work."""
        if not decision_id or records <= 0:
            return
        try:
            await execute_query(
                "UPDATE taxon_name_decision SET times_reused = times_reused + $1, "
                "last_reused_at = NOW() WHERE id = $2", records, decision_id)
        except Exception as e:  # noqa: BLE001
            logger.warning("failed to count reuse of decision %s: %s", decision_id, e)

    @staticmethod
    async def listing(q: str = "", status: str = "active", limit: int = 200,
                      offset: int = 0) -> Dict[str, Any]:
        """The reference table itself, for the curator to read and correct."""
        where = ["1=1"]
        params: List[Any] = []
        if status and status != "all":
            params.append(status)
            where.append(f"d.status = ${len(params)}")
        if q:
            params.append(f"%{q.strip().lower()}%")
            where.append(f"(d.name_key LIKE ${len(params)} "
                         f"OR lower(COALESCE(d.taxon_name, '')) LIKE ${len(params)})")
        clause = " AND ".join(where)
        total = await execute_single_query(
            f"SELECT count(*) AS n FROM taxon_name_decision d WHERE {clause}", *params)
        params.extend([limit, offset])
        rows = await execute_query(
            "SELECT d.*, tt.\"FullScientificName\" AS current_taxon_name, "
            '       f."FamilyName" AS family, '
            "       prev.\"FullScientificName\" AS previous_taxon_name "
            "FROM taxon_name_decision d "
            'LEFT JOIN "TaxonomicTable" tt ON tt."TaxonID" = d.taxon_id '
            'LEFT JOIN "Family" f ON f."FamilyID" = tt."FamilyID" '
            'LEFT JOIN "TaxonomicTable" prev ON prev."TaxonID" = d.previous_taxon_id '
            f"WHERE {clause} "
            f"ORDER BY d.times_reused DESC, d.decided_at DESC "
            f"LIMIT ${len(params) - 1} OFFSET ${len(params)}", *params)
        return {"items": [dict(r) for r in rows],
                "total": total["n"] if total else 0}

    # ---- correcting ----------------------------------------------------------------------

    @staticmethod
    async def retire(decision_id: int, retired_by: str = "",
                     note: str = "") -> Dict[str, Any]:
        """Stop applying a decision to new imports, without erasing it.

        Records already pre-filled from it are left as they are: they were reviewed (or are
        waiting to be) on their own merits, and rewriting them from here would be a silent
        edit of data a curator may have since confirmed.
        """
        row = await execute_single_query(
            "UPDATE taxon_name_decision SET status = 'retired', retired_at = NOW(), "
            "  retired_by = $1, note = COALESCE(NULLIF($2, ''), note) "
            "WHERE id = $3 AND status = 'active' RETURNING *",
            (retired_by or "")[:120], note, decision_id)
        if not row:
            return {"error": "no active decision with that id"}
        used = await execute_single_query(
            "SELECT count(*) AS n FROM verbatim_taxonomic WHERE historical_decision_id = $1",
            decision_id)
        logger.info("name decision #%s retired by %s ('%s' -> %s); %s already pre-filled "
                    "records left untouched", decision_id, retired_by or "?",
                    row["name_key"], row["taxon_id"], used["n"] if used else 0)
        return {"decision": dict(row),
                "records_already_prefilled": used["n"] if used else 0}

    @staticmethod
    async def revise(decision_id: int, taxon_id: int, revised_by: str = "",
                     note: str = "") -> Dict[str, Any]:
        """Point an existing decision at a different taxon, keeping the old one visible."""
        taxon = await execute_single_query(
            'SELECT "TaxonID", "FullScientificName" AS n FROM "TaxonomicTable" '
            'WHERE "TaxonID" = $1', taxon_id)
        if not taxon:
            return {"error": f"taxon {taxon_id} not found"}
        async with get_db() as conn:
            row = await conn.fetchrow(
                "UPDATE taxon_name_decision SET previous_taxon_id = taxon_id, "
                "  taxon_id = $1, taxon_name = $2, updated_at = NOW(), updated_by = $3, "
                "  note = COALESCE(NULLIF($4, ''), note) "
                "WHERE id = $5 AND status = 'active' RETURNING *",
                taxon_id, taxon["n"], (revised_by or "")[:120], note, decision_id)
        if not row:
            return {"error": "no active decision with that id"}
        logger.info("name decision #%s revised by %s: '%s' -> %s '%s'",
                    decision_id, revised_by or "?", row["name_key"], taxon_id, taxon["n"])
        return {"decision": dict(row)}