"""The reference table of name decisions: what the curator has ruled an imported name means.

Read, correct and revoke. Nothing here creates entries -- those are written as a side effect
of the curator deciding a record (record editor, apply-suggestion, apply-by-name), because a
decision is only worth reusing if it was actually applied to specimens.

Revocability is the point of these endpoints, not a convenience: an entry pre-fills the answer
into every future batch carrying that spelling, so a wrong one left in place is worse than no
table at all. See migrations/taxon_name_decision.sql.
"""
import logging

from fastapi import APIRouter, Query
from pydantic import BaseModel
from typing import Any, Dict, Optional

from app.db.database import execute_query
from app.services.name_decision_service import NameDecisionService

router = APIRouter()
logger = logging.getLogger("name_decision_api")
service = NameDecisionService()


# Same shape the other endpoint modules declare locally (family_policy, batch_review): data is
# required, so a response that omits it turns into a 500.
class ResponseModel(BaseModel):
    code: int
    data: Dict[str, Any] = {}
    message: Optional[str] = None


class RetireModel(BaseModel):
    retired_by: str = ""
    note: str = ""


class ReviseModel(BaseModel):
    taxon_id: int
    revised_by: str = ""
    note: str = ""


@router.get("/decisions", response_model=ResponseModel)
async def list_decisions(
    q: str = Query("", description="filter by imported name or taxon name"),
    status: str = Query("active", description="active | retired | all"),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Every recorded name decision, the ones actually being reused first."""
    try:
        return ResponseModel(code=20000, data=await service.listing(
            q=q, status=status, limit=limit, offset=offset))
    except Exception as e:  # noqa: BLE001
        logger.exception("failed to list name decisions")
        return ResponseModel(code=50000, message=f"Failed to list the decisions: {e}")


@router.get("/decisions/stats", response_model=ResponseModel)
async def decision_stats():
    """Totals for the page header: how many rulings exist and how much work they saved."""
    try:
        rows = await execute_query(
            "SELECT count(*) FILTER (WHERE status = 'active') AS active, "
            "       count(*) FILTER (WHERE status = 'retired') AS retired, "
            "       COALESCE(sum(times_reused) FILTER (WHERE status = 'active'), 0) "
            "         AS records_prefilled, "
            "       count(*) FILTER (WHERE status = 'active' AND times_reused > 0) AS reused "
            "FROM taxon_name_decision")
        return ResponseModel(code=20000, data=dict(rows[0]) if rows else {})
    except Exception as e:  # noqa: BLE001
        return ResponseModel(code=50000, message=f"Failed to load the totals: {e}")


@router.get("/decisions/{decision_id}/records", response_model=ResponseModel)
async def decision_records(decision_id: int, limit: int = Query(100, ge=1, le=1000)):
    """The records this decision pre-filled, so a suspect ruling can be traced to what it
    touched before it is revoked."""
    try:
        rows = await execute_query(
            'SELECT p."PrimaryID", p.batch_serial_id, p."CatalogNumber", p."TaxonID", '
            "       coalesce(p.species_verification_status, 'pending') AS species_status, "
            "       vt.verbatim_genus, vt.verbatim_species "
            "FROM verbatim_taxonomic vt "
            'JOIN primary_temp p ON p."verbatim_taxonid" = vt."verbatim_taxonid" '
            "WHERE vt.historical_decision_id = $1 "
            'ORDER BY p."PrimaryID" LIMIT $2', decision_id, limit)
        return ResponseModel(code=20000, data={"items": [dict(r) for r in rows],
                                               "total": len(rows)})
    except Exception as e:  # noqa: BLE001
        return ResponseModel(code=50000, message=f"Failed to list the records: {e}")


@router.post("/decisions/{decision_id}/retire", response_model=ResponseModel)
async def retire_decision(decision_id: int, body: RetireModel):
    """Stop applying this decision to new imports. The entry is kept, and records already
    pre-filled from it are left alone."""
    try:
        result = await service.retire(decision_id, body.retired_by, body.note)
        if "error" in result:
            return ResponseModel(code=40000, message=result["error"])
        msg = "No longer applied to new imports"
        if result["records_already_prefilled"]:
            msg += (f"; {result['records_already_prefilled']} record(s) already pre-filled "
                    f"from it are unchanged")
        return ResponseModel(code=20000, data=result, message=msg)
    except Exception as e:  # noqa: BLE001
        return ResponseModel(code=50000, message=f"Failed to retire the decision: {e}")


@router.post("/decisions/{decision_id}/revise", response_model=ResponseModel)
async def revise_decision(decision_id: int, body: ReviseModel):
    """Point this decision at a different taxon. The previous answer stays visible on the row."""
    try:
        result = await service.revise(decision_id, body.taxon_id, body.revised_by, body.note)
        if "error" in result:
            return ResponseModel(code=40000, message=result["error"])
        return ResponseModel(code=20000, data=result,
                             message="Future imports will use the new taxon")
    except Exception as e:  # noqa: BLE001
        return ResponseModel(code=50000, message=f"Failed to revise the decision: {e}")


@router.get("/decisions/lookup", response_model=ResponseModel)
async def lookup_decision(genus: str = Query(""), species: str = Query("")):
    """What (if anything) has been decided for one imported name."""
    try:
        found = await service.lookup(genus, species)
        return ResponseModel(code=20000, data={"decision": found})
    except Exception as e:  # noqa: BLE001
        return ResponseModel(code=50000, message=f"Failed to look up the name: {e}")