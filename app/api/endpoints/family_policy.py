"""
Family-classification rulings API.

One decision per (local family -> reference family) disagreement, recorded once for the
whole museum instead of being re-confirmed on every batch record. See
app/services/family_policy_service.py for why, and migrations/family_reference_policy.sql
for the numbers that motivated it.

The /reassign endpoints are the exception and are marked as such: they DO edit taxonomy
(TaxonomicTable."FamilyID"), because "neither our family nor the Catalog's is right" cannot be
answered by recording an opinion. They live here anyway -- the curator reaches them from the
same disagreement row, and splitting them off would only make that one decision span two APIs.
See app/services/family_reassign_service.py.
"""
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel

from app.services.family_policy_service import FamilyPolicyService
from app.services.family_reassign_service import FamilyReassignService

router = APIRouter()
service = FamilyPolicyService()
reassign_service = FamilyReassignService()


class ResponseModel(BaseModel):
    code: int
    data: Dict[str, Any] = {}
    message: Optional[str] = None


class RulingModel(BaseModel):
    local_family: str
    reference_family: str
    # keep_local     -> suppress this pair's warning (the museum keeps its family)
    # adopt_reference-> recorded intent to move; does NOT suppress, because moving a family
    #                   is a separate bulk edit of TaxonomicTable."FamilyID"
    decision: str = "keep_local"
    note: Optional[str] = None
    created_by: str


class RevokeModel(BaseModel):
    revoked_by: str
    reason: Optional[str] = None


@router.get("/disagreements", response_model=ResponseModel)
async def list_disagreements(
    include_covered: bool = Query(False,
                                  description="also return pairs already ruled on"),
):
    """The decision list: one row per (local family -> reference family) pair, with how
    many taxa and current determinations ride on it, plus sample genera."""
    try:
        result = await service.disagreements(include_covered=include_covered)
        if "error" in result:
            return ResponseModel(code=40000, message=result["error"])
        return ResponseModel(code=20000, data=result)
    except Exception as e:  # noqa: BLE001
        return ResponseModel(code=50000, message=f"Failed to list disagreements: {e}")


@router.get("/disagreements/taxa", response_model=ResponseModel)
async def disagreement_taxa(
    local_family: str = Query(..., description="the family we file them under"),
    reference_family: str = Query(..., description="the family the reference puts them in"),
):
    """The actual taxa behind one disagreement row: genus, species, specimen count.

    Without this the curator is asked to rule on two family names in the abstract.
    """
    try:
        result = await service.affected_taxa(local_family, reference_family)
        if "error" in result:
            return ResponseModel(code=40000, message=result["error"])
        return ResponseModel(code=20000, data=result)
    except Exception as e:  # noqa: BLE001
        return ResponseModel(code=50000, message=f"Failed to list affected taxa: {e}")


@router.get("", response_model=ResponseModel)
async def list_rulings(
    include_revoked: bool = Query(False, description="also return revoked rulings"),
    with_coverage: bool = Query(True, description="include how many taxa/specimens each covers"),
):
    """Current rulings. Revoked ones are kept and can be shown -- who exempted what and
    when is part of the record. Coverage says what each ruling is holding down."""
    try:
        rows = await service.list_rulings(include_revoked=include_revoked,
                                          with_coverage=with_coverage)
        return ResponseModel(code=20000, data={
            "items": rows,
            "total": len(rows),
            "covered_taxa": sum(r.get("taxa", 0) for r in rows if not r["revoked_at"]),
            "covered_determinations": sum(r.get("determinations", 0)
                                          for r in rows if not r["revoked_at"]),
        })
    except Exception as e:  # noqa: BLE001
        return ResponseModel(code=50000, message=f"Failed to list rulings: {e}")


@router.post("", response_model=ResponseModel)
async def add_ruling(body: RulingModel):
    """Record a ruling for one pair."""
    try:
        result = await service.add_ruling(
            body.local_family, body.reference_family, body.decision,
            body.note, body.created_by)
        if "error" in result:
            return ResponseModel(code=40000, message=result["error"])
        verb = ("suppresses the warning" if body.decision == "keep_local"
                else "recorded as intent only; the warning stays until the family is moved")
        return ResponseModel(code=20000, data=result,
                             message=f"Ruling saved -- {verb}")
    except Exception as e:  # noqa: BLE001
        return ResponseModel(code=50000, message=f"Failed to add ruling: {e}")


@router.post("/{ruling_id}/revoke", response_model=ResponseModel)
async def revoke_ruling(ruling_id: int, body: RevokeModel):
    """Revoke a ruling (soft): the row stays, and the pair starts warning again."""
    try:
        result = await service.revoke_ruling(ruling_id, body.revoked_by, body.reason)
        if "error" in result:
            return ResponseModel(code=40000, message=result["error"])
        return ResponseModel(code=20000, data=result,
                             message="Ruling revoked; this pair will warn again")
    except Exception as e:  # noqa: BLE001
        return ResponseModel(code=50000, message=f"Failed to revoke ruling: {e}")


# ---------------------------------------------------------------------------------------
# Reassignment: the "neither is right" answer. These change data.
# ---------------------------------------------------------------------------------------

class ReassignModel(BaseModel):
    taxon_ids: List[int]
    # Exactly one of the two: an existing family, or a name to create.
    target_family_id: Optional[int] = None
    target_family_name: Optional[str] = None
    # The disagreement row the curator came from -- context for the history list only.
    source_local_family: Optional[str] = None
    source_reference_family: Optional[str] = None
    note: Optional[str] = None
    performed_by: str


class UndoReassignModel(BaseModel):
    undone_by: str


@router.get("/taxon/{taxon_id}", response_model=ResponseModel)
async def taxon_for_move(taxon_id: int):
    """One taxon with its current family and specimen load, for opening the move dialog on it
    directly.

    The disagreement list can only be entered per (local family -> reference family) pair, but
    a curator arriving from batch review has a record in front of them, not a pair -- and the
    family they think is wrong may not disagree with the Catalog at all, in which case no pair
    row exists to click. This is the way in for that case.
    """
    try:
        rows = await reassign_service._taxa_rows([taxon_id])  # noqa: SLF001 - same package
        if not rows:
            return ResponseModel(code=40400, message=f"Taxon {taxon_id} not found")
        return ResponseModel(code=20000, data={"taxon": dict(rows[0])})
    except Exception as e:  # noqa: BLE001
        return ResponseModel(code=50000, message=f"Failed to load the taxon: {e}")


@router.post("/reassign/preview", response_model=ResponseModel)
async def preview_reassign(body: ReassignModel):
    """What the move would do -- including which taxa are already in the target family, and
    which NEW disagreements it would create and silence on the curator's behalf."""
    try:
        result = await reassign_service.preview(
            body.taxon_ids, body.target_family_id, body.target_family_name)
        if "error" in result:
            return ResponseModel(code=40000, message=result["error"])
        return ResponseModel(code=20000, data=result)
    except Exception as e:  # noqa: BLE001
        return ResponseModel(code=50000, message=f"Failed to preview the move: {e}")


@router.post("/reassign", response_model=ResponseModel)
async def reassign(body: ReassignModel):
    """Move the selected taxa to the chosen family.

    Writes TaxonomicTable."FamilyID" and nothing else; "Determination" is untouched, so no
    specimen is re-identified. Every taxon's previous family is recorded in family_fix_audit
    and the whole action is undoable.
    """
    try:
        result = await reassign_service.reassign(
            taxon_ids=body.taxon_ids,
            performed_by=body.performed_by,
            target_family_id=body.target_family_id,
            target_family_name=body.target_family_name,
            source_local_family=body.source_local_family,
            source_reference_family=body.source_reference_family,
            note=body.note)
        if "error" in result:
            return ResponseModel(code=40000, message=result["error"])
        msg = (f"{result['taxa_moved']} taxa moved to {result['target_family_name']}"
               f"{' (family created)' if result['target_family_created'] else ''}")
        if result["rulings_created"]:
            msg += f"; {len(result['rulings_created'])} follow-up warning(s) silenced"
        return ResponseModel(code=20000, data=result, message=msg)
    except Exception as e:  # noqa: BLE001
        return ResponseModel(code=50000, message=f"Failed to move the taxa: {e}")


@router.get("/reassign/history", response_model=ResponseModel)
async def reassign_history(limit: int = Query(50, ge=1, le=500)):
    """Past moves, newest first. Undone ones stay in the list -- who moved what, and when,
    is part of the record."""
    try:
        rows = await reassign_service.history(limit)
        return ResponseModel(code=20000, data={"items": rows, "total": len(rows)})
    except Exception as e:  # noqa: BLE001
        return ResponseModel(code=50000, message=f"Failed to list family moves: {e}")


@router.get("/reassign/{op_id}/taxa", response_model=ResponseModel)
async def reassign_taxa(op_id: int):
    """The per-taxon before/after of one move, from family_fix_audit."""
    try:
        rows = await reassign_service.operation_taxa(op_id)
        return ResponseModel(code=20000, data={"items": rows, "total": len(rows)})
    except Exception as e:  # noqa: BLE001
        return ResponseModel(code=50000, message=f"Failed to list the moved taxa: {e}")


@router.post("/reassign/{op_id}/undo", response_model=ResponseModel)
async def undo_reassign(op_id: int, body: UndoReassignModel):
    """Put every taxon in this move back into the family it came from.

    Refused if any of them has been moved again since -- restoring would silently overwrite a
    later decision.
    """
    try:
        result = await reassign_service.undo(op_id, body.undone_by)
        if "error" in result:
            return ResponseModel(code=40000, message=result["error"])
        return ResponseModel(code=20000, data=result,
                             message=f"Move undone: {result['taxa_restored']} taxa restored")
    except Exception as e:  # noqa: BLE001
        return ResponseModel(code=50000, message=f"Failed to undo the move: {e}")