"""
Family-classification rulings API.

One decision per (local family -> reference family) disagreement, recorded once for the
whole museum instead of being re-confirmed on every batch record. See
app/services/family_policy_service.py for why, and migrations/family_reference_policy.sql
for the numbers that motivated it.

Kept as its own router (not folded into synonym-review) because it is a different object:
these endpoints only read/write rulings, they never edit taxonomy.
"""
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel

from app.services.family_policy_service import FamilyPolicyService

router = APIRouter()
service = FamilyPolicyService()


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