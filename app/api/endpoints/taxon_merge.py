"""
Duplicate-taxon merge API.

Its own router, because a merge is a different object from a name correction: it moves
specimens between two rows that spell the SAME name, and it never stamps a curator on the
determination. See app/services/taxon_merge_service.py and migrations/taxon_merge.sql.
"""
from typing import Any, Dict, Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel

from app.services.taxon_merge_service import TaxonMergeService

router = APIRouter()
service = TaxonMergeService()


class ResponseModel(BaseModel):
    code: int
    data: Dict[str, Any] = {}
    message: Optional[str] = None


class MergeModel(BaseModel):
    winner_taxon_id: int
    loser_taxon_id: int
    merged_by: str
    notes: Optional[str] = None
    # Guard: a merge is for two rows spelling the same name. Overriding it means the
    # curator has decided these really are the same taxon under different spellings.
    allow_different_names: bool = False


class UndoMergeModel(BaseModel):
    undone_by: str


@router.get("/duplicates", response_model=ResponseModel)
async def list_duplicates():
    """Duplicated FullScientificName groups still unresolved, with each member's specimen
    count and a recommended winner (the taxon the collection is already on)."""
    try:
        return ResponseModel(code=20000, data=await service.duplicate_groups())
    except Exception as e:  # noqa: BLE001
        return ResponseModel(code=50000, message=f"Failed to list duplicates: {e}")


@router.get("/preview", response_model=ResponseModel)
async def preview_merge(
    winner_taxon_id: int = Query(...),
    loser_taxon_id: int = Query(...),
):
    """What a merge would move. Changes nothing."""
    try:
        result = await service.preview(winner_taxon_id, loser_taxon_id)
        if "error" in result:
            return ResponseModel(code=40000, message=result["error"])
        return ResponseModel(code=20000, data=result)
    except Exception as e:  # noqa: BLE001
        return ResponseModel(code=50000, message=f"Preview failed: {e}")


@router.post("/merge", response_model=ResponseModel)
async def merge_taxa(body: MergeModel):
    """Move the loser's current determinations onto the winner and tag the loser as merged.

    Nothing is deleted; determiner and date are copied forward, so no specimen gains a
    re-identification it never had.
    """
    try:
        result = await service.merge(body.winner_taxon_id, body.loser_taxon_id,
                                     body.merged_by, body.notes,
                                     allow_different_names=body.allow_different_names)
        if "error" in result:
            return ResponseModel(code=40000, message=result["error"])
        return ResponseModel(
            code=20000, data=result,
            message=(f"Merged taxon {result['loser_taxon_id']} into "
                     f"{result['winner_taxon_id']}: {result['determinations_moved']} "
                     f"determinations moved, nothing deleted"))
    except Exception as e:  # noqa: BLE001
        return ResponseModel(code=50000, message=f"Merge failed: {e}")


@router.post("/merge/{apply_log_id}/undo", response_model=ResponseModel)
async def undo_merge(apply_log_id: int, body: UndoMergeModel):
    """Revert a merge: remove the inserted determinations, restore the retired ones, and
    clear the merged tag."""
    try:
        result = await service.undo(apply_log_id, body.undone_by)
        if "error" in result:
            return ResponseModel(code=40000, message=result["error"])
        return ResponseModel(code=20000, data=result, message="Merge undone")
    except Exception as e:  # noqa: BLE001
        return ResponseModel(code=50000, message=f"Undo failed: {e}")