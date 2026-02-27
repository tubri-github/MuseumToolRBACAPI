"""
Taxon Synonym Review API endpoints.

Provides endpoints for scanning taxonomic names against the TaxonRank database
to detect synonyms, and a review workflow for specialists to accept/reject suggestions.
"""

from fastapi import APIRouter, Query
from typing import Dict, Any, List, Optional
from pydantic import BaseModel

from app.services.synonym_service import SynonymService
from app.db.taxon_database import is_taxon_db_configured, execute_taxon_query

router = APIRouter()
synonym_service = SynonymService()


# --- Pydantic Models ---

class ResponseModel(BaseModel):
    code: int
    data: Dict[str, Any] = {}
    message: Optional[str] = None


class ReviewDecisionModel(BaseModel):
    reviewed_by: str
    notes: Optional[str] = None
    final_valid_name: Optional[str] = None  # specialist can override the suggested name


class BatchReviewDecisionModel(BaseModel):
    review_ids: List[int]
    reviewed_by: str
    notes: Optional[str] = None


class CorrectDecisionModel(BaseModel):
    reviewed_by: str
    notes: Optional[str] = None
    final_valid_name: str  # required: the reviewer-chosen correct name
    final_genus: Optional[str] = None
    final_species: Optional[str] = None
    correction_source: Optional[str] = None  # 'reference_db' or 'manual'
    taxonrank_ref_id: Optional[int] = None  # ID from taxa table if selected from reference DB
    create_in_local: bool = False  # whether to insert into TaxonomicTable


class ResetDecisionModel(BaseModel):
    reviewed_by: str


# --- Endpoints ---

@router.post("/scan", response_model=ResponseModel)
async def trigger_synonym_scan():
    """
    Trigger a full scan of all taxon names in TaxonomicTable against the TaxonRank database.
    Detects synonyms and creates review records for specialist review.
    """
    try:
        if not is_taxon_db_configured():
            return ResponseModel(
                code=40000,
                message="TaxonRank database is not configured. Set TAXON_DB_* in .env"
            )

        result = await synonym_service.scan_all_taxon_names()

        return ResponseModel(
            code=20000,
            data=result,
            message=f"Scan completed. Found {result['total_detected']} synonyms."
        )
    except Exception as e:
        return ResponseModel(code=50000, message=f"Scan failed: {str(e)}")


@router.get("/reviews", response_model=ResponseModel)
async def get_review_list(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    status: Optional[str] = Query(None, description="Filter: pending, accepted, rejected, skipped"),
    issue: Optional[str] = Query(None, description="Filter: spelling, synonym, spelling+synonym"),
    search: Optional[str] = Query(None, description="Search in taxon names"),
    sort_by: str = Query("created_at", description="Sort field"),
    sort_order: str = Query("desc", description="asc or desc")
):
    """Get paginated list of synonym reviews for specialist review."""
    try:
        result = await synonym_service.get_review_items(
            page=page, page_size=page_size,
            status_filter=status, issue_filter=issue,
            search=search, sort_by=sort_by, sort_order=sort_order
        )
        return ResponseModel(code=20000, data=result.get("data", {}))
    except Exception as e:
        return ResponseModel(code=50000, message=f"Failed to get reviews: {str(e)}")


@router.get("/reviews/{review_id}", response_model=ResponseModel)
async def get_review_detail(review_id: int):
    """Get detailed information about a single review item, including WoRMS links."""
    try:
        result = await synonym_service.get_review_detail(review_id)
        if not result:
            return ResponseModel(code=40400, message="Review not found")
        return ResponseModel(code=20000, data=result)
    except Exception as e:
        return ResponseModel(code=50000, message=f"Failed to get review detail: {str(e)}")


@router.post("/reviews/{review_id}/accept", response_model=ResponseModel)
async def accept_review(review_id: int, decision: ReviewDecisionModel):
    """Accept a synonym suggestion. Records the decision only (does not modify TaxonomicTable)."""
    try:
        result = await synonym_service.accept_synonym(
            review_id=review_id,
            reviewed_by=decision.reviewed_by,
            notes=decision.notes,
            final_valid_name=decision.final_valid_name
        )
        if 'error' in result:
            return ResponseModel(code=40000, message=result['error'])
        return ResponseModel(code=20000, data=result, message="Synonym accepted")
    except Exception as e:
        return ResponseModel(code=50000, message=f"Failed to accept: {str(e)}")


@router.post("/reviews/{review_id}/reject", response_model=ResponseModel)
async def reject_review(review_id: int, decision: ReviewDecisionModel):
    """Reject a synonym suggestion."""
    try:
        result = await synonym_service.reject_synonym(
            review_id=review_id,
            reviewed_by=decision.reviewed_by,
            notes=decision.notes
        )
        if 'error' in result:
            return ResponseModel(code=40000, message=result['error'])
        return ResponseModel(code=20000, data=result, message="Synonym rejected")
    except Exception as e:
        return ResponseModel(code=50000, message=f"Failed to reject: {str(e)}")


@router.post("/reviews/{review_id}/skip", response_model=ResponseModel)
async def skip_review(review_id: int, decision: ReviewDecisionModel):
    """Skip (defer) a review item for later."""
    try:
        result = await synonym_service.skip_synonym(
            review_id=review_id,
            reviewed_by=decision.reviewed_by,
            notes=decision.notes
        )
        if 'error' in result:
            return ResponseModel(code=40000, message=result['error'])
        return ResponseModel(code=20000, data=result, message="Review skipped")
    except Exception as e:
        return ResponseModel(code=50000, message=f"Failed to skip: {str(e)}")


@router.post("/reviews/batch-accept", response_model=ResponseModel)
async def batch_accept_reviews(decision: BatchReviewDecisionModel):
    """Batch accept multiple synonym suggestions."""
    try:
        result = await synonym_service.batch_accept(
            review_ids=decision.review_ids,
            reviewed_by=decision.reviewed_by,
            notes=decision.notes
        )
        return ResponseModel(
            code=20000, data=result,
            message=f"Accepted {result['accepted_count']} of {result['total_requested']} reviews"
        )
    except Exception as e:
        return ResponseModel(code=50000, message=f"Batch accept failed: {str(e)}")


@router.post("/reviews/batch-reject", response_model=ResponseModel)
async def batch_reject_reviews(decision: BatchReviewDecisionModel):
    """Batch reject multiple synonym suggestions."""
    try:
        result = await synonym_service.batch_reject(
            review_ids=decision.review_ids,
            reviewed_by=decision.reviewed_by,
            notes=decision.notes
        )
        return ResponseModel(
            code=20000, data=result,
            message=f"Rejected {result['rejected_count']} of {result['total_requested']} reviews"
        )
    except Exception as e:
        return ResponseModel(code=50000, message=f"Batch reject failed: {str(e)}")


@router.post("/reviews/{review_id}/correct", response_model=ResponseModel)
async def correct_review(review_id: int, decision: CorrectDecisionModel):
    """Correct a review: the reviewer provides their own valid name."""
    try:
        result = await synonym_service.correct_synonym(
            review_id=review_id,
            reviewed_by=decision.reviewed_by,
            notes=decision.notes,
            final_valid_name=decision.final_valid_name,
            final_genus=decision.final_genus,
            final_species=decision.final_species,
            correction_source=decision.correction_source,
            taxonrank_ref_id=decision.taxonrank_ref_id,
            create_in_local=decision.create_in_local
        )
        if 'error' in result:
            return ResponseModel(code=40000, message=result['error'])
        return ResponseModel(code=20000, data=result, message="Review corrected")
    except Exception as e:
        return ResponseModel(code=50000, message=f"Failed to correct: {str(e)}")


@router.post("/reviews/{review_id}/reset", response_model=ResponseModel)
async def reset_review(review_id: int, decision: ResetDecisionModel):
    """Reset a processed review back to pending status."""
    try:
        result = await synonym_service.reset_synonym(
            review_id=review_id,
            reviewed_by=decision.reviewed_by
        )
        if 'error' in result:
            return ResponseModel(code=40000, message=result['error'])
        return ResponseModel(code=20000, data=result, message="Review reset to pending")
    except Exception as e:
        return ResponseModel(code=50000, message=f"Failed to reset: {str(e)}")


@router.get("/taxon-search", response_model=ResponseModel)
async def search_reference_taxa(
    keyword: str = Query(..., min_length=2, description="Search keyword"),
    limit: int = Query(15, ge=1, le=50)
):
    """Search the TaxonRank reference database (taxonomic_dev) for taxa matching a keyword."""
    try:
        if not is_taxon_db_configured():
            return ResponseModel(code=40000, message="TaxonRank database is not configured")

        query = """
            SELECT t.id, t.scientific_name, t.rank, t.status, t.valid_id,
                   COALESCE(v.scientific_name, t.scientific_name) AS valid_name
            FROM taxa t
            LEFT JOIN taxa v ON t.valid_id = v.id
            WHERE t.scientific_name ILIKE $1
            ORDER BY
                CASE WHEN t.scientific_name ILIKE $2 THEN 0 ELSE 1 END,
                t.scientific_name
            LIMIT $3
        """
        results = await execute_taxon_query(query, f"%{keyword}%", f"{keyword}%", limit)

        return ResponseModel(code=20000, data={"items": results, "total": len(results)})
    except Exception as e:
        return ResponseModel(code=50000, message=f"Search failed: {str(e)}")


@router.get("/stats", response_model=ResponseModel)
async def get_review_stats():
    """Get synonym review statistics for the dashboard."""
    try:
        result = await synonym_service.get_stats()
        return ResponseModel(code=20000, data=result)
    except Exception as e:
        return ResponseModel(code=50000, message=f"Failed to get stats: {str(e)}")


@router.get("/config-status", response_model=ResponseModel)
async def get_config_status():
    """Check if TaxonRank database is configured and accessible."""
    try:
        configured = is_taxon_db_configured()
        accessible = False
        taxon_count = 0

        if configured:
            try:
                result = await execute_taxon_query("SELECT COUNT(*) AS cnt FROM taxa")
                accessible = True
                taxon_count = result[0]['cnt'] if result else 0
            except Exception as e:
                return ResponseModel(
                    code=20000,
                    data={
                        "configured": True,
                        "accessible": False,
                        "error": str(e)
                    },
                    message="TaxonRank DB is configured but not accessible"
                )

        return ResponseModel(
            code=20000,
            data={
                "configured": configured,
                "accessible": accessible,
                "taxon_count": taxon_count,
            },
            message="TaxonRank DB is ready" if accessible
                    else "TaxonRank DB is not configured"
        )
    except Exception as e:
        return ResponseModel(code=50000, message=f"Config check failed: {str(e)}")