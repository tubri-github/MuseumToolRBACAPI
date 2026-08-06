"""
Taxon Synonym Review API endpoints.

Provides endpoints for scanning taxonomic names against the TaxonRank database
to detect synonyms, and a review workflow for specialists to accept/reject suggestions.
"""

from fastapi import APIRouter, Query, BackgroundTasks
from typing import Dict, Any, List, Optional
from pydantic import BaseModel

from app.services.synonym_service import SynonymService
from app.services.taxon_check_service import TaxonCheckService
from app.services.taxon_apply_service import TaxonApplyService
from app.db.taxon_database import is_taxon_db_configured, execute_taxon_query
from app.db.database import (execute_paginated_query_with_count, execute_single_query,
                             execute_query, execute_mutation)

router = APIRouter()
synonym_service = SynonymService()
taxon_check_service = TaxonCheckService()
taxon_apply_service = TaxonApplyService()


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


# ===================================================================================
# Taxon recheck (rewrite): whole-DB check of in-use taxa + determination write-back.
# Reuses the taxon_synonym_review table; adds category/appliable/family_mismatch etc.
# ===================================================================================

class ApplyDecisionModel(BaseModel):
    applied_by: str
    target_taxon_id: Optional[int] = None   # needs-manual: curator-selected target from reference
    target_name: Optional[str] = None       # or an accepted name; must already exist locally
    notes: Optional[str] = None
    # Creating a taxon here can only GUESS its family, and a taxon with no family silently
    # drops out of family checks, statistics and the tree. New names are created through the
    # taxon-creation form, where the curator sets the family, and then applied by id. Left as
    # an explicit escape hatch rather than removed.
    allow_create: bool = False


class PreviewModel(BaseModel):
    target_taxon_id: Optional[int] = None
    target_name: Optional[str] = None


class UndoModel(BaseModel):
    undone_by: str


class DismissModel(BaseModel):
    """'I looked at this and the museum keeps its current name.' Records the decision
    WITHOUT touching any data, so the row stops coming back on every scan."""
    reviewed_by: str
    notes: Optional[str] = None


class BatchDismissModel(DismissModel):
    """Dismiss many at once. Either an explicit id list, or every pending row matching
    the same filters the list endpoint uses (so 'dismiss everything I'm looking at' works)."""
    review_ids: Optional[List[int]] = None
    category: Optional[str] = None
    appliable: Optional[bool] = None
    family_mismatch: Optional[bool] = None


class ResetReviewModel(BaseModel):
    """Put a dismissed row back to pending (undo of a dismiss)."""
    reviewed_by: str


@router.post("/recheck/scan", response_model=ResponseModel)
async def recheck_scan():
    """Scan all IN-USE taxa (Determination IsCurrent) against CoF, classify, and persist
    actionable rows as pending reviews. Replaces the pending set."""
    try:
        if not is_taxon_db_configured():
            return ResponseModel(code=40000, message="CoF reference DB is not configured")
        stats = await taxon_check_service.scan(persist=True)
        return ResponseModel(code=20000, data=stats,
                             message=f"Scan done: {stats.get('persisted', 0)} actionable taxa")
    except Exception as e:
        return ResponseModel(code=50000, message=f"Recheck scan failed: {str(e)}")


@router.get("/recheck/reviews", response_model=ResponseModel)
async def recheck_reviews(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    status: Optional[str] = Query(None),
    category: Optional[str] = Query(None, description="HYBRID/TRINOMIAL/EXACT_SYNONYM/RECOMBINATION/..."),
    appliable: Optional[bool] = Query(None),
    family_mismatch: Optional[bool] = Query(None),
    search: Optional[str] = Query(None),
    sort_by: str = Query("in_use_count"),
    sort_order: str = Query("desc"),
):
    """Paginated review list with the new classification fields + filters."""
    try:
        where, params, i = [], [], 1
        for col, val in (("review_status", status), ("category", category)):
            if val:
                where.append(f"{col} = ${i}")
                params.append(val)
                i += 1
        for col, val in (("appliable", appliable), ("family_mismatch", family_mismatch)):
            if val is not None:
                where.append(f"{col} = ${i}")
                params.append(val)
                i += 1
        if search:
            where.append(f"(current_full_name ILIKE ${i} OR suggested_full_name ILIKE ${i})")
            params.append(f"%{search}%")
            i += 1
        where_sql = (" WHERE " + " AND ".join(where)) if where else ""

        allowed = {"in_use_count", "category", "current_full_name", "created_at", "review_status"}
        sort_by = sort_by if sort_by in allowed else "in_use_count"
        sort_dir = "ASC" if sort_order.lower() == "asc" else "DESC"

        main = (f"SELECT id, taxon_id, current_full_name, current_family, suggested_full_name, "
                f"category, appliable, in_use_count, candidates, family_mismatch, reference_family, "
                f"review_status, reviewed_by, reviewed_at, final_valid_name, scan_batch_id "
                f"FROM taxon_synonym_review{where_sql} ORDER BY {sort_by} {sort_dir}, id")
        count = f"SELECT COUNT(*) FROM taxon_synonym_review{where_sql}"
        result = await execute_paginated_query_with_count(main, count, params, page, page_size)
        return ResponseModel(code=20000, data=result.get("data", {}))
    except Exception as e:
        return ResponseModel(code=50000, message=f"Failed to list reviews: {str(e)}")


@router.get("/recheck/stats", response_model=ResponseModel)
async def recheck_stats():
    """Category / appliable / family-mismatch counts over the pending review set."""
    try:
        rows = await execute_query(
            "SELECT category, COUNT(*) n, SUM(in_use_count) dets, "
            "SUM(CASE WHEN appliable THEN 1 ELSE 0 END) appliable, "
            "SUM(CASE WHEN family_mismatch THEN 1 ELSE 0 END) family_mismatch "
            "FROM taxon_synonym_review WHERE review_status='pending' GROUP BY category")
        by_cat = {r["category"]: {"count": r["n"], "determinations": r["dets"],
                                  "appliable": r["appliable"], "family_mismatch": r["family_mismatch"]}
                  for r in rows}
        totals = await execute_single_query(
            "SELECT COUNT(*) total, SUM(CASE WHEN appliable THEN 1 ELSE 0 END) appliable, "
            "SUM(CASE WHEN family_mismatch THEN 1 ELSE 0 END) family_mismatch "
            "FROM taxon_synonym_review WHERE review_status='pending'")
        # progress: how much of the backlog has been dealt with, and how
        settled = await execute_single_query(
            "SELECT COUNT(*) FILTER (WHERE review_status='applied')   AS applied, "
            "       COUNT(*) FILTER (WHERE review_status='dismissed') AS dismissed "
            "FROM taxon_synonym_review")
        return ResponseModel(code=20000, data={"by_category": by_cat, "totals": totals,
                                               "settled": settled})
    except Exception as e:
        return ResponseModel(code=50000, message=f"Failed to get stats: {str(e)}")


@router.post("/recheck/reviews/{review_id}/preview", response_model=ResponseModel)
async def recheck_preview(review_id: int, body: PreviewModel):
    """Dry-run: how many determinations would change and to which target (no mutation)."""
    try:
        result = await taxon_apply_service.preview(
            review_id, target_taxon_id=body.target_taxon_id, target_name=body.target_name)
        if "error" in result:
            return ResponseModel(code=40400, message=result["error"])
        return ResponseModel(code=20000, data=result)
    except Exception as e:
        return ResponseModel(code=50000, message=f"Preview failed: {str(e)}")


# determinations at/above this count run as a background task (frontend polls apply-log status).
APPLY_BG_THRESHOLD = 500


@router.post("/recheck/reviews/{review_id}/apply", response_model=ResponseModel)
async def recheck_apply(review_id: int, decision: ApplyDecisionModel,
                        background_tasks: BackgroundTasks):
    """Confirm a correction: retire old current determinations and add new ones pointing at
    the target taxon (curator + today), for every affected Primary. Undoable. Large applies
    (>= APPLY_BG_THRESHOLD determinations) run in the background; poll apply-log for status."""
    try:
        review = await execute_single_query(
            "SELECT in_use_count FROM taxon_synonym_review WHERE id=$1", review_id)
        if not review:
            return ResponseModel(code=40400, message="Review not found")

        # Validate the target BEFORE choosing a path: a big apply is handed to a background
        # task, so an unvalidated bad request would answer "started" and only fail later,
        # out of sight in the apply log.
        bad = await taxon_apply_service.validate_target(
            decision.target_taxon_id, decision.target_name, decision.allow_create)
        if bad:
            return ResponseModel(code=40000, message=bad)

        if (review["in_use_count"] or 0) >= APPLY_BG_THRESHOLD:
            started = await taxon_apply_service.start_background_apply(
                review_id, applied_by=decision.applied_by,
                target_taxon_id=decision.target_taxon_id, target_name=decision.target_name,
                notes=decision.notes)
            if "error" in started:
                return ResponseModel(code=40000, message=started["error"])
            background_tasks.add_task(
                taxon_apply_service.run_background, started["apply_log_id"], review_id,
                decision.applied_by, decision.target_taxon_id, decision.target_name,
                decision.notes, decision.allow_create)
            return ResponseModel(
                code=20000, data=started,
                message=f"Apply started in background ({started['total_planned']} determinations); "
                        f"poll /recheck/apply-log/{started['apply_log_id']}")

        result = await taxon_apply_service.apply(
            review_id, applied_by=decision.applied_by,
            target_taxon_id=decision.target_taxon_id, target_name=decision.target_name,
            notes=decision.notes, allow_create=decision.allow_create)
        if "error" in result:
            return ResponseModel(code=40000, message=result["error"])
        return ResponseModel(
            code=20000, data=result,
            message=f"Applied: {result['determinations_changed']} determinations updated")
    except Exception as e:
        return ResponseModel(code=50000, message=f"Apply failed: {str(e)}")


@router.get("/recheck/apply-log/{log_id}", response_model=ResponseModel)
async def recheck_apply_log_detail(log_id: int):
    """Poll a single apply's status (running | applied | failed | undone)."""
    try:
        row = await execute_single_query(
            "SELECT id, review_id, old_taxon_id, new_taxon_id, old_name, new_name, "
            "new_taxon_created, determinations_changed, total_planned, applied_by, applied_at, "
            "status, error_message, undone_by, undone_at FROM taxon_recheck_apply_log WHERE id=$1",
            log_id)
        if not row:
            return ResponseModel(code=40400, message="Apply log not found")
        return ResponseModel(code=20000, data=row)
    except Exception as e:
        return ResponseModel(code=50000, message=f"Failed to get apply log: {str(e)}")


@router.get("/recheck/apply-log", response_model=ResponseModel)
async def recheck_apply_log(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    status: Optional[str] = Query(None, description="applied | undone"),
    operation: Optional[str] = Query(None, description="recheck_apply | taxon_merge"),
):
    """List applies (for the undo UI). Merges share this log, so `operation` separates them."""
    try:
        where, params, i = [], [], 1
        if status:
            where.append(f"status = ${i}")
            params.append(status)
            i += 1
        if operation:
            where.append(f"operation = ${i}")
            params.append(operation)
            i += 1
        where_sql = (" WHERE " + " AND ".join(where)) if where else ""
        main = (f"SELECT id, operation, review_id, old_taxon_id, new_taxon_id, old_name, "
                f"new_name, new_taxon_created, determinations_changed, applied_by, applied_at, "
                f"status, undone_by, undone_at FROM taxon_recheck_apply_log{where_sql} "
                f"ORDER BY applied_at DESC")
        count = f"SELECT COUNT(*) FROM taxon_recheck_apply_log{where_sql}"
        result = await execute_paginated_query_with_count(main, count, params, page, page_size)
        return ResponseModel(code=20000, data=result.get("data", {}))
    except Exception as e:
        return ResponseModel(code=50000, message=f"Failed to list apply-log: {str(e)}")


@router.post("/recheck/apply-log/{log_id}/undo", response_model=ResponseModel)
async def recheck_undo(log_id: int, body: UndoModel):
    """Undo an apply: restore retired determinations, remove inserted ones, reindex ES."""
    try:
        result = await taxon_apply_service.undo(log_id, undone_by=body.undone_by)
        if "error" in result:
            return ResponseModel(code=40000, message=result["error"])
        return ResponseModel(code=20000, data=result, message="Apply undone")
    except Exception as e:
        return ResponseModel(code=50000, message=f"Undo failed: {str(e)}")


# ----------------------------------------------------------------------------------
# Dismiss: the second way out of a review.
#
# A scan produces ~1337 rows but only ~433 are appliable; the rest (EXACT_VALID,
# RECOMBINATION, TRINOMIAL, HYBRID, NO_BINOMIAL, NOT_IN_COF) are usually "looked at it,
# the museum keeps its name". Without a way to record that, those rows stay 'pending'
# and come back identical on every scan -- the curator would re-read the same hundreds
# of rows forever. Dismissing changes NO taxonomic data: it only stamps the decision on
# the review row, so it deliberately writes no taxon_recheck_apply_log entry (that log is
# for changes that moved data and can be undone). Reversed with /reset.
# ----------------------------------------------------------------------------------

@router.post("/recheck/reviews/{review_id}/dismiss", response_model=ResponseModel)
async def recheck_dismiss(review_id: int, body: DismissModel):
    """Record 'reviewed, keeping the current name' for one row. Only pending rows."""
    try:
        row = await execute_single_query(
            "SELECT review_status FROM taxon_synonym_review WHERE id=$1", review_id)
        if not row:
            return ResponseModel(code=40400, message="Review not found")
        if row["review_status"] != "pending":
            return ResponseModel(
                code=40000,
                message=f"Only a pending review can be dismissed "
                        f"(this one is '{row['review_status']}'). "
                        f"An applied review is reverted through its apply-log undo.")
        await execute_mutation(
            "UPDATE taxon_synonym_review SET review_status='dismissed', reviewed_by=$1, "
            "reviewed_at=NOW(), review_notes=$2, updated_at=NOW() WHERE id=$3",
            body.reviewed_by[:50], body.notes, review_id)
        return ResponseModel(code=20000, data={"review_id": review_id, "status": "dismissed"},
                             message="Review dismissed (no data was changed)")
    except Exception as e:
        return ResponseModel(code=50000, message=f"Dismiss failed: {str(e)}")


@router.post("/recheck/reviews/batch-dismiss", response_model=ResponseModel)
async def recheck_batch_dismiss(body: BatchDismissModel):
    """Dismiss many pending rows: an explicit review_ids list, or every pending row
    matching category / appliable / family_mismatch.

    A selector is REQUIRED. Without one this would silently dismiss the entire backlog,
    which is exactly the mistake that is hardest to notice afterwards.
    """
    try:
        has_filter = any(v is not None for v in
                         (body.category, body.appliable, body.family_mismatch))
        if not body.review_ids and not has_filter:
            return ResponseModel(
                code=40000,
                message="Refusing to dismiss everything: pass review_ids, or at least one "
                        "of category / appliable / family_mismatch.")

        where = ["review_status = 'pending'"]
        params, i = [], 1
        if body.review_ids:
            where.append(f"id = ANY(${i}::int[])")
            params.append(body.review_ids)
            i += 1
        for col, val in (("category", body.category), ("appliable", body.appliable),
                         ("family_mismatch", body.family_mismatch)):
            if val is not None:
                where.append(f"{col} = ${i}")
                params.append(val)
                i += 1

        where_sql = " AND ".join(where)
        affected = await execute_query(
            f"UPDATE taxon_synonym_review SET review_status='dismissed', reviewed_by=${i}, "
            f"reviewed_at=NOW(), review_notes=${i + 1}, updated_at=NOW() "
            f"WHERE {where_sql} RETURNING id",
            *params, body.reviewed_by[:50], body.notes)
        return ResponseModel(
            code=20000,
            data={"dismissed": len(affected), "review_ids": [r["id"] for r in affected]},
            message=f"{len(affected)} reviews dismissed (no data was changed)")
    except Exception as e:
        return ResponseModel(code=50000, message=f"Batch dismiss failed: {str(e)}")


@router.post("/recheck/reviews/{review_id}/reset", response_model=ResponseModel)
async def recheck_reset(review_id: int, body: ResetReviewModel):
    """Put a dismissed review back to pending.

    Deliberately refuses 'applied' rows: an applied review moved determinations, and the
    only correct way back is the apply-log undo, which also restores the data. Resetting
    it here would leave the row looking untouched while the data stayed changed.
    """
    try:
        row = await execute_single_query(
            "SELECT review_status FROM taxon_synonym_review WHERE id=$1", review_id)
        if not row:
            return ResponseModel(code=40400, message="Review not found")
        if row["review_status"] == "applied":
            return ResponseModel(
                code=40000,
                message="This review was applied and changed data. Undo it from the "
                        "apply history instead -- that reverts the determinations too.")
        if row["review_status"] == "pending":
            return ResponseModel(code=20000, data={"review_id": review_id, "status": "pending"},
                                 message="Already pending")
        if row["review_status"] != "dismissed":
            # 'accepted' / 'rejected' come from the older synonym-review flow; this endpoint
            # is the undo of a dismiss and must not silently reopen those decisions.
            return ResponseModel(
                code=40000,
                message=f"Only a dismissed review can be reset here "
                        f"(this one is '{row['review_status']}').")
        await execute_mutation(
            "UPDATE taxon_synonym_review SET review_status='pending', reviewed_by=NULL, "
            "reviewed_at=NULL, review_notes=NULL, updated_at=NOW() WHERE id=$1", review_id)
        return ResponseModel(code=20000, data={"review_id": review_id, "status": "pending"},
                             message="Review reset to pending")
    except Exception as e:
        return ResponseModel(code=50000, message=f"Reset failed: {str(e)}")