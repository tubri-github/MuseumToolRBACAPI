import os
import uuid
import json
from datetime import datetime
from typing import Dict, Any, List, Optional, Union

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from fastapi.encoders import jsonable_encoder
from fastapi.responses import StreamingResponse, FileResponse
from pydantic import BaseModel

from app.db.database import execute_query, execute_mutation, execute_transaction
from app.utils.validation import ImportValidationUtils
from app.utils.species_validation import SpeciesNameValidator
router = APIRouter()

# Pydantic models
class ResponseModel(BaseModel):
    code: int
    data: Dict = {}
    message: Optional[str] = None


class PaginationParams:
    def __init__(
            self,
            page: int = Query(1, ge=1, description="Page number, starting from 1"),
            page_size: int = Query(10, ge=1, le=100, description="Records per page")
    ):
        self.page = page
        self.page_size = page_size


class FilterParams:
    def __init__(
            self,
            status: Optional[str] = Query(None, description="Filter by processing status"),
            search: Optional[str] = Query(None, description="Search term")
    ):
        self.status = status
        self.search = search


class VerbatimTaxonomicModel(BaseModel):
    verbatim_taxonid: Optional[int] = None
    verbatim_family: Optional[str] = None
    verbatim_genus: Optional[str] = None
    verbatim_species: Optional[str] = None
    verbatim_subspecies: Optional[str] = None
    original_text: Optional[str] = None


class VerbatimLocalityModel(BaseModel):
    verbatim_localityid: Optional[int] = None
    verbatim_locality_string: Optional[str] = None
    verbatim_drainage: Optional[str] = None
    verbatim_country: Optional[str] = None
    verbatim_state: Optional[str] = None
    verbatim_county: Optional[str] = None
    verbatim_waterbody: Optional[str] = None
    verbatim_lat: Optional[str] = None
    verbatim_lon: Optional[str] = None
    verbatim_collect_date: Optional[str] = None
    verbatim_collector: Optional[str] = None
    field_number: Optional[str] = None  # Added field_number to verbatim locality model
    original_text: Optional[str] = None


class PrimaryRecordUpdateModel(BaseModel):
    id: int
    catalog_number: Optional[int] = None
    taxon_id: Optional[int] = None
    locality_id: Optional[int] = None
    collection_date: Optional[str] = None
    total_number: Optional[int] = None
    storage: Optional[str] = None
    jar_size: Optional[str] = None
    prev_number: Optional[str] = None
    inventory: Optional[str] = None
    remarks: Optional[str] = None
    review_flag: Optional[bool] = None

    species_verification_status: Optional[str] = None
    locality_verification_status: Optional[str] = None
    record_verification_status: Optional[str] = None
    verification_notes: Optional[str] = None

class BatchVerificationUpdateModel(BaseModel):
    record_ids: List[int]
    verification_type: str  # 'species', 'locality', 'record', 'all'
    status: str  # 'verified', 'rejected', 'needs_review', 'pending'
    notes: Optional[str] = None

# Initialize helper classes
validation_utils = ImportValidationUtils()
species_validator = SpeciesNameValidator()


# Batch management endpoints
@router.get("/batches", response_model=ResponseModel)
async def get_verbatim_batches(
        pagination: PaginationParams = Depends(),
        filter_params: FilterParams = Depends()
):
    """
    Get list of verbatim batches for processing
    可以按批次状态筛选：未处理、处理中、已完成
    """
    try:
        # Base query - 只查询 primary_temp 表
        base_query = """
        SELECT DISTINCT batch_serial_id,
               MIN("TimeStampModified") as import_date,
               COUNT(*) as total_records,
               SUM(CASE WHEN "TaxonID" IS NOT NULL THEN 1 ELSE 0 END) as taxonomic_processed,
               SUM(CASE WHEN "Locality1ID" IS NOT NULL THEN 1 ELSE 0 END) as locality_processed,
               SUM(CASE WHEN "TaxonID" IS NOT NULL AND "Locality1ID" IS NOT NULL THEN 1 ELSE 0 END) as fully_processed,
               MAX(CASE WHEN final_primary_id IS NOT NULL THEN 1 ELSE 0 END) as is_migrated
        FROM primary_temp
        WHERE batch_serial_id IS NOT NULL
        """

        count_query = """
        SELECT COUNT(DISTINCT batch_serial_id) as count
        FROM primary_temp
        WHERE batch_serial_id IS NOT NULL
        """

        # Add filters if provided
        where_clauses = []
        params = []
        param_index = 1

        if filter_params.status:
            if filter_params.status == 'pending':
                where_clauses.append("""
                EXISTS (
                    SELECT 1 FROM primary_temp p2
                    WHERE p2.batch_serial_id = primary_temp.batch_serial_id
                    AND (p2."TaxonID" IS NULL OR p2."Locality1ID" IS NULL)
                )
                """)
            elif filter_params.status == 'completed':
                where_clauses.append("""
                NOT EXISTS (
                    SELECT 1 FROM primary_temp p2
                    WHERE p2.batch_serial_id = primary_temp.batch_serial_id
                    AND (p2."TaxonID" IS NULL OR p2."Locality1ID" IS NULL)
                )
                """)
            elif filter_params.status == 'migrated':
                where_clauses.append("final_primary_id IS NOT NULL")

        if filter_params.search:
            where_clauses.append(f"batch_serial_id ILIKE ${param_index}")
            params.append(f"%{filter_params.search}%")
            param_index += 1

        # Add where clauses to query
        if where_clauses:
            additional_where = " AND " + " AND ".join(where_clauses)
            base_query += additional_where
            count_query += additional_where

        # Add group by and pagination
        base_query += """ 
        GROUP BY batch_serial_id
        ORDER BY import_date DESC
        LIMIT $""" + str(param_index) + " OFFSET $" + str(param_index + 1)

        params.extend([pagination.page_size, (pagination.page - 1) * pagination.page_size])

        # Execute queries
        batch_results = await execute_query(base_query, *params)
        count_result = await execute_query(count_query, *params[:param_index - 1])

        # Format results
        batches = []
        for batch in batch_results:
            total = batch["total_records"]
            taxonomic_processed = batch["taxonomic_processed"]
            locality_processed = batch["locality_processed"]
            fully_processed = batch["fully_processed"]

            # Calculate completion percentages
            taxonomic_percent = round((taxonomic_processed / total) * 100, 1) if total > 0 else 0
            locality_percent = round((locality_processed / total) * 100, 1) if total > 0 else 0
            overall_percent = round((fully_processed / total) * 100, 1) if total > 0 else 0

            status = "completed" if overall_percent == 100 else "in_progress"

            batches.append({
                "batch_serial_id": batch["batch_serial_id"],
                "import_date": batch["import_date"].isoformat() if batch["import_date"] else None,
                "total_records": total,
                "status": status,
                "progress": {
                    "taxonomic": {
                        "processed": taxonomic_processed,
                        "total": total,
                        "percent": taxonomic_percent
                    },
                    "locality": {
                        "processed": locality_processed,
                        "total": total,
                        "percent": locality_percent
                    },
                    "overall": {
                        "processed": fully_processed,
                        "total": total,
                        "percent": overall_percent
                    }
                }
            })

        total_count = count_result[0]["count"] if count_result else 0

        return ResponseModel(
            code=20000,
            data={
                "items": batches,
                "total": total_count
            }
        )
    except Exception as e:
        return ResponseModel(
            code=50000,
            message=f"Failed to get verbatim batches: {str(e)}"
        )


@router.get("/batches/{batch_serial_id}", response_model=ResponseModel)
async def get_batch_info(batch_serial_id: str):
    """
    Get detailed information about a specific batch
    获取指定批次的详细信息，包括导入时间、记录总数、处理进度等
    """
    try:
        # Query batch details from primary_temp
        batch_query = """
        SELECT
            batch_serial_id,
            MIN("TimeStampModified") as import_date,
            COUNT(*) as total_records,
            SUM(CASE WHEN "TaxonID" IS NOT NULL THEN 1 ELSE 0 END) as taxonomic_processed,
            SUM(CASE WHEN "Locality1ID" IS NOT NULL THEN 1 ELSE 0 END) as locality_processed,
            SUM(CASE WHEN "TaxonID" IS NOT NULL AND "Locality1ID" IS NOT NULL THEN 1 ELSE 0 END) as fully_processed,
            SUM(CASE WHEN review_flag = false THEN 1 ELSE 0 END) as reviewed_records
        FROM primary_temp
        WHERE batch_serial_id = $1
        GROUP BY batch_serial_id
        """

        batch_result = await execute_query(batch_query, batch_serial_id)

        if not batch_result:
            return ResponseModel(
                code=40400,
                message=f"Batch with serial ID {batch_serial_id} not found"
            )

        batch = batch_result[0]
        total = batch["total_records"]
        taxonomic_processed = batch["taxonomic_processed"]
        locality_processed = batch["locality_processed"]
        fully_processed = batch["fully_processed"]
        reviewed_records = batch["reviewed_records"]

        # Calculate completion percentages
        taxonomic_percent = round((taxonomic_processed / total) * 100, 1) if total > 0 else 0
        locality_percent = round((locality_processed / total) * 100, 1) if total > 0 else 0
        overall_percent = round((fully_processed / total) * 100, 1) if total > 0 else 0
        review_percent = round((reviewed_records / total) * 100, 1) if total > 0 else 0

        # Get additional batch metadata from system logs if available
        log_query = """
        SELECT action_details 
        FROM system_logs 
        WHERE action_type = 'batch_import' 
        AND action_details::jsonb->>'batchSerialId' = $1
        ORDER BY created_at DESC 
        LIMIT 1
        """

        log_result = await execute_query(log_query, batch_serial_id)

        metadata = {}
        if log_result:
            try:
                log_data = json.loads(log_result[0]["action_details"])
                metadata = {
                    "file_name": log_data.get("fileName", "Unknown"),
                    "import_mode": log_data.get("importMode", "Unknown"),
                    "start_time": log_data.get("startTime")
                }
            except:
                pass

        batch_info = {
            "batch_serial_id": batch["batch_serial_id"],
            "import_date": batch["import_date"].isoformat() if batch["import_date"] else None,
            "total_records": total,
            "metadata": metadata,
            "progress": {
                "taxonomic": {
                    "processed": taxonomic_processed,
                    "total": total,
                    "percent": taxonomic_percent
                },
                "locality": {
                    "processed": locality_processed,
                    "total": total,
                    "percent": locality_percent
                },
                "review": {
                    "processed": reviewed_records,
                    "total": total,
                    "percent": review_percent
                },
                "overall": {
                    "processed": fully_processed,
                    "total": total,
                    "percent": overall_percent
                }
            },
            "status": "completed" if overall_percent == 100 else "in_progress"
        }

        return ResponseModel(
            code=20000,
            data=batch_info
        )
    except Exception as e:
        return ResponseModel(
            code=50000,
            message=f"Failed to get batch info: {str(e)}"
        )


@router.get("/batches/{batch_serial_id}/records", response_model=ResponseModel)
async def get_batch_records(
        batch_serial_id: str,
        pagination: PaginationParams = Depends(),
        filter_params: FilterParams = Depends()
):
    """
    Get all records for a specific batch with pagination and filtering
    获取指定批次的所有记录，支持分页和筛选，包含物种匹配信息
    """
    try:
        # Base query - 只在SELECT中添加验证状态字段，其他保持不变
        base_query = """
        SELECT
            p."PrimaryID",
            p."CatalogNumber",
            p."verbatim_taxonid",
            p."verbatim_localityid",
            p."TaxonID",
            p."Locality1ID",
            p."TotalNumber",
            p."Storage",
            p."JarSize",
            p."PrevNumber",
            p."Inventory",
            p."Remarks",
            p."TimeStampModified",
            p."review_flag",
            p."match_type",
            p."species_verification_status",
            p."locality_verification_status",
            p."record_verification_status",
            p."overall_verification_status",
            p."verification_notes",
            p."verification_warnings",
            vt."verbatim_family",
            vt."verbatim_genus", 
            vt."verbatim_species",
            vt."match_status",
            vt."matched_taxon_id",
            vt."match_confidence",
            vt."match_details",
            vl."verbatim_locality_string",
            vl."verbatim_fieldno" as verbatim_field_number,
            vl."verbatim_drainage",
            vl."verbatim_country",
            vl."verbatim_state", 
            vl."verbatim_county",
            vl."verbatim_waterbody",
            vl."verbatim_lat",
            vl."verbatim_lon",
            vl."verbatim_collect_date" as verbatim_collect_date,
            vl."verbatim_collector",
            t."Genus" as matched_genus,
            t."Species" as matched_species,
            l."LocalityString" as matched_locality,
            l."FieldNo" as matched_field_number,
            suggested_t."Genus" as suggested_genus,
            suggested_t."Species" as suggested_species
        FROM primary_temp p
        LEFT JOIN verbatim_taxonomic vt ON p."verbatim_taxonid" = vt."verbatim_taxonid"
        LEFT JOIN verbatim_locality vl ON p."verbatim_localityid" = vl."verbatim_localityid"
        LEFT JOIN "TaxonomicTable" t ON p."TaxonID" = t."TaxonID"
        LEFT JOIN locality1 l ON p."Locality1ID" = l."Locality1ID"
        LEFT JOIN "TaxonomicTable" suggested_t ON vt."matched_taxon_id" = suggested_t."TaxonID"
        WHERE p.batch_serial_id = $1
        """

        count_query = """
        SELECT COUNT(*) as count
        FROM primary_temp p
        LEFT JOIN verbatim_taxonomic vt ON p."verbatim_taxonid" = vt."verbatim_taxonid"
        LEFT JOIN verbatim_locality vl ON p."verbatim_localityid" = vl."verbatim_localityid"
        WHERE p.batch_serial_id = $1
        """

        # Add filters if provided - 添加新的验证状态过滤选项
        where_clauses = []
        params = [batch_serial_id]
        param_index = 2

        if filter_params.status:
            if filter_params.status == 'pending_taxonomic':
                where_clauses.append('COALESCE(p."species_verification_status", \'pending\') = \'pending\'')
            elif filter_params.status == 'pending_locality':
                where_clauses.append('COALESCE(p."locality_verification_status", \'pending\') = \'pending\'')
            elif filter_params.status == 'pending_record':
                where_clauses.append('COALESCE(p."record_verification_status", \'pending\') = \'pending\'')
            elif filter_params.status == 'pending_any':
                where_clauses.append('(p."TaxonID" IS NULL OR p."Locality1ID" IS NULL)')
            elif filter_params.status == 'completed':
                where_clauses.append('COALESCE(p."overall_verification_status", \'pending\') = \'completed\'')
            elif filter_params.status == 'needs_review':
                where_clauses.append('p."review_flag" = true')
            elif filter_params.status == 'has_match_suggestion':
                where_clauses.append('vt."matched_taxon_id" IS NOT NULL')
            elif filter_params.status == 'has_errors':
                where_clauses.append("(p.\"verification_warnings\" IS NOT NULL AND p.\"verification_warnings\" != '[]' AND p.\"verification_warnings\" LIKE '%\"severity\": \"error\"%')")
            elif filter_params.status == 'has_warnings':
                where_clauses.append("(p.\"verification_warnings\" IS NOT NULL AND p.\"verification_warnings\" != '[]' AND p.\"verification_warnings\" LIKE '%\"severity\": \"warning\"%')")

        if filter_params.search:
            where_clauses.append(f"""(
                p."CatalogNumber"::text ILIKE ${param_index} OR
                vl."verbatim_fieldno" ILIKE ${param_index} OR
                l."FieldNo" ILIKE ${param_index} OR
                vt."verbatim_genus" ILIKE ${param_index} OR
                vt."verbatim_species" ILIKE ${param_index} OR
                vl."verbatim_locality_string" ILIKE ${param_index}
            )""")
            params.append(f"%{filter_params.search}%")
            param_index += 1

        # Add where clauses to query (保持现有逻辑不变)
        if where_clauses:
            additional_where = " AND " + " AND ".join(where_clauses)
            base_query += additional_where
            count_query += additional_where

        # Add order and pagination (保持现有逻辑不变)
        base_query += """ 
        ORDER BY p."CatalogNumber"
        LIMIT $""" + str(param_index) + " OFFSET $" + str(param_index + 1)

        params.extend([pagination.page_size, (pagination.page - 1) * pagination.page_size])

        # Execute queries (保持现有逻辑不变)
        records_result = await execute_query(base_query, *params)
        count_result = await execute_query(count_query, *params[:param_index - 1])

        # Format results - 只在现有格式化中添加verification_info
        records = []
        for record in records_result:
            # Determine processing status (保持现有逻辑不变)
            taxonomic_status = "processed" if record["TaxonID"] is not None else "pending"
            locality_status = "processed" if record["Locality1ID"] is not None else "pending"
            overall_status = "completed" if taxonomic_status == "processed" and locality_status == "processed" else "in_progress"

            # Parse match details if available (保持现有逻辑不变)
            match_details = None
            if record["match_details"]:
                try:
                    match_details = json.loads(record["match_details"]) if isinstance(record["match_details"], str) else record["match_details"]
                except:
                    match_details = None

            # Format the record - 只添加verification_info部分，其他保持不变
            formatted_record = {
                "id": record["PrimaryID"],
                "catalog_number": record["CatalogNumber"],
                "processing_status": {
                    "taxonomic": taxonomic_status,
                    "locality": locality_status,
                    "overall": overall_status,
                    "needs_review": record["review_flag"]
                },
                # 新增验证信息部分 - 这是唯一的添加
                "verification_info": {
                    "species": {
                        "status": record.get("species_verification_status", "pending")
                    },
                    "locality": {
                        "status": record.get("locality_verification_status", "pending")
                    },
                    "record": {
                        "status": record.get("record_verification_status", "pending")
                    },
                    "overall": {
                        "status": record.get("overall_verification_status", "pending")
                    },
                    "notes": record.get("verification_notes"),
                    "warnings": json.loads(record["verification_warnings"]) if record.get("verification_warnings") else []
                },
                "verbatim_data": {
                    "taxonomic": {
                        "id": record["verbatim_taxonid"],
                        "family": record["verbatim_family"],
                        "genus": record["verbatim_genus"],
                        "species": record["verbatim_species"]
                    },
                    "locality": {
                        "id": record["verbatim_localityid"],
                        "locality_string": record["verbatim_locality_string"],
                        "field_number": record["verbatim_field_number"],
                        "drainage": record["verbatim_drainage"],
                        "country": record["verbatim_country"],
                        "state": record["verbatim_state"],
                        "county": record["verbatim_county"],
                        "waterbody": record["verbatim_waterbody"],
                        "latitude": record["verbatim_lat"],
                        "longitude": record["verbatim_lon"],
                        "collect_date": record["verbatim_collect_date"].isoformat() if record[
                            "verbatim_collect_date"] else None,
                        "collector": record["verbatim_collector"]
                    }
                },
                "matched_data": {
                    "taxonomic": {
                        "id": record["TaxonID"],
                        "genus": record["matched_genus"],
                        "species": record["matched_species"],
                    },
                    "locality": {
                        "id": record["Locality1ID"],
                        "locality": record["matched_locality"],
                        "field_number": record["matched_field_number"],
                        "collection_date": record["verbatim_collect_date"].isoformat() if record[
                            "verbatim_collect_date"] else None
                    }
                },
                "match_suggestions": {
                    "taxonomic": {
                        "status": record["match_status"],
                        "confidence": record["match_confidence"],
                        "suggested_taxon_id": record["matched_taxon_id"],
                        "suggested_data": {
                            "genus": record["suggested_genus"],
                            "species": record["suggested_species"],
                        } if record["suggested_genus"] or record["suggested_species"] else None,
                        "match_details": match_details,
                        "has_suggestion": record["matched_taxon_id"] is not None,
                        "suggestion_applied": record["TaxonID"] == record["matched_taxon_id"] if record[
                            "matched_taxon_id"] else False
                    }
                },
                "record_data": {
                    "total_number": record["TotalNumber"],
                    "storage": record["Storage"],
                    "jar_size": record["JarSize"],
                    "prev_number": record["PrevNumber"],
                    "inventory": record["Inventory"],
                    "remarks": record["Remarks"],
                    "last_modified": record["TimeStampModified"].isoformat() if record["TimeStampModified"] else None,
                    "match_type": record["match_type"]
                },
                # Store the original API record for reference (保持现有逻辑不变)
                "_apiData": record
            }

            records.append(formatted_record)

        total_count = count_result[0]["count"] if count_result else 0

        # Also get progress statistics for this batch - 只添加验证状态统计
        progress_query = """
        SELECT 
            COUNT(*) as total_records,
            SUM(CASE WHEN "TaxonID" IS NOT NULL THEN 1 ELSE 0 END) as taxonomic_processed,
            SUM(CASE WHEN "Locality1ID" IS NOT NULL THEN 1 ELSE 0 END) as locality_processed,
            SUM(CASE WHEN "TaxonID" IS NOT NULL AND "Locality1ID" IS NOT NULL THEN 1 ELSE 0 END) as fully_processed,
            SUM(CASE WHEN "species_verification_status" = 'verified' THEN 1 ELSE 0 END) as species_verified,
            SUM(CASE WHEN "locality_verification_status" = 'verified' THEN 1 ELSE 0 END) as locality_verified,
            SUM(CASE WHEN "record_verification_status" = 'verified' THEN 1 ELSE 0 END) as record_verified,
            SUM(CASE WHEN "overall_verification_status" = 'completed' THEN 1 ELSE 0 END) as fully_verified,
            SUM(CASE WHEN vt."matched_taxon_id" IS NOT NULL THEN 1 ELSE 0 END) as has_taxonomic_suggestions
        FROM primary_temp p
        LEFT JOIN verbatim_taxonomic vt ON p."verbatim_taxonid" = vt."verbatim_taxonid"
        WHERE p.batch_serial_id = $1
        """

        progress_result = await execute_query(progress_query, batch_serial_id)
        progress_data = progress_result[0] if progress_result else None

        if progress_data:
            total = progress_data["total_records"]
            progress = {
                "taxonomic": {
                    "processed": progress_data["taxonomic_processed"],
                    "percent": round((progress_data["taxonomic_processed"] / total) * 100, 1) if total > 0 else 0,
                    "verified": progress_data["species_verified"],
                    "has_suggestions": progress_data["has_taxonomic_suggestions"],
                    "suggestions_percent": round((progress_data["has_taxonomic_suggestions"] / total) * 100, 1) if total > 0 else 0
                },
                "locality": {
                    "processed": progress_data["locality_processed"],
                    "percent": round((progress_data["locality_processed"] / total) * 100, 1) if total > 0 else 0,
                    "verified": progress_data["locality_verified"]
                },
                "record": {
                    "verified": progress_data["record_verified"],
                    "percent": round((progress_data["record_verified"] / total) * 100, 1) if total > 0 else 0
                },
                "overall": {
                    "processed": progress_data["fully_processed"],
                    "percent": round((progress_data["fully_processed"] / total) * 100, 1) if total > 0 else 0,
                    "completed": progress_data["fully_verified"]
                }
            }
        else:
            progress = None

        return ResponseModel(
            code=20000,
            data={
                "items": records,
                "total": total_count,
                "progress": progress
            }
        )
    except Exception as e:
        return ResponseModel(
            code=50000,
            data={
                "message":f"Failed to get batch records: {str(e)}"
            }

        )




# Verbatim taxonomic data endpoints
@router.get("/taxonomic/{verbatim_taxonomic_id}", response_model=ResponseModel)
async def get_verbatim_taxonomic(verbatim_taxonomic_id: int):
    """
    Get verbatim taxonomic data by ID
    获取verbatim taxonomic数据，包括原始的科、属、种信息
    """
    try:
        query = """
        SELECT * 
        FROM verbatim_taxonomic
        WHERE "verbatim_taxonid" = $1
        """

        result = await execute_query(query, verbatim_taxonomic_id)

        if not result:
            return ResponseModel(
                code=40400,
                message=f"Verbatim taxonomic data with ID {verbatim_taxonomic_id} not found"
            )

        # Format response
        verbatim_data = {
            "verbatim_taxonid": result[0]["verbatim_taxonid"],
            "verbatim_family": result[0]["verbatim_family"],
            "verbatim_genus": result[0]["verbatim_genus"],
            "verbatim_species": result[0]["verbatim_species"],
        }

        # Get potential matches
        if verbatim_data["verbatim_genus"] and verbatim_data["verbatim_species"]:
            scientific_name = f"{verbatim_data['verbatim_genus']} {verbatim_data['verbatim_species']}"
            potential_matches = await get_taxonomic_matches(scientific_name)
        else:
            potential_matches = []

        return ResponseModel(
            code=20000,
            data={
                "verbatim_data": verbatim_data,
                "potential_matches": potential_matches
            }
        )
    except Exception as e:
        return ResponseModel(
            code=50000,
            message=f"Failed to get verbatim taxonomic data: {str(e)}"
        )


@router.post("/taxonomic/auto-match", response_model=ResponseModel)
async def auto_match_species(verbatim_data: VerbatimTaxonomicModel):
    """
    Auto-match verbatim taxonomic data with taxonomic database
    自动匹配verbatim taxonomic数据与taxonomic数据库
    """
    try:
        # Validate and prepare the data
        if not verbatim_data.verbatim_genus or not verbatim_data.verbatim_species:
            return ResponseModel(
                code=40000,
                message="Both genus and species are required for auto-matching"
            )

        # Format the scientific name
        scientific_name = f"{verbatim_data.verbatim_genus} {verbatim_data.verbatim_species}"

        # Use the species validator to find matches
        match_result = await species_validator.match_taxonomic_name(scientific_name)

        if not match_result or "error" in match_result:
            return ResponseModel(
                code=40400,
                message=f"No matches found for {scientific_name}"
            )

        # Get the best match if available
        best_match = None
        if "exact_match" in match_result and match_result["exact_match"]:
            best_match = match_result["exact_match"]
        elif "close_matches" in match_result and match_result["close_matches"]:
            # Take the first close match as the best match
            best_match = match_result["close_matches"][0]

        # Format matches
        matches = []
        if "exact_match" in match_result and match_result["exact_match"]:
            matches.append({
                "taxon_id": match_result["exact_match"]["taxon_id"],
                "full_name": match_result["exact_match"]["full_name"],
                "family": match_result["exact_match"]["family"],
                "genus": match_result["exact_match"]["genus"],
                "species": match_result["exact_match"]["species"],
                "subspecies": match_result["exact_match"]["subspecies"],
                "author": match_result["exact_match"]["author"],
                "match_type": "exact",
                "similarity": 100
            })

        if "close_matches" in match_result:
            for match in match_result["close_matches"]:
                matches.append({
                    "taxon_id": match["taxon_id"],
                    "full_name": match["full_name"],
                    "family": match["family"],
                    "genus": match["genus"],
                    "species": match["species"],
                    "subspecies": match["subspecies"],
                    "author": match["author"],
                    "match_type": "close",
                    "similarity": match["similarity"] if "similarity" in match else None
                })

        return ResponseModel(
            code=20000,
            data={
                "matches": matches,
                "best_match": best_match,
                "verbatim_data": verbatim_data.dict()
            }
        )
    except Exception as e:
        return ResponseModel(
            code=50000,
            message=f"Failed to auto-match species: {str(e)}"
        )


async def get_taxonomic_matches(scientific_name: str, limit: int = 5):
    """
    Helper function to get potential taxonomic matches
    获取潜在的分类匹配
    """
    try:
        # Use species validator for matching
        match_result = await species_validator.match_taxonomic_name(scientific_name)

        matches = []
        if match_result and "error" not in match_result:
            # Add exact match if available
            if "exact_match" in match_result and match_result["exact_match"]:
                matches.append({
                    "taxon_id": match_result["exact_match"]["taxon_id"],
                    "full_name": match_result["exact_match"]["full_name"],
                    "family": match_result["exact_match"]["family"],
                    "genus": match_result["exact_match"]["genus"],
                    "species": match_result["exact_match"]["species"],
                    "match_type": "exact",
                    "similarity": 100
                })

            # Add close matches
            if "close_matches" in match_result:
                for match in match_result["close_matches"]:
                    matches.append({
                        "taxon_id": match["taxon_id"],
                        "full_name": match["full_name"],
                        "family": match["family"],
                        "genus": match["genus"],
                        "species": match["species"],
                        "match_type": "close",
                        "similarity": match["similarity"] if "similarity" in match else None
                    })

        # Limit the number of matches
        return matches[:limit]
    except Exception as e:
        print(f"Error getting taxonomic matches: {str(e)}")
        return []


# Verbatim locality data endpoints
@router.get("/locality/{verbatim_locality_id}", response_model=ResponseModel)
async def get_verbatim_locality(verbatim_locality_id: int):
    """
    Get verbatim locality data by ID
    获取verbatim locality数据，包括原始的地点信息
    """
    try:
        # 修改查询以包含所有字段
        query = """
        SELECT 
            "verbatim_localityid",
            "verbatim_locality_string",
            "verbatim_drainage",
            "verbatim_country",
            "verbatim_state",
            "verbatim_county",
            "verbatim_waterbody",
            "verbatim_lat",
            "verbatim_lon",
            "verbatim_collect_date",
            "verbatim_collector",
            "verbatim_fieldno",
            "original_text"
        FROM verbatim_locality
        WHERE "verbatim_localityid" = $1
        """

        result = await execute_query(query, verbatim_locality_id)

        if not result:
            return ResponseModel(
                code=40400,
                message=f"Verbatim locality data with ID {verbatim_locality_id} not found"
            )

        # 格式化完整的响应数据
        verbatim_data = {
            "verbatim_localityid": result[0]["verbatim_localityid"],
            "verbatim_locality_string": result[0]["verbatim_locality_string"],
            "verbatim_drainage": result[0]["verbatim_drainage"],
            "verbatim_country": result[0]["verbatim_country"],
            "verbatim_state": result[0]["verbatim_state"],
            "verbatim_county": result[0]["verbatim_county"],
            "verbatim_waterbody": result[0]["verbatim_waterbody"],
            "verbatim_lat": result[0]["verbatim_lat"],
            "verbatim_lon": result[0]["verbatim_lon"],
            "verbatim_collect_date": result[0]["verbatim_collect_date"].isoformat() if result[0]["verbatim_collect_date"] else None,
            "verbatim_collector": result[0]["verbatim_collector"],
            "verbatim_fieldno": result[0]["verbatim_fieldno"],  # 对应 field_number
            "original_text": result[0]["original_text"]
        }

        # Get potential locality matches if coordinates are available
        potential_matches = []
        if verbatim_data["verbatim_lat"] and verbatim_data["verbatim_lon"]:
            potential_matches = await get_locality_matches_by_coordinates(
                verbatim_data["verbatim_lat"],
                verbatim_data["verbatim_lon"]
            )
        elif verbatim_data["verbatim_locality_string"]:
            # If no coordinates but locality string is available
            potential_matches = await get_locality_matches_by_text(
                verbatim_data["verbatim_locality_string"],
                verbatim_data["verbatim_country"],
                verbatim_data["verbatim_state"],
                verbatim_data["verbatim_county"]
            )

        return ResponseModel(
            code=20000,
            data={
                "verbatim_data": verbatim_data,
                "potential_matches": potential_matches
            }
        )
    except Exception as e:
        return ResponseModel(
            code=50000,
            message=f"Failed to get verbatim locality data: {str(e)}"
        )

@router.post("/locality/auto-match", response_model=ResponseModel)
async def auto_match_locality(verbatim_data: VerbatimLocalityModel):
    """
    Auto-match verbatim locality data with locality database
    自动匹配verbatim locality数据与locality数据库
    """
    try:
        matches = []

        # Check if we have coordinates for matching
        if verbatim_data.verbatim_lat and verbatim_data.verbatim_lon:
            # Convert to float if they are strings
            lat = float(verbatim_data.verbatim_lat) if isinstance(verbatim_data.verbatim_lat,
                                                                  str) else verbatim_data.verbatim_lat
            lon = float(verbatim_data.verbatim_lon) if isinstance(verbatim_data.verbatim_lon,
                                                                  str) else verbatim_data.verbatim_lon

            # Find localities by coordinates with a small buffer
            coord_matches = await get_locality_matches_by_coordinates(lat, lon)
            matches.extend(coord_matches)

        # If no matches by coordinates or no coordinates available, try text matching
        if not matches and verbatim_data.verbatim_locality_string:
            text_matches = await get_locality_matches_by_text(
                verbatim_data.verbatim_locality_string,
                verbatim_data.verbatim_country,
                verbatim_data.verbatim_state,
                verbatim_data.verbatim_county
            )
            matches.extend(text_matches)

        # If field_number is available, try to find exact match by field_number
        if not matches and verbatim_data.field_number:
            field_matches = await get_locality_matches_by_field_number(verbatim_data.field_number)
            matches.extend(field_matches)

        # Get best match if available
        best_match = matches[0] if matches else None

        return ResponseModel(
            code=20000,
            data={
                "matches": matches,
                "best_match": best_match,
                "verbatim_data": verbatim_data.dict(exclude_unset=True)
            }
        )
    except Exception as e:
        return ResponseModel(
            code=50000,
            message=f"Failed to auto-match locality: {str(e)}"
        )


async def get_locality_matches_by_coordinates(lat: float, lon: float, distance_km: float = 1.0, limit: int = 5):
    """
    Helper function to get locality matches by coordinates
    根据坐标获取地点匹配
    """
    try:
        # Convert distance to approximate degrees (very rough approximation)
        # 1 degree latitude ≈ 111 km, 1 degree longitude varies with latitude
        # More precise calculations would require a spatial database like PostGIS
        distance_deg = distance_km / 111.0

        # Updated query to include FieldNumber
        query = """
        SELECT "LocalityID", "Locality", "Drainage", "Country", "State", 
               "County", "Waterbody", "Latitude", "Longitude", "FieldNumber",
               SQRT(POWER("Latitude"::float - $1, 2) + POWER("Longitude"::float - $2, 2)) as distance
        FROM locality
        WHERE "Latitude" BETWEEN $1 - $3 AND $1 + $3
          AND "Longitude" BETWEEN $2 - $3 AND $2 + $3
        ORDER BY distance
        LIMIT $4
        """

        results = await execute_query(query, lat, lon, distance_deg, limit)

        matches = []
        for result in results:
            matches.append({
                "locality_id": result["LocalityID"],
                "locality": result["Locality"],
                "drainage": result["Drainage"],
                "country": result["Country"],
                "state": result["State"],
                "county": result["County"],
                "waterbody": result["Waterbody"],
                "latitude": result["Latitude"],
                "longitude": result["Longitude"],
                "field_number": result["FieldNumber"],  # Added field_number
                "distance_km": result["distance"] * 111.0,  # Convert back to km
                "match_type": "coordinate"
            })

        return matches
    except Exception as e:
        print(f"Error getting locality matches by coordinates: {str(e)}")
        return []


async def get_locality_matches_by_text(locality_string: str, country: str = None, state: str = None, county: str = None,
                                       limit: int = 5):
    """
    Helper function to get locality matches by text
    根据文本获取地点匹配
    """
    try:
        query_parts = ["SELECT *, 0 as match_score FROM locality WHERE 1=1"]
        params = []
        param_index = 1

        # Create a scoring system for matching
        score_components = []

        if locality_string:
            query_parts.append(f"AND \"Locality\" ILIKE ${param_index}")
            params.append(f"%{locality_string}%")
            score_components.append(f"CASE WHEN \"Locality\" ILIKE ${param_index} THEN 3 ELSE 0 END")
            param_index += 1

        if country:
            query_parts.append(f"AND \"Country\" ILIKE ${param_index}")
            params.append(f"%{country}%")
            score_components.append(f"CASE WHEN \"Country\" ILIKE ${param_index} THEN 1 ELSE 0 END")
            param_index += 1

        if state:
            query_parts.append(f"AND \"State\" ILIKE ${param_index}")
            params.append(f"%{state}%")
            score_components.append(f"CASE WHEN \"State\" ILIKE ${param_index} THEN 2 ELSE 0 END")
            param_index += 1

        if county:
            query_parts.append(f"AND \"County\" ILIKE ${param_index}")
            params.append(f"%{county}%")
            score_components.append(f"CASE WHEN \"County\" ILIKE ${param_index} THEN 1 ELSE 0 END")
            param_index += 1

        # If no specific criteria, return empty results
        if not score_components:
            return []

        # Combine the base query with the scoring
        score_sql = " + ".join(score_components)
        query = f"""
        SELECT *, ({score_sql}) as match_score 
        FROM locality 
        WHERE {" AND ".join(query_parts[1:])}
        ORDER BY match_score DESC
        LIMIT ${param_index}
        """

        params.append(limit)

        results = await execute_query(query, *params)

        matches = []
        for result in results:
            # Only include results with a minimum match score
            if result["match_score"] > 0:
                matches.append({
                    "locality_id": result["LocalityID"],
                    "locality": result["Locality"],
                    "drainage": result["Drainage"],
                    "country": result["Country"],
                    "state": result["State"],
                    "county": result["County"],
                    "waterbody": result["Waterbody"],
                    "latitude": result["Latitude"],
                    "longitude": result["Longitude"],
                    "field_number": result["FieldNumber"],  # Added field_number
                    "match_score": result["match_score"],
                    "match_type": "text"
                })

        return matches
    except Exception as e:
        print(f"Error getting locality matches by text: {str(e)}")
        return []


# New function to find localities by field number
async def get_locality_matches_by_field_number(field_number: str, limit: int = 5):
    """
    Helper function to get locality matches by field number
    根据字段编号获取地点匹配
    """
    try:
        query = """
        SELECT *
        FROM locality
        WHERE "FieldNumber" ILIKE $1
        LIMIT $2
        """

        results = await execute_query(query, f"%{field_number}%", limit)

        matches = []
        for result in results:
            matches.append({
                "locality_id": result["LocalityID"],
                "locality": result["Locality"],
                "drainage": result["Drainage"],
                "country": result["Country"],
                "state": result["State"],
                "county": result["County"],
                "waterbody": result["Waterbody"],
                "latitude": result["Latitude"],
                "longitude": result["Longitude"],
                "field_number": result["FieldNumber"],
                "match_type": "field_number",
                "match_score": 5  # Higher score for field_number matches
            })

        return matches
    except Exception as e:
        print(f"Error getting locality matches by field number: {str(e)}")
        return []


# Data creation endpoints
@router.post("/taxonomic", response_model=ResponseModel)
async def create_taxonomic(taxonomic_data: Dict[str, Any]):
    """
    Create a new taxonomic record
    创建新的taxonomic记录
    """
    try:
        # Validate required fields
        required_fields = ["genus", "species"]
        for field in required_fields:
            if field not in taxonomic_data or not taxonomic_data[field]:
                return ResponseModel(
                    code=40000,
                    message=f"Field '{field}' is required"
                )

        # Check if the taxonomic record already exists
        check_query = """
        SELECT "TaxonID" 
        FROM taxonomic
        WHERE "Genus" = $1 AND "Species" = $2
        """

        check_result = await execute_query(
            check_query,
            taxonomic_data["genus"],
            taxonomic_data["species"]
        )

        if check_result:
            return ResponseModel(
                code=40900,
                message=f"Taxonomic record for {taxonomic_data['genus']} {taxonomic_data['species']} already exists",
                data={"taxon_id": check_result[0]["TaxonID"]}
            )

        # Create new taxonomic record
        insert_query = """
        INSERT INTO taxonomic (
            "Family", "Genus", "Species", "Subspecies", "Author", "Status", "FullName"
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING "TaxonID"
        """

        # Generate full name
        full_name = f"{taxonomic_data['genus']} {taxonomic_data['species']}"
        if taxonomic_data.get("subspecies"):
            full_name += f" {taxonomic_data['subspecies']}"

        insert_result = await execute_query(
            insert_query,
            taxonomic_data.get("family"),
            taxonomic_data["genus"],
            taxonomic_data["species"],
            taxonomic_data.get("subspecies"),
            taxonomic_data.get("author"),
            taxonomic_data.get("status", "valid"),
            full_name
        )

        if not insert_result:
            return ResponseModel(
                code=50000,
                message="Failed to create taxonomic record"
            )

        taxon_id = insert_result[0]["TaxonID"]

        return ResponseModel(
            code=20000,
            data={
                "taxon_id": taxon_id,
                "message": f"Taxonomic record created successfully with ID {taxon_id}"
            }
        )
    except Exception as e:
        return ResponseModel(
            code=50000,
            message=f"Failed to create taxonomic record: {str(e)}"
        )


@router.post("/locality", response_model=ResponseModel)
async def create_locality(locality_data: Dict[str, Any]):
    """
    Create a new locality record
    创建新的locality记录
    """
    try:
        # Validate required fields
        if "locality" not in locality_data or not locality_data["locality"]:
            return ResponseModel(
                code=40000,
                message="Field 'locality' is required"
            )

        # Check if the locality already exists (based on multiple criteria)
        check_clauses = ["\"Locality\" = $1"]
        check_params = [locality_data["locality"]]
        param_index = 2

        if "latitude" in locality_data and "longitude" in locality_data:
            check_clauses.append(f"\"Latitude\" = ${param_index} AND \"Longitude\" = ${param_index + 1}")
            check_params.extend([locality_data["latitude"], locality_data["longitude"]])
            param_index += 2

        # Added field_number to check criteria
        if "field_number" in locality_data and locality_data["field_number"]:
            check_clauses.append(f"\"FieldNumber\" = ${param_index}")
            check_params.append(locality_data["field_number"])
            param_index += 1

        check_query = f"""
        SELECT "LocalityID" 
        FROM locality
        WHERE {" OR ".join(check_clauses)}
        """

        check_result = await execute_query(check_query, *check_params)

        if check_result:
            return ResponseModel(
                code=40900,
                message=f"Locality record already exists",
                data={"locality_id": check_result[0]["LocalityID"]}
            )

        # Create new locality record - added FieldNumber to fields
        insert_query = """
        INSERT INTO locality (
            "Locality", "Drainage", "Country", "State", "County", 
            "Waterbody", "Latitude", "Longitude", "FieldNumber"
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        RETURNING "LocalityID"
        """

        insert_result = await execute_query(
            insert_query,
            locality_data["locality"],
            locality_data.get("drainage"),
            locality_data.get("country"),
            locality_data.get("state"),
            locality_data.get("county"),
            locality_data.get("waterbody"),
            locality_data.get("latitude"),
            locality_data.get("longitude"),
            locality_data.get("field_number")  # Added field_number
        )

        if not insert_result:
            return ResponseModel(
                code=50000,
                message="Failed to create locality record"
            )

        locality_id = insert_result[0]["LocalityID"]

        return ResponseModel(
            code=20000,
            data={
                "locality_id": locality_id,
                "message": f"Locality record created successfully with ID {locality_id}"
            }
        )
    except Exception as e:
        return ResponseModel(
            code=50000,
            message=f"Failed to create locality record: {str(e)}"
        )


# Record update endpoints
# =====================================================
# 第二部分：更新现有的 update_verbatim_record 方法
# =====================================================

@router.put("/records/{record_id}", response_model=ResponseModel)
async def update_verbatim_record(record_id: int, update_data: PrimaryRecordUpdateModel):
    """
    Update a Primary record with taxonomic and locality references
    更新Primary记录，包括分类和地点引用以及验证状态
    """
    try:
        # Verify the record exists - 只添加验证状态字段到查询
        check_query = """
        SELECT "PrimaryID", "CatalogNumber", "TaxonID", "Locality1ID", "review_flag",
               "verbatim_localityid", "species_verification_status",
               "locality_verification_status", "record_verification_status"
        FROM primary_temp
        WHERE "PrimaryID" = $1
        """

        check_result = await execute_query(check_query, record_id)

        if not check_result:
            return ResponseModel(
                code=40400,
                message=f"Record with ID {record_id} not found"
            )

        existing_record = check_result[0]

        # Prepare update fields for Primary table
        update_fields = []
        update_values = []
        param_index = 1

        if hasattr(update_data, "taxon_id") and update_data.taxon_id is not None:
            if update_data.species_verification_status is None:
                update_data.species_verification_status = 'verified'
        elif hasattr(update_data, "taxon_id") and update_data.taxon_id is None:
            if update_data.species_verification_status is None:
                update_data.species_verification_status = 'pending'

            # 如果更新了locality_id，自动设置locality_verification_status
        if hasattr(update_data, "locality_id") and update_data.locality_id is not None:
            if update_data.locality_verification_status is None:
                update_data.locality_verification_status = 'verified'
        elif hasattr(update_data, "locality_id") and update_data.locality_id is None:
            if update_data.locality_verification_status is None:
                update_data.locality_verification_status = 'pending'

        # Map of field names to database column names for Primary table
        field_mapping = {
            "taxon_id": "TaxonID",
            "locality_id": "Locality1ID",
            "collection_date": "CollectionDate",
            "total_number": "TotalNumber",
            "storage": "Storage",
            "jar_size": "JarSize",
            "prev_number": "PrevNumber",
            "inventory": "Inventory",
            "remarks": "Remarks",
            "review_flag": "review_flag",
            "species_verification_status": "species_verification_status",
            "locality_verification_status": "locality_verification_status",
            "record_verification_status": "record_verification_status",
            "verification_notes": "verification_notes"
        }

        # Add fields to update for Primary table
        for field, db_column in field_mapping.items():
            if hasattr(update_data, field) and getattr(update_data, field) is not None:
                update_fields.append(f"\"{db_column}\" = ${param_index}")
                update_values.append(getattr(update_data, field))
                param_index += 1

        # 自动计算 overall_verification_status（派生字段）
        # 获取将要更新后的状态值
        species_status = update_data.species_verification_status if hasattr(update_data, "species_verification_status") and update_data.species_verification_status else existing_record.get("species_verification_status", "pending")
        locality_status = update_data.locality_verification_status if hasattr(update_data, "locality_verification_status") and update_data.locality_verification_status else existing_record.get("locality_verification_status", "pending")
        record_status = update_data.record_verification_status if hasattr(update_data, "record_verification_status") and update_data.record_verification_status else existing_record.get("record_verification_status", "pending")

        # 计算overall状态：只有当三个都是verified时才是completed
        if species_status == "verified" and locality_status == "verified" and record_status == "verified":
            overall_status = "completed"
        else:
            overall_status = "pending"

        update_fields.append(f"\"overall_verification_status\" = ${param_index}")
        update_values.append(overall_status)
        param_index += 1

        # Always update timestamp
        update_fields.append(f"\"TimeStampModified\" = ${param_index}")
        update_values.append(datetime.now())
        param_index += 1

        # If nothing to update in Primary table, check for field_number updates (保持现有逻辑不变)
        has_primary_updates = len(update_fields) > 1  # More than just timestamp

        # Build and execute update query for primary_temp table if needed (保持现有逻辑不变)
        if has_primary_updates:
            update_query = f"""
            UPDATE primary_temp
            SET {", ".join(update_fields)}
            WHERE "PrimaryID" = ${param_index}
            RETURNING "PrimaryID", "CatalogNumber", "TimeStampModified"
            """

            update_values.append(record_id)

            update_result = await execute_query(update_query, *update_values)

            if not update_result:
                return ResponseModel(
                    code=50000,
                    message="Failed to update record"
                )
        else:
            # No Primary table updates, but still need timestamp for response (保持现有逻辑不变)
            update_result = [{
                "PrimaryID": record_id,
                "TimeStampModified": datetime.now()
            }]

        # If taxonomic or locality fields were updated, also update preparation records if needed (保持现有逻辑不变)
        prep_update_needed = False
        if hasattr(update_data, "total_number") and update_data.total_number is not None:
            prep_update_needed = True

        if prep_update_needed:
            prep_update_query = """
            UPDATE preparation_temp
            SET "Count" = $1, "TimeStampModified" =$2
            WHERE "PrimaryID" = $3
            RETURNING "PreparationID"
            """

            await execute_query(
                prep_update_query,
                update_data.total_number,
                datetime.now(),
                record_id
            )

        # Handle field_number update (保持现有逻辑不变)
        field_number_updated = False

        # Update verbatim_locality if needed (保持现有逻辑不变)
        if hasattr(update_data, "field_number") and existing_record["verbatim_localityid"]:
            verbatim_locality_update = """
            UPDATE verbatim_locality
            SET "field_number" = $1, "TimeStampModified" = $2
            WHERE "verbatim_localityid" = $3
            RETURNING "verbatim_localityid"
            """

            verbatim_result = await execute_query(
                verbatim_locality_update,
                update_data.field_number,
                datetime.now(),
                existing_record["verbatim_localityid"]
            )

            field_number_updated = True if verbatim_result else False

        # Update matched locality if needed (保持现有逻辑不变)
        if hasattr(update_data, "field_number") and existing_record["Locality1ID"]:
            locality_update = """
            UPDATE locality1
            SET "FieldNo" = $1, "TimeStampModified" = $2
            WHERE "Locality1ID" = $3
            RETURNING "Locality1ID"
            """

            locality_result = await execute_query(
                locality_update,
                update_data.field_number,
                datetime.now(),
                existing_record["Locality1ID"]
            )

            field_number_updated = True if locality_result else field_number_updated


        # If taxonomic or locality fields were updated, check if the record is now fully processed (保持现有逻辑不变)
        taxonomic_processed = (existing_record["TaxonID"] is not None) or (
                hasattr(update_data, "taxon_id") and update_data.taxon_id is not None
        )
        locality_processed = (existing_record["Locality1ID"] is not None) or (
                hasattr(update_data, "locality_id") and update_data.locality_id is not None
        )

        # If both are processed and review flag hasn't been explicitly set, mark as reviewed (保持现有逻辑不变)
        if taxonomic_processed and locality_processed and not hasattr(update_data, "review_flag"):
            review_update_query = """
            UPDATE primary_temp
            SET "review_flag" = false, "TimeStampModified" = $1
            WHERE "PrimaryID" = $2
            """

            await execute_query(review_update_query, datetime.now(), record_id)

        # Get the batch_serial_id for this record (保持现有逻辑不变)
        batch_query = """
        SELECT batch_serial_id
        FROM primary_temp
        WHERE "PrimaryID" = $1
        """

        batch_result = await execute_query(batch_query, record_id)
        batch_serial_id = batch_result[0]["batch_serial_id"] if batch_result else None

        # Add to system logs (保持现有逻辑不变)
        log_query = """
        INSERT INTO system_logs (action_type, action_details, created_at)
        VALUES ($1, $2, $3)
        """

        updated_fields = [field for field, db_column in field_mapping.items()
                          if hasattr(update_data, field) and getattr(update_data, field) is not None]

        if field_number_updated:
            updated_fields.append("field_number")

        log_details = {
            "record_id": record_id,
            "catalog_number": update_result[0]["CatalogNumber"] if "CatalogNumber" in update_result[0] else None,
            "fields_updated": updated_fields,
            "batch_serial_id": batch_serial_id,
            "timestamp": datetime.now().isoformat()
        }

        await execute_mutation(
            log_query,
            "record_update",
            json.dumps(log_details),
            datetime.now()
        )

        # Re-validate the record after update to check if warnings are still valid
        await revalidate_single_record(record_id)

        return ResponseModel(
            code=20000,
            data={
                "record_id": record_id,
                "message": "Record updated successfully",
                "updated_at": update_result[0]["TimeStampModified"].isoformat() if "TimeStampModified" in update_result[
                    0] and update_result[0]["TimeStampModified"] else None,
                "updated_fields": updated_fields
            }
        )
    except Exception as e:
        return ResponseModel(
            code=50000,
            data={
                "message":f"Failed to update record: {str(e)}"
                  }
        )

@router.post("/batches/{batch_serial_id}/bulk-update", response_model=ResponseModel)
async def bulk_update_records(
        batch_serial_id: str,
        updates: Dict[str, Any]
):
    """
    Bulk update multiple records within a batch
    批量更新批次内的多条记录
    """
    try:
        # Validate input
        if "record_ids" not in updates or not isinstance(updates["record_ids"], list) or not updates["record_ids"]:
            return ResponseModel(
                code=40000,
                message="Field 'record_ids' is required and must be a non-empty list"
            )

        record_ids = updates["record_ids"]

        # Remove record_ids from updates
        field_updates = {k: v for k, v in updates.items() if k != "record_ids"}

        if not field_updates:
            return ResponseModel(
                code=40000,
                message="No update fields provided"
            )

        # Separate field_number from other fields
        field_number = field_updates.pop("field_number", None)

        # Map field names to database column names for Primary table
        field_mapping = {
            "taxon_id": "TaxonID",
            "locality_id": "LocalityID",
            "collection_date": "CollectionDate",
            "total_number": "TotalNumber",
            "storage": "Storage",
            "jar_size": "JarSize",
            "prev_number": "PrevNumber",
            "inventory": "Inventory",
            "remarks": "Remarks",
            "review_flag": "review_flag",
            "species_verification_status": "species_verification_status",
            "locality_verification_status": "locality_verification_status",
            "record_verification_status": "record_verification_status",
            "verification_notes": "verification_notes"

        }

        # Prepare update fields for Primary table
        update_fields = []
        update_values = []
        param_index = 1

        # Add fields to update for Primary table
        for field, value in field_updates.items():
            if field in field_mapping and value is not None:
                update_fields.append(f"\"{field_mapping[field]}\" = ${param_index}")
                update_values.append(value)
                param_index += 1

        # Track what was actually updated
        updated_fields = list(field_updates.keys())

        # Always update timestamp
        update_fields.append(f"\"TimeStampModified\" = ${param_index}")
        update_values.append(datetime.now())
        param_index += 1

        # Update primary_temp records if there are fields to update
        primary_updated_ids = []
        if update_fields:
            # Build and execute update query for primary_temp table
            record_placeholders = ", ".join([f"${i}" for i in range(param_index, param_index + len(record_ids))])
            update_query = f"""
            UPDATE primary_temp
            SET {", ".join(update_fields)}
            WHERE "PrimaryID" IN ({record_placeholders})
            AND batch_serial_id = ${param_index + len(record_ids)}
            RETURNING "PrimaryID"
            """

            update_values.extend(record_ids)
            update_values.append(batch_serial_id)

            update_result = await execute_query(update_query, *update_values)
            primary_updated_ids = [r["PrimaryID"] for r in update_result]

            # If total_number was updated, also update preparation_temp records
            if "total_number" in field_updates:
                # For each primary ID, update its preparation_temp records
                for primary_id in primary_updated_ids:
                    prep_update_query = """
                    UPDATE preparation_temp
                    SET "Count" = $1, "TimeStampModified" = $2
                    WHERE "PrimaryID" = $3
                    """

                    await execute_mutation(
                        prep_update_query,
                        field_updates["total_number"],
                        datetime.now(),
                        primary_id
                    )

        # Update field_number in locality tables if needed
        field_number_updated_ids = []
        if field_number is not None:
            # First, get verbatim_locality_ids and locality_ids for all records
            ids_query = """
            SELECT "PrimaryID", "verbatim_localityid", "Locality1ID"
            FROM primary_temp
            WHERE "PrimaryID" IN ({}) AND batch_serial_id = $1
            """.format(",".join([str(id) for id in record_ids]))

            ids_result = await execute_query(ids_query, batch_serial_id)

            # Update verbatim_locality records
            verbatim_ids = [r["verbatim_localityid"] for r in ids_result if r["verbatim_localityid"] is not None]
            if verbatim_ids:
                verbatim_placeholders = ", ".join([f"${i + 1}" for i in range(len(verbatim_ids))])
                verbatim_update = f"""
                UPDATE verbatim_locality
                SET "field_number" = $1, "TimeStampModified" = $2
                WHERE "verbatim_localityid" IN ({verbatim_placeholders})
                RETURNING "verbatim_localityid"
                """

                verbatim_result = await execute_query(
                    verbatim_update,
                    field_number,
                    datetime.now(),
                    *verbatim_ids
                )

                # Track records that had field_number updated
                for r in ids_result:
                    if r["verbatim_localityid"] in [vr["verbatim_localityid"] for vr in verbatim_result]:
                        field_number_updated_ids.append(r["PrimaryID"])

            # Update locality records
            locality_ids = [r["Locality1ID"] for r in ids_result if r["Locality1ID"] is not None]
            if locality_ids:
                locality_placeholders = ", ".join([f"${i + 1}" for i in range(len(locality_ids))])
                locality_update = f"""
                UPDATE locality1
                SET "FieldNo" = $1, "TimeStampModified" = $2
                WHERE "Locality1ID" IN ({locality_placeholders})
                RETURNING "Locality1ID"
                """

                locality_result = await execute_query(
                    locality_update,
                    field_number,
                    datetime.now(),
                    *locality_ids
                )

                # Track records that had field_number updated
                for r in ids_result:
                    if r["Locality1ID"] in [lr["Locality1ID"] for lr in locality_result]:
                        if r["PrimaryID"] not in field_number_updated_ids:
                            field_number_updated_ids.append(r["PrimaryID"])

            if field_number_updated_ids:
                updated_fields.append("field_number")

        # Combine all updated IDs
        all_updated_ids = list(set(primary_updated_ids + field_number_updated_ids))

        # Add to system logs
        log_query = """
        INSERT INTO system_logs (action_type, action_details, created_at)
        VALUES ($1, $2, $3)
        """

        log_details = {
            "batch_serial_id": batch_serial_id,
            "record_count": len(all_updated_ids),
            "fields_updated": updated_fields,
            "timestamp": datetime.now().isoformat()
        }

        await execute_mutation(
            log_query,
            "batch_record_update",
            json.dumps(log_details),
            datetime.now()
        )

        return ResponseModel(
            code=20000,
            data={
                "updated_count": len(all_updated_ids),
                "message": f"Successfully updated {len(all_updated_ids)} records",
                "updated_ids": all_updated_ids,
                "updated_fields": updated_fields
            }
        )
    except Exception as e:
        return ResponseModel(
            code=50000,
            message=f"Failed to bulk update records: {str(e)}"
        )


@router.post("/batches/{batch_serial_id}/complete", response_model=ResponseModel)
async def mark_batch_completed(batch_serial_id: str):
    """
    Mark a batch as completed
    将批次标记为已完成
    """
    try:
        # Check if the batch exists and all records are fully verified
        check_query = """
        SELECT COUNT(*) as count,
               -- SUM(CASE WHEN "TaxonID" IS NULL OR "Locality1ID" IS NULL THEN 1 ELSE 0 END) as incomplete,
               SUM(CASE WHEN "overall_verification_status" != 'completed' THEN 1 ELSE 0 END) as not_verified
        FROM primary_temp
        WHERE batch_serial_id = $1
        """

        check_result = await execute_query(check_query, batch_serial_id)

        if not check_result or check_result[0]["count"] == 0:
            return ResponseModel(
                code=40400,
                message=f"Batch with serial ID {batch_serial_id} not found"
            )

        # 注释掉 TaxonID/Locality1ID 检查 - 如果后续需要可以打开
        # Check if all records are processed (have TaxonID and Locality1ID)
        # incomplete_count = check_result[0]["incomplete"]
        # if incomplete_count > 0:
        #     return ResponseModel(
        #         code=40000,
        #         message=f"Cannot mark batch as completed. {incomplete_count} records are still missing TaxonID or Locality1ID."
        #     )

        # Check if all records are verified (overall_verification_status = 'completed')
        not_verified_count = check_result[0]["not_verified"]
        if not_verified_count > 0:
            return ResponseModel(
                code=40000,
                message=f"Cannot mark batch as completed. {not_verified_count} records have not been fully verified (overall_verification_status != 'completed')."
            )

        # Mark all records in the batch as not needing review
        update_query = """
        UPDATE primary_temp
        SET "review_flag" = false, "TimeStampModified" = $1
        WHERE batch_serial_id = $2
        RETURNING "PrimaryID"
        """

        update_result = await execute_query(update_query, datetime.now(), batch_serial_id)

        # Add to system logs
        log_query = """
        INSERT INTO system_logs (action_type, action_details, created_at)
        VALUES ($1, $2, $3)
        """

        log_details = {
            "batch_serial_id": batch_serial_id,
            "records_count": len(update_result),
            "status": "completed",
            "timestamp": datetime.now().isoformat()
        }

        await execute_mutation(
            log_query,
            "batch_completion",
            json.dumps(log_details),
            datetime.now()
        )

        return ResponseModel(
            code=20000,
            data={
                "batch_serial_id": batch_serial_id,
                "message": f"Batch marked as completed with {len(update_result)} records",
                "completed_at": datetime.now().isoformat()
            }
        )
    except Exception as e:
        return ResponseModel(
            code=50000,
            message=f"Failed to mark batch as completed: {str(e)}"
        )


@router.get("/batches/{batch_serial_id}/progress", response_model=ResponseModel)
async def get_batch_progress(batch_serial_id: str):
    """
    Get detailed progress statistics for a batch
    获取批次的详细进度统计
    """
    try:
        # Query batch progress
        progress_query = """
        SELECT
            batch_serial_id,
            COUNT(*) as total_records,
            SUM(CASE WHEN "TaxonID" IS NOT NULL THEN 1 ELSE 0 END) as taxonomic_processed,
            SUM(CASE WHEN "Locality1ID" IS NOT NULL THEN 1 ELSE 0 END) as locality_processed,
            SUM(CASE WHEN "TaxonID" IS NOT NULL AND "Locality1ID" IS NOT NULL THEN 1 ELSE 0 END) as fully_processed,
            SUM(CASE WHEN review_flag = false THEN 1 ELSE 0 END) as reviewed_records
        FROM primary_temp
        WHERE batch_serial_id = $1
        GROUP BY batch_serial_id
        """

        progress_result = await execute_query(progress_query, batch_serial_id)

        if not progress_result:
            return ResponseModel(
                code=40400,
                message=f"Batch with serial ID {batch_serial_id} not found"
            )

        batch = progress_result[0]
        total = batch["total_records"]

        # Calculate time statistics
        time_query = """
        SELECT
            MIN("TimeStampModified") as start_time,
            MAX("TimeStampModified") as last_update,
            MAX("TimeStampModified") - MIN("TimeStampModified") as duration
        FROM primary_temp
        WHERE batch_serial_id = $1
        """

        time_result = await execute_query(time_query, batch_serial_id)
        time_stats = time_result[0] if time_result else {}

        # Format response
        progress_data = {
            "batch_serial_id": batch_serial_id,
            "total_records": total,
            "progress": {
                "taxonomic": {
                    "processed": batch["taxonomic_processed"],
                    "pending": total - batch["taxonomic_processed"],
                    "percent": round((batch["taxonomic_processed"] / total) * 100, 1) if total > 0 else 0
                },
                "locality": {
                    "processed": batch["locality_processed"],
                    "pending": total - batch["locality_processed"],
                    "percent": round((batch["locality_processed"] / total) * 100, 1) if total > 0 else 0
                },
                "review": {
                    "processed": batch["reviewed_records"],
                    "pending": total - batch["reviewed_records"],
                    "percent": round((batch["reviewed_records"] / total) * 100, 1) if total > 0 else 0
                },
                "overall": {
                    "processed": batch["fully_processed"],
                    "pending": total - batch["fully_processed"],
                    "percent": round((batch["fully_processed"] / total) * 100, 1) if total > 0 else 0
                }
            },
            "time_stats": {
                "start_time": time_stats.get("start_time").isoformat() if time_stats.get("start_time") else None,
                "last_update": time_stats.get("last_update").isoformat() if time_stats.get("last_update") else None,
                "duration": str(time_stats.get("duration")) if time_stats.get("duration") else None
            },
            "status": "completed" if batch["fully_processed"] == total else "in_progress"
        }

        return ResponseModel(
            code=20000,
            data=progress_data
        )
    except Exception as e:
        return ResponseModel(
            code=50000,
            message=f"Failed to get batch progress: {str(e)}"
        )


@router.get("/batches/{batch_serial_id}/export", response_model=None)
async def export_batch_results(batch_serial_id: str):
    """
    Export batch results to Excel file
    将批次结果导出为Excel文件
    """
    try:
        # Query all records in the batch with related data - updated to include field_number from locality tables
        query = """
        SELECT
            p."PrimaryID",
            p."CatalogNumber",
            p."verbatim_taxonid",
            p."verbatim_localityid",
            p."TaxonID",
            p."Locality1ID",
            p."TotalNumber",
            p."Storage",
            p."JarSize",
            p."PrevNumber",
            p."Inventory",
            p."Remarks",
            p."TimeStampModified",
            p."review_flag",
            vt."verbatim_family",
            vt."verbatim_genus",
            vt."verbatim_species",
            vl."verbatim_locality_string",
            vl."verbatim_country",
            vl."verbatim_state",
            vl."verbatim_county",
            vl."verbatim_drainage",
            vl."verbatim_waterbody",
            vl."verbatim_lat",
            vl."verbatim_lon",
            vl."verbatim_fieldno" as verbatim_field_number,
            vl."verbatim_collect_date" as verbatim_collection_date,
            vl."verbatim_collector",
            fam."FamilyName" as matched_family,
            t."Genus" as matched_genus,
            t."Species" as matched_species,
            l."LocalityString" as matched_locality,
            l."Country" as matched_country,
            l."State" as matched_state,
            l."County" as matched_county,
            l."Drainage" as matched_drainage,
            l."WaterBody" as matched_waterbody,
            l."Lat" as matched_lat,
            l."Lon" as matched_lon,
            l."FieldNo" as matched_field_number,
            prep."PreparationID",
            prep."PreparationType",
            prep."Count"
        FROM primary_temp p
        LEFT JOIN verbatim_taxonomic vt ON p."verbatim_taxonid" = vt."verbatim_taxonid"
        LEFT JOIN verbatim_locality vl ON p."verbatim_localityid" = vl."verbatim_localityid"
        LEFT JOIN "TaxonomicTable" t ON p."TaxonID" = t."TaxonID"
        LEFT JOIN "Family" fam ON t."FamilyID" = fam."FamilyID"
        LEFT JOIN locality1 l ON p."Locality1ID" = l."Locality1ID"
        LEFT JOIN preparation_temp prep ON p."PrimaryID" = prep."PrimaryID"
        WHERE p.batch_serial_id = $1
        ORDER BY p."CatalogNumber"
        """

        result = await execute_query(query, batch_serial_id)

        if not result:
            raise HTTPException(status_code=404, detail=f"Batch with serial ID {batch_serial_id} not found")

        # Create a pandas DataFrame from the results
        df = pd.DataFrame(result)

        # Format dates
        date_columns = ["verbatim_collection_date", "TimeStampModified"]
        for col in date_columns:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: x.isoformat() if x else None)

        # Reorder and rename columns for better readability - updated to include field_number columns
        column_mapping = {
            "CatalogNumber": "Catalog Number",
            "matched_family": "Family",
            "matched_genus": "Genus",
            "matched_species": "Species",
            "matched_locality": "Locality",
            "matched_country": "Country",
            "matched_state": "State",
            "matched_county": "County",
            "matched_drainage": "Drainage",
            "matched_waterbody": "Waterbody",
            "matched_lat": "Latitude",
            "matched_lon": "Longitude",
            "matched_field_number": "Field Number",
            "TotalNumber": "Total Number",
            "Storage": "Storage",
            "JarSize": "Jar Size",
            "PrevNumber": "Previous Number",
            "Inventory": "Inventory",
            "Remarks": "Remarks",
            "PreparationType": "Preparation Type",
            "Count": "Specimen Count",
            "verbatim_family": "Verbatim Family",
            "verbatim_genus": "Verbatim Genus",
            "verbatim_species": "Verbatim Species",
            "verbatim_locality_string": "Verbatim Locality",
            "verbatim_country": "Verbatim Country",
            "verbatim_state": "Verbatim State",
            "verbatim_county": "Verbatim County",
            "verbatim_drainage": "Verbatim Drainage",
            "verbatim_waterbody": "Verbatim Waterbody",
            "verbatim_lat": "Verbatim Latitude",
            "verbatim_lon": "Verbatim Longitude",
            "verbatim_field_number": "Verbatim Field Number",
            "verbatim_collection_date": "Verbatim Collection Date",
            "verbatim_collector": "Verbatim Collector"
        }

        # Keep only the columns we want to export
        export_columns = list(column_mapping.keys())
        export_df = df[export_columns].rename(columns=column_mapping)

        # Create a temporary file for the Excel export
        os.makedirs("temp", exist_ok=True)
        temp_file = f"temp/batch_export_{batch_serial_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

        # Write to Excel
        with pd.ExcelWriter(temp_file, engine='xlsxwriter') as writer:
            export_df.to_excel(writer, sheet_name='Records', index=False)

            # Create a summary sheet
            summary_data = {
                "Total Records": len(df),
                "Taxonomic Processed": sum(df["TaxonID"].notna()),
                "Locality Processed": sum(df["Locality1ID"].notna()),
                "Fully Processed": sum((df["TaxonID"].notna()) & (df["Locality1ID"].notna())),
                "Batch ID": batch_serial_id,
                "Export Date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }

            summary_df = pd.DataFrame(list(summary_data.items()), columns=['Metric', 'Value'])
            summary_df.to_excel(writer, sheet_name='Summary', index=False)

            # Adjust column widths
            for sheet_name in writer.sheets:
                worksheet = writer.sheets[sheet_name]
                for i, col in enumerate(df.columns):
                    max_len = max(df[col].astype(str).apply(len).max(), len(col) + 2)
                    worksheet.set_column(i, i, max_len)

        # Return the file as a response
        return FileResponse(
            path=temp_file,
            filename=f"batch_export_{batch_serial_id}.xlsx",
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to export batch results: {str(e)}")


@router.get("/statistics", response_model=ResponseModel)
async def get_verbatim_statistics(days: int = Query(30, ge=1, le=365)):
    """
    Get verbatim processing statistics
    获取verbatim处理统计信息
    """
    try:
        # Query recent batch statistics
        batch_query = """
        SELECT
            COUNT(DISTINCT batch_serial_id) as total_batches,
            SUM(CASE WHEN NOT EXISTS (
                SELECT 1 FROM primary_temp p2
                WHERE p2.batch_serial_id = p1.batch_serial_id
                AND (p2."TaxonID" IS NULL OR p2."Locality1ID" IS NULL)
            ) THEN 1 ELSE 0 END) as completed_batches
        FROM (
            SELECT DISTINCT batch_serial_id
            FROM primary_temp
            WHERE "TimeStampModified" >= NOW() - INTERVAL '%s days'
        ) p1
        """

        batch_result = await execute_query(batch_query % days)

        # Query recent record statistics
        record_query = """
        SELECT
            COUNT(*) as total_records,
            SUM(CASE WHEN "TaxonID" IS NOT NULL THEN 1 ELSE 0 END) as taxonomic_processed,
            SUM(CASE WHEN "Locality1ID" IS NOT NULL THEN 1 ELSE 0 END) as locality_processed,
            SUM(CASE WHEN "TaxonID" IS NOT NULL AND "Locality1ID" IS NOT NULL THEN 1 ELSE 0 END) as fully_processed,
            COUNT(DISTINCT batch_serial_id) as batches_count
        FROM primary_temp
        WHERE "TimeStampModified" >= NOW() - INTERVAL '%s days'
        """

        record_result = await execute_query(record_query % days)

        # Query top active batches
        active_batch_query = """
        SELECT
            batch_serial_id,
            COUNT(*) as total_records,
            SUM(CASE WHEN "TaxonID" IS NOT NULL THEN 1 ELSE 0 END) as taxonomic_processed,
            SUM(CASE WHEN "Locality1ID" IS NOT NULL THEN 1 ELSE 0 END) as locality_processed,
            SUM(CASE WHEN "TaxonID" IS NOT NULL AND "Locality1ID" IS NOT NULL THEN 1 ELSE 0 END) as fully_processed,
            MIN("TimeStampModified") as import_date,
            MAX("TimeStampModified") as last_modified
        FROM primary_temp
        WHERE "TimeStampModified" >= NOW() - INTERVAL '%s days'
        GROUP BY batch_serial_id
        ORDER BY last_modified DESC
        LIMIT 5
        """

        active_batch_result = await execute_query(active_batch_query % days)

        # Format results
        batch_stats = batch_result[0] if batch_result else {}
        record_stats = record_result[0] if record_result else {}

        active_batches = []
        for batch in active_batch_result:
            total = batch["total_records"]
            active_batches.append({
                "batch_serial_id": batch["batch_serial_id"],
                "total_records": total,
                "processed_percent": round((batch["fully_processed"] / total) * 100, 1) if total > 0 else 0,
                "import_date": batch["import_date"].isoformat() if batch["import_date"] else None,
                "last_modified": batch["last_modified"].isoformat() if batch["last_modified"] else None
            })

        statistics = {
            "period_days": days,
            "batches": {
                "total": batch_stats.get("total_batches", 0),
                "completed": batch_stats.get("completed_batches", 0),
                "completion_rate": round(
                    (batch_stats.get("completed_batches", 0) / batch_stats.get("total_batches", 1)) * 100, 1)
            },
            "records": {
                "total": record_stats.get("total_records", 0),
                "taxonomic_processed": record_stats.get("taxonomic_processed", 0),
                "taxonomic_rate": round(
                    (record_stats.get("taxonomic_processed", 0) / record_stats.get("total_records", 1)) * 100, 1),
                "locality_processed": record_stats.get("locality_processed", 0),
                "locality_rate": round(
                    (record_stats.get("locality_processed", 0) / record_stats.get("total_records", 1)) * 100, 1),
                "fully_processed": record_stats.get("fully_processed", 0),
                "completion_rate": round(
                    (record_stats.get("fully_processed", 0) / record_stats.get("total_records", 1)) * 100, 1)
            },
            "active_batches": active_batches
        }

        return ResponseModel(
            code=20000,
            data=statistics
        )
    except Exception as e:
        return ResponseModel(
            code=50000,
            message=f"Failed to get verbatim statistics: {str(e)}"
        )


@router.post("/records/batch-verify", response_model=ResponseModel)
async def batch_update_verification_status(update_data: BatchVerificationUpdateModel):
    """
    批量更新记录的验证状态 - 新增端点，不影响现有API
    """
    try:
        if not update_data.record_ids:
            return ResponseModel(
                code=40000,
                message="No record IDs provided"
            )

        # 构建更新字段
        update_fields = []
        params = [update_data.status]
        param_index = 2

        if update_data.verification_type in ['species', 'all']:
            update_fields.append(f'"species_verification_status" = $1')

        if update_data.verification_type in ['locality', 'all']:
            update_fields.append(f'"locality_verification_status" = $1')

        if update_data.verification_type in ['record', 'all']:
            update_fields.append(f'"record_verification_status" = $1')

        if update_data.notes:
            update_fields.append(f'"verification_notes" = ${param_index}')
            params.append(update_data.notes)
            param_index += 1

        # 始终更新时间戳
        update_fields.append(f'"TimeStampModified" = ${param_index}')
        params.append(datetime.now())
        param_index += 1

        # 构建查询
        record_placeholders = ", ".join([f"${i + param_index - 1}" for i in range(len(update_data.record_ids))])

        update_query = f"""
        UPDATE primary_temp
        SET {", ".join(update_fields)}
        WHERE "PrimaryID" IN ({record_placeholders})
        RETURNING "PrimaryID"
        """

        # 添加记录ID到参数
        final_params = params + update_data.record_ids

        result = await execute_query(update_query, *final_params)

        return ResponseModel(
            code=20000,
            data={
                "updated_count": len(result),
                "message": f"Successfully updated verification status for {len(result)} records",
                "verification_type": update_data.verification_type,
                "status": update_data.status
            }
        )

    except Exception as e:
        return ResponseModel(
            code=50000,
            message=f"Failed to batch update verification status: {str(e)}"
        )


# 新增应用分类建议端点 - 添加验证状态更新
@router.post("/records/{record_id}/apply-suggestion", response_model=ResponseModel)
async def apply_taxonomic_suggestion(record_id: int):
    """
    应用分类建议并更新验证状态
    """
    try:
        # 获取记录和建议信息
        suggestion_query = """
        SELECT p."PrimaryID", p."verbatim_taxonid", vt."matched_taxon_id"
        FROM primary_temp p
        LEFT JOIN verbatim_taxonomic vt ON p."verbatim_taxonid" = vt."verbatim_taxonid"
        WHERE p."PrimaryID" = $1 AND vt."matched_taxon_id" IS NOT NULL
        """

        suggestion_result = await execute_query(suggestion_query, record_id)

        if not suggestion_result:
            return ResponseModel(
                code=40400,
                message="No taxonomic suggestion found for this record"
            )

        suggested_taxon_id = suggestion_result[0]["matched_taxon_id"]

        # 应用建议并更新验证状态
        apply_query = """
        UPDATE primary_temp
        SET "TaxonID" = $1,
            "species_verification_status" = 'verified',
            "TimeStampModified" = $2
        WHERE "PrimaryID" = $3
        RETURNING "PrimaryID"
        """

        apply_result = await execute_query(apply_query, suggested_taxon_id, datetime.now(), record_id)

        if apply_result:
            # 标记建议已应用
            mark_applied_query = """
            UPDATE verbatim_taxonomic
            SET "suggestion_applied" = true
            WHERE "verbatim_taxonid" = (
                SELECT "verbatim_taxonid" FROM primary_temp WHERE "PrimaryID" = $1
            )
            """

            await execute_mutation(mark_applied_query, record_id)

            return ResponseModel(
                code=20000,
                data={
                    "record_id": record_id,
                    "applied_taxon_id": suggested_taxon_id,
                    "message": "Taxonomic suggestion applied successfully"
                }
            )
        else:
            return ResponseModel(
                code=50000,
                message="Failed to apply taxonomic suggestion"
            )

    except Exception as e:
        return ResponseModel(
            code=50000,
            message=f"Failed to apply taxonomic suggestion: {str(e)}"
        )


# Helper function to revalidate a single record after update
async def revalidate_single_record(record_id: int):
    """
    重新验证单个记录的warnings，在记录更新后调用
    检查：
    1. TotalNumber 是否为有效数字
    2. Storage 是否有效
    3. 其他 record details 字段
    移除已修复的warnings，保留仍存在的warnings
    """
    try:
        # 查询记录当前数据
        query = """
        SELECT
            p."PrimaryID",
            p."TotalNumber",
            p."Storage",
            p."JarSize",
            p."PrevNumber",
            p."Inventory",
            p."verification_warnings",
            p."TaxonID",
            p."Locality1ID",
            vl."verbatim_fieldno"
        FROM primary_temp p
        LEFT JOIN verbatim_locality vl ON p."verbatim_localityid" = vl."verbatim_localityid"
        WHERE p."PrimaryID" = $1
        """

        result = await execute_query(query, record_id)
        if not result or len(result) == 0:
            return

        record = result[0]
        new_warnings = []

        # 验证 TotalNumber
        total_number = record.get("TotalNumber")
        if total_number is None or total_number == "":
            new_warnings.append({
                "field": "TotalNumber",
                "issue_type": "missing_value",
                "severity": "warning",
                "message": "TotalNumber is empty, defaulted to 1"
            })
        elif not isinstance(total_number, (int, float)):
            try:
                int(total_number)
            except (ValueError, TypeError):
                new_warnings.append({
                    "field": "TotalNumber",
                    "issue_type": "data_type",
                    "severity": "error",
                    "message": f"TotalNumber value '{total_number}' is not a valid number"
                })

        # 验证 Storage
        storage = record.get("Storage")
        if storage is None or str(storage).strip() == "":
            new_warnings.append({
                "field": "Storage",
                "issue_type": "missing_value",
                "severity": "warning",
                "message": "Storage location is empty"
            })

        # 验证 JarSize
        jar_size = record.get("JarSize")
        if jar_size is None or str(jar_size).strip() == "":
            new_warnings.append({
                "field": "JarSize",
                "issue_type": "missing_value",
                "severity": "warning",
                "message": "Jar size is not specified"
            })

        # 根据warnings的存在情况设置 record_verification_status
        has_errors = any(w.get("severity") == "error" for w in new_warnings)
        record_status = "pending" if has_errors else "verified"

        # 同时检查 species 和 locality 状态来计算 overall_status
        species_status_query = """
        SELECT
            "species_verification_status",
            "locality_verification_status"
        FROM primary_temp
        WHERE "PrimaryID" = $1
        """
        status_result = await execute_query(species_status_query, record_id)

        if status_result:
            species_status = status_result[0].get("species_verification_status", "pending")
            locality_status = status_result[0].get("locality_verification_status", "pending")

            if species_status == "verified" and locality_status == "verified" and record_status == "verified":
                overall_status = "completed"
            else:
                overall_status = "pending"
        else:
            overall_status = "pending"

        # 更新记录的warnings和verification status
        warnings_json = json.dumps(new_warnings) if new_warnings else None

        update_query = """
        UPDATE primary_temp
        SET
            "verification_warnings" = $1,
            "record_verification_status" = $2,
            "overall_verification_status" = $3,
            "TimeStampModified" = NOW()
        WHERE "PrimaryID" = $4
        """

        await execute_mutation(
            update_query,
            warnings_json,
            record_status,
            overall_status,
            record_id
        )

        print(f"Re-validated record {record_id}: {len(new_warnings)} warnings, status={record_status}")

    except Exception as e:
        print(f"Error revalidating record {record_id}: {str(e)}")
        # Don't raise exception, just log it - revalidation failure shouldn't block update