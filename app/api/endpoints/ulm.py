from fastapi import APIRouter, Query, Depends, HTTPException, status, Body
from typing import Optional, List, Dict, Any
from pydantic import BaseModel

from app.db.database import execute_query, execute_mutation, execute_single_query
from app.services.es_sync import handle_data_change

router = APIRouter()


class ULMModel(BaseModel):
    PrimaryID: Optional[int] = None
    prevNumber: Optional[str] = None
    updateCollectdate: Optional[str] = None
    jarSize: Optional[str] = None
    totalNumber: Optional[int] = None
    remarks: Optional[str] = None
    typeStatus: Optional[str] = None
    family: Optional[str] = None
    genus: Optional[str] = None
    species: Optional[str] = None
    country: Optional[str] = None
    county: Optional[str] = None
    state: Optional[str] = None
    waterbody: Optional[str] = None
    drainage: Optional[str] = None
    locality: Optional[str] = None
    collector: Optional[str] = None
    reviewrequired: Optional[bool] = None
    recheckcomment: Optional[str] = None
    editor: Optional[str] = None
    reviewer: Optional[str] = None


class ReportModel(BaseModel):
    prevNumber: str
    dataset: str
    jarSize: str
    user: str


class ResponseModel(BaseModel):
    code: int
    data: Dict[str, Any]


@router.get("/ulm", response_model=ResponseModel)
async def get_ulm(
        prevNumber: str,
        dataset: str
):
    """
    Get ULM record by prev number and dataset.
    Mirrors the original getULM function.
    """
    query = """
    SELECT * FROM ulm_temp ut 
    WHERE ut."dataset" = $1 AND ut."PrevNumber" = $2
    """

    records = await execute_query(query, dataset, prevNumber)

    return {
        "code": 20000,
        "data": {
            "items": records,
            "total": len(records)
        }
    }


@router.get("/ulmrandom", response_model=ResponseModel)
async def get_ulm_random():
    """
    Get a random ULM record that hasn't been checked.
    Mirrors the original getULMRandom function.
    """
    query = """
    SELECT * FROM ulm_temp ut 
    WHERE ut.checked = false 
    ORDER BY random() 
    LIMIT 1
    """

    records = await execute_query(query)

    return {
        "code": 20000,
        "data": {
            "items": records,
            "total": len(records)
        }
    }


@router.get("/ulmlotlist", response_model=ResponseModel)
async def get_ulm_list(
        ids: Optional[str] = None,
        family: Optional[str] = None,
        genus: Optional[str] = None,
        species: Optional[str] = None,
        jarSize: Optional[str] = None,
        minNum: Optional[int] = None,
        maxNum: Optional[int] = None,
        typeStatus: Optional[str] = None,
        remarks: Optional[str] = None,
        country: Optional[str] = None,
        waterbody: Optional[str] = None,
        drainage: Optional[str] = None,
        locality: Optional[str] = None,
        collector: Optional[str] = None,
        reviewrequired: Optional[str] = None,
        startdate: Optional[str] = None,
        enddate: Optional[str] = None,
        dataset: Optional[str] = None,
        page: int = Query(1, ge=1),
        limit: int = Query(10, ge=1, le=100)
):
    """
    Get ULM records with filtering.
    Mirrors the original getULMRandomList function.
    """
    # Start building the SQL query
    sql_header = 'SELECT * FROM ulm_temp lv '
    total_sql = 'SELECT COUNT(*) FROM ulm_temp lv '

    # Process query parameters
    where_clauses = []
    params = []
    param_index = 1

    # Process IDs if provided
    if ids and ids != "":
        id_list = [id_str for id_str in ids.split(',') if id_str]
        if id_list:
            where_clauses.append(f'"PrevNumber" = ANY(${param_index}::text[])')
            params.append(id_list)
            param_index += 1

    # Add other filters
    if family:
        where_clauses.append(f'TRIM("family") = ${param_index}')
        params.append(family)
        param_index += 1

    if genus:
        where_clauses.append(f'TRIM("genus") = ${param_index}')
        params.append(genus)
        param_index += 1

    if species:
        where_clauses.append(f'TRIM("species") = ${param_index}')
        params.append(species)
        param_index += 1

    if collector:
        where_clauses.append(f'"collectorname" = ${param_index}')
        params.append(collector)
        param_index += 1

    if jarSize:
        where_clauses.append(f'"JarSize" = ${param_index}')
        params.append(jarSize)
        param_index += 1

    if maxNum is not None:
        where_clauses.append(f'"TotalNumber" <= ${param_index}')
        params.append(maxNum)
        param_index += 1

    if minNum is not None:
        where_clauses.append(f'"TotalNumber" >= ${param_index}')
        params.append(minNum)
        param_index += 1

    if startdate:
        where_clauses.append(f'"collectordate" >= ${param_index}')
        params.append(startdate)
        param_index += 1

    if enddate:
        where_clauses.append(f'"collectordate" <= ${param_index}')
        params.append(enddate)
        param_index += 1

    if reviewrequired:
        where_clauses.append(f'"recheckrequired" = ${param_index}')
        params.append(reviewrequired)
        param_index += 1

    if dataset:
        where_clauses.append(f'"dataset" = ${param_index}')
        params.append(dataset)
        param_index += 1

    # Build the WHERE clause
    where_clause = " WHERE " + " AND ".join(where_clauses) if where_clauses else " WHERE 1=1"

    # Build the complete queries
    count_query = total_sql + where_clause
    offset = (page - 1) * limit
    pagesql = where_clause + f' ORDER BY "PrimaryID" OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY'
    main_query = sql_header + pagesql

    # Execute count query
    count_result = await execute_single_query(count_query, *params)
    total = count_result["count"] if count_result else 0

    # Execute main query
    records = await execute_query(main_query, *params)

    return {
        "code": 20000,
        "data": {
            "items": records,
            "total": total
        }
    }


@router.get("/reportulm", response_model=ResponseModel)
async def report_ulm(
        prevNumber: str,
        dataset: str,
        jarSize: str,
        user: str
):
    """
    Report a ULM record.
    Mirrors the original reportULM function.
    """
    query = """
    INSERT INTO ulm_report ("prevnumber", "dataset", "jarsize", "user") 
    VALUES ($1, $2, $3, $4)
    """

    await execute_mutation(query, prevNumber, dataset, jarSize, user)

    return {
        "code": 20000,
        "data": {
            "items": [],
            "total": 0
        }
    }


@router.post("/updateulmrandom", response_model=ResponseModel)
async def update_ulm_lot(data: ULMModel):
    """
    Update a ULM record.
    Mirrors the original updateULMLot function.
    """
    if not data.PrimaryID:
        raise HTTPException(status_code=400, detail="PrimaryID is required")

    query = """
    UPDATE "ulm_temp" 
    SET "updatecollectordate" = $2,
        "JarSize" = $3,
        "TotalNumber" = $4,
        "Remarks" = $5,
        "TypeStatus" = $6,
        "Family" = $7,
        "genus" = $8,
        "species" = $9,
        "country" = $10,
        "state" = $12,
        "county" = $11,
        "drainage" = $14,
        "waterbody" = $13,
        "Location" = $15,
        "collectorname" = $16,
        "recheckrequired" = $17,
        "recheckcomment" = $18,
        "editor" = $19,
        "reviewer" = $20,
        "TimeStampModified" = now(),
        "checked" = true 
    WHERE "PrimaryID" = $1
    """

    try:
        await execute_mutation(
            query,
            data.PrimaryID,
            data.updateCollectdate,
            data.jarSize,
            data.totalNumber,
            data.remarks,
            data.typeStatus,
            data.family,
            data.genus,
            data.species,
            data.country,
            data.county,
            data.state,
            data.waterbody,
            data.drainage,
            data.locality,
            data.collector,
            data.reviewrequired,
            data.recheckcomment,
            data.editor,
            data.reviewer
        )

        # Sync the updated data to Elasticsearch
        await handle_data_change("ulm_temp", data.PrimaryID, "UPDATE")

        return {
            "code": 20000,
            "data": {
                "items": [],
                "total": 0
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ulmstatsu", response_model=ResponseModel)
async def get_ulm_stats_by_user():
    """
    Get ULM statistics by user.
    Mirrors the original getULMstatisByUser function.
    """
    query = """
    SELECT to_char(uv.week, 'YYYY-MM-dd') as week, uv.editor, uv.cnt 
    FROM ulm_statis_view uv
    """

    records = await execute_query(query)

    return {
        "code": 20000,
        "data": {
            "items": records,
            "total": len(records)
        }
    }


@router.get("/ulmstatsreview", response_model=ResponseModel)
async def get_ulm_stats_by_review():
    """
    Get ULM statistics by review status.
    Mirrors the original getULMstatisByReview function.
    """
    query = """
    SELECT * FROM ulm_status_statis_view uv
    """

    records = await execute_query(query)

    return {
        "code": 20000,
        "data": {
            "items": records,
            "total": len(records)
        }
    }


@router.get("/ulmreportdata", response_model=ResponseModel)
async def get_ulm_report_excel():
    """
    Generate Excel report for ULM data.
    This is a simplified version of the original getULMReportExcel function
    which used cursors. For full implementation, additional Excel generation
    code would be needed.
    """
    query = """
    SELECT * FROM ulm_temp LIMIT 100
    """

    records = await execute_query(query)

    # In a complete implementation, you would generate an Excel file here
    # and return a URL to download it

    from datetime import datetime
    timestamp = datetime.now().timestamp()
    filename = f"ULM-List-{timestamp}.xlsx"

    return {
        "code": 20000,
        "data": {
            "filename": filename,
            "url": f"/ulm-report/{filename}"
        }
    }


@router.get("/ulmnotfoundreportdata", response_model=ResponseModel)
async def get_ulm_not_found_report_excel():
    """
    Generate Excel report for not found ULM data.
    This is a simplified version of the original getULMNotFoundReportExcel function.
    """
    query = """
    SELECT * FROM ulm_report LIMIT 100
    """

    records = await execute_query(query)

    # In a complete implementation, you would generate an Excel file here
    # and return a URL to download it

    from datetime import datetime
    timestamp = datetime.now().timestamp()
    filename = f"ULM-NotFound-Report-{timestamp}.xlsx"

    return {
        "code": 20000,
        "data": {
            "filename": filename,
            "url": f"/ulm-report/{filename}"
        }
    }