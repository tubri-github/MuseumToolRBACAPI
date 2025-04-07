from fastapi import APIRouter, Query, Depends, HTTPException, status
from typing import Optional, List, Dict, Any
from pydantic import BaseModel

from app.db.database import execute_query, execute_mutation
from app.services.es_sync import handle_data_change

router = APIRouter()


class OSTModel(BaseModel):
    ostcatalog: str
    tucatalog: Optional[str] = None
    othercatalog: Optional[str] = None
    count: Optional[int] = None
    type: Optional[str] = None
    inventory: Optional[str] = None
    scientificname: Optional[str] = None
    locality: Optional[str] = None
    datecollected: Optional[str] = None
    updateCollectdate: Optional[str] = None
    fieldnumber: Optional[str] = None
    collector: Optional[str] = None
    remarks: Optional[str] = None
    tl: Optional[float] = None
    sl: Optional[float] = None
    fl: Optional[float] = None
    gm: Optional[float] = None
    scientificnameremarks: Optional[str] = None
    recheckedrequried: Optional[bool] = None
    reviewer: Optional[str] = None
    recheckcomment: Optional[str] = None
    taxonid: Optional[int] = None
    identifyby: Optional[str] = None
    recordeddate: Optional[str] = None


class ResponseModel(BaseModel):
    code: int
    data: Dict[str, Any]


@router.get("/ost", response_model=ResponseModel)
async def get_ost(ostcatalog: str):
    """
    Get OST record by ostcatalog.
    Mirrors the original getOST function.
    """
    query = """
    SELECT * FROM ostelogy ut 
    WHERE ut."ostcatalog" = $1
    """

    records = await execute_query(query, ostcatalog)

    return {
        "code": 20000,
        "data": {
            "items": records,
            "total": len(records)
        }
    }


@router.get("/ostlist", response_model=ResponseModel)
async def get_ost_random_list(
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
        enddate: Optional[str] = None
):
    """
    Get OST records with filtering.
    Mirrors the original getOSTRandomList function.
    """
    # Start building the SQL query
    sql = 'SELECT * FROM ostelogy WHERE (1=1)'

    # Add filter conditions
    params = []
    param_index = 1

    # Process IDs if provided
    if ids and ids != "":
        id_list = [int(id_str) for id_str in ids.split(',') if id_str]
        if id_list:
            sql += f" AND (\"ostcatalog\" = ANY(${param_index}::text[]))"
            params.append(id_list)
            param_index += 1

    # Add other filters
    if family:
        sql += f" AND (TRIM(\"family\") = '${family}')"

    if genus:
        sql += f" AND (TRIM(\"genus\") = '${genus}')"

    if species:
        sql += f" AND (TRIM(\"species\") = '${species}')"

    if collector:
        sql += f" AND (\"collector\" = '${collector}')"

    if jarSize:
        sql += f" AND (\"JarSize\" = '${jarSize}')"

    if maxNum is not None:
        sql += f" AND (\"count\" <= {maxNum})"

    if minNum is not None:
        sql += f" AND (\"count\" >= {minNum})"

    if startdate:
        sql += f" AND (\"datecollected\" >= '${startdate}')"

    if enddate:
        sql += f" AND (\"datecollected\" <= '${enddate}')"

    if reviewrequired:
        sql += f" AND (\"recheckedrequried\" = '${reviewrequired}')"

    # Execute the query
    records = await execute_query(sql, *params)

    return {
        "code": 20000,
        "data": {
            "items": records,
            "total": len(records)
        }
    }


@router.get("/reportost", response_model=ResponseModel)
async def report_ost(
        ostcatalog: str,
        user: str
):
    """
    Report an OST record.
    Mirrors the original reportOST function.
    """
    query = """
    INSERT INTO ost_report ("ostcatalog", "user") 
    VALUES ($1, $2)
    """

    await execute_mutation(query, ostcatalog, user)

    return {
        "code": 20000,
        "data": {
            "items": [],
            "total": 0
        }
    }


@router.post("/updateost", response_model=ResponseModel)
async def update_ost(data: OSTModel):
    """
    Update an OST record.
    Mirrors the original updateOST function.
    """
    if not data.ostcatalog:
        raise HTTPException(status_code=400, detail="ostcatalog is required")

    query = """
    UPDATE "ostelogy" 
    SET "tucatalog" = $2,
        "othercatalog" = $3,
        "count" = $4,
        "type" = $5,
        "inventory" = $6,
        "scientificname" = $7,
        "locality" = $8,
        "datecollected" = $9,
        "updatecollectdate" = $10,
        "fieldnumber" = $11,
        "collector" = $12,
        "remarks" = $13,
        "tl" = $14,
        "sl" = $15,
        "fl" = $16,
        "gm" = $17,
        "scientificnameremarks" = $18,
        "recheckedrequried" = $19,
        "reviewer" = $20,
        "recheckcomment" = $21,
        "taxonid" = $22,
        "identifyby" = $23,
        "recordeddate" = $24,
        "timestampmodified" = now()
    WHERE "ostcatalog" = $1
    """

    try:
        await execute_mutation(
            query,
            data.ostcatalog,
            data.tucatalog,
            data.othercatalog,
            data.count,
            data.type,
            data.inventory,
            data.scientificname,
            data.locality,
            data.datecollected,
            data.updateCollectdate,
            data.fieldnumber,
            data.collector,
            data.remarks,
            data.tl,
            data.sl,
            data.fl,
            data.gm,
            data.scientificnameremarks,
            data.recheckedrequried,
            data.reviewer,
            data.recheckcomment,
            data.taxonid,
            data.identifyby,
            data.recordeddate
        )

        # Sync the updated data to Elasticsearch
        await handle_data_change("ostelogy", data.ostcatalog, "UPDATE")

        return {
            "code": 20000,
            "data": {
                "items": [],
                "total": 0
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))