from datetime import datetime, date

from fastapi import APIRouter, Query, Depends, HTTPException, status
from typing import Dict, Any, List, Optional
from pydantic import BaseModel
from starlette.responses import StreamingResponse

from app.db.database import execute_query, execute_mutation, execute_proc, execute_paginated_query_with_count
from app.services.es_sync import handle_data_change
from io import BytesIO
import pandas as pd
from sqlalchemy import text


router = APIRouter()


class LotModel(BaseModel):
    primaryID: Optional[int] = None
    scientificName: Optional[str] = None
    prevNumber: Optional[str] = None
    dateCataloged: Optional[datetime] = None
    jarSize: Optional[str] = None
    storage: Optional[str] = None
    typeStatus: Optional[str] = None
    inventory: Optional[str] = None
    remarks: Optional[str] = None
    localityId: Optional[int] = None
    catalogerId: Optional[int] = None
    totalNumber: Optional[int] = None
    preparation: Optional[List[Dict[str, Any]]] = None
    zDetermination: Optional[List[Dict[str, Any]]] = None
    oldDeterminationDetails: Optional[List[Dict[str, Any]]] = None
    oldPreparationDetails: Optional[List[Dict[str, Any]]] = None


class DeaccessionModel(BaseModel):
    primaryID: int
    totalNumber: int
    dateDeaccesion: datetime
    numberDeaccessioned: int
    remarks: Optional[str] = None


class ResponseModel(BaseModel):
    code: int
    data: Dict[str, Any]

class PaginationParams:
    def __init__(
        self,
        page: int = Query(1, ge=1, description="页码，从1开始"),
        page_size: int = Query(10, ge=1, le=100, description="每页记录数")
    ):
        self.page = page
        self.page_size = page_size


router = APIRouter()

@router.get("/validate-data")
async def validate_data():

    # 取出所有数据
    query = """
         SELECT p."PrimaryID", p."CatalogNumber", p."ScientificName", p."PrevNumber", 
               p."DateCataloged", p."JarSize", p."Storage", p."TypeStatus", p."Inventory", 
               p."Remarks", p."Locality1ID", l."Lon", l."Lat",  p."CatalogerID", p."TotalNumber", p."TimeStampModified",
               d."TaxonID", tt."FullScientificName", tt."Genus", tt."Species", f."FamilyID", f."FamilyName",
               l."FieldNo", l."LocalityString", l."Country", l."State", l."County", l."Drainage", l."WaterBody", l."StartDate"
        FROM "Primary" p
        LEFT JOIN "Determination" d ON p."PrimaryID" = d."PrimaryID" AND d."IsCurrent" = true
        LEFT JOIN "TaxonomicTable" tt ON d."TaxonID" = tt."TaxonID"
        LEFT JOIN "Family" f ON tt."FamilyID" = f."FamilyID"
        LEFT JOIN locality1 l ON p."Locality1ID" = l."Locality1ID"
    """
    records = await execute_query(query)
    df = pd.DataFrame(records)
    issues = []

    def add_issues(condition, issue_type):
        matches = df[condition].copy()
        matches["IssueType"] = issue_type
        issues.append(matches)

    # 校验 1: 缺失 ScientificName
    add_issues(df["FullScientificName"].isnull() | (df["FullScientificName"].str.strip() == ""), "MissingScientificName")

    # 校验 2: 日期缺失
    add_issues(df["StartDate"].isnull(), "MissingDateCollected")

    # 校验 4: LocalityID 缺失
    add_issues(df["Locality1ID"].isnull(), "MissingLocality")

    add_issues(
        df["Lat"].isnull() | df["Lon"].isnull(), "MissingCoordinates"
    )
    add_issues(
        (df["Lat"].notnull() & ~df["Lat"].between(-90, 90)) |
        (df["Lon"].notnull() & ~df["Lon"].between(-180, 180)),
        "InvalidCoordinates"
    )

    # 合并所有问题数据
    if issues:
        issues_df = pd.concat(issues, ignore_index=True)
    else:
        issues_df = pd.DataFrame(columns=list(df.columns) + ["IssueType"])

    # 汇总表
    summary_df = issues_df.groupby("IssueType").size().reset_index(name="Count")

    # 输出 Excel
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        summary_df.to_excel(writer, sheet_name="Summary", index=False)
        issues_df.to_excel(writer, sheet_name="Issues", index=False)

    output.seek(0)
    return StreamingResponse(output, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                             headers={"Content-Disposition": "attachment; filename=data_validation_report.xlsx"})

@router.get("/lot/{ids}/{limit}", response_model=ResponseModel)
async def get_lots(ids: str, pagination: PaginationParams = Depends()):
    """
    Get lots by IDs.
    Mirrors the original getLots function.
    """
    id_list = [int(id_str) for id_str in ids.split(',') if id_str]
    query = """
        SELECT * FROM (
            (((SELECT p."PrimaryID" as "MainPrimaryID", p."Remarks" as "PrimaryRemarks", p.* 
              FROM "Primary" p 
              WHERE p."CatalogNumber" = ANY($1::int[])) a 
             LEFT JOIN "Determination" d2 ON d2."PrimaryID" = a."PrimaryID") join1 
            LEFT JOIN locality1 l ON l."Locality1ID" = join1."Locality1ID") join2 
           LEFT JOIN "TaxonomicTable" tt ON tt."TaxonID" = join2."TaxonID") join3 
        LEFT JOIN "Family" f ON f."FamilyID" = join3."FamilyID"
        """
    count_query = """
            SELECT COUNT(*) FROM "Primary" 
            WHERE "CatalogNumber" = ANY($1::int[])
            """

    return await execute_paginated_query_with_count(
        main_query=query,
        count_query=count_query,
        params=[id_list],
        page=pagination.page,
        page_size=pagination.page_size
    )


@router.get("/lotString/{catid}", response_model=ResponseModel)
async def get_lot_string(catid: int):
    """
    Get lot string by catalog ID.
    Mirrors the original getLotString function.
    """
    query = """
    SELECT tt2."PrimaryID" as "LotID", 
           CONCAT(tt2."CatalogNumber", '(', tt2."TotalNumber", ') Pri = ', "TaxonomicTable"."FullScientificName", ':', tt2."JarSize") as "LotString",
           tt2."TotalNumber" 
    FROM (
        SELECT "Determination"."TaxonID", tt1.* 
        FROM (
            SELECT * FROM "Primary" WHERE "Primary"."CatalogNumber" = $1
        ) as tt1 
        LEFT JOIN "Determination" ON tt1."PrimaryID" = "Determination"."PrimaryID" AND "Determination"."IsCurrent" = true
    ) as tt2 
    LEFT JOIN "TaxonomicTable" ON "TaxonomicTable"."TaxonID" = tt2."TaxonID"
    """

    records = await execute_query(query, catid)

    return {
        "code": 20000,
        "data": {
            "items": records,
            "total": len(records)
        }
    }


@router.get("/jarsizes", response_model=ResponseModel)
async def get_jar_sizes():
    """
    Get jar sizes.
    Mirrors the original getJarSizes function.
    """
    query = """
    SELECT * FROM "JarSizes"
    """

    records = await execute_query(query)

    return {
        "code": 20000,
        "data": {
            "items": records,
            "total": len(records)
        }
    }


@router.get("/preparation", response_model=ResponseModel)
async def get_preparation():
    """
    Get preparation types.
    Mirrors the original getPreparation function.
    """
    query = """
    SELECT * FROM "PreparationTypes" pt
    """

    records = await execute_query(query)

    return {
        "code": 20000,
        "data": {
            "items": records,
            "total": len(records)
        }
    }


@router.post("/deaccession", response_model=ResponseModel)
async def deaccession(data: DeaccessionModel):
    """
    Deaccession a lot.
    Mirrors the original deaccestion function.
    """
    if data.totalNumber == 0 or data.totalNumber < data.numberDeaccessioned:
        raise HTTPException(
            status_code=400,
            detail="Total Number is 0 or total number is smaller than deaccessioned number"
        )

    query1 = """
    INSERT INTO "Deaccession" ("DeaccessionDate", "NumberDeaccessioned", "Remarks", "PrimaryID")
    VALUES ($1, $2, $3, $4)
    """

    try:
        await execute_mutation(
            query1,
            data.dateDeaccesion,
            data.numberDeaccessioned,
            data.remarks,
            data.primaryID
        )

        # Update total number
        updated_total = data.totalNumber - data.numberDeaccessioned

        query2 = """
        UPDATE "Primary" SET "TotalNumber" = $1 WHERE "Primary"."PrimaryID" = $2
        """

        await execute_mutation(query2, updated_total, data.primaryID)

        # Sync the updated data to Elasticsearch
        await handle_data_change("Primary", data.primaryID, "UPDATE")

        return {
            "code": 20000,
            "data": {
                "items": [],
                "total": 0
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/deaccession/{priid}", response_model=ResponseModel)
async def get_deaccession(priid: int):
    """
    Get deaccession information by primary ID.
    Mirrors the original getDeaccestion function.
    """
    query = """
    SELECT d."DeaccessionDate" as "dateDeaccesion",
           d."NumberDeaccessioned" as "numberDeaccessioned",
           d."Remarks" as "remarks"
    FROM "Deaccession" d
    WHERE "PrimaryID" = $1
    """

    records = await execute_query(query, priid)

    return {
        "code": 20000,
        "data": {
            "items": records,
            "total": len(records)
        }
    }


@router.get("/lots", response_model=ResponseModel)
async def get_lots_advanced(
        ids: Optional[str] = None,
        locality: Optional[int] = None,
        taxonId: Optional[int] = None,
        familyID: Optional[int] = None,
        fieldNo: Optional[str] = None,
        jarSize: Optional[str] = None,
        Storage: Optional[str] = None,
        Inventory: Optional[str] = None,
        maxNumber: Optional[int] = None,
        minNumber: Optional[int] = None,
        startDate: Optional[date] = None,
        endDate: Optional[date] = None,
        minLotsNumber: Optional[int] = None,
        maxLotsNumber: Optional[int] = None,
        pagination: PaginationParams = Depends(),
):

    """
    Get lots with advanced filtering.
    Mirrors the original getLotsAdvanced function.
    """
    # Start building the SQL query
    sql = """
    SELECT * FROM (
        SELECT * FROM (
            (SELECT d2."TaxonID" as "DTaxonID", d2.*, join1.* FROM (
                SELECT * FROM (
                    SELECT p."PrimaryID" as "MainPrimaryID", p."Remarks" as "PrimaryRemarks", p.* 
                    FROM "Primary" p WHERE (1=1)
    """

    # Add filter conditions
    params = []
    param_index = 1

    # Process IDs if provided
    if ids and ids != "":
        id_list = [int(id_str) for id_str in ids.split(',') if id_str]
        if id_list:
            sql += f" AND (p.\"CatalogNumber\" = ANY(${param_index}::int[]))"
            params.append(id_list)
            param_index += 1

    # Add other filters to primary table
    if jarSize:
        sql += f" AND (p.\"JarSize\" = '{jarSize}')"

    if Storage:
        sql += f" AND (p.\"Storage\" = '{Storage}')"

    if Inventory:
        sql += f" AND (p.\"Inventory\" = '{Inventory}')"

    if maxNumber:
        sql += f" AND (p.\"TotalNumber\" <= {maxNumber})"

    if minNumber:
        sql += f" AND (p.\"TotalNumber\" >= {minNumber})"

    if locality:
        sql += f" AND (p.\"Locality1ID\" = {locality})"

    if startDate:
        sql += f" AND (p.\"DateCataloged\" >= '{startDate}')"

    if endDate:
        sql += f" AND (p.\"DateCataloged\" <= '{endDate}')"

    sql += """
                ) as a INNER JOIN locality1 l ON l."Locality1ID" = a."Locality1ID"
    """

    # Add filter for field number
    if fieldNo:
        sql += f" WHERE l.\"FieldNo\" ~* '{fieldNo}'"

    sql += """
            ) join1 LEFT JOIN "Determination" d2 
            ON d2."PrimaryID" = join1."PrimaryID" AND d2."IsCurrent" = true
            ) join2 LEFT JOIN "TaxonomicTable" tt ON tt."TaxonID" = join2."TaxonID"
    """

    # Add filter for lot numbers
    if minLotsNumber:
        sql += f" AND (join2.\"lotsnumber\" >= '{minLotsNumber}')"

    if maxLotsNumber:
        sql += f" AND (join2.\"lotsnumber\" <= '{maxLotsNumber}')"

    sql += """
        ) join3 LEFT JOIN "Family" f ON f."FamilyID" = join3."FamilyID" AND f."FamilyID" != 0
    """

    # Add filter for taxonomic ID and family ID
    if taxonId:
        sql += f" AND (\"DTaxonID\" = '{taxonId}')"

    if familyID:
        sql += f" AND f.\"FamilyID\" = '{familyID}'"

    sql += """
    ) join4 LEFT JOIN "Preparation" pp ON pp."PrimaryID" = join4."MainPrimaryID"
    """

    count_sql = f"SELECT COUNT(*) FROM ({sql}) AS count_query"

    # Add ORDER BY
    sql += ' ORDER BY "MainPrimaryID" DESC'



    return await execute_paginated_query_with_count(
        main_query=sql,
        count_query=count_sql,
        params=params,
        page=pagination.page,
        page_size=pagination.page_size
    )


@router.get("/determinations/{primaryID}", response_model=ResponseModel)
async def get_determinations_by_primary_id(primaryID: int):
    """
    Get determinations by primary ID.
    Mirrors the original getDeterminationsByPrimaryID function.
    """
    query = """
    SELECT * FROM (
        (SELECT dd."Remarks" as "DeterminationRemarks", dd.* 
         FROM "Determination" dd 
         WHERE dd."PrimaryID" = $1) dd 
        LEFT JOIN "TaxonomicTable" tt ON tt."TaxonID" = dd."TaxonID"
    ) join3
    """

    records = await execute_query(query, primaryID)

    return {
        "code": 20000,
        "data": {
            "items": records,
            "total": len(records)
        }
    }


@router.get("/preparations/{primaryID}", response_model=ResponseModel)
async def get_preparation_by_primary_id(primaryID: int):
    """
    Get preparations by primary ID.
    Mirrors the original getPreparationByPrimaryID function.
    """
    query = """
    SELECT * FROM "Preparation" dd 
    WHERE dd."PrimaryID" = $1
    """

    records = await execute_query(query, primaryID)

    return {
        "code": 20000,
        "data": {
            "items": records,
            "total": len(records)
        }
    }


@router.post("/lot", response_model=ResponseModel)
async def new_lot(data: LotModel):
    """
    Create a new lot.
    Mirrors the original newLot function.
    """
    try:
        # Convert determination data to JSON
        determinations = []
        for det in data.zDetermination:
            determinations.append({
                "isCurrent": det.get("isCurrent", False),
                "taxonId": det.get("taxonId", None),
                "determinerID": det.get("determination", {}).get("determinerID", None),
                "determinerName": det.get("determination", {}).get("determinerName", None),
                "date": det.get("date", None),
                "remarks": det.get("remarks", None)
            })

        # Convert preparation data to JSON
        preparations = []
        for prep in data.preparation:
            preparations.append({
                "preparationType": prep.get("preparationType", None),
                "count": prep.get("count", None)
            })

        # Call the stored procedure
        result = await execute_proc(
            "add_lot_procedure",
            data.scientificName,
            data.prevNumber,
            data.dateCataloged if data.dateCataloged else None,
            data.jarSize,
            data.storage,
            data.typeStatus,
            data.inventory,
            data.remarks,
            data.localityId if data.localityId else None,
            data.catalogerId if data.catalogerId else None,
            data.totalNumber if data.totalNumber else None,
            determinations,
            preparations
        )

        return {
            "code": 20000,
            "data": {
                "items": {"CatalogNumber": result},
                "total": 1
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/lot", response_model=ResponseModel)
@router.post("/updatelot", response_model=ResponseModel)
async def update_lot(data: LotModel):
    """
    Update a lot.
    Mirrors the original updateLot function.
    """
    try:
        # Format determinations
        determinations = []
        for det in data.zDetermination:
            determinations.append({
                "isCurrent": det.get("isCurrent", False),
                "determinationID": det.get("determinationID", None),
                "determinerID": det.get("determination", {}).get("determinerID", None),
                "determinerName": det.get("determination", {}).get("determinerName", None),
                "taxonId": det.get("taxonId", None),
                "date": det.get("date", None),
                "remarks": det.get("remarks", None)
            })

        # Format preparations
        preparations = []
        for prep in data.preparation:
            preparations.append({
                "preparationID": prep.get("preparationID", None),
                "preparationType": prep.get("preparationType", None),
                "count": prep.get("count", None)
            })

        # Call the stored procedure
        await execute_proc(
            "update_lot_procedure",
            data.primaryID,
            data.scientificName,
            data.prevNumber,
            data.dateCataloged,
            data.jarSize,
            data.storage,
            data.typeStatus,
            data.inventory,
            data.remarks,
            data.localityId,
            data.catalogerId,
            data.totalNumber,
            determinations,
            preparations
        )

        # Sync the updated data to Elasticsearch
        # await handle_data_change("Primary", data.primaryID, "UPDATE")

        return {
            "code": 20000,
            "data": {
                "items": "update success",
                "total": 1
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/lotcount/{year}", response_model=ResponseModel)
async def get_lots_numbers_by_year(year: str):
    """
    Get lot count by year.
    Mirrors the original getLotsNumbersByYear function.
    """
    date_value = datetime.strptime(f"{year}-01-01", "%Y-%m-%d").date()

    query = """
    SELECT * FROM "Primary" p 
    WHERE "DateCataloged" >= $1
    """

    records = await execute_query(query, date_value)

    return {
        "code": 20000,
        "data": {
            "total": len(records)
        }
    }