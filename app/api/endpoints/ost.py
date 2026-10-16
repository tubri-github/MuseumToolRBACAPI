from fastapi import APIRouter, Query, Depends, HTTPException, status
from typing import Optional, List, Dict, Any
from pydantic import BaseModel
from datetime import date

from app.db.database import execute_query, execute_mutation

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


# datecollected 是 varchar，混有 MM/DD/YYYY 和 DD/MM/YYYY；第一段 > 12 视为 DD/MM。
# 其他格式一律 NULL（不参与日期过滤）。
_OST_DATE_EXPR = """CASE
    WHEN o.datecollected ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' THEN
        CASE
            WHEN SPLIT_PART(o.datecollected, '/', 1)::int > 12
            THEN TO_DATE(o.datecollected, 'DD/MM/YYYY')
            ELSE TO_DATE(o.datecollected, 'MM/DD/YYYY')
        END
    ELSE NULL END"""


# ostelogy 没有 family 列，学名是自由文本、几乎没挂 taxonid。
# Family (inferred)：有 taxonid 就用该 taxon 的科；否则按学名首词（属名）去 TaxonomicTable 查科，
# 一个属对到多个科时用 " / " 拼起来。拼错的属名查不到（约 8%）。
_OST_FROM = """
FROM ostelogy o
LEFT JOIN "TaxonomicTable" t ON
    CASE WHEN o.taxonid ~ '^[0-9]+$' THEN o.taxonid::integer ELSE NULL END = t."TaxonID"
LEFT JOIN "Family" tf ON tf."FamilyID" = t."FamilyID"
LEFT JOIN (
    SELECT UPPER(SPLIT_PART(TRIM(gt."FullScientificName"), ' ', 1)) AS genus,
           STRING_AGG(DISTINCT gfam."FamilyName", ' / ') AS families
    FROM "TaxonomicTable" gt
    JOIN "Family" gfam ON gfam."FamilyID" = gt."FamilyID"
    GROUP BY 1
) gf ON gf.genus = UPPER(SPLIT_PART(TRIM(o.scientificname), ' ', 1))
"""
_OST_FAMILY_EXPR = 'COALESCE(tf."FamilyName", gf.families)'


def _blank_to_none(v: Optional[str]) -> Optional[str]:
    return None if v is None or v == "" else v


def _parse_date(v: Optional[str], name: str) -> Optional[date]:
    v = _blank_to_none(v)
    if v is None:
        return None
    try:
        return date.fromisoformat(v[:10])
    except ValueError:
        raise HTTPException(status_code=400, detail=f"{name} must be YYYY-MM-DD")


def _range_int(v: Optional[str]) -> Optional[int]:
    # 旧版：空串和 "0" 都视为未填
    if v is None or v == "" or v == "0":
        return None
    return int(v)


@router.get("/ostsearch", response_model=ResponseModel)
async def search_ost(
        ostcatalogs: Optional[str] = None,
        type: Optional[str] = None,
        inventory: Optional[str] = None,
        scientificname: Optional[str] = None,
        locality: Optional[str] = None,
        fieldnumber: Optional[str] = None,
        collector: Optional[str] = None,
        startdate: Optional[str] = None,
        enddate: Optional[str] = None,
        recheckedrequried: Optional[str] = None,
        taxonid: Optional[str] = None,
        family: Optional[str] = None,
        remarks: Optional[str] = None,
        countMin: Optional[str] = None,
        countMax: Optional[str] = None,
        tlMin: Optional[str] = None,
        tlMax: Optional[str] = None,
        slMin: Optional[str] = None,
        slMax: Optional[str] = None,
        flMin: Optional[str] = None,
        flMax: Optional[str] = None,
        exportAll: Optional[str] = None,
        limit: Optional[str] = None,
        page: Optional[str] = None,
):
    """
    Osteology advanced search over the ostelogy table.
    Mirrors the original searchOST function (old pg:2774).
    """
    where = " WHERE 1=1"
    params: List[Any] = []

    def add(clause: str, *values):
        nonlocal where
        idx = len(params) + 1
        # 不用 str.format：日期表达式里的正则含 {1,2}/{4}
        for i in range(len(values)):
            clause = clause.replace("{" + str(i) + "}", f"${idx + i}")
        where += " AND " + clause
        params.extend(values)

    ostcatalogs = _blank_to_none(ostcatalogs)
    if ostcatalogs is not None:
        catalogs = [c.strip() for c in ostcatalogs.split(",") if c.strip()]
        if catalogs:
            add("o.ostcatalog = ANY({0}::text[])", catalogs)

    type = _blank_to_none(type)
    if type is not None:
        add("LOWER(o.type) = LOWER({0})", type)

    inventory = _blank_to_none(inventory)
    if inventory is not None:
        add("o.inventory = {0}", inventory)

    scientificname = _blank_to_none(scientificname)
    if scientificname is not None:
        clean = scientificname.replace("*", "").replace("?", "")
        add("(o.scientificname ILIKE {0} OR similarity(COALESCE(o.scientificname,''), {1}) > 0.4)",
            f"%{clean}%", clean)

    remarks = _blank_to_none(remarks)
    if remarks is not None:
        add("(o.remarks ILIKE {0} OR o.scientificnameremarks ILIKE {0})", f"%{remarks}%")

    count_expr = "CASE WHEN o.count ~ '^[0-9]+$' THEN o.count::integer ELSE NULL END"
    for expr, lo, hi in (
            (count_expr, countMin, countMax),
            ("o.tl", tlMin, tlMax),
            ("o.sl", slMin, slMax),
            ("o.fl", flMin, flMax),
    ):
        lo_v, hi_v = _range_int(lo), _range_int(hi)
        if lo_v is not None:
            add(expr + " >= {0}", lo_v)
        if hi_v is not None:
            add(expr + " <= {0}", hi_v)

    for col, val in (("o.locality", locality), ("o.fieldnumber", fieldnumber), ("o.collector", collector)):
        val = _blank_to_none(val)
        if val is not None:
            add(col + " ILIKE {0}", f"%{val}%")

    start_d = _parse_date(startdate, "startdate")
    if start_d is not None:
        add(_OST_DATE_EXPR + " >= {0}::date", start_d)
    end_d = _parse_date(enddate, "enddate")
    if end_d is not None:
        add(_OST_DATE_EXPR + " <= {0}::date", end_d)

    recheckedrequried = _blank_to_none(recheckedrequried)
    if recheckedrequried is not None:
        add("o.recheckedrequried = {0}", recheckedrequried == "true")

    family = _blank_to_none(family)
    if family is not None:
        add(_OST_FAMILY_EXPR + " ILIKE {0}", f"%{family.strip()}%")

    taxonid = _blank_to_none(taxonid)
    if taxonid is not None:
        add("o.taxonid = {0}", str(taxonid))

    count_rows = await execute_query(f"SELECT COUNT(*) AS total {_OST_FROM} {where}", *params)
    total = int(count_rows[0]["total"])

    data_sql = f"""
    SELECT o.*, t."FullScientificName" AS "TaxonName", {_OST_FAMILY_EXPR} AS "InferredFamily"
    {_OST_FROM}
    {where}
    ORDER BY o.ostcatalog DESC
    """
    data_params = list(params)
    if exportAll != "true":
        limit_v = int(limit) if limit else 100
        page_v = int(page) if page else 1
        n = len(params)
        data_sql += f" LIMIT ${n + 1} OFFSET ${n + 2}"
        data_params += [limit_v, (page_v - 1) * limit_v]

    records = await execute_query(data_sql, *data_params)

    return {
        "code": 20000,
        "data": {
            "items": records,
            "total": total
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


        return {
            "code": 20000,
            "data": {
                "items": [],
                "total": 0
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))