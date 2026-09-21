from datetime import datetime, date

import asyncpg
from fastapi import APIRouter, Query, Depends, HTTPException, status
from typing import Dict, Any, List, Optional
from pydantic import BaseModel
from starlette.responses import StreamingResponse

from app.db.database import execute_query, execute_mutation, execute_proc, execute_paginated_query_with_count, execute_transaction, get_db
from app.services.filter_engine import FilterSpec, FieldDef, build_where, build_global_search, parse_json_param
from app.services.synonym_service import SynonymService
from app.utils.request_params import parse_int_list, parse_int_or_none, parse_year_start

_synonym_service = SynonymService()

# TaxonomicTable 的「名」表达式：优先 Genus+Species（最准），空白时退回 FullScientificName
_TT_NAME = "COALESCE(NULLIF(TRIM(COALESCE(\"Genus\",'')||' '||COALESCE(\"Species\",'')),''), \"FullScientificName\")"


def _int_array_sql(ids):
    """把整数 id 列表渲染成安全的 SQL 字面量数组（ids 来自 DB，非用户字符串）。"""
    return "ARRAY[" + ",".join(str(int(x)) for x in ids) + "]::int[]"


async def _taxon_base_name(taxon_id: int):
    rows = await execute_query(
        'SELECT "FullScientificName", "Genus", "Species" FROM "TaxonomicTable" WHERE "TaxonID" = $1',
        taxon_id,
    )
    if not rows:
        return None
    r = rows[0]
    gs = " ".join(x for x in [(r.get("Genus") or "").strip(), (r.get("Species") or "").strip()] if x).strip()
    return gs or (r.get("FullScientificName") or "").strip() or None


async def _expand_taxon_tiers(taxon_id: int, threshold: float):
    """选中一个 TaxonID → 三层 TaxonID 集合：
    ① 该 taxon 本身；
    ② 「拼写变体」= 同属 + 种加词 trigram 相似（避免把同属不同种 / `sp.` 当相似）；
    ③ taxonomy_dev 同义词等价名映射回主库的 taxon。
    """
    rows = await execute_query(
        'SELECT "Genus", "Species", "FullScientificName" FROM "TaxonomicTable" WHERE "TaxonID" = $1',
        taxon_id,
    )
    tier1 = [taxon_id]
    tier2, tier3 = [], []
    if not rows:
        return {"all": tier1, "t1": tier1, "t2": tier2, "t3": tier3}
    r = rows[0]
    genus = (r.get("Genus") or "").strip()
    species = (r.get("Species") or "").strip()
    base = (genus + " " + species).strip() or (r.get("FullScientificName") or "").strip()

    # ② 同属 + 种加词相似（真正的拼写变体；sp./不同种自然被排除）
    if genus and species:
        sim_rows = await execute_query(
            'SELECT "TaxonID" FROM "TaxonomicTable" '
            'WHERE "TaxonID" <> $1 AND LOWER(TRIM("Genus")) = LOWER(TRIM($2)) '
            'AND "Species" IS NOT NULL AND TRIM("Species") <> \'\' '
            'AND similarity("Species", $3) >= $4',
            taxon_id, genus, species, threshold,
        )
        tier2 = [x["TaxonID"] for x in sim_rows]

    # ③ 同义词等价名 → 映射回主库 TaxonID
    if base:
        group = await _synonym_service.resolve_group(base)
        lowered = [n.strip().lower() for n in group.get("names", []) if n]
        if lowered:
            t3_rows = await execute_query(
                f'SELECT "TaxonID" FROM "TaxonomicTable" WHERE LOWER(TRIM({_TT_NAME})) = ANY($1::text[])',
                lowered,
            )
            exclude = set(tier1) | set(tier2)
            tier3 = [x["TaxonID"] for x in t3_rows if x["TaxonID"] not in exclude]

    all_ids = list(dict.fromkeys(tier1 + tier2 + tier3))
    return {"all": all_ids, "t1": tier1, "t2": tier2, "t3": tier3}


async def _expand_family_ids(family_id: int):
    """选中一个 FamilyID → 该科 + taxonomy_dev 里的同义科映射回主库的 FamilyID。"""
    fam_ids = [family_id]
    rows = await execute_query('SELECT "FamilyName" FROM "Family" WHERE "FamilyID" = $1', family_id)
    if rows and rows[0].get("FamilyName"):
        group = await _synonym_service.resolve_group(rows[0]["FamilyName"])
        lowered = [n.strip().lower() for n in group.get("names", []) if n]
        if lowered:
            fr = await execute_query(
                'SELECT "FamilyID" FROM "Family" WHERE LOWER(TRIM("FamilyName")) = ANY($1::text[])',
                lowered,
            )
            fam_ids = list(dict.fromkeys(fam_ids + [r["FamilyID"] for r in fr]))
    return fam_ids
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
    collection: Optional[str] = None  # 节点的 collection 类型。root: fluid|osteology|tissue；子节点: osteology|tissue
    parentId: Optional[int] = None    # 有值=建子节点(挂在该 PrimaryID 下)；无=建 root lot
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
        page: int = Query(1, ge=-1, description="页码，从1开始"),
        page_size: int = Query(10, ge=1, le=100, description="每页记录数")
    ):
        self.page = page
        self.page_size = page_size


router = APIRouter()


# ---------------------------------------------------------------------------
# Lots 搜索字段注册表（驱动通用过滤引擎 + filter-metadata）
# 注意：搜索查询不 JOIN Preparation（一对多会乘行）；制作记录走 /preparations/{primaryID}。
#       Determination 限定 IsCurrent=true，只取当前鉴定。
# ---------------------------------------------------------------------------
LOTS_SPEC = FilterSpec(
    base='"Primary" p',
    joins="""
        LEFT JOIN "Determination" d ON d."PrimaryID" = p."PrimaryID" AND d."IsCurrent" = true
        LEFT JOIN "TaxonomicTable" tt ON tt."TaxonID" = d."TaxonID"
        LEFT JOIN "Family" f ON f."FamilyID" = tt."FamilyID"
        LEFT JOIN locality1 l ON l."Locality1ID" = p."Locality1ID"
        LEFT JOIN verbatim_locality vl ON vl."verbatim_localityid" = p."verbatim_localityid"
        LEFT JOIN verbatim_taxonomic vtx ON vtx."verbatim_taxonid" = p."verbatim_taxonid"
    """,
    select="""
        p."PrimaryID", p."CatalogNumber", p."PrevNumber", p."DateCataloged",
        p."JarSize", p."Storage", p."TypeStatus", p."Inventory", p."TotalNumber",
        p."Remarks", p."Locality1ID", p."CatalogerID", p."TimeStampModified",
        p."parent_id", p."collection", p."identifier", p."batch_serial_id",
        d."TaxonID", tt."FullScientificName", tt."Genus", tt."Species", tt."Subspecies",
        f."FamilyID", f."FamilyName", f."FamilyNumber",
        l."FieldNo", l."LocalityString", l."Country", l."State", l."County",
        l."Drainage", l."WaterBody", l."Lat", l."Lon", l."StartDate", l."VerbatimDate",
        l."VerbatimCollectors",
        l."VerbatimCollectors" AS "Collector",
        l."VerbatimDate" AS "CollectedDate",
        -- verbatim locality 列已下线：新设计每条记录已建 locality1，前端改显示外联 locality1。
        -- 以下注释保留，日后若要回显 verbatim locality 再启用：
        -- vl."verbatim_locality_string", vl."verbatim_country", vl."verbatim_state",
        -- vl."verbatim_county", vl."verbatim_drainage", vl."verbatim_waterbody", vl."verbatim_fieldno",
        vtx."verbatim_family", vtx."verbatim_genus", vtx."verbatim_species",
        (SELECT string_agg(DISTINCT pp."PreparationType", ', ')
           FROM "Preparation" pp WHERE pp."PrimaryID" = p."PrimaryID") AS "Preparations",
        (SELECT SUM(pp."Count")
           FROM "Preparation" pp WHERE pp."PrimaryID" = p."PrimaryID") AS "PrepCount",
        (SELECT COUNT(*) FROM "Primary" c WHERE c.parent_id = p."PrimaryID") AS "ChildCount"
    """,
    order_by='p."PrimaryID" DESC',
    fields={
        "catalog_number":  FieldDef('p."CatalogNumber"', "idlist", "Catalog No.", "Specimen"),
        "batch_serial_id": FieldDef('p."batch_serial_id"', "text", "Batch No.", "Specimen"),
        "prev_number":     FieldDef('p."PrevNumber"', "text", "Prev Number", "Specimen"),
        "jar_size":        FieldDef('p."JarSize"', "enum", "Jar Size", "Specimen"),
        "storage":         FieldDef('p."Storage"', "enum", "Storage", "Specimen"),
        "type_status":     FieldDef('p."TypeStatus"', "text", "Type Status", "Specimen"),
        "inventory":       FieldDef('p."Inventory"', "text", "Inventory", "Specimen"),
        "total_number":    FieldDef('p."TotalNumber"', "number", "Total Number", "Specimen"),
        "remarks":         FieldDef('p."Remarks"', "text", "Remarks", "Specimen"),
        "family":          FieldDef('f."FamilyName"', "text", "Family", "Taxonomy"),
        "genus":           FieldDef('tt."Genus"', "text", "Genus", "Taxonomy"),
        "species":         FieldDef('tt."Species"', "text", "Species", "Taxonomy"),
        "scientific_name": FieldDef('tt."FullScientificName"', "text", "Scientific Name", "Taxonomy"),
        # 鉴定限定词（开放命名法）：从 Species 派生。affinis 是真实种名，故 aff. 用词边界正则避开它。
        "id_qualifier": FieldDef(
            """CASE
                WHEN tt."Species" IS NULL OR TRIM(tt."Species") = '' THEN NULL
                WHEN tt."Species" ILIKE '%cf.%' THEN 'cf.'
                WHEN tt."Species" ~* '(^|[^a-z])aff[. ]' THEN 'aff.'
                WHEN tt."Species" ~* '(^|[^a-z])nr[. ]' THEN 'nr.'
                WHEN tt."Species" ILIKE 'sp. nov%' THEN 'sp. nov.'
                WHEN tt."Species" ILIKE 'spp.%' OR tt."Species" = 'spp.' THEN 'spp.'
                WHEN tt."Species" ILIKE 'indet%' THEN 'indet.'
                WHEN tt."Species" ILIKE 'sp.%' OR tt."Species" IN ('sp', 'sp.') THEN 'sp.'
                ELSE 'determined' END""",
            "enum", "ID Qualifier", "Taxonomy",
            options=["determined", "sp.", "spp.", "cf.", "aff.", "nr.", "sp. nov.", "indet."]),
        "field_no":        FieldDef('l."FieldNo"', "text", "Field No.", "Locality"),
        "locality_string": FieldDef('l."LocalityString"', "text", "Locality String", "Locality"),
        "country":         FieldDef('l."Country"', "text", "Country", "Locality"),
        "state":           FieldDef('l."State"', "text", "State", "Locality"),
        "county":          FieldDef('l."County"', "text", "County", "Locality"),
        "drainage":        FieldDef('l."Drainage"', "text", "Drainage", "Locality"),
        "water_body":      FieldDef('l."WaterBody"', "text", "Water Body", "Locality"),
        "collector":       FieldDef('l."VerbatimCollectors"', "text", "Collector", "Locality"),
        "collected_date":  FieldDef('l."VerbatimDate"', "text", "Collected Date", "Locality"),
        "date_cataloged":  FieldDef('p."DateCataloged"', "date", "Date Cataloged", "Dates"),
        # verbatim（原始著录）—— taxonomy 仍显示；verbatim LOCALITY 已下线（每条已建 locality1，
        # 前端改显示外联 locality1），以下 verbatim locality 字段注释保留，日后要回显再启用。
        "verbatim_family":          FieldDef('vtx."verbatim_family"', "text", "Family (verbatim)", "Verbatim"),
        "verbatim_genus":           FieldDef('vtx."verbatim_genus"', "text", "Genus (verbatim)", "Verbatim"),
        "verbatim_species":         FieldDef('vtx."verbatim_species"', "text", "Species (verbatim)", "Verbatim"),
        # "verbatim_field_no":        FieldDef('vl."verbatim_fieldno"', "text", "Field No. (verbatim)", "Verbatim"),
        # "verbatim_locality_string": FieldDef('vl."verbatim_locality_string"', "text", "Locality String (verbatim)", "Verbatim"),
        # "verbatim_country":         FieldDef('vl."verbatim_country"', "text", "Country (verbatim)", "Verbatim"),
        # "verbatim_state":           FieldDef('vl."verbatim_state"', "text", "State (verbatim)", "Verbatim"),
        # "verbatim_county":          FieldDef('vl."verbatim_county"', "text", "County (verbatim)", "Verbatim"),
        # "verbatim_drainage":        FieldDef('vl."verbatim_drainage"', "text", "Drainage (verbatim)", "Verbatim"),
        # "verbatim_water_body":      FieldDef('vl."verbatim_waterbody"', "text", "Water Body (verbatim)", "Verbatim"),
    },
    global_search_cols=[
        "catalog_number", "prev_number", "scientific_name", "genus", "species", "family",
        "field_no", "locality_string", "country", "state", "county", "drainage", "water_body", "remarks",
    ],
)


@router.get("/lots/filter-metadata", response_model=ResponseModel)
async def get_lots_filter_metadata():
    """返回 lots 可过滤列清单（key/label/group/type/operators），驱动前端 chip 选择器。
    末尾追加两个「pick」伪字段：scientific name / family 的 taxon 感知 typeahead（前端特殊渲染，
    映射到 taxon_id/family_id + 三层开关，不走通用 SQL 过滤）。
    """
    fields = LOTS_SPEC.to_metadata() + [
        {"key": "taxon_pick", "label": "Scientific Name (taxon match)", "group": "Taxonomy",
         "type": "taxon", "operators": ["match"]},
        {"key": "family_pick", "label": "Family (taxon match)", "group": "Taxonomy",
         "type": "family", "operators": ["match"]},
    ]
    return {"code": 20000, "data": {"fields": fields}}


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
    # ids 来自 deaccession 页的 catalog# 输入框，前端变量为空时会拼出 "null"/"undefined"，
    # 老写法 int() 裸跑直接 500（logs/errors.log 里 /lot/null/1 已有 6 次）。
    id_list = parse_int_list(ids)
    if not id_list:
        # 返回与 execute_paginated_query_with_count 相同的形状，调用方不用分两种情况处理
        return {"code": 20000, "data": {
            "items": [], "total": 0, "page": pagination.page, "page_size": pagination.page_size,
            "total_pages": 0, "has_next": False, "has_prev": False}}
    # 展平为直接 JOIN（ON 用真实表别名，避免旧版嵌套子查询里 "TaxonID" 等列名歧义报错）；
    # 只取当前鉴定（IsCurrent=true），一条 lot 一行；冲突列加别名（CurrentTaxonID/PrimaryRemarks）。
    query = """
        SELECT
            p."PrimaryID" AS "MainPrimaryID",
            p."Remarks" AS "PrimaryRemarks",
            p.*,
            l."LocalityString",
            l."FieldNo",
            d."TaxonID" AS "CurrentTaxonID",
            tt."FullScientificName",
            f."FamilyName"
        FROM "Primary" p
        LEFT JOIN "Determination" d ON d."PrimaryID" = p."PrimaryID" AND d."IsCurrent" = true
        LEFT JOIN locality1 l ON l."Locality1ID" = p."Locality1ID"
        LEFT JOIN "TaxonomicTable" tt ON tt."TaxonID" = d."TaxonID"
        LEFT JOIN "Family" f ON f."FamilyID" = tt."FamilyID"
        WHERE p."CatalogNumber" = ANY($1::int[])
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


@router.get("/lot-by-primary/{primary_id}", response_model=ResponseModel)
async def get_lot_by_primary(primary_id: int):
    """按 PrimaryID 取单条 lot 的编辑数据（root 或子节点都行 —— 子节点没有 CatalogNumber，
    edit 必须用 PrimaryID）。返回结构同 /lot/{ids}/{limit}（items[0]）。"""
    query = """
        SELECT
            p."PrimaryID" AS "MainPrimaryID",
            p."Remarks" AS "PrimaryRemarks",
            p.*,
            l."LocalityString",
            l."FieldNo",
            d."TaxonID" AS "CurrentTaxonID",
            tt."FullScientificName",
            f."FamilyName"
        FROM "Primary" p
        LEFT JOIN "Determination" d ON d."PrimaryID" = p."PrimaryID" AND d."IsCurrent" = true
        LEFT JOIN locality1 l ON l."Locality1ID" = p."Locality1ID"
        LEFT JOIN "TaxonomicTable" tt ON tt."TaxonID" = d."TaxonID"
        LEFT JOIN "Family" f ON f."FamilyID" = tt."FamilyID"
        WHERE p."PrimaryID" = $1
        """
    records = await execute_query(query, primary_id)
    return {"code": 20000, "data": {"items": records, "total": len(records)}}


@router.get("/lotString/{catid}", response_model=ResponseModel)
async def get_lot_string(catid: str):
    """
    Get lot string by catalog ID.
    Mirrors the original getLotString function.

    catid arrives straight from the loan form's Catalog # box, so it is taken as
    text: anything that is not a catalog number a column can hold (letters, a
    "TU " prefix, the literal "undefined" from an empty row) is a search that
    matches nothing, and the form shows its existing "No result". Declaring it
    as int made FastAPI answer 422 with a validation dump instead.
    """
    catalog_number = parse_int_or_none(catid)
    if catalog_number is None:
        return {"code": 20000, "data": {"items": [], "total": 0}}

    # 展平为直接 JOIN（避免旧版 tt1.* 暴露的 Primary.TaxonID 与 Determination.TaxonID 在子查询里重名歧义）；
    # 用当前鉴定（IsCurrent）的 TaxonID 关联学名。
    query = """
    SELECT p."PrimaryID" as "LotID",
           CONCAT(p."CatalogNumber", '(', p."TotalNumber", ') Pri = ', tt."FullScientificName", ':', p."JarSize") as "LotString",
           p."TotalNumber"
    FROM "Primary" p
    LEFT JOIN "Determination" d ON d."PrimaryID" = p."PrimaryID" AND d."IsCurrent" = true
    LEFT JOIN "TaxonomicTable" tt ON tt."TaxonID" = d."TaxonID"
    WHERE p."CatalogNumber" = $1
    """

    records = await execute_query(query, catalog_number)

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
        search: Optional[str] = Query(None, description="全局模糊搜索（v1 走 DB 多列 ILIKE；以后可路由到 ES）"),
        ids: Optional[str] = Query(None, description="Catalog number 列表，逗号分隔"),
        field_filters: Optional[str] = Query(
            None,
            description='JSON: {api_name:[值/__EMPTY__/__NOT_EMPTY__]} 包含式匹配，同字段多值 OR、跨字段 AND'
        ),
        structured_filters: Optional[str] = Query(
            None,
            description='JSON: [{"field":..,"op":"between|eq|gte|lte|in|equals|..","values":[..]}] 精确/区间'
        ),
        sort_by: Optional[str] = Query(None, description="排序字段（api 字段名，白名单内）"),
        sort_order: Optional[str] = Query(None, description="asc | desc"),
        fuzzy_threshold: float = Query(0.4, ge=0.0, le=1.0, description="全局框 trigram 相似度阈值；0.4 时 percida✓/percda✗"),
        taxon_id: Optional[int] = Query(None, description="typeahead 选中的物种 TaxonID"),
        family_id: Optional[int] = Query(None, description="typeahead 选中的 FamilyID"),
        parent_id: Optional[int] = Query(None, description="有值=取该 PrimaryID 的直接子节点（展开树用）；无=顶层只返回根记录"),
        incl_similar: bool = Query(False, description="taxon 搜索是否含「相似写法」层（tier②）"),
        incl_synonym: bool = Query(True, description="taxon/family 搜索是否含「同义/接受名」层（tier③）"),
        pagination: PaginationParams = Depends(),
):
    """
    Lots 高级搜索（通用过滤引擎驱动，全参数化，复用 batch_review 范式）。
    契约见 app/services/filter_engine.py 与 docs/prototypes/lots_search_prototype.html。
    """
    # 解析 ids（逗号分隔整数）
    id_list = None
    if ids:
        id_list = []
        for s in ids.split(','):
            s = s.strip()
            if s:
                try:
                    id_list.append(int(s))
                except ValueError:
                    pass
        id_list = id_list or None

    where_clauses = []
    params = []
    p = 1
    rank_expr = None

    # 全局模糊框：子串 OR trigram 相似，并带相关性排序（精确>子串>相似）
    if search and str(search).strip():
        g_where, rank_expr, g_params, p = build_global_search(LOTS_SPEC, search, fuzzy_threshold, p)
        if g_where:
            where_clauses.append(g_where)
            params.extend(g_params)

    # 其余精确过滤（ids / field_filters / structured_filters），参数接在全局框之后
    w2, p2 = build_where(
        LOTS_SPEC,
        ids=id_list,
        field_filters=parse_json_param(field_filters),
        structured_filters=parse_json_param(structured_filters),
        start_param=p,
    )
    where_clauses.extend(w2)
    params.extend(p2)

    # 树：顶层只显示根（parent_id IS NULL）；展开时按 parent_id 取直接子节点。
    # parent_id 是 FastAPI 校验过的 int，安全内联（不占参数位）。
    if parent_id is not None:
        where_clauses.append(f'p.parent_id = {int(parent_id)}')
    else:
        where_clauses.append('p.parent_id IS NULL')

    # taxon typeahead：三层 TaxonID（① 精确 ② 相似 ③ 同义词）。id 来自 DB，用字面量数组拼，不占参数位。
    taxon_order = None
    if taxon_id:
        tiers = await _expand_taxon_tiers(taxon_id, fuzzy_threshold)
        # 按开关组装要包含的层：① 永远在；② 相似 / ③ 同义 由 incl_similar / incl_synonym 控制
        ids = list(tiers["t1"])
        if incl_similar:
            ids += tiers["t2"]
        if incl_synonym:
            ids += tiers["t3"]
        ids = list(dict.fromkeys(ids))
        if ids:
            where_clauses.append(f'd."TaxonID" = ANY({_int_array_sql(ids)})')
            taxon_order = (
                f'CASE WHEN d."TaxonID" = ANY({_int_array_sql(tiers["t1"])}) THEN 1 '
                f'WHEN d."TaxonID" = ANY({_int_array_sql(tiers["t2"])}) THEN 2 ELSE 3 END, '
                f'd."TaxonID", p."PrimaryID" DESC'
            )
        else:
            where_clauses.append("1=0")

    # family typeahead：该科（+ 同义科，受 incl_synonym 控制）
    if family_id:
        fam_ids = await _expand_family_ids(family_id) if incl_synonym else [family_id]
        where_clauses.append(f'f."FamilyID" = ANY({_int_array_sql(fam_ids)})')

    # 排序优先级：显式点表头 > taxon 三层 > 全局词相关性 > 默认
    order_by = LOTS_SPEC.order_clause(sort_by, sort_order)
    if not order_by:
        if taxon_order:
            order_by = taxon_order
        elif rank_expr:
            order_by = rank_expr + ' DESC, p."PrimaryID" DESC'
    main_query, count_query = LOTS_SPEC.build_queries(where_clauses, order_by=order_by)

    result = await execute_paginated_query_with_count(
        main_query=main_query,
        count_query=count_query,
        params=params,
        page=pagination.page,
        page_size=pagination.page_size
    )

    # 来源色标：每条结果按其名在 taxonomy_dev 的状态标 valid/synonym（连不上则无标签）
    items = result.get("data", {}).get("items", [])
    if items:
        names = []
        for it in items:
            gs = " ".join(x for x in [(it.get("Genus") or "").strip(), (it.get("Species") or "").strip()] if x).strip()
            names.append(gs or (it.get("FullScientificName") or "").strip())
        try:
            statuses = await _synonym_service.tag_status(names)
            for it, nm in zip(items, names):
                it["taxon_status"] = statuses.get(nm.strip().lower()) if nm else None
        except Exception:
            pass

    return result


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


# 可以作为根 lot 的 collection 类型。image 永远是子节点(凭证叶子),不能当 root;
# fluid 是缺省(整鱼/液浸)。见 collection 树规则(migrations/add_collection_tree.sql)。
ROOT_COLLECTIONS = {"fluid", "osteology", "tissue"}
# 可作为子节点(完整记录)的 collection。image 暂不走此表单(留给单独的图片上传页)。
SUB_LOT_COLLECTIONS = {"osteology", "tissue"}


def _normalize_determinations(zdet) -> list:
    """把前端 zDetermination 规整成统一结构(root 和子节点共用)。

    跳过完全空的行 —— 表单默认就带一行 isCurrent=true 但 taxon 为空的鉴定,以前照插不误,
    于是每个没选 taxon 就保存的 lot 都会多出一条 IsCurrent=true / TaxonID=NULL 的垃圾鉴定。
    后果:lots 搜索是经 Determination(IsCurrent) 连 taxon 名的,这种 lot 搜出来没有分类名;
    等策展人事后补上真正的鉴定,同一条记录就有两条 IsCurrent=true,当前鉴定变得不确定。
    (preparation 一直是跳过空行的,这里之前漏了。)
    """
    out = []
    for det in (zdet or []):
        row = {
            "isCurrent": det.get("isCurrent", False),
            "taxonId": det.get("taxonId") or None,
            "determinerID": (det.get("determination", {}) or {}).get("determinerID") or None,
            "determinerName": (det.get("determination", {}) or {}).get("determinerName") or None,
            "date": det.get("date") or None,
            "remarks": det.get("remarks") or None,
        }
        # isCurrent 不算“有内容”:它默认就是 true,不能靠它判断这行是不是真填了东西
        if not any(row[k] for k in ("taxonId", "determinerID", "determinerName", "date", "remarks")):
            continue
        out.append(row)
    return out


def _opt_int(v):
    """空 -> None,但 **0 要保留**。
    原来到处写 `x if x else None`,0 是 falsy,于是 TotalNumber=0(整批已退还/销毁)会被悄悄存成 NULL。"""
    if v is None or (isinstance(v, str) and not v.strip()):
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _cataloged_date(dt):
    """DateCataloged 归一成"当天零点"的 naive datetime。
    前端 date-picker 不带 value-format 时发的是带 Z 的 ISO,pydantic 解析成 tz-aware;
    root 走存储过程(参数是 date)会落成 00:00,子节点直接存则落成 05:00 —— 同一天建的父子记录
    日期字段长得不一样。这里统一只取日期部分。"""
    if dt is None:
        return None
    return datetime(dt.year, dt.month, dt.day)


def _normalize_preparations(preps) -> list:
    """把前端 preparation 规整(root 和子节点共用)。空 count -> None(否则 ::INTEGER 报错);
    跳过完全空的行(表单默认会带一行空的)。"""
    out = []
    for prep in (preps or []):
        ptype = prep.get("preparationType") or None
        pcount = prep.get("count")
        pcount = None if pcount in (None, "") else pcount
        if ptype is None and pcount is None:
            continue
        out.append({"preparationType": ptype, "count": pcount})
    return out


@router.post("/lot", response_model=ResponseModel)
async def new_lot(data: LotModel):
    """
    Create a new lot (root) — or, when parentId is set, a full sub-record node in the
    collection tree (same form/payload, just linked under a parent with an identifier).
    """
    try:
        determinations = _normalize_determinations(data.zDetermination)
        preparations = _normalize_preparations(data.preparation)

        # 子节点路径:有 parentId → 建一条挂在父节点下的完整记录(生成 identifier,不分配 catalog)。
        if data.parentId:
            return await _create_sub_lot(data, determinations, preparations)

        # root 路径:校验 collection(缺省 fluid),image 不能当 root。
        collection = (data.collection or "fluid").lower()
        if collection not in ROOT_COLLECTIONS:
            raise HTTPException(
                status_code=400,
                detail=f"collection must be one of: {', '.join(sorted(ROOT_COLLECTIONS))} "
                       f"(image is a voucher leaf and cannot be a root lot)",
            )

        result = await execute_proc(
            "add_lot_procedure",
            data.scientificName,
            data.prevNumber,
            _cataloged_date(data.dateCataloged),
            data.jarSize,
            data.storage,
            data.typeStatus,
            data.inventory,
            data.remarks,
            _opt_int(data.localityId),
            _opt_int(data.catalogerId),
            _opt_int(data.totalNumber),   # 0 是合法的标本数,别被当成"没填"
            determinations,
            preparations,
            collection
        )

        # 一并回查 PrimaryID，前端建完 root 可直接载入其 collection 树（不必再按 catalog 反查）
        row = await execute_query('SELECT "PrimaryID" FROM "Primary" WHERE "CatalogNumber" = $1', result)
        primary_id = row[0]["PrimaryID"] if row else None

        return {
            "code": 20000,
            "data": {
                "items": {"CatalogNumber": result, "PrimaryID": primary_id},
                "total": 1
            }
        }

    except HTTPException:
        raise  # 让 400(collection 校验等)原样透出，别被下面吞成 500
    except asyncpg.exceptions.UniqueViolationError as e:
        # catalog 号是 MAX+1 且没加锁，两个人同时建 root lot 会算出同一个号，
        # 靠 Primary_CatalogNumber_key 挡住。翻译成人话，别把 Postgres 原文丢给策展人。
        print(f"Catalog number collision while creating a lot: {e}")
        raise HTTPException(
            status_code=409,
            detail="Catalog number was taken by another record just now. Please submit again.")
    except asyncpg.exceptions.ForeignKeyViolationError as e:
        print(f"FK violation while creating a lot: {e}")
        raise HTTPException(
            status_code=400,
            detail="A referenced record (locality, cataloger or taxon) no longer exists.")
    except Exception as e:
        print(f"Error creating lot: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def _create_sub_lot(data: "LotModel", determinations: list, preparations: list) -> dict:
    """建一个完整的子节点记录(parent_id + identifier,不占 catalog 号)。
    继承(taxon/locality)由前端预填后整体提交,后端只管存。
    每个节点都是一条 Primary 记录,各自带 Determination + Preparation。同一事务。
    """
    coll = (data.collection or "").lower()
    if coll not in SUB_LOT_COLLECTIONS:
        raise HTTPException(
            status_code=400,
            detail=f"sub-record collection must be one of: {', '.join(sorted(SUB_LOT_COLLECTIONS))} "
                   f"(image is handled separately)",
        )
    parent = await execute_query(
        'SELECT "PrimaryID", collection FROM "Primary" WHERE "PrimaryID" = $1', data.parentId)
    if not parent:
        raise HTTPException(status_code=404, detail="parent record not found")
    if (parent[0].get("collection") or "") == "image":
        raise HTTPException(status_code=400, detail="image is a voucher leaf and cannot have sub-records")

    async with get_db() as conn:
        async with conn.transaction():
            # 算号必须和插入在同一事务里（内部会取 advisory lock）：
            # 以前在事务外先算好再进来插，两个并发请求会拿到同一个 identifier。
            ident = await _next_collection_identifier(COLLECTION_PREFIX[coll], conn)
            # CatalogNumber 显式 NULL(子节点用 identifier);DateCataloged 列是 date,
            # data.dateCataloged 是 datetime(date 的子类)asyncpg 可直接编码。
            pid = await conn.fetchval(
                'INSERT INTO "Primary" '
                '(parent_id, collection, identifier, "CatalogNumber", "PrevNumber", "DateCataloged", '
                ' "JarSize", "Storage", "TypeStatus", "Inventory", "Remarks", "Locality1ID", '
                ' "CatalogerID", "TotalNumber") '
                'VALUES ($1,$2,$3,NULL,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) RETURNING "PrimaryID"',
                data.parentId, coll, ident,
                data.prevNumber,
                # DateCataloged 列是 timestamp。和 root 用同一个归一化(只取日期,零点),
                # 否则同一天建的父子记录一个 00:00 一个 05:00。
                _cataloged_date(data.dateCataloged),
                data.jarSize, data.storage, data.typeStatus, data.inventory, data.remarks,
                _opt_int(data.localityId),
                _opt_int(data.catalogerId),
                _opt_int(data.totalNumber),
            )
            for det in determinations:
                # Date1 来自原始 dict(字符串/None),用 ::text::date 兼容字符串日期。
                await conn.execute(
                    'INSERT INTO "Determination" '
                    '("PrimaryID","IsCurrent","TaxonID","Determiner","DeterminerName","Date1","Remarks") '
                    'VALUES ($1,$2,$3,$4,$5,$6::text::date,$7)',
                    pid, det["isCurrent"], det["taxonId"], det["determinerID"],
                    det["determinerName"], det["date"], det["remarks"],
                )
            for prep in preparations:
                await conn.execute(
                    'INSERT INTO "Preparation" ("PrimaryID","PreparationType","Count") VALUES ($1,$2,$3)',
                    pid, prep["preparationType"], prep["count"],
                )
    return {"code": 20000, "data": {"items": {"PrimaryID": pid, "identifier": ident, "collection": coll}, "total": 1}}


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
    date_value = parse_year_start(year)

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


# ---------------------------------------------------------------------------
# Collection 树（跨馆藏关联）—— 见 migrations/add_collection_tree.sql
# 根 lot = fluid（数字 CatalogNumber）；子记录 = osteology/tissue/image，
# CatalogNumber 留 NULL，外显号 identifier = OST-/TIS-/IMG- + 顺序号；树用 parent_id(→PrimaryID)。
# 与 "Preparation"(保存方式) 表正交，互不影响。
# ---------------------------------------------------------------------------
COLLECTION_PREFIX = {"osteology": "OST", "tissue": "TIS", "image": "IMG"}


async def _next_collection_identifier(prefix: str, conn=None) -> str:
    """下一个 PREFIX-n 标识号（每个前缀独立递增）。

    必须在**调用方的事务里**执行(传 conn),并先取 advisory lock：算号是 MAX+1,
    以前在事务外裸跑,两个并发请求会算出同一个号,而 identifier 当时没有唯一约束,
    两条重号记录会双双写进去且不报错。现在:
      ① advisory lock 让同一 prefix 的"算号+插入"串行,正常情况下不会撞;
      ② uq_primary_identifier 唯一索引兜底(migrations/add_primary_identifier_unique.sql)。
    lock 随事务提交自动释放。
    """
    sql = ("SELECT COALESCE(MAX(CAST(substring(identifier from '[0-9]+$') AS INTEGER)), 0) + 1 AS n "
           'FROM "Primary" WHERE identifier ~ $1')
    pattern = f'^{prefix}-[0-9]+$'
    if conn is None:
        # 没有事务上下文时退回旧行为（只有唯一索引兜底）。正常路径都应该传 conn。
        rows = await execute_query(sql, pattern)
        return f"{prefix}-{rows[0]['n']}"
    # 每个 prefix 一把锁；hashtext 把前缀映射成 lock key
    await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1))", f"collection_identifier:{prefix}")
    n = await conn.fetchval(sql, pattern)
    return f"{prefix}-{n}"


class SubRecordModel(BaseModel):
    parent_primary_id: int
    collection: str  # osteology | tissue | image
    total_number: Optional[int] = None
    remarks: Optional[str] = None
    inherit_locality: bool = True


@router.post("/sub-record", response_model=ResponseModel)
async def add_sub_record(data: SubRecordModel):
    """在某个节点下加一个子记录（不同 collection 的派生记录）。"""
    coll = (data.collection or "").lower()
    if coll not in COLLECTION_PREFIX:
        raise HTTPException(status_code=400, detail="collection must be one of: osteology, tissue, image")
    parent = await execute_query(
        'SELECT "PrimaryID", collection, "Locality1ID" FROM "Primary" WHERE "PrimaryID" = $1',
        data.parent_primary_id,
    )
    if not parent:
        raise HTTPException(status_code=404, detail="parent record not found")
    if (parent[0].get("collection") or "") == "image":
        raise HTTPException(status_code=400, detail="image is a voucher leaf and cannot have sub-records")

    ident = await _next_collection_identifier(COLLECTION_PREFIX[coll])
    loc = parent[0].get("Locality1ID") if data.inherit_locality else None
    # CatalogNumber 显式置 NULL：它 DEFAULT 0 且有唯一索引，子记录不用它（用 identifier），
    # 多个 NULL 在 PG 唯一索引里互不冲突。
    rows = await execute_query(
        'INSERT INTO "Primary" (parent_id, collection, identifier, "CatalogNumber", "Locality1ID", "TotalNumber", "Remarks") '
        'VALUES ($1, $2, $3, NULL, $4, $5, $6) RETURNING "PrimaryID"',
        data.parent_primary_id, coll, ident, loc, data.total_number, data.remarks,
    )
    return {"code": 20000, "data": {"items": {"PrimaryID": rows[0]["PrimaryID"], "identifier": ident, "collection": coll}, "total": 1}}


@router.get("/tree/{primary_id}", response_model=ResponseModel)
async def get_lot_tree(primary_id: int):
    """取一个节点及其全部后代（递归 CTE）。前端按 parent_id 自行组装成树。"""
    rows = await execute_query(
        '''
        WITH RECURSIVE tree AS (
            SELECT "PrimaryID", parent_id, collection, identifier, "CatalogNumber", "TotalNumber", "Remarks"
            FROM "Primary" WHERE "PrimaryID" = $1
            UNION ALL
            SELECT c."PrimaryID", c.parent_id, c.collection, c.identifier, c."CatalogNumber", c."TotalNumber", c."Remarks"
            FROM "Primary" c JOIN tree t ON c.parent_id = t."PrimaryID"
        )
        SELECT * FROM tree
        ''',
        primary_id,
    )
    return {"code": 20000, "data": {"items": rows, "total": len(rows)}}


@router.delete("/sub-record/{primary_id}", response_model=ResponseModel)
async def delete_sub_record(primary_id: int, cascade: bool = Query(False)):
    """删除一个子记录节点（即存模式下的纠错入口）。
    - 只能删子记录（parent_id 非空）；root lot 不走此接口（用 deaccession / 删 lot 流程）。
    - 默认只删叶子；若节点有后代，需 cascade=true 才连整棵子树一起删（防误删整片）。
    """
    node = await execute_query(
        'SELECT "PrimaryID", parent_id, collection, identifier FROM "Primary" WHERE "PrimaryID" = $1',
        primary_id,
    )
    if not node:
        raise HTTPException(status_code=404, detail="record not found")
    if node[0]["parent_id"] is None:
        raise HTTPException(
            status_code=400,
            detail="this is a root lot, not a sub-record; cannot delete via this endpoint",
        )

    # 收集子树（含自身）
    descendants = await execute_query(
        '''
        WITH RECURSIVE tree AS (
            SELECT "PrimaryID" FROM "Primary" WHERE "PrimaryID" = $1
            UNION ALL
            SELECT c."PrimaryID" FROM "Primary" c JOIN tree t ON c.parent_id = t."PrimaryID"
        )
        SELECT "PrimaryID" FROM tree
        ''',
        primary_id,
    )
    ids = [r["PrimaryID"] for r in descendants]
    child_count = len(ids) - 1
    if child_count > 0 and not cascade:
        raise HTTPException(
            status_code=400,
            detail=f"node has {child_count} sub-record(s); pass cascade=true to delete the whole subtree",
        )

    # 一个事务里：先删依赖表（Determination/Preparation），再删 Primary（整棵子树一条 set 删除，
    # parent_id 自引用 FK 为 NO ACTION，语句末统一校验，子树内引用同时消失，不违例）。
    await execute_transaction([
        {"sql": 'DELETE FROM "Determination" WHERE "PrimaryID" = ANY($1::int[])', "params": [ids]},
        {"sql": 'DELETE FROM "Preparation" WHERE "PrimaryID" = ANY($1::int[])', "params": [ids]},
        {"sql": 'DELETE FROM "Primary" WHERE "PrimaryID" = ANY($1::int[])', "params": [ids]},
    ])
    return {"code": 20000, "data": {"items": {"deleted_primary_ids": ids, "count": len(ids)}, "total": len(ids)}}