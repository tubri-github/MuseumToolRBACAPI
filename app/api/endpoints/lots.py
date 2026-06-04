from datetime import datetime, date

from fastapi import APIRouter, Query, Depends, HTTPException, status
from typing import Dict, Any, List, Optional
from pydantic import BaseModel
from starlette.responses import StreamingResponse

from app.db.database import execute_query, execute_mutation, execute_proc, execute_paginated_query_with_count
from app.services.es_sync import handle_data_change
from app.services.filter_engine import FilterSpec, FieldDef, build_where, build_global_search, parse_json_param
from app.services.synonym_service import SynonymService

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
    """,
    select="""
        p."PrimaryID", p."CatalogNumber", p."PrevNumber", p."DateCataloged",
        p."JarSize", p."Storage", p."TypeStatus", p."Inventory", p."TotalNumber",
        p."Remarks", p."Locality1ID", p."CatalogerID", p."TimeStampModified",
        d."TaxonID", tt."FullScientificName", tt."Genus", tt."Species", tt."Subspecies",
        f."FamilyID", f."FamilyName", f."FamilyNumber",
        l."FieldNo", l."LocalityString", l."Country", l."State", l."County",
        l."Drainage", l."WaterBody", l."Lat", l."Lon", l."StartDate", l."VerbatimDate",
        (SELECT string_agg(DISTINCT pp."PreparationType", ', ')
           FROM "Preparation" pp WHERE pp."PrimaryID" = p."PrimaryID") AS "Preparations",
        (SELECT SUM(pp."Count")
           FROM "Preparation" pp WHERE pp."PrimaryID" = p."PrimaryID") AS "PrepCount"
    """,
    order_by='p."PrimaryID" DESC',
    fields={
        "catalog_number":  FieldDef('p."CatalogNumber"', "idlist", "Catalog No.", "标本 Specimen"),
        "prev_number":     FieldDef('p."PrevNumber"', "text", "Prev Number", "标本 Specimen"),
        "jar_size":        FieldDef('p."JarSize"', "enum", "Jar Size", "标本 Specimen"),
        "storage":         FieldDef('p."Storage"', "enum", "Storage", "标本 Specimen"),
        "type_status":     FieldDef('p."TypeStatus"', "text", "Type Status", "标本 Specimen"),
        "inventory":       FieldDef('p."Inventory"', "text", "Inventory", "标本 Specimen"),
        "total_number":    FieldDef('p."TotalNumber"', "number", "Total Number", "标本 Specimen"),
        "remarks":         FieldDef('p."Remarks"', "text", "Remarks", "标本 Specimen"),
        "family":          FieldDef('f."FamilyName"', "text", "Family", "分类 Taxonomy"),
        "genus":           FieldDef('tt."Genus"', "text", "Genus", "分类 Taxonomy"),
        "species":         FieldDef('tt."Species"', "text", "Species", "分类 Taxonomy"),
        "scientific_name": FieldDef('tt."FullScientificName"', "text", "Scientific Name", "分类 Taxonomy"),
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
            "enum", "ID Qualifier", "分类 Taxonomy",
            options=["determined", "sp.", "spp.", "cf.", "aff.", "nr.", "sp. nov.", "indet."]),
        "field_no":        FieldDef('l."FieldNo"', "text", "Field No.", "产地 Locality"),
        "locality_string": FieldDef('l."LocalityString"', "text", "Locality String", "产地 Locality"),
        "country":         FieldDef('l."Country"', "text", "Country", "产地 Locality"),
        "state":           FieldDef('l."State"', "text", "State", "产地 Locality"),
        "county":          FieldDef('l."County"', "text", "County", "产地 Locality"),
        "drainage":        FieldDef('l."Drainage"', "text", "Drainage", "产地 Locality"),
        "water_body":      FieldDef('l."WaterBody"', "text", "Water Body", "产地 Locality"),
        "date_cataloged":  FieldDef('p."DateCataloged"', "date", "Date Cataloged", "日期 Dates"),
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
        {"key": "taxon_pick", "label": "Scientific Name (taxon match)", "group": "分类 Taxonomy",
         "type": "taxon", "operators": ["match"]},
        {"key": "family_pick", "label": "Family (taxon match)", "group": "分类 Taxonomy",
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


# ---------------------------------------------------------------------------
# Collection 树（跨馆藏关联）—— 见 migrations/add_collection_tree.sql
# 根 lot = fluid（数字 CatalogNumber）；子记录 = osteology/tissue/image，
# CatalogNumber 留 NULL，外显号 identifier = OST-/TIS-/IMG- + 顺序号；树用 parent_id(→PrimaryID)。
# 与 "Preparation"(保存方式) 表正交，互不影响。
# ---------------------------------------------------------------------------
COLLECTION_PREFIX = {"osteology": "OST", "tissue": "TIS", "image": "IMG"}


async def _next_collection_identifier(prefix: str) -> str:
    """下一个 PREFIX-n 标识号（每个前缀独立递增）。低并发场景用 MAX+1。"""
    rows = await execute_query(
        "SELECT COALESCE(MAX(CAST(substring(identifier from '[0-9]+$') AS INTEGER)), 0) + 1 AS n "
        'FROM "Primary" WHERE identifier ~ $1',
        f'^{prefix}-[0-9]+$',
    )
    return f"{prefix}-{rows[0]['n']}"


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