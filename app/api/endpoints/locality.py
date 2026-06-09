from datetime import datetime, date

import os

import httpx
from fastapi import APIRouter, Query, Depends, HTTPException, status
from typing import Dict, Any, List, Optional, Union
from pydantic import BaseModel

from app.db.database import execute_query, execute_mutation, execute_paginated_query_with_count
from app.services.es_sync import handle_data_change
from app.services.filter_engine import FilterSpec, FieldDef, build_where, build_global_search, parse_json_param

router = APIRouter()


class _Pagination:
    def __init__(
        self,
        page: int = Query(1, ge=1, description="页码，从1开始"),
        page_size: int = Query(20, ge=1, le=100, description="每页记录数")
    ):
        self.page = page
        self.page_size = page_size


# Locality 搜索字段注册表（复用 lots 的过滤引擎）。建在 locality1 上，一行一产地，无需 DISTINCT。
# 策展人要的核心：locality_string 等 contains 子串搜索；外加 lat/lon 空值筛选找"待 geolocate"的产地。
LOCALITY_SPEC = FilterSpec(
    base='locality1 l',
    joins='',
    select='''
        l."Locality1ID", l."FieldNo", l."LocalityString", l."Drainage", l."WaterBody",
        l."Continent", l."Country", l."State", l."County", l."Island",
        l."Lat", l."Lon", l."ElevationMethod",
        l."StartDate", l."EndDate", l."VerbatimDate", l."VerbatimCollectors",
        l."year", l."month", l."day", l."Inventory", l."TimeStampModified"
    ''',
    order_by='l."Locality1ID" DESC',
    fields={
        "field_no":         FieldDef('l."FieldNo"', "text", "Field No.", "Locality"),
        "locality_string":  FieldDef('l."LocalityString"', "text", "Locality String", "Locality"),
        "drainage":         FieldDef('l."Drainage"', "text", "Drainage", "Locality"),
        "water_body":       FieldDef('l."WaterBody"', "text", "Water Body", "Locality"),
        "continent":        FieldDef('l."Continent"', "text", "Continent", "Geography"),
        "country":          FieldDef('l."Country"', "text", "Country", "Geography"),
        "state":            FieldDef('l."State"', "text", "State", "Geography"),
        "county":           FieldDef('l."County"', "text", "County", "Geography"),
        "island":           FieldDef('l."Island"', "text", "Island", "Geography"),
        "collectors":       FieldDef('l."VerbatimCollectors"', "text", "Collectors", "Collecting"),
        "verbatim_date":    FieldDef('l."VerbatimDate"', "text", "Verbatim Date", "Collecting"),
        "lat":              FieldDef('l."Lat"', "number", "Latitude", "Coordinates"),
        "lon":              FieldDef('l."Lon"', "number", "Longitude", "Coordinates"),
    },
    global_search_cols=[
        "field_no", "locality_string", "drainage", "water_body",
        "continent", "country", "state", "county", "collectors",
    ],
)


class LocalityModel(BaseModel):
    fieldNo: str
    localityString: Optional[str] = None
    drainage: Optional[str] = None
    waterbody: Optional[str] = None
    country: Optional[str] = None
    continent: Optional[str] = None
    state: Optional[str] = None
    county: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    verbatimDate: Optional[str] = None
    remark: Optional[str] = None
    inventory: Optional[str] = None
    verbatimCollectors: Optional[str] = None
    zCollectorsLocality: Optional[List[Dict[str, Any]]] = None


class ResponseModel(BaseModel):
    code: int
    data: Dict[str, Any]

class LocalitySearchModel(BaseModel):
    fieldNo: Optional[str] = None
    localityString: Optional[str] = None
    country: Optional[str] = None
    state: Optional[str] = None
    county: Optional[str] = None
    drainage: Optional[str] = None
    waterbody: Optional[str] = None
    fuzzySearch: Optional[bool] = True

class LocalityCreateModel(BaseModel):
    FieldNo: str
    LocalityString: str
    Drainage: Optional[str] = None
    Country: str
    State: str
    County: Optional[str] = None
    Continent: str
    Island: Optional[str] = None
    IslandGroup: Optional[str] = None
    ElevationMethod: Optional[str] = None
    WaterBody: Optional[str] = None
    Lon: Optional[float] = None
    Lat: Optional[float] = None
    StartDate: Optional[Union[str, date]] = None  # 允许字符串或日期对象
    EndDate: Optional[Union[str, date]] = None    # 允许字符串或日期对象
    VerbatimDate: Optional[str] = None
    year: Optional[int] = None
    month: Optional[int] = None
    day: Optional[int] = None
    Remarks: Optional[str] = None
    Inventory: Optional[str] = None
    VerbatimCollectors: Optional[str] = None

    class Config:
        # 允许字符串日期自动转换
        json_encoders = {
            date: lambda v: v.isoformat() if v else None
        }

@router.get("/locality/{keyword}", response_model=ResponseModel)
async def get_locality(keyword: str):
    """
    Get locality by keyword.
    Mirrors the original getLocality function.
    """
    query = """
    SELECT ll."Locality1ID",
           concat(ll."Inventory", ';', ll."FieldNo", ';', ll."LocalityString", ';', 
                  ll."Drainage", ';', ll."Country", ';', ll."State", ';', ll."County", ';', 
                  ll."Continent", ';', ll."WaterBody") as "LocalityString",
           similarity($1, ll."FieldNo") as sim
    FROM "locality1" ll
    WHERE (ll."FieldNo" <-> $1) < 0.95
    ORDER BY sim DESC
    LIMIT 20
    """

    records = await execute_query(query, keyword)

    return {
        "code": 20000,
        "data": {
            "items": records,
            "total": len(records)
        }
    }


@router.post("/locality", response_model=ResponseModel)
async def new_locality(data: LocalityModel):
    """
    Create a new locality.
    Mirrors the original newLocality function.
    """
    # Get current date components for the insertion
    query1 = """
    SELECT EXTRACT(YEAR FROM CURRENT_DATE) as year,
           EXTRACT(MONTH FROM CURRENT_DATE) as month,
           EXTRACT(DAY FROM CURRENT_DATE) as day
    """

    date_result = await execute_query(query1)
    year = int(date_result[0]["year"])
    month = int(date_result[0]["month"])
    day = int(date_result[0]["day"])

    # Insert new locality
    query2 = """
    INSERT INTO "locality1" (
        "FieldNo", "LocalityString", "Drainage", "WaterBody", "Country", "Continent",
        "State", "County", "Lat", "Lon", "StartDate", "EndDate", "VerbatimDate",
        "Remarks", "Inventory", "VerbatimCollectors", "year", "month", "day"
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
    RETURNING "Locality1ID", "FieldNo"
    """

    try:
        # Execute the insertion
        locality_result = await execute_query(
            query2,
            data.fieldNo,
            data.localityString,
            data.drainage,
            data.waterbody,
            data.country,
            data.continent,
            data.state,
            data.county,
            data.latitude,
            data.longitude,
            data.startDate,
            data.endDate,
            data.verbatimDate,
            data.remark,
            data.inventory,
            data.verbatimCollectors,
            year,
            month,
            day
        )

        if not locality_result or len(locality_result) == 0:
            raise HTTPException(status_code=500, detail="Failed to create locality")

        locality_id = locality_result[0]["Locality1ID"]
        field_number = locality_result[0]["FieldNo"]

        # Insert collectors for the locality if provided
        if data.zCollectorsLocality and len(data.zCollectorsLocality) > 0:
            # Build values for insertion
            collectors_values = []
            for collector in data.zCollectorsLocality:
                collectors_values.append(f"('{field_number}', '{collector['collectorID']}', {locality_id})")

            collectors_values_str = ", ".join(collectors_values)

            query3 = f"""
            INSERT INTO "CollectorsLocality" ("StationFieldNumber", "CollectorID", "Locality1ID")
            VALUES {collectors_values_str}
            """

            await execute_mutation(query3)

        # Sync the new data to Elasticsearch
        await handle_data_change("locality1", locality_id, "INSERT")

        return {
            "code": 20000,
            "data": {
                "localityID": locality_id,
                "total": 1
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/localitycount/{year}", response_model=ResponseModel)
async def get_locality_numbers_by_year(year: str):
    """
    Get locality count by year.
    Mirrors the original getLocalityNumbersByYear function.
    """
    date_value = datetime.strptime(f"{year}-01-01", "%Y-%m-%d").date()

    query = """
    SELECT * FROM locality1 l
    WHERE "StartDate" >= $1
    """

    records = await execute_query(query, date_value)

    return {
        "code": 20000,
        "data": {
            "total": len(records)
        }
    }


@router.get("/localityAdvanced", response_model=ResponseModel)
async def get_locality_advanced(
        search: Optional[str] = Query(None, description="全局模糊搜索"),
        field_filters: Optional[str] = Query(None, description='JSON: {api_name:[值/__EMPTY__/__NOT_EMPTY__]}'),
        structured_filters: Optional[str] = Query(None, description='JSON: [{"field":..,"op":..,"values":[..]}]'),
        sort_by: Optional[str] = Query(None),
        sort_order: Optional[str] = Query(None),
        fuzzy_threshold: float = Query(0.4, ge=0.0, le=1.0),
        pagination: _Pagination = Depends(),
):
    """Locality 高级搜索（复用 lots 过滤引擎）。一行一产地，无 DISTINCT，可直接用相关性排序。
    策展人要的 locality_string contains、以及 lat/lon 空值筛选（找待 geolocate 的产地）都走它。"""
    where_clauses = []
    params = []
    p = 1
    rank_expr = None

    if search and str(search).strip():
        g_where, rank_expr, g_params, p = build_global_search(LOCALITY_SPEC, search, fuzzy_threshold, p)
        if g_where:
            where_clauses.append(g_where)
            params.extend(g_params)

    w2, p2 = build_where(
        LOCALITY_SPEC,
        field_filters=parse_json_param(field_filters),
        structured_filters=parse_json_param(structured_filters),
        start_param=p,
    )
    where_clauses.extend(w2)
    params.extend(p2)

    order_by = LOCALITY_SPEC.order_clause(sort_by, sort_order)
    if not order_by and rank_expr:
        order_by = rank_expr + ' DESC, l."Locality1ID" DESC'
    main_query, count_query = LOCALITY_SPEC.build_queries(where_clauses, order_by=order_by)

    return await execute_paginated_query_with_count(
        main_query=main_query,
        count_query=count_query,
        params=params,
        page=pagination.page,
        page_size=pagination.page_size,
    )


@router.get("/filter-metadata", response_model=ResponseModel)
async def get_locality_filter_metadata():
    """返回 locality 可过滤列清单，驱动前端 chip 选择器。
    用单段路径 /filter-metadata（不能放 /locality/ 下，否则被 /locality/{keyword} 抢；
    且在 /{locality_id} 之前定义，先匹配）。"""
    return {"code": 20000, "data": {"fields": LOCALITY_SPEC.to_metadata()}}


# GEOLocate 公共地理参照服务（geo-locate.org）：按文字 locality + 行政区反查候选坐标。
_GEOLOCATE_URL = "https://www.geo-locate.org/webservices/geolocatesvcv2/glcwrap.aspx"


@router.get("/georeference", response_model=ResponseModel)
async def georeference_locality(
        locality: str = Query(..., description="locality 文字描述"),
        country: Optional[str] = Query(None),
        state: Optional[str] = Query(None),
        county: Optional[str] = Query(None),
        enable_h2o: bool = Query(True, description="水体定位（河流/湖泊，鱼类产地建议开）"),
):
    """代理 GEOLocate 公共服务，返回规整后的候选坐标列表（lat/lon/精度/score/不确定半径/解析模式）。
    走后端代理避免前端跨域(CORS)。单段路径，避开 /locality/{keyword}。"""
    if not locality or not locality.strip():
        raise HTTPException(status_code=400, detail="locality is required")
    params = {"locality": locality, "fmt": "json", "doUncert": "true"}
    if country:
        params["country"] = country
    if state:
        params["state"] = state
    if county:
        params["county"] = county
    if enable_h2o:
        params["enableH2O"] = "true"
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(_GEOLOCATE_URL, params=params, headers={"User-Agent": "museum-tool/1.0"})
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"GEOLocate request failed: {e}")

    feats = (data.get("resultSet") or {}).get("features") or data.get("features") or []
    candidates = []
    for f in feats:
        coords = ((f.get("geometry") or {}).get("coordinates") or []) + [None, None]
        props = f.get("properties") or {}
        candidates.append({
            "lon": coords[0], "lat": coords[1],
            "precision": props.get("precision"),
            "score": props.get("score"),
            "uncertaintyMeters": props.get("uncertaintyRadiusMeters"),
            "parsePattern": props.get("parsePattern"),
        })
    return {"code": 20000, "data": {"items": candidates, "total": len(candidates)}}


class CoordsUpdateModel(BaseModel):
    localityId: int
    lat: float
    lon: float


@router.post("/update-coords", response_model=ResponseModel)
async def update_locality_coords(data: CoordsUpdateModel):
    """把（georeference 选定的）经纬度存回某条 locality。"""
    await execute_mutation(
        'UPDATE locality1 SET "Lat" = $1, "Lon" = $2, "TimeStampModified" = NOW() WHERE "Locality1ID" = $3',
        data.lat, data.lon, data.localityId,
    )
    return {"code": 20000, "data": {"items": {"Locality1ID": data.localityId, "Lat": data.lat, "Lon": data.lon}, "total": 1}}


class LocalityUpdateModel(BaseModel):
    localityId: int
    # 字段名对齐 localityform 的 form（编辑复用 add 表单）
    fieldNo: Optional[str] = None
    localityString: Optional[str] = None
    drainage: Optional[str] = None
    waterbody: Optional[str] = None
    country: Optional[str] = None
    continent: Optional[str] = None
    state: Optional[str] = None
    county: Optional[str] = None
    latitude: Optional[Union[str, float]] = None
    longitude: Optional[Union[str, float]] = None
    startDate: Optional[str] = None
    endDate: Optional[str] = None
    verbatimDate: Optional[str] = None
    remark: Optional[str] = None
    inventory: Optional[str] = None
    verbatimCollectors: Optional[str] = None


def _to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


@router.post("/update-locality", response_model=ResponseModel)
async def update_locality(data: LocalityUpdateModel):
    """更新一条已有 locality（编辑复用 add 表单，按 Locality1ID 改）。
    StartDate/EndDate 是 timestamp（前端可能传带 Z 的字符串），用 NULLIF::timestamp 兼容；Lat/Lon 字符串转 float。"""
    await execute_mutation(
        '''
        UPDATE locality1 SET
            "FieldNo" = $2, "LocalityString" = $3, "Drainage" = $4, "WaterBody" = $5,
            "Country" = $6, "Continent" = $7, "State" = $8, "County" = $9,
            "Lat" = $10, "Lon" = $11,
            "StartDate" = NULLIF($12, '')::timestamp, "EndDate" = NULLIF($13, '')::timestamp,
            "VerbatimDate" = $14, "Remarks" = $15, "Inventory" = $16, "VerbatimCollectors" = $17,
            "TimeStampModified" = NOW()
        WHERE "Locality1ID" = $1
        ''',
        data.localityId, data.fieldNo, data.localityString, data.drainage, data.waterbody,
        data.country, data.continent, data.state, data.county,
        _to_float(data.latitude), _to_float(data.longitude),
        data.startDate or '', data.endDate or '',
        data.verbatimDate, data.remark, data.inventory, data.verbatimCollectors,
    )
    return {"code": 20000, "data": {"items": {"Locality1ID": data.localityId}, "total": 1}}


# ---- 地理位置建议（gazetteer）：本地受控词表 + GeoNames 全球库 ----
GEONAMES_USERNAME = os.getenv("GEONAMES_USERNAME", "tubrimap")
# 本地表（扁平，只名字）：level -> (表, 列)
_LOCAL_GEO = {
    "continent": ('"Continents"', '"Continent"'),
    "country": ('"Countries"', '"Country"'),
    "state": ('"States"', '"State"'),
    "county": ('"Counties"', '"County"'),
}
# GeoNames featureCode：按行政层级筛
_GEONAMES_FC = {"continent": "CONT", "country": "PCLI", "state": "ADM1", "county": "ADM2"}


async def _local_geo_suggest(level: str, q: str, limit: int):
    t = _LOCAL_GEO.get(level)
    if not t:
        return []
    table, col = t
    if q:
        rows = await execute_query(
            f'SELECT {col} AS name FROM {table} WHERE {col} ILIKE $1 ORDER BY {col} LIMIT {limit}', f"%{q}%")
    else:
        rows = await execute_query(f'SELECT {col} AS name FROM {table} ORDER BY {col} LIMIT {limit}')
    return [{"name": r["name"], "source": "local"} for r in rows if r["name"]]


async def _geonames_suggest(level: str, q: str, limit: int):
    """GeoNames 全球库（带行政层级 + 坐标）。失败/未启用 web services → 返回 []（优雅降级）。"""
    if not q:
        return []
    params = {"q": q, "maxRows": limit, "username": GEONAMES_USERNAME, "style": "MEDIUM", "orderby": "relevance"}
    fc = _GEONAMES_FC.get(level)
    if fc:
        params["featureCode"] = fc
    try:
        async with httpx.AsyncClient(timeout=12) as client:
            resp = await client.get("http://api.geonames.org/searchJSON", params=params,
                                    headers={"User-Agent": "museum-tool/1.0"})
            data = resp.json()
    except Exception:
        return []
    if not isinstance(data, dict) or data.get("status"):
        return []  # status 错误对象（如账号未启用 web services）
    out = []
    for g in data.get("geonames", []):
        out.append({
            "name": g.get("name"),
            "country": g.get("countryName"),
            "countryCode": g.get("countryCode"),
            "state": g.get("adminName1"),
            "county": g.get("adminName2"),
            "lat": g.get("lat"),
            "lng": g.get("lng"),
            "source": "geonames",
        })
    return out


@router.get("/geo-suggest", response_model=ResponseModel)
async def geo_suggest(
        q: str = Query("", description="输入片段"),
        level: str = Query("country", description="continent|country|state|county"),
        limit: int = Query(8, ge=1, le=20),
):
    """地名建议：先本地受控词表（快、可控），再 GeoNames（全球 + 官方名 + 层级 + 坐标）。
    单段路径，避开 /locality/{keyword} 与 /{locality_id}。"""
    ql = (q or "").strip()
    local = await _local_geo_suggest(level, ql, limit)
    geo = await _geonames_suggest(level, ql, limit)
    seen = set()
    items = []
    # 去重按 (name + 层级)：GeoNames 的同名不同地（Hancock/Ohio vs Hancock/Mississippi）要都保留，
    # 否则会被同名的本地条目折叠掉，丢了层级这个最大价值。
    for s in local + geo:
        nm = (s.get("name") or "").strip().lower()
        if not nm:
            continue
        key = (nm, (s.get("state") or "").strip().lower(), (s.get("country") or "").strip().lower())
        if key in seen:
            continue
        seen.add(key)
        items.append(s)
    return {"code": 20000, "data": {"items": items, "total": len(items)}}


@router.get("/country", response_model=ResponseModel)
async def get_country(keyword: Optional[str] = None):
    """
    Get countries.
    Mirrors the original getCountry function.
    """
    query = """
    SELECT ct."CountryID", ct."Country" FROM "Countries" ct
    """

    records = await execute_query(query)

    return {
        "code": 20000,
        "data": {
            "items": records,
            "total": len(records)
        }
    }


@router.get("/state", response_model=ResponseModel)
async def get_states(keyword: Optional[str] = None):
    """
    Get states.
    Mirrors the original getStates function.
    """
    query = """
    SELECT * FROM "States" pt
    """

    records = await execute_query(query)

    return {
        "code": 20000,
        "data": {
            "items": records,
            "total": len(records)
        }
    }


@router.get("/county", response_model=ResponseModel)
async def get_county():
    """
    Get counties.
    Mirrors the original getCounty function.
    """
    query = """
    SELECT * FROM "Counties"
    """

    records = await execute_query(query)

    return {
        "code": 20000,
        "data": {
            "items": records,
            "total": len(records)
        }
    }


@router.get("/continent", response_model=ResponseModel)
async def get_continent(keyword: Optional[str] = None):
    """
    Get continents.
    Mirrors the original getContinent function.
    """
    query = """
    SELECT * FROM "Continents"
    """

    records = await execute_query(query)

    return {
        "code": 20000,
        "data": {
            "items": records,
            "total": len(records),
        }
    }



class LocalitySearchModel(BaseModel):
    fieldNo: Optional[str] = None
    localityString: Optional[str] = None
    country: Optional[str] = None
    state: Optional[str] = None
    county: Optional[str] = None
    drainage: Optional[str] = None
    waterbody: Optional[str] = None
    fuzzySearch: Optional[bool] = True


class LocalityCreateModel(BaseModel):
    FieldNo: str
    LocalityString: str
    Drainage: Optional[str] = None
    Country: str
    State: str
    County: Optional[str] = None
    Continent: str
    Island: Optional[str] = None
    IslandGroup: Optional[str] = None
    ElevationMethod: Optional[str] = None
    WaterBody: Optional[str] = None
    Lon: Optional[float] = None
    Lat: Optional[float] = None
    StartDate: Optional[str] = None
    EndDate: Optional[str] = None
    StartTime: Optional[int] = None
    EndTime: Optional[int] = None
    VerbatimDate: Optional[str] = None
    year: Optional[int] = None
    month: Optional[int] = None
    day: Optional[int] = None
    Remarks: Optional[str] = None
    Inventory: Optional[str] = None
    VerbatimCollectors: Optional[str] = None
    ElevationMethodID: Optional[int] = None


@router.get("/search", response_model=ResponseModel)
async def search_localities_unified(
        query: Optional[str] = Query(None, description="General search query"),
        field_no: Optional[str] = Query(None, description="Exact field number"),
        locality_id: Optional[int] = Query(None, description="Exact locality ID"),
        country: Optional[str] = Query(None, description="Country filter"),
        state: Optional[str] = Query(None, description="State filter"),
        county: Optional[str] = Query(None, description="County filter"),
        drainage: Optional[str] = Query(None, description="Drainage filter"),
        fuzzy: bool = Query(True, description="Enable fuzzy search"),
        limit: int = Query(50, ge=1, le=100, description="Maximum results")
):
    """
    统一的地点搜索接口，支持多种搜索方式
    """
    try:
        base_select = """
        SELECT 
            l."Locality1ID",
            l."FieldNo",
            l."LocalityString",
            l."Drainage",
            l."Country",
            l."State",
            l."County",
            l."Continent",
            l."Island",
            l."Island Group" as "IslandGroup",
            l."ElevationMethod",
            l."ElevationMethodID",
            l."WaterBody",
            l."Lon",
            l."Lat",
            l."StartDate",
            l."EndDate",
            l."StartTime",
            l."EndTime",
            l."VerbatimDate",
            l."year",
            l."month",
            l."day",
            l."Inventory",
            l."TimeStampModified",
            l."VerbatimCollectors"
        FROM locality1 l
        WHERE 1=1
        """

        where_clauses = []
        params = []
        param_index = 1

        # 1. 精确ID搜索（最高优先级）
        if locality_id is not None:
            where_clauses.append(f'l."Locality1ID" = ${param_index}')
            params.append(locality_id)

            final_query = base_select + " AND " + " AND ".join(where_clauses)
            records = await execute_query(final_query, *params)

            return {
                "code": 20000,
                "data": {
                    "items": records,
                    "total": len(records)
                }
            }

        # 2. 精确Field No搜索
        if field_no:
            where_clauses.append(f'l."FieldNo" = ${param_index}')
            params.append(field_no)
            param_index += 1

        # 3. 通用查询搜索
        elif query:
            if fuzzy:
                search_condition = f'''(
                    l."FieldNo" ILIKE ${param_index} OR 
                    l."LocalityString" ILIKE ${param_index} OR 
                    l."Country" ILIKE ${param_index} OR 
                    l."State" ILIKE ${param_index}
                )'''
                where_clauses.append(search_condition)
                params.append(f"%{query}%")
                param_index += 1
            else:
                search_condition = f'(l."FieldNo" = ${param_index} OR l."LocalityString" = ${param_index})'
                where_clauses.append(search_condition)
                params.append(query)
                param_index += 1

        # 4. 地理过滤条件
        if country:
            where_clauses.append(f'l."Country" ILIKE ${param_index}')
            params.append(f"%{country}%")
            param_index += 1

        if state:
            where_clauses.append(f'l."State" ILIKE ${param_index}')
            params.append(f"%{state}%")
            param_index += 1

        if county:
            where_clauses.append(f'l."County" ILIKE ${param_index}')
            params.append(f"%{county}%")
            param_index += 1

        if drainage:
            where_clauses.append(f'l."Drainage" ILIKE ${param_index}')
            params.append(f"%{drainage}%")
            param_index += 1

        # 构建最终查询
        if where_clauses:
            final_query = base_select + " AND " + " AND ".join(where_clauses)
        else:
            final_query = base_select

        # 添加排序和限制
        if field_no:
            final_query += " ORDER BY l.\"Locality1ID\""
        elif query and fuzzy:
            # 修复：为模糊搜索添加额外的参数
            order_param_index = param_index
            final_query += f''' ORDER BY (
                CASE 
                    WHEN l."FieldNo" ILIKE ${order_param_index} THEN 100
                    WHEN l."LocalityString" ILIKE ${order_param_index} THEN 80
                    WHEN l."Country" ILIKE ${order_param_index} THEN 60
                    ELSE 40
                END
            ) DESC, l."Locality1ID"'''
            # 为ORDER BY子句添加参数
            params.append(f"%{query}%")
        else:
            final_query += ' ORDER BY l."TimeStampModified" DESC NULLS LAST, l."Locality1ID" DESC'

        final_query += f" LIMIT {limit}"

        records = await execute_query(final_query, *params)

        return {
            "code": 20000,
            "data": {
                "items": records,
                "total": len(records)
            }
        }

    except Exception as e:
        return {
            "code": 50000,
            "data": {
                "message": f"Failed to search localities: {str(e)}"
            }
        }

# 检查Field No是否存在
@router.get("/check-fieldno/{field_no}", response_model=ResponseModel)
async def check_fieldno_exists(field_no: str):
    """
    检查字段编号是否已存在
    """
    try:
        query = """
        SELECT "Locality1ID", "FieldNo", "LocalityString"
        FROM locality1
        WHERE "FieldNo" = $1
        LIMIT 1
        """

        result = await execute_query(query, field_no)
        exists = len(result) > 0

        return {
            "code": 20000,
            "data": {
                "exists": exists,
                "fieldNo": field_no,
                "locality": result[0] if exists else None
            }
        }
    except Exception as e:
        return {
            "code": 50000,
            "message": f"Failed to check field number: {str(e)}"
        }


# 创建新地点（简化版）
@router.post("/create", response_model=ResponseModel)
async def create_new_locality(locality_data: LocalityCreateModel):
    """
    创建新的地点记录
    """
    try:
        # 检查FieldNo是否已存在
        check_result = await execute_query(
            'SELECT "Locality1ID" FROM locality1 WHERE "FieldNo" = $1',
            locality_data.FieldNo
        )

        if check_result:
            return {
                "code": 40900,
                "message": f"Field No '{locality_data.FieldNo}' already exists",
                "data": {"existing_locality_id": check_result[0]["Locality1ID"]}
            }

        # 处理日期转换
        start_date = None
        end_date = None

        if locality_data.StartDate:
            try:
                if isinstance(locality_data.StartDate, str):
                    start_date = datetime.strptime(locality_data.StartDate, '%Y-%m-%d').date()
                else:
                    start_date = locality_data.StartDate
            except ValueError as e:
                return {
                    "code": 40000,
                    "message": f"Invalid StartDate format: {str(e)}"
                }

        if locality_data.EndDate:
            try:
                if isinstance(locality_data.EndDate, str):
                    end_date = datetime.strptime(locality_data.EndDate, '%Y-%m-%d').date()
                else:
                    end_date = locality_data.EndDate
            except ValueError as e:
                return {
                    "code": 40000,
                    "message": f"Invalid EndDate format: {str(e)}"
                }

        # 从日期中提取年月日
        year = None
        month = None
        day = None

        if start_date:
            year = start_date.year
            month = start_date.month
            day = start_date.day
        elif locality_data.year:
            year = locality_data.year
            if locality_data.month:
                month = locality_data.month
            if locality_data.day:
                day = locality_data.day

        # 插入新locality记录
        insert_query = """
        INSERT INTO locality1 (
            "FieldNo", "LocalityString", "Drainage", "Country", "State", "County",
            "Continent", "Island", "Island Group", "ElevationMethod", "WaterBody",
            "Lon", "Lat", "StartDate", "EndDate", "VerbatimDate", 
            "year", "month", "day", "Remarks", "Inventory",
            "VerbatimCollectors", "TimeStampModified"
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
            $17, $18, $19, $20, $21, $22, NOW()
        ) RETURNING "Locality1ID", "FieldNo", "LocalityString"
        """

        result = await execute_query(
            insert_query,
            locality_data.FieldNo,
            locality_data.LocalityString,
            locality_data.Drainage,
            locality_data.Country,
            locality_data.State,
            locality_data.County,
            locality_data.Continent,
            locality_data.Island,
            locality_data.IslandGroup,
            locality_data.ElevationMethod,
            locality_data.WaterBody,
            locality_data.Lon,
            locality_data.Lat,
            start_date,  # 使用转换后的 date 对象
            end_date,  # 使用转换后的 date 对象
            locality_data.VerbatimDate,
            year,
            month,
            day,
            locality_data.Remarks,
            locality_data.Inventory,
            locality_data.VerbatimCollectors
        )

        if not result:
            return {
                "code": 50000,
                "message": "Failed to create locality"
            }

        new_locality = result[0]

        # 同步到Elasticsearch（如果可用）
        try:
            await handle_data_change("locality1", new_locality["Locality1ID"], "INSERT")
        except Exception as es_error:
            print(f"Elasticsearch sync warning: {es_error}")

        return {
            "code": 20000,
            "data": {
                "Locality1ID": new_locality["Locality1ID"],
                "FieldNo": new_locality["FieldNo"],
                "LocalityString": new_locality["LocalityString"],
                "message": f"Locality created successfully"
            }
        }
    except Exception as e:
        print(f"Error creating locality: {str(e)}")
        return {
            "code": 50000,
            "message": f"Failed to create locality: {str(e)}"
        }


# 获取单个地点详情
@router.get("/{locality_id}", response_model=ResponseModel)
async def get_locality_by_id(locality_id: int):
    """
    根据ID获取地点详情
    """
    try:
        query = """
        SELECT
            l."Locality1ID",
            l."FieldNo",
            l."LocalityString",
            l."Drainage",
            l."Country",
            l."State",
            l."County",
            l."Continent",
            l."Island",
            l."Island Group" as "IslandGroup",
            l."ElevationMethod",
            l."ElevationMethodID",
            l."WaterBody",
            l."Lon",
            l."Lat",
            l."StartDate",
            l."EndDate",
            l."StartTime",
            l."EndTime",
            l."VerbatimDate",
            l."year",
            l."month",
            l."day",
            l."Remarks",
            l."Inventory",
            l."TimeStampModified",
            l."VerbatimCollectors"
        FROM locality1 l
        WHERE l."Locality1ID" = $1
        """

        result = await execute_query(query, locality_id)

        if not result:
            return {
                "code": 40400,
                "message": f"Locality with ID {locality_id} not found"
            }

        return {
            "code": 20000,
            "data": {
                "items": result,
                "total": 1
            }
        }
    except Exception as e:
        return {
            "code": 50000,
            "message": f"Failed to get locality details: {str(e)}"
        }