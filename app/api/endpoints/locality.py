from datetime import datetime, date, timezone

import os

import httpx
from fastapi import APIRouter, Query, Depends, HTTPException, status
from typing import Dict, Any, List, Optional, Union
from pydantic import BaseModel

import asyncpg

from app.db.database import execute_query, execute_mutation, execute_paginated_query_with_count, get_db
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


# ---- 表单入参归一化 ----
# 前端表单里的空值一律是 ''（el-input / el-date-picker 的初值），到了这里要变成 NULL；
# 数字列拿到的也是字符串。asyncpg 不做任何隐式转换，所以在进 SQL 前统一处理。

def _blank(v):
    """'' / 全空白 -> None（FieldNo 等可空列要存 NULL，UNIQUE 允许多个 NULL，'' 只允许一个）。"""
    if v is None:
        return None
    if isinstance(v, str) and not v.strip():
        return None
    return v


def _to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _to_coord(v, field, lo, hi):
    """经纬度：空 -> NULL；填了但解析不出来 -> 400。
    原来一律 _to_float 静默返回 None，策展人把 38.12417 打成 '38.12.417' 时坐标会无声消失。
    顺带做范围校验，挡住 lat/lon 填反这类错误。"""
    if _blank(v) is None:
        return None
    f = _to_float(v)
    if f is None:
        raise HTTPException(status_code=400, detail=f"Invalid {field}: {v}")
    if not (lo <= f <= hi):
        raise HTTPException(status_code=400, detail=f"{field} out of range ({lo}..{hi}): {f}")
    return f


def _to_ts(v, field=""):
    """表单日期 -> datetime（timestamp 列）。
    前端 date-picker 默认发的是带 Z 的 ISO（'2026-08-12T05:00:00.000Z'），也兼容 'YYYY-MM-DD'。
    带时区的按 UTC 落到 naive，和 locality1 的 timestamp without time zone 对齐。"""
    if _blank(v) is None:
        return None
    if isinstance(v, datetime):
        return v.astimezone(timezone.utc).replace(tzinfo=None) if v.tzinfo else v
    if isinstance(v, date):
        return datetime(v.year, v.month, v.day)
    s = str(v).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid date{' for ' + field if field else ''}: {v}")
    return dt.astimezone(timezone.utc).replace(tzinfo=None) if dt.tzinfo else dt


def _date_warnings(start_ts: Optional[datetime], end_ts: Optional[datetime]) -> List[str]:
    """采集日期区间的提醒。**只提醒不拦截**：库里已经有 EndDate 早于 StartDate 的历史记录，
    硬拦会让策展人连打开那些记录再保存都做不到。同一天算正常（当天采完）。"""
    warnings = []
    if start_ts and end_ts and end_ts < start_ts:
        warnings.append(
            f"End date {end_ts.date()} is before start date {start_ts.date()}. "
            f"Saved as entered — please check the dates.")
    return warnings


def _ymd(ts: Optional[datetime]):
    """locality1 的 year/month/day 是采集日期的拆分列，跟着 StartDate 走；无 StartDate 则为 NULL。"""
    return (ts.year, ts.month, ts.day) if ts else (None, None, None)


def _collector_ids(rows: Optional[List[Dict[str, Any]]]) -> List[int]:
    """挑出真正选了人的 collector 行。表单默认带一行空的 {collectorName:'', collectorID:''}，
    直接插会以 '' 撞 integer 列（22P02）；顺带去重，避免同一个人重复关联。"""
    out = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        try:
            cid = int(row.get("collectorID"))
        except (TypeError, ValueError):
            continue
        if cid and cid not in out:
            out.append(cid)
    return out


class LocalityModel(BaseModel):
    # FieldNo 可为空（现场不一定给得到），空串按 NULL 存
    fieldNo: Optional[str] = None
    localityString: Optional[str] = None
    drainage: Optional[str] = None
    waterbody: Optional[str] = None
    country: Optional[str] = None
    continent: Optional[str] = None
    state: Optional[str] = None
    county: Optional[str] = None
    # 表单是 el-input，来的是字符串；不填时是 ''，声明成 float 会直接 422
    latitude: Optional[Union[str, float]] = None
    longitude: Optional[Union[str, float]] = None
    startDate: Optional[Union[str, datetime]] = None
    endDate: Optional[Union[str, datetime]] = None
    verbatimDate: Optional[str] = None
    remark: Optional[str] = None
    inventory: Optional[str] = None
    verbatimCollectors: Optional[str] = None
    zCollectorsLocality: Optional[List[Dict[str, Any]]] = None


class ResponseModel(BaseModel):
    code: int
    data: Dict[str, Any]

# 注：LocalitySearchModel / LocalityCreateModel 原本这里还有一份同名定义，但文件后面各自
# 又定义了一次，Python 只有后者生效——改前面那份不起任何作用，容易看错。已删。

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
    # year/month/day 是采集日期的拆分列，不是入库日期：从 StartDate 推。
    # （原来写的是 CURRENT_DATE，等于把"今天"当成采集年月日；没有 StartDate 时留 NULL 才是诚实的。）
    start_ts = _to_ts(data.startDate, "startDate")
    end_ts = _to_ts(data.endDate, "endDate")
    year, month, day = _ymd(start_ts)
    warnings = _date_warnings(start_ts, end_ts)

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

    field_no = _blank(data.fieldNo)
    collector_ids = _collector_ids(data.zCollectorsLocality)

    try:
        # locality + collectors 必须一起成败：以前分两次独立连接写，collectors 失败会留下
        # 一条没有采集人的孤儿产地，用户重试还会撞 FieldNo 唯一约束。
        async with get_db() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    query2,
                    field_no,
                    _blank(data.localityString),
                    _blank(data.drainage),
                    _blank(data.waterbody),
                    _blank(data.country),
                    _blank(data.continent),
                    _blank(data.state),
                    _blank(data.county),
                    _to_coord(data.latitude, "latitude", -90, 90),
                    _to_coord(data.longitude, "longitude", -180, 180),
                    start_ts,
                    end_ts,
                    _blank(data.verbatimDate),
                    _blank(data.remark),
                    _blank(data.inventory),
                    _blank(data.verbatimCollectors),
                    year,
                    month,
                    day
                )

                if not row:
                    raise HTTPException(status_code=500, detail="Failed to create locality")

                locality_id = row["Locality1ID"]
                field_number = row["FieldNo"]

                # Insert collectors for the locality if provided.
                # 全参数化：FieldNo 带撇号（O'Brien-1）以前会把拼出来的 SQL 打断。
                if collector_ids:
                    await conn.execute(
                        '''
                        INSERT INTO "CollectorsLocality" ("StationFieldNumber", "CollectorID", "Locality1ID")
                        SELECT $1, c, $3 FROM unnest($2::int[]) AS c
                        ''',
                        field_number, collector_ids, locality_id,
                    )

        if warnings:
            print(f"Locality {locality_id} created with warnings: {'; '.join(warnings)}")

        return {
            "code": 20000,
            "data": {
                "localityID": locality_id,
                "warnings": warnings,
                "total": 1
            }
        }

    except HTTPException:
        # 400（日期格式）等已经是给前端看的错误，别被下面重新包成 500
        raise
    except asyncpg.exceptions.UniqueViolationError:
        # FieldNo 唯一约束。放在这里而不是先 SELECT 再插，是为了同时挡住并发下的竞态。
        raise HTTPException(status_code=409, detail=f"Field No '{field_no}' already exists")
    except asyncpg.exceptions.ForeignKeyViolationError:
        raise HTTPException(status_code=400, detail="One of the selected collectors no longer exists")
    except Exception as e:
        print(f"Error creating locality: {e}")
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
    startDate: Optional[Union[str, datetime]] = None
    endDate: Optional[Union[str, datetime]] = None
    verbatimDate: Optional[str] = None
    remark: Optional[str] = None
    inventory: Optional[str] = None
    verbatimCollectors: Optional[str] = None
    zCollectorsLocality: Optional[List[Dict[str, Any]]] = None
    # 必须显式置 true 才会重写采集人关联行。
    # 不能只看 zCollectorsLocality 有没有值：**旧版前端也会把表单里默认那行空的发上来**，
    # 那样"整体重写"就变成了把这条产地的采集人全删光。后端先上线、前端还没刷新时会中招。
    manageCollectors: Optional[bool] = False


@router.post("/update-locality", response_model=ResponseModel)
async def update_locality(data: LocalityUpdateModel):
    """更新一条已有 locality（编辑复用 add 表单，按 Locality1ID 改）。
    日期/数字/空串的归一化和新建走同一套 helper；year/month/day 跟着 StartDate 一起改，
    否则改了采集日期这三列还留着旧值。采集人关联行按提交的列表重写。"""
    start_ts = _to_ts(data.startDate, "startDate")
    end_ts = _to_ts(data.endDate, "endDate")
    year, month, day = _ymd(start_ts)
    field_no = _blank(data.fieldNo)
    warnings = _date_warnings(start_ts, end_ts)

    try:
        # 产地本体 + 采集人关联行一起成败
        async with get_db() as conn:
            async with conn.transaction():
                # 改动前的 FieldNo：下面只有在它真的变了时才去动关联行的冗余副本。
                # 不能无条件同步——库里有 10 条两侧不一致的历史记录，其中 2 条恰恰是 locality1
                # 这边写错了（见 stationfieldno_drift_report.xlsx），无条件覆盖会把真编号销毁。
                old_field_no = await conn.fetchval(
                    'SELECT "FieldNo" FROM locality1 WHERE "Locality1ID" = $1', data.localityId)

                status = await conn.execute(
                    '''
                    UPDATE locality1 SET
                        "FieldNo" = $2, "LocalityString" = $3, "Drainage" = $4, "WaterBody" = $5,
                        "Country" = $6, "Continent" = $7, "State" = $8, "County" = $9,
                        "Lat" = $10, "Lon" = $11,
                        "StartDate" = $12, "EndDate" = $13,
                        "VerbatimDate" = $14, "Remarks" = $15, "Inventory" = $16, "VerbatimCollectors" = $17,
                        "year" = $18, "month" = $19, "day" = $20,
                        "TimeStampModified" = NOW()
                    WHERE "Locality1ID" = $1
                    ''',
                    data.localityId, field_no, _blank(data.localityString), _blank(data.drainage),
                    _blank(data.waterbody), _blank(data.country), _blank(data.continent), _blank(data.state),
                    _blank(data.county),
                    _to_coord(data.latitude, "latitude", -90, 90),
                    _to_coord(data.longitude, "longitude", -180, 180),
                    start_ts, end_ts,
                    _blank(data.verbatimDate), _blank(data.remark), _blank(data.inventory),
                    _blank(data.verbatimCollectors),
                    year, month, day,
                )

                # 以前不检查影响行数：改一条不存在的产地也会返回成功
                if status.split()[-1] == "0":
                    raise HTTPException(status_code=404, detail=f"Locality {data.localityId} not found")

                if data.manageCollectors:
                    # 按列表整体重写（含 StationFieldNumber），删掉的人才会真的消失。
                    # RETURNING 是为了把删掉了什么打进日志——老数据里有 CollectorID 为 NULL 的
                    # 遗留行（界面显示不出来），会在这里被一并清掉，属于对历史数据的改动，要留痕。
                    removed = await conn.fetch(
                        'DELETE FROM "CollectorsLocality" WHERE "Locality1ID" = $1'
                        ' RETURNING "CollectorID", "StationFieldNumber"',
                        data.localityId)
                    collector_ids = _collector_ids(data.zCollectorsLocality)
                    if collector_ids:
                        await conn.execute(
                            '''
                            INSERT INTO "CollectorsLocality" ("StationFieldNumber", "CollectorID", "Locality1ID")
                            SELECT $1, c, $3 FROM unnest($2::int[]) AS c
                            ''',
                            field_no, collector_ids, data.localityId,
                        )
                    null_rows = sum(1 for r in removed if r["CollectorID"] is None)
                    if null_rows:
                        print(f"Locality {data.localityId}: collector rewrite dropped {null_rows} "
                              f"legacy row(s) with NULL CollectorID")
                    # 重写会把历史上不一致的 StationFieldNumber 一并抹成当前 FieldNo，留痕
                    overwritten = {r["StationFieldNumber"] for r in removed
                                   if r["StationFieldNumber"] is not None
                                   and r["StationFieldNumber"] != field_no}
                    if overwritten:
                        print(f"Locality {data.localityId}: collector rewrite overwrote "
                              f"StationFieldNumber {sorted(overwritten)!r} with {field_no!r}")
                elif field_no != old_field_no:
                    # FieldNo 真的改了才同步冗余副本。没改就别碰——那 10 条历史不一致的记录
                    # 要等 curator 裁决，不能因为一次无关的编辑就被悄悄抹平。
                    synced = await conn.fetch(
                        'UPDATE "CollectorsLocality" SET "StationFieldNumber" = $2'
                        ' WHERE "Locality1ID" = $1 AND "StationFieldNumber" IS DISTINCT FROM $2'
                        ' RETURNING "CollectorLocalityID"',
                        data.localityId, field_no,
                    )
                    if synced:
                        print(f"Locality {data.localityId}: FieldNo {old_field_no!r} -> {field_no!r}, "
                              f"synced StationFieldNumber on {len(synced)} collector row(s)")
    except HTTPException:
        raise
    except asyncpg.exceptions.UniqueViolationError:
        # 改成了另一条产地已占用的 FieldNo
        raise HTTPException(status_code=409, detail=f"Field No '{field_no}' already exists")
    except asyncpg.exceptions.ForeignKeyViolationError:
        raise HTTPException(status_code=400, detail="One of the selected collectors no longer exists")
    except Exception as e:
        print(f"Error updating locality {data.localityId}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    if warnings:
        print(f"Locality {data.localityId} updated with warnings: {'; '.join(warnings)}")

    return {"code": 20000,
            "data": {"items": {"Locality1ID": data.localityId}, "warnings": warnings, "total": 1}}


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
    # FieldNo 可为空，和 POST /locality 保持一致（现场不一定给得到）。
    # 原来声明成必填 str，而调用方 RecordsProcessor 会把空值整个删掉不发 -> 422。
    FieldNo: Optional[str] = None
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
    Lon: Optional[Union[str, float]] = None
    Lat: Optional[Union[str, float]] = None
    StartDate: Optional[Union[str, datetime]] = None
    EndDate: Optional[Union[str, datetime]] = None
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
        # 注意：ResponseModel 要求 data 必填，缺了会被 FastAPI 变成 500 ResponseValidationError，
        # 前端就永远看不到这里想返回的错误码/信息了。下面所有错误分支同理。
        return {
            "code": 50000,
            "data": {"message": f"Failed to check field number: {str(e)}"}
        }


# 创建新地点（简化版）
@router.post("/create", response_model=ResponseModel)
async def create_new_locality(locality_data: LocalityCreateModel):
    """
    创建新的地点记录
    """
    field_no = _blank(locality_data.FieldNo)  # '' -> NULL（UNIQUE 只允许一个 ''，但允许多个 NULL）
    try:
        # 检查FieldNo是否已存在（为空时跳过：多条无编号产地是允许的）
        if field_no is not None:
            check_result = await execute_query(
                'SELECT "Locality1ID" FROM locality1 WHERE "FieldNo" = $1', field_no)

            if check_result:
                return {
                    "code": 40900,
                    "message": f"Field No '{field_no}' already exists",
                    "data": {"existing_locality_id": check_result[0]["Locality1ID"]}
                }

        # 和 POST /locality 用同一个解析器：原来只认严格的 'YYYY-MM-DD'，
        # 前端哪天改成发带 Z 的 ISO 就会挂（date-picker 的默认行为就是发那个）。
        start_date = _to_ts(locality_data.StartDate, "StartDate")
        end_date = _to_ts(locality_data.EndDate, "EndDate")
        warnings = _date_warnings(start_date, end_date)

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
            field_no,
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
            _to_coord(locality_data.Lon, "Lon", -180, 180),
            _to_coord(locality_data.Lat, "Lat", -90, 90),
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
                "data": {"message": "Failed to create locality"}
            }

        new_locality = result[0]

        if warnings:
            print(f"Locality {new_locality['Locality1ID']} created with warnings: {'; '.join(warnings)}")

        return {
            "code": 20000,
            "data": {
                "Locality1ID": new_locality["Locality1ID"],
                "FieldNo": new_locality["FieldNo"],
                "LocalityString": new_locality["LocalityString"],
                "warnings": warnings,
                "message": f"Locality created successfully"
            }
        }
    except HTTPException:
        # _to_ts 抛的 400（日期格式）要原样上抛，别被下面吞成 50000
        raise
    except Exception as e:
        print(f"Error creating locality: {str(e)}")
        return {
            "code": 50000,
            "data": {"message": f"Failed to create locality: {str(e)}"}
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
                "data": {"message": f"Locality with ID {locality_id} not found"}
            }

        # 关联的采集人：编辑表单要能把已有的人显示出来（以前没查，表单永远只有一行空的，
        # 策展人在上面改了还会被静默丢弃）。名字一并带上，前端 el-select 才有 label 可显示。
        collectors = await execute_query(
            '''
            SELECT cl."CollectorID" AS "collectorID",
                   trim(both ' ' from concat_ws(' ', c."FirstName", c."LastName")) AS "collectorName"
            FROM "CollectorsLocality" cl
            LEFT JOIN "Collectors" c ON c."CollectorID" = cl."CollectorID"
            WHERE cl."Locality1ID" = $1 AND cl."CollectorID" IS NOT NULL
            ORDER BY cl."CollectorLocalityID"
            ''',
            locality_id,
        )

        return {
            "code": 20000,
            "data": {
                "items": result,
                "collectors": collectors,
                "total": 1
            }
        }
    except Exception as e:
        return {
            "code": 50000,
            "data": {"message": f"Failed to get locality details: {str(e)}"}
        }