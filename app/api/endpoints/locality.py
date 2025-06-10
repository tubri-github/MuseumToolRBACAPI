from datetime import datetime, date

from fastapi import APIRouter, Query, Depends, HTTPException, status
from typing import Dict, Any, List, Optional, Union
from pydantic import BaseModel

from app.db.database import execute_query, execute_mutation
from app.services.es_sync import handle_data_change

router = APIRouter()


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
        fieldNo: Optional[str] = None,
        limit: Optional[int] = 100
):
    """
    Get localities with advanced filtering.
    Mirrors the original getLocalityAdvanced function.
    """
    if not fieldNo or fieldNo == "":
        query = """
        SELECT * FROM locality1 LIMIT $1
        """
        records = await execute_query(query, limit)
    else:
        query = """
        SELECT * FROM locality1 l 
        WHERE l."FieldNo" ~* $1
        LIMIT $2
        """
        records = await execute_query(query, fieldNo, limit)

    return {
        "code": 20000,
        "data": {
            "items": records,
            "total": len(records)
        }
    }


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
            l."IslandGroup" as "Island Group",
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