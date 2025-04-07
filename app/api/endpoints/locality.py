from datetime import datetime

from fastapi import APIRouter, Query, Depends, HTTPException, status
from typing import Dict, Any, List, Optional
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