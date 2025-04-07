from fastapi import APIRouter, Query, Depends, HTTPException, status
from typing import Dict, Any, List, Optional
from pydantic import BaseModel

from app.db.database import execute_query, execute_mutation
from app.services.es_sync import handle_data_change

router = APIRouter()


class TaxonModel(BaseModel):
    genus: str
    species: str
    subspecies: Optional[str] = None
    remarks: Optional[str] = None
    fullScientificName: str
    familyID: Optional[int] = None


class ResponseModel(BaseModel):
    code: int
    data: Dict[str, Any]


@router.get("/taxons/{keyword}", response_model=ResponseModel)
async def get_taxons(keyword: str):
    """
    Get taxa by keyword.
    Mirrors the original getTaxons function.
    """
    query = """
    SELECT tt."TaxonID", tt."FullScientificName" 
    FROM "TaxonomicTable" tt 
    ORDER BY similarity(tt."FullScientificName", $1) DESC
    """

    records = await execute_query(query, keyword)

    return {
        "code": 20000,
        "data": {
            "items": records,
            "total": len(records)
        }
    }


@router.get("/familysearch/{keyword}", response_model=ResponseModel)
async def get_family(keyword: str):
    """
    Get families by keyword.
    Mirrors the original getFamily function.
    """
    query = """
    SELECT tt."FamilyID", tt."FamilyName" 
    FROM "Family" tt 
    ORDER BY similarity(tt."FamilyName", $1) DESC
    """

    records = await execute_query(query, keyword)

    return {
        "code": 20000,
        "data": {
            "items": records,
            "total": len(records)
        }
    }


@router.get("/determination", response_model=ResponseModel)
async def get_determinations():
    """
    Get determinations.
    Mirrors the original getDeterminations function.
    """
    query = """
    SELECT * FROM "Determination" d 
    JOIN "TaxonomicTable" tt ON d."TaxonID" = tt."TaxonID"
    LIMIT 100
    """

    records = await execute_query(query)

    return {
        "code": 20000,
        "data": {
            "items": records,
            "total": len(records)
        }
    }


@router.post("/taxon", response_model=ResponseModel)
async def new_taxon(data: TaxonModel):
    """
    Create a new taxon.
    Mirrors the original newTaxon function.
    """
    query = """
    INSERT INTO "TaxonomicTable" (
        "Genus", "Species", "Subspecies", "Remarks", "FullScientificName", "FamilyID"
    )
    VALUES ($1, $2, $3, $4, $5, $6)
    RETURNING "TaxonID"
    """

    try:
        result = await execute_query(
            query,
            data.genus,
            data.species,
            data.subspecies,
            data.remarks,
            data.fullScientificName,
            data.familyID
        )

        # Sync the new data to Elasticsearch
        if result and len(result) > 0:
            taxon_id = result[0]["TaxonID"]
            await handle_data_change("TaxonomicTable", taxon_id, "INSERT")

        return {
            "code": 20000,
            "data": {
                "items": result,
                "total": len(result) if result else 0
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/determiners", response_model=ResponseModel)
async def get_determiners():
    """
    Get determiners.
    Mirrors the original getDeterminers function.
    """
    query = """
    SELECT c2."DeterminerID", c2."GroupName" as "DeterminerName" 
    FROM "Determiners" c2
    """

    records = await execute_query(query)

    return {
        "code": 20000,
        "data": {
            "items": records,
            "total": len(records)
        }
    }