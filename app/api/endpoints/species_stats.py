from fastapi import APIRouter, Query, Depends, HTTPException, status
from typing import Dict, Any, List

from app.db.database import execute_query

router = APIRouter()


@router.get("/speciesStats", response_model=Dict[str, Any])
async def get_scientific_statistic():
    """
    Get scientific statistics including catalog counts by scientific name, inventory, and storage.
    Mirrors the original getScientificStatistic function.
    """
    query = """
    SELECT finaljoin."FullScientificName", finaljoin."Inventory", finaljoin."Storage",
           count(*) as "CatalogCount" 
    FROM (
        SELECT * 
        FROM (
            ((SELECT p."PrimaryID" as "MainPrimaryID", p.* FROM "Primary" p) a 
            LEFT JOIN "Determination" d2 ON d2."PrimaryID" = a."PrimaryID" AND d2."IsCurrent" IS TRUE) 
            JOIN2 LEFT JOIN "TaxonomicTable" tt ON tt."TaxonID" = JOIN2."TaxonID"
        ) JOIN3 
        LEFT JOIN "Family" f ON f."FamilyID" = JOIN3."FamilyID"
    ) AS finaljoin 
    GROUP BY finaljoin."FullScientificName", finaljoin."Inventory", finaljoin."Storage" 
    ORDER BY "CatalogCount" DESC 
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


@router.get("/familyList", response_model=Dict[str, Any])
async def get_family_list():
    """
    Get family list with counts.
    Mirrors the original getFamilyList function.
    """
    query = """
    SELECT "FamilyName", SUM(count) AS count
    FROM taxonomy_counts_view
    GROUP BY "FamilyName"
    ORDER BY count DESC
    """

    records = await execute_query(query)

    return {
        "code": 20000,
        "data": {
            "items": records,
            "total": len(records)
        }
    }


@router.get("/genusList", response_model=Dict[str, Any])
async def get_genus_list(familyName: str):
    """
    Get genus list for a specific family with counts.
    Mirrors the original getGenusList function.
    """
    query = """
    SELECT "Genus", SUM(count) AS count
    FROM taxonomy_counts_view
    WHERE "FamilyName" = $1
    GROUP BY "Genus"
    ORDER BY count DESC
    """

    records = await execute_query(query, familyName)

    return {
        "code": 20000,
        "data": {
            "items": records,
            "total": len(records)
        }
    }


@router.get("/speciesList", response_model=Dict[str, Any])
async def get_species_list(familyName: str, genus: str):
    """
    Get species list for a specific family and genus with counts.
    Mirrors the original getSpeciesList function.
    """
    query = """
    SELECT "Species", count
    FROM taxonomy_counts_view
    WHERE "FamilyName" = $1 AND "Genus" = $2
    ORDER BY count DESC
    """

    records = await execute_query(query, familyName, genus)

    return {
        "code": 20000,
        "data": {
            "items": records,
            "total": len(records)
        }
    }


@router.get("/timeline", response_model=Dict[str, Any])
async def get_monthly_collection_timeline():
    """
    Get monthly collection timeline data.
    Mirrors the original getMonthlyCollectionTimeline function.
    """
    query = """
    SELECT year, month, count 
    FROM yearly_monthly_collection_counts_view 
    ORDER BY year, month
    """

    records = await execute_query(query)

    return {
        "code": 20000,
        "data": {
            "items": records,
            "total": len(records)
        }
    }