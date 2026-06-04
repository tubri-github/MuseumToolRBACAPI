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


class CreateFamilyModel(BaseModel):
    family_name: str
    family_number: Optional[str] = None
    alias2: Optional[str] = None


class ResponseModel(BaseModel):
    code: int
    data: Dict[str, Any]


@router.get("/taxons/{keyword}", response_model=ResponseModel)
async def get_taxons(keyword: str):
    """
    Get taxa by keyword. Returns:
      1. TaxonomicTable rows joined with Family (so frontend has FamilyName,
         Genus, Species — enough to identify family-level placeholders).
      2. Fallback: Family-table-only matches with TaxonID=None. Frontend
         treats these as "virtual" family-level options and applies them
         via apply_family_taxon (find-or-create placeholder taxon).
    """
    # 1. Main query: TaxonomicTable + Family JOIN, filter by keyword
    taxon_query = """
    SELECT
        tt."TaxonID",
        tt."FullScientificName",
        tt."Genus",
        tt."Species",
        tt."Subspecies",
        tt."FamilyID",
        fam."FamilyName",
        tt."Remarks"
    FROM "TaxonomicTable" tt
    LEFT JOIN "Family" fam ON tt."FamilyID" = fam."FamilyID"
    WHERE LOWER(tt."FullScientificName") LIKE LOWER('%' || $1 || '%')
       OR LOWER(COALESCE(fam."FamilyName", '')) LIKE LOWER('%' || $1 || '%')
    ORDER BY similarity(tt."FullScientificName", $1) DESC
    LIMIT 50
    """
    taxon_records = await execute_query(taxon_query, keyword)

    # 2. Family fallback: skip families that already have a family-level
    # placeholder in the main results (Genus & Species both empty), so we
    # don't duplicate. Families that only have genus/species rows are still
    # surfaced as a virtual family-level option.
    families_with_placeholder = set()
    for r in taxon_records:
        if r["FamilyID"] is None:
            continue
        no_genus = not r["Genus"] or str(r["Genus"]).strip() == ""
        no_species = not r["Species"] or str(r["Species"]).strip() == ""
        if no_genus and no_species:
            families_with_placeholder.add(r["FamilyID"])

    family_query = """
    SELECT "FamilyID", "FamilyName"
    FROM "Family"
    WHERE LOWER("FamilyName") LIKE LOWER('%' || $1 || '%')
      AND NOT ("FamilyID" = ANY($2::int[]))
    ORDER BY similarity("FamilyName", $1) DESC
    LIMIT 20
    """
    family_records = await execute_query(
        family_query, keyword, list(families_with_placeholder)
    )

    # 3. Build unified items list. Virtual entries (TaxonID=None) signal
    # the frontend to call apply_family_taxon on selection.
    items = [dict(r) for r in taxon_records]
    for f in family_records:
        items.append({
            "TaxonID": None,
            "FullScientificName": f["FamilyName"],
            "Genus": None,
            "Species": None,
            "Subspecies": None,
            "FamilyID": f["FamilyID"],
            "FamilyName": f["FamilyName"],
            "Remarks": None,
        })

    return {
        "code": 20000,
        "data": {
            "items": items,
            "total": len(items)
        }
    }


@router.post("/family", response_model=ResponseModel)
async def create_family(payload: CreateFamilyModel):
    """
    Create a new Family record. Used when a verbatim family name is not yet
    in the Family table (e.g., during batch review of a family-only record).

    Does NOT create a TaxonomicTable placeholder row here — that is created
    lazily by `apply_family_taxon` when the reviewer actually applies the
    family to a record. Keeps responsibilities separated.
    """
    family_name = payload.family_name.strip() if payload.family_name else ""
    if not family_name:
        return {
            "code": 40000,
            "data": {},
            "message": "Family name is required"
        }

    # Duplicate check (case/whitespace insensitive)
    check_query = """
    SELECT "FamilyID", "FamilyName"
    FROM "Family"
    WHERE LOWER(TRIM("FamilyName")) = LOWER($1)
    LIMIT 1
    """
    existing = await execute_query(check_query, family_name)
    if existing:
        return {
            "code": 40900,
            "data": {
                "family_id": existing[0]["FamilyID"],
                "family_name": existing[0]["FamilyName"]
            },
            "message": f"Family '{existing[0]['FamilyName']}' already exists"
        }

    insert_query = """
    INSERT INTO "Family" ("FamilyName", "FamilyNumber", "Alias2", created_at, created_via)
    VALUES ($1, $2, $3, NOW(), 'reviewer_add')
    RETURNING "FamilyID", "FamilyName", "FamilyNumber", "Alias2"
    """
    family_number = payload.family_number.strip() if payload.family_number else None
    alias2 = payload.alias2.strip() if payload.alias2 else None
    result = await execute_query(insert_query, family_name, family_number, alias2)

    if not result:
        raise HTTPException(status_code=500, detail="Failed to create family record")

    return {
        "code": 20000,
        "data": {
            "family_id": result[0]["FamilyID"],
            "family_name": result[0]["FamilyName"],
            "family_number": result[0]["FamilyNumber"],
            "alias2": result[0]["Alias2"]
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
    WHERE tt."FamilyName" IS NOT NULL
      AND (tt."FamilyName" ILIKE '%'||$1||'%' OR similarity(tt."FamilyName", $1) > 0.2)
    ORDER BY (tt."FamilyName" ILIKE $1||'%') DESC, similarity(tt."FamilyName", $1) DESC
    LIMIT 50
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
        "Genus", "Species", "Subspecies", "Remarks", "FullScientificName", "FamilyID",
        created_at, created_via
    )
    VALUES ($1, $2, $3, $4, $5, $6, NOW(), 'reviewer_create')
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