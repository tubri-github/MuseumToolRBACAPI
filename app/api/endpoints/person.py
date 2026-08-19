from fastapi import APIRouter, HTTPException, status
from typing import Dict, Any, List, Optional
from pydantic import BaseModel

from app.db.database import execute_query, execute_mutation

router = APIRouter()


class PersonModel(BaseModel):
    lastName: str
    firstName: Optional[str] = None
    middleName: Optional[str] = None
    groupName: Optional[str] = None
    title: Optional[str] = None
    abbreviation: Optional[str] = None
    institution: Optional[str] = None
    phone1: Optional[str] = None
    phone2: Optional[str] = None
    fax: Optional[str] = None
    email: Optional[str] = None
    jobTitle: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    postalCode: Optional[str] = None
    remarks: Optional[str] = None
    agentType: Optional[str] = None


class StaffModel(BaseModel):
    lastName: str
    firstName: str
    middleName: Optional[str] = None
    title: Optional[str] = None
    agentType: Optional[str] = None
    initials: str


class ResponseModel(BaseModel):
    code: int
    data: Dict[str, Any]


@router.get("/collectors/{keyword}", response_model=ResponseModel)
async def get_collectors(keyword: str):
    """
    Get collectors by keyword.
    Mirrors the original getCollectors function, returning all results.
    """
    query = """
    SELECT tt."CollectorID", tt."FirstName", tt."LastName" 
    FROM "Collectors" tt 
    ORDER BY similarity(tt."LastName", $1) DESC
    """

    records = await execute_query(query, keyword)

    return {
        "code": 20000,
        "data": {
            "items": records,
            "total": len(records)
        }
    }


@router.post("/collectors", response_model=ResponseModel)
async def new_collector(data: PersonModel):
    """
    Create a new collector.
    Mirrors the original newCollector function.
    """
    query = """
    INSERT INTO "Collectors" (
        "FirstName", "LastName", "MiddleName", "GroupName", "Title", "Abbreviation", 
        "Institution", "Phone1", "Phone2", "Fax", "Email", "JobTitle", 
        "Address", "City", "State", "Country", "PostalCode", "Remarks"
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
    RETURNING "CollectorID"
    """

    try:
        result = await execute_query(
            query,
            data.firstName,
            data.lastName,
            data.middleName,
            data.groupName,
            data.title,
            data.abbreviation,
            data.institution,
            data.phone1,
            data.phone2,
            data.fax,
            data.email,
            data.jobTitle,
            data.address,
            data.city,
            data.state,
            data.country,
            data.postalCode,
            data.remarks
        )

        return {
            "code": 20000,
            "data": {
                "items": result,
                "total": len(result) if result else 0
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/staff", response_model=ResponseModel)
async def get_staff():
    """
    Get staff members.
    Mirrors the original getStaff function, returning all results.
    """
    query = """
    SELECT CONCAT(c2."Name", 
           CASE WHEN c2."FirstName" IS NOT NULL 
                THEN concat('(', c2."FirstName", ' ', c2."LastName", ')') 
                ELSE '' 
           END) as "StaffName", 
           c2."SatffID" 
    FROM "Staff" c2
    """

    records = await execute_query(query)

    return {
        "code": 20000,
        "data": {
            "items": records,
            "total": len(records)
        }
    }


@router.post("/staff", response_model=ResponseModel)
async def new_staff(data: StaffModel):
    """
    Create a new staff member.
    Mirrors the original newStaff function.
    """
    query = """
    INSERT INTO "Staff" (
        "FirstName", "LastName", "MiddleInitial", "Title", "AgentType", "Name"
    )
    VALUES ($1, $2, $3, $4, $5, $6)
    RETURNING "SatffID"
    """

    try:
        # column order is FirstName, LastName -- these two used to be passed the
        # other way round, which stored every new staff member name-reversed
        result = await execute_query(
            query,
            data.firstName,
            data.lastName,
            data.middleName,
            data.title,
            data.agentType,
            data.initials
        )

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
    Mirrors the original getDeterminers function, returning all results.
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


@router.post("/determiner", response_model=ResponseModel)
async def new_determiner(data: PersonModel):
    """
    Create a new determiner.
    Mirrors the original newDeterminer function.
    """
    query = """
    INSERT INTO "Determiners" (
        "FirstName", "LastName", "MiddleName", "GroupName", "Title", "Abbreviation", 
        "Institution", "Phone1", "Phone2", "Fax", "Email", "JobTitle", 
        "Address", "City", "State", "Country", "PostalCode", "Remarks"
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
    RETURNING "DeterminerID"
    """

    try:
        result = await execute_query(
            query,
            data.firstName,
            data.lastName,
            data.middleName,
            data.groupName,
            data.title,
            data.abbreviation,
            data.institution,
            data.phone1,
            data.phone2,
            data.fax,
            data.email,
            data.jobTitle,
            data.address,
            data.city,
            data.state,
            data.country,
            data.postalCode,
            data.remarks
        )

        return {
            "code": 20000,
            "data": {
                "items": result,
                "total": len(result) if result else 0
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/loanpeople", response_model=ResponseModel)
async def get_loan_people():
    """
    Get loan people.
    Mirrors the original getLoanPeople function, returning all results.
    """
    query = """
    SELECT c2."FirstName", c2."LastName", c2."AgentID" as "LoanPeopleID" 
    FROM "LoanPeople" c2
    """

    records = await execute_query(query)

    return {
        "code": 20000,
        "data": {
            "items": records,
            "total": len(records)
        }
    }


@router.post("/loanpeople", response_model=ResponseModel)
async def new_loan_people(data: PersonModel):
    """
    Create a new loan person.
    Mirrors the original newLoanPeople function.
    """
    full_name = f"{data.firstName} {data.lastName}" if data.firstName else data.lastName

    query = """
    INSERT INTO "LoanPeople" (
        "FullName", "FirstName", "LastName", "MiddleName", "GroupName", "Title", 
        "Abbreviation", "Institution", "Phone1", "Phone2", "Fax", "Email", 
        "JobTitle", "Address", "City", "State", "Country", "PostalCode", "Remarks"
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
    RETURNING "AgentID"
    """

    try:
        result = await execute_query(
            query,
            full_name,
            data.firstName,
            data.lastName,
            data.middleName,
            data.groupName,
            data.title,
            data.abbreviation,
            data.institution,
            data.phone1,
            data.phone2,
            data.fax,
            data.email,
            data.jobTitle,
            data.address,
            data.city,
            data.state,
            data.country,
            data.postalCode,
            data.remarks
        )

        return {
            "code": 20000,
            "data": {
                "items": result,
                "total": len(result) if result else 0
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))