from datetime import datetime, date

from fastapi import APIRouter, Query, Depends, HTTPException, status
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field

from app.db.database import execute_query, execute_mutation, execute_proc, execute_single_query, \
    execute_paginated_query_with_count
from app.services.es_sync import handle_data_change

router = APIRouter()


class LoanItemModel(BaseModel):
    LoanItemID: Optional[str] = None
    PrimaryID: str
    Quantity: Optional[int] = None
    QuantityReturned: Optional[int] = None
    QuantityResolved: Optional[int] = None
    DescriptionOfMaterial: Optional[str] = None
    InComments: Optional[str] = None
    OutComments: Optional[str] = None
    Remarks: Optional[str] = None


class LoanModel(BaseModel):
    loanId: Optional[int] = None
    loanNumber: str
    loanNumberNoType: Optional[str] = None
    transactionType: str
    loanDate: Optional[date] = None
    closed: bool = False
    dateClosed: Optional[datetime] = None
    text1: Optional[str] = None
    text2: Optional[str] = None
    loanPplID: Optional[int] = None
    agentID: Optional[str] = None
    loanAgents: Optional[str] = None
    organizationID: Optional[str] = None
    shipToAddress: Optional[str] = None
    shipToCity: Optional[str] = None
    shipToState: Optional[str] = None
    shipToZipCode: Optional[str] = None
    shipToCountry: Optional[str] = None
    shipToRemark: Optional[str] = None
    shipToMethod: Optional[str] = None
    loanDetails: List[LoanItemModel]
    updatedLoanDetails: Optional[List[LoanItemModel]] = []
    deletedLoanDetails: Optional[List[LoanItemModel]] = []


class ResponseModel(BaseModel):
    code: int
    data: Dict[str, Any]


class PaginationParams:
    def __init__(
        self,
        page: int = Query(1, ge=1, description="页码，从1开始"),
        page_size: int = Query(10, ge=1, le=100, description="每页记录数")
    ):
        self.page = page
        self.page_size = page_size



@router.get("/loan/{loanid}", response_model=ResponseModel)
async def get_loan(loanid: str):
    """
    Get loan by loan ID.
    Mirrors the original getLoan function.
    """
    query = """
    SELECT * FROM loan_view lv 
    WHERE lv."LoanNumber" = $1
    """

    records = await execute_query(query, loanid)

    return {
        "code": 20000,
        "data": {
            "lots": records,
            "total": len(records)
        }
    }


@router.get("/loanAdvanced", response_model=ResponseModel)
async def get_loan_advanced(
        loanNumber: Optional[str] = None,
        ids: Optional[str] = None,
        localityId: Optional[str] = None,
        taxonId: Optional[str] = None,
        familyID: Optional[str] = None,
        loanPplID: Optional[str] = None,
        fieldNo: Optional[str] = None,
        jarSize: Optional[str] = None,
        storage: Optional[str] = None,
        inventory: Optional[str] = None,
        maxNumber: Optional[str] = None,
        minNumber: Optional[str] = None,
        loanOpenStartDate: Optional[str] = None,
        loanOpenEndDate: Optional[str] = None,
        loanClosedStartDate: Optional[str] = None,
        loanClosedEndDate: Optional[str] = None,
        catalogStartDate: Optional[str] = None,
        catalogEndDate: Optional[str] = None,
        pagination: PaginationParams = Depends(),
):
    """
    Get loans with advanced filtering.
    Mirrors the original getLoanAdvanced function.
    """
    sql = """
    SELECT DISTINCT "ID", "FullName", "AgentID", "OrganizationID", "TransactionType", 
           "LoanDate", "DateClosed", "LoanNumber" 
    FROM loan_view lv 
    WHERE (1=1)
    """

    # Process query parameters
    params = []
    param_index = 1

    # Process IDs if provided
    if ids and ids != "":
        id_list = [int(id_str) for id_str in ids.split(',') if id_str]
        if id_list:
            sql += f" AND (lv.\"CatalogNumber\" = ANY(${param_index}::int[]))"
            params.append(id_list)
            param_index += 1

    # Add other filters
    if loanNumber:
        sql += f" AND (TRIM(lv.\"LoanNumber\") = '{loanNumber}')"

    if loanPplID:
        sql += f" AND (lv.\"LoanPeopleID\" = '{loanPplID}')"

    if loanOpenStartDate:
        sql += f" AND (lv.\"LoanDate\" >= '{loanOpenStartDate}')"

    if loanOpenEndDate:
        sql += f" AND (lv.\"LoanDate\" <= '{loanOpenEndDate}')"

    if loanClosedStartDate:
        sql += f" AND (lv.\"DateClosed\" >= '{loanClosedStartDate}')"

    if loanClosedEndDate:
        sql += f" AND (lv.\"DateClosed\" <= '{loanClosedEndDate}')"

    if jarSize:
        sql += f" AND (lv.\"JarSize\" = '{jarSize}')"

    if storage:
        sql += f" AND (lv.\"Storage\" = '{storage}')"

    if inventory:
        sql += f" AND (lv.\"Inventory\" = '{inventory}')"

    if maxNumber:
        sql += f" AND (lv.\"TotalNumber\" <= {maxNumber})"

    if minNumber:
        sql += f" AND (lv.\"TotalNumber\" >= {minNumber})"

    if localityId:
        sql += f" AND (lv.\"Locality1ID\" = {localityId})"

    if catalogStartDate:
        sql += f" AND (lv.\"DateCataloged\" >= '{catalogStartDate}')"

    if catalogEndDate:
        sql += f" AND (lv.\"DateCataloged\" <= '{catalogEndDate}')"

    if fieldNo:
        sql += f" AND lv.\"FieldNo\" ~* '{fieldNo}'"

    if taxonId:
        sql += f" AND (lv.\"TaxonID\" = '{taxonId}')"

    if familyID:
        sql += f" AND (lv.\"FamilyID\" = '{familyID}')"

    # 添加排序
    sql += " ORDER BY \"LoanDate\" DESC"

    # 构建计数查询
    base_sql = sql.split(' ORDER BY ')[0]  # 去掉 ORDER BY 子句
    count_sql = f"SELECT COUNT(*) FROM ({base_sql}) AS count_query"

    # 使用您现有的函数执行分页查询和计数
    return await execute_paginated_query_with_count(
        main_query=sql,
        count_query=count_sql,
        params=params,
        page=pagination.page,
        page_size=pagination.page_size
    )

@router.get("/loanpeople", response_model=ResponseModel)
async def get_loan_people():
    """
    Get loan people.
    Mirrors the original getLoanPeople function.
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


@router.get("/newloan", response_model=ResponseModel)
async def generate_new_loan_id():
    """
    Generate a new loan ID.
    Mirrors the original generateNewLoanID function.
    """
    query = """
    SELECT t2."LoanNumber" FROM t2 
    WHERE t2."TransactionType" LIKE 'Loan' 
    AND t2."LoanNumber" LIKE extract(year from CURRENT_DATE) || '%'
    """

    records = await execute_query(query)

    max_num = 1
    curr_year = await execute_single_query("SELECT extract(year from CURRENT_DATE) as year")
    curr_year = int(curr_year["year"])

    if records and len(records) > 0:
        # Extract numbers from loan numbers
        numbers = []
        for record in records:
            loan_num = record["LoanNumber"]
            try:
                # Extract the number part (e.g., '001' from '2023-001L')
                num_part = loan_num.split('-')[1][:3]
                numbers.append(int(num_part))
            except (IndexError, ValueError):
                continue

        if numbers:
            max_num = max(numbers) + 1

    new_loan_number = f"{curr_year}-{str(max_num).zfill(3)}"

    return {
        "code": 20000,
        "data": {
            "LoanNumberNoType": new_loan_number
        }
    }


@router.get("/newGift", response_model=ResponseModel)
async def generate_new_gift_id():
    """
    Generate a new gift ID.
    Mirrors the original generateNewGiftID function.
    """
    query = """
    SELECT t2."LoanNumber" FROM t2 
    WHERE t2."TransactionType" LIKE 'Gift' 
    AND t2."LoanNumber" LIKE extract(year from CURRENT_DATE) || '%'
    """

    records = await execute_query(query)

    max_num = 1
    curr_year = await execute_single_query("SELECT extract(year from CURRENT_DATE) as year")
    curr_year = int(curr_year["year"])

    if records and len(records) > 0:
        # Extract numbers from gift numbers
        numbers = []
        for record in records:
            gift_num = record["LoanNumber"]
            try:
                # Extract the number part (e.g., '001' from '2023-001G')
                num_part = gift_num.split('-')[1][:3]
                numbers.append(int(num_part))
            except (IndexError, ValueError):
                continue

        if numbers:
            max_num = max(numbers) + 1

    new_gift_number = f"{curr_year}-{str(max_num).zfill(3)}"

    return {
        "code": 20000,
        "data": {
            "GiftNumberNoType": new_gift_number
        }
    }


@router.post("/loan", response_model=ResponseModel)
async def new_loan(data: LoanModel):
    """
    Create a new loan.
    Mirrors the original newLoan function.
    """
    try:
        # Convert loan details to JSON
        loan_details = []
        for item in data.loanDetails:
            loan_details.append({
                "PrimaryID": item.PrimaryID,
                "Quantity": item.Quantity,
                "QuantityReturned": item.QuantityReturned,
                "QuantityResolved": item.QuantityResolved,
                "DescriptionOfMaterial": item.DescriptionOfMaterial,
                "InComments": item.InComments,
                "OutComments": item.OutComments,
                "Remarks": item.Remarks
            })

        # Call the stored procedure
        await execute_proc(
            "add_loan_procedure",
            data.loanNumber,
            data.transactionType,
            data.loanDate if data.loanDate else None,
            data.closed,
            data.dateClosed if data.dateClosed else None,
            data.text1,
            data.text2,
            data.loanPplID if data.loanPplID else None,
            data.agentID,
            data.loanAgents,
            data.organizationID,
            data.shipToAddress,
            data.shipToCity,
            data.shipToState,
            data.shipToZipCode,
            data.shipToCountry,
            data.shipToRemark,
            data.shipToMethod,
            loan_details
        )

        # Sync the updated data to Elasticsearch
        await handle_data_change(f"t2", data.loanId, "INSERT")

        return {
            "code": 20000,
             "data": {
                "items": {
                    "loanNumber": data.loanNumber
                },
                "total": 1
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/uploan", response_model=ResponseModel)
async def update_loan(data: LoanModel):
    """
    Update a loan.
    Mirrors the original updateLoan function.
    """
    try:
        # Convert loan details to JSON
        loan_details = []
        for item in data.loanDetails:
            loan_details.append({
                "loanId": data.loanId,
                "LoanItemID": item.LoanItemID,
                "PrimaryID": item.PrimaryID,
                "Quantity": item.Quantity,
                "QuantityReturned": item.QuantityReturned,
                "QuantityResolved": item.QuantityResolved,
                "DescriptionOfMaterial": item.DescriptionOfMaterial,
                "InComments": item.InComments,
                "OutComments": item.OutComments,
                "Remarks": item.Remarks
            })

        # Call the stored procedure
        await execute_proc(
            "update_loan_procedure",
            data.loanId,
            data.loanNumber,
            data.transactionType,
            data.loanDate if data.loanDate else None,
            data.closed,
            data.dateClosed if data.dateClosed else None,
            data.text1,
            data.text2,
            data.loanPplID if data.loanPplID else None,
            data.agentID,
            data.loanAgents,
            data.organizationID,
            data.shipToAddress,
            data.shipToCity,
            data.shipToState,
            data.shipToZipCode,
            data.shipToCountry,
            data.shipToRemark,
            data.shipToMethod,
            loan_details
        )

        # Sync the updated data to Elasticsearch
        # await handle_data_change(f"t2", data.loanId, "UPDATE")

        return {
            "code": 20000,
             "data": {
                "items": {
                    "loanNumber": data.loanNumber
                },
                "total": 1
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/loancount/{year}", response_model=ResponseModel)
async def get_loan_numbers_by_year(year: str):
    """
    Get loan count by year.
    Mirrors the original getLoanNumbersByYear function.
    """
    date_value = datetime.strptime(f"{year}-01-01", "%Y-%m-%d").date()

    query = """
    SELECT * FROM t2 
    WHERE t2."LoanDate" IS NOT NULL 
    AND t2."LoanDate"::timestamp >= $1::timestamp
    """

    records = await execute_query(query, date_value)

    return {
        "code": 20000,
        "data": {
            "total": len(records)
        }
    }


@router.post("/loanpeople", response_model=ResponseModel)
async def new_loan_people(
        lastName: str,
        firstName: Optional[str] = None,
        middleName: Optional[str] = None,
        groupName: Optional[str] = None,
        title: Optional[str] = None,
        abbreviation: Optional[str] = None,
        institution: Optional[str] = None,
        phone1: Optional[str] = None,
        phone2: Optional[str] = None,
        fax: Optional[str] = None,
        email: Optional[str] = None,
        jobTitle: Optional[str] = None,
        address: Optional[str] = None,
        city: Optional[str] = None,
        state: Optional[str] = None,
        country: Optional[str] = None,
        postalCode: Optional[str] = None,
        remarks: Optional[str] = None,
        agentType: Optional[str] = None
):
    """
    Create a new loan person.
    Mirrors the original newLoanPeople function.
    """
    full_name = f"{firstName} {lastName}" if firstName else lastName

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
            firstName,
            lastName,
            middleName,
            groupName,
            title,
            abbreviation,
            institution,
            phone1,
            phone2,
            fax,
            email,
            jobTitle,
            address,
            city,
            state,
            country,
            postalCode,
            remarks
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