from datetime import datetime, date

from fastapi import APIRouter, Query, Depends, HTTPException, status
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field, validator

from app.db.database import execute_query, execute_mutation, execute_proc, execute_single_query, \
    execute_paginated_query_with_count
from app.services.filter_engine import FilterSpec, FieldDef, build_where, build_global_search, parse_json_param
from app.api.endpoints.person import PersonModel, new_loan_people as create_loan_person

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

    # frontend sends "" for blank quantity inputs -> coerce to None
    @validator("Quantity", "QuantityReturned", "QuantityResolved", pre=True)
    def _blank_qty_to_none(cls, v):
        return None if v in ("", None) else v


class LoanModel(BaseModel):
    loanId: Optional[int] = None
    loanNumber: str
    loanNumberNoType: Optional[str] = None
    transactionType: str
    loanDate: Optional[date] = None
    closed: bool = False
    dateClosed: Optional[date] = None   # proc p_dateclosed is DATE, not datetime
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

    # frontend sends "" for empty optionals -> coerce to None for int fields
    @validator("loanId", "loanPplID", pre=True)
    def _blank_int_to_none(cls, v):
        return None if v in ("", None) else v

    # closed may arrive as "YES"/"NO"/"" — blank means not closed
    @validator("closed", pre=True)
    def _blank_closed_to_false(cls, v):
        return False if v in ("", None) else v

    # frontend sends full ISO datetime (e.g. 2026-06-16T17:10:28.198Z); proc wants DATE
    @validator("loanDate", "dateClosed", pre=True)
    def _parse_to_date(cls, v):
        if v in ("", None):
            return None
        if isinstance(v, datetime):
            return v.date()
        if isinstance(v, str):
            s = v.strip().replace("Z", "+00:00")
            try:
                return datetime.fromisoformat(s).date()
            except ValueError:
                try:
                    return date.fromisoformat(v.strip()[:10])
                except ValueError:
                    return v
        return v


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


def _pdate(col: str) -> str:
    """把混合格式的文本日期解析成 date：'YYYY-MM-DD[ ...]' / 'M/D/YYYY' / 'M/D/YY'，解析不了为 NULL。
    loan_view 的 LoanDate/DateClosed 是 varying 且格式不统一（老数据），筛选/排序前需转换。"""
    return (
        "CASE "
        f"WHEN trim({col}) ~ '^[0-9]{{4}}-[0-9]{{1,2}}-[0-9]{{1,2}}' THEN to_date(left(trim({col}),10),'YYYY-MM-DD') "
        f"WHEN trim({col}) ~ '^[0-9]{{1,2}}/[0-9]{{1,2}}/[0-9]{{4}}$' THEN to_date(trim({col}),'FMMM/FMDD/YYYY') "
        f"WHEN trim({col}) ~ '^[0-9]{{1,2}}/[0-9]{{1,2}}/[0-9]{{2}}$' THEN to_date(trim({col}),'FMMM/FMDD/YY') "
        "ELSE NULL END"
    )


_LOAN_DATE_SQL = _pdate('lv."LoanDate"')
_DATE_CLOSED_SQL = _pdate('lv."DateClosed"')

# Loan 搜索字段注册表（复用 lots 的过滤引擎）。建在 loan_view 上，无需 join。
# 结果 DISTINCT 到借阅级（一行 = 一个 loan）；用标本/产地/分类列筛选 → 命中的借阅。
# 解析后的 LoanDateParsed/DateClosedParsed 放进 SELECT，才能在 DISTINCT 下按日期排序。
LOAN_SPEC = FilterSpec(
    base='loan_view lv',
    joins='',
    select=f'''
        DISTINCT lv."ID", lv."LoanNumber", lv."TransactionType",
        lv."LoanDate", lv."DateClosed", lv."Closed",
        lv."FullName", lv."LastName", lv."OrganizationID", lv."AgentID",
        lv."LoanAgents", lv."LoanPeopleID",
        {_LOAN_DATE_SQL} AS "LoanDateParsed",
        {_DATE_CLOSED_SQL} AS "DateClosedParsed"
    ''',
    order_by=f'{_LOAN_DATE_SQL} DESC NULLS LAST',
    fields={
        "loan_number":      FieldDef('lv."LoanNumber"', "text", "Loan #", "Loan"),
        "transaction_type": FieldDef('lv."TransactionType"', "enum", "Transaction Type", "Loan", options=["Loan", "Gift"]),
        "loan_person":      FieldDef('lv."FullName"', "text", "Loan Person", "Loan"),
        "last_name":        FieldDef('lv."LastName"', "text", "Last Name", "Loan"),
        "organization":     FieldDef('lv."OrganizationID"', "text", "Organization", "Loan"),
        "closed":           FieldDef('lv."Closed"', "enum", "Closed?", "Loan", options=["true", "false"]),
        "catalog_number":   FieldDef('lv."CatalogNumber"', "idlist", "Catalog No.", "Specimen"),
        "jar_size":         FieldDef('lv."JarSize"', "enum", "Jar Size", "Specimen"),  # 前端经 optionLoaders 加载真实选项
        "storage":          FieldDef('lv."Storage"', "text", "Storage", "Specimen"),
        "total_number":     FieldDef('lv."TotalNumber"', "number", "Total Number", "Specimen"),
        "scientific_name":  FieldDef('lv."FullScientificName"', "text", "Scientific Name", "Taxonomy"),
        "family":           FieldDef('lv."FamilyName"', "text", "Family", "Taxonomy"),
        "field_no":         FieldDef('lv."FieldNo"', "text", "Field No.", "Locality"),
        "locality_string":  FieldDef('lv."LocalityString"', "text", "Locality String", "Locality"),
        "state":            FieldDef('lv."LocalityState"', "text", "State", "Locality"),
        "county":           FieldDef('lv."LocalityCounty"', "text", "County", "Locality"),
        "drainage":         FieldDef('lv."Drainage"', "text", "Drainage", "Locality"),
        "loan_date":        FieldDef(_LOAN_DATE_SQL, "date", "Loan Date", "Dates"),
        "date_closed":      FieldDef(_DATE_CLOSED_SQL, "date", "Date Closed", "Dates"),
        "date_cataloged":   FieldDef('lv."DateCataloged"', "date", "Date Cataloged", "Dates"),
    },
    global_search_cols=[
        "loan_number", "scientific_name", "family", "catalog_number",
        "locality_string", "last_name", "organization", "field_no",
    ],
)

# 仅允许按「借阅级（在 SELECT DISTINCT 列表里的）」字段排序，否则 DISTINCT + ORDER BY 会报错。
_LOAN_SORTABLE = {
    "loan_number", "transaction_type", "loan_date", "date_closed",
    "loan_person", "last_name", "organization", "closed",
}


@router.get("/loanAdvanced", response_model=ResponseModel)
async def get_loan_advanced(
        search: Optional[str] = Query(None, description="全局模糊搜索"),
        ids: Optional[str] = Query(None, description="Catalog number 列表，逗号分隔"),
        field_filters: Optional[str] = Query(None, description='JSON: {api_name:[值/__EMPTY__/__NOT_EMPTY__]}'),
        structured_filters: Optional[str] = Query(None, description='JSON: [{"field":..,"op":..,"values":[..]}]'),
        sort_by: Optional[str] = Query(None, description="排序字段（借阅级白名单内）"),
        sort_order: Optional[str] = Query(None, description="asc | desc"),
        fuzzy_threshold: float = Query(0.4, ge=0.0, le=1.0),
        pagination: PaginationParams = Depends(),
):
    """借阅高级搜索（复用 lots 过滤引擎；结果 DISTINCT 到借阅级）。
    标本/产地/分类列做筛选（作用在 item 级行上）→ DISTINCT 后得到命中的借阅。"""
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

    search_param_pos = None  # 原始查询词在哪个 $n（build_global_search 的 p_q）；relevance 排序复用，不另占参数
    if search and str(search).strip():
        pos = p
        g_where, _rank, g_params, p = build_global_search(LOAN_SPEC, search, fuzzy_threshold, p)
        if g_where:
            where_clauses.append(g_where)
            params.extend(g_params)
            search_param_pos = pos

    w2, p2 = build_where(
        LOAN_SPEC,
        ids=id_list,
        field_filters=parse_json_param(field_filters),
        structured_filters=parse_json_param(structured_filters),
        start_param=p,
    )
    where_clauses.extend(w2)
    params.extend(p2)

    # 相关性排序：有全局搜索时，loan# 匹配越精确排越前（exact>prefix>contains），其次按日期。
    # 用 build_global_search 的 item 级 rank 在 DISTINCT 下不可行，故按 loan# (借阅级) 算 rel，可放进 SELECT。
    rel_sql = ""
    rel_order = ""
    if search_param_pos is not None:
        rp = search_param_pos  # 复用全局搜索的原始查询词参数（$rp），main 与 count 参数数一致
        rel_sql = (
            f', CASE WHEN lower(lv."LoanNumber") = lower(${rp}) THEN 3 '
            f"WHEN lv.\"LoanNumber\" ILIKE ${rp} || '%' THEN 2 "
            f"WHEN lv.\"LoanNumber\" ILIKE '%' || ${rp} || '%' THEN 1 ELSE 0 END AS \"_rel\""
        )
        rel_order = '"_rel" DESC, '

    where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    explicit_order = LOAN_SPEC.order_clause(sort_by, sort_order) if sort_by in _LOAN_SORTABLE else None
    order_by = explicit_order if explicit_order else (rel_order + f'{_LOAN_DATE_SQL} DESC NULLS LAST')

    main_query = f"SELECT {LOAN_SPEC.select}{rel_sql} FROM loan_view lv{where_sql} ORDER BY {order_by}"
    # 借阅级计数：COUNT(DISTINCT loan ID)，否则 total 会按 item 行数多算
    count_query = f'SELECT COUNT(DISTINCT lv."ID") FROM loan_view lv{where_sql}'

    return await execute_paginated_query_with_count(
        main_query=main_query,
        count_query=count_query,
        params=params,
        page=pagination.page,
        page_size=pagination.page_size,
    )


@router.get("/filter-metadata", response_model=ResponseModel)
async def get_loan_filter_metadata():
    """返回 loan 可过滤列清单（key/label/group/type/operators），驱动前端 chip 选择器。"""
    return {"code": 20000, "data": {"fields": LOAN_SPEC.to_metadata()}}

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


# The number scans live at module level so the regression tests can run them
# against a fixture. TransactionType casing in t2 is inconsistent ('Loan', 'LOAN',
# 'loan', 'LOAn', 'Gift', 'GIFT'), so match on upper() -- a case-sensitive match
# hides rows and hands out a number that is already in use.
LOAN_NUMBERS_QUERY = """
SELECT t2."LoanNumber" FROM t2
WHERE upper(t2."TransactionType") = 'LOAN'
AND t2."LoanNumber" LIKE extract(year from CURRENT_DATE) || '%'
"""

GIFT_NUMBERS_QUERY = """
SELECT t2."LoanNumber" FROM t2
WHERE upper(t2."TransactionType") = 'GIFT'
AND t2."LoanNumber" LIKE extract(year from CURRENT_DATE) || '%'
"""


@router.get("/newloan", response_model=ResponseModel)
async def generate_new_loan_id():
    """
    Generate a new loan ID.
    Mirrors the original generateNewLoanID function.
    """
    records = await execute_query(LOAN_NUMBERS_QUERY)

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


@router.get("/newgift", response_model=ResponseModel)
async def generate_new_gift_id():
    """
    Generate a new gift ID.
    Mirrors the original generateNewGiftID function.
    """
    records = await execute_query(GIFT_NUMBERS_QUERY)

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
async def new_loan_people(data: PersonModel):
    """
    Create a new loan person (loan / gift recipient).

    The person form posts here -- src/api/table.js maps addNewLoanPeople to
    'loan/loanpeople'. This route used to declare every field as a plain function
    scalar, which makes FastAPI read them as QUERY parameters: the JSON body was
    ignored and every submit failed with 422 "lastName field required", so no
    recipient was ever created. It now takes a body and delegates to the person
    module, which already had the correct implementation, so the two routes
    (/api/loan/loanpeople and /api/person/loanpeople) cannot drift apart.
    """
    return await create_loan_person(data)
