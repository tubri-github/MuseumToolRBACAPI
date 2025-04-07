from fastapi import APIRouter, Query, Depends, HTTPException, status, Body
from typing import Optional, List, Dict, Any
from pydantic import BaseModel

import asyncio

from app.db.elasticsearch import get_es_client
from app.services.search import search_ulm, search_ost, search_locality, search_taxon, search_loan, search_lots, dsl_from_filters, search_generic
from app.services.es_sync import sync_all_data, sync_unified_data

router = APIRouter(prefix="/search")

class SearchResponse(BaseModel):
    code: int
    data: Dict[str, Any]

@router.get("/ulm", response_model=SearchResponse)
async def search_ulm_endpoint(
    query: Optional[str] = None,
    ids: Optional[str] = None,
    family: Optional[str] = None,
    genus: Optional[str] = None,
    species: Optional[str] = None,
    jarSize: Optional[str] = None,
    minNum: Optional[int] = None,
    maxNum: Optional[int] = None,
    typeStatus: Optional[str] = None,
    country: Optional[str] = None,
    waterbody: Optional[str] = None,
    drainage: Optional[str] = None,
    locality: Optional[str] = None,
    collector: Optional[str] = None,
    reviewrequired: Optional[str] = None,
    startdate: Optional[str] = None,
    enddate: Optional[str] = None,
    dataset: Optional[str] = None,
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100)
):
    """
    Search ULM data with optional filtering.
    """
    # Process IDs if provided
    id_list = None
    if ids and ids != "":
        id_list = [id_str for id_str in ids.split(',') if id_str]

    # Build filter object
    filters = {
        "ids": id_list,
        "Family": family,
        "genus": genus,
        "species": species,
        "JarSize": jarSize,
        "minNum": minNum,
        "maxNum": maxNum,
        "TypeStatus": typeStatus,
        "country": country,
        "waterbody": waterbody,
        "drainage": drainage,
        "Location": locality,
        "collectorname": collector,
        "recheckrequired": reviewrequired,
        "startdate": startdate,
        "enddate": enddate,
        "dataset": dataset
    }

    # Remove None values
    filters = {k: v for k, v in filters.items() if v is not None}

    result = await search_ulm(query, filters, page, limit)
    return result

@router.get("/ost", response_model=SearchResponse)
async def search_ost_endpoint(
    query: Optional[str] = None,
    ostcatalog: Optional[str] = None,
    tucatalog: Optional[str] = None,
    type: Optional[str] = None,
    inventory: Optional[str] = None,
    scientificname: Optional[str] = None,
    fieldnumber: Optional[str] = None,
    collector: Optional[str] = None,
    min_tl: Optional[float] = None,
    max_tl: Optional[float] = None,
    min_sl: Optional[float] = None,
    max_sl: Optional[float] = None,
    min_fl: Optional[float] = None,
    max_fl: Optional[float] = None,
    min_gm: Optional[float] = None,
    max_gm: Optional[float] = None,
    recheckedrequried: Optional[str] = None,
    reviewer: Optional[str] = None,
    taxonid: Optional[int] = None,
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100)
):
    """
    Search OST data with optional filtering.
    """
    # Build filter object
    filters = {
        "ostcatalog": ostcatalog,
        "tucatalog": tucatalog,
        "type": type,
        "inventory": inventory,
        "scientificname": scientificname,
        "fieldnumber": fieldnumber,
        "collector": collector,
        "recheckedrequried": recheckedrequried,
        "reviewer": reviewer,
        "taxonid": taxonid
    }

    # Handle range filters for measurements
    if min_tl is not None or max_tl is not None:
        filters["tl"] = {"min": min_tl, "max": max_tl}

    if min_sl is not None or max_sl is not None:
        filters["sl"] = {"min": min_sl, "max": max_sl}

    if min_fl is not None or max_fl is not None:
        filters["fl"] = {"min": min_fl, "max": max_fl}

    if min_gm is not None or max_gm is not None:
        filters["gm"] = {"min": min_gm, "max": max_gm}

    # Remove None values
    filters = {k: v for k, v in filters.items() if v is not None}

    result = await search_ost(query, filters, page, limit)
    return result

@router.get("/lots", response_model=SearchResponse)
async def search_lots_endpoint(
    query: Optional[str] = None,
    ids: Optional[str] = None,
    localityID: Optional[int] = None,
    taxonId: Optional[int] = None,
    familyID: Optional[int] = None,
    fieldNo: Optional[str] = None,
    jarSize: Optional[str] = None,
    storage: Optional[str] = None,
    inventory: Optional[str] = None,
    minNum: Optional[int] = None,
    maxNum: Optional[int] = None,
    startDate: Optional[str] = None,
    endDate: Optional[str] = None,
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100)
):
    """
    Search Lots data with optional filtering.
    """
    # Process IDs if provided
    id_list = None
    if ids and ids != "":
        id_list = [int(id_str) for id_str in ids.split(',') if id_str]

    # Build filter object
    filters = {
        "ids": id_list,
        "localityID": localityID,
        "taxonId": taxonId,
        "familyID": familyID,
        "fieldNo": fieldNo,
        "jarSize": jarSize,
        "Storage": storage,
        "Inventory": inventory,
        "minNumber": minNum,
        "maxNumber": maxNum,
        "startDate": startDate,
        "endDate": endDate
    }

    # Remove None values
    filters = {k: v for k, v in filters.items() if v is not None}

    # Use locality search with lots filter
    result = await search_lots(query, filters, page, limit)
    return result

@router.get("/loans", response_model=SearchResponse)
async def search_loans_endpoint(
    query: Optional[str] = None,
    loanNumber: Optional[str] = None,
    ids: Optional[str] = None,
    localityID: Optional[int] = None,
    taxonId: Optional[int] = None,
    familyID: Optional[int] = None,
    loanPplID: Optional[int] = None,
    fieldNo: Optional[str] = None,
    jarSize: Optional[str] = None,
    storage: Optional[str] = None,
    inventory: Optional[str] = None,
    minNumber: Optional[int] = None,
    maxNumber: Optional[int] = None,
    loanOpenStartDate: Optional[str] = None,
    loanOpenEndDate: Optional[str] = None,
    loanClosedStartDate: Optional[str] = None,
    loanClosedEndDate: Optional[str] = None,
    catalogStartDate: Optional[str] = None,
    catalogEndDate: Optional[str] = None,
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100)
):
    """
    Search Loan data with optional filtering.
    """
    # Process IDs if provided
    id_list = None
    if ids and ids != "":
        id_list = [int(id_str) for id_str in ids.split(',') if id_str]

    # Build filter object
    filters = {
        "loanNumber": loanNumber,
        "ids": id_list,
        "localityID": localityID,
        "taxonId": taxonId,
        "familyID": familyID,
        "loanPeopleID": loanPplID,
        "fieldNo": fieldNo,
        "jarSize": jarSize,
        "storage": storage,
        "inventory": inventory,
        "minNumber": minNumber,
        "maxNumber": maxNumber,
        "loanOpenStartDate": loanOpenStartDate,
        "loanOpenEndDate": loanOpenEndDate,
        "loanClosedStartDate": loanClosedStartDate,
        "loanClosedEndDate": loanClosedEndDate,
        "collectStartDate": catalogStartDate,
        "collectEndDate": catalogEndDate
    }

    # Remove None values
    filters = {k: v for k, v in filters.items() if v is not None}

    result = await search_loan(query, filters, page, limit)
    return result

@router.get("/locality", response_model=SearchResponse)
async def search_locality_endpoint(
    query: Optional[str] = None,
    fieldNo: Optional[str] = None,
    country: Optional[str] = None,
    continent: Optional[str] = None,
    state: Optional[str] = None,
    county: Optional[str] = None,
    drainage: Optional[str] = None,
    waterbody: Optional[str] = None,
    inventory: Optional[str] = None,
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100)
):
    """
    Search Locality data with optional filtering.
    """
    # Build filter object
    filters = {
        "fieldNo": fieldNo,
        "Country": country,
        "Continent": continent,
        "State": state,
        "County": county,
        "Drainage": drainage,
        "WaterBody": waterbody,
        "Inventory": inventory
    }

    # Remove None values
    filters = {k: v for k, v in filters.items() if v is not None}

    result = await search_locality(query, filters, page, limit)
    return result

@router.get("/taxon", response_model=SearchResponse)
async def search_taxon_endpoint(
    query: Optional[str] = None,
    genus: Optional[str] = None,
    species: Optional[str] = None,
    subspecies: Optional[str] = None,
    familyName: Optional[str] = None,
    familyID: Optional[int] = None,
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100)
):
    """
    Search Taxonomic data with optional filtering.
    """
    # Build filter object
    filters = {
        "Genus": genus,
        "Species": species,
        "Subspecies": subspecies,
        "FamilyName": familyName,
        "FamilyID": familyID
    }

    # Remove None values
    filters = {k: v for k, v in filters.items() if v is not None}

    result = await search_taxon(query, filters, page, limit)
    return result

@router.post("/sync", response_model=Dict[str, str])
async def sync_data_endpoint(force_full_sync: bool = False):
    """
    Trigger a synchronization from PostgreSQL to Elasticsearch.
    """
    await sync_all_data(force_full_sync)
    return {"message": "Data synchronization completed successfully"}

@router.post("/unifiedsync", response_model=Dict[str, str])
async def sync_unifieddata_endpoint(force_full_sync: bool = False):
    """
    Trigger a synchronization from PostgreSQL to Elasticsearch.
    """
    await sync_unified_data(force_full_sync)
    return {"message": "Data synchronization completed successfully"}

class UnifiedSearchResponse(BaseModel):
    code: int
    data: Dict[str, Any]
# @router.post("/unified")
# async def unified_search(
#     body: Dict[str, Any] = Body(...)
# ) -> Dict[str, Any]:
#     query = body.get("query", "")
#     filters = body.get("filters", [])  # expects List[{field, op, value}]
#     limit = body.get("limit", 5)
#     pages = body.get("pages", {})
#
#     page_lots = pages.get("lots", 1)
#     page_locality = pages.get("locality", 1)
#     page_loan = pages.get("loan", 1)
#     page_taxon = pages.get("taxon", 1)
#
#     dsl_filters = dsl_from_filters(filters)
#
#     results = await asyncio.gather(
#         search_generic(index="lots", query=query, filters=dsl_filters, page=page_lots, limit=limit),
#         search_generic(index="locality", query=query, filters=dsl_filters, page=page_locality, limit=limit),
#         search_generic(index="loan", query=query, filters=dsl_filters, page=page_loan, limit=limit),
#         search_generic(index="taxon", query=query, filters=dsl_filters, page=page_taxon, limit=limit),
#     )
#
#     lots_result, locality_result, loan_result, taxon_result = results
#
#     return {
#         "code": 20000,
#         "data": {
#             "lots": {
#                 "total": lots_result["data"]["total"],
#                 "page": page_lots,
#                 "items": lots_result["data"]["items"]
#             },
#             "locality": {
#                 "total": locality_result["data"]["total"],
#                 "page": page_locality,
#                 "items": locality_result["data"]["items"]
#             },
#             "loan": {
#                 "total": loan_result["data"]["total"],
#                 "page": page_loan,
#                 "items": loan_result["data"]["items"]
#             },
#             "taxon": {
#                 "total": taxon_result["data"]["total"],
#                 "page": page_taxon,
#                 "items": taxon_result["data"]["items"]
#             }
#         }
#     }
@router.post("/unified", response_model=UnifiedSearchResponse)
async def unified_search(
        query: Optional[str] = Body(default=""),
        filters: List[Dict[str, Any]] = Body(default=[]),
        page: int = Body(default=1),
        limit: int = Body(default=10),
        document_types: List[str] = Body(default=["lot", "locality", "taxon", "loan"])
):
    """
    Unified search across all document types.

    This endpoint searches across all data (lots, localities, taxonomic records, and loans)
    using a single query and returns results grouped by document type.

    Args:
        query: The search query string.
        filters: List of filter objects with field, op, and value properties.
        page: Page number for pagination.
        limit: Number of results per page.
        document_types: List of document types to search. Default is all types.

    Example:
        ```
        {
          "query": "fish",
          "filters": [
            {"field": "total_number", "op": ">=", "value": 1},
            {"field": "country", "op": "like", "value": "USA"}
          ],
          "page": 1,
          "limit": 10,
          "document_types": ["lot", "locality", "taxon", "loan"]
        }
        ```
    """
    es = await get_es_client()

    # Build the search query
    search_body = {
        "from": (page - 1) * limit,
        "size": limit,
        "track_total_hits": True,
        "query": {
            "bool": {
                "must": [],
                "filter": []
            }
        }
    }

    # Add text search if query is provided
    if query:
        search_body["query"]["bool"]["must"].append({
            "match": {
                "full_text": {
                    "query": query,
                    "fuzziness": "AUTO"
                }
            }
        })
    else:
        search_body["query"]["bool"]["must"].append({"match_all": {}})

    # Add document type filter
    if document_types and len(document_types) < 4:  # Only add if not all types are selected
        search_body["query"]["bool"]["filter"].append({
            "terms": {"document_type": document_types}
        })

    # Process filters
    for f in filters:
        field = f.get("field")
        op = f.get("op")
        value = f.get("value")

        if not field or op is None or value is None:
            continue

        # Default to standard field name
        field_name = field

        # Handle field mapping for common fields
        field_mapping = {
            "catalogNumber": "catalog_number",
            "prevNumber": "prev_number",
            "jarSize": "jar_size",
            "totalNumber": "total_number",
            "fieldNo": "field_no",
            "waterBody": "water_body",
            "scientificName": "scientific_name",
            "familyName": "family_name",
            "loanNumber": "loan_number",
            "transactionType": "transaction_type"
        }

        if field in field_mapping:
            field_name = field_mapping[field]

        # Determine if field should use keyword suffix for exact matching
        keyword_fields = [
            "country", "state", "county", "water_body", "drainage", "jar_size",
            "inventory", "type_status", "storage", "continent", "genus", "species",
            "family_name", "scientific_name", "loan_number", "transaction_type"
        ]

        if op in ["==", "!=", "like", "not like"] and field_name in keyword_fields:
            field_name = f"{field_name}.keyword"

        # Add appropriate filter based on operator
        if op == "==" or op == "=":
            search_body["query"]["bool"]["filter"].append({
                "term": {field_name: value}
            })
        elif op == "!=":
            search_body["query"]["bool"]["must_not"] = search_body["query"]["bool"].get("must_not", [])
            search_body["query"]["bool"]["must_not"].append({
                "term": {field_name: value}
            })
        elif op == ">":
            search_body["query"]["bool"]["filter"].append({
                "range": {field_name: {"gt": value}}
            })
        elif op == ">=":
            search_body["query"]["bool"]["filter"].append({
                "range": {field_name: {"gte": value}}
            })
        elif op == "<":
            search_body["query"]["bool"]["filter"].append({
                "range": {field_name: {"lt": value}}
            })
        elif op == "<=":
            search_body["query"]["bool"]["filter"].append({
                "range": {field_name: {"lte": value}}
            })
        elif op == "like":
            search_body["query"]["bool"]["filter"].append({
                "wildcard": {field_name: f"*{value}*"}
            })
        elif op == "not like":
            search_body["query"]["bool"]["must_not"] = search_body["query"]["bool"].get("must_not", [])
            search_body["query"]["bool"]["must_not"].append({
                "wildcard": {field_name: f"*{value}*"}
            })

    # Add aggregation to count results by document type
    search_body["aggs"] = {
        "document_types": {
            "terms": {
                "field": "document_type"
            }
        }
    }

    # Execute the search
    result = await es.search(index="unified", body=search_body)

    # Extract and format results
    hits = result["hits"]["hits"]
    total = result["hits"]["total"]["value"]

    # Group results by document type
    grouped_results = {
        "lot": [],
        "locality": [],
        "taxon": [],
        "loan": []
    }

    for hit in hits:
        doc_type = hit["_source"]["document_type"]
        if doc_type in grouped_results:
            grouped_results[doc_type].append(hit["_source"])

    # Extract document type counts from aggregations
    doc_type_counts = {}
    if "aggregations" in result and "document_types" in result["aggregations"]:
        for bucket in result["aggregations"]["document_types"]["buckets"]:
            doc_type_counts[bucket["key"]] = bucket["doc_count"]

    # Format the final response
    response = {
        "code": 20000,
        "data": {
            "total": total,
            "page": page,
            "counts": doc_type_counts,
            "results": grouped_results
        }
    }

    return response


@router.post("/unified/related", response_model=UnifiedSearchResponse)
async def unified_related_search(
        query: Optional[str] = Body(default=""),
        filters: List[Dict[str, Any]] = Body(default=[]),
        page: int = Body(default=1),
        limit: int = Body(default=10),
        include_related: bool = Body(default=True)
):
    """
    Unified search across all document types.

    This endpoint searches across all data (lots, localities, taxonomic records, and loans)
    using a single query and returns results grouped by document type.

    Args:
        query: The search query string.
        filters: List of filter objects with field, op, and value properties.
        page: Page number for pagination.
        limit: Number of results per page.
        document_types: List of document types to search. Default is all types.

    Example:
        ```
        {
          "query": "fish",
          "filters": [
            {"field": "total_number", "op": ">=", "value": 1},
            {"field": "country", "op": "like", "value": "USA"}
          ],
          "page": 1,
          "limit": 10,
          "document_types": ["lot", "locality", "taxon", "loan"]
        }
        ```
    """
    es = await get_es_client()

    # Build the search query
    search_body = {
        "from": (page - 1) * limit,
        "size": limit,
        "track_total_hits": True,
        "query": {
            "bool": {
                "must": [],
                "filter": []
            }
        }
    }

    # Add text search if query is provided
    if query:
        search_body["query"]["bool"]["must"].append({
            "match": {
                "full_text": {
                    "query": query,
                    "fields": ["full_text"],
                    "fuzziness": "AUTO"
                }
            }
        })
    else:
        search_body["query"]["bool"]["must"].append({"match_all": {}})

    # Process filters
    for f in filters:
        field = f.get("field")
        op = f.get("op")
        value = f.get("value")

        if not field or op is None or value is None:
            continue

        # Default to standard field name
        field_name = field

        # Handle field mapping for common fields
        field_mapping = {
            "catalogNumber": "catalog_number",
            "prevNumber": "prev_number",
            "jarSize": "jar_size",
            "totalNumber": "total_number",
            "fieldNo": "field_no",
            "waterBody": "water_body",
            "scientificName": "scientific_name",
            "familyName": "family_name",
            "loanNumber": "loan_number",
            "transactionType": "transaction_type"
        }

        if field in field_mapping:
            field_name = field_mapping[field]

        # Determine if field should use keyword suffix for exact matching
        keyword_fields = [
            "country", "state", "county", "water_body", "drainage", "jar_size",
            "inventory", "type_status", "storage", "continent", "genus", "species",
            "family_name", "scientific_name", "loan_number", "transaction_type"
        ]

        if op in ["==", "!=", "like", "not like"] and field_name in keyword_fields:
            field_name = f"{field_name}.keyword"

        # Add appropriate filter based on operator
        if op == "==" or op == "=":
            search_body["query"]["bool"]["filter"].append({
                "term": {field_name: value}
            })
        elif op == "!=":
            search_body["query"]["bool"]["must_not"] = search_body["query"]["bool"].get("must_not", [])
            search_body["query"]["bool"]["must_not"].append({
                "term": {field_name: value}
            })
        elif op == ">":
            search_body["query"]["bool"]["filter"].append({
                "range": {field_name: {"gt": value}}
            })
        elif op == ">=":
            search_body["query"]["bool"]["filter"].append({
                "range": {field_name: {"gte": value}}
            })
        elif op == "<":
            search_body["query"]["bool"]["filter"].append({
                "range": {field_name: {"lt": value}}
            })
        elif op == "<=":
            search_body["query"]["bool"]["filter"].append({
                "range": {field_name: {"lte": value}}
            })
        elif op == "like":
            search_body["query"]["bool"]["filter"].append({
                "wildcard": {field_name: f"*{value}*"}
            })
        elif op == "not like":
            search_body["query"]["bool"]["must_not"] = search_body["query"]["bool"].get("must_not", [])
            search_body["query"]["bool"]["must_not"].append({
                "wildcard": {field_name: f"*{value}*"}
            })

    # Add aggregation to count results by document type
    search_body["aggs"] = {
        "document_types": {
            "terms": {
                "field": "document_type"
            }
        }
    }

    # Execute the search
    result = await es.search(index="unified", body=search_body)

    related_ids = {
        "locality_ids": set(),
        "taxon_ids": set(),
        "loan_ids": set(),
        "lot_ids": set()
    }

    # Extract and format results
    hits = result["hits"]["hits"]
    total = result["hits"]["total"]["value"]

    # Group results by document type
    grouped_results = {
        "lot": [],
        "locality": [],
        "taxon": [],
        "loan": []
    }

    for hit in hits:
        source = hit["_source"]
        doc_type = source["document_type"]
        if doc_type == "lot":
            related_ids["lot_ids"].add(source["source_id"])
            if source.get("locality_id"):
                related_ids["locality_ids"].add(str(source["locality_id"]))
            if source.get("taxon_id"):
                related_ids["taxon_ids"].add(str(source["taxon_id"]))
        elif doc_type == "locality":
            related_ids["locality_ids"].add(source["source_id"])
        elif doc_type == "taxon":
            related_ids["taxon_ids"].add(source["source_id"])
        elif doc_type == "loan":
            related_ids["loan_ids"].add(source["source_id"])

    if related_ids["lot_ids"]:
        loan_search = {
            "size": 100,  # 限制数量以避免过大
            "query": {
                "terms": {
                    "lot_id": list(related_ids["lot_ids"])
                }
            }
        }
        loan_result = await es.search(index="unified", body=loan_search)
        for hit in loan_result["hits"]["hits"]:
            if hit["_source"]["document_type"] == "loan":
                related_ids["loan_ids"].add(hit["_source"]["source_id"])

    # 查找所有与lot关联的loan记录
    if related_ids["lot_ids"]:
        loan_search = {
            "size": 100,  # 限制数量以避免过大
            "query": {
                "terms": {
                    "lot_id": list(related_ids["lot_ids"])
                }
            }
        }
        loan_result = await es.search(index="unified", body=loan_search)
        for hit in loan_result["hits"]["hits"]:
            if hit["_source"]["document_type"] == "loan":
                related_ids["loan_ids"].add(hit["_source"]["source_id"])

    # 第三步：收集所有关联记录
    related_queries = []

    # 添加lot关联查询
    if related_ids["lot_ids"]:
        related_queries.append({
            "terms": {
                "source_id": list(related_ids["lot_ids"]),
                "boost": 0.7  # 降低相关性得分
            }
        })

    # 添加locality关联查询
    if related_ids["locality_ids"]:
        related_queries.append({
            "terms": {
                "source_id": list(related_ids["locality_ids"]),
                "boost": 0.6
            }
        })

    # 添加taxon关联查询
    if related_ids["taxon_ids"]:
        related_queries.append({
            "terms": {
                "source_id": list(related_ids["taxon_ids"]),
                "boost": 0.6
            }
        })

    # 添加loan关联查询
    if related_ids["loan_ids"]:
        related_queries.append({
            "terms": {
                "source_id": list(related_ids["loan_ids"]),
                "boost": 0.5
            }
        })

    # 执行关联搜索
    if related_queries:
        related_search = {
            "size": 200,  # 获取更多相关记录
            "query": {
                "bool": {
                    "should": related_queries,
                    "minimum_should_match": 1
                }
            }
        }

        related_result = await es.search(index="unified", body=related_search)

        # 合并结果
        all_hits = {}
        for hit in result["hits"]["hits"]:
            doc_id = hit["_source"]["document_id"]
            all_hits[doc_id] = hit

        related_counts = {"lot": 0, "locality": 0, "taxon": 0, "loan": 0}
        for hit in related_result["hits"]["hits"]:
            doc_id = hit["_source"]["document_id"]
            doc_type = hit["_source"]["document_type"]
            if doc_id not in all_hits:
                all_hits[doc_id] = hit
                if doc_type in related_counts:
                    related_counts[doc_type] += 1

        # 正确获取主查询每类的总数（不限分页）
        doc_type_counts = {"lot": 0, "locality": 0, "taxon": 0, "loan": 0}
        for bucket in result.get("aggregations", {}).get("document_types", {}).get("buckets", []):
            doc_type = bucket["key"]
            if doc_type in doc_type_counts:
                doc_type_counts[doc_type] = bucket["doc_count"]

        # 合并主+关联总数
        total_counts = {
            k: doc_type_counts[k] + related_counts[k]
            for k in doc_type_counts
        }

        # 重新格式化结果
        formatted_result = {
            "code": 20000,
            "data": {
                "total": len(all_hits),
                "page": page,
                "counts": total_counts,
                "results": {
                    "lot": [],
                    "locality": [],
                    "taxon": [],
                    "loan": []
                }
            }
        }

        # 分组结果
        for hit_id, hit in all_hits.items():
            doc_type = hit["_source"]["document_type"]
            if doc_type in formatted_result["data"]["results"]:
                formatted_result["data"]["results"][doc_type].append(hit["_source"])

        return formatted_result


@router.get("/suggest", response_model=UnifiedSearchResponse)
async def unified_suggest(
        query: str,
        field: str = "full_text",
        size: int = 10
):
    """
    Get suggestions based on a partial query.

    Args:
        query: The partial query to get suggestions for.
        field: The field to get suggestions from. Default is full_text.
        size: Number of suggestions to return.
    """
    es = await get_es_client()

    # Map common fields to their unified index counterparts
    field_mapping = {
        "scientificName": "scientific_name",
        "fieldNo": "field_no",
        "country": "country",
        "state": "state",
        "fullText": "full_text"
    }

    search_field = field_mapping.get(field, field)

    # Build the suggestion query
    search_body = {
        "suggest": {
            "text": query,
            "completion": {
                "field": f"{search_field}.suggest",
                "size": size,
                "fuzzy": {
                    "fuzziness": "AUTO"
                }
            }
        }
    }

    # Execute the suggest query
    result = await es.search(index="unified", body=search_body)

    # Extract suggestions
    suggestions = []
    if "suggest" in result and "completion" in result["suggest"]:
        for option in result["suggest"]["completion"][0]["options"]:
            suggestions.append(option["text"])

    return {
        "code": 20000,
        "data": {
            "suggestions": suggestions
        }
    }
