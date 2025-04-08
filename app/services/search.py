from typing import Dict, List, Any, Optional
from elasticsearch.exceptions import NotFoundError
from app.db.elasticsearch import get_es_client


async def search_ulm(
        query: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None,
        page: int = 1,
        limit: int = 10
) -> Dict[str, Any]:
    """
    Search ULM data in Elasticsearch.

    Args:
        query: The search.py query string.
        filters: Dictionary of field-value pairs to filter results.
        page: Page number for pagination.
        limit: Number of results per page.

    Returns:
        Dictionary containing search.py results and metadata.
    """
    es = await get_es_client()

    # Calculate offset for pagination
    offset = (page - 1) * limit

    # Build the search.py query
    search_body = {
        "from": offset,
        "size": limit,
        "track_total_hits":True,
        "query": {
            "bool": {
                "must": [],
                "filter": []
            }
        },
        "sort": [
            {"PrimaryID": {"order": "desc"}}
        ]
    }

    # Add text search.py if query is provided
    if query:
        search_body["query"]["bool"]["must"].append({
            "multi_match": {
                "query": query,
                "fields": [
                    "PrevNumber^3",
                    "Family^2",
                    "genus^2",
                    "species^2",
                    "collectorname^2",
                    "Remarks",
                    "Location",
                    "country",
                    "state",
                    "county",
                    "waterbody",
                    "drainage"
                ],
                "type": "best_fields",
                "fuzziness": "AUTO"
            }
        })
    else:
        search_body["query"]["bool"]["must"].append({"match_all": {}})

    # Add filters if provided
    if filters:
        for field, value in filters.items():
            if value is not None:
                if isinstance(value, list):
                    if field == "ids":
                        search_body["query"]["bool"]["filter"].append({
                            "terms": {"PrevNumber.keyword": value}
                        })
                    elif field in ["Family", "genus", "species", "country", "state", "county", "waterbody", "drainage",
                                   "collectorname"]:
                        search_body["query"]["bool"]["filter"].append({
                            "terms": {f"{field}.keyword": value}
                        })
                    else:
                        search_body["query"]["bool"]["filter"].append({
                            "terms": {field: value}
                        })
                elif field in ["minNum", "maxNum"]:
                    # Handle min/max total number filters
                    range_field = "TotalNumber"
                    if field == "minNum":
                        search_body["query"]["bool"]["filter"].append({
                            "range": {range_field: {"gte": value}}
                        })
                    else:
                        search_body["query"]["bool"]["filter"].append({
                            "range": {range_field: {"lte": value}}
                        })
                elif field in ["startdate", "enddate"]:
                    # Handle date range filters
                    range_field = "collectordate"
                    if field == "startdate":
                        search_body["query"]["bool"]["filter"].append({
                            "range": {range_field: {"gte": value}}
                        })
                    else:
                        search_body["query"]["bool"]["filter"].append({
                            "range": {range_field: {"lte": value}}
                        })
                elif field in ["checked", "recheckrequired"]:
                    # Handle boolean fields
                    if isinstance(value, str):
                        bool_value = value.lower() == "true"
                        search_body["query"]["bool"]["filter"].append({
                            "term": {field: bool_value}
                        })
                    else:
                        search_body["query"]["bool"]["filter"].append({
                            "term": {field: value}
                        })
                else:
                    # Default field filter
                    if field in ["Family", "genus", "species", "country", "state", "county", "waterbody", "drainage",
                                 "collectorname", "dataset", "JarSize"]:
                        search_body["query"]["bool"]["filter"].append({
                            "term": {f"{field}.keyword": value}
                        })
                    else:
                        search_body["query"]["bool"]["filter"].append({
                            "term": {field: value}
                        })

    # Execute the search.py
    result = await es.search(index="ulm", body=search_body)

    # Extract and format the results
    hits = result["hits"]["hits"]
    total = result["hits"]["total"]["value"]

    items = [hit["_source"] for hit in hits]

    return {
        "code": 20000,
        "data": {
            "items": items,
            "total": total,
        }
    }


async def search_ost(
        query: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None,
        page: int = 1,
        limit: int = 10
) -> Dict[str, Any]:
    """
    Search OST data in Elasticsearch.

    Args:
        query: The search.py query string.
        filters: Dictionary of field-value pairs to filter results.
        page: Page number for pagination.
        limit: Number of results per page.

    Returns:
        Dictionary containing search.py results and metadata.
    """
    es = await get_es_client()

    # Calculate offset for pagination
    offset = (page - 1) * limit

    # Build the search.py query
    search_body = {
        "from": offset,
        "size": limit,
        "track_total_hits": True,
        "query": {
            "bool": {
                "must": [],
                "filter": []
            }
        },
        "sort": [
            {"ostcatalog.keyword": {"order": "asc"}}
        ]
    }

    # Add text search.py if query is provided
    if query:
        search_body["query"]["bool"]["must"].append({
            "multi_match": {
                "query": query,
                "fields": [
                    "ostcatalog^3",
                    "tucatalog^3",
                    "scientificname^2",
                    "locality",
                    "fieldnumber^2",
                    "collector^2",
                    "remarks",
                    "scientificnameremarks"
                ],
                "type": "best_fields",
                "fuzziness": "AUTO"
            }
        })
    else:
        search_body["query"]["bool"]["must"].append({"match_all": {}})

    # Add filters if provided
    if filters:
        for field, value in filters.items():
            if value is not None:
                if field == "ostcatalog":
                    search_body["query"]["bool"]["filter"].append({
                        "term": {f"{field}.keyword": value}
                    })
                elif field in ["count", "taxonid"]:
                    search_body["query"]["bool"]["filter"].append({
                        "term": {field: value}
                    })
                elif field in ["tl", "sl", "fl", "gm"]:
                    # Handle numeric filters as range
                    if isinstance(value, dict) and ("min" in value or "max" in value):
                        range_filter = {}
                        if "min" in value and value["min"]:
                            range_filter["gte"] = value["min"]
                        if "max" in value and value["max"]:
                            range_filter["lte"] = value["max"]
                        if range_filter:
                            search_body["query"]["bool"]["filter"].append({
                                "range": {field: range_filter}
                            })
                    else:
                        search_body["query"]["bool"]["filter"].append({
                            "term": {field: value}
                        })
                elif field in ["recheckedrequried"]:
                    # Handle boolean fields
                    if isinstance(value, str):
                        bool_value = value.lower() == "true"
                        search_body["query"]["bool"]["filter"].append({
                            "term": {field: bool_value}
                        })
                    else:
                        search_body["query"]["bool"]["filter"].append({
                            "term": {field: value}
                        })
                else:
                    # Default field filter
                    if field in ["type", "inventory", "scientificname", "fieldnumber", "collector", "reviewer"]:
                        search_body["query"]["bool"]["filter"].append({
                            "term": {f"{field}.keyword": value}
                        })
                    else:
                        search_body["query"]["bool"]["filter"].append({
                            "term": {field: value}
                        })

    # Execute the search.py
    result = await es.search(index="ost", body=search_body)

    # Extract and format the results
    hits = result["hits"]["hits"]
    total = result["hits"]["total"]["value"]

    items = [hit["_source"] for hit in hits]

    return {
        "code": 20000,
        "data": {
            "items": items,
            "total": total,
        }
    }


async def search_locality(
        query: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None,
        fuzzy: bool = True,
        page: int = 1,
        limit: int = 10
) -> Dict[str, Any]:
    """
    在Elasticsearch中搜索地理位置数据。

    返回包括筛选选项的结果，适用于表格筛选界面。
    """
    es = await get_es_client()

    # 计算分页偏移量
    offset = (page - 1) * limit

    # 构建搜索查询
    search_body = {
        "from": offset,
        "size": limit,
        "query": {
            "bool": {
                "should": [],
                "filter": []
            }
        },
        "highlight": {
            "fields": {
                "full_text": {},
                "locality_string": {},
                "drainage": {},
                "country": {},
                "state": {},
                "county": {},
                "continent": {},
                "island": {},
                "island_group": {},
                "waterbody": {},
                "verbatim_collectors": {}
            },
            "pre_tags": ["<span class='highlight'>"],
            "post_tags": ["</span>"]
        },
        "aggs": {
            # 为表格筛选添加更多列的聚合
            "countries": {
                "terms": {
                    "field": "country.keyword",
                    "size": 100
                }
            },
            "states": {
                "terms": {
                    "field": "state.keyword",
                    "size": 100
                }
            },
            "counties": {
                "terms": {
                    "field": "county.keyword",
                    "size": 100
                }
            },
            "continents": {
                "terms": {
                    "field": "continent.keyword",
                    "size": 20
                }
            },
            "drainages": {
                "terms": {
                    "field": "drainage.keyword",
                    "size": 100
                }
            },
            "waterbodies": {
                "terms": {
                    "field": "waterbody.keyword",
                    "size": 100
                }
            },
            "islands": {
                "terms": {
                    "field": "island.keyword",
                    "size": 100
                }
            },
            "island_groups": {
                "terms": {
                    "field": "island_group.keyword",
                    "size": 100
                }
            },
            "collectors": {
                "terms": {
                    "field": "verbatim_collectors.keyword",
                    "size": 100
                }
            }
        }
    }

    # 添加文本搜索（如果提供了查询）
    if query:
        search_body["query"]["bool"]["should"] = [
            # 精确匹配 field_no
            {
                "term": {
                    "field_no.keyword": {
                        "value": query,
                        "boost": 10
                    }
                }
            },
            # 精确短语匹配 locality_string.raw
            {
                "match_phrase": {
                    "locality_string.raw": {
                        "query": query,
                        "boost": 5
                    }
                }
            },
            # 多字段模糊匹配
            {
                "multi_match": {
                    "query": query,
                    "fields": [
                        "full_text^2",
                        "name^3",
                        "locality_string^3",
                        "drainage",
                        "country",
                        "state",
                        "county",
                        "continent",
                        "island",
                        "island_group",
                        "waterbody",
                        "verbatim_collectors"
                    ],
                    "fuzziness": "AUTO" if fuzzy else "0",
                    "boost": 1
                }
            }
        ]
        search_body["query"]["bool"]["minimum_should_match"] = 1
    else:
        # 高级搜索：处理各个字段的查询
        if filters:
            for field, value in filters.items():
                if not value:
                    continue

                # field_no 和 start_date 用精确匹配
                if field in ["field_no", "start_date"]:
                    search_body["query"]["bool"]["should"].append({
                        "term": {
                            f"{field}.keyword": {
                                "value": value,
                                "boost": 10
                            }
                        }
                    })
                    continue

                # 先 match_phrase 精确短语匹配
                search_body["query"]["bool"]["should"].append({
                    "match_phrase": {
                        field: {
                            "query": value,
                            "boost": 3
                        }
                    }
                })

                # 再 fallback 到模糊 match 查询
                search_body["query"]["bool"]["should"].append({
                    "match": {
                        field: {
                            "query": value,
                            "fuzziness": "AUTO" if fuzzy else "0",
                            "boost": 1
                        }
                    }
                })

            if not search_body["query"]["bool"]["should"]:
                search_body["query"]["bool"]["should"].append({"match_all": {}})

            search_body["query"]["bool"]["minimum_should_match"] = 1
        else:
            # 如果没有任何查询参数，返回所有结果
            search_body["query"]["bool"]["should"].append({"match_all": {}})

    # 执行搜索
    result = await es.search(index="geo_gazetteer", body=search_body)

    # 提取并格式化结果
    hits = result["hits"]["hits"]
    total = result["hits"]["total"]["value"]

    items = []
    for hit in hits:
        item = hit["_source"]
        # 添加高亮内容
        if "highlight" in hit:
            item["highlights"] = hit["highlight"]
        items.append(item)

    # 处理聚合，为表格列筛选提供选项
    filter_options = {}
    if "aggregations" in result:
        aggs = result["aggregations"]

        # 提取每个字段的唯一值作为表格筛选选项
        for field_name, agg_name in [
            ("country", "countries"),
            ("state", "states"),
            ("county", "counties"),
            ("continent", "continents"),
            ("drainage", "drainages"),
            ("waterbody", "waterbodies"),
            ("island", "islands"),
            ("island_group", "island_groups"),
            ("verbatim_collectors", "collectors")
        ]:
            if agg_name in aggs:
                filter_options[field_name] = [
                    {"value": bucket["key"], "count": bucket["doc_count"]}
                    for bucket in aggs[agg_name]["buckets"]
                    if bucket["key"]  # 排除空值
                ]

    # 提取所有可能的表格列
    columns = set()
    for item in items:
        columns.update(item.keys())

    # 过滤掉不需要作为表格列的字段
    excluded_columns = {"highlights", "full_text", "related_terms", "synonyms", "source_text"}
    columns = [col for col in columns if col not in excluded_columns]

    return {
        "code": 20000,
        "data": {
            "items": items,
            "total": total,
            "columns": columns,
            "filter_options": filter_options
        }
    }

async def search_taxon(
        query: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None,
        page: int = 1,
        limit: int = 10
) -> Dict[str, Any]:
    """
    Search Taxonomic data in Elasticsearch.

    Args:
        query: The search.py query string.
        filters: Dictionary of field-value pairs to filter results.
        page: Page number for pagination.
        limit: Number of results per page.

    Returns:
        Dictionary containing search.py results and metadata.
    """
    es = await get_es_client()

    # Calculate offset for pagination
    offset = (page - 1) * limit

    # Build the search.py query
    search_body = {
        "from": offset,
        "size": limit,
        "query": {
            "bool": {
                "must": [],
                "filter": []
            }
        },
        "sort": [
            {"FullScientificName.keyword": {"order": "asc"}}
        ]
    }

    # Add text search.py if query is provided
    if query:
        search_body["query"]["bool"]["must"].append({
            "multi_match": {
                "query": query,
                "fields": [
                    "FullScientificName^3",
                    "Genus^2",
                    "Species^2",
                    "Subspecies^2",
                    "FamilyName^2",
                    "Remarks"
                ],
                "type": "best_fields",
                "fuzziness": "AUTO"
            }
        })
    else:
        search_body["query"]["bool"]["must"].append({"match_all": {}})

    # Add filters if provided
    if filters:
        for field, value in filters.items():
            if value is not None:
                if field in ["Genus", "Species", "Subspecies", "FamilyName"]:
                    search_body["query"]["bool"]["filter"].append({
                        "term": {f"{field}.keyword": value}
                    })
                elif field in ["FamilyID", "TaxonID"]:
                    search_body["query"]["bool"]["filter"].append({
                        "term": {field: value}
                    })
                else:
                    search_body["query"]["bool"]["filter"].append({
                        "term": {field: value}
                    })

    # Execute the search.py
    result = await es.search(index="taxon", body=search_body)

    # Extract and format the results
    hits = result["hits"]["hits"]
    total = result["hits"]["total"]["value"]

    items = [hit["_source"] for hit in hits]

    return {
        "code": 20000,
        "data": {
            "items": items,
            "total": total,
        }
    }


async def search_loan(
        query: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None,
        page: int = 1,
        limit: int = 10
) -> Dict[str, Any]:
    """
    Search Loan data in Elasticsearch.

    Args:
        query: The search.py query string.
        filters: Dictionary of field-value pairs to filter results.
        page: Page number for pagination.
        limit: Number of results per page.

    Returns:
        Dictionary containing search.py results and metadata.
    """
    es = await get_es_client()

    # Calculate offset for pagination
    offset = (page - 1) * limit

    # Build the search.py query
    search_body = {
        "from": offset,
        "size": limit,
        "track_total_hits": True,
        "query": {
            "bool": {
                "must": [],
                "filter": []
            }
        },
        "sort": [
            {"LoanNumber.keyword": {"order": "desc"}}
        ]
    }

    # Add text search.py if query is provided
    if query:
        search_body["query"]["bool"]["must"].append({
            "multi_match": {
                "query": query,
                "fields": [
                    "LoanNumber^3",
                    "FullName^2",
                    "FirstName",
                    "LastName",
                    "TransactionType",
                    "Text1",
                    "Text2",
                    "ShipToAddress",
                    "ShipToCity",
                    "ShipToState",
                    "ShipToCountry",
                    "ShipToRemarks"
                ],
                "type": "best_fields",
                "fuzziness": "AUTO"
            }
        })
    else:
        search_body["query"]["bool"]["must"].append({"match_all": {}})

    # Add filters if provided
    if filters:
        for field, value in filters.items():
            if value is not None:
                if field == "loanNumber":
                    search_body["query"]["bool"]["filter"].append({
                        "term": {"LoanNumber.keyword": value}
                    })
                elif field == "ids":  # CatalogNumber list
                    # We need to handle this differently - would need a join query
                    # For simplicity, we'll skip this filter in this implementation
                    pass
                elif field in ["loanOpenStartDate", "loanOpenEndDate"]:
                    # Handle date range filters for LoanDate
                    range_field = "LoanDate"
                    if field == "loanOpenStartDate":
                        search_body["query"]["bool"]["filter"].append({
                            "range": {range_field: {"gte": value}}
                        })
                    else:
                        search_body["query"]["bool"]["filter"].append({
                            "range": {range_field: {"lte": value}}
                        })
                elif field in ["loanClosedStartDate", "loanClosedEndDate"]:
                    # Handle date range filters for DateClosed
                    range_field = "DateClosed"
                    if field == "loanClosedStartDate":
                        search_body["query"]["bool"]["filter"].append({
                            "range": {range_field: {"gte": value}}
                        })
                    else:
                        search_body["query"]["bool"]["filter"].append({
                            "range": {range_field: {"lte": value}}
                        })
                elif field == "loanPeopleID":
                    search_body["query"]["bool"]["filter"].append({
                        "term": {"LoanPeopleID": value}
                    })
                elif field == "Closed":
                    if isinstance(value, str):
                        bool_value = value.lower() == "true"
                        search_body["query"]["bool"]["filter"].append({
                            "term": {field: bool_value}
                        })
                    else:
                        search_body["query"]["bool"]["filter"].append({
                            "term": {field: value}
                        })
                elif field in ["TransactionType", "ShipToCity", "ShipToState", "ShipToCountry"]:
                    search_body["query"]["bool"]["filter"].append({
                        "term": {f"{field}.keyword": value}
                    })
                else:
                    # Skip other filters that would require joins with other tables
                    pass

    # Execute the search.py
    result = await es.search(index="loan", body=search_body)

    # Extract and format the results
    hits = result["hits"]["hits"]
    total = result["hits"]["total"]["value"]

    items = [hit["_source"] for hit in hits]

    return {
        "code": 20000,
        "data": {
            "items": items,
            "total": total,
        }
    }


async def search_lots(
        query: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None,
        page: int = 1,
        limit: int = 10
) -> Dict[str, Any]:
    """
    Search Lots (Primary) data in Elasticsearch.

    Args:
        query: The search.py query string.
        filters: Dictionary of field-value pairs to filter results.
        page: Page number for pagination.
        limit: Number of results per page.

    Returns:
        Dictionary containing search.py results and metadata.
    """
    es = await get_es_client()

    # Calculate offset for pagination
    offset = (page - 1) * limit

    # Build the search.py query
    search_body = {
        "from": offset,
        "size": limit,
        "track_total_hits": True,
        "query": {
            "bool": {
                "must": [],
                "filter": []
            }
        },
        "sort": [
            {"PrimaryID": {"order": "desc"}}
        ]
    }

    # Add text search.py if query is provided
    if query:
        search_body["query"]["bool"]["must"].append({
            "multi_match": {
                "query": query,
                "fields": [
                    "ScientificName^3",
                    "PrevNumber^2",
                    "FullScientificName^2",
                    "Genus",
                    "Species",
                    "Family",
                    "JarSize",
                    "Storage",
                    "TypeStatus",
                    "Inventory",
                    "Remarks",
                    "FieldNo",
                    "LocalityString",
                    "Country",
                    "State",
                    "County",
                    "Drainage",
                    "WaterBody"
                ],
                "type": "best_fields",
                "fuzziness": "AUTO"
            }
        })
    else:
        search_body["query"]["bool"]["must"].append({"match_all": {}})

    # Add filters if provided
    if filters:
        for field, value in filters.items():
            if value is not None:
                if field == "ids":
                    if isinstance(value, list):
                        search_body["query"]["bool"]["filter"].append({
                            "terms": {"CatalogNumber": value}
                        })
                    else:
                        search_body["query"]["bool"]["filter"].append({
                            "term": {"CatalogNumber": value}
                        })
                elif field == "localityID":
                    search_body["query"]["bool"]["filter"].append({
                        "term": {"Locality1ID": value}
                    })
                elif field == "taxonId":
                    search_body["query"]["bool"]["filter"].append({
                        "term": {"TaxonID": value}
                    })
                elif field == "familyID":
                    search_body["query"]["bool"]["filter"].append({
                        "term": {"FamilyID": value}
                    })
                elif field == "fieldNo":
                    search_body["query"]["bool"]["filter"].append({
                        "regexp": {"FieldNo": f".*{value}.*"}
                    })
                elif field in ["minNumber", "maxNumber"]:
                    # Handle min/max total number filters
                    range_field = "TotalNumber"
                    if field == "minNumber":
                        search_body["query"]["bool"]["filter"].append({
                            "range": {range_field: {"gte": value}}
                        })
                    else:
                        search_body["query"]["bool"]["filter"].append({
                            "range": {range_field: {"lte": value}}
                        })
                elif field in ["startDate", "endDate"]:
                    # Handle date range filters
                    range_field = "DateCataloged"
                    if field == "startDate":
                        search_body["query"]["bool"]["filter"].append({
                            "range": {range_field: {"gte": value}}
                        })
                    else:
                        search_body["query"]["bool"]["filter"].append({
                            "range": {range_field: {"lte": value}}
                        })
                else:
                    # Default field filter
                    if field in ["JarSize", "Storage", "TypeStatus", "Inventory"]:
                        search_body["query"]["bool"]["filter"].append({
                            "term": {field: value}
                        })
                    else:
                        # For other fields, we'll skip for now
                        pass

    # Execute the search.py
    result = await es.search(index="lots", body=search_body)

    # Extract and format the results
    hits = result["hits"]["hits"]
    total = result["hits"]["total"]["value"]

    items = [hit["_source"] for hit in hits]

    return {
        "code": 20000,
        "data": {
            "items": items,
            "total": total,
        }
    }

def dsl_from_filters(filters: List[Dict[str, Any]]) -> Dict[str, Any]:
    must = []
    must_not = []

    for f in filters:
        field = f.get("field")
        op = f.get("op")
        value = f.get("value")

        if not field or op is None or value is None:
            continue

        keyword_field = f"{field}.keyword"

        if op == "==":
            must.append({"term": {keyword_field: value}})
        elif op == "!=":
            must_not.append({"term": {keyword_field: value}})
        elif op in (">", "<", ">=", "<="):
            range_key = {">": "gt", "<": "lt", ">=": "gte", "<=": "lte"}[op]
            must.append({"range": {field: {range_key: value}}})
        elif op == "like":
            must.append({"wildcard": {keyword_field: f"*{value}*"}})
        elif op == "not like":
            must_not.append({"wildcard": {keyword_field: f"*{value}*"}})
        else:
            must.append({"match": {field: value}})

    return {"bool": {"must": must, "must_not": must_not}}


async def search_generic(index: str, query: str, filters: Dict[str, Any], page: int, limit: int) -> Dict[str, Any]:
    es = await get_es_client()

    from_ = (page - 1) * limit
    search_query = {
        "query": {
            "bool": {
                "must": [
                    {"multi_match": {"query": query, "fields": ["*"], "fuzziness": "AUTO"}}
                ] + filters.get("bool", {}).get("must", []),
                "must_not": filters.get("bool", {}).get("must_not", [])
            }
        },
        "from": from_,
        "size": limit
    }
    resp = await es.search(index=index, body=search_query)
    hits = resp["hits"]["hits"]
    return {
        "data": {
            "total": resp["hits"]["total"]["value"],
            "items": [hit["_source"] for hit in hits]
        }
    }

