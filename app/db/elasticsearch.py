from datetime import datetime

from elasticsearch import AsyncElasticsearch
from app.core.config import settings

# Global Elasticsearch client instance
es_client = None
UNIFIED_INDEX_MAPPING = {
    "mappings": {
        "properties": {
            # Core identification fields
            "document_id": {"type": "keyword"},
            "document_type": {"type": "keyword"},  # "lot", "locality", "taxon", "loan"
            "source_id": {"type": "keyword"},  # Original ID from source table

            # Lots (Primary) fields
            "catalog_number": {"type": "integer"},
            "prev_number": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "total_number": {"type": "integer"},
            "jar_size": {"type": "keyword"},
            "storage": {"type": "keyword"},
            "type_status": {"type": "keyword"},
            "inventory": {"type": "keyword"},
            "date_cataloged": {"type": "date",
                               "format": "strict_date_optional_time||yyyy-MM-dd||yyyy/MM/dd||epoch_millis"},

            # Locality fields
            "field_no": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "locality_string": {"type": "text"},
            "country": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "state": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "county": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "drainage": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "water_body": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "continent": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "lat": {"type": "float"},
            "lon": {"type": "float"},

            # Taxonomic fields
            "scientific_name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "genus": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "species": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "subspecies": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "family_name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},

            # Loan fields
            "loan_number": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "transaction_type": {"type": "keyword"},
            "loan_date": {"type": "date", "format": "yyyy-MM-dd||strict_date_optional_time||yyyy/MM/dd||MM/dd/yyyy||MM/dd/yy||d MMM yyyy||d MMMM yyyy||epoch_millis","null_value": "1000-01-01"},
            "date_closed": {"type": "date", "format": "yyyy-MM-dd||strict_date_optional_time||yyyy/MM/dd||MM/dd/yyyy||MM/dd/yy||d MMM yyyy||d MMMM yyyy||epoch_millis","null_value": "1000-01-01"},
            "is_closed": {"type": "text"},
            "lender_name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},

            # Relationship IDs (for tracking relationships between entities)
            "locality_id": {"type": "integer"},
            "taxon_id": {"type": "integer"},
            "family_id": {"type": "integer"},
            "loan_id": {"type": "integer"},
            "address": {"type": "text", "fields": {"keyword": {"type": "keyword"}, "suggest": {"type": "completion"}}},
            "ship_method": {"type": "text", "fields": {"keyword": {"type": "keyword"}, "suggest": {"type": "completion"}}},

            # Content fields
            "remarks": {"type": "text"},

            # Full-text search field that combines all text fields
            "full_text": {"type": "text"},

            # Timestamps
            "date_modified": {"type": "date",
                              "format": "strict_date_optional_time||yyyy-MM-dd||yyyy/MM/dd||epoch_millis"},
            "date_created": {"type": "date",
                             "format": "strict_date_optional_time||yyyy-MM-dd||yyyy/MM/dd||epoch_millis"}
        }
    }
}


async def ensure_unified_index_exists():
    """Ensure unified index exists in Elasticsearch."""
    es = await get_es_client()

    if not await es.indices.exists(index="unified"):
        await es.indices.create(index="unified", body=UNIFIED_INDEX_MAPPING)
        print("Created unified index")

        # Create metadata for unified sync tracking
        await es.index(
            index=".sync-metadata",
            id="unified-last-sync",
            document={
                "last_primary_id": 0,
                "last_locality_id": 0,
                "last_taxon_id": 0,
                "last_loan_id": 0,
                "last_sync_time": datetime.now().isoformat()
            }
        )

async def init_es():
    """Initialize the Elasticsearch client during application startup."""
    global es_client
    es_client = AsyncElasticsearch(
        hosts=[f"http://{settings.ELASTICSEARCH_HOST}:{settings.ELASTICSEARCH_PORT}"]
    )

    # Check if the required indices exist, create them if they don't
    await ensure_indices_exist()


async def close_es():
    """Close the Elasticsearch client during application shutdown."""
    global es_client
    if es_client:
        await es_client.close()


async def get_es_client() -> AsyncElasticsearch:
    """Return the Elasticsearch client instance."""
    return es_client


async def ensure_indices_exist():
    """Ensure that all required Elasticsearch indices exist."""
    global es_client

    # ULM index mapping
    ulm_mapping = {
        "mappings": {
            "properties": {
                "PrimaryID": {"type": "integer"},
                "CatalogNumber": {"type": "integer"},
                "PrevNumber": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "dataset": {"type": "keyword"},
                "Family": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "genus": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "species": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "TotalNumber": {"type": "integer"},
                "JarSize": {"type": "keyword"},
                "TypeStatus": {"type": "keyword"},
                "Inventory": {"type": "keyword"},
                "Remarks": {"type": "text"},
                "Location": {"type": "text"},
                "country": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "state": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "county": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "waterbody": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "drainage": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "collectorname": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "collectordate": {"type": "date", "format": "strict_date_optional_time||yyyy-MM-dd||yyyy/MM/dd||epoch_millis"},
                "checked": {"type": "boolean"},
                "recheckrequired": {"type": "boolean"},
                "recheckcomment": {"type": "text"},
                "reviewer": {"type": "keyword"}
            }
        }
    }

    # OST index mapping
    ost_mapping = {
        "mappings": {
            "properties": {
                "ostcatalog": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "tucatalog": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "othercatalog": {"type": "text"},
                "count": {"type": "integer"},
                "type": {"type": "keyword"},
                "inventory": {"type": "keyword"},
                "scientificname": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "locality": {"type": "text"},
                "datecollected": {"type": "date", "format": "strict_date_optional_time||yyyy-MM-dd||yyyy/MM/dd||epoch_millis"},
                "fieldnumber": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "collector": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "remarks": {"type": "text"},
                "tl": {"type": "float"},
                "sl": {"type": "float"},
                "fl": {"type": "float"},
                "gm": {"type": "float"},
                "scientificnameremarks": {"type": "text"},
                "recheckedrequried": {"type": "boolean"},
                "recheckcomment": {"type": "text"},
                "reviewer": {"type": "keyword"},
                "taxonid": {"type": "integer"}
            }
        }
    }

    # Locality index mapping
    locality_mapping = {
        "mappings": {
            "properties": {
                "Locality1ID": {"type": "integer"},
                "FieldNo": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "LocalityString": {"type": "text"},
                "Drainage": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "WaterBody": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "Country": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "Continent": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "State": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "County": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "Lat": {"type": "float"},
                "Lon": {"type": "float"},
                "StartDate": {"type": "date", "format": "strict_date_optional_time||yyyy-MM-dd||yyyy/MM/dd||epoch_millis"},
                "EndDate": {"type": "date", "format": "strict_date_optional_time||yyyy-MM-dd||yyyy/MM/dd||epoch_millis"},
                "VerbatimDate": {"type": "text"},
                "Remarks": {"type": "text"},
                "Inventory": {"type": "keyword"},
                "VerbatimCollectors": {"type": "text"}
            }
        }
    }

    # Taxonomic index mapping
    taxon_mapping = {
        "mappings": {
            "properties": {
                "TaxonID": {"type": "integer"},
                "FullScientificName": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "Genus": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "Species": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "Subspecies": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "FamilyID": {"type": "integer"},
                "FamilyName": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "Remarks": {"type": "text"}
            }
        }
    }

    # Loans mapping
    loan_mapping = {
        "mappings": {
            "properties": {
                "ID": {"type": "integer"},
                "LoanNumber": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "TransactionType": {"type": "keyword"},
                "LoanDate": {"type": "date", "format": "strict_date_optional_time||yyyy-MM-dd||yyyy/MM/dd||epoch_millis"},
                "Closed": {"type": "boolean"},
                "DateClosed": {"type": "date", "format": "strict_date_optional_time||yyyy-MM-dd||yyyy/MM/dd||epoch_millis"},
                "Text1": {"type": "text"},
                "Text2": {"type": "text"},
                "LoanPeopleID": {"type": "integer"},
                "AgentID": {"type": "text"},
                "LoanAgents": {"type": "text"},
                "OrganizationID": {"type": "text"},
                "FullName": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "FirstName": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "LastName": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "ShipToAddress": {"type": "text"},
                "ShipToCity": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "ShipToState": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "ShipToZipCode": {"type": "text"},
                "ShipToCountry": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "ShipToRemarks": {"type": "text"},
                "ShipToMethod": {"type": "text"}
            }
        }
    }

    # Lots mapping (overlapping with Primary table)
    lots_mapping = {
        "mappings": {
            "properties": {
                "PrimaryID": {"type": "integer"},
                "CatalogNumber": {"type": "integer"},
                "ScientificName": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "PrevNumber": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "DateCataloged": {"type": "date", "format": "strict_date_optional_time||yyyy-MM-dd||yyyy/MM/dd||epoch_millis"},
                "JarSize": {"type": "keyword"},
                "Storage": {"type": "keyword"},
                "TypeStatus": {"type": "keyword"},
                "Inventory": {"type": "keyword"},
                "Remarks": {"type": "text"},
                "Locality1ID": {"type": "integer"},
                "CatalogerID": {"type": "integer"},
                "TotalNumber": {"type": "integer"},
                "TimeStampModified": {"type": "date", "format": "strict_date_optional_time||yyyy-MM-dd||yyyy/MM/dd||epoch_millis"},
                "TaxonID": {"type": "integer"},
                "FamilyID": {"type": "integer"},
                "FullScientificName": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "Genus": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "Species": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "Family": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "FieldNo": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "LocalityString": {"type": "text"},
                "Country": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "State": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "County": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "Drainage": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "WaterBody": {"type": "text", "fields": {"keyword": {"type": "keyword"}}}
            }
        }
    }


    # Check and create indices if needed
    indices = {
        "ulm": ulm_mapping,
        "ost": ost_mapping,
        "locality": locality_mapping,
        "taxon": taxon_mapping,
        "loan": loan_mapping,
        "lots": lots_mapping,
    }

    # Create metadata index for tracking sync state
    metadata_mapping = {
        "mappings": {
            "properties": {
                "last_primary_id": {"type": "integer"},
                "last_ostcatalog": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "last_locality_id": {"type": "integer"},
                "last_taxon_id": {"type": "integer"},
                "last_loan_id": {"type": "integer"},
                "last_sync_time": {"type": "date"}
            }
        }
    }

    if not await es_client.indices.exists(index=".sync-metadata"):
        await es_client.indices.create(index=".sync-metadata", body=metadata_mapping)
        print(f"Created sync metadata index")

    for index_name, mapping in indices.items():
        if not await es_client.indices.exists(index=index_name):
            await es_client.indices.create(index=index_name, body=mapping)
            print(f"Created index: {index_name}")