import re
from typing import List, Dict, Any, Optional
import asyncio
import logging
from datetime import datetime

from app.db.database import execute_query
from app.db.elasticsearch import get_es_client, ensure_unified_index_exists
from app.core.config import settings

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)



async def sync_ulm_data(force_full_sync: bool = False):
    """
    Synchronize ULM data from PostgreSQL to Elasticsearch.

    Args:
        force_full_sync: If True, perform a full sync regardless of last sync state.
    """
    es = await get_es_client()

    # Get the last synchronized record ID if not doing a full sync
    last_sync_id = 0
    if not force_full_sync:
        try:
            last_sync_info = await es.get(index=".sync-metadata", id="ulm-last-sync")
            last_sync_id = last_sync_info["_source"]["last_primary_id"]
        except:
            logger.info("No previous sync metadata found for ULM, performing full sync.")

    # Query for records to synchronize
    batch_size = settings.SYNC_BATCH_SIZE
    offset = 0

    while True:
        query = """
        SELECT * FROM ulm_temp 
        WHERE "PrimaryID" > $1
        ORDER BY "PrimaryID"
        LIMIT $2
        """

        records = await execute_query(query, last_sync_id, batch_size)

        if not records:
            break

        # Prepare batch operations for Elasticsearch
        operations = []
        max_id = 0

        for record in records:
            max_id = max(max_id, record["PrimaryID"])

            # Add to bulk operations
            operations.extend([
                {"index": {"_index": "ulm", "_id": str(record["PrimaryID"])}},
                record
            ])

        if operations:
            # Execute bulk operations
            response = await es.bulk(body=operations, refresh=True)
            if response["errors"]:
                for item in response["items"]:
                    if "error" in item["index"]:
                        logger.error(f"Bulk error: {item['index']['error']}")
            else:
                logger.info(f"Bulk succeeded: inserted {len(records)} records")

            # Update the last synchronized ID
            await es.index(
                index=".sync-metadata",
                id="ulm-last-sync",
                document={"last_primary_id": max_id, "last_sync_time": datetime.now().isoformat()}
            )

            logger.info(f"Synchronized {len(records)} ULM records up to ID {max_id}")

        # If we got fewer records than the batch size, we're done
        if len(records) < batch_size:
            break

        # Update the last sync ID for the next batch
        last_sync_id = max_id


async def sync_ost_data(force_full_sync: bool = False):
    """
    Synchronize OST data from PostgreSQL to Elasticsearch.

    Args:
        force_full_sync: If True, perform a full sync regardless of last sync state.
    """
    es = await get_es_client()

    # Get the last synchronized record ID if not doing a full sync
    last_sync_catalog = ""
    if not force_full_sync:
        try:
            last_sync_info = await es.get(index=".sync-metadata", id="ost-last-sync")
            last_sync_catalog = last_sync_info["_source"]["last_ostcatalog"]
        except:
            logger.info("No previous sync metadata found for OST, performing full sync.")

    # Query for records to synchronize
    batch_size = settings.SYNC_BATCH_SIZE

    while True:
        query = """
        SELECT * FROM ostelogy 
        WHERE "ostcatalog" > $1
        ORDER BY "ostcatalog"
        LIMIT $2
        """

        records = await execute_query(query, last_sync_catalog, batch_size)

        if not records:
            break

        # Prepare batch operations for Elasticsearch
        operations = []
        max_catalog = ""

        for record in records:
            if record["ostcatalog"] > max_catalog:
                max_catalog = record["ostcatalog"]

            # Add to bulk operations
            operations.extend([
                {"index": {"_index": "ost", "_id": record["ostcatalog"]}},
                record
            ])

        if operations:
            # Execute bulk operations
            response = await es.bulk(body=operations, refresh=True)
            if response["errors"]:
                for item in response["items"]:
                    if "error" in item["index"]:
                        logger.error(f"Bulk error: {item['index']['error']}")
            else:
                logger.info(f"Bulk succeeded: inserted {len(records)} records")

            # Update the last synchronized catalog
            await es.index(
                index=".sync-metadata",
                id="ost-last-sync",
                document={"last_ostcatalog": max_catalog, "last_sync_time": datetime.now().isoformat()}
            )

            logger.info(f"Synchronized {len(records)} OST records up to catalog {max_catalog}")

        # If we got fewer records than the batch size, we're done
        if len(records) < batch_size:
            break

        # Update the last sync catalog for the next batch
        last_sync_catalog = max_catalog


async def sync_locality_data(force_full_sync: bool = False):
    """
    Synchronize Locality data from PostgreSQL to Elasticsearch.

    Args:
        force_full_sync: If True, perform a full sync regardless of last sync state.
    """
    es = await get_es_client()

    # Get the last synchronized record ID if not doing a full sync
    last_sync_id = 0
    if not force_full_sync:
        try:
            last_sync_info = await es.get(index=".sync-metadata", id="locality-last-sync")
            last_sync_id = last_sync_info["_source"]["last_locality_id"]
        except:
            logger.info("No previous sync metadata found for Locality, performing full sync.")

    # Query for records to synchronize
    batch_size = settings.SYNC_BATCH_SIZE

    while True:
        query = """
        SELECT * FROM locality1 
        WHERE "Locality1ID" > $1
        ORDER BY "Locality1ID"
        LIMIT $2
        """

        records = await execute_query(query, last_sync_id, batch_size)

        if not records:
            break

        # Prepare batch operations for Elasticsearch
        operations = []
        max_id = 0

        for record in records:
            max_id = max(max_id, record["Locality1ID"])

            # Add to bulk operations
            operations.extend([
                {"index": {"_index": "locality", "_id": str(record["Locality1ID"])}},
                record
            ])

        if operations:
            # Execute bulk operations
            response = await es.bulk(body=operations, refresh=True)
            if response["errors"]:
                for item in response["items"]:
                    if "error" in item["index"]:
                        logger.error(f"Bulk error: {item['index']['error']}")
            else:
                logger.info(f"Bulk succeeded: inserted {len(records)} records")

            # Update the last synchronized ID
            await es.index(
                index=".sync-metadata",
                id="locality-last-sync",
                document={"last_locality_id": max_id, "last_sync_time": datetime.now().isoformat()}
            )

            logger.info(f"Synchronized {len(records)} Locality records up to ID {max_id}")

        # If we got fewer records than the batch size, we're done
        if len(records) < batch_size:
            break

        # Update the last sync ID for the next batch
        last_sync_id = max_id


async def sync_taxonomic_data(force_full_sync: bool = False):
    """
    Synchronize Taxonomic data from PostgreSQL to Elasticsearch.

    Args:
        force_full_sync: If True, perform a full sync regardless of last sync state.
    """
    es = await get_es_client()

    # Get the last synchronized record ID if not doing a full sync
    last_sync_id = 0
    if not force_full_sync:
        try:
            last_sync_info = await es.get(index=".sync-metadata", id="taxon-last-sync")
            last_sync_id = last_sync_info["_source"]["last_taxon_id"]
        except:
            logger.info("No previous sync metadata found for Taxonomic data, performing full sync.")

    # Query for records to synchronize
    batch_size = settings.SYNC_BATCH_SIZE

    while True:
        query = """
        SELECT tt."TaxonID", tt."Genus", tt."Species", tt."Subspecies", tt."FullScientificName", 
               tt."Remarks", tt."FamilyID", f."FamilyName"
        FROM "TaxonomicTable" tt
        LEFT JOIN "Family" f ON tt."FamilyID" = f."FamilyID"
        WHERE tt."TaxonID" > $1
        ORDER BY tt."TaxonID"
        LIMIT $2
        """

        records = await execute_query(query, last_sync_id, batch_size)

        if not records:
            break

        # Prepare batch operations for Elasticsearch
        operations = []
        max_id = 0

        for record in records:
            max_id = max(max_id, record["TaxonID"])

            # Add to bulk operations
            operations.extend([
                {"index": {"_index": "taxon", "_id": str(record["TaxonID"])}},
                record
            ])

        if operations:
            # Execute bulk operations
            response = await es.bulk(body=operations, refresh=True)
            if response["errors"]:
                for item in response["items"]:
                    if "error" in item["index"]:
                        logger.error(f"Bulk error: {item['index']['error']}")
            else:
                logger.info(f"Bulk succeeded: inserted {len(records)} records")

            # Update the last synchronized ID
            await es.index(
                index=".sync-metadata",
                id="taxon-last-sync",
                document={"last_taxon_id": max_id, "last_sync_time": datetime.now().isoformat()}
            )

            logger.info(f"Synchronized {len(records)} Taxonomic records up to ID {max_id}")

        # If we got fewer records than the batch size, we're done
        if len(records) < batch_size:
            break

        # Update the last sync ID for the next batch
        last_sync_id = max_id


async def sync_loan_data(force_full_sync: bool = False):
    """
    Synchronize Loan data from PostgreSQL to Elasticsearch.

    Args:
        force_full_sync: If True, perform a full sync regardless of last sync state.
    """
    es = await get_es_client()

    # Get the last synchronized record ID if not doing a full sync
    last_sync_id = 0
    if not force_full_sync:
        try:
            last_sync_info = await es.get(index=".sync-metadata", id="loan-last-sync")
            last_sync_id = last_sync_info["_source"]["last_loan_id"]
        except:
            logger.info("No previous sync metadata found for Loan data, performing full sync.")

    # Query for records to synchronize
    batch_size = settings.SYNC_BATCH_SIZE

    while True:
        query = """
        SELECT DISTINCT lv."ID", lv."LoanNumber", lv."FullName", lv."AgentID", lv."OrganizationID", 
               lv."TransactionType", lv."LoanDate", lv."DateClosed", lv."Text1", lv."Text2", 
               lv."ShipToAddress", lv."ShipToCity", lv."ShipToState", lv."ShipToZipCode", 
               lv."ShipToCountry", lv."ShipToRemarks", lv."ShipToMethod", lv."Closed"  
        FROM loan_view lv
        WHERE lv."ID" > $1
        ORDER BY lv."ID"
        LIMIT $2
        """

        records = await execute_query(query, last_sync_id, batch_size)

        if not records:
            break

        # Prepare batch operations for Elasticsearch
        operations = []
        max_id = 0

        for record in records:
            max_id = max(max_id, record["ID"])

            # Add to bulk operations
            operations.extend([
                {"index": {"_index": "loan", "_id": str(record["ID"])}},
                record
            ])

        if operations:
            # Execute bulk operations
            response = await es.bulk(body=operations, refresh=True)
            if response["errors"]:
                for item in response["items"]:
                    if "error" in item["index"]:
                        logger.error(f"Bulk error: {item['index']['error']}")
            else:
                logger.info(f"Bulk succeeded: inserted {len(records)} records")

            # Update the last synchronized ID
            await es.index(
                index=".sync-metadata",
                id="loan-last-sync",
                document={"last_loan_id": max_id, "last_sync_time": datetime.now().isoformat()}
            )

            logger.info(f"Synchronized {len(records)} Loan records up to ID {max_id}")

        # If we got fewer records than the batch size, we're done
        if len(records) < batch_size:
            break

        # Update the last sync ID for the next batch
        last_sync_id = max_id


async def sync_lots_data(force_full_sync: bool = False):
    """
    Synchronize Lots (Primary) data from PostgreSQL to Elasticsearch.

    Args:
        force_full_sync: If True, perform a full sync regardless of last sync state.
    """
    es = await get_es_client()

    # Get the last synchronized record ID if not doing a full sync
    last_sync_id = 0
    if not force_full_sync:
        try:
            last_sync_info = await es.get(index=".sync-metadata", id="lots-last-sync")
            last_sync_id = last_sync_info["_source"]["last_primary_id"]
        except:
            logger.info("No previous sync metadata found for Lots data, performing full sync.")

    # Query for records to synchronize
    batch_size = settings.SYNC_BATCH_SIZE

    while True:
        query = """
        SELECT p."PrimaryID", p."CatalogNumber", p."ScientificName", p."PrevNumber", 
               p."DateCataloged", p."JarSize", p."Storage", p."TypeStatus", p."Inventory", 
               p."Remarks", p."Locality1ID", p."CatalogerID", p."TotalNumber", p."TimeStampModified",
               d."TaxonID", tt."FullScientificName", tt."Genus", tt."Species", f."FamilyID", f."FamilyName",
               l."FieldNo", l."LocalityString", l."Country", l."State", l."County", l."Drainage", l."WaterBody"
        FROM "Primary" p
        LEFT JOIN "Determination" d ON p."PrimaryID" = d."PrimaryID" AND d."IsCurrent" = true
        LEFT JOIN "TaxonomicTable" tt ON d."TaxonID" = tt."TaxonID"
        LEFT JOIN "Family" f ON tt."FamilyID" = f."FamilyID"
        LEFT JOIN locality1 l ON p."Locality1ID" = l."Locality1ID"
        WHERE p."PrimaryID" > $1
        ORDER BY p."PrimaryID"
        LIMIT $2
        """

        records = await execute_query(query, last_sync_id, batch_size)

        if not records:
            break

        # Prepare batch operations for Elasticsearch
        operations = []
        max_id = 0

        for record in records:
            max_id = max(max_id, record["PrimaryID"])

            # Add to bulk operations
            operations.extend([
                {"index": {"_index": "lots", "_id": str(record["PrimaryID"])}},
                record
            ])

        if operations:
            # Execute bulk operations
            response = await es.bulk(body=operations, refresh=True)
            if response["errors"]:
                for item in response["items"]:
                    if "error" in item["index"]:
                        logger.error(f"Bulk error: {item['index']['error']}")
            else:
                logger.info(f"Bulk succeeded: inserted {len(records)} records")

            # Update the last synchronized ID
            await es.index(
                index=".sync-metadata",
                id="lots-last-sync",
                document={"last_primary_id": max_id, "last_sync_time": datetime.now().isoformat()}
            )

            logger.info(f"Synchronized {len(records)} Lots records up to ID {max_id}")

        # If we got fewer records than the batch size, we're done
        if len(records) < batch_size:
            break

        # Update the last sync ID for the next batch
        last_sync_id = max_id


async def handle_data_change(table_name: str, record_id: Any, operation: str):
    """
    Handle data changes for real-time synchronization with Elasticsearch.

    Args:
        table_name: The name of the table where the change occurred.
        record_id: The ID or unique identifier of the changed record.
        operation: The operation type ('INSERT', 'UPDATE', 'DELETE').
    """
    if table_name == "ulm_temp":
        if operation == "DELETE":
            es = await get_es_client()
            await es.delete(index="ulm", id=str(record_id), ignore=[404])
        else:  # INSERT or UPDATE
            query = """
            SELECT * FROM ulm_temp WHERE "PrimaryID" = $1
            """
            records = await execute_query(query, record_id)

            if records and len(records) > 0:
                es = await get_es_client()
                await es.index(index="ulm", id=str(record_id), document=records[0])

    elif table_name == "ostelogy":
        if operation == "DELETE":
            es = await get_es_client()
            await es.delete(index="ost", id=record_id, ignore=[404])
        else:  # INSERT or UPDATE
            query = """
            SELECT * FROM ostelogy WHERE "ostcatalog" = $1
            """
            records = await execute_query(query, record_id)

            if records and len(records) > 0:
                es = await get_es_client()
                await es.index(index="ost", id=record_id, document=records[0])

    elif table_name == "locality1":
        if operation == "DELETE":
            es = await get_es_client()
            await es.delete(index="locality", id=str(record_id), ignore=[404])
        else:  # INSERT or UPDATE
            query = """
            SELECT * FROM locality1 WHERE "Locality1ID" = $1
            """
            records = await execute_query(query, record_id)

            if records and len(records) > 0:
                es = await get_es_client()
                await es.index(index="locality", id=str(record_id), document=records[0])

    elif table_name == "TaxonomicTable":
        if operation == "DELETE":
            es = await get_es_client()
            await es.delete(index="taxon", id=str(record_id), ignore=[404])
        else:  # INSERT or UPDATE
            query = """
            SELECT tt."TaxonID", tt."Genus", tt."Species", tt."Subspecies", tt."FullScientificName", 
                   tt."Remarks", tt."FamilyID", f."FamilyName"
            FROM "TaxonomicTable" tt
            LEFT JOIN "Family" f ON tt."FamilyID" = f."FamilyID"
            WHERE tt."TaxonID" = $1
            """
            records = await execute_query(query, record_id)

            if records and len(records) > 0:
                es = await get_es_client()
                await es.index(index="taxon", id=str(record_id), document=records[0])

    elif table_name == "Primary" or table_name == "Determination":
        if operation == "DELETE" and table_name == "Primary":
            es = await get_es_client()
            await es.delete(index="lots", id=str(record_id), ignore=[404])
        else:  # INSERT or UPDATE
            # For Primary table, record_id is PrimaryID
            # For Determination table, record_id is DeterminationID, need to get PrimaryID
            primary_id = record_id
            if table_name == "Determination":
                query = "SELECT \"PrimaryID\" FROM \"Determination\" WHERE \"DeterminationID\" = $1"
                records = await execute_query(query, record_id)
                if records and len(records) > 0:
                    primary_id = records[0]["PrimaryID"]
                else:
                    return  # Determination not found

            query = """
            SELECT p."PrimaryID", p."CatalogNumber", p."ScientificName", p."PrevNumber", 
                p."DateCataloged", p."JarSize", p."Storage", p."TypeStatus", p."Inventory", 
                p."Remarks", p."Locality1ID", p."CatalogerID", p."TotalNumber", p."TimeStampModified",
                d."TaxonID", tt."FullScientificName", tt."Genus", tt."Species", f."FamilyID", f."FamilyName",
                l."FieldNo", l."LocalityString", l."Country", l."State", l."County", l."Drainage", l."WaterBody"
            FROM "Primary" p
            LEFT JOIN "Determination" d ON p."PrimaryID" = d."PrimaryID" AND d."IsCurrent" = true
            LEFT JOIN "TaxonomicTable" tt ON d."TaxonID" = tt."TaxonID"
            LEFT JOIN "Family" f ON tt."FamilyID" = f."FamilyID"
            LEFT JOIN locality1 l ON p."Locality1ID" = l."Locality1ID"
            WHERE p."PrimaryID" = $1
            """
            records = await execute_query(query, primary_id)

            if records and len(records) > 0:
                es = await get_es_client()
                await es.index(index="lots", id=str(primary_id), document=records[0])

    elif table_name.startswith("t2") or table_name.startswith("t1"):  # Loan tables
        if operation == "DELETE" and table_name == "t2":
            es = await get_es_client()
            await es.delete(index="loan", id=str(record_id), ignore=[404])
        else:  # INSERT or UPDATE
            # For t2 table, record_id is ID
            # For t1 table, need to get t2_ID
            loan_id = record_id
            if table_name == "t1":
                query = "SELECT \"t2_ID\" FROM t1 WHERE \"LoanItemID\" = $1"
                records = await execute_query(query, record_id)
                if records and len(records) > 0:
                    loan_id = records[0]["t2_ID"]
                else:
                    return  # Loan item not found

            query = """
            SELECT DISTINCT lv."ID", lv."LoanNumber", lv."FullName", lv."AgentID", lv."OrganizationID", 
                lv."TransactionType", lv."LoanDate", lv."DateClosed", lv."Text1", lv."Text2", 
                lv."ShipToAddress", lv."ShipToCity", lv."ShipToState", lv."ShipToZipCode", 
                lv."ShipToCountry", lv."ShipToRemarks", lv."ShipToMethod", lv."Closed"  
            FROM loan_view lv
            WHERE lv."ID" = $1
            """
            records = await execute_query(query, loan_id)

            if records and len(records) > 0:
                es = await get_es_client()
                await es.index(index="loan", id=str(loan_id), document=records[0])


async def sync_all_data(force_full_sync: bool = False):
    """Synchronize all data types from PostgreSQL to Elasticsearch."""
    await sync_ulm_data(force_full_sync)
    await sync_ost_data(force_full_sync)
    await sync_locality_data(force_full_sync)
    await sync_taxonomic_data(force_full_sync)
    await sync_loan_data(force_full_sync)
    await sync_lots_data(force_full_sync)
    logger.info("Full data synchronization completed")


async def sync_lots_to_unified(force_full_sync: bool = False):
    """
    Synchronize Lots (Primary) data to unified index.
    """
    es = await get_es_client()

    # Get the last synchronized record ID
    last_sync_id = 0
    if not force_full_sync:
        try:
            last_sync_info = await es.get(index=".sync-metadata", id="unified-last-sync")
            last_sync_id = last_sync_info["_source"]["last_primary_id"]
        except:
            logger.info("No previous sync metadata found for unified lots, performing full sync.")

    batch_size = settings.SYNC_BATCH_SIZE

    while True:
        query = """
        SELECT p."PrimaryID", p."CatalogNumber", p."ScientificName", p."PrevNumber", 
               p."DateCataloged", p."JarSize", p."Storage", p."TypeStatus", p."Inventory", 
               p."Remarks", p."Locality1ID", p."CatalogerID", p."TotalNumber", p."TimeStampModified",
               d."TaxonID", tt."FullScientificName", tt."Genus", tt."Species", f."FamilyID", f."FamilyName",
               l."FieldNo", l."LocalityString", l."Country", l."State", l."County", l."Drainage", l."WaterBody",
               l."Continent", l."Lat", l."Lon"
        FROM "Primary" p
        LEFT JOIN "Determination" d ON p."PrimaryID" = d."PrimaryID" AND d."IsCurrent" = true
        LEFT JOIN "TaxonomicTable" tt ON d."TaxonID" = tt."TaxonID"
        LEFT JOIN "Family" f ON tt."FamilyID" = f."FamilyID"
        LEFT JOIN locality1 l ON p."Locality1ID" = l."Locality1ID"
        WHERE p."PrimaryID" > $1
        ORDER BY p."PrimaryID"
        LIMIT $2
        """

        records = await execute_query(query, last_sync_id, batch_size)

        if not records:
            break

        operations = []
        max_id = 0

        for record in records:
            max_id = max(max_id, record["PrimaryID"])

            # Create document for unified index
            unified_doc = {
                "document_id": f"lot-{record['PrimaryID']}",
                "document_type": "lot",
                "source_id": str(record["PrimaryID"]),

                # Lots fields
                "catalog_number": record["CatalogNumber"],
                "prev_number": record["PrevNumber"],
                "total_number": record["TotalNumber"],
                "jar_size": record["JarSize"],
                "storage": record["Storage"],
                "type_status": record["TypeStatus"],
                "inventory": record["Inventory"],
                "date_cataloged": record["DateCataloged"],

                # Locality fields (from joined locality)
                "locality_id": record["Locality1ID"],
                "field_no": record["FieldNo"],
                "locality_string": record["LocalityString"],
                "country": record["Country"],
                "state": record["State"],
                "county": record["County"],
                "drainage": record["Drainage"],
                "water_body": record["WaterBody"],
                "continent": record["Continent"],
                "lat": record["Lat"],
                "lon": record["Lon"],

                # Taxonomic fields (from joined taxonomic)
                "taxon_id": record["TaxonID"],
                "family_id": record["FamilyID"],
                "scientific_name": record["FullScientificName"] or record["ScientificName"],
                "genus": record["Genus"],
                "species": record["Species"],
                "family_name": record["FamilyName"],

                # Content fields
                "remarks": record["Remarks"],

                # Timestamps
                "date_modified": record["TimeStampModified"],
                "date_created": record["DateCataloged"]
            }

            # Build full_text field by combining all text fields
            full_text_parts = []
            for field in ["catalog_number", "prev_number", "scientific_name", "genus", "species",
                          "family_name", "field_no", "locality_string", "country", "state",
                          "county", "drainage", "water_body", "remarks"]:
                if unified_doc.get(field):
                    full_text_parts.append(str(unified_doc[field]))

            unified_doc["full_text"] = " ".join(full_text_parts)

            # Add to bulk operations
            operations.extend([
                {"index": {"_index": "unified", "_id": unified_doc["document_id"]}},
                unified_doc
            ])

        if operations:
            # Execute bulk operations
            response = await es.bulk(body=operations, refresh=True)
            if response["errors"]:
                for item in response["items"]:
                    if "error" in item["index"]:
                        logger.error(f"Bulk error: {item['index']['error']}")
            else:
                logger.info(f"Bulk succeeded: inserted {len(records)} lot records")

            # Update sync metadata
            current_sync_info = await es.get(index=".sync-metadata", id="unified-last-sync")
            current_info = current_sync_info["_source"]

            await es.index(
                index=".sync-metadata",
                id="unified-last-sync",
                document={
                    "last_primary_id": max_id,
                    "last_locality_id": current_info["last_locality_id"],
                    "last_taxon_id": current_info["last_taxon_id"],
                    "last_loan_id": current_info["last_loan_id"],
                    "last_sync_time": datetime.now().isoformat()
                }
            )

        if len(records) < batch_size:
            break

        last_sync_id = max_id


async def sync_locality_to_unified(force_full_sync: bool = False):
    """
    Synchronize Locality data to unified index.
    """
    es = await get_es_client()

    # Get the last synchronized record ID
    last_sync_id = 0
    if not force_full_sync:
        try:
            last_sync_info = await es.get(index=".sync-metadata", id="unified-last-sync")
            last_sync_id = last_sync_info["_source"]["last_locality_id"]
        except:
            logger.info("No previous sync metadata found for unified locality, performing full sync.")

    batch_size = settings.SYNC_BATCH_SIZE

    while True:
        query = """
        SELECT l."Locality1ID", l."FieldNo", l."LocalityString", l."Drainage", l."WaterBody",
               l."Country", l."Continent", l."State", l."County", l."Lat", l."Lon",
               l."StartDate", l."EndDate", l."VerbatimDate", l."Remarks", l."Inventory",
               l."VerbatimCollectors"
        FROM locality1 l
        WHERE l."Locality1ID" > $1
        ORDER BY l."Locality1ID"
        LIMIT $2
        """

        records = await execute_query(query, last_sync_id, batch_size)

        if not records:
            break

        operations = []
        max_id = 0

        for record in records:
            max_id = max(max_id, record["Locality1ID"])

            # Create document for unified index
            unified_doc = {
                "document_id": f"locality-{record['Locality1ID']}",
                "document_type": "locality",
                "source_id": str(record["Locality1ID"]),

                # Locality fields
                "locality_id": record["Locality1ID"],
                "field_no": record["FieldNo"],
                "locality_string": record["LocalityString"],
                "country": record["Country"],
                "state": record["State"],
                "county": record["County"],
                "drainage": record["Drainage"],
                "water_body": record["WaterBody"],
                "continent": record["Continent"],
                "lat": record["Lat"],
                "lon": record["Lon"],
                "inventory": record["Inventory"],

                # Content fields
                "remarks": record["Remarks"],

                # Timestamps
                "date_created": record["StartDate"]
            }

            # Build full_text field by combining all text fields
            full_text_parts = []
            for field in ["field_no", "locality_string", "country", "state", "county",
                          "drainage", "water_body", "continent", "remarks"]:
                if unified_doc.get(field):
                    full_text_parts.append(str(unified_doc[field]))

            unified_doc["full_text"] = " ".join(full_text_parts)

            # Add to bulk operations
            operations.extend([
                {"index": {"_index": "unified", "_id": unified_doc["document_id"]}},
                unified_doc
            ])

        if operations:
            # Execute bulk operations
            response = await es.bulk(body=operations, refresh=True)
            if response["errors"]:
                for item in response["items"]:
                    if "error" in item["index"]:
                        logger.error(f"Bulk error: {item['index']['error']}")
            else:
                logger.info(f"Bulk succeeded: inserted {len(records)} locality records")

            # Update sync metadata
            current_sync_info = await es.get(index=".sync-metadata", id="unified-last-sync")
            current_info = current_sync_info["_source"]

            await es.index(
                index=".sync-metadata",
                id="unified-last-sync",
                document={
                    "last_primary_id": current_info["last_primary_id"],
                    "last_locality_id": max_id,
                    "last_taxon_id": current_info["last_taxon_id"],
                    "last_loan_id": current_info["last_loan_id"],
                    "last_sync_time": datetime.now().isoformat()
                }
            )

        if len(records) < batch_size:
            break

        last_sync_id = max_id


async def sync_taxon_to_unified(force_full_sync: bool = False):
    """
    Synchronize Taxonomic data to unified index.
    """
    es = await get_es_client()

    # Get the last synchronized record ID
    last_sync_id = 0
    if not force_full_sync:
        try:
            last_sync_info = await es.get(index=".sync-metadata", id="unified-last-sync")
            last_sync_id = last_sync_info["_source"]["last_taxon_id"]
        except:
            logger.info("No previous sync metadata found for unified taxon, performing full sync.")

    batch_size = settings.SYNC_BATCH_SIZE

    while True:
        query = """
        SELECT tt."TaxonID", tt."Genus", tt."Species", tt."Subspecies", tt."FullScientificName", 
               tt."Remarks", tt."FamilyID", f."FamilyName"
        FROM "TaxonomicTable" tt
        LEFT JOIN "Family" f ON tt."FamilyID" = f."FamilyID"
        WHERE tt."TaxonID" > $1
        ORDER BY tt."TaxonID"
        LIMIT $2
        """

        records = await execute_query(query, last_sync_id, batch_size)

        if not records:
            break

        operations = []
        max_id = 0

        for record in records:
            max_id = max(max_id, record["TaxonID"])

            # Create document for unified index
            unified_doc = {
                "document_id": f"taxon-{record['TaxonID']}",
                "document_type": "taxon",
                "source_id": str(record["TaxonID"]),

                # Taxonomic fields
                "taxon_id": record["TaxonID"],
                "family_id": record["FamilyID"],
                "scientific_name": record["FullScientificName"],
                "genus": record["Genus"],
                "species": record["Species"],
                "subspecies": record["Subspecies"],
                "family_name": record["FamilyName"],

                # Content fields
                "remarks": record["Remarks"]
            }

            # Build full_text field by combining all text fields
            full_text_parts = []
            for field in ["scientific_name", "genus", "species", "subspecies", "family_name", "remarks"]:
                if unified_doc.get(field):
                    full_text_parts.append(str(unified_doc[field]))

            unified_doc["full_text"] = " ".join(full_text_parts)

            # Add to bulk operations
            operations.extend([
                {"index": {"_index": "unified", "_id": unified_doc["document_id"]}},
                unified_doc
            ])

        if operations:
            # Execute bulk operations
            response = await es.bulk(body=operations, refresh=True)
            if response["errors"]:
                for item in response["items"]:
                    if "error" in item["index"]:
                        logger.error(f"Bulk error: {item['index']['error']}")
            else:
                logger.info(f"Bulk succeeded: inserted {len(records)} taxon records")

            # Update sync metadata
            current_sync_info = await es.get(index=".sync-metadata", id="unified-last-sync")
            current_info = current_sync_info["_source"]

            await es.index(
                index=".sync-metadata",
                id="unified-last-sync",
                document={
                    "last_primary_id": current_info["last_primary_id"],
                    "last_locality_id": current_info["last_locality_id"],
                    "last_taxon_id": max_id,
                    "last_loan_id": current_info["last_loan_id"],
                    "last_sync_time": datetime.now().isoformat()
                }
            )

        if len(records) < batch_size:
            break

        last_sync_id = max_id


def convert_date_format(date_string):
    """
    Convert various date formats to yyyy-MM-dd format for Elasticsearch
    """
    # Strip any trailing spaces
    date_string = date_string.strip()

    # Check if it's already in yyyy-MM-dd format (with or without trailing space)
    if re.match(r'^\d{4}-\d{2}-\d{2}$', date_string):
        return date_string

    # Handle MM/DD/YYYY format (e.g., "10/21/2011")
    if re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', date_string):
        try:
            dt = datetime.strptime(date_string, '%m/%d/%Y')
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            pass

    # Handle MM/DD/YY format (e.g., "12/11/19")
    if re.match(r'^\d{1,2}/\d{1,2}/\d{2}$', date_string):
        try:
            dt = datetime.strptime(date_string, '%m/%d/%y')
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            pass

    # Handle MM-DD-YYYY format (e.g., "6-25-2010")
    if re.match(r'^\d{1,2}-\d{1,2}-\d{4}$', date_string):
        try:
            dt = datetime.strptime(date_string, '%m-%d-%Y')
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            pass

    # Handle day month year format (e.g., "22 Sept 2015")
    month_patterns = [
        '%d %b %Y',  # 7 Oct 2015
        '%d %B %Y'  # 22 September 2015
    ]
    for pattern in month_patterns:
        try:
            dt = datetime.strptime(date_string, pattern)
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            continue

    # If we can't parse it, return the original string and log it
    print(f"Could not parse date: {date_string}")
    return date_string


async def sync_loan_to_unified(force_full_sync: bool = False):
    """
    Synchronize Loan data to unified index.
    """
    es = await get_es_client()

    # Get the last synchronized record ID
    last_sync_id = 0
    if not force_full_sync:
        try:
            last_sync_info = await es.get(index=".sync-metadata", id="unified-last-sync")
            last_sync_id = last_sync_info["_source"]["last_loan_id"]
        except:
            logger.info("No previous sync metadata found for unified loan, performing full sync.")

    batch_size = settings.SYNC_BATCH_SIZE

    while True:
        query = """
        SELECT lv."ID", lv."LoanNumber", lv."FullName", lv."AgentID", lv."OrganizationID", 
               lv."TransactionType", lv."LoanDate", lv."DateClosed", lv."Text1", lv."Text2", 
               lv."ShipToAddress", lv."ShipToCity", lv."ShipToState", lv."ShipToZipCode", 
               lv."ShipToCountry", lv."ShipToRemarks", lv."ShipToMethod", lv."Closed",
               t1."PrimaryID"
        FROM loan_view lv
        LEFT JOIN t1 ON lv."ID" = t1."t2_ID"
        WHERE lv."ID" > $1
        ORDER BY lv."ID"
        LIMIT $2
        """

        records = await execute_query(query, last_sync_id, batch_size)

        if not records:
            break

        operations = []
        max_id = 0
        processed_ids = set()

        for record in records:
            loan_id = record["ID"]
            max_id = max(max_id, loan_id)

            # Skip duplicates (due to join with t1)
            doc_id = f"loan-{loan_id}"
            if doc_id in processed_ids:
                continue

            processed_ids.add(doc_id)

            if record["LoanDate"] != '' and record["LoanDate"] is not None:
                loanDate = convert_date_format(record["LoanDate"])
            else:
                loanDate = '1000-01-01'

            if record["DateClosed"] != '' and record["DateClosed"] is not None:
                dateClosed = convert_date_format(record["DateClosed"])
            else:
                dateClosed = '1000-01-01'

            # Create document for unified index
            unified_doc = {
                "document_id": doc_id,
                "document_type": "loan",
                "source_id": str(loan_id),

                # Loan fields
                "loan_id": loan_id,
                "loan_number": record["LoanNumber"],
                "transaction_type": record["TransactionType"],
                "loan_date": loanDate,
                "date_closed": dateClosed,
                "is_closed": record["Closed"],
                "lender_name": record["FullName"],

                # Location fields (from shipping info)
                "address": record["ShipToAddress"],
                "country": record["ShipToCountry"],
                "state": record["ShipToState"],
                "city": record["ShipToCity"],
                "ship_method": record["ShipToMethod"],

                # Content fields
                "remarks": f"{record['Text1']} {record['Text2']} {record['ShipToRemarks']}".strip()
            }

            # Build full_text field by combining all text fields
            full_text_parts = []
            for field in ["loan_number", "transaction_type", "lender_name","address", "country", "state", "city", "ship_method","remarks"]:
                if unified_doc.get(field):
                    full_text_parts.append(str(unified_doc[field]))

            unified_doc["full_text"] = " ".join(full_text_parts)

            # Add to bulk operations
            operations.extend([
                {"index": {"_index": "unified", "_id": unified_doc["document_id"]}},
                unified_doc
            ])

        if operations:
            # Execute bulk operations
            response = await es.bulk(body=operations, refresh=True)
            if response["errors"]:
                for item in response["items"]:
                    if "error" in item["index"]:
                        logger.error(f"Bulk error: {item['index']['error']}")
            else:
                logger.info(f"Bulk succeeded: inserted {len(processed_ids)} loan records")

            # Update sync metadata
            current_sync_info = await es.get(index=".sync-metadata", id="unified-last-sync")
            current_info = current_sync_info["_source"]

            await es.index(
                index=".sync-metadata",
                id="unified-last-sync",
                document={
                    "last_primary_id": current_info["last_primary_id"],
                    "last_locality_id": current_info["last_locality_id"],
                    "last_taxon_id": current_info["last_taxon_id"],
                    "last_loan_id": max_id,
                    "last_sync_time": datetime.now().isoformat()
                }
            )

        if len(records) < batch_size:
            break

        last_sync_id = max_id


async def sync_unified_data(force_full_sync: bool = False):
    """
    Synchronize all data to the unified index.
    """
    # Ensure the unified index exists
    await ensure_unified_index_exists()

    # Sync all data types to unified index
    await sync_lots_to_unified(force_full_sync)
    await sync_locality_to_unified(force_full_sync)
    await sync_taxon_to_unified(force_full_sync)
    await sync_loan_to_unified(force_full_sync)

    logger.info("Unified index synchronization completed")
