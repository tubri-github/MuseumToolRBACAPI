"""
TaxonRank database connection module.

Provides async connection helpers for the TaxonRank database (separate from the main DB),
used for synonym resolution via the taxa table.
"""

import asyncpg
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Dict, List, Any, Optional
import json

from app.core.config import settings


def is_taxon_db_configured() -> bool:
    """Check if the TaxonRank database is configured."""
    return bool(settings.TAXON_DB_HOST and settings.TAXON_DB_NAME)


async def get_taxon_connection():
    """Establish a connection to the TaxonRank PostgreSQL database."""
    if not is_taxon_db_configured():
        raise RuntimeError("TaxonRank database not configured. Set TAXON_DB_* in .env")

    conn = await asyncpg.connect(
        host=settings.TAXON_DB_HOST,
        port=settings.TAXON_DB_PORT,
        user=settings.TAXON_DB_USER,
        password=settings.TAXON_DB_PASSWORD,
        database=settings.TAXON_DB_NAME
    )

    await conn.set_type_codec(
        'json', encoder=json.dumps, decoder=json.loads, schema='pg_catalog'
    )
    await conn.set_type_codec(
        'jsonb', encoder=json.dumps, decoder=json.loads, schema='pg_catalog'
    )

    return conn


@asynccontextmanager
async def get_taxon_db() -> AsyncGenerator[asyncpg.Connection, None]:
    """Context manager for TaxonRank database connection."""
    conn = await get_taxon_connection()
    try:
        yield conn
    finally:
        await conn.close()


async def execute_taxon_query(query: str, *args) -> List[Dict[str, Any]]:
    """Execute a query against the TaxonRank database and return results as list of dicts."""
    async with get_taxon_db() as conn:
        records = await conn.fetch(query, *args)
        return [dict(record) for record in records]


async def execute_taxon_single_query(query: str, *args) -> Optional[Dict[str, Any]]:
    """Execute a query against the TaxonRank database and return a single record."""
    async with get_taxon_db() as conn:
        record = await conn.fetchrow(query, *args)
        return dict(record) if record else None