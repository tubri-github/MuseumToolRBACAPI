from fastapi import APIRouter, Query, Depends, HTTPException, status
from typing import Dict, Any, List

from app.db.database import execute_query

router = APIRouter()


@router.get("/recentadded", response_model=Dict[str, Any])
async def get_recent_added_list():
    """
    Get recently added items list.
    Mirrors the original getRecentAddedList function.
    """
    query = """
    SELECT to_char(rc.createdtime, 'YYYY-MM-dd HH24:MI:SS') as timestamp,
           rc.type, rc.typeidforuser as id
    FROM recentadded rc
    ORDER BY rc.createdtime DESC
    LIMIT 20
    """

    records = await execute_query(query)

    return {
        "code": 20000,
        "data": {
            "items": records,
            "total": len(records)
        }
    }