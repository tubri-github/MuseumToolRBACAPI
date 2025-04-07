import asyncpg
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Dict, List, Any, Optional
import json

from app.core.config import settings


async def get_connection():
    """Establish a connection to the PostgreSQL database."""
    conn = await asyncpg.connect(
        host=settings.POSTGRES_HOST,
        port=settings.POSTGRES_PORT,
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD,
        database=settings.POSTGRES_DB
    )

    # Register custom types (like JSON support)
    await conn.set_type_codec(
        'json',
        encoder=json.dumps,
        decoder=json.loads,
        schema='pg_catalog'
    )
    await conn.set_type_codec(
        'jsonb',
        encoder=json.dumps,
        decoder=json.loads,
        schema='pg_catalog'
    )

    return conn


@asynccontextmanager
async def get_db() -> AsyncGenerator[asyncpg.Connection, None]:
    """Context manager for database connection."""
    conn = await get_connection()
    try:
        yield conn
    finally:
        await conn.close()


async def execute_query(query: str, *args) -> List[Dict[str, Any]]:
    """Execute a query and return the results as a list of dictionaries."""
    async with get_db() as conn:
        records = await conn.fetch(query, *args)
        return [dict(record) for record in records]


async def execute_single_query(query: str, *args) -> Optional[Dict[str, Any]]:
    """Execute a query and return a single record as a dictionary."""
    async with get_db() as conn:
        record = await conn.fetchrow(query, *args)
        return dict(record) if record else None


async def execute_mutation(query: str, *args) -> int:
    """Execute a mutation query (INSERT, UPDATE, DELETE) and return the number of affected rows."""
    async with get_db() as conn:
        status = await conn.execute(query, *args)
        if status.startswith("INSERT") or status.startswith("UPDATE") or status.startswith("DELETE"):
            parts = status.split()
            try:
                return int(parts[1])
            except (IndexError, ValueError):
                return 0
        return 0


async def execute_proc(proc_name: str, *args) -> Any:
    """Execute a stored procedure."""
    async with get_db() as conn:
        result = await conn.fetchval(f"SELECT {proc_name}({','.join(['$' + str(i + 1) for i in range(len(args))])})",
                                     *args)
        return result


async def execute_paginated_query(
        query: str,
        params: List[Any] = None,
        page: int = 1,
        page_size: int = 10
) -> Dict[str, Any]:
    """
    执行分页查询并返回结果与分页元数据。

    Args:
        query: 主查询 SQL
        params: 查询参数
        page: 页码 (从1开始)
        page_size: 每页记录数

    Returns:
        包含查询结果和分页元数据的字典
    """
    if params is None:
        params = []

    # 计算偏移量
    offset = (page - 1) * page_size

    # 拆分查询来获取总记录数
    # 假设查询格式为 "SELECT xxx FROM xxx WHERE xxx"
    query_parts = query.lower().split('from', 1)

    if len(query_parts) > 1:
        # 构建计数查询
        count_query = f"SELECT COUNT(*) FROM{query_parts[1]}"

        # 如果有 ORDER BY 子句，需要移除
        order_by_pos = count_query.lower().find('order by')
        if order_by_pos > -1:
            count_query = count_query[:order_by_pos]

        # 移除任何 OFFSET 和 LIMIT 子句
        offset_pos = count_query.lower().find('offset')
        if offset_pos > -1:
            count_query = count_query[:offset_pos]

        limit_pos = count_query.lower().find('limit')
        if limit_pos > -1:
            count_query = count_query[:limit_pos]
    else:
        # 如果无法解析查询，使用原始查询，但可能会有问题
        count_query = f"SELECT COUNT(*) FROM ({query}) as subquery"

    # 添加分页
    paginated_query = f"{query} OFFSET {offset} LIMIT {page_size}"

    # 执行查询
    async with get_db() as conn:
        # 获取总记录数
        count_record = await conn.fetchval(count_query, *params)
        total = int(count_record) if count_record is not None else 0

        # 获取分页数据
        records = await conn.fetch(paginated_query, *params)
        record_list = [dict(record) for record in records]

        # 计算总页数
        total_pages = (total + page_size - 1) // page_size if total > 0 else 0

        return {
            "code": 20000,
            "data": {
                "items": record_list,
                "total": total,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_prev": page > 1
            }
        }


async def execute_paginated_query_with_count(
        main_query: str,
        count_query: str,
        params: List[Any] = None,
        page: int = 1,
        page_size: int = 10
) -> Dict[str, Any]:
    """
    使用单独的计数查询执行分页查询，适用于复杂查询情况。

    Args:
        main_query: 主查询 SQL
        count_query: 计数查询 SQL
        params: 查询参数
        page: 页码 (从1开始)
        page_size: 每页记录数

    Returns:
        包含查询结果和分页元数据的字典
    """
    if params is None:
        params = []

    # 计算偏移量
    offset = (page - 1) * page_size

    # 添加分页
    paginated_query = f"{main_query} OFFSET {offset} LIMIT {page_size}"

    # 执行查询
    async with get_db() as conn:
        # 获取总记录数
        count_record = await conn.fetchval(count_query, *params)
        total = int(count_record) if count_record is not None else 0

        # 获取分页数据
        records = await conn.fetch(paginated_query, *params)
        record_list = [dict(record) for record in records]

        # 计算总页数
        total_pages = (total + page_size - 1) // page_size if total > 0 else 0

        return {
            "code": 20000,
            "data": {
                "items": record_list,
                "total": total,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "has_next": page < total_pages,
                "has_prev": page > 1
            }
        }