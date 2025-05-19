import asyncio
from datetime import datetime
from typing import List, Dict, Any, Optional
from app.db.database import execute_query, execute_mutation, execute_transaction


class DatabaseUtils:
    """数据库操作工具类"""

    @staticmethod
    async def get_next_catalog_numbers(count: int) -> List[int]:
        """
        获取一批下一个可用的编目号，并锁定以防止冲突
        使用事务确保在高并发下不会重复
        """
        try:
            # 准备事务中的SQL语句
            statements = [
                {
                    "sql": """
                    LOCK TABLE "Primary" IN ACCESS EXCLUSIVE MODE;
                    """,
                    "params": []
                },
                {
                    "sql": """
                    CREATE TEMP TABLE IF NOT EXISTS temp_catalog_range (
                        start_number INTEGER,
                        end_number INTEGER,
                        allocated_at TIMESTAMP DEFAULT NOW()
                    );
                    """,
                    "params": []
                },
                {
                    "sql": """
                    INSERT INTO temp_catalog_range (start_number, end_number)
                    SELECT COALESCE(MAX("CatalogNumber"), 0) + 1, COALESCE(MAX("CatalogNumber"), 0) + $1
                    FROM "Primary";
                    """,
                    "params": [count]
                },
                {
                    "sql": """
                    SELECT start_number, end_number 
                    FROM temp_catalog_range 
                    ORDER BY allocated_at DESC 
                    LIMIT 1;
                    """,
                    "params": []
                }
            ]

            # 执行事务
            results = await execute_transaction(statements)

            # 获取最后一个语句的结果（SELECT 查询结果）
            result = results[3]  # 第四个查询的结果

            if not result:
                raise Exception("Failed to allocate catalog numbers")

            start_num = result[0]['start_number']
            end_num = result[0]['end_number']

            # 生成编号列表
            catalog_numbers = list(range(start_num, end_num + 1))

            return catalog_numbers

        except Exception as e:
            print(f"Error allocating catalog numbers: {str(e)}")
            raise

    @staticmethod
    async def get_next_batch_serial_id() -> str:
        """获取下一个批次序列号"""
        try:
            current_date = datetime.now().strftime("%Y%m%d")

            query = """
            SELECT COALESCE(MAX(CAST(SUBSTRING(batch_serial_id FROM LENGTH(batch_serial_id) - 2) AS INTEGER)), 0) as max_seq
            FROM "Primary"
            WHERE batch_serial_id LIKE $1 || '%'
            """

            result = await execute_query(query, current_date)
            max_seq = result[0]['max_seq'] if result else 0
            next_seq = max_seq + 1

            batch_serial_id = f"{current_date}-{next_seq:03d}"
            return batch_serial_id

        except Exception as e:
            print(f"Error generating batch serial ID: {str(e)}")
            raise

    @staticmethod
    async def insert_verbatim_taxonomic_records(records: List[Dict]) -> List[int]:
        """
        批量插入 verbatim_taxonomic 记录
        返回插入的 verbatim_taxonomic_id 列表
        """
        if not records:
            return []

        statements = []

        for record in records:
            insert_sql = """
            INSERT INTO verbatim_taxonomic (
                "verbatim_family", "verbatim_genus", "verbatim_species"
            ) VALUES (
                $1, $2, $3
            ) RETURNING "verbatim_taxonid"
            """

            params = [
                record.get("verbatim_family"),
                record.get("verbatim_genus"),
                record.get("verbatim_species")
            ]

            statements.append({
                "sql": insert_sql,
                "params": params
            })

        try:
            results = await execute_transaction(statements)
            # 提取返回的VerbatimTaxonomicID
            verbatim_taxonomic_ids = []
            for result in results:
                if result and len(result) > 0:
                    verbatim_taxonomic_ids.append(result[0]["verbatim_taxonid"])

            return verbatim_taxonomic_ids

        except Exception as e:
            print(f"Error inserting verbatim taxonomic records: {str(e)}")
            raise

    @staticmethod
    async def insert_verbatim_locality_records(records: List[Dict]) -> List[int]:
        """
        批量插入 verbatim_locality 记录
        返回插入的 verbatim_locality_id 列表
        """
        if not records:
            return []

        statements = []

        for record in records:
            insert_sql = """
            INSERT INTO verbatim_locality (
                "verbatim_locality_string", "verbatim_drainage", "verbatim_country", 
                "verbatim_state", "verbatim_county", "verbatim_waterbody", 
                "verbatim_lat", "verbatim_lon","verbatim_collect_date","verbatim_collector"
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
            ) RETURNING "verbatim_localityid"
            """

            params = [
                record.get("verbatim_locality_string"),
                record.get("verbatim_drainage"),
                record.get("verbatim_country"),
                record.get("verbatim_state"),
                record.get("verbatim_county"),
                record.get("verbatim_waterbody"),
                record.get("verbatim_latitude"),
                record.get("verbatim_longitude"),
                record.get("verbatim_collect_date"),
                record.get("verbatim_collector"),
            ]

            statements.append({
                "sql": insert_sql,
                "params": params
            })

        try:
            results = await execute_transaction(statements)
            # 提取返回的VerbatimLocalityID
            verbatim_locality_ids = []
            for result in results:
                if result and len(result) > 0:
                    verbatim_locality_ids.append(result[0]["verbatim_localityid"])

            return verbatim_locality_ids

        except Exception as e:
            print(f"Error inserting verbatim locality records: {str(e)}")
            raise

    @staticmethod
    async def insert_primary_records(records: List[Dict], batch_serial_id: str) -> List[int]:
        """
        批量插入Primary记录
        返回插入的PrimaryID列表
        """
        if not records:
            return []

        statements = []

        for record in records:
            insert_sql = """
            INSERT INTO "Primary" (
                "CatalogNumber", "verbatim_taxonid", "verbatim_localityid",
               "TotalNumber", "Storage",
                "JarSize", "PrevNumber", "Inventory", "Remarks", "match_type",
                "review_flag", "batch_serial_id", "TimeStampModified", "DateCataloged"
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW(), NOW()
            ) RETURNING "PrimaryID"
            """

            params = [
                record.get("catalog_number"),
                record.get("verbatim_taxonomic_id"),
                record.get("verbatim_locality_id"),
                record.get("total_number", 1),
                record.get("storage"),
                record.get("jar_size"),
                record.get("prev_number"),
                record.get("inventory"),
                record.get("remarks"),
                record.get("match_type", "no_match"),
                record.get("review_flag", True),  # 默认需要人工验证
                batch_serial_id
            ]

            statements.append({
                "sql": insert_sql,
                "params": params
            })

        try:
            results = await execute_transaction(statements)
            # 提取返回的PrimaryID
            primary_ids = []
            for result in results:
                if result and len(result) > 0:
                    primary_ids.append(result[0]["PrimaryID"])

            return primary_ids

        except Exception as e:
            print(f"Error inserting primary records: {str(e)}")
            raise

    @staticmethod
    async def insert_preparation_records(primary_ids: List[int], total_numbers: List[int], prep_type="Fluid") -> List[
        int]:
        """
        为Primary记录批量插入Preparation记录
        primary_ids: Primary记录的ID列表
        total_numbers: 对应的数量列表
        prep_type: 制备类型，默认为"Fluid"
        """
        if not primary_ids:
            return []

        if len(primary_ids) != len(total_numbers):
            raise ValueError("primary_ids and total_numbers must have the same length")

        statements = []

        for primary_id, count in zip(primary_ids, total_numbers):
            insert_sql = """
            INSERT INTO "Preparation" (
                "PrimaryID", "PreparationType", "Count"
            ) VALUES (
                $1, $2, $3
            ) RETURNING "PreparationID"
            """

            params = [primary_id, prep_type, count]

            statements.append({
                "sql": insert_sql,
                "params": params
            })

        try:
            results = await execute_transaction(statements)
            # 提取返回的PreparationID
            prep_ids = []
            for result in results:
                if result and len(result) > 0:
                    prep_ids.append(result[0]["PreparationID"])

            return prep_ids

        except Exception as e:
            print(f"Error inserting preparation records: {str(e)}")
            raise

    @staticmethod
    async def log_import_activity(file_id: str, action: str, details: Dict, user_id: Optional[int] = None):
        """记录导入活动到系统日志"""
        log_query = """
        INSERT INTO system_logs(action_type, action_details, created_at, user_id) 
        VALUES($1, $2, $3, $4)
        """

        import json
        await execute_mutation(
            log_query,
            action,
            json.dumps({**details, "fileId": file_id}),
            datetime.now(),
            user_id
        )

    @staticmethod
    async def get_import_statistics(days: int = 30) -> Dict:
        """获取导入统计信息"""
        query = """
        SELECT 
            action_details::jsonb->>'status' as status,
            COUNT(*) as count,
            SUM(CAST(action_details::jsonb->>'totalRecords' AS INTEGER)) as total_records,
            SUM(CAST(action_details::jsonb->>'importedCount' AS INTEGER)) as imported_records
        FROM system_logs
        WHERE action_type = 'batch_import'
        AND created_at >= NOW() - INTERVAL '%s days'
        GROUP BY action_details::jsonb->>'status'
        """

        result = await execute_query(query % days)

        stats = {
            "completed": {"count": 0, "total_records": 0, "imported_records": 0},
            "failed": {"count": 0, "total_records": 0, "imported_records": 0},
            "in_progress": {"count": 0, "total_records": 0, "imported_records": 0}
        }

        for row in result or []:
            status = row.get("status", "unknown")
            if status in stats:
                stats[status] = {
                    "count": row.get("count", 0),
                    "total_records": row.get("total_records", 0) or 0,
                    "imported_records": row.get("imported_records", 0) or 0
                }

        return stats

    @staticmethod
    async def cleanup_old_logs(days: int = 90):
        """清理超过指定天数的导入日志"""
        query = """
        DELETE FROM system_logs
        WHERE action_type = 'batch_import'
        AND created_at < NOW() - INTERVAL '%s days'
        """

        try:
            await execute_mutation(query % days)
            print(f"Cleaned up import logs older than {days} days")
        except Exception as e:
            print(f"Error cleaning up logs: {str(e)}")

    @staticmethod
    async def validate_database_consistency():
        """验证数据库一致性"""
        checks = []

        # 检查Primary表中的orphaned记录
        orphan_primary_query = """
        SELECT COUNT(*) as count
        FROM "Primary" p
        LEFT JOIN preparation prep ON p."PrimaryID" = prep."PrimaryID"
        WHERE prep."PrimaryID" IS NULL
        """

        result = await execute_query(orphan_primary_query)
        orphan_count = result[0]["count"] if result else 0
        checks.append({
            "check": "orphaned_primary_records",
            "count": orphan_count,
            "status": "warning" if orphan_count > 0 else "ok"
        })

        # 检查重复的CatalogNumber
        duplicate_catalog_query = """
        SELECT "CatalogNumber", COUNT(*) as count
        FROM "Primary"
        WHERE "CatalogNumber" IS NOT NULL
        GROUP BY "CatalogNumber"
        HAVING COUNT(*) > 1
        """

        result = await execute_query(duplicate_catalog_query)
        duplicate_count = len(result) if result else 0
        checks.append({
            "check": "duplicate_catalog_numbers",
            "count": duplicate_count,
            "status": "error" if duplicate_count > 0 else "ok"
        })

        # 检查Verbatim表的关联
        verbatim_taxonomic_orphan_query = """
        SELECT COUNT(*) as count
        FROM "Primary" p
        WHERE p."verbatim_taxonid" IS NOT NULL 
        AND NOT EXISTS (
            SELECT 1 FROM verbatim_taxonomic vt 
            WHERE vt."verbatim_taxonid" = p."verbatim_taxonid"
        )
        """

        result = await execute_query(verbatim_taxonomic_orphan_query)
        verbatim_orphan_count = result[0]["count"] if result else 0
        checks.append({
            "check": "orphaned_verbatim_taxonomic_references",
            "count": verbatim_orphan_count,
            "status": "error" if verbatim_orphan_count > 0 else "ok"
        })

        return {
            "timestamp": datetime.now().isoformat(),
            "checks": checks,
            "overall_status": "error" if any(c["status"] == "error" for c in checks) else "ok"
        }