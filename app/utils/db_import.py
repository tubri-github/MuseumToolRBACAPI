import asyncio
import json
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
        """获取下一个批次序列号 - 只查询 primary_temp 表"""
        try:
            current_date = datetime.now().strftime("%Y%m%d")

            # 只查询 primary_temp 表获取最大序号
            query = """
            SELECT COALESCE(MAX(CAST(SUBSTRING(batch_serial_id FROM POSITION('-' IN batch_serial_id) + 1) AS INTEGER)), 0) as max_seq
            FROM primary_temp
            WHERE batch_serial_id LIKE $1 || '-%'
            """

            result = await execute_query(query, current_date)
            max_seq = result[0]['max_seq'] if result else 0
            next_seq = max_seq + 1

            batch_serial_id = f"{current_date}-{next_seq:03d}"
            return batch_serial_id

        except Exception as e:
            print(f"Error generating batch serial ID: {str(e)}")
            raise

    # 在 paste-3.txt 中修改 insert_verbatim_taxonomic_records 方法

    @staticmethod
    async def insert_verbatim_taxonomic_records(records: List[Dict]) -> List[int]:
        """
        批量插入 verbatim_taxonomic 记录，包含匹配信息
        返回插入的 verbatim_taxonomic_id 列表
        """
        if not records:
            return []

        statements = []

        for record in records:
            insert_sql = """
            INSERT INTO verbatim_taxonomic (
                "verbatim_family", "verbatim_genus", "verbatim_species", 
                "match_status", "matched_taxon_id", "match_confidence", "match_details"
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7
            ) RETURNING "verbatim_taxonid"
            """

            # 准备匹配详情的 JSON 数据
            match_details = None
            if record.get("match_info"):
                match_details = json.dumps(record["match_info"])

            params = [
                record.get("verbatim_family"),
                record.get("verbatim_genus"),
                record.get("verbatim_species"),
                record.get("match_status", "no_match"),
                record.get("matched_taxon_id"),
                record.get("match_confidence"),
                match_details
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
                "verbatim_lat", "verbatim_lon","verbatim_collect_date","verbatim_collector","verbatim_fieldno"
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
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
                record.get("verbatim_fieldno"),
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
    async def get_next_temp_catalog_number(batch_serial_id: str) -> str:
        """
        获取下一个临时catalog number
        格式: {batch_serial_id}-{序号}，例如: 20250107-001-001
        """
        try:
            query = """
            SELECT COALESCE(MAX(
                CAST(SUBSTRING("CatalogNumber" FROM LENGTH($1) + 2) AS INTEGER)
            ), 0) as max_seq
            FROM primary_temp
            WHERE "CatalogNumber" LIKE $1 || '-%'
            """

            result = await execute_query(query, batch_serial_id)
            max_seq = result[0]['max_seq'] if result else 0
            next_seq = max_seq + 1

            temp_catalog_number = f"{batch_serial_id}-{next_seq:03d}"
            return temp_catalog_number

        except Exception as e:
            print(f"Error generating temp catalog number: {str(e)}")
            raise

    @staticmethod
    async def insert_primary_temp_records(records: List[Dict], batch_serial_id: str) -> List[int]:
        """
        批量插入 primary_temp 记录（用于 verbatim 导入）
        返回插入的 PrimaryID 列表
        """
        if not records:
            return []

        statements = []

        # 获取当前批次的最大序号
        max_seq_query = """
        SELECT COALESCE(MAX(
            CAST(SUBSTRING("CatalogNumber" FROM LENGTH($1) + 2) AS INTEGER)
        ), 0) as max_seq
        FROM primary_temp
        WHERE "CatalogNumber" LIKE $1 || '-%'
        """

        result = await execute_query(max_seq_query, batch_serial_id)
        current_seq = result[0]['max_seq'] if result else 0

        for i, record in enumerate(records):
            # 生成临时 catalog number
            current_seq += 1
            temp_catalog_number = f"{batch_serial_id}-{current_seq:03d}"

            insert_sql = """
            INSERT INTO primary_temp (
                "CatalogNumber", "verbatim_taxonid", "verbatim_localityid",
                "TotalNumber", "Storage", "JarSize", "PrevNumber",
                "Inventory", "Remarks", "match_type", "review_flag",
                "batch_serial_id", "TimeStampModified", "DateCataloged"
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW(), NOW()
            ) RETURNING "PrimaryID"
            """

            params = [
                temp_catalog_number,
                record.get("verbatim_taxonomic_id"),
                record.get("verbatim_locality_id"),
                record.get("total_number", 1),
                record.get("storage"),
                record.get("jar_size"),
                record.get("prev_number"),
                record.get("inventory"),
                record.get("remarks"),
                record.get("match_type", "no_match"),
                record.get("review_flag", True),
                batch_serial_id
            ]

            statements.append({
                "sql": insert_sql,
                "params": params
            })

        try:
            results = await execute_transaction(statements)
            # 提取返回的PrimaryID (from primary_temp)
            primary_temp_ids = []
            for result in results:
                if result and len(result) > 0:
                    primary_temp_ids.append(result[0]["PrimaryID"])

            return primary_temp_ids

        except Exception as e:
            print(f"Error inserting primary_temp records: {str(e)}")
            raise

    @staticmethod
    async def insert_primary_records(records: List[Dict], batch_serial_id: str) -> List[int]:
        """
        批量插入Primary记录（正式导入，生成正式catalog number）
        返回插入的PrimaryID列表
        """
        if not records:
            return []

        # 获取正式的 catalog numbers
        catalog_numbers = await DatabaseUtils.get_next_catalog_numbers(len(records))

        statements = []

        for i, record in enumerate(records):
            insert_sql = """
            INSERT INTO "Primary" (
                "CatalogNumber", "verbatim_taxonid", "verbatim_localityid",
                "TaxonID", "Locality1ID", "TotalNumber", "Storage",
                "JarSize", "PrevNumber", "Inventory", "Remarks", "match_type",
                "review_flag", "batch_serial_id", "TimeStampModified", "DateCataloged"
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW(), NOW()
            ) RETURNING "PrimaryID"
            """

            params = [
                catalog_numbers[i],
                record.get("verbatim_taxonomic_id"),
                record.get("verbatim_locality_id"),
                record.get("taxon_id"),
                record.get("locality_id"),
                record.get("total_number", 1),
                record.get("storage"),
                record.get("jar_size"),
                record.get("prev_number"),
                record.get("inventory"),
                record.get("remarks"),
                record.get("match_type", "no_match"),
                record.get("review_flag", False),  # 正式导入默认不需要review
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
    async def insert_preparation_temp_records(primary_temp_ids: List[int], total_numbers: List[int], prep_type="Fluid") -> List[int]:
        """
        为 primary_temp 记录批量插入 preparation_temp 记录
        primary_temp_ids: PrimaryID 列表 (from primary_temp table)
        total_numbers: 对应的数量列表
        prep_type: 制备类型，默认为"Fluid"
        """
        if not primary_temp_ids:
            return []

        if len(primary_temp_ids) != len(total_numbers):
            raise ValueError("primary_temp_ids and total_numbers must have the same length")

        statements = []

        for primary_temp_id, count in zip(primary_temp_ids, total_numbers):
            insert_sql = """
            INSERT INTO preparation_temp (
                "PrimaryID", "PreparationType", "Count"
            ) VALUES (
                $1, $2, $3
            ) RETURNING "PreparationID"
            """

            params = [primary_temp_id, prep_type, count]

            statements.append({
                "sql": insert_sql,
                "params": params
            })

        try:
            results = await execute_transaction(statements)
            # 提取返回的PreparationID (from preparation_temp)
            prep_temp_ids = []
            for result in results:
                if result and len(result) > 0:
                    prep_temp_ids.append(result[0]["PreparationID"])

            return prep_temp_ids

        except Exception as e:
            print(f"Error inserting preparation_temp records: {str(e)}")
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
    async def migrate_batch_from_temp_to_primary(batch_serial_id: str) -> Dict[str, Any]:
        """
        将批次数据从 primary_temp 迁移到 Primary 表
        生成正式的 catalog number，并记录映射关系

        Returns:
            Dict with migration results including counts and catalog numbers
        """
        try:
            # 1. 检查批次是否存在且所有记录都已验证
            check_query = """
            SELECT
                COUNT(*) as total_records,
                SUM(CASE WHEN "TaxonID" IS NOT NULL AND "Locality1ID" IS NOT NULL THEN 1 ELSE 0 END) as verified_records,
                SUM(CASE WHEN "overall_verification_status" = 'completed' THEN 1 ELSE 0 END) as fully_verified_records,
                SUM(CASE WHEN final_primary_id IS NOT NULL THEN 1 ELSE 0 END) as already_migrated
            FROM primary_temp
            WHERE batch_serial_id = $1
            """

            check_result = await execute_query(check_query, batch_serial_id)

            if not check_result or check_result[0]['total_records'] == 0:
                raise Exception(f"No records found for batch {batch_serial_id}")

            total = check_result[0]['total_records']
            verified = check_result[0]['verified_records']
            fully_verified = check_result[0]['fully_verified_records']
            already_migrated = check_result[0]['already_migrated']

            if already_migrated > 0:
                raise Exception(f"Batch {batch_serial_id} has already been migrated ({already_migrated} records)")

            # 检查是否所有记录都完成验证（overall_verification_status = 'completed'）
            if fully_verified < total:
                raise Exception(f"Cannot migrate batch {batch_serial_id}. Only {fully_verified}/{total} records have overall_verification_status = 'completed'. All records must be fully verified before migration.")

            # 2. 获取所有待迁移的记录
            fetch_temp_records_query = """
            SELECT * FROM primary_temp
            WHERE batch_serial_id = $1 AND final_primary_id IS NULL
            ORDER BY "PrimaryID"
            """

            temp_records = await execute_query(fetch_temp_records_query, batch_serial_id)

            if not temp_records:
                raise Exception(f"No unmigrated records found for batch {batch_serial_id}")

            # 3. 获取正式的 catalog numbers
            catalog_numbers = await DatabaseUtils.get_next_catalog_numbers(len(temp_records))

            # 4. 批量插入到 Primary 表
            statements = []
            temp_id_to_catalog = {}  # 映射临时ID到正式catalog number

            for i, temp_record in enumerate(temp_records):
                catalog_number = catalog_numbers[i]
                temp_id_to_catalog[temp_record['PrimaryID']] = catalog_number

                insert_primary_sql = """
                INSERT INTO "Primary" (
                    "CatalogNumber", "verbatim_taxonid", "verbatim_localityid",
                    "TaxonID", "Locality1ID", "TotalNumber", "Storage",
                    "JarSize", "PrevNumber", "Inventory", "Remarks", "match_type",
                    "review_flag", "batch_serial_id", "species_verification_status",
                    "locality_verification_status", "record_verification_status",
                    "TimeStampModified", "DateCataloged"
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, NOW(), NOW()
                ) RETURNING "PrimaryID"
                """

                params = [
                    catalog_number,
                    temp_record.get('verbatim_taxonid'),
                    temp_record.get('verbatim_localityid'),
                    temp_record.get('TaxonID'),
                    temp_record.get('Locality1ID'),
                    temp_record.get('TotalNumber', 1),
                    temp_record.get('Storage'),
                    temp_record.get('JarSize'),
                    temp_record.get('PrevNumber'),
                    temp_record.get('Inventory'),
                    temp_record.get('Remarks'),
                    temp_record.get('match_type'),
                    False,  # 正式导入后不需要review
                    batch_serial_id,
                    temp_record.get('species_verification_status'),
                    temp_record.get('locality_verification_status'),
                    temp_record.get('record_verification_status')
                ]

                statements.append({
                    "sql": insert_primary_sql,
                    "params": params
                })

            # 执行插入
            insert_results = await execute_transaction(statements)

            # 5. 更新 primary_temp 表，记录映射关系
            update_statements = []
            primary_id_map = {}  # 映射 primary_temp.PrimaryID 到 Primary.PrimaryID

            for i, temp_record in enumerate(temp_records):
                if insert_results[i] and len(insert_results[i]) > 0:
                    primary_id = insert_results[i][0]['PrimaryID']
                    temp_id = temp_record['PrimaryID']
                    primary_id_map[temp_id] = primary_id

                    update_temp_sql = """
                    UPDATE primary_temp
                    SET final_catalog_number = $1,
                        final_primary_id = $2,
                        "TimeStampModified" = NOW()
                    WHERE "PrimaryID" = $3
                    """

                    update_statements.append({
                        "sql": update_temp_sql,
                        "params": [catalog_numbers[i], primary_id, temp_id]
                    })

            await execute_transaction(update_statements)

            # 6. 迁移 preparation_temp 记录
            fetch_prep_temp_query = """
            SELECT pt.* FROM preparation_temp pt
            INNER JOIN primary_temp prt ON pt."PrimaryID" = prt."PrimaryID"
            WHERE prt.batch_serial_id = $1 AND pt.final_preparation_id IS NULL
            """

            prep_temp_records = await execute_query(fetch_prep_temp_query, batch_serial_id)

            if prep_temp_records:
                prep_statements = []

                for prep_temp in prep_temp_records:
                    primary_temp_id = prep_temp['PrimaryID']
                    if primary_temp_id in primary_id_map:
                        primary_id = primary_id_map[primary_temp_id]

                        insert_prep_sql = """
                        INSERT INTO "Preparation" (
                            "PrimaryID", "PreparationType", "Count"
                        ) VALUES ($1, $2, $3)
                        RETURNING "PreparationID"
                        """

                        prep_statements.append({
                            "sql": insert_prep_sql,
                            "params": [primary_id, prep_temp.get('PreparationType', 'Fluid'), prep_temp.get('Count', 1)]
                        })

                prep_results = await execute_transaction(prep_statements)

                # 更新 preparation_temp
                update_prep_statements = []
                for i, prep_temp in enumerate(prep_temp_records):
                    if prep_results[i] and len(prep_results[i]) > 0:
                        prep_id = prep_results[i][0]['PreparationID']

                        update_prep_sql = """
                        UPDATE preparation_temp
                        SET final_preparation_id = $1,
                            "TimeStampModified" = NOW()
                        WHERE "PreparationID" = $2
                        """

                        update_prep_statements.append({
                            "sql": update_prep_sql,
                            "params": [prep_id, prep_temp['PreparationID']]
                        })

                await execute_transaction(update_prep_statements)

            return {
                "batch_serial_id": batch_serial_id,
                "migrated_count": len(temp_records),
                "preparation_migrated_count": len(prep_temp_records) if prep_temp_records else 0,
                "catalog_number_range": {
                    "start": catalog_numbers[0],
                    "end": catalog_numbers[-1]
                },
                "timestamp": datetime.now().isoformat()
            }

        except Exception as e:
            print(f"Error migrating batch from temp to primary: {str(e)}")
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