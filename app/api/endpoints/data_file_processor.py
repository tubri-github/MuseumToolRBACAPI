import os
import uuid
import json
import shutil

import numpy as np
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List, Optional

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, Query, BackgroundTasks
from fastapi.encoders import jsonable_encoder
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.db.database import execute_query, execute_mutation
from app.utils.species_validation import SpeciesNameValidator, validate_scientific_name
from app.utils.validation import ImportValidationUtils
from app.utils.db_import import DatabaseUtils
from app.services.name_decision_service import NameDecisionService, name_key
from app.services.taxon_reference_check import (
    build_suggestion_warning,
    family_reference_warning, family_suggestion_warning,
)
from app.services.synonym_service import SynonymService

_synonym_service = SynonymService()

router = APIRouter()


# Pydantic模型
class ImportMappingModel(BaseModel):
    fileId: str
    mappings: Dict[str, str]
    fieldNoOption: Optional[str] = None


class ConfirmImportModel(BaseModel):
    fileId: str
    verbatimImport: Optional[bool] = False  # 是否使用verbatim导入方式


class ResponseModel(BaseModel):
    code: int
    data:  dict = {}
    message: Optional[str] = None


class PaginationParams:
    def __init__(
            self,
            page: int = Query(1, ge=1, description="页码，从1开始"),
            page_size: int = Query(10, ge=1, le=100, description="每页记录数")
    ):
        self.page = page
        self.page_size = page_size


# 内存存储（生产环境建议使用Redis）
file_storage = {}
mapping_storage = {}
validation_storage = {}
import_status = {}

# 工具类实例
species_validator = SpeciesNameValidator()
validation_utils = ImportValidationUtils()
db_utils = DatabaseUtils()


# 存储和获取函数
async def store_import_file_info(file_id: str, file_path: str, filename: str, columns: List[str], row_count: int):
    """存储上传文件信息"""
    file_storage[file_id] = {
        "filePath": file_path,
        "fileName": filename,
        "columns": columns,
        "rowCount": row_count,
        "createdAt": datetime.now().isoformat()
    }
    return file_id


async def get_import_file_info(file_id: str):
    """获取文件信息"""
    return file_storage.get(file_id)


async def store_mapping_info(file_id: str, mappings: Dict[str, str]):
    """存储字段映射信息"""
    mapping_storage[file_id] = mappings
    return file_id


async def get_mapping_info(file_id: str):
    """获取字段映射信息"""
    return mapping_storage.get(file_id)


async def store_validation_result(file_id: str, result: Dict):
    """存储验证结果"""
    validation_storage[file_id] = result
    return file_id


async def get_validation_result(file_id: str):
    """获取验证结果"""
    return validation_storage.get(file_id)


async def persist_batch_source_file(file_info: Dict, batch_serial_id: str,
                                    import_mode: str, row_count: Optional[int] = None):
    """Retain the original uploaded file for a successfully imported batch and register
    it (batch_source_file) so the batch detail page can show + download it.

    Copies temp/<id>_<name> -> storage/batch_sources/<batch_serial_id><ext> (outside the
    temp dir that gets cleaned). Best-effort: any failure is logged, never breaks import.
    """
    try:
        src = file_info.get("filePath")
        name = file_info.get("fileName") or "source"
        if not src or not os.path.exists(src):
            print(f"persist_batch_source_file: source missing for {batch_serial_id} ({src})")
            return
        os.makedirs("storage/batch_sources", exist_ok=True)
        ext = os.path.splitext(name)[1] or os.path.splitext(src)[1]
        dest = f"storage/batch_sources/{batch_serial_id}{ext}"
        shutil.copy2(src, dest)
        await execute_mutation(
            """
            INSERT INTO batch_source_file
                (batch_serial_id, file_name, stored_path, import_mode, row_count, uploaded_at)
            VALUES ($1, $2, $3, $4, $5, NOW())
            ON CONFLICT (batch_serial_id) DO UPDATE SET
                file_name = EXCLUDED.file_name,
                stored_path = EXCLUDED.stored_path,
                import_mode = EXCLUDED.import_mode,
                row_count = EXCLUDED.row_count,
                uploaded_at = NOW()
            """,
            batch_serial_id, name, dest, import_mode, row_count,
        )
    except Exception as e:  # best-effort: must not break the import
        print(f"persist_batch_source_file failed for {batch_serial_id}: {e}")


async def store_import_status(file_id: str, status_data: Dict):
    """存储导入状态"""
    import_status[file_id] = status_data

    # 记录到系统日志
    await db_utils.log_import_activity(
        file_id,
        "batch_import",
        status_data,
        status_data.get("userId")
    )


async def get_import_status(file_id: str):
    """获取导入状态"""
    status = import_status.get(file_id)
    if not status:
        # 从数据库查询
        query = """
        SELECT action_details, created_at 
        FROM system_logs 
        WHERE action_type = 'batch_import' 
        AND action_details::jsonb->>'fileId' = $1
        ORDER BY created_at DESC 
        LIMIT 1
        """
        result = await execute_query(query, file_id)
        if result:
            status = json.loads(result[0]["action_details"])
            status["timestamp"] = result[0]["created_at"].isoformat()

    return status


# API端点
@router.post("/upload", response_model=ResponseModel)
async def upload_file(file: UploadFile = File(...)):
    """
    上传ULM数据文件并获取列信息
    支持CSV和Excel格式
    """
    filename = file.filename
    if not (filename.endswith('.csv') or filename.endswith('.xlsx') or filename.endswith('.xls')):
        return ResponseModel(
            code=40000,
            data={},
            message="Invalid file format. Only CSV and Excel files are supported."
        )

    try:
        # 创建临时目录
        os.makedirs("temp", exist_ok=True)

        content = await file.read()
        file_id = str(uuid.uuid4())
        temp_path = f"temp/{file_id}_{filename}"

        with open(temp_path, "wb") as f:
            f.write(content)

        # 读取文件并获取列信息
        if filename.endswith('.csv'):
            df = pd.read_csv(temp_path)
        else:
            df = pd.read_excel(temp_path)

        columns = df.columns.tolist()
        row_count = len(df)

        # 存储文件信息
        await store_import_file_info(file_id, temp_path, filename, columns, row_count)

        return ResponseModel(
            code=20000,
            data={
                "fileId": file_id,
                "fileName": filename,
                "columns": columns,
                "rowCount": row_count,
                # "preview": df.head(5).to_dict('records')  # 提供前5行预览
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# 在 paste-2.txt 中修改 validate_mapping 函数
# 替换原来的物种匹配部分

@router.post("/validateMapping", response_model=ResponseModel)
async def validate_mapping(mapping_data: ImportMappingModel):
    """
    验证字段映射和数据
    包括物种名称匹配、数据格式验证等
    """
    try:
        file_id = mapping_data.fileId
        mappings = mapping_data.mappings

        # 存储映射信息
        await store_mapping_info(file_id, mappings)

        file_info = await get_import_file_info(file_id)
        if not file_info:
            return ResponseModel(
                code=404,
                data={},
                message="File not found. Please upload again."
            )

        # 读取文件
        file_path = file_info["filePath"]
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            df = pd.read_excel(file_path)

        # 验证结果存储
        validation_result = {
            "fileId": file_id,
            "totalRecords": len(df),
            "issues": [],
            "speciesMatching": {},
            "validationSummary": {}
        }

        # 1. 验证必需字段
        required_fields = ["sourceId", "prevNumber"]
        missing_fields = [field for field in required_fields if field not in mappings or not mappings[field]]

        if missing_fields:
            return ResponseModel(
                code=40000,
                data={},
                message=f"Missing required field mappings: {', '.join(missing_fields)}"
            )

        # 1b. Source unique ID 校验：本文件内唯一 + 非空 + 整数（存为 source_primary_id 做溯源）。
        #     注意：是 batch 内唯一，不是数据库列唯一约束。
        src_col = mappings.get("sourceId")
        if not src_col or src_col not in df.columns:
            return ResponseModel(code=40000, data={},
                message=f"Mapped Source unique ID column '{src_col}' not found in the file.")
        src_series = df[src_col]
        blank_cnt = int((src_series.isna() | (src_series.astype(str).str.strip() == "")).sum())
        if blank_cnt:
            return ResponseModel(code=40000, data={},
                message=f"Source unique ID '{src_col}' has {blank_cnt} empty value(s); every row needs one.")
        try:
            src_series.astype(str).str.strip().astype(float).astype("int64")
        except (ValueError, TypeError):
            return ResponseModel(code=40000, data={},
                message=f"Source unique ID '{src_col}' must contain integer values.")
        dup_vals = src_series[src_series.duplicated(keep=False)]
        if len(dup_vals):
            sample = ", ".join(map(str, list(dict.fromkeys(dup_vals.tolist()))[:5]))
            return ResponseModel(code=40000, data={},
                message=f"Source unique ID '{src_col}' must be unique within the file. Duplicates: {sample}")

        # 2. 批量匹配分类学名称 - 使用物种验证器
        taxonomic_data = []
        for idx, row in df.iterrows():
            family = row.get(mappings["family"]) if mappings.get("family") else ""
            genus = row.get(mappings["genus"]) if mappings.get("genus") else ""
            species = row.get(mappings["species"]) if mappings.get("species") else ""

            taxonomic_data.append({
                "row_index": idx,
                "family": str(family) if pd.notna(family) else "",
                "genus": str(genus) if pd.notna(genus) else "",
                "species": str(species) if pd.notna(species) else ""
            })

        if taxonomic_data:
            # 执行批量匹配
            matching_result = await species_validator.batch_match_taxonomic_names(
                [{"family": item["family"], "genus": item["genus"], "species": item["species"]}
                 for item in taxonomic_data]
            )

            if "error" not in matching_result:
                # 重新整理匹配结果，添加原始行索引和详细信息
                matches = matching_result["matches"]
                for match_type in matches:
                    for match in matches[match_type]:
                        original_idx = match["import_index"]
                        match["row_index"] = taxonomic_data[original_idx]["row_index"]

                        # 添加详细的匹配信息供后续使用
                        match["original_family"] = taxonomic_data[original_idx]["family"]
                        match["original_genus"] = taxonomic_data[original_idx]["genus"]
                        match["original_species"] = taxonomic_data[original_idx]["species"]

                validation_result["speciesMatching"] = matching_result

        # 3. 验证数值字段
        numeric_fields = ["totalNumber"]
        for field_key in numeric_fields:
            if field_key in mappings and mappings[field_key]:
                column = mappings[field_key]
                invalid_numeric = []

                for idx, value in df[column].items():
                    if pd.notna(value):
                        try:
                            float(value)
                        except (ValueError, TypeError):
                            invalid_numeric.append(idx)

                if invalid_numeric:
                    validation_result["issues"].append({
                        "type": f"numeric_{field_key.lower()}",
                        "description": f"Invalid numeric value in {field_key}",
                        "count": len(invalid_numeric),
                        "examples": [str(df.iloc[i][column]) for i in invalid_numeric[:3]],
                        "invalidIndices": invalid_numeric
                    })

        # 4. 验证日期格式
        if "collectionDate" in mappings and mappings["collectionDate"]:
            date_column = mappings["collectionDate"]
            invalid_dates = []
            date_examples = []

            for idx, value in df[date_column].items():
                is_valid, formatted_date = validation_utils.validate_date(value)
                if not is_valid and not pd.isna(value):
                    invalid_dates.append(idx)
                    if len(date_examples) < 3:
                        date_examples.append(str(value))

            if invalid_dates:
                validation_result["issues"].append({
                    "type": "date_format",
                    "description": "Invalid date format",
                    "count": len(invalid_dates),
                    "examples": date_examples,
                    "invalidIndices": invalid_dates
                })

        # 计算有效记录数
        all_invalid_indices = set()
        for issue in validation_result["issues"]:
            all_invalid_indices.update(issue.get("invalidIndices", []))

        validation_result["validRecords"] = len(df) - len(all_invalid_indices)
        validation_result["invalidRecords"] = len(all_invalid_indices)

        # 创建验证摘要
        validation_result["validationSummary"] = {
            "totalRecords": len(df),
            "validRecords": validation_result["validRecords"],
            "invalidRecords": validation_result["invalidRecords"],
            "issuesByType": {issue["type"]: issue["count"] for issue in validation_result["issues"]}
        }

        # 存储验证结果
        await store_validation_result(file_id, validation_result)
        validation_result = convert_numpy_types(validation_result)

        return ResponseModel(
            code=20000,
            data=jsonable_encoder(validation_result)
        )

    except Exception as e:
        return ResponseModel(
            code=500,
            data={},
            message=f"Validation failed: {str(e)}"
        )

def _verbatim_date_text(value) -> Optional[str]:
    """把上传文件里的日期单元格转成原文字符串。
    Excel 日期格会被 pandas 读成 Timestamp（str 后带 " 00:00:00"），纯年份会读成 1987.0，这两种要还原。"""
    if value is None or pd.isna(value):
        return None
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    text = str(value).strip()
    return text or None


def convert_numpy_types(obj):
    if isinstance(obj, dict):
        return {k: convert_numpy_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(i) for i in obj]
    elif isinstance(obj, (np.integer, np.int64)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64)):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return convert_numpy_types(obj.tolist())
    else:
        return obj

@router.post("/confirmImport", response_model=ResponseModel)
async def confirm_import(import_data: ConfirmImportModel, background_tasks: BackgroundTasks):
    """
    确认并执行数据导入
    支持两种模式：直接导入和verbatim导入
    """
    try:
        file_id = import_data.fileId
        verbatim_import = import_data.verbatimImport

        file_info = await get_import_file_info(file_id)
        mappings = await get_mapping_info(file_id)
        validation_result = await get_validation_result(file_id)

        if not all([file_info, mappings, validation_result]):
            return ResponseModel(
                code=404,
                data={},
                message="Import information not found. Please start again."
            )

        # 生成批次序列号
        batch_serial_id = await db_utils.get_next_batch_serial_id()

        # 更新导入状态为进行中
        await store_import_status(file_id, {
            "fileId": file_id,
            "status": "in_progress",
            "fileName": file_info["fileName"],
            "totalRecords": validation_result["totalRecords"],
            "importMode": "verbatim" if verbatim_import else "direct",
            "batchSerialId": batch_serial_id,
            "startTime": datetime.now().isoformat()
        })

        # 在后台执行导入
        if verbatim_import:
            background_tasks.add_task(process_verbatim_import, file_id, batch_serial_id)
        else:
            background_tasks.add_task(process_direct_import, file_id, batch_serial_id)

        return ResponseModel(
            code=20000,
            data={
                "fileId": file_id,
                "status": "in_progress",
                "importMode": "verbatim" if verbatim_import else "direct",
                "batchSerialId": batch_serial_id,
                "message": "Import process started. Check import status for progress."
            }
        )

    except Exception as e:
        return ResponseModel(
            code=500,
            data={},
            message=f"Failed to start import: {str(e)}"
        )


def _curator_decided_species(record: Dict) -> bool:
    """Has a person settled this record's species, as opposed to the importer matching it?

    Any one of three marks is enough, and each covers a case the others miss:
      - a signature on the verbatim row (written by the record editor, apply-suggestion and
        name-group apply). The only positive proof, but the column was added on 2026-08-14, so
        everything decided before that has none.
      - "TaxonID" differs from the suggestion. The curator replaced the answer -- exactly what
        `Dorosoma petenense` -> mexicanus corrections look like.
      - verified while the match is not 'exact'. The importer only ever auto-verifies exact
        matches, so a verified fuzzy/phonetic/no_match record was verified by hand. This is
        how the 195 no_match records a curator typed a taxon into are recognised.
    """
    if (record.get("verified_by_name") or "").strip():
        return True
    current, suggested = record.get("current_taxon_id"), record.get("matched_taxon_id")
    if current is not None and current != suggested:
        return True
    return ((record.get("current_species_status") or "") == "verified"
            and record.get("species_match_status") != "exact")


async def auto_verify_imported_records(primary_temp_ids: List[int], batch_serial_id: str):
    """
    自动验证导入的记录
    规则:
    1. 如果species name是exact match -> species_verification_status = 'verified'
    2. 如果field number在locality1中找到exact match -> locality_verification_status = 'verified'
    3. record details验证: 允许空值和轻微错误 -> record_verification_status = 'verified' (但记录warnings)

    A record the curator has already decided is left alone (see _curator_decided_species).
    This function runs on import, where nothing has been decided yet, but it is ALSO what the
    "Re-validate" button re-runs on a batch the curator has been working in for weeks -- and
    there it used to overwrite their work: 332 hand-corrected "TaxonID" values were pushed back
    to the importer's suggestion (170 records off `Dorosoma petenense` onto the mexicanus
    subspecies alone), 448 hand-verified records were knocked back to pending because their
    match_status is not 'exact', and 435 verification_notes were replaced with "Auto-verified
    on import". Batch 20251119-001 had already been migrated in that state.
    """
    try:
        from app.db.database import execute_transaction

        # 批量查询所有需要验证的记录
        # The curator's own state is selected too, so their decisions can be recognised and
        # kept rather than recomputed from the import-time match.
        query = """
        SELECT
            p."PrimaryID",
            p."verbatim_taxonid",
            p."verbatim_localityid",
            p."TotalNumber",
            p."Storage",
            p."JarSize",
            p."PrevNumber",
            p."Inventory",
            p."verification_warnings",
            p."TaxonID" as current_taxon_id,
            p."species_verification_status" as current_species_status,
            p."verification_notes" as current_notes,
            vt."match_status" as species_match_status,
            vt."matched_taxon_id",
            vt."verified_by_name",
            vt."verbatim_family",
            vl."verbatim_fieldno"
        FROM primary_temp p
        LEFT JOIN verbatim_taxonomic vt ON p."verbatim_taxonid" = vt."verbatim_taxonid"
        LEFT JOIN verbatim_locality vl ON p."verbatim_localityid" = vl."verbatim_localityid"
        WHERE p.batch_serial_id = $1
        """

        records = await execute_query(query, batch_serial_id)

        if not records:
            print(f"No records found for batch {batch_serial_id}")
            return

        # The two family checks used to run per RECORD: 2-4 queries each, so a 21717-record
        # batch cost ~43000 round trips and the 64186-record one did not finish inside ten
        # minutes -- with a loading mask over the curator's screen the whole time. Both checks
        # depend only on the taxon (the reference lookup and the taxon's own family), and a
        # batch has a few hundred distinct taxa, so they are resolved once per taxon here.
        # The record-specific half of the suggestion check is the imported family, which the
        # query above now carries, so build_suggestion_warning can be called directly.
        taxon_ids = {t for r in records
                     for t in (r.get("matched_taxon_id"), r.get("current_taxon_id"))
                     if t is not None}
        taxon_family = {}
        if taxon_ids:
            for row in await execute_query(
                    'SELECT tt."TaxonID", f."FamilyName" FROM "TaxonomicTable" tt '
                    'LEFT JOIN "Family" f ON f."FamilyID" = tt."FamilyID" '
                    'WHERE tt."TaxonID" = ANY($1::int[])', sorted(taxon_ids)):
                taxon_family[row["TaxonID"]] = row["FamilyName"]
        ref_warning_cache = {}
        for tid in sorted(taxon_ids):
            ref_warning_cache[tid] = await family_reference_warning(tid)
        print(f"Family checks resolved for {len(taxon_ids)} distinct taxa "
              f"(was once per record)")

        # 为每条记录构建更新语句
        update_statements = []
        curator_kept = 0

        for record in records:
            primary_id = record["PrimaryID"]
            species_status = "pending"
            locality_status = "pending"
            record_status = "pending"
            warnings = []
            family_warnings = []  # family reference / suggestion checks (added to detail below)

            # 读取已有的import warnings
            existing_warnings = []
            if record.get("verification_warnings"):
                try:
                    existing_warnings = json.loads(record["verification_warnings"])
                    if not isinstance(existing_warnings, list):
                        existing_warnings = []
                except:
                    existing_warnings = []

            # 1. 验证 species name (exact match 自动验证)
            curator_decided = _curator_decided_species(record)
            if curator_decided:
                # Keep the decision exactly as it stands: the status, the "TaxonID" (not
                # written here at all) and the notes below. The family checks still run so a
                # warning that became true later is not lost, but they cannot change a status
                # a person set.
                species_status = record.get("current_species_status") or "pending"
                if record.get("current_taxon_id") is not None:
                    ref_w = ref_warning_cache.get(record["current_taxon_id"])
                    sugg_w = build_suggestion_warning(
                        record.get("verbatim_family"),
                        taxon_family.get(record["current_taxon_id"]))
                    if ref_w:
                        family_warnings.append(ref_w)
                    if sugg_w:
                        family_warnings.append(sugg_w)
                curator_kept += 1
                print(f"Record {primary_id}: Species left as decided by a curator "
                      f"({species_status}, TaxonID={record.get('current_taxon_id')})")
            elif record["species_match_status"] == "exact":
                species_status = "verified"
                # 同时把匹配到的 taxon 回填到 TaxonID（仿照下面 locality 分支写 Locality1ID）。
                # 之前这里只置 verified、没写 TaxonID，导致 exact 自动验证的记录 TaxonID 为 NULL，
                # 卡在迁移的 TaxonID-NOT-NULL 门槛上。
                matched_taxon_id = record.get("matched_taxon_id")
                if matched_taxon_id is not None:
                    update_statements.append({
                        "sql": 'UPDATE primary_temp SET "TaxonID" = $1 WHERE "PrimaryID" = $2',
                        "params": [matched_taxon_id, primary_id]
                    })
                    # family checks on the auto-applied taxon:
                    #  - reference mismatch -> warning + downgrade species to pending (review)
                    #  - imported-vs-matched family mismatch -> warning only (surfaces the
                    #    wrong/reclassified source family, e.g. DOROSOMA filed as CYPRINIDAE)
                    ref_w = ref_warning_cache.get(matched_taxon_id)
                    sugg_w = build_suggestion_warning(record.get("verbatim_family"),
                                                      taxon_family.get(matched_taxon_id))
                    if ref_w:
                        family_warnings.append(ref_w)
                        species_status = "pending"
                    if sugg_w:
                        family_warnings.append(sugg_w)
                print(f"Record {primary_id}: Species verified (exact match), TaxonID={matched_taxon_id}")
            else:
                print(f"Record {primary_id}: Species pending (match_status: {record['species_match_status']})")

            # 2. 验证 locality (field number exact match)
            verbatim_fieldno = record.get("verbatim_fieldno")
            if verbatim_fieldno:
                # 查找locality1中是否有匹配的FieldNo
                locality_query = """
                SELECT "Locality1ID", "FieldNo"
                FROM locality1
                WHERE "FieldNo" = $1
                LIMIT 1
                """
                locality_result = await execute_query(locality_query, verbatim_fieldno)

                if locality_result and len(locality_result) > 0:
                    locality_status = "verified"
                    # 同时更新 Locality1ID
                    locality_id = locality_result[0]["Locality1ID"]
                    print(f"Record {primary_id}: Locality verified and linked (FieldNo: {verbatim_fieldno} -> Locality1ID: {locality_id})")

                    # 添加更新Locality1ID的语句
                    update_locality_sql = """
                    UPDATE primary_temp
                    SET "Locality1ID" = $1
                    WHERE "PrimaryID" = $2
                    """
                    update_statements.append({
                        "sql": update_locality_sql,
                        "params": [locality_id, primary_id]
                    })
                else:
                    print(f"Record {primary_id}: Locality pending (FieldNo not found: {verbatim_fieldno})")
            else:
                print(f"Record {primary_id}: Locality pending (no field number)")

            # 3. 验证 record details (宽松验证:允许空值和minor errors)
            record_warnings = []
            record_warnings_detail = []  # 详细的warning信息，包含字段和类型

            # 3.1 检查 TotalNumber - 必须是有效数字
            total_number_value = record.get("TotalNumber")
            if total_number_value is None or total_number_value == 0:
                record_warnings.append("TotalNumber is missing or zero")
                record_warnings_detail.append({
                    "field": "TotalNumber",
                    "issue_type": "empty_value",
                    "severity": "warning",
                    "message": "TotalNumber is missing or zero"
                })
            elif not isinstance(total_number_value, (int, float)) or total_number_value <= 0:
                # 检查是否是有效的正数
                record_warnings.append(f"TotalNumber has invalid value: {total_number_value}")
                record_warnings_detail.append({
                    "field": "TotalNumber",
                    "issue_type": "invalid_type",
                    "severity": "error",
                    "message": f"TotalNumber must be a positive number, got: {total_number_value}"
                })

            # 3.2 检查其他字段空值
            if not record.get("Storage"):
                record_warnings.append("Storage is empty")
                record_warnings_detail.append({
                    "field": "Storage",
                    "issue_type": "empty_value",
                    "severity": "warning",
                    "message": "Storage is empty"
                })

            if not record.get("PrevNumber"):
                record_warnings.append("PrevNumber is empty")
                record_warnings_detail.append({
                    "field": "PrevNumber",
                    "issue_type": "empty_value",
                    "severity": "warning",
                    "message": "PrevNumber is empty"
                })

            if not record.get("Inventory"):
                record_warnings.append("Inventory is empty")
                record_warnings_detail.append({
                    "field": "Inventory",
                    "issue_type": "empty_value",
                    "severity": "warning",
                    "message": "Inventory is empty"
                })

            # 3.3 检查 JarSize - 如果存在，应该是字符串
            if record.get("JarSize") and not isinstance(record.get("JarSize"), str):
                record_warnings.append(f"JarSize has invalid type: {type(record.get('JarSize'))}")
                record_warnings_detail.append({
                    "field": "JarSize",
                    "issue_type": "invalid_type",
                    "severity": "warning",
                    "message": f"JarSize should be text, got: {record.get('JarSize')}"
                })

            # 检查是否有 error (severity: error) - 如果有error就设置为pending
            has_errors = any(w.get("severity") == "error" for w in record_warnings_detail)

            if has_errors:
                record_status = "pending"
                warnings.extend(record_warnings)
                print(f"Record {primary_id}: Record details pending due to errors: {', '.join(record_warnings)}")
            else:
                # 即使有warnings,也设置为verified (按照需求3)
                record_status = "verified"
                if record_warnings:
                    warnings.extend(record_warnings)
                    print(f"Record {primary_id}: Record details verified with warnings: {', '.join(record_warnings)}")
                else:
                    print(f"Record {primary_id}: Record details verified (no warnings)")

            # 4. 计算overall status
            overall_status = "pending"
            if species_status == "verified" and record_status == "verified":
                overall_status = "completed"

            # 5. 构建更新语句
            verification_notes = f"Auto-verified on import. Warnings: {'; '.join(warnings)}" if warnings else "Auto-verified on import"
            # Never overwrite a note a person wrote. The field is shared, and the only thing
            # distinguishing the two is that this function's own text starts with a known
            # prefix -- anything else came from the record editor and is the curator's.
            _existing_notes = (record.get("current_notes") or "").strip()
            if _existing_notes and not _existing_notes.startswith("Auto-verified on import"):
                verification_notes = _existing_notes

            # 合并已有的import warnings、字段warnings、family检查warnings
            all_warnings = existing_warnings + record_warnings_detail + family_warnings
            warnings_json_str = json.dumps(all_warnings) if all_warnings else None

            update_sql = """
            UPDATE primary_temp
            SET
                "species_verification_status" = $1,
                "locality_verification_status" = $2,
                "record_verification_status" = $3,
                "overall_verification_status" = $4,
                "verification_notes" = $5,
                "verification_warnings" = $6,
                "TimeStampModified" = NOW()
            WHERE "PrimaryID" = $7
            """

            update_statements.append({
                "sql": update_sql,
                "params": [species_status, locality_status, record_status, overall_status, verification_notes, warnings_json_str, primary_id]
            })

        # 批量执行更新
        if update_statements:
            print(f"Executing {len(update_statements)} verification updates...")
            await execute_transaction(update_statements)
            print(f"Auto-verification completed for {len(records)} records"
                  + (f", {curator_kept} left untouched as already decided by a curator"
                     if curator_kept else ""))

    except Exception as e:
        print(f"Error during auto-verification: {str(e)}")
        # 不抛出异常,允许导入继续


async def process_direct_import(file_id: str, batch_serial_id: str, user_id: Optional[int] = None):
    """
    处理直接导入（解析物种名称，关联taxonomic表）
    """
    try:
        # 获取导入信息
        file_info = await get_import_file_info(file_id)
        mappings = await get_mapping_info(file_id)
        validation_result = await get_validation_result(file_id)

        file_path = file_info["filePath"]
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            df = pd.read_excel(file_path)

        # 获取无效记录索引
        invalid_indices = set()
        for issue in validation_result.get("issues", []):
            invalid_indices.update(issue.get("invalidIndices", []))

        # 获取匹配结果
        species_matching = validation_result.get("speciesMatching", {})
        all_matches = {}

        # 整理所有类型的匹配结果
        for match_type, matches in species_matching.get("matches", {}).items():
            for match in matches:
                row_idx = match.get("row_index")
                if row_idx is not None:
                    all_matches[row_idx] = {
                        "matched": match_type != "no_match",
                        "taxon_id": match.get("taxon_id"),
                        "match_type": match_type,
                        "confidence": match.get("similarity", match.get("confidence", 0))
                    }

        # 筛选有效记录
        valid_records = []
        total_numbers = []

        for index, row in df.iterrows():
            if index not in invalid_indices:
                # 获取物种匹配结果
                match_result = all_matches.get(index, {
                    "matched": False,
                    "taxon_id": None,
                    "match_type": "no_match",
                    "confidence": 0
                })

                # CoF 校正（同 verbatim）：verbatim 名 -> CoF accepted -> 正确 rank 的本地 taxon
                _g = row.get(mappings["genus"]) if mappings.get("genus") else ""
                _s = row.get(mappings["species"]) if mappings.get("species") else ""
                try:
                    _cof = await _synonym_service.resolve_to_local_taxon(str(_g or ""), str(_s or ""))
                except Exception:
                    _cof = None
                if _cof and _cof.get("taxon_id"):
                    match_result = {**match_result, "matched": True,
                                    "taxon_id": _cof["taxon_id"], "match_type": "exact"}
                elif _cof and not _cof.get("local_found"):
                    print(f"[CoF] direct row {index}: accepted '{_cof.get('accepted_name')}' "
                          f"not in local; kept {match_result.get('taxon_id')}")

                record = {
                    "taxon_id": match_result.get("taxon_id") if match_result["matched"] else None,
                    "verbatim_taxonomic_id": None,  # 直接导入模式下不存储verbatim
                    "verbatim_locality_id": None,
                    "collection_date": None,
                    "locality_id": None,
                    # "field_number": None,
                    "total_number": 1,
                    "storage": None,
                    "jar_size": None,
                    "prev_number": None,
                    "inventory": None,
                    "remarks": "Imported via batch import - direct mode",
                    "match_type": match_result["match_type"]
                }

                # 映射字段
                for field, column in mappings.items():
                    if column and column in df.columns:
                        value = row[column]

                        if field == "collectionDate":
                            is_valid, formatted_date = validation_utils.validate_date(value)
                            record["collection_date"] = formatted_date if is_valid else None
                        # elif field == "fieldNumber":
                        #     record["field_number"] = str(value) if not pd.isna(value) else None
                        elif field == "totalNumber":
                            record["total_number"] = int(float(value)) if not pd.isna(value) else 1
                        elif field == "storage":
                            record["storage"] = str(value) if not pd.isna(value) else None
                        elif field == "jarSize":
                            record["jar_size"] = str(value) if not pd.isna(value) else None
                        elif field == "prevNumber":
                            record["prev_number"] = str(value) if not pd.isna(value) else None
                        elif field == "inventory":
                            record["inventory"] = str(value) if not pd.isna(value) else None
                        elif field == "remarks":
                            record["remarks"] = str(value) if not pd.isna(value) else record["remarks"]

                valid_records.append(record)
                total_numbers.append(record["total_number"])

        if not valid_records:
            await store_import_status(file_id, {
                "fileId": file_id,
                "status": "completed",
                "fileName": file_info["fileName"],
                "totalRecords": len(df),
                "importedCount": 0,
                "skippedCount": len(df),
                "success": True,
                "userId": user_id,
                "batchSerialId": batch_serial_id
            })
            return

        # TODO(#3 冗余 + 并发撞号): 这里算的 catalog number 其实没用 ——
        #   下面 insert_primary_records 内部会【再分配一次】并用它自己的号，
        #   record["catalog_number"] 没有任何下游读取。等 #2 改造时一并删掉这段，
        #   并发撞号的根治也在 #2(见 db_import.insert_primary_records 的 TODO)。暂缓。
        # 获取编目号
        catalog_numbers = await db_utils.get_next_catalog_numbers(len(valid_records))

        # 为记录分配编目号
        for i, record in enumerate(valid_records):
            record["catalog_number"] = catalog_numbers[i]


        # 插入Primary记录
        primary_ids = await db_utils.insert_primary_records(valid_records, batch_serial_id)

        # 插入Preparation记录
        prep_ids = await db_utils.insert_preparation_records(primary_ids, total_numbers, "Fluid")

        # 更新导入状态
        await store_import_status(file_id, {
            "fileId": file_id,
            "status": "completed",
            "fileName": file_info["fileName"],
            "totalRecords": len(df),
            "importedCount": len(primary_ids),
            "skippedCount": len(df) - len(primary_ids),
            "success": True,
            "userId": user_id,
            "batchSerialId": batch_serial_id,
            "fieldMappings": mappings,
            "preparationIds": prep_ids
        })

        # 保留源上传文件并登记到 batch（供详情页查看/下载），best-effort
        await persist_batch_source_file(file_info, batch_serial_id, "direct", len(primary_ids))

        # 清理临时文件
        if os.path.exists(file_path):
            os.remove(file_path)

    except Exception as e:
        await store_import_status(file_id, {
            "fileId": file_id,
            "status": "failed",
            "error": str(e),
            "userId": user_id,
            "batchSerialId": batch_serial_id
        })


async def process_verbatim_import(file_id: str, batch_serial_id: str, user_id: Optional[int] = None):
    """
    处理verbatim导入（保存原始数据到verbatim表，用户后续验证）
    源数据格式相同，但会将所有原始信息存储到verbatim表中，包含物种匹配信息
    """
    try:
        # 获取导入信息
        file_info = await get_import_file_info(file_id)
        mappings = await get_mapping_info(file_id)
        validation_result = await get_validation_result(file_id)

        file_path = file_info["filePath"]
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            df = pd.read_excel(file_path)

        # 获取物种匹配结果
        species_matching = validation_result.get("speciesMatching", {})
        all_matches = {}

        # 整理所有类型的匹配结果
        for match_type, matches in species_matching.get("matches", {}).items():
            for match in matches:
                row_idx = match.get("row_index")
                if row_idx is not None:
                    # 提取匹配信息
                    match_info = {
                        "match_type": match_type,
                        "import_name": match.get("import_name", ""),
                        "db_name": match.get("db_name", ""),
                        "similarity": match.get("similarity", match.get("confidence", 0)),
                        "normalized_name": match.get("import_normalized", ""),
                        "db_normalized": match.get("db_normalized", "")
                    }

                    if match_type == "phonetic" and match.get("potential_matches"):
                        # 对于语音匹配，取第一个潜在匹配
                        potential = match["potential_matches"][0]
                        match_info.update({
                            "db_name": potential.get("db_name", ""),
                            "db_normalized": potential.get("db_normalized", "")
                        })

                    all_matches[row_idx] = {
                        "matched": match_type != "no_match",
                        "taxon_id": match.get("taxon_id") if match_type != "phonetic" else (
                            match["potential_matches"][0].get("taxon_id") if match.get("potential_matches") else None
                        ),
                        "match_status": match_type,
                        "confidence": match.get("similarity", match.get("confidence", 0)),
                        "match_info": match_info
                    }

        # Curator decisions from earlier batches, fetched once for the whole spreadsheet. The
        # keys are built the same way apply-by-name groups records, so a decision made through
        # either route is found by the other.
        _genus_col = mappings.get("genus", "")
        _species_col = mappings.get("species", "")
        _keys = {name_key(str(r.get(_genus_col, "") or ""), str(r.get(_species_col, "") or ""))
                 for _, r in df.iterrows()} if (_genus_col or _species_col) else set()
        _name_decisions = await NameDecisionService.lookup_many([k for k in _keys if k])
        _decision_reuse = {}
        if _name_decisions:
            print(f"[decisions] {len(_name_decisions)} of {len(_keys)} distinct imported names "
                  f"have a curator decision from an earlier batch")

        # verbatim导入包含所有记录，即使验证失败的记录也会导入
        valid_records = []
        verbatim_taxonomic_records = []
        verbatim_locality_records = []
        total_numbers = []

        for index, row in df.iterrows():
            # 获取物种匹配结果
            match_result = all_matches.get(index, {
                "matched": False,
                "taxon_id": None,
                "match_status": "no_match",
                "confidence": 0,
                "match_info": None
            })

            # 1. 准备verbatim taxonomic记录 - 存储原始的分类学信息和匹配结果
            family_value = row.get(mappings.get("family", ""), "") if mappings.get("family") else ""
            genus_value = row.get(mappings.get("genus", ""), "") if mappings.get("genus") else ""
            species_value = row.get(mappings.get("species", ""), "") if mappings.get("species") else ""

            # CoF 校正：把 verbatim 名经 CoF 解析到 accepted 名，再锁定正确 rank 的本地 taxon
            # （修正 'Anchoa mitchilli' 撞到本地亚种、同义词未解析等问题）。
            #  - CoF 不认该名 -> cof=None -> 保留原本地匹配（边界1）
            #  - CoF 有 accepted 但本地无此 taxon -> 保留原匹配 + 记日志（边界2）
            #  - CoF 有 accepted 且本地命中 -> 用 CoF 正确的本地 taxon 覆盖（主路径）
            try:
                cof = await _synonym_service.resolve_to_local_taxon(
                    str(genus_value or ""), str(species_value or ""))
            except Exception:
                cof = None
            if cof and cof.get("taxon_id"):
                match_result = {
                    **match_result,
                    "matched": True,
                    "taxon_id": cof["taxon_id"],
                    "match_status": "exact",
                    "confidence": 100,
                    "match_info": {"source": "cof_resolved",
                                   "accepted_name": cof.get("accepted_name"),
                                   "cof_status": cof.get("status")},
                }
            elif cof and not cof.get("local_found"):
                # CoF 有 accepted 但本地没有 -> 保留 DB 近似建议 + 附「建议创建 CoF 名」
                _mi = match_result.get("match_info")
                _mi = _mi if isinstance(_mi, dict) else {}
                match_result = {**match_result, "match_info": {**_mi, "cof_create": {
                    "genus": cof.get("cof_genus"),
                    "species": cof.get("cof_species"),
                    "subspecies": cof.get("cof_subspecies"),
                    "family": cof.get("cof_family"),
                    "accepted_name": cof.get("accepted_name"),
                    "status": cof.get("status"),
                }}}
                print(f"[CoF] row {index}: accepted '{cof.get('accepted_name')}' "
                      f"({cof.get('cof_family')}) not in local -> create-suggestion attached")

            # Last resort: has a curator already decided what this spelling means?
            #
            # Only consulted when neither name matching nor CoF produced an exact answer --
            # i.e. exactly the rows that would otherwise land on the curator's desk for a
            # judgement someone has already made on a previous batch.
            #
            # It fills in the taxon and marks the row, but deliberately does NOT set
            # match_status to 'exact', because 'exact' is what auto-verifies a record. The
            # record stays pending with the answer pre-filled, so a wrong old decision shows
            # up for review instead of spreading silently through every future import.
            historical_decision_id = None
            if match_result.get("match_status") != "exact":
                _decision = _name_decisions.get(
                    name_key(str(genus_value or ""), str(species_value or "")))
                if _decision and _decision.get("taxon_id"):
                    historical_decision_id = _decision["id"]
                    _mi = match_result.get("match_info")
                    _mi = _mi if isinstance(_mi, dict) else {}
                    match_result = {
                        **match_result,
                        "matched": True,
                        "taxon_id": _decision["taxon_id"],
                        "match_info": {**_mi, "historical_decision": {
                            "id": _decision["id"],
                            "taxon_id": _decision["taxon_id"],
                            "taxon_name": _decision.get("current_taxon_name")
                                          or _decision.get("taxon_name"),
                            "decided_by": _decision.get("decided_by"),
                            "decided_at": (_decision["decided_at"].isoformat()
                                           if _decision.get("decided_at") else None),
                            "source_batch": _decision.get("source_batch"),
                        }},
                    }
                    _decision_reuse[_decision["id"]] = _decision_reuse.get(
                        _decision["id"], 0) + 1

            verbatim_taxonomic_record = {
                "verbatim_family": str(family_value) if pd.notna(family_value) and str(family_value).strip() else None,
                "verbatim_genus": str(genus_value) if pd.notna(genus_value) and str(genus_value).strip() else None,
                "verbatim_species": str(species_value) if pd.notna(species_value) and str(
                    species_value).strip() else None,
                "verbatim_subspecies": None,  # 目前不支持亚种
                "original_text": f"{family_value} {genus_value} {species_value}".strip() if any(
                    [family_value, genus_value, species_value]) else None,
                # 添加匹配信息
                "match_status": match_result["match_status"],
                "matched_taxon_id": match_result["taxon_id"],
                "match_confidence": match_result["confidence"],
                "match_info": match_result["match_info"],
                "historical_decision_id": historical_decision_id
            }

            # 2. 准备verbatim locality记录 - 存储原始的地点信息
            verbatim_locality_record = {
                "verbatim_locality_string": None,
                "verbatim_drainage": None,
                "verbatim_country": None,
                "verbatim_state": None,
                "verbatim_county": None,
                "verbatim_waterbody": None,
                "verbatim_latitude": None,
                "verbatim_longitude": None,
                "verbatim_fieldno": None,
                "verbatim_collector": None,
                "verbatim_collect_date": None,
                "original_text": None
            }

            # 检查并映射locality相关字段
            locality_fields = {
                "localityString": "verbatim_locality_string",
                "country": "verbatim_country",
                "state": "verbatim_state",
                "county": "verbatim_county",
                "drainage": "verbatim_drainage",
                "waterbody": "verbatim_waterbody",
                "latitude": "verbatim_latitude",
                "longitude": "verbatim_longitude",
                "fieldNumber": "verbatim_fieldno",
                "collectorName": "verbatim_collector"
            }

            locality_parts = []
            has_locality_data = False

            for field_key, verbatim_key in locality_fields.items():
                if field_key in mappings and mappings[field_key] and mappings[field_key] in df.columns:
                    value = row.get(mappings[field_key])
                    if pd.notna(value) and str(value).strip():
                        verbatim_locality_record[verbatim_key] = str(value).strip()
                        locality_parts.append(f"{field_key}: {str(value).strip()}")
                        has_locality_data = True

            # 采集日期存原文（标签照印），不过 validate_date —— 非标准写法（"VI-1987"、"summer 1987"）也要留住。
            # 之前这里漏了，verbatim_collect_date 永远是 NULL → complete 后 locality1.VerbatimDate 全空。
            if mappings.get("collectionDate") and mappings["collectionDate"] in df.columns:
                verbatim_locality_record["verbatim_collect_date"] = _verbatim_date_text(
                    row.get(mappings["collectionDate"]))

            # 如果有地点数据，设置original_text
            if has_locality_data:
                verbatim_locality_record["original_text"] = " | ".join(locality_parts)

            verbatim_taxonomic_records.append(verbatim_taxonomic_record)
            verbatim_locality_records.append(verbatim_locality_record)

            # 3. 准备Primary记录
            record = {
                "taxon_id": None,  # verbatim模式下不自动关联taxonomic表
                "collection_date": None,
                "locality_id": None,
                "total_number": 1,
                "storage": None,
                "jar_size": None,
                "prev_number": None,
                "inventory": None,
                "remarks": "Imported via batch import - verbatim mode",
                "match_type": match_result["match_status"],  # 使用实际的匹配状态
                # 添加验证状态字段 (将在后面根据匹配结果设置)
                "species_verification_status": "pending",
                "locality_verification_status": "pending",
                "record_verification_status": "pending"
            }

            # 映射其他标准字段到Primary记录
            for field, column in mappings.items():
                if column and column in df.columns:
                    value = row[column]

                    # 跳过已处理的taxonomic和locality字段
                    if field in ["family", "genus", "species"] + list(locality_fields.keys()):
                        continue

                    if field == "collectionDate":
                        is_valid, formatted_date = validation_utils.validate_date(value)
                        record["collection_date"] = formatted_date if is_valid else None
                    elif field == "totalNumber":
                        try:
                            if pd.isna(value):
                                record["total_number"] = 1
                                record["import_warnings"] = record.get("import_warnings", []) + [
                                    f"TotalNumber was empty, defaulted to 1"
                                ]
                            else:
                                record["total_number"] = int(float(value))
                        except (ValueError, TypeError):
                            # 记录转换失败的原始值
                            record["total_number"] = 1  # 默认值
                            record["import_warnings"] = record.get("import_warnings", []) + [
                                f"TotalNumber had invalid value '{value}' (not a number), defaulted to 1"
                            ]
                    elif field == "storage":
                        record["storage"] = str(value) if not pd.isna(value) else None
                    elif field == "jarSize":
                        record["jar_size"] = str(value) if not pd.isna(value) else None
                    elif field == "typeStatus":
                        record["type_status"] = str(value) if not pd.isna(value) else None
                    elif field == "prevNumber":
                        record["prev_number"] = str(value) if not pd.isna(value) else None
                    elif field == "inventory":
                        record["inventory"] = str(value) if not pd.isna(value) else None
                    elif field == "remarks":
                        existing_remarks = record["remarks"]
                        if not pd.isna(value) and str(value).strip():
                            record["remarks"] = f"{str(value).strip()} | {existing_remarks}"

            # Source unique ID -> source_primary_id (traceability back to the source dataset).
            # Validated unique+integer at validate_mapping; store as int.
            src_col = mappings.get("sourceId")
            if src_col and src_col in df.columns:
                sval = row.get(src_col)
                try:
                    record["source_primary_id"] = int(float(sval)) if pd.notna(sval) else None
                except (ValueError, TypeError):
                    record["source_primary_id"] = None

            valid_records.append(record)
            total_numbers.append(record["total_number"])

        # 4. 批量插入verbatim记录
        print(f"Inserting {len(verbatim_taxonomic_records)} verbatim taxonomic records...")
        verbatim_taxonomic_ids = await db_utils.insert_verbatim_taxonomic_records(verbatim_taxonomic_records)

        print(f"Inserting {len(verbatim_locality_records)} verbatim locality records...")
        verbatim_locality_ids = await db_utils.insert_verbatim_locality_records(verbatim_locality_records)

        # TODO(#4 死分配): verbatim 导入写的是 primary_temp，它会【自己生成临时字符串号】
        #   {batch}-{seq}(见 insert_primary_temp_records)，这里算的官方整数号根本没人用。
        #   官方号要等批次迁移(#1)时才真正生成。可直接删掉本次分配。暂缓清理。
        # 5. 获取编目号
        catalog_numbers = await db_utils.get_next_catalog_numbers(len(valid_records))

        # 6. 为Primary记录分配编目号和verbatim IDs
        for i, record in enumerate(valid_records):
            record["catalog_number"] = catalog_numbers[i]
            record["verbatim_taxonomic_id"] = verbatim_taxonomic_ids[i] if i < len(verbatim_taxonomic_ids) else None
            record["verbatim_locality_id"] = verbatim_locality_ids[i] if i < len(verbatim_locality_ids) else None

        # 7. 插入 primary_temp 记录（verbatim 模式使用临时表）
        print(f"Inserting {len(valid_records)} primary_temp records...")
        primary_temp_ids = await db_utils.insert_primary_temp_records(valid_records, batch_serial_id)

        # 8. 插入 preparation_temp 记录
        print(f"Inserting {len(primary_temp_ids)} preparation_temp records...")
        prep_temp_ids = await db_utils.insert_preparation_temp_records(primary_temp_ids, total_numbers, "Fluid")

        # 8.5. 自动验证导入的记录并更新验证状态
        print(f"Auto-verifying {len(primary_temp_ids)} records...")
        await auto_verify_imported_records(primary_temp_ids, batch_serial_id)

        # 8.6. Count how many records each reused decision pre-filled, so the reference table
        # can show which entries are actually carrying weight (and which never fire).
        for _did, _n in _decision_reuse.items():
            await NameDecisionService.mark_reused(_did, _n)
        if _decision_reuse:
            print(f"[decisions] pre-filled {sum(_decision_reuse.values())} records from "
                  f"{len(_decision_reuse)} earlier curator decisions (left pending for review)")

        # 9. 更新导入状态
        await store_import_status(file_id, {
            "fileId": file_id,
            "status": "completed",
            "fileName": file_info["fileName"],
            "totalRecords": len(df),
            "importedCount": len(primary_temp_ids),
            "skippedCount": 0,  # verbatim模式下不跳过记录
            "success": True,
            "userId": user_id,
            "batchSerialId": batch_serial_id,
            "fieldMappings": mappings,
            "preparationTempIds": prep_temp_ids,
            "verbatimTaxonomicIds": verbatim_taxonomic_ids,
            "verbatimLocalityIds": verbatim_locality_ids,
            "note": "Records imported in verbatim mode (temp tables) - all original data and matching results preserved for manual review and final import"
        })

        # 9.5. 保留源上传文件并登记到 batch（供详情页查看/下载），best-effort
        await persist_batch_source_file(file_info, batch_serial_id, "verbatim", len(primary_temp_ids))

        # 10. 清理临时文件
        if os.path.exists(file_path):
            os.remove(file_path)

        print(f"Verbatim import done: {len(primary_temp_ids)} records (staged in temp tables)")

    except Exception as e:
        print(f"Verbatim import failed: {str(e)}")
        await store_import_status(file_id, {
            "fileId": file_id,
            "status": "failed",
            "error": str(e),
            "userId": user_id,
            "batchSerialId": batch_serial_id
        })


@router.get("/importStatus/{fileId}", response_model=ResponseModel)
async def get_batch_import_status(fileId: str):
    """获取批量导入状态"""
    try:
        status = await get_import_status(fileId)
        if not status:
            return ResponseModel(
                code=404,
                data={},
                message="Import status not found."
            )

        return ResponseModel(
            code=20000,
            data=status
        )
    except Exception as e:
        return ResponseModel(
            code=500,
            data={},
            message=f"Failed to get import status: {str(e)}"
        )


@router.get("/downloadTemplate")
async def download_template():
    """下载导入模板文件"""
    try:
        os.makedirs("static/templates", exist_ok=True)
        template_path = "static/templates/ulm_import_template.xlsx"

        if not os.path.exists(template_path):
            # 创建模板文件
            template_df = pd.DataFrame({
                "Family": ["Cyprinidae", "Salmonidae", "Format: Capitalized family name"],
                "Genus": ["Cyprinus", "Salmo", "Format: Capitalized genus name"],
                "Species": ["carpio", "trutta", "Format: lowercase species name"],
                "Collection_Date": ["2025-02-15", "2025-03-20", "Format: YYYY-MM-DD"],
                "Field_Number": ["FIELD-001", "FIELD-002", "Optional - field collection number"],
                "Total_Number": ["1", "2", "Required - number of specimens"],
                "Storage": ["Tank A1", "Tank B2", "Optional"],
                "Jar_Size": ["Large", "Medium", "Optional"],
                "Prev_Number": ["OLD-001", "OLD-002", "Optional - previous catalog number"],
                "Inventory": ["INV-2025-001", "INV-2025-002", "Optional - inventory number"],
                "Remarks": ["Sample remarks", "Another note", "Optional"],
                "Locality_String": ["Lake Michigan", "River Delta", "Optional - verbatim locality"],
                "Country": ["USA", "Canada", "Optional"],
                "State": ["Michigan", "Ontario", "Optional"],
                "County": ["Wayne", "Essex", "Optional"],
                "Drainage": ["Great Lakes Basin", "Mississippi River", "Optional - drainage basin information"],
                "Waterbody": ["Lake Huron", "St. Clair River", "Optional - water body name (lake, river, etc.)"],
                "Latitude": ["42.3314", "42.2808", "Optional - decimal degrees"],
                "Longitude": ["-83.0458", "-82.9534", "Optional - decimal degrees"]
            })
            template_df.to_excel(template_path, index=False)

        file_like = open(template_path, mode="rb")
        return StreamingResponse(
            file_like,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=ulm_import_template.xlsx"}
        )
    except Exception as e:
        return ResponseModel(
            code=500,
            data={},
            message=f"Failed to get template: {str(e)}"
        )


@router.get("/importHistory", response_model=ResponseModel)
async def get_import_history(pagination: PaginationParams = Depends()):
    """获取导入历史记录"""
    try:
        query = """
        SELECT action_details, created_at, user_id
        FROM system_logs
        WHERE action_type = 'batch_import'
        ORDER BY created_at DESC
        LIMIT $1 OFFSET $2
        """

        count_query = """
        SELECT COUNT(*) as count
        FROM system_logs
        WHERE action_type = 'batch_import'
        """

        records = await execute_query(query, pagination.page_size, (pagination.page - 1) * pagination.page_size)
        count_result = await execute_query(count_query)
        total_count = count_result[0]["count"] if count_result else 0

        history_items = []
        for record in records:
            try:
                details = json.loads(record["action_details"])
                history_items.append({
                    "importId": details.get("fileId", "Unknown"),
                    "fileName": details.get("fileName", "Unknown"),
                    "importDate": record["created_at"].isoformat(),
                    "status": details.get("status", "Unknown"),
                    "totalRecords": details.get("totalRecords", 0),
                    "importedCount": details.get("importedCount", 0),
                    "skippedCount": details.get("skippedCount", 0),
                    "userId": record["user_id"],
                    "importMode": details.get("importMode", "unknown"),
                    "batchSerialId": details.get("batchSerialId", "unknown"),
                    "error": details.get("error", None)
                })
            except:
                history_items.append({
                    "importId": "Unknown",
                    "fileName": "Unknown",
                    "importDate": record["created_at"].isoformat(),
                    "status": "error",
                    "totalRecords": 0,
                    "importedCount": 0,
                    "skippedCount": 0,
                    "userId": record["user_id"],
                    "importMode": "unknown",
                    "batchSerialId": "unknown",
                    "error": "Could not parse import details"
                })

        return ResponseModel(
            code=20000,
            data={
                "items": history_items,
                "total": total_count
            }
        )
    except Exception as e:
        return ResponseModel(
            code=500,
            data={},
            message=f"Failed to get import history: {str(e)}"
        )


@router.get("/exportIssuesList/{fileId}")
async def export_issues_list(fileId: str):
    """导出验证问题列表"""
    try:
        validation_result = await get_validation_result(fileId)
        if not validation_result:
            return ResponseModel(
                code=404,
                data={},
                message="Validation result not found."
            )

        file_info = await get_import_file_info(fileId)
        if not file_info:
            return ResponseModel(
                code=404,
                data={},
                message="File information not found."
            )

        # 读取原文件
        file_path = file_info["filePath"]
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            df = pd.read_excel(file_path)

        # 创建问题列表
        issues_list = []

        # 处理各种验证问题
        for issue in validation_result.get("issues", []):
            issue_type = issue.get("type", "unknown")
            description = issue.get("description", "")
            invalid_indices = issue.get("invalidIndices", [])

            for idx in invalid_indices:
                if idx < len(df):
                    row_data = df.iloc[idx].to_dict()
                    issues_list.append({
                        "row_number": idx + 2,  # +2 because Excel is 1-indexed and includes header
                        "issue_type": issue_type,
                        "description": description,
                        "row_data": row_data
                    })

        # 处理物种匹配问题
        species_matching = validation_result.get("speciesMatching", {})
        unmatched_species = []
        for match_type in ["no_match"]:
            matches = species_matching.get("matches", {}).get(match_type, [])
            for match in matches:
                unmatched_species.append({
                    "row_index": match.get("row_index", match.get("import_index")),
                    "species_name": match.get("original_name", ""),
                    "reason": match.get("reason", match_type)
                })

        for unmatched in unmatched_species:
            idx = unmatched["row_index"]
            if idx < len(df):
                row_data = df.iloc[idx].to_dict()
                issues_list.append({
                    "row_number": idx + 2,
                    "issue_type": "species_not_matched",
                    "description": f"Species '{unmatched['species_name']}' not found in database",
                    "reason": unmatched.get("reason", "unknown"),
                    "row_data": row_data
                })

        if not issues_list:
            return ResponseModel(
                code=20000,
                data={"message": "No issues found in the validation."}
            )

        # 创建Excel文件
        issues_df = pd.DataFrame(issues_list)

        # 创建输出文件路径
        output_filename = f"issues_list_{fileId}.xlsx"
        output_path = f"temp/{output_filename}"

        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            # 写入问题摘要
            summary_data = {}
            for item in issues_list:
                issue_type = item["issue_type"]
                summary_data[issue_type] = summary_data.get(issue_type, 0) + 1

            summary_df = pd.DataFrame([
                {"Issue Type": k, "Count": v} for k, v in summary_data.items()
            ])
            summary_df.to_excel(writer, sheet_name="Summary", index=False)

            # 写入详细问题列表
            issues_detail = []
            for issue in issues_list:
                detail_row = {
                    "Row Number": issue["row_number"],
                    "Issue Type": issue["issue_type"],
                    "Description": issue["description"]
                }
                # 添加原始数据
                for key, value in issue["row_data"].items():
                    detail_row[f"Original_{key}"] = value
                issues_detail.append(detail_row)

            detail_df = pd.DataFrame(issues_detail)
            detail_df.to_excel(writer, sheet_name="Detailed Issues", index=False)

        # 返回文件
        file_like = open(output_path, mode="rb")
        return StreamingResponse(
            file_like,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={output_filename}"}
        )

    except Exception as e:
        return ResponseModel(
            code=500,
            data={},
            message=f"Failed to export issues list: {str(e)}"
        )


@router.delete("/cleanupImport/{fileId}", response_model=ResponseModel)
async def cleanup_import(fileId: str):
    """清理导入相关的临时文件和数据"""
    try:
        # 获取文件信息
        file_info = await get_import_file_info(fileId)

        # 清理临时文件
        files_to_cleanup = []
        if file_info and "filePath" in file_info:
            files_to_cleanup.append(file_info["filePath"])

        # 可能存在的问题列表文件
        files_to_cleanup.append(f"temp/issues_list_{fileId}.xlsx")

        for file_path in files_to_cleanup:
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except:
                    pass  # 忽略删除失败

        # 从内存中移除数据
        storage_keys = [fileId]
        for key in storage_keys:
            if key in file_storage:
                del file_storage[key]
            if key in mapping_storage:
                del mapping_storage[key]
            if key in validation_storage:
                del validation_storage[key]
            if key in import_status:
                del import_status[key]

        return ResponseModel(
            code=20000,
            data={
                "success": True,
                "message": "Import data cleaned up successfully"
            }
        )
    except Exception as e:
        return ResponseModel(
            code=500,
            data={},
            message=f"Failed to cleanup import: {str(e)}"
        )


@router.get("/taxonomicNames", response_model=ResponseModel)
async def get_taxonomic_names(search: Optional[str] = None, pagination: PaginationParams = Depends()):
    """获取分类学名称，用于物种名称验证参考"""
    try:
        if search:
            query = """
            SELECT "TaxonID", "FullName", "Genus", "Species", "Subspecies", 
                   "Family", "Author", "Status"
            FROM taxonomic
            WHERE "FullName" ILIKE $1 OR "Genus" ILIKE $1 OR "Species" ILIKE $1
            ORDER BY "FullName"
            LIMIT $2 OFFSET $3
            """

            count_query = """
            SELECT COUNT(*) as count
            FROM taxonomic
            WHERE "FullName" ILIKE $1 OR "Genus" ILIKE $1 OR "Species" ILIKE $1
            """

            search_param = f"%{search}%"
            records = await execute_query(query, search_param, pagination.page_size,
                                          (pagination.page - 1) * pagination.page_size)
            count_result = await execute_query(count_query, search_param)
        else:
            query = """
            SELECT "TaxonID", "FullName", "Genus", "Species", "Subspecies",
                   "Family", "Author", "Status"
            FROM taxonomic
            ORDER BY "FullName"
            LIMIT $1 OFFSET $2
            """

            count_query = """
            SELECT COUNT(*) as count
            FROM taxonomic
            """

            records = await execute_query(query, pagination.page_size, (pagination.page - 1) * pagination.page_size)
            count_result = await execute_query(count_query)

        total_count = count_result[0]["count"] if count_result else 0

        return ResponseModel(
            code=20000,
            data={
                "items": records,
                "total": total_count
            }
        )
    except Exception as e:
        return ResponseModel(
            code=500,
            data={},
            message=f"Failed to get taxonomic names: {str(e)}"
        )


@router.get("/importStatistics", response_model=ResponseModel)
async def get_import_statistics(days: int = Query(30, ge=1, le=365)):
    """获取导入统计信息"""
    try:
        stats = await db_utils.get_import_statistics(days)

        return ResponseModel(
            code=20000,
            data={
                "period_days": days,
                "statistics": stats,
                "summary": {
                    "total_imports": sum(s["count"] for s in stats.values()),
                    "total_records_processed": sum(s["total_records"] for s in stats.values()),
                    "total_records_imported": sum(s["imported_records"] for s in stats.values()),
                    "success_rate": round(
                        stats["completed"]["count"] / sum(s["count"] for s in stats.values()) * 100, 2
                    ) if sum(s["count"] for s in stats.values()) > 0 else 0
                }
            }
        )
    except Exception as e:
        return ResponseModel(
            code=500,
            data={},
            message=f"Failed to get import statistics: {str(e)}"
        )


@router.post("/validateDatabaseConsistency", response_model=ResponseModel)
async def validate_database_consistency():
    """验证数据库一致性"""
    try:
        consistency_report = await db_utils.validate_database_consistency()

        return ResponseModel(
            code=20000,
            data=consistency_report
        )
    except Exception as e:
        return ResponseModel(
            code=500,
            data={},
            message=f"Failed to validate database consistency: {str(e)}"
        )


class ConfirmBatchImportModel(BaseModel):
    batchSerialId: str


@router.post("/confirmBatchImport", response_model=ResponseModel)
async def confirm_batch_import(request: ConfirmBatchImportModel):
    """
    确认批次导入，将 primary_temp 数据迁移到 Primary 表
    生成正式的 catalog number
    """
    try:
        batch_serial_id = request.batchSerialId

        # 调用迁移函数
        result = await db_utils.migrate_batch_from_temp_to_primary(batch_serial_id)

        # 部分迁移：迁了 migrated_count 条，skipped_count 条 pending 留待下次 batch
        return ResponseModel(
            code=20000,
            data=result,
            message=(
                f"Migrated {result['migrated_count']} records "
                f"(catalog {result['catalog_number_range']['start']}-{result['catalog_number_range']['end']}). "
                f"Skipped {result['skipped_count']} pending record(s) — they stay in this batch and will be "
                f"migrated in a future batch-complete once verified."
            )
        )

    except Exception as e:
        return ResponseModel(
            code=500,
            data={},
            message=f"Failed to confirm batch import: {str(e)}"
        )


@router.get("/batches/{batch_serial_id}/verificationSummary", response_model=ResponseModel)
async def get_batch_verification_summary(batch_serial_id: str):
    """
    获取批次的验证摘要统计
    返回各种验证状态的记录数，包括warnings和errors的统计
    """
    try:
        query = """
        WITH warning_analysis AS (
            SELECT
                "PrimaryID",
                "verification_warnings",
                CASE
                    WHEN "verification_warnings" IS NOT NULL AND "verification_warnings" != '[]'
                         AND "verification_warnings" LIKE '%"severity": "error"%' THEN TRUE
                    ELSE FALSE
                END as has_errors,
                CASE
                    WHEN "verification_warnings" IS NOT NULL AND "verification_warnings" != '[]'
                         AND "verification_warnings" LIKE '%"severity": "warning"%' THEN TRUE
                    ELSE FALSE
                END as has_warnings
            FROM primary_temp
            WHERE batch_serial_id = $1
        )
        SELECT
            COUNT(*) as total_records,
            SUM(CASE WHEN p."overall_verification_status" = 'completed' THEN 1 ELSE 0 END) as fully_verified,
            SUM(CASE WHEN wa.has_errors THEN 1 ELSE 0 END) as has_errors,
            SUM(CASE WHEN wa.has_warnings THEN 1 ELSE 0 END) as has_warnings,
            SUM(CASE WHEN COALESCE(p."species_verification_status", 'pending') = 'pending'
                     OR COALESCE(p."locality_verification_status", 'pending') = 'pending'
                     OR COALESCE(p."record_verification_status", 'pending') = 'pending' THEN 1 ELSE 0 END) as has_pending,
            SUM(CASE WHEN p."species_verification_status" = 'verified' THEN 1 ELSE 0 END) as species_verified,
            SUM(CASE WHEN p."locality_verification_status" = 'verified' THEN 1 ELSE 0 END) as locality_verified,
            SUM(CASE WHEN p."record_verification_status" = 'verified' THEN 1 ELSE 0 END) as record_verified,
            SUM(CASE WHEN COALESCE(p."species_verification_status", 'pending') = 'pending' THEN 1 ELSE 0 END) as pending_taxonomic,
            SUM(CASE WHEN COALESCE(p."locality_verification_status", 'pending') = 'pending' THEN 1 ELSE 0 END) as pending_locality,
            SUM(CASE WHEN COALESCE(p."record_verification_status", 'pending') = 'pending' THEN 1 ELSE 0 END) as pending_record,
            SUM(CASE WHEN p.final_primary_id IS NOT NULL THEN 1 ELSE 0 END) as cataloged
        FROM primary_temp p
        JOIN warning_analysis wa ON p."PrimaryID" = wa."PrimaryID"
        WHERE p.batch_serial_id = $1
        """

        result = await execute_query(query, batch_serial_id)

        if not result:
            return ResponseModel(
                code=404,
                message=f"Batch {batch_serial_id} not found"
            )

        stats = result[0]
        total = stats["total_records"]

        summary = {
            "batch_serial_id": batch_serial_id,
            "total_records": total,
            "statistics": {
                "fully_verified": {
                    "count": stats["fully_verified"],
                    "percentage": round((stats["fully_verified"] / total * 100), 1) if total > 0 else 0
                },
                "cataloged": {
                    "count": stats["cataloged"],
                    "remaining": stats["fully_verified"] - stats["cataloged"],
                    "percentage": round((stats["cataloged"] / stats["fully_verified"] * 100), 1) if stats["fully_verified"] > 0 else 0
                },
                "has_errors": {
                    "count": stats["has_errors"],
                    "percentage": round((stats["has_errors"] / total * 100), 1) if total > 0 else 0
                },
                "has_warnings": {
                    "count": stats["has_warnings"],
                    "percentage": round((stats["has_warnings"] / total * 100), 1) if total > 0 else 0
                },
                "has_pending": {
                    "count": stats["has_pending"],
                    "percentage": round((stats["has_pending"] / total * 100), 1) if total > 0 else 0
                },
                "species_verified": {
                    "count": stats["species_verified"],
                    "percentage": round((stats["species_verified"] / total * 100), 1) if total > 0 else 0
                },
                "locality_verified": {
                    "count": stats["locality_verified"],
                    "percentage": round((stats["locality_verified"] / total * 100), 1) if total > 0 else 0
                },
                "record_verified": {
                    "count": stats["record_verified"],
                    "percentage": round((stats["record_verified"] / total * 100), 1) if total > 0 else 0
                },
                "pending_taxonomic": {
                    "count": stats["pending_taxonomic"],
                    "percentage": round((stats["pending_taxonomic"] / total * 100), 1) if total > 0 else 0
                },
                "pending_locality": {
                    "count": stats["pending_locality"],
                    "percentage": round((stats["pending_locality"] / total * 100), 1) if total > 0 else 0
                },
                "pending_record": {
                    "count": stats["pending_record"],
                    "percentage": round((stats["pending_record"] / total * 100), 1) if total > 0 else 0
                }
            }
        }

        return ResponseModel(
            code=20000,
            data=summary
        )

    except Exception as e:
        return ResponseModel(
            code=500,
            message=f"Failed to get verification summary: {str(e)}"
        )


@router.get("/batches/{batch_serial_id}/debugWarnings")
async def debug_warnings(batch_serial_id: str):
    """
    调试API: 查看批次中所有记录的warnings信息
    """
    try:
        query = """
        SELECT
            "PrimaryID",
            "CatalogNumber",
            "verification_warnings",
            "species_verification_status",
            "locality_verification_status",
            "record_verification_status",
            LENGTH("verification_warnings") as warnings_length
        FROM primary_temp
        WHERE batch_serial_id = $1
        ORDER BY "CatalogNumber"
        LIMIT 20
        """

        records = await execute_query(query, batch_serial_id)

        # 格式化输出
        debug_info = []
        for r in records:
            debug_info.append({
                "catalog_number": r["CatalogNumber"],
                "warnings_raw": r["verification_warnings"],
                "warnings_length": r["warnings_length"],
                "species_status": r["species_verification_status"],
                "locality_status": r["locality_verification_status"],
                "record_status": r["record_verification_status"]
            })

        return ResponseModel(
            code=20000,
            data={
                "batch_serial_id": batch_serial_id,
                "total_checked": len(records),
                "records": debug_info
            }
        )

    except Exception as e:
        return ResponseModel(
            code=500,
            message=f"Debug failed: {str(e)}"
        )


@router.post("/batches/{batch_serial_id}/revalidate", response_model=ResponseModel)
async def revalidate_batch_records(batch_serial_id: str):
    """
    重新验证批次中的所有记录
    用于已导入的批次重新运行验证逻辑，更新warnings和verification status
    """
    try:
        # 获取批次中的所有primary_temp记录ID
        query = """
        SELECT "PrimaryID"
        FROM primary_temp
        WHERE batch_serial_id = $1
        """
        records = await execute_query(query, batch_serial_id)

        if not records:
            return ResponseModel(
                code=404,
                message=f"No records found for batch {batch_serial_id}"
            )

        primary_temp_ids = [r["PrimaryID"] for r in records]

        # 重新运行验证
        await auto_verify_imported_records(primary_temp_ids, batch_serial_id)

        return ResponseModel(
            code=20000,
            data={
                "batch_serial_id": batch_serial_id,
                "records_validated": len(primary_temp_ids)
            },
            message=f"Successfully re-validated {len(primary_temp_ids)} records"
        )

    except Exception as e:
        return ResponseModel(
            code=500,
            message=f"Failed to revalidate batch: {str(e)}"
        )


@router.get("/batches/{batch_serial_id}/exportIssues")
async def export_batch_issues(
    batch_serial_id: str,
    issue_type: Optional[str] = Query(None, description="Filter: 'warnings', 'errors', 'all'")
):
    """
    导出批次中有issues的记录为Excel
    支持筛选：warnings、errors或all
    Excel中会使用颜色标记不同严重程度的问题
    """
    try:
        # 构建查询条件
        where_conditions = ["p.batch_serial_id = $1"]
        params = [batch_serial_id]

        if issue_type == "warnings":
            where_conditions.append("""
                (p."verification_warnings" IS NOT NULL AND p."verification_warnings" != '[]')
                AND p."overall_verification_status" = 'completed'
            """)
        elif issue_type == "errors":
            where_conditions.append("""
                (p."species_verification_status" = 'pending' OR p."locality_verification_status" = 'pending')
            """)
        else:  # all
            where_conditions.append("""
                (
                    (p."verification_warnings" IS NOT NULL AND p."verification_warnings" != '[]')
                    OR p."species_verification_status" = 'pending'
                    OR p."locality_verification_status" = 'pending'
                )
            """)

        query = f"""
        SELECT
            p."PrimaryID",
            p."CatalogNumber",
            p."species_verification_status",
            p."locality_verification_status",
            p."record_verification_status",
            p."overall_verification_status",
            p."verification_notes",
            p."verification_warnings",
            p."TotalNumber",
            p."Storage",
            p."JarSize",
            p."PrevNumber",
            p."Inventory",
            p."Remarks",
            vt."verbatim_family",
            vt."verbatim_genus",
            vt."verbatim_species",
            vt."match_status",
            vl."verbatim_fieldno",
            vl."verbatim_locality_string"
        FROM primary_temp p
        LEFT JOIN verbatim_taxonomic vt ON p."verbatim_taxonid" = vt."verbatim_taxonid"
        LEFT JOIN verbatim_locality vl ON p."verbatim_localityid" = vl."verbatim_localityid"
        WHERE {" AND ".join(where_conditions)}
        ORDER BY p."CatalogNumber"
        """

        records = await execute_query(query, *params)

        if not records:
            return ResponseModel(
                code=20000,
                data={"message": "No issues found in this batch."}
            )

        # 准备Excel数据
        excel_data = []
        for record in records:
            # 解析warnings JSON
            warnings_list = []
            if record.get("verification_warnings"):
                try:
                    warnings_data = json.loads(record["verification_warnings"])
                    warnings_list = [w["message"] for w in warnings_data]
                except:
                    pass

            row_data = {
                "Catalog Number": record["CatalogNumber"],
                "Family": record.get("verbatim_family", ""),
                "Genus": record.get("verbatim_genus", ""),
                "Species": record.get("verbatim_species", ""),
                "Species Status": record["species_verification_status"],
                "Match Type": record.get("match_status", ""),
                "Field Number": record.get("verbatim_fieldno", ""),
                "Locality": record.get("verbatim_locality_string", ""),
                "Locality Status": record["locality_verification_status"],
                "Total Number": record.get("TotalNumber", ""),
                "Storage": record.get("Storage", ""),
                "Jar Size": record.get("JarSize", ""),
                "Prev Number": record.get("PrevNumber", ""),
                "Inventory": record.get("Inventory", ""),
                "Record Status": record["record_verification_status"],
                "Overall Status": record["overall_verification_status"],
                "Warnings": "; ".join(warnings_list) if warnings_list else "",
                "Notes": record.get("verification_notes", "")
            }
            excel_data.append(row_data)

        # 创建DataFrame
        df = pd.DataFrame(excel_data)

        # 生成Excel文件
        output_filename = f"batch_{batch_serial_id}_issues_{issue_type or 'all'}.xlsx"
        output_path = f"temp/{output_filename}"

        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Issues', index=False)

            # 获取workbook和worksheet对象
            workbook = writer.book
            worksheet = writer.sheets['Issues']

            # 定义格式
            warning_format = workbook.add_format({'bg_color': '#FFF3CD', 'font_color': '#856404'})
            error_format = workbook.add_format({'bg_color': '#F8D7DA', 'font_color': '#721C24'})
            success_format = workbook.add_format({'bg_color': '#D4EDDA', 'font_color': '#155724'})

            # 应用条件格式
            for row_num, row in enumerate(excel_data, start=1):
                # Species Status列着色
                species_col = df.columns.get_loc("Species Status")
                if row["Species Status"] == "pending":
                    worksheet.write(row_num, species_col, row["Species Status"], error_format)
                elif row["Species Status"] == "verified":
                    worksheet.write(row_num, species_col, row["Species Status"], success_format)

                # Locality Status列着色
                locality_col = df.columns.get_loc("Locality Status")
                if row["Locality Status"] == "pending":
                    worksheet.write(row_num, locality_col, row["Locality Status"], error_format)
                elif row["Locality Status"] == "verified":
                    worksheet.write(row_num, locality_col, row["Locality Status"], success_format)

                # Overall Status列着色
                overall_col = df.columns.get_loc("Overall Status")
                if row["Overall Status"] == "pending":
                    worksheet.write(row_num, overall_col, row["Overall Status"], error_format)
                elif row["Overall Status"] == "completed":
                    worksheet.write(row_num, overall_col, row["Overall Status"], success_format)

                # Warnings列着色（如果有warnings）
                warnings_col = df.columns.get_loc("Warnings")
                if row["Warnings"]:
                    worksheet.write(row_num, warnings_col, row["Warnings"], warning_format)

        # 返回文件
        file_like = open(output_path, mode="rb")
        return StreamingResponse(
            file_like,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={output_filename}"}
        )

    except Exception as e:
        return ResponseModel(
            code=500,
            message=f"Failed to export issues: {str(e)}"
        )