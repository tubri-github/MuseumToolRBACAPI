import os
import uuid
import json

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
        required_fields = ["prevNumber"]
        missing_fields = [field for field in required_fields if field not in mappings or not mappings[field]]

        if missing_fields:
            return ResponseModel(
                code=40000,
                data={},
                message=f"Missing required field mappings: {', '.join(missing_fields)}"
            )

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
                        elif field == "localityId":
                            record["locality_id"] = value if not pd.isna(value) else None
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

        # 获取编目号
        catalog_numbers = await db_utils.get_next_catalog_numbers(len(valid_records))

        # 为记录分配编目号
        for i, record in enumerate(valid_records):
            record["catalog_number"] = catalog_numbers[i]

            # # 如果没有字段编号，生成一个
            # if not record["field_number"]:
            #     record["field_number"] = validation_utils.generate_field_number(
            #         sequence=i + 1
            #     )

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
            "preparationIds": prep_ids
        })

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
                "match_info": match_result["match_info"]
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
                "fieldNumber": "verbatim_fieldno"
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
                "match_type": match_result["match_status"]  # 使用实际的匹配状态
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
                    elif field == "localityId":
                        record["locality_id"] = value if not pd.isna(value) else None
                    elif field == "totalNumber":
                        try:
                            record["total_number"] = int(float(value)) if not pd.isna(value) else 1
                        except (ValueError, TypeError):
                            record["total_number"] = 1  # 默认值
                    elif field == "storage":
                        record["storage"] = str(value) if not pd.isna(value) else None
                    elif field == "jarSize":
                        record["jar_size"] = str(value) if not pd.isna(value) else None
                    elif field == "prevNumber":
                        record["prev_number"] = str(value) if not pd.isna(value) else None
                    elif field == "inventory":
                        record["inventory"] = str(value) if not pd.isna(value) else None
                    elif field == "remarks":
                        existing_remarks = record["remarks"]
                        if not pd.isna(value) and str(value).strip():
                            record["remarks"] = f"{str(value).strip()} | {existing_remarks}"

            valid_records.append(record)
            total_numbers.append(record["total_number"])

        # 4. 批量插入verbatim记录
        print(f"插入 {len(verbatim_taxonomic_records)} 条 verbatim taxonomic 记录...")
        verbatim_taxonomic_ids = await db_utils.insert_verbatim_taxonomic_records(verbatim_taxonomic_records)

        print(f"插入 {len(verbatim_locality_records)} 条 verbatim locality 记录...")
        verbatim_locality_ids = await db_utils.insert_verbatim_locality_records(verbatim_locality_records)

        # 5. 获取编目号
        catalog_numbers = await db_utils.get_next_catalog_numbers(len(valid_records))

        # 6. 为Primary记录分配编目号和verbatim IDs
        for i, record in enumerate(valid_records):
            record["catalog_number"] = catalog_numbers[i]
            record["verbatim_taxonomic_id"] = verbatim_taxonomic_ids[i] if i < len(verbatim_taxonomic_ids) else None
            record["verbatim_locality_id"] = verbatim_locality_ids[i] if i < len(verbatim_locality_ids) else None

        # 7. 插入Primary记录
        print(f"插入 {len(valid_records)} 条 Primary 记录...")
        primary_ids = await db_utils.insert_primary_records(valid_records, batch_serial_id)

        # 8. 插入Preparation记录
        print(f"插入 {len(primary_ids)} 条 Preparation 记录...")
        prep_ids = await db_utils.insert_preparation_records(primary_ids, total_numbers, "Fluid")

        # 9. 更新导入状态
        await store_import_status(file_id, {
            "fileId": file_id,
            "status": "completed",
            "fileName": file_info["fileName"],
            "totalRecords": len(df),
            "importedCount": len(primary_ids),
            "skippedCount": 0,  # verbatim模式下不跳过记录
            "success": True,
            "userId": user_id,
            "batchSerialId": batch_serial_id,
            "preparationIds": prep_ids,
            "verbatimTaxonomicIds": verbatim_taxonomic_ids,
            "verbatimLocalityIds": verbatim_locality_ids,
            "note": "Records imported in verbatim mode - all original data and matching results preserved in verbatim tables for manual review"
        })

        # 10. 清理临时文件
        if os.path.exists(file_path):
            os.remove(file_path)

        print(f"Verbatim导入完成: {len(primary_ids)} 条记录")

    except Exception as e:
        print(f"Verbatim导入失败: {str(e)}")
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
                "Locality_ID": ["1", "2", "Must be a valid Locality1ID from the system"],
                "Field_Number": ["FIELD-001", "FIELD-002", "Optional - auto-generated if empty"],
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