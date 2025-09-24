import pandas as pd
import re
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from app.db.database import execute_query


class ImportValidationUtils:
    """导入验证工具类"""

    @staticmethod
    def validate_date(date_str) -> Tuple[bool, Optional[str]]:
        """
        验证日期字符串并转换为YYYY-MM-DD格式
        """
        if not date_str or pd.isna(date_str):
            return False, None

        try:
            date_str = str(date_str).strip()

            # 尝试各种日期格式
            date_formats = [
                "%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%d-%m-%Y",
                "%d/%m/%Y", "%b %d, %Y", "%d %b %Y", "%B %d, %Y",
                "%Y%m%d", "%m-%d-%Y", "%d.%m.%Y"
            ]

            for fmt in date_formats:
                try:
                    date_obj = datetime.strptime(date_str, fmt)
                    return True, date_obj.strftime("%Y-%m-%d")
                except ValueError:
                    continue

            return False, None
        except Exception:
            return False, None

    @staticmethod
    async def validate_locality_id(locality_id) -> bool:
        """验证Locality ID是否存在于数据库中"""
        if pd.isna(locality_id) or str(locality_id).strip() == "":
            return False

        query = """
        SELECT COUNT(*) as count
        FROM locality1
        WHERE "Locality1ID" = $1
        """

        result = await execute_query(query, str(locality_id).strip())
        return result and result[0]["count"] > 0

    @staticmethod
    def validate_numeric_field(value, field_name: str, min_val=None, max_val=None) -> Tuple[bool, str]:
        """验证数值字段"""
        if pd.isna(value) or value == "":
            return True, ""  # 空值通常是允许的

        try:
            num_val = float(value)
            if min_val is not None and num_val < min_val:
                return False, f"{field_name} cannot be less than {min_val}"
            if max_val is not None and num_val > max_val:
                return False, f"{field_name} cannot be greater than {max_val}"
            return True, ""
        except (ValueError, TypeError):
            return False, f"{field_name} must be a valid number"

    @staticmethod
    def validate_coordinate(coord_str, coord_type: str) -> Tuple[bool, Optional[float], str]:
        """
        验证坐标（经度或纬度）
        coord_type: 'latitude' 或 'longitude'
        """
        if pd.isna(coord_str) or str(coord_str).strip() == "":
            return True, None, ""

        try:
            coord_val = float(coord_str)

            if coord_type == 'latitude':
                if -90 <= coord_val <= 90:
                    return True, coord_val, ""
                else:
                    return False, None, "Latitude must be between -90 and 90"
            elif coord_type == 'longitude':
                if -180 <= coord_val <= 180:
                    return True, coord_val, ""
                else:
                    return False, None, "Longitude must be between -180 and 180"
            else:
                return False, None, f"Unknown coordinate type: {coord_type}"

        except (ValueError, TypeError):
            return False, None, f"{coord_type.capitalize()} must be a valid number"

    @staticmethod
    def clean_text_field(text_str, max_length=None) -> str:
        """清理文本字段"""
        if pd.isna(text_str):
            return ""

        # 转换为字符串并清理
        cleaned = str(text_str).strip()

        # 移除多余的空格
        cleaned = re.sub(r'\s+', ' ', cleaned)

        # 截断过长的文本
        if max_length and len(cleaned) > max_length:
            cleaned = cleaned[:max_length].strip()

        return cleaned

    @staticmethod
    async def get_valid_locality_ids(limit=1000) -> List[str]:
        """获取有效的Locality ID列表"""
        query = """
        SELECT "Locality1ID"
        FROM locality1
        ORDER BY "Locality1ID"
        LIMIT $1
        """

        result = await execute_query(query, limit)
        return [str(row["Locality1ID"]) for row in result] if result else []


    @staticmethod
    def sanitize_scientific_name(name_str) -> str:
        """清理科学名称"""
        if pd.isna(name_str):
            return ""

        # 基本清理
        cleaned = str(name_str).strip()

        # 移除多余空格
        cleaned = re.sub(r'\s+', ' ', cleaned)

        # 确保第一个字母大写（属名）
        words = cleaned.split()
        if words:
            words[0] = words[0].capitalize()
            if len(words) > 1:
                # 种名小写
                words[1] = words[1].lower()
            cleaned = ' '.join(words)

        return cleaned

    @staticmethod
    def extract_validation_errors(df: pd.DataFrame, validation_results: Dict) -> List[Dict]:
        """从验证结果中提取错误信息"""
        error_list = []

        for field, errors in validation_results.items():
            if errors:
                for error in errors:
                    error_list.append({
                        "field": field,
                        "row_index": error.get("row_index"),
                        "value": error.get("value"),
                        "error_message": error.get("message"),
                        "error_type": error.get("type", "validation_error")
                    })

        return error_list

    @staticmethod
    def create_validation_summary(total_records: int, validation_results: Dict) -> Dict:
        """创建验证摘要"""
        total_errors = 0
        error_by_field = {}

        for field, errors in validation_results.items():
            error_count = len(errors) if errors else 0
            if error_count > 0:
                error_by_field[field] = error_count
                total_errors += error_count

        valid_records = total_records - len(set(
            error.get("row_index") for errors in validation_results.values()
            for error in errors if error.get("row_index") is not None
        ))

        return {
            "total_records": total_records,
            "valid_records": valid_records,
            "invalid_records": total_records - valid_records,
            "total_errors": total_errors,
            "error_by_field": error_by_field,
            "validation_rate": round(valid_records / total_records * 100, 2) if total_records > 0 else 0
        }