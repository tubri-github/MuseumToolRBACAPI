"""
通用搜索/过滤引擎（跨模块复用：lots / locality / loan / ost …）。

设计目标：把 batch_review 里那套「白名单 + 参数化 + 空格归一 ILIKE + 空值哨兵」的过滤逻辑
抽成一个与具体模块无关的引擎。每个模块声明自己的 FilterSpec（字段白名单 + 类型 + JOIN +
全局搜索列），引擎负责把请求参数翻译成参数化的 WHERE 子句。

请求契约（前端原型已验证，见 docs/prototypes/lots_search_prototype.html）：
  - search:             全局模糊框 → 在 global_search_cols 上多列 ILIKE（OR）。v1 走 DB，
                        以后可把这一支路由到 ES，只需替换调用方，不动引擎。
  - ids:                整数列表 → catalog_number = ANY($n::int[])
  - field_filters:      {api_name: [值/哨兵]} → 包含式 ILIKE（同字段多值 OR，跨字段 AND）
                        + __EMPTY__ / __NOT_EMPTY__ 哨兵。与 batch_review 完全兼容。
  - structured_filters: [{field, op, values}] → 需要显式操作符的精确/区间比较
                        op ∈ equals|is|eq|gte|lte|on|after|before|between|in

防注入：列只能来自 spec.fields 白名单；所有值走 $N 占位符；不在白名单的字段静默跳过。
"""
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple
import json

EMPTY_SENTINEL = "__EMPTY__"
NOT_EMPTY_SENTINEL = "__NOT_EMPTY__"

# 每种类型在前端可用的操作符（同时用于 filter-metadata 输出）
TYPE_OPERATORS: Dict[str, List[str]] = {
    "text":   ["contains", "equals", "fuzzy", "empty", "not_empty"],
    "enum":   ["is", "empty", "not_empty"],
    "idlist": ["in", "empty", "not_empty"],
    "number": ["eq", "gte", "lte", "between", "in"],
    "date":   ["on", "after", "before", "between", "empty", "not_empty"],
}

# fuzzy 操作符默认 trigram 相似度阈值（0.4：percdae/percida 命中、percda 不命中）
FUZZY_THRESHOLD = 0.4

# structured 比较操作符 → SQL 运算符
_CMP = {"eq": "=", "gte": ">=", "lte": "<=", "on": "=", "after": ">", "before": "<"}


@dataclass
class FieldDef:
    sql: str                              # SQL 列表达式，如 'p."CatalogNumber"'
    type: str                             # text|enum|idlist|number|date
    label: str = ""                       # 前端展示名（metadata 用）
    group: str = ""                       # 前端分组（metadata 用）
    options: Optional[List[str]] = None   # enum 可选值（可选）


@dataclass
class FilterSpec:
    base: str                                                  # 基表+别名，如 '"Primary" p'
    fields: Dict[str, FieldDef]
    select: str                                                # SELECT 列清单
    joins: str = ""
    order_by: str = ""                                         # 如 'p."PrimaryID" DESC'
    global_search_cols: List[str] = field(default_factory=list)  # 全局框扫哪些字段（api_name）

    def to_metadata(self) -> List[Dict[str, Any]]:
        """供 GET /<module>/filter-metadata 返回，驱动前端 chip 选择器。"""
        out = []
        for key, fd in self.fields.items():
            item = {
                "key": key,
                "label": fd.label or key,
                "group": fd.group or "",
                "type": fd.type,
                "operators": TYPE_OPERATORS.get(fd.type, []),
            }
            if fd.options:
                item["options"] = fd.options
            out.append(item)
        return out

    def build_queries(self, where_clauses: List[str], order_by: Optional[str] = None) -> Tuple[str, str]:
        """根据 WHERE 子句拼出 (main_query, count_query)，共用同一份 JOIN 保证一致。
        order_by 显式传入时覆盖默认排序（用于表头点击排序，整个结果集排序后再分页）。"""
        where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
        main = f"SELECT {self.select} FROM {self.base} {self.joins}{where_sql}"
        ob = order_by or self.order_by
        if ob:
            main += f' ORDER BY {ob}'
        count = f"SELECT COUNT(*) FROM {self.base} {self.joins}{where_sql}"
        return main, count

    def order_clause(self, sort_by: Optional[str], sort_order: Optional[str]) -> Optional[str]:
        """把前端传的 sort_by(api 字段名) + sort_order 翻译成安全的 ORDER BY 片段（白名单防注入）。"""
        fd = self.fields.get(sort_by) if sort_by else None
        if not fd:
            return None
        direction = "ASC" if str(sort_order).lower() in ("asc", "ascending") else "DESC"
        return f"{fd.sql} {direction} NULLS LAST"


def _cast_text(expr: str) -> str:
    return f"{expr}::text"


def _coerce(fd: FieldDef, v: Any) -> Any:
    """把字符串值转成与列类型匹配的 Python 值，便于 asyncpg 正确编码。"""
    if fd.type in ("number", "idlist"):
        try:
            return int(v)
        except (TypeError, ValueError):
            try:
                return float(v)
            except (TypeError, ValueError):
                return v
    if fd.type == "date":
        if isinstance(v, (date, datetime)):
            return v
        s = str(v).strip()
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                continue
        return s
    return str(v)


def parse_json_param(raw: Optional[str]) -> Any:
    """安全解析 query 里的 JSON 字符串（field_filters / structured_filters）。失败返回 None。"""
    if not raw:
        return None
    try:
        return json.loads(raw)
    except (ValueError, TypeError):
        return None


def build_global_search(
    spec: FilterSpec,
    search: str,
    threshold: float,
    start_param: int,
) -> Tuple[Optional[str], Optional[str], List[Any], int]:
    """全局模糊框：在 global_search_cols 上做 (子串 ILIKE) OR (trigram 相似 >= 阈值)，
    并产出相关性排序表达式：精确等于=3 > 子串=2 > similarity(0..1)。
    返回 (where_clause, rank_expr, params, next_param)。pg_trgm 已装；列上有 GIN trigram 索引。
    """
    q = str(search).strip()
    cols = [spec.fields[k].sql for k in spec.global_search_cols if k in spec.fields]
    if not q or not cols:
        return None, None, [], start_param

    p_q = start_param        # 原始查询词（similarity / 精确比较用）
    p_like = start_param + 1  # %q%（子串）
    p_thr = start_param + 2   # 相似度阈值
    params: List[Any] = [q, f"%{q}%", threshold]

    like_terms = [f"{_cast_text(c)} ILIKE ${p_like}" for c in cols]
    sim_terms = [f"similarity({_cast_text(c)}, ${p_q})" for c in cols]
    where = "((" + " OR ".join(like_terms) + ") OR GREATEST(" + ", ".join(sim_terms) + f") >= ${p_thr})"

    rank_cases = [
        f"CASE WHEN LOWER({_cast_text(c)}) = LOWER(${p_q}) THEN 3 "
        f"WHEN {_cast_text(c)} ILIKE ${p_like} THEN 2 "
        f"ELSE similarity({_cast_text(c)}, ${p_q}) END"
        for c in cols
    ]
    rank = "GREATEST(" + ", ".join(rank_cases) + ")"
    return where, rank, params, start_param + 3


def build_where(
    spec: FilterSpec,
    *,
    search: Optional[str] = None,
    ids: Optional[List[int]] = None,
    field_filters: Optional[Dict[str, Any]] = None,
    structured_filters: Optional[List[Dict[str, Any]]] = None,
    start_param: int = 1,
) -> Tuple[List[str], List[Any]]:
    """把请求参数翻译成 (where_clauses, params)。params 用 $start_param 起的占位符。"""
    where: List[str] = []
    params: List[Any] = []
    p = start_param

    # 1) ids → catalog_number = ANY($n::int[])
    if ids:
        cat = spec.fields.get("catalog_number")
        if cat:
            where.append(f'{cat.sql} = ANY(${p}::int[])')
            params.append(ids)
            p += 1

    # 2) 全局模糊框 → 多列 ILIKE（共用一个参数，OR）
    if search and str(search).strip():
        cols = [spec.fields[k].sql for k in spec.global_search_cols if k in spec.fields]
        if cols:
            ors = [f'{_cast_text(c)} ILIKE ${p}' for c in cols]
            params.append(f"%{str(search).strip()}%")
            p += 1
            where.append("(" + " OR ".join(ors) + ")")

    # 3) field_filters → 包含式 ILIKE + 哨兵（同字段多值 OR，跨字段 AND）
    if isinstance(field_filters, dict):
        for key, raw_values in field_filters.items():
            fd = spec.fields.get(key)
            if not fd:                       # 不在白名单：静默跳过（防注入）
                continue
            if isinstance(raw_values, str):
                raw_values = [raw_values]
            if not isinstance(raw_values, list):
                continue

            normal, has_empty, has_not_empty = [], False, False
            for v in raw_values:
                if v == EMPTY_SENTINEL:
                    has_empty = True
                elif v == NOT_EMPTY_SENTINEL:
                    has_not_empty = True
                elif v is not None and str(v).strip() != "":
                    normal.append(str(v).strip())
            if not normal and not has_empty and not has_not_empty:
                continue

            or_terms: List[str] = []
            for val in normal:
                if fd.type == "enum":
                    # 枚举做大小写无关的精确等于（空格归一）
                    or_terms.append(f"LOWER(TRIM({_cast_text(fd.sql)})) = LOWER(TRIM(${p}))")
                    params.append(val)
                else:
                    # 列和值都剥内部空白再 ILIKE，容忍 "A.AFFINIS" vs "A. affinis"
                    or_terms.append(
                        f"REGEXP_REPLACE({_cast_text(fd.sql)}, '[[:space:]]+', '', 'g') "
                        f"ILIKE REGEXP_REPLACE(${p}, '[[:space:]]+', '', 'g')"
                    )
                    params.append(f"%{val}%")
                p += 1
            if has_empty:
                or_terms.append(f"({fd.sql} IS NULL OR TRIM({_cast_text(fd.sql)}) = '')")
            if has_not_empty:
                or_terms.append(f"({fd.sql} IS NOT NULL AND TRIM({_cast_text(fd.sql)}) <> '')")
            where.append("(" + " OR ".join(or_terms) + ")")

    # 4) structured_filters → 显式操作符的精确/区间比较
    if isinstance(structured_filters, list):
        for sf in structured_filters:
            if not isinstance(sf, dict):
                continue
            key = sf.get("field")
            op = sf.get("op")
            vals = sf.get("values") or []
            fd = spec.fields.get(key)
            if not fd or not op:
                continue

            if op in ("equals", "is"):
                # 同一列多值 = OR（如 jar_size is 0.5L 或 1.0L）
                vlist = [str(v).strip() for v in vals if str(v).strip() != ""]
                if not vlist:
                    continue
                terms = []
                for v in vlist:
                    terms.append(f"LOWER(TRIM({_cast_text(fd.sql)})) = LOWER(TRIM(${p}))")
                    params.append(v)
                    p += 1
                where.append("(" + " OR ".join(terms) + ")")
            elif op == "in":
                coerced = [_coerce(fd, v) for v in vals if str(v).strip() != ""]
                if coerced:
                    placeholders = []
                    for cv in coerced:
                        placeholders.append(f"${p}")
                        params.append(cv)
                        p += 1
                    where.append(f"{fd.sql} IN (" + ",".join(placeholders) + ")")
            elif op == "between":
                lo = vals[0] if len(vals) > 0 else None
                hi = vals[1] if len(vals) > 1 else None
                if lo not in (None, ""):
                    where.append(f"{fd.sql} >= ${p}")
                    params.append(_coerce(fd, lo))
                    p += 1
                if hi not in (None, ""):
                    where.append(f"{fd.sql} <= ${p}")
                    params.append(_coerce(fd, hi))
                    p += 1
            elif op in _CMP:
                if not vals:
                    continue
                where.append(f"{fd.sql} {_CMP[op]} ${p}")
                params.append(_coerce(fd, vals[0]))
                p += 1
            elif op == "fuzzy":
                # 单列 trigram 模糊：similarity >= 阈值（容忍拼写错误）
                if not vals:
                    continue
                where.append(f"similarity({_cast_text(fd.sql)}, ${p}) >= ${p + 1}")
                params.append(str(vals[0]))
                params.append(FUZZY_THRESHOLD)
                p += 2

    return where, params
