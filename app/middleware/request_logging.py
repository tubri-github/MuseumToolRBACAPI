# app/middleware/optimized_request_logging.py
import json
import time
import traceback
import uuid
from datetime import datetime
from typing import Optional, Dict, Any
from fastapi import Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp, Receive, Send, Message
import asyncio
import aiofiles
import os
from pathlib import Path
import logging
from logging.handlers import RotatingFileHandler


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    def __init__(
            self,
            app: ASGIApp,
            log_dir: str = "logs",
            max_file_size: int = 50 * 1024 * 1024,  # 50MB
            backup_count: int = 10,
            exclude_paths: list = None,
            log_format: str = "json",  # "json" or "text"
            log_request_body: bool = False,  # 是否对**所有**请求记录请求体（有性能代价）
            log_body_on_error: bool = True,  # 只在 4xx/5xx 时记录请求体+响应体（排障用，代价很小）
            max_body_bytes: int = 64 * 1024  # 超过这个大小不缓冲（跳过文件上传）
    ):
        super().__init__(app)
        self.log_dir = Path(log_dir)
        self.max_file_size = max_file_size
        self.backup_count = backup_count
        self.exclude_paths = exclude_paths or ["/docs", "/redoc", "/openapi.json", "/favicon.ico", "/metrics"]
        self.log_format = log_format
        self.log_request_body = log_request_body
        self.log_body_on_error = log_body_on_error
        self.max_body_bytes = max_body_bytes

        # 确保日志目录存在
        self.log_dir.mkdir(parents=True, exist_ok=True)

        # 设置日志记录器
        self._setup_loggers()

    def _setup_loggers(self):
        """设置不同类型的日志记录器"""

        # 主请求日志
        self.request_logger = logging.getLogger("request_logger")
        self.request_logger.setLevel(logging.INFO)
        self.request_logger.handlers.clear()  # 清除已有的处理器
        request_handler = RotatingFileHandler(
            self.log_dir / "requests.log",
            maxBytes=self.max_file_size,
            backupCount=self.backup_count,
            encoding='utf-8'
        )
        request_handler.setFormatter(logging.Formatter('%(message)s'))
        self.request_logger.addHandler(request_handler)
        self.request_logger.propagate = False

        # 错误日志
        self.error_logger = logging.getLogger("error_logger")
        self.error_logger.setLevel(logging.ERROR)
        self.error_logger.handlers.clear()
        error_handler = RotatingFileHandler(
            self.log_dir / "errors.log",
            maxBytes=self.max_file_size,
            backupCount=self.backup_count,
            encoding='utf-8'
        )
        error_handler.setFormatter(logging.Formatter('%(message)s'))
        self.error_logger.addHandler(error_handler)
        self.error_logger.propagate = False

        # 慢请求日志
        self.slow_logger = logging.getLogger("slow_logger")
        self.slow_logger.setLevel(logging.WARNING)
        self.slow_logger.handlers.clear()
        slow_handler = RotatingFileHandler(
            self.log_dir / "slow_requests.log",
            maxBytes=self.max_file_size,
            backupCount=self.backup_count,
            encoding='utf-8'
        )
        slow_handler.setFormatter(logging.Formatter('%(message)s'))
        self.slow_logger.addHandler(slow_handler)
        self.slow_logger.propagate = False

    async def dispatch(self, request: Request, call_next):
        # 跳过不需要记录的路径
        if request.url.path in self.exclude_paths:
            return await call_next(request)

        # 生成请求ID并添加到请求状态
        request_id = str(uuid.uuid4())
        request.state.request_id = request_id
        start_time = time.time()

        # 收集基础请求信息（不读取请求体）
        request_data = self._collect_basic_request_data(request, request_id)

        # 请求体：要么全量记录(log_request_body)，要么先缓冲着、只有出错时才写进日志
        # (log_body_on_error)。缓冲后必须把 body 重新塞回去，否则下游读不到。
        buffered_body = None
        if request.method in ["POST", "PUT", "PATCH"] and (self.log_request_body or self.log_body_on_error):
            buffered_body = await self._buffer_request_body(request)
            if self.log_request_body and buffered_body is not None:
                request_data["body"] = self._redact(buffered_body)

        # 执行请求
        try:
            response = await call_next(request)

            # 收集响应信息
            processing_time = time.time() - start_time

            # 4xx/5xx：把请求体和响应体一起记下来。以前只有"未处理异常"才算错误，
            # 而 FastAPI 把 HTTPException(500) 转成正常响应返回 —— call_next 不抛异常，
            # 于是 errors.log 里根本没有这些 500，排障时等于什么都没记。
            error_data = None
            if response.status_code >= 400:
                response_body = await self._capture_response_body(response)
                response = self._rebuild_response(response, response_body)
                error_data = {
                    "error_type": f"HTTP_{response.status_code}",
                    "error_message": self._decode(response_body),
                    "status_code": response.status_code,
                }
                if buffered_body is not None:
                    error_data["request_body"] = self._redact(buffered_body)

            response_data = self._collect_response_data(response, processing_time)

            # 异步记录日志（不阻塞响应）
            asyncio.create_task(self._log_request(request_data, response_data, error_data))

            return response

        except Exception as e:
            # 收集错误信息
            processing_time = time.time() - start_time
            error_data = {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc()[-4000:],
                "status_code": 500
            }
            if buffered_body is not None:
                error_data["request_body"] = self._redact(buffered_body)

            response_data = {
                "status_code": 500,
                "processing_time": processing_time,
                "response_size": 0
            }

            # 异步记录错误日志
            asyncio.create_task(self._log_request(request_data, response_data, error_data))

            # 返回错误响应
            return JSONResponse(
                status_code=500,
                content={"detail": "Internal server error", "request_id": request_id}
            )

    def _collect_basic_request_data(self, request: Request, request_id: str) -> Dict[str, Any]:
        """收集基础请求数据（不读取请求体）"""
        # 解析用户代理
        user_agent = request.headers.get("user-agent", "")
        browser_info = self._parse_user_agent(user_agent)

        # 获取客户端IP
        client_ip = self._get_client_ip(request)

        return {
            "request_id": request_id,
            "timestamp": datetime.utcnow().isoformat(),
            "method": request.method,
            "url": str(request.url),
            "path": request.url.path,
            "query_params": dict(request.query_params),
            "headers": {k: v for k, v in request.headers.items()
                        if k.lower() not in ['authorization', 'cookie']},  # 排除敏感头
            "client_ip": client_ip,
            "user_agent": user_agent[:200],  # 限制长度
            "browser": browser_info["browser"],
            "browser_version": browser_info["version"],
            "os": browser_info["os"],
            "device": browser_info["device"]
        }

    # 请求体里不该进日志的字段（大小写不敏感）
    _SENSITIVE_KEYS = ("password", "passwd", "token", "secret", "authorization", "api_key")

    async def _buffer_request_body(self, request: Request) -> Optional[bytes]:
        """读出请求体并**重新塞回去**，否则下游 endpoint 会读到空 body。
        跳过文件上传和超大请求（排障要的是表单 JSON，不是几十 MB 的 xlsx）。"""
        content_type = request.headers.get("content-type", "")
        if "multipart/form-data" in content_type:
            return None
        try:
            length = int(request.headers.get("content-length") or 0)
        except ValueError:
            length = 0
        if length > self.max_body_bytes:
            return None
        try:
            body = await request.body()
        except Exception:
            return None
        if len(body) > self.max_body_bytes:
            return None

        # 把读走的字节重新提供给下游
        async def receive() -> Message:
            return {"type": "http.request", "body": body, "more_body": False}

        request._receive = receive
        return body

    async def _capture_response_body(self, response: Response) -> bytes:
        """把（出错的）响应体读出来，好把 detail 记进日志。仅在 4xx/5xx 时调用。"""
        if hasattr(response, "body") and response.body is not None:
            return bytes(response.body)
        chunks = []
        try:
            async for chunk in response.body_iterator:
                chunks.append(chunk if isinstance(chunk, bytes) else str(chunk).encode())
                if sum(len(c) for c in chunks) > self.max_body_bytes:
                    break
        except Exception:
            pass
        return b"".join(chunks)

    @staticmethod
    def _rebuild_response(response: Response, body: bytes) -> Response:
        """body_iterator 被读干了，要用同样的状态码/头重建一个可返回的响应。"""
        if hasattr(response, "body") and response.body is not None:
            return response
        rebuilt = Response(
            content=body,
            status_code=response.status_code,
            headers=dict(response.headers),
            media_type=response.media_type,
        )
        # Content-Length 由 Response 重新算，避免和原头里的值冲突
        rebuilt.headers["content-length"] = str(len(body))
        return rebuilt

    @staticmethod
    def _decode(body: bytes) -> str:
        return body.decode("utf-8", errors="ignore")[:2000] if body else ""

    def _redact(self, body: bytes) -> str:
        """尽量按 JSON 结构打码敏感字段；不是 JSON 就原样截断。"""
        text = self._decode(body)
        if not text:
            return ""
        try:
            data = json.loads(text)
        except (ValueError, TypeError):
            return text

        def scrub(node):
            if isinstance(node, dict):
                return {k: ("***" if any(s in k.lower() for s in self._SENSITIVE_KEYS) else scrub(v))
                        for k, v in node.items()}
            if isinstance(node, list):
                return [scrub(x) for x in node]
            return node

        try:
            return json.dumps(scrub(data), ensure_ascii=False)[:4000]
        except (TypeError, ValueError):
            return text

    def _collect_response_data(self, response: Response, processing_time: float) -> Dict[str, Any]:
        """收集响应数据"""
        # 获取响应大小
        response_size = 0
        if hasattr(response, 'body') and response.body:
            response_size = len(response.body)

        return {
            "status_code": response.status_code,
            "processing_time": round(processing_time, 3),
            "response_size": response_size
        }

    def _get_client_ip(self, request: Request) -> str:
        """获取真实客户端IP"""
        # 检查代理头
        forwarded_for = request.headers.get("x-forwarded-for")
        if forwarded_for:
            return forwarded_for.split(",")[0].strip()

        real_ip = request.headers.get("x-real-ip")
        if real_ip:
            return real_ip

        # 默认使用直接连接IP
        return request.client.host if request.client else "unknown"

    def _parse_user_agent(self, user_agent: str) -> Dict[str, str]:
        """解析用户代理字符串"""
        browser_info = {
            "browser": "unknown",
            "version": "unknown",
            "os": "unknown",
            "device": "desktop"
        }

        if not user_agent:
            return browser_info

        user_agent_lower = user_agent.lower()

        # 检测浏览器
        if "chrome" in user_agent_lower and "edg" not in user_agent_lower:
            browser_info["browser"] = "Chrome"
        elif "firefox" in user_agent_lower:
            browser_info["browser"] = "Firefox"
        elif "safari" in user_agent_lower and "chrome" not in user_agent_lower:
            browser_info["browser"] = "Safari"
        elif "edg" in user_agent_lower:
            browser_info["browser"] = "Edge"
        elif "opera" in user_agent_lower:
            browser_info["browser"] = "Opera"

        # 检测操作系统
        if "windows" in user_agent_lower:
            browser_info["os"] = "Windows"
        elif "mac" in user_agent_lower:
            browser_info["os"] = "macOS"
        elif "linux" in user_agent_lower:
            browser_info["os"] = "Linux"
        elif "android" in user_agent_lower:
            browser_info["os"] = "Android"
            browser_info["device"] = "mobile"
        elif "iphone" in user_agent_lower or "ipad" in user_agent_lower:
            browser_info["os"] = "iOS"
            browser_info["device"] = "mobile" if "iphone" in user_agent_lower else "tablet"

        return browser_info

    async def _log_request(self, request_data: Dict[str, Any], response_data: Dict[str, Any],
                           error_data: Optional[Dict[str, Any]]):
        """异步记录请求日志到不同的文件"""
        try:
            # 合并日志数据
            log_entry = {
                **request_data,
                **response_data,
                "is_error": error_data is not None
            }

            if error_data:
                log_entry.update(error_data)

            # 格式化日志
            if self.log_format == "json":
                log_message = json.dumps(log_entry, ensure_ascii=False)
            else:
                log_message = self._format_text_log(log_entry)

            # 记录到主请求日志
            self.request_logger.info(log_message)

            # 记录到特定类型的日志
            if error_data:
                # 错误日志
                error_log = {
                    "timestamp": log_entry["timestamp"],
                    "request_id": log_entry["request_id"],
                    "method": log_entry["method"],
                    "path": log_entry["path"],
                    "query_params": log_entry.get("query_params"),
                    "client_ip": log_entry["client_ip"],
                    "error_type": error_data["error_type"],
                    "error_message": error_data["error_message"],
                    # 出错时把提交上来的参数一并记下 —— 没有它，远端报错只能靠猜
                    "request_body": error_data.get("request_body"),
                    "traceback": error_data.get("traceback"),
                    "user_agent": log_entry["user_agent"]
                }
                self.error_logger.error(json.dumps(error_log, ensure_ascii=False))

            elif log_entry["processing_time"] > 2.0:
                # 慢请求日志（超过2秒）
                slow_log = {
                    "timestamp": log_entry["timestamp"],
                    "request_id": log_entry["request_id"],
                    "method": log_entry["method"],
                    "path": log_entry["path"],
                    "processing_time": log_entry["processing_time"],
                    "client_ip": log_entry["client_ip"],
                    "browser": log_entry["browser"]
                }
                self.slow_logger.warning(json.dumps(slow_log, ensure_ascii=False))

        except Exception as e:
            # 日志记录失败时，至少打印到控制台
            print(f"Failed to log request: {e}")

    def _format_text_log(self, log_entry: Dict[str, Any]) -> str:
        """格式化为可读的文本格式"""
        return (
            f"{log_entry['timestamp']} | "
            f"{log_entry['request_id'][:8]} | "
            f"{log_entry['method']} {log_entry['path']} | "
            f"{log_entry['client_ip']} | "
            f"{log_entry['status_code']} | "
            f"{log_entry['processing_time']}s | "
            f"{log_entry['browser']} | "
            f"{log_entry['os']} | "
            f"{'ERROR: ' + log_entry.get('error_message', '') if log_entry['is_error'] else 'OK'}"
        )