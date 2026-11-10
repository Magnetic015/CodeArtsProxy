from __future__ import annotations

import base64
import binascii
import json
import re
from collections.abc import Awaitable, Callable
from typing import Any

from fastapi import HTTPException, Request

from .openai_utils import openai_error

MAX_REQUEST_HARD_CAP = 140 * 1024 * 1024
DEFAULT_REQUEST_LIMIT = 2 * 1024 * 1024


class BodyLimitExceeded(Exception):
    pass


def body_limit(request: Request, max_bytes: int) -> None:
    """Retain the cheap header check for callers outside the ASGI middleware."""
    value = request.headers.get("content-length")
    if value and value.isdigit() and int(value) > max_bytes:
        raise HTTPException(status_code=413, detail="request body exceeds configured limit")


def sanitize_filename(filename: str | None) -> str:
    value = (filename or "upload").replace("\\", "/").rsplit("/", 1)[-1]
    value = re.sub(r'[\x00-\x1f\x7f";\r\n]', "_", value).strip(". ")
    if not value:
        value = "upload"
    if len(value.encode("utf-8")) > 255:
        value = value.encode("utf-8")[:255].decode("utf-8", "ignore").strip(". ")
    return value or "upload"


def data_image_size(url: str) -> int:
    match = re.fullmatch(r"data:image/[^;,]+;base64,([A-Za-z0-9+/=]+)", url)
    if not match:
        raise ValueError("invalid base64 image data URL")
    try:
        return len(base64.b64decode(match.group(1), validate=True))
    except (binascii.Error, ValueError) as exc:
        raise ValueError("invalid base64 image data URL") from exc


def validate_attachment_budget(messages: list[Any], max_bytes: int) -> None:
    total = 0
    for message in messages:
        content = getattr(message, "content", None)
        if not isinstance(content, list):
            continue
        for part in content:
            if not isinstance(part, dict) or part.get("type") != "image_url":
                continue
            image_url = part.get("image_url")
            url = image_url.get("url") if isinstance(image_url, dict) else None
            if isinstance(url, str):
                total += data_image_size(url)
                if total > max_bytes:
                    raise ValueError("attachment total exceeds request limit")


def _settings_from_scope(scope: dict[str, Any]) -> Any:
    target: Any = scope.get("app")
    for _ in range(8):
        state = getattr(target, "state", None)
        runtime = getattr(state, "runtime", None)
        if runtime is not None:
            return getattr(runtime, "settings", None)
        target = getattr(target, "app", None)
        if target is None:
            break
    return None


def request_limit_for_scope(scope: dict[str, Any], settings: Any = None) -> int:
    path = str(scope.get("path", ""))
    settings = settings if settings is not None else _settings_from_scope(scope)
    max_file = int(getattr(settings, "max_file_bytes", 50 * 1024 * 1024))
    max_total = int(getattr(settings, "max_total_attachment_bytes", 100 * 1024 * 1024))
    if path == "/v1/files":
        return min(MAX_REQUEST_HARD_CAP, max_file + 256 * 1024)
    if path in {"/v1/chat/completions", "/native/chat"}:
        # JSON/base64 overhead is bounded by the configured decoded attachment budget.
        return min(MAX_REQUEST_HARD_CAP, max_total * 4 // 3 + 2 * 1024 * 1024)
    if path.startswith("/v1/") or path.startswith("/native/") or path.startswith("/admin/"):
        return DEFAULT_REQUEST_LIMIT
    return 0


class BodyLimitMiddleware:
    """Enforce request bytes while ASGI receives the body, not just Content-Length."""

    def __init__(
        self,
        app: Callable[..., Awaitable[Any]],
        settings_getter: Callable[[], Any] | None = None,
    ):
        self.app = app
        self.settings_getter = settings_getter

    async def __call__(self, scope: dict[str, Any], receive: Callable[..., Awaitable[Any]], send):
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        settings = self.settings_getter() if self.settings_getter is not None else None
        limit = request_limit_for_scope(scope, settings)
        if limit <= 0:
            await self.app(scope, receive, send)
            return

        headers = {key.lower(): value for key, value in scope.get("headers", [])}
        content_length = headers.get(b"content-length")
        if content_length:
            try:
                if int(content_length) > limit:
                    await self._send_too_large(send)
                    return
            except ValueError:
                pass

        received = 0
        response_started = False

        async def limited_receive():
            nonlocal received
            message = await receive()
            if message.get("type") == "http.request":
                received += len(message.get("body", b""))
                if received > limit:
                    raise BodyLimitExceeded
            return message

        async def tracked_send(message):
            nonlocal response_started
            if message.get("type") == "http.response.start":
                response_started = True
            await send(message)

        try:
            await self.app(scope, limited_receive, tracked_send)
        except BodyLimitExceeded:
            if not response_started:
                await self._send_too_large(send)

    @staticmethod
    async def _send_too_large(send) -> None:
        body = json.dumps(
            openai_error("request body exceeds configured limit", code="body_too_large"),
            ensure_ascii=False,
        ).encode("utf-8")
        await send(
            {
                "type": "http.response.start",
                "status": 413,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode("ascii")),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})
