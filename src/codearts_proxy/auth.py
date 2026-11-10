from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from typing import Any

from fastapi import HTTPException, Request, status
from fastapi.security.utils import get_authorization_scheme_param

from .openai_utils import openai_error
from .runtime import Runtime
from .service import Caller
from .state import StateStore


def runtime(request: Request) -> Runtime:
    value = getattr(request.app.state, "runtime", None)
    if value is None:
        raise HTTPException(status_code=503, detail="proxy runtime is not ready")
    return value


def _bearer(request: Request) -> str | None:
    value = request.headers.get("authorization", "")
    scheme, token = get_authorization_scheme_param(value)
    return token if scheme.lower() == "bearer" and token else None


def _header_bearer(headers: dict[bytes, bytes]) -> str | None:
    value = headers.get(b"authorization", b"").decode("latin-1")
    scheme, token = get_authorization_scheme_param(value)
    return token if scheme.lower() == "bearer" and token else None


def caller(request: Request) -> Caller:
    rt = runtime(request)
    token = _bearer(request)
    if not token or token not in rt.settings.inference_api_keys:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid API key",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return Caller(token, StateStore.owner_hash(token))


def admin(request: Request) -> Runtime:
    rt = runtime(request)
    token = request.headers.get("x-admin-key") or _bearer(request)
    if not token or token != rt.settings.admin_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid admin key",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return rt


def operator_caller(request: Request) -> Caller:
    """Accept an inference key or the admin key for owned session operations."""
    rt = runtime(request)
    token = request.headers.get("x-admin-key") or _bearer(request)
    if not token or (
        token != rt.settings.admin_api_key and token not in rt.settings.inference_api_keys
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid API key",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return Caller(token, StateStore.owner_hash(token))


class AuthenticationGateMiddleware:
    """Reject protected requests before FastAPI parses JSON or multipart bodies."""

    def __init__(
        self,
        app: Callable[..., Awaitable[Any]],
        runtime_getter: Callable[[], Runtime | None] | None = None,
    ):
        self.app = app
        self.runtime_getter = runtime_getter

    @staticmethod
    async def _reject(send, message: str, code: str) -> None:
        body = json.dumps(
            openai_error(message, "authentication_error", code), ensure_ascii=False
        ).encode("utf-8")
        await send(
            {
                "type": "http.response.start",
                "status": 401,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"www-authenticate", b"Bearer"),
                    (b"content-length", str(len(body)).encode("ascii")),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})

    @staticmethod
    def _runtime(scope: dict[str, Any]) -> Runtime | None:
        target: Any = scope.get("app")
        for _ in range(8):
            state = getattr(target, "state", None)
            runtime_value = getattr(state, "runtime", None)
            if runtime_value is not None:
                return runtime_value
            target = getattr(target, "app", None)
            if target is None:
                break
        return None

    async def __call__(self, scope: dict[str, Any], receive, send):
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        path = str(scope.get("path", ""))
        if not (
            path.startswith("/v1/")
            or path.startswith("/native/")
            or path.startswith("/admin/")
        ):
            await self.app(scope, receive, send)
            return

        rt = (
            self.runtime_getter()
            if self.runtime_getter is not None
            else self._runtime(scope)
        )
        if rt is None:
            await self.app(scope, receive, send)
            return
        headers = {key.lower(): value for key, value in scope.get("headers", [])}
        bearer = _header_bearer(headers)
        admin_key = headers.get(b"x-admin-key", b"").decode("latin-1") or bearer
        inference_keys = getattr(rt.settings, "inference_api_keys", frozenset())
        admin_value = getattr(rt.settings, "admin_api_key", "")

        if path.startswith("/v1/") and (not bearer or bearer not in inference_keys):
            await self._reject(send, "invalid API key", "invalid_api_key")
            return
        if path.startswith("/admin/") and admin_key != admin_value:
            await self._reject(send, "invalid admin key", "invalid_admin_key")
            return
        if path == "/native/chat" and admin_key != admin_value:
            await self._reject(send, "invalid admin key", "invalid_admin_key")
            return
        if path.startswith("/native/") and path != "/native/chat":
            if admin_key != admin_value and (not bearer or bearer not in inference_keys):
                await self._reject(send, "invalid API key", "invalid_api_key")
                return
        await self.app(scope, receive, send)
