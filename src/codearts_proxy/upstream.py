from __future__ import annotations

import codecs
import json
import re
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

import httpx

from .config import AccountManager, ConfigError, Settings
from .constants import (
    ATTACHMENTS,
    BASE_HEADERS,
    IDENTITY,
    MODELS,
    SESSION_CREATE,
    SESSION_MESSAGES,
    SESSION_STOP,
)
from .errors import UpstreamError


@dataclass
class SSEEvent:
    event: str
    data: dict[str, Any] | str


class SSEParser:
    """Bounded incremental decoder. Only blank-line-terminated events are emitted."""

    def __init__(self, max_event_bytes: int = 2 * 1024 * 1024):
        self.max_event_bytes = max_event_bytes
        self._decoder = codecs.getincrementaldecoder("utf-8")("strict")
        self._buffer = ""
        self._event = "message"
        self._data: list[str] = []
        self._size = 0

    def feed(self, chunk: bytes) -> list[SSEEvent]:
        try:
            self._buffer += self._decoder.decode(chunk)
        except UnicodeDecodeError as exc:
            raise UpstreamError("invalid UTF-8 in upstream stream") from exc
        events = []
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            line = line.removesuffix("\r")
            if line.startswith(":"):
                continue
            if len(line.encode()) > self.max_event_bytes:
                raise UpstreamError("upstream SSE line is too large")
            if line.startswith("event:"):
                self._event = line[6:].strip() or "message"
            elif line.startswith("data:"):
                value = line[5:].removeprefix(" ")
                self._size += len(value.encode()) + 1
                if self._size > self.max_event_bytes:
                    raise UpstreamError("upstream SSE event is too large")
                self._data.append(value)
            elif not line:
                event = self._flush()
                if event is not None:
                    events.append(event)
        if len(self._buffer.encode()) > self.max_event_bytes:
            raise UpstreamError("upstream SSE line is too large")
        return events

    def finish(self) -> list[SSEEvent]:
        try:
            self._decoder.decode(b"", final=True)
        except UnicodeDecodeError as exc:
            raise UpstreamError("truncated UTF-8 in upstream stream") from exc
        # An unfinished event is not a terminal success, even if it says 'done'.
        self._buffer = ""
        self._data.clear()
        self._size = 0
        return []

    def _flush(self) -> SSEEvent | None:
        name = self._event
        self._event = "message"
        if not self._data:
            return None
        raw = "\n".join(self._data)
        self._data.clear()
        self._size = 0
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise UpstreamError("invalid JSON in upstream SSE event") from exc
        if not isinstance(data, dict):
            raise UpstreamError("upstream SSE payload must be an object")
        return SSEEvent(name, data)


def _identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,160}", value):
        raise UpstreamError("invalid upstream resource identifier")
    return value


class CodeArtsClient:
    def __init__(self, settings: Settings, accounts: AccountManager):
        self.settings = settings
        self.accounts = accounts
        # Every request supplies its own Cookie header. Never rely on jar state.
        self.client = httpx.AsyncClient(
            base_url=settings.origin,
            follow_redirects=False,
            trust_env=False,
            proxy=settings.upstream_proxy,
            timeout=httpx.Timeout(settings.upstream_timeout, connect=10.0),
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
        )

    async def close(self) -> None:
        await self.client.aclose()

    def _headers(
        self, alias: str, accept: str = "application/json", agent: str | None = None
    ) -> dict[str, str]:
        try:
            cookie, cftk = self.accounts.credentials(alias)
        except (OSError, ConfigError) as exc:
            raise UpstreamError(
                "account credentials unavailable", code="missing_credentials", auth_expired=True
            ) from exc
        headers = dict(BASE_HEADERS)
        headers.update(
            {
                "Referer": f"{self.settings.origin}/chat/home",
                "Cookie": cookie,
                "cftk": cftk,
                "Accept": accept,
            }
        )
        if agent:
            headers["Agent-Type"] = agent
        return headers

    @staticmethod
    def _error(response: httpx.Response, body: Any = None) -> UpstreamError:
        if (
            response.headers.get("HW-AJAX-REDIRECT")
            or response.status_code in {401, 403}
            or 300 <= response.status_code < 400
        ):
            return UpstreamError(
                "CodeArts authentication expired or denied",
                response.status_code,
                "auth_expired",
                auth_expired=True,
            )
        code = body.get("error_code") if isinstance(body, dict) else None
        safe_code = str(code) if code and re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", str(code)) else None
        return UpstreamError(
            "CodeArts upstream request failed",
            response.status_code,
            safe_code,
            retryable=response.status_code in {408, 429, 500, 502, 503, 504},
        )

    @staticmethod
    async def _read_json(response: httpx.Response) -> Any:
        chunks, size = [], 0
        async for part in response.aiter_bytes():
            size += len(part)
            if size > 2 * 1024 * 1024:
                raise UpstreamError("upstream JSON response exceeds limit")
            chunks.append(part)
        raw = b"".join(chunks)
        if response.headers.get("HW-AJAX-REDIRECT") or 300 <= response.status_code < 400:
            raise CodeArtsClient._error(response)
        if not raw:
            raise UpstreamError("empty upstream response", code="empty_response")
        try:
            payload = json.loads(raw)
        except (ValueError, UnicodeError) as exc:
            raise UpstreamError("upstream did not return JSON") from exc
        if response.status_code >= 400:
            raise CodeArtsClient._error(response, payload)
        return payload

    async def _json_request(
        self,
        alias: str,
        method: str,
        path: str,
        *,
        body: Any = None,
        params: dict[str, Any] | None = None,
        agent: str | None = None,
    ) -> Any:
        try:
            async with self.client.stream(
                method, path, headers=self._headers(alias, agent=agent), json=body, params=params
            ) as response:
                return await self._read_json(response)
        except httpx.HTTPError as exc:
            raise UpstreamError("CodeArts network error", code="network_error") from exc

    async def identity(self, alias: str) -> dict[str, Any]:
        payload = await self._json_request(alias, "GET", IDENTITY)
        if (
            not isinstance(payload, dict)
            or not isinstance(payload.get("userId"), str)
            or not payload["userId"]
            or not payload.get("domainId")
        ):
            raise UpstreamError("invalid CodeArts identity response", auth_expired=True)
        return payload

    async def models(self, alias: str) -> list[dict[str, Any]]:
        payload = await self._json_request(alias, "GET", MODELS, agent="PromptCenter")
        if not isinstance(payload, dict) or not isinstance(payload.get("builtinModels"), list):
            raise UpstreamError("invalid CodeArts model list")
        return [item for item in payload["builtinModels"] if isinstance(item, dict)]

    async def create_session(self, alias: str) -> str:
        payload = await self._json_request(alias, "POST", SESSION_CREATE, body={}, agent="CodeBase")
        result = payload.get("result") if isinstance(payload, dict) else None
        sid = result.get("session_id") if isinstance(result, dict) else None
        if not isinstance(sid, str):
            raise UpstreamError("CodeArts did not return a session id")
        return _identifier(sid)

    async def stop_session(self, alias: str, session_id: str) -> dict[str, Any]:
        payload = await self._json_request(
            alias,
            "POST",
            SESSION_STOP.format(session_id=_identifier(session_id)),
            body={},
            agent="CodeBase",
        )
        if not isinstance(payload, dict) or payload.get("status") != "ok":
            raise UpstreamError("CodeArts stop was not acknowledged")
        result = payload.get("result")
        if not isinstance(result, dict) or result.get("stopped") is not True:
            raise UpstreamError("CodeArts stop was not confirmed")
        return payload

    async def upload_attachment(
        self, alias: str, content: bytes, filename: str, mime: str
    ) -> dict[str, Any]:
        try:
            async with self.client.stream(
                "POST",
                ATTACHMENTS,
                headers=self._headers(alias, agent="CodeBase"),
                files={"file": (filename, content, mime)},
            ) as response:
                payload = await self._read_json(response)
        except httpx.HTTPError as exc:
            raise UpstreamError(
                "attachment upload outcome is unknown", code="upload_unknown"
            ) from exc
        result = payload.get("result") if isinstance(payload, dict) else None
        if not isinstance(result, dict) or not isinstance(result.get("attachment_id"), str):
            raise UpstreamError("attachment upload returned no attachment id")
        _identifier(result["attachment_id"])
        return result

    async def message_events(
        self, alias: str, session_id: str, body: dict[str, Any]
    ) -> AsyncIterator[SSEEvent]:
        path = SESSION_MESSAGES.format(session_id=_identifier(session_id))
        try:
            async with self.client.stream(
                "POST",
                path,
                headers=self._headers(alias, "text/event-stream", "CodeBase"),
                json=body,
                timeout=httpx.Timeout(self.settings.upstream_stream_timeout, connect=10),
            ) as response:
                if response.headers.get("HW-AJAX-REDIRECT") or response.status_code != 200:
                    raise self._error(response)
                if "text/event-stream" not in response.headers.get("content-type", "").lower():
                    raise UpstreamError("expected SSE from CodeArts", code="invalid_stream")
                parser = SSEParser()
                total = 0
                async for chunk in response.aiter_bytes():
                    total += len(chunk)
                    if total > 16 * 1024 * 1024:
                        raise UpstreamError("upstream turn exceeded stream limit")
                    for event in parser.feed(chunk):
                        yield event
                        if event.event == "done":
                            return
                for event in parser.finish():
                    yield event
        except httpx.TimeoutException as exc:
            raise UpstreamError("CodeArts stream timed out", code="stream_timeout") from exc
        except httpx.HTTPError as exc:
            raise UpstreamError(
                "CodeArts stream connection failed", code="stream_disconnected"
            ) from exc

    async def ops_get(self, alias: str, path: str, params: dict[str, Any] | None = None) -> Any:
        return await self._json_request(alias, "GET", path, params=params, agent="PromptCenter")

    async def ops_post(self, alias: str, path: str, body: dict[str, Any]) -> Any:
        return await self._json_request(alias, "POST", path, body=body, agent="PromptCenter")
