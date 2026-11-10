from __future__ import annotations

import inspect
import json
import time
from collections.abc import AsyncIterator
from contextlib import aclosing
from typing import Any

from fastapi import APIRouter, Depends, File, Form, Header, HTTPException, Request, UploadFile
from fastapi.responses import StreamingResponse

from ..auth import caller, runtime
from ..errors import OwnershipError, UnsupportedFeature, UpstreamError
from ..limits import sanitize_filename, validate_attachment_budget
from ..openai_utils import (
    chunk,
    completion_id,
    model_object,
    openai_error,
    usage_chunk,
    usage_from_done,
    valid_finish_reason,
)
from ..runtime import Runtime
from ..schemas import ChatCompletionRequest, FileObject
from ..service import Caller
from ..upstream import SSEEvent

router = APIRouter(prefix="/v1", tags=["openai"])

_TERMINAL_UPSTREAM_ERRORS = frozenset({"stream_ended", "CODEBASE.00010628.200"})


def _sse(value: dict[str, Any]) -> str:
    return "data: " + json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n\n"


def _event_data(event: SSEEvent) -> dict[str, Any]:
    return event.data if isinstance(event.data, dict) else {"raw": event.data}


def _event_code(data: dict[str, Any]) -> str | None:
    value = data.get("error_code") or data.get("code") or data.get("error")
    if isinstance(value, dict):
        value = value.get("code") or value.get("error_code")
    return str(value) if value else None


def _event_http_error(event: SSEEvent) -> HTTPException | None:
    data = _event_data(event)
    if event.event == "error":
        code = _event_code(data)
        if code in _TERMINAL_UPSTREAM_ERRORS:
            return HTTPException(502, "upstream stream ended before completion")
        return HTTPException(502, "upstream CodeArts request failed")
    if event.event == "stopped":
        return HTTPException(502, "upstream CodeArts request was cancelled")
    if event.event == "tool_authorization":
        return HTTPException(409, "upstream tool authorization is required")
    if event.event == "question":
        return HTTPException(409, "upstream question requires operator input")
    return None


def _http_error(exc: Exception) -> HTTPException:
    if isinstance(exc, HTTPException):
        return exc
    if isinstance(exc, UnsupportedFeature):
        return HTTPException(400, str(exc))
    if isinstance(exc, OwnershipError):
        return HTTPException(403, "resource is not owned by caller")
    if isinstance(exc, UpstreamError):
        if exc.auth_expired:
            return HTTPException(
                502,
                "CodeArts authentication expired",
                headers={"X-CodeArts-Auth": "expired"},
            )
        return HTTPException(
            exc.status_code if exc.status_code and 400 <= exc.status_code < 600 else 502,
            "upstream CodeArts request failed",
        )
    if isinstance(exc, (ValueError, TypeError)):
        return HTTPException(400, "invalid request")
    return HTTPException(500, "internal proxy error")


def _apply_account_header(
    request: ChatCompletionRequest, rt: Runtime, account_header: str | None
) -> ChatCompletionRequest:
    if account_header and request.account and account_header != request.account:
        raise HTTPException(400, "request account conflicts with X-CodeArts-Account")
    account = request.account or account_header
    if account:
        aliases = tuple(getattr(rt.accounts, "aliases", ()))
        if aliases and account not in aliases:
            raise OwnershipError("unknown CodeArts account alias")
        if account != request.account:
            request = request.model_copy(update={"account": account})
    return request


async def _validate_chat_request(
    rt: Runtime,
    request: ChatCompletionRequest,
    api_caller: Caller,
    account_header: str | None,
) -> ChatCompletionRequest:
    request = _apply_account_header(request, rt, account_header)
    try:
        validate_attachment_budget(request.messages, rt.settings.max_total_attachment_bytes)
    except ValueError as exc:
        raise UnsupportedFeature(str(exc)) from exc
    validator = getattr(rt.chat, "validate_request", None)
    if validator is not None:
        result = validator(request, api_caller)
        if inspect.isawaitable(result):
            await result
    return request


async def _close_stream(stream: Any) -> None:
    close = getattr(stream, "aclose", None)
    if close is not None:
        await close()


async def _prime_stream(
    rt: Runtime, request: ChatCompletionRequest, api_caller: Caller
) -> tuple[Any, tuple[str, str, SSEEvent]]:
    stream = rt.chat.stream(request, api_caller)
    try:
        first = await anext(stream)
    except StopAsyncIteration as exc:
        await _close_stream(stream)
        raise HTTPException(502, "upstream stream ended before completion") from exc
    except Exception:
        await _close_stream(stream)
        raise
    event_error = _event_http_error(first[2])
    if event_error is not None:
        await _close_stream(stream)
        raise event_error
    return stream, first


async def _with_first(first: tuple[str, str, SSEEvent], stream: Any) -> AsyncIterator:
    yield first
    async for item in stream:
        yield item


async def _event_chunks(
    stream: Any,
    first: tuple[str, str, SSEEvent],
    request: ChatCompletionRequest,
    cid: str,
):
    include_usage = bool(request.stream_options and request.stream_options.include_usage)
    done = False
    async with aclosing(stream):
        yield _sse(chunk(cid, request.model, {"role": "assistant"}))
        try:
            async for _, _, event in _with_first(first, stream):
                data = _event_data(event)
                if event.event in {"message", "text"}:
                    content = data.get("content")
                    if isinstance(content, str) and content:
                        yield _sse(chunk(cid, request.model, {"content": content}))
                    continue
                if event.event == "done":
                    done = True
                    usage = usage_from_done(data)
                    yield _sse(
                        chunk(
                            cid,
                            request.model,
                            {},
                            valid_finish_reason(data.get("finish_reason")),
                        )
                    )
                    if include_usage and usage is not None:
                        yield _sse(usage_chunk(cid, request.model, usage))
                    yield "data: [DONE]\n\n"
                    return
                if event.event == "error":
                    if _event_code(data) in _TERMINAL_UPSTREAM_ERRORS:
                        break
                    yield _sse(openai_error("upstream CodeArts request failed", "upstream_error"))
                    return
                if event.event in {"stopped", "tool_authorization", "question"}:
                    detail = _event_http_error(event)
                    message = detail.detail if detail else "upstream CodeArts request failed"
                    code = {
                        "stopped": "cancelled",
                        "tool_authorization": "tool_authorization_required",
                        "question": "question_required",
                    }.get(event.event, "upstream_error")
                    yield _sse(openai_error(str(message), "upstream_error", code))
                    return
                # step_finish and other informational events are not final.
        except Exception:
            if not done:
                yield _sse(openai_error("upstream CodeArts request failed", "upstream_error"))
            return
        if not done:
            yield _sse(openai_error("upstream stream ended before completion", "upstream_error"))


@router.get("/models")
async def models(
    rt: Runtime = Depends(runtime),
    api_caller: Caller = Depends(caller),
    x_codearts_account: str | None = Header(default=None),
):
    aliases = [x_codearts_account] if x_codearts_account else list(rt.accounts.aliases)
    if x_codearts_account and x_codearts_account not in rt.accounts.aliases:
        raise _http_error(OwnershipError("unknown CodeArts account alias"))
    values: dict[str, dict[str, Any]] = {}
    failures: list[Exception] = []
    for alias in aliases:
        try:
            for item in await rt.router.models(alias):
                model_id = str(item.get("model_id", ""))
                if model_id and item.get("enable", True):
                    values[model_id] = model_object(model_id)
        except Exception as exc:
            failures.append(exc)
    if not values and failures:
        raise _http_error(failures[0])
    return {"object": "list", "data": list(values.values())}


@router.post("/chat/completions")
async def chat_completions(
    request: ChatCompletionRequest,
    rt: Runtime = Depends(runtime),
    api_caller: Caller = Depends(caller),
    x_codearts_account: str | None = Header(default=None),
):
    try:
        request = await _validate_chat_request(rt, request, api_caller, x_codearts_account)
        if request.stream:
            stream, first = await _prime_stream(rt, request, api_caller)
            return StreamingResponse(
                _event_chunks(stream, first, request, completion_id()),
                media_type="text/event-stream",
                headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
            )

        cid = completion_id()
        text: list[str] = []
        usage = None
        completed = False
        stream = rt.chat.stream(request, api_caller)
        async with aclosing(stream):
            async for _, _, event in stream:
                data = _event_data(event)
                if event.event in {"message", "text"}:
                    content = data.get("content")
                    if isinstance(content, str):
                        text.append(content)
                elif event.event == "done":
                    usage = usage_from_done(data)
                    completed = True
                    break
                elif event.event in {
                    "error",
                    "stopped",
                    "tool_authorization",
                    "question",
                }:
                    detail = _event_http_error(event)
                    if detail is not None:
                        raise detail
            if not completed:
                raise HTTPException(502, "upstream stream ended before completion")

        result: dict[str, Any] = {
            "id": cid,
            "object": "chat.completion",
            "created": int(time.time()),
            "model": request.model,
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": "".join(text)},
                    "finish_reason": "stop",
                }
            ],
        }
        if usage is not None:
            result["usage"] = usage
        return result
    except HTTPException:
        raise
    except Exception as exc:
        raise _http_error(exc) from exc


@router.post("/files")
async def upload_file(
    request: Request,
    file: UploadFile = File(...),
    purpose: str = Form(...),
    rt: Runtime = Depends(runtime),
    api_caller: Caller = Depends(caller),
    x_codearts_account: str | None = Header(default=None),
):
    if purpose not in {"user_data", "vision"}:
        raise HTTPException(400, "purpose must be user_data or vision")
    filename = sanitize_filename(file.filename)
    mime = file.content_type or "application/octet-stream"
    limit = rt.settings.max_image_bytes if mime.startswith("image/") else rt.settings.max_file_bytes
    content = await file.read(limit + 1)
    if len(content) > limit:
        raise HTTPException(413, "file exceeds configured size limit")
    try:
        result = await rt.files.upload(api_caller, filename, mime, content, x_codearts_account)
        output = {
            "id": result.get("id"),
            "object": "file",
            "bytes": int(result.get("bytes", len(content))),
            "created_at": int(result.get("created_at", time.time())),
            "filename": sanitize_filename(result.get("filename", filename)),
            "purpose": purpose,
            "status": result.get("status", "processed"),
        }
        file_purposes = getattr(request.app.state, "file_purposes", None)
        if file_purposes is None:
            file_purposes = {}
            request.app.state.file_purposes = file_purposes
        file_purposes[output["id"]] = purpose
        return FileObject.model_validate(output).model_dump()
    except Exception as exc:
        raise _http_error(exc) from exc


@router.get("/files/{file_id}")
async def get_file(
    file_id: str,
    request: Request,
    rt: Runtime = Depends(runtime),
    api_caller: Caller = Depends(caller),
):
    try:
        row = rt.files.get(api_caller, file_id)
        file_purposes = getattr(request.app.state, "file_purposes", {})
        output = {
            "id": row.get("file_id", row.get("id", file_id)),
            "object": "file",
            "bytes": int(row.get("size", row.get("bytes", 0))),
            "created_at": int(row.get("created_at", 0)),
            "filename": sanitize_filename(row.get("filename")),
            "purpose": row.get("purpose", file_purposes.get(file_id, "user_data")),
            "status": row.get("status", "processed"),
        }
        return FileObject.model_validate(output).model_dump()
    except OwnershipError as exc:
        raise HTTPException(404, "file not found") from exc
    except Exception as exc:
        raise _http_error(exc) from exc


@router.delete("/files/{file_id}")
async def delete_file(
    file_id: str,
    request: Request,
    rt: Runtime = Depends(runtime),
    api_caller: Caller = Depends(caller),
):
    try:
        if not rt.state.delete_file(file_id, api_caller.owner_hash):
            raise OwnershipError("file not found")
        getattr(request.app.state, "file_purposes", {}).pop(file_id, None)
        return {"id": file_id, "object": "file", "deleted": True}
    except OwnershipError as exc:
        raise HTTPException(404, "file not found") from exc
    except Exception as exc:
        raise _http_error(exc) from exc
