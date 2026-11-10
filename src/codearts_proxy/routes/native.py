from __future__ import annotations

import json
from contextlib import aclosing
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from fastapi.responses import StreamingResponse

from ..auth import admin, operator_caller, runtime
from ..errors import OwnershipError, UpstreamError
from ..runtime import Runtime
from ..schemas import ChatCompletionRequest
from ..service import Caller
from ..state import StateStore
from ..upstream import SSEEvent
from .openai import _validate_chat_request

router = APIRouter(prefix="/native", tags=["native"])


def _raw_sse(event: SSEEvent) -> str:
    name = event.event.replace("\r", "").replace("\n", "_") or "message"
    data: Any = event.data
    return f"event: {name}\ndata: {json.dumps(data, ensure_ascii=False, separators=(',', ':'))}\n\n"


@router.get("/accounts")
async def account_status(
    rt: Runtime = Depends(runtime),
    api_caller: Caller = Depends(operator_caller),
    x_codearts_account: str | None = Header(default=None),
):
    aliases = [x_codearts_account] if x_codearts_account else list(rt.accounts.aliases)
    if x_codearts_account and x_codearts_account not in rt.accounts.aliases:
        raise HTTPException(404, "unknown CodeArts account alias")
    items = []
    for alias in aliases:
        try:
            identity = await rt.router.refresh_identity(alias)
            stored = rt.state.principal(alias) or {}
            items.append(
                {
                    "account": alias,
                    "available": True,
                    "region": identity.get("region"),
                    "principal_hash": stored.get("principal_hash"),
                }
            )
        except Exception:
            items.append({"account": alias, "available": False})
    return {"accounts": items}


@router.post("/chat")
async def native_chat(
    body: ChatCompletionRequest,
    request: Request,
    rt: Runtime = Depends(admin),
    x_codearts_account: str | None = Header(default=None),
):
    token = request.headers.get("x-admin-key")
    if not token:
        authorization = request.headers.get("authorization", "")
        scheme, value = authorization.split(" ", 1) if " " in authorization else ("", "")
        if scheme.lower() == "bearer":
            token = value
    if not token:
        raise HTTPException(401, "invalid admin key")
    api_caller = Caller(token, StateStore.owner_hash(token))
    body = await _validate_chat_request(rt, body, api_caller, x_codearts_account)
    stream = rt.chat.stream(body, api_caller)
    try:
        first = await anext(stream)
    except StopAsyncIteration as exc:
        await stream.aclose()
        raise HTTPException(502, "upstream stream ended before completion") from exc
    except Exception:
        await stream.aclose()
        raise
    async def events():
        async with aclosing(stream):
            yield _raw_sse(first[2])
            try:
                async for _, _, event in stream:
                    yield _raw_sse(event)
                    if event.event in {
                        "done",
                        "error",
                        "stopped",
                        "tool_authorization",
                        "question",
                    }:
                        return
            except Exception:
                yield _raw_sse(SSEEvent("proxy_error", {"error": "upstream request failed"}))

    return StreamingResponse(
        events(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post("/sessions/{session_id}/stop")
async def stop_session(
    session_id: str,
    rt: Runtime = Depends(runtime),
    api_caller: Caller = Depends(operator_caller),
):
    try:
        await rt.chat.stop_owned(api_caller, session_id)
        return {"session_id": session_id, "stopped": True}
    except OwnershipError as exc:
        raise HTTPException(404, "session not found") from exc
    except UpstreamError as exc:
        raise HTTPException(502, "upstream CodeArts request failed") from exc
