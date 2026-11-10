from __future__ import annotations

import time
import uuid
from typing import Any

FINISH_REASONS = frozenset({"stop", "length", "tool_calls", "content_filter"})


def completion_id() -> str:
    return "chatcmpl-" + uuid.uuid4().hex


def model_object(model_id: str) -> dict[str, Any]:
    return {"id": model_id, "object": "model", "created": int(time.time()), "owned_by": "codearts"}


def openai_error(
    message: str,
    error_type: str = "invalid_request_error",
    code: str | None = None,
    param: str | None = None,
) -> dict[str, Any]:
    return {
        "error": {
            "message": message,
            "type": error_type,
            "param": param,
            "code": code,
        }
    }


def valid_finish_reason(value: Any, default: str = "stop") -> str:
    return value if isinstance(value, str) and value in FINISH_REASONS else default


def chunk(
    cid: str,
    model: str,
    delta: dict[str, Any],
    finish_reason: str | None = None,
    usage: dict[str, Any] | None = None,
) -> dict[str, Any]:
    value: dict[str, Any] = {
        "id": cid,
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": model,
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}],
    }
    if usage is not None:
        value["usage"] = usage
    return value


def usage_chunk(cid: str, model: str, usage: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": cid,
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": model,
        "choices": [],
        "usage": usage,
    }


def _int_value(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


def _first_int(values: dict[str, Any], *keys: str) -> int | None:
    for key in keys:
        value = _int_value(values.get(key))
        if value is not None:
            return value
    return None


def usage_from_done(data: dict[str, Any]) -> dict[str, Any] | None:
    tokens = data.get("tokens") if isinstance(data, dict) else None
    if not isinstance(tokens, dict):
        tokens = data.get("usage") if isinstance(data, dict) else None
    if not isinstance(tokens, dict):
        return None

    explicit_input = _first_int(tokens, "input", "input_tokens")
    input_uncached = _first_int(
        tokens, "input_uncached", "input_uncached_tokens", "inputUncached"
    )
    cache_read = _first_int(
        tokens,
        "cache_read",
        "cached_input",
        "cache_read_tokens",
        "cache_read_input_tokens",
        "cacheRead",
    )
    output = _first_int(tokens, "output", "output_tokens")
    reasoning = _first_int(tokens, "reasoning", "reasoning_tokens")
    explicit_total = _first_int(tokens, "total", "total_tokens")

    if explicit_input is not None:
        prompt_tokens = explicit_input
    elif input_uncached is not None:
        prompt_tokens = input_uncached + (cache_read or 0)
    else:
        prompt_tokens = None

    completion_tokens = None
    if output is not None:
        completion_tokens = output + (reasoning or 0)

    total_tokens = explicit_total
    if prompt_tokens is not None and completion_tokens is not None:
        computed_total = prompt_tokens + completion_tokens
        if total_tokens is not None and total_tokens != computed_total:
            return None
        total_tokens = computed_total

    if prompt_tokens is None or completion_tokens is None or total_tokens is None:
        return None

    usage: dict[str, Any] = {
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": total_tokens,
    }
    if cache_read is not None:
        usage["prompt_tokens_details"] = {"cached_tokens": cache_read}
    if reasoning is not None:
        usage["completion_tokens_details"] = {"reasoning_tokens": reasoning}
    return usage
