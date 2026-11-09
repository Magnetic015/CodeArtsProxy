from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from codearts_proxy.limits import BodyLimitMiddleware
from codearts_proxy.openai_utils import usage_from_done
from codearts_proxy.schemas import ChatCompletionRequest


def valid_request(**updates):
    value = {"model": "m", "messages": [{"role": "user", "content": "hello"}]}
    value.update(updates)
    return value


def test_chat_schema_rejects_unknown_and_silent_parameters():
    with pytest.raises(ValidationError):
        ChatCompletionRequest.model_validate(valid_request(unknown_parameter=True))
    with pytest.raises(ValidationError):
        ChatCompletionRequest.model_validate(valid_request(temperature=0.2))
    with pytest.raises(ValidationError):
        ChatCompletionRequest.model_validate(valid_request(tools=[]))
    request = ChatCompletionRequest.model_validate(
        valid_request(stream=True, stream_options={"include_usage": True}, n=1)
    )
    assert request.stream_options is not None
    assert request.stream_options.include_usage is True


def test_schema_rejects_remote_urls_and_unsupported_parts():
    with pytest.raises(ValidationError):
        ChatCompletionRequest.model_validate(
            valid_request(
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {"type": "image_url", "image_url": {"url": "https://example.test/a.png"}}
                        ],
                    }
                ]
            )
        )
    with pytest.raises(ValidationError):
        ChatCompletionRequest.model_validate(
            valid_request(
                messages=[
                    {"role": "user", "content": [{"type": "audio", "audio": {}}]}
                ]
            )
        )


def test_usage_requires_proven_totals_and_maps_cached_reasoning_tokens():
    usage = usage_from_done(
        {
            "tokens": {
                "input_uncached": 4,
                "cache_read": 2,
                "output": 3,
                "reasoning": 1,
                "total": 10,
            }
        }
    )
    assert usage == {
        "prompt_tokens": 6,
        "completion_tokens": 4,
        "total_tokens": 10,
        "prompt_tokens_details": {"cached_tokens": 2},
        "completion_tokens_details": {"reasoning_tokens": 1},
    }
    assert usage_from_done({"tokens": {"input": 1, "output": 2, "total": 99}}) is None
    assert usage_from_done({"tokens": {"input": 1}}) is None


@pytest.mark.asyncio
async def test_body_limit_enforces_without_content_length_only():
    sent = []
    app = SimpleNamespace(
        state=SimpleNamespace(
            runtime=SimpleNamespace(
                settings=SimpleNamespace(max_file_bytes=1, max_total_attachment_bytes=1)
            )
        )
    )

    async def inner(scope, receive, send):
        await receive()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    middleware = BodyLimitMiddleware(inner)
    scope = {
        "type": "http",
        "path": "/v1/files",
        "app": app,
        "headers": [(b"content-length", b"300000")],
    }

    async def receive():
        return {"type": "http.request", "body": b"x", "more_body": False}

    async def send(message):
        sent.append(message)

    await middleware(scope, receive, send)
    assert sent[0]["status"] == 413
    assert b'"error"' in sent[1]["body"]
