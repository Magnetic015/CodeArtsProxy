import json
from contextlib import aclosing

import pytest

from codearts_proxy.config import AccountManager, Settings
from codearts_proxy.errors import OwnershipError, UnsupportedFeature
from codearts_proxy.schemas import ChatCompletionRequest
from codearts_proxy.service import AccountRouter, Caller, ChatService, FileService
from codearts_proxy.state import StateStore
from codearts_proxy.upstream import SSEEvent


class FakeUpstream:
    def __init__(self):
        self.calls = []
        self.closed = False
        self.failure = False

    async def identity(self, alias):
        return {"userId": alias, "domainId": "synthetic", "region": "cn-north-4"}

    async def models(self, alias):
        return [{"model_id": "example", "enable": True}]

    async def create_session(self, alias):
        self.calls.append((alias, "create"))
        return "owned-test-session"

    async def upload_attachment(self, alias, content, filename, mime):
        self.calls.append((alias, "upload"))
        return {"attachment_id": "test-attachment"}

    async def message_events(self, alias, sid, body):
        try:
            self.calls.append((alias, "message"))
            yield SSEEvent("message", {"content": "hello"})
            if self.failure:
                yield SSEEvent("error", {"error": "synthetic failure"})
            else:
                yield SSEEvent("done", {"model": "example"})
        finally:
            self.closed = True

    async def stop_session(self, alias, sid):
        self.calls.append((alias, "stop"))
        return {"status": "ok", "result": {"stopped": True}}


def services(tmp_path):
    cfg = tmp_path / "accounts.json"
    cfg.write_text(
        json.dumps(
            {
                "accounts": [
                    {"name": a, "cookie": f"session={a}; devclouddevuibjtcftk=test"}
                    for a in ("a", "b")
                ]
            }
        )
    )
    settings = Settings(
        origin="https://example.invalid",
        accounts_file=str(cfg),
        state_db=str(tmp_path / "state.db"),
        inference_api_keys=frozenset({"client"}),
        admin_api_key="admin",
        checkin_mutations=False,
        checkin_confirm=False,
        timezone="Asia/Shanghai",
        max_image_bytes=1000,
        max_file_bytes=2000,
        max_total_attachment_bytes=3000,
        upstream_timeout=1,
        upstream_stream_timeout=2,
        max_concurrent_per_account=1,
    )
    state = StateStore(settings.state_db)
    upstream = FakeUpstream()
    router = AccountRouter(settings, AccountManager(settings), upstream, state)
    return (
        ChatService(settings, router, upstream, state),
        FileService(settings, router, upstream, state),
        state,
        upstream,
    )


def request(content="hello", account="a"):
    return ChatCompletionRequest(
        model="example", account=account, messages=[{"role": "user", "content": content}]
    )


@pytest.mark.asyncio
async def test_upload_record_and_implicit_account_affinity(tmp_path):
    chat, files, state, upstream = services(tmp_path)
    caller = Caller("client", "owner")
    try:
        uploaded = await files.upload(caller, "report.txt", "text/plain", b"sample", "b")
        content = [
            {"type": "text", "text": "read this"},
            {"type": "file", "file": {"file_id": uploaded["id"]}},
        ]
        events = [event async for event in chat.stream(request(content, None), caller)]
        assert events[-1][0] == "b"
        assert all(alias == "b" for alias, _ in upstream.calls)
        assert state.file(uploaded["id"], "owner")["size"] == 6
        assert upstream.closed
    finally:
        state.close()


@pytest.mark.asyncio
async def test_cross_account_file_is_rejected_before_any_chat_write(tmp_path):
    chat, files, state, upstream = services(tmp_path)
    caller = Caller("client", "owner")
    try:
        uploaded = await files.upload(caller, "report.txt", "text/plain", b"sample", "b")
        upstream.calls.clear()
        content = [{"type": "file", "file": {"file_id": uploaded["id"]}}]
        with pytest.raises(OwnershipError):
            chat.validate_request(request(content, "a"), caller)
        assert upstream.calls == []
    finally:
        state.close()


@pytest.mark.asyncio
async def test_cancelled_consumer_closes_and_stops_only_owned_turn(tmp_path):
    chat, _, state, upstream = services(tmp_path)
    try:
        async with aclosing(chat.stream(request(), Caller("client", "owner"))) as stream:
            first = await anext(stream)
            assert first[2].event == "message"
        assert upstream.closed
        assert upstream.calls.count(("a", "stop")) == 1
        assert not chat.router._semaphores["a"].locked()
    finally:
        state.close()


@pytest.mark.asyncio
async def test_error_stops_owned_turn_and_releases_capacity(tmp_path):
    chat, _, state, upstream = services(tmp_path)
    upstream.failure = True
    try:
        events = [e async for e in chat.stream(request(), Caller("client", "owner"))]
        assert events[-1][2].event == "error"
        assert upstream.calls.count(("a", "stop")) == 1
        assert not chat.router._semaphores["a"].locked()
    finally:
        state.close()


@pytest.mark.asyncio
async def test_bad_later_attachment_prevents_any_upload(tmp_path):
    chat, _, state, upstream = services(tmp_path)
    try:
        # Structurally accepted PNG prefix, then a malformed base64 image.
        content = [
            {"type": "image_url", "image_url": {"url": "data:image/png;base64,iVBORw0KGgo="}},
            {"type": "image_url", "image_url": {"url": "data:image/png;base64,AAAA"}},
        ]
        with pytest.raises(UnsupportedFeature):
            chat.validate_request(request(content), Caller("client", "owner"))
        assert upstream.calls == []
    finally:
        state.close()
