import json
from contextlib import aclosing

import httpx
import pytest

from codearts_proxy.config import AccountManager, Settings
from codearts_proxy.errors import UpstreamError
from codearts_proxy.upstream import CodeArtsClient, SSEParser


def setup_client(tmp_path, handler):
    account_file = tmp_path / "accounts.json"
    account_file.write_text(
        json.dumps(
            {
                "accounts": [
                    {"name": "a", "cookie": "session=a; devclouddevuibjtcftk=csrf-a"},
                    {"name": "b", "cookie": "session=b; devclouddevuibjtcftk=csrf-b"},
                ]
            }
        )
    )
    settings = Settings(
        origin="https://example.invalid",
        accounts_file=str(account_file),
        state_db=str(tmp_path / "db.sqlite"),
        inference_api_keys=frozenset({"client"}),
        admin_api_key="admin",
        checkin_mutations=False,
        checkin_confirm=False,
        timezone="Asia/Shanghai",
        max_image_bytes=1024,
        max_file_bytes=2048,
        max_total_attachment_bytes=4096,
        upstream_timeout=1,
        upstream_stream_timeout=2,
        max_concurrent_per_account=1,
    )
    upstream = CodeArtsClient(settings, AccountManager(settings))
    original = upstream.client
    upstream.client = httpx.AsyncClient(
        base_url=settings.origin, transport=httpx.MockTransport(handler), trust_env=False
    )
    return upstream, original


@pytest.mark.asyncio
async def test_account_cookie_headers_never_share_jar_identity(tmp_path):
    seen = []

    def handler(request):
        seen.append((request.headers["cookie"], request.headers["cftk"]))
        return httpx.Response(
            200,
            headers={"set-cookie": "session=wrong; Path=/"},
            json={"userId": "fake", "domainId": "test"},
        )

    upstream, original = setup_client(tmp_path, handler)
    await original.aclose()
    try:
        await upstream.identity("a")
        await upstream.identity("b")
        await upstream.identity("a")
    finally:
        await upstream.close()
    assert seen == [
        ("session=a; devclouddevuibjtcftk=csrf-a", "csrf-a"),
        ("session=b; devclouddevuibjtcftk=csrf-b", "csrf-b"),
        ("session=a; devclouddevuibjtcftk=csrf-a", "csrf-a"),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,headers,content",
    [
        (200, {"HW-AJAX-REDIRECT": "https://example.invalid/login"}, b""),
        (302, {"location": "https://other.invalid/"}, b""),
        (200, {"content-type": "text/html"}, b"login page"),
    ],
)
async def test_stream_rejects_login_redirects_and_non_sse(tmp_path, status, headers, content):
    hits = []

    def handler(request):
        hits.append(request.url)
        return httpx.Response(status, headers=headers, content=content)

    upstream, original = setup_client(tmp_path, handler)
    await original.aclose()
    try:
        with pytest.raises(UpstreamError):
            async with aclosing(upstream.message_events("a", "session-test", {})) as events:
                await anext(events)
    finally:
        await upstream.close()
    assert len(hits) == 1


def test_incomplete_done_frame_never_yields_success():
    parser = SSEParser()
    assert parser.feed(b'event: done\ndata: {"tokens": {}}') == []
    assert parser.finish() == []


def test_unbounded_line_and_malformed_json_fail_closed():
    with pytest.raises(UpstreamError):
        SSEParser(8).feed(b"data: " + b"x" * 10)
    with pytest.raises(UpstreamError):
        SSEParser().feed(b"event: message\ndata: broken\n\n")


class ClosingStream(httpx.AsyncByteStream):
    def __init__(self):
        self.closed = False
        self.late_read = False

    async def __aiter__(self):
        yield b'event: done\ndata: {"model":"example"}\n\n'
        self.late_read = True
        yield b'event: message\ndata: {"content":"must not read"}\n\n'

    async def aclose(self):
        self.closed = True


@pytest.mark.asyncio
async def test_done_stops_reading_and_closes_stream(tmp_path):
    stream = ClosingStream()
    upstream, original = setup_client(
        tmp_path,
        lambda _: httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=stream),
    )
    await original.aclose()
    try:
        events = [event async for event in upstream.message_events("a", "session-test", {})]
    finally:
        await upstream.close()
    assert [e.event for e in events] == ["done"]
    assert stream.closed and not stream.late_read
