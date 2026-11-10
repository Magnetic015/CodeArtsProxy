import pytest

from codearts_proxy.upstream import SSEParser


def test_sse_parser_handles_fragmented_utf8_and_named_events():
    parser = SSEParser()
    payload = (
        "event: message\n"
        'data: {"content":"你好"}\n\n'
        "event: done\n"
        'data: {"tokens":{"input":1,"output":2,"total":3}}\n\n'
    ).encode()
    events = []
    for part in (payload[:7], payload[7:19], payload[19:37], payload[37:]):
        events.extend(parser.feed(part))
    events.extend(parser.finish())
    assert [e.event for e in events] == ["message", "done"]
    assert events[0].data == {"content": "你好"}
    assert events[1].data["tokens"]["total"] == 3


def test_sse_parser_rejects_oversized_event():
    parser = SSEParser(max_event_bytes=4)
    with pytest.raises(Exception):
        parser.feed(b"event: message\ndata: 12345\n\n")
