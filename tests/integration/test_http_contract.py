from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from codearts_proxy.auth import caller, runtime
from codearts_proxy.routes.openai import router
from codearts_proxy.service import Caller
from codearts_proxy.upstream import SSEEvent


class FakeRouter:
    aliases = ("a",)

    async def models(self, alias=None):
        return [{"model_id": "glm-5.3-flash", "enable": True}]


class FakeChat:
    async def stream(self, request, api_caller):
        yield "a", "owned-session", SSEEvent("message", {"content": "hello"})
        yield (
            "a",
            "owned-session",
            SSEEvent("done", {"tokens": {"input": 1, "output": 1, "total": 2}}),
        )
        yield "a", "owned-session", SSEEvent("message", {"content": "must-not-leak"})


class FakeFiles:
    async def upload(self, *args, **kwargs):
        return {
            "id": "file-1",
            "bytes": 1,
            "created_at": 1,
            "filename": "a.txt",
            "purpose": "user_data",
        }

    def get(self, *args, **kwargs):
        return {"file_id": "file-1", "size": 1, "created_at": 1, "filename": "a.txt"}


def make_client():
    app = FastAPI()
    app.include_router(router)
    rt = SimpleNamespace(
        router=FakeRouter(),
        accounts=SimpleNamespace(aliases=("a",)),
        settings=SimpleNamespace(max_total_attachment_bytes=100 * 1024 * 1024),
        chat=FakeChat(),
        files=FakeFiles(),
    )
    app.dependency_overrides[runtime] = lambda: rt
    app.dependency_overrides[caller] = lambda: Caller("client", "owner")
    return TestClient(app)


def test_models_and_json_completion_contract():
    with make_client() as client:
        models = client.get("/v1/models")
        assert models.status_code == 200
        assert models.json()["data"][0]["id"] == "glm-5.3-flash"

        response = client.post(
            "/v1/chat/completions",
            headers={"Authorization": "Bearer client"},
            json={"model": "glm-5.3-flash", "messages": [{"role": "user", "content": "hi"}]},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["choices"][0]["message"]["content"] == "hello"
        assert body["usage"]["total_tokens"] == 2


def test_streaming_emits_sdk_ready_sse_and_stops_after_done():
    with make_client() as client:
        response = client.post(
            "/v1/chat/completions",
            headers={"Authorization": "Bearer client"},
            json={
                "model": "glm-5.3-flash",
                "stream": True,
                "stream_options": {"include_usage": True},
                "messages": [{"role": "user", "content": "hi"}],
            },
        )
        assert response.status_code == 200
        assert response.text.endswith("data: [DONE]\n\n")
        assert '"content":"hello"' in response.text
        assert "must-not-leak" not in response.text
        assert '"choices":[]' in response.text
        assert response.text.count("\n\n") >= 4
