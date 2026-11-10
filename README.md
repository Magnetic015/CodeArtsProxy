# CodeArtsProxy

Docker-ready gateway for account-authorized CodeArts model sessions, file attachments, and daily account maintenance.

## Scope

The service exposes a deliberately narrow OpenAI-compatible surface:

- `GET /v1/models`
- `POST /v1/chat/completions` with JSON or true named-event-to-SSE translation
- `POST /v1/files`, `GET/DELETE /v1/files/{id}`
- `GET /healthz`, `GET /readyz`
- authenticated native account/session and maintenance routes

CodeArts runs an agent runtime on the upstream side. Its server-side tool traces are not converted into client-executable OpenAI `tool_calls`; client `tools` and `tool_choice` are rejected until a faithful mapping exists. Remote image URLs are rejected; use a data image or `/v1/files`.

## Configuration

Copy the example files outside the repository, mount them read-only, and set separate inference/admin keys. Never commit cookies, CSRF tokens, state databases, logs, or captured responses.

```bash
uv sync --group dev
uv run pytest -q
uv run uvicorn codearts_proxy.app:app --reload --port 8080
```

The Docker Compose example uses a persistent state volume and a management-only keepalive endpoint. `GET /healthz` has no upstream side effects. Set `CODEARTS_CHECKIN_MUTATIONS=true` only after reviewing the activity semantics for the configured tenant; set `CODEARTS_CHECKIN_CONFIRM=true` only when the tenant confirms the upstream confirm transition.

## Docker

```bash
docker compose up --build
```

The example binds to loopback. Put TLS/authenticated ingress in front of it before exposing it beyond the host.
