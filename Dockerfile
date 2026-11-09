FROM python:3.12.10-slim-bookworm AS build
COPY --from=ghcr.io/astral-sh/uv:0.12.5 /uv /usr/local/bin/uv
WORKDIR /app
ENV UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN uv sync --locked --no-dev --no-editable

FROM python:3.12.10-slim-bookworm
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PATH="/app/.venv/bin:$PATH"
WORKDIR /app
RUN groupadd --system --gid 10001 app \
    && useradd --system --uid 10001 --gid app app \
    && mkdir -p /var/lib/codearts-proxy \
    && chown app:app /var/lib/codearts-proxy
COPY --from=build /app/.venv /app/.venv
USER 10001:10001
EXPOSE 8080
HEALTHCHECK --interval=5m --timeout=15s --start-period=20s \
    CMD ["python", "-m", "codearts_proxy.healthcheck"]
ENTRYPOINT ["python", "-m", "uvicorn", "codearts_proxy.app:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1", "--no-access-log"]
