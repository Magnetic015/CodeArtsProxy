from __future__ import annotations

import inspect
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from .auth import AuthenticationGateMiddleware
from .config import load_settings
from .limits import BodyLimitMiddleware
from .maintenance import MaintenanceManager
from .openai_utils import openai_error
from .routes import admin, native, openai
from .runtime import build_runtime


@asynccontextmanager
async def lifespan(app: FastAPI):
    rt = None
    try:
        settings = load_settings()
        rt = build_runtime(settings)
        rt.maintenance = MaintenanceManager(rt)
        app.state.runtime = rt
        app.state.startup_error = None
    except Exception:
        app.state.runtime = None
        app.state.startup_error = "runtime unavailable"
    try:
        yield
    finally:
        if rt is not None:
            maintenance = getattr(rt, "maintenance", None)
            close_maintenance = getattr(maintenance, "close", None)
            try:
                if close_maintenance is not None:
                    result = close_maintenance()
                    if inspect.isawaitable(result):
                        await result
            finally:
                await rt.close()


app = FastAPI(title="CodeArtsProxy", version="0.1.0", lifespan=lifespan)
app.add_middleware(
    BodyLimitMiddleware,
    settings_getter=lambda: getattr(
        getattr(app.state, "runtime", None), "settings", None
    ),
)
app.add_middleware(
    AuthenticationGateMiddleware,
    runtime_getter=lambda: getattr(app.state, "runtime", None),
)
app.include_router(openai.router)
app.include_router(native.router)
app.include_router(admin.router)


def _is_api_path(path: str) -> bool:
    return path.startswith("/v1/") or path.startswith("/native/") or path.startswith("/admin/")


def _error_type(status_code: int) -> str:
    if status_code == 401:
        return "authentication_error"
    if status_code == 403:
        return "permission_error"
    if status_code == 404:
        return "not_found_error"
    if status_code == 409:
        return "conflict_error"
    if status_code >= 500:
        return "upstream_error" if status_code == 502 else "server_error"
    return "invalid_request_error"


@app.exception_handler(HTTPException)
async def api_http_exception(request: Request, exc: HTTPException):
    if not _is_api_path(request.url.path):
        return JSONResponse(
            {"detail": exc.detail}, status_code=exc.status_code, headers=exc.headers
        )
    detail = exc.detail if isinstance(exc.detail, str) else "request failed"
    return JSONResponse(
        openai_error(detail, _error_type(exc.status_code)),
        status_code=exc.status_code,
        headers=exc.headers,
    )


@app.exception_handler(RequestValidationError)
async def api_validation_exception(request: Request, exc: RequestValidationError):
    if not _is_api_path(request.url.path):
        return JSONResponse({"detail": "invalid request"}, status_code=422)
    param = None
    if exc.errors():
        location = exc.errors()[0].get("loc", ())
        if location:
            param = ".".join(str(item) for item in location if item not in {"body", "query"})[:128]
    return JSONResponse(
        openai_error("invalid request", "invalid_request_error", "invalid_request", param),
        status_code=400,
    )


@app.exception_handler(Exception)
async def api_unhandled_exception(request: Request, exc: Exception):
    if not _is_api_path(request.url.path):
        return JSONResponse({"detail": "internal server error"}, status_code=500)
    return JSONResponse(openai_error("internal proxy error", "server_error"), status_code=500)


@app.get("/healthz", tags=["health"])
async def healthz():
    return {"status": "ok"}


@app.get("/readyz", tags=["health"])
async def readyz():
    if getattr(app.state, "runtime", None) is None:
        return JSONResponse({"status": "not_ready"}, status_code=503)
    return {"status": "ready"}
