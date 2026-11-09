from __future__ import annotations

from fastapi import APIRouter, Depends

from ..auth import admin
from ..maintenance import MaintenanceManager
from ..runtime import Runtime
from ..schemas import KeepaliveRequest

router = APIRouter(prefix="/admin", tags=["admin"])


def manager(rt: Runtime) -> MaintenanceManager:
    value = getattr(rt, "maintenance", None)
    if value is None:
        raise RuntimeError("maintenance manager is not ready")
    return value


@router.post("/keepalive", status_code=202)
async def keepalive(body: KeepaliveRequest, rt: Runtime = Depends(admin)):
    queued = await manager(rt).trigger(body.accounts, observe_only=body.force_observe)
    return {"accepted": True, "accounts": queued}


@router.get("/keepalive/status")
async def keepalive_status(rt: Runtime = Depends(admin)):
    return {"items": await manager(rt).status()}
