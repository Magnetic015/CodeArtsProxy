from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class UpstreamError(Exception):
    message: str
    status_code: int | None = None
    code: str | None = None
    auth_expired: bool = False
    retryable: bool = False
    payload: Any = None

    def __str__(self) -> str:
        return self.message


class UnsupportedFeature(ValueError):
    pass


class OwnershipError(ValueError):
    pass


class CheckinPending(RuntimeError):
    pass
