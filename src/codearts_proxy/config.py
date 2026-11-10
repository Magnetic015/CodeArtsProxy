from __future__ import annotations

import hashlib
import json
import math
import os
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

DEFAULT_ORIGIN = "https://devcloud.cn-north-4.huaweicloud.com"
_CFTK_COOKIE = "devclouddevuibjtcftk"


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class AccountConfig:
    name: str
    cookie_file: str | None = None
    cookie: str | None = None
    cftk: str | None = None


@dataclass(frozen=True)
class Settings:
    origin: str
    accounts_file: str
    state_db: str
    inference_api_keys: frozenset[str]
    admin_api_key: str
    checkin_mutations: bool
    checkin_confirm: bool
    timezone: str
    max_image_bytes: int
    max_file_bytes: int
    max_total_attachment_bytes: int
    upstream_timeout: float
    upstream_stream_timeout: float
    max_concurrent_per_account: int
    admin_keepalive_token: str | None = None
    upstream_proxy: str | None = None
    queue_timeout: float = 5.0
    max_waiters_per_account: int = 8


def _bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    if value.lower() not in {"1", "true", "yes", "on", "0", "false", "no", "off"}:
        raise ConfigError("invalid boolean setting")
    return value.lower() in {"1", "true", "yes", "on"}


def _number(env: dict, name: str, default: float, upper: float) -> float:
    try:
        value = float(env.get(name, default))
    except (ValueError, TypeError) as exc:
        raise ConfigError(f"invalid numeric setting: {name}") from exc
    if not math.isfinite(value) or not 0 < value <= upper:
        raise ConfigError(f"out-of-range setting: {name}")
    return value


def _secret(env: dict, name: str) -> str:
    if env.get(name) and env.get(name + "_FILE"):
        raise ConfigError(f"specify only {name} or {name}_FILE")
    if env.get(name + "_FILE"):
        return Path(env[name + "_FILE"]).read_text(encoding="utf-8").strip()
    return env.get(name, "").strip()


def load_account_configs(path: str) -> list[AccountConfig]:
    config_path = Path(path).resolve()
    data = json.loads(config_path.read_text(encoding="utf-8-sig"))
    entries = data.get("accounts") if isinstance(data, dict) else data
    if not isinstance(entries, list) or not entries:
        raise ConfigError("accounts file must contain a non-empty accounts list")
    result = []
    seen = set()
    for entry in entries:
        if not isinstance(entry, dict):
            raise ConfigError("invalid account descriptor")
        name = entry.get("name")
        if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", name):
            raise ConfigError("account names must be simple aliases")
        if name in seen:
            raise ConfigError("duplicate account alias")
        seen.add(name)
        cookie_file, cookie = entry.get("cookie_file"), entry.get("cookie")
        if bool(cookie_file) == bool(cookie):
            raise ConfigError(f"account {name} needs exactly one credential source")
        if cookie_file:
            fp = Path(cookie_file)
            cookie_file = str(fp if fp.is_absolute() else config_path.parent / fp)
        result.append(AccountConfig(name, cookie_file, cookie, entry.get("cftk")))
    return result


def load_settings(env: dict[str, str] | None = None) -> Settings:
    env = dict(os.environ) if env is None else env
    keys = frozenset(
        k.strip() for k in _secret(env, "CODEARTS_INFERENCE_API_KEYS").split(",") if k.strip()
    )
    admin = _secret(env, "CODEARTS_ADMIN_API_KEY")
    if not keys or not admin:
        raise ConfigError("inference and admin API keys are required")
    if admin in keys:
        raise ConfigError("admin key must differ from inference keys")
    if any("\n" in k or "\r" in k for k in (*keys, admin)):
        raise ConfigError("API keys must be single-line values")
    origin = env.get("CODEARTS_ORIGIN", DEFAULT_ORIGIN).rstrip("/")
    # A different origin is solely an explicit operator opt-in for a test mock.
    if origin != DEFAULT_ORIGIN:
        parsed = urlsplit(origin)
        if (
            not _bool(env.get("CODEARTS_ALLOW_TEST_ORIGIN"))
            or parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.path
            or parsed.query
            or parsed.fragment
        ):
            raise ConfigError("custom upstream origin requires explicit test-origin opt-in")
    timezone = env.get("CODEARTS_TIMEZONE", "Asia/Shanghai")
    ZoneInfo(timezone)
    concurrency = _number(env, "CODEARTS_MAX_CONCURRENT_PER_ACCOUNT", 1, 3)
    if not concurrency.is_integer():
        raise ConfigError("account concurrency must be an integer")
    return Settings(
        origin=origin,
        accounts_file=env.get("CODEARTS_ACCOUNTS_FILE", "./accounts.json"),
        state_db=env.get("CODEARTS_STATE_DB", "./state/state.sqlite3"),
        inference_api_keys=keys,
        admin_api_key=admin,
        checkin_mutations=_bool(env.get("CODEARTS_CHECKIN_MUTATIONS")),
        checkin_confirm=_bool(env.get("CODEARTS_CHECKIN_CONFIRM")),
        timezone=timezone,
        max_image_bytes=int(_number(env, "CODEARTS_MAX_IMAGE_BYTES", 10 * 1024**2, 10 * 1024**2)),
        max_file_bytes=int(_number(env, "CODEARTS_MAX_FILE_BYTES", 50 * 1024**2, 50 * 1024**2)),
        max_total_attachment_bytes=int(
            _number(env, "CODEARTS_MAX_TOTAL_ATTACHMENT_BYTES", 100 * 1024**2, 100 * 1024**2)
        ),
        upstream_timeout=_number(env, "CODEARTS_UPSTREAM_TIMEOUT", 30, 120),
        upstream_stream_timeout=_number(env, "CODEARTS_UPSTREAM_STREAM_TIMEOUT", 300, 600),
        max_concurrent_per_account=int(concurrency),
        upstream_proxy=env.get("CODEARTS_UPSTREAM_PROXY") or None,
        queue_timeout=_number(env, "CODEARTS_QUEUE_TIMEOUT", 5, 30),
    )


def extract_cftk(cookie: str) -> str:
    match = re.search(r"(?:^|;\s*)" + re.escape(_CFTK_COOKIE) + r"=([^;]+)", cookie)
    return match.group(1).strip() if match else ""


class AccountManager:
    """Credentials are reloaded from the mounted file on every operation."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self._configs = load_account_configs(settings.accounts_file)

    @property
    def aliases(self) -> tuple[str, ...]:
        return tuple(a.name for a in self._configs)

    def config(self, alias: str) -> AccountConfig:
        for account in self._configs:
            if account.name == alias:
                return account
        raise ConfigError("unknown account alias")

    def credentials(self, alias: str) -> tuple[str, str]:
        cfg = self.config(alias)
        cookie = (
            Path(cfg.cookie_file).read_text(encoding="utf-8-sig")
            if cfg.cookie_file
            else cfg.cookie or ""
        ).strip()
        if cookie.lower().startswith("cookie:"):
            cookie = cookie[7:].strip()
        if "\n" in cookie or "\r" in cookie:
            raise ConfigError(f"account {alias} has an invalid multiline cookie")
        cftk = cfg.cftk or extract_cftk(cookie)
        if not cookie or not cftk or "\n" in cftk or "\r" in cftk:
            raise ConfigError(f"account {alias} has missing/invalid cookie or cftk")
        return cookie, cftk

    def fingerprint(self, alias: str) -> str:
        cookie, cftk = self.credentials(alias)
        return hashlib.sha256((cookie + "\0" + cftk).encode()).hexdigest()
