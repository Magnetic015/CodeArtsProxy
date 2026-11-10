from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any

_CAMPAIGN_NONE = -1
_SCHEMA_VERSION = 3


class StateStore:
    """Small SQLite state store; no raw upstream credentials are stored."""

    def __init__(self, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._closed = False
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock, self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS principals (
                  account TEXT PRIMARY KEY,
                  principal_hash TEXT NOT NULL,
                  domain_hash TEXT,
                  project_hash TEXT,
                  region TEXT,
                  verified_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS files (
                  file_id TEXT PRIMARY KEY,
                  owner_hash TEXT NOT NULL,
                  account TEXT NOT NULL,
                  principal_hash TEXT NOT NULL,
                  upstream_attachment_id TEXT NOT NULL,
                  filename TEXT NOT NULL,
                  mime TEXT NOT NULL,
                  size INTEGER NOT NULL,
                  created_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS sessions (
                  session_id TEXT PRIMARY KEY,
                  owner_hash TEXT NOT NULL,
                  account TEXT NOT NULL,
                  principal_hash TEXT NOT NULL,
                  created_at INTEGER NOT NULL,
                  status TEXT NOT NULL DEFAULT 'active'
                );
                CREATE TABLE IF NOT EXISTS checkin_runs (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  account TEXT NOT NULL,
                  principal_hash TEXT NOT NULL,
                  service_day TEXT NOT NULL,
                  campaign_id INTEGER NOT NULL DEFAULT -1,
                  operation TEXT NOT NULL,
                  status TEXT NOT NULL,
                  detail TEXT,
                  created_at INTEGER NOT NULL,
                  updated_at INTEGER NOT NULL,
                  UNIQUE(principal_hash, service_day, campaign_id, operation)
                );
                CREATE TABLE IF NOT EXISTS leases (
                  name TEXT PRIMARY KEY,
                  owner TEXT NOT NULL,
                  expires_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS mutation_intents (
                  intent_key TEXT PRIMARY KEY,
                  account TEXT NOT NULL,
                  principal_hash TEXT,
                  operation TEXT NOT NULL,
                  service_day TEXT NOT NULL,
                  status TEXT NOT NULL,
                  detail TEXT,
                  created_at INTEGER NOT NULL,
                  updated_at INTEGER NOT NULL
                );
                """
            )
            self._ensure_column("files", "principal_hash", "TEXT")
            self._ensure_column("sessions", "principal_hash", "TEXT")
            self._ensure_column("mutation_intents", "principal_hash", "TEXT")
            self._migrate_checkin_runs()
            self._conn.execute(f"PRAGMA user_version={_SCHEMA_VERSION}")

    def _ensure_column(self, table: str, column: str, definition: str) -> None:
        columns = {row["name"] for row in self._conn.execute(f'PRAGMA table_info("{table}")')}
        if column not in columns:
            self._conn.execute(f'ALTER TABLE "{table}" ADD COLUMN "{column}" {definition}')

    def _migrate_checkin_runs(self) -> None:
        columns = {
            row["name"]: row
            for row in self._conn.execute('PRAGMA table_info("checkin_runs")')
        }
        if not columns:
            return
        required = {"account", "principal_hash", "service_day", "campaign_id", "operation"}
        needs_rebuild = not required.issubset(columns)
        if not needs_rebuild:
            needs_rebuild = int(columns["campaign_id"]["notnull"] or 0) != 1
        if not needs_rebuild:
            expected = ["principal_hash", "service_day", "campaign_id", "operation"]
            has_expected_unique = False
            for index in self._conn.execute('PRAGMA index_list("checkin_runs")'):
                if int(index["unique"] or 0) != 1:
                    continue
                name = str(index["name"]).replace('"', '""')
                index_columns = [
                    row["name"]
                    for row in self._conn.execute(f'PRAGMA index_info("{name}")')
                ]
                if index_columns == expected:
                    has_expected_unique = True
                    break
            needs_rebuild = not has_expected_unique
        if not needs_rebuild:
            return

        rows = self._conn.execute("SELECT * FROM checkin_runs").fetchall()
        self._conn.execute(
            """
            CREATE TABLE checkin_runs_new (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              account TEXT NOT NULL,
              principal_hash TEXT NOT NULL,
              service_day TEXT NOT NULL,
              campaign_id INTEGER NOT NULL DEFAULT -1,
              operation TEXT NOT NULL,
              status TEXT NOT NULL,
              detail TEXT,
              created_at INTEGER NOT NULL,
              updated_at INTEGER NOT NULL,
              UNIQUE(principal_hash, service_day, campaign_id, operation)
            )
            """
        )
        ordered = sorted(
            (dict(row) for row in rows),
            key=lambda row: (int(row.get("updated_at") or 0), int(row.get("id") or 0)),
            reverse=True,
        )
        seen: set[tuple[str, str, int, str]] = set()
        now = int(time.time())
        for row in ordered:
            principal_hash = str(row.get("principal_hash") or "")
            service_day = str(row.get("service_day") or "")
            campaign_id = self._campaign_db_value(row.get("campaign_id"))
            operation = str(row.get("operation") or "daily")
            key = (principal_hash, service_day, campaign_id, operation)
            if key in seen:
                continue
            seen.add(key)
            created_at = int(row.get("created_at") or now)
            updated_at = int(row.get("updated_at") or created_at)
            detail = self._encode_detail(self._decode_detail(row.get("detail")))
            self._conn.execute(
                """
                INSERT INTO checkin_runs_new(
                  account,principal_hash,service_day,campaign_id,operation,status,detail,
                  created_at,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?)
                """,
                (
                    str(row.get("account") or ""),
                    principal_hash,
                    service_day,
                    campaign_id,
                    operation,
                    str(row.get("status") or "failed"),
                    detail,
                    created_at,
                    updated_at,
                ),
            )
        self._conn.execute("DROP TABLE checkin_runs")
        self._conn.execute("ALTER TABLE checkin_runs_new RENAME TO checkin_runs")

    @staticmethod
    def owner_hash(api_key: str) -> str:
        return hashlib.sha256(api_key.encode("utf-8")).hexdigest()

    @staticmethod
    def value_hash(value: Any) -> str:
        return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()

    @staticmethod
    def _campaign_db_value(campaign_id: Any) -> int:
        if campaign_id is None or isinstance(campaign_id, bool):
            return _CAMPAIGN_NONE
        try:
            return int(campaign_id)
        except (TypeError, ValueError):
            return _CAMPAIGN_NONE

    @staticmethod
    def _campaign_public_value(campaign_id: Any) -> int | None:
        value = StateStore._campaign_db_value(campaign_id)
        return None if value == _CAMPAIGN_NONE else value

    @staticmethod
    def _blocked_detail_key(key: str) -> bool:
        lowered = key.lower().replace("-", "_")
        blocked = (
            "token",
            "cookie",
            "credential",
            "authorization",
            "password",
            "secret",
            "identity",
            "userid",
            "domainid",
            "projectid",
            "header",
            "payload",
            "response",
        )
        return any(part in lowered for part in blocked)

    @classmethod
    def _sanitize_detail_value(cls, value: Any, depth: int = 0) -> Any:
        if depth > 3:
            return None
        if value is None or isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value
        if isinstance(value, str):
            return value[:256]
        if isinstance(value, list):
            return [cls._sanitize_detail_value(item, depth + 1) for item in value[:16]]
        if isinstance(value, dict):
            result: dict[str, Any] = {}
            for key, item in list(value.items())[:32]:
                key_text = str(key)
                if cls._blocked_detail_key(key_text):
                    continue
                clean = cls._sanitize_detail_value(item, depth + 1)
                if clean is not None:
                    result[key_text[:64]] = clean
            return result
        return str(value)[:256]

    @classmethod
    def _sanitize_detail(cls, detail: Any) -> Any:
        if detail is None:
            return None
        clean = cls._sanitize_detail_value(detail)
        return clean if clean not in ({}, []) else None

    @classmethod
    def _decode_detail(cls, detail: Any) -> Any:
        if detail is None or isinstance(detail, (dict, list)):
            return cls._sanitize_detail(detail)
        try:
            return cls._sanitize_detail(json.loads(str(detail)))
        except (TypeError, ValueError, json.JSONDecodeError):
            return cls._sanitize_detail(str(detail))

    @classmethod
    def _encode_detail(cls, detail: Any) -> str | None:
        clean = cls._sanitize_detail(detail)
        return (
            json.dumps(clean, ensure_ascii=False, separators=(",", ":"))
            if clean is not None
            else None
        )

    def remember_principal(self, account: str, identity: dict[str, Any]) -> str:
        principal = str(identity.get("userId") or identity.get("id") or "")
        domain = str(identity.get("domainId") or "")
        project = str(identity.get("projectId") or "")
        principal_hash = self.value_hash(f"{domain}:{principal}")
        with self._lock, self._conn:
            self._conn.execute(
                """INSERT INTO principals(
                       account,principal_hash,domain_hash,project_hash,region,verified_at)
                   VALUES(?,?,?,?,?,?)
                   ON CONFLICT(account) DO UPDATE SET principal_hash=excluded.principal_hash,
                   domain_hash=excluded.domain_hash,project_hash=excluded.project_hash,
                   region=excluded.region,verified_at=excluded.verified_at""",
                (
                    account,
                    principal_hash,
                    self.value_hash(domain),
                    self.value_hash(project),
                    str(identity.get("region") or "")[:128],
                    int(time.time()),
                ),
            )
        return principal_hash

    def principal(self, account: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM principals WHERE account=?", (account,)
            ).fetchone()
        return dict(row) if row else None

    def _current_principal_hash(self, account: str) -> str | None:
        row = self._conn.execute(
            "SELECT principal_hash FROM principals WHERE account=?", (account,)
        ).fetchone()
        if not row or not row["principal_hash"]:
            return None
        return str(row["principal_hash"])

    def _require_principal(self, account: str, expected: str | None = None) -> str:
        current = self._current_principal_hash(account)
        if not current:
            raise ValueError("principal is not established")
        if expected is not None and expected != current:
            raise ValueError("principal mapping is stale")
        return current

    def put_file(
        self,
        owner_hash: str,
        account: str,
        upstream_id: str,
        filename: str,
        mime: str,
        size: int,
        principal_hash: str | None = None,
    ) -> str:
        file_id = "file-" + uuid.uuid4().hex
        with self._lock, self._conn:
            current = self._require_principal(account, principal_hash)
            self._conn.execute(
                """INSERT INTO files(
                       file_id,owner_hash,account,principal_hash,upstream_attachment_id,
                       filename,mime,size,created_at)
                   VALUES(?,?,?,?,?,?,?,?,?)""",
                (
                    file_id,
                    owner_hash,
                    account,
                    current,
                    upstream_id,
                    filename,
                    mime,
                    int(size),
                    int(time.time()),
                ),
            )
        return file_id

    def file(self, file_id: str, owner_hash: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                """SELECT f.* FROM files f
                   JOIN principals p ON p.account=f.account AND p.principal_hash=f.principal_hash
                   WHERE f.file_id=? AND f.owner_hash=?""",
                (file_id, owner_hash),
            ).fetchone()
        return dict(row) if row else None

    def delete_file(self, file_id: str, owner_hash: str) -> bool:
        with self._lock, self._conn:
            cur = self._conn.execute(
                """DELETE FROM files WHERE file_id=? AND owner_hash=?
                   AND principal_hash=(
                       SELECT principal_hash FROM principals WHERE account=files.account
                   )""",
                (file_id, owner_hash),
            )
        return cur.rowcount > 0

    def bind_session(
        self,
        session_id: str,
        owner_hash: str,
        account: str,
        principal_hash: str | None = None,
    ) -> None:
        with self._lock, self._conn:
            current = self._require_principal(account, principal_hash)
            self._conn.execute(
                """INSERT OR REPLACE INTO sessions(
                       session_id,owner_hash,account,principal_hash,created_at,status)
                   VALUES(?,?,?,?,?,?)""",
                (session_id, owner_hash, account, current, int(time.time()), "active"),
            )

    def session(self, session_id: str, owner_hash: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                """SELECT s.* FROM sessions s
                   JOIN principals p ON p.account=s.account AND p.principal_hash=s.principal_hash
                   WHERE s.session_id=? AND s.owner_hash=?""",
                (session_id, owner_hash),
            ).fetchone()
        return dict(row) if row else None

    def close_session(self, session_id: str, owner_hash: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """UPDATE sessions SET status='closed' WHERE session_id=? AND owner_hash=?
                   AND principal_hash=(
                       SELECT principal_hash FROM principals WHERE account=sessions.account
                   )""",
                (session_id, owner_hash),
            )

    def acquire_lease(self, name: str, owner: str, ttl: int = 120) -> bool:
        now = int(time.time())
        expires_at = now + max(1, int(ttl))
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT owner,expires_at FROM leases WHERE name=?", (name,)
                ).fetchone()
                if row and int(row["expires_at"]) > now and row["owner"] != owner:
                    self._conn.rollback()
                    return False
                if row:
                    self._conn.execute(
                        """UPDATE leases SET owner=?,expires_at=? WHERE name=?
                           AND (expires_at<=? OR owner=?)""",
                        (owner, expires_at, name, now, owner),
                    )
                else:
                    self._conn.execute(
                        "INSERT INTO leases(name,owner,expires_at) VALUES(?,?,?)",
                        (name, owner, expires_at),
                    )
                self._conn.commit()
                return True
            except Exception:
                self._conn.rollback()
                raise

    def release_lease(self, name: str, owner: str) -> None:
        with self._lock, self._conn:
            self._conn.execute("DELETE FROM leases WHERE name=? AND owner=?", (name, owner))

    def mutation_intent(
        self,
        key: str,
        account: str,
        operation: str,
        service_day: str,
        status: str = "intent",
        detail: Any = None,
        principal_hash: str | None = None,
    ) -> bool:
        """Reserve an intent once; return True only to the caller that owns it."""
        encoded = self._encode_detail(detail)
        now = int(time.time())
        with self._lock, self._conn:
            principal = principal_hash or self._current_principal_hash(account) or ""
            cur = self._conn.execute(
                """INSERT OR IGNORE INTO mutation_intents(
                       intent_key,account,principal_hash,operation,service_day,status,detail,
                       created_at,updated_at)
                   VALUES(?,?,?,?,?,?,?,?,?)""",
                (key, account, principal, operation, service_day, status, encoded, now, now),
            )
        return cur.rowcount > 0

    def reserve_mutation_intent(
        self,
        key: str,
        account: str,
        operation: str,
        service_day: str,
        principal_hash: str | None = None,
        detail: Any = None,
    ) -> bool:
        return self.mutation_intent(
            key,
            account,
            operation,
            service_day,
            status="reserved",
            detail=detail,
            principal_hash=principal_hash,
        )

    def read_mutation_intent(self, key: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM mutation_intents WHERE intent_key=?", (key,)
            ).fetchone()
        return dict(row) if row else None

    get_mutation_intent = read_mutation_intent

    def update_mutation_intent(self, key: str, status: str, detail: Any = None) -> bool:
        encoded = self._encode_detail(detail)
        with self._lock, self._conn:
            cur = self._conn.execute(
                """UPDATE mutation_intents SET status=?,detail=?,updated_at=?
                   WHERE intent_key=?""",
                (status, encoded, int(time.time()), key),
            )
        return cur.rowcount > 0

    def mutation_intent_state(self, key: str) -> dict[str, Any] | None:
        return self.read_mutation_intent(key)

    def checkin_state(
        self,
        account: str,
        principal_hash: str,
        service_day: str,
        campaign_id: int | None,
        operation: str,
    ) -> dict[str, Any] | None:
        campaign = self._campaign_db_value(campaign_id)
        with self._lock:
            row = self._conn.execute(
                """SELECT * FROM checkin_runs
                   WHERE principal_hash=? AND service_day=? AND campaign_id=? AND operation=?""",
                (principal_hash, service_day, campaign, operation),
            ).fetchone()
        if not row:
            return None
        result = dict(row)
        result["campaign_id"] = self._campaign_public_value(result.get("campaign_id"))
        return result

    def record_checkin(
        self,
        account: str,
        principal_hash: str,
        service_day: str,
        campaign_id: int | None,
        operation: str,
        status: str,
        detail: Any = None,
    ) -> None:
        encoded = self._encode_detail(detail)
        now = int(time.time())
        campaign = self._campaign_db_value(campaign_id)
        with self._lock, self._conn:
            self._conn.execute(
                """INSERT INTO checkin_runs(
                       account,principal_hash,service_day,campaign_id,operation,status,detail,
                       created_at,updated_at)
                   VALUES(?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(principal_hash,service_day,campaign_id,operation)
                   DO UPDATE SET account=excluded.account,status=excluded.status,
                       detail=excluded.detail,updated_at=excluded.updated_at""",
                (
                    account,
                    principal_hash,
                    service_day,
                    campaign,
                    operation,
                    status,
                    encoded,
                    now,
                    now,
                ),
            )

    def checkin_rows(self, account: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        limit = max(0, int(limit))
        with self._lock:
            if account:
                rows = self._conn.execute(
                    "SELECT * FROM checkin_runs WHERE account=? ORDER BY updated_at DESC LIMIT ?",
                    (account, limit),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM checkin_runs ORDER BY updated_at DESC LIMIT ?", (limit,)
                ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["campaign_id"] = self._campaign_public_value(item.get("campaign_id"))
            result.append(item)
        return result

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._conn.close()
