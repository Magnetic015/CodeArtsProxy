import sqlite3
from concurrent.futures import ThreadPoolExecutor

from codearts_proxy.state import StateStore


def identity(user_id="u-1", domain_id="d-1", project_id="p-1"):
    return {
        "userId": user_id,
        "domainId": domain_id,
        "projectId": project_id,
        "region": "cn-north-4",
    }


def test_file_and_session_bindings_reject_stale_principal(tmp_path):
    state = StateStore(str(tmp_path / "state.db"))
    state.remember_principal("a", identity())
    owner = StateStore.owner_hash("api-key")

    file_id = state.put_file(owner, "a", "upstream-file", "a.txt", "text/plain", 3)
    state.bind_session("session-1", owner, "a")
    assert state.file(file_id, owner)["upstream_attachment_id"] == "upstream-file"
    assert state.session("session-1", owner)["status"] == "active"

    state.remember_principal("a", identity(user_id="u-2"))
    assert state.file(file_id, owner) is None
    assert state.session("session-1", owner) is None
    assert not state.delete_file(file_id, owner)
    state.close()


def test_checkin_ledger_is_canonical_and_null_campaign_is_unique(tmp_path):
    state = StateStore(str(tmp_path / "state.db"))
    principal_hash = state.remember_principal("a", identity())
    state.record_checkin("a", principal_hash, "2026-11-10", None, "daily", "pending")
    state.record_checkin("b", principal_hash, "2026-11-10", None, "daily", "failed")
    rows = state.checkin_rows()
    assert len(rows) == 1
    assert rows[0]["campaign_id"] is None
    assert rows[0]["status"] == "failed"
    assert state.checkin_state("different-alias", principal_hash, "2026-11-10", None, "daily")[
        "status"
    ] == "failed"
    state.close()


def test_mutation_intent_returns_only_one_owner(tmp_path):
    path = tmp_path / "state.db"
    first = StateStore(str(path))
    second = StateStore(str(path))
    principal_hash = first.remember_principal("a", identity())

    assert first.mutation_intent(
        "principal:day:claim:34",
        "a",
        "claim",
        "2026-11-10",
        principal_hash=principal_hash,
    )
    assert not second.mutation_intent(
        "principal:day:claim:34",
        "b",
        "claim",
        "2026-11-10",
        principal_hash=principal_hash,
    )
    assert second.read_mutation_intent("principal:day:claim:34")["status"] == "intent"
    assert second.update_mutation_intent("principal:day:claim:34", "pending")
    assert first.read_mutation_intent("principal:day:claim:34")["status"] == "pending"
    first.close()
    second.close()


def test_lease_compare_is_atomic_across_store_instances(tmp_path):
    path = tmp_path / "state.db"
    first = StateStore(str(path))
    second = StateStore(str(path))

    def acquire(store, owner):
        return store.acquire_lease("service:day", owner, ttl=60)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(
            executor.map(
                lambda args: acquire(*args),
                ((first, "owner-a"), (second, "owner-b")),
            )
        )
    assert sorted(results) == [False, True]
    first.close()
    second.close()


def test_old_schema_migrates_fail_closed(tmp_path):
    path = tmp_path / "legacy.db"
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE principals (
          account TEXT PRIMARY KEY,
          principal_hash TEXT NOT NULL,
          domain_hash TEXT,
          project_hash TEXT,
          region TEXT,
          verified_at INTEGER NOT NULL
        );
        CREATE TABLE files (
          file_id TEXT PRIMARY KEY,
          owner_hash TEXT NOT NULL,
          account TEXT NOT NULL,
          upstream_attachment_id TEXT NOT NULL,
          filename TEXT NOT NULL,
          mime TEXT NOT NULL,
          size INTEGER NOT NULL,
          created_at INTEGER NOT NULL
        );
        CREATE TABLE sessions (
          session_id TEXT PRIMARY KEY,
          owner_hash TEXT NOT NULL,
          account TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          status TEXT NOT NULL DEFAULT 'active'
        );
        CREATE TABLE checkin_runs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          account TEXT NOT NULL,
          principal_hash TEXT NOT NULL,
          service_day TEXT NOT NULL,
          campaign_id INTEGER,
          operation TEXT NOT NULL,
          status TEXT NOT NULL,
          detail TEXT,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          UNIQUE(account, principal_hash, service_day, campaign_id, operation)
        );
        CREATE TABLE leases (
          name TEXT PRIMARY KEY,
          owner TEXT NOT NULL,
          expires_at INTEGER NOT NULL
        );
        CREATE TABLE mutation_intents (
          intent_key TEXT PRIMARY KEY,
          account TEXT NOT NULL,
          operation TEXT NOT NULL,
          service_day TEXT NOT NULL,
          status TEXT NOT NULL,
          detail TEXT,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        );
        """
    )
    conn.execute(
        "INSERT INTO principals VALUES(?,?,?,?,?,?)",
        ("a", "old-principal", "d", "p", "cn", 1),
    )
    conn.execute(
        "INSERT INTO files VALUES(?,?,?,?,?,?,?,?)",
        ("file-old", "owner", "a", "upstream", "old.txt", "text/plain", 1, 1),
    )
    conn.execute(
        "INSERT INTO checkin_runs VALUES(?,?,?,?,?,?,?,?,?,?)",
        (1, "a", "old-principal", "2026-11-10", None, "daily", "pending", None, 1, 1),
    )
    conn.execute(
        "INSERT INTO checkin_runs VALUES(?,?,?,?,?,?,?,?,?,?)",
        (2, "a", "old-principal", "2026-11-10", None, "daily", "failed", None, 2, 2),
    )
    conn.commit()
    conn.close()

    state = StateStore(str(path))
    assert len(state.checkin_rows()) == 1
    assert state.checkin_rows()[0]["status"] == "failed"
    # Legacy resources have no trusted principal binding and are rejected.
    assert state.file("file-old", "owner") is None
    state.close()
