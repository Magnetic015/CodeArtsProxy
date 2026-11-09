from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo


class MaintenanceManager:
    """Bounded, deduplicated maintenance queue for daily check-in attempts."""

    def __init__(self, runtime: Any):
        self.runtime = runtime
        settings = runtime.settings
        aliases = list(getattr(runtime.accounts, "aliases", ()))
        self._cooldown = max(
            0.0,
            float(getattr(settings, "maintenance_cooldown_seconds", 300.0)),
        )
        self._max_pending = max(
            1,
            int(getattr(settings, "maintenance_queue_size", max(1, len(aliases)))),
        )
        self._tasks: dict[str, asyncio.Task[Any]] = {}
        self._task_modes: dict[str, bool] = {}
        self._results: dict[str, dict[str, Any]] = {}
        self._last_completed: dict[str, tuple[float, bool]] = {}
        self._lock = asyncio.Lock()
        self._closed = False

    @staticmethod
    def _safe_result(alias: str, result: Any) -> dict[str, Any]:
        if not isinstance(result, dict):
            return {"account": alias, "outcome": "failed"}
        allowed = {
            "account",
            "status",
            "claimable",
            "campaign_id",
            "outcome",
            "step",
            "reason",
            "title",
            "claim_verified",
        }
        clean = {key: value for key, value in result.items() if key in allowed}
        clean["account"] = alias
        return clean

    def _collect_done(self, alias: str, task: asyncio.Task[Any]) -> None:
        if not task.done() or alias in self._results:
            return
        observe_only = self._task_modes.get(alias, False)
        try:
            result = task.result()
            status = "completed"
            safe_result = self._safe_result(alias, result)
        except asyncio.CancelledError:
            status = "cancelled"
            safe_result = {"account": alias, "outcome": "cancelled"}
        except Exception:
            status = "failed"
            safe_result = {"account": alias, "outcome": "failed"}
        self._results[alias] = {
            "account": alias,
            "status": status,
            "observe_only": observe_only,
            "finished_at": time.time(),
            "result": safe_result,
        }
        self._last_completed[alias] = (time.monotonic(), observe_only)

    def _done_callback(self, alias: str, task: asyncio.Task[Any]) -> None:
        self._collect_done(alias, task)

    async def trigger(
        self, aliases: list[str] | None = None, observe_only: bool = False
    ) -> dict[str, str]:
        if aliases is None:
            names = list(self.runtime.accounts.aliases)
        else:
            names = []
            seen: set[str] = set()
            for alias in aliases:
                if alias not in seen:
                    names.append(alias)
                    seen.add(alias)
        accepted: dict[str, str] = {}
        async with self._lock:
            if self._closed:
                return {alias: "closed" for alias in names}
            known = set(self.runtime.accounts.aliases)
            for alias in names:
                if alias not in known:
                    accepted[alias] = "unknown_account"
                    continue
                task = self._tasks.get(alias)
                if task is not None and task.done():
                    self._collect_done(alias, task)
                if task is not None and not task.done():
                    accepted[alias] = "already_running"
                    continue
                previous = self._last_completed.get(alias)
                if previous:
                    completed_at, previous_observe_only = previous
                    within_cooldown = time.monotonic() - completed_at < self._cooldown
                    # An observe-only attempt cannot satisfy a later mutation request.
                    if within_cooldown and (observe_only or not previous_observe_only):
                        accepted[alias] = "cooldown"
                        continue
                live = sum(1 for item in self._tasks.values() if not item.done())
                if live >= self._max_pending:
                    accepted[alias] = "queue_full"
                    continue
                task = asyncio.create_task(self._run_one(alias, observe_only))
                self._tasks[alias] = task
                self._task_modes[alias] = observe_only
                task.add_done_callback(lambda done, name=alias: self._done_callback(name, done))
                accepted[alias] = "queued"
        return accepted

    async def _run_one(self, alias: str, observe_only: bool) -> dict[str, Any]:
        try:
            timezone_name = getattr(self.runtime.settings, "timezone", "UTC")
            try:
                zone = ZoneInfo(timezone_name)
            except Exception:
                zone = UTC
            day = datetime.now(zone).date().isoformat()
            return await self.runtime.checkin.run(alias, day, observe_only=observe_only)
        except asyncio.CancelledError:
            raise
        except Exception:
            # CheckinService records durable failures where it has a principal; the
            # manager must still resolve the task without leaking exception text.
            return {"account": alias, "outcome": "failed", "status": "failed"}

    def task_status(self, alias: str | None = None) -> dict[str, Any] | list[dict[str, Any]] | None:
        if alias is not None:
            task = self._tasks.get(alias)
            if task is not None and task.done():
                self._collect_done(alias, task)
            result = self._results.get(alias)
            return dict(result) if result else None
        for name, task in self._tasks.items():
            if task.done():
                self._collect_done(name, task)
        return [dict(self._results[name]) for name in self._results]

    async def status(self) -> list[dict[str, Any]]:
        rows = self.runtime.state.checkin_rows(limit=100)
        allowed = {"account", "service_day", "campaign_id", "operation", "status", "updated_at"}
        return [{key: row[key] for key in allowed if key in row} for row in rows]

    async def close(self) -> None:
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            tasks = list(self._tasks.values())
            for task in tasks:
                if not task.done():
                    task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        async with self._lock:
            for alias, task in list(self._tasks.items()):
                if task.done():
                    self._collect_done(alias, task)
            self._tasks.clear()
            self._task_modes.clear()
