from __future__ import annotations

import hashlib
import time
from typing import TYPE_CHECKING, Any

from .constants import OPS_BASE
from .errors import UpstreamError
from .state import StateStore

if TYPE_CHECKING:
    from .config import Settings


_FINAL_STATUSES = {"CONFIRMED", "CONSUMED", "REDEEMED", "ALREADY_CLAIMED"}
_CLAIMED_STATUSES = {"CLAIMED", "CLAIM_PENDING", "PENDING", "PROCESSING"}
_UNKNOWN_PRINCIPAL_PREFIX = "unresolved:"


def _upstream_idempotent_key(intent_key: str) -> str:
    """Derive an upstream-safe idempotentKey from the internal intent key.

    The ops/claim endpoint rejects ':' in idempotentKey with
    ``{"error_code":"PROMPTCENTER.00000001","error_msg":"idempotentKey : 参数无效"}``
    (HTTP 400). The intent key is colon-joined, so it must never be sent
    verbatim. Keep the internal key untouched for the state machine, and
    send a deterministic colon-free digest instead: stable across retries
    of the same intent, distinct across accounts/days/campaigns.
    """
    return hashlib.sha256(intent_key.encode("utf-8")).hexdigest()


async def business_success(payload: Any) -> bool:
    """Return true only for the documented integer success codes."""
    if not isinstance(payload, dict):
        return False
    code = payload.get("code")
    return type(code) is int and code in (0, 200)


class _DispatchUncertain(Exception):
    pass


class CheckinService:
    """Fail-closed daily check-in workflow with durable mutation intents."""

    def __init__(self, settings: Settings, router: Any, upstream: Any, state: StateStore):
        self.settings = settings
        self.router = router
        self.upstream = upstream
        self.state = state

    @staticmethod
    def _campaign_id(value: Any) -> int | None:
        if value is None or isinstance(value, bool):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @classmethod
    def _configured_campaign_id(cls, settings: Settings) -> int | None:
        return cls._campaign_id(getattr(settings, "checkin_campaign_id", None))

    @staticmethod
    def _claim_evidence(value: Any) -> bool:
        if not isinstance(value, dict):
            return False
        for key in (
            "claimVerified",
            "claim_verified",
            "claimed",
            "claimSuccess",
            "claimedSuccessfully",
            "verified",
        ):
            if value.get(key) is True:
                return True
        evidence = value.get("evidence")
        if evidence is True:
            return True
        if isinstance(evidence, dict):
            return any(
                evidence.get(key) is True
                for key in ("claimVerified", "claim_verified", "claimed", "verified", "success")
            )
        data = value.get("data")
        if isinstance(data, dict):
            return CheckinService._claim_evidence(data)
        return False

    @staticmethod
    def _status(value: Any) -> str:
        if not isinstance(value, str):
            return "UNKNOWN"
        return value.strip().upper()[:64] or "UNKNOWN"

    @staticmethod
    def _is_credit_campaign(item: dict[str, Any]) -> bool:
        for key in (
            "campaignType",
            "campaign_type",
            "rewardType",
            "reward_type",
            "category",
            "kind",
        ):
            if key in item and str(item.get(key) or "").upper() != "CREDIT":
                return False
        return True

    @classmethod
    def _select_campaign(
        cls, payload: Any, settings: Settings
    ) -> tuple[dict[str, Any] | None, str]:
        if not isinstance(payload, dict):
            return None, "malformed"
        data = payload.get("data")
        items = data.get("items") if isinstance(data, dict) else None
        if not isinstance(items, list):
            return None, "not_found"
        configured_id = cls._configured_campaign_id(settings)
        candidates: list[dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict) or item.get("type") != "USER_LOGIN":
                continue
            if not cls._is_credit_campaign(item):
                continue
            extra = item.get("extra")
            if isinstance(extra, dict) and "triggerEvent" in extra:
                if extra.get("triggerEvent") != "user.login":
                    continue
            campaign_id = cls._campaign_id(item.get("campaignId"))
            if campaign_id is None:
                continue
            if configured_id is not None and campaign_id != configured_id:
                continue
            candidates.append(item)
        if not candidates:
            return None, "not_found"
        if len(candidates) != 1:
            return None, "ambiguous"
        return candidates[0], "selected"

    @classmethod
    def _info(cls, alias: str, campaign: dict[str, Any] | None, reason: str) -> dict[str, Any]:
        if campaign is None:
            return {
                "account": alias,
                "status": "AMBIGUOUS" if reason == "ambiguous" else "NOT_FOUND",
                "claimable": False,
                "campaign_id": None,
                "claim_verified": False,
            }
        title = campaign.get("title")
        return {
            "account": alias,
            "status": cls._status(campaign.get("status")),
            "claimable": campaign.get("claimable") is True,
            "campaign_id": cls._campaign_id(campaign.get("campaignId")),
            "claim_verified": cls._claim_evidence(campaign),
            "title": str(title)[:200] if isinstance(title, str) else None,
        }

    @staticmethod
    def _is_final(info: dict[str, Any]) -> bool:
        return str(info.get("status") or "").upper() in _FINAL_STATUSES

    @staticmethod
    def _is_claimed_pending(info: dict[str, Any]) -> bool:
        return str(info.get("status") or "").upper() in _CLAIMED_STATUSES

    def _known_principal(self, alias: str) -> str | None:
        row = self.state.principal(alias)
        value = row.get("principal_hash") if row else None
        return str(value) if value else None

    def _fallback_principal(self, alias: str) -> str:
        return self._known_principal(alias) or StateStore.value_hash(
            f"{_UNKNOWN_PRINCIPAL_PREFIX}{alias}"
        )

    def _record(
        self,
        alias: str,
        principal_hash: str,
        service_day: str,
        campaign_id: int | None,
        status: str,
        detail: dict[str, Any] | None = None,
    ) -> None:
        self.state.record_checkin(
            alias,
            principal_hash,
            service_day,
            campaign_id,
            "daily",
            status,
            detail,
        )

    @staticmethod
    def _public(info: dict[str, Any], outcome: str, step: str | None = None) -> dict[str, Any]:
        result = {
            key: value
            for key, value in info.items()
            if key in {"account", "status", "claimable", "campaign_id", "claim_verified", "title"}
        }
        result["outcome"] = outcome
        if step:
            result["step"] = step
        return result

    @staticmethod
    def _failure_kind(exc: Exception) -> str:
        if isinstance(exc, UpstreamError):
            if exc.auth_expired:
                return "auth_expired"
            code = str(exc.code or "").lower()
            if code in {"missing_credential", "credential_missing", "auth_missing"}:
                return "missingcredential"
            message = str(exc.message).lower()
        else:
            message = str(exc).lower()
        if "credential" in message or "cookie" in message or "authentication" in message:
            return "missingcredential"
        return "failed"

    async def inspect(self, alias: str) -> dict[str, Any]:
        identity = await self.router.refresh_identity(alias)
        if not isinstance(identity, dict):
            raise ValueError("identity unavailable")
        self.state.remember_principal(alias, identity)
        payload = await self.upstream.ops_get(alias, f"{OPS_BASE}/delivery", {"channel": "WEB"})
        campaign, reason = self._select_campaign(payload, self.settings)
        return self._info(alias, campaign, reason)

    async def _dispatch(
        self,
        *,
        key: str,
        alias: str,
        principal_hash: str,
        service_day: str,
        operation: str,
        path: str,
        body: dict[str, Any],
        step: str,
    ) -> tuple[str, Any | None]:
        owned = self.state.mutation_intent(
            key,
            alias,
            operation,
            service_day,
            status="reserved",
            detail={"step": step},
            principal_hash=principal_hash,
        )
        if not owned:
            existing = self.state.read_mutation_intent(key)
            existing_status = str(existing.get("status") if existing else "pending")
            if existing_status == "succeeded":
                return "succeeded", None
            if existing_status == "failed":
                return "failed", None
            return "pending", None
        try:
            response = await self.upstream.ops_post(alias, path, body)
        except Exception as exc:
            self.state.update_mutation_intent(
                key,
                "pending",
                {"step": step, "dispatch": "uncertain"},
            )
            raise _DispatchUncertain from exc
        if not await business_success(response):
            self.state.update_mutation_intent(
                key,
                "failed",
                {"step": step, "business_success": False},
            )
            return "failed", response
        self.state.update_mutation_intent(
            key,
            "succeeded",
            {"step": step, "business_success": True, "evidence": self._claim_evidence(response)},
        )
        return "succeeded", response

    async def _reconcile(
        self,
        alias: str,
        principal_hash: str,
        service_day: str,
        campaign_id: int,
        *,
        allow_confirm: bool,
        step: str,
    ) -> dict[str, Any]:
        try:
            after = await self.inspect(alias)
        except Exception:
            self._record(
                alias,
                principal_hash,
                service_day,
                campaign_id,
                "pending",
                {"step": step, "reconciliation": "unavailable"},
            )
            return {
                "account": alias,
                "campaign_id": campaign_id,
                "status": "UNKNOWN",
                "claimable": False,
                "outcome": "pending",
                "step": step,
            }
        if after.get("campaign_id") != campaign_id:
            self._record(
                alias,
                principal_hash,
                service_day,
                campaign_id,
                "pending",
                {"step": step, "reconciliation": "campaign_changed"},
            )
            return self._public(after, "pending", step)
        if self._is_final(after):
            self._record(alias, principal_hash, service_day, campaign_id, "claimed_verified", after)
            return self._public(after, "claimed_verified")
        if self._is_claimed_pending(after) and not getattr(
            self.settings, "checkin_confirm", False
        ):
            self._record(alias, principal_hash, service_day, campaign_id, "claimed_verified", after)
            return self._public(after, "claimed_verified")
        outcome = "pending"
        self._record(alias, principal_hash, service_day, campaign_id, outcome, {"step": step})
        return self._public(after, outcome, step)

    async def _confirm(
        self,
        alias: str,
        principal_hash: str,
        service_day: str,
        campaign_id: int,
    ) -> dict[str, Any]:
        key = f"{principal_hash}:{service_day}:confirm:{campaign_id}"
        try:
            status, _ = await self._dispatch(
                key=key,
                alias=alias,
                principal_hash=principal_hash,
                service_day=service_day,
                operation="confirm",
                path=f"{OPS_BASE}/confirm",
                body={"campaignId": campaign_id},
                step="confirm",
            )
        except _DispatchUncertain:
            return await self._reconcile(
                alias,
                principal_hash,
                service_day,
                campaign_id,
                allow_confirm=False,
                step="confirm",
            )
        if status != "succeeded":
            self._record(
                alias,
                principal_hash,
                service_day,
                campaign_id,
                "pending",
                {"step": "confirm", "business_success": False},
            )
            return {
                "account": alias,
                "campaign_id": campaign_id,
                "status": "CLAIMED",
                "claimable": False,
                "outcome": "pending",
                "step": "confirm",
            }
        return await self._reconcile(
            alias,
            principal_hash,
            service_day,
            campaign_id,
            allow_confirm=False,
            step="confirm_reconcile",
        )

    async def _claim_and_finish(
        self,
        alias: str,
        principal_hash: str,
        service_day: str,
        info: dict[str, Any],
    ) -> dict[str, Any]:
        campaign_id = info.get("campaign_id")
        if not isinstance(campaign_id, int):
            self._record(alias, principal_hash, service_day, None, "no_campaign", info)
            return self._public(info, "no_campaign")
        key = f"{principal_hash}:{service_day}:claim:{campaign_id}"
        try:
            status, response = await self._dispatch(
                key=key,
                alias=alias,
                principal_hash=principal_hash,
                service_day=service_day,
                operation="claim",
                path=f"{OPS_BASE}/claim",
                body={
                    "campaignId": campaign_id,
                    "idempotentKey": _upstream_idempotent_key(key),
                    "channel": "WEB",
                },
                step="claim",
            )
        except _DispatchUncertain:
            return await self._reconcile(
                alias,
                principal_hash,
                service_day,
                campaign_id,
                allow_confirm=False,
                step="claim",
            )
        if status != "succeeded":
            self._record(
                alias,
                principal_hash,
                service_day,
                campaign_id,
                "pending",
                {"step": "claim", "business_success": False},
            )
            return self._public(info, "pending", "claim")
        try:
            after = await self.inspect(alias)
        except Exception:
            return await self._reconcile(
                alias,
                principal_hash,
                service_day,
                campaign_id,
                allow_confirm=False,
                step="claim_reconcile",
            )
        if after.get("campaign_id") != campaign_id:
            self._record(
                alias,
                principal_hash,
                service_day,
                campaign_id,
                "pending",
                {"step": "claim_reconcile", "reconciliation": "campaign_changed"},
            )
            return self._public(after, "pending", "claim_reconcile")
        if self._is_final(after):
            self._record(alias, principal_hash, service_day, campaign_id, "claimed_verified", after)
            return self._public(after, "claimed_verified")
        claim_verified = bool(
            after.get("claim_verified")
            or self._claim_evidence(response)
            or self._is_claimed_pending(after)
        )
        if claim_verified and self._is_claimed_pending(after):
            if not getattr(self.settings, "checkin_confirm", False):
                self._record(
                    alias, principal_hash, service_day, campaign_id, "claimed_verified", after
                )
                return self._public(after, "claimed_verified")
            return await self._confirm(alias, principal_hash, service_day, campaign_id)
        self._record(
            alias,
            principal_hash,
            service_day,
            campaign_id,
            "pending",
            {"step": "claim_reconcile"},
        )
        return self._public(after, "pending", "claim_reconcile")

    async def run(self, alias: str, service_day: str, observe_only: bool = False) -> dict[str, Any]:
        principal_hash = self._fallback_principal(alias)
        lease_name: str | None = None
        lease_owner = f"checkin-{time.time_ns()}"
        try:
            try:
                info = await self.inspect(alias)
            except Exception as exc:
                outcome = self._failure_kind(exc)
                self._record(alias, principal_hash, service_day, None, outcome, {"step": "startup"})
                return {"account": alias, "outcome": outcome, "status": outcome}
            principal_hash = self._fallback_principal(alias)
            clean_info = dict(info)
            campaign_id = clean_info.get("campaign_id")
            if clean_info.get("status") == "AMBIGUOUS":
                self._record(alias, principal_hash, service_day, None, "ambiguous", clean_info)
                return self._public(clean_info, "ambiguous")
            if campaign_id is None:
                self._record(alias, principal_hash, service_day, None, "no_campaign", clean_info)
                return self._public(clean_info, "no_campaign")
            if self._is_final(clean_info):
                self._record(
                    alias, principal_hash, service_day, campaign_id, "already_claimed", clean_info
                )
                return self._public(clean_info, "already_claimed")
            if self._is_claimed_pending(clean_info) and not clean_info.get("claim_verified"):
                self._record(alias, principal_hash, service_day, campaign_id, "pending", clean_info)
                return self._public(clean_info, "pending")
            if observe_only or not getattr(self.settings, "checkin_mutations", False):
                self._record(
                    alias, principal_hash, service_day, campaign_id, "observe_only", clean_info
                )
                return self._public(clean_info, "observe_only")

            lease_name = f"checkin:{principal_hash}:{service_day}"
            if not self.state.acquire_lease(lease_name, lease_owner, 180):
                return {"account": alias, "outcome": "pending", "reason": "already_running"}

            if self._is_claimed_pending(clean_info):
                if not clean_info.get("claim_verified"):
                    self._record(
                        alias, principal_hash, service_day, campaign_id, "pending", clean_info
                    )
                    return self._public(clean_info, "pending")
                return await self._confirm(alias, principal_hash, service_day, campaign_id)

            if not clean_info.get("claimable"):
                trigger_key = f"{principal_hash}:{service_day}:login"
                try:
                    trigger_status, _ = await self._dispatch(
                        key=trigger_key,
                        alias=alias,
                        principal_hash=principal_hash,
                        service_day=service_day,
                        operation="login",
                        path=f"{OPS_BASE}/events/trigger",
                        body={
                            "triggerEvent": "user.login",
                            "channel": "WEB",
                            "idempotentKey": trigger_key,
                        },
                        step="trigger",
                    )
                except _DispatchUncertain:
                    return await self._reconcile(
                        alias,
                        principal_hash,
                        service_day,
                        campaign_id,
                        allow_confirm=False,
                        step="trigger",
                    )
                if trigger_status != "succeeded":
                    self._record(
                        alias,
                        principal_hash,
                        service_day,
                        campaign_id,
                        "failed" if trigger_status == "failed" else "pending",
                        {"step": "trigger", "business_success": False},
                    )
                    return self._public(
                        clean_info,
                        "failed" if trigger_status == "failed" else "pending",
                        "trigger",
                    )
                try:
                    refreshed = await self.inspect(alias)
                except Exception:
                    return await self._reconcile(
                        alias,
                        principal_hash,
                        service_day,
                        campaign_id,
                        allow_confirm=False,
                        step="trigger_reconcile",
                    )
                refreshed_principal = self._fallback_principal(alias)
                refreshed = dict(refreshed)
                if (
                    refreshed_principal != principal_hash
                    or refreshed.get("campaign_id") != campaign_id
                ):
                    self._record(
                        alias,
                        principal_hash,
                        service_day,
                        campaign_id,
                        "pending",
                        {
                            "step": "trigger_reconcile",
                            "reconciliation": "principal_or_campaign_changed",
                        },
                    )
                    return self._public(refreshed, "pending", "trigger_reconcile")
                if self._is_final(refreshed):
                    self._record(
                        alias,
                        principal_hash,
                        service_day,
                        campaign_id,
                        "already_claimed",
                        refreshed,
                    )
                    return self._public(refreshed, "already_claimed")
                if not refreshed.get("claimable"):
                    self._record(
                        alias, principal_hash, service_day, campaign_id, "ineligible", refreshed
                    )
                    return self._public(refreshed, "ineligible")
                clean_info = refreshed

            return await self._claim_and_finish(alias, principal_hash, service_day, clean_info)
        except Exception as exc:
            outcome = self._failure_kind(exc)
            self._record(alias, principal_hash, service_day, None, outcome, {"step": "run"})
            return {"account": alias, "outcome": outcome, "status": outcome}
        finally:
            if lease_name:
                self.state.release_lease(lease_name, lease_owner)
