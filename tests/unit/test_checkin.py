import json

import pytest

from codearts_proxy.checkin import _upstream_idempotent_key
from codearts_proxy.config import AccountManager, Settings
from codearts_proxy.service import AccountRouter, CheckinService
from codearts_proxy.state import StateStore


class FakeUpstream:
    def __init__(self, delivery, trigger=None, claim=None, confirm=None):
        self.delivery = delivery
        self.trigger = trigger or {"code": 0}
        self.claim = claim or {"code": 0}
        self.confirm = confirm or {"code": 0}
        self.posts = []

    async def identity(self, alias):
        return {"userId": "u-1", "domainId": "d-1", "projectId": "p-1", "region": "cn-north-4"}

    async def models(self, alias):
        return [{"model_id": "glm-5.3-flash", "enable": True}]

    async def ops_get(self, alias, path, params=None):
        return self.delivery

    async def ops_post(self, alias, path, body):
        self.posts.append((path, body))
        if path.endswith("/events/trigger"):
            return self.trigger
        if path.endswith("/claim"):
            return self.claim
        return self.confirm


class DummySettings:
    max_concurrent_per_account = 1
    checkin_mutations = True
    checkin_confirm = True
    max_image_bytes = 10 * 1024 * 1024
    max_file_bytes = 50 * 1024 * 1024
    max_total_attachment_bytes = 100 * 1024 * 1024


def make_service(tmp_path, upstream):
    cfg = tmp_path / "accounts.json"
    cfg.write_text(
        json.dumps({"accounts": [{"name": "a", "cookie": "devclouddevuibjtcftk=token"}]})
    )
    settings = Settings(
        origin="https://example.invalid",
        accounts_file=str(cfg),
        state_db=str(tmp_path / "state.db"),
        inference_api_keys=frozenset({"key"}),
        admin_api_key="admin",
        checkin_mutations=True,
        checkin_confirm=True,
        timezone="Asia/Shanghai",
        max_image_bytes=1000,
        max_file_bytes=2000,
        max_total_attachment_bytes=3000,
        upstream_timeout=1,
        upstream_stream_timeout=2,
        max_concurrent_per_account=1,
    )
    accounts = AccountManager(settings)
    state = StateStore(settings.state_db)
    router = AccountRouter(settings, accounts, upstream, state)
    return CheckinService(settings, router, upstream, state), upstream


@pytest.mark.asyncio
async def test_confirmed_campaign_does_not_write(tmp_path):
    upstream = FakeUpstream(
        {
            "data": {
                "items": [
                    {
                        "campaignId": 34,
                        "type": "USER_LOGIN",
                        "status": "CONFIRMED",
                        "claimable": False,
                    }
                ]
            }
        }
    )
    service, upstream = make_service(tmp_path, upstream)
    result = await service.run("a", "2026-11-10")
    assert result["outcome"] == "already_claimed"
    assert upstream.posts == []


@pytest.mark.asyncio
async def test_claim_failure_never_confirms(tmp_path):
    upstream = FakeUpstream(
        {
            "data": {
                "items": [
                    {
                        "campaignId": 34,
                        "type": "USER_LOGIN",
                        "status": "ELIGIBLE",
                        "claimable": True,
                    }
                ]
            }
        },
        claim={"code": 500, "error_msg": "not today"},
    )
    service, upstream = make_service(tmp_path, upstream)
    result = await service.run("a", "2026-11-10")
    assert result["outcome"] == "pending"
    assert not any(path.endswith("/confirm") for path, _ in upstream.posts)


@pytest.mark.asyncio
async def test_observe_only_has_no_mutations(tmp_path):
    upstream = FakeUpstream(
        {
            "data": {
                "items": [
                    {
                        "campaignId": 34,
                        "type": "USER_LOGIN",
                        "status": "ELIGIBLE",
                        "claimable": True,
                    }
                ]
            }
        }
    )
    service, upstream = make_service(tmp_path, upstream)
    result = await service.run("a", "2026-11-10", observe_only=True)
    assert result["outcome"] == "observe_only"
    assert upstream.posts == []


class SequenceUpstream(FakeUpstream):
    def __init__(self, deliveries, claim=None, confirm=None, trigger=None, claim_error=False):
        super().__init__(deliveries[0], trigger=trigger, claim=claim, confirm=confirm)
        self.deliveries = list(deliveries)
        self.delivery_calls = 0
        self.claim_error = claim_error

    async def identity(self, alias):
        return {"userId": "u-1", "domainId": "d-1", "projectId": "p-1", "region": "cn-north-4"}

    async def ops_get(self, alias, path, params=None):
        index = min(self.delivery_calls, len(self.deliveries) - 1)
        self.delivery_calls += 1
        return self.deliveries[index]

    async def ops_post(self, alias, path, body):
        self.posts.append((path, body))
        if path.endswith("/claim") and self.claim_error:
            raise RuntimeError("dispatch timeout")
        if path.endswith("/events/trigger"):
            return self.trigger
        if path.endswith("/claim"):
            return self.claim
        return self.confirm


def make_sequence_service(tmp_path, upstream, aliases=("a",), state_db=None):
    cfg = tmp_path / "accounts.json"
    cfg.write_text(
        json.dumps(
            {
                "accounts": [
                    {"name": alias, "cookie": "devclouddevuibjtcftk=token"}
                    for alias in aliases
                ]
            }
        )
    )
    settings = Settings(
        origin="https://example.invalid",
        accounts_file=str(cfg),
        state_db=str(state_db or (tmp_path / "state.db")),
        inference_api_keys=frozenset({"key"}),
        admin_api_key="admin",
        checkin_mutations=True,
        checkin_confirm=True,
        timezone="Asia/Shanghai",
        max_image_bytes=1000,
        max_file_bytes=2000,
        max_total_attachment_bytes=3000,
        upstream_timeout=1,
        upstream_stream_timeout=2,
        max_concurrent_per_account=1,
    )
    accounts = AccountManager(settings)
    state = StateStore(settings.state_db)
    router = AccountRouter(settings, accounts, upstream, state)
    return CheckinService(settings, router, upstream, state), state


def campaign(status="ELIGIBLE", claimable=True, campaign_id=34):
    return {
        "data": {
            "items": [
                {
                    "campaignId": campaign_id,
                    "type": "USER_LOGIN",
                    "status": status,
                    "claimable": claimable,
                    "campaignType": "CREDIT",
                    "extra": {"triggerEvent": "user.login"},
                }
            ]
        }
    }


@pytest.mark.asyncio
async def test_noneligible_never_claims(tmp_path):
    upstream = SequenceUpstream([campaign(claimable=False), campaign(claimable=False)])
    service, _ = make_sequence_service(tmp_path, upstream)
    result = await service.run("a", "2026-11-10")
    assert result["outcome"] == "ineligible"
    assert not any(path.endswith("/claim") for path, _ in upstream.posts)


@pytest.mark.asyncio
async def test_pending_intent_is_not_replayed_after_restart_or_alias_change(tmp_path):
    db = tmp_path / "state.db"
    upstream = SequenceUpstream([campaign()], claim_error=True)
    service, state = make_sequence_service(tmp_path, upstream, aliases=("a", "b"), state_db=db)
    first = await service.run("a", "2026-11-10")
    assert first["outcome"] == "pending"
    state.close()

    restarted, restarted_state = make_sequence_service(
        tmp_path, upstream, aliases=("a", "b"), state_db=db
    )
    second = await restarted.run("b", "2026-11-10")
    assert second["outcome"] == "pending"
    assert sum(path.endswith("/claim") for path, _ in upstream.posts) == 1
    restarted_state.close()


@pytest.mark.asyncio
async def test_successful_claim_uses_final_delivery_readback(tmp_path):
    upstream = SequenceUpstream(
        [
            campaign(),
            campaign(status="CLAIMED", claimable=False),
            campaign(status="CONFIRMED", claimable=False),
        ],
        claim={"code": 0},
        confirm={"code": 0},
    )
    service, state = make_sequence_service(tmp_path, upstream)
    result = await service.run("a", "2026-11-10")
    assert result["outcome"] == "claimed_verified"
    assert [path.rsplit("/", 1)[-1] for path, _ in upstream.posts] == ["claim", "confirm"]
    assert upstream.delivery_calls == 3
    assert state.checkin_rows("a")[0]["status"] == "claimed_verified"
    state.close()


@pytest.mark.asyncio
async def test_claim_idempotent_key_is_colon_free(tmp_path):
    """Upstream ops/claim rejects ':' in idempotentKey with HTTP 400
    PROMPTCENTER.00000001 "idempotentKey : 参数无效", so the internal
    colon-joined intent key must never be sent verbatim."""
    upstream = SequenceUpstream(
        [
            campaign(),
            campaign(status="CLAIMED", claimable=False),
            campaign(status="CONFIRMED", claimable=False),
        ],
        claim={"code": 0},
        confirm={"code": 0},
    )
    service, state = make_sequence_service(tmp_path, upstream)
    await service.run("a", "2026-11-10")

    claim_bodies = [body for path, body in upstream.posts if path.endswith("/claim")]
    assert claim_bodies, "expected a claim call"
    key = claim_bodies[0]["idempotentKey"]
    assert ":" not in key
    # deterministic: deriving from the same intent key always yields the same value
    assert _upstream_idempotent_key("x:y:claim:34") == _upstream_idempotent_key("x:y:claim:34")
    # distinct across accounts/days/campaigns
    assert _upstream_idempotent_key("x:y:claim:34") != _upstream_idempotent_key("x:y:claim:3")
    state.close()


@pytest.mark.asyncio
async def test_malformed_business_response_never_confirms(tmp_path):
    upstream = SequenceUpstream([campaign()], claim={"status": "success"})
    service, state = make_sequence_service(tmp_path, upstream)
    result = await service.run("a", "2026-11-10")
    assert result["outcome"] == "pending"
    assert not any(path.endswith("/confirm") for path, _ in upstream.posts)
    assert state.checkin_rows("a")[0]["status"] == "pending"
    state.close()


@pytest.mark.asyncio
async def test_inspect_does_not_expose_principal_hash(tmp_path):
    upstream = SequenceUpstream([campaign()])
    service, state = make_sequence_service(tmp_path, upstream)
    result = await service.inspect("a")
    assert "principal_hash" not in result
    assert result["account"] == "a"
    state.close()
