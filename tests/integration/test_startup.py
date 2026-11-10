import json

import pytest

from codearts_proxy.app import app, lifespan


@pytest.mark.asyncio
async def test_application_startup_with_synthetic_account(tmp_path, monkeypatch):
    accounts = tmp_path / "accounts.json"
    accounts.write_text(
        json.dumps({"accounts": [{"name": "a", "cookie": "devclouddevuibjtcftk=dummy"}]})
    )
    monkeypatch.setenv("CODEARTS_INFERENCE_API_KEYS", "client")
    monkeypatch.setenv("CODEARTS_ADMIN_API_KEY", "admin")
    monkeypatch.setenv("CODEARTS_ACCOUNTS_FILE", str(accounts))
    monkeypatch.setenv("CODEARTS_STATE_DB", str(tmp_path / "state.sqlite3"))
    async with lifespan(app):
        assert app.state.startup_error is None, app.state.startup_error
        assert app.state.runtime is not None
