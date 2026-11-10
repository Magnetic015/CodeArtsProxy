import json

from codearts_proxy.config import AccountManager, Settings, extract_cftk


def settings(tmp_path, accounts):
    path = tmp_path / "accounts.json"
    path.write_text(json.dumps({"accounts": accounts}))
    return Settings(
        origin="https://example.invalid",
        accounts_file=str(path),
        state_db=str(tmp_path / "state.db"),
        inference_api_keys=frozenset({"key"}),
        admin_api_key="admin",
        checkin_mutations=False,
        checkin_confirm=False,
        timezone="Asia/Shanghai",
        max_image_bytes=100,
        max_file_bytes=200,
        max_total_attachment_bytes=300,
        upstream_timeout=1,
        upstream_stream_timeout=2,
        max_concurrent_per_account=1,
    )


def test_extract_cftk():
    assert extract_cftk("a=1; devclouddevuibjtcftk=token-1; b=2") == "token-1"


def test_account_manager_reloads_cookie_file(tmp_path):
    cookie_file = tmp_path / "a.cookie"
    cookie_file.write_text("devclouddevuibjtcftk=one")
    manager = AccountManager(settings(tmp_path, [{"name": "a", "cookie_file": str(cookie_file)}]))
    assert manager.credentials("a")[1] == "one"
    cookie_file.write_text("devclouddevuibjtcftk=two")
    assert manager.credentials("a")[1] == "two"
