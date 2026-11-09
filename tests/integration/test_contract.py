import pytest

from codearts_proxy.config import ConfigError, load_settings


def test_settings_reject_missing_secrets(monkeypatch):
    monkeypatch.delenv("CODEARTS_INFERENCE_API_KEYS", raising=False)
    monkeypatch.delenv("CODEARTS_ADMIN_API_KEY", raising=False)
    with pytest.raises(ConfigError):
        load_settings()


def test_settings_separates_inference_and_admin_keys(monkeypatch):
    monkeypatch.setenv("CODEARTS_INFERENCE_API_KEYS", "client-a,client-b")
    monkeypatch.setenv("CODEARTS_ADMIN_API_KEY", "admin-a")
    settings = load_settings()
    assert settings.inference_api_keys == frozenset({"client-a", "client-b"})
    assert settings.admin_api_key == "admin-a"
    assert settings.admin_api_key not in settings.inference_api_keys
