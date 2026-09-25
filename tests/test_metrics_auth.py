import importlib
import sys

import pytest


@pytest.fixture
def auth_module(monkeypatch):
    monkeypatch.delenv("API_TOKEN", raising=False)
    monkeypatch.delenv("OIDC_ISSUER", raising=False)
    monkeypatch.delenv("OIDC_AUDIENCE", raising=False)
    if "metrics_auth" in sys.modules:
        del sys.modules["metrics_auth"]
    return importlib.import_module("metrics_auth")


def test_bearer_authorized_api_token(auth_module, monkeypatch):
    monkeypatch.setattr(auth_module, "API_TOKEN", "secret-token")
    assert auth_module.bearer_authorized("secret-token") is True
    assert auth_module.bearer_authorized("wrong-token") is False


def test_bearer_authorized_oidc(auth_module, monkeypatch):
    monkeypatch.setattr(auth_module, "API_TOKEN", None)
    monkeypatch.setattr(auth_module, "OIDC_ISSUER", "https://issuer.example.com")
    monkeypatch.setattr(
        auth_module, "verify_oidc_access_token", lambda token: token == "valid-jwt"
    )
    assert auth_module.bearer_authorized("valid-jwt") is True
    assert auth_module.bearer_authorized("bad-jwt") is False
