"""Bearer auth for metrics API: static API token and/or OIDC JWT verification."""

from __future__ import annotations

import hmac
import os
from functools import lru_cache
from typing import Optional, Sequence

import jwt
from jwt import PyJWKClient

OIDC_ISSUER = os.getenv("OIDC_ISSUER", "").strip().rstrip("/") or None
_oidc_audience_raw = os.getenv("OIDC_AUDIENCE", "").strip()
OIDC_AUDIENCES: Sequence[str] = tuple(
    part.strip() for part in _oidc_audience_raw.split(",") if part.strip()
)
API_TOKEN = os.getenv("API_TOKEN", "").strip() or None

_jwk_client: Optional[PyJWKClient] = None


def oidc_configured() -> bool:
    return OIDC_ISSUER is not None


def metrics_auth_enabled() -> bool:
    return API_TOKEN is not None or oidc_configured()


def _api_token_valid(provided: Optional[str]) -> bool:
    if API_TOKEN is None or provided is None:
        return False
    if len(provided) != len(API_TOKEN):
        return False
    return hmac.compare_digest(provided, API_TOKEN)


@lru_cache(maxsize=1)
def _jwks_uri() -> str:
    if OIDC_ISSUER is None:
        raise RuntimeError("OIDC_ISSUER is not configured")
    return f"{OIDC_ISSUER}/.well-known/jwks.json"


def _get_jwk_client() -> PyJWKClient:
    global _jwk_client
    if _jwk_client is None:
        _jwk_client = PyJWKClient(_jwks_uri(), cache_keys=True)
    return _jwk_client


def verify_oidc_access_token(token: str) -> bool:
    if not oidc_configured():
        return False
    try:
        signing_key = _get_jwk_client().get_signing_key_from_jwt(token)
        decode_kwargs: dict = {
            "algorithms": ["RS256", "RS384", "RS512", "ES256", "ES384", "ES512"],
            "issuer": OIDC_ISSUER,
            "options": {"require": ["exp", "iss"]},
        }
        if OIDC_AUDIENCES:
            decode_kwargs["audience"] = list(OIDC_AUDIENCES)
        jwt.decode(token, signing_key.key, **decode_kwargs)
        return True
    except jwt.PyJWTError:
        return False


def bearer_authorized(provided: Optional[str]) -> bool:
    if provided is None:
        return False
    if _api_token_valid(provided):
        return True
    if oidc_configured():
        return verify_oidc_access_token(provided)
    return False
