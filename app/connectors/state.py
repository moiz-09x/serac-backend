"""HMAC-signed OAuth state parameter to prevent CSRF attacks."""

import hashlib
import hmac
import json
import time


def _secret() -> bytes:
    from app.core.config import settings

    return settings.oauth_state_secret.encode()


def sign_state(tenant_id: str) -> str:
    """Return a signed state string encoding tenant_id + timestamp."""
    payload = json.dumps({"tenant_id": tenant_id, "ts": int(time.time())})
    sig = hmac.new(_secret(), payload.encode(), hashlib.sha256).hexdigest()
    # encode as payload.signature so it survives URL encoding
    import base64

    return base64.urlsafe_b64encode(f"{payload}||{sig}".encode()).decode()


def verify_state(state: str, max_age: int = 600) -> str:
    """Verify state and return tenant_id. Raises ValueError on failure."""
    import base64

    try:
        decoded = base64.urlsafe_b64decode(state.encode()).decode()
        payload_str, sig = decoded.rsplit("||", 1)
    except Exception:
        raise ValueError("Malformed state parameter")

    expected = hmac.new(_secret(), payload_str.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(sig, expected):
        raise ValueError("Invalid state signature")

    data = json.loads(payload_str)
    if int(time.time()) - data["ts"] > max_age:
        raise ValueError("State parameter expired")

    return data["tenant_id"]
