import base64
import os

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


def _get_key() -> bytes:
    from app.core.config import settings

    raw = settings.token_encryption_key
    if not raw:
        raise RuntimeError("TOKEN_ENCRYPTION_KEY is not set")
    return base64.b64decode(raw)


def encrypt(plaintext: str) -> str:
    """Encrypt a string and return a base64-encoded nonce+ciphertext blob."""
    key = _get_key()
    nonce = os.urandom(12)
    ct = AESGCM(key).encrypt(nonce, plaintext.encode(), None)
    return base64.b64encode(nonce + ct).decode()


def decrypt(blob: str) -> str:
    """Decrypt a blob produced by encrypt()."""
    key = _get_key()
    raw = base64.b64decode(blob)
    nonce, ct = raw[:12], raw[12:]
    return AESGCM(key).decrypt(nonce, ct, None).decode()
