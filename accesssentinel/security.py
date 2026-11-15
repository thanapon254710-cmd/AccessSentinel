"""Security helpers: password hashing (Argon2id), constant-time token comparison, one-way references.
Nothing here logs or returns plaintext secrets."""
import hashlib
import hmac
import secrets

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError, VerifyMismatchError

_ph = PasswordHasher()                       # argon2-cffi defaults: Argon2id, RFC 9106 low-memory profile

# Verifying against this hash when the user does not exist keeps login timing similar for unknown users.
_DUMMY_HASH = _ph.hash(secrets.token_urlsafe(16))


def hash_password(password: str) -> str:
    if not password or len(password) < 8:
        raise ValueError("password must be at least 8 characters")
    return _ph.hash(password)


def verify_password(stored_hash: str | None, password: str) -> bool:
    try:
        return _ph.verify(stored_hash or _DUMMY_HASH, password) and stored_hash is not None
    except (VerifyMismatchError, VerificationError, InvalidHashError):
        return False


def tokens_equal(a: str | None, b: str | None) -> bool:
    if not a or not b:
        return False
    return hmac.compare_digest(a.encode(), b.encode())


def new_session_token() -> str:
    return secrets.token_urlsafe(32)


def one_way_ref(value: str, n: int = 16) -> str:
    """Short irreversible reference (e.g. of a session token or user agent) safe to store in events/logs."""
    return hashlib.sha256(value.encode()).hexdigest()[:n]
