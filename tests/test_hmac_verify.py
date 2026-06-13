"""
Tests for HMAC signature verification.

Coverage:
1. Valid signature + valid timestamp → True
2. Invalid signature → False  
3. Expired timestamp → False
4. Malformed timestamp → False
"""
import hmac
import hashlib
import time

import pytest

from app.hmac_verify import verify_hmac


# Shared test fixtures
SECRET = b"test_secret_123"
BODY = b'{"event":"payment.succeeded","amount":9999}'


def _make_signature(body: bytes, timestamp: str, secret: bytes) -> str:
    """Helper: generate a valid signature header for a given body/timestamp/secret."""
    payload = timestamp.encode() + b"." + body
    digest = hmac.new(secret, payload, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def test_valid_signature_and_timestamp_returns_true():
    """Happy path: a freshly signed request should verify."""
    ts = str(int(time.time()))
    sig = _make_signature(BODY, ts, SECRET)
    
    result = verify_hmac(BODY, sig, ts, SECRET)
    
    assert result is True


def test_tampered_body_returns_false():
    """If the body is modified after signing, verification must fail."""
    ts = str(int(time.time()))
    sig = _make_signature(BODY, ts, SECRET)
    
    tampered_body = b'{"event":"payment.succeeded","amount":99999999}'
    result = verify_hmac(tampered_body, sig, ts, SECRET)
    
    assert result is False


def test_expired_timestamp_returns_false():
    """Request older than replay_window must be rejected."""
    # 10 minutes ago = 600 seconds, beyond default 300 second window
    expired_ts = str(int(time.time()) - 600)
    sig = _make_signature(BODY, expired_ts, SECRET)
    
    result = verify_hmac(BODY, sig, expired_ts, SECRET)
    
    assert result is False


def test_malformed_timestamp_returns_false():
    """Non-numeric timestamp must be rejected, not crash."""
    bad_ts = "not_a_number"
    # signature doesn't matter; we should reject on timestamp parse
    sig = "sha256=anything"
    
    result = verify_hmac(BODY, sig, bad_ts, SECRET)
    
    assert result is False