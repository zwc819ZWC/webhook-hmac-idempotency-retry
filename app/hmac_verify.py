"""
HMAC signature verification with timestamp-based replay protection.

This module verifies that a webhook request:
1. Came from a sender holding the shared secret(via HMAC-SHA256)
2. Was sent recently(within replay_window_seconds)
3. Uses constant-time compare to prevent timing attacks
"""
import hmac
import hashlib
import time


def verify_hmac(
    raw_body: bytes,
    signature_header: str,
    timestamp_header: str,
    secret: bytes,
    replay_windows_seconds: int = 300,
) -> bool:
    """
    Verify an HMAC-SHA256 signatrue on a webhook reuqest.

    Args:
        raw_body: The raw HTTP request body(bytes, NOT decoded JSON).
                  Must be the exact bytes that were signed.
        signature_header: The X-Webhook-signature header value,
                          e.g. "sha256=abcd1234..."
        timestamp_header: The X-Webhook-Timestamp header value,e.g. "1718180000" (Unix seconds).
        secret: The shared secret as bytes.
        replay_window_seconds: How old can the timestamp be?
                               Default 5 minutes.
    
    Returns:
        True if signature is valid AND timestamp is within window.
        False otherwise.
    """
    # Step1:Parse timstamp.If not a valid integer,reject.
    try:
        ts = int(timestamp_header)
    except (ValueError, TypeError):
        return False

    # Step 2: Check timestamp is within replay window.
    # abs() handles both "too old" and "future timstamp" (clock skew 时钟偏差)
    now = int(time.time())
    if abs(now -ts) > replay_windows_seconds:
        return False

    # Step 3: Compute expected signatrue.
    # we sign "timestamp.body" (Stripe-style) to bind timestamp to body.
    # This prevents an attacker from reusing a valid signature with a different timestamp.
    payload = timestamp_header.encode() + b"." + raw_body
    expected_digest = hmac.new(secret,payload,hashlib.sha256).hexdigest()
    expected_header = f"sha256={expected_digest}"

    # Step 4:constant-time compare.
    # Using == would leak timing info(attacker can guess bytes one at a time).
    # hmac.compare_digest takes the same time regardless of where they differ.
    return hmac.compare_digest(signature_header,expected_header)
