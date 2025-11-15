"""
Message signing for LinearJC.

Implements HMAC-SHA256 message signing with timestamp validation.
Follows SPEC.md section 10.2.
"""
import hmac
import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any

logger = logging.getLogger(__name__)


class MessageSigningError(Exception):
    """Error during message signing or verification."""
    pass


def sign_message(message_dict: Dict[str, Any], shared_secret: str) -> Dict[str, Any]:
    """
    Sign a message with HMAC-SHA256 using envelope pattern.

    Creates an envelope containing the payload, timestamp, and signature.
    The signature is computed over payload + timestamp.

    Args:
        message_dict: Message payload to sign
        shared_secret: Shared secret for HMAC

    Returns:
        Envelope dict with 'payload', 'timestamp', and 'signature'

    Example:
        >>> msg = {"job_id": "test.job", "data": "value"}
        >>> envelope = sign_message(msg, "secret")
        >>> 'payload' in envelope and 'timestamp' in envelope and 'signature' in envelope
        True
    """
    # Serialize payload to canonical JSON (compact, sorted keys)
    payload = json.dumps(message_dict, sort_keys=True, separators=(',', ':'))

    # Create timestamp (ISO 8601 format, UTC)
    timestamp = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')

    # Compute signature over payload + timestamp
    to_sign = payload + timestamp
    signature = hmac.new(
        shared_secret.encode('utf-8'),
        to_sign.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()

    logger.debug(f"Signed message with timestamp {timestamp}")

    # Return envelope
    return {
        'payload': payload,
        'timestamp': timestamp,
        'signature': signature
    }


def verify_message(
    envelope: Dict[str, Any],
    shared_secret: str,
    max_age_seconds: int = 60
) -> Dict[str, Any]:
    """
    Verify a signed message envelope and return the payload.

    Args:
        envelope: Envelope dict with 'payload', 'timestamp', 'signature'
        shared_secret: Shared secret for HMAC
        max_age_seconds: Maximum message age in seconds (default: 60)

    Returns:
        Parsed payload dict

    Raises:
        MessageSigningError: If verification fails

    Example:
        >>> envelope = {"payload": "{...}", "timestamp": "...", "signature": "..."}
        >>> payload = verify_message(envelope, "secret")
    """
    # Check required fields
    if 'payload' not in envelope:
        raise MessageSigningError("Envelope missing 'payload' field")

    if 'timestamp' not in envelope:
        raise MessageSigningError("Envelope missing 'timestamp' field")

    if 'signature' not in envelope:
        raise MessageSigningError("Envelope missing 'signature' field")

    # Extract fields
    payload = envelope['payload']
    timestamp_str = envelope['timestamp']
    provided_signature = envelope['signature']

    # Parse and validate timestamp
    try:
        # Handle both 'Z' suffix and +00:00 timezone
        if timestamp_str.endswith('Z'):
            ts_for_parse = timestamp_str[:-1] + '+00:00'
        else:
            ts_for_parse = timestamp_str

        message_time = datetime.fromisoformat(ts_for_parse)

        # Ensure timezone-aware
        if message_time.tzinfo is None:
            raise MessageSigningError("Timestamp must be timezone-aware")

    except (ValueError, TypeError) as e:
        raise MessageSigningError(f"Invalid timestamp format: {e}")

    # Check message age
    now = datetime.now(timezone.utc)
    age_seconds = (now - message_time).total_seconds()

    if age_seconds < 0:
        raise MessageSigningError(
            f"Message timestamp is in the future (clock skew?)"
        )

    if age_seconds > max_age_seconds:
        raise MessageSigningError(
            f"Message too old: {age_seconds:.1f}s (max: {max_age_seconds}s)"
        )

    # Verify signature
    to_verify = payload + timestamp_str
    expected_signature = hmac.new(
        shared_secret.encode('utf-8'),
        to_verify.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()

    # Constant-time comparison to prevent timing attacks
    if not hmac.compare_digest(provided_signature, expected_signature):
        raise MessageSigningError("Invalid signature")

    logger.debug(f"Verified message (age: {age_seconds:.1f}s)")

    # Return parsed payload
    try:
        return json.loads(payload)
    except json.JSONDecodeError as e:
        raise MessageSigningError(f"Invalid JSON in payload: {e}")


def create_signed_message(
    message_data: Dict[str, Any],
    shared_secret: str
) -> Dict[str, Any]:
    """
    Create a new signed message envelope (convenience wrapper).

    Args:
        message_data: Message payload
        shared_secret: Shared secret for HMAC

    Returns:
        Envelope dict with 'payload', 'timestamp', and 'signature'

    Example:
        >>> envelope = create_signed_message({"job_id": "test"}, "secret")
        >>> 'payload' in envelope and 'timestamp' in envelope
        True
    """
    return sign_message(message_data, shared_secret)


def verify_and_extract(
    envelope: Dict[str, Any],
    shared_secret: str,
    max_age_seconds: int = 60
) -> Dict[str, Any]:
    """
    Verify envelope and return the payload (convenience wrapper).

    Args:
        envelope: Envelope dict with 'payload', 'timestamp', 'signature'
        shared_secret: Shared secret for HMAC
        max_age_seconds: Maximum message age in seconds

    Returns:
        Parsed payload dict

    Raises:
        MessageSigningError: If verification fails

    Example:
        >>> envelope = {"payload": "{\"job_id\":\"test\"}", "timestamp": "...", "signature": "..."}
        >>> payload = verify_and_extract(envelope, "secret")
        >>> payload
        {'job_id': 'test'}
    """
    return verify_message(envelope, shared_secret, max_age_seconds)
