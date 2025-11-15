"""
Security utilities for LinearJC coordinator.

Provides path validation, secret validation, and other security helpers.
"""
import logging
import re
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)


class SecurityError(Exception):
    """Security validation failed."""
    pass


def validate_path(
    path: str,
    allowed_roots: Optional[List[str]] = None,
    allow_relative: bool = False,
    description: str = "path"
) -> Path:
    """
    Validate a file system path for security issues.

    Checks for:
    - Path traversal attempts (..)
    - Shell metacharacters
    - Null bytes
    - Optionally validates path is within allowed roots

    Args:
        path: Path to validate
        allowed_roots: List of allowed root directories (optional)
        allow_relative: Allow relative paths (default: False)
        description: Description for error messages

    Returns:
        Resolved absolute Path object

    Raises:
        SecurityError: If path is invalid or unsafe

    Example:
        >>> allowed = ["/data", "/tmp/linearjc"]
        >>> validate_path("/data/input.txt", allowed)
        PosixPath('/data/input.txt')
    """
    if not path:
        raise SecurityError(f"Empty {description}")

    # Check for null bytes
    if '\0' in path:
        raise SecurityError(f"Null byte in {description}: {path!r}")

    # Check for suspicious characters (shell metacharacters)
    suspicious_chars = [';', '&', '|', '$', '`', '\n', '\r']
    for char in suspicious_chars:
        if char in path:
            raise SecurityError(
                f"Suspicious character {char!r} in {description}: {path}"
            )

    try:
        p = Path(path)
    except Exception as e:
        raise SecurityError(f"Invalid {description}: {e}")

    # Check for path traversal patterns
    path_parts = p.parts
    if '..' in path_parts:
        raise SecurityError(f"Path traversal detected in {description}: {path}")

    # Resolve to absolute path
    if not allow_relative:
        try:
            p = p.resolve()
        except Exception as e:
            raise SecurityError(f"Cannot resolve {description}: {e}")

        # Check it's absolute after resolution
        if not p.is_absolute():
            raise SecurityError(f"Path must be absolute: {path}")

    # If allowed_roots specified, check path is within one of them
    if allowed_roots:
        p_resolved = p.resolve() if allow_relative else p

        # Convert allowed roots to resolved paths
        allowed_resolved = []
        for root in allowed_roots:
            try:
                allowed_resolved.append(Path(root).resolve())
            except Exception as e:
                logger.warning(f"Invalid allowed root {root}: {e}")
                continue

        # Check if path is within any allowed root
        is_allowed = False
        for root in allowed_resolved:
            try:
                p_resolved.relative_to(root)
                is_allowed = True
                break
            except ValueError:
                continue

        if not is_allowed:
            raise SecurityError(
                f"Path outside allowed directories: {path}\n"
                f"Allowed roots: {allowed_roots}"
            )

    logger.debug(f"Validated {description}: {p}")
    return p


def validate_job_id(job_id: str) -> str:
    """
    Validate job ID format.

    Job IDs must:
    - Only contain alphanumeric, dots, dashes, underscores
    - Not contain path separators or path traversal
    - Be between 1 and 200 characters

    Args:
        job_id: Job identifier to validate

    Returns:
        The validated job ID

    Raises:
        SecurityError: If job ID is invalid

    Example:
        >>> validate_job_id("hello.world")
        'hello.world'
        >>> validate_job_id("../etc/passwd")
        SecurityError: Invalid job ID format
    """
    if not job_id:
        raise SecurityError("Empty job ID")

    if len(job_id) > 200:
        raise SecurityError(f"Job ID too long (max 200 chars): {job_id}")

    # Only alphanumeric, dots, dashes, underscores
    if not re.match(r'^[a-zA-Z0-9._-]+$', job_id):
        raise SecurityError(
            f"Invalid job ID format: {job_id}\n"
            f"Only alphanumeric, dots, dashes, and underscores allowed"
        )

    # No path separators or traversal
    if '..' in job_id or '/' in job_id or '\\' in job_id:
        raise SecurityError(
            f"Job ID cannot contain path separators or ..: {job_id}"
        )

    return job_id


def validate_registry_key(key: str) -> str:
    """
    Validate data registry key format.

    Registry keys must:
    - Only contain alphanumeric, dots, dashes, underscores, slashes
    - Not contain path traversal (..)
    - Be between 1 and 200 characters

    Args:
        key: Registry key to validate

    Returns:
        The validated registry key

    Raises:
        SecurityError: If registry key is invalid

    Example:
        >>> validate_registry_key("data/input1")
        'data/input1'
    """
    if not key:
        raise SecurityError("Empty registry key")

    if len(key) > 200:
        raise SecurityError(f"Registry key too long (max 200 chars): {key}")

    # Allow alphanumeric, dots, dashes, underscores, slashes
    if not re.match(r'^[a-zA-Z0-9._/-]+$', key):
        raise SecurityError(
            f"Invalid registry key format: {key}\n"
            f"Only alphanumeric, dots, dashes, underscores, slashes allowed"
        )

    # No path traversal
    if '..' in key:
        raise SecurityError(f"Registry key cannot contain ..: {key}")

    return key


def validate_shared_secret(secret: str) -> None:
    """
    Validate shared secret meets security requirements.

    Checks for:
    - Minimum length (32 characters)
    - Not a common/weak secret
    - Sufficient entropy (basic check)

    Args:
        secret: Shared secret to validate

    Raises:
        SecurityError: If secret is weak or invalid

    Example:
        >>> validate_shared_secret("dev-secret-change-in-production")
        SecurityError: Insecure secret detected
    """
    if not secret:
        raise SecurityError("Empty shared secret")

    # Check minimum length
    if len(secret) < 32:
        raise SecurityError(
            f"Shared secret too short (minimum 32 characters)\n"
            f"Generate a secure secret with:\n"
            f"  python3 -c 'import secrets; print(secrets.token_hex(32))'"
        )

    # Check for common insecure patterns
    insecure_patterns = [
        'dev-secret',
        'change-in-production',
        'minioadmin',
        'password',
        'secret123',
        'test',
        'demo',
        'example',
        'sample'
    ]

    secret_lower = secret.lower()
    for pattern in insecure_patterns:
        if pattern in secret_lower:
            raise SecurityError(
                f"Insecure secret detected! Contains '{pattern}'.\n"
                f"Please generate a secure secret with:\n"
                f"  python3 -c 'import secrets; print(secrets.token_hex(32))'"
            )

    # Basic entropy check: require at least some variety
    unique_chars = len(set(secret))
    if unique_chars < 10:
        raise SecurityError(
            f"Shared secret has insufficient entropy "
            f"(only {unique_chars} unique characters)\n"
            f"Generate a secure secret with:\n"
            f"  python3 -c 'import secrets; print(secrets.token_hex(32))'"
        )

    logger.debug("Shared secret validation passed")


def validate_minio_credentials(access_key: str, secret_key: str) -> None:
    """
    Validate Minio credentials are not default/weak values.

    Args:
        access_key: Minio access key
        secret_key: Minio secret key

    Raises:
        SecurityError: If credentials are default or weak
    """
    # Check for empty
    if not access_key or not secret_key:
        raise SecurityError("Empty Minio credentials")

    # Check for default Minio credentials
    if access_key == 'minioadmin' or secret_key == 'minioadmin':
        raise SecurityError(
            "Default Minio credentials detected!\n"
            "Please set secure credentials for Minio.\n"
            "Default 'minioadmin' credentials must not be used."
        )

    # Check minimum length
    if len(access_key) < 8 or len(secret_key) < 16:
        raise SecurityError(
            "Minio credentials too short\n"
            f"Access key: minimum 8 characters (got {len(access_key)})\n"
            f"Secret key: minimum 16 characters (got {len(secret_key)})"
        )

    logger.debug("Minio credentials validation passed")
