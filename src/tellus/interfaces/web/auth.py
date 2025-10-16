"""
Authentication middleware and dependencies for Tellus REST API.

This module provides API key authentication for securing API endpoints.
API keys can be provided via the X-API-Key header or Authorization header.
"""

import os
import secrets
from typing import Optional

from fastapi import Header, HTTPException, Security, status
from fastapi.security import APIKeyHeader

# API key scheme for OpenAPI documentation
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
api_key_auth_header = APIKeyHeader(name="Authorization", auto_error=False)


def get_api_key_from_env() -> Optional[str]:
    """
    Get the API key from environment variable.

    Returns:
        The API key if set, None otherwise
    """
    return os.getenv("TELLUS_API_KEY")


def is_authentication_enabled() -> bool:
    """
    Check if API authentication is enabled.

    Authentication is enabled if TELLUS_API_KEY is set in the environment.

    Returns:
        True if authentication is enabled, False otherwise
    """
    return get_api_key_from_env() is not None and get_api_key_from_env().strip() != ""


async def verify_api_key(
    x_api_key: Optional[str] = Security(api_key_header),
    authorization: Optional[str] = Security(api_key_auth_header),
) -> str:
    """
    Verify the API key from request headers.

    Checks for API key in two places (in order of precedence):
    1. X-API-Key header
    2. Authorization header (must be in format "Bearer <key>")

    Args:
        x_api_key: API key from X-API-Key header
        authorization: Authorization header value

    Returns:
        The verified API key

    Raises:
        HTTPException: If authentication is enabled but key is missing or invalid
    """
    # If authentication is disabled, allow all requests
    if not is_authentication_enabled():
        return "authentication_disabled"

    expected_key = get_api_key_from_env()

    # Try X-API-Key header first
    if x_api_key:
        # Use constant-time comparison to prevent timing attacks
        if secrets.compare_digest(x_api_key, expected_key):
            return x_api_key

    # Try Authorization header with Bearer scheme
    if authorization:
        if authorization.startswith("Bearer "):
            provided_key = authorization[7:]  # Remove "Bearer " prefix
            if secrets.compare_digest(provided_key, expected_key):
                return provided_key

    # If we reach here, authentication failed
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or missing API key",
        headers={"WWW-Authenticate": "ApiKey"},
    )


async def optional_api_key(
    x_api_key: Optional[str] = Security(api_key_header),
    authorization: Optional[str] = Security(api_key_auth_header),
) -> Optional[str]:
    """
    Optional API key verification for endpoints that support both authenticated and unauthenticated access.

    This is useful for endpoints that may provide different data or rate limits based on authentication.

    Args:
        x_api_key: API key from X-API-Key header
        authorization: Authorization header value

    Returns:
        The verified API key if valid, None if no key provided or authentication disabled
    """
    if not is_authentication_enabled():
        return None

    try:
        return await verify_api_key(x_api_key, authorization)
    except HTTPException:
        # If key is provided but invalid, still raise the error
        if x_api_key or authorization:
            raise
        # No key provided, return None
        return None


def generate_api_key(length: int = 32) -> str:
    """
    Generate a cryptographically secure random API key.

    Args:
        length: Length of the API key in bytes (default: 32 bytes = 64 hex characters)

    Returns:
        A random API key as a hexadecimal string

    Example:
        >>> key = generate_api_key()
        >>> len(key)
        64
    """
    return secrets.token_hex(length)
