"""Authentication helpers for the moniker client."""

from __future__ import annotations

import base64
import logging
import os
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .config import ClientConfig

logger = logging.getLogger(__name__)

# Empty dict singleton - avoid allocation on hot path
_EMPTY_HEADERS: dict[str, str] = {}

# Optional gssapi import for Kerberos
try:
    import gssapi
    GSSAPI_AVAILABLE = True
except ImportError:
    gssapi = None  # type: ignore
    GSSAPI_AVAILABLE = False


@dataclass
class ClientAuth:
    """Authentication helper for moniker-client."""

    # Cache for JWT headers (token doesn't change often)
    _jwt_cache: dict[str, str] = field(default_factory=dict, repr=False)
    _jwt_cache_token: str | None = field(default=None, repr=False)

    def get_auth_headers(self, config: ClientConfig) -> dict[str, str]:
        """
        Get Authorization header based on config.

        Fast path: returns immediately if no auth configured.

        Args:
            config: Client configuration with auth settings

        Returns:
            Dictionary with Authorization header, or empty dict if no auth
        """
        # Fast path - no auth configured (most common in dev/notebooks)
        auth_method = config.auth_method
        if not auth_method:
            return _EMPTY_HEADERS

        if auth_method == "kerberos":
            return self._get_kerberos_headers(config)
        elif auth_method == "jwt":
            return self._get_jwt_headers_cached(config)
        return _EMPTY_HEADERS

    def _get_kerberos_headers(self, config: ClientConfig) -> dict[str, str]:
        """
        Get Kerberos SPNEGO Negotiate header.

        Uses gssapi to obtain a SPNEGO token for the configured service principal.
        Requires a valid Kerberos ticket (obtained via kinit).

        Args:
            config: Client configuration with kerberos_service_principal

        Returns:
            {"Authorization": "Negotiate <base64-token>"} or empty dict on failure
        """
        if not GSSAPI_AVAILABLE:
            logger.warning(
                "gssapi not available - install with: pip install moniker-client[auth]"
            )
            return _EMPTY_HEADERS

        if not config.kerberos_service_principal:
            logger.warning("Kerberos auth requested but no service principal configured")
            return _EMPTY_HEADERS

        try:
            # Create the target service name
            service_name = gssapi.Name(
                config.kerberos_service_principal,
                name_type=gssapi.NameType.kerberos_principal,
            )

            # Create security context
            ctx = gssapi.SecurityContext(
                name=service_name,
                usage="initiate",
            )

            # Get the initial SPNEGO token
            token = ctx.step()

            if token:
                token_b64 = base64.b64encode(token).decode("ascii")
                return {"Authorization": f"Negotiate {token_b64}"}
            else:
                logger.warning("Failed to obtain Kerberos token")
                return _EMPTY_HEADERS

        except Exception as e:
            logger.warning(f"Kerberos authentication failed: {e}")
            return _EMPTY_HEADERS

    def _get_jwt_headers_cached(self, config: ClientConfig) -> dict[str, str]:
        """Get JWT headers with caching to avoid repeated token lookups."""
        token = self._get_jwt_token(config)
        if not token:
            return _EMPTY_HEADERS

        # Cache hit - same token as before
        if token == self._jwt_cache_token and self._jwt_cache:
            return self._jwt_cache

        # Cache miss - build and cache
        self._jwt_cache = {"Authorization": f"Bearer {token}"}
        self._jwt_cache_token = token
        return self._jwt_cache

    def _get_jwt_headers(self, config: ClientConfig) -> dict[str, str]:
        """
        Get JWT Bearer token header.

        Retrieves token from (in order of priority):
        1. config.jwt_token (explicit token)
        2. Environment variable specified by config.jwt_token_env
        3. File specified by config.jwt_token_file

        Args:
            config: Client configuration with JWT settings

        Returns:
            {"Authorization": "Bearer <token>"} or empty dict if no token
        """
        token = self._get_jwt_token(config)
        if token:
            return {"Authorization": f"Bearer {token}"}
        return _EMPTY_HEADERS

    def _get_jwt_token(self, config: ClientConfig) -> str | None:
        """Get JWT token from config, environment, or file."""
        # 1. Explicit token in config
        if config.jwt_token:
            return config.jwt_token

        # 2. Environment variable
        if config.jwt_token_env:
            token = os.environ.get(config.jwt_token_env)
            if token:
                return token

        # 3. Token file
        if config.jwt_token_file:
            try:
                with open(config.jwt_token_file, "r") as f:
                    return f.read().strip()
            except Exception as e:
                logger.warning(f"Failed to read JWT token file: {e}")

        return None


# Default instance
_client_auth = ClientAuth()


def get_auth_headers(config: ClientConfig) -> dict[str, str]:
    """Get authentication headers for the given config."""
    return _client_auth.get_auth_headers(config)
