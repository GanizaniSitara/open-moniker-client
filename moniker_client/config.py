"""Client configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Config file search paths (in order of precedence, last wins)
CONFIG_SEARCH_PATHS = [
    Path.home() / ".moniker" / "client.yaml",  # User-level defaults
    Path(".moniker.yaml"),  # Project-level overrides
]


@dataclass
class ClientConfig:
    """
    Configuration for the moniker client.

    Precedence (lowest to highest):
    1. Defaults
    2. ~/.moniker/client.yaml
    3. .moniker.yaml (project root)
    4. Environment variables (MONIKER_*)
    5. Constructor arguments
    """
    # Moniker service URL
    service_url: str = field(
        default_factory=lambda: os.environ.get("MONIKER_SERVICE_URL", "http://localhost:8050")
    )

    # Identity headers
    app_id: str | None = field(
        default_factory=lambda: os.environ.get("MONIKER_APP_ID")
    )
    team: str | None = field(
        default_factory=lambda: os.environ.get("MONIKER_TEAM")
    )

    # Request timeout (seconds)
    timeout: float = field(
        default_factory=lambda: float(os.environ.get("MONIKER_TIMEOUT", "30"))
    )

    # Report telemetry back to service
    report_telemetry: bool = field(
        default_factory=lambda: os.environ.get("MONIKER_REPORT_TELEMETRY", "true").lower() == "true"
    )

    # Cache resolved connections locally (seconds, 0 = disabled)
    cache_ttl: float = field(
        default_factory=lambda: float(os.environ.get("MONIKER_CACHE_TTL", "60"))
    )

    # Authentication method: "kerberos", "jwt", or None
    auth_method: str | None = field(
        default_factory=lambda: os.environ.get("MONIKER_AUTH_METHOD")
    )

    # Kerberos settings
    kerberos_service_principal: str | None = field(
        default_factory=lambda: os.environ.get("MONIKER_SERVICE_PRINCIPAL")
    )

    # JWT settings
    jwt_token: str | None = None  # Direct token (not from env for security)
    jwt_token_env: str = field(
        default_factory=lambda: os.environ.get("MONIKER_JWT_ENV", "MONIKER_JWT")
    )
    jwt_token_file: str | None = field(
        default_factory=lambda: os.environ.get("MONIKER_JWT_FILE")
    )

    # Database credentials (not from service for security)
    # These are used by the client when connecting to sources
    snowflake_user: str | None = field(
        default_factory=lambda: os.environ.get("SNOWFLAKE_USER")
    )
    snowflake_password: str | None = field(
        default_factory=lambda: os.environ.get("SNOWFLAKE_PASSWORD")
    )
    snowflake_private_key_path: str | None = field(
        default_factory=lambda: os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH")
    )

    oracle_user: str | None = field(
        default_factory=lambda: os.environ.get("ORACLE_USER")
    )
    oracle_password: str | None = field(
        default_factory=lambda: os.environ.get("ORACLE_PASSWORD")
    )

    mssql_user: str | None = field(
        default_factory=lambda: os.environ.get("MSSQL_USER")
    )
    mssql_password: str | None = field(
        default_factory=lambda: os.environ.get("MSSQL_PASSWORD")
    )

    # Additional credentials as dict
    credentials: dict[str, Any] = field(default_factory=dict)

    # Deprecation awareness (feature toggle)
    deprecation_enabled: bool = field(
        default_factory=lambda: os.environ.get("MONIKER_DEPRECATION_ENABLED", "false").lower() == "true"
    )
    warn_on_deprecated: bool = field(
        default_factory=lambda: os.environ.get("MONIKER_WARN_DEPRECATED", "true").lower() == "true"
    )
    deprecation_callback: Any = None  # callable(path, message, successor)

    # Retry configuration for transient failures
    retry_max_attempts: int = field(
        default_factory=lambda: int(os.environ.get("MONIKER_RETRY_MAX_ATTEMPTS", "3"))
    )
    retry_backoff_factor: float = field(
        default_factory=lambda: float(os.environ.get("MONIKER_RETRY_BACKOFF_FACTOR", "0.5"))
    )
    retry_status_codes: tuple[int, ...] = field(
        default_factory=lambda: (502, 503, 504)
    )

    def get_credential(self, source_type: str, key: str) -> str | None:
        """Get a credential for a source type."""
        # Check specific attributes first
        if source_type == "snowflake":
            if key == "user":
                return self.snowflake_user
            if key == "password":
                return self.snowflake_password
            if key == "private_key_path":
                return self.snowflake_private_key_path
        elif source_type == "oracle":
            if key == "user":
                return self.oracle_user
            if key == "password":
                return self.oracle_password
        elif source_type == "mssql":
            if key == "user":
                return self.mssql_user
            if key == "password":
                return self.mssql_password

        # Check credentials dict
        return self.credentials.get(f"{source_type}_{key}")

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ClientConfig:
        """Create config from dictionary."""
        return cls(
            service_url=data.get("service_url", os.environ.get("MONIKER_SERVICE_URL", "http://localhost:8050")),
            app_id=data.get("app_id", os.environ.get("MONIKER_APP_ID")),
            team=data.get("team", os.environ.get("MONIKER_TEAM")),
            timeout=float(data.get("timeout", os.environ.get("MONIKER_TIMEOUT", "30"))),
            report_telemetry=data.get("report_telemetry", os.environ.get("MONIKER_REPORT_TELEMETRY", "true").lower() == "true"),
            cache_ttl=float(data.get("cache_ttl", os.environ.get("MONIKER_CACHE_TTL", "60"))),
            auth_method=data.get("auth_method", os.environ.get("MONIKER_AUTH_METHOD")),
            kerberos_service_principal=data.get("kerberos_service_principal", os.environ.get("MONIKER_SERVICE_PRINCIPAL")),
            jwt_token=data.get("jwt_token"),
            jwt_token_env=data.get("jwt_token_env", os.environ.get("MONIKER_JWT_ENV", "MONIKER_JWT")),
            jwt_token_file=data.get("jwt_token_file", os.environ.get("MONIKER_JWT_FILE")),
            snowflake_user=data.get("snowflake_user", os.environ.get("SNOWFLAKE_USER")),
            snowflake_password=data.get("snowflake_password", os.environ.get("SNOWFLAKE_PASSWORD")),
            snowflake_private_key_path=data.get("snowflake_private_key_path", os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH")),
            oracle_user=data.get("oracle_user", os.environ.get("ORACLE_USER")),
            oracle_password=data.get("oracle_password", os.environ.get("ORACLE_PASSWORD")),
            credentials=data.get("credentials", {}),
            retry_max_attempts=int(data.get("retry_max_attempts", os.environ.get("MONIKER_RETRY_MAX_ATTEMPTS", "3"))),
            retry_backoff_factor=float(data.get("retry_backoff_factor", os.environ.get("MONIKER_RETRY_BACKOFF_FACTOR", "0.5"))),
        )

    @classmethod
    def from_yaml(cls, path: str | Path) -> ClientConfig:
        """Load config from YAML file."""
        import yaml
        with open(path, "r") as f:
            data = yaml.safe_load(f)
        return cls.from_dict(data or {})

    @classmethod
    def load(cls, config_file: str | Path | None = None) -> ClientConfig:
        """
        Load config with auto-discovery.

        Search order (last wins):
        1. ~/.moniker/client.yaml
        2. .moniker.yaml
        3. Explicit config_file argument
        4. Environment variables always override file values
        """
        merged: dict[str, Any] = {}

        # Load from default paths
        for path in CONFIG_SEARCH_PATHS:
            if path.exists():
                import yaml
                with open(path, "r") as f:
                    data = yaml.safe_load(f) or {}
                merged.update(data)

        # Load explicit config file
        if config_file:
            import yaml
            with open(config_file, "r") as f:
                data = yaml.safe_load(f) or {}
            merged.update(data)

        return cls.from_dict(merged)
