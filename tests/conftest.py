"""Shared pytest fixtures for moniker_client tests."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from unittest.mock import MagicMock

import pytest


@dataclass
class MockResolvedSource:
    """Mock ResolvedSource for testing adapters."""

    moniker: str = "test/moniker"
    path: str = "test/moniker"
    source_type: str = "oracle"
    connection: dict[str, Any] = field(default_factory=dict)
    query: str | None = None
    params: dict[str, Any] = field(default_factory=dict)
    schema_info: dict[str, Any] | None = None
    read_only: bool = True
    ownership: dict[str, Any] = field(default_factory=dict)
    binding_path: str = "test"
    sub_path: str | None = None


@dataclass
class MockClientConfig:
    """Mock ClientConfig for testing adapters."""

    service_url: str = "http://localhost:8050"
    timeout: float = 30.0
    app_id: str | None = "test-app"
    team: str | None = "test-team"

    # Oracle credentials
    oracle_user: str | None = None
    oracle_password: str | None = None

    # Snowflake credentials
    snowflake_user: str | None = None
    snowflake_password: str | None = None
    snowflake_private_key_path: str | None = None

    # REST credentials
    credentials: dict[str, Any] = field(default_factory=dict)

    # Retry configuration
    retry_max_attempts: int = 3
    retry_backoff_factor: float = 0.5
    retry_status_codes: tuple[int, ...] = (502, 503, 504)

    # Other settings
    cache_ttl: float = 60.0
    report_telemetry: bool = False

    def get_credential(self, source_type: str, key: str) -> str | None:
        """Get a credential for a source type."""
        if source_type == "oracle":
            if key == "user":
                return self.oracle_user
            if key == "password":
                return self.oracle_password
        elif source_type == "snowflake":
            if key == "user":
                return self.snowflake_user
            if key == "password":
                return self.snowflake_password
            if key == "private_key_path":
                return self.snowflake_private_key_path
        return self.credentials.get(f"{source_type}_{key}")


@pytest.fixture
def mock_resolved_source():
    """Factory fixture for creating MockResolvedSource instances."""
    def _factory(**kwargs) -> MockResolvedSource:
        return MockResolvedSource(**kwargs)
    return _factory


@pytest.fixture
def mock_config():
    """Factory fixture for creating MockClientConfig instances."""
    def _factory(**kwargs) -> MockClientConfig:
        return MockClientConfig(**kwargs)
    return _factory


@pytest.fixture
def oracle_resolved_source():
    """Pre-configured ResolvedSource for Oracle tests."""
    return MockResolvedSource(
        moniker="test/oracle/data",
        path="test/oracle/data",
        source_type="oracle",
        connection={
            "host": "oracle.example.com",
            "port": 1521,
            "service_name": "ORCL",
        },
        query="SELECT * FROM employees",
        params={},
    )


@pytest.fixture
def oracle_config():
    """Pre-configured ClientConfig for Oracle tests."""
    return MockClientConfig(
        oracle_user="test_user",
        oracle_password="test_password",
    )


@pytest.fixture
def rest_resolved_source():
    """Pre-configured ResolvedSource for REST tests."""
    return MockResolvedSource(
        moniker="test/rest/api",
        path="test/rest/api",
        source_type="rest",
        connection={
            "base_url": "https://api.example.com",
            "auth_type": "bearer",
        },
        query="/v1/data",
        params={},
    )


@pytest.fixture
def rest_config():
    """Pre-configured ClientConfig for REST tests."""
    return MockClientConfig(
        credentials={
            "rest_bearer_token": "test-token-123",
        },
    )


@pytest.fixture
def mock_httpx_response():
    """Factory fixture for creating mock httpx responses."""
    def _factory(
        status_code: int = 200,
        json_data: Any = None,
        headers: dict | None = None,
    ):
        response = MagicMock()
        response.status_code = status_code
        response.json.return_value = json_data or {}
        response.headers = headers or {}
        response.raise_for_status = MagicMock()
        if status_code >= 400:
            from httpx import HTTPStatusError
            response.raise_for_status.side_effect = HTTPStatusError(
                f"HTTP {status_code}",
                request=MagicMock(),
                response=response,
            )
        return response
    return _factory


@pytest.fixture
def mock_oracle_cursor():
    """Factory fixture for creating mock Oracle cursor."""
    def _factory(
        columns: list[str] | None = None,
        rows: list[tuple] | None = None,
    ):
        cursor = MagicMock()
        cursor.description = [(col, None, None, None, None, None, None) for col in (columns or [])]
        cursor.fetchall.return_value = rows or []
        return cursor
    return _factory


@pytest.fixture
def mock_oracle_connection(mock_oracle_cursor):
    """Factory fixture for creating mock Oracle connection."""
    def _factory(
        columns: list[str] | None = None,
        rows: list[tuple] | None = None,
    ):
        conn = MagicMock()
        cursor = mock_oracle_cursor(columns=columns, rows=rows)
        conn.cursor.return_value = cursor
        conn.ping.return_value = None
        return conn
    return _factory
