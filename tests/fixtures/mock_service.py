"""
Mock Moniker Service for testing without real service dependency.

Provides httpx MockTransport for simulating service responses in pytest.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Callable
from unittest.mock import patch

import httpx


@dataclass
class MockMonikerService:
    """
    Mock the moniker service HTTP layer.

    Usage in tests:
        @pytest.fixture
        def mock_service():
            svc = MockMonikerService()
            svc.add_resolution("test/data", {
                "moniker": "moniker://test/data",
                "path": "test/data",
                "source_type": "static",
                ...
            })
            return svc

        def test_read_flow(mock_service):
            with mock_service.patch_httpx():
                client = MonikerClient(config=ClientConfig(
                    service_url="http://mock"
                ))
                # Now all httpx calls go through MockMonikerService
    """

    resolutions: dict[str, dict] = field(default_factory=dict)
    descriptions: dict[str, dict] = field(default_factory=dict)
    trees: dict[str, dict] = field(default_factory=dict)
    metadata_store: dict[str, dict] = field(default_factory=dict)
    samples: dict[str, dict] = field(default_factory=dict)
    fetches: dict[str, dict] = field(default_factory=dict)
    lineages: dict[str, dict] = field(default_factory=dict)
    children: dict[str, list[str]] = field(default_factory=dict)

    # For tracking calls
    call_log: list[tuple[str, str, dict | None]] = field(default_factory=list)

    # Custom handlers for advanced testing
    custom_handlers: dict[str, Callable[[httpx.Request], httpx.Response]] = field(
        default_factory=dict
    )

    def add_resolution(self, path: str, resolved_data: dict) -> None:
        """Register a mock resolution for /resolve/{path}."""
        self.resolutions[path] = resolved_data

    def add_description(self, path: str, desc_data: dict) -> None:
        """Register mock description data for /describe/{path}."""
        self.descriptions[path] = desc_data

    def add_tree(self, path: str, tree_data: dict) -> None:
        """Register mock tree data for /tree/{path}."""
        self.trees[path] = tree_data

    def add_metadata(self, path: str, metadata: dict) -> None:
        """Register mock metadata for /metadata/{path}."""
        self.metadata_store[path] = metadata

    def add_sample(self, path: str, sample_data: dict) -> None:
        """Register mock sample data for /sample/{path}."""
        self.samples[path] = sample_data

    def add_fetch(self, path: str, fetch_data: dict) -> None:
        """Register mock fetch data for /fetch/{path}."""
        self.fetches[path] = fetch_data

    def add_lineage(self, path: str, lineage_data: dict) -> None:
        """Register mock lineage data for /lineage/{path}."""
        self.lineages[path] = lineage_data

    def add_children(self, path: str, child_list: list[str]) -> None:
        """Register mock children for /list/{path}."""
        self.children[path] = child_list

    def add_custom_handler(
        self, pattern: str, handler: Callable[[httpx.Request], httpx.Response]
    ) -> None:
        """Add a custom handler for a URL pattern (regex)."""
        self.custom_handlers[pattern] = handler

    def _handle_request(self, request: httpx.Request) -> httpx.Response:
        """Route request to appropriate handler."""
        path = str(request.url.path)
        method = request.method
        params = dict(request.url.params) if request.url.params else None

        self.call_log.append((method, str(request.url), params))

        # Check custom handlers first
        for pattern, handler in self.custom_handlers.items():
            if re.match(pattern, path):
                return handler(request)

        # /resolve/{path}
        if match := re.match(r"/resolve/(.+)", path):
            moniker_path = match.group(1)
            if moniker_path in self.resolutions:
                return httpx.Response(200, json=self.resolutions[moniker_path])
            return httpx.Response(404, json={"detail": f"Not found: {moniker_path}"})

        # /describe/{path}
        if match := re.match(r"/describe/(.+)", path):
            moniker_path = match.group(1)
            if moniker_path in self.descriptions:
                return httpx.Response(200, json=self.descriptions[moniker_path])
            return httpx.Response(404, json={"detail": f"Not found: {moniker_path}"})

        # /list/{path} or /list (root)
        if match := re.match(r"/list(?:/(.*))?", path):
            moniker_path = match.group(1) or ""
            if moniker_path in self.children:
                return httpx.Response(200, json={"children": self.children[moniker_path]})
            return httpx.Response(200, json={"children": []})

        # /lineage/{path}
        if match := re.match(r"/lineage/(.+)", path):
            moniker_path = match.group(1)
            if moniker_path in self.lineages:
                return httpx.Response(200, json=self.lineages[moniker_path])
            return httpx.Response(404, json={"detail": f"Not found: {moniker_path}"})

        # /fetch/{path}
        if match := re.match(r"/fetch/(.+)", path):
            moniker_path = match.group(1)
            if moniker_path in self.fetches:
                return httpx.Response(200, json=self.fetches[moniker_path])
            return httpx.Response(404, json={"detail": f"Not found: {moniker_path}"})

        # /metadata/{path}
        if match := re.match(r"/metadata/(.+)", path):
            moniker_path = match.group(1)
            if moniker_path in self.metadata_store:
                return httpx.Response(200, json=self.metadata_store[moniker_path])
            return httpx.Response(404, json={"detail": f"Not found: {moniker_path}"})

        # /sample/{path}
        if match := re.match(r"/sample/(.+)", path):
            moniker_path = match.group(1)
            if moniker_path in self.samples:
                return httpx.Response(200, json=self.samples[moniker_path])
            return httpx.Response(404, json={"detail": f"Not found: {moniker_path}"})

        # /tree/{path} or /tree (root)
        if match := re.match(r"/tree(?:/(.*))?", path):
            moniker_path = match.group(1) or ""
            if moniker_path in self.trees:
                return httpx.Response(200, json=self.trees[moniker_path])
            # Return empty tree for unknown paths
            return httpx.Response(
                200,
                json={
                    "path": moniker_path,
                    "name": moniker_path.split("/")[-1] if moniker_path else "root",
                    "children": [],
                },
            )

        # /telemetry/access - always accept
        if path == "/telemetry/access":
            return httpx.Response(200, json={"status": "ok"})

        # Default 404
        return httpx.Response(404, json={"detail": f"Unknown endpoint: {path}"})

    def get_transport(self) -> httpx.MockTransport:
        """Get httpx MockTransport for use with httpx.Client."""
        return httpx.MockTransport(self._handle_request)

    def patch_httpx(self):
        """
        Context manager to patch httpx.Client to use mock transport.

        Usage:
            with mock_service.patch_httpx():
                client = MonikerClient(...)
                result = client.read("path")
        """
        transport = self.get_transport()

        original_init = httpx.Client.__init__

        def patched_init(self_client, *args, **kwargs):
            kwargs["transport"] = transport
            # Remove base_url conflict if any
            original_init(self_client, *args, **kwargs)

        return patch.object(httpx.Client, "__init__", patched_init)

    def get_calls(self, endpoint: str | None = None) -> list[tuple[str, str, dict | None]]:
        """
        Get logged calls, optionally filtered by endpoint.

        Args:
            endpoint: Optional endpoint pattern to filter (e.g., "/resolve")

        Returns:
            List of (method, url, params) tuples
        """
        if endpoint is None:
            return self.call_log
        return [(m, u, p) for m, u, p in self.call_log if endpoint in u]

    def clear_calls(self) -> None:
        """Clear the call log."""
        self.call_log.clear()


# =============================================================================
# Convenience factory functions for common test scenarios
# =============================================================================


def create_mock_service_for_static() -> MockMonikerService:
    """Create mock service configured for static adapter tests."""
    svc = MockMonikerService()
    svc.add_resolution(
        "test/static/json",
        {
            "moniker": "moniker://test/static/json",
            "path": "test/static/json",
            "source_type": "static",
            "connection": {"base_path": "/tmp/test"},
            "query": "data.json",
            "params": {"format": "json"},
            "schema_info": None,
            "read_only": True,
            "ownership": {"adop": "test-team"},
            "binding_path": "test/static",
            "sub_path": "json",
        },
    )
    return svc


def create_mock_service_for_oracle() -> MockMonikerService:
    """Create mock service configured for Oracle adapter tests."""
    svc = MockMonikerService()
    svc.add_resolution(
        "risk/cvar/758/A",
        {
            "moniker": "moniker://risk/cvar/758/A",
            "path": "risk/cvar/758/A",
            "source_type": "oracle",
            "connection": {
                "host": "oracle.example.com",
                "port": 1521,
                "service_name": "ORCL",
            },
            "query": "SELECT * FROM cvar_data WHERE port_no = '758' AND port_type = 'A'",
            "params": {},
            "schema_info": None,
            "read_only": True,
            "ownership": {"adop": "risk-team"},
            "binding_path": "risk/cvar",
            "sub_path": "758/A",
        },
    )
    svc.add_description(
        "risk/cvar/758/A",
        {
            "path": "risk/cvar/758/A",
            "display_name": "CVaR Portfolio 758 Type A",
            "ownership": {"adop": "risk-team"},
            "source_type": "oracle",
        },
    )
    return svc


def create_mock_service_for_snowflake() -> MockMonikerService:
    """Create mock service configured for Snowflake adapter tests."""
    svc = MockMonikerService()
    svc.add_resolution(
        "rates/treasury/yields",
        {
            "moniker": "moniker://rates/treasury/yields",
            "path": "rates/treasury/yields",
            "source_type": "snowflake",
            "connection": {
                "account": "myaccount",
                "warehouse": "COMPUTE_WH",
                "database": "MARKET_DATA",
                "schema": "RATES",
            },
            "query": "SELECT * FROM treasury_yields",
            "params": {},
            "schema_info": None,
            "read_only": True,
            "ownership": {"adop": "rates-team"},
            "binding_path": "rates/treasury",
            "sub_path": "yields",
        },
    )
    return svc


def create_mock_service_for_integration() -> MockMonikerService:
    """
    Create a fully-featured mock service for integration tests.

    Includes resolutions, descriptions, trees, and sample data.
    """
    svc = MockMonikerService()

    # Add static source resolution
    svc.add_resolution(
        "test/data",
        {
            "moniker": "moniker://test/data",
            "path": "test/data",
            "source_type": "static",
            "connection": {"base_path": "/tmp"},
            "query": "test.json",
            "params": {},
            "schema_info": None,
            "read_only": True,
            "ownership": {"adop": "test-team"},
            "binding_path": "test",
            "sub_path": "data",
        },
    )

    # Add description
    svc.add_description(
        "test/data",
        {
            "path": "test/data",
            "display_name": "Test Data",
            "description": "Sample test dataset",
            "ownership": {"adop": "test-team"},
            "source_type": "static",
        },
    )

    # Add metadata
    svc.add_metadata(
        "test/data",
        {
            "moniker": "moniker://test/data",
            "path": "test/data",
            "display_name": "Test Data",
            "description": "Sample test dataset for integration testing",
            "semantic_tags": ["test", "sample"],
            "ownership": {"adop": "test-team"},
            "schema": {
                "columns": [
                    {"name": "id", "type": "integer"},
                    {"name": "value", "type": "string"},
                ]
            },
        },
    )

    # Add sample
    svc.add_sample(
        "test/data",
        {
            "moniker": "moniker://test/data",
            "path": "test/data",
            "source_type": "static",
            "row_count": 2,
            "columns": ["id", "value"],
            "data": [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}],
        },
    )

    # Add fetch result
    svc.add_fetch(
        "test/data",
        {
            "moniker": "moniker://test/data",
            "path": "test/data",
            "source_type": "static",
            "row_count": 3,
            "columns": ["id", "value"],
            "data": [
                {"id": 1, "value": "a"},
                {"id": 2, "value": "b"},
                {"id": 3, "value": "c"},
            ],
            "truncated": False,
            "execution_time_ms": 5.2,
        },
    )

    # Add tree
    svc.add_tree(
        "test",
        {
            "path": "test",
            "name": "test",
            "children": [
                {
                    "path": "test/data",
                    "name": "data",
                    "children": [],
                    "source_type": "static",
                    "has_source_binding": True,
                }
            ],
            "ownership": {"adop": "test-team"},
        },
    )

    # Add children
    svc.add_children("test", ["data"])
    svc.add_children("", ["test"])

    return svc
