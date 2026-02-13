"""
Integration tests for MonikerClient using MockMonikerService.

Tests the full client flow: resolution -> adapter -> data
"""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import httpx
import pytest

from moniker_client import MonikerClient, Moniker
from moniker_client.client import (
    NotFoundError,
    FetchResult,
    MetadataResult,
    SampleResult,
    TreeNode,
    ResolvedSource,
)
from moniker_client.config import ClientConfig

from tests.fixtures.mock_service import (
    MockMonikerService,
    create_mock_service_for_integration,
    create_mock_service_for_oracle,
    create_mock_service_for_snowflake,
)


class TestMonikerClientDescribe:
    """Test client describe() endpoint."""

    def test_describe_returns_metadata(self):
        """Test describe returns path metadata."""
        mock_svc = create_mock_service_for_integration()

        with mock_svc.patch_httpx():
            client = MonikerClient(config=ClientConfig(service_url="http://mock"))
            result = client.describe("test/data")

        assert result["path"] == "test/data"
        assert result["display_name"] == "Test Data"
        assert result["ownership"]["adop"] == "test-team"

    def test_describe_not_found_raises(self):
        """Test describe raises NotFoundError for unknown path."""
        mock_svc = MockMonikerService()

        with mock_svc.patch_httpx():
            client = MonikerClient(config=ClientConfig(service_url="http://mock"))
            with pytest.raises(NotFoundError):
                client.describe("nonexistent/path")


class TestMonikerClientFetch:
    """Test client fetch() endpoint (server-side execution)."""

    def test_fetch_returns_fetch_result(self):
        """Test fetch returns FetchResult with data."""
        mock_svc = create_mock_service_for_integration()

        with mock_svc.patch_httpx():
            client = MonikerClient(config=ClientConfig(service_url="http://mock"))
            result = client.fetch("test/data")

        assert isinstance(result, FetchResult)
        assert result.path == "test/data"
        assert result.row_count == 3
        assert len(result.data) == 3
        assert result.columns == ["id", "value"]

    def test_fetch_not_found_raises(self):
        """Test fetch raises NotFoundError for unknown path."""
        mock_svc = MockMonikerService()

        with mock_svc.patch_httpx():
            client = MonikerClient(config=ClientConfig(service_url="http://mock"))
            with pytest.raises(NotFoundError):
                client.fetch("nonexistent/path")


class TestMonikerClientMetadata:
    """Test client metadata() endpoint."""

    def test_metadata_returns_metadata_result(self):
        """Test metadata returns MetadataResult."""
        mock_svc = create_mock_service_for_integration()

        with mock_svc.patch_httpx():
            client = MonikerClient(config=ClientConfig(service_url="http://mock"))
            result = client.metadata("test/data")

        assert isinstance(result, MetadataResult)
        assert result.path == "test/data"
        assert result.display_name == "Test Data"
        assert "test" in result.semantic_tags

    def test_metadata_not_found_raises(self):
        """Test metadata raises NotFoundError for unknown path."""
        mock_svc = MockMonikerService()

        with mock_svc.patch_httpx():
            client = MonikerClient(config=ClientConfig(service_url="http://mock"))
            with pytest.raises(NotFoundError):
                client.metadata("nonexistent/path")


class TestMonikerClientSample:
    """Test client sample() endpoint."""

    def test_sample_returns_sample_result(self):
        """Test sample returns SampleResult."""
        mock_svc = create_mock_service_for_integration()

        with mock_svc.patch_httpx():
            client = MonikerClient(config=ClientConfig(service_url="http://mock"))
            result = client.sample("test/data", limit=5)

        assert isinstance(result, SampleResult)
        assert result.path == "test/data"
        assert result.row_count == 2

    def test_sample_not_found_raises(self):
        """Test sample raises NotFoundError for unknown path."""
        mock_svc = MockMonikerService()

        with mock_svc.patch_httpx():
            client = MonikerClient(config=ClientConfig(service_url="http://mock"))
            with pytest.raises(NotFoundError):
                client.sample("nonexistent/path")


class TestMonikerClientTree:
    """Test client tree() endpoint."""

    def test_tree_returns_tree_node(self):
        """Test tree returns TreeNode."""
        mock_svc = create_mock_service_for_integration()

        with mock_svc.patch_httpx():
            client = MonikerClient(config=ClientConfig(service_url="http://mock"))
            result = client.tree("test")

        assert isinstance(result, TreeNode)
        assert result.path == "test"
        assert len(result.children) == 1
        assert result.children[0].name == "data"

    def test_tree_unknown_path_returns_empty(self):
        """Test tree returns empty node for unknown path."""
        mock_svc = MockMonikerService()

        with mock_svc.patch_httpx():
            client = MonikerClient(config=ClientConfig(service_url="http://mock"))
            result = client.tree("unknown/path")

        assert isinstance(result, TreeNode)
        assert result.children == []


class TestMonikerClientListChildren:
    """Test client list_children() endpoint."""

    def test_list_children_returns_list(self):
        """Test list_children returns child paths."""
        mock_svc = create_mock_service_for_integration()

        with mock_svc.patch_httpx():
            client = MonikerClient(config=ClientConfig(service_url="http://mock"))
            result = client.list_children("test")

        assert result == ["data"]

    def test_list_children_empty_for_unknown(self):
        """Test list_children returns empty for unknown path."""
        mock_svc = MockMonikerService()

        with mock_svc.patch_httpx():
            client = MonikerClient(config=ClientConfig(service_url="http://mock"))
            result = client.list_children("unknown")

        assert result == []


class TestMonikerClientResolve:
    """Test client resolve() endpoint."""

    def test_resolve_returns_resolved_source(self):
        """Test resolve returns ResolvedSource."""
        mock_svc = create_mock_service_for_integration()

        with mock_svc.patch_httpx():
            client = MonikerClient(config=ClientConfig(service_url="http://mock"))
            result = client.resolve("test/data")

        assert isinstance(result, ResolvedSource)
        assert result.path == "test/data"
        assert result.source_type == "static"

    def test_resolve_not_found_raises(self):
        """Test resolve raises NotFoundError for unknown path."""
        mock_svc = MockMonikerService()

        with mock_svc.patch_httpx():
            client = MonikerClient(config=ClientConfig(service_url="http://mock"))
            with pytest.raises(NotFoundError):
                client.resolve("nonexistent/path")


class TestResolutionCaching:
    """Test resolution caching behavior."""

    def test_resolution_is_cached(self):
        """Test resolutions are cached."""
        mock_svc = create_mock_service_for_integration()

        with mock_svc.patch_httpx():
            client = MonikerClient(
                config=ClientConfig(service_url="http://mock", cache_ttl=60)
            )
            # First resolve
            client.resolve("test/data")
            # Second resolve should use cache
            client.resolve("test/data")

        # Should only have one resolve call
        resolve_calls = mock_svc.get_calls("/resolve")
        assert len(resolve_calls) == 1

    def test_cache_disabled_when_ttl_zero(self):
        """Test cache is disabled when cache_ttl=0."""
        mock_svc = create_mock_service_for_integration()

        with mock_svc.patch_httpx():
            client = MonikerClient(
                config=ClientConfig(service_url="http://mock", cache_ttl=0)
            )
            # First resolve
            client.resolve("test/data")
            # Second resolve should NOT use cache
            client.resolve("test/data")

        # Should have two resolve calls
        resolve_calls = mock_svc.get_calls("/resolve")
        assert len(resolve_calls) == 2


class TestTelemetryReporting:
    """Test telemetry reporting."""

    def test_telemetry_reported_when_enabled(self):
        """Test telemetry is reported when enabled."""
        mock_svc = create_mock_service_for_integration()

        # Mock adapter to avoid real data source
        mock_adapter = MagicMock()
        mock_adapter.fetch.return_value = [{"id": 1}]

        with mock_svc.patch_httpx():
            # Patch at the point where it's used in client.py
            with patch("moniker_client.client.get_adapter", return_value=mock_adapter):
                client = MonikerClient(
                    config=ClientConfig(
                        service_url="http://mock",
                        report_telemetry=True,
                    )
                )
                client.read("test/data")

        # Should have telemetry call
        telemetry_calls = mock_svc.get_calls("/telemetry")
        assert len(telemetry_calls) == 1

    def test_telemetry_not_reported_when_disabled(self):
        """Test telemetry is NOT reported when disabled."""
        mock_svc = create_mock_service_for_integration()

        mock_adapter = MagicMock()
        mock_adapter.fetch.return_value = [{"id": 1}]

        with mock_svc.patch_httpx():
            # Patch at the point where it's used in client.py
            with patch("moniker_client.client.get_adapter", return_value=mock_adapter):
                client = MonikerClient(
                    config=ClientConfig(
                        service_url="http://mock",
                        report_telemetry=False,
                    )
                )
                client.read("test/data")

        # Should NOT have telemetry call
        telemetry_calls = mock_svc.get_calls("/telemetry")
        assert len(telemetry_calls) == 0


class TestMonikerFluentAPI:
    """Test Moniker fluent API."""

    def test_moniker_path_normalization(self):
        """Test Moniker normalizes paths."""
        m1 = Moniker("test/data")
        m2 = Moniker("moniker://test/data")
        m3 = Moniker("/test/data/")

        assert m1.path == "test/data"
        assert m2.path == "test/data"
        assert m3.path == "test/data"

    def test_moniker_uri_property(self):
        """Test Moniker uri property."""
        m = Moniker("test/data")
        assert m.uri == "moniker://test/data"

    def test_moniker_child_navigation(self):
        """Test Moniker child navigation."""
        m = Moniker("test")
        child = m / "data"
        assert child.path == "test/data"

    def test_moniker_parent_navigation(self):
        """Test Moniker parent navigation."""
        m = Moniker("test/data/subpath")
        parent = m.parent()
        assert parent is not None
        assert parent.path == "test/data"

    def test_moniker_parent_at_root_returns_none(self):
        """Test Moniker parent at root returns None."""
        m = Moniker("test")
        parent = m.parent()
        assert parent is None

    def test_moniker_describe_delegates_to_client(self):
        """Test Moniker.describe() delegates to client."""
        mock_svc = create_mock_service_for_integration()

        with mock_svc.patch_httpx():
            client = MonikerClient(config=ClientConfig(service_url="http://mock"))
            m = Moniker("test/data", client=client)
            result = m.describe()

        assert result["path"] == "test/data"

    def test_moniker_fetch_delegates_to_client(self):
        """Test Moniker.fetch() delegates to client."""
        mock_svc = create_mock_service_for_integration()

        with mock_svc.patch_httpx():
            client = MonikerClient(config=ClientConfig(service_url="http://mock"))
            m = Moniker("test/data", client=client)
            result = m.fetch()

        assert isinstance(result, FetchResult)


class TestClientHeaders:
    """Test client sends correct headers."""

    def test_client_sends_app_id_header(self):
        """Test client sends X-App-Id header."""
        mock_svc = create_mock_service_for_integration()

        # Track headers from requests
        headers_received = []

        def capture_headers(request):
            headers_received.append(dict(request.headers))
            # Return a valid response directly instead of calling _handle_request
            return httpx.Response(
                200,
                json={
                    "path": "test/data",
                    "display_name": "Test Data",
                    "ownership": {"adop": "test-team"},
                },
            )

        mock_svc.add_custom_handler(r"/describe/.*", capture_headers)

        with mock_svc.patch_httpx():
            client = MonikerClient(
                config=ClientConfig(
                    service_url="http://mock",
                    app_id="test-app",
                    team="test-team",
                )
            )
            client.describe("test/data")

        assert len(headers_received) > 0
        # Check headers contain app_id and team
        headers = headers_received[0]
        assert headers.get("x-app-id") == "test-app"
        assert headers.get("x-team") == "test-team"
