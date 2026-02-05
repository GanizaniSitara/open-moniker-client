"""Unit tests for the REST adapter."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from moniker_client.adapters.rest import RestAdapter


class TestRestAdapterURLConstruction:
    """Tests for REST URL construction."""

    def test_url_construction_basic(self, mock_resolved_source, mock_config):
        """Test basic URL construction."""
        adapter = RestAdapter()
        resolved = mock_resolved_source(
            source_type="rest",
            connection={"base_url": "https://api.example.com"},
            query="/v1/data",
        )

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"result": "ok"}

        with patch("httpx.Client") as mock_client:
            mock_instance = MagicMock()
            mock_instance.request.return_value = mock_response
            mock_client.return_value.__enter__.return_value = mock_instance
            adapter.fetch(resolved, mock_config())

        call_args = mock_instance.request.call_args
        # Positional args: method, url
        assert call_args[0][1] == "https://api.example.com/v1/data"

    def test_url_construction_with_trailing_slash(self, mock_resolved_source, mock_config):
        """Test URL construction handles trailing slashes."""
        adapter = RestAdapter()
        resolved = mock_resolved_source(
            source_type="rest",
            connection={"base_url": "https://api.example.com/"},
            query="v1/data",
        )

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}

        with patch("httpx.Client") as mock_client:
            mock_instance = MagicMock()
            mock_instance.request.return_value = mock_response
            mock_client.return_value.__enter__.return_value = mock_instance
            adapter.fetch(resolved, mock_config())

        call_args = mock_instance.request.call_args
        # Positional args: method, url
        assert "api.example.com" in call_args[0][1]

    def test_url_construction_no_query(self, mock_resolved_source, mock_config):
        """Test URL construction with no query path."""
        adapter = RestAdapter()
        resolved = mock_resolved_source(
            source_type="rest",
            connection={"base_url": "https://api.example.com"},
            query=None,
        )

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}

        with patch("httpx.Client") as mock_client:
            mock_instance = MagicMock()
            mock_instance.request.return_value = mock_response
            mock_client.return_value.__enter__.return_value = mock_instance
            adapter.fetch(resolved, mock_config())

        call_args = mock_instance.request.call_args
        # Positional args: method, url
        assert call_args[0][1] == "https://api.example.com"

    def test_missing_base_url_raises(self, mock_resolved_source, mock_config):
        """Test missing base_url raises ValueError."""
        adapter = RestAdapter()
        resolved = mock_resolved_source(
            source_type="rest",
            connection={},
            query="/v1/data",
        )

        with pytest.raises(ValueError, match="base_url required"):
            adapter.fetch(resolved, mock_config())


class TestRestAdapterQueryParams:
    """Tests for REST query params handling."""

    def test_query_params_from_query_params(self, mock_resolved_source, mock_config):
        """Test query params from query_params field."""
        adapter = RestAdapter()
        resolved = mock_resolved_source(
            source_type="rest",
            connection={"base_url": "https://api.example.com"},
            query="/v1/data",
            params={"query_params": {"foo": "bar", "baz": 123}},
        )

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}

        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.request.return_value = mock_response
            adapter.fetch(resolved, mock_config())

        call_args = mock_client.return_value.__enter__.return_value.request.call_args
        assert call_args[1]["params"] == {"foo": "bar", "baz": 123}

    def test_query_params_from_moniker_params_fallback(self, mock_resolved_source, mock_config):
        """Test query params fallback to moniker_params."""
        adapter = RestAdapter()
        resolved = mock_resolved_source(
            source_type="rest",
            connection={"base_url": "https://api.example.com"},
            query="/v1/data",
            params={"moniker_params": {"legacy": "param"}},
        )

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}

        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.request.return_value = mock_response
            adapter.fetch(resolved, mock_config())

        call_args = mock_client.return_value.__enter__.return_value.request.call_args
        assert call_args[1]["params"] == {"legacy": "param"}

    def test_query_params_merge_precedence(self, mock_resolved_source, mock_config):
        """Test query_params takes precedence over moniker_params."""
        adapter = RestAdapter()
        resolved = mock_resolved_source(
            source_type="rest",
            connection={"base_url": "https://api.example.com"},
            query="/v1/data",
            params={
                "moniker_params": {"key": "old_value", "legacy": "param"},
                "query_params": {"key": "new_value", "new": "param"},
            },
        )

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}

        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.request.return_value = mock_response
            adapter.fetch(resolved, mock_config())

        call_args = mock_client.return_value.__enter__.return_value.request.call_args
        params = call_args[1]["params"]
        assert params["key"] == "new_value"  # query_params wins
        assert params["legacy"] == "param"  # moniker_params preserved
        assert params["new"] == "param"  # query_params included


class TestRestAdapterAuth:
    """Tests for REST authentication."""

    def test_bearer_auth_from_config(self, mock_resolved_source, mock_config):
        """Test Bearer auth from config credentials."""
        adapter = RestAdapter()
        resolved = mock_resolved_source(
            source_type="rest",
            connection={
                "base_url": "https://api.example.com",
                "auth_type": "bearer",
            },
            query="/v1/data",
        )
        config = mock_config(credentials={"rest_bearer_token": "secret-token"})

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}

        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.request.return_value = mock_response
            adapter.fetch(resolved, config)

        call_args = mock_client.return_value.__enter__.return_value.request.call_args
        assert call_args[1]["headers"]["Authorization"] == "Bearer secret-token"

    def test_bearer_auth_from_params(self, mock_resolved_source, mock_config):
        """Test Bearer auth from params (takes precedence)."""
        adapter = RestAdapter()
        resolved = mock_resolved_source(
            source_type="rest",
            connection={
                "base_url": "https://api.example.com",
                "auth_type": "bearer",
            },
            query="/v1/data",
            params={"bearer_token": "param-token"},
        )
        config = mock_config(credentials={"rest_bearer_token": "config-token"})

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}

        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.request.return_value = mock_response
            adapter.fetch(resolved, config)

        call_args = mock_client.return_value.__enter__.return_value.request.call_args
        assert call_args[1]["headers"]["Authorization"] == "Bearer param-token"

    def test_api_key_auth(self, mock_resolved_source, mock_config):
        """Test API key auth."""
        adapter = RestAdapter()
        resolved = mock_resolved_source(
            source_type="rest",
            connection={
                "base_url": "https://api.example.com",
                "auth_type": "api_key",
                "api_key_header": "X-Custom-Key",
            },
            query="/v1/data",
        )
        config = mock_config(credentials={"rest_api_key": "my-api-key"})

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}

        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.request.return_value = mock_response
            adapter.fetch(resolved, config)

        call_args = mock_client.return_value.__enter__.return_value.request.call_args
        assert call_args[1]["headers"]["X-Custom-Key"] == "my-api-key"

    def test_basic_auth(self, mock_resolved_source, mock_config):
        """Test Basic auth."""
        import base64

        adapter = RestAdapter()
        resolved = mock_resolved_source(
            source_type="rest",
            connection={
                "base_url": "https://api.example.com",
                "auth_type": "basic",
            },
            query="/v1/data",
        )
        config = mock_config(credentials={
            "rest_username": "user",
            "rest_password": "pass",
        })

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}

        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.request.return_value = mock_response
            adapter.fetch(resolved, config)

        call_args = mock_client.return_value.__enter__.return_value.request.call_args
        expected = "Basic " + base64.b64encode(b"user:pass").decode()
        assert call_args[1]["headers"]["Authorization"] == expected


class TestRestAdapterRetry:
    """Tests for REST retry logic."""

    def test_retry_on_503(self, mock_resolved_source, mock_config):
        """Test retry on 503 status."""
        adapter = RestAdapter()
        resolved = mock_resolved_source(
            source_type="rest",
            connection={"base_url": "https://api.example.com"},
            query="/v1/data",
        )
        config = mock_config(retry_max_attempts=3, retry_backoff_factor=0.01)

        mock_response_503 = MagicMock()
        mock_response_503.status_code = 503

        mock_response_200 = MagicMock()
        mock_response_200.status_code = 200
        mock_response_200.json.return_value = {"success": True}

        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.request.side_effect = [
                mock_response_503,
                mock_response_200,
            ]
            result = adapter.fetch(resolved, config)

        assert result == {"success": True}
        assert mock_client.return_value.__enter__.return_value.request.call_count == 2

    def test_retry_on_timeout(self, mock_resolved_source, mock_config):
        """Test retry on timeout."""
        import httpx

        adapter = RestAdapter()
        resolved = mock_resolved_source(
            source_type="rest",
            connection={"base_url": "https://api.example.com"},
            query="/v1/data",
        )
        config = mock_config(retry_max_attempts=3, retry_backoff_factor=0.01)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"success": True}

        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.request.side_effect = [
                httpx.TimeoutException("timeout"),
                mock_response,
            ]
            result = adapter.fetch(resolved, config)

        assert result == {"success": True}

    def test_retry_exhausted_raises(self, mock_resolved_source, mock_config):
        """Test exception raised when retries exhausted."""
        import httpx

        adapter = RestAdapter()
        resolved = mock_resolved_source(
            source_type="rest",
            connection={"base_url": "https://api.example.com"},
            query="/v1/data",
        )
        config = mock_config(retry_max_attempts=2, retry_backoff_factor=0.01)

        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.request.side_effect = httpx.TimeoutException("timeout")
            with pytest.raises(TimeoutError, match="timed out after 2 attempts"):
                adapter.fetch(resolved, config)


class TestRestAdapterResponsePath:
    """Tests for response path extraction."""

    def test_extract_path_simple(self, mock_resolved_source, mock_config):
        """Test simple path extraction."""
        adapter = RestAdapter()
        resolved = mock_resolved_source(
            source_type="rest",
            connection={"base_url": "https://api.example.com"},
            query="/v1/data",
            params={"response_path": "data.results"},
        )

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "status": "ok",
            "data": {
                "results": [{"id": 1}, {"id": 2}],
            },
        }

        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.request.return_value = mock_response
            result = adapter.fetch(resolved, mock_config())

        assert result == [{"id": 1}, {"id": 2}]

    def test_extract_path_with_array_index(self, mock_resolved_source, mock_config):
        """Test path extraction with array index."""
        adapter = RestAdapter()
        resolved = mock_resolved_source(
            source_type="rest",
            connection={"base_url": "https://api.example.com"},
            query="/v1/data",
            params={"response_path": "items.0"},
        )

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "items": [{"first": True}, {"second": True}],
        }

        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.request.return_value = mock_response
            result = adapter.fetch(resolved, mock_config())

        assert result == {"first": True}

    def test_extract_path_not_found(self, mock_resolved_source, mock_config):
        """Test path extraction returns None when not found."""
        adapter = RestAdapter()
        data = {"a": {"b": 1}}
        assert adapter._extract_path(data, "a.c") is None
        assert adapter._extract_path(data, "x.y.z") is None


class TestRestAdapterListChildren:
    """Tests for REST list_children."""

    def test_list_children_from_endpoint(self, mock_resolved_source, mock_config):
        """Test list_children from configured endpoint."""
        adapter = RestAdapter()
        resolved = mock_resolved_source(
            source_type="rest",
            connection={
                "base_url": "https://api.example.com",
                "children_endpoint": "/v1/children",
            },
        )

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = ["child1", "child2", "child3"]

        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = mock_response
            result = adapter.list_children(resolved, mock_config())

        assert result == ["child1", "child2", "child3"]

    def test_list_children_from_dict_items(self, mock_resolved_source, mock_config):
        """Test list_children extracts names from dict items."""
        adapter = RestAdapter()
        resolved = mock_resolved_source(
            source_type="rest",
            connection={
                "base_url": "https://api.example.com",
                "children_endpoint": "/v1/children",
            },
        )

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"name": "item1", "id": 1},
            {"name": "item2", "id": 2},
        ]

        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = mock_response
            result = adapter.list_children(resolved, mock_config())

        assert result == ["item1", "item2"]

    def test_list_children_from_nested_response(self, mock_resolved_source, mock_config):
        """Test list_children from nested children key."""
        adapter = RestAdapter()
        resolved = mock_resolved_source(
            source_type="rest",
            connection={
                "base_url": "https://api.example.com",
                "children_endpoint": "/v1/children",
            },
        )

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "children": ["a", "b", "c"],
        }

        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = mock_response
            result = adapter.list_children(resolved, mock_config())

        assert result == ["a", "b", "c"]

    def test_list_children_no_endpoint_returns_empty(self, mock_resolved_source, mock_config):
        """Test list_children returns empty without endpoint."""
        adapter = RestAdapter()
        resolved = mock_resolved_source(
            source_type="rest",
            connection={"base_url": "https://api.example.com"},
        )

        result = adapter.list_children(resolved, mock_config())
        assert result == []

    def test_list_children_error_returns_empty(self, mock_resolved_source, mock_config):
        """Test list_children returns empty on error."""
        adapter = RestAdapter()
        resolved = mock_resolved_source(
            source_type="rest",
            connection={
                "base_url": "https://api.example.com",
                "children_endpoint": "/v1/children",
            },
        )

        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.side_effect = Exception("Network error")
            result = adapter.list_children(resolved, mock_config())

        assert result == []


class TestRestAdapterHealthCheck:
    """Tests for REST health check."""

    def test_health_check_success(self, mock_resolved_source, mock_config):
        """Test health check success."""
        adapter = RestAdapter()
        resolved = mock_resolved_source(
            source_type="rest",
            connection={
                "base_url": "https://api.example.com",
                "health_endpoint": "/health",
            },
        )

        mock_response = MagicMock()
        mock_response.status_code = 200

        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = mock_response
            result = adapter.health_check(resolved, mock_config())

        assert result["healthy"] is True
        assert "latency_ms" in result

    def test_health_check_failure_status(self, mock_resolved_source, mock_config):
        """Test health check failure on bad status."""
        adapter = RestAdapter()
        resolved = mock_resolved_source(
            source_type="rest",
            connection={"base_url": "https://api.example.com"},
        )

        mock_response = MagicMock()
        mock_response.status_code = 500

        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.return_value = mock_response
            result = adapter.health_check(resolved, mock_config())

        assert result["healthy"] is False
        assert "500" in result["message"]

    def test_health_check_no_base_url(self, mock_resolved_source, mock_config):
        """Test health check fails without base_url."""
        adapter = RestAdapter()
        resolved = mock_resolved_source(
            source_type="rest",
            connection={},
        )

        result = adapter.health_check(resolved, mock_config())

        assert result["healthy"] is False
        assert "base_url" in result["message"]

    def test_health_check_timeout(self, mock_resolved_source, mock_config):
        """Test health check handles timeout."""
        import httpx

        adapter = RestAdapter()
        resolved = mock_resolved_source(
            source_type="rest",
            connection={"base_url": "https://api.example.com"},
        )

        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.get.side_effect = httpx.TimeoutException("timeout")
            result = adapter.health_check(resolved, mock_config())

        assert result["healthy"] is False
        assert "timed out" in result["message"].lower()


class TestRestAdapterResult:
    """Tests for AdapterResult return."""

    def test_fetch_returns_adapter_result(self, mock_resolved_source, mock_config):
        """Test fetch returns AdapterResult when requested."""
        adapter = RestAdapter()
        resolved = mock_resolved_source(
            source_type="rest",
            connection={"base_url": "https://api.example.com"},
            query="/v1/data",
        )

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{"id": 1}, {"id": 2}]

        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.request.return_value = mock_response
            from moniker_client.adapters.base import AdapterResult
            result = adapter.fetch(resolved, mock_config(), return_result=True)

        assert isinstance(result, AdapterResult)
        assert result.data == [{"id": 1}, {"id": 2}]
        assert result.row_count == 2
        assert result.source_type == "rest"
        assert result.execution_time_ms is not None


class TestRestAdapterResponseValidation:
    """Tests for response validation."""

    def test_validate_response_success(self, mock_resolved_source, mock_config):
        """Test response validation with valid schema."""
        adapter = RestAdapter()
        resolved = mock_resolved_source(
            source_type="rest",
            connection={"base_url": "https://api.example.com"},
            query="/v1/data",
            params={
                "response_schema": {
                    "type": "array",
                    "items": {"type": "object"},
                },
            },
        )

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{"id": 1}]

        with patch("httpx.Client") as mock_client:
            mock_client.return_value.__enter__.return_value.request.return_value = mock_response
            # Should not raise
            result = adapter.fetch(resolved, mock_config())

        assert result == [{"id": 1}]

    def test_validate_response_failure(self, mock_resolved_source, mock_config):
        """Test response validation fails with invalid data."""
        adapter = RestAdapter()
        resolved = mock_resolved_source(
            source_type="rest",
            connection={"base_url": "https://api.example.com"},
            query="/v1/data",
            params={
                "response_schema": {
                    "type": "array",
                    "items": {"type": "object"},
                },
            },
        )

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = "not an array"

        # Only test if jsonschema is available
        try:
            import jsonschema  # noqa: F401
            with patch("httpx.Client") as mock_client:
                mock_client.return_value.__enter__.return_value.request.return_value = mock_response
                with pytest.raises(ValueError, match="validation failed"):
                    adapter.fetch(resolved, mock_config())
        except ImportError:
            pytest.skip("jsonschema not installed")
