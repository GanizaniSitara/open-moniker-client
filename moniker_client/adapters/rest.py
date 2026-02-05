"""REST API adapter - direct HTTP calls."""

from __future__ import annotations

import time
from typing import Any, TYPE_CHECKING
from urllib.parse import urljoin

if TYPE_CHECKING:
    from ..client import ResolvedSource
    from ..config import ClientConfig

from .base import BaseAdapter, AdapterResult


class RestAdapter(BaseAdapter):
    """
    Adapter for REST API sources.

    Makes HTTP calls directly to the configured endpoint.

    Features:
    - Query params alignment with service (query_params with moniker_params fallback)
    - list_children() support via children_endpoint
    - Retry logic for transient failures
    - Optional JSON schema validation
    - Health check endpoint support
    """

    def fetch(
        self,
        resolved: ResolvedSource,
        config: ClientConfig,
        **kwargs,
    ) -> Any:
        start_time = time.perf_counter()
        conn_info = resolved.connection
        params = resolved.params

        base_url = conn_info.get("base_url")
        if not base_url:
            raise ValueError("base_url required for REST source")

        # Build URL
        url_path = resolved.query or ""
        url = urljoin(base_url, url_path)

        # Method
        method = params.get("method", "GET").upper()

        # Headers
        headers = dict(conn_info.get("headers", {}))

        # Apply auth
        auth_type = conn_info.get("auth_type", "none")
        self._apply_auth(headers, auth_type, config, resolved)

        # Query params - use query_params with moniker_params as fallback for backwards compatibility
        query_params = params.get("query_params", {})
        moniker_params = params.get("moniker_params", {})

        # Merge: query_params takes precedence
        merged_params = {**moniker_params, **query_params}

        # Make request with retry logic
        data = self._request_with_retry(
            method=method,
            url=url,
            headers=headers,
            params=merged_params,
            config=config,
        )

        execution_time = (time.perf_counter() - start_time) * 1000

        # Extract nested data if response_path is set
        response_path = params.get("response_path")
        if response_path:
            data = self._extract_path(data, response_path)

        # Validate response if schema provided
        response_schema = params.get("response_schema")
        if response_schema:
            self._validate_response(data, response_schema)

        # Return AdapterResult if requested
        if kwargs.get("return_result"):
            row_count = len(data) if isinstance(data, list) else 1
            return AdapterResult(
                data=data,
                row_count=row_count,
                execution_time_ms=execution_time,
                source_type="rest",
            )

        return data

    def _request_with_retry(
        self,
        method: str,
        url: str,
        headers: dict,
        params: dict,
        config: ClientConfig,
    ) -> Any:
        """Make HTTP request with retry logic for transient failures."""
        import httpx

        max_attempts = config.retry_max_attempts
        backoff_factor = config.retry_backoff_factor
        retry_status_codes = config.retry_status_codes

        last_exception: Exception | None = None

        for attempt in range(max_attempts):
            try:
                with httpx.Client(timeout=config.timeout) as client:
                    response = client.request(
                        method,
                        url,
                        headers=headers,
                        params=params if params else None,
                    )

                    # Check if we should retry based on status code
                    if response.status_code in retry_status_codes and attempt < max_attempts - 1:
                        wait_time = backoff_factor * (2 ** attempt)
                        time.sleep(wait_time)
                        continue

                    if response.status_code == 404:
                        from ..client import NotFoundError
                        raise NotFoundError(f"Resource not found: {url}")

                    response.raise_for_status()
                    return response.json()

            except httpx.TimeoutException as e:
                last_exception = e
                if attempt < max_attempts - 1:
                    wait_time = backoff_factor * (2 ** attempt)
                    time.sleep(wait_time)
                    continue
                raise TimeoutError(f"Request to {url} timed out after {max_attempts} attempts") from e

            except httpx.ConnectError as e:
                last_exception = e
                if attempt < max_attempts - 1:
                    wait_time = backoff_factor * (2 ** attempt)
                    time.sleep(wait_time)
                    continue
                raise ConnectionError(f"Failed to connect to {url} after {max_attempts} attempts") from e

        # Should not reach here, but just in case
        if last_exception:
            raise last_exception
        raise RuntimeError("Unexpected retry loop exit")

    def _apply_auth(
        self,
        headers: dict,
        auth_type: str,
        config: ClientConfig,
        resolved: ResolvedSource,
    ) -> None:
        """Apply authentication to headers."""
        params = resolved.params

        if auth_type == "bearer":
            # Check params first, then config
            token = params.get("bearer_token") or config.credentials.get("rest_bearer_token")
            if token:
                headers["Authorization"] = f"Bearer {token}"

        elif auth_type == "api_key":
            key = params.get("api_key") or config.credentials.get("rest_api_key")
            header_name = resolved.connection.get("api_key_header", "X-API-Key")
            if key:
                headers[header_name] = key

        elif auth_type == "basic":
            import base64
            username = params.get("username") or config.credentials.get("rest_username", "")
            password = params.get("password") or config.credentials.get("rest_password", "")
            creds = base64.b64encode(f"{username}:{password}".encode()).decode()
            headers["Authorization"] = f"Basic {creds}"

    def _extract_path(self, data: Any, path: str) -> Any:
        """Extract nested data using dot notation."""
        for key in path.split("."):
            if isinstance(data, dict):
                data = data.get(key)
            elif isinstance(data, list) and key.isdigit():
                idx = int(key)
                data = data[idx] if 0 <= idx < len(data) else None
            else:
                return None
        return data

    def _validate_response(self, data: Any, schema: dict[str, Any]) -> None:
        """Validate response against JSON schema (if jsonschema is available)."""
        try:
            import jsonschema
            jsonschema.validate(data, schema)
        except ImportError:
            # jsonschema not installed, skip validation
            pass
        except Exception as e:
            raise ValueError(f"Response validation failed: {e}") from e

    def list_children(
        self,
        resolved: ResolvedSource,
        config: ClientConfig,
    ) -> list[str]:
        """
        List children from REST endpoint.

        Requires children_endpoint in connection config.
        """
        import httpx

        conn_info = resolved.connection
        children_endpoint = conn_info.get("children_endpoint")

        if not children_endpoint:
            return []

        base_url = conn_info.get("base_url")
        if not base_url:
            return []

        url = urljoin(base_url, children_endpoint)

        # Headers
        headers = dict(conn_info.get("headers", {}))

        # Apply auth
        auth_type = conn_info.get("auth_type", "none")
        self._apply_auth(headers, auth_type, config, resolved)

        try:
            with httpx.Client(timeout=config.timeout) as client:
                response = client.get(url, headers=headers)
                response.raise_for_status()
                data = response.json()

                # Handle different response formats
                if isinstance(data, list):
                    # Assume list of strings or dicts with 'name' key
                    children = []
                    for item in data:
                        if isinstance(item, str):
                            children.append(item)
                        elif isinstance(item, dict):
                            name = item.get("name") or item.get("id") or item.get("path")
                            if name:
                                children.append(str(name))
                    return children
                elif isinstance(data, dict):
                    # Look for children in common keys
                    for key in ["children", "items", "results", "data"]:
                        if key in data and isinstance(data[key], list):
                            return self._extract_children_names(data[key])
                return []

        except Exception:
            return []

    def _extract_children_names(self, items: list) -> list[str]:
        """Extract names from list of items."""
        children = []
        for item in items:
            if isinstance(item, str):
                children.append(item)
            elif isinstance(item, dict):
                name = item.get("name") or item.get("id") or item.get("path")
                if name:
                    children.append(str(name))
        return children

    def health_check(
        self,
        resolved: ResolvedSource,
        config: ClientConfig,
    ) -> dict[str, Any]:
        """
        Check REST endpoint health.

        Uses health_endpoint from connection config, or falls back to base_url.
        """
        import httpx

        conn_info = resolved.connection
        base_url = conn_info.get("base_url")

        if not base_url:
            return {
                "healthy": False,
                "message": "base_url not configured",
            }

        # Use specific health endpoint if configured
        health_endpoint = conn_info.get("health_endpoint", "")
        url = urljoin(base_url, health_endpoint)

        # Headers
        headers = dict(conn_info.get("headers", {}))

        # Apply auth
        auth_type = conn_info.get("auth_type", "none")
        self._apply_auth(headers, auth_type, config, resolved)

        start = time.perf_counter()
        try:
            with httpx.Client(timeout=min(config.timeout, 10)) as client:
                response = client.get(url, headers=headers)
                latency = (time.perf_counter() - start) * 1000

                if response.status_code < 400:
                    return {
                        "healthy": True,
                        "message": f"OK (status {response.status_code})",
                        "latency_ms": latency,
                        "details": {"url": url},
                    }
                else:
                    return {
                        "healthy": False,
                        "message": f"Unhealthy (status {response.status_code})",
                        "latency_ms": latency,
                        "details": {"url": url},
                    }

        except httpx.TimeoutException:
            return {
                "healthy": False,
                "message": "Health check timed out",
                "latency_ms": (time.perf_counter() - start) * 1000,
            }
        except httpx.ConnectError as e:
            return {
                "healthy": False,
                "message": f"Connection failed: {e}",
                "latency_ms": (time.perf_counter() - start) * 1000,
            }
        except Exception as e:
            return {
                "healthy": False,
                "message": str(e),
                "latency_ms": (time.perf_counter() - start) * 1000,
            }
