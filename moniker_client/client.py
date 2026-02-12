"""Main client class and convenience functions."""

from __future__ import annotations

import logging
import time
import warnings
from dataclasses import dataclass, field
from typing import Any

import httpx

from .auth import get_auth_headers
from .config import ClientConfig
from .adapters import get_adapter
from .resilience import RetryConfig, retry_with_backoff, ClientCircuitBreaker


class MonikerError(Exception):
    """Base exception for moniker client errors."""
    pass


class ResolutionError(MonikerError):
    """Failed to resolve moniker."""
    pass


class FetchError(MonikerError):
    """Failed to fetch data from source."""
    pass


class NotFoundError(MonikerError):
    """Moniker path not found."""
    pass


class AccessDeniedError(MonikerError):
    """Access denied due to policy restrictions."""
    pass


@dataclass
class FetchResult:
    """Result from server-side data fetch."""
    moniker: str
    path: str
    source_type: str
    row_count: int
    columns: list[str]
    data: list[dict[str, Any]]
    truncated: bool = False
    query_executed: str | None = None
    execution_time_ms: float | None = None


@dataclass
class MetadataResult:
    """Rich metadata for AI/agent discoverability."""
    moniker: str
    path: str
    display_name: str | None = None
    description: str | None = None
    data_profile: dict[str, Any] | None = None
    temporal_coverage: dict[str, Any] | None = None
    relationships: dict[str, Any] | None = None
    sample_data: list[dict[str, Any]] | None = None
    schema: dict[str, Any] | None = None
    semantic_tags: list[str] = field(default_factory=list)
    data_quality: dict[str, Any] | None = None
    ownership: dict[str, Any] | None = None
    documentation: dict[str, Any] | None = None
    query_patterns: dict[str, Any] | None = None
    cost_indicators: dict[str, Any] | None = None
    nl_description: str | None = None
    use_cases: list[str] = field(default_factory=list)


@dataclass
class SampleResult:
    """Sample data preview from a source."""
    moniker: str
    path: str
    source_type: str
    row_count: int
    columns: list[str]
    data: list[dict[str, Any]]


@dataclass
class TreeNode:
    """A node in the moniker tree hierarchy."""
    path: str
    name: str
    children: list["TreeNode"] = field(default_factory=list)
    ownership: dict[str, Any] | None = None
    source_type: str | None = None
    has_source_binding: bool = False
    description: str | None = None

    def print(
        self,
        indent: str = "",
        is_last: bool = True,
        show_ownership: bool = True,
        show_source: bool = True,
        _is_root: bool = True,
    ) -> str:
        """
        Return a string representation of this tree node and its children.

        Args:
            indent: Current indentation prefix
            is_last: Whether this is the last child of parent
            show_ownership: Include ownership annotations
            show_source: Include source type annotations
        """
        lines = []

        # Build the line prefix
        if not _is_root:
            connector = "└── " if is_last else "├── "
        else:
            connector = ""

        # Build annotations
        annotations = []
        if show_ownership and self.ownership:
            owner = self.ownership.get("accountable_owner") or self.ownership.get("adop")
            if owner:
                annotations.append(f"owner: {owner}")
        if show_source and self.source_type:
            annotations.append(f"source: {self.source_type}")

        annotation_str = f"  [{', '.join(annotations)}]" if annotations else ""

        # Add this node
        lines.append(f"{indent}{connector}{self.name}/{annotation_str}")

        # Prepare indent for children
        if not _is_root:
            child_indent = indent + ("    " if is_last else "│   ")
        else:
            child_indent = ""

        # Add children
        for i, child in enumerate(self.children):
            is_last_child = (i == len(self.children) - 1)
            lines.append(child.print(child_indent, is_last_child, show_ownership, show_source, _is_root=False))

        return "\n".join(lines)

    def __str__(self) -> str:
        return self.print()


@dataclass
class SearchResult:
    """Result from catalog search."""
    query: str
    total_results: int
    results: list[dict[str, Any]]


@dataclass
class CatalogStats:
    """Catalog statistics."""
    total_monikers: int
    by_status: dict[str, int]
    by_source_type: dict[str, int]
    by_classification: dict[str, int]
    ownership_coverage: float


@dataclass
class SchemaInfo:
    """Schema information for a moniker."""
    moniker: str
    path: str
    columns: list[dict[str, Any]]
    primary_key: list[str] | None = None
    description: str | None = None
    granularity: str | None = None
    semantic_tags: list[str] = field(default_factory=list)
    related_monikers: list[str] = field(default_factory=list)


@dataclass
class ResolvedSource:
    """Resolved source information from the service."""
    moniker: str
    path: str
    source_type: str
    connection: dict[str, Any]
    query: str | None
    params: dict[str, Any]
    schema_info: dict[str, Any] | None
    read_only: bool
    ownership: dict[str, Any]
    binding_path: str
    sub_path: str | None
    # Deprecation / migration fields
    status: str | None = None
    deprecation_message: str | None = None
    successor: str | None = None
    sunset_deadline: str | None = None
    migration_guide_url: str | None = None
    redirected_from: str | None = None

    @property
    def is_deprecated(self) -> bool:
        return self.status == "deprecated"


class Moniker:
    """
    Fluent API for working with a moniker path.

    Usage:
        m = Moniker("risk.cvar/DESK_A/20240115/ALL")

        # Get metadata
        meta = m.metadata()
        print(meta.semantic_tags)

        # Fetch data (server-side execution)
        result = m.fetch(limit=100)
        print(result.data)

        # Quick sample
        preview = m.sample(5)

        # Read data (client-side execution)
        data = m.read()

        # Describe ownership
        info = m.describe()

        # Navigate to child
        child = m / "subpath"
        # or
        child = m.child("subpath")
    """

    def __init__(
        self,
        path: str,
        client: "MonikerClient | None" = None,
    ):
        """
        Create a Moniker object.

        Args:
            path: Moniker path (with or without scheme)
            client: Optional MonikerClient (uses default if not provided)
        """
        # Normalize path - strip scheme if present
        if path.startswith("moniker://"):
            path = path[len("moniker://"):]
        self._path = path.strip("/")
        self._client = client

    @property
    def path(self) -> str:
        """The moniker path."""
        return self._path

    @property
    def uri(self) -> str:
        """Full moniker URI with scheme."""
        return f"moniker://{self._path}"

    @property
    def client(self) -> "MonikerClient":
        """Get the client (default if not set)."""
        if self._client is None:
            self._client = _get_client()
        return self._client

    def __str__(self) -> str:
        return self.uri

    def __repr__(self) -> str:
        return f"Moniker({self._path!r})"

    def __truediv__(self, other: str) -> "Moniker":
        """Navigate to child path using / operator."""
        return self.child(other)

    def child(self, subpath: str) -> "Moniker":
        """Navigate to a child path."""
        new_path = f"{self._path}/{subpath.strip('/')}"
        return Moniker(new_path, client=self._client)

    def parent(self) -> "Moniker | None":
        """Get parent moniker, or None if at root."""
        if "/" not in self._path:
            return None
        parent_path = "/".join(self._path.split("/")[:-1])
        return Moniker(parent_path, client=self._client)

    def read(self, **kwargs) -> Any:
        """Read data (client-side execution via adapter)."""
        return self.client.read(self._path, **kwargs)

    def fetch(self, limit: int | None = None, **params) -> FetchResult:
        """Fetch data (server-side execution)."""
        return self.client.fetch(self._path, limit=limit, **params)

    def metadata(self) -> MetadataResult:
        """Get rich AI-discoverable metadata."""
        return self.client.metadata(self._path)

    def sample(self, limit: int = 5) -> SampleResult:
        """Get a quick sample of data."""
        return self.client.sample(self._path, limit=limit)

    def describe(self) -> dict[str, Any]:
        """Get ownership and catalog metadata."""
        return self.client.describe(self._path)

    def resolve(self) -> ResolvedSource:
        """Resolve to source connection info."""
        return self.client.resolve(self._path)

    def lineage(self) -> dict[str, Any]:
        """Get ownership lineage."""
        return self.client.lineage(self._path)

    def children(self) -> list[str]:
        """List child paths."""
        return self.client.list_children(self._path)

    def tree(self, depth: int | None = None) -> TreeNode:
        """
        Get the tree structure starting from this moniker.

        Args:
            depth: Maximum depth to traverse (None for unlimited)

        Returns:
            TreeNode representing this moniker and its descendants
        """
        return self.client.tree(self._path, depth=depth)

    def schema(self) -> SchemaInfo:
        """Get schema information for this moniker."""
        return self.client.schema(self._path)

    def print_tree(
        self,
        depth: int | None = None,
        show_ownership: bool = True,
        show_source: bool = True,
    ) -> str:
        """
        Get a human-readable tree representation.

        Args:
            depth: Maximum depth to traverse (None for unlimited)
            show_ownership: Include ownership annotations
            show_source: Include source type annotations

        Returns:
            Formatted tree string
        """
        tree = self.tree(depth=depth)
        return tree.print(show_ownership=show_ownership, show_source=show_source)


@dataclass
class MonikerClient:
    """
    Client for accessing data via monikers.

    Usage:
        client = MonikerClient()
        data = client.read("market-data/prices/equity/AAPL")

        # Or with custom config
        client = MonikerClient(config=ClientConfig(
            service_url="http://moniker-svc:8050",
            app_id="my-app",
            team="my-team",
        ))
    """
    config: ClientConfig = field(default_factory=ClientConfig)

    # Local cache of resolutions
    _cache: dict[str, tuple[ResolvedSource, float]] = field(default_factory=dict, init=False)

    # Resilience
    _retry_config: RetryConfig = field(default_factory=RetryConfig, init=False)
    _circuit_breaker: ClientCircuitBreaker = field(default_factory=ClientCircuitBreaker, init=False)

    def read(self, moniker: str, **kwargs) -> Any:
        """
        Read data for a moniker.

        Args:
            moniker: Moniker path (with or without scheme)
            **kwargs: Additional parameters passed to the source adapter

        Returns:
            The data from the source
        """
        start = time.perf_counter()
        outcome = "success"
        error_message = None
        row_count = None
        source_type = None
        deprecated = False
        successor = None

        try:
            # Normalize moniker
            if not moniker.startswith("moniker://"):
                moniker = f"moniker://{moniker}"

            # Resolve moniker to source info
            resolved = self._resolve(moniker)
            source_type = resolved.source_type
            deprecated = resolved.is_deprecated
            successor = resolved.successor

            # Get adapter for source type
            adapter = get_adapter(resolved.source_type)

            # Fetch data directly from source
            data = adapter.fetch(resolved, self.config, **kwargs)

            if isinstance(data, (list, dict)):
                row_count = len(data)

            return data

        except NotFoundError:
            outcome = "not_found"
            raise
        except Exception as e:
            outcome = "error"
            error_message = str(e)
            raise FetchError(f"Failed to fetch {moniker}: {e}") from e

        finally:
            # Report telemetry
            if self.config.report_telemetry:
                latency = (time.perf_counter() - start) * 1000
                self._report_telemetry(
                    moniker=moniker,
                    outcome=outcome,
                    latency_ms=latency,
                    source_type=source_type,
                    row_count=row_count,
                    error_message=error_message,
                    deprecated=deprecated,
                    successor=successor,
                )

    def describe(self, moniker: str) -> dict[str, Any]:
        """Get metadata about a moniker path."""
        if not moniker.startswith("moniker://"):
            moniker = f"moniker://{moniker}"

        path = moniker.replace("moniker://", "")

        with httpx.Client(timeout=self.config.timeout) as client:
            response = client.get(
                f"{self.config.service_url}/describe/{path}",
                headers=self._get_headers(),
            )
            if response.status_code == 404:
                raise NotFoundError(f"Path not found: {path}")
            response.raise_for_status()
            return response.json()

    def list_children(self, moniker: str = "") -> list[str]:
        """List children of a moniker path."""
        if moniker and not moniker.startswith("moniker://"):
            moniker = f"moniker://{moniker}"

        path = moniker.replace("moniker://", "") if moniker else ""

        with httpx.Client(timeout=self.config.timeout) as client:
            response = client.get(
                f"{self.config.service_url}/list/{path}",
                headers=self._get_headers(),
            )
            response.raise_for_status()
            return response.json().get("children", [])

    def lineage(self, moniker: str) -> dict[str, Any]:
        """Get ownership lineage for a moniker path."""
        if not moniker.startswith("moniker://"):
            moniker = f"moniker://{moniker}"

        path = moniker.replace("moniker://", "")

        with httpx.Client(timeout=self.config.timeout) as client:
            response = client.get(
                f"{self.config.service_url}/lineage/{path}",
                headers=self._get_headers(),
            )
            if response.status_code == 404:
                raise NotFoundError(f"Path not found: {path}")
            response.raise_for_status()
            return response.json()

    def resolve(self, moniker: str) -> ResolvedSource:
        """
        Resolve a moniker to source connection info.

        Usually you don't need this - use read() instead.
        This is useful if you want to manage the connection yourself.
        """
        if not moniker.startswith("moniker://"):
            moniker = f"moniker://{moniker}"
        return self._resolve(moniker)

    def batch_resolve(self, monikers: list[str]) -> dict[str, ResolvedSource]:
        """
        Resolve multiple monikers in a single call.

        More efficient than resolving one at a time when you need
        connection info for many monikers.

        Args:
            monikers: List of moniker paths

        Returns:
            Dict mapping moniker path to ResolvedSource
        """
        results = {}
        # Normalize monikers
        normalized = []
        for m in monikers:
            if not m.startswith("moniker://"):
                m = f"moniker://{m}"
            normalized.append(m)

        # Check cache first, collect uncached
        uncached = []
        for m in normalized:
            if self.config.cache_ttl > 0 and m in self._cache:
                resolved, cached_at = self._cache[m]
                if time.time() - cached_at < self.config.cache_ttl:
                    results[m.replace("moniker://", "")] = resolved
                    continue
            uncached.append(m)

        # Resolve uncached via batch endpoint
        if uncached:
            self._circuit_breaker.before_request()
            try:
                paths = [m.replace("moniker://", "") for m in uncached]
                with httpx.Client(timeout=self.config.timeout) as client:
                    response = client.post(
                        f"{self.config.service_url}/resolve/batch",
                        headers=self._get_headers(),
                        json={"monikers": [f"moniker://{p}" for p in paths]},
                    )
                    response.raise_for_status()
                    data = response.json()

                for item in data.get("results", []):
                    resolved = ResolvedSource(
                        moniker=item["moniker"],
                        path=item["path"],
                        source_type=item["source_type"],
                        connection=item["connection"],
                        query=item.get("query"),
                        params=item.get("params", {}),
                        schema_info=item.get("schema_info"),
                        read_only=item.get("read_only", True),
                        ownership=item.get("ownership", {}),
                        binding_path=item.get("binding_path", ""),
                        sub_path=item.get("sub_path"),
                        status=item.get("status"),
                        deprecation_message=item.get("deprecation_message"),
                        successor=item.get("successor"),
                        sunset_deadline=item.get("sunset_deadline"),
                        migration_guide_url=item.get("migration_guide_url"),
                        redirected_from=item.get("redirected_from"),
                    )
                    path = item["path"]
                    results[path] = resolved

                    # Emit deprecation warnings (gated by feature toggle)
                    if (getattr(self.config, 'deprecation_enabled', False)
                            and resolved.is_deprecated
                            and getattr(self.config, 'warn_on_deprecated', True)):
                        msg = (
                            f"Moniker '{resolved.path}' is deprecated."
                            f"{' ' + resolved.deprecation_message if resolved.deprecation_message else ''}"
                            f"{' Successor: ' + resolved.successor if resolved.successor else ''}"
                        )
                        warnings.warn(msg, DeprecationWarning, stacklevel=3)
                        logging.getLogger("moniker_client").warning(msg)

                        callback = getattr(self.config, 'deprecation_callback', None)
                        if callback:
                            callback(resolved.path, resolved.deprecation_message, resolved.successor)

                    # Cache
                    if self.config.cache_ttl > 0:
                        self._cache[f"moniker://{path}"] = (resolved, time.time())

                self._circuit_breaker.on_success()
            except Exception as e:
                self._circuit_breaker.on_failure()
                raise

        return results

    def batch_read(self, monikers: list[str], **kwargs) -> dict[str, Any]:
        """
        Read data for multiple monikers.

        Resolves all monikers first (batched), then fetches data for each.

        Args:
            monikers: List of moniker paths
            **kwargs: Additional parameters for adapters

        Returns:
            Dict mapping moniker path to data (or exception)
        """
        # Batch resolve
        resolved_map = self.batch_resolve(monikers)

        results = {}
        for path, resolved in resolved_map.items():
            try:
                adapter = get_adapter(resolved.source_type)
                data = adapter.fetch(resolved, self.config, **kwargs)
                results[path] = data
            except Exception as e:
                results[path] = e

        return results

    def fetch(
        self,
        moniker: str,
        limit: int | None = None,
        **params,
    ) -> FetchResult:
        """
        Fetch data via server-side query execution.

        Unlike read(), this executes the query on the server and returns
        the data directly. Useful when:
        - Client doesn't have direct source access
        - You want server-side query optimization
        - You need execution timing info

        Args:
            moniker: Moniker path (with or without scheme)
            limit: Maximum rows to return (default: server-side limit)
            **params: Additional query parameters

        Returns:
            FetchResult with data and execution metadata
        """
        if not moniker.startswith("moniker://"):
            moniker = f"moniker://{moniker}"

        path = moniker.replace("moniker://", "")

        # Build query params
        query_params = {}
        if limit is not None:
            query_params["limit"] = limit
        query_params.update(params)

        with httpx.Client(timeout=self.config.timeout) as client:
            response = client.get(
                f"{self.config.service_url}/fetch/{path}",
                headers=self._get_headers(),
                params=query_params if query_params else None,
            )

            if response.status_code == 404:
                raise NotFoundError(f"Path not found: {path}")
            if response.status_code == 403:
                data = response.json()
                raise AccessDeniedError(data.get("detail", "Access denied"))
            response.raise_for_status()

            data = response.json()

        return FetchResult(
            moniker=data["moniker"],
            path=data["path"],
            source_type=data["source_type"],
            row_count=data["row_count"],
            columns=data["columns"],
            data=data["data"],
            truncated=data.get("truncated", False),
            query_executed=data.get("query_executed"),
            execution_time_ms=data.get("execution_time_ms"),
        )

    def metadata(self, moniker: str) -> MetadataResult:
        """
        Get rich metadata for AI/agent discoverability.

        Returns comprehensive metadata including:
        - Data profile (row counts, column stats)
        - Temporal coverage (date ranges)
        - Schema information
        - Semantic tags for discovery
        - Cost indicators for query planning
        - Documentation links
        - Related datasets

        This is designed for AI agents to understand available data
        before deciding how to query it.

        Args:
            moniker: Moniker path (with or without scheme)

        Returns:
            MetadataResult with rich discovery metadata
        """
        if not moniker.startswith("moniker://"):
            moniker = f"moniker://{moniker}"

        path = moniker.replace("moniker://", "")

        with httpx.Client(timeout=self.config.timeout) as client:
            response = client.get(
                f"{self.config.service_url}/metadata/{path}",
                headers=self._get_headers(),
            )

            if response.status_code == 404:
                raise NotFoundError(f"Path not found: {path}")
            response.raise_for_status()

            data = response.json()

        return MetadataResult(
            moniker=data["moniker"],
            path=data["path"],
            display_name=data.get("display_name"),
            description=data.get("description"),
            data_profile=data.get("data_profile"),
            temporal_coverage=data.get("temporal_coverage"),
            relationships=data.get("relationships"),
            sample_data=data.get("sample_data"),
            schema=data.get("schema"),
            semantic_tags=data.get("semantic_tags", []),
            data_quality=data.get("data_quality"),
            ownership=data.get("ownership"),
            documentation=data.get("documentation"),
            query_patterns=data.get("query_patterns"),
            cost_indicators=data.get("cost_indicators"),
            nl_description=data.get("nl_description"),
            use_cases=data.get("use_cases", []),
        )

    def sample(self, moniker: str, limit: int = 5) -> SampleResult:
        """
        Get a quick sample of data from a source.

        Lightweight operation to preview data without full query.
        Useful for:
        - Quick data exploration
        - Validating schema expectations
        - AI agents sampling before larger queries

        Args:
            moniker: Moniker path (with or without scheme)
            limit: Number of sample rows (default: 5)

        Returns:
            SampleResult with preview data
        """
        if not moniker.startswith("moniker://"):
            moniker = f"moniker://{moniker}"

        path = moniker.replace("moniker://", "")

        with httpx.Client(timeout=self.config.timeout) as client:
            response = client.get(
                f"{self.config.service_url}/sample/{path}",
                headers=self._get_headers(),
                params={"limit": limit},
            )

            if response.status_code == 404:
                raise NotFoundError(f"Path not found: {path}")
            response.raise_for_status()

            data = response.json()

        return SampleResult(
            moniker=data["moniker"],
            path=data["path"],
            source_type=data["source_type"],
            row_count=data["row_count"],
            columns=data["columns"],
            data=data["data"],
        )

    def tree(self, moniker: str = "", depth: int | None = None) -> TreeNode:
        """
        Get the tree structure of the catalog starting from a path.

        Args:
            moniker: Starting path (empty for root)
            depth: Maximum depth to traverse (None for unlimited)

        Returns:
            TreeNode representing the hierarchy with metadata
        """
        if moniker and not moniker.startswith("moniker://"):
            moniker = f"moniker://{moniker}"

        path = moniker.replace("moniker://", "") if moniker else ""

        params = {}
        if depth is not None:
            params["depth"] = depth

        with httpx.Client(timeout=self.config.timeout) as client:
            response = client.get(
                f"{self.config.service_url}/tree/{path}" if path else f"{self.config.service_url}/tree",
                headers=self._get_headers(),
                params=params if params else None,
            )
            response.raise_for_status()
            data = response.json()

        def build_tree(node_data: dict) -> TreeNode:
            return TreeNode(
                path=node_data["path"],
                name=node_data["name"],
                children=[build_tree(c) for c in node_data.get("children", [])],
                ownership=node_data.get("ownership"),
                source_type=node_data.get("source_type"),
                has_source_binding=node_data.get("has_source_binding", False),
                description=node_data.get("description"),
            )

        return build_tree(data)

    def search(
        self,
        query: str,
        status: str | None = None,
        limit: int = 50,
    ) -> SearchResult:
        """
        Search the catalog for monikers matching a query.

        Args:
            query: Search query string
            status: Optional status filter (e.g., 'active', 'deprecated')
            limit: Maximum number of results

        Returns:
            SearchResult with matching catalog entries
        """
        params: dict[str, Any] = {"q": query, "limit": limit}
        if status is not None:
            params["status"] = status

        with httpx.Client(timeout=self.config.timeout) as client:
            response = client.get(
                f"{self.config.service_url}/catalog/search",
                headers=self._get_headers(),
                params=params,
            )
            response.raise_for_status()
            data = response.json()

        return SearchResult(
            query=query,
            total_results=data.get("total_results", len(data.get("results", []))),
            results=data.get("results", []),
        )

    def catalog_stats(self) -> CatalogStats:
        """
        Get catalog statistics.

        Returns:
            CatalogStats with aggregate counts and coverage metrics
        """
        with httpx.Client(timeout=self.config.timeout) as client:
            response = client.get(
                f"{self.config.service_url}/catalog/stats",
                headers=self._get_headers(),
            )
            response.raise_for_status()
            data = response.json()

        return CatalogStats(
            total_monikers=data.get("total_monikers", 0),
            by_status=data.get("by_status", {}),
            by_source_type=data.get("by_source_type", {}),
            by_classification=data.get("by_classification", {}),
            ownership_coverage=data.get("ownership_coverage", 0.0),
        )

    def schema(self, moniker: str) -> SchemaInfo:
        """
        Get schema information for a moniker.

        Extracts schema from the metadata endpoint.

        Args:
            moniker: Moniker path (with or without scheme)

        Returns:
            SchemaInfo with column definitions and metadata
        """
        meta = self.metadata(moniker)
        schema = meta.schema or {}
        return SchemaInfo(
            moniker=meta.moniker,
            path=meta.path,
            columns=schema.get("columns", []),
            primary_key=schema.get("primary_key"),
            description=meta.description,
            granularity=schema.get("granularity"),
            semantic_tags=meta.semantic_tags,
            related_monikers=[
                r.get("moniker", "") for r in (meta.relationships or {}).get("related", [])
            ] if meta.relationships else [],
        )

    def _resolve(self, moniker: str) -> ResolvedSource:
        """Internal resolve with caching, retry, and circuit breaker."""
        # Check cache
        if self.config.cache_ttl > 0 and moniker in self._cache:
            resolved, cached_at = self._cache[moniker]
            if time.time() - cached_at < self.config.cache_ttl:
                return resolved

        path = moniker.replace("moniker://", "")

        self._circuit_breaker.before_request()
        try:
            def _do_resolve():
                with httpx.Client(timeout=self.config.timeout) as client:
                    response = client.get(
                        f"{self.config.service_url}/resolve/{path}",
                        headers=self._get_headers(),
                    )

                    if response.status_code == 404:
                        raise NotFoundError(f"No source binding for: {path}")

                    if response.status_code != 200:
                        raise ResolutionError(f"Resolution failed: {response.text}")

                    return response.json()

            data = retry_with_backoff(_do_resolve, self._retry_config)

            resolved = ResolvedSource(
                moniker=data["moniker"],
                path=data["path"],
                source_type=data["source_type"],
                connection=data["connection"],
                query=data.get("query"),
                params=data.get("params", {}),
                schema_info=data.get("schema_info"),
                read_only=data.get("read_only", True),
                ownership=data.get("ownership", {}),
                binding_path=data.get("binding_path", ""),
                sub_path=data.get("sub_path"),
                status=data.get("status"),
                deprecation_message=data.get("deprecation_message"),
                successor=data.get("successor"),
                sunset_deadline=data.get("sunset_deadline"),
                migration_guide_url=data.get("migration_guide_url"),
                redirected_from=data.get("redirected_from"),
            )

            # Emit deprecation warnings if applicable (gated by feature toggle)
            if (getattr(self.config, 'deprecation_enabled', False)
                    and resolved.is_deprecated
                    and getattr(self.config, 'warn_on_deprecated', True)):
                msg = (
                    f"Moniker '{resolved.path}' is deprecated."
                    f"{' ' + resolved.deprecation_message if resolved.deprecation_message else ''}"
                    f"{' Successor: ' + resolved.successor if resolved.successor else ''}"
                )
                warnings.warn(msg, DeprecationWarning, stacklevel=3)
                logging.getLogger("moniker_client").warning(msg)

                # Invoke callback if configured
                callback = getattr(self.config, 'deprecation_callback', None)
                if callback:
                    callback(resolved.path, resolved.deprecation_message, resolved.successor)

            # Cache
            if self.config.cache_ttl > 0:
                self._cache[moniker] = (resolved, time.time())

            self._circuit_breaker.on_success()
            return resolved
        except NotFoundError:
            raise  # Don't count 404s as circuit breaker failures
        except Exception:
            self._circuit_breaker.on_failure()
            raise

    def _get_headers(self) -> dict[str, str]:
        """Build request headers including authentication."""
        headers = {}
        if self.config.app_id:
            headers["X-App-ID"] = self.config.app_id
        if self.config.team:
            headers["X-Team"] = self.config.team

        # Add authentication headers
        auth_headers = get_auth_headers(self.config)
        headers.update(auth_headers)

        return headers

    def _report_telemetry(
        self,
        moniker: str,
        outcome: str,
        latency_ms: float,
        source_type: str | None,
        row_count: int | None,
        error_message: str | None,
        deprecated: bool = False,
        successor: str | None = None,
    ) -> None:
        """Report access telemetry back to the service."""
        try:
            with httpx.Client(timeout=5) as client:
                client.post(
                    f"{self.config.service_url}/telemetry/access",
                    headers=self._get_headers(),
                    json={
                        "moniker": moniker,
                        "outcome": outcome,
                        "latency_ms": latency_ms,
                        "source_type": source_type,
                        "row_count": row_count,
                        "error_message": error_message,
                        "deprecated": deprecated,
                        "successor": successor,
                    },
                )
        except Exception:
            # Don't fail the read because telemetry failed
            pass


# Module-level default client
_default_client: MonikerClient | None = None


def _get_client() -> MonikerClient:
    """Get or create the default client."""
    global _default_client
    if _default_client is None:
        _default_client = MonikerClient()
    return _default_client


def read(moniker: str, **kwargs) -> Any:
    """
    Read data for a moniker using the default client.

    Usage:
        from moniker_client import read
        data = read("market-data/prices/equity/AAPL")
    """
    return _get_client().read(moniker, **kwargs)


def describe(moniker: str) -> dict[str, Any]:
    """Get metadata about a moniker path."""
    return _get_client().describe(moniker)


def list_children(moniker: str = "") -> list[str]:
    """List children of a moniker path."""
    return _get_client().list_children(moniker)


def lineage(moniker: str) -> dict[str, Any]:
    """Get ownership lineage for a moniker path."""
    return _get_client().lineage(moniker)


def fetch(moniker: str, limit: int | None = None, **params) -> FetchResult:
    """
    Fetch data via server-side query execution.

    Usage:
        from moniker_client import fetch
        result = fetch("risk.cvar/DESK_A/20240115/ALL", limit=100)
        print(result.data)  # List of rows
        print(result.columns)  # Column names
    """
    return _get_client().fetch(moniker, limit=limit, **params)


def metadata(moniker: str) -> MetadataResult:
    """
    Get rich metadata for AI/agent discoverability.

    Usage:
        from moniker_client import metadata
        meta = metadata("risk.cvar")
        print(meta.description)
        print(meta.semantic_tags)
        print(meta.cost_indicators)
    """
    return _get_client().metadata(moniker)


def sample(moniker: str, limit: int = 5) -> SampleResult:
    """
    Get a quick sample of data from a source.

    Usage:
        from moniker_client import sample
        result = sample("govies.treasury/US/10Y/ALL")
        print(result.data)  # Sample rows
    """
    return _get_client().sample(moniker, limit=limit)


def tree(moniker: str = "", depth: int | None = None) -> TreeNode:
    """
    Get the tree structure of the catalog.

    Usage:
        from moniker_client import tree

        # Get full tree
        t = tree()
        print(t)  # Pretty-printed tree

        # Get tree from specific path
        t = tree("risk")
        print(t.print(show_ownership=True))

        # Limit depth
        t = tree(depth=2)
    """
    return _get_client().tree(moniker, depth=depth)


def print_tree(
    moniker: str = "",
    depth: int | None = None,
    show_ownership: bool = True,
    show_source: bool = True,
) -> str:
    """
    Get a human-readable tree representation of the catalog.

    Usage:
        from moniker_client import print_tree
        print(print_tree())  # Full tree
        print(print_tree("commodities", depth=3))  # Subtree
    """
    t = tree(moniker, depth=depth)
    return t.print(show_ownership=show_ownership, show_source=show_source)


def search(query: str, status: str | None = None, limit: int = 50) -> SearchResult:
    """
    Search the catalog for monikers matching a query.

    Usage:
        from moniker_client import search
        results = search("equity")
        print(results.total_results)
    """
    return _get_client().search(query, status=status, limit=limit)


def catalog_stats() -> CatalogStats:
    """
    Get catalog statistics.

    Usage:
        from moniker_client import catalog_stats
        stats = catalog_stats()
        print(stats.total_monikers)
    """
    return _get_client().catalog_stats()
