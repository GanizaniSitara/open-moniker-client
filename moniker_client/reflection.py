"""Catalog reflection API for discovery and introspection.

CatalogReflector provides a high-level facade over the MonikerClient
for catalog discovery, search, and schema introspection.
"""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .client import MonikerClient, SearchResult, CatalogStats, SchemaInfo


class CatalogReflector:
    """
    High-level facade for catalog discovery and introspection.

    Composes over MonikerClient to provide convenient methods for
    exploring the catalog: searching, filtering by status/tag,
    listing domains, and inspecting schemas.

    Usage:
        from moniker_client.reflection import CatalogReflector

        reflector = CatalogReflector()

        # Search
        results = reflector.search("equity")

        # Stats
        stats = reflector.stats()
        print(stats.total_monikers)

        # List source types
        sources = reflector.sources()

        # Browse domains
        domains = reflector.domains()

        # Find deprecated monikers
        old = reflector.deprecated()
    """

    def __init__(self, client: "MonikerClient | None" = None):
        """
        Create a CatalogReflector.

        Args:
            client: Optional MonikerClient instance. If not provided,
                    a default client is lazily created on first use.
        """
        self._client = client

    @property
    def client(self) -> "MonikerClient":
        """Get the underlying client (lazy-initialized)."""
        if self._client is None:
            from .client import _get_client
            self._client = _get_client()
        return self._client

    def search(
        self,
        query: str,
        status: str | None = None,
        source_type: str | None = None,
        limit: int = 50,
    ) -> "SearchResult":
        """
        Search the catalog for monikers matching a query.

        Args:
            query: Search query string
            status: Optional status filter (e.g., 'active', 'deprecated')
            source_type: Optional source type filter (post-filter on results)
            limit: Maximum number of results

        Returns:
            SearchResult with matching catalog entries
        """
        result = self.client.search(query, status=status, limit=limit)

        # Post-filter by source_type if specified
        if source_type is not None:
            filtered = [
                r for r in result.results
                if r.get("source_type") == source_type
            ]
            from .client import SearchResult
            return SearchResult(
                query=result.query,
                total_results=len(filtered),
                results=filtered,
            )

        return result

    def stats(self) -> "CatalogStats":
        """
        Get catalog statistics.

        Returns:
            CatalogStats with aggregate counts and coverage metrics
        """
        return self.client.catalog_stats()

    def schema(self, moniker: str) -> "SchemaInfo":
        """
        Get schema information for a moniker.

        Args:
            moniker: Moniker path

        Returns:
            SchemaInfo with column definitions and metadata
        """
        return self.client.schema(moniker)

    def sources(self) -> dict[str, int]:
        """
        Get a count of monikers by source type.

        Returns:
            Dict mapping source type names to counts
        """
        stats = self.stats()
        return dict(stats.by_source_type)

    def domains(self) -> list[dict[str, Any]]:
        """
        List top-level domains (root children of the catalog tree).

        Returns:
            List of dicts with domain info (path, name, children count, etc.)
        """
        tree = self.client.tree("", depth=1)
        return [
            {
                "path": child.path,
                "name": child.name,
                "source_type": child.source_type,
                "has_source_binding": child.has_source_binding,
                "description": child.description,
                "children_count": len(child.children),
            }
            for child in tree.children
        ]

    def deprecated(self) -> list[dict[str, Any]]:
        """
        Find all deprecated monikers.

        Returns:
            List of dicts with deprecated moniker info, enriched with
            description where available.
        """
        result = self.client.search("", status="deprecated", limit=500)
        return result.results

    def by_status(self, status: str) -> list[dict[str, Any]]:
        """
        Find monikers with a given status.

        Args:
            status: Status to filter by (e.g., 'active', 'deprecated', 'draft')

        Returns:
            List of dicts with matching moniker info
        """
        result = self.client.search("", status=status, limit=500)
        return result.results

    def by_tag(self, tag: str) -> list[dict[str, Any]]:
        """
        Find monikers tagged with a specific tag.

        Args:
            tag: Tag to search for

        Returns:
            List of dicts with matching moniker info, filtered to those
            that contain the specified tag.
        """
        result = self.client.search(tag, limit=500)
        return [
            r for r in result.results
            if tag in r.get("tags", [])
        ]
