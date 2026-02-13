"""Base adapter interface for client-side data fetching."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from ..client import ResolvedSource
    from ..config import ClientConfig


@dataclass
class AdapterResult:
    """
    Result from adapter fetch operations.

    Aligns with service response structure and includes timing/execution metadata.
    """

    data: Any
    """The fetched data (usually list of dicts or dict)."""

    row_count: int | None = None
    """Number of rows/items returned."""

    columns: list[str] = field(default_factory=list)
    """Column names if applicable."""

    execution_time_ms: float | None = None
    """Time taken to execute the query/fetch in milliseconds."""

    source_type: str | None = None
    """Type of source adapter used."""

    query_executed: str | None = None
    """The actual query that was executed (for debugging)."""

    truncated: bool = False
    """Whether results were truncated due to limits."""

    metadata: dict[str, Any] = field(default_factory=dict)
    """Additional metadata from the fetch operation."""


class BaseAdapter(ABC):
    """
    Base class for client-side data adapters.

    Each adapter connects directly to a data source type
    and fetches the data.
    """

    @abstractmethod
    def fetch(
        self,
        resolved: ResolvedSource,
        config: ClientConfig,
        **kwargs,
    ) -> Any:
        """
        Fetch data from the source.

        Args:
            resolved: Resolved source info from the moniker service
            config: Client configuration (includes credentials)
            **kwargs: Additional adapter-specific parameters

        Returns:
            The fetched data (usually list of dicts or dict)
        """
        ...

    def list_children(
        self,
        resolved: ResolvedSource,
        config: ClientConfig,
    ) -> list[str]:
        """
        List children at the source level.

        Default returns empty - override for sources that support it.
        """
        return []

    def health_check(
        self,
        resolved: ResolvedSource,
        config: ClientConfig,
    ) -> dict[str, Any]:
        """
        Check health/connectivity of the data source.

        Returns:
            Dict with at least 'healthy' (bool) and optionally:
            - 'latency_ms': Connection latency
            - 'message': Status message
            - 'details': Additional diagnostic info

        Default returns healthy=True - override for sources that support it.
        """
        return {"healthy": True, "message": "Health check not implemented"}
