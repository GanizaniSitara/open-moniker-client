"""Oracle adapter - direct connection to Oracle database."""

from __future__ import annotations

import time
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from ..client import ResolvedSource
    from ..config import ClientConfig

from .base import BaseAdapter, AdapterResult


# Reserved parameter keys that should not be used as WHERE filters
_RESERVED_PARAMS = frozenset({
    "moniker_version",
    "moniker_revision",
    "as_of",
    "limit",
    "offset",
    "order_by",
    "method",
    "response_path",
    "query_params",
    "moniker_params",
})


class OracleAdapter(BaseAdapter):
    """
    Adapter for direct Oracle database connection.

    Credentials come from ClientConfig (environment variables).

    Features:
    - Temporal queries via Oracle Flashback (as_of parameter)
    - Parameter filtering (auto-generates WHERE clauses)
    - Connection caching within adapter instance
    - Improved error handling
    """

    def __init__(self):
        self._connection_cache: dict[str, Any] = {}

    def fetch(
        self,
        resolved: ResolvedSource,
        config: ClientConfig,
        **kwargs,
    ) -> Any:
        try:
            import oracledb
        except ImportError:
            raise ImportError("oracledb required: pip install oracledb")

        start_time = time.perf_counter()
        conn_info = resolved.connection
        params = resolved.params

        # Build DSN
        dsn = self._build_dsn(conn_info)

        # Get credentials - check resolved.params first, then config
        user = params.get("oracle_user") or config.get_credential("oracle", "user")
        password = params.get("oracle_password") or config.get_credential("oracle", "password")

        if not user or not password:
            raise ValueError(
                "Oracle credentials not configured. "
                "Set ORACLE_USER and ORACLE_PASSWORD environment variables."
            )

        # Build query with temporal and filter support
        query = self._build_query(resolved)
        if not query:
            raise ValueError("No query provided for Oracle source")

        # Get or create connection
        conn = self._get_connection(dsn, user, password, oracledb)

        try:
            cursor = conn.cursor()
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            cursor.close()

            # Convert to list of dicts
            data = [dict(zip(columns, row)) for row in rows]

            execution_time = (time.perf_counter() - start_time) * 1000

            # Return AdapterResult if requested, otherwise raw data for backwards compatibility
            if kwargs.get("return_result"):
                return AdapterResult(
                    data=data,
                    row_count=len(data),
                    columns=columns,
                    execution_time_ms=execution_time,
                    source_type="oracle",
                    query_executed=query,
                )

            return data

        except Exception as e:
            # Provide specific error messages
            error_msg = str(e)
            if "ORA-12541" in error_msg:
                raise ConnectionError(
                    f"Cannot connect to Oracle: No listener at {dsn}. "
                    "Check that the database is running and the DSN is correct."
                ) from e
            elif "ORA-01017" in error_msg:
                raise PermissionError(
                    "Oracle authentication failed. Check username and password."
                ) from e
            elif "ORA-12170" in error_msg:
                raise TimeoutError(
                    f"Oracle connection timed out connecting to {dsn}."
                ) from e
            elif "ORA-00942" in error_msg:
                raise ValueError(
                    "Oracle table or view does not exist. Check the query."
                ) from e
            raise

    def _build_dsn(self, conn_info: dict[str, Any]) -> str:
        """Build Oracle DSN from connection info."""
        dsn = conn_info.get("dsn")
        if dsn:
            return dsn

        host = conn_info.get("host", "localhost")
        port = conn_info.get("port", 1521)
        service_name = conn_info.get("service_name")

        if service_name:
            return f"{host}:{port}/{service_name}"

        raise ValueError("Oracle DSN or host/port/service_name required")

    def _build_query(self, resolved: ResolvedSource) -> str | None:
        """Build query with temporal and filter support."""
        query = resolved.query
        if not query:
            return None

        params = resolved.params

        # Handle temporal queries (Oracle Flashback)
        as_of = params.get("as_of") or params.get("moniker_version")
        if as_of:
            # Inject AS OF clause for temporal queries
            # Query should be like "SELECT * FROM table" -> "SELECT * FROM table AS OF TIMESTAMP ..."
            query = self._inject_flashback(query, as_of)

        # Handle parameter filtering
        filters = self._extract_filters(params)
        if filters:
            query = self._inject_where_clause(query, filters)

        # Handle limit
        limit = params.get("limit")
        if limit is not None:
            query = self._inject_limit(query, limit)

        return query

    def _inject_flashback(self, query: str, as_of: str) -> str:
        """Inject Oracle Flashback AS OF clause."""
        # Find the FROM clause and inject AS OF after table name
        # This is a simplified approach - a full SQL parser would be better
        query_upper = query.upper()

        # Handle different timestamp formats
        if as_of.isdigit():
            # SCN (System Change Number)
            flashback_clause = f" AS OF SCN {as_of}"
        else:
            # Timestamp - try to parse and format
            flashback_clause = f" AS OF TIMESTAMP TO_TIMESTAMP('{as_of}', 'YYYY-MM-DD HH24:MI:SS')"

        # Find FROM clause position
        from_pos = query_upper.find(" FROM ")
        if from_pos == -1:
            return query

        # Find end of table name (next keyword or end)
        end_markers = [" WHERE ", " GROUP ", " ORDER ", " HAVING ", " UNION ", ";"]
        end_pos = len(query)

        for marker in end_markers:
            pos = query_upper.find(marker, from_pos + 6)
            if pos != -1 and pos < end_pos:
                end_pos = pos

        # Insert flashback clause after table name
        return query[:end_pos] + flashback_clause + query[end_pos:]

    def _extract_filters(self, params: dict[str, Any]) -> dict[str, Any]:
        """Extract filter parameters (non-reserved params)."""
        filters = {}

        # Get params from moniker_params if present
        moniker_params = params.get("moniker_params", {})
        if isinstance(moniker_params, dict):
            for key, value in moniker_params.items():
                if key not in _RESERVED_PARAMS and value is not None:
                    filters[key] = value

        # Also check top-level params
        for key, value in params.items():
            if key not in _RESERVED_PARAMS and not isinstance(value, dict) and value is not None:
                filters[key] = value

        return filters

    def _inject_where_clause(self, query: str, filters: dict[str, Any]) -> str:
        """Inject WHERE clause for filters."""
        if not filters:
            return query

        query_upper = query.upper()

        # Build conditions
        conditions = []
        for key, value in filters.items():
            if isinstance(value, str):
                conditions.append(f"{key} = '{value}'")
            elif isinstance(value, (list, tuple)):
                # IN clause
                if all(isinstance(v, str) for v in value):
                    values_str = ", ".join(f"'{v}'" for v in value)
                else:
                    values_str = ", ".join(str(v) for v in value)
                conditions.append(f"{key} IN ({values_str})")
            else:
                conditions.append(f"{key} = {value}")

        condition_str = " AND ".join(conditions)

        # Find if WHERE already exists
        where_pos = query_upper.find(" WHERE ")
        if where_pos != -1:
            # Append to existing WHERE
            return query[:where_pos + 7] + condition_str + " AND " + query[where_pos + 7:]

        # Find position to insert WHERE
        end_markers = [" GROUP ", " ORDER ", " HAVING ", " UNION ", ";"]
        insert_pos = len(query)

        for marker in end_markers:
            pos = query_upper.find(marker)
            if pos != -1 and pos < insert_pos:
                insert_pos = pos

        return query[:insert_pos] + " WHERE " + condition_str + query[insert_pos:]

    def _inject_limit(self, query: str, limit: int) -> str:
        """Inject row limit using FETCH FIRST (Oracle 12c+)."""
        query_upper = query.upper()

        # Check if already has FETCH clause
        if "FETCH " in query_upper:
            return query

        # Remove trailing semicolon if present
        query = query.rstrip(";").rstrip()

        return f"{query} FETCH FIRST {limit} ROWS ONLY"

    def _get_connection(self, dsn: str, user: str, password: str, oracledb: Any) -> Any:
        """Get cached connection or create new one."""
        cache_key = f"{user}@{dsn}"

        if cache_key in self._connection_cache:
            conn = self._connection_cache[cache_key]
            # Verify connection is still valid
            try:
                conn.ping()
                return conn
            except Exception:
                # Connection is stale, remove from cache
                del self._connection_cache[cache_key]

        # Create new connection
        conn = oracledb.connect(user=user, password=password, dsn=dsn)
        self._connection_cache[cache_key] = conn
        return conn

    def close_connections(self) -> None:
        """Close all cached connections."""
        for conn in self._connection_cache.values():
            try:
                conn.close()
            except Exception:
                pass
        self._connection_cache.clear()

    def list_children(
        self,
        resolved: ResolvedSource,
        config: ClientConfig,
    ) -> list[str]:
        """List tables in the schema."""
        try:
            import oracledb
        except ImportError:
            return []

        conn_info = resolved.connection
        params = resolved.params

        user = params.get("oracle_user") or config.get_credential("oracle", "user")
        password = params.get("oracle_password") or config.get_credential("oracle", "password")

        if not user or not password:
            return []

        try:
            dsn = self._build_dsn(conn_info)
        except ValueError:
            return []

        try:
            conn = self._get_connection(dsn, user, password, oracledb)
            cursor = conn.cursor()
            cursor.execute("SELECT table_name FROM user_tables ORDER BY table_name")
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
            return tables
        except Exception:
            return []

    def health_check(
        self,
        resolved: ResolvedSource,
        config: ClientConfig,
    ) -> dict[str, Any]:
        """Check Oracle database connectivity."""
        try:
            import oracledb
        except ImportError:
            return {
                "healthy": False,
                "message": "oracledb package not installed",
            }

        conn_info = resolved.connection
        params = resolved.params

        user = params.get("oracle_user") or config.get_credential("oracle", "user")
        password = params.get("oracle_password") or config.get_credential("oracle", "password")

        if not user or not password:
            return {
                "healthy": False,
                "message": "Oracle credentials not configured",
            }

        try:
            dsn = self._build_dsn(conn_info)
        except ValueError as e:
            return {
                "healthy": False,
                "message": str(e),
            }

        start = time.perf_counter()
        try:
            conn = self._get_connection(dsn, user, password, oracledb)
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM DUAL")
            cursor.fetchone()
            cursor.close()
            latency = (time.perf_counter() - start) * 1000

            return {
                "healthy": True,
                "message": "Connected successfully",
                "latency_ms": latency,
                "details": {"dsn": dsn},
            }
        except Exception as e:
            return {
                "healthy": False,
                "message": str(e),
                "latency_ms": (time.perf_counter() - start) * 1000,
            }
