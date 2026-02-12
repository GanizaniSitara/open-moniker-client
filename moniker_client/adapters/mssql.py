"""MS-SQL adapter - direct connection to SQL Server via pyodbc."""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from ..client import ResolvedSource
    from ..config import ClientConfig

from .base import BaseAdapter


class MSSQLAdapter(BaseAdapter):
    """
    Adapter for direct MS-SQL Server connection.

    Credentials come from ClientConfig (environment variables).
    Requires pyodbc and an appropriate ODBC driver.
    """

    def fetch(
        self,
        resolved: ResolvedSource,
        config: ClientConfig,
        **kwargs,
    ) -> Any:
        try:
            import pyodbc
        except ImportError:
            raise ImportError("pyodbc required: pip install pyodbc")

        conn_info = resolved.connection

        # Build connection string
        server = conn_info.get("server", "localhost")
        port = conn_info.get("port", 1433)
        database = conn_info.get("database")
        driver = conn_info.get("driver", "ODBC Driver 18 for SQL Server")

        # Get credentials from config
        user = config.get_credential("mssql", "user")
        password = config.get_credential("mssql", "password")

        if not user or not password:
            raise ValueError(
                "MS-SQL credentials not configured. "
                "Set MSSQL_USER and MSSQL_PASSWORD environment variables."
            )

        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server},{port};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password}"
        )

        # Execute query
        query = resolved.query
        if not query:
            raise ValueError("No query provided for MS-SQL source")

        conn = pyodbc.connect(conn_str)
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            cursor.close()

            # Convert to list of dicts
            return [dict(zip(columns, row)) for row in rows]
        finally:
            conn.close()

    def list_children(
        self,
        resolved: ResolvedSource,
        config: ClientConfig,
    ) -> list[str]:
        """List tables in the database."""
        try:
            import pyodbc
        except ImportError:
            return []

        conn_info = resolved.connection
        user = config.get_credential("mssql", "user")
        password = config.get_credential("mssql", "password")

        if not user or not password:
            return []

        server = conn_info.get("server", "localhost")
        port = conn_info.get("port", 1433)
        database = conn_info.get("database")
        driver = conn_info.get("driver", "ODBC Driver 18 for SQL Server")

        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server},{port};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password}"
        )

        try:
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME"
            )
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            return tables
        except Exception:
            return []
