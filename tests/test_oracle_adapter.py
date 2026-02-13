"""Unit tests for the Oracle adapter."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from moniker_client.adapters.oracle import OracleAdapter, _RESERVED_PARAMS


class TestOracleAdapterDSN:
    """Tests for Oracle DSN building."""

    def test_build_dsn_from_explicit_dsn(self):
        """Test DSN from explicit dsn field."""
        adapter = OracleAdapter()
        conn_info = {"dsn": "oracle.example.com:1521/ORCL"}
        dsn = adapter._build_dsn(conn_info)
        assert dsn == "oracle.example.com:1521/ORCL"

    def test_build_dsn_from_host_port_service(self):
        """Test DSN from host/port/service_name."""
        adapter = OracleAdapter()
        conn_info = {
            "host": "oracle.example.com",
            "port": 1521,
            "service_name": "ORCL",
        }
        dsn = adapter._build_dsn(conn_info)
        assert dsn == "oracle.example.com:1521/ORCL"

    def test_build_dsn_default_port(self):
        """Test DSN uses default port when not specified."""
        adapter = OracleAdapter()
        conn_info = {
            "host": "oracle.example.com",
            "service_name": "ORCL",
        }
        dsn = adapter._build_dsn(conn_info)
        assert dsn == "oracle.example.com:1521/ORCL"

    def test_build_dsn_default_host(self):
        """Test DSN uses default host when not specified."""
        adapter = OracleAdapter()
        conn_info = {"service_name": "ORCL"}
        dsn = adapter._build_dsn(conn_info)
        assert dsn == "localhost:1521/ORCL"

    def test_build_dsn_no_service_name_raises(self):
        """Test DSN building without service_name raises ValueError."""
        adapter = OracleAdapter()
        conn_info = {"host": "oracle.example.com"}
        with pytest.raises(ValueError, match="Oracle DSN or host/port/service_name required"):
            adapter._build_dsn(conn_info)


class TestOracleAdapterFlashback:
    """Tests for Oracle Flashback temporal query support."""

    def test_inject_flashback_with_timestamp(self):
        """Test Flashback injection with timestamp."""
        adapter = OracleAdapter()
        query = "SELECT * FROM employees"
        result = adapter._inject_flashback(query, "2024-01-15 10:30:00")
        assert "AS OF TIMESTAMP" in result
        assert "2024-01-15 10:30:00" in result
        assert "SELECT * FROM employees" in result

    def test_inject_flashback_with_scn(self):
        """Test Flashback injection with SCN."""
        adapter = OracleAdapter()
        query = "SELECT * FROM employees"
        result = adapter._inject_flashback(query, "123456789")
        assert "AS OF SCN 123456789" in result

    def test_inject_flashback_preserves_where_clause(self):
        """Test Flashback preserves existing WHERE clause."""
        adapter = OracleAdapter()
        query = "SELECT * FROM employees WHERE dept_id = 10"
        result = adapter._inject_flashback(query, "2024-01-15 10:30:00")
        assert "AS OF TIMESTAMP" in result
        assert "WHERE dept_id = 10" in result

    def test_inject_flashback_no_from_clause(self):
        """Test Flashback with no FROM clause returns unchanged."""
        adapter = OracleAdapter()
        query = "SELECT 1 + 1"
        result = adapter._inject_flashback(query, "2024-01-15 10:30:00")
        assert result == query


class TestOracleAdapterFiltering:
    """Tests for Oracle parameter filtering."""

    def test_extract_filters_from_params(self):
        """Test extracting filters from params."""
        adapter = OracleAdapter()
        params = {
            "dept_id": 10,
            "status": "active",
            "limit": 100,  # reserved, should be excluded
        }
        filters = adapter._extract_filters(params)
        assert filters == {"dept_id": 10, "status": "active"}
        assert "limit" not in filters

    def test_extract_filters_from_moniker_params(self):
        """Test extracting filters from moniker_params."""
        adapter = OracleAdapter()
        params = {
            "moniker_params": {
                "dept_id": 10,
                "status": "active",
            },
        }
        filters = adapter._extract_filters(params)
        assert filters == {"dept_id": 10, "status": "active"}

    def test_extract_filters_excludes_reserved(self):
        """Test reserved params are excluded."""
        adapter = OracleAdapter()
        for reserved in _RESERVED_PARAMS:
            params = {reserved: "value", "custom_field": "custom"}
            filters = adapter._extract_filters(params)
            assert reserved not in filters
            assert filters.get("custom_field") == "custom"

    def test_inject_where_clause_new(self):
        """Test injecting new WHERE clause."""
        adapter = OracleAdapter()
        query = "SELECT * FROM employees"
        filters = {"dept_id": 10, "status": "active"}
        result = adapter._inject_where_clause(query, filters)
        assert "WHERE" in result
        assert "dept_id = 10" in result
        assert "status = 'active'" in result

    def test_inject_where_clause_existing(self):
        """Test appending to existing WHERE clause."""
        adapter = OracleAdapter()
        query = "SELECT * FROM employees WHERE salary > 50000"
        filters = {"dept_id": 10}
        result = adapter._inject_where_clause(query, filters)
        assert result.count("WHERE") == 1
        assert "dept_id = 10" in result
        assert "salary > 50000" in result

    def test_inject_where_clause_with_list(self):
        """Test IN clause for list values."""
        adapter = OracleAdapter()
        query = "SELECT * FROM employees"
        filters = {"dept_id": [10, 20, 30]}
        result = adapter._inject_where_clause(query, filters)
        assert "dept_id IN (10, 20, 30)" in result

    def test_inject_where_clause_with_string_list(self):
        """Test IN clause for string list values."""
        adapter = OracleAdapter()
        query = "SELECT * FROM employees"
        filters = {"status": ["active", "pending"]}
        result = adapter._inject_where_clause(query, filters)
        assert "status IN ('active', 'pending')" in result


class TestOracleAdapterLimit:
    """Tests for Oracle FETCH FIRST limit injection."""

    def test_inject_limit(self):
        """Test injecting FETCH FIRST limit."""
        adapter = OracleAdapter()
        query = "SELECT * FROM employees"
        result = adapter._inject_limit(query, 100)
        assert "FETCH FIRST 100 ROWS ONLY" in result

    def test_inject_limit_with_existing_fetch(self):
        """Test limit not duplicated if FETCH exists."""
        adapter = OracleAdapter()
        query = "SELECT * FROM employees FETCH FIRST 50 ROWS ONLY"
        result = adapter._inject_limit(query, 100)
        assert result == query  # unchanged

    def test_inject_limit_removes_trailing_semicolon(self):
        """Test limit handles trailing semicolon."""
        adapter = OracleAdapter()
        query = "SELECT * FROM employees;"
        result = adapter._inject_limit(query, 100)
        assert result.endswith("FETCH FIRST 100 ROWS ONLY")
        assert not result.endswith(";")


class TestOracleAdapterBuildQuery:
    """Tests for complete query building."""

    def test_build_query_with_all_features(self, mock_resolved_source):
        """Test building query with flashback, filters, and limit."""
        adapter = OracleAdapter()
        resolved = mock_resolved_source(
            query="SELECT * FROM employees",
            params={
                "as_of": "2024-01-15 10:30:00",
                "dept_id": 10,
                "limit": 100,
            },
        )
        query = adapter._build_query(resolved)
        assert "AS OF TIMESTAMP" in query
        assert "WHERE" in query
        assert "dept_id = 10" in query
        assert "FETCH FIRST 100 ROWS ONLY" in query

    def test_build_query_no_query(self, mock_resolved_source):
        """Test building with no query returns None."""
        adapter = OracleAdapter()
        resolved = mock_resolved_source(query=None)
        assert adapter._build_query(resolved) is None

    def test_build_query_moniker_version_as_flashback(self, mock_resolved_source):
        """Test moniker_version is used for flashback."""
        adapter = OracleAdapter()
        resolved = mock_resolved_source(
            query="SELECT * FROM employees",
            params={"moniker_version": "2024-01-15 10:30:00"},
        )
        query = adapter._build_query(resolved)
        assert "AS OF TIMESTAMP" in query


class TestOracleAdapterFetch:
    """Tests for Oracle fetch operation."""

    def test_fetch_missing_credentials(self, mock_resolved_source, mock_config):
        """Test fetch raises without credentials."""
        adapter = OracleAdapter()
        resolved = mock_resolved_source(
            connection={"service_name": "ORCL"},
            query="SELECT * FROM employees",
        )
        config = mock_config(oracle_user=None, oracle_password=None)

        mock_oracledb = MagicMock()
        with patch.dict("sys.modules", {"oracledb": mock_oracledb}):
            with pytest.raises(ValueError, match="Oracle credentials not configured"):
                adapter.fetch(resolved, config)

    def test_fetch_missing_query(self, mock_resolved_source, mock_config):
        """Test fetch raises without query."""
        adapter = OracleAdapter()
        resolved = mock_resolved_source(
            connection={"service_name": "ORCL"},
            query=None,
        )
        config = mock_config(oracle_user="user", oracle_password="pass")

        # Mock oracledb import
        mock_oracledb = MagicMock()
        with patch.dict("sys.modules", {"oracledb": mock_oracledb}):
            with pytest.raises(ValueError, match="No query provided"):
                adapter.fetch(resolved, config)

    def test_fetch_returns_list_of_dicts(
        self, mock_resolved_source, mock_config, mock_oracle_connection
    ):
        """Test fetch returns list of dicts."""
        adapter = OracleAdapter()
        resolved = mock_resolved_source(
            connection={"service_name": "ORCL"},
            query="SELECT id, name FROM employees",
        )
        config = mock_config(oracle_user="user", oracle_password="pass")

        mock_conn = mock_oracle_connection(
            columns=["ID", "NAME"],
            rows=[(1, "Alice"), (2, "Bob")],
        )
        mock_oracledb = MagicMock()
        mock_oracledb.connect.return_value = mock_conn

        with patch.dict("sys.modules", {"oracledb": mock_oracledb}):
            result = adapter.fetch(resolved, config)

        assert result == [
            {"ID": 1, "NAME": "Alice"},
            {"ID": 2, "NAME": "Bob"},
        ]

    def test_fetch_returns_adapter_result_when_requested(
        self, mock_resolved_source, mock_config, mock_oracle_connection
    ):
        """Test fetch returns AdapterResult when return_result=True."""
        adapter = OracleAdapter()
        resolved = mock_resolved_source(
            connection={"service_name": "ORCL"},
            query="SELECT id, name FROM employees",
        )
        config = mock_config(oracle_user="user", oracle_password="pass")

        mock_conn = mock_oracle_connection(
            columns=["ID", "NAME"],
            rows=[(1, "Alice")],
        )
        mock_oracledb = MagicMock()
        mock_oracledb.connect.return_value = mock_conn

        with patch.dict("sys.modules", {"oracledb": mock_oracledb}):
            from moniker_client.adapters.base import AdapterResult
            result = adapter.fetch(resolved, config, return_result=True)

        assert isinstance(result, AdapterResult)
        assert result.data == [{"ID": 1, "NAME": "Alice"}]
        assert result.row_count == 1
        assert result.columns == ["ID", "NAME"]
        assert result.source_type == "oracle"


class TestOracleAdapterErrorHandling:
    """Tests for Oracle error handling."""

    def test_connection_error_no_listener(self, mock_resolved_source, mock_config):
        """Test specific error for no listener."""
        adapter = OracleAdapter()
        # Clear connection cache to ensure connect is called
        adapter._connection_cache.clear()

        resolved = mock_resolved_source(
            connection={"service_name": "ORCL"},
            query="SELECT 1 FROM DUAL",
        )
        config = mock_config(oracle_user="user", oracle_password="pass")

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("ORA-12541: TNS:no listener")
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.ping.return_value = None

        mock_oracledb = MagicMock()
        mock_oracledb.connect.return_value = mock_conn

        with patch.dict("sys.modules", {"oracledb": mock_oracledb}):
            with pytest.raises(ConnectionError, match="No listener"):
                adapter.fetch(resolved, config)

    def test_connection_error_auth_failed(self, mock_resolved_source, mock_config):
        """Test specific error for auth failure."""
        adapter = OracleAdapter()
        adapter._connection_cache.clear()

        resolved = mock_resolved_source(
            connection={"service_name": "ORCL"},
            query="SELECT 1 FROM DUAL",
        )
        config = mock_config(oracle_user="user", oracle_password="pass")

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("ORA-01017: invalid username/password")
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.ping.return_value = None

        mock_oracledb = MagicMock()
        mock_oracledb.connect.return_value = mock_conn

        with patch.dict("sys.modules", {"oracledb": mock_oracledb}):
            with pytest.raises(PermissionError, match="authentication failed"):
                adapter.fetch(resolved, config)

    def test_connection_error_timeout(self, mock_resolved_source, mock_config):
        """Test specific error for connection timeout."""
        adapter = OracleAdapter()
        adapter._connection_cache.clear()

        resolved = mock_resolved_source(
            connection={"service_name": "ORCL"},
            query="SELECT 1 FROM DUAL",
        )
        config = mock_config(oracle_user="user", oracle_password="pass")

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("ORA-12170: TNS:Connect timeout")
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.ping.return_value = None

        mock_oracledb = MagicMock()
        mock_oracledb.connect.return_value = mock_conn

        with patch.dict("sys.modules", {"oracledb": mock_oracledb}):
            with pytest.raises(TimeoutError, match="timed out"):
                adapter.fetch(resolved, config)


class TestOracleAdapterConnectionCaching:
    """Tests for Oracle connection caching."""

    def test_connection_is_cached(self, mock_resolved_source, mock_config):
        """Test connections are cached and reused."""
        adapter = OracleAdapter()
        resolved = mock_resolved_source(
            connection={"service_name": "ORCL"},
            query="SELECT 1 FROM DUAL",
        )
        config = mock_config(oracle_user="user", oracle_password="pass")

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = MagicMock(
            description=[("X", None)],
            fetchall=MagicMock(return_value=[(1,)]),
        )
        mock_conn.ping.return_value = None

        mock_oracledb = MagicMock()
        mock_oracledb.connect.return_value = mock_conn

        with patch.dict("sys.modules", {"oracledb": mock_oracledb}):
            # First call
            adapter.fetch(resolved, config)
            # Second call should reuse connection
            adapter.fetch(resolved, config)

        # connect should only be called once
        assert mock_oracledb.connect.call_count == 1

    def test_stale_connection_is_replaced(self, mock_resolved_source, mock_config):
        """Test stale connections are replaced."""
        adapter = OracleAdapter()
        resolved = mock_resolved_source(
            connection={"service_name": "ORCL"},
            query="SELECT 1 FROM DUAL",
        )
        config = mock_config(oracle_user="user", oracle_password="pass")

        stale_conn = MagicMock()
        stale_conn.ping.side_effect = Exception("Connection lost")

        fresh_conn = MagicMock()
        fresh_conn.cursor.return_value = MagicMock(
            description=[("X", None)],
            fetchall=MagicMock(return_value=[(1,)]),
        )
        fresh_conn.ping.return_value = None

        mock_oracledb = MagicMock()
        mock_oracledb.connect.side_effect = [stale_conn, fresh_conn]

        with patch.dict("sys.modules", {"oracledb": mock_oracledb}):
            # First call caches stale connection
            adapter._connection_cache["user@localhost:1521/ORCL"] = stale_conn
            # Fetch should detect stale and get new connection
            adapter.fetch(resolved, config)

        assert mock_oracledb.connect.call_count == 1  # Only fresh_conn

    def test_close_connections(self, mock_resolved_source, mock_config):
        """Test close_connections clears cache."""
        adapter = OracleAdapter()

        mock_conn = MagicMock()
        adapter._connection_cache["user@dsn"] = mock_conn

        adapter.close_connections()

        assert len(adapter._connection_cache) == 0
        mock_conn.close.assert_called_once()


class TestOracleAdapterHealthCheck:
    """Tests for Oracle health check."""

    def test_health_check_success(self, mock_resolved_source, mock_config):
        """Test health check success."""
        adapter = OracleAdapter()
        resolved = mock_resolved_source(connection={"service_name": "ORCL"})
        config = mock_config(oracle_user="user", oracle_password="pass")

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.ping.return_value = None

        mock_oracledb = MagicMock()
        mock_oracledb.connect.return_value = mock_conn

        with patch.dict("sys.modules", {"oracledb": mock_oracledb}):
            result = adapter.health_check(resolved, config)

        assert result["healthy"] is True
        assert "latency_ms" in result

    def test_health_check_no_credentials(self, mock_resolved_source, mock_config):
        """Test health check fails without credentials."""
        adapter = OracleAdapter()
        resolved = mock_resolved_source(connection={"service_name": "ORCL"})
        config = mock_config(oracle_user=None, oracle_password=None)

        mock_oracledb = MagicMock()
        with patch.dict("sys.modules", {"oracledb": mock_oracledb}):
            result = adapter.health_check(resolved, config)

        assert result["healthy"] is False
        assert "credentials" in result["message"].lower()

    def test_health_check_no_oracledb(self, mock_resolved_source, mock_config):
        """Test health check when oracledb not installed."""
        adapter = OracleAdapter()
        resolved = mock_resolved_source(connection={"service_name": "ORCL"})
        config = mock_config(oracle_user="user", oracle_password="pass")

        # Don't mock oracledb - let the import fail
        result = adapter.health_check(resolved, config)

        assert result["healthy"] is False
        assert "not installed" in result["message"].lower()
