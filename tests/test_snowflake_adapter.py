"""Unit tests for the Snowflake adapter."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest

from moniker_client.adapters.snowflake import SnowflakeAdapter


def create_snowflake_mock(mock_conn):
    """Create a mock snowflake.connector module."""
    mock_sf = MagicMock()
    mock_sf.connect.return_value = mock_conn
    return mock_sf


@pytest.fixture
def mock_snowflake_module():
    """
    Fixture to properly mock snowflake.connector.

    Yields a factory function that sets up the mock and returns it.
    """
    def _setup(mock_conn):
        mock_sf = MagicMock()
        mock_sf.connect.return_value = mock_conn

        # Need to mock the import path properly
        mock_snowflake = MagicMock()
        mock_snowflake.connector = mock_sf

        return mock_snowflake, mock_sf

    return _setup


class TestSnowflakeAdapterConnectionParams:
    """Tests for Snowflake connection parameter building."""

    def test_connection_params_basic(self, mock_resolved_source, mock_config, mock_snowflake_module):
        """Test basic connection parameters are passed correctly."""
        resolved = mock_resolved_source(
            source_type="snowflake",
            connection={
                "account": "test-account",
                "warehouse": "TEST_WH",
                "database": "TEST_DB",
                "schema": "PUBLIC",
            },
            query="SELECT 1",
        )
        config = mock_config(snowflake_user="user", snowflake_password="pass")

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("1", None)]
        mock_cursor.fetchall.return_value = [(1,)]
        mock_conn.cursor.return_value = mock_cursor

        mock_snowflake, mock_sf = mock_snowflake_module(mock_conn)

        # Remove any existing snowflake modules to force re-import
        for key in list(sys.modules.keys()):
            if key.startswith("snowflake"):
                del sys.modules[key]

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_sf}):
            adapter = SnowflakeAdapter()
            adapter.fetch(resolved, config)

            mock_sf.connect.assert_called_once()
            call_kwargs = mock_sf.connect.call_args[1]
            assert call_kwargs["account"] == "test-account"
            assert call_kwargs["warehouse"] == "TEST_WH"
            assert call_kwargs["database"] == "TEST_DB"
            assert call_kwargs["schema"] == "PUBLIC"

    def test_connection_params_with_role(self, mock_resolved_source, mock_config, mock_snowflake_module):
        """Test role is included when provided."""
        resolved = mock_resolved_source(
            source_type="snowflake",
            connection={
                "account": "test-account",
                "warehouse": "TEST_WH",
                "database": "TEST_DB",
                "schema": "PUBLIC",
                "role": "ANALYST_ROLE",
            },
            query="SELECT 1",
        )
        config = mock_config(snowflake_user="user", snowflake_password="pass")

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("1", None)]
        mock_cursor.fetchall.return_value = [(1,)]
        mock_conn.cursor.return_value = mock_cursor

        mock_snowflake, mock_sf = mock_snowflake_module(mock_conn)

        for key in list(sys.modules.keys()):
            if key.startswith("snowflake"):
                del sys.modules[key]

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_sf}):
            adapter = SnowflakeAdapter()
            adapter.fetch(resolved, config)

            call_kwargs = mock_sf.connect.call_args[1]
            assert call_kwargs["role"] == "ANALYST_ROLE"

    def test_connection_default_schema(self, mock_resolved_source, mock_config, mock_snowflake_module):
        """Test schema defaults to PUBLIC when not specified."""
        resolved = mock_resolved_source(
            source_type="snowflake",
            connection={
                "account": "test-account",
                "warehouse": "TEST_WH",
                "database": "TEST_DB",
                # no schema
            },
            query="SELECT 1",
        )
        config = mock_config(snowflake_user="user", snowflake_password="pass")

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("1", None)]
        mock_cursor.fetchall.return_value = [(1,)]
        mock_conn.cursor.return_value = mock_cursor

        mock_snowflake, mock_sf = mock_snowflake_module(mock_conn)

        for key in list(sys.modules.keys()):
            if key.startswith("snowflake"):
                del sys.modules[key]

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_sf}):
            adapter = SnowflakeAdapter()
            adapter.fetch(resolved, config)

            call_kwargs = mock_sf.connect.call_args[1]
            assert call_kwargs["schema"] == "PUBLIC"


class TestSnowflakeAdapterCredentials:
    """Tests for Snowflake credential handling."""

    def test_credentials_user_password(self, mock_resolved_source, mock_config, mock_snowflake_module):
        """Test username/password credentials are passed."""
        resolved = mock_resolved_source(
            source_type="snowflake",
            connection={"account": "test-account", "warehouse": "WH", "database": "DB"},
            query="SELECT 1",
        )
        config = mock_config(snowflake_user="myuser", snowflake_password="mypass")

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("1", None)]
        mock_cursor.fetchall.return_value = [(1,)]
        mock_conn.cursor.return_value = mock_cursor

        mock_snowflake, mock_sf = mock_snowflake_module(mock_conn)

        for key in list(sys.modules.keys()):
            if key.startswith("snowflake"):
                del sys.modules[key]

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_sf}):
            adapter = SnowflakeAdapter()
            adapter.fetch(resolved, config)

            call_kwargs = mock_sf.connect.call_args[1]
            assert call_kwargs["user"] == "myuser"
            assert call_kwargs["password"] == "mypass"

    def test_credentials_private_key(self, mock_resolved_source, mock_config, mock_snowflake_module):
        """Test private key path is passed when configured."""
        resolved = mock_resolved_source(
            source_type="snowflake",
            connection={"account": "test-account", "warehouse": "WH", "database": "DB"},
            query="SELECT 1",
        )
        config = mock_config(
            snowflake_user="myuser",
            snowflake_password=None,
            snowflake_private_key_path="/path/to/key.pem",
        )

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("1", None)]
        mock_cursor.fetchall.return_value = [(1,)]
        mock_conn.cursor.return_value = mock_cursor

        mock_snowflake, mock_sf = mock_snowflake_module(mock_conn)

        for key in list(sys.modules.keys()):
            if key.startswith("snowflake"):
                del sys.modules[key]

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_sf}):
            adapter = SnowflakeAdapter()
            adapter.fetch(resolved, config)

            call_kwargs = mock_sf.connect.call_args[1]
            assert call_kwargs["user"] == "myuser"
            assert call_kwargs["private_key_file"] == "/path/to/key.pem"
            assert "password" not in call_kwargs

    def test_missing_credentials_raises(self, mock_resolved_source, mock_config, mock_snowflake_module):
        """Test missing credentials raises ValueError."""
        resolved = mock_resolved_source(
            source_type="snowflake",
            connection={"account": "test-account", "warehouse": "WH", "database": "DB"},
            query="SELECT 1",
        )
        config = mock_config(
            snowflake_user=None,
            snowflake_password=None,
            snowflake_private_key_path=None,
        )

        mock_conn = MagicMock()
        mock_snowflake, mock_sf = mock_snowflake_module(mock_conn)

        for key in list(sys.modules.keys()):
            if key.startswith("snowflake"):
                del sys.modules[key]

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_sf}):
            adapter = SnowflakeAdapter()
            with pytest.raises(ValueError, match="Snowflake credentials not configured"):
                adapter.fetch(resolved, config)


class TestSnowflakeAdapterFetch:
    """Tests for Snowflake fetch operation."""

    def test_fetch_returns_list_of_dicts(
        self, mock_resolved_source, mock_config, mock_snowflake_module
    ):
        """Test fetch returns list of dicts."""
        resolved = mock_resolved_source(
            source_type="snowflake",
            connection={"account": "test", "warehouse": "WH", "database": "DB"},
            query="SELECT id, name FROM users",
        )
        config = mock_config(snowflake_user="user", snowflake_password="pass")

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("ID", None), ("NAME", None)]
        mock_cursor.fetchall.return_value = [(1, "Alice"), (2, "Bob")]
        mock_conn.cursor.return_value = mock_cursor

        mock_snowflake, mock_sf = mock_snowflake_module(mock_conn)

        for key in list(sys.modules.keys()):
            if key.startswith("snowflake"):
                del sys.modules[key]

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_sf}):
            adapter = SnowflakeAdapter()
            result = adapter.fetch(resolved, config)

            assert result == [
                {"ID": 1, "NAME": "Alice"},
                {"ID": 2, "NAME": "Bob"},
            ]

    def test_fetch_closes_connection(self, mock_resolved_source, mock_config, mock_snowflake_module):
        """Test fetch closes connection after use."""
        resolved = mock_resolved_source(
            source_type="snowflake",
            connection={"account": "test", "warehouse": "WH", "database": "DB"},
            query="SELECT 1",
        )
        config = mock_config(snowflake_user="user", snowflake_password="pass")

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("1", None)]
        mock_cursor.fetchall.return_value = [(1,)]
        mock_conn.cursor.return_value = mock_cursor

        mock_snowflake, mock_sf = mock_snowflake_module(mock_conn)

        for key in list(sys.modules.keys()):
            if key.startswith("snowflake"):
                del sys.modules[key]

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_sf}):
            adapter = SnowflakeAdapter()
            adapter.fetch(resolved, config)

            mock_cursor.close.assert_called_once()
            mock_conn.close.assert_called_once()

    def test_fetch_executes_query(self, mock_resolved_source, mock_config, mock_snowflake_module):
        """Test fetch executes the provided query."""
        resolved = mock_resolved_source(
            source_type="snowflake",
            connection={"account": "test", "warehouse": "WH", "database": "DB"},
            query="SELECT * FROM my_table WHERE status = 'active'",
        )
        config = mock_config(snowflake_user="user", snowflake_password="pass")

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("COL1", None)]
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cursor

        mock_snowflake, mock_sf = mock_snowflake_module(mock_conn)

        for key in list(sys.modules.keys()):
            if key.startswith("snowflake"):
                del sys.modules[key]

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_sf}):
            adapter = SnowflakeAdapter()
            adapter.fetch(resolved, config)

            mock_cursor.execute.assert_called_once_with(
                "SELECT * FROM my_table WHERE status = 'active'"
            )

    def test_fetch_missing_query_raises(self, mock_resolved_source, mock_config, mock_snowflake_module):
        """Test fetch raises without query."""
        resolved = mock_resolved_source(
            source_type="snowflake",
            connection={"account": "test", "warehouse": "WH", "database": "DB"},
            query=None,
        )
        config = mock_config(snowflake_user="user", snowflake_password="pass")

        mock_conn = MagicMock()
        mock_snowflake, mock_sf = mock_snowflake_module(mock_conn)

        for key in list(sys.modules.keys()):
            if key.startswith("snowflake"):
                del sys.modules[key]

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_sf}):
            adapter = SnowflakeAdapter()
            with pytest.raises(ValueError, match="No query provided"):
                adapter.fetch(resolved, config)


class TestSnowflakeAdapterListChildren:
    """Tests for Snowflake list_children (SHOW TABLES)."""

    def test_list_children_returns_tables(self, mock_resolved_source, mock_config, mock_snowflake_module):
        """Test list_children returns table names."""
        resolved = mock_resolved_source(
            source_type="snowflake",
            connection={"account": "test", "warehouse": "WH", "database": "DB"},
        )
        config = mock_config(snowflake_user="user", snowflake_password="pass")

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        # SHOW TABLES returns name in second column
        mock_cursor.fetchall.return_value = [
            ("created", "users", "TABLE", "DB", "PUBLIC"),
            ("created", "orders", "TABLE", "DB", "PUBLIC"),
            ("created", "products", "TABLE", "DB", "PUBLIC"),
        ]
        mock_conn.cursor.return_value = mock_cursor

        mock_snowflake, mock_sf = mock_snowflake_module(mock_conn)

        for key in list(sys.modules.keys()):
            if key.startswith("snowflake"):
                del sys.modules[key]

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_sf}):
            adapter = SnowflakeAdapter()
            result = adapter.list_children(resolved, config)

            assert result == ["users", "orders", "products"]
            mock_cursor.execute.assert_called_once_with("SHOW TABLES")

    def test_list_children_no_credentials_returns_empty(self, mock_resolved_source, mock_config, mock_snowflake_module):
        """Test list_children returns empty list without credentials."""
        resolved = mock_resolved_source(
            source_type="snowflake",
            connection={"account": "test", "warehouse": "WH", "database": "DB"},
        )
        config = mock_config(snowflake_user=None, snowflake_password=None)

        mock_conn = MagicMock()
        mock_snowflake, mock_sf = mock_snowflake_module(mock_conn)

        for key in list(sys.modules.keys()):
            if key.startswith("snowflake"):
                del sys.modules[key]

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_sf}):
            adapter = SnowflakeAdapter()
            result = adapter.list_children(resolved, config)

            assert result == []

    def test_list_children_error_returns_empty(self, mock_resolved_source, mock_config, mock_snowflake_module):
        """Test list_children returns empty list on error."""
        resolved = mock_resolved_source(
            source_type="snowflake",
            connection={"account": "test", "warehouse": "WH", "database": "DB"},
        )
        config = mock_config(snowflake_user="user", snowflake_password="pass")

        mock_conn = MagicMock()
        mock_snowflake, mock_sf = mock_snowflake_module(mock_conn)
        mock_sf.connect.side_effect = Exception("Connection failed")

        for key in list(sys.modules.keys()):
            if key.startswith("snowflake"):
                del sys.modules[key]

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_sf}):
            adapter = SnowflakeAdapter()
            result = adapter.list_children(resolved, config)

            assert result == []


class TestSnowflakeAdapterErrorHandling:
    """Tests for Snowflake error handling."""

    def test_import_error_without_snowflake(self, mock_resolved_source, mock_config):
        """Test ImportError when snowflake-connector not installed."""
        # Skip if snowflake is actually installed
        try:
            import snowflake.connector
            pytest.skip("snowflake-connector-python is installed, cannot test ImportError")
        except ImportError:
            pass

        resolved = mock_resolved_source(
            source_type="snowflake",
            connection={"account": "test", "warehouse": "WH", "database": "DB"},
            query="SELECT 1",
        )
        config = mock_config(snowflake_user="user", snowflake_password="pass")

        adapter = SnowflakeAdapter()
        with pytest.raises(ImportError, match="snowflake-connector-python required"):
            adapter.fetch(resolved, config)

    def test_connection_error_propagates(self, mock_resolved_source, mock_config, mock_snowflake_module):
        """Test connection errors propagate."""
        resolved = mock_resolved_source(
            source_type="snowflake",
            connection={"account": "test", "warehouse": "WH", "database": "DB"},
            query="SELECT 1",
        )
        config = mock_config(snowflake_user="user", snowflake_password="pass")

        mock_conn = MagicMock()
        mock_snowflake, mock_sf = mock_snowflake_module(mock_conn)
        mock_sf.connect.side_effect = Exception("250001: Could not connect to Snowflake")

        for key in list(sys.modules.keys()):
            if key.startswith("snowflake"):
                del sys.modules[key]

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_sf}):
            adapter = SnowflakeAdapter()
            with pytest.raises(Exception, match="Could not connect"):
                adapter.fetch(resolved, config)

    def test_query_execution_error_propagates(self, mock_resolved_source, mock_config, mock_snowflake_module):
        """Test query execution errors propagate."""
        resolved = mock_resolved_source(
            source_type="snowflake",
            connection={"account": "test", "warehouse": "WH", "database": "DB"},
            query="SELECT * FROM nonexistent_table",
        )
        config = mock_config(snowflake_user="user", snowflake_password="pass")

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("002003: SQL compilation error")
        mock_conn.cursor.return_value = mock_cursor

        mock_snowflake, mock_sf = mock_snowflake_module(mock_conn)

        for key in list(sys.modules.keys()):
            if key.startswith("snowflake"):
                del sys.modules[key]

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_sf}):
            adapter = SnowflakeAdapter()
            with pytest.raises(Exception, match="SQL compilation error"):
                adapter.fetch(resolved, config)

    def test_connection_closed_on_error(self, mock_resolved_source, mock_config, mock_snowflake_module):
        """Test connection is closed even on error."""
        resolved = mock_resolved_source(
            source_type="snowflake",
            connection={"account": "test", "warehouse": "WH", "database": "DB"},
            query="SELECT 1",
        )
        config = mock_config(snowflake_user="user", snowflake_password="pass")

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("Query failed")
        mock_conn.cursor.return_value = mock_cursor

        mock_snowflake, mock_sf = mock_snowflake_module(mock_conn)

        for key in list(sys.modules.keys()):
            if key.startswith("snowflake"):
                del sys.modules[key]

        with patch.dict("sys.modules", {"snowflake": mock_snowflake, "snowflake.connector": mock_sf}):
            adapter = SnowflakeAdapter()
            with pytest.raises(Exception):
                adapter.fetch(resolved, config)

            mock_conn.close.assert_called_once()
