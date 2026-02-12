"""Mock MS-SQL adapter for demos and testing.

Uses SQLite in-memory to simulate SQL Server responses.
"""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from typing import Any, TYPE_CHECKING
import random
import re

if TYPE_CHECKING:
    from ..client import ResolvedSource
    from ..config import ClientConfig

from .base import BaseAdapter


# Sample data configuration
DEPARTMENTS = ["Engineering", "Sales", "Marketing", "Finance", "Operations"]

EMPLOYEES = [
    ("Alice Johnson", "Engineering", "Senior Engineer"),
    ("Bob Smith", "Sales", "Account Manager"),
    ("Carol White", "Marketing", "Campaign Lead"),
    ("David Brown", "Finance", "Analyst"),
    ("Eve Davis", "Operations", "Operations Manager"),
    ("Frank Miller", "Engineering", "Staff Engineer"),
    ("Grace Wilson", "Sales", "Sales Director"),
    ("Henry Taylor", "Marketing", "Content Strategist"),
]

PRODUCTS = [
    ("PROD-001", "Widget A", "Hardware", 29.99),
    ("PROD-002", "Widget B", "Hardware", 49.99),
    ("PROD-003", "Service Plan Basic", "Service", 9.99),
    ("PROD-004", "Service Plan Pro", "Service", 29.99),
    ("PROD-005", "Gadget X", "Hardware", 199.99),
    ("PROD-006", "Analytics Suite", "Software", 499.99),
]


class MockMSSQLAdapter(BaseAdapter):
    """
    Mock MS-SQL adapter using SQLite for demos.

    This adapter doesn't require real SQL Server credentials - it generates
    sample enterprise data for demonstration purposes.
    """

    _db: sqlite3.Connection | None = None

    def __init__(self):
        self._ensure_db()

    def _ensure_db(self) -> sqlite3.Connection:
        """Ensure database is initialized with sample data."""
        if MockMSSQLAdapter._db is None:
            MockMSSQLAdapter._db = self._create_mock_db()
        return MockMSSQLAdapter._db

    def _create_mock_db(self) -> sqlite3.Connection:
        """Create an in-memory SQLite database with sample data."""
        conn = sqlite3.connect(":memory:", check_same_thread=False)
        conn.row_factory = sqlite3.Row

        # Create employees table
        conn.execute("""
            CREATE TABLE dbo_employees (
                employee_id INTEGER PRIMARY KEY,
                full_name TEXT,
                department TEXT,
                title TEXT,
                hire_date TEXT,
                salary REAL,
                is_active INTEGER
            )
        """)

        # Create orders table
        conn.execute("""
            CREATE TABLE dbo_orders (
                order_id INTEGER PRIMARY KEY,
                product_code TEXT,
                product_name TEXT,
                category TEXT,
                quantity INTEGER,
                unit_price REAL,
                order_date TEXT,
                customer_region TEXT
            )
        """)

        # Generate employee data
        random.seed(42)
        base_date = date(2020, 1, 15)
        emp_rows = []
        for i, (name, dept, title) in enumerate(EMPLOYEES, start=1):
            hire_date = base_date + timedelta(days=random.randint(0, 1000))
            salary = random.uniform(60000, 180000)
            emp_rows.append((
                i, name, dept, title,
                hire_date.strftime("%Y-%m-%d"),
                round(salary, 2),
                1,
            ))

        conn.executemany("""
            INSERT INTO dbo_employees
            (employee_id, full_name, department, title, hire_date, salary, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, emp_rows)

        # Generate order data
        regions = ["East", "West", "Central", "South"]
        order_rows = []
        order_base = date(2024, 1, 1)
        for order_id in range(1, 201):
            prod = random.choice(PRODUCTS)
            order_date = order_base + timedelta(days=random.randint(0, 365))
            qty = random.randint(1, 50)
            order_rows.append((
                order_id,
                prod[0],
                prod[1],
                prod[2],
                qty,
                prod[3],
                order_date.strftime("%Y-%m-%d"),
                random.choice(regions),
            ))

        conn.executemany("""
            INSERT INTO dbo_orders
            (order_id, product_code, product_name, category, quantity,
             unit_price, order_date, customer_region)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, order_rows)

        conn.commit()
        print(f"[MockMSSQL] Initialized with {len(emp_rows)} employees, {len(order_rows)} orders")
        return conn

    def _translate_mssql_to_sqlite(self, query: str) -> str:
        """Translate MS-SQL T-SQL syntax to SQLite."""
        translated = query

        # Remove dbo. prefix (e.g., dbo.employees -> dbo_employees)
        translated = re.sub(r"\bdbo\.", "dbo_", translated)

        # Handle GETDATE() -> date('now')
        translated = re.sub(r"\bGETDATE\s*\(\s*\)", "date('now')", translated, flags=re.IGNORECASE)

        # Handle CAST(expr AS DATE) -> date(expr)
        translated = re.sub(
            r"\bCAST\s*\(\s*(.+?)\s+AS\s+DATE\s*\)",
            r"date(\1)",
            translated,
            flags=re.IGNORECASE,
        )

        # Handle CONVERT(DATE, 'YYYYMMDD', 112) -> date literal
        translated = re.sub(
            r"\bCONVERT\s*\(\s*DATE\s*,\s*'(\d{8})'\s*,\s*112\s*\)",
            lambda m: f"'{m.group(1)[:4]}-{m.group(1)[4:6]}-{m.group(1)[6:8]}'",
            translated,
            flags=re.IGNORECASE,
        )

        # Handle DATEADD(unit, -N, expr) -> date(expr, 'N unit')
        def _dateadd_to_sqlite(m):
            unit = m.group(1).upper()
            offset = m.group(2).strip()
            expr = m.group(3).strip()
            unit_map = {"YEAR": "years", "MONTH": "months", "WEEK": "days", "DAY": "days"}
            sqlite_unit = unit_map.get(unit, "days")
            # Parse the offset (e.g., -3)
            try:
                val = int(offset)
            except ValueError:
                val = offset
            if unit == "WEEK" and isinstance(val, int):
                val = val * 7
            return f"date({expr}, '{val} {sqlite_unit}')"

        translated = re.sub(
            r"\bDATEADD\s*\(\s*(\w+)\s*,\s*(-?\d+)\s*,\s*(.+?)\s*\)",
            _dateadd_to_sqlite,
            translated,
            flags=re.IGNORECASE,
        )

        # Handle ISNULL -> COALESCE
        translated = re.sub(r"\bISNULL\s*\(", "COALESCE(", translated, flags=re.IGNORECASE)

        return translated

    def fetch(
        self,
        resolved: ResolvedSource,
        config: ClientConfig,
        **kwargs,
    ) -> Any:
        """Execute query against mock database."""
        query = resolved.query
        if not query:
            raise ValueError("No query provided for MS-SQL source")

        # Translate T-SQL syntax to SQLite
        sqlite_query = self._translate_mssql_to_sqlite(query)

        db = self._ensure_db()
        cursor = db.cursor()

        try:
            cursor.execute(sqlite_query)
            columns = [desc[0].upper() for desc in cursor.description]
            rows = cursor.fetchall()

            # Convert to list of dicts
            result = [dict(zip(columns, tuple(row))) for row in rows]
            print(f"[MockMSSQL] Query returned {len(result)} rows")
            return result

        except Exception as e:
            print(f"[MockMSSQL] Query error: {e}")
            print(f"[MockMSSQL] Query was: {sqlite_query[:200]}...")
            raise

    def list_children(
        self,
        resolved: ResolvedSource,
        config: ClientConfig,
    ) -> list[str]:
        """List available 'tables'."""
        return ["dbo_employees", "dbo_orders"]


def enable_mock_mssql():
    """
    Replace the MS-SQL adapter with the mock adapter.

    Call this at the start of your demo script:

        from moniker_client.adapters.mock_mssql import enable_mock_mssql
        enable_mock_mssql()
    """
    from . import register_adapter
    register_adapter("mssql", MockMSSQLAdapter())
    print("[MockMSSQL] Mock MS-SQL adapter enabled")
