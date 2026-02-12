#!/usr/bin/env python
"""Moniker Simulated Environment — one-command bootstrap.

Usage:
    python bring_up.py              # Boot, smoke test, print summary, exit
    python bring_up.py --server     # Boot, smoke test, keep server running
    python bring_up.py --check      # CI mode: exit 0 if all tests pass, 1 otherwise
    python bring_up.py --port 9090  # Custom port

Importable:
    from bring_up import boot_environment, get_client
"""
from __future__ import annotations

import argparse
import sys
import threading
import time
import urllib.request
import urllib.error
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# sys.path — make moniker-svc and moniker-data importable
# ---------------------------------------------------------------------------
_SVC_SRC = Path.home() / "open-moniker-svc" / "src"
_DATA_SRC = Path.home() / "open-moniker-svc" / "external" / "moniker-data" / "src"
_CLIENT_ROOT = Path.home() / "open-moniker-client"

for p in (_SVC_SRC, _DATA_SRC, _CLIENT_ROOT):
    sp = str(p)
    if p.exists() and sp not in sys.path:
        sys.path.insert(0, sp)

import os
os.environ.setdefault("DOMAINS_CONFIG", str(Path.home() / "open-moniker-svc" / "domains.yaml"))

# ---------------------------------------------------------------------------
# Colorama fallback (same pattern as open-moniker-svc/demo.py)
# ---------------------------------------------------------------------------
try:
    from colorama import Fore, Style, init as colorama_init
    colorama_init()
except ImportError:
    class Fore:
        CYAN = YELLOW = GREEN = MAGENTA = BLUE = RED = WHITE = ""
    class Style:
        RESET_ALL = BRIGHT = DIM = ""

# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------
@dataclass
class AdapterInfo:
    name: str
    engine: str
    tables: int
    rows: int
    description: str


@dataclass
class SmokeResult:
    name: str
    passed: bool
    detail: str = ""


@dataclass
class EnvironmentInfo:
    port: int
    adapters: list[AdapterInfo] = field(default_factory=list)
    smoke_results: list[SmokeResult] = field(default_factory=list)
    catalog_monikers: int = 0
    catalog_source_types: int = 0
    catalog_domains: int = 0

# ---------------------------------------------------------------------------
# Adapter warming
# ---------------------------------------------------------------------------

def _warm_oracle() -> AdapterInfo:
    from moniker_data.adapters.oracle import execute_query
    rows = execute_query("SELECT COUNT(*) AS CNT FROM te_stress_tail_risk_pnl")
    count = rows[0]["CNT"] if rows else 0
    return AdapterInfo("Oracle", "SQLite", 1, count, "CVaR risk data")


def _warm_snowflake() -> AdapterInfo:
    from moniker_data.adapters.snowflake import MockSnowflakeAdapter
    sf = MockSnowflakeAdapter()
    total = 0
    table_count = 0
    for tbl in sf.list_tables():
        r = sf.execute(f"SELECT COUNT(*) AS CNT FROM {tbl}")
        total += r[0]["CNT"] if r else 0
        table_count += 1
    return AdapterInfo("Snowflake", "SQLite", table_count, total, "Govies, rates, sovereign")


def _warm_mssql() -> AdapterInfo:
    from moniker_data.adapters.mssql import execute_query
    r1 = execute_query("SELECT COUNT(*) AS CNT FROM credit_exposures")
    r2 = execute_query("SELECT COUNT(*) AS CNT FROM credit_limits")
    cnt1 = r1[0]["CNT"] if r1 else 0
    cnt2 = r2[0]["CNT"] if r2 else 0
    return AdapterInfo("MS-SQL", "SQLite", 2, cnt1 + cnt2, "Credit exposures & limits")


def _warm_rest() -> AdapterInfo:
    from moniker_data.adapters.rest import MockRestAdapter
    rest = MockRestAdapter()
    energy = rest.get_energy()
    metals = rest.get_metals()
    return AdapterInfo("REST", "in-memory", 2, len(energy) + len(metals), "Energy & metals commodities")


def _warm_excel() -> AdapterInfo:
    from moniker_data.adapters.excel import MockExcelAdapter
    xl = MockExcelAdapter()
    pools = xl.get_pool_data()
    return AdapterInfo("Excel", "in-memory", 3, len(pools), "MBS pool-level data")


def warm_adapters() -> list[AdapterInfo]:
    """Pre-initialize all mock data stores and return inventory info."""
    results = []
    for fn in (_warm_oracle, _warm_snowflake, _warm_mssql, _warm_rest, _warm_excel):
        try:
            results.append(fn())
        except Exception as exc:
            results.append(AdapterInfo(fn.__name__, "error", 0, 0, str(exc)))
    return results

# ---------------------------------------------------------------------------
# Server management
# ---------------------------------------------------------------------------

def start_server(port: int = 8050, timeout: float = 15.0) -> None:
    """Start the FastAPI server in a daemon thread and wait until healthy."""
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        import uvicorn
        from moniker_svc.main import app  # noqa: F811

    def _run():
        uvicorn.run(app, host="127.0.0.1", port=port, log_level="warning")

    t = threading.Thread(target=_run, daemon=True, name="moniker-svc")
    t.start()

    # Poll /health until ready
    url = f"http://127.0.0.1:{port}/health"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=2) as resp:
                if resp.status == 200:
                    return
        except Exception:
            pass
        time.sleep(0.3)

    raise RuntimeError(f"Server did not become healthy within {timeout}s")

# ---------------------------------------------------------------------------
# HTTP helpers (stdlib only — no httpx dependency for the bootstrap script)
# ---------------------------------------------------------------------------

def _get(url: str, timeout: float = 10.0) -> tuple[int, dict | list | None]:
    """GET *url*, return (status_code, parsed_json_or_None)."""
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = json.loads(resp.read().decode())
            return resp.status, body
    except urllib.error.HTTPError as exc:
        try:
            body = json.loads(exc.read().decode())
        except Exception:
            body = None
        return exc.code, body
    except Exception:
        return 0, None

# ---------------------------------------------------------------------------
# Smoke tests
# ---------------------------------------------------------------------------

def run_smoke_tests(base_url: str) -> list[SmokeResult]:
    """Hit key API endpoints and return pass/fail results."""
    results: list[SmokeResult] = []

    def _test(name: str, url: str, check):
        status, body = _get(url)
        try:
            ok = check(status, body)
            results.append(SmokeResult(name, ok, f"HTTP {status}"))
        except Exception as exc:
            results.append(SmokeResult(name, False, str(exc)))

    # 1. Health
    _test("Health check",
          f"{base_url}/health",
          lambda s, b: s == 200 and b.get("status") == "healthy")

    # 2. Catalog stats
    _test("Catalog stats",
          f"{base_url}/catalog/stats",
          lambda s, b: s == 200 and b.get("total_monikers", 0) > 0)

    # 3. Catalog search
    _test("Catalog search",
          f"{base_url}/catalog/search?q=credit",
          lambda s, b: s == 200 and len(b.get("results", [])) > 0)

    # 4. Fetch: MS-SQL (credit.exposures)
    _test("Fetch: MS-SQL",
          f"{base_url}/fetch/credit.exposures?limit=5",
          lambda s, b: s == 200 and len(b.get("data", [])) > 0)

    # 5. Fetch: MS-SQL (credit.limits)
    _test("Fetch: credit.limits",
          f"{base_url}/fetch/credit.limits?limit=5",
          lambda s, b: s == 200 and len(b.get("data", [])) > 0)

    # 6. Fetch: Excel (reports/regulatory)
    _test("Fetch: Excel",
          f"{base_url}/fetch/reports/regulatory/2026Q1/summary?limit=5",
          lambda s, b: s == 200 and isinstance(b.get("data"), list))

    # 7. Fetch: REST (commodities.derivatives)
    _test("Fetch: REST",
          f"{base_url}/fetch/commodities.derivatives/energy/CL?limit=5",
          lambda s, b: s == 200 and isinstance(b.get("data"), list))

    # 8. Metadata
    _test("Metadata",
          f"{base_url}/metadata/credit.exposures",
          lambda s, b: s == 200 and b.get("schema") is not None)

    # 9. Resolve: MS-SQL
    _test("Resolve: MS-SQL",
          f"{base_url}/resolve/credit.exposures",
          lambda s, b: s == 200 and b.get("source_type") == "mssql")

    # 10. Resolve: Snowflake
    _test("Resolve: Snowflake",
          f"{base_url}/resolve/prices.equity/AAPL",
          lambda s, b: s == 200 and b.get("source_type") == "snowflake")

    # 11. Describe
    _test("Describe",
          f"{base_url}/describe/credit",
          lambda s, b: s == 200 and "ownership" in (b or {}))

    # 12. Lineage
    _test("Lineage",
          f"{base_url}/lineage/credit.exposures",
          lambda s, b: s == 200 and "ownership" in (b or {}))

    return results

# ---------------------------------------------------------------------------
# Pretty-print summary
# ---------------------------------------------------------------------------

def print_summary(env: EnvironmentInfo) -> None:
    """Print the professional summary banner."""
    B = Style.BRIGHT
    R = Style.RESET_ALL
    C = Fore.CYAN
    G = Fore.GREEN
    Y = Fore.YELLOW
    M = Fore.MAGENTA
    W = Fore.WHITE
    RED = Fore.RED

    print()
    print(f"{B}{C}{'=' * 63}{R}")
    print(f"{B}{C}  MONIKER — Simulated Environment{R}")
    print(f"{B}{C}{'=' * 63}{R}")

    # -- Data Inventory --
    print(f"\n  {B}{W}DATA INVENTORY{R}")
    print(f"  {'─' * 50}")
    total_rows = 0
    for a in env.adapters:
        unit = "tables" if a.engine == "SQLite" else ("feeds" if a.name == "REST" else "agencies")
        rows_label = f"~{a.rows:,} {'rows' if a.engine == 'SQLite' else ('items' if a.name == 'REST' else 'pools')}"
        print(f"  {G}{a.name:<18}{R} ({a.engine})  {a.tables} {unit:<10} {rows_label:<16} {Style.DIM}{a.description}{R}")
        total_rows += a.rows
    print(f"  {'':18}               {'─' * 12}")
    print(f"  {'':18}  Total        {B}{W}~{total_rows:,} rows{R}")

    # -- Catalog --
    print(f"\n  {B}{W}CATALOG{R}")
    print(f"  {'─' * 50}")
    print(f"  {env.catalog_monikers} monikers | {env.catalog_source_types} source types | {env.catalog_domains} domains")

    # -- Smoke Tests --
    print(f"\n  {B}{W}SMOKE TESTS{R}")
    print(f"  {'─' * 50}")
    passed = [r for r in env.smoke_results if r.passed]
    failed = [r for r in env.smoke_results if not r.passed]

    # Print in rows of 3
    names = []
    for r in env.smoke_results:
        mark = f"{G}✓{R}" if r.passed else f"{RED}✗{R}"
        names.append(f"{mark} {r.name}")

    for i in range(0, len(names), 3):
        row = names[i:i + 3]
        print("  " + "   ".join(f"{n:<24}" for n in row))

    color = G if not failed else RED
    print(f"  {color}{B}{len(passed)}/{len(env.smoke_results)} passed{R}")

    if failed:
        print(f"\n  {RED}Failed:{R}")
        for r in failed:
            print(f"    {RED}✗{R} {r.name}: {r.detail}")

    # -- Server --
    base = f"http://localhost:{env.port}"
    print(f"\n  {B}{W}SERVER: {C}{base}{R}")
    print(f"  {'─' * 50}")
    print(f"  API docs:    {C}{base}/docs{R}")
    print(f"  Health:      {C}{base}/health{R}")
    print(f"  Catalog:     {C}{base}/catalog/stats{R}")
    print()

# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def boot_environment(port: int = 8050) -> EnvironmentInfo:
    """Boot the full simulated environment. Importable from notebooks/scripts."""
    env = EnvironmentInfo(port=port)

    # 1. Warm adapters
    print(f"\n{Style.BRIGHT}{Fore.CYAN}[1/3]{Style.RESET_ALL} Warming data adapters …")
    env.adapters = warm_adapters()
    for a in env.adapters:
        status = f"{Fore.GREEN}✓{Style.RESET_ALL}" if a.rows > 0 else f"{Fore.RED}✗{Style.RESET_ALL}"
        print(f"  {status} {a.name}: {a.rows:,} rows")

    # 2. Start server
    print(f"\n{Style.BRIGHT}{Fore.CYAN}[2/3]{Style.RESET_ALL} Starting FastAPI server on port {port} …")
    start_server(port=port)
    print(f"  {Fore.GREEN}✓{Style.RESET_ALL} Server healthy")

    # Grab catalog stats
    base_url = f"http://127.0.0.1:{port}"
    status, stats = _get(f"{base_url}/catalog/stats")
    if status == 200 and stats:
        env.catalog_monikers = stats.get("total_monikers", 0)
        env.catalog_source_types = len(stats.get("by_source_type", {}))
        env.catalog_domains = len(stats.get("by_status", {}))
        # Try to get domain count from the by_source_type keys or use a heuristic
        # The "domains" count is best estimated from top-level catalog paths
        _, catalog_body = _get(f"{base_url}/catalog?limit=200")
        if catalog_body and "paths" in catalog_body:
            domains = {p.split(".")[0].split("/")[0] for p in catalog_body["paths"] if p}
            env.catalog_domains = len(domains)

    # 3. Smoke tests
    print(f"\n{Style.BRIGHT}{Fore.CYAN}[3/3]{Style.RESET_ALL} Running smoke tests …")
    env.smoke_results = run_smoke_tests(base_url)
    passed = sum(1 for r in env.smoke_results if r.passed)
    total = len(env.smoke_results)
    color = Fore.GREEN if passed == total else Fore.RED
    print(f"  {color}{passed}/{total} passed{Style.RESET_ALL}")

    return env

# ---------------------------------------------------------------------------
# Client helper
# ---------------------------------------------------------------------------

def get_client(port: int = 8050):
    """Return a MonikerClient pointed at the running server."""
    from moniker_client.config import ClientConfig
    from moniker_client.client import MonikerClient
    return MonikerClient(config=ClientConfig(service_url=f"http://localhost:{port}"))

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Boot the Moniker simulated environment",
    )
    parser.add_argument("--server", action="store_true",
                        help="Keep the server running after smoke tests (Ctrl+C to stop)")
    parser.add_argument("--check", action="store_true",
                        help="CI mode: exit 0 if all smoke tests pass, 1 otherwise")
    parser.add_argument("--port", type=int, default=8050,
                        help="Server port (default: 8050)")
    args = parser.parse_args()

    env = boot_environment(port=args.port)
    print_summary(env)

    all_passed = all(r.passed for r in env.smoke_results)

    if args.check:
        sys.exit(0 if all_passed else 1)

    if args.server:
        print(f"  Ready for demo. Press Ctrl+C to stop.\n")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print(f"\n{Fore.CYAN}Shutting down.{Style.RESET_ALL}")
    else:
        if not all_passed:
            sys.exit(1)


if __name__ == "__main__":
    main()
