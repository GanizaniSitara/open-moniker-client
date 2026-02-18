#!/usr/bin/env python
"""Launch Moniker service + Jupyter notebook in one command.

Works on Windows, macOS, and Linux.

Usage:
    python launch.py              # Start service + Jupyter, open browser
    python launch.py --port 9090  # Use custom port
"""
import argparse
import os
import sys
import subprocess
import time
import webbrowser
from pathlib import Path

# Colors (fallback-safe)
try:
    from colorama import Fore, Style, init as colorama_init
    colorama_init()
except ImportError:
    class Fore:
        CYAN = GREEN = YELLOW = RED = WHITE = ""
    class Style:
        RESET_ALL = BRIGHT = ""

def main():
    parser = argparse.ArgumentParser(description="Launch Moniker service + Jupyter")
    parser.add_argument("--port", type=int, default=8050, help="Service port (default: 8050)")
    parser.add_argument("--jupyter-port", type=int, default=8888, help="Jupyter port (default: 8888)")
    parser.add_argument("--no-browser", action="store_true", help="Don't open browser automatically")
    args = parser.parse_args()

    print(f"{Style.BRIGHT}{Fore.CYAN}{'=' * 60}{Style.RESET_ALL}")
    print(f"{Style.BRIGHT}{Fore.CYAN}  MONIKER — Full Environment Launcher{Style.RESET_ALL}")
    print(f"{Style.BRIGHT}{Fore.CYAN}{'=' * 60}{Style.RESET_ALL}\n")

    # Setup paths
    home = Path.home()
    svc_src = home / "open-moniker-svc" / "src"
    data_src = home / "open-moniker-svc" / "external" / "moniker-data" / "src"
    client_root = home / "open-moniker-client"
    notebook_dir = client_root / "notebooks"

    # Set PYTHONPATH for subprocess
    pythonpath_parts = [str(svc_src), str(data_src), str(client_root)]
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(pythonpath_parts)

    print(f"{Style.BRIGHT}[1/2]{Style.RESET_ALL} Starting Moniker service on port {args.port}...")

    # Start service using bring_up.py --server
    service_cmd = [sys.executable, str(client_root / "bring_up.py"), "--server", "--port", str(args.port)]
    service_proc = subprocess.Popen(
        service_cmd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        cwd=str(client_root),
        text=True,
        bufsize=1
    )

    # Wait for service startup by reading output
    startup_complete = False
    for line in service_proc.stdout:
        print(f"  {line.rstrip()}")
        if "Ready for demo" in line or "SERVER:" in line:
            startup_complete = True
            break
        if service_proc.poll() is not None:
            print(f"{Fore.RED}✗ Service failed to start{Style.RESET_ALL}")
            return 1

    if startup_complete:
        print(f"  {Fore.GREEN}✓ Service ready on http://localhost:{args.port}{Style.RESET_ALL}\n")

    print(f"{Style.BRIGHT}[2/2]{Style.RESET_ALL} Starting Jupyter notebook on port {args.jupyter_port}...")

    # Start Jupyter
    jupyter_cmd = [
        sys.executable, "-m", "jupyter", "notebook",
        "--ip=0.0.0.0",
        f"--port={args.jupyter_port}",
        "--NotebookApp.token=",
        "--NotebookApp.password=",
        f"--notebook-dir={notebook_dir}",
        "--no-browser" if args.no_browser else ""
    ]
    jupyter_cmd = [c for c in jupyter_cmd if c]  # Remove empty strings

    jupyter_proc = subprocess.Popen(
        jupyter_cmd,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )

    # Give Jupyter a moment to start
    time.sleep(2)
    print(f"  {Fore.GREEN}✓ Jupyter ready on http://localhost:{args.jupyter_port}{Style.RESET_ALL}\n")

    # Open browser to notebook
    notebook_url = f"http://localhost:{args.jupyter_port}/notebooks/practical_workflows.ipynb"
    if not args.no_browser:
        print(f"{Style.BRIGHT}Opening browser...{Style.RESET_ALL}")
        webbrowser.open(notebook_url)

    print(f"\n{Style.BRIGHT}{Fore.GREEN}✓ All services running{Style.RESET_ALL}")
    print(f"\n  {Style.BRIGHT}Service:{Style.RESET_ALL}  {Fore.CYAN}http://localhost:{args.port}{Style.RESET_ALL}")
    print(f"  {Style.BRIGHT}Jupyter:{Style.RESET_ALL}  {Fore.CYAN}{notebook_url}{Style.RESET_ALL}")
    print(f"  {Style.BRIGHT}API Docs:{Style.RESET_ALL} {Fore.CYAN}http://localhost:{args.port}/docs{Style.RESET_ALL}")
    print(f"\n{Fore.YELLOW}Press Ctrl+C to stop all services{Style.RESET_ALL}\n")

    # Keep running and monitor processes
    try:
        while True:
            time.sleep(1)
            # Check if processes are still alive
            if service_proc.poll() is not None:
                print(f"\n{Fore.RED}✗ Service stopped unexpectedly{Style.RESET_ALL}")
                break
            if jupyter_proc.poll() is not None:
                print(f"\n{Fore.RED}✗ Jupyter stopped unexpectedly{Style.RESET_ALL}")
                break
    except KeyboardInterrupt:
        print(f"\n\n{Fore.CYAN}Shutting down...{Style.RESET_ALL}")
        service_proc.terminate()
        jupyter_proc.terminate()
        service_proc.wait(timeout=5)
        jupyter_proc.wait(timeout=5)
        print(f"{Fore.GREEN}✓ Clean shutdown{Style.RESET_ALL}\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
