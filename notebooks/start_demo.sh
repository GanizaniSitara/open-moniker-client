#!/usr/bin/env bash
# start_demo.sh — Start the Moniker service and launch Jupyter
#
# Usage:
#   ./start_demo.sh            # starts server + jupyter
#   ./start_demo.sh --no-jupyter  # starts server only
#
set -euo pipefail

SVC_DIR="${HOME}/open-moniker-svc"
NOTEBOOK_DIR="$(cd "$(dirname "$0")" && pwd)"
PORT=8050
SERVER_PID=""

cleanup() {
    echo ""
    echo "Shutting down..."
    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID"
        echo "Server (PID $SERVER_PID) stopped."
    fi
}
trap cleanup EXIT INT TERM

# Start the Moniker service in the background
echo "Starting Moniker service on port ${PORT}..."
cd "$SVC_DIR"
PYTHONPATH=src:external/moniker-data/src \
    python -m uvicorn moniker_svc.main:app --host 0.0.0.0 --port "$PORT" &
SERVER_PID=$!

# Wait for the service to become healthy
echo "Waiting for /health..."
for i in $(seq 1 30); do
    if curl -sf "http://localhost:${PORT}/health" > /dev/null 2>&1; then
        echo "Service is healthy."
        break
    fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "ERROR: Server process exited." >&2
        exit 1
    fi
    sleep 1
done

if ! curl -sf "http://localhost:${PORT}/health" > /dev/null 2>&1; then
    echo "ERROR: Service did not become healthy within 30 seconds." >&2
    exit 1
fi

# Launch Jupyter (unless --no-jupyter)
if [ "${1:-}" != "--no-jupyter" ]; then
    echo "Launching Jupyter..."
    cd "$NOTEBOOK_DIR"
    PYTHONPATH="${SVC_DIR}/src:${SVC_DIR}/external/moniker-data/src:${HOME}/open-moniker-client" \
        jupyter notebook showcase.ipynb
else
    echo "Server running on http://localhost:${PORT}"
    echo "Press Ctrl+C to stop."
    wait "$SERVER_PID"
fi
