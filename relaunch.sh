#!/usr/bin/env bash
# Reload Jupyter notebook — kill, relaunch, open browser.
# Usage: ./relaunch.sh [port]
set -e

PORT="${1:-8888}"
NB="notebooks/showcase.ipynb"
DIR="$(cd "$(dirname "$0")" && pwd)"
LOG="/tmp/jupyter-moniker.log"

# Kill any existing Jupyter on this port
pkill -f "jupyter.*--port.*$PORT" 2>/dev/null && sleep 1 || true

# Launch
jupyter notebook "$DIR/$NB" --no-browser --port "$PORT" --ip 127.0.0.1 > "$LOG" 2>&1 &
PID=$!

# Wait for token
for i in $(seq 1 20); do
    TOKEN=$(python3 -c "
import re
with open('$LOG') as f:
    m = re.search(r'token=([a-f0-9]+)', f.read())
    if m: print(m.group(1))
" 2>/dev/null)
    if [ -n "$TOKEN" ]; then break; fi
    sleep 0.5
done

if [ -z "$TOKEN" ]; then
    echo "Jupyter didn't start in time. Check $LOG"
    exit 1
fi

URL="http://127.0.0.1:${PORT}/notebooks/showcase.ipynb?token=${TOKEN}"
echo "$URL"
xdg-open "$URL" 2>/dev/null || open "$URL" 2>/dev/null || true
