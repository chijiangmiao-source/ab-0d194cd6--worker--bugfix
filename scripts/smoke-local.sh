#!/bin/sh
# Local smoke without docker: server + 2 supervised workers + one-shot verify.
set -u
cd "$(dirname "$0")/.."

DATA="${DATA_DIR:-/tmp/track-export-smoke}"
PORT="${PORT:-8080}"
rm -rf "$DATA"
mkdir -p "$DATA"

DATA_DIR="$DATA" PORT="$PORT" TEST_HOOKS=1 python3 -m app.server &
APP_PID=$!

# restart loop mimics compose `restart: on-failure` for crash-injection scenarios
DATA_DIR="$DATA" TEST_HOOKS=1 LEASE_TTL_SECONDS=8 POLL_INTERVAL_SECONDS=0.3 \
  sh -c 'while true; do python3 -m app.worker; sleep 1; done' &
W1_PID=$!
DATA_DIR="$DATA" TEST_HOOKS=1 LEASE_TTL_SECONDS=8 POLL_INTERVAL_SECONDS=0.3 \
  sh -c 'while true; do python3 -m app.worker; sleep 1; done' &
W2_PID=$!

cleanup() { kill $APP_PID $W1_PID $W2_PID 2>/dev/null; pkill -f 'app.worker' 2>/dev/null; wait 2>/dev/null; }
trap cleanup EXIT

sleep 1
API_BASE="http://localhost:$PORT" DATA_DIR="$DATA" python3 -m verify.verify
exit $?
