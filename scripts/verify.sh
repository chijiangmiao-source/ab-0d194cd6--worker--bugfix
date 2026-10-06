#!/bin/sh
# One-shot acceptance run: build, start app + 2 workers, run verify, report via exit code.
set -u
cd "$(dirname "$0")/.."

docker compose up -d --build --wait app worker
docker compose run --rm verify
code=$?
docker compose down
exit $code
