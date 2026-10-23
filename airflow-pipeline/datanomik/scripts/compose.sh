#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
exec docker compose --env-file .runtime/stack.env -f docker-compose.yaml "$@"
