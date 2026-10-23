#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
export PYTHONPATH="$PWD/src${PYTHONPATH:+:$PYTHONPATH}"
export DATANOMIK_REQUIRE_FULL_TESTS=1
export PYTHONDONTWRITEBYTECODE=1
ruff check --no-cache src dags tests scripts
python -m unittest discover -s tests -v
python -m pip check
