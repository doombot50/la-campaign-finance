#!/usr/bin/env bash
# run_tests.sh — the full test suite for the LA campaign-finance tool.
#
# Layers:
#   1. Python unit tests  — pure server helpers + payload builders (committed
#                           data only; no network, no .la_cache/ release).
#   2. Node  unit tests   — the dashboard's pure JS + static_api.js helpers.
#   3. Parity gates       — the live-vs-static trust gates. These need the full
#                           .la_cache/ data release, so they're SKIPPED unless
#                           it's present (e.g. locally after fetch_cache_assets,
#                           or in the nightly workflow). Force with RUN_PARITY=1.
#
# Exit non-zero if any executed layer fails. Run from anywhere:
#   ./tests/run_tests.sh
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PY="${PYTHON:-python3}"
NODE="${NODE:-node}"
fail=0
run() { echo; echo "── $1 ──"; shift; "$@" || { echo "FAILED: $*"; fail=1; }; }

run "Python unit tests" "$PY" -m unittest discover -s tests -p 'test_*.py' -v
run "Node unit tests" "$NODE" --test tests/test_frontend_units.mjs tests/test_static_api_units.mjs

# Parity gates require the data release in .la_cache/.
if [ -f ".la_cache/la_races.json.gz" ] || [ "${RUN_PARITY:-0}" = "1" ]; then
  run "Static parity gate"        "$PY" test_static_parity.py
  run "Static client parity gate" "$NODE" test_static_client_parity.mjs
else
  echo; echo "── Parity gates: SKIPPED (no .la_cache/ data release; set RUN_PARITY=1 to force) ──"
fi

echo
if [ "$fail" -ne 0 ]; then echo "TESTS FAILED"; exit 1; fi
echo "ALL TESTS PASSED"
