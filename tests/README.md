# Tests

A dependency-free test suite for the Louisiana campaign-finance tool, matching
the project's stdlib-only philosophy: Python `unittest` and Node's built-in
`node --test`, no third-party packages.

## Run everything

```bash
./tests/run_tests.sh
```

This runs the unit layer and then the parity gates **if** the `.la_cache/` data
release is present (otherwise they're skipped). Force the gates with
`RUN_PARITY=1 ./tests/run_tests.sh`.

## Layers

| File | What it covers | Needs data release? |
|---|---|---|
| `tests/test_server_units.py` | `la_ethics_server.py` pure helpers (name normalization, office classification, cycle/date math, ZIP→state, party lookup) and the **races / search / overview / industry** payload builders | No — committed root artifacts only |
| `tests/test_frontend_units.mjs` | The dashboard's pure JS, extracted from `louisiana-campaign-finance.html`: the **Races-tab office-importance ordering** (`_officeRank` / district sort), date helpers, anchors, money/outcome formatting, HTML escaping | No |
| `tests/test_static_api_units.mjs` | `static_api.js` building blocks: `normName` / `wsNorm` (cross-checked against the server's `_norm_name`), party-office detection | No |
| `test_static_parity.py` | Live API == static artifacts (existing gate) | **Yes** |
| `test_static_client_parity.mjs` | Shipped `static_api.js` == live API, end-to-end (existing gate) | **Yes** |

The unit layer runs on a fresh clone with no network — which is why CI
(`.github/workflows/ci.yml`) runs it on every push/PR. The parity gates live in
the nightly workflow, where the data release is seeded into `.la_cache/`.

## How the JS unit tests work

The dashboard ships as one ~7,500-line HTML file with no build step, so the pure
helpers are extracted by name (`tests/_extract.mjs` brace-matches the
declaration and evaluates it in isolation). This only works for self-contained
functions with no DOM/global dependencies — exactly the logic worth unit-testing
here. DOM-coupled behavior (filters applying to inputs, rendering) is out of
scope for this layer.

## Adding tests

- **Server logic:** add a `unittest.TestCase` to `test_server_units.py`. Prefer
  asserting payload *shape* and *ordering contracts* over exact values so the
  tests stay green as the underlying data refreshes nightly.
- **Frontend logic:** if you add a self-contained helper to the HTML, list its
  name in the `extract(...)` call in `test_frontend_units.mjs` and assert on it.
