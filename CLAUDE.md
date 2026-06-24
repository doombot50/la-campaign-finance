# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

An interactive campaign finance dashboard for Louisiana political contributions, expenditures, and loans. A single-page browser app (`louisiana-campaign-finance.html`) talks to a lightweight Python proxy (`la_ethics_server.py`) that streams cached data from the Louisiana Board of Ethics (ethics.la.gov).

**Core philosophy:** Zero third-party runtime dependencies (stdlib only for the server), lazy-loaded file caches, streaming CSV parsing, and a fully client-side IndexedDB cache for instant subsequent page loads.

## Running Locally

```bash
python3 la_ethics_server.py
# → http://localhost:8765
```

On first request the server lazily downloads the relevant 4-year CSV bundle from ethics.la.gov, splits it into per-year gzipped NDJSON under `.la_cache/`, and serves the dashboard. The browser caches parsed records in IndexedDB so subsequent loads are instant.

No linter, no build step. The HTML file is shipped as-is; the JS has no bundler.

A dependency-free test suite lives under `tests/` (Python `unittest` + Node `node --test`, stdlib only). Run it with `./tests/run_tests.sh`; the unit layer needs no network or data release and runs in CI (`.github/workflows/ci.yml`) on every push/PR. See `tests/README.md`.

## Maintenance Scripts

These rebuild the static `.json`/`.json.gz` artifacts committed to the repo. Run them after the underlying source data changes:

```bash
python3 refresh_la_cache.py          # re-download current cycle from ethics.la.gov
python3 build_candidate_index.py     # per-candidate career totals + monthly buckets (also emits la_filer_index.json.gz, the filer-keyed twin)
python3 build_filer_lookup.py        # name → filer_number index
python3 build_entities.py            # canonical entity table (run after refresh)
python3 build_insights.py            # precomputed war chests, top donors (run after entities)
python3 build_election_lookup.py     # election dates
python3 build_election_results.py    # per-race results + vote %
python3 build_donor_industries.py    # donor → industry classification
python3 build_politician_lookup.py   # party + bio enrichment
python3 retag_caches.py              # re-tag party labels + transfers + industry on all cached records
python3 build_static_api.py          # static twins of /api/search, races, overview, industry (server-less mode)
python3 test_static_parity.py        # gate: static artifacts == live API (runs nightly before upload)
node test_static_client_parity.mjs   # gate: shipped static_api.js == live API end-to-end
python3 fetch_historical_data.py     # seed .la_cache/ with 4-year historical bundles (one-time setup)
python3 build_pwa_icons.py           # regenerate committed PWA icons (only if the design changes)
```

COH scraper (requires `pip install pdfplumber`):
```bash
python3 fetch_ethics_coh.py                          # all filers, all annual years
python3 fetch_ethics_coh.py --filer "Mandie Landry"  # single filer
python3 fetch_ethics_coh.py --year 2024              # restrict to one report year
python3 fetch_ethics_coh.py --dry-run                # discover only, no PDF download
python3 fetch_ethics_coh.py --force                  # re-fetch even if cached
python3 fetch_ethics_coh.py --limit 10               # cap at N filers (testing)
```

## Architecture

### Data Flow

```
Browser (louisiana-campaign-finance.html)
    │  /api/* JSON endpoints
    ▼
la_ethics_server.py  (ThreadingHTTPServer, stdlib only)
    │
    ├── .la_cache/                       per-year NDJSON.gz — lazy-downloaded from ethics.la.gov
    │   ├── la_entities.json.gz          canonical filer table (built offline, ships via data-cache release)
    │   └── la_insights.json.gz          precomputed aggregates (built offline, ships via data-cache release)
    ├── la_candidate_index.json.gz       per-candidate career summaries, keyed by normalized name (committed)
    ├── la_filer_index.json.gz           same career summaries keyed by filerNumber — exact identity, no name-collision merges (ships via data-cache release)
    ├── la_candidacies_raw.json.gz       SoS ballot appearances (committed)
    ├── la_filer_lookup.json             name → filer_number (committed)
    ├── la_donor_industries.json         donor → industry (committed)
    ├── la_politicians_lookup.json       party + bio enrichment (committed)
    └── ethics_coh_cache.json            certified F102/F202 COH, scraped by fetch_ethics_coh.py (committed)
```

### Server Module-Level Cache (la_ethics_server.py)

The server holds lazily-loaded module globals (`_CAND_INDEX`, `_ENTITIES`, `_INSIGHTS`, `_SEARCH_INDEX`, etc.) behind threading locks. Each is loaded on first use and supports hot-reload via mtime checks — if a nightly workflow drops a new file on disk, the next request picks it up without a restart.

The `.la_cache/` files follow the pattern `contributions_yr<YYYY>.json.gz`. Current-cycle files (2024–2027) expire after 24 hours and are re-downloaded; older cycles are fetched once.

### Name Normalization

All candidate lookup across datasets (contributions CSV, SoS candidacies, ethics COH) uses a shared `_norm_name()` pipeline: uppercase → strip honorifics/suffixes (DR, MR, JR, II, III…) → keep A–Z and spaces → collapse whitespace. This is the join key throughout `la_candidate_index`, `la_candidacies_raw`, and `ethics_coh_cache`.

~0.3% of normalized names cover more than one distinct filer (two people who share a name, or one person with several committees). The name-keyed join merges them. The **filer-keyed rebuild** addresses this end-to-end, on both the live server and the serverless static twin:

- `build_candidate_index.py` also emits `la_filer_index.json.gz` — career summaries keyed by the raw Ethics `filerNumber` (the only native identity). The name-keyed `la_candidate_index.json.gz` output is byte-identical, so every existing name path and the parity gates are untouched.
- `/api/candidate-history?...&filer=<n>` (live) and `StaticAPI.candidateHistory(name, filer)` (static) both serve a single filer's exact figures from the filer index, falling back to the name index when no filer is known.
- `build_entities.py` joins career by exact filer; `build_pages_site.py` publishes the filer index; `test_static_client_parity.mjs` gates the filer path.
- The dashboard threads `filer_number` from search results into the profile fetch (`showCampaignProfile(name, tab, filer)`).

Remaining edge: surfaces with no filer number — a shared bare-name `#/campaign/<name>` link, the Compare tab, and the profile's in-cycle transaction *lists* (still name-matched) — fall back to the name-keyed merge.

### Browser SPA (louisiana-campaign-finance.html)

~7,500 lines of vanilla HTML/CSS/JS — no framework, no bundler. Key subsystems all in one file:

- **IndexedDB wrapper** — persists parsed cycle data across page loads
- **Token-based search filter** — `zip:`, `party:`, `donor:`, `recipient:`, `category:`, `min:`, `max:` parsed client-side; available tokens change by active tab
- **Chart.js** — Net Cash Flow (cumulative contributions − expenditures) and bar charts; certified COH green anchor dots overlay the cash flow line. Chart.js and Leaflet are **self-hosted under `vendor/`** (no CDN at runtime) and **lazy-loaded** (`ensureChart()` / `ensureLeaflet()`) — neither loads on the Overview landing; every chart render function and the map init self-heal if invoked before their library arrives. The contributions pre-load is deferred to `requestIdleCallback` on a pure Overview landing. The live server serves `vendor/` via an allowlisted `/vendor/<file>` route; `build_pages_site.py` copies the dir into `_site/vendor/`.
- **Service worker** (`sw.js`, Pages-only) — same-origin shell + `vendor/` libs cache stale-while-revalidate; data artifacts stale-while-revalidate; Google Fonts cache-first. Near-instant repeat visits and offline. Registered only in `STATIC_MODE`; shipped to `_site/` root by `build_pages_site.py`. Bump `CACHE_VERSION` in `sw.js` to force-evict.
- **URL state serialization** — cycle, tab, filters → query params for shareability
- **Print-to-PDF** — chart canvases are snapshotted to PNG before printing to prevent reflow

The Net Cash Flow chart shows `contributions − expenditures`, not actual cash on hand. The green dots (certified F102/F202 ending balances) are ground truth; the gold line is an approximation.

### API Endpoints

| Endpoint | Purpose |
|---|---|
| `/api/la-ethics` | Stream contributions NDJSON (supports token filters) |
| `/api/la-expenditures` | Stream expenditures NDJSON |
| `/api/la-loans` | Stream loans NDJSON |
| `/api/candidate-history` | Career profile: financial + races + certified COH |
| `/api/search` | Ranked entity search (built once, hot-reloaded) |
| `/api/overview` | Precomputed party totals, top donors, monthly flow |
| `/api/insights` | War chests, transfers, party totals |
| `/api/entity` | Canonical entity record by name or filer_number |
| `/api/coh` | Certified COH lookup |
| `/api/races` | Elections grouped by (date, office) |
| `/api/industry-breakdown` | Donor industry breakdown per filer |
| `/api/election-results` | Full election dates + results lookup |
| `/api/data-status` | Cache download progress (browser polls this while a cycle loads) |
| `/health` | Health check (used by Render.com) |

### Deployment & CI

- **Render.com** (`render.yaml`): `buildCommand` calls `fetch_cache_assets.py` to pull `.json.gz` files from a `data-cache` GitHub release so the instance starts warm; `startCommand` is `python3 la_ethics_server.py`; health check at `/health`.
- **Nightly GitHub Actions** (`.github/workflows/nightly-data.yml`, 0900 UTC / ~3 AM Central): seeds cache from release → re-downloads current cycle → re-tags party labels → builds entities + insights → uploads refreshed `.json.gz` back to release → runs incremental COH scrape → commits `ethics_coh_cache.json` → push triggers Render auto-deploy.

## Key Constraints

- **No third-party packages** for `la_ethics_server.py` or the dashboard — stdlib only. `pdfplumber` is the sole external dep, used only by `fetch_ethics_coh.py`.
- **No build step** for the frontend — edit `louisiana-campaign-finance.html` directly.
- **Tests** — `tests/` holds a stdlib-only unit suite (server helpers + payload builders in Python `unittest`; the dashboard's and `static_api.js`'s pure JS via `node --test`, extracted by name since there's no build step). It runs on every push/PR via CI. The live-vs-static **parity gates** (`test_static_parity.py`, `test_static_client_parity.mjs`) need the `.la_cache/` data release and run nightly. DOM-coupled frontend behavior (filters, rendering) still has no automated coverage — verify those by running the dashboard.
- Static data files (`.json`, `.json.gz`) in the repo root are **committed** and shipped to Render. `.la_cache/` and `.ethics_pdf_cache/` are gitignored — `.la_cache/` contents (including `la_entities.json.gz` and `la_insights.json.gz`) are distributed via the `data-cache` GitHub release and rebuilt/pulled at deploy time.
