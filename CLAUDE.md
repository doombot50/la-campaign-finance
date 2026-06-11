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

No linter, no test suite, no build step. The HTML file is shipped as-is; the JS has no bundler.

## Maintenance Scripts

These rebuild the static `.json`/`.json.gz` artifacts committed to the repo. Run them after the underlying source data changes:

```bash
python3 refresh_la_cache.py          # re-download current cycle from ethics.la.gov
python3 build_candidate_index.py     # per-candidate career totals + monthly buckets
python3 build_filer_lookup.py        # name → filer_number index
python3 build_entities.py            # canonical entity table (run after refresh)
python3 build_insights.py            # precomputed war chests, top donors (run after entities)
python3 build_election_lookup.py     # election dates
python3 build_election_results.py    # per-race results + vote %
python3 build_donor_industries.py    # donor → industry classification
python3 build_politician_lookup.py   # party + bio enrichment
python3 retag_caches.py              # re-tag party labels on all cached records
python3 fetch_historical_data.py     # seed .la_cache/ with 4-year historical bundles (one-time setup)
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
    ├── .la_cache/              per-year NDJSON.gz — lazy-downloaded from ethics.la.gov
    ├── la_candidate_index.json.gz   per-candidate career summaries (built offline)
    ├── la_candidacies_raw.json.gz   SoS ballot appearances
    ├── la_entities.json.gz          canonical filer table (built offline)
    ├── la_insights.json.gz          precomputed aggregates (built offline)
    ├── la_filer_lookup.json         name → filer_number
    ├── la_donor_industries.json     donor → industry
    ├── la_politicians_lookup.json   party + bio enrichment
    └── ethics_coh_cache.json        certified F102/F202 COH (scraped by fetch_ethics_coh.py)
```

### Server Module-Level Cache (la_ethics_server.py)

The server holds lazily-loaded module globals (`_CAND_INDEX`, `_ENTITIES`, `_INSIGHTS`, `_SEARCH_INDEX`, etc.) behind threading locks. Each is loaded on first use and supports hot-reload via mtime checks — if a nightly workflow drops a new file on disk, the next request picks it up without a restart.

The `.la_cache/` files follow the pattern `contributions_yr<YYYY>.json.gz`. Current-cycle files (2024–2027) expire after 24 hours and are re-downloaded; older cycles are fetched once.

### Name Normalization

All candidate lookup across datasets (contributions CSV, SoS candidacies, ethics COH) uses a shared `_norm_name()` pipeline: uppercase → strip honorifics/suffixes (DR, MR, JR, II, III…) → keep A–Z and spaces → collapse whitespace. This is the join key throughout `la_candidate_index`, `la_candidacies_raw`, and `ethics_coh_cache`.

~0.3% of candidates have multiple filer IDs under one normalized name and roll up incorrectly. The roadmap item is a filer-keyed rebuild.

### Browser SPA (louisiana-campaign-finance.html)

~7,500 lines of vanilla HTML/CSS/JS — no framework, no bundler. Key subsystems all in one file:

- **IndexedDB wrapper** — persists parsed cycle data across page loads
- **Token-based search filter** — `zip:`, `party:`, `donor:`, `recipient:`, `category:`, `min:`, `max:` parsed client-side; available tokens change by active tab
- **Chart.js** — Net Cash Flow (cumulative contributions − expenditures) and bar charts; certified COH green anchor dots overlay the cash flow line
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
| `/health` | Health check (used by Render.com) |

### Deployment & CI

- **Render.com** (`render.yaml`): `buildCommand` calls `fetch_cache_assets.py` to pull `.json.gz` files from a `data-cache` GitHub release so the instance starts warm; `startCommand` is `python3 la_ethics_server.py`; health check at `/health`.
- **Nightly GitHub Actions** (`.github/workflows/nightly-data.yml`, 0900 UTC / ~3 AM Central): seeds cache from release → re-downloads current cycle → re-tags party labels → builds entities + insights → uploads refreshed `.json.gz` back to release → runs incremental COH scrape → commits `ethics_coh_cache.json` → push triggers Render auto-deploy.

## Key Constraints

- **No third-party packages** for `la_ethics_server.py` or the dashboard — stdlib only. `pdfplumber` is the sole external dep, used only by `fetch_ethics_coh.py`.
- **No build step** for the frontend — edit `louisiana-campaign-finance.html` directly.
- **No test suite** — filter/search logic bugs must be caught by running the dashboard manually.
- Static data files (`.json`, `.json.gz`) are **committed to the repo** and shipped to Render. `.la_cache/` and `.ethics_pdf_cache/` are gitignored (rebuilt at runtime).
