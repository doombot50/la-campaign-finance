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
python3 build_entity_profiles.py     # per-entity LIFETIME edge lists: top donors-in (by filer) + recipients-out (by donor name)
python3 build_entity_activity.py     # reshard transactions BY FILER: per-committee full itemized history (capped), all cycles
python3 build_cycle_aggregates.py    # per-cycle additive sums (stat cards + party/industry) for instant first paint (run after retag)
python3 build_filer_lookup.py        # name → filer_number index
python3 build_entities.py            # canonical entity table (run after refresh)
python3 build_insights.py            # precomputed war chests, top donors (run after entities)
python3 build_election_lookup.py     # election dates
python3 build_election_results.py    # per-race results + vote %
python3 build_donor_industries.py    # donor → industry classification
python3 build_politician_lookup.py   # party + bio enrichment
python3 retag_caches.py              # re-tag party labels + transfers + industry on all cached records
python3 build_money_wins.py          # "does money win?" — money-leader win rate by office tier (feeds does-money-win.html; run after candidate-index + election-results)
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
    │   ├── la_insights.json.gz          precomputed aggregates (built offline, ships via data-cache release)
    │   ├── la_entity_donors.json.gz     per-filer receiving edge lists (top donors-in), built offline; ships whole (~2 MB)
    │   └── la_entity_giving.json.gz     name-keyed giving edge lists (top recipients-out), built offline; Pages ships this hash-sharded
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

### Per-Entity Lifetime Edge Lists (`/api/entity-profile`)

The profile's lifetime **giving** and **receiving** lists used to require loading every cycle's rows into the browser. `build_entity_profiles.py` precomputes them in one pass over `contributions_yr*.json.gz`:

- `la_entity_donors.json.gz` — `filerNumber` → receiving side (`top_donors`, `total_raised`, donor count, date span). Keyed by exact filer.
- `la_entity_giving.json.gz` — `_norm_name(donor)` → giving side (`top_recipients` with their filer link, `total_given`). Name-keyed, because donors carry no filer on the row (same constraint as the [[project-la-pay-to-play]] tool).

`build_entity_profile_payload(filer, name)` joins both into `{filer, name, receiving, giving}`. The Career tab fetches it in parallel (`_fetchEntityProfile` → `renderCampEdges`); a pure donor with no candidacy still shows their giving panel.

**Pages sharding gotcha:** the giving map is ~22 MB, too big to ship whole, so `build_pages_site.py` hash-shards it into `GIVING_SHARDS` (128) buckets `la_entity_giving_shard_<n>.json.gz`; a donor profile fetches only its one bucket (~172 KB). The receiving map (~2 MB) ships whole. The **FNV-1a/32 shard function is replicated in three places that MUST stay identical** — `shard_of` (build_pages_site.py), `_giving_shard_of` (la_ethics_server.py), and `fnv1a` (static_api.js) — or a lookup misses its bucket. The server's `/data/la_entity_giving_shard_<n>.json.gz` route synthesizes a bucket on the fly so it emulates Pages for `test_static_client_parity.mjs` and `?static=1`. Both parity gates cover `/api/entity-profile`.

### Per-Entity FULL ACTIVITY (`/api/entity-activity`)

The data normally ships sharded by **year** (load a year = everyone's rows). To show *all of one committee's activity* across all cycles without loading every cycle, `build_entity_activity.py` reshards transactions **by filer**: per-filer contributions-received + expenditures + loans, each capped to the `CAP` (1500) most-recent rows, with exact totals (`nc/ne/nl`) kept for the "N of M" disclosure. Lifetime *totals* already come exactly from the aggregates, and a browser can't render 200k rows (the table paginates), so the cap is the right tradeoff — most committees fall under it and show everything.

`la_entity_activity.json.gz` is one ~29 MB map (server holds it in memory, slices per filer for `/api/entity-activity?filer=<n>`). **Pages can't ship it whole**, so `build_pages_site.py` explodes it into `activity/<filer>.json.gz` (one file per filer) and `StaticAPI.entityActivity` fetches exactly one; the server's `/data/activity/<filer>.json.gz` route synthesizes a bundle on the fly to emulate Pages for `?static=1` and `test_static_client_parity.mjs`. The profile auto-fetches it (`_fetchCampActivity` → `_campRows`) and the Contributions/Expenditures/Loans tabs render full activity with a banner; falls back to loaded-cycle lists when no filer is known. Both parity gates cover it.

### Browser SPA (louisiana-campaign-finance.html)

~7,500 lines of vanilla HTML/CSS/JS — no framework, no bundler. Key subsystems all in one file:

- **IndexedDB wrapper** — persists parsed cycle data across page loads
- **Concurrent cycle loading** — a multi-cycle selection streams every cycle in parallel (bounded by `_loadConcurrency()`: 4 desktop / 2 low-memory) into per-cycle buffers; contributions, expenditures, and loans all fan out through the shared `_runLimit` runner. A single paint scheduler (`_makeLoadProgress`) owns mid-stream partial renders — 15k/60k-row thresholds fire once globally, then repaints back off adaptively as the dataset (and re-render cost) grows. A load-generation token (`_loadGen` and friends) makes superseded loads abandon their streams, so switching cycles mid-load can't mix rows from a deselected cycle into the new view.
- **Token-based search filter** — `zip:`, `party:`, `donor:`, `recipient:`, `category:`, `min:`, `max:` parsed client-side; available tokens change by active tab
- **Chart.js** — Net Cash Flow (cumulative contributions − expenditures) and bar charts; certified COH green anchor dots overlay the cash flow line. Chart.js and Leaflet are **self-hosted under `vendor/`** (no CDN at runtime) and **lazy-loaded** (`ensureChart()` / `ensureLeaflet()`) — neither loads on the Overview landing; every chart render function and the map init self-heal if invoked before their library arrives. The contributions pre-load is deferred to `requestIdleCallback` on a pure Overview landing. The live server serves `vendor/` via an allowlisted `/vendor/<file>` route; `build_pages_site.py` copies the dir into `_site/vendor/`.
- **Fonts** — Libre Franklin + IBM Plex Mono are **self-hosted** (latin woff2 under `vendor/fonts/`, `@font-face` in the inline `<style>`); no Google Fonts origin at runtime. CARTO map tiles are the only remaining third-party origin (dns-prefetched, loaded only when the map opens).
- **Service worker** (`sw.js`, Pages-only) — same-origin shell + `vendor/` libs + fonts cache stale-while-revalidate; data artifacts stale-while-revalidate. Near-instant repeat visits and offline. Registered only in `STATIC_MODE`; shipped to `_site/` root by `build_pages_site.py`. Bump `CACHE_VERSION` in `sw.js` to force-evict.
- **URL state serialization** — cycle, tab, filters → query params for shareability
- **Print-to-PDF** — chart canvases are snapshotted to PNG before printing to prevent reflow

The Net Cash Flow chart shows `contributions − expenditures`, not actual cash on hand. The green dots (certified F102/F202 ending balances) are ground truth; the gold line is an approximation.

### "Does Money Win?" data story (`does-money-win.html`)

A standalone, self-contained scrollytelling page (inline CSS/JS, hand-drawn **inline SVG** charts — no Chart.js, no framework, matching the zero-dep philosophy) that answers: how often does the biggest war chest actually win, and does it depend on the office? It reasons over `la_money_wins.json`, precomputed by `build_money_wins.py`, which **joins artifacts already committed** — `la_candidacies_raw.json.gz` (every SoS candidacy, name-keyed) + `la_candidate_index.json.gz` (per-cycle fundraising) + `la_election_results.json` (the `ambiguous` same-name flag). Zero new data sources.

- **Race unit:** raw candidacies grouped by `(date, office)`; counts when exactly one `Elected` winner, ≥2 candidates, no `ambiguous` name, non-federal, and ≥2 candidates who deployed money (so "money leader" is a real comparison).
- **Money metric: SPENDING, not fundraising.** `spend_through_month` sums the candidate index's monthly `out` in-cycle through the election month. Louisiana candidates — local ones especially — bankroll campaigns with personal loans that never appear as contributions received, so ranking by money *raised* mislabels who the money candidate was. Both metrics are truncated at the election month; full-cycle would inflate winners who keep raising and spending after they win.
  - The switch matters: the biggest spender ≠ the biggest fundraiser in **14.9%** of races; **18.4%** of the raised-based "upsets" are races the winner actually **outspent**; and the raised-based tier gap (Legislative 74.1% vs Local 61.5%, p=0.016) collapses to 68% vs 63% on spending (not significant). The overall headline is robust either way (66.0% raised / 64.9% spent).
  - The payload therefore ships **both** under `compare` (`raised`, `spent`, `by_tier`, `leader_flips`, `false_upsets`) and the page has a dedicated "Which money?" section that shows the contrast rather than hiding it. `metric` names the primary measure (`"spent"`).
  - Because the metric is spending, the page's "outspent N×" / "Spent $X" language is now literally true; `upsets[]` carries `lead_spent`/`winner_spent` **and** `lead_raised`/`winner_raised`.
- **Serving:** the page fetches `data/la_money_wins.json` — one relative path that resolves on both the live server (its `/data/<name>` route falls back to the committed repo-root file; the page itself is served by a dedicated `/does-money-win.html` route) and Pages (`build_pages_site.py` copies the HTML into `_site/` and lists the JSON in `REQUIRED_ROOT` → `_site/data/`). Linked from the dashboard Overview. Unit-tested by `tests/test_money_wins.py` (stdlib, CI).

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
| `/api/cycle-aggregates` | Per-cycle additive sums; summed client-side for instant stat-card + breakdown paint before rows stream |
| `/api/entity` | Canonical entity record by name or filer_number |
| `/api/entity-profile` | Lifetime giving + receiving edge lists for one entity (`receiving` by filer, `giving` by name); row-free |
| `/api/entity-activity` | One filer's full itemized history (contributions/expenditures/loans, capped most-recent) across all cycles |
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
