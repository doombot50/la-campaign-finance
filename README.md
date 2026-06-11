# Louisiana Campaign Finance Dashboard

An interactive dashboard over Louisiana campaign-finance data from
[ethics.la.gov](https://www.ethics.la.gov/) — money in, money out, loans, top
donors, races, side-by-side campaign comparisons, and certified Cash-on-Hand
from annual ethics filings (F102 candidate + F202 PAC).

A single-page browser app (`louisiana-campaign-finance.html`) talks to a small
Python proxy (`la_ethics_server.py`) that streams cached snapshots of the
state's contribution / expenditure / loan CSVs, augmented with per-candidate
career data and certified bank-balance anchors scraped from annual reports
filed by both candidates and PACs.

## Quick start

```bash
# Python 3.8+, no third-party packages needed for the server.
python3 la_ethics_server.py
# → open http://localhost:8765
```

On first launch the server lazily downloads the relevant 4-year CSV bundle
from ethics.la.gov, splits it into one gzipped NDJSON file per calendar year
under `.la_cache/`, and serves the dashboard at `/`. The browser caches the
parsed records in IndexedDB so subsequent loads are instant.

## Features

- **Money In / Money Out / Loans** — filterable tables and charts over every
  contribution, expenditure, and loan filed in the selected cycle.
- **Unified header search** drives entity lookup (candidates and committees,
  ranked by total raised) and *also* filtering via token syntax:
  `zip:70801`, `party:rep`, `donor:exxon`, `recipient:acme`, `category:media`,
  `min:5k`, `max:1.5m`, … Available tokens change with the active tab —
  contributions tokens on Money In, expenditure tokens on Money Out. The **?**
  button next to the search bar opens a full guide.
- **Campaign profile** modal (any campaign name → click) — header stats,
  party + most recent election result, full contribution / expenditure / loan
  tables, a multi-cycle career chart, and a **Net Cash Flow** chart annotated
  with certified Cash-on-Hand anchor dots from F102 (candidate) and F202
  (PAC) Annual filings.
- **Contributor profile** modal — every committee a donor gave to in the
  selected cycle, with totals.
- **Compare** — add up to three campaigns via the unified search; each becomes
  a side-by-side profile card with party, election result, this-cycle stats,
  all-time totals, certified COH, and a mini cash-flow chart. The leader per
  metric is highlighted.
- **Races** — every election grouped by (date, office) with candidate finance
  side-by-side.
- **Top Donors** leaderboard.
- **Filter chips** with a click-to-remove model + Clear all.
- **Print** the campaign profile to PDF — chart canvases are snapshotted to
  PNG before printing so layout doesn't reflow mid-render.
- **URL state persistence** — cycle, tab, sub-tab, and every active filter
  serialize to the URL so dashboards are shareable.
- **IndexedDB** caching of the loaded cycle (90 MB+ contributions payload is
  fetched once per cycle and persisted).

## Reading the chart, honestly

The **Net Cash Flow** chart on each campaign profile plots cumulative
`contributions − expenditures` over time. It is **not** the campaign's actual
Cash on Hand. Real CoH also reflects loans, loan repayments, refunds,
in-kind contributions (which inflate "raised" but produce no cash), and
transfers between committees — none of which sit in the contribution /
expenditure streams that drive the line.

The **green dots** on top of that line are the only ground-truth values: each
is a certified ending balance reported on an F102 Annual filing by that
filer. Where the gold line and green dots disagree, the dots are right.

The chart aggregates by *candidate name string*, so a candidate with multiple
committees (e.g., a personal campaign committee + a leadership PAC) is shown
across all of them whenever those committees use the same name in the
underlying records. About 0.3% of names in the current cache match more than
one filer ID — for those, the chart over-rolls-up.

## Architecture

```
            ┌────────────────────────┐
            │  louisiana-campaign-   │   ← single-page browser app
            │   finance.html (SPA)   │       (Chart.js + Leaflet, no build step)
            └───────────┬────────────┘
                        │  /api/la-ethics, /api/la-expenditures, /api/la-loans
                        │  /api/candidate-history, /api/coh, /api/search,
                        │  /api/races, /api/overview, /api/industry-breakdown
                        ▼
            ┌────────────────────────┐
            │  la_ethics_server.py   │   ← stdlib http.server, no deps
            │  (Python 3.8+)         │
            └───────────┬────────────┘
                        │
        ┌───────────────┼─────────────────────────────┐
        ▼               ▼                             ▼
  .la_cache/      la_candidate_index.json.gz     ethics_coh_cache.json
  per-year NDJSON   per-candidate career data       certified F102 COH
  (gzip)            (built offline)                  per filer (scraped)
        ▲                ▲                             ▲
        │                │                             │
   ethics.la.gov    build_candidate_           fetch_ethics_coh.py
   CSV bundles      index.py                   (PDF scraper +
                                                pdfplumber parser)
```

## Repository layout

| Path | What |
|---|---|
| `louisiana-campaign-finance.html` | Single-page dashboard (HTML/CSS/JS, no build step) |
| `la_ethics_server.py` | Python proxy server — downloads and streams CSV data, exposes JSON APIs, serves the HTML |
| `requirements.txt` | Python deps (none — stdlib only) |
| `render.yaml` | Render.com web-service config |
| `.la_cache/` | Per-year NDJSON-gzip caches of contributions / expenditures / loans (auto-populated) |
| `la_candidate_index.json.gz` | Per-candidate career index: per-cycle totals, monthly in/out, all-time totals |
| `la_candidacies_raw.json.gz` | Raw candidacy records (date, office, outcome, vote_pct) |
| `la_election_lookup.json` / `la_election_results.json` | Election dates + per-race results |
| `la_filer_lookup.json` | name → numeric filerNumber, 5,494 entries |
| `la_politicians_lookup.json` | party + bio enrichment for known politicians |
| `la_donor_industries.json` | donor → industry classification |
| `ethics_coh_cache.json` | Certified Cash-on-Hand from F102 Annual reports (scraped per filer; multi-year `reports` list) |
| `.ethics_pdf_cache/` | Local PDF cache for the COH scraper |

## Build & maintenance scripts

These rebuild the static artifacts above. Most don't need to be re-run unless
the underlying source data changes meaningfully.

| Script | Output | When to re-run |
|---|---|---|
| `fetch_historical_data.py` | seeds `.la_cache/` with all 4-year CSV bundles | once per project setup, or to add historical years |
| `build_candidate_index.py` | `.la_cache/la_candidate_index.json.gz` | nightly (workflow) after `retag_caches.py`; aggregates per-candidate career data + monthly buckets, transfer-aware. The committed repo-root copy is only a cold-boot fallback — the server prefers the `.la_cache/` copy when newer |
| `build_filer_lookup.py` | `la_filer_lookup.json` | after a `.la_cache/` refresh |
| `build_election_lookup.py` | `la_election_lookup.json` | when state election dates change |
| `build_election_results.py` | `la_election_results.json` | after new races complete |
| `build_donor_industries.py` | `la_donor_industries.json` | when donor classification rules change |
| `build_politician_lookup.py` | `la_politicians_lookup.json` | when party / bio data needs refresh |
| `fetch_ethics_coh.py` | `ethics_coh_cache.json` (+ `.ethics_pdf_cache/` PDFs) | to add/refresh certified COH anchors |
| `retag_caches.py` | re-tags existing `.la_cache/` records | after a server-side parser change |

### Running the COH scraper

```bash
python3 fetch_ethics_coh.py                          # all candidates, all annual years
python3 fetch_ethics_coh.py --filer "Mandie Landry"  # single filer, all years
python3 fetch_ethics_coh.py --year 2024              # restrict to one report year
python3 fetch_ethics_coh.py --dry-run                # discover only, no PDF
python3 fetch_ethics_coh.py --force                  # re-fetch even if cached
python3 fetch_ethics_coh.py --limit 10               # cap at N (testing)
```

The scraper:
1. Looks up the filer's ID (CAN-style for candidates, PAC-style for PACs)
   via `SearchByNameAdv.aspx`.
2. Lists every Annual report on `ViewEFiler.aspx?FilerID=…` — F102
   (candidate) or F202 (PAC).
3. Downloads each report PDF (cached in `.ethics_pdf_cache/`).
4. Extracts the "Funds on hand at beginning / closing" Summary-Page lines
   with pdfplumber.
5. Writes a per-entity `reports: [...]` list (oldest → newest) — each item
   stamped with its `form_type` (`F102` or `F202`) — plus a flat-fields
   copy of the most-recent report for backward compatibility.

PDF downloads use a 90 s timeout with 3× retry-with-exponential-backoff
(2 s, 4 s, 8 s) and atomic writes (`.partial` → rename), so a flaky connection
doesn't leave half-written PDFs in the cache.

> **Note:** Requires `pdfplumber` (`pip install pdfplumber`). This is the only
> third-party dependency in the project; the server and dashboard need none.

## Deployment

Deployed on [Render](https://render.com) via `render.yaml`:

- `python3 la_ethics_server.py` (the server binds to `$PORT` from env on
  Render; defaults to 8765 locally).
- Health check at `/health`.

Static data files (`.json`, `.json.gz`) are committed and shipped to Render.
`.la_cache/` is seeded at build time from the rolling `data-cache` GitHub
release (see `fetch_cache_assets.py`), which the nightly workflow keeps
populated with **every** cycle's contribution / expenditure / loan files plus
the derived artifacts (`la_entities.json.gz`, `la_insights.json.gz`,
`la_candidate_index.json.gz`) — so the free instance never has to download a
100MB CSV from ethics.la.gov at request time. Anything still missing is
downloaded on demand and cached on the instance's disk between requests.

## Known limitations

- **Cash on Hand from transactions is unreliable** — see *Reading the chart,
  honestly*. Use the certified anchor dots for ground truth.
- **Name-keyed career index** — the candidate index keys aggregation by
  normalized candidate-name string, not by `filerNumber`. About 0.3% of
  candidates have multiple filers under one name and roll up; a filer-keyed
  rebuild is on the roadmap.
- **Annual-only certified COH** — we scrape F102 (candidate) and F202 (PAC)
  Annual reports, giving one anchor per year. F101 / F201 campaign-period
  reports (pre-primary, pre-general, post-election) would add intra-year
  resolution; not currently fetched.
- **No automated tests** — bugs in filter / search logic have to be caught by
  hand against the live dashboard. Test harness for the filter / token layer
  is on the roadmap.

## Roadmap

- Lightweight test harness for the filter / token-search / `_currentTab`
  logic — the most common bug surface.
- Filer-keyed candidate index rebuild (fixes the 0.3% multi-committee
  roll-up).
- Optional F101 / F201 campaign-period anchors on the Net Cash Flow chart
  (denser dots during active campaigns and PAC pushes).
- Independent-expenditure / super-PAC attribution view.
