#!/usr/bin/env python3
"""
build_pages_site.py — assemble the server-less GitHub Pages site
==================================================================
Lays out _site/ for actions/upload-pages-artifact:

  _site/index.html        ← louisiana-campaign-finance.html, verbatim
  _site/static_api.js     ← the static data layer, verbatim
  _site/sw.js             ← service worker (shell + lib cache; root scope)
  _site/vendor/           ← self-hosted Leaflet + Chart.js (+ images) + fonts/
  _site/manifest.json + icons  ← PWA assets (committed, see build_pwa_icons.py)
  _site/.nojekyll
  _site/data/             ← every .la_cache/*.json.gz (records + nightly
                            artifacts, seeded from the data-cache release by
                            fetch_cache_assets.py) plus the committed root
                            data files the static layer reads

Completeness gate: a deploy with missing year files or artifacts would be a
silently-broken site, so this script FAILS unless everything the static data
layer can ask for is present. Better no deploy than a wrong one.

Stdlib only. Run by .github/workflows/deploy-pages.yml.
"""
import glob
import os
import re
import shutil
import sys
from datetime import date

BASE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(BASE, '.la_cache')
SITE = os.path.join(BASE, '_site')

# Artifacts the static data layer fetches by name (see static_api.js)
REQUIRED_ARTIFACTS = [
    'la_search_index.json.gz',
    'la_races.json.gz',
    'la_overview.json.gz',
    'la_industry_breakdown.json.gz',
    'la_insights.json.gz',
    'la_entities.json.gz',
    'la_candidate_index.json.gz',
]
# Optional artifacts — shipped via the *.json.gz glob below when present, but a
# missing one is NOT site-breaking (the static layer degrades gracefully), so it
# must not block a deploy. la_filer_index.json.gz lands here on the first nightly
# after it was introduced; until then candidateHistory falls back to name-keyed.
OPTIONAL_ARTIFACTS = [
    'la_filer_index.json.gz',
]
# Committed repo-root files the layer also reads
REQUIRED_ROOT = [
    'la_candidacies_raw.json.gz',
    'ethics_coh_cache.json',
    'la_election_results.json',
]


# The SPA decides at runtime whether to answer /api/* reads from a live server
# or from the static artifacts (StaticAPI). Its hostname heuristic only matches
# *.github.io, so a Pages site on a custom domain (e.g. charliestephens.xyz)
# would wrongly try to reach a server and 404. The static build KNOWS its output
# is server-less, so it hard-pins STATIC_MODE on — domain-agnostic and immune to
# any future domain change. The source file is untouched, so the live server
# (which serves it verbatim) still evaluates STATIC_MODE as false.
_STATIC_MODE_RE = re.compile(r'const STATIC_MODE = [^;]*;', re.S)


def _emit_index_html(src, dst):
    with open(src, encoding='utf-8') as f:
        html = f.read()
    html, n = _STATIC_MODE_RE.subn(
        'const STATIC_MODE = true; '
        '/* hard-pinned by build_pages_site.py: this artifact is the static '
        'Pages site (server-less, any domain) */',
        html, count=1)
    if n != 1:
        # Fail loud, consistent with the completeness gate: a missed injection
        # would publish a site that 404s on every /api read on a custom domain.
        raise SystemExit('build_pages_site.py: could not pin STATIC_MODE — the '
                         '`const STATIC_MODE = ...;` declaration was not found '
                         'in louisiana-campaign-finance.html')
    with open(dst, 'w', encoding='utf-8') as f:
        f.write(html)


def main():
    problems = []

    # ── completeness: artifacts ───────────────────────────────────────────
    for name in REQUIRED_ARTIFACTS:
        if not os.path.exists(os.path.join(CACHE, name)):
            problems.append(f'missing artifact: .la_cache/{name}')
    for name in REQUIRED_ROOT:
        if not os.path.exists(os.path.join(BASE, name)):
            problems.append(f'missing committed file: {name}')

    # Optional artifacts: note but never block (the static layer degrades).
    for name in OPTIONAL_ARTIFACTS:
        if not os.path.exists(os.path.join(CACHE, name)):
            print(f'  note: optional artifact .la_cache/{name} not present yet '
                  '(static layer falls back gracefully)')

    # ── completeness: record year files for every cycle the UI offers ────
    # Contributions must cover 2000..current contiguously (the state's
    # bundles start at 2000; the 1999 half of the 1999–2000 cycle doesn't
    # exist upstream and both server and static layer skip it identically).
    # Expenditures/loans: require at least one file per 4-year bundle.
    this_year = date.today().year
    for yr in range(2000, this_year + 1):
        if not os.path.exists(os.path.join(CACHE, f'contributions_yr{yr}.json.gz')):
            problems.append(f'missing record file: contributions_yr{yr}.json.gz')
    for rtype in ('expenditures', 'loans'):
        for start in range(2000, this_year + 1, 4):
            bundle_years = range(start, min(start + 4, this_year + 1))
            if not any(os.path.exists(os.path.join(CACHE, f'{rtype}_yr{y}.json.gz'))
                       for y in bundle_years):
                problems.append(f'missing record bundle: {rtype} {start}-{start+3}')

    if problems:
        print('SITE ASSEMBLY BLOCKED — deploying would publish a broken site:')
        for p in problems:
            print(f'  ✗ {p}')
        return 1

    # ── assemble ──────────────────────────────────────────────────────────
    if os.path.exists(SITE):
        shutil.rmtree(SITE)
    data_dir = os.path.join(SITE, 'data')
    os.makedirs(data_dir)

    _emit_index_html(os.path.join(BASE, 'louisiana-campaign-finance.html'),
                     os.path.join(SITE, 'index.html'))
    shutil.copy2(os.path.join(BASE, 'static_api.js'), SITE)
    # sw.js must sit at the site root so its scope covers the whole project path.
    shutil.copy2(os.path.join(BASE, 'sw.js'), SITE)
    # Self-hosted Leaflet + Chart.js (+ Leaflet's images/) — served same-origin,
    # injected lazily by ensureLeaflet()/ensureChart().
    shutil.copytree(os.path.join(BASE, 'vendor'), os.path.join(SITE, 'vendor'))
    for name in ('manifest.json', 'icon-192.png', 'icon-512.png',
                 'apple-touch-icon.png'):
        shutil.copy2(os.path.join(BASE, name), SITE)
    open(os.path.join(SITE, '.nojekyll'), 'w').close()
    open(os.path.join(SITE, 'CNAME'), 'w').write('finance.charliestephens.xyz')

    n = 0
    for path in sorted(glob.glob(os.path.join(CACHE, '*.json.gz'))):
        shutil.copy2(path, data_dir)
        n += 1
    for name in REQUIRED_ROOT:
        shutil.copy2(os.path.join(BASE, name), data_dir)
        n += 1

    total_mb = sum(os.path.getsize(os.path.join(dp, f))
                   for dp, _, fs in os.walk(SITE) for f in fs) / 1e6
    print(f'Assembled _site/: {n} data files, {total_mb:.0f} MB total')
    if total_mb > 900:
        print('WARNING: approaching the 1GB GitHub Pages site limit')
    return 0


if __name__ == '__main__':
    sys.exit(main())
