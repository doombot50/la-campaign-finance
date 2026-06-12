#!/usr/bin/env python3
"""
build_static_api.py — nightly static dumps of the read-only API payloads
==========================================================================
Foundation of the server-less ("Tier 3") deployment: every payload the
dashboard reads from /api/* gets a static .json.gz twin built here, nightly,
from the same builder functions the live endpoints call — one computation
site, so the static artifacts cannot drift from what the API would answer.

Artifacts (written to .la_cache/ so the workflow's existing upload glob
ships them with the data-cache release):

  la_search_index.json.gz       full entity search index (entries incl.
                                name_upper); the static client runs the same
                                two-tier ranking /api/search runs server-side
  la_races.json.gz              build_races_payload('all', '') — every race;
                                the static client filters office/year locally
  la_overview.json.gz           build_overview_payload() — landing-page stats
                                ('upcoming' flags are as-of built_at; the
                                client can re-derive from each entry's iso)
  la_industry_breakdown.json.gz per-(filer, cycle) industry breakdowns in a
                                single streaming pass over the contribution
                                caches (the endpoint's per-filer scan, done
                                once for everyone)

test_static_parity.py asserts these match the live endpoints byte-for-byte
before the nightly upload publishes them.

Run by .github/workflows/nightly-data.yml after build_insights.py.
Stdlib only.
"""
import glob
import gzip
import json
import os
import re
import sys
from datetime import datetime, timezone

import la_ethics_server as srv


def _write(name, payload):
    path = os.path.join(srv.CACHE_DIR, name)
    tmp = path + '.tmp'
    with gzip.open(tmp, 'wt', encoding='utf-8') as f:
        json.dump(payload, f, separators=(',', ':'))
    os.replace(tmp, path)
    print(f'  {name}: {os.path.getsize(path)/1024:.0f} KB')


def build_industry_dump():
    """Single pass over all contribution caches -> per-(filer, cycle)
    breakdowns identical to build_industry_breakdown(filer, cycle).

    Per-filer buckets accumulate in file order within each cycle's year
    files — the same order the endpoint's own scan sees — so top-donor
    ties and first-seen industry ordering resolve identically.
    """
    srv._load_donor_industries()
    # cycle_key -> filer -> bucket
    cycles: dict = {}
    for path in sorted(glob.glob(os.path.join(srv.CACHE_DIR, 'contributions_yr*.json.gz'))):
        m = re.search(r'_yr(\d{4})', path)
        if not m:
            continue
        year = int(m.group(1))
        cycle_key = srv.get_csv_key(year)
        filers = cycles.setdefault(cycle_key, {})
        with gzip.open(path, 'rt', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                fn = str(rec.get('filerNumber', '') or '')
                if not fn:
                    continue
                bucket = filers.get(fn)
                if bucket is None:
                    bucket = filers[fn] = {'totals': {}, 'counts': {}, 'top_donors': {}}
                srv._industry_bucket_add(bucket, rec)

    out: dict = {}
    n_pairs = 0
    for cycle_key, filers in cycles.items():
        for fn, bucket in filers.items():
            breakdown = srv._industry_bucket_payload(bucket)
            if not breakdown:
                continue
            out.setdefault(fn, {})[cycle_key] = breakdown
            n_pairs += 1
    print(f'  industry: {len(out)} filers, {n_pairs} (filer, cycle) pairs')
    return out


def main():
    built_at = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    print('Building static API artifacts...')

    _write('la_search_index.json.gz', {
        'built_at': built_at,
        'entries': srv.build_search_entries(),
    })

    _write('la_races.json.gz', {
        'built_at': built_at,
        **srv.build_races_payload('all', ''),
    })

    _write('la_overview.json.gz', {
        'built_at': built_at,
        **srv.build_overview_payload(),
    })

    _write('la_industry_breakdown.json.gz', {
        'built_at': built_at,
        'has_industry_data': bool(srv._DONOR_IND),
        'filers': build_industry_dump(),
    })

    print('Done.')


if __name__ == '__main__':
    sys.exit(main())
