#!/usr/bin/env python3
"""
refresh_la_cache.py — refresh .la_cache/ NDJSON files (nightly workflow helper)
================================================================================
Run by .github/workflows/nightly-data.yml on the Actions runner, after the
runner seeds .la_cache/ from the `data-cache` GitHub release.

Behaviour per (report_type, csv_key):
  - current key (2024-2027): ALWAYS re-downloaded — existing files get their
    mtime pushed past CACHE_TTL so download_and_cache's freshness gate lets
    the download proceed. Atomic .tmp -> rename means a failed download
    leaves the seeded files intact.
  - older keys: downloaded only if any per-year file is missing (e.g. first
    seed run, or a release asset was lost). Otherwise untouched.

Stdlib only. Reuses la_ethics_server's streaming CSV downloader, so peak
memory is O(1) per record regardless of CSV size.
"""
import gzip
import json
import os
import sys
import time

import la_ethics_server as srv

# Every bundle the dashboard can serve (cycle selector goes back to 2000).
# Older keys are a one-time download — after the first run they live in the
# release and get seeded, so each night only re-downloads the current cycle.
# Carrying the full history in the release means Render's build seeds every
# cycle, and the free instance never has to pull a 100MB CSV from
# ethics.la.gov at request time.
REQUIRED = {
    'contributions': sorted(srv.CSV_URLS),
    'expenditures':  sorted(srv.EXPENDITURE_URLS),
    'loans':         sorted(srv.LOAN_URLS),
}
CURRENT_KEY = srv.get_csv_key(time.localtime().tm_year)


def _existing_year_files(key, rtype):
    return [p for y in srv._key_years(key)
            if os.path.exists(p := srv._year_cache_path(y, rtype))]


def _needs_schema_backfill(paths):
    """True if the first cached record lacks 'reportNumber' — i.e. it was parsed
    before the filing-link fields existed. A one-shot trigger to re-download an
    otherwise-skipped historical cycle so every transaction gets its source PDF
    link; becomes a no-op once all cycles carry the field."""
    for p in paths:
        try:
            with gzip.open(p, 'rt', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        return 'reportNumber' not in json.loads(line)
        except Exception:
            return False   # unreadable → leave it; a later run / current-cycle path handles it
    return False


def main():
    os.makedirs(srv.CACHE_DIR, exist_ok=True)
    failures = []

    for rtype, keys in REQUIRED.items():
        for key in keys:
            have = _existing_year_files(key, rtype)
            if key == CURRENT_KEY:
                # Force: age existing files past the TTL so the gate opens.
                old = time.time() - srv.CACHE_TTL - 60
                for p in have:
                    os.utime(p, (old, old))
                print(f'[{rtype}/{key}] current cycle -> forcing re-download')
            elif have and _needs_schema_backfill(have):
                old = time.time() - srv.CACHE_TTL - 60
                for p in have:
                    os.utime(p, (old, old))
                print(f'[{rtype}/{key}] present but missing filing fields -> re-downloading (backfill)')
            elif have:
                print(f'[{rtype}/{key}] {len(have)} year files present -> skip')
                continue
            else:
                print(f'[{rtype}/{key}] missing -> downloading')

            try:
                srv.download_and_cache(key, rtype)
            except Exception as e:
                print(f'[{rtype}/{key}] FAILED: {e}')
                failures.append(f'{rtype}/{key}')

    if failures:
        print(f'\nERROR: {len(failures)} download(s) failed: {", ".join(failures)}')
        sys.exit(1)
    print('\nAll cache files refreshed.')


if __name__ == '__main__':
    main()
