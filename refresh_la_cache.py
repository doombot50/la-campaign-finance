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
import os
import sys
import time

import la_ethics_server as srv

# What the dashboard serves. Keep in sync with the server's boot prefetch
# and the cycle selector in louisiana-campaign-finance.html.
REQUIRED = {
    'contributions': ['2020-2023', '2024-2027'],
    'expenditures':  ['2024-2027'],
    'loans':         ['2024-2027'],
}
CURRENT_KEY = srv.get_csv_key(time.localtime().tm_year)


def _existing_year_files(key, rtype):
    return [p for y in srv._key_years(key)
            if os.path.exists(p := srv._year_cache_path(y, rtype))]


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
