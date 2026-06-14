#!/usr/bin/env python3
"""
fetch_cache_assets.py — pull .la_cache/ snapshots from the data-cache release
================================================================================
Run during the Render build (see render.yaml) so every deploy boots with a
warm contribution/expenditure/loan cache instead of re-downloading 100MB+
CSVs from ethics.la.gov on the first visitor's request.

Assets live on the rolling `data-cache` GitHub release, refreshed nightly by
.github/workflows/nightly-data.yml. Repo is public, so no token is needed.

Non-fatal by design: if the release doesn't exist yet (first deploy before
the first workflow run) the build proceeds with a cold cache — the server
self-warms the current cycle in a background thread at boot.

Stdlib only.
"""
import json
import os
import sys
import time
import urllib.request

REPO    = 'doombot50/la-campaign-finance'
TAG     = 'data-cache'
API_URL = f'https://api.github.com/repos/{REPO}/releases/tags/{TAG}'
DEST    = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.la_cache')

UA = {'User-Agent': 'la-campaign-finance-build/1.0',
      'Accept': 'application/vnd.github+json'}

RETRIES = 3   # per asset, with exponential backoff


def _download_asset(url, dest, expected_size):
    """Download one asset atomically with retry. Writes to .part then renames,
    so a partial/truncated body never lands as a real cache file. Verifies the
    byte count against the release metadata when known — a short read that
    doesn't raise (CDN hiccup) is treated as a failure and retried.
    Returns True on success, False after exhausting retries."""
    part = dest + '.part'
    last_err = None
    for attempt in range(1, RETRIES + 1):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': UA['User-Agent']})
            n = 0
            with urllib.request.urlopen(req, timeout=120) as resp, open(part, 'wb') as out:
                while chunk := resp.read(1 << 16):
                    out.write(chunk)
                    n += len(chunk)
            if expected_size and n != expected_size:
                raise IOError(f'size mismatch: got {n:,}, expected {expected_size:,}')
            os.replace(part, dest)   # atomic — only a complete file is visible
            return True
        except Exception as e:
            last_err = e
            if os.path.exists(part):
                try: os.remove(part)
                except OSError: pass
            if attempt < RETRIES:
                backoff = 2 ** attempt   # 2s, 4s
                print(f'    {os.path.basename(dest)}: attempt {attempt}/{RETRIES} '
                      f'failed ({e}); retrying in {backoff}s')
                time.sleep(backoff)
    print(f'  FAILED {os.path.basename(dest)}: {last_err}')
    return False


def main():
    os.makedirs(DEST, exist_ok=True)
    try:
        req = urllib.request.Request(API_URL, headers=UA)
        with urllib.request.urlopen(req, timeout=30) as resp:
            release = json.load(resp)
    except Exception as e:
        print(f'WARN: could not fetch release "{TAG}" ({e}). '
              f'Building with cold cache; server will self-warm at boot.')
        return

    assets = [a for a in release.get('assets', [])
              if a.get('name', '').endswith('.json.gz')]
    if not assets:
        print(f'WARN: release "{TAG}" has no .json.gz assets yet. Cold cache.')
        return

    ok = 0
    for a in assets:
        if _download_asset(a['browser_download_url'],
                           os.path.join(DEST, a['name']), a.get('size')):
            print(f'  {a["name"]}  ({a.get("size", 0):,} bytes)')
            ok += 1

    print(f'Downloaded {ok}/{len(assets)} cache assets into {DEST}')
    if ok < len(assets):
        # Partial cache is still better than none for Render (server self-warms);
        # the Pages completeness gate (build_pages_site.py) still blocks a
        # broken deploy, so this never publishes a half-empty site.
        print('WARN: some assets failed after retries; missing cycles will '
              'download on demand (Render) or block the Pages deploy.')


if __name__ == '__main__':
    main()
