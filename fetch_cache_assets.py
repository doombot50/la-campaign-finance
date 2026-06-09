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
import urllib.request

REPO    = 'doombot50/la-campaign-finance'
TAG     = 'data-cache'
API_URL = f'https://api.github.com/repos/{REPO}/releases/tags/{TAG}'
DEST    = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.la_cache')

UA = {'User-Agent': 'la-campaign-finance-build/1.0',
      'Accept': 'application/vnd.github+json'}


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
        name, url = a['name'], a['browser_download_url']
        try:
            req = urllib.request.Request(url, headers={'User-Agent': UA['User-Agent']})
            with urllib.request.urlopen(req, timeout=120) as resp, \
                 open(os.path.join(DEST, name), 'wb') as out:
                while chunk := resp.read(1 << 16):
                    out.write(chunk)
            print(f'  {name}  ({a.get("size", 0):,} bytes)')
            ok += 1
        except Exception as e:
            print(f'  FAILED {name}: {e}')

    print(f'Downloaded {ok}/{len(assets)} cache assets into {DEST}')
    if ok < len(assets):
        # Partial cache is still better than none; don't fail the build.
        print('WARN: some assets failed; missing cycles will download on demand.')


if __name__ == '__main__':
    main()
