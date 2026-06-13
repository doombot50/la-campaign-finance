#!/usr/bin/env python3
"""
test_static_parity.py — the trust gate for the static API artifacts
=====================================================================
Boots la_ethics_server.py against the local .la_cache/ and asserts that the
artifacts written by build_static_api.py are byte-equal (as parsed JSON) to
what the live endpoints answer. Runs in the nightly workflow AFTER the dumps
and BEFORE the release upload: a mismatch fails the run, so a drifted
artifact can never ship.

Checks:
  /api/races?office=all          == la_races.json.gz
  /api/overview                  == la_overview.json.gz   (same-day run)
  /api/search?q=…                == the same two-tier ranking applied to
                                    la_search_index.json.gz entries
  /api/industry-breakdown        == la_industry_breakdown.json.gz for a
                                    sample of (filer, cycle) pairs spanning
                                    large and small filers

Stdlib only. Exit 0 = parity; exit 1 = drift (with a diff summary).
"""
import gzip
import json
import os
import socket
import subprocess
import sys
import time
import urllib.request

BASE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(BASE, '.la_cache')

FAILS = []


def check(label, ok, detail=''):
    print(f'  {"OK  " if ok else "FAIL"} {label}' + (f' — {detail}' if detail and not ok else ''))
    if not ok:
        FAILS.append((label, detail))


def load_gz(name):
    with gzip.open(os.path.join(CACHE, name), 'rt', encoding='utf-8') as f:
        return json.load(f)


def get(port, path):
    with urllib.request.urlopen(f'http://127.0.0.1:{port}{path}', timeout=120) as r:
        return json.load(r)


def diff_summary(a, b, path='$'):
    """First point of divergence between two JSON values, human-readable."""
    if type(a) is not type(b):
        return f'{path}: type {type(a).__name__} != {type(b).__name__}'
    if isinstance(a, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a: return f'{path}.{k}: missing on server side'
            if k not in b: return f'{path}.{k}: missing in artifact'
            d = diff_summary(a[k], b[k], f'{path}.{k}')
            if d: return d
        return None
    if isinstance(a, list):
        if len(a) != len(b):
            return f'{path}: length {len(a)} != {len(b)}'
        for i, (x, y) in enumerate(zip(a, b)):
            d = diff_summary(x, y, f'{path}[{i}]')
            if d: return d
        return None
    if a != b:
        return f'{path}: {a!r} != {b!r}'
    return None


def rank_search(entries, q):
    """Mirror of the /api/search two-tier ranking (and of the static client's)."""
    q = q.strip().upper()
    if len(q) < 2:
        return []
    word, contains = [], []
    for e in entries:
        nu = e['name_upper']
        pos = nu.find(q)
        if pos < 0:
            continue
        (word if pos == 0 or nu[pos - 1] == ' ' else contains).append(e)
    word.sort(key=lambda e: -e['total_raised'])
    contains.sort(key=lambda e: -e['total_raised'])
    return [{
        'name':         e['name'],
        'is_candidate': e['is_candidate'],
        'total_raised': e['total_raised'],
        'n_cycles':     e['n_cycles'],
        'n_races':      e['n_races'],
        'last_office':  e['last_office'],
        'last_outcome': e['last_outcome'],
        'last_date':    e['last_date'],
        'filer_number': e['filer_number'],
    } for e in (word + contains)[:25]]


def main():
    # ── Boot the server on a free port ────────────────────────────────────
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        port = s.getsockname()[1]
    # LA_OFFLINE: serve the on-disk snapshot as-is (no prefetch / re-download),
    # so the live API can't drift from the static artifacts mid-comparison.
    env = dict(os.environ, PORT=str(port), LA_OFFLINE='1')
    proc = subprocess.Popen([sys.executable, os.path.join(BASE, 'la_ethics_server.py')],
                            env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(60):
            try:
                get(port, '/health')
                break
            except Exception:
                time.sleep(0.5)
        else:
            print('FATAL: server did not come up'); return 1

        print('Static API parity:')

        # ── races ─────────────────────────────────────────────────────────
        art = load_gz('la_races.json.gz')
        live = get(port, '/api/races?office=all')
        for k in ('races', 'total', 'years'):
            d = diff_summary(live[k], art[k], f'races.{k}')
            check(f'/api/races?office=all · {k}', d is None, d or '')

        # ── overview ──────────────────────────────────────────────────────
        art = load_gz('la_overview.json.gz')
        live = get(port, '/api/overview')
        for k in live:
            d = diff_summary(live[k], art.get(k), f'overview.{k}')
            check(f'/api/overview · {k}', d is None, d or '')

        # ── search ────────────────────────────────────────────────────────
        entries = load_gz('la_search_index.json.gz')['entries']
        for q in ('landry', 'pac', 'broome', 'smith', 'xyzzy-no-match'):
            live = get(port, f'/api/search?q={q}')
            mine = rank_search(entries, q)
            d = diff_summary(live['results'], mine, f'search[{q}]')
            check(f'/api/search?q={q} ({len(mine)} results)', d is None, d or '')

        # ── industry breakdown (sampled) ──────────────────────────────────
        dump = load_gz('la_industry_breakdown.json.gz')['filers']
        # sample: 3 biggest filers by pair count + 2 smallest, on their first cycle
        sized = sorted(dump.items(), key=lambda kv: -sum(len(v) for v in kv[1].values()))
        sample = sized[:3] + sized[-2:]
        for fn, by_cycle in sample:
            cyc = sorted(by_cycle)[0]
            live = get(port, f'/api/industry-breakdown?filer={fn}&cycle={cyc}')
            d = diff_summary(live['breakdown'], by_cycle[cyc], f'industry[{fn}/{cyc}]')
            check(f'/api/industry-breakdown filer={fn} cycle={cyc}', d is None, d or '')

    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()

    if FAILS:
        print(f'\nPARITY FAILED: {len(FAILS)} mismatch(es). Static artifacts must not ship.')
        return 1
    print('\nAll static artifacts match the live API.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
