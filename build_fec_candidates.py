#!/usr/bin/env python3
"""
FEC candidate IDs for Louisiana's U.S. House and Senate candidates
==================================================================
Federal candidates report their money to the FEC, so the dashboard shows no
fundraising for federal races and links each candidate to the FEC instead.
This builds the map that lets that link go straight to the candidate's FEC
page (fec.gov/data/candidate/<id>/) rather than to a name search.

Output: la_fec_candidates.json (committed; served at data/la_fec_candidates.json
by the live server's repo-root fallback and shipped by build_pages_site.py).

    {"seats": {"H": {"LETLOW": [{"id": "H0LA05202", "first": "JULIA BARNHILL",
                                  "years": [2020, 2021, 2022, ...]}]},
               "S": {...}}}

A seat is the chamber, "H" or "S" — not the district: FEC keeps only a
candidate's latest district (Peter Williams ran in LA-5 in 2014 but is filed
under LA-2), so district would miss real matches. Each candidate is listed
under every token of their FEC last name, so a SoS ballot name that uses only
one of them ("KAREN CARTER" for "CARTER PETERSON, KAREN") still finds them.
`years` is the union of FEC election_years and cycles.

The matcher (`match`) is mirrored by `_fecIdFor` in the dashboard; the two
must agree (tests/test_fec_candidates.py and tests/test_frontend_units.mjs
pin the same cases). It returns an ID only when exactly one candidate fits
seat + last name + year (+ first name when needed) — otherwise the dashboard
falls back to the FEC name search, so a namesake never gets the wrong link.

Usage:
    python3 build_fec_candidates.py            # fetch, report coverage, write if changed
    python3 build_fec_candidates.py --check    # coverage report only, no write

Stdlib only. Uses FEC_API_KEY from the environment if set, else the key
already committed in build_politician_lookup.py.
"""

import argparse, gzip, json, os, re, sys, time, urllib.parse, urllib.request

BASE = os.path.dirname(os.path.abspath(__file__))
OUT_FILE = os.path.join(BASE, 'la_fec_candidates.json')
CANDIDACIES = os.path.join(BASE, 'la_candidacies_raw.json.gz')
FEC_BASE = 'https://api.open.fec.gov/v1'
MIN_RECORDS = 300   # LA has ~700 House/Senate candidates on file; fewer = a bad fetch

_SUFFIX = re.compile(r'\b(DR|MR|MRS|MS|JR|SR|II|III|IV|ESQ|PHD|MD|HON)\.?\b')


def norm(s):
    """Same normalization as the dashboard's _normNameJS."""
    s = _SUFFIX.sub('', (s or '').upper())
    s = re.sub(r'[^A-Z\s]', ' ', s)
    return ' '.join(s.split())


def seat_of(office):
    """SoS office string → 'S', 'H', or None (not a House/Senate race)."""
    o = (office or '').upper()
    if re.search(r'\bU\.?\s*S\.?\s+SENAT|UNITED STATES SENAT', o):
        return 'S'
    if re.search(r'\bU\.?\s*S\.?\s+REP|UNITED STATES REP|CONGRESS', o):
        return 'H'
    return None


def year_of(date):
    p = (date or '').split('/')
    try:
        return int(p[2]) if len(p) == 3 else int((date or '')[:4])
    except ValueError:
        return 0


def match(seats, name, office, date):
    """FEC candidate ID for a SoS candidacy, or None when not exactly one fits."""
    seat = seat_of(office)
    toks = norm(name).split()
    if not seat or not toks or not seats:
        return None
    pool = (seats.get(seat) or {}).get(toks[-1]) or []
    yr = year_of(date)
    if yr:
        # Odd-year special elections fall in the next even FEC cycle.
        pool = [c for c in pool if yr in c['years'] or yr + 1 in c['years']]
    ids = {c['id'] for c in pool}
    if len(ids) > 1:
        first = toks[0]
        pool = [c for c in pool if c['first'].split()[:1] == [first]]
        ids = {c['id'] for c in pool}
    return next(iter(ids)) if len(ids) == 1 else None


def _api_key():
    key = os.environ.get('FEC_API_KEY')
    if key:
        return key
    sys.path.insert(0, BASE)
    from build_politician_lookup import DEFAULT_KEY
    return DEFAULT_KEY


def fetch(office, key):
    out, page = [], 1
    while True:
        q = urllib.parse.urlencode({'state': 'LA', 'office': office, 'per_page': 100,
                                    'page': page, 'sort': 'name', 'api_key': key})
        for attempt in range(4):
            try:
                with urllib.request.urlopen(f'{FEC_BASE}/candidates/?{q}', timeout=60) as r:
                    data = json.load(r)
                break
            except Exception as e:
                if attempt == 3:
                    raise
                print(f'  {office} page {page}: {e} — retrying')
                time.sleep(2 * (attempt + 1))
        out.extend(data.get('results') or [])
        pages = (data.get('pagination') or {}).get('pages') or 1
        if page >= pages:
            return out
        page += 1
        time.sleep(0.3)


def build_seats(records):
    seats = {}
    for r in records:
        cid = r.get('candidate_id') or ''
        office = r.get('office')
        if office not in ('H', 'S'):
            continue
        seat = office
        raw = r.get('name') or ''
        last, _, first = raw.partition(',')
        last, first = norm(last), norm(first)
        if not cid or not last:
            continue
        years = sorted({int(y) for y in (r.get('election_years') or []) + (r.get('cycles') or [])
                        if str(y).isdigit()})
        entry = {'id': cid, 'first': first, 'years': years}
        for tok in dict.fromkeys(last.split()):
            seats.setdefault(seat, {}).setdefault(tok, []).append(entry)
    for by_last in seats.values():
        for lst in by_last.values():
            lst.sort(key=lambda e: e['id'])
    return {k: dict(sorted(v.items())) for k, v in sorted(seats.items())}


def coverage(seats):
    """Share of House/Senate SoS candidacies that resolve to one FEC ID."""
    if not os.path.exists(CANDIDACIES):
        return None
    with gzip.open(CANDIDACIES, 'rt') as f:
        raw = json.load(f)
    total = hit = 0
    misses = []
    for name, rows in raw.items():
        for c in rows:
            if not seat_of(c.get('office')) or c.get('party_office'):
                continue
            total += 1
            if match(seats, name, c['office'], c.get('date')):
                hit += 1
            else:
                misses.append((c.get('date', ''), name))
    return total, hit, misses


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[1])
    ap.add_argument('--check', action='store_true', help='report coverage, do not write')
    args = ap.parse_args()

    key = _api_key()
    records = fetch('H', key) + fetch('S', key)
    print(f'FEC records: {len(records)}')
    if len(records) < MIN_RECORDS:
        sys.exit(f'only {len(records)} FEC records (< {MIN_RECORDS}) — refusing to write')
    seats = build_seats(records)

    cov = coverage(seats)
    if cov:
        total, hit, misses = cov
        print(f'SoS House/Senate candidacies matched: {hit}/{total} ({hit / max(total, 1):.1%})')
        recent = sorted(m for m in misses if year_of(m[0]) >= 2010)
        if recent:
            print('  unmatched since 2010 (link falls back to FEC search):')
            for d, n in recent[-25:]:
                print(f'    {d}  {n}')

    payload = {
        'source': 'api.open.fec.gov /v1/candidates (state=LA, office=H,S)',
        'seats': seats,
    }
    body = json.dumps(payload, separators=(',', ':'), sort_keys=True) + '\n'
    if args.check:
        return
    old = open(OUT_FILE).read() if os.path.exists(OUT_FILE) else None
    if old == body:
        print('la_fec_candidates.json unchanged')
        return
    with open(OUT_FILE + '.tmp', 'w') as f:
        f.write(body)
    os.replace(OUT_FILE + '.tmp', OUT_FILE)
    print(f'wrote la_fec_candidates.json ({len(body) // 1024} KB)')


if __name__ == '__main__':
    main()
