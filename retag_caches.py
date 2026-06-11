#!/usr/bin/env python3
"""Nightly annotation pass over the cached .la_cache/*.json.gz records:

1. Party re-tag — re-runs lookup_party with the current politician lookup so
   corrections and matcher improvements reach old-cycle files that are never
   re-parsed from CSV.
2. Transfer flagging (contributions only) — sets isTransfer=true when the
   contributor resolves to a known kind='committee' entity (name or alias) in
   la_entities.json.gz. Person-named contributors are deliberately NOT flagged:
   a candidate committee files under the person's name, so we cannot tell a
   politician's personal donation apart from their committee's transfer — and
   false-flagging personal money would be worse than missing some transfers.

Note: in the nightly workflow this runs AFTER build_entities.py, so the
transfer pass always has a same-night entity table. (It used to run before,
relying on the previous night's table seeded from the release — which didn't
exist on the first runs, so no records ever got flagged.)

Rewrites each cache in place (mtime becomes 'now', which is newer than the
lookup file, so _bust_stale_caches leaves them and the 24h TTL resets).
"""
import gzip, json, os, glob, re
from collections import Counter
import la_ethics_server as srv

srv._load_politician_lookup()
CACHE = srv.CACHE_DIR

# Committee-entity name set for transfer detection (empty if no entity table yet)
_ENT_PATH = os.path.join(CACHE, 'la_entities.json.gz')
committee_names = set()
if os.path.exists(_ENT_PATH):
    with gzip.open(_ENT_PATH, 'rt', encoding='utf-8') as f:
        _ents = json.load(f).get('entities', {})
    for e in _ents.values():
        if e.get('kind') == 'committee':
            for nm in [e.get('name', '')] + (e.get('aliases') or []):
                key = re.sub(r'\s+', ' ', nm.strip().upper())
                if key:
                    committee_names.add(key)
    print(f'Transfer detection: {len(committee_names)} committee names loaded')
else:
    print('Transfer detection: no entity table yet — skipping isTransfer flags')

def _is_transfer(contributor):
    return re.sub(r'\s+', ' ', (contributor or '').strip().upper()) in committee_names

paths = (glob.glob(os.path.join(CACHE, 'contributions_yr*.json.gz')) +
         glob.glob(os.path.join(CACHE, 'expenditures_yr*.json.gz')) +
         glob.glob(os.path.join(CACHE, 'loans_yr*.json.gz')))

grand = Counter()
n_transfer = 0
transfer_amt = 0.0
for path in sorted(paths):
    is_contrib = 'contributions_' in os.path.basename(path)
    recs, changed = [], 0
    with gzip.open(path, 'rt', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            old = r.get('party')
            new = srv.lookup_party(r.get('candidate', ''))
            if new != old:
                changed += 1
            r['party'] = new
            grand[new] += 1
            if is_contrib and committee_names:
                if _is_transfer(r.get('contributor')):
                    r['isTransfer'] = True
                    n_transfer += 1
                    transfer_amt += float(r.get('amount') or 0)
                else:
                    r.pop('isTransfer', None)
            recs.append(r)
    tmp = path + '.tmp'
    with gzip.open(tmp, 'wt', encoding='utf-8', compresslevel=1) as f:
        for r in recs:
            f.write(json.dumps(r, separators=(',', ':')) + '\n')
    os.replace(tmp, path)
    print(f'  {os.path.basename(path):28} {len(recs):>8} recs, {changed:>6} re-tagged')

print('\nParty distribution across all re-tagged records:')
for k, v in grand.most_common():
    print(f'  {k:5} {v:,}')
if committee_names:
    print(f'\nInter-committee transfers flagged: {n_transfer:,} records, ${transfer_amt:,.0f}')
