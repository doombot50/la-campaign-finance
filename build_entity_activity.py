#!/usr/bin/env python3
"""Reshard transactions BY FILER so a committee's complete activity loads as one
small fetch instead of every cycle's rows — the "FULL ACTIVITY" view.

The data normally ships sharded by YEAR (load a year = everyone's rows). For
"all of one committee's activity" that's backwards. This regroups each filer's
own contributions-received, expenditures, and loans across all cycles.

Lifetime *totals* are already exact from the aggregates (la_candidate_index +
la_entity_donors), and a browser can't usefully render 200k rows (the table
paginates at 20), so each list is capped at the CAP most-recent rows. The total
count is kept alongside so the UI can disclose "showing N of M". Most committees
fall under the cap and show everything; only a handful of statewide/long-tenured
filers are trimmed.

  la_entity_activity.json.gz   filerNumber -> {
      c:[{d,c,a,p,z,pa}],   # received contributions (most recent CAP)
      e:[{d,r,a,de,pa}],    # expenditures
      l:[{d,l,a,rt}],       # loans
      nc, ne, nl,           # TOTAL counts (for "showing N of M")
      cap }

Run after refresh_la_cache.py. Local check on a subset:
    py build_entity_activity.py --since 2022 --probe 1625
"""
import os, json, gzip, time, sys, glob, re

BASE  = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(BASE, '.la_cache')
OUT   = os.path.join(CACHE, 'la_entity_activity.json.gz')
CAP   = 1500   # most-recent rows kept per category per filer

_since = int(sys.argv[sys.argv.index('--since') + 1]) if '--since' in sys.argv else 0
_probe = sys.argv[sys.argv.index('--probe') + 1] if '--probe' in sys.argv else None

# filer -> {'c':[], 'e':[], 'l':[], 'nc':0, 'ne':0, 'nl':0}
act = {}
def _bucket(fn):
    b = act.get(fn)
    if b is None:
        b = {'c': [], 'e': [], 'l': [], 'nc': 0, 'ne': 0, 'nl': 0}
        act[fn] = b
    return b

# Slimmers — short keys, only the columns the profile tables render
# (s = the row's state: contributor / recipient / lender respectively).
def _slim_c(r): return {'d': r.get('date'), 'c': r.get('contributor'), 'a': r.get('amount'),
                        'p': r.get('party'), 'z': r.get('contributorZip'),
                        'pa': r.get('parish'), 's': r.get('contributorState')}
def _slim_e(r): return {'d': r.get('date'), 'r': r.get('contributor'), 'a': r.get('amount'),
                        'de': r.get('description'), 'pa': r.get('parish'),
                        's': r.get('recipientState')}
def _slim_l(r): return {'d': r.get('date'), 'l': r.get('contributor'), 'a': r.get('amount'),
                        'rt': r.get('rate'), 'pa': r.get('parish'), 's': r.get('lenderState')}

def ingest(kind, slim, lst_key, cnt_key):
    """Process every year file for one record type, newest year first so the
    kept rows are the most recent. Counts are exact; lists stop at CAP."""
    n = 0
    files = sorted(glob.glob(os.path.join(CACHE, f'{kind}_yr*.json.gz')), reverse=True)
    for path in files:
        m = re.search(r'_yr(\d{4})', path)
        if _since and m and int(m.group(1)) < _since:
            continue
        with gzip.open(path, 'rt', encoding='utf-8') as f:
            for line in f:
                try:
                    r = json.loads(line)
                except Exception:
                    continue
                fn = (r.get('filerNumber') or '').strip()
                if not fn:
                    continue
                b = _bucket(fn)
                b[cnt_key] += 1
                if len(b[lst_key]) < CAP:
                    b[lst_key].append(slim(r))
                n += 1
    return n

t0 = time.time()
nc = ingest('contributions', _slim_c, 'c', 'nc')
print(f'  contributions: {nc:,} rows')
ne = ingest('expenditures',  _slim_e, 'e', 'ne')
print(f'  expenditures:  {ne:,} rows')
nl = ingest('loans',         _slim_l, 'l', 'nl')
print(f'  loans:         {nl:,} rows')

# Sort each kept list newest-first (within-year order isn't guaranteed sorted).
for b in act.values():
    b['c'].sort(key=lambda x: x.get('d') or '', reverse=True)
    b['e'].sort(key=lambda x: x.get('d') or '', reverse=True)
    b['l'].sort(key=lambda x: x.get('d') or '', reverse=True)
    b['cap'] = CAP

print(f'Resharded {len(act):,} filers in {time.time()-t0:.1f}s.')

if _probe:
    b = act.get(_probe)
    if b:
        print(f'\n=== filer {_probe}: {b["nc"]:,} contribs / {b["ne"]:,} exps / {b["nl"]:,} loans '
              f'(kept {len(b["c"])}/{len(b["e"])}/{len(b["l"])}) ===')
        for r in b['c'][:5]:
            print(f'  + {r["d"]}  {(r["c"] or "")[:34]:<34} ${(r["a"] or 0):>12,.0f}')
        for r in b['e'][:3]:
            print(f'  - {r["d"]}  {(r["r"] or "")[:34]:<34} ${(r["a"] or 0):>12,.0f}  {(r["de"] or "")[:24]}')
    else:
        print(f'\n(no activity for filer {_probe})')

with gzip.open(OUT, 'wt', encoding='utf-8') as f:
    json.dump(act, f, separators=(',', ':'), ensure_ascii=False)
print(f'\nWrote {OUT}  ({os.path.getsize(OUT)/1e6:.1f} MB)')
