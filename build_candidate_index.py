#!/usr/bin/env python3
"""Build la_candidate_index.json.gz — per-candidate, per-cycle financial summary.

Reads all .la_cache/contributions_yr*.json.gz, expenditures_yr*.json.gz, and
loans_yr*.json.gz and emits a compact index keyed by normalized candidate name:

  "JEFF LANDRY": {
    "cycles": {
      "2024-2027": {"raised":6618001,"spent":0,"borrowed":0,"n_c":3527,"n_e":0,"n_l":0,"donors":2780},
      "2020-2023": {"raised":4100000,"spent":3850000,...}
    },
    "total_raised": ..., "total_spent": ..., "total_borrowed": ...,
    "first_cycle": "2016-2019", "last_cycle": "2024-2027", "n_cycles": 3
  }

Used by the /api/candidate-history server endpoint to power the multi-cycle
career tab in the campaign profile modal without loading full cycle files.
"""
import json, gzip, os, re, time
from collections import defaultdict

CACHE = '.la_cache'
OUT   = 'la_candidate_index.json.gz'

def get_cycle(year):
    y = int(year)
    if y >= 2024: return '2024-2027'
    if y >= 2020: return '2020-2023'
    if y >= 2016: return '2016-2019'
    if y >= 2012: return '2012-2015'
    if y >= 2008: return '2008-2011'
    if y >= 2004: return '2004-2007'
    return '2000-2003'

def normalize(name):
    name = name.upper()
    name = re.sub(r'\b(DR|MR|MRS|MS|JR|SR|II|III|IV|ESQ|PHD|MD)\.?\b', '', name)
    name = re.sub(r'[^A-Z\s]', ' ', name)
    return ' '.join(name.split())

# norm_name -> cycle -> {raised, spent, borrowed, n_c, n_e, n_l, donors: set}
_empty = lambda: {'raised':0,'spent':0,'borrowed':0,'n_c':0,'n_e':0,'n_l':0,'donors':set()}
index  = defaultdict(lambda: defaultdict(_empty))

def ingest(path, kind):
    n = 0
    with gzip.open(path, 'rt', encoding='utf-8') as f:
        for line in f:
            try:
                r    = json.loads(line)
                cand = r.get('candidate')
                if not cand or cand == 'Unknown': continue
                norm  = normalize(cand)
                if len(norm.split()) < 2: continue
                year  = (r.get('date') or '0000-')[:4]
                cycle = get_cycle(year)
                amt   = float(r.get('amount') or 0)
                e     = index[norm][cycle]
                if kind == 'c':
                    e['raised'] += amt; e['n_c'] += 1
                    donor = (r.get('contributor') or '').strip()
                    if donor: e['donors'].add(donor)
                elif kind == 'e':
                    e['spent']    += amt; e['n_e'] += 1
                else:
                    e['borrowed'] += amt; e['n_l'] += 1
                n += 1
            except: pass
    return n

t0 = time.time()
total = 0
for fn in sorted(os.listdir(CACHE)):
    fp = os.path.join(CACHE, fn)
    if   fn.startswith('contributions_yr'):  k = 'c'
    elif fn.startswith('expenditures_yr'):   k = 'e'
    elif fn.startswith('loans_yr'):          k = 'l'
    else: continue
    n = ingest(fp, k)
    print(f'  {fn}: {n:,} records')
    total += n

print(f'Ingested {total:,} records in {time.time()-t0:.1f}s. Building index...')

CYCLE_ORDER = ['2000-2003','2004-2007','2008-2011','2012-2015',
               '2016-2019','2020-2023','2024-2027']

out = {}
for norm, cycles in index.items():
    cycle_data = {}
    total_raised = total_spent = total_borrowed = 0
    for cycle_label in CYCLE_ORDER:
        if cycle_label not in cycles: continue
        d = cycles[cycle_label]
        raised    = round(d['raised'],    2)
        spent     = round(d['spent'],     2)
        borrowed  = round(d['borrowed'],  2)
        total_raised   += raised
        total_spent    += spent
        total_borrowed += borrowed
        cycle_data[cycle_label] = {
            'raised': raised, 'spent': spent, 'borrowed': borrowed,
            'n_c': d['n_c'], 'n_e': d['n_e'], 'n_l': d['n_l'],
            'donors': len(d['donors']),
        }
    if not cycle_data: continue
    labels = list(cycle_data.keys())
    out[norm] = {
        'cycles':          cycle_data,
        'total_raised':    round(total_raised,   2),
        'total_spent':     round(total_spent,    2),
        'total_borrowed':  round(total_borrowed, 2),
        'first_cycle':     labels[0],
        'last_cycle':      labels[-1],
        'n_cycles':        len(labels),
    }

with gzip.open(OUT, 'wt', encoding='utf-8') as f:
    json.dump(out, f, separators=(',',':'))

import os as _os
sz = _os.path.getsize(OUT) / 1024
print(f'Wrote {OUT}: {len(out):,} candidates, {sz:.0f} KB')
