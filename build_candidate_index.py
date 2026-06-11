#!/usr/bin/env python3
"""Build la_candidate_index.json.gz — per-candidate, per-cycle financial summary
plus monthly in/out buckets for the cross-cycle cash-on-hand chart.

  "JEFF LANDRY": {
    "cycles": {
      "2024-2027": {"raised":6618001,"spent":0,"borrowed":0,"n_c":3527,"n_e":0,"n_l":0,"donors":2780,"transfers_in":50000,"n_t":4},
      "2020-2023": {...}
    },
    "monthly": {                         ← keyed YYYY-MM
      "2016-03": {"in":5000,"out":0},
      "2020-09": {"in":120000,"out":45000},
      ...
    },
    "total_raised": ..., "total_spent": ..., "total_borrowed": ...,
    "total_transfers_in": ...,
    "first_cycle": "2016-2019", "last_cycle": "2024-2027", "n_cycles": 3
  }

`raised` stays GROSS (all contribution records). `transfers_in` is the slice of
`raised` that came from committee-to-committee transfers (records flagged
isTransfer by retag_caches.py), so consumers can disclose net = raised −
transfers_in, matching the dashboard's headline Total Raised semantics.

Run nightly by .github/workflows/nightly-data.yml after retag_caches.py.
Output goes to .la_cache/ so it ships with the data-cache release upload glob;
the server prefers that copy over the committed repo-root fallback when newer.
"""
import json, gzip, os, re, time
from collections import defaultdict

BASE  = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(BASE, '.la_cache')
OUT   = os.path.join(CACHE, 'la_candidate_index.json.gz')

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

def month_key(date_str):
    """'2023-09-14' or '2023-09' -> 'YYYY-MM', returns None if unparseable."""
    d = (date_str or '').strip()
    if len(d) >= 7 and d[4] == '-':
        return d[:7]
    return None

# norm_name -> cycle -> {raised, spent, borrowed, n_c, n_e, n_l, donors: set, ...}
_empty = lambda: {'raised':0,'spent':0,'borrowed':0,'transfers_in':0,
                  'n_c':0,'n_e':0,'n_l':0,'n_t':0,'donors':set()}
index  = defaultdict(lambda: defaultdict(_empty))

# norm_name -> month_key -> {in: float, out: float}
monthly = defaultdict(lambda: defaultdict(lambda: {'in':0.0,'out':0.0}))

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
                date  = r.get('date') or ''
                year  = date[:4]
                if not year.isdigit(): continue
                cycle = get_cycle(year)
                amt   = float(r.get('amount') or 0)
                mk    = month_key(date)

                e = index[norm][cycle]
                if kind == 'c':
                    e['raised'] += amt; e['n_c'] += 1
                    if r.get('isTransfer'):
                        e['transfers_in'] += amt; e['n_t'] += 1
                    donor = (r.get('contributor') or '').strip()
                    if donor: e['donors'].add(donor)
                    if mk: monthly[norm][mk]['in'] += amt
                elif kind == 'e':
                    e['spent']    += amt; e['n_e'] += 1
                    if mk: monthly[norm][mk]['out'] += amt
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
    total_raised = total_spent = total_borrowed = total_transfers = 0
    for cycle_label in CYCLE_ORDER:
        if cycle_label not in cycles: continue
        d = cycles[cycle_label]
        raised    = round(d['raised'],       2)
        spent     = round(d['spent'],        2)
        borrowed  = round(d['borrowed'],     2)
        xfer      = round(d['transfers_in'], 2)
        total_raised    += raised
        total_spent     += spent
        total_borrowed  += borrowed
        total_transfers += xfer
        cycle_data[cycle_label] = {
            'raised': raised, 'spent': spent, 'borrowed': borrowed,
            'n_c': d['n_c'], 'n_e': d['n_e'], 'n_l': d['n_l'],
            'donors': len(d['donors']),
        }
        if xfer:
            cycle_data[cycle_label]['transfers_in'] = xfer
            cycle_data[cycle_label]['n_t'] = d['n_t']
    if not cycle_data: continue
    labels = list(cycle_data.keys())

    # Monthly buckets — only include months with non-zero activity, rounded to 2dp
    mon = {}
    for mk, vals in sorted(monthly[norm].items()):
        i = round(vals['in'],  2)
        o = round(vals['out'], 2)
        if i or o:
            mon[mk] = {'in': i, 'out': o}

    out[norm] = {
        'cycles':          cycle_data,
        'monthly':         mon,
        'total_raised':    round(total_raised,   2),
        'total_spent':     round(total_spent,    2),
        'total_borrowed':  round(total_borrowed, 2),
        'first_cycle':     labels[0],
        'last_cycle':      labels[-1],
        'n_cycles':        len(labels),
    }
    if total_transfers:
        out[norm]['total_transfers_in'] = round(total_transfers, 2)

tmp = OUT + '.tmp'
with gzip.open(tmp, 'wt', encoding='utf-8') as f:
    json.dump(out, f, separators=(',',':'))
os.replace(tmp, OUT)

sz = os.path.getsize(OUT) / 1024
n_xf = sum(1 for v in out.values() if v.get('total_transfers_in'))
xf_total = sum(v.get('total_transfers_in', 0) for v in out.values())
print(f'Wrote {OUT}: {len(out):,} candidates, {sz:.0f} KB')
print(f'  transfer-aware: {n_xf:,} entities with transfers_in totaling ${xf_total:,.0f}')
