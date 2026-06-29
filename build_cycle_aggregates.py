#!/usr/bin/env python3
"""Per-cycle contribution aggregates for INSTANT first paint on the Money In tab.

renderStats/renderPartyChart/renderDonorTypeChart all tally additive sums over
the loaded rows. Precomputing those sums per cycle lets the dashboard paint the
headline stat cards + the party/industry breakdowns the instant a cycle (or
several) is selected — summed client-side — while the rows stream in to enable
filtering and the table. Biggest win for the "All cycles" selection Phase 1
unlocked, where there's otherwise nothing to show until ~80 MB has streamed.

Served identically by the live endpoint and the static artifact (both just
return this map), so there is no live-vs-static computation to drift.

  la_cycle_agg.json.gz   cycle ("2024" = 2023-24) -> {
      gross, xfer, xfer_n, n, oos,         # headline stat-card inputs
      by_parish:   {parish: amount},       # excl. Out of State (oos is separate)
      by_party:    {DEM|REP|IND|LBT|GRN|OTH: amount},
      by_industry: {industry: amount} }    # baked industry, donorType fallback

Run AFTER retag_caches.py (so isTransfer + industry are baked on the rows).
"""
import os, json, gzip, time, glob, re
from collections import defaultdict

BASE  = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(BASE, '.la_cache')
OUT   = os.path.join(CACHE, 'la_cycle_agg.json.gz')

# Mirrors the dashboard: a calendar year belongs to the even-year cycle covering
# [c-1, c] (2023 & 2024 -> "2024"). cycle = year rounded up to even.
def _cycle_of(year):
    return year + (year % 2)

_PARTY = {'DEM', 'REP', 'IND', 'LBT', 'GRN', 'OTH'}
# Same legacy donorType -> industry fallback as the dashboard's _getIndustry.
_DT_TO_IND = {'IND': 'Individual', 'PAC': 'PAC/Political', 'BUS': 'Other',
              'UNION': 'Labor/Unions', 'ORG': 'Other', 'OTH': 'Other'}

def _blank():
    return {'gross': 0.0, 'xfer': 0.0, 'xfer_n': 0, 'n': 0, 'oos': 0.0,
            'by_parish': defaultdict(float), 'by_party': defaultdict(float),
            'by_industry': defaultdict(float)}

agg = defaultdict(_blank)

t0 = time.time()
total = 0
for path in sorted(glob.glob(os.path.join(CACHE, 'contributions_yr*.json.gz'))):
    with gzip.open(path, 'rt', encoding='utf-8') as f:
        for line in f:
            try:
                r = json.loads(line)
            except Exception:
                continue
            date = r.get('date') or ''
            yr = date[:4]
            if not yr.isdigit():
                continue
            cyc = str(_cycle_of(int(yr)))
            amt = float(r.get('amount') or 0)
            b = agg[cyc]
            b['gross'] += amt
            b['n'] += 1
            if r.get('isTransfer'):
                b['xfer'] += amt
                b['xfer_n'] += 1
            parish = r.get('parish') or ''
            if parish == 'Out of State':
                b['oos'] += amt
            elif parish:
                b['by_parish'][parish] += amt
            party = r.get('party') or 'OTH'
            b['by_party'][party if party in _PARTY else 'OTH'] += amt
            ind = r.get('industry') or _DT_TO_IND.get(r.get('donorType') or '', 'Other')
            b['by_industry'][ind] += amt
            total += 1

# Round + drop tiny parish noise so the artifact stays small.
out = {}
for cyc, b in agg.items():
    out[cyc] = {
        'gross':  round(b['gross'], 2),
        'xfer':   round(b['xfer'], 2),
        'xfer_n': b['xfer_n'],
        'n':      b['n'],
        'oos':    round(b['oos'], 2),
        'by_parish':   {k: round(v, 2) for k, v in b['by_parish'].items()},
        'by_party':    {k: round(v, 2) for k, v in b['by_party'].items()},
        'by_industry': {k: round(v, 2) for k, v in b['by_industry'].items()},
    }

with gzip.open(OUT, 'wt', encoding='utf-8') as f:
    json.dump(out, f, separators=(',', ':'), ensure_ascii=False)
print(f'Aggregated {total:,} contributions into {len(out)} cycles in {time.time()-t0:.1f}s.')
print(f'Wrote {OUT}  ({os.path.getsize(OUT)/1024:.0f} KB)')
for cyc in sorted(out, reverse=True)[:3]:
    b = out[cyc]
    print(f'  {cyc}: gross ${b["gross"]:,.0f}  n={b["n"]:,}  parties={len(b["by_party"])}  industries={len(b["by_industry"])}')
