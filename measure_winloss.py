#!/usr/bin/env python3
"""Measure HIGH-CONFIDENCE win/loss badge coverage using the shared matcher."""
import json, gzip, os
from collections import Counter
import election_match as em

CACHE = '.la_cache'
em.load()

# filer party from the politician lookup (for the cross-check inside match())
lookup = json.load(open('la_politicians_lookup.json', encoding='utf-8'))
def filer_party(filer):
    e = lookup.get(em._normalize(filer)) or lookup.get(filer)
    return e.get('party') if e else None

# unique filers + activity weights
fc, fd = Counter(), Counter()
for fn in os.listdir(CACHE):
    if fn.startswith('contributions_yr'):
        with gzip.open(os.path.join(CACHE, fn), 'rt', encoding='utf-8') as f:
            for line in f:
                try:
                    r = json.loads(line); c = r.get('candidate')
                    if c and c != 'Unknown':
                        fc[c] += 1; fd[c] += r.get('amount', 0) or 0
                except: pass

all_filers = list(fc)
committees = [f for f in all_filers if em.looks_like_committee(f)]
individuals = [f for f in all_filers if not em.looks_like_committee(f)]

tiers = Counter(); rec = Counter(); dol = Counter()
samples = {'T1': [], 'T2': []}
for f in individuals:
    m = em.match(f, filer_party(f))
    if m:
        t = m['_tier']; tiers[t] += 1; rec[t] += fc[f]; dol[t] += fd[f]
        if len(samples[t]) < 8:
            samples[t].append((f, m['outcome'], m['vote_pct'], m['office'][:32], m['election_date']))

tot = len(individuals)
hc = tiers['T1'] + tiers['T2']
tot_rec = sum(fc.values()); ind_rec = sum(fc[f] for f in individuals)
tot_dol = sum(fd.values()); ind_dol = sum(fd[f] for f in individuals)
hc_rec = rec['T1'] + rec['T2']; hc_dol = dol['T1'] + dol['T2']

amb = sum(1 for v in em._RESULTS.values() if v.get('ambiguous'))
print('='*60)
print('TRUSTWORTHY WIN/LOSS COVERAGE (post multiplicity re-scrape)')
print('='*60)
print(f'SoS result names: {len(em._RESULTS):,}  (ambiguous flagged: {amb:,})')
print(f'Unique filers: {len(all_filers):,}  | committees: {len(committees):,}  | individuals: {tot:,}')
print()
print(f'High-confidence badge (of individuals): {hc:,} = {100*hc/tot:.1f}%')
print(f'   T1 exact full-name ......... {tiers["T1"]:,} ({100*tiers["T1"]/tot:.1f}%)')
print(f'   T2 unambiguous first+last .. {tiers["T2"]:,} ({100*tiers["T2"]/tot:.1f}%)')
print()
print('Weighted by activity:')
print(f'   individual-candidate records badged: {100*hc_rec/ind_rec:.1f}%')
print(f'   individual-candidate $ badged ....: {100*hc_dol/ind_dol:.1f}%')
print(f'   ALL records badged ...............: {100*hc_rec/tot_rec:.1f}%')
print(f'   ALL $ badged .....................: {100*hc_dol/tot_dol:.1f}%')
print()
for t in ('T1', 'T2'):
    print(f'Sample {t}:')
    for s in samples[t]:
        print(f'   {s[0][:28]:28} {s[1]:9} {s[2]}%  {s[3]}  {s[4]}')
