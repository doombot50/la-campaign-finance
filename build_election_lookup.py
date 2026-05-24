#!/usr/bin/env python3
"""Emit la_election_lookup.json — the flat, dashboard-facing artifact.

Derived from la_election_results.json. Produces a map with two key types so the
browser only needs a simple normalize() + dict lookups (no fuzzy logic client-side):
  - exact normalized full name        (T1)
  - unambiguous "FIRST LAST"          (T2, only when exactly one candidate has it)
Ambiguous entries are dropped entirely.
"""
import json, re
from collections import defaultdict

R = json.load(open('la_election_results.json', encoding='utf-8'))

firstlast = defaultdict(list)
for norm in R:
    t = norm.split()
    if len(t) >= 2:
        firstlast[(t[0], t[-1])].append(norm)

def slim(e):
    return {'outcome': e['outcome'], 'vote_pct': e['vote_pct'],
            'office': e['office'], 'election_date': e['election_date'], 'party': e['party']}

out = {}
# Pass 1: exact full-name keys (T1)
for norm, e in R.items():
    if e.get('ambiguous'):
        continue
    out[norm] = slim(e)
# Pass 2: unambiguous first+last keys (T2), never overwriting a T1 key
for norm, e in R.items():
    if e.get('ambiguous'):
        continue
    t = norm.split()
    key = f'{t[0]} {t[-1]}'
    if key != norm and len(firstlast[(t[0], t[-1])]) == 1 and key not in out:
        out[key] = slim(e)

json.dump(out, open('la_election_lookup.json', 'w', encoding='utf-8'), separators=(',', ':'))
print(f'Wrote la_election_lookup.json: {len(out):,} keys '
      f'({sum(1 for v in R.values() if not v.get("ambiguous")):,} non-ambiguous source names)')
