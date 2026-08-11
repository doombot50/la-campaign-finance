#!/usr/bin/env python3
"""Build la_filer_lookup.json — maps normalized candidate name -> Ethics filer number.

Used by the Races view to link SoS election candidates to their Ethics
contribution records so /api/industry-breakdown can be called per candidate.

Usage:
    py -3 build_filer_lookup.py
"""
import json, gzip, os, re
from collections import defaultdict

CACHE = '.la_cache'
OUT   = 'la_filer_lookup.json'

def normalize(name):
    name = name.upper()
    name = re.sub(r'\b(DR|MR|MRS|MS|JR|SR|II|III|IV|ESQ|PHD|MD)\.?\b', '', name)
    name = re.sub(r'[^A-Z\s]', ' ', name)
    return ' '.join(name.split())

# norm_name -> {filer_number -> count}  (pick most-common filer per name)
lookup = defaultdict(lambda: defaultdict(int))

# The .json.gz suffix is required: a leftover `*.json.gz.tmp` from an interrupted
# download/retag also starts with the prefix, sorts newest under reverse=True, and
# would be read as a (possibly truncated) year file.
files = sorted([f for f in os.listdir(CACHE)
                if f.startswith('contributions_yr') and f.endswith('.json.gz')],
               reverse=True)
print(f'Scanning {len(files)} year files...')

total = 0
for fname in files:
    fp = os.path.join(CACHE, fname)
    n = 0
    with gzip.open(fp, 'rt', encoding='utf-8') as f:
        for line in f:
            try:
                r = json.loads(line)
                cand = (r.get('candidate') or '').strip()
                fnum = str(r.get('filerNumber') or '').strip()
                if cand and fnum and cand != 'Unknown':
                    norm = normalize(cand)
                    if len(norm.split()) >= 2:
                        lookup[norm][fnum] += 1
                        n += 1
            except Exception:
                pass
    total += n
    print(f'  {fname}: {n:,} records')

# Collapse to single best filer number per normalized name
out = {norm: max(fnums, key=fnums.get) for norm, fnums in lookup.items()}

with open(OUT, 'w', encoding='utf-8') as f:
    json.dump(out, f)

print(f'\nWrote {OUT}: {len(out):,} candidates from {total:,} records')
