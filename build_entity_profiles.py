#!/usr/bin/env python3
"""Precompute per-entity LIFETIME edge lists — the piece the dashboard still
computes client-side from loaded cycle rows.

Two artifacts, both keyed for row-free lookup:

  la_entity_donors.json.gz   filerNumber -> receiving side
     {name, party, n_donors, total_raised, first, last,
      top_donors: [{name, amount, n}, ...]}        # who gave TO this committee

  la_entity_giving.json.gz   normName(donor) -> giving side
     {name, n_recipients, total_given,
      top_recipients: [{name, filer, amount, n}, ...]}  # who this donor gave TO

Receiving is keyed by the exact Ethics filerNumber (no name-collision merge).
Giving is keyed by normalized donor name — donors carry no filer number on the
row, so name is the only identity (same constraint the pay-to-play tool lives
with). get_cycle/normalize/month_key are copied verbatim from
build_candidate_index.py so every join key matches the existing indexes.

Run after refresh_la_cache.py. Validate a single entity:
    py build_entity_profiles.py --name "Gumbo PAC"
"""
import os, re, json, gzip, time, heapq, sys
from collections import defaultdict, Counter

BASE  = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(BASE, '.la_cache')
OUT_RECV = os.path.join(CACHE, 'la_entity_donors.json.gz')
OUT_GIVE = os.path.join(CACHE, 'la_entity_giving.json.gz')
TOP_N    = 50   # edges kept per entity; covers every realistic profile view

def normalize(name):
    name = (name or '').upper()
    name = re.sub(r'\b(DR|MR|MRS|MS|JR|SR|II|III|IV|ESQ|PHD|MD)\.?\b', '', name)
    name = re.sub(r'[^A-Z\s]', ' ', name)
    return ' '.join(name.split())

# ── aggregation state ────────────────────────────────────────────────────────
# recv[fn][donor_norm] and give[donor_norm][fn] share the SAME [amount, n] leaf
# list, so each contribution edge costs one list, not two.
recv = defaultdict(dict)   # filerNumber  -> {donor_norm: [amount, n]}
give = defaultdict(dict)   # donor_norm   -> {filerNumber: [amount, n]}

recv_meta = defaultdict(lambda: {'raised': 0.0, 'n': 0, 'first': '', 'last': '',
                                 'names': Counter(), 'party': Counter()})
give_meta = defaultdict(lambda: {'given': 0.0, 'n': 0, 'names': Counter()})
fn_name   = defaultdict(Counter)   # filerNumber -> {candidate spelling: count}

def ingest(path):
    n = 0
    with gzip.open(path, 'rt', encoding='utf-8') as f:
        for line in f:
            try:
                r = json.loads(line)
                fn = (r.get('filerNumber') or '').strip()
                donor_raw = (r.get('contributor') or '').strip()
                if not fn or not donor_raw:
                    continue
                donor = normalize(donor_raw)
                if not donor:
                    continue
                amt  = float(r.get('amount') or 0)
                date = (r.get('date') or '')
                cand = (r.get('candidate') or '').strip()
                party = (r.get('party') or '').strip()

                # shared leaf list — one allocation per unique (filer, donor) edge
                lst = recv[fn].get(donor)
                if lst is None:
                    lst = [0.0, 0]
                    recv[fn][donor] = lst
                    give[donor][fn] = lst
                lst[0] += amt
                lst[1] += 1

                rm = recv_meta[fn]
                rm['raised'] += amt; rm['n'] += 1
                if cand:  rm['names'][cand] += 1
                if party: rm['party'][party] += 1
                if date:
                    if not rm['first'] or date < rm['first']: rm['first'] = date
                    if not rm['last']  or date > rm['last']:  rm['last']  = date

                gm = give_meta[donor]
                gm['given'] += amt; gm['n'] += 1
                gm['names'][donor_raw] += 1
                if cand: fn_name[fn][cand] += 1
                n += 1
            except Exception:
                pass
    return n

t0 = time.time()
total = 0
for fn in sorted(os.listdir(CACHE)):
    if fn.startswith('contributions_yr') and fn.endswith('.json.gz'):
        c = ingest(os.path.join(CACHE, fn))
        total += c
        print(f'  {fn}: {c:,} records')
print(f'Ingested {total:,} contribution rows in {time.time()-t0:.1f}s. Building edge lists...')

def _disp(counter):
    return counter.most_common(1)[0][0] if counter else ''

# ── receiving side: top donors per filer ─────────────────────────────────────
recv_out = {}
for fn, donors in recv.items():
    rm = recv_meta[fn]
    top = heapq.nlargest(TOP_N, donors.items(), key=lambda kv: kv[1][0])
    recv_out[fn] = {
        'name':         _disp(fn_name[fn]) or _disp(rm['names']),
        'party':        _disp(rm['party']),
        'n_donors':     len(donors),
        'total_raised': round(rm['raised'], 2),
        'first':        rm['first'],
        'last':         rm['last'],
        'top_donors':   [{'name': _disp(give_meta[d]['names']) or d,
                          'amount': round(v[0], 2), 'n': v[1]} for d, v in top],
    }

# ── giving side: top recipients per donor ────────────────────────────────────
give_out = {}
for donor, recips in give.items():
    gm = give_meta[donor]
    top = heapq.nlargest(TOP_N, recips.items(), key=lambda kv: kv[1][0])
    give_out[donor] = {
        'name':          _disp(gm['names']),
        'n_recipients':  len(recips),
        'total_given':   round(gm['given'], 2),
        'top_recipients': [{'name': _disp(fn_name[f]), 'filer': f,
                            'amount': round(v[0], 2), 'n': v[1]} for f, v in top],
    }

def _write(path, obj):
    with gzip.open(path, 'wt', encoding='utf-8') as f:
        json.dump(obj, f, separators=(',', ':'), ensure_ascii=False)
    return os.path.getsize(path)

# ── optional single-entity validation ───────────────────────────────────────
if '--name' in sys.argv:
    q = normalize(sys.argv[sys.argv.index('--name') + 1])
    print(f'\n=== GIVING (as donor "{q}") ===')
    g = give_out.get(q)
    if g:
        print(f"  {g['name']}: gave ${g['total_given']:,.0f} across "
              f"{g['n_recipients']} recipients in {g['n']} gifts" if 'n' in g
              else f"  {g['name']}: gave ${g['total_given']:,.0f} to {g['n_recipients']} recipients")
        for r in g['top_recipients'][:8]:
            print(f"    -> {r['name'] or '(filer '+r['filer']+')':<35} ${r['amount']:>12,.0f}  ({r['n']} gifts)")
    else:
        print('  (no giving records under that normalized name)')
    print(f'\n=== RECEIVING (committees named "{q}") ===')
    hits = [(fn, v) for fn, v in recv_out.items() if normalize(v['name']) == q]
    for fn, v in sorted(hits, key=lambda x: -x[1]['total_raised'])[:3]:
        print(f"  {v['name']} [filer {fn}, {v['party']}]: raised ${v['total_raised']:,.0f} "
              f"from {v['n_donors']} donors ({v['first']}..{v['last']})")
        for d in v['top_donors'][:8]:
            print(f"    <- {d['name']:<35} ${d['amount']:>12,.0f}  ({d['n']} gifts)")
    if not hits:
        print('  (no committee whose display name normalizes to that)')

sz_r = _write(OUT_RECV, recv_out)
sz_g = _write(OUT_GIVE, give_out)
print(f'\nWrote {OUT_RECV}  ({len(recv_out):,} filers, {sz_r/1e6:.1f} MB)')
print(f'Wrote {OUT_GIVE}  ({len(give_out):,} donors, {sz_g/1e6:.1f} MB)')
print(f'Total {time.time()-t0:.1f}s')
