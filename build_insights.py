#!/usr/bin/env python3
"""
build_insights.py — nightly precomputed insight blocks
========================================================
One audited computation site for cross-dataset insights, instead of ad-hoc
re-derivation per request or per browser. Runs nightly after
build_entities.py; output ships to the data-cache release with the other
.la_cache assets and is served by /api/insights.

Blocks (each self-describing, with method notes for UI tooltips):
  war_chests   — top certified COH from the entity table, recent filings only
                 (coh.year within 3 years; older balances are stale history)
  top_donors   — top contributors by total over the cached window, grouped by
                 reported name (no entity resolution for donors yet), EXCLUDING
                 committee-to-committee transfers and clerk filing fees
  party_totals — gross contribution totals by record party tag
  monthly_flow — contributions per calendar month (powers overview sparkline)
  transfers    — inter-committee transfer rollup, total and per-year

Stdlib only. Single streaming pass over the contribution caches.
"""
import gzip
import json
import glob
import os
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone

import la_ethics_server as srv

OUT_PATH = os.path.join(srv.CACHE_DIR, 'la_insights.json.gz')
ENT_PATH = os.path.join(srv.CACHE_DIR, 'la_entities.json.gz')


def main():
    now_year = datetime.now().year

    # ── War chests from the entity table ─────────────────────────────────────
    war_chests = []
    if os.path.exists(ENT_PATH):
        with gzip.open(ENT_PATH, 'rt', encoding='utf-8') as f:
            ents = json.load(f).get('entities', {})
        for e in ents.values():
            coh = e.get('coh')
            if not coh or coh.get('ending') is None:
                continue
            if not isinstance(coh.get('year'), int) or coh['year'] < now_year - 3:
                continue
            war_chests.append({
                'name':         e['name'],
                'filer_number': e['filer_number'],
                'kind':         e['kind'],
                'party':        e['party'],
                'party_source': e['party_source'],
                'ending':       coh['ending'],
                'year':         coh['year'],
                'pdf_url':      coh.get('pdf_url'),
            })
        war_chests.sort(key=lambda w: -w['ending'])
        war_chests = war_chests[:25]

    # ── Streaming pass over contribution caches ──────────────────────────────
    donor_totals = defaultdict(float)
    donor_counts = Counter()
    party_totals = defaultdict(float)
    party_counts = Counter()
    monthly      = defaultdict(float)
    tr_count     = Counter()   # year -> n transfers
    tr_amount    = defaultdict(float)
    years_seen   = set()

    for path in sorted(glob.glob(os.path.join(srv.CACHE_DIR, 'contributions_yr*.json.gz'))):
        yr = int(re.search(r'_yr(\d{4})', path).group(1))
        years_seen.add(yr)
        with gzip.open(path, 'rt', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                amt = float(rec.get('amount') or 0)
                if amt <= 0:
                    continue
                party_totals[rec.get('party') or 'OTH'] += amt
                party_counts[rec.get('party') or 'OTH'] += 1
                month = (rec.get('date') or '')[:7]
                if len(month) == 7:
                    monthly[month] += amt
                if rec.get('isTransfer'):
                    tr_count[yr]  += 1
                    tr_amount[yr] += amt
                    continue   # transfers are not "donors"
                if rec.get('isFilingFee'):
                    continue   # clerk-forwarded qualifying fees are not donors
                donor = re.sub(r'\s+', ' ', (rec.get('contributor') or '').strip().upper())
                if donor and donor != 'UNKNOWN':
                    donor_totals[donor] += amt
                    donor_counts[donor] += 1

    # Top donors: prefer the resolved donor table (spelling/nickname variants
    # merged within ZIP) so one person isn't split across spellings. Fall back to
    # the raw name grouping if the resolver hasn't run yet.
    donor_method = ('Identity-resolved: spelling, middle-initial, and nickname '
                    'variants merged within the same surname, generational suffix, '
                    'and ZIP (organizations merged by name). Excludes committee '
                    'transfers and clerk filing fees. Each entry lists its merged '
                    'variants.')
    de_path = os.path.join(srv.CACHE_DIR, 'la_donor_entities.json.gz')
    if os.path.exists(de_path):
        with gzip.open(de_path, 'rt', encoding='utf-8') as f:
            de = json.load(f).get('donors', {})
        ranked = sorted(de.values(), key=lambda d: -d['total'])[:100]
        top_donors = [{'name': d['name'], 'total': d['total'], 'count': d['count'],
                       'kind': d['kind'], 'n_variants': d['n_variants'],
                       'variants': d.get('variants', [])} for d in ranked]
    else:
        donor_method = ('Grouped by contributor name as reported; spelling variants '
                        'NOT merged (resolver not yet run). Excludes committee '
                        'transfers and clerk filing fees.')
        top_donors = [{'name': n, 'total': round(t, 2), 'count': donor_counts[n],
                       'n_variants': 1, 'variants': []}
                      for n, t in sorted(donor_totals.items(), key=lambda x: -x[1])[:100]]

    window = {'first_year': min(years_seen), 'last_year': max(years_seen)} if years_seen else {}

    out = {
        'built_at': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        'window':   window,
        'war_chests': {
            'method': 'Certified ending cash on hand per LA Ethics annual reports '
                      f'(filings {now_year - 3} or later). Click an amount for the source PDF.',
            'items':  war_chests,
        },
        'top_donors': {
            'method': donor_method,
            'items':  top_donors,
        },
        'party_totals': {
            'method': 'Gross contributions by recipient party tag (heuristic name match '
                      'for some filers).',
            'items':  {p: {'total': round(t, 2), 'count': party_counts[p]}
                       for p, t in party_totals.items()},
        },
        'monthly_flow': {
            'method': 'Gross contributions per calendar month.',
            'items':  {m: round(t, 2) for m, t in sorted(monthly.items())},
        },
        'transfers': {
            'method': 'Contributions whose contributor matches a known PAC/committee entity. '
                      'Person-named candidate committees cannot be flagged.',
            'total':  {'count': sum(tr_count.values()),
                       'amount': round(sum(tr_amount.values()), 2)},
            'by_year': {str(y): {'count': tr_count[y], 'amount': round(tr_amount[y], 2)}
                        for y in sorted(tr_count)},
        },
    }

    tmp = OUT_PATH + '.tmp'
    with gzip.open(tmp, 'wt', encoding='utf-8') as f:
        json.dump(out, f, separators=(',', ':'))
    os.replace(tmp, OUT_PATH)

    print(f'Wrote {OUT_PATH}')
    print(f'  war chests:  {len(war_chests)} (top: {war_chests[0]["name"]} '
          f'${war_chests[0]["ending"]:,.0f})' if war_chests else '  war chests:  none')
    print(f'  top donors:  {len(top_donors)} (top: {top_donors[0]["name"]} '
          f'${top_donors[0]["total"]:,.0f})' if top_donors else '  top donors:  none')
    print(f'  transfers:   {sum(tr_count.values()):,} records, '
          f'${sum(tr_amount.values()):,.0f}')
    print(f'  months:      {len(monthly)}   parties: {dict(party_counts)}')


if __name__ == '__main__':
    main()
