#!/usr/bin/env python3
"""
build_entities.py — canonical entity table for the LA campaign-finance dashboard
==================================================================================
One record per filer (committee or candidate committee), keyed by the LA Ethics
CSV filerNumber — the only identifier the raw records carry natively. Every
other dataset joins to the spine here, ONCE, at build time, so downstream
features read real joins instead of re-running fuzzy name matches.

Spine:    .la_cache/*_yr*.json.gz records  -> filerNumber ↔ display name
                                              (empirically 1:1 per filer)
Joins:    la_politicians_lookup.json       -> party + provenance (via
                                              lookup_party_detail, incl.
                                              committee-name unwrap)
          la_filer_lookup.json             -> alias spellings (norm name ->
                                              filer_number map covers history
                                              beyond the cached year window)
          ethics_coh_cache.json            -> ethics portal FilerID (CAN/PAC),
                                              certified COH + report year
          la_candidate_index.json.gz       -> career totals + cycle span
          la_candidacies_raw.json.gz       -> races run, latest office/outcome

Kind classification (best evidence wins):
  'candidate' — ethics FilerID starts CAN, or the name matches a SoS candidacy
  'committee' — FilerID starts PAC/OP, or committee-shaped name (PAC/committee/
                fund/party tokens)
  'unknown'   — neither signal

Output: .la_cache/la_entities.json.gz — ships to the data-cache release via the
nightly workflow's existing *.json.gz upload glob, and lands on Render via
fetch_cache_assets.py.

Run by .github/workflows/nightly-data.yml after retag_caches.py; safe to run
locally any time.
"""
import gzip
import json
import glob
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone

import la_ethics_server as srv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_PATH = os.path.join(srv.CACHE_DIR, 'la_entities.json.gz')

_COMMITTEE_TOKENS = re.compile(
    r'\b(PAC|COMMITTEE|FUND|PARTY|CAUCUS|ASSOCIATION|ASSN|LLC|INC|CORP|UNION|'
    r'LOCAL|LEAGUE|COALITION|ALLIANCE|FEDERATION|CITIZENS|FRIENDS)\b', re.I)


def _norm(s: str) -> str:
    return re.sub(r'\s+', ' ', (s or '').strip().upper())


def scan_spine():
    """filerNumber -> {name, first_year, last_year, n_records, raised, spent}"""
    spine = {}
    for path in sorted(glob.glob(os.path.join(srv.CACHE_DIR, '*_yr*.json.gz'))):
        fname = os.path.basename(path)
        m = re.match(r'(contributions|expenditures|loans)_yr(\d{4})', fname)
        if not m:
            continue
        rtype, year = m.group(1), int(m.group(2))
        with gzip.open(path, 'rt', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                fn = (rec.get('filerNumber') or '').strip()
                if not fn:
                    continue
                e = spine.get(fn)
                if e is None:
                    e = spine[fn] = {'names': Counter(), 'first': year, 'last': year,
                                     'n': 0, 'raised': 0.0, 'spent': 0.0}
                e['names'][(rec.get('candidate') or '').strip()] += 1
                e['first'] = min(e['first'], year)
                e['last']  = max(e['last'], year)
                e['n']    += 1
                amt = float(rec.get('amount') or 0)
                if rtype == 'contributions':
                    e['raised'] += amt
                elif rtype == 'expenditures':
                    e['spent'] += amt
    return spine


def main():
    print('Loading lookups...')
    srv._load_politician_lookup()
    srv._load_ethics_coh()
    srv._load_filer_lookup()
    srv._load_career_data()

    coh_cache    = srv._ETHICS_COH or {}
    filer_map    = srv._FILER_NUM or {}     # norm name -> filer_number
    cand_index   = srv._CAND_INDEX or {}    # norm name -> career (may merge collisions)
    filer_career = srv._FILER_CAREER or {}  # filer_number -> career (exact identity)
    cand_races   = srv._CAND_RACES or {}

    # Invert the filer lookup: filer_number -> [norm names] (alias source)
    fn_aliases = defaultdict(list)
    for nm, fn in filer_map.items():
        fn_aliases[str(fn)].append(nm)

    # Pre-normalize join keys for COH / career / races
    coh_by_norm   = {_norm(k): (k, v) for k, v in coh_cache.items()}
    index_by_norm = {_norm(k): k for k in cand_index}
    races_by_norm = {_norm(k): k for k in cand_races}

    def _join_key(names):
        """First normalized variant that hits the given map; None otherwise."""
        def find(m):
            for n in names:
                if _norm(n) in m:
                    return _norm(n)
            return None
        return find

    print('Scanning record spine...')
    spine = scan_spine()
    print(f'  {len(spine)} distinct filer numbers')
    if not spine:
        # Publishing an empty entity table would silently strip every party
        # label, transfer flag (retag_caches reads this file) and war chest.
        # Fail with the actual cause instead of a ZeroDivisionError in the
        # summary print below.
        sys.exit('ABORT: no records found in .la_cache/*_yr*.json.gz — the record '
                 'cache is missing or empty; not overwriting la_entities.json.gz.')

    entities = {}
    n_party = n_coh = n_career = n_races = 0

    for fn, s in spine.items():
        display = s['names'].most_common(1)[0][0]
        names = [display] + [n for n, _ in s['names'].most_common()[1:]]
        # Alias spellings from the historical filer lookup
        for alias in fn_aliases.get(fn, []):
            if _norm(alias) not in {_norm(x) for x in names}:
                names.append(alias)

        find = _join_key(names)

        # Party + provenance
        pd = srv.lookup_party_detail(display)
        if pd['party'] == 'OTH' and pd['source'] is None:
            for alt in names[1:]:
                pd = srv.lookup_party_detail(alt)
                if pd['party'] != 'OTH':
                    break

        # Certified COH join
        coh_key = find(coh_by_norm)
        ethics_filer_id = coh = None
        if coh_key:
            orig_key, centry = coh_by_norm[coh_key]
            ethics_filer_id = centry.get('filer_id')
            if centry.get('ending_coh') is not None:
                coh = {'ending': centry['ending_coh'],
                       'year': centry.get('report_year'),
                       'pdf_url': centry.get('pdf_url')}

        # Career join — prefer the filer-keyed index (exact identity, so two
        # filers sharing a normalized name keep their own totals). Fall back to
        # the name-keyed index for filers not present there yet.
        ci = filer_career.get(fn)
        if ci is None:
            idx_key = find(index_by_norm)
            ci = cand_index[index_by_norm[idx_key]] if idx_key else None
        career = None
        if ci is not None:
            career = {'total_raised': ci.get('total_raised'),
                      'total_spent':  ci.get('total_spent'),
                      'total_transfers_in': ci.get('total_transfers_in', 0),
                      'first_cycle':  ci.get('first_cycle'),
                      'last_cycle':   ci.get('last_cycle'),
                      'n_cycles':     ci.get('n_cycles')}

        races_key = find(races_by_norm)
        races = None
        if races_key:
            rr = [r for r in cand_races[races_by_norm[races_key]]
                  if not r.get('party_office')]
            if rr:
                def _date_key(r):
                    # M/D/YYYY — parse numerically; lexicographic compare is wrong
                    p = (r.get('date') or '').split('/')
                    try:
                        return (int(p[2]), int(p[0]), int(p[1]))
                    except (IndexError, ValueError):
                        return (0, 0, 0)
                latest = max(rr, key=_date_key)
                races = {'count': len(rr),
                         'latest_office': latest.get('office'),
                         'latest_date': latest.get('date'),
                         'latest_outcome': latest.get('outcome')}

        # Kind classification — strongest evidence first
        if ethics_filer_id and ethics_filer_id.upper().startswith('CAN'):
            kind = 'candidate'
        elif races is not None:
            kind = 'candidate'
        elif ethics_filer_id and ethics_filer_id.upper().startswith(('PAC', 'OP')):
            kind = 'committee'
        elif _COMMITTEE_TOKENS.search(display):
            kind = 'committee'
        else:
            kind = 'candidate' if career else 'unknown'

        entities[fn] = {
            'filer_number':    fn,
            'name':            display,
            'aliases':         names[1:],
            'kind':            kind,
            'party':           pd['party'],
            'party_source':    pd['source'],
            'party_matched':   pd['matched'],
            'ethics_filer_id': ethics_filer_id,
            'coh':             coh,
            'career':          career,
            'races':           races,
            'window':          {'first_year': s['first'], 'last_year': s['last'],
                                'n_records': s['n'],
                                'raised': round(s['raised'], 2),
                                'spent':  round(s['spent'], 2)},
        }
        n_party  += pd['source'] is not None
        n_coh    += coh is not None
        n_career += career is not None
        n_races  += races is not None

    out = {
        'built_at': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        'count':    len(entities),
        'entities': entities,
    }
    tmp = OUT_PATH + '.tmp'
    with gzip.open(tmp, 'wt', encoding='utf-8') as f:
        json.dump(out, f, separators=(',', ':'))
    os.replace(tmp, OUT_PATH)

    kinds = Counter(e['kind'] for e in entities.values())
    print(f'\nWrote {OUT_PATH}')
    print(f'  entities:     {len(entities)}')
    print(f'  kinds:        {dict(kinds)}')
    print(f'  party source: {n_party} matched ({n_party/len(entities):.0%})')
    print(f'  certified COH:{n_coh}')
    print(f'  career join:  {n_career} ({n_career/len(entities):.0%})')
    print(f'  races join:   {n_races} ({n_races/len(entities):.0%})')


if __name__ == '__main__':
    main()
