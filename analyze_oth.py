#!/usr/bin/env python3
"""
Analyze OTH-classified filers in the cached Ethics contribution data.

Loads whatever per-year cache files exist in .la_cache/ and prints
the top OTH filers by total dollar volume, with a best-guess category
for each (Looks like a Person / PAC / Party Org / Other Entity).

Usage:
    python analyze_oth.py
    python analyze_oth.py --year 2019
    python analyze_oth.py --top 100
"""

import gzip, json, os, re, argparse

BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(BASE_DIR, '.la_cache')

def _looks_like(name: str) -> str:
    """Best-guess category for an OTH filer name."""
    n = name.upper()
    if any(x in n for x in ['PAC', 'POLITICAL ACTION', 'POLITICAL COMMITTEE']):
        return 'PAC'
    if any(x in n for x in [
        'REPUBLICAN PARTY', 'DEMOCRATIC PARTY', 'REPUBLICAN CAUCUS',
        'DEMOCRATIC CAUCUS', 'GOP', 'DEMOPAC', 'NRCC', 'DCCC', 'NRSC',
        'DSCC', 'LAGOP', 'LA GOP',
    ]):
        return 'Party Org'
    if any(x in n for x in [
        'UNION', 'AFL-CIO', 'SEIU', 'IBEW', 'TEAMSTER', 'UFCW',
        'LOCAL ', 'BROTHERHOOD', 'WORKERS',
    ]):
        return 'Union'
    if any(x in n for x in [
        'LLC', 'INC', 'CORP', 'LLP', 'LTD', 'CO.', ' CO ',
        'COMPANY', 'ENTERPRISES', 'HOLDINGS', 'GROUP', 'FUND',
        'ASSOCIATION', 'ASSOC', 'FOUNDATION', 'TRUST',
        'COMMITTEE', 'COUNCIL', 'CLUB', 'SOCIETY', 'LEAGUE',
    ]):
        return 'Org / Entity'
    # Heuristic: 1-3 all-caps tokens, each 2+ chars → probably a person
    tokens = [t for t in n.split() if re.match(r'^[A-Z]{2,}$', t)]
    if 1 <= len(tokens) <= 4:
        return 'Looks like a Person'
    return 'Unknown'

def load_cache(years=None):
    if not os.path.isdir(CACHE_DIR):
        print(f'Cache directory not found: {CACHE_DIR}')
        return []
    records = []
    files = sorted(f for f in os.listdir(CACHE_DIR) if f.startswith('contributions_yr'))
    if not files:
        print('No contribution cache files found. Run the server and load a year first.')
        return []
    for fname in files:
        yr = int(fname.replace('contributions_yr', '').replace('.json.gz', ''))
        if years and yr not in years:
            continue
        path = os.path.join(CACHE_DIR, fname)
        with gzip.open(path, 'rt', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except Exception:
                        pass
        print(f'  Loaded {fname}')
    return records

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--year',  type=int, nargs='+', help='Filter to specific year(s)')
    ap.add_argument('--top',   type=int, default=60, help='How many filers to show (default 60)')
    ap.add_argument('--person-only', action='store_true',
                    help='Show only filers that look like a person')
    args = ap.parse_args()

    print(f'Loading cache from {CACHE_DIR} …')
    records = load_cache(years=set(args.year) if args.year else None)
    if not records:
        return

    print(f'  {len(records):,} total records loaded\n')

    # Aggregate OTH filers
    oth = [r for r in records if r.get('party') == 'OTH']
    print(f'OTH records: {len(oth):,}  ({100*len(oth)/len(records):.1f}% of total)\n')

    totals = {}   # candidate -> {amount, count, category}
    for r in oth:
        name = r.get('candidate', 'Unknown')
        if name not in totals:
            totals[name] = {'amount': 0.0, 'count': 0, 'category': _looks_like(name)}
        totals[name]['amount'] += r.get('amount', 0)
        totals[name]['count']  += 1

    ranked = sorted(totals.items(), key=lambda x: -x[1]['amount'])

    if args.person_only:
        ranked = [(n, v) for n, v in ranked if v['category'] == 'Looks like a Person']

    # Summary by category
    cat_totals = {}
    for name, v in totals.items():
        c = v['category']
        cat_totals[c] = cat_totals.get(c, {'amount': 0, 'filers': 0})
        cat_totals[c]['amount']  += v['amount']
        cat_totals[c]['filers']  += 1

    print('─' * 70)
    print('OTH BREAKDOWN BY CATEGORY')
    print('─' * 70)
    print(f'{"Category":<25}  {"Filers":>7}  {"Total $":>14}')
    print('─' * 70)
    for cat, v in sorted(cat_totals.items(), key=lambda x: -x[1]['amount']):
        print(f'{cat:<25}  {v["filers"]:>7,}  ${v["amount"]:>13,.0f}')
    print()

    print('─' * 70)
    print(f'TOP {args.top} OTH FILERS BY TOTAL CONTRIBUTIONS RECEIVED')
    print('─' * 70)
    print(f'{"Candidate":<40}  {"Category":<22}  {"Total $":>12}  {"#":>5}')
    print('─' * 70)
    for name, v in ranked[:args.top]:
        print(f'{name[:40]:<40}  {v["category"]:<22}  ${v["amount"]:>11,.0f}  {v["count"]:>5,}')

    # Specifically highlight "Looks like a Person" — these are probably missed candidates
    persons = [(n, v) for n, v in ranked if v['category'] == 'Looks like a Person']
    print(f'\n→ {len(persons)} OTH filers that look like unidentified candidates')
    print(f'  Top 20 by amount:')
    for name, v in persons[:20]:
        print(f'    ${v["amount"]:>12,.0f}   {name}')

if __name__ == '__main__':
    import io, sys
    # Capture all output and write to both console and a report file
    buf = io.StringIO()
    class Tee:
        def write(self, s):
            buf.write(s)
            sys.__stdout__.write(s)
        def flush(self):
            sys.__stdout__.flush()
    sys.stdout = Tee()
    main()
    sys.stdout = sys.__stdout__
    out_path = os.path.join(BASE_DIR, 'oth_analysis.txt')
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(buf.getvalue())
    print(f'\nReport saved to {out_path}')
