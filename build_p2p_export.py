#!/usr/bin/env python3
"""
build_p2p_export.py — emit a flat donor file for the la-pay-to-play repo
========================================================================
The pay-to-play tool (separate repo) consumes campaign-finance donations as a
flat file dropped into its data/external/. This script is the *only* coupling
point: it reads the cached LA Ethics contributions NDJSON and writes a CSV in
exactly the schema paytoplay.ingest.cf_import expects.

Stdlib only (csv/gzip/json) — keeps this repo's zero-dependency rule. The
pay-to-play side (pandas/pyarrow) converts the CSV to parquet on import.

Output columns (paytoplay DONATIONS schema + recipient_filer):

    id                stable hash of the donation row
    donor_name        contributor, light relationship-prefix cleanup
    employer          ""  — NOT in the LA Ethics contributions export
    donor_address     "<city>, <state> <zip>" — no street line upstream
    amount            float
    date              ISO date
    recipient_name    the candidate/committee that received it
    recipient_filer   Ethics FilerNumber — collision-free recipient identity
    recipient_office  backfilled by filer/name from SoS lookups (often blank
                      on the raw contribution)
    source_url        derived: https://eap.ethics.la.gov/CFSearch/<rpt>.pdf

Reality notes (measured against the cache, see build commentary):
  - employer/occupation, street address, and officeDescription are ~0% filled
    in the upstream contributions bundle, so the employer-based person lane and
    the street-address lane in the matcher cannot fire from this data. Office is
    recovered here from the SoS-derived lookups keyed by filer number.

Usage:
  python3 build_p2p_export.py                 # all cached years -> ./cf_donations.csv
  python3 build_p2p_export.py --since 2016     # only 2016+
  python3 build_p2p_export.py --out "/path/to/la-pay-to-play/data/external/cf_donations.csv"
"""
import argparse
import csv
import glob
import gzip
import hashlib
import json
import os
import re
import sys

import la_ethics_server as srv   # reuse _norm_name; import is side-effect free

BASE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(BASE, '.la_cache')

OUT_COLUMNS = [
    'id', 'donor_name', 'employer', 'donor_address', 'amount', 'date',
    'recipient_name', 'recipient_filer', 'recipient_office', 'source_url',
]

# Relationship/role prefixes that are noise on a donor name (the surname that
# follows is the real entity). Honorifics (MR/DR/...) are left for the
# pay-to-play normalizer, which already strips them.
_REL_PREFIX = re.compile(r'^\s*(SPOUSE OF|SPOUSE|MINOR CHILD OF|MINOR CHILD|ESTATE OF)\b[\s,]*',
                         re.IGNORECASE)

FILING_URL = 'https://eap.ethics.la.gov/CFSearch/{}.pdf'


def _load_office_by_filer():
    """filerNumber -> office, resolved through the SoS-derived lookups.

    la_filer_lookup.json is name->filer; invert it and join each name to an
    office via la_politicians_lookup.json (preferred) or la_election_results.json.
    Both lookups are keyed by the same _norm_name space as filer_lookup.
    """
    def _maybe(path):
        p = os.path.join(BASE, path)
        if not os.path.exists(p):
            print(f'  note: {path} absent — office backfill degraded', file=sys.stderr)
            return {}
        with open(p, encoding='utf-8') as fh:
            return json.load(fh)

    filer_lookup = _maybe('la_filer_lookup.json')          # NAME -> filer
    politicians = _maybe('la_politicians_lookup.json')      # NAME -> {office,...}
    elections = _maybe('la_election_results.json')          # NAME -> {office,...}

    def office_for_name(name):
        rec = politicians.get(name) or elections.get(name)
        if isinstance(rec, dict):
            return (rec.get('office') or '').strip()
        return ''

    by_filer = {}
    for name, filer in filer_lookup.items():
        if filer in by_filer:
            continue
        office = office_for_name(name)
        if office:
            by_filer[filer] = office
    return by_filer, politicians, elections


def _clean_donor(name):
    name = (name or '').strip()
    prev = None
    while name != prev:                 # strip stacked prefixes ("SPOUSE OF ...")
        prev = name
        name = _REL_PREFIX.sub('', name).strip()
    return name or (prev or '').strip()


def _address(rec):
    city = (rec.get('city') or '').strip()
    state = (rec.get('contributorState') or '').strip()
    zip_ = (rec.get('contributorZip') or '').strip()
    left = city
    right = ' '.join(p for p in (state, zip_) if p)
    return ', '.join(p for p in (left, right) if p)


def _row_id(rec):
    raw = '|'.join(str(rec.get(k, '')) for k in
                   ('reportNumber', 'filerNumber', 'contributor', 'date', 'amount'))
    return 'D' + hashlib.md5(raw.encode('utf-8')).hexdigest()[:16]


def _year_files(since):
    files = []
    for p in sorted(glob.glob(os.path.join(CACHE, 'contributions_yr*.json.gz'))):
        m = re.search(r'yr(\d{4})', os.path.basename(p))
        if m and (since is None or int(m.group(1)) >= since):
            files.append(p)
    return files


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--since', type=int, default=None,
                    help='only export contribution years >= this (default: all)')
    ap.add_argument('--out', default=os.path.join(BASE, 'cf_donations.csv'),
                    help='output CSV path (default: ./cf_donations.csv)')
    args = ap.parse_args()

    files = _year_files(args.since)
    if not files:
        print('No cached contributions_yr*.json.gz found in .la_cache/.', file=sys.stderr)
        sys.exit(1)

    office_by_filer, politicians, elections = _load_office_by_filer()
    print(f'office map: {len(office_by_filer):,} filers resolved')

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    n = 0
    office_hits = 0
    office_buckets = {}
    with open(args.out, 'w', newline='', encoding='utf-8') as out:
        w = csv.DictWriter(out, fieldnames=OUT_COLUMNS)
        w.writeheader()
        for path in files:
            with gzip.open(path, 'rt', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    rec = json.loads(line)
                    filer = (rec.get('filerNumber') or '').strip()
                    cand = (rec.get('candidate') or '').strip()
                    office = (rec.get('officeDescription') or '').strip()
                    if not office and filer:
                        office = office_by_filer.get(filer, '')
                    if not office and cand:
                        nm = srv._norm_name(cand)
                        rc = politicians.get(nm) or elections.get(nm)
                        if isinstance(rc, dict):
                            office = (rc.get('office') or '').strip()
                    if office:
                        office_hits += 1
                        office_buckets[office] = office_buckets.get(office, 0) + 1
                    rpt = (rec.get('reportNumber') or '').strip()
                    try:
                        amt = float(rec.get('amount') or 0)
                    except (TypeError, ValueError):
                        amt = 0.0
                    w.writerow({
                        'id': _row_id(rec),
                        'donor_name': _clean_donor(rec.get('contributor')),
                        'employer': '',
                        'donor_address': _address(rec),
                        'amount': amt,
                        'date': (rec.get('date') or '').strip(),
                        'recipient_name': cand,
                        'recipient_filer': filer,
                        'recipient_office': office,
                        'source_url': FILING_URL.format(rpt) if rpt else '',
                    })
                    n += 1
            print(f'  {os.path.basename(path)}: cumulative {n:,} rows')

    pct = (100 * office_hits / n) if n else 0
    print(f'\nwrote {n:,} donation rows -> {args.out}')
    print(f'office resolved on {office_hits:,} rows ({pct:.1f}%)')
    top = sorted(office_buckets.items(), key=lambda kv: -kv[1])[:12]
    if top:
        print('top recipient_office values (curate config/agency_officials.yml against these):')
        for office, c in top:
            print(f'  {c:8,}  {office}')


if __name__ == '__main__':
    main()
