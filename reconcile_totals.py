#!/usr/bin/env python3
"""
reconcile_totals.py — cross-check our CSV-derived totals against the filers'
own certified report figures.
================================================================================
The strongest trust statement the dashboard can make: "our numbers match the
candidate's certified filing." Each contribution/expenditure row carries the
ReportNumber it was filed on (e.g. LA-132570), and that report's Summary Page
states the certified period totals:

    Line 1  Contributions (Schedule A-1)   <- compare to sum of our contribution
                                              rows with that reportNumber
    Line 9  Expenditures  (Schedule E-1)   <- compare to sum of our expenditure
                                              rows with that reportNumber

So the reconciliation is EXACT and per-report — no fuzzy period matching. This
script re-parses the already-cached report PDFs in .ethics_pdf_cache/ (no
network) and reports the match rate + largest discrepancies. A high match rate
is direct evidence our pipeline (dedup, transfer flagging, parsing) reproduces
the official record; discrepancies pinpoint exactly where to look.

Requires pdfplumber (same as fetch_ethics_coh.py). Stdlib otherwise.
Usage:
    py -3 reconcile_totals.py                 # full report
    py -3 reconcile_totals.py --limit 200     # cap PDFs parsed (faster)
    py -3 reconcile_totals.py --tol 0.01      # match tolerance (fraction)
"""
import argparse
import glob
import gzip
import json
import os
import re
import sys
from collections import defaultdict

try:
    import pdfplumber
except ImportError:
    print("ERROR: pdfplumber not installed. Run: py -3 -m pip install pdfplumber")
    sys.exit(1)

BASE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(BASE, '.la_cache')
PDF_CACHE = os.path.join(BASE, '.ethics_pdf_cache')

# Summary-page line items. Anchored on the line number so "1. Contributions"
# never matches "4. TOTAL CONTRIBUTIONS". Amount is on the same line.
_RE_LINE1 = re.compile(r'(?m)^\s*1\.\s+Contributions\b.*?\$\s*([\d,]+\.\d{2})')
_RE_LINE9 = re.compile(r'(?m)^\s*9\.\s+Expenditures\b.*?\$\s*([\d,]+\.\d{2})')


def _report_number_from_path(path):
    """'.../LA-132570.pdf' -> 'LA-132570'"""
    return os.path.splitext(os.path.basename(path))[0]


def certified_totals(wanted=None, limit=None):
    """reportNumber -> {'contrib': float|None, 'expend': float|None} from cached PDFs.
    `wanted`, if given, restricts parsing to those report numbers — typically the
    set we actually have records for, so we don't parse decades of old annuals
    with no CSV rows to compare."""
    out = {}
    pdfs = sorted(glob.glob(os.path.join(PDF_CACHE, 'LA-*.pdf')))
    if wanted is not None:
        pdfs = [p for p in pdfs if _report_number_from_path(p) in wanted]
    if limit:
        pdfs = pdfs[:limit]
    print(f'  parsing {len(pdfs):,} report PDF(s) with CSV rows to compare')
    for i, path in enumerate(pdfs, 1):
        rn = _report_number_from_path(path)
        try:
            with pdfplumber.open(path) as pdf:
                c = e = None
                for page in pdf.pages[:8]:
                    text = page.extract_text() or ''
                    up = text.upper()
                    if 'SUMMARY PAGE' not in up or 'SUMMARY PAGE (CONTINUED)' in up:
                        continue
                    m1, m9 = _RE_LINE1.search(text), _RE_LINE9.search(text)
                    if m1:
                        c = float(m1.group(1).replace(',', ''))
                    if m9:
                        e = float(m9.group(1).replace(',', ''))
                    if c is not None or e is not None:
                        break
                if c is not None or e is not None:
                    out[rn] = {'contrib': c, 'expend': e}
        except Exception as ex:
            print(f'  ERR {rn}: {ex}')
        if i % 200 == 0:
            print(f'  parsed {i}/{len(pdfs)} PDFs...')
    return out


def csv_sums_by_report(rtype):
    """reportNumber -> summed amount of our cached records of this type."""
    sums = defaultdict(float)
    for p in sorted(glob.glob(os.path.join(CACHE, f'{rtype}_yr*.json.gz'))):
        with gzip.open(p, 'rt', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except json.JSONDecodeError:
                    continue
                rn = r.get('reportNumber')
                if rn:
                    sums[rn] += float(r.get('amount') or 0)
    return sums


def _reconcile(label, certified_key, csv_sums, certified, tol):
    rows = []
    for rn, cert in certified.items():
        cval = cert.get(certified_key)
        if cval is None:
            continue
        ours = csv_sums.get(rn)
        if ours is None:
            continue          # report has no rows of this type in our cache
        diff = ours - cval
        denom = cval if cval else (ours or 1)
        pct = abs(diff) / denom if denom else 0
        rows.append((rn, cval, ours, diff, pct))

    if not rows:
        print(f'\n{label}: no overlapping reports to compare.')
        return
    matched = [r for r in rows if r[4] <= tol]
    print(f'\n{label}: {len(rows):,} reports compared, '
          f'{len(matched):,} within {tol:.0%} ({len(matched)/len(rows)*100:.1f}% match)')
    cert_sum = sum(r[1] for r in rows)
    our_sum  = sum(r[2] for r in rows)
    print(f'  aggregate certified: ${cert_sum:,.0f}   ours: ${our_sum:,.0f}   '
          f'delta: ${our_sum-cert_sum:,.0f} ({(our_sum-cert_sum)/cert_sum*100:+.2f}%)')
    worst = sorted(rows, key=lambda r: -abs(r[3]))[:10]
    print(f'  largest discrepancies (report: certified -> ours = delta):')
    for rn, cval, ours, diff, pct in worst:
        print(f'    {rn:>12}  ${cval:>13,.2f} -> ${ours:>13,.2f}  = ${diff:>+13,.2f} ({pct*100:.0f}%)')


def main():
    ap = argparse.ArgumentParser(description='Reconcile CSV totals vs certified filings')
    ap.add_argument('--limit', type=int, help='cap number of PDFs parsed (faster)')
    ap.add_argument('--tol', type=float, default=0.005, help='match tolerance as a fraction (default 0.5%%)')
    args = ap.parse_args()

    print('Summing our cached records by report number...')
    contrib_sums = csv_sums_by_report('contributions')
    expend_sums  = csv_sums_by_report('expenditures')
    wanted = set(contrib_sums) | set(expend_sums)

    print('Reading certified totals from cached report PDFs...')
    cert = certified_totals(wanted=wanted, limit=args.limit)
    print(f'  {len(cert):,} reports with a parseable Summary Page')

    _reconcile('CONTRIBUTIONS (Line 1, Schedule A-1)', 'contrib', contrib_sums, cert, args.tol)
    _reconcile('EXPENDITURES (Line 9, Schedule E-1)',  'expend',  expend_sums,  cert, args.tol)


if __name__ == '__main__':
    main()
