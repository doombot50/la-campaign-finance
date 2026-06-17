#!/usr/bin/env python3
"""
build_certified_totals.py — persist certified per-report totals to a committed JSON
====================================================================================
reconcile_scrape.py downloads report PDFs into .ethics_pdf_cache/ (gitignored,
thousands of files). This extracts the certified Summary-Page figures from every
cached PDF — Line 1 Contributions (Schedule A-1) and Line 9 Expenditures
(Schedule E-1) — into one small committed artifact, so:

  * the reconciliation value survives without keeping the PDFs in git,
  * reconcile_totals can read totals instantly instead of re-parsing PDFs,
  * a future "matches certified filing" UI badge has the numbers on hand.

Output: certified_report_totals.json  { reportNumber: {contrib, expend} }
Run after reconcile_scrape.py. Requires pdfplumber (parses the cached PDFs).
"""
import json
import os
import time

import reconcile_totals as rt

OUT = os.path.join(rt.BASE, 'certified_report_totals.json')


def main():
    print('Parsing certified totals from every cached report PDF...')
    cert = rt.certified_totals()          # wanted=None -> all cached PDFs
    payload = {
        'built_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'count':    len(cert),
        'note':     ('Certified Summary-Page figures per report: contrib = Line 1 '
                     '(Schedule A-1 contributions), expend = Line 9 (Schedule E-1 '
                     'expenditures). Source: eap.ethics.la.gov report PDFs.'),
        'reports':  cert,
    }
    tmp = OUT + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(payload, f, separators=(',', ':'), sort_keys=True)
    os.replace(tmp, OUT)
    size_kb = os.path.getsize(OUT) / 1024
    print(f'Wrote {OUT}  ({len(cert):,} reports, {size_kb:,.0f} KB)')

    # Coverage payoff — compare against our CSV sums (no extra PDF parsing).
    contrib = rt.csv_sums_by_report('contributions')
    total_csv = sum(contrib.values())
    covered = sum(v for rn, v in contrib.items()
                  if rn in cert and cert[rn].get('contrib') is not None)
    if total_csv:
        print(f'Contribution dollars now reconcilable: ${covered:,.0f} / '
              f'${total_csv:,.0f} ({covered/total_csv*100:.1f}%)')


if __name__ == '__main__':
    main()
