#!/usr/bin/env python3
"""
reconcile_scrape.py — fetch the report PDFs that carry the most contribution
dollars, so reconcile_totals.py can cross-check most of the money.
================================================================================
Unlike fetch_ethics_coh.py (which searches per filer, then discovers reports),
this needs no search at all: every contribution row already carries its
ReportNumber, and the PDF lives at a direct URL —

    https://eap.ethics.la.gov/CFSearch/{ReportNumber}.pdf

Contribution dollars are steeply concentrated by report (the top ~27% of
reports hold ~90% of the money), so a dollar-ranked fetch reconciles the vast
majority of contributions without downloading the long tail of tiny filings.

PDFs land in .ethics_pdf_cache/ (shared with the COH scraper); already-cached
reports are skipped, so re-runs are cheap and resumable. Purely an internal
QA aid — the user-facing "view filing" links point at ethics.la.gov live and
need none of this.

Requires nothing beyond stdlib. Usage:
    py -3 reconcile_scrape.py --dry-run            # rank only, show targets + coverage
    py -3 reconcile_scrape.py --limit 1500         # fetch top 1500 uncached reports
    py -3 reconcile_scrape.py --min-amount 5000    # only reports with >= $5k on them
"""
import argparse
import glob
import gzip
import json
import os
import socket
import sys
import time
import urllib.request
from collections import defaultdict

BASE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(BASE, '.la_cache')
PDF_CACHE = os.path.join(BASE, '.ethics_pdf_cache')
PDF_URL = 'https://eap.ethics.la.gov/CFSearch/{rn}.pdf'
UA = 'Mozilla/5.0 LACampaignFinanceReconcile/1.0'
DELAY = 1.0   # seconds between downloads — be polite


def report_dollars():
    """reportNumber -> summed contribution + expenditure dollars on that report."""
    sums = defaultdict(float)
    for rtype in ('contributions', 'expenditures'):
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


def _download(rn, retries=3):
    """Direct-download one report PDF atomically. Returns True on success."""
    dest = os.path.join(PDF_CACHE, f'{rn}.pdf')
    part = dest + '.part'
    url = PDF_URL.format(rn=rn)
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': UA})
            with urllib.request.urlopen(req, timeout=90) as resp, open(part, 'wb') as out:
                if resp.status != 200:
                    raise IOError(f'HTTP {resp.status}')
                while chunk := resp.read(1 << 16):
                    out.write(chunk)
            if os.path.getsize(part) < 1000:        # too small to be a real PDF
                raise IOError('suspiciously small file')
            os.replace(part, dest)
            return True
        except urllib.error.HTTPError as e:
            if 400 <= e.code < 500 and e.code != 408:
                print(f'  {rn}: HTTP {e.code} (permanent) — skipping')
                break                                # 404 etc. won't fix on retry
            last = e
        except (urllib.error.URLError, socket.timeout, TimeoutError, ConnectionError, IOError) as e:
            last = e
        if os.path.exists(part):
            try: os.remove(part)
            except OSError: pass
        if attempt < retries:
            time.sleep(2 ** attempt)
    return False


def main():
    ap = argparse.ArgumentParser(description='Dollar-ranked report-PDF fetch for reconciliation')
    ap.add_argument('--limit', type=int, default=1000, help='max reports to fetch (default 1000)')
    ap.add_argument('--min-amount', type=float, default=0, help='skip reports below this $ total')
    ap.add_argument('--dry-run', action='store_true', help='rank + show targets, download nothing')
    args = ap.parse_args()

    os.makedirs(PDF_CACHE, exist_ok=True)
    have = {os.path.splitext(os.path.basename(p))[0]
            for p in glob.glob(os.path.join(PDF_CACHE, 'LA-*.pdf'))}

    sums = report_dollars()
    total = sum(sums.values())
    if not total:
        print('No reports with a reportNumber yet — run the schema backfill first '
              '(refresh_la_cache re-parses cycles to add ReportNumber).')
        return

    ranked = sorted(sums.items(), key=lambda kv: -kv[1])
    targets = [(rn, amt) for rn, amt in ranked
               if rn not in have and amt >= args.min_amount][:args.limit]

    have_dollars = sum(amt for rn, amt in ranked if rn in have)
    target_dollars = sum(amt for _, amt in targets)
    print(f'reports referenced: {len(ranked):,}   total ${total:,.0f}')
    print(f'already cached: {len(have & set(sums))} reports (${have_dollars:,.0f}, '
          f'{have_dollars/total*100:.1f}%)')
    print(f'this run targets {len(targets):,} reports (${target_dollars:,.0f}, '
          f'+{target_dollars/total*100:.1f}% coverage)')
    print(f'projected coverage after run: {(have_dollars+target_dollars)/total*100:.1f}%')

    if args.dry_run:
        print('\n(dry run) top 10 targets:')
        for rn, amt in targets[:10]:
            print(f'  {rn:>12}  ${amt:>14,.0f}  {PDF_URL.format(rn=rn)}')
        return

    ok = fail = 0
    for i, (rn, amt) in enumerate(targets, 1):
        if _download(rn):
            ok += 1
        else:
            fail += 1
            print(f'  [{i}/{len(targets)}] FAILED {rn}')
        if i % 50 == 0:
            print(f'  {i}/{len(targets)} done ({ok} ok, {fail} failed)')
        time.sleep(DELAY)
    print(f'\nFetched {ok} PDFs ({fail} failed). Run reconcile_totals.py to compare.')


if __name__ == '__main__':
    main()
