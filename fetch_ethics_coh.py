#!/usr/bin/env python3
"""
fetch_ethics_coh.py — Louisiana Ethics COH Scraper
====================================================
Fetches certified Cash on Hand (Lines 14 & 18) from the Louisiana Board of Ethics
F102 Annual reports for each candidate in la_candidate_index.json.gz.

Flow per filer:
  1. If we already have filer_id (CAN-style) cached, skip to step 3.
  2. Search SearchByNameAdv.aspx + click through to discover ViewEFiler.aspx?FilerID=...
  3. Parse ViewEFiler.aspx (no auth needed) to find most recent F102 Annual report.
  4. Download PDF from https://eap.ethics.la.gov/CFSearch/LA-{ReportID}.pdf
  5. Parse Lines 14 (Beginning COH) and 18 (Ending COH) from the Summary Page.
  6. Write ethics_coh_cache.json (incremental — saved after each filer).

Usage:
    py -3 fetch_ethics_coh.py                          # all candidates in index
    py -3 fetch_ethics_coh.py --filer "MANDIE LANDRY"  # single filer
    py -3 fetch_ethics_coh.py --year 2025              # filter to that report year
    py -3 fetch_ethics_coh.py --dry-run                # search + discover, no PDF
    py -3 fetch_ethics_coh.py --force                  # re-fetch even if cached
    py -3 fetch_ethics_coh.py --limit 10               # cap at N filers (testing)

PDFs cached in .ethics_pdf_cache/ to skip re-downloads.
FilerIDs (CAN-style) cached in ethics_coh_cache.json under each entry.
"""

import urllib.request
import urllib.parse
import http.cookiejar
import html.parser
import json
import os
import re
import time
import sys
import argparse
import gzip
from datetime import datetime, timezone

try:
    import pdfplumber
except ImportError:
    print("ERROR: pdfplumber not installed. Run: py -3 -m pip install pdfplumber")
    sys.exit(1)

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
FILER_LOOKUP  = os.path.join(BASE_DIR, 'la_filer_lookup.json')
COH_CACHE     = os.path.join(BASE_DIR, 'ethics_coh_cache.json')
PDF_CACHE_DIR = os.path.join(BASE_DIR, '.ethics_pdf_cache')
CAND_INDEX    = os.path.join(BASE_DIR, 'la_candidate_index.json.gz')

# ── Ethics portal URLs ────────────────────────────────────────────────────────
ETHICS_CF   = 'https://www.ethics.la.gov/CampaignFinanceSearch'
SEARCH_URL  = f'{ETHICS_CF}/SearchByNameAdv.aspx'
RESULTS_URL = f'{ETHICS_CF}/SearchResultsAdv.aspx'
VIEW_URL    = f'{ETHICS_CF}/ViewEFiler.aspx?FilerID={{filer_id}}'
PDF_URL     = 'https://eap.ethics.la.gov/CFSearch/LA-{report_id}.pdf'

DELAY   = 1.2   # seconds between web requests — be polite
TIMEOUT = 30    # seconds per HTTP request

os.makedirs(PDF_CACHE_DIR, exist_ok=True)

UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
      'AppleWebKit/537.36 (KHTML, like Gecko) '
      'Chrome/124.0 Safari/537.36')


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _make_opener():
    """Return a urllib opener with a fresh cookie jar (needed for search flow)."""
    jar = http.cookiejar.CookieJar()
    return urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))

_OPENER = _make_opener()   # shared opener — preserves ASP.NET session across requests


def _fetch(url, data=None, referer=None, opener=None):
    """GET or POST (if data dict given). Returns decoded HTML string."""
    op = opener or _OPENER
    headers = {
        'User-Agent': UA,
        'Accept': 'text/html,application/xhtml+xml,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    }
    if referer:
        headers['Referer'] = referer
    if data is not None:
        body = urllib.parse.urlencode(data).encode('utf-8')
        headers['Content-Type'] = 'application/x-www-form-urlencoded'
        req = urllib.request.Request(url, data=body, headers=headers)
    else:
        req = urllib.request.Request(url, headers=headers)
    with op.open(req, timeout=TIMEOUT) as resp:
        raw = resp.read()
        final_url = resp.url
    for enc in ('utf-8', 'windows-1252', 'latin-1'):
        try:
            return raw.decode(enc), final_url
        except UnicodeDecodeError:
            continue
    return raw.decode('utf-8', errors='replace'), final_url


def _download_binary(url):
    req = urllib.request.Request(url, headers={'User-Agent': UA})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
        return resp.read()


# ── ASP.NET hidden-field extraction ──────────────────────────────────────────

class _HiddenParser(html.parser.HTMLParser):
    def __init__(self):
        super().__init__()
        self.fields = {}

    def handle_starttag(self, tag, attrs):
        if tag != 'input':
            return
        a = dict(attrs)
        if a.get('type', '').lower() == 'hidden' and a.get('name'):
            self.fields[a['name']] = a.get('value', '')


def _hidden(html_text):
    p = _HiddenParser()
    p.feed(html_text)
    return p.fields


# ── Search & discover FilerID ─────────────────────────────────────────────────

def discover_filer_id(name: str) -> str | None:
    """
    Run the 3-step search flow to discover the FilerID (e.g. 'CAN993944') for
    a given filer name. Uses a fresh opener+cookie-jar per call so each search
    starts with a clean ASP.NET session.

    Returns the FilerID string, or None if not found.
    """
    op = _make_opener()

    # Step 1: GET search form (establishes session cookie)
    html1, _ = _fetch(SEARCH_URL, opener=op)
    time.sleep(0.4)
    hidden1 = _hidden(html1)

    # Step 2: POST search form
    post1 = dict(hidden1)
    post1.update({
        '__EVENTTARGET':   'ctl00$ContentPlaceHolder1$SearchLinkButton',
        '__EVENTARGUMENT': '',
        'ctl00$ContentPlaceHolder1$NameTextBox':   name,
        'ctl00$ContentPlaceHolder1$OfficeTextBox': '',
        'ctl00$ContentPlaceHolder1$YearTextBox':   '',
        'ctl00$ContentPlaceHolder1$DateFromRadDateInput': '',
        'ctl00$ContentPlaceHolder1$DateToRadDateInput':   '',
        'ctl00_ContentPlaceHolder1_DateFromRadDateInput_ClientState': '',
        'ctl00_ContentPlaceHolder1_DateToRadDateInput_ClientState':   '',
    })
    html2, results_url = _fetch(SEARCH_URL, data=post1, referer=SEARCH_URL, opener=op)
    time.sleep(DELAY)

    # Step 3: Check record count; bail if 0
    m_count = re.search(r'(\d[\d,]*)\s+records?\s+were\s+found', html2, re.I)
    if m_count:
        count = int(m_count.group(1).replace(',', ''))
        if count == 0:
            return None

    # Step 4: Extract VIEWSTATE from results page; click first FullNameLinkButton
    hidden2 = _hidden(html2)
    if not hidden2.get('__VIEWSTATE'):
        # Sometimes the results land directly on SearchResultsAdv without __VIEWSTATE
        # Re-fetch the results page to get its VIEWSTATE
        html2b, _ = _fetch(results_url, opener=op)
        hidden2 = _hidden(html2b)
        html2 = html2b

    # Find the first row's FullNameLinkButton ID
    m_btn = re.search(
        r'ctl00_ContentPlaceHolder1_ResultsGridView_(ctl\d+)_FullNameLinkButton',
        html2
    )
    if not m_btn:
        return None
    row_id = m_btn.group(1)
    btn_target = f'ctl00$ContentPlaceHolder1$ResultsGridView${row_id}$FullNameLinkButton'

    post2 = dict(hidden2)
    post2['__EVENTTARGET']   = btn_target
    post2['__EVENTARGUMENT'] = ''
    _, view_url = _fetch(results_url, data=post2, referer=results_url, opener=op)
    time.sleep(DELAY)

    # Step 5: Extract FilerID from the redirect URL
    m_fid = re.search(r'FilerID=([A-Z0-9]+)', view_url, re.I)
    if m_fid:
        return m_fid.group(1)

    return None


# ── ViewEFiler page parser ────────────────────────────────────────────────────

def get_annual_reports(filer_id: str) -> list:
    """
    Fetch ViewEFiler.aspx?FilerID={filer_id} and return list of dicts for
    F102 Annual reports, sorted newest-first.

    Each dict: {report_id, year_start, year_end, date_filed}
    """
    url = VIEW_URL.format(filer_id=filer_id)
    html_text, _ = _fetch(url)
    time.sleep(DELAY)

    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html_text, re.S | re.I)
    results = []

    for row in rows:
        # Must contain a ReportID, F102 report type, and "(ANN)" (Annual) marker
        if 'ReportID=' not in row:
            continue
        if 'F102' not in row:          # candidates only; skip F202 (PAC), etc.
            continue
        if '(ANN)' not in row and 'Annual' not in row:
            continue
        # Skip rows marked as Superseded
        if 'Superseded' in row:
            continue

        m_id = re.search(r'ReportID=(\d+)', row)
        if not m_id:
            continue
        report_id = int(m_id.group(1))

        # Extract dates from the row text
        clean = re.sub(r'<[^>]+>', ' ', row)
        clean = re.sub(r'&nbsp;', ' ', clean)
        clean = re.sub(r'\s+', ' ', clean).strip()

        # Dates in row: may include election date, period start, period end, filed date.
        # Annual F102 period always starts 1/1/YYYY — use that to identify reporting year.
        dates = re.findall(r'\b(\d{1,2}/\d{1,2}/\d{4})\b', clean)
        year_start = year_end = date_filed = None
        date_filed = dates[-1] if dates else None
        # Reporting period start is always 1/1/YYYY for Annual reports
        jan_dates = [d for d in dates if d.startswith('1/1/')]
        if jan_dates:
            year_start = int(jan_dates[0].split('/')[-1])
        # Period end: 12/31/YYYY
        dec_dates  = [d for d in dates if d.startswith('12/31/')]
        if dec_dates:
            year_end = int(dec_dates[0].split('/')[-1])
        # Fallback: use filed date year
        if year_start is None and date_filed:
            year_start = int(date_filed.split('/')[-1]) - 1

        results.append({
            'report_id':  report_id,
            'year_start': year_start,
            'year_end':   year_end,
            'date_filed': date_filed,
            'row_text':   clean,
        })

    # Sort newest-first by date_filed then report_id descending
    def _key(r):
        if r['date_filed']:
            try:
                return datetime.strptime(r['date_filed'], '%m/%d/%Y')
            except ValueError:
                pass
        return datetime(2000, 1, 1)

    results.sort(key=_key, reverse=True)
    return results


# ── PDF download ──────────────────────────────────────────────────────────────

def download_pdf(report_id: int, dry_run: bool = False) -> str | None:
    local = os.path.join(PDF_CACHE_DIR, f'LA-{report_id}.pdf')
    if os.path.exists(local):
        return local
    if dry_run:
        return None

    url = PDF_URL.format(report_id=report_id)
    try:
        data = _download_binary(url)
        with open(local, 'wb') as f:
            f.write(data)
        time.sleep(DELAY)
        return local
    except Exception as e:
        print(f'    ERR PDF download ({url}): {e}')
        return None


# ── PDF COH parser ────────────────────────────────────────────────────────────

_RE14 = re.compile(
    r'14\.\s+Funds?\s+on\s+hand\s+at\s+begin\w*'
    r'(?:.*?)'
    r'\$\s*([\d,]+\.\d{2})',
    re.DOTALL | re.IGNORECASE
)
_RE18 = re.compile(
    r'18\.\s+Funds?\s+on\s+hand\s+at\s+clos\w*'
    r'(?:.*?)'
    r'\$\s*([\d,]+\.\d{2})',
    re.DOTALL | re.IGNORECASE
)


def parse_coh(pdf_path: str) -> tuple:
    """
    Return (beginning_coh, ending_coh) floats from the F102 Summary Page.
    Returns (None, None) on failure.
    """
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages[:8]:
                text = page.extract_text() or ''
                upper = text.upper()
                if 'SUMMARY PAGE' not in upper:
                    continue
                if 'SUMMARY PAGE (CONTINUED)' in upper:
                    continue

                m14 = _RE14.search(text)
                m18 = _RE18.search(text)
                if m14 or m18:
                    beg = float(m14.group(1).replace(',', '')) if m14 else None
                    end = float(m18.group(1).replace(',', '')) if m18 else None
                    return beg, end
        return None, None
    except Exception as e:
        print(f'    ERR PDF parse ({pdf_path}): {e}')
        return None, None


# ── Candidate list ────────────────────────────────────────────────────────────

def _norm(name: str) -> str:
    return re.sub(r'\s+', ' ', name.strip().upper())


def load_candidates() -> list:
    if os.path.exists(CAND_INDEX):
        with gzip.open(CAND_INDEX, 'rt', encoding='utf-8') as f:
            idx = json.load(f)
        return [_norm(n) for n in idx.keys()]
    if os.path.exists(FILER_LOOKUP):
        with open(FILER_LOOKUP, 'r', encoding='utf-8') as f:
            return [_norm(n) for n in json.load(f).keys()]
    return []


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description='Fetch certified COH from ethics.la.gov')
    ap.add_argument('--filer',    metavar='NAME',
                    help='Process only this filer')
    ap.add_argument('--year',     type=int, metavar='YYYY',
                    help='Prefer annual report for this year (default: most recent)')
    ap.add_argument('--dry-run',  action='store_true',
                    help='Search + discover FilerID, no PDF download')
    ap.add_argument('--force',    action='store_true',
                    help='Re-fetch even if already in cache')
    ap.add_argument('--limit',    type=int, metavar='N',
                    help='Cap at N filers (testing)')
    args = ap.parse_args()

    # Load existing cache
    cache: dict = {}
    if os.path.exists(COH_CACHE):
        with open(COH_CACHE, 'r', encoding='utf-8') as f:
            cache = json.load(f)
        print(f'Loaded existing cache: {len(cache)} entries')

    # Build candidate list
    if args.filer:
        candidates = [_norm(args.filer)]
    else:
        candidates = load_candidates()
        print(f'Processing {len(candidates)} candidates from index')
        if args.limit:
            candidates = candidates[:args.limit]
            print(f'  (limited to first {args.limit})')

    n_ok = n_skip = n_err = n_noreport = 0

    for i, name in enumerate(candidates, 1):
        tag = f'[{i}/{len(candidates)}] {name}'

        if not args.force and name in cache and cache[name].get('ending_coh') is not None:
            print(f'SKIP {tag} (cached)')
            n_skip += 1
            continue

        print(f'\n{tag}')

        # ── Step 1: Get FilerID (from cache or by searching) ──────────────────
        filer_id = cache.get(name, {}).get('filer_id')
        if not filer_id:
            print(f'  searching for filer ID...')
            try:
                filer_id = discover_filer_id(name)
            except Exception as e:
                print(f'  ERR discover: {e}')
                n_err += 1
                continue
            if not filer_id:
                # Try just last name
                last = name.split()[-1]
                if last != name:
                    print(f'  -> no result; retrying with last name "{last}"')
                    try:
                        time.sleep(DELAY)
                        filer_id = discover_filer_id(last)
                    except Exception as e:
                        print(f'  ERR retry: {e}')
                if not filer_id:
                    print(f'  -> filer not found on ethics portal')
                    n_noreport += 1
                    continue

        print(f'  filer_id = {filer_id}')

        # ── Step 2: Get annual reports list from ViewEFiler ───────────────────
        try:
            annual = get_annual_reports(filer_id)
        except Exception as e:
            print(f'  ERR ViewEFiler: {e}')
            n_err += 1
            continue

        if not annual:
            print(f'  -> no F102 Annual reports found')
            n_noreport += 1
            # Cache the filer_id even if no reports, to skip search next time
            cache.setdefault(name, {})['filer_id'] = filer_id
            _save_cache(cache)
            continue

        # Filter to requested year
        if args.year:
            yr_match = [r for r in annual if r['year_start'] == args.year or r['year_end'] == args.year]
            if yr_match:
                annual = yr_match
            else:
                print(f'  -> no {args.year} annual report; using most recent')

        best = annual[0]
        report_id  = best['report_id']
        year_label = best.get('year_start') or '?'
        date_filed = best.get('date_filed') or ''
        print(f'  report: {report_id}  year={year_label}  filed={date_filed}')
        print(f'    row: {best["row_text"][:120]}')

        if args.dry_run:
            print(f'  (dry-run — skipping PDF)')
            # Still cache the filer_id
            cache.setdefault(name, {})['filer_id'] = filer_id
            _save_cache(cache)
            continue

        # ── Step 3: Download PDF ──────────────────────────────────────────────
        pdf_path = download_pdf(report_id)
        if pdf_path is None:
            n_err += 1
            continue

        # ── Step 4: Parse COH ─────────────────────────────────────────────────
        beg_coh, end_coh = parse_coh(pdf_path)
        if beg_coh is None and end_coh is None:
            print(f'  ERR could not extract COH from PDF')
            n_err += 1
            continue

        print(f'  OK  beginning=${beg_coh:,.2f}  ending=${end_coh:,.2f}')

        cache[name] = {
            'filer_id':      filer_id,
            'report_id':     report_id,
            'report_period': 'Annual',
            'report_year':   year_label,
            'date_filed':    date_filed,
            'beginning_coh': beg_coh,
            'ending_coh':    end_coh,
            'pdf_url':       PDF_URL.format(report_id=report_id),
            'fetched_at':    datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        }
        n_ok += 1
        _save_cache(cache)

    print(f'\n{"="*60}')
    print(f'Done.')
    print(f'  Fetched:    {n_ok}')
    print(f'  Skipped:    {n_skip}  (already cached)')
    print(f'  No report:  {n_noreport}')
    print(f'  Errors:     {n_err}')
    print(f'  Cache:      {COH_CACHE}  ({len(cache)} total entries)')


def _save_cache(cache):
    with open(COH_CACHE, 'w', encoding='utf-8') as f:
        json.dump(cache, f, indent=2)


if __name__ == '__main__':
    main()
