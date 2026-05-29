#!/usr/bin/env python3
"""
fetch_ethics_coh.py — Louisiana Ethics COH Scraper
====================================================
Fetches certified Cash on Hand from the Louisiana Board of Ethics annual
reports — both F102 (candidate) and F202 (PAC) — for every entry in
la_candidate_index.json.gz.

Flow per filer:
  1. If we already have filer_id (CAN-style) cached, skip to step 3.
  2. Search SearchByNameAdv.aspx + click through to discover ViewEFiler.aspx?FilerID=...
  3. Parse ViewEFiler.aspx (no auth needed) to find ALL Annual reports
     (F102 for candidates, F202 for PACs).
  4. For each Annual report, download the PDF from
     https://eap.ethics.la.gov/CFSearch/LA-{ReportID}.pdf and extract the
     "Funds on hand at beginning/closing" line items from the Summary Page.
  5. Write ethics_coh_cache.json (incremental — saved after each filer).
     Cache shape (per candidate name):
       {
         "filer_id":   "CAN999999",
         "reports":    [ {report_id, report_year, beginning_coh, ending_coh,
                          date_filed, pdf_url, ...}, ... ],   # oldest → newest
         "fetched_at": "...",
         # Plus the most-recent report's fields flattened for backward-compat
         # with code paths that read `ending_coh` / `report_year` directly.
       }

Usage:
    py -3 fetch_ethics_coh.py                          # all candidates, all years
    py -3 fetch_ethics_coh.py --filer "MANDIE LANDRY"  # single filer, all years
    py -3 fetch_ethics_coh.py --year 2025              # restrict to that report year
    py -3 fetch_ethics_coh.py --dry-run                # search + discover, no PDF
    py -3 fetch_ethics_coh.py --force                  # re-fetch even if cached
    py -3 fetch_ethics_coh.py --limit 10               # cap at N filers (testing)

PDFs cached in .ethics_pdf_cache/ to skip re-downloads.
FilerIDs (CAN-style) cached in ethics_coh_cache.json under each entry.

CACHE KEY LIMITATION
--------------------
ethics_coh_cache.json is keyed by the *normalized display name* used in our
la_candidate_index.json.gz (upper-cased, whitespace-collapsed). This means:

  - The search query sent to ethics.la.gov is the dashboard name verbatim.
    If the ethics portal spells a name differently (e.g., "J. CAMERON HENRY"
    vs "J CAMERON HENRY", or a hyphenated surname), the search may return no
    match even though the filer exists.

  - On a full rebuild (py -3 fetch_ethics_coh.py with no --filer), only names
    in the candidate index get scraped. Newly-added candidates or names that
    changed between builds won't appear until the next full run.

  - Remedy: if --filer returns no match, try a last-name-only retry (already
    done automatically) or pass a shorter/alternate spelling via --filer
    manually, then the discovered filer_id will be reused on future runs.

  - Long-term fix (not yet implemented): build a FilerID index keyed by
    ethics portal name at scrape time, then fuzzy-match dashboard names
    against it rather than relying on the search form returning the right hit.
"""

import urllib.request
import urllib.parse
import http.cookiejar
import html.parser
import json
import os
import re
import socket
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


def _download_binary(url, timeout=90, retries=3):
    """Download bytes from `url` with retry-with-backoff on transient timeouts.

    PDFs from eap.ethics.la.gov are large and the server sometimes streams them
    slowly; the per-socket read timeout fires far more often than the connect
    fails. We retry 3× with exponential backoff before raising, and treat 4xx
    HTTP errors as permanent (don't retry them).
    """
    last_err = None
    req = urllib.request.Request(url, headers={'User-Agent': UA})
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            # 4xx is permanent — don't burn retries on 404s etc.
            if 400 <= e.code < 500 and e.code != 408:
                raise
            last_err = e
        except (urllib.error.URLError, socket.timeout, TimeoutError, ConnectionError) as e:
            last_err = e
        if attempt < retries:
            backoff = 2 ** attempt   # 2s, 4s, 8s
            print(f'    transient error on attempt {attempt}/{retries} ({last_err}); retrying in {backoff}s...')
            time.sleep(backoff)
    raise last_err


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
        # Must contain a ReportID, an Annual report type, and "(ANN)" marker.
        # F102 = candidate annual; F202 = PAC annual. Both share the same
        # Summary Page layout (beginning + closing cash on hand line items).
        if 'ReportID=' not in row:
            continue
        if 'F102' in row:    form_type = 'F102'
        elif 'F202' in row:  form_type = 'F202'
        else:                continue
        # Require the specific "(ANN)" report-type code. The plain word "Annual"
        # appears in unrelated table chrome on the PAC listings and was matching
        # monthly / campaign-period reports — e.g., Republican Party of LA was
        # listing 55 "annual" reports because of this.
        if '(ANN)' not in row:
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
        # Sanity check: source data occasionally has sentinel dates like
        # "1/1/3001" that fool the heuristic. Reject implausible years and
        # fall back to date_filed - 1 (annuals close out year-after-filing).
        if year_start is not None and (year_start < 1980 or year_start > 2050):
            year_start = None
        if year_end is not None and (year_end < 1980 or year_end > 2050):
            year_end = None
        if year_start is None and date_filed:
            year_start = int(date_filed.split('/')[-1]) - 1

        results.append({
            'report_id':  report_id,
            'year_start': year_start,
            'year_end':   year_end,
            'date_filed': date_filed,
            'row_text':   clean,
            'form_type':  form_type,    # 'F102' (candidate) or 'F202' (PAC)
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

    # Dedupe by reporting year: an annual report can have amendments (each is
    # a separate row with the same year_start). Keep the latest by report_id
    # — amendments correct earlier versions, so the highest ID is authoritative.
    by_year = {}
    for r in results:
        yr = r.get('year_start')
        if yr is None:
            by_year.setdefault(('no-year', r['report_id']), r)   # keep all year-unknowns
            continue
        prev = by_year.get(yr)
        if prev is None or r['report_id'] > prev['report_id']:
            by_year[yr] = r
    return list(by_year.values())


# ── PDF download ──────────────────────────────────────────────────────────────

def download_pdf(report_id: int, dry_run: bool = False) -> str | None:
    local = os.path.join(PDF_CACHE_DIR, f'LA-{report_id}.pdf')
    # Treat suspiciously small cached files (<1KB) as broken partial downloads
    # from earlier runs and re-fetch. A real F102 PDF is always tens of KB+.
    if os.path.exists(local):
        if os.path.getsize(local) >= 1024:
            return local
        print(f'    note: cached PDF {os.path.basename(local)} is tiny ({os.path.getsize(local)}B); re-fetching')
        try: os.remove(local)
        except OSError: pass
    if dry_run:
        return None

    url = PDF_URL.format(report_id=report_id)
    # Write atomically (download → temp → rename) so an interrupted run can't
    # leave a half-written .pdf in the cache that future runs treat as complete.
    tmp = local + '.partial'
    try:
        data = _download_binary(url)
        with open(tmp, 'wb') as f:
            f.write(data)
        os.replace(tmp, local)
        time.sleep(DELAY)
        return local
    except Exception as e:
        print(f'    ERR PDF download ({url}): {e}')
        try:
            if os.path.exists(tmp): os.remove(tmp)
        except OSError: pass
        return None


# ── PDF COH parser ────────────────────────────────────────────────────────────

# F102 (candidate) Summary Page uses lines 14 / 18 for beginning / closing
# Funds on Hand. F202 (PAC) uses the same wording on its Summary Page but
# possibly different line numbers, so we match any `N.` prefix.
_RE_BEG = re.compile(
    r'\d+\.\s+Funds?\s+on\s+hand\s+at\s+begin\w*'
    r'(?:.*?)'
    r'\$\s*([\d,]+\.\d{2})',
    re.DOTALL | re.IGNORECASE
)
_RE_END = re.compile(
    r'\d+\.\s+Funds?\s+on\s+hand\s+at\s+clos\w*'
    r'(?:.*?)'
    r'\$\s*([\d,]+\.\d{2})',
    re.DOTALL | re.IGNORECASE
)


def parse_coh(pdf_path: str) -> tuple:
    """
    Return (beginning_coh, ending_coh) floats from the report Summary Page.
    Works for both F102 (candidate) and F202 (PAC) annual reports — both share
    the same Summary Page layout with numbered "Funds on hand at beginning"
    and "Funds on hand at close" line items.

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

                m_beg = _RE_BEG.search(text)
                m_end = _RE_END.search(text)
                if m_beg or m_end:
                    beg = float(m_beg.group(1).replace(',', '')) if m_beg else None
                    end = float(m_end.group(1).replace(',', '')) if m_end else None
                    return beg, end
        return None, None
    except Exception as e:
        print(f'    ERR PDF parse ({pdf_path}): {e}')
        return None, None


# ── Candidate list ────────────────────────────────────────────────────────────

def _norm(name: str) -> str:
    return re.sub(r'\s+', ' ', name.strip().upper())


# PAC-shaped name heuristic — anything whose name contains one of these markers
# is treated as a PAC / committee for filtering purposes.
_PAC_HINTS = ('PAC', 'POLITICAL ACTION', 'COMMITTEE', 'FUND', 'PARTY', 'CAUCUS', 'TRUST')

def _looks_like_pac(name: str) -> bool:
    n = name.upper()
    return any(h in n for h in _PAC_HINTS)

def load_candidates(by_raised: bool = True, pacs_only: bool = False) -> list:
    """Return the list of candidate names to process.

    by_raised=True (default) sorts by total_raised descending so the most
    consequential filers get scraped first — critical for IE / super-PAC work
    where you'd rather have RGA / Gumbo PAC / GOP cached than the long tail
    of inactive 1998-era candidates.

    pacs_only=True filters to PAC-shaped names (PAC / COMMITTEE / FUND / PARTY
    / CAUCUS / TRUST in the name) for focused PAC scrape runs.
    """
    entries = []   # list of (name, total_raised)
    if os.path.exists(CAND_INDEX):
        with gzip.open(CAND_INDEX, 'rt', encoding='utf-8') as f:
            idx = json.load(f)
        for n, d in idx.items():
            entries.append((_norm(n), float(d.get('total_raised') or 0)))
    elif os.path.exists(FILER_LOOKUP):
        with open(FILER_LOOKUP, 'r', encoding='utf-8') as f:
            entries = [(_norm(n), 0.0) for n in json.load(f).keys()]
    if pacs_only:
        entries = [(n, r) for n, r in entries if _looks_like_pac(n)]
    if by_raised:
        entries.sort(key=lambda x: -x[1])
    return [n for n, _ in entries]


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description='Fetch certified COH from ethics.la.gov')
    ap.add_argument('--filer',    metavar='NAME',
                    help='Process only this filer')
    ap.add_argument('--year',     type=int, metavar='YYYY',
                    help='Restrict to a single report year (default: all years)')
    ap.add_argument('--dry-run',  action='store_true',
                    help='Search + discover FilerID, no PDF download')
    ap.add_argument('--force',    action='store_true',
                    help='Re-fetch even if already in cache')
    ap.add_argument('--limit',    type=int, metavar='N',
                    help='Cap at N filers (testing)')
    ap.add_argument('--pacs-only', action='store_true',
                    help='Only scrape PAC-shaped names (PAC / COMMITTEE / FUND / PARTY / CAUCUS / TRUST)')
    ap.add_argument('--alphabetical', action='store_true',
                    help='Process in index order instead of the default by-total-raised descending')
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
        candidates = load_candidates(by_raised=not args.alphabetical, pacs_only=args.pacs_only)
        order = 'alphabetical' if args.alphabetical else 'by total_raised desc'
        scope = 'PACs only' if args.pacs_only else 'all candidates'
        print(f'Processing {len(candidates)} {scope} from index, ordered {order}')
        if args.limit:
            candidates = candidates[:args.limit]
            print(f'  (limited to first {args.limit})')

    n_ok = n_skip = n_err = n_noreport = 0

    for i, name in enumerate(candidates, 1):
        tag = f'[{i}/{len(candidates)}] {name}'

        # Skip only if we already have the new multi-report list cached.
        # Entries with only the old single-report fields get re-processed so they
        # upgrade to the new schema on the next run (no --force required).
        if not args.force and name in cache and isinstance(cache[name].get('reports'), list) and cache[name]['reports']:
            print(f'SKIP {tag} (cached, {len(cache[name]["reports"])} reports)')
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

        # Optional year filter — restrict the set of reports we process this run.
        # If the filter excludes everything, fall back to processing all reports
        # (so the filer still gets at least its full history cached).
        if args.year:
            yr_match = [r for r in annual if r['year_start'] == args.year or r['year_end'] == args.year]
            if yr_match:
                annual = yr_match
            else:
                print(f'  -> no {args.year} annual report; processing all years instead')

        print(f'  {len(annual)} annual report{"s" if len(annual)!=1 else ""} to process')

        if args.dry_run:
            for rep in annual:
                print(f'    (dry-run) report {rep["report_id"]}  year={rep.get("year_start") or "?"}  filed={rep.get("date_filed") or ""}')
            cache.setdefault(name, {})['filer_id'] = filer_id
            _save_cache(cache)
            continue

        # ── Step 3+4: Download and parse every annual report ──────────────────
        reports_out = []
        for rep in annual:
            report_id  = rep['report_id']
            year_label = rep.get('year_start') or '?'
            date_filed = rep.get('date_filed') or ''

            pdf_path = download_pdf(report_id)
            if pdf_path is None:
                print(f'  ERR  download failed for report {report_id} ({year_label})')
                continue

            beg_coh, end_coh = parse_coh(pdf_path)
            if beg_coh is None and end_coh is None:
                print(f'  ERR  could not extract COH from report {report_id} ({year_label})')
                continue

            # Format defensively — sometimes only one of the two lines parses
            # (e.g., a brand-new PAC's first report has no beginning balance,
            # or a final report leaves the closing field blank).
            _fmt = lambda v: f'${v:,.2f}' if v is not None else '(none)'
            print(f'  OK   {year_label}  beginning={_fmt(beg_coh)}  ending={_fmt(end_coh)}')

            reports_out.append({
                'report_id':     report_id,
                'report_period': 'Annual',
                'report_year':   year_label,
                'form_type':     rep.get('form_type', 'F102'),   # F102=cand, F202=PAC
                'date_filed':    date_filed,
                'beginning_coh': beg_coh,
                'ending_coh':    end_coh,
                'pdf_url':       PDF_URL.format(report_id=report_id),
            })

        if not reports_out:
            n_err += 1
            continue

        # Sort oldest → newest so chart consumers can iterate in chronological order.
        def _yr_key(r):
            try:    return int(r['report_year'])
            except: return 0
        reports_out.sort(key=_yr_key)
        latest = reports_out[-1]

        cache[name] = {
            'filer_id':      filer_id,
            'reports':       reports_out,
            # Flat fields = most-recent report. Kept for backward compatibility with
            # code paths that pre-date the `reports` list (e.g., the COH stat panel).
            'report_id':     latest['report_id'],
            'report_period': 'Annual',
            'report_year':   latest['report_year'],
            'form_type':     latest.get('form_type', 'F102'),
            'date_filed':    latest['date_filed'],
            'beginning_coh': latest['beginning_coh'],
            'ending_coh':    latest['ending_coh'],
            'pdf_url':       latest['pdf_url'],
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
