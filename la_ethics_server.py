#!/usr/bin/env python3
"""
Louisiana Ethics Campaign Finance Data Proxy
=============================================
Downloads CSVs from the Louisiana Board of Ethics, parses them,
caches them locally, and serves them as JSON with CORS headers so
the dashboard HTML can fetch them from a file:// or localhost page.

Usage:
    python la_ethics_server.py

Then reload the dashboard — LA Ethics data loads automatically.
Cache is stored in .la_cache/ and refreshed every 24 hours.
"""

from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from urllib.parse import urlparse, parse_qs
import urllib.request
import json, csv, io, os, re, time, gzip, threading, gc, sys
from datetime import date

class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle each HTTP request in its own thread so a slow download never blocks the server."""
    daemon_threads = True

PORT = int(os.environ.get('PORT', 8765))
BIND_HOST = '0.0.0.0'   # listen on all interfaces when deployed; localhost when running locally
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(BASE_DIR, '.la_cache')
os.makedirs(CACHE_DIR, exist_ok=True)
CACHE_TTL = 86400  # 24 hours
# Offline mode (LA_OFFLINE=1): never touch the network or mutate the record
# cache — no stale-busting, no background prefetch, no on-demand re-download.
# The parity gates set this so the live server serves the EXACT on-disk snapshot
# build_static_api.py dumped, instead of re-downloading the volatile current
# cycle mid-run and drifting from the static artifacts it's being compared to.
OFFLINE = os.environ.get('LA_OFFLINE') == '1'
HTML_FILE = os.path.join(BASE_DIR, 'louisiana-campaign-finance.html')

# ── Candidate career data (lazy-loaded on first request) ──────────────────────
import re as _re
_CAND_INDEX  = None   # la_candidate_index.json.gz  — financial summary per cycle
_FILER_CAREER = None  # la_filer_index.json.gz      — same summary keyed by filerNumber
_CAND_RACES  = None   # la_candidacies_raw.json.gz  — all SoS race appearances
_CAND_LOCK   = threading.Lock()
_CAND_IDX_MTIME = 0   # mtime of index file when last loaded

# ── Donor industry lookup (lazy-loaded, hot-reloadable) ────────────────────────
_DONOR_IND       = None   # la_donor_industries.json — {donor_name: industry_bucket}
_DONOR_IND_MTIME = 0
_DONOR_IND_LOCK  = threading.Lock()

# ── Filer number lookup (lazy-loaded once from cache files) ───────────────────
_FILER_NUM       = None   # la_filer_lookup.json — {norm_name: filer_number}
_FILER_NUM_LOCK  = threading.Lock()

# ── Ethics certified COH cache (lazy-loaded, hot-reloadable) ──────────────────
_ETHICS_COH      = None   # ethics_coh_cache.json — {norm_name: {ending_coh, ...}}
_ETHICS_COH_MTIME = 0
_ETHICS_COH_LOCK = threading.Lock()

def _load_ethics_coh():
    global _ETHICS_COH, _ETHICS_COH_MTIME
    with _ETHICS_COH_LOCK:
        cpath = os.path.join(BASE_DIR, 'ethics_coh_cache.json')
        if not os.path.exists(cpath):
            if _ETHICS_COH is None:
                _ETHICS_COH = {}
            return
        mtime = os.path.getmtime(cpath)
        if _ETHICS_COH is not None and mtime <= _ETHICS_COH_MTIME:
            return
        try:
            with open(cpath, 'r', encoding='utf-8') as f:
                _ETHICS_COH = json.load(f)
            _ETHICS_COH_MTIME = mtime
        except Exception:
            if _ETHICS_COH is None:
                _ETHICS_COH = {}

def _get_ethics_coh(name: str) -> dict | None:
    """Return COH entry for normalized name, or None if not available."""
    _load_ethics_coh()
    norm = re.sub(r'\s+', ' ', name.strip().upper())
    return _ETHICS_COH.get(norm) if _ETHICS_COH else None

# ── Canonical entity table (built nightly by build_entities.py) ─────────────
_ENTITIES        = None   # filer_number -> entity record
_ENTITIES_BYNAME = None   # normalized display name / alias -> filer_number
_ENTITIES_MTIME  = 0
_ENTITIES_LOCK   = threading.Lock()

def _load_entities():
    global _ENTITIES, _ENTITIES_BYNAME, _ENTITIES_MTIME
    with _ENTITIES_LOCK:
        p = os.path.join(CACHE_DIR, 'la_entities.json.gz')
        if not os.path.exists(p):
            if _ENTITIES is None:
                _ENTITIES, _ENTITIES_BYNAME = {}, {}
            return
        mtime = os.path.getmtime(p)
        if _ENTITIES is not None and mtime <= _ENTITIES_MTIME:
            return
        with gzip.open(p, 'rt', encoding='utf-8') as f:
            data = json.load(f)
        ents = data.get('entities', {})
        byname = {}
        for fn, e in ents.items():
            for nm in [e.get('name', '')] + (e.get('aliases') or []):
                key = re.sub(r'\s+', ' ', nm.strip().upper())
                if key:
                    byname[key] = fn
        _ENTITIES, _ENTITIES_BYNAME, _ENTITIES_MTIME = ents, byname, mtime
        print(f'  Entities: {len(ents)} loaded from la_entities.json.gz')

def _get_entity(name=None, filer=None):
    """Canonical entity by filer number (preferred) or any known name/alias."""
    _load_entities()
    if filer:
        ent = _ENTITIES.get(str(filer))
        if ent:
            return ent
        # filer not in the entity table — fall through to a name match if given
    if name:
        fn = _ENTITIES_BYNAME.get(re.sub(r'\s+', ' ', name.strip().upper()))
        return _ENTITIES.get(fn) if fn else None
    return None


# ── Precomputed insight blocks (built nightly by build_insights.py) ─────────
_INSIGHTS       = None
_INSIGHTS_MTIME = 0
_INSIGHTS_LOCK  = threading.Lock()

def _load_insights():
    global _INSIGHTS, _INSIGHTS_MTIME
    with _INSIGHTS_LOCK:
        p = os.path.join(CACHE_DIR, 'la_insights.json.gz')
        if not os.path.exists(p):
            if _INSIGHTS is None:
                _INSIGHTS = {}
            return
        mtime = os.path.getmtime(p)
        if _INSIGHTS is not None and mtime <= _INSIGHTS_MTIME:
            return
        with gzip.open(p, 'rt', encoding='utf-8') as f:
            _INSIGHTS = json.load(f)
        _INSIGHTS_MTIME = mtime


def _load_filer_lookup():
    global _FILER_NUM
    with _FILER_NUM_LOCK:
        if _FILER_NUM is not None:
            return
        fpath = os.path.join(BASE_DIR, 'la_filer_lookup.json')
        if os.path.exists(fpath):
            with open(fpath, 'r', encoding='utf-8') as f:
                _FILER_NUM = json.load(f)
        else:
            _FILER_NUM = {}

# ── Election results (lazy-loaded, used by /api/races) ───────────────────────
_ELECTION_RESULTS      = None
_ELECTION_RESULTS_LOCK = threading.Lock()

def _load_election_results():
    global _ELECTION_RESULTS
    with _ELECTION_RESULTS_LOCK:
        if _ELECTION_RESULTS is not None:
            return
        rpath = os.path.join(BASE_DIR, 'la_election_results.json')
        if os.path.exists(rpath):
            with open(rpath, 'r', encoding='utf-8') as f:
                _ELECTION_RESULTS = json.load(f)
        else:
            _ELECTION_RESULTS = {}

def _office_type(office):
    """Classify an office string into a broad category."""
    o = office.upper()
    # Federal — these report fundraising to the FEC, not the Board of Ethics,
    # so any finance figures attached downstream are state-office residue only.
    if 'PRESIDENT' in o and _re.search(
            r'UNITED STATES|U\.?\s*S\.?|ELECTOR|NOMINEE|PREFERENCE', o):
        return 'president'
    if _re.search(r'\bU\.?\s*S\.?\s+SENAT|UNITED STATES SENAT', o): return 'us_senate'
    if _re.search(r'\bU\.?\s*S\.?\s+REP|CONGRESS', o):               return 'us_house'
    if 'GOVERNOR' in o and 'LT' not in o and 'LIEUTENANT' not in o:  return 'governor'
    if 'LIEUTENANT GOVERNOR' in o or 'LT. GOVERNOR' in o:            return 'lt_governor'
    if 'STATE SENATOR' in o:                                          return 'state_senate'
    if 'STATE REPRESENTATIVE' in o:                                   return 'state_house'
    if 'SUPREME COURT' in o:                                          return 'supreme_court'
    if any(x in o for x in ['BESE','PUBLIC SERVICE','PSC']):          return 'board'
    if any(x in o for x in ['ATTORNEY GENERAL','SECRETARY OF STATE',
                             'TREASURER','COMMISSIONER OF']):          return 'statewide'
    if 'JUSTICE' in o or 'JUDGE' in o:                               return 'judicial'
    if any(x in o for x in ['SHERIFF','MAYOR','PARISH PRESIDENT','DISTRICT ATTORNEY',
                             'ASSESSOR','CLERK OF COURT']):             return 'local'
    return 'other'

def _cycle_for_year(yy):
    if yy >= 2024: return '2024-2027'
    if yy >= 2020: return '2020-2023'
    if yy >= 2016: return '2016-2019'
    if yy >= 2012: return '2012-2015'
    if yy >= 2008: return '2008-2011'
    if yy >= 2004: return '2004-2007'
    return '2000-2003'

def _date_key(d):
    """MM/DD/YYYY -> sortable 'YYYYMMDD' (empty string if unparseable)."""
    p = (d or '').split('/')
    if len(p) == 3 and len(p[2]) == 4:
        return f'{p[2]}{p[0]:>02}{p[1]:>02}'
    return ''

def _iso_date(d):
    """MM/DD/YYYY -> 'YYYY-MM-DD' (empty string if unparseable)."""
    p = (d or '').split('/')
    if len(p) == 3 and len(p[2]) == 4:
        return f'{p[2]}-{p[0].zfill(2)}-{p[1].zfill(2)}'
    return ''

# ── Unified entity search index (lazy-built once from career data) ────────────
_SEARCH_INDEX      = None   # list of {name, name_upper, is_candidate, total_raised, ...}
_SEARCH_INDEX_LOCK = threading.Lock()

def _build_search_index():
    """Merge SoS candidacies + Ethics finance index into one searchable list."""
    global _SEARCH_INDEX
    with _SEARCH_INDEX_LOCK:
        if _SEARCH_INDEX is not None:
            return
        _load_career_data()
        _load_filer_lookup()
        entries = {}

        # Everyone who has ever appeared on a ballot
        for name, cands in (_CAND_RACES or {}).items():
            real = [c for c in cands if not c.get('party_office')]
            last = max(real, key=lambda c: _date_key(c.get('date', ''))) if real else None
            entries[name] = {
                'name':         name,
                'is_candidate': True,
                'last_office':  last.get('office', '')  if last else '',
                'last_outcome': last.get('outcome', '') if last else '',
                'last_date':    last.get('date', '')    if last else '',
                'n_races':      len(real),
                'total_raised': 0.0,
                'n_cycles':     0,
            }

        # Anyone with Ethics finance records (adds committees/PACs + $ totals).
        # Net of committee-to-committee transfers, matching the dashboard's
        # headline Total Raised semantics.
        for name, idx in (_CAND_INDEX or {}).items():
            cycles = idx.get('cycles', {})
            total  = sum(c.get('raised', 0) - c.get('transfers_in', 0)
                         for c in cycles.values())
            e = entries.get(name)
            if e is None:
                e = entries[name] = {
                    'name': name, 'is_candidate': False,
                    'last_office': '', 'last_outcome': '', 'last_date': '',
                    'n_races': 0, 'total_raised': 0.0, 'n_cycles': 0,
                }
            e['total_raised'] = total
            e['n_cycles']     = len(cycles)

        for name, e in entries.items():
            e['name_upper']   = name.upper()
            e['filer_number'] = (_FILER_NUM or {}).get(_norm_name(name), '')

        _SEARCH_INDEX = list(entries.values())

def _load_donor_industries():
    global _DONOR_IND, _DONOR_IND_MTIME
    ind_path = os.path.join(BASE_DIR, 'la_donor_industries.json')
    current_mtime = os.path.getmtime(ind_path) if os.path.exists(ind_path) else 0
    with _DONOR_IND_LOCK:
        if _DONOR_IND is not None and current_mtime == _DONOR_IND_MTIME:
            return
        if os.path.exists(ind_path):
            with open(ind_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            _DONOR_IND = data.get('industries', data)  # handle both formats
            _DONOR_IND_MTIME = current_mtime
        else:
            _DONOR_IND = {}

def _norm_name(name):
    n = name.upper()
    n = _re.sub(r'\b(DR|MR|MRS|MS|JR|SR|II|III|IV|ESQ|PHD|MD)\.?\b', '', n)
    n = _re.sub(r'[^A-Z\s]', ' ', n)
    return ' '.join(n.split())

# ── Cross-dataset person name matching ──────────────────────────────────────
# A candidate's name differs across sources: SoS ballots carry nicknames and
# maiden names ("Eddie Rispone", "Liz Baker Murrill") while Ethics finance/COH
# use the formal name ("Edward L Rispone", "Elizabeth Murrill"). A
# (nickname-canonical first, last) key bridges them. Same curated nickname map
# the donor resolver (build_donor_entities.py) imports from here.
_NICK_GROUPS = [
    "ROBERT BOB BOBBY ROB ROBBIE", "WILLIAM BILL BILLY WILL WILLIE",
    "RICHARD RICK RICKY DICK RICH", "JAMES JIM JIMMY JIMMIE",
    "JOHN JOHNNY JACK JON", "EDWARD ED EDDIE EDDY NED",
    "GERALD GERARD JERRY JEROLD JERROLD", "MICHAEL MIKE MIKEY MICK",
    "CHARLES CHARLIE CHUCK CHAS", "THOMAS TOM TOMMY",
    "JOSEPH JOE JOEY", "DANIEL DAN DANNY", "DAVID DAVE",
    "RONALD RON RONNIE", "DONALD DON DONNIE", "KENNETH KEN KENNY",
    "ANTHONY TONY", "STEPHEN STEVEN STEVE STEVIE", "ANDREW ANDY DREW",
    "MATTHEW MATT", "CHRISTOPHER CHRIS", "NICHOLAS NICK",
    "BENJAMIN BEN BENNY", "SAMUEL SAM SAMMY", "TIMOTHY TIM",
    "PATRICK PAT", "FREDERICK FRED FREDDIE", "GREGORY GREG",
    "JEFFREY JEFF", "JONATHAN JON", "LAWRENCE LARRY",
    "RAYMOND RAY", "DOUGLAS DOUG", "PHILIP PHIL PHILLIP",
    "ALEXANDER ALEX", "EUGENE GENE", "VINCENT VINCE VINNIE",
    "FRANCIS FRANK FRANKIE", "ALBERT AL", "WALTER WALT",
    "HENRY HANK HARRY", "THEODORE TED TEDDY", "LEONARD LEN LENNY",
    "ELIZABETH LIZ BETH BETTY LIZZIE", "MARGARET MAGGIE MEG PEGGY MARGE",
    "KATHERINE KATHRYN KATHY KATE KATIE KAY", "PATRICIA PAT PATTY TRICIA",
    "JENNIFER JEN JENNY", "DEBORAH DEB DEBBIE", "BARBARA BARB",
    "SUSAN SUE SUSIE", "REBECCA BECKY", "VICTORIA VICKI VICKY",
    "CYNTHIA CINDY", "CHRISTINE CHRISTINA CHRIS TINA", "NICOLE NIKKI",
    "STEPHANIE STEPH", "JESSICA JESS", "PAMELA PAM", "SANDRA SANDY",
    "THADDEUS THAD", "THERESA TERESA TERRY TERRI",
]
_NICK = {}
for _grp in _NICK_GROUPS:
    _forms = _grp.split()
    for _f in _forms:
        _NICK.setdefault(_f, _forms[0])

def _name_key(name):
    """(_nick_first, last) identity for matching a person across datasets, or
    None if fewer than 2 name tokens. Drops middle/maiden tokens; _norm_name has
    already stripped honorifics and generational suffixes. A leading single-letter
    initial is skipped when a fuller name follows, so a formal "J Douglas Welborn"
    keys off DOUGLAS and meets the "Doug Welborn" ballot spelling."""
    toks = _norm_name(name).split()
    if len(toks) < 2:
        return None
    i = 1 if (len(toks[0]) == 1 and len(toks) >= 3) else 0
    return (_NICK.get(toks[i], toks[i]), toks[-1])

def _namekey_index(keys):
    """Map (nick_first, last) -> the single source key with that identity.
    Collisions (two distinct keys sharing an identity, e.g. ROBERT vs BOB JONES)
    are dropped so a fuzzy fallback never attaches the wrong person's money —
    exact-name matching still covers those cases."""
    idx, collided = {}, set()
    for k in keys:
        nk = _name_key(k)
        if not nk:
            continue
        if nk in idx and idx[nk] != k:
            collided.add(nk)
        else:
            idx[nk] = k
    for nk in collided:
        idx.pop(nk, None)
    return idx

def _career_index_path():
    """Freshest available candidate index: the nightly-rebuilt copy seeded into
    .la_cache/ from the data-cache release wins over the committed repo-root
    fallback when both exist and the cache copy is at least as new."""
    cache_p = os.path.join(CACHE_DIR, 'la_candidate_index.json.gz')
    repo_p  = os.path.join(BASE_DIR,  'la_candidate_index.json.gz')
    if os.path.exists(cache_p):
        if not os.path.exists(repo_p) or os.path.getmtime(cache_p) >= os.path.getmtime(repo_p):
            return cache_p
    return repo_p

def _filer_career_path():
    """Freshest la_filer_index.json.gz — same precedence rule as the name index
    (.la_cache copy from the nightly release wins over a committed repo-root
    fallback when at least as new). Built-time-only artifact; may be absent on a
    cold checkout, in which case the server falls back to name-keyed lookups."""
    cache_p = os.path.join(CACHE_DIR, 'la_filer_index.json.gz')
    repo_p  = os.path.join(BASE_DIR,  'la_filer_index.json.gz')
    if os.path.exists(cache_p):
        if not os.path.exists(repo_p) or os.path.getmtime(cache_p) >= os.path.getmtime(repo_p):
            return cache_p
    return repo_p

def _load_career_data():
    global _CAND_INDEX, _FILER_CAREER, _CAND_RACES, _CAND_IDX_MTIME
    idx_path   = _career_index_path()
    races_path = os.path.join(BASE_DIR, 'la_candidacies_raw.json.gz')
    # Check if file has changed since last load (allows hot-reload after rebuild)
    current_mtime = os.path.getmtime(idx_path) if os.path.exists(idx_path) else 0
    with _CAND_LOCK:
        if _CAND_INDEX is not None and current_mtime == _CAND_IDX_MTIME:
            return
        if os.path.exists(idx_path):
            with gzip.open(idx_path, 'rt', encoding='utf-8') as f:
                _CAND_INDEX = json.load(f)
            _CAND_IDX_MTIME = current_mtime
        else:
            _CAND_INDEX = {}
        # Filer-keyed twin (additive; reloaded in lockstep with the name index).
        filer_path = _filer_career_path()
        if os.path.exists(filer_path):
            with gzip.open(filer_path, 'rt', encoding='utf-8') as f:
                _FILER_CAREER = json.load(f)
        else:
            _FILER_CAREER = {}
        if os.path.exists(races_path):
            with gzip.open(races_path, 'rt', encoding='utf-8') as f:
                _CAND_RACES = json.load(f)
        else:
            _CAND_RACES = {}

# ── Shared API payload builders ────────────────────────────────────────────────
# One computation site per payload: the live endpoints AND the nightly static
# dump (build_static_api.py) call these, so the static artifacts served in
# server-less mode cannot drift from what the API would have said.

def _industry_years(cycle):
    """Cycle param ('YYYY-YYYY' or '') -> list of record years, or None if bad."""
    if cycle:
        try:
            y0, y1 = [int(p) for p in cycle.split('-')]
            return list(range(y0, y1 + 1))
        except Exception:
            return None
    return list(range(2000, date.today().year + 2))


def _classify_industry(rec):
    """Industry for one contribution record. Prefers the value baked onto the
    record by the nightly retag pass (normalized-name lookup); falls back to
    the raw-name lookup for records written before baking existed."""
    baked = rec.get('industry')
    if baked:
        return baked
    donor = (rec.get('contributor') or '').strip()
    return (_DONOR_IND.get(donor) if _DONOR_IND else None) or 'Other'


def _industry_bucket_add(bucket, rec):
    """Accumulate one record into a {totals, counts, top_donors} bucket."""
    amount = float(rec.get('amount') or 0)
    if amount <= 0:
        return
    industry = _classify_industry(rec)
    donor = (rec.get('contributor') or '').strip()
    bucket['totals'][industry] = bucket['totals'].get(industry, 0.0) + amount
    bucket['counts'][industry] = bucket['counts'].get(industry, 0) + 1
    bucket['top_donors'].setdefault(industry, []).append((amount, donor))


def _industry_bucket_payload(bucket):
    """Bucket -> sorted breakdown list (top-5 donors per industry)."""
    totals, counts, top_donors = bucket['totals'], bucket['counts'], bucket['top_donors']
    top5 = {ind: sorted(d, key=lambda x: -x[0])[:5] for ind, d in top_donors.items()}
    return [
        {
            'industry': ind,
            'total': round(totals[ind], 2),
            'count': counts[ind],
            'top_donors': [{'amount': a, 'name': n} for a, n in top5.get(ind, [])],
        }
        for ind in sorted(totals, key=lambda i: -totals[i])
    ]


def build_industry_breakdown(filer, cycle=''):
    """Per-filer industry breakdown — payload of /api/industry-breakdown."""
    _load_donor_industries()
    year_range = _industry_years(cycle)
    if year_range is None:
        return {'error': 'invalid cycle format; use YYYY-YYYY'}
    bucket = {'totals': {}, 'counts': {}, 'top_donors': {}}
    for yr in year_range:
        fp = os.path.join(CACHE_DIR, f'contributions_yr{yr}.json.gz')
        if not os.path.exists(fp):
            continue
        with gzip.open(fp, 'rt', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if str(rec.get('filerNumber', '')) != filer:
                    continue
                _industry_bucket_add(bucket, rec)
    return {
        'filer': filer,
        'cycle': cycle or 'all',
        'has_industry_data': bool(_DONOR_IND),
        'breakdown': _industry_bucket_payload(bucket),
    }


def build_races_payload(office_filter='major', year_filter=''):
    """All races grouped by (date, office) with candidate finance —
    payload of /api/races."""
    _load_career_data()
    _load_filer_lookup()
    _load_ethics_coh()

    office_filter = (office_filter or 'major').lower()
    year_filter   = (year_filter or '').strip()

    MAJOR_TYPES    = {'governor','statewide','lt_governor'}
    FEDERAL_TYPES  = {'us_senate','us_house','president'}
    STATE_TYPES    = {'state_senate','state_house'}
    BOARD_TYPES    = {'board','supreme_court'}
    JUDICIAL_TYPES = {'judicial'}
    LOCAL_TYPES    = {'local'}

    races_by_key = {}

    # Nickname/maiden-name-aware fallback indexes: SoS ballot names ("Eddie
    # Rispone", "Liz Baker Murrill") often differ from the Ethics finance/COH
    # spelling ("Edward L Rispone", "Elizabeth Murrill"). Built once per call;
    # collision-guarded so we never attach the wrong person's figures.
    ci_idx  = _namekey_index(_CAND_INDEX.keys()) if _CAND_INDEX else {}
    coh_idx = _namekey_index(_ETHICS_COH.keys()) if _ETHICS_COH else {}
    fn_idx  = _namekey_index(_FILER_NUM.keys())  if _FILER_NUM  else {}

    if _CAND_RACES:
        for cand_name, candidacies in _CAND_RACES.items():
            for c in candidacies:
                if c.get('party_office'):
                    continue
                race_date = c.get('date', '')
                office    = c.get('office', '')
                if not race_date or not office:
                    continue
                parts = race_date.split('/')
                try:
                    yr = int(parts[2]) if len(parts) == 3 else 0
                except Exception:
                    yr = 0

                otype = _office_type(office)

                # Apply office type filter
                if office_filter == 'major':
                    if otype not in MAJOR_TYPES: continue
                elif office_filter == 'federal':
                    if otype not in FEDERAL_TYPES: continue
                elif office_filter == 'state':
                    if otype not in STATE_TYPES: continue
                elif office_filter == 'board':
                    if otype not in BOARD_TYPES: continue
                elif office_filter == 'judicial':
                    if otype not in JUDICIAL_TYPES: continue
                elif office_filter == 'local':
                    if otype not in LOCAL_TYPES: continue
                elif office_filter != 'all':
                    # treat as a specific office_type string
                    if otype != office_filter: continue

                if year_filter and str(yr) != year_filter:
                    continue

                key = (race_date, office)
                if key not in races_by_key:
                    races_by_key[key] = {
                        'date': race_date,
                        'year': yr,
                        'office': office,
                        'office_type': otype,
                        'candidates': [],
                    }

                # Match to Ethics finance data. Try exact name first, then the
                # nickname/maiden-name-aware (first,last) fallback so SoS ballot
                # spellings resolve to the formal finance/COH record.
                norm_name = _norm_name(cand_name)
                nk = _name_key(cand_name)
                filer_num = (_FILER_NUM.get(norm_name) or
                             _FILER_NUM.get(cand_name.upper(), ''))
                if not filer_num and nk and nk in fn_idx:
                    filer_num = _FILER_NUM.get(fn_idx[nk], '')
                cycle_key = _cycle_for_year(yr)
                raised = spent = borrowed = 0
                ci_key = (cand_name if (_CAND_INDEX and cand_name in _CAND_INDEX)
                          else (ci_idx.get(nk) if nk else None))
                if ci_key:
                    cyc = _CAND_INDEX[ci_key].get('cycles', {}).get(cycle_key, {})
                    raised   = cyc.get('raised', 0)
                    spent    = cyc.get('spent',  0)
                    borrowed = cyc.get('borrowed', 0)   # self-loans: matters for
                                                        # self-funders (Rispone)

                # Certified COH from ethics annual filing (same fallback)
                coh_entry   = _get_ethics_coh(cand_name)
                if not coh_entry and nk and nk in coh_idx:
                    coh_entry = _ETHICS_COH.get(coh_idx[nk])
                coh_ending  = coh_entry.get('ending_coh')  if coh_entry else None
                coh_year    = coh_entry.get('report_year') if coh_entry else None
                coh_pdf_url = coh_entry.get('pdf_url')     if coh_entry else None
                coh_report_id = coh_entry.get('report_id') if coh_entry else None

                races_by_key[key]['candidates'].append({
                    'name':         cand_name,
                    'party':        c.get('party', ''),
                    'outcome':      c.get('outcome', ''),
                    'vote_pct':     c.get('vote_pct'),
                    'filer_number': filer_num,
                    'cycle':        cycle_key,
                    'raised':       raised,
                    'spent':        spent,
                    'borrowed':     borrowed,
                    'coh_ending':   coh_ending,
                    'coh_year':     coh_year,
                    'coh_pdf_url':  coh_pdf_url,
                    'coh_report_id': coh_report_id,
                })

    race_list = sorted(races_by_key.values(),
                       key=lambda r: r['date'], reverse=True)
    for race in race_list:
        race['candidates'].sort(
            key=lambda c: (c.get('vote_pct') or 0), reverse=True)

    all_years = sorted({r['year'] for r in race_list if r['year'] > 0},
                       reverse=True)
    return {'races': race_list, 'total': len(race_list), 'years': all_years}


def build_candidate_history_payload(name, filer=''):
    """Career profile for one person — financial index + SoS race list +
    certified COH — payload of /api/candidate-history.

    A filer number (from a search result / entity) pins exact identity via the
    filer-keyed index, so two people sharing a normalized name never merge.
    Without one, the lookup falls back through the name-keyed index: exact
    normalized name, then first+last, then the nickname/maiden-name key — the
    same cross-dataset bridge build_races_payload uses, so a SoS ballot spelling
    ("Liz Baker Murrill") and the formal finance/COH spelling ("Elizabeth
    Murrill") resolve to the same career, election history, and certified COH.
    Without it the profile could silently miss this filer's records whenever it
    is reached by the variant spelling (e.g. a #/campaign/<name> permalink).
    """
    _load_career_data()
    _load_ethics_coh()
    name  = (name or '').strip()
    filer = (filer or '').strip()
    norm  = _norm_name(name)
    toks  = norm.split()
    nk      = _name_key(name)
    ci_idx  = _namekey_index(_CAND_INDEX.keys())  if _CAND_INDEX  else {}
    cr_idx  = _namekey_index(_CAND_RACES.keys())  if _CAND_RACES  else {}
    coh_idx = _namekey_index(_ETHICS_COH.keys())  if _ETHICS_COH  else {}
    financial = ((_FILER_CAREER.get(filer) if filer else None) or
                 _CAND_INDEX.get(norm) or
                 (_CAND_INDEX.get(f'{toks[0]} {toks[-1]}') if len(toks) >= 2 else None) or
                 (_CAND_INDEX.get(ci_idx[nk]) if nk and nk in ci_idx else None) or
                 {})
    races_raw = (_CAND_RACES.get(norm) or
                 (_CAND_RACES.get(f'{toks[0]} {toks[-1]}') if len(toks) >= 2 else None) or
                 (_CAND_RACES.get(cr_idx[nk]) if nk and nk in cr_idx else None) or
                 [])

    # Sort races by date, exclude party-committee offices
    def _is_party_office(o):
        o = o.upper()
        return any(x in o for x in ('DSCC','RSCC','DPEC','RPEC',
                   'CENTRAL COMMITTEE','EXECUTIVE COMMITTEE','PARTY COMMITTEE'))
    races = sorted(
        [r for r in races_raw if not _is_party_office(r.get('office',''))],
        key=lambda r: r.get('date','')
    )

    # Certified COH from ethics_coh_cache.json (same nickname/maiden fallback).
    ethics_coh = _get_ethics_coh(name)
    if not ethics_coh and nk and nk in coh_idx:
        ethics_coh = _ETHICS_COH.get(coh_idx[nk])

    # Hybrid live-cash estimate: certified Dec-31 balance + net flows since.
    # Flows come from the index's monthly buckets — the same aggregation as every
    # other number on the profile. Buckets hold contributions in / expenditures
    # out only; loans excluded (noted in the UI tooltip).
    coh_estimate = None
    if ethics_coh and ethics_coh.get('ending_coh') is not None:
        try:
            base_year = int(ethics_coh.get('report_year'))
        except (TypeError, ValueError):
            base_year = None
        if base_year and base_year < date.today().year:
            cutoff = f'{base_year}-12'   # filing certifies through Dec 31
            raised_since = spent_since = 0.0
            for mk, flows in (financial.get('monthly') or {}).items():
                if mk > cutoff:
                    raised_since += flows.get('in', 0) or 0
                    spent_since  += flows.get('out', 0) or 0
            coh_estimate = {
                'base':         ethics_coh['ending_coh'],
                'base_year':    base_year,
                'raised_since': round(raised_since, 2),
                'spent_since':  round(spent_since, 2),
                'estimate':     round(ethics_coh['ending_coh'] + raised_since - spent_since, 2),
            }

    return {
        'financial':    financial,
        'races':        races,
        'norm':         norm,
        'ethics_coh':   ethics_coh,   # None if not yet scraped
        'coh_estimate': coh_estimate, # None unless certified base exists
        'entity':       _get_entity(filer=filer or None, name=name),  # canonical entity or None
    }


def build_overview_payload():
    """Landing-page aggregate stats — payload of /api/overview."""
    _build_search_index()
    today_iso = time.strftime('%Y-%m-%d')

    total_raised = sum(e['total_raised'] for e in _SEARCH_INDEX)
    n_candidates = sum(1 for e in _SEARCH_INDEX if e['is_candidate'] and e['n_cycles'] > 0)
    n_committees = sum(1 for e in _SEARCH_INDEX if not e['is_candidate'])

    # Elections: races (distinct offices) + candidate counts per date
    date_races, date_cands = {}, {}
    for name, cands in (_CAND_RACES or {}).items():
        for c in cands:
            if c.get('party_office'):
                continue
            d = c.get('date', '')
            if not d:
                continue
            date_races.setdefault(d, set()).add(c.get('office', ''))
            date_cands[d] = date_cands.get(d, 0) + 1

    elections = []
    for d in date_races:
        iso = _iso_date(d)
        elections.append({
            'date':       d,
            'iso':        iso,
            'n_races':    len(date_races[d]),
            'n_cands':    date_cands.get(d, 0),
            'upcoming':   bool(iso and iso > today_iso),
            '_k':         _date_key(d),
        })
    elections.sort(key=lambda x: x['_k'], reverse=True)
    upcoming = [e for e in elections if e['upcoming']]
    upcoming.sort(key=lambda x: x['_k'])   # soonest first
    recent   = [e for e in elections if not e['upcoming']][:8]
    for e in elections:
        e.pop('_k', None)

    years = sorted({d.split('/')[-1] for d in date_races if len(d.split('/')) == 3},
                   reverse=True)

    top = sorted(_SEARCH_INDEX, key=lambda e: -e['total_raised'])[:12]
    top_fundraisers = [{
        'name':         e['name'],
        'total_raised': e['total_raised'],
        'filer_number': e['filer_number'],
        'last_office':  e['last_office'],
        'is_candidate': e['is_candidate'],
    } for e in top]

    return {
        'total_raised':    total_raised,
        'n_candidates':    n_candidates,
        'n_committees':    n_committees,
        'n_elections':     len(date_races),
        'years':           years,
        'upcoming_elections': upcoming[:6],
        'recent_elections':   recent,
        'top_fundraisers':    top_fundraisers,
    }


def build_search_entries():
    """The full search index entries (with name_upper) — the static client
    runs the same two-tier ranking over this dump that /api/search runs
    server-side."""
    _build_search_index()
    return _SEARCH_INDEX


# ── Download status tracker ────────────────────────────────────────────────────
# Maps cache_key -> {'status': 'idle'|'downloading'|'ready'|'error', 'message': str}
_dl_status: dict = {}
_dl_status_lock = threading.Lock()

def set_status(key, status, message=''):
    with _dl_status_lock:
        _dl_status[key] = {'status': status, 'message': message, 'ts': time.time()}

def get_status(key):
    with _dl_status_lock:
        return _dl_status.get(key, {'status': 'idle', 'message': ''})

# ── CSV source URLs ────────────────────────────────────────────────────────────
CSV_URLS = {
    '2024-2027': 'https://www.ethics.la.gov/Pub/CampFinan/DataDownload/ContributionReports/Contributions_2024_to_2027.csv',
    '2020-2023': 'https://www.ethics.la.gov/Pub/CampFinan/DataDownload/ContributionReports/Contributions_2020_to_2023.csv',
    '2016-2019': 'https://www.ethics.la.gov/Pub/CampFinan/DataDownload/ContributionReports/Contributions_2016_to_2019.csv',
    '2012-2015': 'https://www.ethics.la.gov/Pub/CampFinan/DataDownload/ContributionReports/Contributions_2012_to_2015.csv',
    '2008-2011': 'https://www.ethics.la.gov/Pub/CampFinan/DataDownload/ContributionReports/Contributions_2008_to_2011.csv',
    '2004-2007': 'https://www.ethics.la.gov/Pub/CampFinan/DataDownload/ContributionReports/Contributions_2004_to_2007.csv',
    '2000-2003': 'https://www.ethics.la.gov/Pub/CampFinan/DataDownload/ContributionReports/Contributions_2000_to_2003.csv',
}

EXPENDITURE_URLS = {
    '2024-2027': 'https://www.ethics.la.gov/Pub/CampFinan/DataDownload/ExpenditureReports/Expenditures_2024_to_2027.csv',
    '2020-2023': 'https://www.ethics.la.gov/Pub/CampFinan/DataDownload/ExpenditureReports/Expenditures_2020_to_2023.csv',
    '2016-2019': 'https://www.ethics.la.gov/Pub/CampFinan/DataDownload/ExpenditureReports/Expenditures_2016_to_2019.csv',
    '2012-2015': 'https://www.ethics.la.gov/Pub/CampFinan/DataDownload/ExpenditureReports/Expenditures_2012_to_2015.csv',
    '2008-2011': 'https://www.ethics.la.gov/Pub/CampFinan/DataDownload/ExpenditureReports/Expenditures_2008_to_2011.csv',
    '2004-2007': 'https://www.ethics.la.gov/Pub/CampFinan/DataDownload/ExpenditureReports/Expenditures_2004_to_2007.csv',
    '2000-2003': 'https://www.ethics.la.gov/Pub/CampFinan/DataDownload/ExpenditureReports/Expenditures_2000_to_2003.csv',
}

LOAN_URLS = {
    '2024-2027': 'https://www.ethics.la.gov/Pub/CampFinan/DataDownload/LoanReports/Loans_2024_to_2027.csv',
    '2020-2023': 'https://www.ethics.la.gov/Pub/CampFinan/DataDownload/LoanReports/Loans_2020_to_2023.csv',
    '2016-2019': 'https://www.ethics.la.gov/Pub/CampFinan/DataDownload/LoanReports/Loans_2016_to_2019.csv',
    '2012-2015': 'https://www.ethics.la.gov/Pub/CampFinan/DataDownload/LoanReports/Loans_2012_to_2015.csv',
    '2008-2011': 'https://www.ethics.la.gov/Pub/CampFinan/DataDownload/LoanReports/Loans_2008_to_2011.csv',
    '2004-2007': 'https://www.ethics.la.gov/Pub/CampFinan/DataDownload/LoanReports/Loans_2004_to_2007.csv',
    '2000-2003': 'https://www.ethics.la.gov/Pub/CampFinan/DataDownload/LoanReports/Loans_2000_to_2003.csv',
}

def get_csv_key(year):
    y = int(year)
    if y >= 2024: return '2024-2027'
    if y >= 2020: return '2020-2023'
    if y >= 2016: return '2016-2019'
    if y >= 2012: return '2012-2015'
    if y >= 2008: return '2008-2011'
    if y >= 2004: return '2004-2007'
    return '2000-2003'

# ── ZIP-code prefix → US state abbreviation ──────────────────────────────────
# Keys are the first 3 digits of a ZIP code (string).  Coverage: all 50 states
# + DC, PR, VI, GU, and military APO/FPO codes.
_ZIP3_STATE: dict = {}
def _build_zip3_state():
    """Populate _ZIP3_STATE from compact range specs."""
    global _ZIP3_STATE
    # Each entry: (start_prefix, end_prefix_inclusive, state_abbr)
    ranges = [
        (6,9,'PR'),(10,27,'MA'),(28,29,'RI'),(30,38,'NH'),(39,49,'ME'),
        (50,59,'VT'),(60,69,'CT'),(70,89,'NJ'),(90,98,'AE'),(99,99,'AE'),
        (100,149,'NY'),(150,196,'PA'),(197,199,'DE'),
        (200,205,'DC'),(206,206,'MD'),(207,212,'MD'),(214,219,'MD'),
        (201,201,'VA'),(220,246,'VA'),(247,268,'WV'),
        (270,289,'NC'),(290,299,'SC'),(300,319,'GA'),
        (320,339,'FL'),(340,340,'AA'),(341,349,'FL'),
        (350,368,'AL'),(369,369,'AL'),
        (370,385,'TN'),(386,397,'MS'),(398,399,'GA'),
        (400,427,'KY'),(430,458,'OH'),(460,479,'IN'),(480,499,'MI'),
        (500,528,'IA'),(530,549,'WI'),(550,567,'MN'),
        (570,577,'SD'),(580,588,'ND'),(590,599,'MT'),
        (600,629,'IL'),(630,658,'MO'),(660,679,'KS'),(680,693,'NE'),
        (700,708,'LA'),(710,714,'LA'),
        (716,729,'AR'),(730,749,'OK'),(750,799,'TX'),(885,885,'TX'),
        (800,816,'CO'),(820,831,'WY'),(832,838,'ID'),(840,847,'UT'),
        (850,865,'AZ'),(870,884,'NM'),
        (889,898,'NV'),
        (900,961,'CA'),(962,964,'AP'),(965,965,'GU'),(966,966,'GU'),
        (967,968,'HI'),(969,969,'GU'),
        (970,979,'OR'),(980,994,'WA'),(995,999,'AK'),
    ]
    for lo, hi, st in ranges:
        for n in range(lo, hi + 1):
            _ZIP3_STATE[f'{n:03d}'] = st
_build_zip3_state()

def _zip_to_state(zip_code: str) -> str:
    """Return a 2-letter state abbreviation for a ZIP code, or '' if unknown.

    Only genuine 5-digit (or 9-digit ZIP+4) codes resolve. A malformed length —
    e.g. a 4-digit '1934' from a data-entry error — must NOT be mapped from its
    truncated prefix, which would invent a wrong state (193 -> PA) for an
    obviously-local donor. Better to return unknown and let the city decide."""
    z = (zip_code or '').strip().replace('-', '')
    if len(z) in (5, 9) and z[:5].isdigit():
        return _ZIP3_STATE.get(z[:3], '')
    return ''

# ── City → Parish lookup ──────────────────────────────────────────────────────
CITY_TO_PARISH = {
    'NEW ORLEANS': 'Orleans', 'BATON ROUGE': 'East Baton Rouge',
    'SHREVEPORT': 'Caddo', 'METAIRIE': 'Jefferson', 'BOSSIER CITY': 'Bossier',
    'KENNER': 'Jefferson', 'LAFAYETTE': 'Lafayette', 'LAKE CHARLES': 'Calcasieu',
    'MONROE': 'Ouachita', 'WEST MONROE': 'Ouachita', 'ALEXANDRIA': 'Rapides',
    'HOUMA': 'Terrebonne', 'MANDEVILLE': 'St. Tammany', 'SLIDELL': 'St. Tammany',
    'COVINGTON': 'St. Tammany', 'HAMMOND': 'Tangipahoa', 'PRAIRIEVILLE': 'Ascension',
    'GONZALES': 'Ascension', 'DONALDSONVILLE': 'Ascension',
    'DENHAM SPRINGS': 'Livingston', 'WALKER': 'Livingston',
    'ZACHARY': 'East Baton Rouge', 'BAKER': 'East Baton Rouge',
    'PORT ALLEN': 'West Baton Rouge', 'BRUSLY': 'West Baton Rouge',
    'PLAQUEMINE': 'Iberville', 'WHITE CASTLE': 'Iberville',
    'NEW ROADS': 'Pointe Coupee', 'GRETNA': 'Jefferson', 'MARRERO': 'Jefferson',
    'HARVEY': 'Jefferson', 'WESTWEGO': 'Jefferson', 'HARAHAN': 'Jefferson',
    'RIVER RIDGE': 'Jefferson', 'TERRYTOWN': 'Jefferson', 'AVONDALE': 'Jefferson',
    'CHALMETTE': 'St. Bernard', 'ARABI': 'St. Bernard', 'MERAUX': 'St. Bernard',
    'BELLE CHASSE': 'Plaquemines', 'EMPIRE': 'Plaquemines',
    'SULPHUR': 'Calcasieu', 'WESTLAKE': 'Calcasieu', 'IOWA': 'Calcasieu',
    'DEQUINCY': 'Calcasieu', 'DERIDDER': 'Beauregard', 'LEESVILLE': 'Vernon',
    'NATCHITOCHES': 'Natchitoches', 'MANY': 'Sabine', 'MANSFIELD': 'De Soto',
    'MINDEN': 'Webster', 'SPRINGHILL': 'Webster', 'ARCADIA': 'Bienville',
    'HOMER': 'Claiborne', 'HAYNESVILLE': 'Claiborne', 'RUSTON': 'Lincoln',
    'GRAMBLING': 'Lincoln', 'FARMERVILLE': 'Union', 'BASTROP': 'Morehouse',
    'TALLULAH': 'Madison', 'DELHI': 'Richland', 'RAYVILLE': 'Richland',
    'WINNSBORO': 'Franklin', 'COLUMBIA': 'Caldwell', 'MARKSVILLE': 'Avoyelles',
    'BUNKIE': 'Avoyelles', 'COTTONPORT': 'Avoyelles', 'JENA': 'LaSalle',
    'VIDALIA': 'Concordia', 'FERRIDAY': 'Concordia', 'JONESVILLE': 'Catahoula',
    'JONESBORO': 'Jackson', 'WINNFIELD': 'Winn', 'COUSHATTA': 'Red River',
    'BOGALUSA': 'Washington', 'FRANKLINTON': 'Washington', 'AMITE': 'Tangipahoa',
    'PONCHATOULA': 'Tangipahoa', 'GREENSBURG': 'St. Helena',
    'CLINTON': 'East Feliciana', 'ST. FRANCISVILLE': 'West Feliciana',
    'SAINT FRANCISVILLE': 'West Feliciana', 'THIBODAUX': 'Lafourche',
    'CUT OFF': 'Lafourche', 'LOCKPORT': 'Lafourche', 'GOLDEN MEADOW': 'Lafourche',
    'RACELAND': 'Lafourche', 'GALLIANO': 'Lafourche', 'LAROSE': 'Lafourche',
    'MORGAN CITY': 'St. Mary', 'BERWICK': 'St. Mary', 'FRANKLIN': 'St. Mary',
    'PATTERSON': 'St. Mary', 'CENTERVILLE': 'St. Mary',
    'NEW IBERIA': 'Iberia', 'JEANERETTE': 'Iberia', 'DELCAMBRE': 'Iberia',
    'BREAUX BRIDGE': 'St. Martin', 'ST. MARTINVILLE': 'St. Martin',
    'SAINT MARTINVILLE': 'St. Martin', 'HENDERSON': 'St. Martin',
    'OPELOUSAS': 'St. Landry', 'EUNICE': 'St. Landry', 'PORT BARRE': 'St. Landry',
    'ABBEVILLE': 'Vermilion', 'KAPLAN': 'Vermilion', 'GUEYDAN': 'Vermilion',
    'CROWLEY': 'Acadia', 'RAYNE': 'Acadia', 'CHURCH POINT': 'Acadia',
    'VILLE PLATTE': 'Evangeline', 'MAMOU': 'Evangeline', 'BASILE': 'Evangeline',
    'JENNINGS': 'Jefferson Davis', 'WELSH': 'Jefferson Davis', 'LAKE ARTHUR': 'Jefferson Davis',
    'LAPLACE': 'St. John the Baptist', 'RESERVE': 'St. John the Baptist',
    'DESTREHAN': 'St. Charles', 'LULING': 'St. Charles', 'BOUTTE': 'St. Charles',
    'HAHNVILLE': 'St. Charles', 'PARADIS': 'St. Charles',
    'VACHERIE': 'St. James', 'CONVENT': 'St. James', 'GRAMERCY': 'St. James',
    'LUTCHER': 'St. James', 'PAULINA': 'St. James',
    'GRAY': 'Terrebonne', 'BAYOU CANE': 'Terrebonne', 'SCHRIEVER': 'Terrebonne',
    'THIBODAUX': 'Lafourche', 'NAPOLEONVILLE': 'Assumption', 'BELLE ROSE': 'Assumption',
    # ── Previously unmapped parishes ─────────────────────────────────────────
    # West Carroll
    'OAK GROVE': 'West Carroll',
    # East Carroll
    'LAKE PROVIDENCE': 'East Carroll',
    # Allen
    'OBERLIN': 'Allen', 'KINDER': 'Allen', 'OAKDALE': 'Allen',
    # Grant
    'COLFAX': 'Grant', 'POLLOCK': 'Grant', 'DRY PRONG': 'Grant',
    # Tensas
    'ST. JOSEPH': 'Tensas', 'SAINT JOSEPH': 'Tensas', 'NEWELLTON': 'Tensas',
    # Cameron
    'CAMERON': 'Cameron', 'HACKBERRY': 'Cameron', 'GRAND CHENIER': 'Cameron',
    'GRAND LAKE': 'Cameron', 'JOHNSON BAYOU': 'Cameron',
}

# ── ZIP-to-parish fallback ────────────────────────────────────────────────────
# When a contributor's city isn't in CITY_TO_PARISH, try their 5-digit ZIP code.
# Two levels: ZIP5 exact match → ZIP3 prefix regional heuristic.
# Only applies to Louisiana ZIPs (we already have _ZIP3_STATE to verify).

_ZIP5_PARISH: dict = {
    # ── Orleans Parish ────────────────────────────────────────────────────────
    **{z: 'Orleans' for z in [
        '70112','70113','70114','70115','70116','70117','70118','70119',
        '70122','70124','70125','70126','70127','70128','70129','70130','70131',
        '70139','70140','70141','70142','70143','70145','70146','70148',
        '70150','70151','70152','70153','70154','70156','70157','70158',
        '70159','70160','70161','70162','70163','70164','70165','70166',
        '70167','70170','70172','70174','70175','70176','70177','70178',
        '70179','70182','70183','70184','70185','70186','70187','70189',
        '70190','70195',
    ]},
    # ── Jefferson Parish ─────────────────────────────────────────────────────
    **{z: 'Jefferson' for z in [
        '70001','70002','70003','70004','70005','70006',
        '70053','70056','70058','70060','70062','70063','70064','70065',
        '70072','70094','70096','70123',
    ]},
    # ── St. Bernard Parish ────────────────────────────────────────────────────
    **{z: 'St. Bernard' for z in ['70043','70044','70075','70085','70086','70092']},
    # ── Plaquemines Parish ────────────────────────────────────────────────────
    **{z: 'Plaquemines' for z in ['70037','70038','70039','70040','70041','70050','70052','70082','70083','70084','70091']},
    # ── St. Charles Parish ────────────────────────────────────────────────────
    **{z: 'St. Charles' for z in ['70030','70031','70032','70047','70049','70057','70068','70069','70087']},
    # ── St. John the Baptist ──────────────────────────────────────────────────
    **{z: 'St. John the Baptist' for z in ['70068','70076','70090']},
    # ── St. James Parish ─────────────────────────────────────────────────────
    **{z: 'St. James' for z in ['70070','70071','70079','70086','70090']},
    # ── Lafourche Parish ─────────────────────────────────────────────────────
    **{z: 'Lafourche' for z in [
        '70301','70302','70310','70339','70341','70343','70344','70345',
        '70346','70354','70357','70358','70359','70360','70361','70363',
        '70364','70373','70374','70375',
    ]},
    # ── Terrebonne Parish ────────────────────────────────────────────────────
    **{z: 'Terrebonne' for z in [
        '70310','70360','70361','70363','70364','70365','70395','70397',
    ]},
    # ── Assumption Parish ────────────────────────────────────────────────────
    **{z: 'Assumption' for z in ['70339','70380','70390','70391','70392','70393','70394']},
    # ── Ascension Parish ─────────────────────────────────────────────────────
    **{z: 'Ascension' for z in ['70346','70706','70711','70734','70769','70774','70778']},
    # ── Iberville Parish ─────────────────────────────────────────────────────
    **{z: 'Iberville' for z in ['70764','70767','70770','70784']},
    # ── West Baton Rouge ─────────────────────────────────────────────────────
    **{z: 'West Baton Rouge' for z in ['70714','70719','70736','70737','70760','70767']},
    # ── East Baton Rouge ─────────────────────────────────────────────────────
    **{z: 'East Baton Rouge' for z in [
        '70801','70802','70803','70804','70805','70806','70807','70808',
        '70809','70810','70811','70812','70813','70814','70815','70816',
        '70817','70818','70819','70820','70821','70822','70823','70825',
        '70826','70827','70831','70833','70835','70836','70837','70873',
        '70874','70879','70883','70884','70891','70892','70893','70894',
        '70895','70896','70898',
    ]},
    # ── Livingston Parish ────────────────────────────────────────────────────
    **{z: 'Livingston' for z in ['70422','70706','70707','70726','70733','70754','70785']},
    # ── St. Helena Parish ────────────────────────────────────────────────────
    **{z: 'St. Helena' for z in ['70422','70441','70443','70462']},
    # ── Tangipahoa Parish ────────────────────────────────────────────────────
    **{z: 'Tangipahoa' for z in [
        '70401','70402','70403','70404','70420','70421','70422',
        '70431','70436','70444','70450','70451','70452','70453',
        '70454','70455','70456','70466',
    ]},
    # ── Washington Parish ────────────────────────────────────────────────────
    **{z: 'Washington' for z in [
        '70427','70431','70438','70449','70456','70463',
    ]},
    # ── St. Tammany Parish ────────────────────────────────────────────────────
    **{z: 'St. Tammany' for z in [
        '70420','70428','70429','70433','70434','70435','70437',
        '70445','70446','70447','70448','70458','70459','70460',
        '70461','70464','70465','70466','70471',
    ]},
    # ── East Feliciana Parish ─────────────────────────────────────────────────
    **{z: 'East Feliciana' for z in ['70722','70730','70732','70748','70775']},
    # ── West Feliciana Parish ─────────────────────────────────────────────────
    **{z: 'West Feliciana' for z in ['70775']},
    # ── Pointe Coupee Parish ──────────────────────────────────────────────────
    **{z: 'Pointe Coupee' for z in ['70760','70762','70763','70764','70773','70782','70783']},
    # ── St. Landry Parish ────────────────────────────────────────────────────
    **{z: 'St. Landry' for z in [
        '70535','70570','70571','70576','70577','70578','70582','70584',
        '70589',
    ]},
    # ── Evangeline Parish ────────────────────────────────────────────────────
    **{z: 'Evangeline' for z in ['70512','70524','70526','70531','70535','70546','70557','70558','70586']},
    # ── Lafayette Parish ─────────────────────────────────────────────────────
    **{z: 'Lafayette' for z in [
        '70501','70502','70503','70504','70505','70506','70507','70508',
        '70509','70593','70598',
    ]},
    # ── St. Martin Parish ────────────────────────────────────────────────────
    **{z: 'St. Martin' for z in [
        '70512','70513','70514','70516','70517','70518','70519',
        '70520','70544','70563','70582',
    ]},
    # ── Iberia Parish ────────────────────────────────────────────────────────
    **{z: 'Iberia' for z in ['70513','70560','70561','70562','70563','70569']},
    # ── Vermilion Parish ─────────────────────────────────────────────────────
    **{z: 'Vermilion' for z in [
        '70510','70514','70515','70538','70540','70542','70543','70550',
        '70551','70552','70575','70591',
    ]},
    # ── Acadia Parish ────────────────────────────────────────────────────────
    **{z: 'Acadia' for z in [
        '70522','70525','70526','70528','70531','70532','70533','70534',
        '70541','70554','70555','70556','70578','70585','70586','70589',
    ]},
    # ── Jefferson Davis Parish ────────────────────────────────────────────────
    **{z: 'Jefferson Davis' for z in [
        '70544','70630','70638','70640','70641','70644','70647','70648',
        '70651','70652','70653','70656',
    ]},
    # ── Calcasieu Parish ─────────────────────────────────────────────────────
    **{z: 'Calcasieu' for z in [
        '70601','70602','70603','70604','70605','70606','70607','70609',
        '70611','70612','70615','70616','70629','70632','70633','70634',
        '70637','70648','70655','70661','70663','70664','70665','70669',
    ]},
    # ── Beauregard Parish ────────────────────────────────────────────────────
    **{z: 'Beauregard' for z in ['70615','70630','70634','70637','70638','70644','70654','70656','70661']},
    # ── Allen Parish ─────────────────────────────────────────────────────────
    **{z: 'Allen' for z in ['70644','70648','70655','70656','71463']},
    # ── Cameron Parish ────────────────────────────────────────────────────────
    **{z: 'Cameron' for z in ['70607','70631','70632','70633','70643','70645']},
    # ── Vernon Parish ────────────────────────────────────────────────────────
    **{z: 'Vernon' for z in [
        '71446','71447','71448','71449','71459','71461','71462','71463','71465',
    ]},
    # ── Sabine Parish ────────────────────────────────────────────────────────
    **{z: 'Sabine' for z in ['71424','71430','71449','71452','71460','71462','71463','71469','71486']},
    # ── Natchitoches Parish ────────────────────────────────────────────────────
    **{z: 'Natchitoches' for z in [
        '71406','71414','71415','71418','71419','71424','71425','71426',
        '71427','71447','71449','71451','71452','71454','71455','71456',
        '71457','71458','71459','71460','71461','71462','71463','71467',
        '71468','71469','71472','71479','71480','71484','71485','71486',
    ]},
    # ── De Soto Parish ────────────────────────────────────────────────────────
    **{z: 'De Soto' for z in [
        '71023','71024','71030','71039','71043','71046','71052','71060',
        '71061','71070','71072','71073','71078','71082',
    ]},
    # ── Red River Parish ─────────────────────────────────────────────────────
    **{z: 'Red River' for z in ['71019','71027','71039','71043','71046','71052','71063','71064','71065','71082']},
    # ── Caddo Parish ─────────────────────────────────────────────────────────
    **{z: 'Caddo' for z in [
        '71001','71003','71004','71007','71008','71009','71016','71023',
        '71024','71027','71028','71029','71033','71037','71038','71039',
        '71040','71043','71044','71046','71047','71052','71055','71060',
        '71061','71064','71065','71066','71067','71068','71070','71071',
        '71072','71073','71074','71075','71078','71080','71082','71101',
        '71102','71103','71104','71105','71106','71107','71108','71109',
        '71110','71115','71118','71119','71120','71129','71130','71133',
        '71134','71135','71136','71137','71138','71148','71149','71150',
        '71151','71152','71153','71154','71156','71161','71162','71163',
        '71164','71165','71166','71171','71172',
    ]},
    # ── Bossier Parish ────────────────────────────────────────────────────────
    **{z: 'Bossier' for z in [
        '71006','71007','71008','71009','71021','71029','71037','71043',
        '71044','71047','71053','71054','71063','71065','71066','71067',
        '71068','71069','71078','71111','71112','71113','71114','71171',
    ]},
    # ── Webster Parish ────────────────────────────────────────────────────────
    **{z: 'Webster' for z in ['71021','71031','71045','71047','71055','71075','71079','71080']},
    # ── Claiborne Parish ─────────────────────────────────────────────────────
    **{z: 'Claiborne' for z in ['71028','71034','71040','71043','71044','71052','71067']},
    # ── Bienville Parish ─────────────────────────────────────────────────────
    **{z: 'Bienville' for z in ['71001','71016','71025','71031','71046','71052','71064','71065','71082']},
    # ── Jackson Parish ────────────────────────────────────────────────────────
    **{z: 'Jackson' for z in ['71203','71220','71235','71237','71247','71251','71260','71261','71269']},
    # ── Lincoln Parish ────────────────────────────────────────────────────────
    **{z: 'Lincoln' for z in ['71227','71235','71245','71270','71272','71273','71275']},
    # ── Union Parish ─────────────────────────────────────────────────────────
    **{z: 'Union' for z in ['71241','71247','71261','71268','71269']},
    # ── Ouachita Parish ────────────────────────────────────────────────────────
    **{z: 'Ouachita' for z in [
        '71201','71202','71203','71207','71208','71209','71210','71211',
        '71212','71213','71218','71220','71221','71222','71223','71225',
        '71226','71229','71230','71231','71232','71233','71234','71235',
        '71238','71240','71241','71243','71245','71247','71248','71249',
        '71250','71251','71252','71253','71254','71256','71257','71258',
        '71259','71260','71261','71262','71263','71264','71265','71266',
        '71268','71269',
    ]},
    # ── Morehouse Parish ─────────────────────────────────────────────────────
    **{z: 'Morehouse' for z in ['71218','71220','71221','71222','71223','71225','71229','71232']},
    # ── West Carroll Parish ────────────────────────────────────────────────────
    **{z: 'West Carroll' for z in ['71256','71263','71264','71266']},
    # ── East Carroll Parish ───────────────────────────────────────────────────
    **{z: 'East Carroll' for z in ['71254','71282']},
    # ── Madison Parish ────────────────────────────────────────────────────────
    **{z: 'Madison' for z in ['71217','71282']},
    # ── Richland Parish ───────────────────────────────────────────────────────
    **{z: 'Richland' for z in ['71239','71269','71270','71276','71279','71281','71286']},
    # ── Franklin Parish ────────────────────────────────────────────────────────
    **{z: 'Franklin' for z in ['71226','71227','71238','71243','71259','71260','71266','71286','71295']},
    # ── Tensas Parish ────────────────────────────────────────────────────────
    **{z: 'Tensas' for z in ['71350','71357','71366']},
    # ── Concordia Parish ─────────────────────────────────────────────────────
    **{z: 'Concordia' for z in ['71334','71350','71360','71369','71373','71377']},
    # ── Catahoula Parish ─────────────────────────────────────────────────────
    **{z: 'Catahoula' for z in ['71331','71343','71360','71368','71373','71378']},
    # ── LaSalle Parish ────────────────────────────────────────────────────────
    **{z: 'LaSalle' for z in ['71328','71342','71345','71348','71368','71371','71405']},
    # ── Winn Parish ───────────────────────────────────────────────────────────
    **{z: 'Winn' for z in ['71483','71485']},
    # ── Grant Parish ─────────────────────────────────────────────────────────
    **{z: 'Grant' for z in ['71403','71411','71416','71417','71418','71419','71423','71426','71435','71467']},
    # ── Rapides Parish ────────────────────────────────────────────────────────
    **{z: 'Rapides' for z in [
        '71301','71302','71303','71306','71307','71309','71315','71316',
        '71320','71322','71324','71325','71327','71328','71329','71330',
        '71331','71332','71333','71334','71336','71339','71340','71341',
        '71342','71343','71344','71345','71346','71348','71350','71351',
        '71353','71354','71355','71356','71357','71358','71360','71361',
        '71362','71363','71365','71366','71367','71368','71369','71371',
        '71373','71375','71377','71378','71404','71405','71411','71416',
        '71417','71418','71419','71423','71424','71425','71426','71427',
        '71430','71432','71433','71434','71435','71438','71439','71440',
        '71441','71442','71443','71446','71447','71449','71450','71452',
        '71454','71455','71456','71457','71462','71463','71465','71467',
        '71468','71469','71472','71479','71480','71483','71484','71485',
        '71486',
    ]},
    # ── Avoyelles Parish ─────────────────────────────────────────────────────
    **{z: 'Avoyelles' for z in [
        '71322','71325','71328','71336','71339','71351','71353','71354',
        '71355','71356','71358','71361','71362','71363','71367','71375',
        '71378',
    ]},
}

_ZIP3_PARISH: dict = {
    # Broad regional fallbacks — only used when ZIP5 isn't in _ZIP5_PARISH.
    # All ZIP3s here must be Louisiana ZIPs (verified by _ZIP3_STATE).
    '700': 'Jefferson',          # Greater New Orleans / west bank
    '701': 'Orleans',            # New Orleans core
    '703': 'Terrebonne',         # Houma / Terrebonne
    '704': 'Tangipahoa',         # Hammond / Tangipahoa
    '705': 'Lafayette',          # Lafayette
    '706': 'Calcasieu',          # Lake Charles / Calcasieu
    '707': 'East Baton Rouge',   # Baton Rouge core
    '708': 'East Baton Rouge',   # Baton Rouge metro
    '710': 'Caddo',              # Shreveport core
    '711': 'Caddo',              # Shreveport / NW Louisiana
    '712': 'Ouachita',           # Monroe / NE Louisiana
    '713': 'Rapides',            # Alexandria / Central Louisiana
    '714': 'Natchitoches',       # Natchitoches / NW Louisiana
}

def _zip_to_parish_fallback(zip_code: str):
    """Return a parish name from ZIP code, or None if not resolvable."""
    z = (zip_code or '').strip().replace('-', '')
    if len(z) < 3 or not z[:3].isdigit():
        return None
    # ZIP5 exact match (highest confidence)
    if len(z) >= 5:
        p = _ZIP5_PARISH.get(z[:5])
        if p:
            return p
    # ZIP3 prefix heuristic — only for known Louisiana ZIPs
    z3 = z[:3]
    if _ZIP3_STATE.get(z3) == 'LA':
        return _ZIP3_PARISH.get(z3)
    return None

# ── Politician party lookup (loaded from la_politicians_lookup.json) ───────────
LOOKUP_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'la_politicians_lookup.json')

# Inline fallback used only if the JSON file is missing
_FALLBACK_LOOKUP = {
    'JEFF LANDRY': 'REP', 'JOHN BEL EDWARDS': 'DEM', 'BOBBY JINDAL': 'REP',
    'KATHLEEN BLANCO': 'DEM', 'JOHN KENNEDY': 'REP', 'BILL CASSIDY': 'REP',
    'MARY LANDRIEU': 'DEM', 'MIKE JOHNSON': 'REP', 'STEVE SCALISE': 'REP',
    'GARRET GRAVES': 'REP', 'TROY CARTER': 'DEM', 'CLAY HIGGINS': 'REP',
    'CEDRIC RICHMOND': 'DEM', 'NANCY LANDRY': 'REP', 'LIZ MURRILL': 'REP',
    'KYLE ARDOIN': 'REP', 'MIKE STRAIN': 'REP', 'JOHN SCHRODER': 'REP',
    'SHARON HEWITT': 'REP', 'LANCE HARRIS': 'REP', 'KATRINA JACKSON': 'DEM',
    'ROYCE DUPLESSIS': 'DEM', 'MANDIE LANDRY': 'DEM', 'LATOYA CANTRELL': 'DEM',
}

_POLITICIAN_LOOKUP: dict = {}

def _normalize_name(name: str) -> str:
    """Uppercase, strip honorifics/punctuation, collapse whitespace."""
    name = name.upper()
    name = re.sub(r'\b(DR|MR|MRS|MS|JR|SR|II|III|IV|ESQ|PHD|MD)\.?\b', '', name)
    name = re.sub(r'[^A-Z\s]', ' ', name)
    return ' '.join(name.split())

def _load_politician_lookup():
    """Load la_politicians_lookup.json into _POLITICIAN_LOOKUP."""
    global _POLITICIAN_LOOKUP
    if os.path.exists(LOOKUP_FILE):
        try:
            with open(LOOKUP_FILE, 'r', encoding='utf-8') as f:
                raw = json.load(f)
            # Keys are already uppercase from build_politician_lookup.py
            _POLITICIAN_LOOKUP = raw
            full_keys = sum(1 for k in raw if ' ' in k)
            print(f'  Politician lookup: {full_keys} full-name entries loaded from {os.path.basename(LOOKUP_FILE)}')
        except Exception as e:
            print(f'  WARNING: could not load {LOOKUP_FILE}: {e}')
    if not _POLITICIAN_LOOKUP:
        _POLITICIAN_LOOKUP = {k: {'party': v} for k, v in _FALLBACK_LOOKUP.items()}
        print(f'  Politician lookup: using built-in fallback ({len(_FALLBACK_LOOKUP)} entries)')

def _bust_stale_caches():
    """Delete old-format or stale cache files so they are rebuilt in NDJSON format."""
    if OFFLINE:
        return   # parity/test mode: leave the on-disk snapshot exactly as-is
    busted = 0
    lookup_mtime = os.path.getmtime(LOOKUP_FILE) if os.path.exists(LOOKUP_FILE) else 0
    for fname in os.listdir(CACHE_DIR):
        fpath = os.path.join(CACHE_DIR, fname)
        if not fname.endswith('.json.gz'):
            continue
        # Only record caches are subject to busting — derived artifacts like
        # la_entities.json.gz live here too and must survive boot.
        if not fname.startswith(('contributions_', 'expenditures_', 'loans_')):
            continue
        is_old_format = '_yr' not in fname
        is_stale = lookup_mtime and os.path.getmtime(fpath) < lookup_mtime
        if is_old_format or is_stale:
            try:
                os.remove(fpath)
                busted += 1
            except OSError:
                pass
    if busted:
        print(f'  Cache: removed {busted} old/stale file(s) - will re-download as needed')

def lookup_party(name: str, _depth: int = 0) -> str:
    """Return DEM/REP/OTH for a filer name. See lookup_party_detail."""
    return lookup_party_detail(name, _depth)['party']


def lookup_party_detail(name: str, _depth: int = 0) -> dict:
    """Return {'party', 'source', 'matched'} for a filer name.

    'source' is the lookup entry's provenance (SoS / SoS-results / curated...),
    'party-org-name' for direct party-committee name signals, or None when
    unmatched. 'matched' is the lookup key that hit, or None.

    Matching strategy (requires >= 2 tokens to avoid false positives):
      0. Comma-swap "LAST, FIRST [suffix]"  ("Kerner, Jr." handled gracefully)
      1. Exact normalized full-name match   ("STEVE SCALISE")
      2. First token + last token           ("AIMEE ADATTO FREEMAN" → "AIMEE FREEMAN")
      3. Skip leading single-letter token   ("J CAMERON HENRY" → "CAMERON HENRY")
      4. Strip single-letter middle tokens  ("ALAN T SEABAUGH" → "ALAN SEABAUGH")
      5. Committee unwrap: extract the person from committee-style names
         ("John Bel Edwards for Louisiana Leadership PAC, LLC",
          "Friends of Jane Doe", "Committee to Elect John Smith") and retry.
    """
    _MISS = {'party': 'OTH', 'source': None, 'matched': None}

    def _hit(entry, key):
        return {'party': entry.get('party', 'OTH'),
                'source': entry.get('source'), 'matched': key}

    if not name or name == 'Unknown':
        return dict(_MISS)

    # ── Party-committee fast path ──────────────────────────────────────────────
    # Filers that ARE a party org (not an individual) can be classified directly
    # from their name without touching the politician lookup.
    _up = name.upper()
    _REP_SIGNALS = [
        'REPUBLICAN PARTY', 'REPUBLICAN SENATE', 'REPUBLICAN HOUSE',
        'REPUBLICAN CAUCUS', 'REPUBLICAN LEADERSHIP', 'GOP ',
        'NRCC', 'NRSC', 'RSLC', 'RNCC', 'RNC',
        'LOUISIANA REPUBLICAN', 'LA REPUBLICAN',
    ]
    _DEM_SIGNALS = [
        'DEMOCRATIC PARTY', 'DEMOCRATIC SENATE', 'DEMOCRATIC HOUSE',
        'DEMOCRATIC CAUCUS', 'DEMOCRATIC LEADERSHIP', 'DEMOPAC',
        'DCCC', 'DSCC', 'DLCC', 'NRDC', 'DNC',
        'LOUISIANA DEMOCRATIC', 'LA DEMOCRATIC', 'LA DEMOCRATS',
        'LOUISIANA DEMOCRATS',
    ]
    if any(s in _up for s in _REP_SIGNALS):
        return {'party': 'REP', 'source': 'party-org-name', 'matched': None}
    if any(s in _up for s in _DEM_SIGNALS):
        return {'party': 'DEM', 'source': 'party-org-name', 'matched': None}

    # 0. Handle "LASTNAME, FIRSTNAME [suffix]" — try swapped form first
    if ',' in name:
        raw_parts = name.split(',', 1)
        swapped = f'{raw_parts[1].strip()} {raw_parts[0].strip()}'
        norm_swapped = _normalize_name(swapped)
        if len(norm_swapped.split()) >= 2:
            entry = _POLITICIAN_LOOKUP.get(norm_swapped)
            if entry:
                return _hit(entry, norm_swapped)

    # Normalize the full name (strips honorifics, punctuation, collapses whitespace)
    norm = _normalize_name(name)
    if not norm:
        return dict(_MISS)
    tokens = norm.split()

    # 1. Exact normalized full-name match
    if len(tokens) >= 2:
        entry = _POLITICIAN_LOOKUP.get(norm)
        if entry:
            return _hit(entry, norm)

    # Extended matching for names with middle names / initials / nicknames
    if len(tokens) >= 3:
        # 2. First token + last token only
        #    e.g. "AIMEE ADATTO FREEMAN" → "AIMEE FREEMAN"
        #    e.g. "THOMAS ALEXANDER PRESSLY" → "THOMAS PRESSLY"
        first_last = f'{tokens[0]} {tokens[-1]}'
        entry = _POLITICIAN_LOOKUP.get(first_last)
        if entry:
            return _hit(entry, first_last)

        # 3. Skip leading single-letter initial ("J CAMERON HENRY" → "CAMERON HENRY")
        if len(tokens[0]) == 1:
            rest = ' '.join(tokens[1:])
            entry = _POLITICIAN_LOOKUP.get(rest)
            if entry:
                return _hit(entry, rest)
            # Also try first+last of remaining tokens ("M KIRK TALBOT" → "KIRK TALBOT")
            fl_rest = f'{tokens[1]} {tokens[-1]}'
            if fl_rest != rest:
                entry = _POLITICIAN_LOOKUP.get(fl_rest)
                if entry:
                    return _hit(entry, fl_rest)

        # 4. Strip single-letter middle tokens, keep all multi-letter tokens
        #    e.g. "ALAN T SEABAUGH" → "ALAN SEABAUGH"
        #    e.g. "TIMOTHY P KERNER" → "TIMOTHY KERNER"
        stripped = [tokens[0]] + [t for t in tokens[1:-1] if len(t) > 1] + [tokens[-1]]
        if len(stripped) < len(tokens) and len(stripped) >= 2:
            key = ' '.join(stripped)
            entry = _POLITICIAN_LOOKUP.get(key)
            if entry:
                return _hit(entry, key)

    # 5. Committee unwrap — candidate-affiliated committees embed the person's
    #    name in boilerplate ("X for Y", "Friends of X", "Committee to Elect X").
    #    Strip org suffixes, extract the person candidates, and retry once.
    #    The >=2-token rule above makes junk extractions ("Citizens") fail safe.
    if _depth < 2:
        base = name.strip()
        while True:
            nb = re.sub(r'[,\s]+(LLC|L\.L\.C\.?|INC\.?|PAC)\s*$', '', base, flags=re.I)
            if nb == base:
                break
            base = nb
        extracted = []
        m = re.match(r'(?i)^(?:friends\s+(?:of|for)|committee\s+to\s+(?:re-?)?elect|'
                     r'committee\s+for|campaign\s+(?:fund\s+)?(?:of|for))\s+(.+)$', base)
        if m:
            extracted.append(m.group(1))
        if re.search(r'(?i)\s+for\s+', base):
            before, after = re.split(r'(?i)\s+for\s+', base, maxsplit=1)
            extracted.extend([before, after])
        for cand in extracted:
            if cand.strip() and cand.strip() != name.strip():
                d = lookup_party_detail(cand, _depth + 1)
                if d['party'] != 'OTH':
                    return d

    return dict(_MISS)

def parse_date(s):
    """'9/26/2026 12:00:00 AM' -> '2026-09-26'. Returns '' for missing/unparseable."""
    if not s: return ''
    part = s.strip().split(' ')[0].split('/')
    if len(part) == 3:
        m, d, y = part
        try:
            return f'{int(y):04d}-{int(m):02d}-{int(d):02d}'
        except ValueError:
            return ''
    # Already ISO-ish: '2026-09-26' or truncated
    cleaned = s.strip()[:10]
    return cleaned if len(cleaned) == 10 and cleaned[4] == '-' else ''

# ── Per-year cache helpers ────────────────────────────────────────────────────
# Cache one gzip file per calendar year instead of one file per 4-year CSV range.
# Loading a 2-year request window uses ~50% of the RAM compared to the old approach.

_locks = {}
_locks_lock = threading.Lock()

def get_lock(key):
    with _locks_lock:
        if key not in _locks:
            _locks[key] = threading.Lock()
        return _locks[key]

def _year_cache_path(year, report_type):
    return os.path.join(CACHE_DIR, f'{report_type}_yr{year}.json.gz')

def _year_is_fresh(year, report_type):
    p = _year_cache_path(year, report_type)
    if OFFLINE:
        return os.path.exists(p)   # any on-disk file counts as fresh — never re-download
    return os.path.exists(p) and (time.time() - os.path.getmtime(p)) < CACHE_TTL

def _key_years(csv_key):
    """'2020-2023' -> [2020, 2021, 2022, 2023]"""
    parts = csv_key.split('-')
    return list(range(int(parts[0]), int(parts[-1]) + 1))

def is_cached_fresh(csv_key, report_type='contributions'):
    """True when every year in the range has a fresh per-year cache file."""
    return all(_year_is_fresh(y, report_type) for y in _key_years(csv_key))

# Each transaction in the bulk data carries the certified report it was filed
# on (ReportNumber, e.g. "LA-113368"), which IS the public PDF's filename:
# https://eap.ethics.la.gov/CFSearch/LA-113368.pdf. We store only reportNumber
# (+ reportCode) per record and let the client derive the URL — storing the full
# URL on 1.1M rows bloated the payload ~20% and slowed every load.
def _filing_fields(row):
    """Return {reportNumber, reportCode} for a bulk-data row. The filing PDF URL
    is derivable from reportNumber, so it is NOT stored per record."""
    return {'reportNumber': (row.get('ReportNumber') or '').strip(),
            'reportCode':   (row.get('ReportCode')   or '').strip()}


def _parse_contribution_row(row):
    amt = float((row.get('ContributionAmt') or '').strip() or 0)
    if amt <= 0:
        return None

    city     = (row.get('ContributorCity')    or '').strip()
    addr_raw = (row.get('ContributorAddress') or '').strip()
    zip_raw  = (row.get('ContributorZip')     or '').strip()

    # If the dedicated ZIP field is blank, try to extract it from the address string.
    # Addresses sometimes arrive as "123 Main St, Alexandria, VA 22301".
    if not zip_raw:
        _zm = re.search(r'\b(\d{5})(?:-\d{4})?\b', addr_raw)
        if _zm:
            zip_raw = _zm.group(1)

    # Resolve contributor state: CSV field first, then ZIP-prefix lookup.
    state_csv        = (row.get('ContributorState') or '').strip().upper()
    contributor_state = state_csv or _zip_to_state(zip_raw)

    # Louisiana Clerks of Court are always in-state, but the Ethics source data
    # sometimes has a wrong state code (e.g. "AR") for these entries.
    # Override to LA whenever the contributor is a Clerk of Court that is NOT
    # a campaign committee (i.e. the actual clerk's office, not "Jane Doe for CoC").
    contributor_name_upper = (row.get('ContributorName') or '').upper()
    is_la_clerk = (
        bool(re.search(r'\bCLERK\b.*\bCOURT\b', contributor_name_upper))
        and not re.search(r'\bFOR\b.*\bCLERK\b', contributor_name_upper)
    )
    if is_la_clerk:
        contributor_state = 'LA'

    # Only map to a Louisiana parish when the contributor is actually from Louisiana.
    # Out-of-state contributors get parish='Out of State' so "Alexandria, VA" is never
    # mistaken for Alexandria in Rapides Parish, and the LA map stays accurate.
    if not contributor_state or contributor_state == 'LA':
        confident_parish = (CITY_TO_PARISH.get(city.upper())
                            or _zip_to_parish_fallback(zip_raw))
        parish = confident_parish or 'East Baton Rouge'
        # If we can place them in a real LA parish from city/ZIP but the state
        # was blank (no source value, malformed ZIP), label it LA — otherwise the
        # row shows a Baton Rouge parish with an empty State column.
        if not contributor_state and confident_parish:
            contributor_state = 'LA'
    else:
        parish = 'Out of State'

    first = (row.get('FilerFirstName') or '').strip().rstrip(',').strip()
    last  = (row.get('FilerLastName')  or '').strip().rstrip(',').strip()
    filer = ' '.join(x for x in [first, last] if x)
    contrib_type = (row.get('ContributionType') or '').strip()
    notes_raw = (row.get('Notes') or row.get('Description') or
                 row.get('ContributionDescription') or row.get('Memo') or '').strip()
    ff_text = f'{contrib_type} {notes_raw}'.upper()
    return {
        'contributor':        (row.get('ContributorName') or 'Unknown').strip(),
        'city':               city,
        'parish':             parish,
        'amount':             round(amt, 2),
        'date':               parse_date(row.get('ContributionDate', '')),
        'candidate':          filer or 'Unknown',
        'party':              lookup_party(filer),
        'source':             'LA Ethics',
        'type':               contrib_type,
        'filerNumber':        (row.get('FilerNumber') or '').strip(),
        'contributorAddress': addr_raw,
        'contributorZip':     zip_raw,
        'contributorState':   contributor_state,
        'employer':           (row.get('ContributorEmployer') or row.get('Employer') or '').strip(),
        'occupation':         (row.get('ContributorOccupation') or row.get('Occupation') or '').strip(),
        'electionYear':       (row.get('ElectionYear')       or '').strip(),
        'officeDescription':  (row.get('OfficeDescription')  or row.get('Office') or '').strip(),
        'filerType':          (row.get('FilerType')          or '').strip(),
        'scheduleType':       (row.get('ScheduleDescription') or row.get('Schedule') or
                               row.get('ScheduleType') or '').strip(),
        **_filing_fields(row),   # reportNumber + reportCode (filing PDF derived client-side)
        'notes':              notes_raw,
        'isFilingFee':        (
                                  any(p in ff_text for p in [
                                      'FILING FEE', 'QUALIFYING FEE',
                                      'QUALIFICATION FEE', 'FILING/QUALIFYING'])
                                  # Payments from any parish Clerk of Court to a party
                                  # committee are qualifying/filing fees forwarded by the
                                  # clerk — flag them even when notes are blank.
                                  # Exclude campaign committees running FOR clerk of court
                                  # (e.g. "Jane Doe for Clerk of Court") — those are ordinary
                                  # contributor-to-candidate contributions, not filing fees.
                                  or (
                                      bool(re.search(r'\bCLERK\b.*\bCOURT\b', (row.get('ContributorName') or '').upper()))
                                      and not re.search(r'\bFOR\b.*\bCLERK\b', (row.get('ContributorName') or '').upper())
                                  )
                              ),
    }

def _parse_expenditure_row(row):
    amt = float((row.get('ExpenditureAmt') or '').strip() or 0)
    if amt <= 0:
        return None
    city           = (row.get('RecipientCity')  or '').upper().strip()
    zip_raw        = (row.get('RecipientZip')   or '').strip()
    state_raw      = (row.get('RecipientState') or '').strip().upper()
    first = (row.get('FilerFirstName') or '').strip().rstrip(',').strip()
    last  = (row.get('FilerLastName')  or '').strip().rstrip(',').strip()
    filer = ' '.join(x for x in [first, last] if x)

    # Resolve parish only for Louisiana recipients; everything else is "Out of State"
    if state_raw and state_raw != 'LA':
        parish = 'Out of State'
    else:
        parish = (
            CITY_TO_PARISH.get(city)
            or _zip_to_parish_fallback(zip_raw)
            or 'East Baton Rouge'
        )

    return {
        'contributor':     (row.get('RecipientName') or 'Unknown').strip(),
        'city':            (row.get('RecipientCity') or '').strip(),
        'recipientState':  state_raw or _zip_to_state(zip_raw),
        'parish':          parish,
        'amount':          round(amt, 2),
        'date':            parse_date(row.get('ExpenditureDate', '')),
        'candidate':       filer or 'Unknown',
        'party':           lookup_party(filer),
        'source':          'LA Ethics (Expenditure)',
        'description':     (row.get('ExpenditureDescription') or '').strip(),
        'filerNumber':     (row.get('FilerNumber') or '').strip(),
        **_filing_fields(row),   # reportNumber + reportCode (filing PDF derived client-side)
    }

def _parse_loan_row(row):
    amt = float((row.get('LoanAmt') or '').strip() or 0)
    if amt <= 0:
        return None
    city      = (row.get('LoanHolderCity')  or '').upper().strip()
    zip_raw   = (row.get('LoanHolderZip')   or '').strip()
    state_raw = (row.get('LoanHolderState') or '').strip().upper()
    first = (row.get('FilerFirstName') or '').strip().rstrip(',').strip()
    last  = (row.get('FilerLastName')  or '').strip().rstrip(',').strip()
    filer = ' '.join(x for x in [first, last] if x)

    if state_raw and state_raw != 'LA':
        parish = 'Out of State'
    else:
        parish = (
            CITY_TO_PARISH.get(city)
            or _zip_to_parish_fallback(zip_raw)
            or 'East Baton Rouge'
        )

    holder = (row.get('LoanHolderName') or 'Unknown').strip()
    rate   = float((row.get('LoanRate') or '0').strip() or 0)

    return {
        'contributor':  holder,
        'city':         (row.get('LoanHolderCity') or '').strip(),
        'lenderState':  state_raw or _zip_to_state(zip_raw),
        'parish':       parish,
        'amount':       round(amt, 2),
        'rate':         round(rate, 4),
        'date':         parse_date(row.get('LoanDate', '')),
        'candidate':    filer or 'Unknown',
        'party':        lookup_party(filer),
        'source':       'LA Ethics (Loan)',
        'filerNumber':  (row.get('FilerNumber') or '').strip(),
        **_filing_fields(row),   # reportNumber + reportCode (filing PDF derived client-side)
    }

def download_and_cache(csv_key, report_type='contributions'):
    """Stream-parse the CSV and write one NDJSON-gzip file per calendar year.

    Records are written to disk one line at a time as they are parsed, so peak
    memory during the download phase is essentially zero — no in-memory accumulation.
    """
    status_key = f'{report_type}_{csv_key}'
    lock_key   = f'{report_type}_{csv_key}_download'

    with get_lock(lock_key):
        if is_cached_fresh(csv_key, report_type):
            set_status(status_key, 'ready', 'cached')
            return
        if OFFLINE:
            # Never hit the network in parity/test mode — serve whatever year
            # files are already on disk (missing ones stream empty, exactly as
            # the static layer's 404 does).
            set_status(status_key, 'ready', 'cached')
            return

        url_map = (CSV_URLS          if report_type == 'contributions' else
                   EXPENDITURE_URLS  if report_type == 'expenditures'  else
                   LOAN_URLS)
        url = url_map.get(csv_key)
        if not url:
            raise ValueError(f'No URL for {report_type}/{csv_key}')

        set_status(status_key, 'downloading', f'Downloading {csv_key} from ethics.la.gov...')
        print(f'  Streaming {url}')

        parse_row = (_parse_contribution_row if report_type == 'contributions' else
                     _parse_expenditure_row  if report_type == 'expenditures'  else
                     _parse_loan_row)

        # Open per-year gzip writers lazily as years are encountered
        year_writers = {}   # {year: open gzip file handle}
        year_counts  = {}
        year_seen    = {}   # {year: set of dedup keys}
        year_dupes   = {}   # {year: int} duplicate rows skipped
        tmp_suffix   = '.tmp'

        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 LACampaignFinanceDashboard/1.0',
            'Accept-Encoding': 'identity',
        })
        try:
            with urllib.request.urlopen(req, timeout=300) as resp:
                text_stream = io.TextIOWrapper(resp, encoding='utf-8-sig', errors='replace')
                reader = csv.DictReader(text_stream)
                for row in reader:
                    try:
                        rec = parse_row(row)
                        if rec is None:
                            continue
                        year = int(rec['date'][:4])
                        if year not in year_writers:
                            tmp = _year_cache_path(year, report_type) + tmp_suffix
                            year_writers[year] = gzip.open(tmp, 'wt', encoding='utf-8', compresslevel=1)
                            year_counts[year]  = 0
                            year_seen[year]    = set()
                            year_dupes[year]   = 0
                        # Deduplicate: the Ethics board publishes contributions once per
                        # report filing (F102 periodic, F103 supplemental, etc.), so the
                        # same contribution can appear on multiple form filings for the
                        # same filer.  Key on (filer, contributor, amount, date, notes);
                        # if all five match we skip the duplicate row.
                        dedup_key = (
                            rec.get('filerNumber', ''),
                            rec.get('contributor', ''),
                            rec.get('amount', 0),
                            rec.get('date', ''),
                            rec.get('notes', ''),
                        )
                        if dedup_key in year_seen[year]:
                            year_dupes[year] += 1
                            continue
                        year_seen[year].add(dedup_key)
                        # Write one JSON line per record — constant memory cost
                        year_writers[year].write(json.dumps(rec, separators=(',', ':')) + '\n')
                        year_counts[year] += 1
                    except Exception:
                        continue
        finally:
            # Always close writers; on success rename .tmp -> final path
            for year, fh in year_writers.items():
                fh.close()
            if year_counts:  # only rename if we got data (not an error mid-stream)
                for year in year_writers:
                    tmp  = _year_cache_path(year, report_type) + tmp_suffix
                    dest = _year_cache_path(year, report_type)
                    if os.path.exists(tmp):
                        os.replace(tmp, dest)

        total = sum(year_counts.values())
        dupes = sum(year_dupes.values())
        print(f'  Wrote {total:,} records for years {sorted(year_counts.keys())} '
              f'(skipped {dupes:,} duplicate rows)')
        gc.collect()
        set_status(status_key, 'ready', f'{total:,} records cached')


def prefetch_background(csv_key, report_type='contributions'):
    """Kick off a background thread to warm the per-year cache."""
    status_key = f'{report_type}_{csv_key}'
    if is_cached_fresh(csv_key, report_type):
        set_status(status_key, 'ready', 'cached')
        return
    def _run():
        try:
            download_and_cache(csv_key, report_type)
            print(f'  Background prefetch done: {csv_key}')
        except Exception as e:
            set_status(status_key, 'error', str(e))
            print(f'  Background prefetch error for {csv_key}: {e}')
    threading.Thread(target=_run, daemon=True).start()

# ── HTTP Handler ──────────────────────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'   # required for chunked transfer-encoding

    def do_OPTIONS(self):
        self.send_response(200)
        self._cors_headers()
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        # Serve the dashboard HTML at root
        if parsed.path in ('/', '/index.html', '/louisiana-campaign-finance.html'):
            if os.path.exists(HTML_FILE):
                with open(HTML_FILE, 'rb') as f:
                    body = f.read()
                self.send_response(200)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(404)
                self.end_headers()
            return

        if parsed.path == '/health':
            self._json({'status': 'ok', 'port': PORT,
                        'cache_dir': CACHE_DIR,
                        'cached_files': os.listdir(CACHE_DIR)})
            return

        if parsed.path == '/api/data-status':
            cycle = params.get('cycle', ['2024'])[0]
            csv_key = get_csv_key(cycle)
            st = get_status(f'contributions_{csv_key}')
            self._json({
                'status':  st['status'],
                'message': st['message'],
                'cached':  is_cached_fresh(csv_key, 'contributions'),
            })
            return

        # Flat election-results lookup (win/loss badges). Static, cached client-side.
        if parsed.path == '/api/election-results':
            lk = os.path.join(BASE_DIR, 'la_election_lookup.json')
            if os.path.exists(lk):
                with open(lk, 'rb') as f:
                    body = f.read()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Content-Length', str(len(body)))
                self._cors_headers()
                self.end_headers()
                self.wfile.write(body)
            else:
                self._json({})   # gracefully empty if not built yet
            return

        # Candidate career history — financial index + SoS race list for one name.
        if parsed.path == '/api/candidate-history':
            name = params.get('name', [''])[0].strip()
            filer = params.get('filer', [''])[0].strip()
            if not name:
                self._json({'error': 'name required'}); return
            self._json(build_candidate_history_payload(name, filer))
            return

        # ── /api/insights — nightly precomputed insight blocks ───────────────
        if parsed.path == '/api/insights':
            _load_insights()
            self._json(_INSIGHTS or {})
            return

        # ── /api/entity — canonical entity lookup by filer number or name ────
        if parsed.path == '/api/entity':
            name  = params.get('name',  [''])[0].strip()
            filer = params.get('filer', [''])[0].strip()
            ent = _get_entity(name=name or None, filer=filer or None)
            self._json(ent or {})
            return

        # ── /api/coh — certified COH lookup for one or all filers ────────────
        if parsed.path == '/api/coh':
            _load_ethics_coh()
            name = params.get('name', [''])[0].strip()
            if name:
                entry = _get_ethics_coh(name)
                self._json(entry or {})
            else:
                # Return full cache (used by the dashboard for batch COH labels)
                self._json(_ETHICS_COH or {})
            return

        # Industry breakdown for a filer across a cycle (or all time).
        # ?filer=FILER_NUMBER&cycle=2024-2027  (cycle optional — omit for all years)
        if parsed.path == '/api/industry-breakdown':
            filer = params.get('filer', [''])[0].strip()
            if not filer:
                self._json({'error': 'filer required'}); return
            cycle = params.get('cycle', [''])[0].strip()  # e.g. "2024-2027" or ""
            payload = build_industry_breakdown(filer, cycle)
            self._json(payload)
            return

        # ── /api/races — all races grouped by (date, office), with candidate finance ─
        if parsed.path == '/api/races':
            office_filter = params.get('office', ['major'])[0].lower()
            year_filter   = params.get('year',   [''])[0].strip()
            self._json(build_races_payload(office_filter, year_filter))
            return

        # ── /api/search — unified candidate/committee lookup ─────────────────
        if parsed.path == '/api/search':
            _build_search_index()
            q = params.get('q', [''])[0].strip().upper()
            if len(q) < 2:
                self._json({'results': [], 'query': q, 'total': 0})
                return
            # Tier 0: query begins a name token (e.g. first/last name) — best.
            # Tier 1: query appears mid-token. Within each tier rank by $ raised
            # so a last-name search surfaces the biggest names first.
            word, contains = [], []
            for e in _SEARCH_INDEX:
                nu = e['name_upper']
                pos = nu.find(q)
                if pos < 0:
                    continue
                at_word_start = pos == 0 or nu[pos - 1] == ' '
                (word if at_word_start else contains).append(e)
            word.sort(key=lambda e: -e['total_raised'])
            contains.sort(key=lambda e: -e['total_raised'])
            ordered = word + contains
            results = [{
                'name':         e['name'],
                'is_candidate': e['is_candidate'],
                'total_raised': e['total_raised'],
                'n_cycles':     e['n_cycles'],
                'n_races':      e['n_races'],
                'last_office':  e['last_office'],
                'last_outcome': e['last_outcome'],
                'last_date':    e['last_date'],
                'filer_number': e['filer_number'],
            } for e in ordered[:25]]
            self._json({'results': results, 'query': q, 'total': len(ordered)})
            return

        # ── /api/overview — landing-page aggregate stats ─────────────────────
        if parsed.path == '/api/overview':
            self._json(build_overview_payload())
            return

        # ── /data/<name> — GitHub Pages emulation for STATIC_MODE testing ────
        # Serves the nightly artifacts exactly as Pages will: .gz files as raw
        # gzip bytes with NO Content-Encoding (the client decompresses via
        # DecompressionStream), .json as plain JSON. Prefers the fresh
        # .la_cache/ copy, falls back to the committed repo-root file.
        # test_static_client_parity.mjs runs the shipped static_api.js against
        # these routes and asserts equality with the live /api/* endpoints.
        if parsed.path.startswith('/data/'):
            name = os.path.basename(parsed.path)
            if not re.fullmatch(r'[A-Za-z0-9_.\-]+\.(json|json\.gz)', name):
                self._empty(404); return
            fpath = None
            for base in (CACHE_DIR, BASE_DIR):
                cand = os.path.join(base, name)
                if os.path.exists(cand):
                    fpath = cand
                    break
            if not fpath:
                self._empty(404); return
            with open(fpath, 'rb') as fh:
                body = fh.read()
            self.send_response(200)
            self._cors_headers()
            self.send_header('Content-Type',
                             'application/gzip' if name.endswith('.gz') else 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        # PWA assets (manifest + icons referenced from the dashboard <head>)
        _PWA_ASSETS = {
            '/manifest.json':        'application/manifest+json',
            '/icon-192.png':         'image/png',
            '/icon-512.png':         'image/png',
            '/apple-touch-icon.png': 'image/png',
        }
        if parsed.path in _PWA_ASSETS:
            fpath = os.path.join(BASE_DIR, parsed.path.lstrip('/'))
            if os.path.exists(fpath):
                with open(fpath, 'rb') as fh:
                    body = fh.read()
                self.send_response(200)
                self.send_header('Content-Type', _PWA_ASSETS[parsed.path])
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self._empty(404)
            return

        # The static data layer itself (loaded by the dashboard's script tag)
        if parsed.path == '/static_api.js':
            fpath = os.path.join(BASE_DIR, 'static_api.js')
            if os.path.exists(fpath):
                with open(fpath, 'rb') as fh:
                    body = fh.read()
                self.send_response(200)
                self._cors_headers()
                self.send_header('Content-Type', 'application/javascript; charset=utf-8')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self._empty(404)
            return

        # Static data files — serve gzip-encoded JSON directly so the browser can
        # load them client-side without going through the candidate-history API.
        _STATIC_DATA = {
            '/la_candidate_index.json.gz': 'la_candidate_index.json.gz',
            '/la_candidacies_raw.json.gz': 'la_candidacies_raw.json.gz',
            '/la_donor_industries.json':   'la_donor_industries.json',
        }
        if parsed.path in _STATIC_DATA:
            fpath = os.path.join(BASE_DIR, _STATIC_DATA[parsed.path])
            if os.path.exists(fpath):
                with open(fpath, 'rb') as fh:
                    body = fh.read()
                self.send_response(200)
                self._cors_headers()
                self.send_header('Content-Type', 'application/json')
                if fpath.endswith('.gz'):
                    self.send_header('Content-Encoding', 'gzip')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(404)
                self._cors_headers()
                self.end_headers()
            return

        if parsed.path not in ('/api/la-ethics', '/api/la-expenditures', '/api/la-loans'):
            self.send_response(404)
            self._cors_headers()
            self.end_headers()
            return

        report_type = ('contributions' if parsed.path == '/api/la-ethics' else
                       'expenditures'  if parsed.path == '/api/la-expenditures' else
                       'loans')
        cycle = params.get('cycle', ['2024'])[0]
        csv_key = get_csv_key(cycle)
        status_key = f'{report_type}_{csv_key}'

        # Gate on the PRIMARY year only.
        # The previous year (y-1) may live in a different 4-year CSV bundle; if so,
        # kick off a background download for that bundle too so it eventually fills in.
        y = int(cycle)
        years_wanted = [y - 1, y]   # ideal two-year window
        prev_csv_key = get_csv_key(y - 1)
        if not _year_is_fresh(y, report_type):
            st = get_status(status_key)
            if st['status'] != 'downloading':
                prefetch_background(csv_key, report_type)
            body = json.dumps({
                'loading': True,
                'status':  'downloading',
                'message': f'Downloading {csv_key} data from ethics.la.gov — please wait…',
            }).encode('utf-8')
            self.send_response(202)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Content-Length', str(len(body)))
            self._cors_headers()
            self.end_headers()
            self.wfile.write(body)
            return

        # If y-1 is in a different bundle and not yet cached, we must wait for it —
        # otherwise we'd return an incomplete dataset and the client would cache it.
        # Kick off the download and return 202 so the client keeps polling.
        if prev_csv_key != csv_key and not _year_is_fresh(y - 1, report_type):
            prefetch_background(prev_csv_key, report_type)
            body = json.dumps({
                'loading': True,
                'status':  'downloading',
                'message': f'Downloading {prev_csv_key} data from ethics.la.gov…',
            }).encode('utf-8')
            self.send_response(202)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Content-Length', str(len(body)))
            self._cors_headers()
            self.end_headers()
            self.wfile.write(body)
            return

        # Both years are on disk — stream them
        years_to_serve = [yr for yr in years_wanted if _year_is_fresh(yr, report_type)]
        ndjson = params.get('format', [''])[0] == 'ndjson'
        try:
            self._stream_years_json(years_to_serve, report_type, ndjson=ndjson)
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f'  Streaming error for {report_type} cycle={cycle}: {e}')

    def _stream_years_json(self, years, report_type, ndjson=False):
        """Stream per-year NDJSON cache files via chunked transfer-encoding —
        as one JSON array by default, or as newline-delimited JSON when the
        client asks (?format=ndjson) so it can parse and render records
        progressively instead of waiting for the full payload.

        Peak RAM = O(1) per record.  No in-memory list, no json.dumps of the full dataset.
        All modern browsers reassemble chunked responses transparently before calling .json().

        Responses are gzip-encoded when the client accepts it (every browser does):
        these are the dashboard's multi-megabyte payloads, and shipping them as
        plain text was the single biggest first-load cost. compresslevel=1 keeps
        CPU negligible while still cutting the transfer ~75-80%.
        """
        accepts_gzip = 'gzip' in (self.headers.get('Accept-Encoding') or '')
        self.send_response(200)
        self.send_header('Content-Type',
                         'application/x-ndjson; charset=utf-8' if ndjson
                         else 'application/json; charset=utf-8')
        self.send_header('Transfer-Encoding', 'chunked')
        if accepts_gzip:
            self.send_header('Content-Encoding', 'gzip')
        self.send_header('Connection', 'close')
        self._cors_headers()
        self.end_headers()

        def wc(data: bytes):
            """Write one HTTP chunk."""
            if not data:
                return
            self.wfile.write(f'{len(data):x}\r\n'.encode('ascii'))
            self.wfile.write(data)
            self.wfile.write(b'\r\n')

        # Sink for the payload: either a streaming gzip wrapper around the
        # chunk writer, or the chunk writer itself.
        if accepts_gzip:
            class _ChunkSink:
                def write(self, data):  wc(data); return len(data)
                def flush(self):        pass
            out = gzip.GzipFile(fileobj=_ChunkSink(), mode='wb', compresslevel=1)
        else:
            class _PlainSink:
                def write(self, data):  wc(data); return len(data)
                def close(self):        pass
            out = _PlainSink()

        if report_type == 'contributions':
            _load_donor_industries()

        try:
            if not ndjson:
                out.write(b'[')
            first = True
            for year in years:
                p = _year_cache_path(year, report_type)
                if not os.path.exists(p):
                    continue
                with gzip.open(p, 'rt', encoding='utf-8') as gf:
                    for raw in gf:
                        raw = raw.strip()
                        if not raw:
                            continue
                        # Industry classification is baked into the cache files
                        # by the nightly retag pass; this per-record fallback
                        # only runs for records written before that change.
                        if (report_type == 'contributions' and _DONOR_IND is not None
                                and '"industry":' not in raw):
                            try:
                                rec = json.loads(raw)
                                donor = (rec.get('contributor') or '').strip()
                                norm  = _norm_name(donor)
                                rec['industry'] = (_DONOR_IND.get(norm)
                                                   or _DONOR_IND.get(donor)
                                                   or 'Other')
                                raw = json.dumps(rec, separators=(',', ':'))
                            except Exception:
                                pass
                        if ndjson:
                            out.write(raw.encode('utf-8') + b'\n')
                        else:
                            out.write((b'' if first else b',') + raw.encode('utf-8'))
                        first = False
            if not ndjson:
                out.write(b']')
            out.close()   # flushes the gzip trailer through the chunk writer
            # Terminating chunk signals end of chunked body
            self.wfile.write(b'0\r\n\r\n')
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass   # client disconnected mid-stream

    def _cors_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')

    def _empty(self, status):
        # Keep-alive safe: a status with no Content-Length leaves HTTP/1.1
        # clients waiting for a body that never comes.
        self.send_response(status)
        self._cors_headers()
        self.send_header('Content-Length', '0')
        self.end_headers()

    def _json(self, data):
        # Compact separators (indent=2 inflated every response 20-40%) and
        # gzip anything non-trivial — candidate-history monthly buckets and
        # search results run to hundreds of KB.
        body = json.dumps(data, separators=(',', ':')).encode('utf-8')
        gzipped = len(body) > 1024 and 'gzip' in (self.headers.get('Accept-Encoding') or '')
        if gzipped:
            body = gzip.compress(body, compresslevel=3)
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        if gzipped:
            self.send_header('Content-Encoding', 'gzip')
        self.send_header('Content-Length', str(len(body)))
        self._cors_headers()
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        print(f'  [{self.address_string()}] {fmt % args}')


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == '__main__':
    print('=' * 60)
    print('  Louisiana Ethics Campaign Finance Data Proxy')
    print('=' * 60)
    print(f'  Listening : http://{BIND_HOST}:{PORT}')
    print(f'  Cache dir : {CACHE_DIR}')
    print()
    _load_politician_lookup()
    _bust_stale_caches()
    print()

    # Warm the cache sequentially in one background thread — never two downloads at once.
    # Pre-warms contributions, expenditures, and loans for the most-recent cycle.
    # Skipped entirely in OFFLINE mode so the cache stays byte-identical to the
    # static snapshot during parity runs.
    if OFFLINE:
        print('  OFFLINE mode — skipping background prefetch (serving on-disk cache as-is).')
    else:
        print('  Pre-fetching 2024-2027 data (contributions, expenditures, loans) in background...')
        def _sequential_prefetch():
            for report_type, csv_key in [
                ('contributions', '2024-2027'),
                ('expenditures',  '2024-2027'),
                ('loans',         '2024-2027'),
            ]:
                if not is_cached_fresh(csv_key, report_type):
                    try:
                        download_and_cache(csv_key, report_type)
                    except Exception as e:
                        print(f'  Prefetch failed {report_type}/{csv_key}: {e}')
                gc.collect()
        threading.Thread(target=_sequential_prefetch, daemon=True).start()

    print()
    print('  Endpoints:')
    print('    GET /health                         -- server status')
    print('    GET /api/data-status?cycle=2024     -- cache status (poll this while loading)')
    print('    GET /api/la-ethics?cycle=2024       -- contributions')
    print('    GET /api/la-expenditures?cycle=2024 -- expenditures')
    print('    GET /api/coh                        -- certified COH from ethics PDFs (all)')
    print('    GET /api/coh?name=MANDIE+LANDRY     -- COH for one filer')
    print()
    print('  Data pre-fetching in background. First user request returns immediately.')
    print('  Cache is refreshed automatically every 24 hours.')
    print()
    print('  Press Ctrl+C to stop.')
    print('=' * 60)

    server = ThreadingHTTPServer((BIND_HOST, PORT), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('\n  Server stopped.')
