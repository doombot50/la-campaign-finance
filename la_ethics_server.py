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

class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle each HTTP request in its own thread so a slow download never blocks the server."""
    daemon_threads = True

PORT = int(os.environ.get('PORT', 8765))
BIND_HOST = '0.0.0.0'   # listen on all interfaces when deployed; localhost when running locally
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(BASE_DIR, '.la_cache')
os.makedirs(CACHE_DIR, exist_ok=True)
CACHE_TTL = 86400  # 24 hours
HTML_FILE = os.path.join(BASE_DIR, 'louisiana-campaign-finance.html')

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
    """Return a 2-letter state abbreviation for a ZIP code, or '' if unknown."""
    z = (zip_code or '').strip().replace('-', '')
    if len(z) >= 3 and z[:3].isdigit():
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
}

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
    busted = 0
    lookup_mtime = os.path.getmtime(LOOKUP_FILE) if os.path.exists(LOOKUP_FILE) else 0
    for fname in os.listdir(CACHE_DIR):
        fpath = os.path.join(CACHE_DIR, fname)
        if not fname.endswith('.json.gz'):
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

def lookup_party(name: str) -> str:
    """Return DEM/REP/OTH for a filer name using the politician lookup.

    Matching strategy (full-name only — avoids false positives from common surnames):
      1. Exact normalized full-name match  ("STEVE SCALISE")
      2. Reversed "LAST, FIRST" format     ("SCALISE, STEVE" -> "STEVE SCALISE")
         Comma check runs on the RAW name before normalization strips punctuation.
    """
    if not name or name == 'Unknown':
        return 'OTH'

    # 2. Handle "LASTNAME, FIRSTNAME" on raw string before normalization strips commas
    if ',' in name:
        raw_parts = name.split(',', 1)
        swapped = f'{raw_parts[1].strip()} {raw_parts[0].strip()}'
        norm_swapped = _normalize_name(swapped)
        entry = _POLITICIAN_LOOKUP.get(norm_swapped)
        if entry:
            return entry.get('party', 'OTH')

    # 1. Exact normalized full-name match (require at least 2 tokens to avoid
    #    accidentally hitting last-name-only shortcut keys in the JSON)
    norm = _normalize_name(name)
    if not norm:
        return 'OTH'
    if len(norm.split()) >= 2:
        entry = _POLITICIAN_LOOKUP.get(norm)
        if entry:
            return entry.get('party', 'OTH')

    return 'OTH'

def parse_date(s):
    """'9/26/2026 12:00:00 AM' -> '2026-09-26'"""
    if not s: return '2024-01-01'
    part = s.strip().split(' ')[0].split('/')
    if len(part) == 3:
        m, d, y = part
        return f'{y}-{m.zfill(2)}-{d.zfill(2)}'
    return s[:10]

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
    return os.path.exists(p) and (time.time() - os.path.getmtime(p)) < CACHE_TTL

def _key_years(csv_key):
    """'2020-2023' -> [2020, 2021, 2022, 2023]"""
    parts = csv_key.split('-')
    return list(range(int(parts[0]), int(parts[-1]) + 1))

def is_cached_fresh(csv_key, report_type='contributions'):
    """True when every year in the range has a fresh per-year cache file."""
    return all(_year_is_fresh(y, report_type) for y in _key_years(csv_key))

def _load_years(years, report_type):
    """Load records from per-year NDJSON-gzip cache files (one JSON object per line)."""
    records = []
    for year in years:
        p = _year_cache_path(year, report_type)
        if os.path.exists(p):
            with gzip.open(p, 'rt', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            records.append(json.loads(line))
                        except Exception:
                            continue
    return records

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

    # Only map to a Louisiana parish when the contributor is actually from Louisiana.
    # Out-of-state contributors get parish='Out of State' so "Alexandria, VA" is never
    # mistaken for Alexandria in Rapides Parish, and the LA map stays accurate.
    if not contributor_state or contributor_state == 'LA':
        parish = CITY_TO_PARISH.get(city.upper(), 'East Baton Rouge')
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
        'reportCode':         (row.get('ReportCode')         or '').strip(),
        'notes':              notes_raw,
        'isFilingFee':        any(p in ff_text for p in [
                                  'FILING FEE', 'QUALIFYING FEE',
                                  'QUALIFICATION FEE', 'FILING/QUALIFYING']),
    }

def _parse_expenditure_row(row):
    amt = float((row.get('ExpenditureAmt') or '').strip() or 0)
    if amt <= 0:
        return None
    city = (row.get('RecipientCity') or '').upper().strip()
    first = (row.get('FilerFirstName') or '').strip().rstrip(',').strip()
    last  = (row.get('FilerLastName')  or '').strip().rstrip(',').strip()
    filer = ' '.join(x for x in [first, last] if x)
    return {
        'contributor': (row.get('RecipientName') or 'Unknown').strip(),
        'city':        (row.get('RecipientCity') or '').strip(),
        'parish':      CITY_TO_PARISH.get(city, 'East Baton Rouge'),
        'amount':      round(amt, 2),
        'date':        parse_date(row.get('ExpenditureDate', '')),
        'candidate':   filer or 'Unknown',
        'party':       lookup_party(filer),
        'source':      'LA Ethics (Expenditure)',
        'description': (row.get('ExpenditureDescription') or '').strip(),
        'filerNumber': (row.get('FilerNumber') or '').strip(),
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

        url_map = CSV_URLS if report_type == 'contributions' else EXPENDITURE_URLS
        url = url_map.get(csv_key)
        if not url:
            raise ValueError(f'No URL for {report_type}/{csv_key}')

        set_status(status_key, 'downloading', f'Downloading {csv_key} from ethics.la.gov...')
        print(f'  Streaming {url}')

        parse_row = _parse_contribution_row if report_type == 'contributions' else _parse_expenditure_row

        # Open per-year gzip writers lazily as years are encountered
        year_writers = {}   # {year: open gzip file handle}
        year_counts  = {}
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
        print(f'  Wrote {total:,} records for years {sorted(year_counts.keys())}')
        gc.collect()
        set_status(status_key, 'ready', f'{total:,} records cached')


def fetch_for_cycle(cycle, report_type='contributions'):
    """Return records for the two-year window [cycle-1, cycle], loading only those years."""
    y = int(cycle)
    years_needed = [y - 1, y]
    csv_key = get_csv_key(cycle)

    # Download if either year is missing
    if not all(_year_is_fresh(yr, report_type) for yr in years_needed):
        download_and_cache(csv_key, report_type)

    records = _load_years(years_needed, report_type)
    gc.collect()
    return records


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

        if parsed.path not in ('/api/la-ethics', '/api/la-expenditures'):
            self.send_response(404)
            self._cors_headers()
            self.end_headers()
            return

        report_type = 'contributions' if parsed.path == '/api/la-ethics' else 'expenditures'
        cycle = params.get('cycle', ['2024'])[0]
        csv_key = get_csv_key(cycle)
        status_key = f'{report_type}_{csv_key}'

        # Gate on the PRIMARY year only.
        # The previous year (y-1) may live in a different 4-year CSV bundle; its absence
        # must not block serving current-year data.  It will be included automatically
        # once that bundle is eventually cached.
        y = int(cycle)
        years_wanted = [y - 1, y]   # ideal two-year window
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

        # Stream only the years that are actually on disk — skip any that aren't cached yet
        years_to_serve = [yr for yr in years_wanted if _year_is_fresh(yr, report_type)]
        try:
            self._stream_years_json(years_to_serve, report_type)
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f'  Streaming error for {report_type} cycle={cycle}: {e}')

    def _stream_years_json(self, years, report_type):
        """Stream per-year NDJSON cache files as a JSON array via chunked transfer-encoding.

        Peak RAM = O(1) per record.  No in-memory list, no json.dumps of the full dataset.
        All modern browsers reassemble chunked responses transparently before calling .json().
        """
        self.send_response(200)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Transfer-Encoding', 'chunked')
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

        try:
            wc(b'[')
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
                        wc((b'' if first else b',') + raw.encode('utf-8'))
                        first = False
            wc(b']')
            # Terminating chunk signals end of chunked body
            self.wfile.write(b'0\r\n\r\n')
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass   # client disconnected mid-stream

    def _cors_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')

    def _json(self, data):
        body = json.dumps(data, indent=2).encode('utf-8')
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
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

    # Warm the cache sequentially in one background thread — never two downloads at once
    print('  Pre-fetching recent contribution data in background (sequential)...')
    def _sequential_prefetch():
        # Only warm the most-recent range on startup; older ranges load on demand.
        for csv_key in ['2024-2027']:
            if not is_cached_fresh(csv_key, 'contributions'):
                try:
                    download_and_cache(csv_key, 'contributions')
                except Exception as e:
                    print(f'  Prefetch failed {csv_key}: {e}')
            gc.collect()
    threading.Thread(target=_sequential_prefetch, daemon=True).start()

    print()
    print('  Endpoints:')
    print('    GET /health                         -- server status')
    print('    GET /api/data-status?cycle=2024     -- cache status (poll this while loading)')
    print('    GET /api/la-ethics?cycle=2024       -- contributions')
    print('    GET /api/la-expenditures?cycle=2024 -- expenditures')
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
