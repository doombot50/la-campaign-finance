#!/usr/bin/env python3
"""
Louisiana Politician Party Lookup Builder
==========================================
Fetches Louisiana candidate data from the FEC API (federal races),
supplements with a curated list of state-level officials (Governor,
Legislature, statewide offices) since 2010, and writes the result to
la_politicians_lookup.json for use by la_ethics_server.py.

Usage:
    python build_politician_lookup.py [--api-key YOUR_FEC_KEY]

Requires no third-party packages (stdlib only).
"""

import urllib.request, urllib.parse, json, re, argparse, time, sys, os

OUT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'la_politicians_lookup.json')

# ── FEC config ─────────────────────────────────────────────────────────────────
FEC_BASE = 'https://api.open.fec.gov/v1'
DEFAULT_KEY = '9xkMNTQ7N9DP3bjyxyE29K40Xfji7ikIGJ4XsZxD'

PARTY_MAP = {
    'DEM': 'DEM', 'REP': 'REP',
    'DEMOCRATIC PARTY': 'DEM', 'REPUBLICAN PARTY': 'REP',
    'D': 'DEM', 'R': 'REP',
}

def normalize(name: str) -> str:
    """Uppercase, strip titles/punctuation, collapse whitespace."""
    name = name.upper()
    name = re.sub(r'\b(DR|MR|MRS|MS|JR|SR|II|III|IV|ESQ|PHD|MD)\.?\b', '', name)
    name = re.sub(r'[^A-Z\s]', ' ', name)
    return ' '.join(name.split())

def parse_fec_name(raw: str):
    """'SCALISE, STEVE' → ('STEVE', 'SCALISE').  Falls back to split."""
    raw = raw.strip()
    if ',' in raw:
        last, first = raw.split(',', 1)
        return normalize(first.strip()), normalize(last.strip())
    parts = raw.split()
    if len(parts) >= 2:
        return normalize(parts[0]), normalize(parts[-1])
    return normalize(raw), ''

# ── FEC fetch ──────────────────────────────────────────────────────────────────
def fetch_fec_candidates(api_key: str) -> list:
    """Pull all Louisiana federal candidates since 2010 from FEC."""
    records = []
    page = 1
    print('Fetching FEC candidates for Louisiana …')
    while True:
        params = urllib.parse.urlencode({
            'state': 'LA',
            'per_page': 100,
            'page': page,
            'sort': 'name',
            'min_first_file_date': '01/01/2010',
            'api_key': api_key,
        })
        url = f'{FEC_BASE}/candidates/?{params}'
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                data = json.load(r)
        except Exception as e:
            print(f'  FEC page {page} error: {e}')
            break

        results = data.get('results', [])
        records.extend(results)
        pagination = data.get('pagination', {})
        total_pages = pagination.get('pages', 1)
        print(f'  Page {page}/{total_pages} — {len(results)} records')

        if page >= total_pages:
            break
        page += 1
        time.sleep(0.3)   # be polite

    print(f'  Total FEC records: {len(records)}')
    return records

def fec_to_lookup(records: list) -> dict:
    lookup = {}
    for r in records:
        raw_party = r.get('party_full') or r.get('party') or ''
        party = PARTY_MAP.get(raw_party.upper()) or PARTY_MAP.get(r.get('party', '').upper())
        if not party:
            continue                         # skip unknowns
        first, last = parse_fec_name(r.get('name', ''))
        if not last:
            continue
        full_key = f'{first} {last}'.strip()
        entry = {
            'party': party,
            'first': first,
            'last': last,
            'office': r.get('office_full', ''),
            'district': r.get('district', ''),
            'election_years': r.get('election_years', []),
            'source': 'FEC',
        }
        # Store by full key (first + last)
        lookup[full_key] = entry
        # Also store by last-name-only key for quick fallback (may be overwritten by
        # later entries — that's fine, ambiguous last names need full matching anyway)
        lookup[last] = entry
    return lookup

# ── Curated state-level politicians ────────────────────────────────────────────
# Format: (first, last, party, office, years_active_approx)
CURATED = [
    # Governors
    ('JEFF',       'LANDRY',       'REP', 'Governor',                [2023]),
    ('JOHN BEL',   'EDWARDS',      'DEM', 'Governor',                [2016, 2020]),
    ('BOBBY',      'JINDAL',       'REP', 'Governor',                [2008, 2012]),
    ('KATHLEEN',   'BLANCO',       'DEM', 'Governor',                [2004]),
    # Lieutenant Governors
    ('BILLY',      'NUNGESSER',    'REP', 'Lt. Governor',            [2016, 2020, 2023]),
    ('JAY',        'DARDENNE',     'REP', 'Lt. Governor',            [2010, 2014]),
    ('MITCH',      'LANDRIEU',     'DEM', 'Lt. Governor',            [2004]),
    # Attorneys General
    ('LIZMURRILL', 'MURRILL',      'REP', 'Attorney General',        [2023]),
    ('LIZ',        'MURRILL',      'REP', 'Attorney General',        [2023]),
    ('JEFF',       'LANDRY',       'REP', 'Attorney General',        [2012, 2016, 2020]),
    ('BUDDY',      'CALDWELL',     'REP', 'Attorney General',        [2008]),
    # Secretaries of State
    ('NANCY',      'LANDRY',       'REP', 'Secretary of State',      [2023]),
    ('KYLE',       'ARDOIN',       'REP', 'Secretary of State',      [2018, 2019, 2023]),
    ('TOM',        'SCHEDLER',     'REP', 'Secretary of State',      [2010, 2015]),
    # Treasurers
    ('JOHN',       'SCHRODER',     'REP', 'Treasurer',               [2018, 2023]),
    ('RON',        'HENSON',       'REP', 'Treasurer',               [2010]),
    # Insurance Commissioner
    ('JIM',        'DONELON',      'REP', 'Insurance Commissioner',  [2010, 2016, 2020, 2023]),
    # Agriculture
    ('MIKE',       'STRAIN',       'REP', 'Agriculture Commissioner',[2008, 2012, 2016, 2020, 2023]),
    # State Senate leadership
    ('PAGE',       'CORTEZ',       'REP', 'State Senate President',  [2020, 2024]),
    ('JOHN',       'ALARIO',       'REP', 'State Senate President',  [2010, 2016]),
    ('NORBY',      'CHABERT',      'REP', 'State Senator',           [2010, 2016, 2020]),
    ('SHARON',     'HEWITT',       'REP', 'State Senator',           [2016, 2020, 2023]),
    ('BODI',       'WHITE',        'REP', 'State Senator',           [2010, 2016, 2020]),
    ('FRED',       'MILLS',        'REP', 'State Senator',           [2012, 2016, 2020]),
    ('FRANKLIN',   'FOIL',         'REP', 'State Senator',           [2012, 2016, 2020]),
    ('MIKE',       'REESE',        'REP', 'State Senator',           [2016, 2020]),
    ('ALAN',       'SEABAUGH',     'REP', 'State Senator',           [2012, 2016, 2020]),
    ('MARK',       'ABRAHAM',      'REP', 'State Senator',           [2012, 2016, 2020]),
    ('RICK',       'WARD',         'REP', 'State Senator',           [2016, 2020]),
    ('BETH',       'MIZELL',       'REP', 'State Senator',           [2016, 2020]),
    ('LANCE',      'HARRIS',       'REP', 'State Representative',    [2008, 2012, 2016, 2020]),
    ('GENE',       'REYNOLDS',     'DEM', 'State Representative',    [2012, 2016, 2020]),
    ('PATRICIA',   'SMITH',        'DEM', 'State Representative',    [2004, 2008, 2012, 2016]),
    ('KATRINA',    'JACKSON',      'DEM', 'State Senator',           [2012, 2016, 2020, 2023]),
    ('ROYCE',      'DUPLESSIS',    'DEM', 'State Senator',           [2020, 2023]),
    ('MANDIE',     'LANDRY',       'DEM', 'State Representative',    [2020, 2023]),
    ('MATTHEW',    'WILLARD',      'DEM', 'State Representative',    [2012, 2016, 2020, 2023]),
    ('TED',        'JAMES',        'DEM', 'State Representative',    [2012, 2016, 2020]),
    ('SAM',        'JONES',        'DEM', 'State Representative',    [2008, 2012, 2016]),
    # House Speakers
    ('CLAY',       'SCHEXNAYDER',  'REP', 'State House Speaker',     [2020, 2023]),
    ('TAYLOR',     'BARRAS',       'REP', 'State House Speaker',     [2016, 2020]),
    ('CHUCK',      'KLECKLEY',     'REP', 'State House Speaker',     [2012, 2016]),
    ('JIM',        'TUCKER',       'REP', 'State House Speaker',     [2008]),
    # Other statewide
    ('STEPHEN',    'WAGUESPACK',   'REP', 'Gubernatorial Candidate', [2023]),
    ('HUNTER',     'LUNDY',        'DEM', 'Gubernatorial Candidate', [2023]),
    ('GARY',       'CHAMBERS',     'DEM', 'US Senate Candidate',     [2022]),
    ('SHAWN',      'WILSON',       'DEM', 'Gubernatorial Candidate', [2023]),
    ('EDDIE',      'RISPONE',      'REP', 'Gubernatorial Candidate', [2019]),
    ('RALPH',      'ABRAHAM',      'REP', 'Gubernatorial Candidate', [2019]),
    # Additional House members
    ('CAMERON',    'HENRY',        'REP', 'State Representative',    [2004, 2008, 2012, 2016, 2020]),
    ('TONY',       'BACALA',       'REP', 'State Representative',    [2016, 2020]),
    ('RICK',       'EDMONDS',      'REP', 'State Representative',    [2016, 2020]),
    ('RAYMOND',    'GAROFALO',     'REP', 'State Representative',    [2012, 2016, 2020]),
    ('STEPHANIE',  'HILFERTY',     'REP', 'State Representative',    [2016, 2020]),
    ('JACK',       'DONAHUE',      'REP', 'State Representative',    [2008, 2012, 2016]),
    ('BEAU',       'BEAULLIEU',    'REP', 'State Representative',    [2016, 2020]),
    ('MIKE',       'FESI',         'REP', 'State Representative',    [2016, 2020]),
    ('ROBBY',      'CARTER',       'DEM', 'State Representative',    [2016, 2020]),
    ('ROBERT',     'CARTER',       'DEM', 'State Representative',    [2016, 2020]),
    ('THOMAS',     'MCMAHON',      'DEM', 'State Representative',    [2016, 2020]),
    # ── Louisiana State Senate (current + recent past) ──────────────────────────
    ('PAGE',       'CORTEZ',       'REP', 'State Senator',            [2016, 2020, 2024]),
    ('BOB',        'OWEN',         'REP', 'State Senator',            [2024]),
    ('EDWARD',     'PRICE',        'DEM', 'State Senator',            [2008, 2012, 2016, 2020, 2024]),
    ('SIDNEY',     'BARTHELEMY',   'DEM', 'State Senator',            [2020, 2024]),
    ('JIMMY',      'HARRIS',       'DEM', 'State Senator',            [2016, 2020, 2024]),
    ('GARY',       'CARTER',       'DEM', 'State Senator',            [2016, 2020, 2024]),
    ('PATRICK',    'CONNICK',      'REP', 'State Senator',            [2016, 2020, 2024]),
    ('KIRK',       'TALBOT',       'REP', 'State Senator',            [2012, 2016, 2020, 2024]),
    ('PATRICK',    'MCMATH',       'REP', 'State Senator',            [2020, 2024]),
    ('VALARIE',    'HODGES',       'REP', 'State Senator',            [2020, 2024]),
    ('LARRY',      'SELDERS',      'DEM', 'State Senator',            [2020, 2024]),
    ('REGINA',     'BARROW',       'DEM', 'State Senator',            [2012, 2016, 2020, 2024]),
    ('CALEB',      'KLEINPETER',   'REP', 'State Senator',            [2024]),
    ('EDDIE',      'LAMBERT',      'REP', 'State Senator',            [2008, 2012, 2016, 2020, 2024]),
    ('GREGORY',    'MILLER',       'REP', 'State Senator',            [2012, 2016, 2020, 2024]),
    ('ROBERT',     'ALLAIN',       'REP', 'State Senator',            [2016, 2020, 2024]),
    ('BLAKE',      'MIGUEZ',       'REP', 'State Senator',            [2024]),
    ('BRACH',      'MYERS',        'REP', 'State Senator',            [2024]),
    ('GERALD',     'BOUDREAUX',    'DEM', 'State Senator',            [2012, 2016, 2020, 2024]),
    ('BOB',        'HENSGENS',     'REP', 'State Senator',            [2016, 2020, 2024]),
    ('JEREMY',     'STINE',        'REP', 'State Senator',            [2024]),
    ('HEATHER',    'CLOUD',        'REP', 'State Senator',            [2016, 2020, 2024]),
    ('JAY',        'LUNEAU',       'DEM', 'State Senator',            [2016, 2020, 2024]),
    ('GLEN',       'WOMACK',       'REP', 'State Senator',            [2024]),
    ('STEWART',    'CATHEY',       'REP', 'State Senator',            [2016, 2020, 2024]),
    ('JAY',        'MORRIS',       'REP', 'State Senator',            [2016, 2020, 2024]),
    ('ADAM',       'BASS',         'REP', 'State Senator',            [2024]),
    ('BILL',       'WHEAT',        'REP', 'State Senator',            [2024]),
    ('THOMAS',     'PRESSLY',      'REP', 'State Senator',            [2020, 2024]),
    ('SAM',        'JENKINS',      'DEM', 'State Senator',            [2008, 2012, 2016, 2020, 2024]),
    ('DANNY',      'MCCORMICK',    'REP', 'State Senator',            [2020, 2024]),
    ('STEVEN',     'JACKSON',      'DEM', 'State Senator',            [2016, 2020, 2024]),
    ('TAMMY',      'PHELPS',       'DEM', 'State Senator',            [2016, 2020, 2024]),
    ('JOY',        'WALTERS',      'DEM', 'State Senator',            [2024]),
    ('DENNIS',     'BAMBURG',      'REP', 'State Senator',            [2020, 2024]),
    ('MICHAEL',    'MELERINE',     'REP', 'State Senator',            [2024]),
    ('LARRY',      'BAGLEY',       'REP', 'State Senator',            [2012, 2016, 2020, 2024]),
    ('RAYMOND',    'CREWS',        'REP', 'State Senator',            [2016, 2020, 2024]),
    ('DODIE',      'HORTON',       'REP', 'State Senator',            [2020, 2024]),
    ('WAYNE',      'MCMAHEN',      'REP', 'State Senator',            [2016, 2020, 2024]),
    ('RASHID',     'YOUNG',        'DEM', 'State Senator',            [2024]),
    ('CHRISTOPHER','TURNER',       'REP', 'State Senator',            [2024]),
    ('MICHAEL',    'ECHOLS',       'REP', 'State Senator',            [2016, 2020, 2024]),
    # ── Louisiana State House of Representatives ──────────────────────────────
    ('FOY',        'GADBERRY',     'REP', 'State Representative',     [2020, 2024]),
    ('ADRIAN',     'FISHER',       'DEM', 'State Representative',     [2024]),
    ('PAT',        'MOORE',        'DEM', 'State Representative',     [2020, 2024]),
    ('JEREMY',     'LACOMBE',      'REP', 'State Representative',     [2020, 2024]),
    ('FRANCIS',    'THOMPSON',     'REP', 'State Representative',     [2004, 2008, 2012, 2016, 2020, 2024]),
    ('NEIL',       'RISER',        'REP', 'State Representative',     [2012, 2016, 2020, 2024]),
    ('TRAVIS',     'JOHNSON',      'DEM', 'State Representative',     [2016, 2020, 2024]),
    ('GABE',       'FIRMENT',      'REP', 'State Representative',     [2020, 2024]),
    ('SHAUN',      'MENA',         'DEM', 'State Representative',     [2024]),
    ('RODNEY',     'SCHAMERHORN',  'REP', 'State Representative',     [2020, 2024]),
    ('JASON',      'DEWITT',       'REP', 'State Representative',     [2020, 2024]),
    ('ED',         'LARVADAIN',    'DEM', 'State Representative',     [2012, 2016, 2020, 2024]),
    ('DARYL',      'DESHOTEL',     'REP', 'State Representative',     [2016, 2020, 2024]),
    ('EDMOND',     'JORDAN',       'DEM', 'State Representative',     [2008, 2012, 2016, 2020, 2024]),
    ('CHARLES',    'OWEN',         'REP', 'State Representative',     [2020, 2024]),
    ('TROY',       'HEBERT',       'REP', 'State Representative',     [2004, 2008, 2012, 2016, 2020, 2024]),
    ('CHANCE',     'HENRY',        'REP', 'State Representative',     [2024]),
    ('JOSH',       'CARLSON',      'REP', 'State Representative',     [2020, 2024]),
    ('TEHMI',      'CHASSION',     'DEM', 'State Representative',     [2012, 2016, 2020, 2024]),
    ('ANNIE',      'SPELL',        'REP', 'State Representative',     [2016, 2020, 2024]),
    ('CHAD',       'BOYER',        'REP', 'State Representative',     [2024]),
    ('RYAN',       'BOURRIAQUE',   'REP', 'State Representative',     [2020, 2024]),
    ('JACOB',      'LANDRY',       'REP', 'State Representative',     [2020, 2024]),
    ('VINNEY',     'ST BLANC',     'REP', 'State Representative',     [2016, 2020, 2024]),
    ('BERYL',      'AMEDEE',       'REP', 'State Representative',     [2012, 2016, 2020, 2024]),
    ('JEROME',     'ZERINGUE',     'REP', 'State Representative',     [2016, 2020, 2024]),
    ('JESSICA',    'DOMANGUE',     'REP', 'State Representative',     [2020, 2024]),
    ('JOSEPH',     'ORGERON',      'REP', 'State Representative',     [2012, 2016, 2020, 2024]),
    ('BRYAN',      'FONTENOT',     'REP', 'State Representative',     [2016, 2020, 2024]),
    ('BETH',       'BILLINGS',     'REP', 'State Representative',     [2024]),
    ('SYLVIA',     'TAYLOR',       'DEM', 'State Representative',     [2012, 2016, 2020, 2024]),
    ('KEN',        'BRASS',        'DEM', 'State Representative',     [2016, 2020, 2024]),
    ('CHASITY',    'MARTINEZ',     'DEM', 'State Representative',     [2020, 2024]),
    ('DENISE',     'MARCELLE',     'DEM', 'State Representative',     [2008, 2012, 2016, 2020, 2024]),
    ('ROY',        'ADAMS',        'DEM', 'State Representative',     [2020, 2024]),
    ('BARBARA',    'CARPENTER',    'DEM', 'State Representative',     [2004, 2008, 2012, 2016, 2020]),
    ('KELLEE',     'DICKERSON',    'REP', 'State Representative',     [2024]),
    ('LAUREN',     'VENTRELLA',    'REP', 'State Representative',     [2020, 2024]),
    ('EMILY',      'CHENEVERT',    'REP', 'State Representative',     [2024]),
    ('TERRY',      'LANDRY',       'DEM', 'State Representative',     [2012, 2016, 2020]),
    ('DIXON',      'MCMAKIN',      'REP', 'State Representative',     [2020, 2024]),
    ('PAUL',       'SAWYER',       'REP', 'State Representative',     [2016, 2020, 2024]),
    ('BARBARA',    'FREIBERG',     'REP', 'State Representative',     [2016, 2020, 2024]),
    ('ROGER',      'WILDER',       'REP', 'State Representative',     [2024]),
    ('KIMBERLY',   'COATES',       'REP', 'State Representative',     [2020, 2024]),
    ('PETER',      'EGAN',         'REP', 'State Representative',     [2024]),
    ('JOHN',       'WYBLE',        'REP', 'State Representative',     [2024]),
    ('STEPHANIE',  'BERAULT',      'REP', 'State Representative',     [2024]),
    ('MARK',       'WRIGHT',       'REP', 'State Representative',     [2020, 2024]),
    ('JOHN',       'ILLG',         'REP', 'State Representative',     [2016, 2020, 2024]),
    ('DEBBIE',     'VILLIO',       'REP', 'State Representative',     [2016, 2020, 2024]),
    ('POLLY',      'THOMAS',       'REP', 'State Representative',     [2012, 2016, 2020, 2024]),
    ('JEFF',       'WILEY',        'REP', 'State Representative',     [2020, 2024]),
    ('LAURIE',     'SCHLEGEL',     'REP', 'State Representative',     [2016, 2020, 2024]),
    ('KYLE',       'GREEN',        'DEM', 'State Representative',     [2020, 2024]),
    ('TIMOTHY',    'KERNER',       'REP', 'State Representative',     [2016, 2020, 2024]),
    ('VINCENT',    'COX',          'REP', 'State Representative',     [2024]),
    ('NICHOLAS',   'MUSCARELLO',   'REP', 'State Representative',     [2016, 2020, 2024]),
    ('RODNEY',     'LYONS',        'DEM', 'State Representative',     [2008, 2012, 2016, 2020, 2024]),
    ('KATHY',      'EDMONSTON',    'REP', 'State Representative',     [2016, 2020, 2024]),
    ('KIM',        'CARVER',       'REP', 'State Representative',     [2020, 2024]),
    ('BRIAN',      'GLORIOSO',     'REP', 'State Representative',     [2016, 2020, 2024]),
    ('JOSEPH',     'STAGNI',       'REP', 'State Representative',     [2016, 2020, 2024]),
    ('ALONZO',     'KNOX',         'DEM', 'State Representative',     [2012, 2016, 2020, 2024]),
    ('SHANE',      'MACK',         'REP', 'State Representative',     [2024]),
    ('MARCUS',     'BRYANT',       'DEM', 'State Representative',     [2024]),
    ('ED',         'MURRAY',       'DEM', 'State Representative',     [2000, 2004, 2008]),
    ('AIMEE',      'FREEMAN',      'DEM', 'State Representative',     [2016, 2020, 2024]),
    ('CANDACE',    'NEWELL',       'DEM', 'State Representative',     [2020, 2024]),
    ('DANA',       'HENRY',        'DEM', 'State Representative',     [2020, 2024]),
    ('VANESSA',    'LAFLEUR',      'DEM', 'State Representative',     [2016, 2020, 2024]),
    ('DELISHA',    'BOYD',         'DEM', 'State Representative',     [2016, 2020, 2024]),
    ('MICHAEL',    'BAYHAM',       'REP', 'State Representative',     [2020, 2024]),
    ('JAY',        'GALLE',        'REP', 'State Representative',     [2020, 2024]),
    ('JACOB',      'BRAUD',        'REP', 'State Representative',     [2024]),
    ('PHILLIP',    'DEVILLIER',    'REP', 'State Representative',     [2016, 2020, 2024]),
    ('WILFORD',    'CARTER',       'DEM', 'State Representative',     [2008, 2012, 2016, 2020, 2024]),
    ('RAYMOND',    'GAROFALO',     'REP', 'State Representative',     [2012, 2016, 2020, 2024]),
    # ── Formal-name / nickname aliases ────────────────────────────────────────────
    # These cover cases where the Ethics report uses a formal first name that
    # differs from the "known as" name stored above (e.g. William vs Billy).
    # The enhanced lookup_party logic handles middle initials automatically, so
    # only true nickname↔formal-name mismatches need explicit entries here.
    ('ELIZABETH',   'MURRILL',       'REP', 'Attorney General',          [2023]),          # Liz
    ('WILLIAM',     'NUNGESSER',     'REP', 'Lt. Governor',              [2016, 2020, 2023]),  # Billy
    ('MICHAEL',     'REESE',         'REP', 'State Senator',             [2016, 2020]),    # Mike
    ('WILLIAM',     'WHEAT',         'REP', 'State Senator',             [2024]),          # Bill
    ('ROBERT',      'OWEN',          'REP', 'State Senator',             [2024]),          # Bob
    ('CRAIG',       'HENSGENS',      'REP', 'State Senator',             [2016, 2020, 2024]),  # Bob (middle)
    ('JOHN',        'MORRIS',        'REP', 'State Senator',             [2016, 2020, 2024]),  # Jay (middle)
    ('PATRICK',     'CORTEZ',        'REP', 'State Senate President',    [2020, 2024]),    # Page (middle)
    ('LAWRENCE',    'BAGLEY',        'REP', 'State Senator',             [2012, 2016, 2020, 2024]),  # Larry
    ('EDWARD',      'JAMES',         'DEM', 'State Representative',      [2012, 2016, 2020]),  # Ted
    ('MICHAEL',     'STRAIN',        'REP', 'Agriculture Commissioner',  [2008, 2012, 2016, 2020, 2023]),  # Mike
    ('JAMES',       'DONELON',       'REP', 'Insurance Commissioner',    [2010, 2016, 2020, 2023]),  # Jim
    ('WENDELL',     'LUNEAU',        'DEM', 'State Senator',             [2016, 2020, 2024]),  # Jay (middle)
    ('JOE',         'STAGNI',        'REP', 'State Representative',      [2016, 2020, 2024]),  # Joseph
    ('ANTHONY',     'BACALA',        'REP', 'State Representative',      [2016, 2020]),    # Tony
    ('PAULETTE',    'THOMAS',        'REP', 'State Representative',      [2012, 2016, 2020, 2024]),  # Polly
    ('MACK',        'WHITE',         'REP', 'State Senator',             [2010, 2016, 2020]),  # Bodi (nickname)
    ('JESSE',       'BASS',          'REP', 'State Senator',             [2024]),          # Adam (middle)
    ('JEFFREY',     'WILEY',         'REP', 'State Representative',      [2020, 2024]),    # Jeff
    ('RICHARD',     'WARD',          'REP', 'State Senator',             [2016, 2020]),    # Rick
    ('VINCENT',     'ST BLANC',      'REP', 'State Representative',      [2016, 2020, 2024]),  # Vinney (nickname)
    ('MICHAEL',     'FESI',          'REP', 'State Representative',      [2016, 2020]),    # Mike
    ('ROBERT',      'ARDOIN',        'REP', 'Secretary of State',        [2018, 2019, 2023]),  # Kyle (middle)
    # ── Additional current/recent legislators not previously listed ────────────
    ('HELENA',      'MORENO',        'DEM', 'State Senator',             [2020, 2024]),
    ('JOHN',        'STEFANSKI',     'REP', 'State Representative',      [2016, 2020, 2024]),
    ('JEAN PAUL',   'COUSSAN',       'REP', 'State Senator',             [2020, 2024]),
    ('TANNER',      'MAGEE',         'REP', 'State Representative',      [2012, 2016, 2020, 2024]),
    ('MACK',        'CORMIER',       'DEM', 'State Representative',      [2016, 2020, 2024]),
    ('CLEO',        'FIELDS',        'DEM', 'State Senator',             [2000, 2004, 2008, 2012, 2016, 2020, 2024]),
    ('TIMOTHY',     'TEMPLE',        'REP', 'Agriculture Commissioner',  [2023]),
    ('JOHN',        'MILKOVICH',     'DEM', 'State Senator',             [2016, 2020]),
    ('RICHARD',     'NELSON',        'REP', 'Commissioner of Administration', [2020, 2023]),
    ('JASON ROGERS','WILLIAMS',      'DEM', 'Orleans DA / State Rep',    [2014, 2016, 2020]),
    ('STUART',      'BISHOP',        'REP', 'State Representative',      [2012, 2016, 2020, 2024]),
    ('SCHUYLER',    'MARVIN',        'REP', 'State Representative',      [2004, 2008, 2012, 2016, 2020, 2024]),
    # ── Nickname / alternate-name aliases for local officials ──────────────────
    # Ethics reports use formal names; SoS stored a different form for these.
    ('TIMOTHY',    'SOIGNET',      'REP', 'Sheriff (Terrebonne)',      [2016, 2020]),  # SoS: TIM SOIGNET
    ('JERRY',      'TURLICH',      'REP', 'Sheriff (Jefferson)',       [2016, 2020]),  # SoS: GERALD JERRY TURLICH
    ('JOSEPH',     'WAITZ',        'REP', 'District Attorney',         [2016, 2020]),  # SoS: JOE WAITZ
    ('THOMAS',     'ARCENEAUX',    'REP', 'State Representative',      [2008, 2012]),  # SoS: TOM ARCENEAUX
    ('TOM',        'ARCENEAUX',    'REP', 'State Representative',      [2008, 2012]),
    ('JEAN PAUL',  'MORRELL',      'DEM', 'State Senator',             [2008, 2012, 2016, 2020]),  # SoS: J P MORRELL
    ('OLIVER',     'THOMAS',       'DEM', 'New Orleans City Council',  [2004, 2006]),  # not in SoS
    ('GARY',       'GUILLORY',     'REP', 'State Representative',      [2008, 2012, 2016]),  # "Stitch"
    ('JULIOUS',    'SIMS',         'DEM', 'State Representative',      [2008, 2012, 2016, 2020]),  # Julious Collin Sims
    # ── New Orleans City Council (current + recent) ──────────────────────────────
    # All NOCC seats are effectively Democrat; SoS often classifies them as NP
    ('AIMEE',      'MCCARRON',     'DEM', 'New Orleans City Council',  [2022, 2024]),
    ('LESLI',      'HARRIS',       'DEM', 'New Orleans City Council',  [2022, 2024]),
    ('FREDDIE',    'KING',         'DEM', 'New Orleans City Council',  [2022, 2024]),
    ('JARED',      'BROSSETT',     'DEM', 'New Orleans City Council',  [2018, 2022]),
    ('JOE',        'GIARRUSSO',    'DEM', 'New Orleans City Council',  [2018, 2022]),
    ('JOSEPH',     'GIARRUSSO',    'DEM', 'New Orleans City Council',  [2018, 2022]),
    ('KRISTIN',    'GISLESON PALMER', 'DEM', 'New Orleans City Council', [2010, 2014, 2018]),
    ('STACY',      'HEAD',         'DEM', 'New Orleans City Council',  [2006, 2010, 2014]),
    ('SUSAN',      'GUIDRY',       'DEM', 'New Orleans City Council',  [2010, 2014]),
    ('CYNTHIA',    'HEDGE-MORRELL','DEM', 'New Orleans City Council',  [2006, 2010]),
    ('JACKIE',     'CLARKSON',     'DEM', 'New Orleans City Council',  [2010, 2014]),
    ('JAMES',      'GRAY',         'DEM', 'New Orleans City Council',  [2010, 2014]),
    ('WILLIE',     'CARTER',       'DEM', 'New Orleans City Council',  [2018, 2022]),
    # Parish/local level notable politicians
    ('LATOYA',     'CANTRELL',     'DEM', 'Mayor (New Orleans)',      [2018, 2022]),
    ('MITCH',      'LANDRIEU',     'DEM', 'Mayor (New Orleans)',      [2010, 2014]),
    ('RAY',        'NAGIN',        'DEM', 'Mayor (New Orleans)',      [2002, 2006]),
    ('SHARON WESTON', 'BROOME',    'DEM', 'Mayor-President (EBR)',    [2016, 2020]),
    ('SHARON',     'BROOME',       'DEM', 'Mayor-President (EBR)',    [2016, 2020]),
    ('JEFF',       'JEFF',         'DEM', 'Mayor-President (EBR)',    []),  # placeholder
    ('MIKE',       'MICHOT',       'REP', 'State Senator',           [2004, 2008]),
    # Additional US House members
    ('CEDRIC',     'RICHMOND',     'DEM', 'House',                   [2010, 2012, 2014, 2016, 2018, 2020]),
    ('BILL',       'JEFFERSON',    'DEM', 'House',                   []),
    ('CHARLIE',    'BOUSTANY',     'REP', 'House',                   [2004, 2006, 2008, 2010, 2012, 2014]),
    ('RODNEY',     'ALEXANDER',    'REP', 'House',                   [2002, 2004, 2006, 2008, 2010, 2012]),
    ('JIM',        'MCCRERY',      'REP', 'House',                   []),
    # Current / recent US House members (may not appear in FEC first-file-date filter)
    ('MIKE',       'JOHNSON',      'REP', 'House',                   [2016, 2018, 2020, 2022, 2024]),
    ('STEVE',      'SCALISE',      'REP', 'House',                   [2008, 2010, 2012, 2014, 2016, 2018, 2020, 2022, 2024]),
    ('GARRET',     'GRAVES',       'REP', 'House',                   [2014, 2016, 2018, 2020, 2022, 2024]),
    ('JULIA',      'LETLOW',       'REP', 'House',                   [2021, 2022, 2024]),
    ('CLAY',       'HIGGINS',      'REP', 'House',                   [2016, 2018, 2020, 2022, 2024]),
    ('TROY',       'CARTER',       'DEM', 'House',                   [2021, 2022, 2024]),
    ('MIKE',       'EZELL',        'REP', 'House',                   [2022, 2024]),
    # US Senate
    ('JOHN',       'KENNEDY',      'REP', 'US Senate',               [2016, 2022]),
    ('BILL',       'CASSIDY',      'REP', 'US Senate',               [2014, 2020]),
    ('DAVID',      'VITTER',       'REP', 'US Senate',               [2004, 2010]),
    ('MARY',       'LANDRIEU',     'DEM', 'US Senate',               [2002, 2008, 2014]),
    ('JOHN',       'BREAUX',       'DEM', 'US Senate',               []),
    ('BOB',        'LIVINGSTON',   'REP', 'House',                   []),
    ('RICHARD',    'BAKER',        'REP', 'House',                   []),
]

# ── Louisiana Secretary of State election candidate fetch ──────────────────────
SOS_BASE = 'https://voterportal.sos.la.gov/ElectionResults/ElectionResults/Data'
SOS_KEEP_PARTIES = {'REP', 'DEM'}

def _sos_fetch(blob: str):
    """GET a blob from the SoS results API; return parsed JSON or None."""
    url = f'{SOS_BASE}?blob={blob}'
    req = urllib.request.Request(url, headers={'User-Agent': 'la-campaign-finance-lookup/1.0'})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.load(r)
    except Exception as e:
        print(f'    SoS fetch error ({blob}): {e}')
        return None

def _parse_sos_desc(desc: str):
    """'Full Name (PARTY)' -> (clean_name, party) or (None, None).

    Handles quotation-mark nicknames in three positions:
      leading  : '"Jeff" Landry'         -> 'Jeff Landry'
      middle   : 'Patrick "Dat" Barthel' -> 'Patrick Barthel'
      suffix   : 'Charles "Chuck" Jones' -> 'Charles Jones'

    Only returns entries whose party is REP or DEM.
    """
    m = re.match(r'^(.*)\s+\(([A-Z]+)\)$', desc.strip())
    if not m:
        return None, None
    party = m.group(2)
    if party not in SOS_KEEP_PARTIES:
        return None, None
    name_raw = m.group(1).strip()
    # Strip surrounding double-quote nicknames but keep the content for
    # leading position ("Jeff" Landry → Jeff Landry) and drop entirely
    # for middle position where they duplicate a word that follows.
    # Strategy: replace any "word(s)" group with its contents, then collapse spaces.
    name_clean = re.sub(r'"([^"]+)"', r'\1', name_raw)
    name_clean = ' '.join(name_clean.split())
    return name_clean, party

def _extract_races(data: dict) -> list:
    """Pull the Race list out of a RacesCandidates response (handles dict/list)."""
    races = data.get('Races', {}).get('Race', [])
    if isinstance(races, dict):
        races = [races]
    return races or []


def _ingest_races(races: list, raw: dict, elec_date: str) -> None:
    """Add every REP/DEM candidate from *races* into the *raw* accumulator.
    Most-recent election date wins on conflicts.
    """
    for race in races:
        office = (race.get('SpecificTitle') or race.get('GeneralTitle') or '').strip()
        choices = race.get('Choice', [])
        if isinstance(choices, dict):
            choices = [choices]
        for choice in choices:
            name_raw, party = _parse_sos_desc(choice.get('Desc', ''))
            if not name_raw or not party:
                continue
            norm = normalize(name_raw)
            if len(norm.split()) < 2:
                continue
            existing = raw.get(norm)
            if not existing or elec_date > existing['date']:
                raw[norm] = {'party': party, 'office': office, 'date': elec_date}


def fetch_sos_candidates(min_year: int = 2010) -> dict:
    """Scrape every REP/DEM candidate from the SoS graphical results portal
    for all elections since *min_year*.

    Two-pass strategy:
      Pass 1 — multiparish file for every election (statewide offices, all
               105 state House seats, all 39 state Senate seats, district
               judges, BESE, constitutional amendments).
      Pass 2 — parish-level files (64 parishes) for "major" 4-year elections,
               i.e. any election whose multiparish data includes at least one
               state legislative race (Level 200/205).  This captures sheriffs,
               assessors, coroners, parish presidents, clerks of court, etc.

    Most-recent election date wins when the same name appears multiple times
    (handles party switches gracefully).
    """
    election_data = _sos_fetch('ElectionDates.htm')
    if not election_data:
        print('  SoS: could not fetch election list — skipping')
        return {}

    elections = [
        e for e in election_data['Dates']['Date']
        if int(e['ElectionDate'][-4:]) >= min_year
    ]
    print(f'  SoS: {len(elections)} elections since {min_year}')

    raw: dict = {}               # norm_name -> {party, office, date}
    major_election_blobs = []    # (blob_date, elec_date) for pass 2

    # ── Pass 1: multiparish (statewide + legislature) ─────────────────────────
    print('  Pass 1: multiparish races …')
    for i, elec in enumerate(elections):
        mm, dd, yyyy = elec['ElectionDate'].split('/')
        blob_date = f'{yyyy}{mm}{dd}'
        data = _sos_fetch(f'{blob_date}/RacesCandidates_Multiparish.htm')
        if not data:
            continue

        races = _extract_races(data)
        _ingest_races(races, raw, elec['ElectionDate'])

        # Detect major 4-year elections: have state legislative races (level 200/205)
        LEGIS_LEVELS = {'200', '205'}
        if any(r.get('OfficeLevel', '') in LEGIS_LEVELS for r in races):
            major_election_blobs.append((blob_date, elec['ElectionDate']))

        if (i + 1) % 25 == 0 or (i + 1) == len(elections):
            print(f'    … {i + 1}/{len(elections)} done')
        time.sleep(0.15)

    print(f'  Pass 1 complete: {len(raw)} candidates from multiparish races')
    print(f'  {len(major_election_blobs)} major 4-year elections detected for parish-level pass')

    # ── Pass 2: parish-level races for major elections ─────────────────────────
    # Covers sheriffs, assessors, coroners, parish presidents, clerks of court,
    # city council, school board, etc.
    if major_election_blobs:
        print('  Pass 2: parish-level races (64 parishes × major elections) …')
        total_parish_requests = 0
        for blob_date, elec_date in major_election_blobs:
            # Get the parish list for this election (IDs are zero-padded 01–64)
            parishes_data = _sos_fetch(f'{blob_date}/ParishesInElection.htm')
            if not parishes_data:
                parish_ids = [f'{p:02d}' for p in range(1, 65)]
            else:
                parishes = parishes_data.get('ParishesInElection', {}).get('Parish', [])
                if isinstance(parishes, dict):   # single-parish election
                    parishes = [parishes]
                parish_ids = [p['ParishValue'] for p in parishes]

            for pid in parish_ids:
                blob = f'{blob_date}/RacesCandidates/ByParish_{pid}.htm'
                data = _sos_fetch(blob)
                if data:
                    _ingest_races(_extract_races(data), raw, elec_date)
                total_parish_requests += 1
                time.sleep(0.10)

            print(f'    … {elec_date} done ({len(parish_ids)} parishes)')

        print(f'  Pass 2 complete: {total_parish_requests} parish requests, '
              f'{len(raw)} total candidates')

    # ── Build final lookup dict ────────────────────────────────────────────────
    lookup: dict = {}
    for norm_name, info in raw.items():
        entry = {
            'party':         info['party'],
            'source':        'SoS',
            'office':        info['office'],
            'election_date': info['date'],
        }
        lookup[norm_name] = entry
        last = norm_name.split()[-1]
        if last not in lookup:
            lookup[last] = entry

    n_rep = sum(1 for v in raw.values() if v['party'] == 'REP')
    n_dem = sum(1 for v in raw.values() if v['party'] == 'DEM')
    print(f'  SoS total: {len(raw)} unique candidates  (REP={n_rep}, DEM={n_dem})')
    return lookup


def curated_to_lookup() -> dict:
    lookup = {}
    for first, last, party, office, years in CURATED:
        fn = normalize(first)
        ln = normalize(last)
        if not ln:
            continue
        full_key = f'{fn} {ln}'.strip()
        entry = {
            'party': party,
            'first': fn,
            'last': ln,
            'office': office,
            'election_years': years,
            'source': 'curated',
        }
        lookup[full_key] = entry
        if ln not in lookup:      # don't overwrite FEC entry for last-name key
            lookup[ln] = entry
    return lookup

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description='Build Louisiana politician party lookup')
    ap.add_argument('--api-key',  default=DEFAULT_KEY, help='FEC API key')
    ap.add_argument('--skip-fec', action='store_true',  help='Skip FEC fetch (offline mode)')
    ap.add_argument('--skip-sos', action='store_true',  help='Skip SoS election fetch')
    ap.add_argument('--min-year', type=int, default=2010,
                    help='Earliest election year to pull from SoS (default: 2010)')
    args = ap.parse_args()

    lookup = {}

    # 1. Curated state politicians (base layer — hand-verified entries)
    print('Loading curated state politicians …')
    curated = curated_to_lookup()
    lookup.update(curated)
    print(f'  {len([k for k in curated if " " in k])} curated entries')

    # 2. FEC federal candidates (overrides curated where both exist)
    if not args.skip_fec:
        fec_records = fetch_fec_candidates(args.api_key)
        fec_lookup = fec_to_lookup(fec_records)
        lookup.update(fec_lookup)
        print(f'  {len([k for k in fec_lookup if " " in k])} FEC entries merged')

    # 3. Louisiana SoS election results — every REP/DEM candidate since min_year.
    #    This is the broadest layer: covers state legislature, statewide offices,
    #    local races (sheriffs, assessors, judges, parish presidents, etc.) and
    #    all losing candidates — all the names that actually appear in Ethics filings.
    #    SoS is last so it wins on conflicts (it is the authoritative LA source).
    if not args.skip_sos:
        print(f'\nFetching Louisiana SoS candidate data (since {args.min_year}) …')
        sos_lookup = fetch_sos_candidates(min_year=args.min_year)
        n_new = sum(1 for k in sos_lookup if ' ' in k and k not in lookup)
        n_upd = sum(1 for k in sos_lookup if ' ' in k and k in lookup)
        lookup.update(sos_lookup)
        print(f'  {n_new} new entries added, {n_upd} existing entries updated from SoS')

    # 4. Summary
    full_name_entries = {k: v for k, v in lookup.items() if ' ' in k}
    dem = sum(1 for v in full_name_entries.values() if v['party'] == 'DEM')
    rep = sum(1 for v in full_name_entries.values() if v['party'] == 'REP')
    print(f'\nTotal unique politicians: {len(full_name_entries)}  (D={dem}, R={rep})')

    with open(OUT_FILE, 'w') as f:
        json.dump(lookup, f, indent=2, default=list)
    print(f'Saved -> {OUT_FILE}')

if __name__ == '__main__':
    main()
