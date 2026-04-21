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

    Matching strategy (requires >= 2 tokens to avoid false positives):
      0. Comma-swap "LAST, FIRST [suffix]"  ("Kerner, Jr." handled gracefully)
      1. Exact normalized full-name match   ("STEVE SCALISE")
      2. First token + last token           ("AIMEE ADATTO FREEMAN" → "AIMEE FREEMAN")
      3. Skip leading single-letter token   ("J CAMERON HENRY" → "CAMERON HENRY")
      4. Strip single-letter middle tokens  ("ALAN T SEABAUGH" → "ALAN SEABAUGH")
    """
    if not name or name == 'Unknown':
        return 'OTH'

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
        return 'REP'
    if any(s in _up for s in _DEM_SIGNALS):
        return 'DEM'

    # 0. Handle "LASTNAME, FIRSTNAME [suffix]" — try swapped form first
    if ',' in name:
        raw_parts = name.split(',', 1)
        swapped = f'{raw_parts[1].strip()} {raw_parts[0].strip()}'
        norm_swapped = _normalize_name(swapped)
        if len(norm_swapped.split()) >= 2:
            entry = _POLITICIAN_LOOKUP.get(norm_swapped)
            if entry:
                return entry.get('party', 'OTH')

    # Normalize the full name (strips honorifics, punctuation, collapses whitespace)
    norm = _normalize_name(name)
    if not norm:
        return 'OTH'
    tokens = norm.split()

    # 1. Exact normalized full-name match
    if len(tokens) >= 2:
        entry = _POLITICIAN_LOOKUP.get(norm)
        if entry:
            return entry.get('party', 'OTH')

    # Extended matching for names with middle names / initials / nicknames
    if len(tokens) >= 3:
        # 2. First token + last token only
        #    e.g. "AIMEE ADATTO FREEMAN" → "AIMEE FREEMAN"
        #    e.g. "THOMAS ALEXANDER PRESSLY" → "THOMAS PRESSLY"
        first_last = f'{tokens[0]} {tokens[-1]}'
        entry = _POLITICIAN_LOOKUP.get(first_last)
        if entry:
            return entry.get('party', 'OTH')

        # 3. Skip leading single-letter initial ("J CAMERON HENRY" → "CAMERON HENRY")
        if len(tokens[0]) == 1:
            rest = ' '.join(tokens[1:])
            entry = _POLITICIAN_LOOKUP.get(rest)
            if entry:
                return entry.get('party', 'OTH')
            # Also try first+last of remaining tokens ("M KIRK TALBOT" → "KIRK TALBOT")
            fl_rest = f'{tokens[1]} {tokens[-1]}'
            if fl_rest != rest:
                entry = _POLITICIAN_LOOKUP.get(fl_rest)
                if entry:
                    return entry.get('party', 'OTH')

        # 4. Strip single-letter middle tokens, keep all multi-letter tokens
        #    e.g. "ALAN T SEABAUGH" → "ALAN SEABAUGH"
        #    e.g. "TIMOTHY P KERNER" → "TIMOTHY KERNER"
        stripped = [tokens[0]] + [t for t in tokens[1:-1] if len(t) > 1] + [tokens[-1]]
        if len(stripped) < len(tokens) and len(stripped) >= 2:
            key = ' '.join(stripped)
            entry = _POLITICIAN_LOOKUP.get(key)
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
        parish = (
            CITY_TO_PARISH.get(city.upper())
            or _zip_to_parish_fallback(zip_raw)
            or 'East Baton Rouge'
        )
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
    city    = (row.get('RecipientCity')  or '').upper().strip()
    zip_raw = (row.get('RecipientZip')   or '').strip()
    first = (row.get('FilerFirstName') or '').strip().rstrip(',').strip()
    last  = (row.get('FilerLastName')  or '').strip().rstrip(',').strip()
    filer = ' '.join(x for x in [first, last] if x)
    return {
        'contributor': (row.get('RecipientName') or 'Unknown').strip(),
        'city':        (row.get('RecipientCity') or '').strip(),
        'parish':      (
            CITY_TO_PARISH.get(city)
            or _zip_to_parish_fallback(zip_raw)
            or 'East Baton Rouge'
        ),
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
