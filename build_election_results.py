#!/usr/bin/env python3
"""Build la_election_results.json — election outcomes for the win/loss feature.

Scrapes the Louisiana SoS results portal (2010+), tracking EVERY candidacy per
normalized name so we can:
  - detect same-name ambiguity (two different people) and exclude them,
  - de-prioritize party-internal offices (DSCC/RSCC/DPEC/RPEC) so a legislator's
    real seat is shown instead of their party-committee race,
  - pick a single representative race per (trustworthy) candidate.

Output entry per normalized name:
  { party, office, outcome, vote_pct, election_date,
    n_candidacies, n_real_offices, parties:[...],
    ambiguous: bool, party_office_only: bool }

Confirmed blob patterns:
  {date}/RacesCandidates_Multiparish.htm  +  {date}/Votes_Multiparish.htm
  {date}/RacesCandidates/ByParish_{pid}.htm + {date}/VotesParish/Votes_{pid}.htm

Local offices (Sheriff, Assessor, School Board -- District N, ...) carry no
parish in their SoS title, so the same title repeats in every parish and would
collapse into a single race. We qualify them with the parish the per-parish file
was scraped from -> "Vermilion Parish Sheriff". State/federal/legislative/
judicial-district offices are left untouched (they span parishes and already
carry a unique district; they also live only in the Multiparish file).

NOTE: this runs incrementally (nightly — see nightly-data.yml). A date already
in la_candidacies_raw.json.gz is skipped once it is settled; it is scraped again
while it is still moving (see dates_to_scrape):
  - upcoming, or held within the last REFRESH_DAYS — candidates withdraw and
    election-night returns become official results;
  - held, but no outcome was ever recorded — it was scraped before the vote.
A re-scrape REPLACES that date's rows, and is discarded if it looks like a
partial fetch (see rescrape_ok), so a flaky night can't erase good results.

Parish qualification only applies to dates as they are scraped. To re-qualify
the full history, delete la_candidacies_raw.json.gz first and re-scrape from
scratch (rescrape-elections.yml).
"""
import urllib.request, json, re, time, sys, gzip
from collections import defaultdict
from datetime import date, timedelta

RAW_FILE = 'la_candidacies_raw.json.gz'

SOS_BASE = 'https://voterportal.sos.la.gov/ElectionResults/ElectionResults/Data'
MIN_YEAR = 2000
# Keep re-scraping an election for this long after it is held: election-night
# returns are unofficial until certified, and recounts can move them.
REFRESH_DAYS = 45

def fetch(blob, timeout=25, retries=2):
    url = f'{SOS_BASE}?blob={blob}'
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'la-campaign-finance/1.0'})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode('utf-8', 'replace'))
        except Exception:
            if attempt < retries:
                time.sleep(0.5)
            else:
                return None

def normalize(name):
    name = name.upper()
    name = re.sub(r'\b(DR|MR|MRS|MS|JR|SR|II|III|IV|ESQ|PHD|MD)\.?\b', '', name)
    name = re.sub(r'[^A-Z\s]', ' ', name)
    return ' '.join(name.split())

def parse_desc(desc):
    m = re.match(r'^(.*)\s+\(([A-Z]+)\)$', desc.strip())
    if not m:
        return None, None
    name = re.sub(r'"([^"]+)"', r'\1', m.group(1).strip())
    return ' '.join(name.split()), m.group(2)

def as_list(x):
    if x is None: return []
    return x if isinstance(x, list) else [x]

PARTY_NORM = {'DEM':'DEM','REP':'REP','D':'DEM','R':'REP',
              'LBT':'LBT','GRN':'GRN','IND':'IND','NOPTY':'IND','OTHER':'OTH'}

def norm_party(p):
    return PARTY_NORM.get(p, 'OTH')

# ── Office classification ───────────────────────────────────────────────────
def is_party_office(office):
    o = office.upper()
    if re.search(r'\b(DSCC|RSCC|DPEC|RPEC)\b', o): return True
    if 'CENTRAL COMMITTEE' in o or 'EXECUTIVE COMMITTEE' in o or 'PARTY COMMITTEE' in o:
        return True
    return False

def office_rank(office):
    """Lower = more significant. Party offices last."""
    o = office.upper()
    if is_party_office(o): return 90
    if 'PRESIDENT' in o and 'CITY' not in o and 'PARISH' not in o and 'VICE' not in o: return 1
    if re.search(r'\bU\.?\s?S\.?\s+SENAT|UNITED STATES SENAT', o): return 2
    if re.search(r'\bU\.?\s?S\.?\s+REP|CONGRESS', o): return 3
    if 'GOVERNOR' in o and 'LT' not in o and 'LIEUTENANT' not in o: return 4
    if 'LIEUTENANT GOVERNOR' in o or 'LT. GOVERNOR' in o or 'LT GOVERNOR' in o: return 5
    if any(x in o for x in ['ATTORNEY GENERAL','SECRETARY OF STATE','TREASURER',
                            'INSURANCE COMMISSIONER','AGRICULTURE','COMMISSIONER OF']): return 6
    if 'STATE SENATOR' in o: return 7
    if 'STATE REPRESENTATIVE' in o: return 8
    if any(x in o for x in ['MAYOR','SHERIFF','DISTRICT ATTORNEY','PARISH PRESIDENT',
                            'CITY-PARISH PRESIDENT','ASSESSOR','CLERK OF COURT','CORONER']): return 10
    if 'JUSTICE' in o or 'JUDGE' in o: return 12
    if any(x in o for x in ['COUNCIL','POLICE JUR','SCHOOL BOARD','BESE','ALDERM',
                            'CONSTABLE','MARSHAL','CHIEF OF POLICE','BOARD']): return 15
    return 20

# Louisiana's 64 parishes are numbered alphabetically (01-64); the SoS per-parish
# result files key off that number. Local race titles ("Sheriff", "Member of
# School Board -- District 7") omit the parish, so the same title repeats in every
# parish and collapses into one race unless we qualify it with where it was scraped.
LA_PARISHES = {
    '01':'Acadia','02':'Allen','03':'Ascension','04':'Assumption','05':'Avoyelles',
    '06':'Beauregard','07':'Bienville','08':'Bossier','09':'Caddo','10':'Calcasieu',
    '11':'Caldwell','12':'Cameron','13':'Catahoula','14':'Claiborne','15':'Concordia',
    '16':'De Soto','17':'East Baton Rouge','18':'East Carroll','19':'East Feliciana',
    '20':'Evangeline','21':'Franklin','22':'Grant','23':'Iberia','24':'Iberville',
    '25':'Jackson','26':'Jefferson','27':'Jefferson Davis','28':'Lafayette',
    '29':'Lafourche','30':'La Salle','31':'Lincoln','32':'Livingston','33':'Madison',
    '34':'Morehouse','35':'Natchitoches','36':'Orleans','37':'Ouachita',
    '38':'Plaquemines','39':'Pointe Coupee','40':'Rapides','41':'Red River',
    '42':'Richland','43':'Sabine','44':'St. Bernard','45':'St. Charles',
    '46':'St. Helena','47':'St. James','48':'St. John the Baptist','49':'St. Landry',
    '50':'St. Martin','51':'St. Mary','52':'St. Tammany','53':'Tangipahoa',
    '54':'Tensas','55':'Terrebonne','56':'Union','57':'Vermilion','58':'Vernon',
    '59':'Washington','60':'Webster','61':'West Baton Rouge','62':'West Carroll',
    '63':'West Feliciana','64':'Winn',
}

def is_local_office(office):
    """Parish-contained local office whose title omits its parish (so the title
    repeats across parishes). State/federal/legislative/judicial-district offices
    carry their own unique district and span parishes, so they're excluded — and
    because multi-parish races live only in the Multiparish file, single-parish
    files never see them anyway."""
    o = office.upper()
    if is_party_office(o): return False
    if 'JUSTICE OF THE PEACE' in o or 'CONSTABLE' in o: return True   # ward-level, not judges
    if 'BESE' in o or 'PUBLIC SERVICE' in o or 'BOARD OF ELEMENTARY' in o:
        return False                                                  # statewide board districts
    return office_rank(o) in (10, 15, 20)   # parish/municipal offices + local "other"

def has_municipality(office):
    """True when the title already names its city/town/village, so it's already
    distinct without a parish prefix."""
    o = office.upper()
    return any(k in o for k in ('CITY OF', 'TOWN OF', 'VILLAGE OF'))

def qualify_office(office, parish):
    """Prefix a local office with its parish: "Sheriff" -> "Vermilion Parish
    Sheriff". Titles that start with "Parish " (e.g. "Parish President") absorb
    the word so we don't double it -> "Vermilion Parish President"."""
    if not parish: return office
    if office.upper().startswith('PARISH '):
        return f'{parish} {office}'
    return f'{parish} Parish {office}'

def date_key(ed):  # 'MM/DD/YYYY' -> 'YYYYMMDD'
    mm, dd, yy = ed.split('/')
    return yy + mm + dd

# ── Scrape: collect ALL candidacies per normalized name ─────────────────────
# `into`: norm_name -> [ {party, office, outcome, vote_pct, date, rank, party_office} ]
def ingest(cand_data, votes_data, ed, parish, into):
    if not cand_data: return 0
    vote_idx, race_tot = {}, defaultdict(int)
    for vr in as_list((votes_data or {}).get('Races', {}).get('Race')):
        rid = vr['ID']
        for vc in as_list(vr.get('Choice')):
            vt = int(vc.get('VoteTotal') or 0)
            vote_idx[(rid, vc['ID'])] = (vt, vc.get('Outcome', ''))
            race_tot[rid] += vt
    n = 0
    for r in as_list(cand_data.get('Races', {}).get('Race')):
        rid = r['ID']
        office = (r.get('SpecificTitle') or r.get('GeneralTitle') or '').strip()
        # Per-parish files hold single-parish races; qualify local offices with
        # the parish so e.g. every parish's "Sheriff" is its own race.
        if parish and is_local_office(office) and not has_municipality(office):
            office = qualify_office(office, parish)
        for ch in as_list(r.get('Choice')):
            name, party = parse_desc(ch.get('Desc', ''))
            if not name: continue
            norm = normalize(name)
            if len(norm.split()) < 2: continue
            vt, outcome = vote_idx.get((rid, ch['ID']), (0, ''))
            tot = race_tot.get(rid, 0)
            pct = round(100 * vt / tot, 1) if tot else None
            into[norm].append({
                'party': norm_party(party), 'office': office, 'outcome': outcome,
                'vote_pct': pct, 'date': ed, 'rank': office_rank(office),
                'party_office': is_party_office(office),
            })
            n += 1
    return n

def parse_date(ed):  # 'MM/DD/YYYY' -> date
    mm, dd, yy = ed.split('/')
    return date(int(yy), int(mm), int(dd))

def dates_to_scrape(portal_dates, by_date, today, refresh_days=REFRESH_DAYS):
    """Which of the portal's election dates to scrape on this run.

    by_date maps each date already in the raw file to its candidacy rows. A date
    is scraped when it is new, or still moving:
      - upcoming, or held within the last `refresh_days`;
      - held, with no outcome recorded on any row (scraped before the vote, and
        no run since — without this a pre-election scrape would stay blank
        forever).
    Older dates with results are settled and skipped; a few blank outcomes among
    recorded ones (11/04/2014 has two) are how the portal left them.
    """
    cutoff = today - timedelta(days=refresh_days)
    todo = []
    for ed in portal_dates:
        rows = by_date.get(ed)
        if rows is None or parse_date(ed) >= cutoff or not any(c.get('outcome') for c in rows):
            todo.append(ed)
    return sorted(todo, key=date_key)

def rescrape_ok(old_rows, new_rows):
    """Whether a re-scrape of one date may replace the rows we already hold.

    fetch() returns None on a transient SoS failure, which silently drops that
    file's candidacies. Results don't un-happen and candidate lists only shrink
    by the odd withdrawal, so a re-scrape with fewer recorded outcomes, or under
    80% of the rows, is a partial fetch — keep what we have and try again on the
    next run. Returns (ok, reason).
    """
    if not new_rows:
        return False, 'no candidacies returned'
    if len(new_rows) < 0.8 * len(old_rows):
        return False, f'row count fell {len(old_rows)} -> {len(new_rows)}'
    old_out = sum(1 for c in old_rows if c.get('outcome'))
    new_out = sum(1 for c in new_rows if c.get('outcome'))
    if new_out < old_out:
        return False, f'recorded outcomes fell {old_out} -> {new_out}'
    return True, ''

def replace_date(cands, ed, fresh):
    """Swap every candidacy on date `ed` for the freshly scraped `fresh`
    (norm -> rows), keeping each name's list chronological and dropping names
    left with no candidacies (a withdrawn first-time candidate)."""
    for norm in list(cands):
        kept = [c for c in cands[norm] if c.get('date') != ed]
        if kept:
            cands[norm] = kept
        else:
            del cands[norm]
    for norm, rows in fresh.items():
        cands.setdefault(norm, []).extend(rows)
        cands[norm].sort(key=lambda c: date_key(c['date']))   # stable

def scrape_date(ed, bd):
    """Scrape one election date -> ({norm: [candidacy, ...]}, n_multiparish, n_parish)."""
    fresh = defaultdict(list)
    n1 = ingest(fetch(f'{bd}/RacesCandidates_Multiparish.htm'),
                fetch(f'{bd}/Votes_Multiparish.htm'), ed, '', fresh)
    n2 = 0
    pe = fetch(f'{bd}/ParishesInElection.htm')
    pids = ([p['ParishValue'] for p in as_list(pe.get('ParishesInElection', {}).get('Parish'))]
            if pe else [f'{p:02d}' for p in range(1, 65)])
    for pid in pids:
        parish = LA_PARISHES.get(str(pid).zfill(2), '')
        n2 += ingest(fetch(f'{bd}/RacesCandidates/ByParish_{pid}.htm'),
                     fetch(f'{bd}/VotesParish/Votes_{pid}.htm'), ed, parish, fresh)
        time.sleep(0.04)
    return fresh, n1, n2

def main():
    # ── Load existing raw candidacies for incremental re-run ───────────────────
    import os
    existing = {}
    if os.path.exists(RAW_FILE):
        with gzip.open(RAW_FILE, 'rt', encoding='utf-8') as _f:
            existing = json.load(_f)
    cands = {norm: list(cs) for norm, cs in existing.items()}
    by_date = defaultdict(list)
    for cs in existing.values():
        for c in cs:
            by_date[c.get('date', '')].append(c)
    if existing:
        print(f'Loaded {len(existing):,} names from existing {RAW_FILE}')
        print(f'Already-scraped election dates: {len(by_date)}')

    el = fetch('ElectionDates.htm')
    if not el:
        print('FATAL: could not fetch election list'); sys.exit(1)
    date2blob = {}
    for d in el['Dates']['Date']:
        mm, dd, yy = d['ElectionDate'].split('/')
        if int(yy) >= MIN_YEAR:
            date2blob[d['ElectionDate']] = f'{yy}{mm}{dd}'
    dates = dates_to_scrape(date2blob, by_date, date.today())
    n_new = sum(1 for ed in dates if ed not in by_date)
    print(f'{n_new} NEW elections to scrape since {MIN_YEAR}, '
          f'{len(dates) - n_new} unsettled to refresh.')

    t0 = time.time()
    for i, ed in enumerate(dates):
        fresh, n1, n2 = scrape_date(ed, date2blob[ed])
        if ed in by_date:
            ok, why = rescrape_ok(by_date[ed], [c for cs in fresh.values() for c in cs])
            if not ok:
                print(f'  {ed}: re-scrape discarded ({why}) — keeping existing rows')
                continue
        replace_date(cands, ed, fresh)
        if ed in by_date or (i + 1) % 10 == 0 or i == len(dates) - 1:
            print(f'  {i+1}/{len(dates)} {ed}: +{n1}mp +{n2}parish | '
                  f'{len(cands)} names | {time.time()-t0:.0f}s')

    if cands == existing:
        # Nothing moved: leave the committed files byte-identical (a rewritten
        # .gz differs by its header timestamp) so the nightly commits nothing.
        print('No change to candidacies — outputs left as they are.')
        return

    # Persist raw candidacies so the collapse logic can be re-tuned later
    # WITHOUT re-scraping the SoS portal.
    with gzip.open(RAW_FILE, 'wt', encoding='utf-8') as f:
        json.dump(cands, f)
    print(f'Saved raw candidacies -> {RAW_FILE}')
    collapse(cands)

def collapse(candidacies):
    """Collapse all candidacies per name into one representative entry.

    Representative race = MOST RECENT non-party-office race (party offices like
    DSCC/RSCC/DPEC/RPEC are deprioritized so a sitting legislator shows their
    real seat, not a party-committee race). Ties broken by office significance.
    """
    out = {}
    n_ambig = 0
    for norm, cs in candidacies.items():
        real = [c for c in cs if not c['party_office']]
        pool = real if real else cs
        parties = sorted({c['party'] for c in pool})

        # Ambiguity: 2+ major parties among the pool whose date ranges OVERLAP
        # (a clean party switch is monotonic in time -> still one person).
        ambiguous = False
        if len([p for p in parties if p in ('DEM', 'REP')]) >= 2:
            by_party = defaultdict(list)
            for c in pool:
                if c['party'] in ('DEM', 'REP'):
                    by_party[c['party']].append(date_key(c['date']))
            r = {p: (min(v), max(v)) for p, v in by_party.items()}
            ps = list(r)
            ambiguous = not (r[ps[0]][1] < r[ps[1]][0] or r[ps[1]][1] < r[ps[0]][0])
        if ambiguous:
            n_ambig += 1

        # Most recent race in the pool; tie-break by most significant office.
        rep = max(pool, key=lambda c: (int(date_key(c['date'])), -c['rank']))

        # Ambiguity: suspicious office downgrade in the most-recent race.
        # e.g. "Shawn Wilson" ran for Governor (rank 4) in 2023 but a *different*
        # Shawn Wilson ran for Police Juror (rank 15) in 2025.  A real person
        # almost never moves to a dramatically less significant office, so a
        # rank gap > 8 between historical best and most-recent race signals a
        # name collision with a same-party candidate.
        if not ambiguous:
            best_rank = min(c['rank'] for c in pool)
            if rep['rank'] - best_rank > 8:
                ambiguous = True
                n_ambig += 1

        out[norm] = {
            'party': rep['party'], 'office': rep['office'], 'outcome': rep['outcome'],
            'vote_pct': rep['vote_pct'], 'election_date': rep['date'],
            'n_candidacies': len(cs), 'n_real_offices': len(real),
            'parties': parties, 'ambiguous': ambiguous,
            'party_office_only': not real,
        }

    with open('la_election_results.json', 'w') as f:
        json.dump(out, f, indent=0)
    print(f'Wrote la_election_results.json: {len(out)} names '
          f'({n_ambig} ambiguous, '
          f'{sum(1 for v in out.values() if v["party_office_only"])} party-office-only)')


if __name__ == '__main__':
    if '--recollapse' in sys.argv:
        # Re-run collapse from saved raw candidacies (no scraping)
        with gzip.open(RAW_FILE, 'rt', encoding='utf-8') as f:
            collapse(json.load(f))
    else:
        main()
