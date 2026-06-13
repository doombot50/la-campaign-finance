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

NOTE: this runs incrementally — already-scraped election dates in
la_candidacies_raw.json.gz are skipped, so the parish qualification only applies
to NEW dates. To re-qualify the full history, delete la_candidacies_raw.json.gz
first and re-scrape from scratch.
"""
import urllib.request, json, re, time, sys, gzip
from collections import defaultdict

RAW_FILE = 'la_candidacies_raw.json.gz'

SOS_BASE = 'https://voterportal.sos.la.gov/ElectionResults/ElectionResults/Data'
MIN_YEAR = 2000

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
candidacies = defaultdict(list)   # norm_name -> [ {party, office, outcome, vote_pct, date, rank, party_office} ]

def ingest(cand_data, votes_data, ed, parish=''):
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
            candidacies[norm].append({
                'party': norm_party(party), 'office': office, 'outcome': outcome,
                'vote_pct': pct, 'date': ed, 'rank': office_rank(office),
                'party_office': is_party_office(office),
            })
            n += 1
    return n

def main():
    # ── Load existing raw candidacies for incremental re-run ───────────────────
    import os
    if os.path.exists(RAW_FILE):
        import gzip as _gz
        with _gz.open(RAW_FILE, 'rt', encoding='utf-8') as _f:
            existing = json.load(_f)
        # Find which election dates are already in the raw data
        already_seen = set()
        for cs in existing.values():
            for c in cs:
                already_seen.add(c.get('date', ''))
        # Pre-populate candidacies from existing data
        for norm, cs in existing.items():
            candidacies[norm].extend(cs)
        print(f'Loaded {len(existing):,} names from existing {RAW_FILE}')
        print(f'Already-scraped election dates: {len(already_seen)}')
    else:
        already_seen = set()

    el = fetch('ElectionDates.htm')
    if not el:
        print('FATAL: could not fetch election list'); sys.exit(1)
    date2blob = {}
    for d in el['Dates']['Date']:
        mm, dd, yy = d['ElectionDate'].split('/')
        ed = d['ElectionDate']
        if int(yy) >= MIN_YEAR and ed not in already_seen:
            date2blob[ed] = f'{yy}{mm}{dd}'
    dates = sorted(date2blob, key=date_key)
    print(f'{len(dates)} NEW elections to scrape since {MIN_YEAR}.')

    t0 = time.time()
    for i, ed in enumerate(dates):
        bd = date2blob[ed]
        n1 = ingest(fetch(f'{bd}/RacesCandidates_Multiparish.htm'),
                    fetch(f'{bd}/Votes_Multiparish.htm'), ed)
        n2 = 0
        pe = fetch(f'{bd}/ParishesInElection.htm')
        pids = ([p['ParishValue'] for p in as_list(pe.get('ParishesInElection', {}).get('Parish'))]
                if pe else [f'{p:02d}' for p in range(1, 65)])
        for pid in pids:
            parish = LA_PARISHES.get(str(pid).zfill(2), '')
            n2 += ingest(fetch(f'{bd}/RacesCandidates/ByParish_{pid}.htm'),
                         fetch(f'{bd}/VotesParish/Votes_{pid}.htm'), ed, parish)
            time.sleep(0.04)
        if (i + 1) % 10 == 0 or i == len(dates) - 1:
            print(f'  {i+1}/{len(dates)} {ed}: +{n1}mp +{n2}parish | '
                  f'{len(candidacies)} names | {time.time()-t0:.0f}s')

    # Persist raw candidacies so the collapse logic can be re-tuned later
    # WITHOUT re-scraping the SoS portal.
    with gzip.open(RAW_FILE, 'wt', encoding='utf-8') as f:
        json.dump(candidacies, f)
    print(f'Saved raw candidacies -> {RAW_FILE}')
    collapse(candidacies)


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

    json.dump(out, open('la_election_results.json', 'w'), indent=0)
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
