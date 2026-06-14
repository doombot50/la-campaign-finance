#!/usr/bin/env python3
"""
build_donor_entities.py — conservative donor identity resolution (A+B+C)
========================================================================
The donor field is 1.1M records / ~243K distinct name strings, fragmented by
punctuation, dropped middle initials, and nicknames ("Edward L. Rispone",
"Edward Rispone", "Eddie Rispone" are one man). The only corroborating signal
the state populates is ZIP (employer/occupation/address are blank). So:

PEOPLE — clustered by union-find WITHIN a (last-name, generational-suffix, zip5)
bucket. Two first-names in the same bucket merge when they are:
  Tier A  identical after punctuation/case normalization
  Tier B  one is an initial/prefix of the other ("E" ↔ "EDWARD"); middle
          initials fall out automatically (first+last keyed)
  Tier C  members of the same curated nickname group ("EDDIE" ↔ "EDWARD")
HARD STOPS (never merged): different ZIP (13% of names span >1 ZIP — could be a
mover or two people, unknowable), or different generational suffix (Sr vs Jr is
parent vs child, often same household ZIP).

ORGANIZATIONS — clustered by normalized name ACROSS zips (a company legitimately
gives from multiple office ZIPs; "NORPAC LLC" is one donor at 7 ZIPs).

Every cluster records its raw variants so the merge is disclosed and auditable
in the UI (same posture as filer aliases / party provenance / transfer flags).
A wrong merge misattributes one person's money to another, so we under-merge by
design rather than guess.

Output: .la_cache/la_donor_entities.json.gz
Stdlib only. Run after refresh; before build_insights.py.
"""
import gzip
import json
import glob
import os
import re
from collections import defaultdict
from datetime import datetime, timezone

import la_ethics_server as srv

OUT_PATH = os.path.join(srv.CACHE_DIR, 'la_donor_entities.json.gz')

_ORG_TOKENS = re.compile(
    r'\b(LLC|L L C|INC|PAC|FUND|CORP|CORPORATION|LLP|LP|LTD|COMMITTEE|ASSOCIATION|'
    r'ASSN|COMPANY|CO|UNION|GROUP|TRUST|PARTNERS|PARTNERSHIP|ENTERPRISES|HOLDINGS|'
    r'FOUNDATION|INSTITUTE|SOCIETY|LEAGUE|COALITION|ALLIANCE|FEDERATION|COUNCIL|'
    r'BANK|FIRM|CLINIC|HOSPITAL|UNIVERSITY|LOCAL|CHAPTER|ACTION|VICTORY|PARTY|'
    r'ADVOCACY|NETWORK|SYSTEMS?|SERVICES|PROPERTIES|REALTY|INSURANCE|ENERGY)\b', re.I)
_GEN_ANY = {'JR', 'SR'}                 # never a middle initial — detect anywhere
_GEN_END = {'II', 'III', 'IV'}          # roman generationals — only trust at end
# Professional credentials: not identity-defining ("John Smith MD" == "John
# Smith"), and dangerous as a pseudo-surname (made JACK BREAUX MD collide with
# JOHN BRASWELL MD). Stripped from the end before first/last are computed.
_CREDS = {'MD', 'DDS', 'DMD', 'DO', 'PHD', 'JD', 'ESQ', 'CPA', 'RN', 'MBA', 'MS',
          'DVM', 'OD', 'PE', 'PA', 'NP', 'APRN', 'MSW', 'LCSW', 'CFA', 'DC', 'MPH'}


def _parse_person(norm):
    """(first, last, suffix) for a person name, or None if not 2+ name tokens.
    Strips trailing credentials; pulls JR/SR from anywhere and II/III/IV from the
    end as the generational suffix (part of identity, so Sr never merges Jr)."""
    toks = norm.split()
    while len(toks) > 2 and toks[-1] in _CREDS:
        toks.pop()
    suffix = ''
    for g in list(toks):
        if g in _GEN_ANY:
            suffix = g
            toks = [t for t in toks if t != g]
            break
    if not suffix and len(toks) > 2 and toks[-1] in _GEN_END:
        suffix = toks[-1]
        toks = toks[:-1]
    if len(toks) < 2:
        return None
    return toks[0], toks[-1], suffix

# Bulk-data placeholders that are not nameable donors — excluded entirely. These
# are LA Ethics aggregate/adjustment rows, not contributions from a real entity.
_NONDONOR = re.compile(
    r'\bNON LA\b|NEGATIVE DISBURSE|UNITEMIZED|\bAGGREGATE\b|\bANONYMOUS\b|'
    r'MISCELLANEOUS|NO NAME (GIVEN|PROVIDED|LISTED)|NOT (PROVIDED|LISTED|APPLICABLE)', re.I)

# Curated common nickname groups (each line = one identity, all forms equivalent).
# Conservative: only well-established formal↔familiar pairs, never ambiguous ones
# we can't anchor. Same-ZIP requirement guards the rare cross-name collision.
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
    "THADDEUS THAD", "THERESA TERESA TERRY TERRI", "GARY",
]
_NICK = {}
for _grp in _NICK_GROUPS:
    forms = _grp.split()
    canon = forms[0]
    for f in forms:
        _NICK.setdefault(f, canon)  # first group wins on the rare overlap


def _norm(n):
    n = n.upper()
    n = re.sub(r"[.,'\"()]", ' ', n)
    n = re.sub(r'[^A-Z0-9 &/-]', ' ', n)
    return re.sub(r'\s+', ' ', n).strip()


def _first_canon(first):
    """Nickname-canonical first name, for Tier-C linking only."""
    return _NICK.get(first, first)


class _UF:
    def __init__(self): self.p = {}
    def find(self, x):
        self.p.setdefault(x, x)
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]; x = self.p[x]
        return x
    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb: self.p[ra] = rb


def main():
    # variant string -> aggregated record stats, plus its parse
    variants = {}   # raw_upper -> {'total','count','zips':Counter,'norm','is_org'}
    for path in sorted(glob.glob(os.path.join(srv.CACHE_DIR, 'contributions_yr*.json.gz'))):
        with gzip.open(path, 'rt', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except json.JSONDecodeError:
                    continue
                raw = (r.get('contributor') or '').strip().upper()
                if not raw or raw == 'UNKNOWN' or _NONDONOR.search(raw):
                    continue
                amt = float(r.get('amount') or 0)
                if amt <= 0:
                    continue
                # exclude committee transfers + clerk filing fees from donor view
                if r.get('isTransfer') or r.get('isFilingFee'):
                    continue
                z = (r.get('contributorZip') or '').strip()[:5]
                z = z if z.isdigit() and len(z) == 5 else ''
                v = variants.get(raw)
                if v is None:
                    nm = _norm(raw)
                    v = variants[raw] = {'total': 0.0, 'count': 0,
                                         'zips': defaultdict(float),
                                         'norm': nm,
                                         'is_org': bool(_ORG_TOKENS.search(nm))}
                v['total'] += amt
                v['count'] += 1
                v['zips'][z] += amt

    # ── cluster ────────────────────────────────────────────────────────────
    # People: union-find within (last, suffix, zip). Orgs: key by normalized name.
    uf = _UF()
    org_key = {}        # raw -> org cluster id (normalized name)
    person_buckets = defaultdict(list)   # (last,suffix,zip) -> [(raw, first, firstcanon)]

    for raw, v in variants.items():
        if v['is_org']:
            org_key[raw] = 'ORG:' + v['norm']
            continue
        parsed = _parse_person(v['norm'])
        if parsed is None:
            org_key[raw] = 'ONE:' + v['norm']   # single token — its own id
            continue
        first, last, suffix = parsed
        fc = _first_canon(first)
        # one variant can span several ZIPs; bucket each (zip) share separately,
        # but assign the raw to its dominant ZIP for clustering identity
        dom_zip = max(v['zips'].items(), key=lambda kv: kv[1])[0] if v['zips'] else ''
        person_buckets[(last, suffix, dom_zip)].append((raw, first, fc))

    # within each person bucket, union first-name-compatible variants
    for bucket, members in person_buckets.items():
        for i in range(len(members)):
            uf.find(members[i][0])
            for j in range(i + 1, len(members)):
                _, fi, fci = members[i]
                _, fj, fcj = members[j]
                same = (
                    fi == fj or
                    fci == fcj or                                  # nickname group
                    (len(fi) == 1 and fj.startswith(fi)) or        # initial ↔ full
                    (len(fj) == 1 and fi.startswith(fj))
                )
                if same:
                    uf.union(members[i][0], members[j][0])

    # ── assemble clusters ────────────────────────────────────────────────────
    clusters = defaultdict(list)   # cluster_id -> [raw,...]
    for raw in variants:
        if raw in org_key:
            clusters[org_key[raw]].append(raw)
        else:
            clusters['P:' + uf.find(raw)].append(raw)

    donors = {}
    for cid, raws in clusters.items():
        total = sum(variants[r]['total'] for r in raws)
        count = sum(variants[r]['count'] for r in raws)
        # canonical display: most complete (token count) then highest $ spelling
        canon = max(raws, key=lambda r: (len(r.split()), variants[r]['total']))
        zips = sorted({z for r in raws for z in variants[r]['zips'] if z})
        donors[cid] = {
            'name': canon,
            'kind': 'org' if cid.startswith(('ORG:', 'ONE:')) else 'person',
            'total': round(total, 2),
            'count': count,
            'n_variants': len(raws),
            'variants': sorted(raws) if len(raws) > 1 else [],
            'zips': zips[:5],
        }

    out = {
        'built_at': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
        'count': len(donors),
        'method': 'A+B+C: punctuation/case + initial + curated nickname, merged '
                  'only within same last-name, generational suffix, and ZIP '
                  '(organizations merge by name across ZIPs). Merges disclosed; '
                  'cross-ZIP and Sr/Jr never merged.',
        'donors': donors,
    }
    tmp = OUT_PATH + '.tmp'
    with gzip.open(tmp, 'wt', encoding='utf-8') as f:
        json.dump(out, f, separators=(',', ':'))
    os.replace(tmp, OUT_PATH)

    merged = sum(1 for d in donors.values() if d['n_variants'] > 1)
    print(f'Wrote {OUT_PATH}')
    print(f'  raw variant strings:  {len(variants):,}')
    print(f'  resolved donors:      {len(donors):,}')
    print(f'  multi-variant donors: {merged:,}')
    top = sorted(donors.values(), key=lambda d: -d['total'])[:8]
    print('  top resolved donors:')
    for d in top:
        print(f'    ${d["total"]:>13,.0f}  {d["name"][:38]:38} '
              f'({d["n_variants"]} variant{"s" if d["n_variants"]!=1 else ""}, {d["kind"]})')


if __name__ == '__main__':
    main()
