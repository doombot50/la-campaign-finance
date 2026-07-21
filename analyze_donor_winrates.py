#!/usr/bin/env python3
"""
analyze_donor_winrates.py — "The donors who never lose" (exploratory)
=====================================================================
Inverts build_money_wins.py: instead of asking whether the top FUNDRAISER wins,
ask which DONORS have the best record of backing eventual winners.

A "bet" is one resolved donor entity's dollars to one candidate in one valid
race, counting only checks dated ON OR BEFORE election day (money after the
result is known is not a bet). Races reuse the money-wins validity filters:
grouped on (date, office); exactly one 'Elected' winner; >= 2 candidates;
non-federal; no candidate flagged `ambiguous` in la_election_results; no
party-committee rows. Each contribution is attributed to the candidate's
NEAREST UPCOMING valid race within 4 years — unlike money-wins' in-cycle
window, this avoids the cycle-boundary artifact where a Nov-2019 check for a
Jan-2020 runoff would vanish, and guarantees each dollar counts toward at most
one race.

Donor identity is the resolved entity table (la_donor_entities.json.gz):
nickname-folded people clustered within last-name+suffix+ZIP, orgs merged
across ZIPs. Committee transfers and clerk filing fees are excluded — bets
are outside money, not money committees pass around.

Per donor we report:
  bets      distinct valid races the donor put money into (pre-election)
  win%      their LEAD candidate (largest dollars in the race) won
  any-win%  any candidate they funded in the race won (hedges pay off)
  hedged    races where they funded >= 2 rival candidates
  inc%      share of bets on incumbents (prior 'Elected' for the same office)
  open-win% win rate restricted to NON-incumbent lead bets (prescience test)
  lead-days dollar-weighted mean days-before-election of their money

Reads .la_cache/contributions_yr*.json.gz + la_donor_entities.json.gz and the
committed la_candidacies_raw.json.gz / la_election_results.json.
Writes la_donor_winrates.json (full per-donor table for qualified donors) and
prints the story tables. Stdlib only.
"""
import gzip
import glob
import json
import os
import re
import sys
from bisect import bisect_left
from collections import defaultdict
from datetime import date

import la_ethics_server as srv
from build_money_wins import build_races, is_federal, tier_of

BASE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(BASE, '.la_cache')

MIN_BETS = 20          # a record needs volume before "never loses" means anything
MAX_LOOKAHEAD_D = 1461  # attribute a check to a race at most 4 years out

# Same placeholder screen as build_donor_entities.py — aggregate rows, not donors.
_NONDONOR = re.compile(
    r'\bNON LA\b|NEGATIVE DISBURSE|UNITEMIZED|\bAGGREGATE\b|\bANONYMOUS\b|'
    r'MISCELLANEOUS|NO NAME (GIVEN|PROVIDED|LISTED)|NOT (PROVIDED|LISTED|APPLICABLE)', re.I)


def _d(iso):
    y, m, dd = int(iso[:4]), int(iso[5:7]), int(iso[8:10])
    return date(y, m, dd)


def _d_us(us):
    mm, dd, yy = us.split('/')
    return date(int(yy), int(mm), int(dd))


def load_valid_races():
    """(date, office) races passing the money-wins validity screen ->
    candidate norm-name -> sorted [(edate, race_id, won, tier, office)]."""
    with gzip.open(os.path.join(BASE, 'la_candidacies_raw.json.gz'), 'rt') as f:
        raw = json.load(f)
    with open(os.path.join(BASE, 'la_election_results.json')) as f:
        results = json.load(f)

    races = build_races(raw)
    cand_races = defaultdict(list)   # norm name -> [(edate, rid, won, tier, office)]
    n_valid = 0
    # incumbency: winner history per (office) -> sorted list of win dates
    office_wins = defaultdict(list)  # (norm, office) -> [edate,...] wins
    for (edate, office), cands in races.items():
        winners = [c for c in cands if c['outcome'] == 'Elected']
        if len(winners) == 1:
            office_wins[(winners[0]['name'], office)].append(_d_us(edate))

    for (edate, office), cands in races.items():
        winners = [c for c in cands if c['outcome'] == 'Elected']
        if len(winners) != 1 or len(cands) < 2:
            continue
        if is_federal(winners[0]['rank']):
            continue
        if any(results.get(c['name'], {}).get('ambiguous') for c in cands):
            continue
        n_valid += 1
        ed = _d_us(edate)
        rid = f'{edate}|{office}'
        tier = tier_of(winners[0]['rank'])
        for c in cands:
            won = c['outcome'] == 'Elected'
            # incumbent: won this same office at an earlier date
            inc = any(w < ed for w in office_wins.get((c['name'], office), ()))
            cand_races[c['name']].append((ed, rid, won, tier, office, inc))

    for v in cand_races.values():
        v.sort(key=lambda t: t[0])
    return cand_races, n_valid


def load_donor_resolver():
    """raw contributor string (upper) -> (cluster_id, canonical name, kind)."""
    with gzip.open(os.path.join(CACHE, 'la_donor_entities.json.gz'), 'rt') as f:
        ents = json.load(f)['donors']
    raw2cid = {}
    meta = {}
    for cid, d in ents.items():
        meta[cid] = (d['name'], d['kind'])
        for v in (d['variants'] or [d['name']]):
            raw2cid[v] = cid
    return raw2cid, meta


def main():
    cand_races, n_valid = load_valid_races()
    print(f'valid races: {n_valid:,}; candidates with >=1 valid race: {len(cand_races):,}',
          file=sys.stderr)
    raw2cid, meta = load_donor_resolver()
    print(f'donor resolver: {len(raw2cid):,} variants -> {len(meta):,} entities',
          file=sys.stderr)

    # bets[cid][rid] = {cand_norm: [dollars, weighted_days]}
    bets = defaultdict(lambda: defaultdict(dict))
    race_info = {}          # rid -> (tier,)
    n_rows = n_matched = 0
    total_bet = win_bet = 0.0   # all bet dollars vs dollars on eventual winners

    for path in sorted(glob.glob(os.path.join(CACHE, 'contributions_yr*.json.gz'))):
        with gzip.open(path, 'rt', encoding='utf-8') as f:
            for line in f:
                try:
                    r = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if r.get('isTransfer') or r.get('isFilingFee'):
                    continue
                amt = float(r.get('amount') or 0)
                if amt <= 0:
                    continue
                cand = r.get('candidate')
                if not cand or cand == 'Unknown':
                    continue
                norm = srv._norm_name(cand)
                runs = cand_races.get(norm)
                if not runs:
                    continue
                raw = (r.get('contributor') or '').strip().upper()
                if not raw or raw == 'UNKNOWN' or _NONDONOR.search(raw):
                    continue
                dt = r.get('date') or ''
                if len(dt) < 10:
                    continue
                try:
                    cdate = _d(dt)
                except ValueError:
                    continue
                n_rows += 1
                # nearest upcoming valid race within the lookahead window
                i = bisect_left(runs, (cdate,))
                if i == len(runs):
                    continue
                ed, rid, won, tier, office, inc = runs[i]
                if (ed - cdate).days > MAX_LOOKAHEAD_D:
                    continue
                cid = raw2cid.get(raw)
                if cid is None:
                    continue
                n_matched += 1
                cell = bets[cid][rid].setdefault(norm, [0.0, 0.0, won, inc])
                cell[0] += amt
                cell[1] += amt * (ed - cdate).days
                race_info[rid] = (tier,)
                total_bet += amt
                if won:
                    win_bet += amt

    print(f'rows dated+candidate-matched: {n_rows:,}; attributed as bets: {n_matched:,}',
          file=sys.stderr)

    # ── per-donor aggregation ────────────────────────────────────────────────
    table = []
    for cid, races in bets.items():
        n = len(races)
        if n < MIN_BETS:
            continue
        wins = anywins = hedged = inc_bets = open_bets = open_wins = 0
        dollars = 0.0
        wdays = 0.0
        tiers = defaultdict(int)
        for rid, cands in races.items():
            lead_norm, lead = max(cands.items(), key=lambda kv: kv[1][0])
            amt_r = sum(v[0] for v in cands.values())
            dollars += amt_r
            wdays += sum(v[1] for v in cands.values())
            if len(cands) > 1:
                hedged += 1
            if lead[2]:
                wins += 1
            if any(v[2] for v in cands.values()):
                anywins += 1
            if lead[3]:
                inc_bets += 1
            else:
                open_bets += 1
                if lead[2]:
                    open_wins += 1
            tiers[race_info[rid][0]] += 1
        name, kind = meta[cid]
        table.append({
            'name': name, 'kind': kind, 'bets': n,
            'wins': wins, 'win_rate': round(wins / n, 3),
            'any_win_rate': round(anywins / n, 3),
            'hedged': hedged,
            'inc_share': round(inc_bets / n, 3),
            'open_bets': open_bets, 'open_wins': open_wins,
            'open_win_rate': round(open_wins / open_bets, 3) if open_bets else None,
            'dollars': round(dollars),
            'avg_days_before': round(wdays / dollars) if dollars else None,
            'tiers': dict(tiers),
        })

    table.sort(key=lambda d: (-d['win_rate'], -d['bets']))

    base = {
        'n_valid_races': n_valid,
        'bet_dollars': round(total_bet),
        'winner_dollar_share': round(win_bet / total_bet, 3) if total_bet else None,
        'qualified_donors': len(table),
        'min_bets': MIN_BETS,
    }
    with open(os.path.join(BASE, 'la_donor_winrates.json'), 'w') as f:
        json.dump({'baseline': base, 'donors': table}, f,
                  separators=(',', ':'), ensure_ascii=False)

    # ── report ───────────────────────────────────────────────────────────────
    mean_wr = sum(d['win_rate'] for d in table) / len(table) if table else 0
    print(f'\nBASELINES  valid races={n_valid:,}  bet ${total_bet/1e6:.1f}M  '
          f'{100*win_bet/total_bet:.1f}% of bet dollars landed on winners  '
          f'mean qualified-donor win rate {100*mean_wr:.1f}%')
    print(f'qualified donors (>= {MIN_BETS} race bets): {len(table):,}\n')

    def show(rows, title):
        print(title)
        print(f'  {"win%":>5} {"open%":>6} {"bets":>5} {"hedge":>5} {"inc%":>5} '
              f'{"$staked":>10} {"days":>5}  name')
        for d in rows:
            ow = f'{100*d["open_win_rate"]:.0f}%' if d['open_win_rate'] is not None else '  —'
            print(f'  {100*d["win_rate"]:4.0f}% {ow:>6} {d["bets"]:5} {d["hedged"]:5} '
                  f'{100*d["inc_share"]:4.0f}% {d["dollars"]:>10,} '
                  f'{d["avg_days_before"] or 0:5}  {d["name"][:52]} ({d["kind"]})')
        print()

    show(table[:25], f'SHARPEST DONORS — top 25 by win rate (>= {MIN_BETS} bets)')
    big = [d for d in table if d['dollars'] >= 250_000]
    show(big[:25], 'SHARPEST BIG MONEY — >= $250k staked, by win rate')
    heavy = sorted(table, key=lambda d: -d['bets'])[:20]
    show(heavy, 'MOST PROLIFIC BETTORS — by number of races')
    worst = sorted(table, key=lambda d: (d['win_rate'], -d['bets']))[:15]
    show(worst, 'WORST RECORDS — lowest win rate')

    # timing: do winners' backers show up early or late?
    sharp = [d for d in table if d['win_rate'] >= 0.8 and d['bets'] >= 30]
    rest = [d for d in table if d['win_rate'] < 0.8]
    if sharp and rest:
        s = sum(d['avg_days_before'] or 0 for d in sharp) / len(sharp)
        r = sum(d['avg_days_before'] or 0 for d in rest) / len(rest)
        print(f'TIMING  >=80% win-rate donors (n={len(sharp)}): money arrives '
              f'{s:.0f} days out on average; everyone else: {r:.0f} days out')


if __name__ == '__main__':
    main()
