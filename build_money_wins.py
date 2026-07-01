#!/usr/bin/env python3
"""
build_money_wins.py — "Does money win?" precompute
===================================================
Joins two artifacts already committed to this repo — never any new data:

  la_candidacies_raw.json.gz   every SoS candidacy 2000+ (name -> [{office, date,
                               vote_pct, outcome, rank, party_office}])
  la_candidate_index.json.gz   per-candidate fundraising (name -> {cycles, monthly})

and asks the obvious unasked question: how often does the candidate with the
biggest war chest actually win, and does that depend on the office tier?

A "race" is reconstructed by grouping raw candidacies on (date, office) — office
is already parish-qualified for local seats by build_election_results.py, so it's
a stable race key. A race counts toward the win-rate stat when:
  - exactly one candidate has outcome == 'Elected' (a clean, decided winner —
    runoff-only primary ballots are excluded), and >= 2 candidates total;
  - no candidate's normalized name is flagged `ambiguous` in la_election_results
    (the project's canonical same-name-collision guard — two people, one name);
  - party-committee rows (DSCC/RSCC/...) are skipped (party_office);
  - >= 2 candidates have reported fundraising > 0 (so "money leader" is a real
    comparison, not the only funded candidate winning by default).

Money metric: cumulative raised WITHIN the election's 4-year cycle THROUGH the
election month (sum of monthly.in for cycle months <= election month). NOT
full-cycle `raised`, which would inflate winners because they keep fundraising
*after* they win (measured: 67.7% full-cycle vs the honest 66.1% through-month).

Output -> la_money_wins.json (committed root file, ~100 KB; served by the live
server's /data/ route and copied into _site/data/ by build_pages_site.py).

Stdlib only. Run after build_candidate_index.py / build_election_results.py.
"""
import json
import gzip
import os
from collections import defaultdict, Counter
from datetime import date

BASE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(BASE, '.la_cache')


def _resolve(name):
    """Prefer the fresh .la_cache/ copy (what the nightly rebuilds), fall back to
    the committed repo-root file (what's present locally / in CI). Mirrors the
    server's CACHE_DIR -> BASE_DIR lookup order."""
    cached = os.path.join(CACHE, name)
    return cached if os.path.exists(cached) else os.path.join(BASE, name)


RAW_FILE = _resolve('la_candidacies_raw.json.gz')
CIDX_FILE = _resolve('la_candidate_index.json.gz')
RESULTS_FILE = _resolve('la_election_results.json')
OUT_FILE = os.path.join(BASE, 'la_money_wins.json')

METHODOLOGY = (
    "Every contested race with a clear winner since 2000, drawn from Louisiana "
    "Secretary of State results joined to Board of Ethics fundraising by "
    "normalized name. A candidate's “war chest” is the money they raised "
    "during the election's four-year cycle through the month of the election — "
    "not the full cycle, which would inflate winners who keep raising money after "
    "they win. A race counts only when at least two candidates reported "
    "fundraising, so “money leader” is a real comparison. Federal offices "
    "(President, U.S. House and Senate) are left out — those campaigns report to "
    "the FEC, not the state, so the Ethics figures don't capture their money. "
    "Same-name filer collisions and party-committee races are excluded. "
    "Candidates who raised too "
    "little to itemize with the Ethics board don't appear in the fundraising data, "
    "so this leans toward races where money was actually raised and reported."
)


# ── pure helpers (imported by tests/test_money_wins.py) ──────────────────────
def cycle_of(election_date):
    """'MM/DD/YYYY' -> the 4-year cycle key used by la_candidate_index, e.g.
    '04/24/2021' -> '2020-2023'."""
    yr = int(election_date.split('/')[2])
    start = 2000 + ((yr - 2000) // 4) * 4
    return f'{start}-{start + 3}'


def is_federal(rank):
    """Ranks 1-3 (President, U.S. Senate, U.S. House) are federal offices. Their
    campaigns report to the FEC, not the Louisiana Board of Ethics, so the state
    fundraising figures for them are incomplete and misleading — excluded."""
    return rank <= 3


def tier_of(rank):
    """office_rank (from build_election_results.office_rank) -> display tier.
    Federal offices are excluded upstream (see is_federal); 4-6 are the statewide
    executive/constitutional offices (Governor, AG, Treasurer, ...)."""
    if rank <= 6:
        return 'Statewide'
    if rank in (7, 8):
        return 'Legislative'
    if rank == 12:
        return 'Judicial'
    return 'Local'   # 10 / 15 / 20 (parish + municipal + local "other")


def money_through_month(entry, election_date):
    """Cumulative raised within the election's cycle, through the election month.

    Returns None when the candidate has no fundraising in that cycle at all (so
    the caller can tell "no data" apart from a genuine 0). Returns 0.0 when they
    have a cycle record but hadn't raised anything by election day."""
    if not entry:
        return None
    cyc = cycle_of(election_date)
    if cyc not in entry.get('cycles', {}):
        return None
    start = int(cyc.split('-')[0])
    mm, _dd, yy = election_date.split('/')
    em = f'{int(yy):04d}-{int(mm):02d}'   # election year-month, zero-padded
    total = 0.0
    for ym, v in entry.get('monthly', {}).items():
        y = int(ym.split('-')[0])
        if start <= y <= start + 3 and ym <= em:
            total += v.get('in', 0) or 0
    return total


def build_races(raw):
    """name-keyed raw candidacies -> {(date, office): [candidate dicts]}.
    Party-committee rows are dropped (they aren't real seats)."""
    races = defaultdict(list)
    for norm, cands in raw.items():
        for c in cands:
            if c.get('party_office'):
                continue
            races[(c['date'], c['office'])].append({
                'name': norm,
                'party': c.get('party'),
                'vote_pct': c.get('vote_pct'),
                'outcome': c.get('outcome'),
                'rank': c.get('rank', 20),
            })
    return races


def advantage_bucket(ratio):
    """Top-funded : runner-up spending ratio -> label."""
    if ratio < 2:
        return '<2x'
    if ratio < 5:
        return '2-5x'
    if ratio < 10:
        return '5-10x'
    return '>10x'


ADVANTAGE_ORDER = ['<2x', '2-5x', '5-10x', '>10x']
TIER_ORDER = ['Statewide', 'Legislative', 'Judicial', 'Local']


# ── main analysis ────────────────────────────────────────────────────────────
def analyze(raw, cidx, results):
    races = build_races(raw)

    tier_total = Counter()
    tier_won = Counter()
    cycle_total = Counter()
    cycle_won = Counter()
    adv_total = Counter()
    adv_won = Counter()
    n_races = 0
    money_won = 0
    scatter = []
    upsets = []

    for (edate, office), cands in races.items():
        winners = [c for c in cands if c['outcome'] == 'Elected']
        if len(winners) != 1 or len(cands) < 2:
            continue
        winner = winners[0]
        # Federal offices (President/U.S. Senate/U.S. House) report to the FEC,
        # not the state — their LA Ethics fundraising is incomplete. Exclude them.
        if is_federal(winner['rank']):
            continue
        # Drop the whole race if any candidate's name is a known collision.
        if any(results.get(c['name'], {}).get('ambiguous') for c in cands):
            continue

        for c in cands:
            c['money'] = money_through_month(cidx.get(c['name']), edate)

        funded = [c for c in cands if c['money'] and c['money'] > 0]

        # Scatter uses races where EVERY candidate's money is known, so the
        # money-share axis is a real share (not distorted by missing rows).
        if all(c['money'] is not None for c in cands):
            tot_money = sum(c['money'] for c in cands)
            if tot_money > 0:
                tier = tier_of(winner['rank'])
                for c in cands:
                    vp = c['vote_pct']
                    if vp is None:
                        continue
                    scatter.append({
                        'ms': round(c['money'] / tot_money, 3),
                        'vs': round(vp / 100.0, 3),
                        'won': c['outcome'] == 'Elected',
                        'tier': tier,
                        'name': c['name'],
                        'office': office,
                        'date': edate,
                    })

        # Win-rate stat needs a real comparison: >= 2 funded candidates.
        if len(funded) < 2:
            continue

        n_races += 1
        tier = tier_of(winner['rank'])
        cyc = cycle_of(edate)
        funded.sort(key=lambda c: c['money'], reverse=True)
        leader = funded[0]
        runner = funded[1]
        leader_won = leader['name'] == winner['name']

        tier_total[tier] += 1
        cycle_total[cyc] += 1
        ratio = leader['money'] / runner['money'] if runner['money'] else float('inf')
        bucket = advantage_bucket(ratio)
        adv_total[bucket] += 1
        if leader_won:
            money_won += 1
            tier_won[tier] += 1
            cycle_won[cyc] += 1
            adv_won[bucket] += 1
        else:
            # The money leader lost — the human heart of the story. Record how
            # badly they outspent the person who actually beat them.
            w_money = next((c['money'] for c in funded if c['name'] == winner['name']), None)
            upsets.append({
                'name': leader['name'],
                'office': office,
                'date': edate,
                'tier': tier,
                'lead_spent': round(leader['money']),
                'lead_vote_pct': leader['vote_pct'],
                'winner': winner['name'],
                'winner_spent': round(w_money) if w_money else None,
                'winner_vote_pct': winner['vote_pct'],
                'ratio': round(leader['money'] / w_money, 1) if w_money else None,
            })

    # Sort upsets by how lopsided the spending was (biggest outspend-and-lost first).
    upsets.sort(key=lambda u: (u['ratio'] or 0), reverse=True)

    by_tier = []
    for t in TIER_ORDER:
        if tier_total[t]:
            by_tier.append({'tier': t, 'n': tier_total[t], 'won': tier_won[t],
                            'rate': round(tier_won[t] / tier_total[t], 3)})

    by_advantage = []
    for b in ADVANTAGE_ORDER:
        if adv_total[b]:
            by_advantage.append({'bucket': b, 'n': adv_total[b], 'won': adv_won[b],
                                 'rate': round(adv_won[b] / adv_total[b], 3)})

    by_cycle = []
    for c in sorted(cycle_total):
        by_cycle.append({'cycle': c, 'n': cycle_total[c], 'won': cycle_won[c],
                         'rate': round(cycle_won[c] / cycle_total[c], 3)})

    years = sorted({edate.split('/')[2] for edate, _ in races})
    span = f'{years[0]}-{years[-1]}' if years else ''

    return {
        'generated': date.today().isoformat(),
        'span': span,
        'overall': {'n_races': n_races, 'money_won': money_won,
                    'rate': round(money_won / n_races, 3) if n_races else 0},
        'by_tier': by_tier,
        'by_advantage': by_advantage,
        'by_cycle': by_cycle,
        'scatter': scatter,
        'scatter_r': _pearson([p['ms'] for p in scatter], [p['vs'] for p in scatter]),
        'upsets': upsets[:40],
        'n_upsets': len(upsets),
        'methodology': METHODOLOGY,
    }


def _pearson(xs, ys):
    n = len(xs)
    if n < 2:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    sx = sum((x - mx) ** 2 for x in xs) ** 0.5
    sy = sum((y - my) ** 2 for y in ys) ** 0.5
    if sx == 0 or sy == 0:
        return None
    return round(cov / (sx * sy), 3)


def main():
    with gzip.open(RAW_FILE, 'rt', encoding='utf-8') as f:
        raw = json.load(f)
    with gzip.open(CIDX_FILE, 'rt', encoding='utf-8') as f:
        cidx = json.load(f)
    with open(RESULTS_FILE, encoding='utf-8') as f:
        results = json.load(f)

    payload = analyze(raw, cidx, results)

    with open(OUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(payload, f, separators=(',', ':'), ensure_ascii=False)

    o = payload['overall']
    size_kb = os.path.getsize(OUT_FILE) / 1024
    print(f'Wrote {os.path.basename(OUT_FILE)} ({size_kb:.0f} KB)')
    print(f'  span {payload["span"]}: {o["n_races"]} contested races, '
          f'money leader won {100 * o["rate"]:.1f}%')
    print(f'  scatter: {len(payload["scatter"])} candidate points, '
          f'r = {payload["scatter_r"]}')
    for row in payload['by_tier']:
        print(f'    {row["tier"]:22} n={row["n"]:4}  money-win {100 * row["rate"]:.0f}%')
    print('  outspend curve: ' + '  '.join(
        f'{r["bucket"]}={100 * r["rate"]:.0f}%' for r in payload['by_advantage']))


if __name__ == '__main__':
    main()
