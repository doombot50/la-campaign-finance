#!/usr/bin/env python3
"""
build_money_wins.py — "Does money win?" precompute
===================================================
Joins two artifacts already committed to this repo — never any new data:

  la_candidacies_raw.json.gz   every SoS candidacy 2000+ (name -> [{office, date,
                               vote_pct, outcome, rank, party_office}])
  la_candidate_index.json.gz   per-candidate money (name -> {cycles, monthly}),
                               where monthly carries BOTH 'in' (contributions
                               received) and 'out' (expenditures)

and asks the obvious unasked question: how often does the candidate who puts the
most money into a race actually win, and does that depend on the office tier?

WHICH MONEY? (this changed — read this)
---------------------------------------
The metric is **spending** through the election month, not fundraising.

Louisiana candidates — local ones especially — routinely bankroll campaigns with
personal loans that never appear as contributions received. Ranking by money
*raised* therefore mislabels who the money candidate was. Measured against the
same races:

  - in 15.2% of races the biggest spender is a different person than the biggest
    fundraiser;
  - 19.0% of the "upsets" a raised-based metric reports are races the winner
    actually outspent — not upsets at all;
  - the raised-based tier gap (Legislative 74.1% vs Local 61.5%, p=0.016) is an
    artifact: by spending it is 67.5% vs 63.9% (p=0.49, indistinguishable from
    chance). Local candidates self-fund more, so fundraising understates them.

The overall headline is robust across both measures (66.0% raised / 65.4% spent),
so the payload ships BOTH under `compare` and the page shows the contrast rather
than hiding it.

Both metrics are cumulative WITHIN the election's 4-year cycle and truncated at
the election month — never full-cycle, which would inflate winners who keep
raising and spending after they win.

A race counts when:
  - exactly one candidate has outcome == 'Elected' (a clean, decided winner —
    runoff-only primary ballots are excluded), and >= 2 candidates total;
  - no candidate's normalized name is flagged `ambiguous` in la_election_results
    (the project's canonical same-name-collision guard — two people, one name);
  - party-committee rows (DSCC/RSCC/...) are skipped (party_office);
  - federal offices are skipped (they report to the FEC, not the state);
  - >= 2 candidates deployed money on the metric being measured.

Output -> la_money_wins.json (committed root file; served by the live server's
/data/ route and copied into _site/data/ by build_pages_site.py).

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
    "Secretary of State results joined to Board of Ethics filings by normalized "
    "name. A candidate's “war chest” here is what they actually **spent** during "
    "the election's four-year cycle through the month of the election — not what "
    "they raised. Louisiana candidates, especially in local races, often bankroll "
    "campaigns with personal loans that never show up as contributions received, "
    "so ranking by money raised mislabels who the money candidate was: the biggest "
    "spender and the biggest fundraiser are different people in about one race in "
    "seven. Both measures are reported side by side. A race counts only when at "
    "least two candidates spent money, so “money leader” is a real comparison. "
    "Federal offices (President, U.S. House and Senate) are left out — those "
    "campaigns report to the FEC, not the state. Same-name filer collisions and "
    "party-committee races are excluded. Candidates whose activity fell below the "
    "Ethics Board's itemization threshold never appear in the finance data at all, "
    "so this leans toward races where money was actually raised, spent, and reported."
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
    figures for them are incomplete and misleading — excluded."""
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


def _through_month(entry, election_date, key):
    """Cumulative monthly `key` within the election's cycle, through the election
    month. `key` is 'in' (contributions received) or 'out' (expenditures).

    Returns None when the candidate has no record for that cycle at all (so the
    caller can tell "no data" apart from a genuine 0). Returns 0.0 when they have
    a cycle record but nothing on that side by election day."""
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
            total += v.get(key, 0) or 0
    return total


def money_through_month(entry, election_date):
    """Cumulative CONTRIBUTIONS RECEIVED in-cycle through the election month."""
    return _through_month(entry, election_date, 'in')


def spend_through_month(entry, election_date):
    """Cumulative EXPENDITURES in-cycle through the election month. This is the
    primary metric — it captures self-funding and loan-financed campaigns, which
    the contributions side misses entirely."""
    return _through_month(entry, election_date, 'out')


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
    """Top-spender : runner-up ratio -> label."""
    if ratio < 2:
        return '<2x'
    if ratio < 5:
        return '2-5x'
    if ratio < 10:
        return '5-10x'
    return '>10x'


ADVANTAGE_ORDER = ['<2x', '2-5x', '5-10x', '>10x']
TIER_ORDER = ['Statewide', 'Legislative', 'Judicial', 'Local']


def _funded(cands, key):
    """Candidates with money > 0 on `key` ('raised'|'spent'), richest first."""
    f = [c for c in cands if c.get(key) and c[key] > 0]
    f.sort(key=lambda c: c[key], reverse=True)
    return f


def _rate(won, n):
    return round(won / n, 3) if n else 0


# ── main analysis ────────────────────────────────────────────────────────────
def analyze(raw, cidx, results):
    races = build_races(raw)

    # ---- pass 1: reduce every race to a comparable record --------------------
    recs = []
    for (edate, office), cands in races.items():
        winners = [c for c in cands if c['outcome'] == 'Elected']
        if len(winners) != 1 or len(cands) < 2:
            continue
        winner = winners[0]
        if is_federal(winner['rank']):
            continue
        if any(results.get(c['name'], {}).get('ambiguous') for c in cands):
            continue
        entries = {c['name']: cidx.get(c['name']) for c in cands}
        for c in cands:
            e = entries[c['name']]
            c['raised'] = money_through_month(e, edate)
            c['spent'] = spend_through_month(e, edate)
        recs.append({'edate': edate, 'office': office, 'cands': cands,
                     'winner': winner, 'tier': tier_of(winner['rank'])})

    # ---- pass 2: the headline stat, on SPENDING ------------------------------
    tier_total, tier_won = Counter(), Counter()
    cycle_total, cycle_won = Counter(), Counter()
    adv_total, adv_won = Counter(), Counter()
    n_races = money_won = 0
    upsets = []

    for r in recs:
        funded = _funded(r['cands'], 'spent')
        if len(funded) < 2:
            continue
        n_races += 1
        leader, runner = funded[0], funded[1]
        winner = r['winner']
        leader_won = leader['name'] == winner['name']
        tier, cyc = r['tier'], cycle_of(r['edate'])
        bucket = advantage_bucket(leader['spent'] / runner['spent'])

        tier_total[tier] += 1
        cycle_total[cyc] += 1
        adv_total[bucket] += 1
        if leader_won:
            money_won += 1
            tier_won[tier] += 1
            cycle_won[cyc] += 1
            adv_won[bucket] += 1
        else:
            # The biggest spender lost — the human heart of the story. Because the
            # metric is spending, "outspent N×" on the page is now literally true.
            w = next((c for c in r['cands'] if c['name'] == winner['name']), None)
            ws = w.get('spent') if w else None
            upsets.append({
                'name': leader['name'],
                'office': r['office'],
                'date': r['edate'],
                'tier': tier,
                'lead_spent': round(leader['spent']),
                'lead_raised': round(leader['raised']) if leader.get('raised') is not None else None,
                'lead_vote_pct': leader['vote_pct'],
                'winner': winner['name'],
                'winner_spent': round(ws) if ws is not None else None,
                'winner_raised': round(w['raised']) if w and w.get('raised') is not None else None,
                'winner_vote_pct': winner['vote_pct'],
                'ratio': round(leader['spent'] / ws, 1) if ws else None,
            })

    upsets.sort(key=lambda u: (u['ratio'] or 0), reverse=True)

    # ---- pass 3: the same question asked of FUNDRAISING, for comparison ------
    r_tier_total, r_tier_won = Counter(), Counter()
    r_n = r_won = 0
    flips = flip_base = 0
    false_upsets = raised_upsets = 0

    for r in recs:
        rf = _funded(r['cands'], 'raised')
        sf = _funded(r['cands'], 'spent')
        winner = r['winner']
        if len(rf) >= 2:
            r_n += 1
            r_tier_total[r['tier']] += 1
            if rf[0]['name'] == winner['name']:
                r_won += 1
                r_tier_won[r['tier']] += 1
            else:
                # a raised-based "upset" — is it real once you look at spending?
                raised_upsets += 1
                w = next((c for c in r['cands'] if c['name'] == winner['name']), None)
                lead_s = rf[0].get('spent')
                if w and w.get('spent') is not None and lead_s is not None and w['spent'] > lead_s:
                    false_upsets += 1
        if len(rf) >= 2 and len(sf) >= 2:
            flip_base += 1
            if rf[0]['name'] != sf[0]['name']:
                flips += 1

    # ---- scatter: spend share vs vote share ---------------------------------
    scatter = []
    for r in recs:
        if any(c.get('spent') is None for c in r['cands']):
            continue
        tot = sum(c['spent'] for c in r['cands'])
        if tot <= 0:
            continue
        for c in r['cands']:
            if c['vote_pct'] is None:
                continue
            scatter.append({
                'ms': round(c['spent'] / tot, 3),
                'vs': round(c['vote_pct'] / 100.0, 3),
                'won': c['outcome'] == 'Elected',
                'tier': r['tier'],
                'name': c['name'],
                'office': r['office'],
                'date': r['edate'],
            })

    by_tier = [{'tier': t, 'n': tier_total[t], 'won': tier_won[t],
                'rate': _rate(tier_won[t], tier_total[t])}
               for t in TIER_ORDER if tier_total[t]]
    by_advantage = [{'bucket': b, 'n': adv_total[b], 'won': adv_won[b],
                     'rate': _rate(adv_won[b], adv_total[b])}
                    for b in ADVANTAGE_ORDER if adv_total[b]]
    by_cycle = [{'cycle': c, 'n': cycle_total[c], 'won': cycle_won[c],
                 'rate': _rate(cycle_won[c], cycle_total[c])}
                for c in sorted(cycle_total)]

    compare_tiers = []
    for t in TIER_ORDER:
        if not (tier_total[t] or r_tier_total[t]):
            continue
        compare_tiers.append({
            'tier': t,
            'raised_n': r_tier_total[t], 'raised_rate': _rate(r_tier_won[t], r_tier_total[t]),
            'spent_n': tier_total[t], 'spent_rate': _rate(tier_won[t], tier_total[t]),
        })

    years = sorted({r['edate'].split('/')[2] for r in recs})
    span = f'{years[0]}-{years[-1]}' if years else ''

    return {
        'generated': date.today().isoformat(),
        'span': span,
        'metric': 'spent',
        'overall': {'n_races': n_races, 'money_won': money_won,
                    'rate': _rate(money_won, n_races)},
        'by_tier': by_tier,
        'by_advantage': by_advantage,
        'by_cycle': by_cycle,
        'scatter': scatter,
        'scatter_r': _pearson([p['ms'] for p in scatter], [p['vs'] for p in scatter]),
        'upsets': upsets[:40],
        'n_upsets': len(upsets),
        'compare': {
            'raised': {'n_races': r_n, 'money_won': r_won, 'rate': _rate(r_won, r_n)},
            'spent': {'n_races': n_races, 'money_won': money_won,
                      'rate': _rate(money_won, n_races)},
            'by_tier': compare_tiers,
            'leader_flips': {'n': flips, 'of': flip_base, 'rate': _rate(flips, flip_base)},
            'false_upsets': {'n': false_upsets, 'of': raised_upsets,
                             'rate': _rate(false_upsets, raised_upsets)},
        },
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

    # Atomic write. la_money_wins.json is a COMMITTED file, and the nightly's
    # "Commit COH cache if changed" step runs with `if: always()` — so a run
    # cancelled part-way through this dump used to commit and publish a truncated
    # JSON that does-money-win.html could not parse. tmp + replace means the file
    # on disk is either the old payload or the whole new one.
    tmp = OUT_FILE + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(payload, f, separators=(',', ':'), ensure_ascii=False)
    os.replace(tmp, OUT_FILE)

    o = payload['overall']
    c = payload['compare']
    size_kb = os.path.getsize(OUT_FILE) / 1024
    print(f'Wrote {os.path.basename(OUT_FILE)} ({size_kb:.0f} KB)')
    print(f'  span {payload["span"]}: {o["n_races"]} races where 2+ candidates spent money')
    print(f'  money (SPENT) leader won {100 * o["rate"]:.1f}%')
    print(f'  money (RAISED) leader won {100 * c["raised"]["rate"]:.1f}% '
          f'over {c["raised"]["n_races"]} races  [comparison only]')
    print(f'  scatter: {len(payload["scatter"])} candidate points, r = {payload["scatter_r"]}')
    for row in payload['by_tier']:
        print(f'    {row["tier"]:22} n={row["n"]:4}  money-win {100 * row["rate"]:.0f}%')
    print('  outspend curve: ' + '  '.join(
        f'{r["bucket"]}={100 * r["rate"]:.0f}%' for r in payload['by_advantage']))
    fl, fu = c['leader_flips'], c['false_upsets']
    print(f'  biggest spender != biggest fundraiser: {fl["n"]}/{fl["of"]} '
          f'({100 * fl["rate"]:.1f}%)')
    print(f'  raised-based "upsets" the winner actually outspent: {fu["n"]}/{fu["of"]} '
          f'({100 * fu["rate"]:.1f}%)')


if __name__ == '__main__':
    main()
