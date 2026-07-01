#!/usr/bin/env python3
"""
test_money_wins.py — unit tests for build_money_wins.py.

Guards the pure join/aggregation logic behind the "does money win?" story:
  - cycle_of / tier_of mapping,
  - money_through_month (the honest metric: in-cycle, through the election month,
    NOT full-cycle which would inflate winners' post-election fundraising),
  - analyze(): race reconstruction, the >=2-funded gate, ambiguous-name exclusion,
    money-leader determination, and the internal-consistency invariants the
    shipped artifact must satisfy (sum(by_tier.n) == overall.n_races, shares in
    [0,1], upsets are genuine money-leader losses).

Stdlib only (unittest). No network, no .la_cache/ release — runs in CI. Also
asserts the committed la_money_wins.json (when present) matches the invariants.
"""
import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import build_money_wins as bmw  # noqa: E402

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class TestHelpers(unittest.TestCase):
    def test_cycle_of(self):
        self.assertEqual(bmw.cycle_of('04/24/2021'), '2020-2023')
        self.assertEqual(bmw.cycle_of('01/01/2000'), '2000-2003')
        self.assertEqual(bmw.cycle_of('12/31/2023'), '2020-2023')
        self.assertEqual(bmw.cycle_of('11/05/2024'), '2024-2027')

    def test_tier_of(self):
        self.assertEqual(bmw.tier_of(4), 'Statewide')   # Governor
        self.assertEqual(bmw.tier_of(6), 'Statewide')   # AG/SoS/...
        self.assertEqual(bmw.tier_of(7), 'Legislative')           # State Senator
        self.assertEqual(bmw.tier_of(8), 'Legislative')           # State Rep
        self.assertEqual(bmw.tier_of(12), 'Judicial')
        self.assertEqual(bmw.tier_of(10), 'Local')
        self.assertEqual(bmw.tier_of(15), 'Local')
        self.assertEqual(bmw.tier_of(20), 'Local')

    def test_is_federal(self):
        self.assertTrue(bmw.is_federal(1))    # President
        self.assertTrue(bmw.is_federal(2))    # U.S. Senate
        self.assertTrue(bmw.is_federal(3))    # U.S. House
        self.assertFalse(bmw.is_federal(4))   # Governor (state)
        self.assertFalse(bmw.is_federal(8))   # State Rep

    def test_money_through_month_sums_only_in_cycle_pre_election(self):
        entry = {
            'cycles': {'2020-2023': {'raised': 999}},
            'monthly': {
                '2020-06': {'in': 100, 'out': 0},   # in cycle, before election
                '2021-03': {'in': 250, 'out': 0},   # in cycle, election month
                '2021-09': {'in': 500, 'out': 0},   # in cycle, AFTER election -> excluded
                '2018-01': {'in': 777, 'out': 0},   # different cycle -> excluded
            },
        }
        self.assertEqual(bmw.money_through_month(entry, '03/15/2021'), 350)

    def test_money_through_month_none_when_no_cycle_record(self):
        entry = {'cycles': {'2016-2019': {'raised': 5}}, 'monthly': {}}
        self.assertIsNone(bmw.money_through_month(entry, '03/15/2021'))
        self.assertIsNone(bmw.money_through_month(None, '03/15/2021'))

    def test_money_through_month_zero_when_cycle_but_no_prior_months(self):
        entry = {'cycles': {'2020-2023': {'raised': 5}},
                 'monthly': {'2021-09': {'in': 500, 'out': 0}}}  # all after election
        self.assertEqual(bmw.money_through_month(entry, '03/15/2021'), 0.0)

    def test_advantage_bucket(self):
        self.assertEqual(bmw.advantage_bucket(1.4), '<2x')
        self.assertEqual(bmw.advantage_bucket(2.0), '2-5x')
        self.assertEqual(bmw.advantage_bucket(7.0), '5-10x')
        self.assertEqual(bmw.advantage_bucket(25.0), '>10x')


def _entry(cyc, month, amount):
    """Minimal candidate-index entry with all money in one pre-election month."""
    return {'cycles': {cyc: {'raised': amount}}, 'monthly': {month: {'in': amount, 'out': 0}}}


class TestAnalyze(unittest.TestCase):
    def setUp(self):
        # A clean State Rep race (rank 8 -> Legislative): richest candidate wins.
        self.raw = {
            'RICH WINNER': [{'office': 'State Representative -- 1st', 'date': '10/12/2019',
                             'vote_pct': 55.0, 'outcome': 'Elected', 'rank': 8, 'party': 'REP'}],
            'POOR LOSER': [{'office': 'State Representative -- 1st', 'date': '10/12/2019',
                            'vote_pct': 45.0, 'outcome': 'Defeated', 'rank': 8, 'party': 'DEM'}],
            # A Local race (rank 10) where the SPENDER LOSES -> an upset.
            'BIG SPENDER': [{'office': 'Acadia Parish Sheriff', 'date': '10/12/2019',
                             'vote_pct': 40.0, 'outcome': 'Defeated', 'rank': 10, 'party': 'REP'}],
            'CHEAP WINNER': [{'office': 'Acadia Parish Sheriff', 'date': '10/12/2019',
                              'vote_pct': 60.0, 'outcome': 'Elected', 'rank': 10, 'party': 'DEM'}],
            # An AMBIGUOUS name in a race -> the whole race is dropped.
            'AMBI GUOUS': [{'office': 'Judge -- District X', 'date': '10/12/2019',
                            'vote_pct': 51.0, 'outcome': 'Elected', 'rank': 12, 'party': 'REP'}],
            'OTHER JUDGE': [{'office': 'Judge -- District X', 'date': '10/12/2019',
                             'vote_pct': 49.0, 'outcome': 'Defeated', 'rank': 12, 'party': 'DEM'}],
            # A race with only ONE funded candidate -> excluded from the stat.
            'LONE FUNDED': [{'office': 'State Senator -- 2nd', 'date': '10/12/2019',
                             'vote_pct': 70.0, 'outcome': 'Elected', 'rank': 7, 'party': 'REP'}],
            'UNFUNDED FOE': [{'office': 'State Senator -- 2nd', 'date': '10/12/2019',
                              'vote_pct': 30.0, 'outcome': 'Defeated', 'rank': 7, 'party': 'DEM'}],
            # A FEDERAL race (rank 3, U.S. House) -> excluded (FEC-reported money).
            'FED WINNER': [{'office': 'U. S. Representative -- 3rd', 'date': '10/12/2019',
                            'vote_pct': 60.0, 'outcome': 'Elected', 'rank': 3, 'party': 'REP'}],
            'FED LOSER': [{'office': 'U. S. Representative -- 3rd', 'date': '10/12/2019',
                           'vote_pct': 40.0, 'outcome': 'Defeated', 'rank': 3, 'party': 'DEM'}],
        }
        self.cidx = {
            'RICH WINNER': _entry('2016-2019', '2019-09', 50000),
            'POOR LOSER': _entry('2016-2019', '2019-09', 10000),
            'BIG SPENDER': _entry('2016-2019', '2019-09', 80000),
            'CHEAP WINNER': _entry('2016-2019', '2019-09', 5000),
            'AMBI GUOUS': _entry('2016-2019', '2019-09', 30000),
            'OTHER JUDGE': _entry('2016-2019', '2019-09', 20000),
            'LONE FUNDED': _entry('2016-2019', '2019-09', 40000),
            'FED WINNER': _entry('2016-2019', '2019-09', 90000),
            'FED LOSER': _entry('2016-2019', '2019-09', 70000),
            # UNFUNDED FOE deliberately absent from the index.
        }
        self.results = {'AMBI GUOUS': {'ambiguous': True}}

    def test_counts_and_money_leader(self):
        out = bmw.analyze(self.raw, self.cidx, self.results)
        # Two races qualify (Legislative + Local). The judicial race is dropped
        # (ambiguous) and the senate race is dropped (only one funded candidate).
        self.assertEqual(out['overall']['n_races'], 2)
        # Legislative: money leader (RICH WINNER) won. Local: money leader
        # (BIG SPENDER) lost. So money won 1 of 2.
        self.assertEqual(out['overall']['money_won'], 1)
        self.assertEqual(out['overall']['rate'], 0.5)

    def test_tier_breakdown_sums_to_total(self):
        out = bmw.analyze(self.raw, self.cidx, self.results)
        self.assertEqual(sum(r['n'] for r in out['by_tier']),
                         out['overall']['n_races'])
        tiers = {r['tier']: r for r in out['by_tier']}
        self.assertEqual(tiers['Legislative']['rate'], 1.0)  # money won
        self.assertEqual(tiers['Local']['rate'], 0.0)        # money lost

    def test_upset_recorded_with_ratio(self):
        out = bmw.analyze(self.raw, self.cidx, self.results)
        self.assertEqual(len(out['upsets']), 1)
        up = out['upsets'][0]
        self.assertEqual(up['name'], 'BIG SPENDER')
        self.assertEqual(up['winner'], 'CHEAP WINNER')
        self.assertEqual(up['lead_spent'], 80000)
        self.assertEqual(up['winner_spent'], 5000)
        self.assertEqual(up['ratio'], 16.0)   # 80000 / 5000

    def test_ambiguous_race_excluded_everywhere(self):
        out = bmw.analyze(self.raw, self.cidx, self.results)
        names_in_scatter = {p['name'] for p in out['scatter']}
        self.assertNotIn('AMBI GUOUS', names_in_scatter)
        self.assertNotIn('OTHER JUDGE', names_in_scatter)

    def test_federal_race_excluded_everywhere(self):
        out = bmw.analyze(self.raw, self.cidx, self.results)
        names_in_scatter = {p['name'] for p in out['scatter']}
        self.assertNotIn('FED WINNER', names_in_scatter)
        self.assertNotIn('FED LOSER', names_in_scatter)
        self.assertNotIn('Statewide', {r['tier'] for r in out['by_tier']})  # none here
        # The two qualifying races are still just the Legislative + Local ones.
        self.assertEqual(out['overall']['n_races'], 2)

    def test_scatter_shares_in_unit_interval(self):
        out = bmw.analyze(self.raw, self.cidx, self.results)
        self.assertTrue(out['scatter'])  # non-empty
        for p in out['scatter']:
            self.assertGreaterEqual(p['ms'], 0.0)
            self.assertLessEqual(p['ms'], 1.0)
            self.assertGreaterEqual(p['vs'], 0.0)
            self.assertLessEqual(p['vs'], 1.0)


class TestShippedArtifact(unittest.TestCase):
    """The committed la_money_wins.json must satisfy the same invariants."""

    def setUp(self):
        path = os.path.join(BASE, 'la_money_wins.json')
        if not os.path.exists(path):
            self.skipTest('la_money_wins.json not built yet')
        with open(path, encoding='utf-8') as f:
            self.d = json.load(f)

    def test_top_level_keys(self):
        for k in ('overall', 'by_tier', 'by_advantage', 'scatter', 'upsets',
                  'methodology', 'span', 'generated'):
            self.assertIn(k, self.d)

    def test_tier_counts_sum_to_total(self):
        self.assertEqual(sum(r['n'] for r in self.d['by_tier']),
                         self.d['overall']['n_races'])

    def test_advantage_buckets_sum_to_total(self):
        self.assertEqual(sum(r['n'] for r in self.d['by_advantage']),
                         self.d['overall']['n_races'])

    def test_scatter_shares_bounded(self):
        for p in self.d['scatter']:
            self.assertTrue(0.0 <= p['ms'] <= 1.0)
            self.assertTrue(0.0 <= p['vs'] <= 1.0)


if __name__ == '__main__':
    unittest.main()
