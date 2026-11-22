#!/usr/bin/env python3
"""
test_fec_candidates.py — unit tests for build_fec_candidates.py.

The matcher decides which FEC page a federal candidate's link opens, and the
dashboard's _fecIdFor mirrors it — both run the shared cases in
fixtures/fec_match_cases.json (the Node twin is in test_frontend_units.mjs).
Also checks build_seats against FEC-shaped records and the committed
la_fec_candidates.json shape. Stdlib only, no network.
"""
import json
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

import build_fec_candidates as b  # noqa: E402

with open(os.path.join(HERE, 'fixtures', 'fec_match_cases.json')) as f:
    FIX = json.load(f)


class TestMatch(unittest.TestCase):
    def test_shared_cases(self):
        for name, office, date, want, why in FIX['cases']:
            with self.subTest(why):
                self.assertEqual(b.match(FIX['seats'], name, office, date), want)

    def test_seat_of(self):
        self.assertEqual(b.seat_of('U. S. Senator'), 'S')
        self.assertEqual(b.seat_of('U. S. Representative -- 3rd Congressional District'), 'H')
        self.assertIsNone(b.seat_of('State Representative -- 96th Representative District'))
        self.assertIsNone(b.seat_of('President of the United States'))

    def test_no_seats_is_none(self):
        self.assertIsNone(b.match(None, 'BILL CASSIDY', 'U. S. Senator', '11/03/2020'))


class TestBuildSeats(unittest.TestCase):
    def setUp(self):
        self.seats = b.build_seats([
            {'candidate_id': 'H8LA02054', 'office': 'H', 'district': '02',
             'name': 'CARTER PETERSON, KAREN', 'election_years': [2006, 2008], 'cycles': [2008]},
            {'candidate_id': 'S4LA00107', 'office': 'S', 'district': '00',
             'name': 'CASSIDY, WILLIAM M', 'election_years': [2014], 'cycles': [2014, 2020]},
            {'candidate_id': 'P00000001', 'office': 'P', 'name': 'SOMEONE, A', 'election_years': [2024]},
            {'candidate_id': '', 'office': 'H', 'name': 'NOID, X', 'election_years': [2024]},
        ])

    def test_chambers_only_and_president_skipped(self):
        self.assertEqual(set(self.seats), {'H', 'S'})

    def test_every_last_name_token_is_a_key(self):
        self.assertEqual(self.seats['H']['CARTER'][0]['id'], 'H8LA02054')
        self.assertEqual(self.seats['H']['PETERSON'][0]['id'], 'H8LA02054')
        self.assertNotIn('NOID', self.seats['H'])

    def test_years_union_election_years_and_cycles(self):
        self.assertEqual(self.seats['S']['CASSIDY'][0]['years'], [2014, 2020])
        self.assertEqual(self.seats['S']['CASSIDY'][0]['first'], 'WILLIAM M')


class TestCommittedFile(unittest.TestCase):
    def test_shape(self):
        path = os.path.join(ROOT, 'la_fec_candidates.json')
        if not os.path.exists(path):
            self.skipTest('la_fec_candidates.json not built')
        with open(path) as f:
            seats = json.load(f)['seats']
        self.assertEqual(set(seats), {'H', 'S'})
        n = 0
        for by_last in seats.values():
            for last, lst in by_last.items():
                self.assertRegex(last, r'^[A-Z]+$')
                for e in lst:
                    self.assertRegex(e['id'], r'^[HS]\d[A-Z]{2}\d{5}$')
                    self.assertTrue(all(isinstance(y, int) for y in e['years']))
                    n += 1
        self.assertGreater(n, 300)
        # Sanity: a long-serving senator resolves.
        self.assertEqual(b.match(seats, 'BILL CASSIDY', 'U. S. Senator', '11/03/2020'), 'S4LA00107')


if __name__ == '__main__':
    unittest.main()
