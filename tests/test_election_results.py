#!/usr/bin/env python3
"""
test_election_results.py — unit tests for build_election_results.py's
incremental scrape.

The scrape runs nightly, so what it decides to re-fetch — and what it refuses to
overwrite — is what keeps win/loss badges, Races and "does money win?" current
without a flaky night erasing results:
  - dates_to_scrape(): new dates, upcoming/recent dates, and held dates that
    were scraped before the vote (no outcome on any row) are fetched; settled
    history is skipped,
  - rescrape_ok(): a partial fetch never replaces good rows,
  - replace_date(): a re-scrape swaps one date's rows, not appends to them,
  - main(): end to end against a fake portal, including the no-change path that
    must leave the committed files untouched.

Stdlib only (unittest). No network — fetch() is replaced with a fake portal.
"""
import contextlib
import gzip
import io
import json
import os
import sys
import tempfile
import unittest
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import build_election_results as ber  # noqa: E402


def row(d, outcome='Elected', office='Mayor -- City of X', party='DEM'):
    return {'party': party, 'office': office, 'outcome': outcome, 'vote_pct': 55.0,
            'date': d, 'rank': 10, 'party_office': False}


class TestDatesToScrape(unittest.TestCase):
    TODAY = date(2026, 11, 14)

    def todo(self, portal, by_date):
        return ber.dates_to_scrape(portal, by_date, self.TODAY)

    def test_new_date_is_scraped(self):
        self.assertEqual(self.todo(['10/14/2023'], {}), ['10/14/2023'])

    def test_settled_history_is_skipped(self):
        by_date = {'10/14/2023': [row('10/14/2023'), row('10/14/2023', 'Defeated')]}
        self.assertEqual(self.todo(['10/14/2023'], by_date), [])

    def test_stray_blank_outcome_does_not_reopen_an_old_date(self):
        # 11/04/2014 carries two blank outcomes among thousands of recorded ones.
        by_date = {'11/04/2014': [row('11/04/2014'), row('11/04/2014', '')]}
        self.assertEqual(self.todo(['11/04/2014'], by_date), [])

    def test_held_date_with_no_outcomes_is_rescraped(self):
        # Scraped before the vote and never revisited — however long ago.
        by_date = {'06/27/2026': [row('06/27/2026', ''), row('06/27/2026', '')]}
        self.assertEqual(self.todo(['06/27/2026'], by_date), ['06/27/2026'])

    def test_recent_and_upcoming_dates_are_refreshed(self):
        by_date = {'11/03/2026': [row('11/03/2026')],      # 11 days ago: unofficial
                   '12/12/2026': [row('12/12/2026', '')]}  # upcoming
        self.assertEqual(self.todo(['12/12/2026', '11/03/2026'], by_date),
                         ['11/03/2026', '12/12/2026'])

    def test_refresh_window_boundary(self):
        by_date = {'09/30/2026': [row('09/30/2026')],   # exactly 45 days ago
                   '09/29/2026': [row('09/29/2026')]}   # 46 days ago
        self.assertEqual(self.todo(['09/29/2026', '09/30/2026'], by_date), ['09/30/2026'])

    def test_sorted_chronologically_not_lexically(self):
        self.assertEqual(self.todo(['02/07/2026', '11/15/2025'], {}),
                         ['11/15/2025', '02/07/2026'])


class TestRescrapeOk(unittest.TestCase):
    def test_empty_rescrape_rejected(self):
        self.assertFalse(ber.rescrape_ok([row('x')], [])[0])

    def test_filled_in_outcomes_accepted(self):
        old = [row('x', ''), row('x', '')]
        self.assertTrue(ber.rescrape_ok(old, [row('x'), row('x', 'Defeated')])[0])

    def test_lost_outcomes_rejected(self):
        old = [row('x'), row('x', 'Defeated')]
        ok, why = ber.rescrape_ok(old, [row('x'), row('x', '')])
        self.assertFalse(ok)
        self.assertIn('outcomes', why)

    def test_big_row_drop_rejected_small_drop_accepted(self):
        old = [row('x', '') for _ in range(10)]
        self.assertFalse(ber.rescrape_ok(old, old[:7])[0])   # a parish file failed
        self.assertTrue(ber.rescrape_ok(old, old[:9])[0])    # one withdrawal

    def test_growth_accepted(self):
        old = [row('x', '')]
        self.assertTrue(ber.rescrape_ok(old, [row('x', ''), row('x', '')])[0])


class TestReplaceDate(unittest.TestCase):
    def test_replaces_rather_than_appends(self):
        cands = {'A B': [row('10/14/2023'), row('06/27/2026', '')]}
        ber.replace_date(cands, '06/27/2026', {'A B': [row('06/27/2026', 'Elected')]})
        self.assertEqual([(c['date'], c['outcome']) for c in cands['A B']],
                         [('10/14/2023', 'Elected'), ('06/27/2026', 'Elected')])

    def test_other_dates_untouched_and_lists_stay_chronological(self):
        cands = {'A B': [row('10/14/2023'), row('11/03/2026')]}
        ber.replace_date(cands, '05/16/2026', {'A B': [row('05/16/2026', 'Runoff')]})
        self.assertEqual([c['date'] for c in cands['A B']],
                         ['10/14/2023', '05/16/2026', '11/03/2026'])

    def test_withdrawn_only_candidacy_drops_the_name(self):
        cands = {'A B': [row('12/12/2026', '')], 'C D': [row('12/12/2026', '')]}
        ber.replace_date(cands, '12/12/2026', {'C D': [row('12/12/2026', '')]})
        self.assertEqual(list(cands), ['C D'])


class FakePortal:
    """Stands in for ber.fetch: serves ElectionDates + one multiparish race per
    date, from {date: [(choice desc, outcome, votes), ...]}."""

    def __init__(self, races):
        self.races = races
        self.fetched = []

    def blob(self, ed):
        mm, dd, yy = ed.split('/')
        return f'{yy}{mm}{dd}'

    def __call__(self, blob, timeout=25, retries=2):
        self.fetched.append(blob)
        if blob == 'ElectionDates.htm':
            return {'Dates': {'Date': [{'ElectionDate': ed} for ed in self.races]}}
        for ed, choices in self.races.items():
            if blob == f'{self.blob(ed)}/ParishesInElection.htm':
                return {'ParishesInElection': {'Parish': []}}
            if blob == f'{self.blob(ed)}/RacesCandidates_Multiparish.htm':
                return {'Races': {'Race': {'ID': '1', 'SpecificTitle': 'State Senator -- 1st District',
                                           'Choice': [{'ID': str(i), 'Desc': d}
                                                      for i, (d, _, _) in enumerate(choices)]}}}
            if blob == f'{self.blob(ed)}/Votes_Multiparish.htm':
                return {'Races': {'Race': {'ID': '1', 'Choice': [
                    {'ID': str(i), 'VoteTotal': str(v), 'Outcome': o}
                    for i, (_, o, v) in enumerate(choices)]}}}
        return None


class TestMainIncremental(unittest.TestCase):
    def setUp(self):
        self._cwd = os.getcwd()
        self._tmp = tempfile.TemporaryDirectory()
        os.chdir(self._tmp.name)
        self._fetch = ber.fetch
        self.addCleanup(self._restore)

    def _restore(self):
        ber.fetch = self._fetch
        os.chdir(self._cwd)
        self._tmp.cleanup()

    def run_main(self, races):
        ber.fetch = FakePortal(races)
        with contextlib.redirect_stdout(io.StringIO()):
            ber.main()
        return ber.fetch

    def raw(self):
        with gzip.open(ber.RAW_FILE, 'rt', encoding='utf-8') as f:
            return json.load(f)

    # Long past, so neither date sits in the refresh window whenever this runs.
    SETTLED = {'10/14/2023': [('Ann Alpha (DEM)', 'Elected', 60), ('Bob Beta (REP)', 'Defeated', 40)]}
    PENDING = {'06/27/2026': [('Ann Alpha (DEM)', '', 0), ('Cy Gamma (REP)', '', 0)]}
    DECIDED = {'06/27/2026': [('Ann Alpha (DEM)', 'Defeated', 45), ('Cy Gamma (REP)', 'Elected', 55)]}

    def test_blank_date_is_filled_in_and_settled_date_is_not_refetched(self):
        self.run_main({**self.SETTLED, **self.PENDING})
        self.assertEqual([c['outcome'] for c in self.raw()['ANN ALPHA']], ['Elected', ''])

        portal = self.run_main({**self.SETTLED, **self.DECIDED})
        raw = self.raw()
        self.assertEqual([(c['date'], c['outcome']) for c in raw['ANN ALPHA']],
                         [('10/14/2023', 'Elected'), ('06/27/2026', 'Defeated')])
        self.assertEqual([c['outcome'] for c in raw['CY GAMMA']], ['Elected'])
        self.assertFalse([b for b in portal.fetched if b.startswith('20231014')])
        with open('la_election_results.json', encoding='utf-8') as f:
            self.assertEqual(json.load(f)['CY GAMMA']['outcome'], 'Elected')

    def test_no_change_leaves_files_untouched(self):
        def snapshot():
            with open(ber.RAW_FILE, 'rb') as f:
                return (f.read(), os.stat(ber.RAW_FILE).st_mtime_ns,
                        os.stat('la_election_results.json').st_mtime_ns)
        self.run_main({**self.SETTLED, **self.DECIDED})
        before = snapshot()
        self.run_main({**self.SETTLED, **self.DECIDED})
        self.assertEqual(snapshot(), before)

    def test_partial_rescrape_keeps_existing_rows(self):
        self.run_main({**self.SETTLED, **self.PENDING})
        before = self.raw()
        # The date is still unsettled, but this night the portal returns nothing.
        self.run_main({**self.SETTLED, '06/27/2026': []})
        self.assertEqual(self.raw(), before)


if __name__ == '__main__':
    unittest.main()
