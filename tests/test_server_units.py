#!/usr/bin/env python3
"""
test_server_units.py — unit tests for la_ethics_server.py's pure logic.

Covers the helpers and payload builders that power every endpoint: name
normalization, office classification, cycle/date math, ZIP→state lookup, party
enrichment, and the races / search / overview / industry payload shapes. These
run against the *committed* root artifacts only (la_candidacies_raw, the
candidate index, the politician lookup, donor industries, COH cache) — no
.la_cache/ data release and no network — so they pass in CI on a fresh clone.

Stdlib only (unittest). Run: python3 -m unittest discover -s tests -p 'test_*.py'
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import la_ethics_server as s  # noqa: E402


class TestNameNormalization(unittest.TestCase):
    def test_strips_honorifics_and_suffixes(self):
        self.assertEqual(s._norm_name('Dr. John A. Smith Jr.'), 'JOHN A SMITH')

    def test_punctuation_becomes_space_and_collapses(self):
        self.assertEqual(s._norm_name("  MARY-JANE  O'BRIEN III "), 'MARY JANE O BRIEN')

    def test_committee_names_preserved(self):
        self.assertEqual(s._norm_name('Committee to Elect Jane Doe'),
                         'COMMITTEE TO ELECT JANE DOE')

    def test_empty(self):
        self.assertEqual(s._norm_name(''), '')

    def test_non_ascii_dropped(self):
        # Documented pipeline keeps only A–Z and spaces.
        self.assertEqual(s._norm_name('Señor Núñez'), 'SE OR N EZ')


class TestOfficeType(unittest.TestCase):
    CASES = {
        'United States Senator': 'us_senate',
        'U. S. Representative -- 3rd Congressional District': 'us_house',
        'Governor': 'governor',
        'Lieutenant Governor': 'lt_governor',
        'State Senator -- 5th Senatorial District': 'state_senate',
        'State Representative -- 42nd Representative District': 'state_house',
        'Justice of the Supreme Court': 'supreme_court',
        'Public Service Commissioner -- District 3': 'board',
        'Attorney General': 'statewide',
        'Mayor': 'local',
        'Sheriff': 'local',
        # The server's classifier is intentionally coarse — these fall through.
        'Constable': 'other',
        'Constitutional Amendment No. 1': 'other',
    }

    def test_classification(self):
        for office, expected in self.CASES.items():
            with self.subTest(office=office):
                self.assertEqual(s._office_type(office), expected)


class TestCycleForYear(unittest.TestCase):
    def test_boundaries(self):
        self.assertEqual(s._cycle_for_year(2003), '2000-2003')
        self.assertEqual(s._cycle_for_year(2004), '2004-2007')
        self.assertEqual(s._cycle_for_year(2020), '2020-2023')
        self.assertEqual(s._cycle_for_year(2024), '2024-2027')

    def test_below_floor_and_above_ceiling(self):
        self.assertEqual(s._cycle_for_year(1999), '2000-2003')
        self.assertEqual(s._cycle_for_year(2030), '2024-2027')


class TestDateHelpers(unittest.TestCase):
    def test_date_key(self):
        self.assertEqual(s._date_key('11/05/2024'), '20241105')
        self.assertEqual(s._date_key('1/2/2003'), '20030102')

    def test_date_key_invalid(self):
        self.assertEqual(s._date_key(''), '')
        self.assertEqual(s._date_key('2024-11-05'), '')   # ISO is not the MM/DD/YYYY form
        self.assertEqual(s._date_key('garbage'), '')

    def test_iso_date(self):
        self.assertEqual(s._iso_date('11/05/2024'), '2024-11-05')
        self.assertEqual(s._iso_date(''), '')
        self.assertEqual(s._iso_date('garbage'), '')

    def test_date_key_sorts_chronologically(self):
        self.assertGreater(s._date_key('12/07/2024'), s._date_key('11/05/2024'))


class TestZipToState(unittest.TestCase):
    def test_louisiana_and_others(self):
        self.assertEqual(s._zip_to_state('70801'), 'LA')   # Baton Rouge
        self.assertEqual(s._zip_to_state('90210'), 'CA')
        self.assertEqual(s._zip_to_state('10001'), 'NY')

    def test_invalid(self):
        self.assertEqual(s._zip_to_state('00000'), '')
        self.assertEqual(s._zip_to_state('abcde'), '')
        self.assertEqual(s._zip_to_state(''), '')


class TestPartyLookup(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        s._load_politician_lookup()   # request paths load this before lookup_party

    def test_known_party(self):
        self.assertEqual(s.lookup_party('JEFF LANDRY'), 'REP')
        self.assertEqual(s.lookup_party('JOHN BEL EDWARDS'), 'DEM')

    def test_unknown_defaults_to_oth(self):
        self.assertEqual(s.lookup_party('NOBODY AT ALL XYZ'), 'OTH')

    def test_detail_shape(self):
        d = s.lookup_party_detail('JEFF LANDRY')
        self.assertIsInstance(d, dict)
        self.assertIn('party', d)


class TestBuildRacesPayload(unittest.TestCase):
    """build_races_payload runs on committed data (candidacies + career index)."""

    @classmethod
    def setUpClass(cls):
        cls.major = s.build_races_payload('major', '')

    def test_top_level_shape(self):
        self.assertEqual(set(self.major), {'races', 'total', 'years'})
        self.assertEqual(self.major['total'], len(self.major['races']))
        self.assertGreater(self.major['total'], 0)

    def test_years_descending(self):
        years = self.major['years']
        self.assertEqual(years, sorted(years, reverse=True))

    def test_race_and_candidate_keys(self):
        race = self.major['races'][0]
        self.assertEqual(set(race), {'candidates', 'date', 'office', 'office_type', 'year'})
        cand = race['candidates'][0]
        for k in ('name', 'party', 'outcome', 'vote_pct', 'filer_number',
                  'cycle', 'raised', 'spent', 'coh_ending'):
            self.assertIn(k, cand)

    def test_races_sorted_by_raw_date_string_desc(self):
        # NOTE: the server orders races by the raw 'MM/DD/YYYY' string descending
        # (lexicographic, i.e. month-first — not strictly chronological). The
        # dashboard re-sorts by a true YYYYMMDD key for display; this asserts the
        # payload's actual contract so a future change is a deliberate one.
        dates = [r['date'] for r in self.major['races']]
        self.assertEqual(dates, sorted(dates, reverse=True))

    def test_candidates_sorted_by_vote_pct_desc(self):
        for race in self.major['races']:
            pcts = [(c['vote_pct'] or 0) for c in race['candidates']]
            self.assertEqual(pcts, sorted(pcts, reverse=True))

    def test_year_filter(self):
        p = s.build_races_payload('all', '2023')
        self.assertTrue(all(r['year'] == 2023 for r in p['races']))
        self.assertGreater(p['total'], 0)

    def test_office_filter_major_only_major_types(self):
        major_types = {'governor', 'us_senate', 'us_house', 'statewide', 'lt_governor'}
        self.assertTrue(all(r['office_type'] in major_types for r in self.major['races']))

    def test_office_filter_all_is_superset(self):
        every = s.build_races_payload('all', '')
        self.assertGreaterEqual(every['total'], self.major['total'])


class TestBuildSearchEntries(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.entries = s.build_search_entries()

    def test_nonempty(self):
        self.assertGreater(len(self.entries), 0)

    def test_entry_keys(self):
        e = self.entries[0]
        for k in ('name', 'name_upper', 'is_candidate', 'total_raised',
                  'n_cycles', 'n_races', 'filer_number'):
            self.assertIn(k, e)

    def test_name_upper_is_upper(self):
        e = self.entries[0]
        self.assertEqual(e['name_upper'], e['name_upper'].upper())


class TestBuildIndustryBreakdown(unittest.TestCase):
    def test_shape(self):
        s._load_filer_lookup()   # populates _FILER_NUM (name -> filer number)
        filer = next(iter((s._FILER_NUM or {}).values()), None)
        self.assertIsNotNone(filer, 'expected a filer number in the committed index')
        ib = s.build_industry_breakdown(filer, '2024-2027')
        self.assertEqual(set(ib), {'breakdown', 'cycle', 'filer', 'has_industry_data'})
        self.assertIsInstance(ib['breakdown'], list)


class TestBuildOverviewPayload(unittest.TestCase):
    def test_shape(self):
        ov = s.build_overview_payload()
        for k in ('n_candidates', 'n_committees', 'n_elections',
                  'top_fundraisers', 'years'):
            self.assertIn(k, ov)
        self.assertIsInstance(ov['top_fundraisers'], list)


class TestCrossLanguageNormParity(unittest.TestCase):
    """Lock the server normalizer to the same outputs the static-client test
    asserts for static_api.js normName — the cross-dataset join needs both
    languages to agree byte-for-byte."""

    PAIRS = [
        ('Dr. John A. Smith Jr.', 'JOHN A SMITH'),
        ("Mary-Jane O'Brien III", 'MARY JANE O BRIEN'),
        ('Committee to Elect Jane Doe', 'COMMITTEE TO ELECT JANE DOE'),
    ]

    def test_pipeline(self):
        for raw, expected in self.PAIRS:
            with self.subTest(raw=raw):
                self.assertEqual(s._norm_name(raw), expected)


if __name__ == '__main__':
    unittest.main(verbosity=2)
