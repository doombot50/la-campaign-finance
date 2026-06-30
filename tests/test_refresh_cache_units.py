#!/usr/bin/env python3
"""
test_refresh_cache_units.py — unit tests for refresh_la_cache.py's self-heal.

Guards the bundle-completeness logic that decides whether a non-current cycle is
re-downloaded. The regression this protects against: a single lost year file
(contributions_yr2003, deleted by an interrupted `gh release upload --clobber`)
was skipped because *any* present year file counted as "have it", so the gap
never healed and the Pages completeness gate blocked every deploy.

Stdlib only (unittest). Mirrors build_pages_site.py's gate:
  - contributions: every year file must be present (upstream is contiguous)
  - expenditures/loans: >=1 file per bundle (empty years are legitimate)
"""
import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import la_ethics_server as srv  # noqa: E402
import refresh_la_cache as rc  # noqa: E402


class TestBundleComplete(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self._orig_cache = srv.CACHE_DIR
        srv.CACHE_DIR = self._tmp.name

    def tearDown(self):
        srv.CACHE_DIR = self._orig_cache
        self._tmp.cleanup()

    def _touch(self, rtype, year):
        open(srv._year_cache_path(year, rtype), 'wb').close()

    # ── contributions: every year must be present ─────────────────────────────
    def test_contributions_all_years_present_is_complete(self):
        for y in (2000, 2001, 2002, 2003):
            self._touch('contributions', y)
        self.assertTrue(rc._bundle_complete('2000-2003', 'contributions'))

    def test_contributions_one_missing_year_is_incomplete(self):
        # The exact 2003 regression: 2000/2001/2002 present, 2003 lost.
        for y in (2000, 2001, 2002):
            self._touch('contributions', y)
        self.assertFalse(rc._bundle_complete('2000-2003', 'contributions'))

    # ── expenditures/loans: one file per bundle is enough ─────────────────────
    def test_expenditures_one_year_present_is_complete(self):
        # expenditures 2008 is genuinely empty upstream; 2009-2011 carry the data.
        for y in (2009, 2010, 2011):
            self._touch('expenditures', y)
        self.assertTrue(rc._bundle_complete('2008-2011', 'expenditures'))
        # Requiring every year here would re-download this bundle forever.
        self.assertFalse(
            os.path.exists(srv._year_cache_path(2008, 'expenditures')))

    def test_loans_one_year_present_is_complete(self):
        self._touch('loans', 2024)
        self.assertTrue(rc._bundle_complete('2024-2027', 'loans'))

    def test_empty_bundle_is_incomplete_for_all_types(self):
        for rtype in ('contributions', 'expenditures', 'loans'):
            self.assertFalse(rc._bundle_complete('2000-2003', rtype))


if __name__ == '__main__':
    unittest.main()
