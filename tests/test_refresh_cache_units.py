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
import gzip
import io
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


# ── download_and_cache: when a .tmp year file may be published ────────────────
# The downloader writes each year to `<year file>.tmp` and promotes it on
# completion. Two ways that used to publish wrong data:
#   1. a socket drop / read timeout part-way through the CSV promoted every
#      TRUNCATED year file over the previous good one, and its fresh mtime then
#      made refresh_la_cache + the Pages gate treat the gap as healed;
#   2. a record whose *date* fell outside the bundle's own years (a data-entry
#      typo, or a late-filed transaction carried on a neighbouring bundle)
#      replaced the complete year file the owning bundle had built.
_CSV_HEADER = ('ContributorName,ContributorCity,ContributorState,ContributorZip,'
               'ContributionAmt,ContributionDate,FilerFirstName,FilerLastName,'
               'FilerNumber,ReportNumber,ReportCode\n')


def _csv_row(amount, date):
    return (f'ACME,Baton Rouge,LA,70801,{amount},{date},'
            f'Jane,Doe,123,LA-1,F102\n')


class _FakeResponse(io.BufferedIOBase):
    """Stands in for urlopen()'s body. Raises at `fail_after` bytes to simulate
    a connection reset mid-stream."""

    def __init__(self, text, fail_after=None):
        self._buf = text.encode('utf-8')
        self._pos = 0
        self._fail_after = fail_after

    def readable(self):
        return True

    def read(self, size=-1):
        if self._fail_after is not None and self._pos >= self._fail_after:
            raise ConnectionResetError('simulated mid-stream drop')
        if size is None or size < 0:
            size = len(self._buf) - self._pos
        if self._fail_after is not None:
            size = min(size, self._fail_after - self._pos)
        chunk = self._buf[self._pos:self._pos + size]
        self._pos += len(chunk)
        return chunk

    read1 = read

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class TestDownloadPromotion(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self._orig_cache = srv.CACHE_DIR
        self._orig_urlopen = srv.urllib.request.urlopen
        self._orig_locks = srv._locks
        srv.CACHE_DIR = self._tmp.name
        srv._locks = {}

    def tearDown(self):
        srv.CACHE_DIR = self._orig_cache
        srv.urllib.request.urlopen = self._orig_urlopen
        srv._locks = self._orig_locks
        self._tmp.cleanup()

    def _serve(self, text, fail_after=None):
        srv.urllib.request.urlopen = (
            lambda req, timeout=None: _FakeResponse(text, fail_after))

    def _rows(self, year):
        p = srv._year_cache_path(year, 'contributions')
        if not os.path.exists(p):
            return None
        with gzip.open(p, 'rt', encoding='utf-8') as f:
            return sum(1 for line in f if line.strip())

    def _expire(self, *years):
        """Age year files past the TTL so the freshness gate lets a re-download in."""
        old = time.time() - srv.CACHE_TTL - 60
        for y in years:
            os.utime(srv._year_cache_path(y, 'contributions'), (old, old))

    def _tmps(self):
        return sorted(f for f in os.listdir(srv.CACHE_DIR) if f.endswith('.tmp'))

    def test_complete_download_publishes_every_year(self):
        self._serve(_CSV_HEADER + ''.join(
            _csv_row(100, f'3/{d}/2024') + _csv_row(200, f'4/{d}/2025')
            for d in range(1, 6)))
        srv.download_and_cache('2024-2027', 'contributions')
        self.assertEqual(self._rows(2024), 5)
        self.assertEqual(self._rows(2025), 5)
        self.assertEqual(self._tmps(), [])

    def test_mid_stream_failure_keeps_the_previous_good_files(self):
        self._serve(_CSV_HEADER + ''.join(
            _csv_row(100, f'3/{d}/2024') for d in range(1, 6)))
        srv.download_and_cache('2024-2027', 'contributions')
        self.assertEqual(self._rows(2024), 5)

        self._expire(2024)
        big = _CSV_HEADER + ''.join(
            _csv_row(100, f'3/{(d % 28) + 1}/2024') for d in range(400))
        self._serve(big, fail_after=len(big.encode('utf-8')) // 2)
        with self.assertRaises(ConnectionResetError):
            srv.download_and_cache('2024-2027', 'contributions')
        # Truncated years discarded, previous good file intact, no .tmp litter.
        self.assertEqual(self._rows(2024), 5)
        self.assertEqual(self._tmps(), [])

    def test_out_of_range_date_never_clobbers_another_bundles_year(self):
        self._serve(_CSV_HEADER + ''.join(
            _csv_row(50, f'5/{d}/2019') for d in range(1, 8)))
        srv.download_and_cache('2016-2019', 'contributions')
        self.assertEqual(self._rows(2019), 7)

        # The 2024-2027 bundle carries one row misdated into 2019.
        self._serve(_CSV_HEADER + _csv_row(100, '3/1/2024')
                    + _csv_row(100, '3/2/2024') + _csv_row(999, '6/1/2019'))
        srv.download_and_cache('2024-2027', 'contributions')
        self.assertEqual(self._rows(2019), 7)   # owning bundle's file survives
        self.assertEqual(self._rows(2024), 2)
        self.assertEqual(self._tmps(), [])

    def test_out_of_range_year_with_no_existing_file_is_still_written(self):
        self._serve(_CSV_HEADER + _csv_row(100, '3/1/2024') + _csv_row(7, '2/2/2018'))
        srv.download_and_cache('2024-2027', 'contributions')
        self.assertEqual(self._rows(2018), 1)   # nothing to clobber
        self.assertEqual(self._rows(2024), 1)


class TestCycleParam(unittest.TestCase):
    """?cycle= used to reach int() unguarded, so junk raised ValueError out of
    do_GET — the request died with no response at all."""

    def test_valid_year(self):
        self.assertEqual(srv._cycle_param({'cycle': ['2016']}), 2016)

    def test_default_when_absent(self):
        self.assertEqual(srv._cycle_param({}), 2024)

    def test_junk_is_rejected(self):
        for bad in ('', 'abc', '2024.5', '20', '02024', ' '):
            self.assertIsNone(srv._cycle_param({'cycle': [bad]}), bad)


if __name__ == '__main__':
    unittest.main()
