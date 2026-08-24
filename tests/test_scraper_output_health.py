"""The output-based scraper health check.

WHY
===
scraper-health.json measures REACHABILITY and counts OK_EMPTY — reachable,
returning nothing — toward "ok". On 2026-08-22 it reported fail_pct 21.8
against a 33.0 threshold, no alarm, while 33 of 55 scrapers had produced
no row for 30+ days and 23 had never produced one at all.

The tests below pin the two things that make an output check trustworthy:
it must classify on the age of the last ROW, and its name matching must
not let one busy source cover for a dead one.
"""
from __future__ import annotations

import datetime as dt
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.scraper_output_health import (  # noqa: E402
    _norm, classify, PRODUCING, QUIET, DEAD, NEVER)


class TestNameMatching(unittest.TestCase):
    """The country bracket must survive normalisation."""

    def test_fda_variants_stay_distinct(self):
        """FDA (GH), FDA (PH) and FDA are three different regulators.
        Collapsing them let Ghana and the Philippines inherit the US
        FDA's daily rows and report PRODUCING — FDA (PH) is FAIL_403 and
        has produced nothing since 2026-06-14."""
        keys = {_norm("FDA/USA"), _norm("FDA (GH)/Ghana"),
                _norm("FDA (PH)/Philippines")}
        self.assertEqual(3, len(keys), f"variants collapsed: {keys}")

    def test_health_label_matches_the_source_label(self):
        self.assertEqual(_norm("AESAN (ES)/Spain"), _norm("AESAN (ES)"))
        self.assertEqual(_norm("RappelConso (FR)/France"),
                         _norm("RappelConso (FR)"))

    def test_aggregator_suffix_is_dropped(self):
        self.assertEqual(_norm("CFS (HK) - aggregator (RappelConso FR)"),
                         _norm("CFS (HK)"))

    def test_two_different_agencies_do_not_merge(self):
        self.assertNotEqual(_norm("Salute (IT)"), _norm("AESAN (ES)"))
        self.assertNotEqual(_norm("MPI (NZ)"), _norm("MFDS (KR)"))


class TestClassification(unittest.TestCase):

    def test_the_boundaries(self):
        self.assertEqual(PRODUCING, classify(0, 14, 30))
        self.assertEqual(PRODUCING, classify(13, 14, 30))
        self.assertEqual(QUIET, classify(14, 14, 30))
        self.assertEqual(QUIET, classify(29, 14, 30))
        self.assertEqual(DEAD, classify(30, 14, 30))
        self.assertEqual(DEAD, classify(215, 14, 30))

    def test_never_is_not_the_same_as_dead(self):
        """A source that has never produced a row is a different fact from
        one that used to and stopped. Reporting both as 'failed' hides
        which scrapers were never wired up."""
        self.assertEqual(NEVER, classify(None, 14, 30))

    def test_a_silent_source_can_never_be_producing(self):
        for age in (30, 58, 149, 215):
            self.assertNotEqual(PRODUCING, classify(age, 14, 30), age)


class TestAgainstTheLiveRegister(unittest.TestCase):

    def test_it_runs_and_finds_the_busy_sources(self):
        from tools.scraper_output_health import last_row_by_source
        xlsx = ROOT / "docs" / "data" / "recalls.xlsx"
        if not xlsx.exists():                          # pragma: no cover
            self.skipTest("recalls.xlsx not present")
        last = last_row_by_source(xlsx)
        self.assertIn(_norm("RappelConso (FR)"), last)
        self.assertIn(_norm("RASFF (EU)"), last)

    def test_no_source_has_a_future_last_row(self):
        """A DateAdded in the future makes a dead scraper look fresh and
        makes 'days since last row' negative."""
        from tools.scraper_output_health import last_row_by_source
        xlsx = ROOT / "docs" / "data" / "recalls.xlsx"
        if not xlsx.exists():                          # pragma: no cover
            self.skipTest("recalls.xlsx not present")
        today = dt.datetime.now(dt.timezone.utc).date()
        future = {src: day for day, src in last_row_by_source(xlsx).values()
                  if day > today.isoformat()}
        self.assertEqual({}, future,
                         f"sources carrying a future DateAdded: {future}")


if __name__ == "__main__":
    unittest.main()
