"""
The fourth agent — the parts that must stay true.

The Cyclospora case is the fixture: one record in the whole register, Outbreak
= 1, Tier 1, week of 13–19 July 2026, and the count detector cannot test it.
If a future change makes that record score a p-value, or stops reporting it
as novel, this suite says so before a reader does.
"""
from __future__ import annotations

import math
import unittest
from pathlib import Path

import pandas as pd

from pipeline import signal_detector as S
from pipeline import signal_review_agent as R

XLSX = Path(__file__).resolve().parent.parent / "docs" / "data" / "recalls.xlsx"


class TestPoissonTail(unittest.TestCase):
    def test_zero_observed_is_certain(self):
        self.assertEqual(R.poisson_sf(0, 2.5), 1.0)

    def test_matches_closed_form_for_k_one(self):
        # P[X >= 1] = 1 - e^{-λ}
        for lam in (0.3, 1.0, 4.2):
            self.assertAlmostEqual(R.poisson_sf(1, lam), 1 - math.exp(-lam), places=12)

    def test_monotone_in_k(self):
        vals = [R.poisson_sf(k, 3.0) for k in range(0, 12)]
        self.assertEqual(vals, sorted(vals, reverse=True))

    def test_zero_rate_cannot_form_a_tail(self):
        # λ = 0 with k > 0: the agent reports "no baseline" rather than 0.0 as
        # a p-value; the helper itself returns 0 and the caller must not
        # present that as significance.
        self.assertEqual(R.poisson_sf(3, 0.0), 0.0)


@unittest.skipUnless(XLSX.exists(), "live workbook not present")
class TestCyclosporaIsNovelNotScored(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.corpus = S.load_corpus(str(XLSX))
        cls.strata = S.build_strata(cls.corpus)
        cls.week = pd.Timestamp("2026-07-18").to_period("W-SUN")

    def test_detector_cannot_score_it(self):
        # the count channel suppresses the stratum before any test runs
        s = self.strata.get("global::Cyclospora")
        self.assertIsNotNone(s, "Cyclospora stratum missing from the corpus")
        idx = self.corpus.weeks.index(self.week)
        b2 = S._window(s.series, self.corpus.weeks, idx, S.BASELINE_WEEKS, S.GUARD_WEEKS)
        self.assertEqual(sum(b2), 0)
        self.assertLess(int(s.series.get(self.week, 0)), S.MIN_ABSOLUTE_COUNT)
        sigs, _ = S.detect(self.corpus, self.strata, asof=self.week)
        self.assertFalse(any(x.stratum_key == "global::Cyclospora" for x in sigs))

    def test_review_agent_reports_it_as_novel(self):
        nov = R.novelty(self.corpus, self.strata, self.week)
        hit = [n for n in nov if n["pathogen"] == "Cyclospora"]
        self.assertEqual(len(hit), 1)
        n = hit[0]
        self.assertFalse(n["testable"])
        self.assertTrue(n["first_appearance_ever"])
        self.assertEqual(n["severity"], "outbreak-flagged")
        self.assertGreaterEqual(n["outbreak_records"], 1)
        self.assertNotIn("p_value", n, "a novel stratum must not carry a p-value")

    def test_a_stratum_with_a_baseline_is_not_novel(self):
        nov = R.novelty(self.corpus, self.strata, self.corpus.weeks[-1])
        self.assertFalse(any(n["pathogen"] == "Listeria monocytogenes" for n in nov))

    def test_outbreak_channel_shape(self):
        oc = R.outbreak_channel(self.corpus, self.week)
        self.assertEqual(len(oc["baseline"]), S.BASELINE_WEEKS)
        self.assertGreaterEqual(oc["observed"], 1)
        if oc["baseline_mean"] > 0:
            self.assertTrue(0.0 <= oc["poisson_p_upper"] <= 1.0)

    def test_news_is_marked_stale_for_a_replayed_week(self):
        nw = R.news_corroboration(XLSX, self.strata, self.week, dry_run=True,
                                  is_latest=False)
        self.assertFalse(nw["applies_to_week"])
        self.assertIn("STALE", nw["note"])


if __name__ == "__main__":
    unittest.main()
