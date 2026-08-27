"""The detector's coverage gate.

WHY
===
TR-2026-01 §2.2 states that signals are reported only where the entire
detection baseline falls after mature collection. Until 2026-08-27,
`signal_detector.py` contained no reference to `source_coverage` at all —
zero mentions of the module, of `observable`, or of `sporadic`. The claim
was true only because the window had been applied by hand.

That is a bad way for a published constraint to exist. These tests pin it
as a property of the code, and pin the two things that make the gate safe
to add to a live system: it is additive, and it flags rather than drops
unless explicitly told to enforce.

Also recorded here: the gate independently reproduces 2026-06-08 as the
window start, which is the date the report states. The manual application
was correct; it just was not checkable.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.signal_detector import (  # noqa: E402
    coverage_window_start, coverage_status, BASELINE_WEEKS, GUARD_WEEKS)
from pipeline.source_coverage import load_register, SourceCoverage  # noqa: E402


def _reg(mature="2026-04-06/2026-04-12", klass="continuous"):
    return {"X": SourceCoverage(
        source="X", kind="primary", coverage_class=klass, records=100,
        first_record="2026-01-01", last_record="2026-08-01",
        onset_week="2026-02-02/2026-02-08", mature_week=mature,
        onset_basis="inferred")}


class TestWindowArithmetic(unittest.TestCase):

    def test_the_window_opens_a_full_baseline_after_maturity(self):
        ws = coverage_window_start(_reg())
        expected = pd.Period("2026-04-06", freq="W-SUN") + (
            BASELINE_WEEKS + GUARD_WEEKS)
        self.assertEqual(str(expected), ws)

    def test_the_latest_maturity_governs_not_the_earliest(self):
        reg = _reg()
        reg["Y"] = SourceCoverage(
            source="Y", kind="primary", coverage_class="continuous",
            records=50, first_record="2026-01-01", last_record="2026-08-01",
            onset_week="2026-01-05/2026-01-11",
            mature_week="2026-06-01/2026-06-07", onset_basis="inferred")
        ws = coverage_window_start(reg)
        self.assertEqual(str(pd.Period("2026-06-01", freq="W-SUN")
                             + (BASELINE_WEEKS + GUARD_WEEKS)), ws)

    def test_non_continuous_sources_do_not_set_the_window(self):
        """An intermittent source maturing late must not push the window
        out — the fleet the baseline rests on is the continuous one."""
        reg = _reg()
        reg["Z"] = SourceCoverage(
            source="Z", kind="primary", coverage_class="intermittent",
            records=12, first_record="2026-01-01", last_record="2026-08-01",
            onset_week="2026-07-06/2026-07-12",
            mature_week="2026-08-10/2026-08-16", onset_basis="inferred")
        self.assertEqual(coverage_window_start(_reg()),
                         coverage_window_start(reg))


class TestGateIsAdditive(unittest.TestCase):

    def test_absent_register_does_not_break_detection(self):
        st = coverage_status(pd.Period("2026-07-13", freq="W-SUN"), register={})
        self.assertEqual("absent", st["coverage_register"])
        self.assertIsNone(st["within_coverage_window"])

    def test_absent_register_says_what_to_do(self):
        st = coverage_status(pd.Period("2026-07-13", freq="W-SUN"), register={})
        self.assertIn("source_coverage", st["coverage_note"])

    def test_a_register_with_no_continuous_sources_yields_no_window(self):
        self.assertIsNone(coverage_window_start(_reg(klass="sporadic")))


class TestClassification(unittest.TestCase):

    def test_weeks_inside_and_outside(self):
        reg = _reg()
        ws = coverage_window_start(reg)
        inside = pd.Period(ws.split("/")[0], freq="W-SUN")
        self.assertTrue(coverage_status(inside, reg)["within_coverage_window"])
        self.assertFalse(
            coverage_status(inside - 1, reg)["within_coverage_window"])

    def test_an_out_of_window_week_says_why(self):
        reg = _reg()
        st = coverage_status(pd.Period("2026-04-20", freq="W-SUN"), reg)
        self.assertFalse(st["within_coverage_window"])
        self.assertIn("scraper fleet coming online", st["coverage_note"])


class TestAgainstThePublishedReport(unittest.TestCase):

    def test_the_live_register_reproduces_the_published_window(self):
        """TR-2026-01 bounds analysis to weeks from 8 June 2026. The gate
        must derive that independently, or the report and the code
        disagree about what is publishable."""
        reg = load_register()
        if not reg:                                        # pragma: no cover
            self.skipTest("register not built")
        ws = coverage_window_start(reg)
        self.assertIsNotNone(ws)
        self.assertTrue(ws.startswith("2026-06-08"),
                        f"gate says {ws}; TR-2026-01 says 2026-06-08")

    def test_every_signal_week_in_the_report_is_inside(self):
        reg = load_register()
        if not reg:                                        # pragma: no cover
            self.skipTest("register not built")
        for wk in ("2026-06-15", "2026-06-29", "2026-07-06", "2026-07-13",
                   "2026-07-20", "2026-07-27", "2026-08-10"):
            st = coverage_status(pd.Period(wk, freq="W-SUN"), reg)
            self.assertTrue(st["within_coverage_window"],
                            f"{wk} is published but falls outside the window")


if __name__ == "__main__":
    unittest.main()


class TestEffectMatchesChannel(unittest.TestCase):
    """`effect` reported next to a COUNT signal must be a count ratio.

    It was the share ratio for every signal, while the published table
    labelled it observed-over-baseline. On the 20 signals inside the
    analytical window the two disagreed on 8 — and one disagreement was
    disqualifying rather than cosmetic: Listeria / France, week of 27
    July, is a COUNT-only signal whose share ratio is 0.85. A signals
    table with an effect below 1 tells the reader the stratum fell in the
    same row that says it alarmed.
    """

    def test_no_reported_effect_is_below_one(self):
        from pipeline.signal_detector import load_corpus, build_strata, detect
        xlsx = ROOT / "docs" / "data" / "recalls.xlsx"
        if not xlsx.exists():                              # pragma: no cover
            self.skipTest("recalls.xlsx not present")
        corpus = load_corpus(str(xlsx))
        strata = build_strata(corpus)
        bad = []
        for w in corpus.weeks:
            sigs, meta = detect(corpus, strata, asof=w)
            if meta.get("within_coverage_window") is False:
                continue
            for s in sigs:
                if s.effect < 1.0:
                    bad.append((str(w), s.label, s.channel, s.effect))
        self.assertEqual([], bad,
                         f"signals reported with an effect below 1: {bad}")

    def test_count_signals_use_the_count_ratio(self):
        from pipeline.signal_detector import load_corpus, build_strata, detect
        xlsx = ROOT / "docs" / "data" / "recalls.xlsx"
        if not xlsx.exists():                              # pragma: no cover
            self.skipTest("recalls.xlsx not present")
        corpus = load_corpus(str(xlsx))
        strata = build_strata(corpus)
        for w in corpus.weeks:
            sigs, meta = detect(corpus, strata, asof=w)
            if meta.get("within_coverage_window") is False:
                continue
            for s in sigs:
                want = s.effect_count if s.channel != "proportion" else s.effect_share
                self.assertAlmostEqual(want, s.effect, places=2,
                                       msg=f"{w} {s.label} {s.channel}")

    def test_both_ratios_are_retained(self):
        """Neither value may be lost — the other is what was published."""
        from pipeline.signal_detector import load_corpus, build_strata, detect
        xlsx = ROOT / "docs" / "data" / "recalls.xlsx"
        if not xlsx.exists():                              # pragma: no cover
            self.skipTest("recalls.xlsx not present")
        corpus = load_corpus(str(xlsx))
        strata = build_strata(corpus)
        sigs, _ = detect(corpus, strata, asof=corpus.weeks[-1])
        for s in sigs:
            self.assertGreater(s.effect_share, 0.0)
            self.assertGreater(s.effect_count, 0.0)
