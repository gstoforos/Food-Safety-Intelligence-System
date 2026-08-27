"""Coverage register: temporal stability, and the inference rules.

WHY THIS FILE
=============
`source_coverage.py` decides which weeks of the corpus are analysable.
TR-2026-01 derives its analytical window from that decision — the window
opens nine weeks after the LATEST maturity among continuous sources — so
an unstable maturity moves a published claim.

It was unstable. Rebuilding the register at successive corpus cut-offs
made `mature_week` oscillate, not merely drift:

    CFIA (CA)   2025-12-29 -> 2026-01-12 -> 2026-03-16 -> 2026-01-12
                -> 2025-12-29 -> 2026-01-12
    FSAI (IE)   2026-04-06 -> 2026-06-01 -> 2026-04-06

Both FSAI values were produced by this code on this corpus. They imply
different analytical windows — 8 June versus 3 August — and under the
second, six of the seven signal weeks in TR-2026-01 fall outside it.

Maturity is therefore frozen once determined. These tests pin that, and
pin the one exception: a source with too little post-onset history is
PROVISIONAL and keeps moving, because freezing it early locks in a
pre-ramp answer.
"""
from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.source_coverage import (  # noqa: E402
    build_register, classify_raw_source, infer_onset, infer_maturity,
    infer_outages, SourceCoverage, MATURITY_REF_WEEKS, ONSET_WINDOW)

WEEKS = [f"2026-{m:02d}-{d:02d}/2026-{m:02d}-{d + 6:02d}"
         for m, d in [(1, 1), (1, 8), (1, 15), (1, 22)]]


def _weeks(n):
    return [f"W{i:03d}" for i in range(n)]


class TestOnset(unittest.TestCase):

    def test_isolated_early_record_does_not_establish_onset(self):
        """RappelConso's single January record is a manual entry, not
        evidence the scraper was live. Crediting it backdates coverage by
        a month and reintroduces the bias the module exists to remove."""
        w = _weeks(20)
        active = {w[0]} | set(w[10:16])
        self.assertEqual(w[10], infer_onset(active, w))

    def test_onset_requires_the_run_to_start_at_an_active_week(self):
        w = _weeks(20)
        active = set(w[5:11])
        self.assertEqual(w[5], infer_onset(active, w))

    def test_no_onset_when_never_sustained(self):
        w = _weeks(20)
        self.assertIsNone(infer_onset({w[2], w[9], w[15]}, w))

    def test_source_shorter_than_the_window(self):
        """Edge: fewer weeks in the corpus than ONSET_WINDOW must not raise
        or claim an onset it cannot support."""
        w = _weeks(3)
        self.assertIsNone(infer_onset(set(w), w))


class TestMaturity(unittest.TestCase):

    def test_a_ramp_is_detected_after_the_flat_start(self):
        w = _weeks(MATURITY_REF_WEEKS + 12)
        vols = {x: 1 for x in w[:8]}
        vols.update({x: 40 for x in w[8:]})
        m = infer_maturity(vols, w, w[0])
        self.assertIsNotNone(m)
        self.assertGreater(m, w[0], "a clear ramp was not detected")

    def test_flat_series_matures_at_onset(self):
        w = _weeks(MATURITY_REF_WEEKS + 12)
        vols = {x: 20 for x in w}
        self.assertEqual(w[0], infer_maturity(vols, w, w[0]))

    def test_no_onset_means_no_maturity(self):
        self.assertIsNone(infer_maturity({}, _weeks(20), None))

    def test_zero_reference_does_not_divide_by_zero(self):
        w = _weeks(MATURITY_REF_WEEKS + 12)
        vols = {x: 5 for x in w[:4]}          # everything recent is zero
        self.assertEqual(w[0], infer_maturity(vols, w, w[0]))


class TestOutages(unittest.TestCase):

    def test_a_trailing_gap_is_not_an_outage(self):
        """Closing a gap that has not ended asserts an ending that cannot
        be observed."""
        w = _weeks(20)
        active = set(w[:10])
        self.assertEqual([], infer_outages(active, w, w[0]))

    def test_an_interior_gap_is_an_outage(self):
        w = _weeks(20)
        active = set(w[:5]) | set(w[12:])
        outs = infer_outages(active, w, w[0])
        self.assertEqual(1, len(outs))
        self.assertEqual(w[5], outs[0]["start"])
        self.assertEqual(w[11], outs[0]["end"])

    def test_a_short_gap_is_not_an_outage(self):
        w = _weeks(20)
        active = set(w[:5]) | set(w[7:])
        self.assertEqual([], infer_outages(active, w, w[0]))


class TestCanonicalisation(unittest.TestCase):

    def test_the_documented_merges(self):
        for a, b in (("FAVV (BE)", "AFSCA"),
                     ("BVL (DE)", "Lebensmittelwarnung.de"),
                     ("Salute (IT)", "Ministero della Salute")):
            self.assertEqual(classify_raw_source(a)[0],
                             classify_raw_source(b)[0], f"{a} vs {b}")

    def test_aggregators_are_not_primary(self):
        n, k = classify_raw_source("CFS (HK) - aggregator (RappelConso FR)")
        self.assertEqual("aggregator", k)

    def test_hong_kong_primary_is_not_swept_up_with_its_aggregator_rows(self):
        self.assertEqual("primary", classify_raw_source("CFS (HK)")[1])

    def test_news_is_not_a_coverage_source(self):
        self.assertEqual("news", classify_raw_source("Food Safety News")[1])

    def test_blank_source(self):
        self.assertEqual("unknown", classify_raw_source(None)[1])
        self.assertEqual("unknown", classify_raw_source("  ")[1])


class TestObservability(unittest.TestCase):

    def _sc(self, **kw):
        base = dict(source="X", kind="primary", coverage_class="continuous",
                    records=50, first_record="2026-01-01",
                    last_record="2026-08-01", onset_week="W002",
                    mature_week="W006", onset_basis="inferred")
        base.update(kw)
        return SourceCoverage(**base)

    def test_immature_weeks_are_not_observable_by_default(self):
        sc = self._sc()
        self.assertFalse(sc.is_observable("W004"))
        self.assertTrue(sc.is_observable("W006"))

    def test_require_mature_false_falls_back_to_onset(self):
        sc = self._sc()
        self.assertTrue(sc.is_observable("W004", require_mature=False))
        self.assertFalse(sc.is_observable("W001", require_mature=False))

    def test_outage_weeks_are_not_observable(self):
        sc = self._sc(outages=[{"start": "W010", "end": "W013", "weeks": "4"}])
        self.assertFalse(sc.is_observable("W011"))
        self.assertTrue(sc.is_observable("W014"))

    def test_sporadic_is_never_observable(self):
        self.assertFalse(self._sc(coverage_class="sporadic").is_observable("W020"))


class TestTemporalStability(unittest.TestCase):
    """The defect this fix exists for, exercised on the live corpus."""

    @classmethod
    def setUpClass(cls):
        cls.xlsx = ROOT / "docs" / "data" / "recalls.xlsx"
        if not cls.xlsx.exists():                          # pragma: no cover
            raise unittest.SkipTest("recalls.xlsx not present")
        df = pd.read_excel(cls.xlsx, "Recalls")
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        cls.df = df[df["Date"].notna()].copy()
        cls.df["wk"] = cls.df["Date"].dt.to_period("W-SUN").astype(str)
        cls.weeks = sorted(cls.df["wk"].unique())
        cls.tmp = tempfile.mkdtemp()

    def _build_at(self, cut, frozen):
        sub = self.df[self.df["wk"] <= cut].drop(columns=["wk"])
        xp = os.path.join(self.tmp, "cut.xlsx")
        sub.to_excel(xp, sheet_name="Recalls", index=False)
        return build_register(xp, frozen=frozen)[0]

    def test_frozen_maturity_does_not_move_as_the_corpus_grows(self):
        carried, seen = {}, {}
        for cut in self.weeks[14::3] + [self.weeks[-1]]:
            reg = self._build_at(cut, carried)
            for name, sc in reg.items():
                if sc.mature_week and sc.mature_basis != "provisional":
                    seen.setdefault(name, set()).add(sc.mature_week)
            carried = reg
        moved = {n: sorted(v) for n, v in seen.items() if len(v) > 1}
        self.assertEqual({}, moved,
                         f"frozen maturity moved for {list(moved)} — a "
                         f"published analytical cutoff can shift under it")

    def test_without_freezing_it_does_move(self):
        """Guard the guard: if this ever stops failing, the instability is
        gone for another reason and the freeze may no longer be needed."""
        seen = {}
        for cut in self.weeks[14::3] + [self.weeks[-1]]:
            for name, sc in self._build_at(cut, {}).items():
                if sc.mature_week:
                    seen.setdefault(name, set()).add(sc.mature_week)
        moved = {n for n, v in seen.items() if len(v) > 1}
        self.assertTrue(moved,
                        "unfrozen maturity is now stable on this corpus; "
                        "re-derive whether freezing is still required")

    def test_a_rebuild_reuses_the_frozen_value(self):
        reg1 = build_register(str(self.xlsx), refreeze=True)[0]
        reg2 = build_register(str(self.xlsx), frozen=reg1)[0]
        for name, sc in reg2.items():
            if name in reg1 and reg1[name].mature_week:
                self.assertEqual(reg1[name].mature_week, sc.mature_week, name)

    def test_provisional_sources_are_flagged_not_frozen(self):
        early = self._build_at(self.weeks[10], {})
        bases = {s.mature_basis for s in early.values() if s.mature_week}
        self.assertIn("provisional", bases,
                      "on a short corpus every maturity was frozen; an "
                      "under-evidenced value is being locked in")


if __name__ == "__main__":
    unittest.main()
