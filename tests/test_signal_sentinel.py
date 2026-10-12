"""
Sentinel lane — C. botulinum is flagged at ONE record (operator 2026-10-12:
"C. bot is deadly").

The count detector needs >= 4 records and a scorable baseline, so it can
never see a botulism notification. Week 41 of 2026 is the fixture: two
C. botulinum records (BLV (CH) pork shank terrine, outbreak; RASFF honey from
Spain) and one Salmonella outbreak record (FSA, Ukrainian eggs). If a change
drops any of them from the sentinel lane, or lets the lane leak into the
detector's alarm list, this suite says so.
"""
from __future__ import annotations

import json
import re
import unittest
from pathlib import Path

import pandas as pd

from pipeline import signal_detector as S
from pipeline import signal_review_agent as R

ROOT = Path(__file__).resolve().parent.parent
XLSX = ROOT / "docs" / "data" / "recalls.xlsx"


class TestSentinelList(unittest.TestCase):
    def test_botulinum_is_a_sentinel(self):
        self.assertIn("Clostridium botulinum", R.SENTINEL_HAZARDS)

    def test_every_sentinel_is_a_canonical_label(self):
        canon = {c for _, c in S.PATHOGEN_RULES}
        for h in R.SENTINEL_HAZARDS:
            self.assertIn(h, canon, f"{h!r} is not a canon_pathogen label — it would never match")

    def test_spellings_reach_the_sentinel(self):
        for raw in ("Clostridium botulinum", "C. botulinum toxin", "botulisme",
                    "Clostridium botulinum (suspected)"):
            self.assertEqual(S.canon_pathogen(raw), "Clostridium botulinum", raw)


class TestBotulismRiskIndicator(unittest.TestCase):
    """Operator 2026-10-12: "Flag them" — container swelling and
    sterilization failures, from the regulator's own words."""

    def test_swelling_wordings(self):
        for t in ("risk of swelling", "swollen pouches", "bulging cans", "blown cans",
                  "bombage des boîtes", "risque de gonflement", "latas abombadas",
                  "envases hinchados", "rigonfiamento delle confezioni"):
            self.assertTrue(R.botulism_risk_indicator("", t), t)

    def test_process_failure_wordings(self):
        for t in ("Inadequate sterilization process", "Insufficient heat treatment",
                  "under-processed", "underprocessed low-acid canned food",
                  "stérilisation insuffisante", "défaut de stérilisation",
                  "not commercially sterile", "esterilización insuficiente"):
            self.assertTrue(R.botulism_risk_indicator("", t), t)

    def test_no_false_hits(self):
        for t in ("Salmonella in sunflower kernels from Bulgaria",
                  "Possible incomplete pasteurization (process deviation)",
                  "Listeria monocytogenes", "", None):
            self.assertFalse(R.botulism_risk_indicator("", t), t)

    def test_pasteurized_but_swollen_still_flags(self):
        self.assertTrue(R.botulism_risk_indicator("", "Pasteurized juice, swollen containers"))


@unittest.skipUnless(XLSX.exists(), "live workbook not present")
class TestWeek41(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.corpus = S.load_corpus(str(XLSX))
        cls.week = pd.Timestamp("2026-10-07").to_period("W-SUN")
        cls.sn = R.sentinel(cls.corpus, cls.week)

    def test_both_botulinum_records_flagged(self):
        bot = [i for i in self.sn["items"] if i["hazard"] == "Clostridium botulinum"]
        self.assertEqual(len(bot), 2)
        self.assertEqual({i["source"] for i in bot}, {"BLV (CH)", "RASFF (EU)"})
        for i in bot:
            self.assertIn("sentinel hazard", i["why"])
            self.assertTrue(i["url"].startswith("https://"), i)

    def test_swollen_crab_flagged_as_indicator(self):
        ind = [i for i in self.sn["items"] if "botulism-risk indicator" in i["why"]]
        self.assertTrue(any(i["source"] == "RappelConso (FR)" and "crab" in i["product"].lower()
                            for i in ind), ind)
        for i in ind:
            self.assertNotIn("sentinel hazard", i["why"])   # an indicator, not a C. bot finding

    def test_outbreak_records_flagged(self):
        self.assertTrue(any(i["source"] == "FSA (UK)" and i["outbreak"]
                            for i in self.sn["items"]))

    def test_order_hazard_then_indicator_then_outbreak(self):
        rank = [0 if "sentinel hazard" in i["why"] else
                1 if "botulism-risk indicator" in i["why"] else 2
                for i in self.sn["items"]]
        self.assertEqual(rank, sorted(rank))

    def test_below_the_detector_floor(self):
        # the reason the lane exists: the detector cannot score this
        strata = S.build_strata(self.corpus)
        s = strata.get("global::Clostridium botulinum")
        self.assertIsNotNone(s)
        self.assertLess(int(s.series.get(self.week, 0)), S.MIN_ABSOLUTE_COUNT)
        sigs, _ = S.detect(self.corpus, strata, asof=self.week)
        self.assertFalse(any("botulinum" in x.label for x in sigs))


class TestPagesRenderIt(unittest.TestCase):
    def test_dashboard_signals_tab(self):
        html = (ROOT / "docs" / "index.html").read_text(encoding="utf-8")
        self.assertIn('id="sg-sentinel"', html)
        self.assertIn("sgSentinel(meta)", html)

    def test_full_page(self):
        html = (ROOT / "docs" / "signals.html").read_text(encoding="utf-8")
        self.assertIn('id="sentinel-card"', html)
        self.assertIn("rv.sentinel", html)

    def test_published_review_carries_the_lane(self):
        p = ROOT / "docs" / "data" / "signals-review.json"
        if not p.exists():
            self.skipTest("no review written yet")
        rv = json.loads(p.read_text(encoding="utf-8-sig"))
        self.assertIn("sentinel", rv)
        self.assertIn("sentinel", rv["meta"]["channels"])
        for i in rv["sentinel"]["items"]:
            self.assertTrue(re.match(r"^https?://", i["url"]), i)


if __name__ == "__main__":
    unittest.main()
