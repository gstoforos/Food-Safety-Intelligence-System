"""Bacillus cereus is Tier 1, whatever the product matrix.

OPERATOR RULE 2026-08-20: "b.cereus netherlands tier 1".

WHAT THIS REPLACES
==================
tests/test_low_moisture_matrix.py encoded the previous rule: bare
B. cereus forced Tier 1 only in a LOW-MOISTURE product, because
cereulide — the heat-stable emetic toxin — forms in dry matrices like
rice, spices and powdered formula. Good microbiology, wrong rule for
this register: it made published severity depend on a matrix inference
the regulator never made.

NVWA's 2026-08-19 warning on Milbona High Protein Pudding names
B. cereus and names the groups at risk (infants, toddlers, pregnant
women, elderly, immunocompromised). A chilled high-moisture dessert
scored Tier 2 on that warning while a garlic powder scored Tier 1 on
the same organism.

Measured across the register before the change, "Bacillus cereus" was
published at THREE tiers — 3 rows T1, 4 rows T2, 1 row T3 — while all
27 rows labelled with the toxin were T1. Same organism, three
severities, decided by a matrix guess.

The rule is FORWARD-ACTING. enforce_tier1 runs on promotion and merge,
not retroactively over Recalls, so the four historical rows in closed
weeks (W17, W27, W29) do not move without an explicit operator
decision. That is deliberate: rewriting a published week is not a side
effect.
"""
from __future__ import annotations

import re
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline._pathogen_scope import enforce_tier1  # noqa: E402


def tier(**row):
    row.setdefault("Tier", 2)
    enforce_tier1(row)
    return int(row["Tier"])


class TestAlwaysTierOne(unittest.TestCase):

    HIGH_MOISTURE = [
        ("chilled dessert", "Milbona High Protein Pudding Chocolate, 200 g"),
        ("fresh herb", "fresh rosemary from Morocco"),
        ("bakery", "tartelette framboises"),
        ("produce", "tomatoes from Türkiye"),
        ("patisserie", "Duchesse pralinée x2, in-store bakery"),
    ]
    LOW_MOISTURE = [
        ("spice", "Garlic Powder 70 g"),
        ("dried", "dried mushrooms from China"),
        ("spice", "ground cinnamon from Madagascar"),
        ("formula", "powdered infant formula 800 g"),
    ]

    def test_high_moisture_products_are_tier_1(self):
        for label, product in self.HIGH_MOISTURE:
            self.assertEqual(1, tier(Pathogen="Bacillus cereus",
                                     Product=product, Tier=2), label)

    def test_low_moisture_products_are_still_tier_1(self):
        for label, product in self.LOW_MOISTURE:
            self.assertEqual(1, tier(Pathogen="Bacillus cereus",
                                     Product=product, Tier=2), label)

    def test_it_lifts_from_tier_3_too(self):
        self.assertEqual(1, tier(Pathogen="Bacillus cereus", Tier=3,
                                 Product="Duchesse pralinée"))

    def test_the_toxin_label_is_unaffected(self):
        for p in ("Cereulide", "Cereulide (B. cereus toxin)",
                  "Cereulide (Bacillus cereus toxin)",
                  "Bacillus cereus / cereulide"):
            self.assertEqual(1, tier(Pathogen=p, Tier=2), p)

    def test_it_is_idempotent(self):
        row = {"Pathogen": "Bacillus cereus", "Tier": 2, "Product": "pudding"}
        for _ in range(3):
            enforce_tier1(row)
        self.assertEqual(1, int(row["Tier"]))
        self.assertEqual(1, str(row["Notes"]).count("tier-guard"))

    def test_it_does_not_reach_other_organisms(self):
        """The carve-out is for B. cereus only — it must not become a
        blanket 'any bacillus' escalation."""
        for p in ("Bacillus subtilis", "Bacillus licheniformis"):
            row = {"Pathogen": p, "Tier": 2, "Product": "rice"}
            enforce_tier1(row)
            self.assertEqual(2, int(row["Tier"]), p)

    def test_the_moisture_gate_is_gone_from_the_source(self):
        src = (ROOT / "pipeline" / "_pathogen_scope.py").read_text(
            encoding="utf-8")
        body = src.split("def enforce_tier1", 1)[1].split("\ndef ", 1)[0]
        code = "\n".join(ln.split("#", 1)[0] for ln in body.splitlines())
        self.assertNotIn("_is_low_moisture_product(row)", code,
                         "enforce_tier1 still gates B. cereus on the "
                         "product matrix")


class TestThePublishedRow(unittest.TestCase):

    def test_the_nvwa_pudding_row_is_tier_1(self):
        import openpyxl
        xlsx = ROOT / "docs" / "data" / "recalls.xlsx"
        if not xlsx.exists():                              # pragma: no cover
            self.skipTest("recalls.xlsx not present")
        wb = openpyxl.load_workbook(xlsx, read_only=True, data_only=True)
        rows = list(wb["Recalls"].values)
        hdr = [str(h) for h in rows[0]]
        hits = [dict(zip(hdr, r)) for r in rows[1:] if r
                and "veiligheidswaarschuwing-milbona"
                in str(dict(zip(hdr, r)).get("URL") or "")]
        if not hits:                                       # pragma: no cover
            self.skipTest("row not in the register")
        self.assertEqual(1, int(hits[0]["Tier"]))


if __name__ == "__main__":
    unittest.main()
