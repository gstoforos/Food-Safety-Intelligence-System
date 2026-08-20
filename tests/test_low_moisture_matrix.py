"""A flavour word is not a food matrix.

WHY (2026-08-20)
================
Bare "Bacillus cereus" is forced to Tier 1 in LOW-MOISTURE products,
because B. cereus spores survive drying and germinate on rehydration —
the cereulide risk. The NVWA row

    Milbona High Protein Pudding Chocolate Flavour, 200 g
    Bacillus cereus · chilled dairy dessert · THT 14-09-2026

was escalated to Tier 1 because "chocolate" is in _LOW_MOISTURE_KEYWORDS.
That entry is correct for chocolate bars, cocoa and chocolate powder. Here
the word is a FLAVOUR DESCRIPTOR on a refrigerated pudding — the opposite
matrix from the one the rule is about.

Same shape as "salmon" inside "Salmonella" and "cholera" inside
"cholerae": a keyword matching a word that describes something other than
what the rule is for.

TWO THINGS THIS FILE GUARDS, AND THE SECOND MATTERS MORE
--------------------------------------------------------
1. Wet matrices are vetoed first, so a flavour word cannot outvote them.
2. The veto stays NARROW. Its first draft also contained "paste",
   "spread", "dip", "sauce" and "soup". Tahini, sesame paste, chocolate
   spread and peanut butter are LOW-moisture matrices and classic
   Salmonella vehicles — sesame paste was the vehicle in the cluster this
   register was tracking the same week. Those five words would have
   quietly demoted exactly the rows the rule exists to catch. Bare "cream"
   was dropped for the same reason: "cream cracker" is dry.

The dry cases below are therefore not filler. They are the regression
that a wider veto would cause.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline._pathogen_scope import (  # noqa: E402
    _is_low_moisture_product, _strip_accents, enforce_tier1)


def dry(product, reason=""):
    return _is_low_moisture_product({"Product": product, "Reason": reason})


class TestWetMatricesAreNotDry(unittest.TestCase):

    def test_the_nvwa_pudding(self):
        self.assertFalse(dry("Milbona High Protein Pudding Chocolate "
                             "Flavour, 200 g"))

    def test_flavour_words_on_wet_products(self):
        for p in ("Chocolate flavour yoghurt 150g",
                  "Vanilla dessert 200g",
                  "Chocolate ice cream 500ml",
                  "Chilled chocolate mousse",
                  "Crème dessert chocolat",
                  "Chocolate milkshake 250ml"):
            self.assertFalse(dry(p), p)


class TestDryMatricesStayDry(unittest.TestCase):
    """The regression a wider veto would have caused."""

    def test_low_moisture_spreads_and_pastes(self):
        for p in ("Purée de Sésame Tahin (pot 350 g)",
                  "Organic sesame paste",
                  "Chocolate spread 400g",
                  "Peanut butter smooth 340g",
                  "Tahini 500g"):
            self.assertTrue(dry(p), f"{p} was vetoed — it is a low-moisture "
                                    f"matrix and a known Salmonella vehicle")

    def test_cream_cracker_is_dry(self):
        self.assertTrue(dry("Cream crackers 200g"))

    def test_the_classic_dry_matrices(self):
        for p in ("Chocolate powder 500g", "Dark chocolate biscuit selection",
                  "Infant formula milk powder", "Basmati rice 1kg",
                  "Dried herbs and spice mix", "Wheat flour 1kg",
                  "Roasted peanuts", "Couscous 500g"):
            self.assertTrue(dry(p), p)

    def test_fresh_still_excludes_first(self):
        self.assertFalse(dry("Fresh chocolate croissant"))


class TestAccentFolding(unittest.TestCase):
    """The keyword list is ASCII, so every accented product name in the
    register was invisible to this classifier."""

    def test_strip_accents(self):
        self.assertEqual("puree de sesame tahin",
                         _strip_accents("purée de sésame tahin"))
        self.assertEqual("epices sechees", _strip_accents("épices séchées"))

    def test_accented_sesame_matches(self):
        self.assertTrue(dry("Purée de Sésame Tahin (pot 350 g)"))

    def test_ascii_is_untouched(self):
        self.assertEqual("plain ascii 123", _strip_accents("plain ascii 123"))


class TestTheTierOutcome(unittest.TestCase):

    def test_pudding_is_not_forced_to_tier_1(self):
        row = {"Pathogen": "Bacillus cereus", "Tier": 2,
               "Product": "Milbona High Protein Pudding Chocolate Flavour",
               "Reason": "Bacillus cereus detected"}
        enforce_tier1(row)
        self.assertEqual(2, int(row["Tier"]))

    def test_dry_matrix_is_still_forced_to_tier_1(self):
        row = {"Pathogen": "Bacillus cereus", "Tier": 3,
               "Product": "Chocolate powder 500g", "Reason": "Bacillus cereus"}
        enforce_tier1(row)
        self.assertEqual(1, int(row["Tier"]))

    def test_the_published_nvwa_row(self):
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
        self.assertEqual(2, int(hits[0]["Tier"]),
                         "the chilled pudding is published as Tier 1 again")


if __name__ == "__main__":
    unittest.main()
