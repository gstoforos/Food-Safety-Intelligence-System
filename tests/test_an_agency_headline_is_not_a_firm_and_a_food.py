# -*- coding: utf-8 -*-
"""A press-release headline is one sentence, not a firm plus a food.

WHY THIS FILE EXISTS (morning fix pass 2026-11-15)
==================================================
Published Tier 1 in the small hours of 2026-11-15, Recalls row 2:

    Date     2026-11-14                 Source   USDA FSIS
    Company  'FSIS Issues Public Health'
    Brand    'FSIS Issues Public Health'
    Product  'for Ready-to-Eat Meat ...'
    Pathogen 'Listeria monocytogenes'
    URL      fsis.usda.gov/recalls-alerts/fsis-issues-public-health-alert-
             ready-eat-meat-products-containing-biscuits-recalled

The notice's headline is "FSIS Issues Public Health Alert for Ready-to-Eat
Meat Products Containing Biscuits Recalled …".
gap_finder_tavily._TITLE_SPLIT_RX looks for a recall verb and cuts the title
at it; _RECALL_VERBS contains the bare word "alert", which in an agency
headline sits in the MIDDLE of the sentence with the agency as its subject.
Left of the cut became Company, that module copies Company into Brand, and
right of the cut became Product. The terminator list does contain "for", but
the Product group needs one character before a terminator can end it, so a
leading "for" cannot stop it and the rest of the headline was taken whole.

The 2026-09-16 audit deleted the "whole headline as Company" FALLBACK after
the Sjømathuset row. This row proves the other half: a MATCH that cuts in
the wrong place yields the same defect, and nothing checked the match.

So both doorways are held here:

  the collector  — a cut whose left side has the agency as its sentence
                   subject, or whose right side opens with a bare function
                   word, is discarded; the row carries no Company and no
                   Product and the gate refuses it. One function, imported
                   by the exa, ddgs and brave finders and by
                   daily_recall_search.
  the gate       — the same shapes are refused outright, plus a truncation
                   ellipsis in any name field, so a row like this cannot be
                   published whatever wrote it.

AND THE LIVE REGISTER, because a rule that only holds the next row is half
a fix: no published row, and no row in Pending or Weekly_Review, may carry
any of these shapes.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline._publish_gate import publish_blockers          # noqa: E402
from pipeline.gap_finder_tavily import (                     # noqa: E402
    _extract_company_product,
    _split_is_a_headline,
)

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"

#: The headline that produced the row, as DDGS delivered it.
FSIS_TITLE = ("FSIS Issues Public Health Alert for Ready-to-Eat Meat "
              "Products Containing Biscuits Recalled ...")


class TestTheCollectorRefusesTheCut(unittest.TestCase):

    def test_the_fsis_headline_yields_no_fields(self):
        company, product = _extract_company_product(FSIS_TITLE, "")
        self.assertEqual(("", ""), (company, product),
                         "the agency headline was split into a firm and a "
                         "food again")

    def test_every_agency_subject_headline_is_refused(self):
        for title in (
            "FSIS Issues Public Health Alert for Ready-to-Eat Meat Products",
            "CFIA Issues Public Health Notice for Frozen Berries",
            "FDA issues a food safety alert for imported sesame paste",
        ):
            company, product = _extract_company_product(title, "")
            self.assertEqual(("", ""), (company, product), title)

    def test_a_product_opening_on_a_function_word_is_refused(self):
        self.assertTrue(_split_is_a_headline("Someone", "for Ready-to-Eat"))
        self.assertTrue(_split_is_a_headline("Someone", "of Chlorella Powder"))
        self.assertTrue(_split_is_a_headline("Someone", "and alerts"))

    def test_a_real_headline_still_splits(self):
        """The fix must not cost the cuts that work."""
        for title, want in (
            ("Acme Foods Inc. recalls Sausage product due to Listeria",
             ("Acme Foods Inc.", "Sausage product")),
            ("Brand X Issues Voluntary Recall of Chicken Breast over E. coli",
             ("Brand X", "Chicken Breast")),
            ("Sjømathuset AS tilbakekaller Lerøy laks loin 600g og 250g "
             "etter mistanke om listeria | Mattilsynet",
             ("Sjømathuset AS",
              "Lerøy laks loin 600g og 250g etter mistanke om listeria")),
        ):
            self.assertEqual(want, _extract_company_product(title, ""), title)

    def test_a_capitalised_opener_is_a_product_not_a_fragment(self):
        """Recalls row 963 is a real multi-item list: 'With fresh cheese
        140g; ktipiti 175g Atelier; …'. The rule is case-sensitive so that
        it stays a product."""
        self.assertFalse(
            _split_is_a_headline("Atelier",
                                 "With fresh cheese 140g; ktipiti 175g"))


class TestTheGateRefusesTheRow(unittest.TestCase):

    BASE = {
        "Date": "2026-11-14", "Source": "USDA FSIS", "Pathogen":
        "Listeria monocytogenes", "Reason": "Listeria monocytogenes",
        "Class": "Recall", "Country": "United States",
        "Region": "North America", "Tier": 1, "Outbreak": 0,
        "URL": ("https://www.fsis.usda.gov/recalls-alerts/fsis-issues-"
                "public-health-alert-ready-eat-meat-products-containing-"
                "biscuits-recalled"),
        "Notes": "x",
    }

    def _blockers(self, **over):
        row = dict(self.BASE)
        row.update(over)
        return publish_blockers(row)

    def test_the_row_as_published_is_refused(self):
        bad = self._blockers(Company="FSIS Issues Public Health",
                             Brand="FSIS Issues Public Health",
                             Product="for Ready-to-Eat Meat ...")
        self.assertTrue(bad, "the gate still publishes a split headline")
        joined = " | ".join(bad)
        self.assertIn("headline", joined)
        self.assertIn("function word", joined)
        self.assertIn("ellipsis", joined)

    def test_an_agency_subject_company_is_refused(self):
        self.assertTrue(self._blockers(
            Company="FSIS Issues Public Health", Brand="",
            Product="Ready-to-eat meat products"))

    def test_a_truncation_ellipsis_in_a_name_field_is_refused(self):
        """Found live on its first run: Recalls row 1453 carried Brand
        'Bestar Europe | Agence fédérale pour la sécurité de ...'."""
        self.assertTrue(self._blockers(
            Company="Bestar Europe",
            Brand="Bestar Europe | Agence fédérale pour la sécurité de ...",
            Product="Peanut butter"))

    def test_the_repaired_row_passes(self):
        self.assertEqual([], self._blockers(
            Company="(not named in the FSIS text held on this row)",
            Brand="",
            Product=("Ready-to-eat meat products containing FDA-regulated "
                     "biscuits")))


class TestTheLiveRegisterCarriesNoneOfIt(unittest.TestCase):

    #: The substrings the three new refusals use. A new rule that only
    #: guards the next row is half a fix.
    MARKERS = ("authority's own headline",
               "bare function word",
               "truncation ellipsis")

    def test_no_live_row_carries_a_split_headline(self):
        import openpyxl
        if not XLSX.exists():                              # pragma: no cover
            self.skipTest("recalls.xlsx not present")
        wb = openpyxl.load_workbook(XLSX, read_only=True, data_only=True)
        offenders = []
        for sheet in ("Recalls", "Pending", "Weekly_Review"):
            if sheet not in wb.sheetnames:                 # pragma: no cover
                continue
            rows = list(wb[sheet].values)
            hdr = [str(h) for h in rows[0]]
            for n, raw in enumerate(rows[1:], start=2):
                if not raw:                                # pragma: no cover
                    continue
                row = dict(zip(hdr, raw))
                for b in publish_blockers(row):
                    if any(m in b for m in self.MARKERS):
                        offenders.append(f"[{sheet} row {n}] {b}")
        self.assertEqual([], offenders,
                         "live rows carry a split page headline:\n  "
                         + "\n  ".join(offenders))


class TestTheLanguageDetectorSeesGerman(unittest.TestCase):
    """The companion miss of the same morning.

    BLV (CH) published 'Mini-Hähnchenbrust-Filetstücke; Mini Klassik, Mini
    Paprika und Mini Curry' on 2026-11-13, Tier 1, Listeria. It carried no
    cleanup stamp and product_needs_english() returned False: this module's
    Latin-script detectors were French-only, because every untranslated
    Product found up to 2026-10-27 had come from RappelConso. It sat in the
    register and on the 13 November daily brief for two days.
    """

    def test_a_german_product_is_caught(self):
        from pipeline._language import product_needs_english
        for p in ("Mini-Hähnchenbrust-Filetstücke; Mini Klassik, Mini "
                  "Paprika und Mini Curry",
                  "Leberwurst fein geschnitten, 100 Gramm",
                  "Gerookte zalm, gesneden"):
            self.assertTrue(product_needs_english(p), p)

    def test_an_english_row_keeping_the_original_still_passes(self):
        """The house convention is English first, the regulator's own words
        in brackets. Both of these are live published rows."""
        from pipeline._language import product_needs_english
        for p in ("Savory, rubbed and dried (Bohnenkraut, gerebelt und "
                  "getrocknet), 25 g",
                  "Kägi Butterbiscuits (butter biscuits)",
                  "Liver sausage, finely sliced, 100 grams",
                  "Mini chicken breast fillet pieces, in the Mini Klassik, "
                  "Mini Paprika and Mini Curry varieties"):
            self.assertFalse(product_needs_english(p), p)

    def test_no_published_product_is_left_in_german(self):
        import openpyxl
        from pipeline._language import product_needs_english
        if not XLSX.exists():                              # pragma: no cover
            self.skipTest("recalls.xlsx not present")
        wb = openpyxl.load_workbook(XLSX, read_only=True, data_only=True)
        rows = list(wb["Recalls"].values)
        hdr = [str(h) for h in rows[0]]
        bad = []
        for n, raw in enumerate(rows[1:], start=2):
            if not raw:                                    # pragma: no cover
                continue
            r = dict(zip(hdr, raw))
            p = str(r.get("Product") or "")
            if p and product_needs_english(p):
                bad.append(f"  row {n} [{r.get('Source')}] {p[:70]!r}")
        self.assertEqual([], bad,
                         "published Products are not in English:\n"
                         + "\n".join(bad[:20]))


if __name__ == "__main__":
    unittest.main()
