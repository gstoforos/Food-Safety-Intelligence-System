# -*- coding: utf-8 -*-
"""A detergent is not a food, and a firm name never carries a calendar date.

WHY THIS FILE EXISTS (morning fix 2026-11-22)
=============================================
Two rows, both PUBLISHED in Recalls on the morning of 2026-11-22.

THE CLEANING PRODUCT
--------------------
    Date      2026-11-13
    Source    NKFH (HU)
    Company   Ardap Care GmbH (Németország)
    Brand     Bactador
    Product   Enzym cleaner
    Pathogen  Pseudomonas aeruginosa
    Reason    Pseudomonas aeruginosa contamination
    URL       https://nkfh.gov.hu/hirek/termekvisszahivas-szag-es-
              folteltavolito-tisztitoszer-visszahivasa-mikrobiologiai-
              fertozes-veszelye-miatt-1

The NKFH notice, read that morning, names the product
"Geruchs- und Fleckenentferner Enzymreiniger 750 ml (Enzimes szag- és
folteltávolító tisztítószer háziállatok utáni takarításhoz)" in a
"750 ml-es műanyag szórófejes flakon" — a spray bottle of enzyme cleaner
for cleaning up after pets, notified through the EU Safety Gate.

This is the SECOND non-food row from the same collector in sixteen days.
The first was the NKFH electric shaver of 2026-11-03, which produced
_NON_FOOD_APPLIANCE_MARKERS (DURABLE GOODS) on 2026-11-06; the list beside
it, _NON_FOOD_PRODUCT_MARKERS, is COSMETICS. A detergent is neither, so
every non-food rule in the file was vacuous again — and Pathogen was filled
and filled correctly, so rule 1 could not help again either.

The rule reads Product and the URL slug and NOT Reason, because a food
recall may legitimately name a cleaning agent as the CONTAMINANT
("detergent residue in milk"); there the row is in scope and must publish.

THE DATED SENTENCE IN A NAME FIELD
----------------------------------
    Date      2026-11-19
    Source    RappelConso (FR)
    Company   "En Attente des Résultats D'Analyses,  Blocage des Coquillages
               Récoltés Dans L'Étang de Thau à Partir du 16 Novembre 2026"
    Brand     (the same sentence)
    URL       https://rappel.conso.gouv.fr/fiche-rappel/24121/interne

Read on the fiche: RappelConso's own "Nom de la marque du produit" field
holds that sentence, because the recall is a prefectoral blocking order with
no brand. Every firm-shape rule in _publish_gate passed it — section
headings, agency acronyms, page titles are all known; a sentence that is
simply not a name was not. The shape with no false positives is the DATE:
measured across all 7,460 rows of the 2026-11-22 workbook, the pattern
matched that row and its Weekly_Review copy and nothing else.

BOTH RULES LIVE AT BOTH DOORWAYS, like every scope rule here: the publish
gate and merge_master.validate_pending_row, imported rather than copied.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd                                             # noqa: E402

from pipeline._publish_gate import (                            # noqa: E402
    _dated_sentence_firm_blockers,
    _non_food_household_product_blockers,
    publish_blockers,
)
from pipeline.merge_master import validate_pending_row          # noqa: E402

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"

CLEANER = {
    "Date": "2026-11-13",
    "Source": "NKFH (HU)",
    "Company": "Ardap Care GmbH",
    "Brand": "Bactador",
    "Product": "Enzym cleaner",
    "Pathogen": "Pseudomonas aeruginosa",
    "Reason": "Pseudomonas aeruginosa contamination",
    "Class": "Recall",
    "Country": "Hungary",
    "Region": "Europe",
    "Tier": 2,
    "Outbreak": 0,
    "URL": ("https://nkfh.gov.hu/hirek/termekvisszahivas-szag-es-"
            "folteltavolito-tisztitoszer-visszahivasa-mikrobiologiai-"
            "fertozes-veszelye-miatt-1"),
    "Notes": "",
}

THAU = {
    "Date": "2026-11-19",
    "Source": "RappelConso (FR)",
    "Company": ("En Attente des Résultats D'Analyses,  Blocage des "
                "Coquillages Récoltés Dans L'Étang de Thau à Partir du "
                "16 Novembre 2026"),
    "Brand": ("En Attente des Résultats D'Analyses,  Blocage des "
              "Coquillages Récoltés Dans L'Étang de Thau à Partir du "
              "16 Novembre 2026"),
    "Product": "Shellfish harvested in the Étang de Thau",
    "Pathogen": "Marine biotoxins",
    "Reason": "PSP marine biotoxins — harvesting blocked as a precaution",
    "Class": "Recall",
    "Country": "France",
    "Region": "Europe",
    "Tier": 1,
    "Outbreak": 0,
    "URL": "https://rappel.conso.gouv.fr/fiche-rappel/24121/interne",
    "Notes": "",
}

#: A real food recall whose CONTAMINANT is a cleaning agent. It must publish.
DETERGENT_IN_FOOD = dict(
    CLEANER,
    Company="Acme Dairy SA",
    Brand="Acme",
    Product="Semi-skimmed UHT milk, 1 L brick, lot 2611A",
    Pathogen="Foreign material",
    Reason="Alkaline cleaning agent residue detected in the filling line",
    URL="https://www.aesan.gob.es/alertas/2026_91",
    Source="AESAN (ES)",
    Country="Spain",
)


class TestACleaningProductIsRefused(unittest.TestCase):
    def test_the_rule_names_the_cleaner(self):
        why = _non_food_household_product_blockers(CLEANER)
        self.assertTrue(why, "the Bactador enzyme cleaner was not refused")
        self.assertIn("cleaner", why[0])

    def test_the_publish_gate_refuses_it(self):
        self.assertTrue(
            any("cleaning or household chemical" in p
                for p in publish_blockers(CLEANER)),
            "the publish gate published a 750 ml bottle of enzyme cleaner")

    def test_the_pending_gate_refuses_it(self):
        ok, why = validate_pending_row(dict(CLEANER), set())
        self.assertFalse(ok, "the Pending gate queued a cleaning product")
        self.assertIn("non_food_household_product", why)

    def test_reason_alone_never_refuses_a_real_food_recall(self):
        self.assertEqual(
            [], _non_food_household_product_blockers(DETERGENT_IN_FOOD),
            "a food recall whose CONTAMINANT is a cleaning agent was refused "
            "— the rule must read Product and the URL, never Reason")


class TestAFirmNameIsNotADatedSentence(unittest.TestCase):
    def test_the_rule_names_the_date(self):
        why = _dated_sentence_firm_blockers(THAU)
        self.assertTrue(why, "a 119-character dated sentence passed as a firm")
        self.assertIn("16 Novembre 2026", why[0])

    def test_the_publish_gate_refuses_it(self):
        self.assertTrue(
            any("carries a calendar date" in p
                for p in publish_blockers(THAU)),
            "the publish gate published a notice sentence as the firm")

    def test_the_pending_gate_refuses_it(self):
        ok, why = validate_pending_row(dict(THAU), set())
        self.assertFalse(ok)
        self.assertIn("firm_name_is_a_dated_sentence", why)

    def test_an_ordinary_firm_name_passes(self):
        for name in ("GAEC La Louisa", "Société Coopérative Agricole des "
                     "Producteurs de Reblochons des Vallées de Thônes",
                     "C.H. Guenther & Son", "Origin: Togo | Notifying: "
                     "Belgium", "Maison du 16", "Café 1900"):
            self.assertEqual(
                [], _dated_sentence_firm_blockers({"Company": name,
                                                   "Brand": name}),
                f"{name!r} was refused as a dated sentence")


class TestTheLiveWorkbook(unittest.TestCase):
    def test_no_published_row_is_a_cleaning_product_or_a_dated_sentence(self):
        if not XLSX.exists():                           # pragma: no cover
            self.skipTest("no workbook")
        bad: list[str] = []
        for sheet in ("Recalls", "Pending", "Weekly_Review"):
            df = pd.read_excel(XLSX, sheet_name=sheet, dtype=str).fillna("")
            for i, r in df.iterrows():
                row = r.to_dict()
                for why in (_non_food_household_product_blockers(row)
                            + _dated_sentence_firm_blockers(row)):
                    bad.append(f"  {sheet} row {i + 2}: {why[:110]}")
        self.assertEqual([], bad, "\n".join(bad))


if __name__ == "__main__":                              # pragma: no cover
    unittest.main()
