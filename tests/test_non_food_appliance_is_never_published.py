# -*- coding: utf-8 -*-
"""A durable good is not a food, and a disposal hazard is not a food hazard.

WHY THIS FILE EXISTS (morning fix 2026-11-06)
=============================================
On the morning of 2026-11-06 the register carried this row, PUBLISHED, in
Recalls — promoted a few hours earlier by pipeline.promote_gate_passing,
whose own report line reads "passes every publish-gate rule":

    Date      2026-11-03
    Source    NKFH (HU)
    Company   Adler Sp. z o. o. (Lengyelország)
    Brand     Camry
    Product   Electric shaver
    Pathogen  Mercury (heavy metal)
    Reason    Serious environmental hazard! The internal power sources of
              the electric shavers contain an excessive amount of mercury.
              Mercury is difficult to dissolve and can permanently damage
              the soil and groundwater when the discarded products reach
              the ground.
    Tier      2
    URL       https://nkfh.gov.hu/hirek/termekvisszahivas-vezetek-nelkueli-
              elektromos-borotva-visszahivasa-sulyos-koernyezetvedelmi-
              kockazat-miatt-1

A cordless electric shaver, recalled because its batteries hold too much
mercury to throw away, in a food recall register.

Three non-food rules already existed and each was built for another shape:
publish-gate rule 1 refuses an EMPTY Pathogen (which is how the 2026-08-01
audit's passenger car and bath toy were stopped), _non_food_drug_blockers
tests dosage forms and pharmacy firms, and _NON_FOOD_PRODUCT_MARKERS is a
list of COSMETICS words. Here Pathogen was filled, and filled correctly —
"mercury" is the word on the page and mercury in food is in scope — so the
only test that could have caught it was vacuous.

The two rules under test are therefore about the row's own text and nothing
else: the product is a durable good, or the harm named is to soil and
groundwater rather than to the person eating. The second is what separates
this row from the register's genuine mercury rows, which say "Mercury
exceeding limit" in fish and nothing about groundwater.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline._publish_gate import (                            # noqa: E402
    _non_food_appliance_blockers, publish_blockers,
)

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"

#: The row as it was actually published, trimmed to the fields the rules read.
SHAVER = {
    "Date": "2026-11-03",
    "Source": "NKFH (HU)",
    "Company": "Adler Sp. z o. o. (Lengyelország)",
    "Brand": "Camry",
    "Product": "Electric shaver",
    "Pathogen": "Mercury (heavy metal)",
    "Reason": ("Serious environmental hazard! The internal power sources of "
               "the electric shavers contain an excessive amount of mercury. "
               "Mercury is difficult to dissolve and can permanently damage "
               "the soil and groundwater when the discarded products reach "
               "the ground."),
    "Class": "Recall",
    "Country": "Hungary",
    "Region": "Europe",
    "Tier": 2,
    "URL": ("https://nkfh.gov.hu/hirek/termekvisszahivas-vezetek-nelkueli-"
            "elektromos-borotva-visszahivasa-sulyos-koernyezetvedelmi-"
            "kockazat-miatt-1"),
}

#: Mercury in a food, which IS in scope. The register has published this
#: shape ("Mercury (heavy metal)", "Mercury exceeding limit"), so a rule
#: that refuses it would be a regression, not a fix.
MERCURY_IN_FISH = {
    "Date": "2026-11-03",
    "Source": "RASFF (EU)",
    "Company": "A fishing company",
    "Brand": "—",
    "Product": "Frozen swordfish loins",
    "Pathogen": "Mercury (heavy metal)",
    "Reason": ("Mercury content of 1.9 mg/kg, above the maximum level of "
               "1.0 mg/kg for this species."),
    "Class": "Recall",
    "Country": "Spain",
    "Region": "Europe",
    "Tier": 2,
    "URL": "https://webgate.ec.europa.eu/rasff-window/screen/notification/999999",
}


class TestTheRuleItself(unittest.TestCase):
    def test_the_shaver_row_is_refused(self):
        why = _non_food_appliance_blockers(SHAVER)
        self.assertTrue(why, "the published 2026-11-03 shaver row must be "
                             "refused by this rule")
        # BOTH tests must fire: the product is an appliance AND the hazard
        # is environmental. Either one alone would have stopped this row,
        # and the row is the only evidence we have that either works.
        self.assertEqual(2, len(why), why)

    def test_the_shaver_row_does_not_pass_the_whole_gate(self):
        self.assertTrue(publish_blockers(SHAVER),
                        "publish_blockers() must refuse the shaver row — on "
                        "2026-11-06 it returned [] and the row was promoted")

    def test_mercury_in_a_food_still_passes(self):
        self.assertEqual([], _non_food_appliance_blockers(MERCURY_IN_FISH),
                         "mercury IN a food is in scope; only a disposal "
                         "hazard is not")

    def test_a_razor_clam_is_a_food(self):
        """The marker list is multi-word on purpose.

        A bare "razor" would refuse razor clams, a bare "battery" would
        refuse battery-farmed eggs, and a bare "mat" or "toy" is too
        common a word to put in a gate. If someone shortens a marker,
        this is the test that says why they should not.
        """
        row = dict(MERCURY_IN_FISH,
                   Product="Live razor clams (Ensis siliqua), 1 kg net",
                   Pathogen="Escherichia coli (generic)",
                   Reason="Escherichia coli above the live-bivalve limit.")
        self.assertEqual([], _non_food_appliance_blockers(row))

    def test_battery_farmed_eggs_are_a_food(self):
        row = dict(MERCURY_IN_FISH,
                   Product="Battery eggs, size M, 10-pack",
                   Pathogen="Salmonella",
                   Reason="Salmonella Enteritidis detected in a sample.")
        self.assertEqual([], _non_food_appliance_blockers(row))


class TestTheRegister(unittest.TestCase):
    """No sheet the dashboard or a subscriber reads may carry this shape."""

    @classmethod
    def setUpClass(cls):
        if not XLSX.exists():                           # pragma: no cover
            raise unittest.SkipTest(f"no workbook at {XLSX}")
        import pandas as pd
        cls.book = pd.ExcelFile(XLSX)

    def _offenders(self, sheet):
        if sheet not in self.book.sheet_names:          # pragma: no cover
            return []
        out = []
        for row in self.book.parse(sheet).to_dict("records"):
            why = _non_food_appliance_blockers(row)
            if why:
                out.append((str(row.get("Date"))[:10], str(row.get("Source")),
                            str(row.get("Product"))[:60], why[0][:80]))
        return out

    def test_recalls_carries_no_non_food_appliance(self):
        self.assertEqual([], self._offenders("Recalls"))

    def test_weekly_review_carries_no_non_food_appliance(self):
        """The Sunday review slice is a mirror of what was promoted.

        The shaver was in it too, at row 16 — a fix applied only to
        Recalls would leave the operator's own review email disagreeing
        with the register.
        """
        self.assertEqual([], self._offenders("Weekly_Review"))

    def test_pending_carries_no_non_food_appliance(self):
        self.assertEqual([], self._offenders("Pending"))


if __name__ == "__main__":                              # pragma: no cover
    unittest.main()
