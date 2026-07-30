"""Regression tests for the Pathogen-vs-Reason hazard-class guard.

WHY THIS FILE EXISTS (audit 2026-07-30)
=======================================
`_pathogen_reason_class_mismatch` was added on 2026-06-14 specifically to
catch rows where the enrichment step invented a Pathogen that its own Reason
field contradicts. It shipped with six hazard classes and no allergen class,
so the most common recall reason in the world was unclassifiable:

    _classify_hazard("...undeclared allergen (peanuts)")  ->  set()

`_pathogen_reason_class_mismatch` bails out whenever EITHER side is
unclassifiable, so it returned False, `_is_clean_row` returned True, the
clean-row shortcut skipped verification entirely, and the row was promoted
with a fabricated pathogen at Tier 1.

Two rows reached production that way and were confirmed wrong against the
live FSANZ pages — the word "Listeria" appears nowhere on either:

    Recalls row  3  2026-07-29  Auxico (Perth) Pty Ltd LGM Hot Chilli Oil 275g
        Reason   "The recall is due to the presence of an undeclared
                  allergen (peanuts)."
        Pathogen "Listeria monocytogenes"   Tier 1     <- fabricated
    Recalls row 55  2026-07-24  Viet Meatballs Chinese Sausage 500g
        Reason   "The presence of an undeclared allergen (gluten)."
        Pathogen "Listeria monocytogenes"   Tier 1     <- fabricated

The guard had no test coverage at all, which is why the hole was invisible.

Run:  python -m pytest tests/test_hazard_class_guard.py -v
"""
from __future__ import annotations
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pipeline.claude_check import (  # noqa: E402
    _classify_hazard,
    _pathogen_reason_class_mismatch,
    _is_clean_row,
)


# The two production rows, verbatim.
ROW_3_REASON = "The recall is due to the presence of an undeclared allergen (peanuts)."
ROW_55_REASON = "The presence of an undeclared allergen (gluten)."


class TestAllergenIsClassifiable(unittest.TestCase):
    """The hole itself: allergen Reason text must not classify to set()."""

    def test_production_rows_classify_as_allergen(self):
        for reason in (ROW_3_REASON, ROW_55_REASON):
            self.assertIn("allergen", _classify_hazard(reason), reason)

    def test_allergen_phrasings_across_regulators(self):
        for reason in (
            "The presence of an undeclared allergen (wheat)",
            "The presence of an undeclared allergen (Egg).",
            "Undeclared allergen (peanut)",
            "Undeclared ingredients — sesame",
            "Recalled due to misbranding and undeclared allergens",
            "Product mislabelled; incorrect label applied",
            "allergene non declare (lait)",
            "Nicht deklariertes Allergen (Milch)",
            "alergeno no declarado (soja)",
        ):
            self.assertTrue(_classify_hazard(reason), reason)

    def test_mould_is_classifiable(self):
        self.assertIn("spoilage", _classify_hazard("Microbial (Mould) contamination."))
        self.assertIn(
            "spoilage",
            _classify_hazard(
                "Unsuccessful pasteurisation resulting in microbial "
                "(Mould) contamination."))


class TestFabricatedPathogenIsCaught(unittest.TestCase):
    """A biological Pathogen on an allergen Reason must be flagged."""

    def test_row_3_flagged(self):
        self.assertTrue(
            _pathogen_reason_class_mismatch("listeria monocytogenes", ROW_3_REASON))

    def test_row_55_flagged(self):
        self.assertTrue(
            _pathogen_reason_class_mismatch("listeria monocytogenes", ROW_55_REASON))

    def test_row_3_no_longer_takes_the_clean_row_shortcut(self):
        """End-to-end: the exact production row must fail _is_clean_row."""
        row = {
            "Date": "2026-07-29",
            "Source": "FSANZ (AU)",
            "Company": "Auxico (Perth) Pty Ltd",
            "Brand": "LGM",
            "Product": "LGM HOT CHILLI OIL 275G",
            "Pathogen": "Listeria monocytogenes",
            "Reason": ROW_3_REASON,
            "URL": "https://www.foodstandards.gov.au/food-recalls/recall-alert/"
                   "auxico-perth-pty-ltd-lgm-hot-chilli-oil-275g",
            "Notes": "[gemini-enrich 2026-07-29: Brand '—'→'LGM']",
        }
        self.assertFalse(
            _is_clean_row(row),
            "row 3 still shortcuts past verification — the guard is still blind")

    def test_mould_reason_with_invented_pathogen_flagged(self):
        self.assertTrue(_pathogen_reason_class_mismatch(
            "listeria monocytogenes", "Microbial (Mould) contamination."))

    def test_biological_reason_with_biotoxin_pathogen_flagged(self):
        """The other production shape: Reason names Listeria, Pathogen says
        histamine (Recalls row 1006 / 1020)."""
        self.assertTrue(_pathogen_reason_class_mismatch(
            "histamine / scombrotoxin", "Presence of Listeria"))


class TestNoFalsePositives(unittest.TestCase):
    """The guard must not fire on rows that agree.

    Verified against the full production workbook: adding the allergen and
    spoilage classes newly flagged exactly rows 3 and 55 out of 1280, and
    un-flagged nothing. These cases pin the ways that could regress.
    """

    def test_agreeing_allergen_row_not_flagged(self):
        self.assertFalse(_pathogen_reason_class_mismatch(
            "undeclared allergen (wheat)",
            "The presence of an undeclared allergen (wheat)"))

    def test_agreeing_mould_row_not_flagged(self):
        self.assertFalse(_pathogen_reason_class_mismatch(
            "mould", "Microbial (Mould) contamination."))

    def test_rasff_milk_category_is_not_an_allergen_claim(self):
        """THE trap this class was written around. RASFF Reason text carries
        'category: milk and milk products' on genuine Listeria and STEC
        notifications. A bare food-name token would classify those as
        allergen and manufacture a mismatch on correct rows."""
        for reason in (
            "Presence of L. monocytogenes in cheeses from Poland; risk: "
            "serious; category: milk and milk products",
            "STEC in raw milk cheese from France; risk: serious; category: "
            "milk and milk products",
            "STEC in Reblochon cheese from France; risk: serious; category: "
            "milk and milk products",
        ):
            self.assertNotIn("allergen", _classify_hazard(reason), reason)
            self.assertFalse(
                _pathogen_reason_class_mismatch("listeria monocytogenes", reason),
                reason)
            self.assertFalse(
                _pathogen_reason_class_mismatch(
                    "shiga toxin-producing e. coli (stec)", reason), reason)

    def test_nut_category_is_not_an_allergen_claim(self):
        reason = ("aflatoxins in hazelnut from Georgia; risk: serious; "
                  "category: nuts, nut products and seeds")
        self.assertNotIn("allergen", _classify_hazard(reason))

    def test_vague_reason_stays_unclassifiable(self):
        """The guard fails SAFE on vague text — it must not start guessing."""
        for reason in ("Pathogen contamination", "Contamination microbiologique",
                       "Rappel pour raison sanitaire", "Microbiological risk",
                       "Non conformite microbiologique"):
            self.assertFalse(_pathogen_reason_class_mismatch(
                "listeria monocytogenes", reason), reason)

    def test_allergen_alongside_a_pathogen_does_not_flag(self):
        """Multi-class Reason: intersection, not equality."""
        self.assertFalse(_pathogen_reason_class_mismatch(
            "listeria monocytogenes",
            "Listeria monocytogenes contamination; product also mislabelled"))


class TestWholeWorkbookRegression(unittest.TestCase):
    """Guard-rail on the real data.

    The two fabricated FSANZ rows were corrected on 2026-07-30 (Pathogen
    relabelled to the allergen, Tier 1 -> 2), so the assertion here is that
    the workbook stays clean of them — not that the defect is still present.

    A separate set of rows is contradictory for a DIFFERENT reason and is
    still unresolved: their Reason text was overwritten with a neighbouring
    row's, proven by the fact that each shares its exact Reason string with a
    row from another source whose Pathogen matches that Reason correctly
    (e.g. the RappelConso row carrying "Aflatoxins in dried figs from
    Turkiye; risk: serious; ..." — verbatim RASFF notification text that
    RappelConso never emits). Adjudicating those needs the source fiche, and
    rappel.conso.gouv.fr serves an incomplete TLS chain, so they are pinned
    here by URL rather than silently tolerated. The list must SHRINK, never
    grow.

    Skips cleanly when the workbook is absent (bare source tree / CI without
    the data files).
    """

    # Corrected 2026-07-30 — must never be flagged again.
    FIXED_URLS = (
        "https://www.foodstandards.gov.au/food-recalls/recall-alert/"
        "auxico-perth-pty-ltd-lgm-hot-chilli-oil-275g",
        "https://www.foodstandards.gov.au/food-recalls/recall-alert/"
        "viet-meatballs-chinese-sausage-500g",
    )

    # Reason-field cross-contamination, awaiting source adjudication.
    KNOWN_UNRESOLVED_FICHES = frozenset({
        "22205", "22206", "22208", "22186", "22157",
        "22113", "22067", "22082", "21987", "21975",
    })

    def _load(self):
        try:
            import openpyxl
        except ImportError:                       # pragma: no cover
            self.skipTest("openpyxl not installed")
        xlsx = ROOT / "docs" / "data" / "recalls.xlsx"
        if not xlsx.exists():                     # pragma: no cover
            self.skipTest("recalls.xlsx not present")
        wb = openpyxl.load_workbook(xlsx, read_only=True)
        rows = list(wb["Recalls"].values)
        hdr = [str(h) for h in rows[0]]
        idx = {h: i for i, h in enumerate(hdr)}
        out = []
        for r in rows[1:]:
            if not r or not r[idx["URL"]]:
                continue
            out.append({
                "URL": str(r[idx["URL"]]),
                "Pathogen": str(r[idx["Pathogen"]] or ""),
                "Reason": str(r[idx["Reason"]] or ""),
                "Tier": r[idx["Tier"]],
            })
        return out

    def test_corrected_rows_stay_corrected(self):
        rows = {r["URL"]: r for r in self._load()}
        for url in self.FIXED_URLS:
            self.assertIn(url, rows, url)
            row = rows[url]
            self.assertFalse(
                _pathogen_reason_class_mismatch(row["Pathogen"].lower(),
                                               row["Reason"]),
                f"{url} has regressed to a contradictory Pathogen: "
                f"{row['Pathogen']!r} vs {row['Reason']!r}")
            self.assertIn("allergen", row["Pathogen"].lower(), url)
            self.assertEqual(2, row["Tier"], url)

    def test_no_new_contradictions_appear(self):
        """Every flagged row must be on the documented unresolved list."""
        unexpected = []
        for row in self._load():
            if not _pathogen_reason_class_mismatch(row["Pathogen"].lower(),
                                                   row["Reason"]):
                continue
            fiche = row["URL"].rstrip("/").split("/")
            ident = next((p for p in fiche if p.isdigit()), row["URL"])
            if ident not in self.KNOWN_UNRESOLVED_FICHES:
                unexpected.append((ident, row["Pathogen"], row["Reason"][:70]))
        self.assertEqual(
            [], unexpected,
            "new Pathogen-vs-Reason contradictions reached the Recalls "
            f"sheet: {unexpected}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
