"""An organism the regulator calls non-pathogenic is not a Tier-1 event.

WHY (2026-08-18)
================
CFIA RA-82493 — Importation Mini Italia, various fresh stretched cheeses
and burrata — states its own hazard as, verbatim:

    "Food - Microbial contamination - E. Coli - non-pathogenic"

Class 2, no illnesses, no pathogenic strain identified. The always-Tier-1
list in _pathogen_scope contains bare "e. coli", because that string
normally means STEC. So enforce_tier1 read "E. coli", forced Tier 2 -> 1,
and stamped "[tier-guard: E. coli is always Tier 1; forced from Tier 2]"
into Notes — publishing a hygiene-indicator finding as a critical event,
with the guard's own note standing as the evidence that it had been
reviewed. Same shape as the 2026-08-04 Hepatitis A / aluminium-slivers
incident this file's sibling guard was written for.

The suppression is deliberately narrow, and the tests that matter here
are the ones proving it does NOT fire:

  * bare "E. coli" with no non-pathogenic wording  -> still Tier 1
  * O157 / STEC / VTEC named anywhere on the row   -> still Tier 1
  * Listeria or Salmonella                         -> untouched

Every pattern is word-boundary anchored. This module's recurring defect
has been substring matching — "cholera" inside "cholerae", "O1" inside
"non-O1", "salmon" inside "Salmonella" — so the traps are pinned below.
"""
from __future__ import annotations

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


class TestSuppression(unittest.TestCase):
    """Cases the rule must recognise as non-pathogenic. All land on Tier 2."""

    def test_the_cfia_row_verbatim(self):
        self.assertEqual(2, tier(
            Pathogen="E. coli",
            Reason="Food - Microbial contamination - E. Coli - non-pathogenic",
            Class="Class 2"))

    def test_generic_e_coli_wording(self):
        self.assertEqual(2, tier(
            Pathogen="E. coli",
            Reason="Various brands of cheese products recalled due to "
                   "generic E. coli"))

    def test_hyphen_and_space_variants(self):
        for wording in ("non-pathogenic", "non pathogenic", "nonpathogenic"):
            self.assertEqual(2, tier(Pathogen="E. coli",
                                     Reason=f"{wording} E. coli detected"),
                             wording)

    def test_french_and_german_wording(self):
        self.assertEqual(2, tier(Pathogen="E. coli",
                                 Reason="E. coli non pathogène"))
        self.assertEqual(2, tier(Pathogen="Escherichia coli",
                                 Reason="nicht pathogene E. coli"))

    def test_a_note_records_the_skip(self):
        row = {"Pathogen": "E. coli", "Tier": 2,
               "Reason": "generic E. coli"}
        enforce_tier1(row)
        self.assertIn("tier-guard 2026-08-18", row["Notes"])
        self.assertIn("indicator", row["Notes"].lower())

    def test_it_is_idempotent(self):
        row = {"Pathogen": "E. coli", "Tier": 2, "Reason": "generic E. coli"}
        enforce_tier1(row)
        first = row["Notes"]
        enforce_tier1(row)
        self.assertEqual(first, row["Notes"], "stamp duplicated on re-run")
        self.assertEqual(2, int(row["Tier"]))


class TestItIsNormalisedToTwo(unittest.TestCase):
    """Operator rule 2026-08-18: "E. Coli non pathogenic tier 2".

    The rule SETS the tier. An earlier version only declined to raise it
    and left whatever arrived, so the same finding could sit at 1, 2 or 3
    depending on which scraper admitted it — and a row that arrived at
    Tier 1 kept the exact overstatement the guard was written to remove.
    """

    NON_PATH = "Food - Microbial contamination - E. Coli - non-pathogenic"

    def test_every_incoming_tier_lands_on_two(self):
        for incoming in (1, 2, 3, 0, None, "", "3"):
            row = {"Pathogen": "E. coli", "Tier": incoming,
                   "Reason": self.NON_PATH}
            enforce_tier1(row)
            self.assertEqual(2, int(row["Tier"]),
                             f"incoming Tier={incoming!r}")

    def test_it_comes_down_from_one(self):
        row = {"Pathogen": "E. coli", "Tier": 1, "Reason": self.NON_PATH}
        enforce_tier1(row)
        self.assertEqual(2, int(row["Tier"]))
        self.assertIn("set from Tier 1", row["Notes"])

    def test_it_comes_up_from_three(self):
        row = {"Pathogen": "E. coli", "Tier": 3, "Reason": self.NON_PATH}
        enforce_tier1(row)
        self.assertEqual(2, int(row["Tier"]))

    def test_repeated_calls_do_not_drift_or_duplicate(self):
        row = {"Pathogen": "E. coli", "Tier": 3, "Reason": self.NON_PATH}
        for _ in range(4):
            enforce_tier1(row)
        self.assertEqual(2, int(row["Tier"]))
        self.assertEqual(1, row["Notes"].count("tier-guard 2026-08-18"))


class TestItStillEscalates(unittest.TestCase):
    """The traps. Every one of these MUST remain Tier 1."""

    def test_bare_e_coli_with_no_qualifier(self):
        self.assertEqual(1, tier(Pathogen="E. coli",
                                 Reason="E. coli detected in cheese"))

    def test_stec_is_pathogenic(self):
        self.assertEqual(1, tier(
            Pathogen="Shiga toxin-producing E. coli (STEC)", Tier=3,
            Reason="STEC in flour"))

    def test_o157_is_pathogenic(self):
        self.assertEqual(1, tier(Pathogen="E. coli O157:H7",
                                 Reason="E. coli O157:H7 detected"))

    def test_a_pathogenic_strain_anywhere_wins(self):
        """'non-pathogenic' in one field cannot cancel a named strain in
        another — the strain is the stronger evidence."""
        for field in ("Reason", "Product", "Notes"):
            row = {"Pathogen": "E. coli", "Tier": 2,
                   "Reason": "non-pathogenic E. coli", field: "VTEC confirmed"}
            enforce_tier1(row)
            self.assertEqual(1, int(row["Tier"]), field)

    def test_other_serogroups(self):
        for sg in ("O26", "O103", "O111", "O121", "O145"):
            self.assertEqual(1, tier(Pathogen="E. coli",
                                     Reason=f"generic E. coli, {sg} isolated"),
                             sg)

    def test_shiga_toxin_spelled_out(self):
        self.assertEqual(1, tier(
            Pathogen="E. coli",
            Reason="non-pathogenic screen, shiga toxin genes present"))

    def test_listeria_is_untouched_by_this_guard(self):
        self.assertEqual(1, tier(Pathogen="Listeria monocytogenes", Tier=3,
                                 Reason="a non-pathogenic organism was also "
                                        "found"))

    def test_salmonella_is_untouched_by_this_guard(self):
        self.assertEqual(1, tier(Pathogen="Salmonella", Tier=2,
                                 Reason="generic screening, non-pathogenic "
                                        "flora noted"))

    def test_the_word_pathogenic_alone_does_not_suppress(self):
        """'pathogenic' is a substring of nothing useful here, but the
        negation must be matched, not the root."""
        self.assertEqual(1, tier(Pathogen="E. coli",
                                 Reason="pathogenic E. coli detected"))


class TestDoNotWidenThisPattern(unittest.TestCase):
    """The label "Escherichia coli (generic)" is NOT the trigger.

    The register uses `Escherichia coli (generic)` in the Pathogen column
    as a catch-all meaning "the source named E. coli but no strain". That
    is a DIFFERENT claim from "the regulator said this organism is
    non-pathogenic", and 8 published rows carry the label.

    The obvious widening — also match "(generic)" after the organism
    name — is wrong, and one live row proves it:

        CFIA, 2026-05-06, Germina brand Brocoli Calabrese seeds
        Pathogen : "Escherichia coli (generic)"
        Reason   : "Possible contamination with PATHOGENIC E. coli"

    Widen the pattern and that row drops from Tier 1 to Tier 2 — a
    pathogenic-E.-coli seed recall downgraded because of a label the
    pipeline chose, not something the regulator said. Same shape as the
    bulk pattern fix that once corrupted 155 rows.

    The trigger stays: the regulator's own wording, in the row's own text.
    """

    def test_the_generic_label_alone_does_not_downgrade(self):
        row = {"Pathogen": "Escherichia coli (generic)", "Tier": 1,
               "Reason": "Possible contamination with pathogenic E. coli"}
        enforce_tier1(row)
        self.assertEqual(1, int(row["Tier"]),
                         "widening to the '(generic)' label downgraded a "
                         "pathogenic E. coli recall")

    def test_shellfish_indicator_rows_are_not_swept_in_by_the_label(self):
        """RASFF bivalve-mollusc rows carry the same label. They are not
        touched either — nothing in them says non-pathogenic."""
        row = {"Pathogen": "Escherichia coli (generic)", "Tier": 1,
               "Reason": "Presence of E. coli in clams from Italy; risk: "
                         "serious; category: bivalve molluscs"}
        enforce_tier1(row)
        self.assertEqual(1, int(row["Tier"]))

    def test_the_regulators_wording_still_fires(self):
        row = {"Pathogen": "Escherichia coli (generic)", "Tier": 1,
               "Reason": "recalled due to generic E. coli"}
        enforce_tier1(row)
        self.assertEqual(2, int(row["Tier"]))


class TestThePublishedRow(unittest.TestCase):

    def test_cfia_ra_82493_is_not_tier_1_in_the_register(self):
        import openpyxl
        xlsx = ROOT / "docs" / "data" / "recalls.xlsx"
        if not xlsx.exists():                              # pragma: no cover
            self.skipTest("recalls.xlsx not present")
        wb = openpyxl.load_workbook(xlsx, read_only=True, data_only=True)
        rows = list(wb["Recalls"].values)
        hdr = [str(h) for h in rows[0]]
        hits = [dict(zip(hdr, r)) for r in rows[1:] if r
                and "various-brands-cheese-products-recalled-due-generic-e-coli"
                in str(dict(zip(hdr, r)).get("URL") or "")]
        if not hits:                                       # pragma: no cover
            self.skipTest("row not in the register")
        self.assertEqual(2, int(hits[0]["Tier"]),
                         "the non-pathogenic E. coli row is published as a "
                         "Tier-1 critical event again")


if __name__ == "__main__":
    unittest.main()
