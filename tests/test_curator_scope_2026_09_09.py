"""The curator's scope test must be the AFTS scope, not the Tier-1 list.

Reported by the nightly operator review of 2026-09-09: two genuine
RappelConso foreign-body recalls could not be corrected because
`curator.check_scope` called `pipeline._pathogen_scope.is_in_scope`, a list
of TIER-1 PATHOGEN names that has never held a foreign-material, heavy-metal,
pest, biotoxin or pesticide term. It is not the monitored scope — it is the
list that decides Tier-1 severity (`is_tier1` is an alias for it).

The monitored scope is the line printed on every daily brief and every weekly
report, and enforced by `pipeline/_publish_gate.py`:

    Pathogens + biotoxins + mycotoxins + foreign material + pest + chemical
    hazards only. Allergen-only, labeling, quality issues excluded.

Measured on the register of 2026-09-09 the old check refused the hazard of
89 of 1,656 PUBLISHED rows. After the fix, 7 — and each of those 7 is a real
policy question, listed at the bottom of this file, not a vocabulary gap.
"""
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _scope_refusals(row):
    from pipeline.agents.curator import check_scope
    return [p for p in check_scope(row) if "outside the monitored scope" in p]


class TestTheScopeStatementIsTheScope(unittest.TestCase):
    """Every hazard family the published scope statement names is in scope."""

    CASES = {
        "Foreign material (glass)": "physical",
        "Foreign material (hard black fragments)": "physical",
        "Physical/foreign-body contamination": "physical",
        "Cadmium (heavy metal)": "chemical",
        "Lead (heavy metal)": "chemical",
        "Rodenticide (rat poison)": "chemical",
        "Pesticide residues": "chemical",
        "Veterinary medicine residues (penicillin)": "chemical",
        "PFOA / PFAS": "chemical",
        "Hydrocyanic acid (acido cianidrico) — chemical hazard": "chemical",
        "Undeclared pharmacological ingredient (yohimbine)": "chemical",
        "Marine biotoxin": "biotoxin",
        "Paralytic shellfish toxins (PSP)": "biotoxin",
        "Lipophilic biotoxins (DSP)": "biotoxin",
        "Phytoplankton biotoxins": "biotoxin",
        "Mushroom toxins (Amanita-class)": "biotoxin",
        "Amanita muscaria toxin (muscimol)": "biotoxin",
        "T-2 / HT-2 toxin": "mycotoxin",
        "Alternaria toxins": "mycotoxin",
        "Cyclospora": "biological",
        "Rodent contamination (physical/microbial hazard)": "pest",
        "Mouse contamination (physical/biological hazard)": "pest",
    }

    def test_each_hazard_gets_the_right_class(self):
        from pipeline._publish_gate import classify_hazard
        for pathogen, cls in self.CASES.items():
            self.assertIn(cls, classify_hazard(pathogen), pathogen)

    def test_the_curator_admits_each_of_them(self):
        for pathogen in self.CASES:
            row = {"Pathogen": pathogen, "Reason": "", "Product": "",
                   "Company": "", "URL": ""}
            self.assertEqual([], _scope_refusals(row), pathogen)


class TestTheBoundaryStillHolds(unittest.TestCase):
    """Widening the classes must not admit what the policy excludes."""

    def test_allergen_only_is_still_out(self):
        row = {"Pathogen": "Undeclared allergen (peanut)",
               "Reason": "Undeclared allergen: peanut not on the label",
               "Product": "", "Company": "", "URL": ""}
        self.assertTrue(_scope_refusals(row))

    def test_quality_and_fermentation_are_still_out(self):
        for pathogen, reason in (("Spoilage", "possible spoilage"),
                                 ("Fermentation", "unintended fermentation"),
                                 ("Quality defect", "quality defect")):
            row = {"Pathogen": pathogen, "Reason": reason, "Product": "",
                   "Company": "", "URL": ""}
            self.assertTrue(_scope_refusals(row), pathogen)

    def test_an_empty_pathogen_is_still_reported(self):
        from pipeline.agents.curator import check_scope
        row = {"Pathogen": "", "Reason": "", "Product": "", "Company": "",
               "URL": ""}
        self.assertTrue(any("Pathogen empty" in p for p in check_scope(row)))

    def test_the_false_friends_gain_no_class(self):
        """Qualified forms only — see the comments on each class."""
        from pipeline._publish_gate import classify_hazard
        self.assertNotIn("pest", classify_hazard("Pesticide residues "
                                                 "(insecticide)"))
        self.assertNotIn("physical", classify_hazard("Slivered almonds"))
        self.assertNotIn("pest", classify_hazard("concentrate ratio 3:1"))
        self.assertEqual(set(), classify_hazard(
            "category: milk and milk products"))


class TestTheRegister(unittest.TestCase):
    """What the fix is worth, measured on the published register."""

    # The hazards still refused on 2026-09-09, and why each is a POLICY
    # question rather than a missing keyword. Listed literally so that a new
    # one shows up as a failure and gets decided, instead of quietly joining
    # the pile.
    #
    #   "None (organoleptic spoilage)"     the MILBONA row, admitted by a
    #                                      documented operator exception
    #   "Coliform / total bacterial count" hygiene indicator, not an organism
    #   "Inadequate sterilization ..."     process deviation, hazard implied
    #   "Possible incomplete pasteuriz..." process deviation, hazard implied
    #
    # CLOSED by the operator review of 2026-09-11 — the two "Unspecified
    # hazard" rows were never policy questions at all, only unread fiches.
    # Both were chased to the regulator page and both name their hazard:
    #
    #   RappelConso 21810 (mique, ferme CAZABONNE)  "Motif du rappel"
    #       reads "listeria" -> Pathogen "Listeria", Tier 3 -> 1
    #   RappelConso 21812 (ZINC LIPOSOMAL, Belle & Bio)  "Motif du rappel"
    #       reads "Détection d'une quantité anormale de plomb dans une
    #       matière première contenue dans ce produit"
    #       -> Pathogen "Lead (heavy metal)", a chemical hazard in scope
    #
    # Their entries are REMOVED from this set rather than left behind, so
    # that a regression which puts either hazard back fails here instead of
    # being silently tolerated. That is what this set is for.
    KNOWN_OPEN = {
        "None (organoleptic spoilage)",
        "Coliform / total bacterial count",
        "Inadequate sterilization (microbiological hazard)",
        "Possible incomplete pasteurization (process deviation)",
    }

    @classmethod
    def setUpClass(cls):
        cls.rows = json.loads(
            (ROOT / "docs" / "data" / "recalls.json").read_text("utf-8"))

    def test_no_published_row_is_refused_except_the_known_policy_cases(self):
        bad = sorted({str(r.get("Pathogen") or "") for r in self.rows
                      if _scope_refusals(r)} - self.KNOWN_OPEN)
        self.assertEqual([], bad,
                         "the curator refuses the hazard of published "
                         f"row(s): {bad} — either the class map is missing "
                         "vocabulary or these rows do not belong in Recalls")
