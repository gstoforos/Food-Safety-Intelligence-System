"""Two axes that inverted themselves, and the hazard group that had no bucket.

WHY THIS EXISTS (2026-09-25)
===========================
Adding FSIS recall 018-2026 to the register — Shanghai Ravioli Corporation,
24,900 lb of frozen product whose FSIS title begins "Recalls NOT-READY-TO-EAT
Frozen Buffalo Chicken Products" — produced two wrong analytical values, and
each had a different cause.

1. ConsumptionState came back "ready-to-eat", the exact opposite of the
   notice. CONSUMPTION_TERMS["cook-before-eating"] carried the single term
   "not ready-to-eat". _find matches substrings and CONSUMPTION_ORDER checks
   cook-before-eating first, so that spelling was guarded — but
   "not-ready-to-eat" (all hyphens) and "not ready to eat" (all spaces) both
   missed the guard and then matched the POSITIVE "ready-to-eat" term as a
   substring of themselves. FSIS writes "NRTE" constantly, so this was latent
   rather than rare: exactly ONE published row of 1,784 contained a negated
   RTE phrase when it was found, which is why no existing row is
   reclassified by the fix.

   This field says whether the consumer is expected to cook the product.
   Inverting it is not cosmetic.

2. HazardGroup would have come back "pathogen-bacterial" for a row with no
   organism named anywhere and no test performed, because nothing in
   _HAZARD_GROUP_RULES matches the uninspected-product canonical and the
   catch-all is bacterial. Same defect as the SUPPLX yohimbine row of
   2026-09-06 (a chemical adulterant filed as bacterial), reached by a
   different route. "hazard-not-assessed" is its own group, kept separate
   from _NOT_A_HAZARD: "None (organoleptic spoilage)" names no hazard, while
   uninspected product names a condition under which any hazard would have
   gone undetected. Not the same finding.

NOT fixed here, deliberately: CATEGORY_ORDER places dairy-soft-cheese ahead
of meat-poultry and food_category returns on first match, so "MOZZARELLA" in
one item name filed this chicken recall as soft cheese. 358 of 1,784
published rows match more than one category and 27 have a dairy term winning
over a meat or fish term, several of them genuinely arguable (a cheese
sausage, a ham-and-emmental tart). Reordering is a register-wide
reclassification and a judgement call, so it is written up rather than taken.
"""
from __future__ import annotations

import pytest

import pipeline.product_axes as PA
from pipeline.enrich_schema import _hazard_group


class TestNegatedReadyToEat:

    @pytest.mark.parametrize("text", [
        "Frozen, not-ready-to-eat buffalo chicken products",
        "not ready-to-eat frozen buffalo chicken",
        "not ready to eat (NRTE) bacon",
        "NRTE smoked bacon",
        "NRTE Buffalo Chicken Rangoon",
        "Recalls Not-Ready-To-Eat Frozen Buffalo Chicken Products",
    ])
    def test_every_spelling_of_the_negation_is_caught(self, text):
        state, _conf, _hit = PA.consumption_state({"Product": text})
        assert state == "cook-before-eating", (
            f"{text!r} was classified {state!r} — a product FSIS says must be "
            f"cooked was recorded as ready to eat")

    @pytest.mark.parametrize("text", [
        "ready-to-eat pickled goat and chicken products",
        "ready to eat dry-cured pork jowl",
        "RTE deli meat",
    ])
    def test_the_positive_still_works(self, text):
        """The guard must not swallow genuine ready-to-eat product. FSIS
        017-2026 and 019-020-2026 are both RTE and both in the register."""
        state, _conf, _hit = PA.consumption_state({"Product": text})
        assert state == "ready-to-eat"

    def test_the_guard_precedes_the_positive_in_the_order(self):
        """Structural, not behavioural: the fix relies on cook-before-eating
        being checked first. If someone reorders these, the substring
        overlap comes straight back."""
        order = PA.CONSUMPTION_ORDER
        assert order.index("cook-before-eating") < order.index("ready-to-eat")

    def test_all_three_spellings_are_present_in_the_vocabulary(self):
        terms = set(PA.CONSUMPTION_TERMS["cook-before-eating"])
        for spelling in ("not ready-to-eat", "not-ready-to-eat",
                         "not ready to eat", "nrte"):
            assert spelling in terms, spelling


class TestHazardNotAssessedIsItsOwnGroup:

    def test_uninspected_product_is_not_filed_as_bacterial(self):
        assert _hazard_group(
            "Uninspected product (hazard not assessed)") == "hazard-not-assessed"

    @pytest.mark.parametrize("raw", [
        "uninspected meat", "produced without the benefit of inspection",
        "false inspection mark",
    ])
    def test_the_wording_variants_land_there_too(self, raw):
        assert _hazard_group(raw) == "hazard-not-assessed"

    @pytest.mark.parametrize("raw,group", [
        ("Listeria monocytogenes", "pathogen-bacterial"),
        ("Shiga toxin-producing E. coli (STEC)", "pathogen-bacterial"),
        ("Aflatoxin", "mycotoxin"),
        ("metal fragment", "foreign-material"),
        ("Undeclared pharmacological agent", "chemical"),
        ("Norovirus", "pathogen-viral"),
        ("heavy metal", "heavy-metal"),
        ("", "unknown"),
    ])
    def test_nothing_else_moved(self, raw, group):
        assert _hazard_group(raw) == group

    def test_a_label_naming_no_hazard_stays_unknown(self):
        """_NOT_A_HAZARD and _NO_HAZARD_ASSESSED must not collapse into each
        other. "None (organoleptic spoilage)" names no hazard; uninspected
        product names one that was never measured."""
        assert _hazard_group("None (organoleptic spoilage)") == "unknown"
        assert _hazard_group("unspecified hazard") == "unknown"


class TestTheRegisterRows:
    """The four rows this all exists for, as they now sit in the register."""

    @staticmethod
    def _uninspected_rows():
        import openpyxl
        from pathlib import Path
        p = (Path(__file__).resolve().parent.parent
             / "docs" / "data" / "recalls.xlsx")
        wb = openpyxl.load_workbook(p, read_only=True, data_only=True)
        ws = wb["Recalls"]
        rows = list(ws.iter_rows(values_only=True))
        hdr = [str(c or "") for c in rows[0]]
        return [dict(zip(hdr, r)) for r in rows[1:]
                if str(dict(zip(hdr, r)).get("Pathogen") or "")
                .startswith("Uninspected product")]

    def test_all_four_are_present(self):
        rows = self._uninspected_rows()
        assert len(rows) == 4, [r.get("Date") for r in rows]

    def test_each_is_on_the_authority_host_with_a_per_recall_path(self):
        for r in self._uninspected_rows():
            url = str(r.get("URL") or "")
            assert url.startswith("https://www.fsis.usda.gov/recalls-alerts/"), url
            assert len(url.rstrip("/").split("/")[-1]) > 12, (
                f"looks like a listing, not a notice: {url}")

    def test_none_of_them_invented_a_pathogen(self):
        for r in self._uninspected_rows():
            p = str(r.get("Pathogen") or "").lower()
            for organism in ("listeria", "salmonella", "coli", "hepatitis"):
                assert organism not in p, (
                    f"{r.get('Date')}: nobody tested this product — "
                    f"Pathogen reads {r.get('Pathogen')!r}")

    def test_the_nrte_row_is_cook_before_eating(self):
        rows = [r for r in self._uninspected_rows()
                if "shanghai-ravioli" in str(r.get("URL") or "")]
        assert len(rows) == 1
        assert rows[0]["ConsumptionState"] == "cook-before-eating"
        assert rows[0]["FoodCategory"] == "meat-poultry"

    def test_every_one_carries_its_provenance(self):
        """fsis.usda.gov answers 403 to the environment these were added
        from, so each row has to say where its facts came from."""
        for r in self._uninspected_rows():
            notes = str(r.get("Notes") or "")
            assert "SOURCES:" in notes, r.get("Date")
            assert "operator review 2026-09-25" in notes, r.get("Date")

    def test_the_import_eligibility_cases_were_not_swept_in(self):
        """Seven 2026 FSIS recalls are import-eligibility violations —
        arguably the same unassessed class, but a separate scope line that
        has not been ruled on. None of them may appear under this hazard."""
        for r in self._uninspected_rows():
            blob = (str(r.get("Company")) + str(r.get("Reason"))).lower()
            for firm in ("corte argentino", "shan distribution", "el eden",
                         "de todito", "sobico", "maple leaf", "bci foods",
                         "asian america", "mays chemical"):
                assert firm not in blob, firm


class TestThePublishGateLearnedTheClass:
    """Rule 8 of the publish gate refuses a row when allergen/labelling is
    the ONLY hazard class it resolves to. FSIS's hazard sentence on every
    uninspected notice reads "may contain undeclared allergens, harmful
    bacteria, or other contaminants", so quoting the regulator — which R5
    requires — made the Blackwing row look allergen-only and it was refused.
    tests/test_publish_gate.py and tests/test_auxico_regression.py both
    caught that, correctly. The gate had to learn the class; the alternative
    was editing the regulator's own sentence to suit the tool."""

    def test_the_canonical_pathogen_carries_a_class(self):
        from pipeline._publish_gate import classify_hazard
        assert "uninspected" in classify_hazard(
            "Uninspected product (hazard not assessed)")

    def test_the_fsis_hazard_sentence_is_not_allergen_only(self):
        from pipeline._publish_gate import classify_hazard
        classes = classify_hazard(
            "Produced without the benefit of federal inspection; may contain "
            "undeclared allergens, harmful bacteria, or other contaminants.")
        assert classes == {"allergen", "uninspected"}
        assert not classes <= {"allergen", "fermentation"}, (
            "this is the subset test rule 8 applies")

    def test_a_genuine_allergen_only_row_is_still_allergen_only(self):
        """The widening must not blunt the scope rule it had to get past."""
        from pipeline._publish_gate import classify_hazard
        for raw in ("The presence of an undeclared allergen (Peanut)",
                    "undeclared milk", "allergen labelling error"):
            assert classify_hazard(raw) <= {"allergen"}, raw

    @pytest.mark.parametrize("raw", [
        # The Prime Line Listeria row, published 2026-09-06. Its Reason says
        # "confirmed by FSIS routine import re-inspection sampling" — a bare
        # "inspection" in the term list would have given a pathogen recall an
        # uninspected class.
        "Possible Listeria monocytogenes contamination, confirmed by FSIS "
        "routine import re-inspection sampling at the port of entry",
        "products were produced under insanitary conditions found during "
        "inspection",
        "passed FSIS inspection",
    ])
    def test_the_word_inspection_alone_grants_nothing(self, raw):
        from pipeline._publish_gate import classify_hazard
        assert "uninspected" not in classify_hazard(raw)
