"""A compounded glutathione injection is not a food recall.

WHY THIS EXISTS (2026-10-04)
============================
On 2026-10-04 the register published, and three reviewers approved:

    FDA · 2026-10-01 · Greenwich Rx · "Glutathione Injection" · endotoxin
    fda.gov/safety/recalls-market-withdrawals-safety-alerts/greenwich-rx-
    issues-voluntary-nationwide-recall-compounded-glutathione-due-elevated-
    endotoxin-levels

Greenwich Rx is a 503A compounding pharmacy in Tomball, Texas; the product
is a compounded sterile injectable. Nobody eats it.

It satisfied every rule the publish gate had:

  * rule 1 only refuses an EMPTY Pathogen, and "endotoxin" is not empty —
    which is why that rule stopped the car, the bath toy and the lamp oil
    but not this;
  * rule 8 refuses a row whose hazard CLASSES are a subset of
    {allergen, fermentation}, and "endotoxin" resolves to no class at all,
    so the subset test was vacuous;
  * the URL was a genuine per-notice permalink on the regulator's own host —
    but fda.gov/safety/recalls-market-withdrawals-safety-alerts is one
    bucket for food, drug, device and cosmetic notices alike, so neither
    the host nor the path says which.

Three tests broke on it the same morning, each naming a different symptom
of the one cause, and the curator's was the one that asked the right
question: "either the class map is missing vocabulary or these rows do not
belong in Recalls". The answer is the second branch. "endotoxin" is
deliberately NOT in tools/alert_vocab.py: it is a parenteral drug hazard,
and teaching the alert vocabulary that it is a food hazard would admit the
next four — four other Texas pharmacies issued the same recall.

WHAT THE RULE MAY NOT COST
Dietary supplements are IN scope — the 2026-05-12 undeclared-pharmaceutical
expansion exists for exactly those — and "infusion" in food is a tea. The
rule therefore tests the DOSAGE FORM and the compounding-pharmacy marker,
never the word "drug" and never the firm's name.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline._publish_gate import publish_blockers, _non_food_drug_blockers

ROOT = Path(__file__).resolve().parents[1]

GREENWICH = {
    "Date": "2026-10-01", "Source": "FDA", "Company": "Greenwich Rx",
    "Brand": "Greewich", "Product": "Glutathione Injection",
    "Pathogen": "endotoxin", "Reason": "Potential elevated endotoxin levels",
    "Class": "Recall", "Country": "United States",
    "Region": "North America", "Tier": 3, "Outbreak": 0,
    "URL": ("https://www.fda.gov/safety/recalls-market-withdrawals-safety-"
            "alerts/greenwich-rx-issues-voluntary-nationwide-recall-"
            "compounded-glutathione-due-elevated-endotoxin-levels"),
}


def test_the_row_that_found_this_is_refused():
    assert publish_blockers(GREENWICH), (
        "the Greenwich Rx compounded glutathione injection passed every gate "
        "rule on 2026-10-04 and was published")


def test_the_refusal_names_the_dosage_form_and_the_compounding():
    blockers = " ".join(_non_food_drug_blockers(GREENWICH)).lower()
    assert "injection" in blockers
    assert "compounded" in blockers


@pytest.mark.parametrize("product", [
    "Glutathione Injection",
    "Compounded Semaglutide injectable, 2.5 mg/mL vial",
    "Methylcobalamin 1000 mcg/mL, 30 mL vials",
    "Ketorolac prefilled syringe",
    "Latanoprost ophthalmic solution",
])
def test_a_dosage_form_is_not_a_food(product):
    row = dict(GREENWICH, Product=product)
    assert _non_food_drug_blockers(row), product


@pytest.mark.parametrize("product,pathogen,reason", [
    # Dietary supplements are IN scope — the undeclared-pharmaceutical
    # expansion of 2026-05-12 is about exactly these.
    ("Weight-loss capsules, 60 ct bottle", "Undeclared drug (sibutramine)",
     "Undeclared pharmaceutical ingredient sibutramine"),
    # "infusion" in food is a tea.
    ("Chamomile herbal infusion tea bags, 20 ct", "Salmonella",
     "Presence of Salmonella"),
    ("Cold-brew coffee infusion, 1 L carton", "Listeria monocytogenes",
     "Presence of Listeria monocytogenes"),
    # Foods whose names contain a dosage-form word as part of another word.
    ("Vialone Nano risotto rice, 1 kg", "Aflatoxin",
     "Aflatoxin above the maximum level"),
    # A real food recall from a firm with a pharmaceutical-sounding name.
    ("Protein bars, 12 x 60 g", "Salmonella", "Presence of Salmonella"),
])
def test_real_food_rows_are_not_touched(product, pathogen, reason):
    row = dict(GREENWICH, Product=product, Pathogen=pathogen, Reason=reason,
               Company="Acme Foods LLC", Brand="Acme",
               URL="https://www.fda.gov/safety/recalls-market-withdrawals-"
                   "safety-alerts/acme-foods-llc-recalls-a-product")
    assert not _non_food_drug_blockers(row), (product, pathogen)


def test_endotoxin_is_not_taught_to_the_alert_vocabulary():
    """Adding it would make the next compounded injection publishable."""
    import tools.alert_vocab as av
    blob = json.dumps(
        {k: sorted(v) if isinstance(v, (list, set, tuple)) else v
         for k, v in vars(av).items()
         if isinstance(v, (list, set, tuple, dict, str))},
        default=str).lower()
    assert "endotoxin" not in blob, (
        "endotoxin is a parenteral drug hazard, not a food hazard — the "
        "Greenwich Rx row is removed from Recalls, not admitted by widening "
        "the alert vocabulary")


def test_no_published_row_is_refused_by_this_rule():
    """The rule must cost the register nothing that belongs in it."""
    rows = json.loads((ROOT / "docs" / "data" / "recalls.json")
                      .read_text(encoding="utf-8"))
    refused = [(r.get("Date"), r.get("Company"), r.get("Product"))
               for r in rows if _non_food_drug_blockers(r)]
    assert not refused, refused
