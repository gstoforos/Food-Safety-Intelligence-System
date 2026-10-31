"""A seized falsified antimalarial is not a food recall.

WHY THIS EXISTS (2026-10-31)
============================
Recalls held this row, promoted 2026-10-30 and published:

    Date      2026-10-24        (the NAFDAC alert is dated 2026-08-19)
    Source    NAFDAC (NG)
    Company   South Africa Foods Company
    Product   Artemether/Lumefantrine 80mg/480mg
    Pathogen  Substandard and Falsified
    Reason    Seizure of suspected substandard and falsified BPPL
              Artemether/Lumefantrine 80mg/480mg
    Tier      3
    URL       nafdac.gov.ng/public-alert-no-042-2026-alert-on-the-seizure-
              of-suspected-substandard-and-falsified-bppl-artemether-
              lumefantrine-80mg-480mg/

Artemether-lumefantrine is the first-line antimalarial. Read on the NAFDAC
alert this morning: a FALSIFIED medical product carrying a fake NAFDAC
registration number (02-3457) with no Marketing Authorisation Holder on the
label, purportedly manufactured by Bandaram Pharma Packleh Ltd (BPPL),
India, seized in Kano State during post-market surveillance, released
19 August 2026. NAFDAC's warning is about treatment failure and drug
resistance.

"South Africa Foods Company" appears nowhere on that page. The word "Foods"
in a mis-parsed company name was the only thing about this row that was
food, and the row's Date was the day the news mirror surfaced it, two
months after the alert.

THIS IS THE GREENWICH Rx DEFECT FROM THE OTHER SIDE
===================================================
tests/test_a_food_register_refuses_a_drug_recall.py (2026-10-04) taught the
gate to read the DOSAGE FORM: a thing that is injected or infused is not
eaten. That rule is sound and it cannot see this row, because an
antimalarial tablet has no dosage-form word the rule may safely match —
"tablets" and "capsules" are dietary-supplement pack forms and supplements
are IN scope.

So this row passed every rule the gate had:

  * rule 1 only refuses an EMPTY Pathogen, and "Substandard and Falsified"
    is not empty;
  * no dosage form, no compounding-pharmacy marker, no clinical-exposure
    marker;
  * the URL is a genuine per-notice permalink on the regulator's own host —
    but NAFDAC is the National Agency for Food AND DRUG Administration and
    Control, so its host says nothing about which.

Four tests broke on it the same morning, each naming a different symptom of
the one cause: the alert vocabulary could reach no term for "Substandard
and Falsified" (tests/test_alert_vocab.py), the curator refused the hazard
(tests/test_curator_scope_2026_09_09.py), and the archive held a rejected
copy of the same URL with no verdict
(tests/test_a_recall_is_not_both_published_and_rejected.py). The register's
answer is the same as it was for the compounded glutathione: the row is
removed, not admitted by widening the vocabulary.

WHAT THE RULE READS, AND WHAT IT MAY NOT COST
=============================================
"Substandard and falsified" is the WHO's term of art for medical products.
Every national medicines regulator uses it verbatim; it never describes a
food. Neither does a drug CLASS (antimalarial, antiretroviral). The rule
reads those, in the row's own Company, Brand, Product, Reason, Pathogen and
URL slug — no network, no model.

It may not cost the register a dietary supplement with an UNDECLARED drug
in it: that is in scope by the 2026-05-12 expansion and the register
publishes those as "Undeclared drug (<molecule>)". So the markers are the
falsification vocabulary and the drug class, never the word "drug" and
never a molecule name on its own. Measured across all 2261 published rows
the morning this was written, they matched exactly one — the row above.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline._publish_gate import (                            # noqa: E402
    publish_blockers, _non_food_drug_blockers)

NAFDAC = {
    "Date": "2026-10-24", "Source": "NAFDAC (NG)",
    "Company": "South Africa Foods Company", "Brand": "",
    "Product": "Artemether/Lumefantrine 80mg/480mg",
    "Pathogen": "Substandard and Falsified",
    "Reason": ("Seizure of suspected substandard and falsified BPPL "
               "Artemether/Lumefantrine 80mg/480mg"),
    "Class": "Alert", "Country": "Nigeria", "Region": "Africa",
    "Tier": 3, "Outbreak": 0,
    "URL": ("https://nafdac.gov.ng/public-alert-no-042-2026-alert-on-the-"
            "seizure-of-suspected-substandard-and-falsified-bppl-artemether-"
            "lumefantrine-80mg-480mg/"),
}


def test_the_row_that_found_this_is_refused():
    assert publish_blockers(NAFDAC), (
        "the NAFDAC falsified-antimalarial row passed every gate rule on "
        "2026-10-30 and was published to a food recall register")


def test_the_refusal_names_the_falsification_language():
    blockers = " ".join(_non_food_drug_blockers(NAFDAC)).lower()
    assert "substandard and falsified" in blockers, blockers


def test_the_dosage_form_rule_could_not_have_caught_it():
    """The point of the new marker: a swallowed drug has no dosage form.

    If a future edit makes a tablet a blocked dosage form, this test is the
    one that should go red — "tablets" and "capsules" are how dietary
    supplements are sold and supplements are in scope.
    """
    from pipeline._publish_gate import _NON_FOOD_DOSAGE_FORMS, _word_in
    product = NAFDAC["Product"].lower()
    assert not any(_word_in(product, t) for t in _NON_FOOD_DOSAGE_FORMS)
    assert "tablet" not in _NON_FOOD_DOSAGE_FORMS
    assert "tablets" not in _NON_FOOD_DOSAGE_FORMS
    assert "capsule" not in _NON_FOOD_DOSAGE_FORMS
    assert "capsules" not in _NON_FOOD_DOSAGE_FORMS


@pytest.mark.parametrize("field,value", [
    ("Reason", "Seizure of suspected substandard and falsified antimalarial"),
    ("Reason", "Alert on the circulation of falsified medicines in Kano"),
    ("Reason", "Counterfeit medicines seized at the border"),
    ("Product", "Artesunate antimalarial injection, 60 mg"),
    ("Product", "Antiretroviral tablets, 30 ct"),
    ("Pathogen", "Substandard and Falsified"),
])
def test_a_medicine_is_refused_wherever_the_row_says_so(field, value):
    row = dict(NAFDAC, Company="Acme Ltd", Brand="Acme",
               Product="Some product", Pathogen="Salmonella",
               Reason="Presence of Salmonella",
               URL="https://nafdac.gov.ng/public-alert-no-001-2026-alert/")
    row[field] = value
    assert _non_food_drug_blockers(row), (field, value)


@pytest.mark.parametrize("product,pathogen,reason", [
    # The 2026-05-12 undeclared-pharmaceutical expansion. IN SCOPE.
    ("Weight-loss capsules, 60 ct bottle", "Undeclared drug (sibutramine)",
     "Undeclared pharmaceutical ingredient sibutramine"),
    ("Herbal coffee sachets, 20 x 10 g", "Undeclared drug (sildenafil)",
     "Undeclared pharmacological ingredient sildenafil"),
    ("Standardized + Full Spectrum Blend Ashwagandha Capsules", "Salmonella",
     "Potential to be contaminated with Salmonella"),
    # A food whose own name contains a drug-sounding fragment.
    ("Malted barley drink powder, 400 g", "Salmonella",
     "Presence of Salmonella"),
    ("Tonic water, 6 x 200 ml", "Foreign material (glass)",
     "Possible presence of glass fragments"),
])
def test_real_food_rows_are_not_touched(product, pathogen, reason):
    row = dict(NAFDAC, Company="Acme Foods LLC", Brand="Acme",
               Product=product, Pathogen=pathogen, Reason=reason,
               URL="https://www.fda.gov/safety/recalls-market-withdrawals-"
                   "safety-alerts/acme-foods-llc-recalls-a-product")
    assert not _non_food_drug_blockers(row), (product, pathogen)


def test_substandard_and_falsified_is_not_taught_to_the_alert_vocabulary():
    """Adding it would make the next seized antimalarial publishable.

    Same decision as endotoxin on 2026-10-04: the hazard is real, it is not
    a FOOD hazard, and the register's repair is to remove the row.
    """
    import tools.alert_vocab as av
    blob = json.dumps(
        {k: sorted(v) if isinstance(v, (list, set, tuple)) else v
         for k, v in vars(av).items()
         if isinstance(v, (list, set, tuple, dict, str))},
        default=str).lower()
    for term in ("substandard and falsified", "falsified", "antimalarial"):
        assert term not in blob, (
            f"{term!r} is a medicines-regulation term, not a food hazard — "
            f"the NAFDAC row is removed from Recalls, not admitted by "
            f"widening the alert vocabulary")


def test_no_published_row_is_refused_by_this_rule():
    """The rule must cost the register nothing that belongs in it."""
    rows = json.loads((ROOT / "docs" / "data" / "recalls.json")
                      .read_text(encoding="utf-8"))
    refused = [(r.get("Date"), r.get("Source"), r.get("Product"))
               for r in rows if _non_food_drug_blockers(r)]
    assert not refused, refused
