"""A disease-surveillance bulletin is not a recall (2026-10-16).

AGES's weekly "Radar für Infektionskrankheiten" reached Pending twice (DE and
EN pages) with Company = the bulletin title and Product = its date, and
passed every publish-gate rule. The gate must refuse that shape, must not
refuse real products whose text merely contains a date, and the register
must hold no such row.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline._publish_gate import publish_blockers

ROOT = Path(__file__).resolve().parent.parent

BULLETINS = [
    {"Product": "15 October 2026 - AGES", "Company": "AGES Radar for Infectious Diseases",
     "URL": "https://www.ages.at/en/human/disease/ages-radar-for-infectious-diseases/details/ages-radar-for-infectious-diseases-15-october-2026"},
    {"Product": "15. Oktober 2026 - AGES", "Company": "AGES-Radar für Infektionskrankheiten",
     "URL": "https://www.ages.at/mensch/krankheit/ages-radar-fuer-infektionskrankheiten/detail/ages-radar-fuer-infektionskrankheiten-15-oktober-2026"},
]
PRODUCTS = [
    "Raw Mild Cheddar Cheese, best by 7/22/27",
    "Pâté 180 g, DLC 15 octobre 2026",
    "Gummies 20 mg — elderberry and Mai Tai",
    "Smoked salmon 200 g, use by 04/11/26",
]


@pytest.mark.parametrize("row", BULLETINS, ids=["en", "de"])
def test_the_gate_refuses_a_surveillance_bulletin(row):
    probs = " ".join(publish_blockers(dict(row, Pathogen="Salmonella",
                                           Reason="Salmonella — outbreak")))
    assert "bulletin" in probs, probs


@pytest.mark.parametrize("product", PRODUCTS)
def test_a_product_that_contains_a_date_is_not_a_bulletin(product):
    probs = publish_blockers({"Product": product, "Company": "Some Firm Ltd",
                              "Pathogen": "Listeria monocytogenes",
                              "Reason": "Listeria monocytogenes detected"})
    assert not any("bulletin" in p or "is a date" in p for p in probs), probs


def test_no_published_row_is_a_bulletin():
    rows = json.loads((ROOT / "docs" / "data" / "recalls.json").read_text(encoding="utf-8-sig"))
    bad = [r["URL"] for r in rows
           if any("bulletin" in p or "is a date" in p for p in publish_blockers(r))]
    assert not bad, bad
