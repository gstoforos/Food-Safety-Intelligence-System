"""The publish gate classifies the hazard wording regulators actually use.

2026-09-30, from the September coverage audit. Each of these was a real,
in-scope recall whose hazard classified as NOTHING at the publish gate — so
the offline promoter could never publish it, and nothing said why:

  CFIA   no name chopped walnuts       "Food - Extraneous Material", shell fragments
  CFIA   So Delicious frozen dessert   "plastic-like and gravel-like fragments"
  FDA    Niwali tejocote               "yellow oleander (Thevetia peruviana)"
  FDA    Lipofit fat burner            "undeclared fluoxetine and 2,4-dinitrophenol"

"Extraneous Material" is CFIA's category name for EVERY foreign-body recall.
"""
from __future__ import annotations

import pytest

from pipeline._publish_gate import classify_hazard

CASES = [
    ("Food - Extraneous Material: shell fragments", "physical"),
    ("recalled due to shell fragments", "physical"),
    ("plastic-like and gravel-like fragments", "physical"),
    ("Products contain yellow oleander (Thevetia peruviana)", "biotoxin"),
    ("cardiac glycosides", "biotoxin"),
    ("undeclared fluoxetine and 2,4-dinitrophenol (DNP)", "chemical"),
    ("contains furosemide", "chemical"),
]


@pytest.mark.parametrize("text,cls", CASES)
def test_classifies(text, cls):
    assert cls in classify_hazard(text), text


@pytest.mark.parametrize("text", [
    "chopped walnuts", "walnut halves in shell", "plastic-like packaging film",
    "gravel road farm eggs",
])
def test_does_not_overreach(text):
    assert not ({"physical"} & classify_hazard(text)), text
