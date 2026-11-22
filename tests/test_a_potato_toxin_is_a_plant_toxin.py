# -*- coding: utf-8 -*-
"""Potato glycoalkaloids are a plant toxin and in scope (2026-11-22).

RappelConso fiche 24142 (Côté marché red potatoes, lot 26128774, "Présence
excessive de glycoalcaloïdes") was refused at the Pending gate as
pathogen_out_of_scope, the fourth plant toxin after pyrrolizidine, tropane
and opium alkaloids that the biotoxin class could not see.
"""
import pytest

from pipeline._pathogen_scope import is_in_afts_scope
from pipeline._publish_gate import classify_hazard


@pytest.mark.parametrize("text", [
    "Glycoalkaloids",
    "Présence excessive de glycoalcaloïdes (substances naturelles de la pomme de terre).",
    "erhöhte Gehalte an Glykoalkaloiden",
    "Solanine above the limit",
])
def test_glycoalkaloids_classify_as_biotoxin(text):
    assert "biotoxin" in classify_hazard(text)


def test_glycoalkaloids_pass_the_pending_scope_test():
    assert is_in_afts_scope("Glycoalkaloids",
                            "Excessive glycoalkaloids (natural substances of the potato)")


def test_an_allergen_still_does_not():
    assert not is_in_afts_scope("Undeclared allergen", "undeclared milk")
