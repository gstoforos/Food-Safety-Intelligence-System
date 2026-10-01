"""Bisphenol A and delta-9-THC over a legal limit are chemical hazards
(2026-10-01). RappelConso 23655 / 23592 / 23594 classified as nothing at
every gate, so in-scope recalls could never publish."""
import pytest

from pipeline._pathogen_scope import is_in_afts_scope
from pipeline._publish_gate import classify_hazard
from pipeline.gap_finder.rules import classify

CASES = [
    "Boites contenant du bisphénol A en quantité supérieure à la réglementation",
    "Bisphenol A above the legal limit",
    "Teneur en delta-9-THC susceptible d'exposer le consommateur à une dose supérieure à la dose aiguë de référence",
]


@pytest.mark.parametrize("t", CASES)
def test_every_gate_sees_a_chemical(t):
    assert "chemical" in classify_hazard(t)
    assert is_in_afts_scope("Chemical contaminant", t) or is_in_afts_scope(t)
    c = classify(reason=t)
    assert (c.verdict, c.category) == ("accept", "synthetic_chemical"), c


def test_a_thc_free_hemp_label_is_not_a_hazard():
    assert "chemical" not in classify_hazard("Huile de chanvre bio, anomalie d'étiquetage")
