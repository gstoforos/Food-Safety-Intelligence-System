"""Pet food in the words a label actually uses (audit 2026-10-05).

RappelConso fiche 23687 — Mama Kana "huile 10% de cbd pour chat", Reason
"... non autorisé pour l'alimentation animale" — passed the Pending gate,
because the French branch of the pet-food pattern needed "aliment pour" or
"nourriture pour", and was archived and re-ingested six times.
"""
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline._pathogen_scope import is_pet_food_product  # noqa: E402


@pytest.mark.parametrize("text", [
    "huile 10% de cbd pour chat et huile 5% de cbd pour chat",
    "huile 10% de cbd pour chien",
    "statut d'additifs non autorisé pour l'alimentation animale",
    "croquettes pour chiens", "friandises pour chats", "pâtée pour chaton",
    "aliments pour animaux de compagnie",
    "snack para perros", "biscotti per cani", "Leckerli für Hunde",
    "brokjes voor katten", "diervoeding", "Futtermittel",
])
def test_pet_food_is_recognised(text):
    assert is_pet_food_product(text)


@pytest.mark.parametrize("text", [
    "Chicken thighs", "Catfish fillets", "pour la table", "Pour chaque chat",
    "Sweet salamella (fresh pork sausage)", "pâté de campagne",
])
def test_human_food_is_left_alone(text):
    assert not is_pet_food_product(text)


def test_the_pending_gate_refuses_the_mama_kana_row_for_good():
    from pipeline.merge_master import _is_terminal_rejection, validate_pending_row
    row = {"Date": "2026-10-02", "Source": "RappelConso (FR)", "Company": "Mama Kana",
           "Product": "huile 10% de cbd pour chat et huile 5% de cbd pour chat",
           "Pathogen": "", "Reason": "statut d'additifs non autorisé pour l'alimentation animale",
           "Country": "France", "URL": "https://rappel.conso.gouv.fr/fiche-rappel/23687/interne"}
    ok, why = validate_pending_row(row, set())
    assert not ok and why.startswith("pet_food")
    assert _is_terminal_rejection(why)
