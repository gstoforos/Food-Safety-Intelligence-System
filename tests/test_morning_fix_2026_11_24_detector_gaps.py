# -*- coding: utf-8 -*-
"""Three detector gaps found by the by-hand sweep of 2026-11-24.

The morning fix pass reads every Product published in the last 26 hours by
hand, because the gates' own stamp has missed one on most mornings since
2026-10-30. On 2026-11-24 it missed two, and the Reason detector missed a
third shape that has been published since April.

WHAT WAS PUBLISHED ON 2026-11-23, UNSTAMPED
===========================================
fiche 24150  E.Leclerc Atout Sud, Listeria monocytogenes, Tier 1
    Product 'jambon cuit supérieur découenné dégraissé au torchon; cornet
             jambon blanc, bruschetta jambon champignons, pizza reine,
             pizza fermière, complet jambon, sandwich jambon beurre,
             sandwich jambon fromage, croque monsieur, croissant au jambon;'

    Wholly French, and product_needs_english() returned False. Two of its
    words are in _FRENCH_FOOD_WORDS, so the food-word clause fired — and
    was vetoed, because that clause requires no word from
    _ENGLISH_PRODUCT_WORDS and "sandwich" was in that list. "sandwich" is
    an ordinary French word. This is the "pâté" defect of 2026-10-27 one
    list entry along: a word shared with French must not count as evidence
    of English.

fiche 24152  Top Budget, foreign material (glass)
    Product 'emmental râpé 200g'
    Reason  'Risque de corps étranger de type verre'

    The Product is three words, one a weight and one ("emmental") shared
    with English, so there is no French function word for the two-hit
    detector and no listed French food word either: "râpé" was missing from
    _FRENCH_FOOD_WORDS although "haché" has been there from the start.

    The Reason carries ONE accent and ONE French function word, so neither
    detect_language() nor the register test's own two-accent rule saw it.

AND TWENTY-FOUR OLDER ROWS
==========================
Measured on main at 8c964246: the one-accent rule finds 25 published
French Reasons, not one. The oldest is from 2026-04-21. All 25 are true
positives; there is no false positive in 2,491 rows.

These tests pin the three fixes against the exact strings that defeated
the detectors, and pin the false-positive modes each fix could introduce.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline._language import (  # noqa: E402
    product_needs_english,
    reason_needs_english,
)


# ── the Product detector ───────────────────────────────────────────────────

FICHE_24150_PRODUCT = (
    "jambon cuit supérieur découenné dégraissé au torchon; cornet jambon "
    "blanc, bruschetta jambon champignons, pizza reine, pizza fermière, "
    "complet jambon, sandwich jambon beurre, sandwich jambon fromage, "
    "croque monsieur, croissant au jambon;"
)


def test_a_french_sandwich_is_not_evidence_of_english():
    """fiche 24150. "sandwich" vetoed the whole detector."""
    assert product_needs_english(FICHE_24150_PRODUCT)


def test_a_french_sandwich_alone_is_caught():
    assert product_needs_english("sandwich jambon beurre")


def test_rape_is_a_french_food_word():
    """fiche 24152. No function word, no listed food word, so nothing fired."""
    assert product_needs_english("emmental râpé 200g")


@pytest.mark.parametrize("text", [
    "fromage râpé 200 g",
    "Gruyère râpé",
    "carottes râpées",
])
def test_the_whole_rape_family_is_caught(text):
    assert product_needs_english(text)


@pytest.mark.parametrize("text", [
    # The false positives removing "sandwich" could introduce: an English
    # description that keeps the regulator's French name in brackets.
    "Ham and butter sandwich (sandwich jambon beurre)",
    "Chicken sandwiches, 2-pack, sold chilled",
    "Grated Emmental cheese, 200 g",
    "Grated cheese (fromage râpé), 200 g pack",
    "Superior cooked ham, rindless and trimmed, torchon-style",
])
def test_an_english_description_keeping_the_french_name_still_passes(text):
    assert not product_needs_english(text)


# ── the Reason detector ────────────────────────────────────────────────────

@pytest.mark.parametrize("text", [
    # Every distinct French Reason the one-accent rule found on main.
    "Risque de corps étranger de type verre",
    "Risque physique (suspicion de corps étranger)",
    "Suspicion de contamination bactériologique",
    "Suspicion de contamination bactériologique;",
    "Suspicion de contamination bactériologique (Listeria)",
    "Eschérichia coli stec",
    "Risques microbiologiques — Escherichia coli shiga toxinogène (STEC)",
])
def test_a_one_accent_french_reason_is_caught(text):
    assert reason_needs_english(text)


@pytest.mark.parametrize("text", [
    # The translations written for those rows must come out clean, or the
    # repair would simply re-trip the gate.
    "Risk of a foreign body of the glass type",
    "Physical risk (suspected foreign body)",
    "Suspected bacteriological contamination",
    "Suspected bacteriological contamination (Listeria)",
    "Escherichia coli STEC",
    "Microbiological risks — Shiga toxin-producing Escherichia coli (STEC)",
])
def test_the_translations_read_as_english(text):
    assert not reason_needs_english(text)


@pytest.mark.parametrize("text", [
    # Under the four-word floor the two-accent rule still applies, so a
    # short English hazard with no listed English word is not flagged.
    "Metal fragments",
    "Mold",
    "Glycoalkaloids exceeding limit",
    "Pesticide residue",
    # And a long English sentence quoting accented names is not flagged.
    "Presence of Listeria detected following laboratory analyses (Aqualeha)",
    "Fromagerie P. Jacquin & Fils brand Valençay AOP \"fromage de chèvre au "
    "lait cru\" recalled due to generic E. coli",
])
def test_an_english_reason_is_not_flagged(text):
    assert not reason_needs_english(text)


def test_reason_needs_english_is_a_superset_of_looks_non_english():
    """The gates replaced looks_non_english() with this; nothing it caught
    before may be lost."""
    from pipeline._language import looks_non_english
    for text in ("Détéction de listéria",
                 "Présence d'alcaloïdes tropaniques (datura)",
                 "Ostrzeżenie publiczne dotyczące żywności: wykrycie "
                 "obecności bakterii Listeria monocytogenes",
                 "Rückruf: Listerien in geräucherten Forellenfilets"):
        if looks_non_english(text):
            assert reason_needs_english(text), text


# ── the gates stamp what the tests catch ───────────────────────────────────

def test_the_offline_promoter_stamps_a_one_accent_french_reason():
    """The gap this closes: the gate used looks_non_english(), so a row the
    register test would fail was published with no stamp on it."""
    from pipeline.promote_gate_passing import stamp_language_cleanup
    row = {"Product": "Grated Emmental cheese, 200 g",
           "Company": "Fromageries Terres d'Or",
           "Brand": "Top Budget",
           "Reason": "Risque de corps étranger de type verre",
           "Notes": ""}
    out = stamp_language_cleanup(dict(row))
    assert "[needs cleanup: Reason not in English]" in out["Notes"]
