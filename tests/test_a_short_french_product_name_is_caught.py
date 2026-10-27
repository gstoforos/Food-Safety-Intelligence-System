"""A short untranslated French Product is caught by the detector.

WHAT WAS WRONG (audit 2026-10-27)
=================================
Two RappelConso rows published untranslated on 2026-10-26, and NEITHER was
stamped, so the Morning Fix — which reads what the gates stamp — could not
have found them by reading stamps:

    'pâté aux olives'                                        fiche 23933
        Listeria monocytogenes, Tier 1, Stemmelen / E.Leclerc Castres
    'noix st jacques lr usaveurs 200 g et 400 g pêchées ane'  fiche 23926
        Listeria monocytogenes, Tier 1, U Saveurs / Magasins U

Both carry exactly ONE French function word ("aux", "et"), and the
function-word detector needs two by design. _FRENCH_FOOD_WORDS, the escape
hatch added on 2026-10-02 for exactly this floor, could not reach either:
the food words in them — "pâté", "olives", "noix" — are all shared with
English, which is the stated ground on which that list excludes words.

Worse, "pâté" was in _ENGLISH_PRODUCT_WORDS, and every clause of
product_needs_english treats one English word as a veto. So the single word
in 'pâté aux olives' that is unmistakably French was being read as proof
that the string had already been translated.

THE REPAIR
==========
  1. "pâté" removed from _ENGLISH_PRODUCT_WORDS. A word shared with French
     is not evidence of English — the mirror of the rule _FRENCH_FOOD_WORDS
     already follows.
  2. A third clause: one _FRENCH_STRUCTURE_WORDS hit and no English word in
     EITHER English vocabulary. The structure words are the French markers
     minus the articles and the "de" family (which appear inside protected
     names: Reblochon de Savoie, Camembert de Normandie, Tome des
     Ouchettes) and minus six English homographs ("est", "lot", "lots",
     "detection", "presence", "analyse").

Both halves were measured on all 2184 published rows before being written:
the repair flags the two defective rows and nothing else, and loses nothing
the old detector caught. The naive widening — one French marker, no English
word — was tried first and flagged 24 correctly-translated rows; the cases
below pin that it stays rejected.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline._language import (                                # noqa: E402
    _ENGLISH_PRODUCT_WORDS, _FRENCH_STRUCTURE_WORDS,
    product_needs_english,
)


# ── the two rows that found this ───────────────────────────────────────────

@pytest.mark.parametrize("text", [
    "pâté aux olives",
    "noix st jacques lr usaveurs 200 g et 400 g pêchées ane",
])
def test_the_two_rows_of_2026_10_26_are_caught(text):
    assert product_needs_english(text)


def test_pate_is_not_evidence_of_english():
    """It is the French spelling. It cannot veto the French detector."""
    assert "pâté" not in _ENGLISH_PRODUCT_WORDS


def test_a_lone_french_preposition_still_fires_with_no_english_word():
    for text in ("saucisse aux herbes", "tarte et crumble",
                 "galettes vendues sur place"):
        assert product_needs_english(text), text


# ── the false-positive mode: a translated name keeping a protected name ────
#
# Every one of these is a REAL published Product. The naive "one French
# marker, no English word" rule flagged all of them.

@pytest.mark.parametrize("text", [
    "Reblochon de Savoie farmhouse AOP 450g (RappelConso fiche 22349)",
    "Camembert de Normandie AOP 250g",
    "Camembert de Normandie AOP Bienheureux",
    "Sainte Maure de Touraine AOP (RappelConso fiche 21968)",
    "Tome des Ouchettes",
    "Gorgonzola D.O.P dolce 200 g",
    "Brie 3 kg 60% Le Baron",
    "Market baguette (baguette du marché)",
    "Sommières roll (roulade de Sommières)",
    "Les dentelles peppered coppa 80g",
    "Les moulinettes - green lentil coquillettes (RappelConso fiche 22693)",
    "Timur berry (Zanthoxylum armatum) / Baie De Timur Nepal",
    "La flam'fine Chorizo Peppers Provolone 180g size",
])
def test_a_protected_or_proper_french_name_is_not_flagged(text):
    assert not product_needs_english(text)


# ── the English homographs the structure list must not contain ────────────

@pytest.mark.parametrize("word", ["est", "lot", "lots", "detection",
                                  "presence", "analyse"])
def test_an_english_homograph_is_not_a_french_signal(word):
    assert word not in _FRENCH_STRUCTURE_WORDS


@pytest.mark.parametrize("text", [
    "Daisy Brand (EST. 21406) ready-to-eat product",
    "Chicken thigh 180 g, lot 7781",
    "Presence of Listeria monocytogenes in cheese",
    "Detection of mold on bulk jam containers",
])
def test_an_english_string_using_one_of_them_passes(text):
    assert not product_needs_english(text)


@pytest.mark.parametrize("word", ["de", "du", "des", "la", "le", "les",
                                  "au", "une", "un"])
def test_the_article_family_is_excluded(word):
    """These occur inside appellations, so one of them is not a signal."""
    assert word not in _FRENCH_STRUCTURE_WORDS


# ── the whole register, under the repaired detector ───────────────────────

def test_no_published_product_is_left_unflagged_and_untranslated():
    """The register-wide assertion lives in
    tests/test_product_and_firm_names_are_english.py; this one pins that
    the repair did not simply move the problem into the stamp."""
    import json
    rows = json.loads((ROOT / "docs" / "data" / "recalls.json")
                      .read_text(encoding="utf-8"))
    offenders = [(r.get("Source"), str(r.get("Product"))[:60])
                 for r in rows
                 if product_needs_english(r.get("Product"))
                 and "[needs cleanup:" not in str(r.get("Notes") or "")]
    assert not offenders, offenders[:8]
