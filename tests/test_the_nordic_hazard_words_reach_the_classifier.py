"""A Nordic regulator's own hazard word must classify, not fall to 'unknown'.

WHAT WAS WRONG (found 2026-10-11)
=================================
Mattilsynet writes a hazard as one compound noun and the gap-finder
classifier had none of them. Four real recalls, all inside AFTS scope, were
archived with "unknown: No matching hazard category — defer to manual
review" — and three of the four had the regulator's OWN per-recall URL
already attached to the archive row:

  2026-09-18  Sbselite blå valmuefrø fra Tyrkia        plantetoksiner
  2026-09-25  Foodtrade AS pistasjkjerner fra USA      muggsoppgift
  2026-09-25  Baza Nordic surkål fra Litauen           glasspartikler
  2026-10-05  BAMA produkter med grønnkål              plantevernmiddel

The BAMA one is the measure of the cost. It was archived on 2026-10-06 with
its authority URL. Over the five days that followed, NINE further rows for
the same recall arrived from the news collectors and each was archived with
"no-authority-url: no official Mattilsynet press-release URL in the source
article" — while the URL sat in Weekly_Rejected the whole time. The pesticide
residues above the limit are squarely in scope (chemical contaminants); the
register simply could not read the word.

TWO EARLIER GAPS OF THE SAME SHAPE are already documented in rules.py: the
Polish pyrrolizidine alkaloids (2026-10-03) and the "ble"/"blé" homograph
(2026-09-26). The homograph fix is why these four rows say 'unknown' today
instead of 'allergen' — it removed the WRONG answer and left no right one.

WHAT THIS TEST PINS
-------------------
The four titles, verbatim, with the category each must reach. Not the
matched term: a later editor may add a better needle, and this test should
not stand in the way of that. Only the verdict and the family are fixed.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline.gap_finder import rules as R                     # noqa: E402

#: Title as Mattilsynet published it -> the hazard family it must reach.
NORDIC = [
    ("BAMA tilbakekaller produkter med grønnkål grunnet funn av "
     "plantevernmiddel over grenseverdi", "synthetic_chemical"),
    ("Sbselite blå valmuefrø fra Tyrkia tilbakekalles etter funn av "
     "plantetoksiner over grenseverdi", "natural_toxin"),
    ("Foodtrade AS tilbakekaller pistasjkjerner fra USA etter funn av "
     "muggsoppgift over grenseverdi", "microbial_toxin"),
    ("Baza Nordic tilbakekaller surkål fra Litauen etter funn av "
     "glasspartikler.", "foreign_matter"),
]


@pytest.mark.parametrize("title,category", NORDIC)
def test_a_nordic_hazard_compound_is_accepted(title, category):
    c = R.classify(title)
    assert c.verdict == "accept", (
        f"{title[:60]!r} classified {c.verdict}/{c.category} "
        f"({c.rule}). This is a real Mattilsynet recall in AFTS scope; a "
        f"gate that cannot read the hazard word throws it away.")
    assert c.category == category, (
        f"{title[:60]!r} -> category {c.category!r}, expected "
        f"{category!r} (matched {c.matched_term!r}).")


@pytest.mark.parametrize("title,_cat", NORDIC)
def test_a_nordic_hazard_compound_is_not_an_allergen(title, _cat):
    """The 2026-09-26 homograph fix must stay fixed.

    Before it, all four of these were filed "Undeclared allergen — out of
    scope (Rule B reject)" with Pathogen set to the Norwegian past-tense
    verb that French "blé" flattens onto.
    """
    c = R.classify(title)
    assert c.category != "allergen", (
        f"{title[:60]!r} classified as an allergen again — the homograph "
        f"regression is back, and an allergen reject DISCARDS the recall.")


def test_the_norwegian_word_for_a_mug_is_not_a_mould_needle():
    """Why bare "mugg" is not in MOULD, stated as a test rather than a note."""
    assert "mugg" not in {t.lower() for t in R.MOULD}, (
        'Bare "mugg" is also the ordinary Norwegian/Swedish word for a mug '
        'and carries no accent for the homograph guard to demand. Use the '
        'compounds: muggsopp, muggvekst, muggsoppgift.')
