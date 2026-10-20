# -*- coding: utf-8 -*-
"""A French food word list that holds the singular and not the plural stamps
half the rows.

THE INCIDENT (2026-10-19, found 2026-10-20)
-------------------------------------------
Twelve RappelConso rows were promoted on 2026-10-19 with their Product still
in French. FOUR of them carried
``[needs cleanup: Product not in English]``; the Morning Fix reads what is
stamped, so the other eight were invisible. Three of the eight were the same
recall family — Tier 1, E. coli O157:H7, ground beef:

    fiche 23866  'steaks hachés 5% 2x125g charolais; …'
    fiche 23865  'steaks hachés 12% 2x125g charolais; …'
    fiche 23864  'steaks hachés 20% 6*100g; steaks hachés 20% 6*125g'

``_FRENCH_FOOD_WORDS`` already held ``haché`` and ``hachée`` — it did not
hold ``hachés``. Nothing else in those strings is French: "steaks" is
English, "charolais" is a cattle breed, and "2x125g" is a weight, so the
two-hit function-word detector had nothing to fire on either. The sibling
row 'viande hachée 20% 350g' WAS stamped, from the same batch, on the same
fiche series, by the same gate — the only difference between them is one
letter of French morphology.

WHAT THIS TEST ASKS
-------------------
That the plural of every inflected entry in the list is in the list. Not a
stemmer: a stemmer in a food-safety path would collapse "fumé" (smoked) and
"fumée" (smoke) and is one more thing to be wrong. Just the pairs, named.

NOT WIDENED FURTHER, ON PURPOSE
-------------------------------
The first draft also added saucisson, rillettes, boudin, merguez and
chipolatas. Measured on all 2104 published rows, those five flagged five
GOOD rows whose English description legitimately keeps the French name —
"Antillean black pudding (boudin noir) BN 80g", "Superior goose rillettes
1 kg Rustadou", "Saucisson Sec 200 g — Tesco-exclusive". A detector that
cries wolf on good rows gets switched off, so they stay out and the last
test below pins them out.
"""
from __future__ import annotations

import pytest

from pipeline._language import _FRENCH_FOOD_WORDS, product_needs_english

#: singular -> plural, for every entry whose plural a regulator actually
#: publishes. Each pair was read off a real RappelConso designation.
PAIRS = {
    "haché": "hachés",
    "hachée": "hachées",
    "saucisse": "saucisses",
    "crevette": "crevettes",
    "bouchée": "bouchées",
    "fromage": "fromages",
    "volaille": "volailles",
}


@pytest.mark.parametrize("singular,plural", sorted(PAIRS.items()))
def test_both_numbers_are_in_the_list(singular, plural):
    assert singular in _FRENCH_FOOD_WORDS, f"{singular!r} dropped"
    assert plural in _FRENCH_FOOD_WORDS, (
        f"{plural!r} is missing while {singular!r} is present — this is the "
        f"2026-10-19 defect exactly: three Tier-1 ground-beef recalls "
        f"published in French because the list held 'haché' and not 'hachés'"
    )


@pytest.mark.parametrize("product", [
    "steaks hachés 5% 2x125g charolais; steaks hachés 5% 4x125g charolais",
    "steaks hachés 12% 2x125g charolais; steaks hachés 12% 4x150g charolais",
    "steaks hachés 20% 6*100g; steaks hachés 20% 6*125g",
    "viande hachée 20% 350g; viande hachée 20% 650g",
    "caissette saucisses de volaille 5 kg s/atm",
    "salade de cervelas 2kg",
    "jambon persillé tranché earl un groin de paradis",
])
def test_the_rows_that_went_out_in_french_are_caught_now(product):
    assert product_needs_english(product), f"still undetected: {product!r}"


@pytest.mark.parametrize("product", [
    "Beef burger patties, 5% fat, Charolais (steaks hachés 5% charolais), "
    "2 x 125 g and 4 x 125 g",
    "Minced beef, 20% fat (viande hachée 20%), 350 g and 650 g",
    "Plain raw poultry sausages, 5 kg catering case "
    "(caissette saucisses de volaille 5 kg s/ATM)",
    "Cervelat sausage salad, 2 kg (salade de cervelas 2 kg)",
    "Sliced parsleyed ham (jambon persillé tranché), lot 4026",
])
def test_a_translation_that_keeps_the_french_original_passes(product):
    """The repaired rows keep the regulator's own wording in brackets so the
    row still matches the fiche. That must not re-trigger the detector —
    which is exactly why the French-word branch requires the absence of an
    English product word."""
    assert not product_needs_english(product), f"false positive: {product!r}"


@pytest.mark.parametrize("product", [
    "Antillean black pudding (boudin noir) BN 80g",
    "Superior goose rillettes 1 kg Rustadou",
    "Saucisson Sec 200 g — Tesco-exclusive",
    "Merguez x6, merguez x12, merguez x18",
])
def test_the_words_that_were_measured_as_false_positives_stay_out(product):
    assert not product_needs_english(product), (
        f"{product!r} is a published row whose English description keeps a "
        f"French name; widening _FRENCH_FOOD_WORDS to boudin / rillettes / "
        f"saucisson / merguez / chipolatas flags it"
    )
