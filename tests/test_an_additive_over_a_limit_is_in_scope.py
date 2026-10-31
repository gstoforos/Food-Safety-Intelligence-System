# -*- coding: utf-8 -*-
"""An additive over a permitted limit is a hazard this database covers.

THE GAP THIS EXISTS TO CLOSE (morning fix 2026-10-30)
=====================================================
The printed AFTS scope, on every brief since 2026-07-29 and in
pipeline/_pathogen_scope.is_in_afts_scope, includes unapproved additives
over a limit. pipeline/_publish_gate.HAZARD_CLASS_KEYWORDS["chemical"]
carried 101 keywords and not one of them was an additive, a colour or a
dye.

So classify_hazard() returned the EMPTY SET for an additive hazard, and
is_in_afts_scope() — which accepts any class except allergen and
fermentation — fell through to False. That produced exactly the asymmetry
the is_in_afts_scope docstring was written to end: a recall arriving with
its hazard NAMED was refused at the Pending gate, while the same recall
arriving with an empty Pathogen was enriched later and published.

MEASURED. CAA (JP) 00000036069, エスビーケー・トヤマ "パントテン酸300㎎",
事由 "パントテン酸カルシウムの食品添加物使用基準違反" — a breach of the
food-additive usage standard for calcium pantothenate — was refused by
merge_master.validate_pending_row with "pathogen_out_of_scope" when this
morning's pass queued it.

And a row ALREADY IN RECALLS shows the gap was not new:

    "Chlorpyrifos (pesticide residue) and unauthorized dye E 122"

classified as chemical only through its PESTICIDE half. The
unauthorised-dye half named a hazard the gate could not see at all, so an
unauthorised dye on its own had never been publishable.

The keywords are deliberately two-word forms plus specific substances. A
bare "additive" matches ordinary ingredient prose and a bare "colour"
matches the colour of a product, which is most of the register's Reason
column — those over-matches are asserted against below, because the fix
for a blind spot must not be a gate that classifies everything.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline._publish_gate import classify_hazard          # noqa: E402
from pipeline._pathogen_scope import is_in_afts_scope       # noqa: E402


# The hazard string the pass published, and the regulator wording behind it.
PANTOTHENATE = "Unauthorised food additive use (calcium pantothenate)"
PANTOTHENATE_REASON = (
    "Violation of the food-additive usage standard for calcium pantothenate.")


@pytest.mark.parametrize("text", [
    PANTOTHENATE,
    PANTOTHENATE_REASON,
    "Unauthorised food additive",
    "unauthorized additive E 171",
    "non-permitted additive",
    "unauthorized dye E 122",
    "unauthorised colour Rhodamine B",
    "additif non autorisé",
    "colorant non autorisé",
    "nicht zugelassener Zusatzstoff",
    "パントテン酸カルシウムの食品添加物使用基準違反",
])
def test_an_additive_hazard_classifies_as_chemical(text):
    assert "chemical" in classify_hazard(text), (
        f"classify_hazard({text!r}) = {sorted(classify_hazard(text))} — an "
        f"additive over a permitted limit is in the printed AFTS scope, and "
        f"a hazard with no class is refused at the Pending gate with "
        f"'pathogen_out_of_scope'")


def test_the_pantothenate_row_is_in_afts_scope():
    """The whole chain, not just the keyword: this is what the gate asks."""
    assert is_in_afts_scope(PANTOTHENATE, PANTOTHENATE_REASON), (
        "the row this gap was found on is still out of scope")


def test_the_published_dye_row_now_classifies_on_its_own_merit():
    """It always classified — but only as a pesticide.

    Strip the pesticide half and what is left must still be a hazard,
    otherwise an unauthorised dye alone is unpublishable.
    """
    assert "chemical" in classify_hazard("unauthorized dye E 122")
    full = "Chlorpyrifos (pesticide residue) and unauthorized dye E 122"
    assert "chemical" in classify_hazard(full)


@pytest.mark.parametrize("text", [
    # Ordinary ingredient and quality prose that must NOT become a hazard.
    "contains additives and preservatives",
    "no additives",
    "colour change in the product",
    "the color of the sauce was off",
    "Product colour and texture not as expected",
    "artificial colours and flavours",
    "food additives declared on the label",   # see the note below
])
def test_ordinary_prose_is_not_turned_into_a_chemical_hazard(text):
    """The over-match guard.

    One of these is a deliberate near-miss: "food additives declared on
    the label" DOES contain "food additive". It is in the list to pin that
    the cost of the two-word token is understood and bounded — a labelling
    sentence that happens to name additives would classify as chemical. If
    this case ever fires in production, the answer is a more specific
    token, not a wider one.
    """
    classes = classify_hazard(text)
    if text == "food additives declared on the label":
        pytest.xfail("known, bounded cost of the 'food additive' token")
    assert "chemical" not in classes, (
        f"classify_hazard({text!r}) = {sorted(classes)} — ordinary prose "
        f"must not manufacture a chemical hazard class")


def test_a_bare_additive_word_is_not_a_token():
    """Why the tokens are two words.

    A bare "additive" or "colour" would reach half the register's Reason
    column. Assert the shape of the keyword list, not only its behaviour,
    so a later widening is a deliberate edit rather than an accident.
    """
    from pipeline._publish_gate import HAZARD_CLASS_KEYWORDS
    chem = {k.strip().lower() for k in HAZARD_CLASS_KEYWORDS["chemical"]}
    for bare in ("additive", "additives", "colour", "color", "colorant",
                 "dye", "e 122"):
        assert bare not in chem, (
            f"{bare!r} is a bare word in the chemical keyword list — it "
            f"will classify ordinary ingredient and quality prose as a "
            f"chemical hazard")
