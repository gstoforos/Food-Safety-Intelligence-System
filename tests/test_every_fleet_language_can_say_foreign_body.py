"""A regulator that will not name the object still named a foreign body.

WHY THIS EXISTS (2026-10-04)
============================
FOREIGN_MATTER could express the GENERIC concept — "a foreign body was
found", which is how most regulators word it when the object has not been
identified — in English, French and Greek only. Every other language in the
43-country fleet could name a specific material (glass, metal, plastic) and
nothing else, so a notice that said only "possible presence of a foreign
body" matched no hazard branch, fell through to

    reject / unknown / "No matching hazard category — defer to manual
    review."

and was archived unpublished. 14 of the 18 fleet languages tested had the
hole.

THE ROW THAT FOUND IT. Italy, 2026-10-02, Franchi Salumi Srl, salamella
dolce, "possibile presenza di un corpo estraneo", refused by
gap_finder/it/rules.py and sitting in Weekly_Rejected while the same recall
sat unpublished in Pending. Italian was not special: "corpo estraneo"
existed in the module, but only inside _FM_CONTEXT_N — the set that
LICENSES a bare material noun and cannot match on its own.

THE SECOND BUG, FOUND BY THE SAME TEST. While checking the languages,
Dutch "mogelijke aanwezigheid van een vreemd voorwerp" came back as
category MOULD on matched_term 'mogel'. Swedish "mögel" de-accents to
"mogel", a substring of the Dutch word "mogelijk" ("possible") — and
"mogelijke aanwezigheid van" opens virtually every NVWA and FAVV notice.
See test_a_short_term_keeps_its_accent_in_every_lexicon.py. The two bugs
compounded: the languages that could not say "foreign body" were sent into
the mould branch, which then mislabelled them.
"""
from __future__ import annotations

import pytest

from pipeline.gap_finder.rules import classify

# (language, the phrasing a regulator in that language actually uses)
FOREIGN_BODY_PHRASINGS = [
    ("en", "Possible presence of a foreign body"),
    ("fr", "présence possible d'un corps étranger"),
    ("it", "possibile presenza di un corpo estraneo"),
    ("it-pl", "rilevati corpi estranei nel prodotto"),
    ("es", "posible presencia de un cuerpo extraño"),
    ("pt", "possível presença de corpo estranho"),
    ("de", "möglicher Fremdkörper im Produkt"),
    ("nl", "mogelijke aanwezigheid van een vreemd voorwerp"),
    ("pl", "możliwa obecność ciała obcego"),
    ("hu", "idegen test a termékben"),
    ("cs", "možná přítomnost cizího předmětu"),
    ("sv", "misstänkt främmande föremål"),
    ("da", "mistanke om fremmedlegeme"),
    ("no", "mistanke om fremmedlegeme i produktet"),
    ("fi", "vieras esine tuotteessa"),
    ("is", "aðskotahlutur í vörunni"),
    ("ro", "posibila prezenta a unui corp strain"),
    ("hr", "moguća prisutnost stranog tijela"),
    ("tr", "yabancı madde bulunma olasılığı"),
    ("el", "πιθανή παρουσία ξένου σώματος"),
]


@pytest.mark.parametrize("lang,text", FOREIGN_BODY_PHRASINGS,
                         ids=[l for l, _ in FOREIGN_BODY_PHRASINGS])
def test_a_generic_foreign_body_is_in_scope_in_every_fleet_language(lang, text):
    c = classify(pathogen="", reason=text, product=text)
    assert (c.verdict, c.category) == ("accept", "foreign_matter"), (
        f"{lang}: {text!r} -> {c.verdict}/{c.category} "
        f"(matched {c.matched_term!r}). A regulator that has not identified "
        f"the object has still named a foreign body, which is in the printed "
        f"AFTS scope.")


@pytest.mark.parametrize("text", [
    "mogelijke aanwezigheid van glasdeeltjes in het product",
    "glasscherven aangetroffen in het product",
    "er zijn metaaldeeltjes aangetroffen in het product",
])
def test_dutch_compounds_the_material_into_one_word(text):
    """"glas" is four characters, so it is word-anchored and cannot reach
    "glasdeeltjes" on its own."""
    c = classify(pathogen="", reason=text, product=text)
    assert c.category == "foreign_matter", (text, c)


@pytest.mark.parametrize("text", [
    # The Dutch opening that collided with Swedish "mögel".
    "mogelijke aanwezigheid van Listeria monocytogenes",
    "Moeder ontvoerde Insiya zwaar teleurgesteld in mogelijke uitspraak",
    "mogelijke overschrijding van de norm",
])
def test_the_dutch_word_for_possible_is_not_mould(text):
    c = classify(pathogen="", reason=text, product=text)
    assert c.category != "mould", (
        f"{text!r} -> mould on {c.matched_term!r}; Swedish 'mögel' "
        f"de-accents to 'mogel', a substring of Dutch 'mogelijk'")


def test_a_named_pathogen_still_decides_the_category():
    """The foreign-body terms must not outrank an organism."""
    t = ("Listeria monocytogenes rilevata nel prodotto; segnalata anche la "
         "possibile presenza di un corpo estraneo")
    c = classify(pathogen="Listeria monocytogenes", reason=t, product=t)
    assert c.category == "pathogen", c
