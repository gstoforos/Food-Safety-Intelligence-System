"""Swedish "mögel" is not the Dutch word for "possible".

WHY THIS EXISTS (2026-10-04)
============================
rules._ACCENT_STRICT is the homograph guard: a short term whose accent is
the only thing distinguishing it from a common word in another language is
matched against text that keeps its diacritics. Its own docstring made a
promise —

    "Derived rather than hand-listed, so a new accented short term in any
     vocabulary is covered automatically."

— and then listed eight vocabularies by hand. MOULD, added 2026-09-07, was
not among them, so no accented mould term had any protection: Swedish
"mögel", Polish "pleśń", Hungarian "penész", Greek "μούχλα". A hand-written
tuple is not a derivation.

The cut-off was the second half of the defect: four characters, and the
term that bit is five. "mögel" flattens to "mogel", which is a substring of
the Dutch word "mogelijk" ("possible"), and "mogelijke aanwezigheid van" is
the standard opening of an NVWA or FAVV notice. Every Dutch and Flemish
recall whose text named no organism classified as mould, Tier 2, on
matched_term 'mogel'.

No published row carried the bad stamp on 2026-10-04 — only because the
pathogen branches run before the mould branch and every Dutch row so far
named an organism. The rows it was waiting for are Dutch foreign-body and
chemical recalls, and those were being produced right then by the
FOREIGN_MATTER gap fixed the same morning.

WHAT THE FIX MAY NOT COST
Three things, each with a case below:
  * the term must keep working for the language it belongs to ("mögel" must
    still reach "mögelangrepp"), so five-character terms substring-match on
    the accented text rather than being word-anchored;
  * a lexicon that deliberately carries BOTH spellings must keep matching
    the unaccented one (FOREIGN_MATTER lists "γυαλί" and "γυαλι");
  * scripts that write no word boundaries must be left out entirely.
    Japanese "カビ" flattens to "カヒ" because NFD splits the dakuten off,
    which looks accented to this code; word-anchoring can never match it,
    because every CJK character around it is a word character. That is how
    this fix first broke
    test_the_classifier_reads_the_fleet_languages::test_accepts on
    "異物（水カビ様の異物）混入".
"""
from __future__ import annotations

import pytest

from pipeline.gap_finder.rules import (classify, _ACCENT_STRICT,
                                       _CLASSIFIED_LEXICONS, MOULD,
                                       _MOULD_N, _normalize)


class TestTheDerivationIsADerivation:

    def test_every_lexicon_classify_reads_is_in_the_derivation(self):
        """The bug was a lexicon in one list and missing from the other."""
        assert MOULD in _CLASSIFIED_LEXICONS, (
            "MOULD was added 2026-09-07 and never joined the _ACCENT_STRICT "
            "source list, so no accented mould term had homograph protection")

    def test_the_swedish_mould_term_is_protected(self):
        assert _ACCENT_STRICT.get("mogel") == "mögel"

    def test_a_five_character_term_is_covered(self):
        """The old cut-off was four, and the term that bit is five."""
        assert len("mogel") == 5
        assert "mogel" in _ACCENT_STRICT


class TestTheDutchCollision:

    @pytest.mark.parametrize("text", [
        "mogelijke aanwezigheid van een vreemd voorwerp",
        "mogelijke aanwezigheid van glasdeeltjes",
        "Het FAVV waarschuwt voor de mogelijke aanwezigheid van een "
        "metaaldeeltje",
        "mogelijkheid van verontreiniging",
    ])
    def test_no_dutch_text_classifies_as_mould_on_mogel(self, text):
        c = classify(pathogen="", reason=text, product=text)
        assert c.matched_term != "mogel", text
        assert c.category != "mould", (text, c)


class TestTheFixCostsNothing:

    @pytest.mark.parametrize("text", [
        "mögel i produkten",
        "mögelangrepp på brödet",
        "misstänkt mögeltillväxt",
    ])
    def test_swedish_mould_still_matches(self, text):
        c = classify(pathogen="", reason=text, product=text)
        assert c.category == "mould", (text, c)

    @pytest.mark.parametrize("text", [
        "θραύσματα γυαλιού στο προϊόν",       # accented
        "θραυσματα γυαλι στο προιον",          # the deliberate flat spelling
    ])
    def test_a_lexicon_that_lists_both_spellings_keeps_both(self, text):
        c = classify(pathogen="", reason=text, product=text)
        assert c.category == "foreign_matter", (text, c)

    def test_japanese_mould_still_matches(self):
        """CJK writes no word boundaries; anchoring can never match there."""
        t = "異物（水カビ様の異物）混入"
        c = classify(pathogen="", reason=t, product=t)
        assert (c.verdict, c.category) == ("accept", "mould"), c

    def test_no_boundaryless_script_term_is_word_anchored(self):
        import re
        boundaryless = [k for k in _ACCENT_STRICT
                        if not re.search(r"[a-zͰ-ϿЀ-ӿ]", k)]
        assert not boundaryless, (
            "these terms are written in a script with no word boundaries and "
            "would be matched with boundary lookarounds that can never "
            f"succeed: {boundaryless}")

    def test_the_ten_rows_of_2026_09_26_are_still_guarded(self):
        """The earlier homograph fix must not have been widened away."""
        assert _ACCENT_STRICT.get("ble") == "blé"
        assert _ACCENT_STRICT.get("agg") == "ägg"
        for t in ("Produktet ble fjernet fra hyllene",
                  "Varen ble solgt i butikk"):
            c = classify(pathogen="", reason=t, product=t)
            assert c.matched_term != "ble", t
