"""French "blé" is not Norwegian "ble", and the metal is not the verb.

WHY THIS EXISTS (2026-09-26)
===========================
rules._normalize strips diacritics so a source writing "aflatossina" matches
"aflatoxin". For terms of four characters or fewer that is too blunt:
de-accenting turns a word in one language into a very common word in another.
Word boundaries were already in place and did not help, because these are
homographs, not substrings.

Ten published rows had a single such token as their ENTIRE Pathogen field:

    'ble'  x3  Norway   French "blé" (wheat) -> "ble", Norwegian for "was".
               One is Baza Nordic's sauerkraut recall for GLASS PARTICLES,
               filed "Undeclared allergen — out of scope" with Pathogen "ble".
    'lead' x5  Nigeria, Hong Kong.  The metal against the verb. One is a
               notice about excessive AFLATOXIN, rejected as a heavy metal.
    'tej'  x1  Poland.  Hungarian "tej" (milk) against Polish "tej partii".
    'agg'  x1  Sweden.  Swedish "ägg" — a genuine egg recall, and the only
               true positive of the ten.

The repo had met this shape twice: _FM_BOUND exists so "stone" does not match
"Blackstone", and pipeline/_publish_gate.py records a bare "hav" matching the
word "have". Boundaries fix the substring half; only keeping the accent, or
anchoring the word, fixes the homograph half.

The cost asymmetry matters for reading these tests. ALLERGENS and HEAVY_METALS
are REJECT vocabularies here, so a false match throws away a real recall,
while a miss merely lets a row through to the register's own allergen gate.
When in doubt these terms must NOT match.
"""
from __future__ import annotations

import pytest

from pipeline.gap_finder.rules import (classify, _ACCENT_STRICT,
                                       _UNANCHORED_HOMOGRAPHS,
                                       _normalize, _normalize_keep_accents)


class TestTheAccentIsWhatDistinguishes:

    def test_the_strict_set_was_derived_not_hand_listed(self):
        """Every accented short term in any vocabulary must be covered, so a
        new one added next year is protected without anyone remembering."""
        assert len(_ACCENT_STRICT) >= 14
        assert _ACCENT_STRICT.get("ble") == "blé"
        assert _ACCENT_STRICT.get("agg") == "ägg"
        assert _ACCENT_STRICT.get("dio") == "dió"

    def test_norwegian_ble_is_not_french_wheat(self):
        """The exact row: glass particles, filed as an allergen."""
        t = ("Baza Nordic tilbakekaller surkål fra Litauen etter funn av "
             "glasspartikler. Produktet ble trukket tilbake fra butikkene.")
        v = classify(pathogen="", reason=t, product=t)
        assert v.category != "allergen", (
            f"classified {v.category!r} on {v.matched_term!r} — a glass recall")
        assert v.matched_term != "ble"

    @pytest.mark.parametrize("text", [
        "Foodtrade AS tilbakekaller pistasjkjerner. Varen ble solgt i butikk.",
        "Sbselite blå valmuefrø fra Tyrkia tilbakekalles, det ble påvist morfin",
        "Produktet ble fjernet fra hyllene",
    ])
    def test_no_norwegian_text_matches_on_ble(self, text):
        v = classify(pathogen="", reason=text, product=text)
        assert v.matched_term != "ble", text

    def test_french_ble_with_its_accent_still_matches(self):
        """The term must keep working for the language it belongs to."""
        t = "Rappel de farine de blé"
        v = classify(pathogen="", reason=t, product=t)
        assert v.category == "allergen"

    def test_swedish_agg_still_matches(self):
        """The one true positive of the ten must survive the fix."""
        t = "Ägg återkallas"
        v = classify(pathogen="", reason=t, product=t)
        assert v.category == "allergen"

    def test_polish_tej_is_not_hungarian_milk(self):
        t = ("GIS ostrzega przed herbatką z pokrzywy. Tej partii nie należy "
             "spożywać.")
        v = classify(pathogen="", reason=t, product=t)
        assert v.category != "allergen"
        assert v.matched_term != "tej"

    def test_hungarian_milk_still_has_longer_terms(self):
        """tej was dropped rather than anchored; tejfehérje and tejtermék
        remain, so Hungarian milk is not wholly invisible."""
        from pipeline.gap_finder.rules import ALLERGENS
        assert "tejfehérje" in ALLERGENS
        assert "tejtermék" in ALLERGENS


class TestTheMetalIsNotTheVerb:

    def test_the_bare_words_are_refused(self):
        for w in ("lead", "tin", "tej"):
            assert w in _UNANCHORED_HOMOGRAPHS

    def test_an_aflatoxin_notice_is_not_a_heavy_metal_recall(self):
        """Hong Kong, 2026-09-21: rejected as heavy metal on 'lead', from
        'may lead to'. It is an aflatoxin notice and belongs in scope."""
        t = ("Food Alerts - Excessive aflatoxin detected in the product; "
             "consumption may lead to health risks")
        v = classify(pathogen="", reason=t, product=t)
        assert v.category != "heavy_metal"
        assert v.verdict == "accept", (
            "this is a mycotoxin recall and the gap finder should take it")

    @pytest.mark.parametrize("text", [
        "this may lead to illness",
        "the investigation will lead to a recall",
        "Public Reminder of products that may lead to harm",
    ])
    def test_the_verb_never_fires(self, text):
        v = classify(pathogen="", reason=text, product=text)
        assert v.matched_term != "lead", text

    @pytest.mark.parametrize("text", [
        "Recall due to elevated lead levels in the product",
        "Product recalled due to lead contamination",
        "excess lead found in the sample",
    ])
    def test_the_anchored_phrases_do_fire(self, text):
        """Same anchoring scrapers/_pathogen_vocab.py already used for lead;
        this module simply never got it."""
        v = classify(pathogen="", reason=text, product=text)
        assert v.category == "heavy_metal", (text, v.matched_term)

    def test_a_tin_can_is_not_a_tin_contamination(self):
        t = "canned in a tin, recalled for Listeria monocytogenes"
        v = classify(pathogen="", reason=t, product=t)
        assert v.category == "pathogen"


class TestNothingElseMoved:
    """A vocabulary change must not shift the ordinary cases."""

    @pytest.mark.parametrize("text,category", [
        ("Rückruf wegen Listeria monocytogenes", "pathogen"),
        ("rappel pour cause de Salmonella", "pathogen"),
        ("aflatossina in arachidi", "microbial_toxin"),
        ("presence of glass fragments", "foreign_matter"),
        ("undeclared allergen: peanut", "allergen"),
    ])
    def test_the_normal_path(self, text, category):
        v = classify(pathogen="", reason=text, product=text)
        assert v.category == category, (text, v.category, v.matched_term)

    def test_the_two_normalisers_differ_only_in_accents(self):
        s = "Aflatossina  in  ARACHIDI"
        assert _normalize(s) == "aflatossina in arachidi"
        assert _normalize_keep_accents("Blé  ET  Œuf") == "blé et œuf"


class TestItalyAcceptsItsOwnNotices:
    """salute.gov.it publishes recall notices as PDFs under
    /new/sites/default/files/external_data/avvisi_sicurezza_alimentare/, which
    is where 13 of the register's 20 Italian URLs live. The old regex —
    r"(avvisiSicurezza|richiami|p3_2_1_3_1)" — refused all of them while
    ACCEPTING the listing page, whose own slug contains "richiami"."""

    @staticmethod
    def _rx():
        from pipeline.gap_finder.countries import get
        return get("it").authority_item_url_regex

    @pytest.mark.parametrize("path", [
        "/new/sites/default/files/external_data/avvisi_sicurezza_alimentare/"
        "CARTELLODIRICHIAMO_HAMBURGERDIBUFALO170g1_1790260408.pdf",
        "/new/sites/default/files/external_data/avvisi_sicurezza_alimentare/"
        "cartello richiamo_1781203550.pdf",
        "/portale/richiami/dettaglioRichiamo.jsp?id=23769",
        "/new/it/news-e-media/notizie/richiamo-di-pate-di-maiale-proveniente",
    ])
    def test_a_real_notice_is_accepted(self, path):
        import re
        assert re.search(self._rx(), path, re.I), path
        assert re.search(self._rx(), "https://www.salute.gov.it" + path, re.I)

    @pytest.mark.parametrize("path", [
        # The listing page. Published as a recall row under the old regex.
        "/new/it/avvisi/avvisi-e-richiami-di-prodotti-alimentari",
        "/portale/news/p3_2_1_1_1.jsp",
        "/new/it/tema/sistema-di-controllo-della-sicurezza-alimentare",
        "/new/it/news-e-media/notizie/latte-crudo-e-prodotti-derivati-linee-guida",
        "/",
        "",
    ])
    def test_a_listing_or_general_page_is_refused(self, path):
        import re
        assert not re.search(self._rx(), path, re.I), path
