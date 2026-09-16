"""Regression tests for the field-pollution bugs found in the 2026-09-16
accuracy brief.

Three distinct defects, all of which put non-data into data columns:

  1. gap_finder_tavily._extract_company_product had an English-only recall
     verb list, so any non-English headline fell through to a last-ditch
     `return t[:120], ""` that copied the WHOLE HEADLINE into Company.
     daily_recall_search then mirrors Company into Brand, so one bad parse
     polluted two columns; the product fallback polluted a third.
     Production row (Pending, 2026-09-16):
        Company = Brand = Product =
          "Sjømathuset AS tilbakekaller Lerøy laks loin 600g og 250g etter
           mistanke om listeria | Mattilsynet\n\n# Sjømathuset AS …"

  2. daily_recall_search's Product fallback split markdown-converted page
     content on ". ", which for an agency page returns the title echo plus
     the ATX heading — newlines and "#" included.

  3. scrapers/europe_eu/gis._clean stripped HTML tags but not markdown
     leaders, so GIS rows reached the sheet with Product/Reason literally
     beginning "> Ostrzeżenie publiczne dotyczące żywności: …".
"""
from __future__ import annotations

import pytest

from pipeline.gap_finder_tavily import (
    _extract_company_product,
    _strip_site_suffix,
)
from pipeline.daily_recall_search import _first_body_sentence
from scrapers.europe_eu.gis import _clean, _subject_from_title
from scrapers.europe_eu.rappelconso import _translate_reason_fr_to_en


# The exact strings that reached the published sheet on 2026-09-16.
MATTILSYNET_TITLE = (
    "Sjømathuset AS tilbakekaller Lerøy laks loin 600g og 250g "
    "etter mistanke om listeria | Mattilsynet"
)
MATTILSYNET_CONTENT = (
    MATTILSYNET_TITLE
    + "\n\n# Sjømathuset AS tilbakekaller Lerøy laks loin 600g og 250g "
      "etter mistanke om listeria"
    + "\n\nSjømathuset AS kaller tilbake Lerøy laks loin på 600 gram og "
      "250 gram, med lotnumrene 315219 og 315215. Kundene bes kaste "
      "produktet."
)
GIS_LISTERIA_TITLE = (
    "> Ostrzeżenie publiczne dotyczące żywności: wykrycie obecności "
    "bakterii Listeria monocytogenes w jednej partii sera podpuszczkowego"
)
GIS_ALKALOID_TITLE = (
    "> Ostrzeżenie publiczne dotyczące żywności: alkaloidy pirolizydynowe "
    "w określonej partii herbatki ziołowej z pokrzywy"
)


# ── Defect 1 — headline never becomes Company ───────────────────────────

def test_norwegian_recall_headline_parses_into_company_and_product():
    company, product = _extract_company_product(MATTILSYNET_TITLE, "")
    assert company == "Sjømathuset AS"
    assert "Lerøy laks loin" in product
    # The site suffix must not survive into either column.
    assert "Mattilsynet" not in company
    assert "Mattilsynet" not in product


@pytest.mark.parametrize("title", [
    # Unparseable headlines: no recall verb, no separator. Company MUST be
    # empty so the confirm agent holds the row for reviewer 2.
    "Tilbaketrekking av parti med røkt laks grunnet funn av bakterier",
    "Publiczne ostrzeżenie o produkcie spożywczym w obrocie handlowym",
    "Informazione al consumatore su un prodotto alimentare in commercio",
])
def test_unparseable_headline_yields_empty_company_not_the_headline(title):
    company, _product = _extract_company_product(title, "")
    assert company == "", (
        "an unparsed headline must leave Company empty; returning the "
        "headline silently passes the publish gate"
    )


def test_company_is_never_longer_than_a_plausible_firm_name():
    for title in (MATTILSYNET_TITLE, GIS_LISTERIA_TITLE, GIS_ALKALOID_TITLE):
        company, _ = _extract_company_product(title, "")
        assert len(company) <= 80


# ── Defect 1b — English parsing must not regress ────────────────────────

@pytest.mark.parametrize("title,expected_company", [
    ("Danone USA Recalls So Delicious Salted Caramel Cluster Pints "
     "due to foreign material", "Danone USA"),
    ("Olymel recalls chicken breast strips for Listeria", "Olymel"),
    ("FDA Alert: Acme Foods Inc. recalls spinach due to E. coli",
     "Acme Foods Inc."),
])
def test_english_headlines_still_parse(title, expected_company):
    company, product = _extract_company_product(title, "")
    assert company == expected_company
    assert product


# ── Defect 1c — site-suffix stripper must be conservative ───────────────

@pytest.mark.parametrize("title", [
    "Lerøy laks loin 600g - 250g",     # bare dash is NOT a site boundary
    "Brie de Melun AOP",               # nothing to strip
    "A | B",                           # too short to strip safely
])
def test_site_suffix_stripper_leaves_real_titles_alone(title):
    assert _strip_site_suffix(title) == title


# ── Defect 2 — Product fallback skips the title echo ────────────────────

def test_product_fallback_returns_body_prose_not_the_title_echo():
    product = _first_body_sentence(MATTILSYNET_CONTENT, MATTILSYNET_TITLE)
    assert product.startswith("Sjømathuset AS kaller tilbake")
    assert "\n" not in product
    assert "#" not in product
    assert "|" not in product


def test_product_fallback_never_emits_markdown_structure():
    for content in (MATTILSYNET_CONTENT,
                    "# Heading only\n\n> quoted line that is long enough"):
        product = _first_body_sentence(content, "")
        assert not product.startswith(("#", ">", "-", "*"))
        assert "\n" not in product


def test_product_fallback_handles_empty_content():
    assert _first_body_sentence("", "") == ""


# ── Defect 3 — GIS markdown leaders stripped ────────────────────────────

@pytest.mark.parametrize("raw", [GIS_LISTERIA_TITLE, GIS_ALKALOID_TITLE])
def test_gis_clean_strips_markdown_blockquote_marker(raw):
    cleaned = _clean(raw)
    assert not cleaned.startswith(">")
    assert cleaned.startswith("Ostrzeżenie publiczne")


@pytest.mark.parametrize("raw,expected_start", [
    (GIS_LISTERIA_TITLE, "wykrycie obecności bakterii Listeria"),
    (GIS_ALKALOID_TITLE, "alkaloidy pirolizydynowe"),
])
def test_gis_product_drops_the_notice_type_boilerplate(raw, expected_start):
    product = _subject_from_title(_clean(raw))
    assert product.startswith(expected_start)
    assert "Ostrzeżenie publiczne" not in product


def test_gis_subject_falls_back_to_full_title_when_prefix_absent():
    # A GIS wording change must degrade to today's behaviour, not an
    # empty Product cell.
    title = "Wycofanie partii produktu z obrotu"
    assert _subject_from_title(title) == title


def test_gis_clean_handles_combined_markers_and_tags():
    assert _clean("> # <strong>Ostrzeżenie</strong> publiczne") == \
        "Ostrzeżenie publiczne"


# ── Defect 4 — French Reason reaching the published sheet ───────────────
#
# tests/test_language_policy.py requires Reason to be ENGLISH (Company,
# Brand and Product legitimately stay in the regulator's language). The
# FR->EN translator was wired in but had no template for RappelConso's two
# commonest motifs, so French Reasons were published verbatim:
#   "Non conformite microbiologique", "Détection salmonelle".
# The "détection" template existed but required "de"/"d'" after the verb,
# which those strings omit.

@pytest.mark.parametrize("french,english", [
    ("Non conformite microbiologique", "Microbiological non-conformity"),
    ("Non conformité microbiologique", "Microbiological non-conformity"),
    ("Détection salmonelle", "Detection of Salmonella"),
    ("Détection e. coli stec", "Detection of E. coli STEC"),
    ("Détection de Campylobacter spp", "Detection of Campylobacter spp"),
    ("Rappel de précaution suite à la détection d'E. coli STEC sur un "
     "produit découpé par un magasin",
     "Precautionary recall following detection of E. coli STEC on a "
     "product cut in store"),
    ("Presence of Salmonella mises en évidence dans le cadre du plan "
     "d'autocontrôle",
     "Presence of Salmonella found during own-check testing"),
])
def test_common_rappelconso_motifs_translate_to_english(french, english):
    assert _translate_reason_fr_to_en(french) == english


@pytest.mark.parametrize("already_english", [
    "Detection of Campylobacter spp",
    "Presence of Listeria monocytogenes",
    "Microbiological non-conformity",
    "Precautionary recall following detection of E. coli STEC",
])
def test_translator_is_idempotent_on_english_input(already_english):
    """`(?i)d[ée]tection` also matches the English "Detection", so with
    `de` made optional the template could re-fire on its own output and
    emit "Detection of of X". The lookahead guard must prevent that."""
    assert _translate_reason_fr_to_en(already_english) == already_english


@pytest.mark.parametrize("french", [
    "Présence de listeria monocytogenes",
    "Présence possible de salmonelles",
    "Contamination possible par Listeria monocytogenes",
    "Détection salmonelle",
    "Non conformite microbiologique",
])
def test_translation_is_stable_under_a_second_pass(french):
    once = _translate_reason_fr_to_en(french)
    assert _translate_reason_fr_to_en(once) == once


def test_translator_still_declines_unknown_french():
    """The translator must never invent meaning — an unrecognised motif is
    returned unchanged for claude-check to handle."""
    unknown = "Corps étrangers dans le produit"
    assert _translate_reason_fr_to_en(unknown) == unknown
