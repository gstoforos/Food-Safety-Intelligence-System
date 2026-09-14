# -*- coding: utf-8 -*-
"""GIS (PL) scraper — the listing parse and the hazard mapping.

The scraper this replaces produced ZERO rows in eight months while
reporting no error: it was pointed at the singular article slug
(/web/gis/ostrzezenie-publiczne-dotyczace-zywnosci) rather than the
warnings listing (/web/gis/ostrzezenia), so a fetch succeeded, the LLM
extractor found nothing to extract, and an empty list came back. The
register's own scraper-health called it SILENT_STALE for 102 days.

The cost was the 28.08.2026 Łowicz pesto warning: botulinum toxin, Tier 1,
outbreak-linked, RASFF-notified onward to the Netherlands, missed for
seventeen days.

These tests cover the two pieces that can be exercised without network:
the deterministic listing parse, and the Polish hazard mapping. The
fixture below is built from the live page of 2026-09-14.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scrapers.europe_eu.gis import (  # noqa: E402
    GIS_LISTING,
    parse_listing,
    pathogen_from_title,
)
from scrapers._models import assign_tier, normalize_pathogen  # noqa: E402


# --------------------------------------------------------------------------
# Fixture: the shape gov.pl renders, with the real slugs and dates of
# 2026-09-14. Date before title, as the live cards do it.
# --------------------------------------------------------------------------

def _card(date: str, slug: str, title: str) -> str:
    return (
        '<li class="art-prev">'
        f'  <span class="art-prev__date">{date}</span>'
        f'  <a href="/web/gis/{slug}" class="title">{title}</a>'
        '</li>'
    )


LISTING_HTML = (
    '<html><body><div class="art-prev-list"><ul>'
    + _card("10.09.2026",
            "ostrzezenie-publiczne-dotyczace-zywnosci-alkaloidy-pirolizydynowe-"
            "w-okreslonej-partii-herbatki-ziolowej-z-pokrzywy3",
            "Ostrzeżenie publiczne dotyczące żywności: alkaloidy pirolizydynowe "
            "w określonej partii herbatki ziołowej z pokrzywy")
    + _card("09.09.2026",
            "ostrzezenie-publiczne-dotyczace-zywnosci-wykrycie-obecnosci-bakterii-"
            "listeria-monocytogenes-w-jednej-partii-sera-podpuszczkowego2",
            "Ostrzeżenie publiczne dotyczące żywności: wykrycie obecności bakterii "
            "Listeria monocytogenes w jednej partii sera podpuszczkowego")
    + _card("31.08.2026",
            "ostrzezenie-publiczne-dotyczace-zywnosci-mozliwa-obecnosc-fragmentow-"
            "szkla-w-jednej-partii-sosu-bolonskiego",
            "Ostrzeżenie publiczne dotyczące żywności: Możliwa obecność fragmentów "
            "szkła w jednej partii sosu bolońskiego")
    + _card("28.08.2026",
            "ostrzezenie-publiczne-dotyczace-zywnosci-mozliwa-obecnosc-toksyny-"
            "botulinowej-w-jednej-partii-zielonego-pesto",
            "Ostrzeżenie publiczne dotyczące żywności Możliwa obecność toksyny "
            "botulinowej w jednej partii zielonego pesto")
    + _card("28.08.2026",
            "ostrzezenie-publiczne-dotyczace-zywnosci-wykrycie-obecnosci-bakterii-"
            "listeria-monocytogenes-w-jednej-partii-serdelkow-wieprzowych",
            "Ostrzeżenie publiczne dotyczące żywności: wykrycie obecności bakterii "
            "Listeria monocytogenes w jednej partii serdelków wieprzowych")
    + _card("17.08.2026",
            "ostrzezenie-publiczne-dotyczace-zywnosci-wykrycie-obecnosci-"
            "cereulidyny-w-jednej-partii-mleka-poczatkowego",
            "Ostrzeżenie publiczne dotyczące żywności: Wykrycie obecności "
            "cereulidyny w jednej partii mleka początkowego")
    + '</ul></div></body></html>'
)

PESTO_URL = ("https://www.gov.pl/web/gis/ostrzezenie-publiczne-dotyczace-zywnosci"
             "-mozliwa-obecnosc-toksyny-botulinowej-w-jednej-partii-zielonego-pesto")


@pytest.fixture(scope="module")
def items():
    return parse_listing(LISTING_HTML)


# --------------------------------------------------------------------------
# the listing URL
# --------------------------------------------------------------------------

def test_listing_url_is_the_plural_warnings_page():
    """The whole outage was one wrong slug. Pin it.

    /web/gis/ostrzezenia          the dated listing        <- correct
    /web/gis/ostrzezenie-…        a single article         <- what broke it
    /web/gis/…---archiwum         an index of 2013-2019    <- useless here
    """
    assert GIS_LISTING == "https://www.gov.pl/web/gis/ostrzezenia"
    assert not GIS_LISTING.rstrip("/").endswith("ostrzezenia-publiczne-dotyczace-zywnosci")
    assert "archiwum" not in GIS_LISTING


def test_scraper_points_at_the_listing_and_nothing_else():
    from scrapers.europe_eu.gis import GISScraper
    assert list(GISScraper.INDEX_URLS) == [GIS_LISTING]


# --------------------------------------------------------------------------
# the deterministic parse
# --------------------------------------------------------------------------

def test_every_warning_is_found(items):
    assert len(items) == 6


def test_dates_are_iso_and_taken_from_the_right_card(items):
    assert [i["date"] for i in items] == [
        "2026-09-10", "2026-09-09", "2026-08-31",
        "2026-08-28", "2026-08-28", "2026-08-17",
    ]


def test_the_pesto_warning_is_found(items):
    """The row that exposed the outage."""
    hit = [i for i in items if "pesto" in i["url"]]
    assert len(hit) == 1
    assert hit[0]["date"] == "2026-08-28"
    assert hit[0]["url"] == PESTO_URL
    assert "toksyny botulinowej" in hit[0]["title"]


def test_urls_are_absolute(items):
    assert all(i["url"].startswith("https://www.gov.pl/web/gis/") for i in items)


def test_duplicate_hrefs_collapse():
    doubled = LISTING_HTML.replace("</ul>", "") + LISTING_HTML
    assert len(parse_listing(doubled)) == 6


def test_regional_station_copies_are_not_picked_up():
    """The same warning is republished by every WSSE/PSSE station.

    Counting those would duplicate every Polish row many times over.
    """
    html = LISTING_HTML.replace("</ul>", "") + (
        '<a href="/web/wsse-wroclaw/ostrzezenie-publiczne-dotyczace-zywnosci-'
        'mozliwa-obecnosc-toksyny-botulinowej-w-jednej-partii-zielonego-pesto">x</a>'
        '<a href="/web/psse-monki/ostrzezenie-publiczne-dotyczace-zywnosci-'
        'mozliwa-obecnosc-toksyny-botulinowej-w-jednej-partii-zielonego-pesto">x</a>'
        '</ul>'
    )
    urls = [i["url"] for i in parse_listing(html)]
    assert len(urls) == 6
    assert not any("wsse-" in u or "psse-" in u for u in urls)


def test_a_card_with_no_date_still_yields_the_warning():
    """Fail soft: a missing date is review's problem, not a dropped row."""
    html = ('<ul><li><a href="/web/gis/ostrzezenie-publiczne-dotyczace-zywnosci-'
            'cos-tam">Ostrzeżenie publiczne dotyczące żywności: coś tam</a></li></ul>')
    got = parse_listing(html)
    assert len(got) == 1
    assert got[0]["date"] == ""


def test_empty_page_yields_nothing_and_does_not_raise():
    assert parse_listing("") == []
    assert parse_listing("<html><body><p>nic</p></body></html>") == []


def test_parse_is_pure_string_handling():
    """No network, so this can run in CI and in a sandbox."""
    import inspect
    src = inspect.getsource(parse_listing)
    for forbidden in ("requests", "urlopen", "fetch(", "session"):
        assert forbidden not in src


# --------------------------------------------------------------------------
# the Polish hazard mapping
# --------------------------------------------------------------------------

@pytest.mark.parametrize("title,expected_canonical,expected_tier", [
    ("Możliwa obecność toksyny botulinowej w jednej partii zielonego pesto",
     "Clostridium botulinum", 1),
    ("Podejrzenie obecności jadu kiełbasianego w konserwie",
     "Clostridium botulinum", 1),
    ("Wykrycie obecności cereulidyny w jednej partii mleka początkowego",
     "Cereulide (B. cereus toxin)", 1),
    ("wykrycie obecności bakterii Listeria monocytogenes w serze",
     "Listeria monocytogenes", 1),
    ("wykrycie obecności bakterii Salmonella w jednej partii",
     "Salmonella", 1),
])
def test_polish_hazard_wording_reaches_the_right_tier(
        title, expected_canonical, expected_tier):
    canonical = normalize_pathogen(pathogen_from_title(title)) or ""
    assert canonical == expected_canonical
    assert assign_tier(canonical, 0) == expected_tier


def test_botulinum_is_not_left_as_the_bare_stem():
    """The ordering bug this guards against.

    CORE holds "botulin", which matches "toksyny botulinowej" — but
    normalize_pathogen("botulin") does not resolve, so the row would be
    stamped Tier 3. The Polish map must be consulted first.
    """
    raw = pathogen_from_title(
        "Możliwa obecność toksyny botulinowej w jednej partii zielonego pesto")
    assert raw != "botulin"
    assert normalize_pathogen(raw) == "Clostridium botulinum"


def test_unrecognised_title_returns_empty_not_a_guess():
    assert pathogen_from_title(
        "zmiany organoleptyczne w napoju herbacianym") == ""
    assert pathogen_from_title("") == ""


def test_non_pathogen_hazards_are_still_labelled():
    """Out of Tier-1 scope, but they must not come back blank —
    a blank hazard is indistinguishable from an unread page."""
    assert pathogen_from_title(
        "Możliwa obecność fragmentów szkła w jednej partii sosu bolońskiego")
    assert pathogen_from_title(
        "alkaloidy pirolizydynowe w określonej partii herbatki ziołowej")
