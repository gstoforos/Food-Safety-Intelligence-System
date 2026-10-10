# -*- coding: utf-8 -*-
"""The FSVO (BLV) listing parse finds RecallSwiss recalls and public warnings.

WHY (audit 2026-10-10)
----------------------
scrapers/europe_non_eu/blv_ch.py was the ten-line GenericLLMScraper shape
and returned nothing from 2026-09-06 on while health read OK_EMPTY. Five
weeks of Swiss notices were missed, among them RecallSwiss 1148 — Le
Grand'Joie pork shank terrine, suspected botulism, two human cases in
Valais — which reached AFTS only as a news article.

Two structural traps are held here, by shape:

1. Every FSVO recall links OFF-HOST to recallswiss.admin.ch. A same-host
   parse drops all of them.
2. A recall's date sits INSIDE its link text. A look-behind date search
   takes the previous entry's date instead.

The titles below are the FSVO's own link texts, read 2026-10-10 on
https://www.blv.admin.ch/blv/de/home/lebensmittel-und-ernaehrung/
rueckrufe-und-oeffentliche-warnungen.html. Only the surrounding markup is
a fixture.
"""
from __future__ import annotations

from datetime import date

import pytest

from scrapers.europe_non_eu import blv_ch
from scrapers.europe_non_eu.blv_ch import (
    BLVScraper, hazard_of, out_of_scope, parse_date, parse_listing, read_title,
)

RS = "https://www.recallswiss.admin.ch/customer-access/#Recalls/"

RECALLS = [
    (1150, "Action Switzerland ruft das Produkt «Cokoc Fruchtgummi Spiegeleier 80 g» "
           "aufgrund von Schimmel zurück", "09.10.2026"),
    (1149, "Lidl Schweiz ruft die Produkte «SOUPE DE CHALET» und «MACARONIS DE CHALET» "
           "wegen einer unterbrochenen Kühlkette zurück", "09.10.2026"),
    (1148, "Le Grand'Joie ruft das Produkt «Terrine de jarret de porc (360g), en bocal "
           "en verre» wegen Verdacht auf Botulismus zurück", "07.10.2026"),
    (1147, "Rapelli Orior Food AG ruft das Produkt «RAP Tartare Manzo 1400g 20x70g Cg» "
           "wegen Nachweis von Escherichia coli zurück", "07.10.2026"),
    (1146, "Kägi Söhne AG ruft das Produkt «Kägi Butterbiscuits» wegen Fremdkörper "
           "(Metallfragmente) zurück", "02.10.2026"),
]

WARNINGS = [
    ("23. September 2026", "Oo7g5dWsbEox",
     "Öffentliche Warnung: Bisphenol A in Bambussprossen-Konserven"),
    ("21. September 2026", "fc3uvVYp-OMK",
     "Öffentliche Warnung: Nicht deklariertes Gluten in Gewürzprodukten der Marke Ezogelin"),
    ("11. September 2026", "QQe-MhuPbath",
     "Öffentliche Warnung: Salmonellen in Bio Crevetten Jumbo und Bio Crevetten "
     "Cocktail von Coop Naturaplan"),
]


def _recall_markup(date_inside_span: bool) -> str:
    items = []
    for n, title, d in RECALLS:
        tail = f"<span class='date'>— {d}</span>" if date_inside_span else f" — {d}"
        items.append(f'<li><a href="{RS}{n}" target="_blank">{title}{tail}</a></li>')
    return "<h2>Rückrufe</h2><ul>" + "".join(items) + "</ul>"


def _warning_markup() -> str:
    cards = []
    for d, nid, title in WARNINGS:
        cards.append(
            f'<div class="card"><p class="meta">{d}</p><h3>{title}</h3>'
            f'<a href="/de/newnsb/{nid}">Mehr über «{title}»</a></div>')
    return "<h2>Öffentliche Warnungen</h2>" + "".join(cards)


def _page(date_inside_span: bool = True) -> str:
    return ("<html><body><nav><a href='/de/home.html'>Home</a> 10.10.2026</nav>"
            + _recall_markup(date_inside_span) + _warning_markup()
            + "<footer><a href='/fr/newnsb/'>x</a></footer></body></html>")


@pytest.mark.parametrize("inside_span", [True, False])
def test_every_recallswiss_recall_is_found_with_its_own_date(inside_span):
    got = {e["url"]: e for e in parse_listing(_page(inside_span))}
    for n, _, d in RECALLS:
        e = got[f"{RS}{n}"]
        dd, mm, yyyy = d.split(".")
        assert e["date"] == f"{yyyy}-{mm}-{dd}", (n, e)


def test_every_warning_is_found_with_its_own_date_and_an_absolute_url():
    got = {e["url"]: e for e in parse_listing(_page())}
    want = {"Oo7g5dWsbEox": "2026-09-23", "fc3uvVYp-OMK": "2026-09-21",
            "QQe-MhuPbath": "2026-09-11"}
    for nid, iso in want.items():
        e = got[f"https://www.blv.admin.ch/de/newnsb/{nid}"]
        assert e["date"] == iso, e


def test_navigation_is_not_an_entry():
    urls = [e["url"] for e in parse_listing(_page())]
    assert len(urls) == len(RECALLS) + len(WARNINGS)
    assert not any(u.endswith("home.html") for u in urls)


def test_the_recall_sentence_gives_company_product_and_hazard():
    t = read_title(RECALLS[2][1] + " — 07.10.2026")
    assert t["company"] == "Le Grand'Joie"
    assert t["product"] == "Terrine de jarret de porc (360g), en bocal en verre"
    assert hazard_of(t["hazard_text"]) == "Clostridium botulinum"

    two = read_title(RECALLS[1][1])
    assert two["product"] == "SOUPE DE CHALET; MACARONIS DE CHALET"


@pytest.mark.parametrize("title,label", [
    (RECALLS[0][1], "Mold"),
    (RECALLS[3][1], "Escherichia coli (generic)"),
    (RECALLS[4][1], "Foreign material (metal)"),
    (WARNINGS[0][2], "Bisphenol A (chemical contaminant)"),
    (WARNINGS[2][2], "Salmonella"),
    ("X ruft das Produkt «Y» wegen STEC zurück", "Shiga toxin-producing E. coli (STEC)"),
    ("X ruft das Produkt «Y» wegen Listerien zurück", "Listeria monocytogenes"),
])
def test_hazard_words_on_the_fsvo_listing_map_to_register_labels(title, label):
    assert hazard_of(title) == label


@pytest.mark.parametrize("title", [RECALLS[1][1], WARNINGS[1][2],
                                   "Falsches Verbrauchsdatum auf Lachs-Sashimi"])
def test_out_of_scope_notices_are_recognised(title):
    assert out_of_scope(title)


def test_in_scope_notices_are_not_called_out_of_scope():
    for _, title, _ in RECALLS[:1] + RECALLS[2:]:
        assert not out_of_scope(title), title


def test_german_month_names_parse():
    assert parse_date("23. September 2026") == "2026-09-23"
    assert parse_date("1. März 2026") == "2026-03-01"
    assert parse_date("31.02.2026") is None


def test_the_scraper_floor_emits_the_botulism_recall_as_a_full_row():
    s = BLVScraper.__new__(BLVScraper)       # no network session needed
    import logging
    s.logger = logging.getLogger("test.blv")
    rows = s._merge_deterministic(_page(), blv_ch.LISTING_URL, [], date(2026, 9, 1))
    by_url = {r.URL: r for r in rows}

    r = by_url[f"{RS}1148"]
    assert r.Date == "2026-10-07"
    assert r.Company == "Le Grand'Joie"
    assert r.Pathogen == "Clostridium botulinum"
    assert r.Reason == "Clostridium botulinum — FSVO recall"      # English
    assert "Verdacht auf Botulismus" in r.Notes      # FSVO's own words kept

    # out of scope: cold chain (1149) and undeclared gluten — never emitted
    assert f"{RS}1149" not in by_url
    assert "https://www.blv.admin.ch/de/newnsb/fc3uvVYp-OMK" not in by_url
    # in scope: 1146/1147/1148/1150 + BPA + Salmonella warnings
    assert len(rows) == 6


def test_the_floor_never_duplicates_what_the_llm_already_found():
    s = BLVScraper.__new__(BLVScraper)
    import logging
    s.logger = logging.getLogger("test.blv")

    class _R:
        URL = f"{RS}1148"
    rows = s._merge_deterministic(_page(), blv_ch.LISTING_URL, [_R()], date(2026, 9, 1))
    assert sum(1 for r in rows if r.URL == f"{RS}1148") == 1
