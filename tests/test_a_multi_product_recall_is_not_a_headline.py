# -*- coding: utf-8 -*-
"""A recall of several products is not a headline, and an FSA alert with no
description still gets a Reason.

WHY (operator pass 2026-10-10)
------------------------------
FSA-PRIN-48-2026 — Greencore, Salmonella, Tier 1 — never published:

1. The FSA scraper copied Reason from the API ``description`` field, which
   this alert did not carry. The publish gate then held the row on
   "Reason is empty".
2. The confirm agent archived it twice (2026-10-08, 2026-10-09) because
   ``_field_integrity_flags`` calls any Product longer than 160 characters
   "a headline". This one is five real product names separated by "; " —
   239 characters — which is how the FSA scraper writes every multi-product
   recall.

Both are held here by shape: a "; "-separated list of real names passes
whatever its total length, and a genuine one-line headline still fails.
"""
from __future__ import annotations

from pipeline.recall_review_agent import _field_integrity_flags
from scrapers.europe_non_eu.fsa_uk import _reason_from_item

HEADLINE_FLAG = "Product looks like a headline, not a product name"

GREENCORE_PRODUCT = (
    "Pinch Pan Poppin’ Chicken Pasanda & Nutty Pilau Rice; Pinch Bangin’ Butter "
    "Chicken & Nutty Pilau Rice; M&S Super Nutty Wholefood with a Soy & Ginger "
    "Dressing; M&S Nutrient Dense Nutty Super Wholefood; M&S Fresh Collection "
    "Pistachio Pesto"
)


def _row(product: str) -> dict:
    return {
        "Source": "FSA (UK)", "Company": "Greencore", "Brand": "—",
        "Product": product, "Pathogen": "Salmonella",
        "Reason": "Greencore recalls several products due to the presence of Salmonella",
        "Country": "United Kingdom", "Region": "Europe",
        "URL": "https://alerts.food.gov.uk/news-alerts/alert/fsa-prin-48-2026",
    }


def test_the_greencore_product_list_is_longer_than_the_old_limit():
    assert len(GREENCORE_PRODUCT) > 160          # the case is real, not trimmed


def test_a_semicolon_list_of_real_products_is_not_a_headline():
    assert HEADLINE_FLAG not in _field_integrity_flags(_row(GREENCORE_PRODUCT))


def test_a_long_single_headline_is_still_a_headline():
    headline = ("Greencore recalls several chilled ready meals and salads sold at "
                "Tesco and Marks and Spencer because of the possible presence of "
                "Salmonella in an ingredient supplied to the factory this week")
    assert len(headline) > 160 and ";" not in headline
    assert HEADLINE_FLAG in _field_integrity_flags(_row(headline))


def test_a_list_hiding_one_headline_sized_item_is_still_flagged():
    long_item = "x" * 170
    assert HEADLINE_FLAG in _field_integrity_flags(_row(f"Pesto 190 g; {long_item}"))


def test_fsa_reason_prefers_description_then_risk_statement_then_title():
    assert _reason_from_item({"description": "D", "title": "T"}) == "D"
    assert _reason_from_item({"problem": [{"riskStatement": "R"}], "title": "T"}) == "R"
    assert _reason_from_item({"problem": {"riskStatement": "R"}, "title": "T"}) == "R"
    title = "Greencore recalls several products due to the presence of Salmonella"
    assert _reason_from_item({"title": title}) == title
    assert _reason_from_item({}) == ""
