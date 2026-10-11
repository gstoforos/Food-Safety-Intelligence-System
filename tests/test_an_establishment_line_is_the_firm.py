# -*- coding: utf-8 -*-
"""FSA Food Alerts for Action name the establishment as a product line.

FSA-FAFA-03-2026 (2026-10-10, Salmonella in raw shell eggs) arrived with
Product "The establishment: Yasensvit Ltd, Kyiv, Ukraine; Raw shell-on eggs
- Product Code 3UA1012066" and Company empty, so the publish gate held a
Tier-1 alert in Pending. The establishment line is the firm.
"""
from __future__ import annotations

from pipeline.official_feeds.sources.uk import _firm, _product

FAFA = {
    "title": "Food Alert for Action: possible Salmonella in raw shell eggs",
    "productDetails": [
        {"productName": "The establishment: Yasensvit Ltd, Kyiv, Ukraine"},
        {"productName": "Raw shell-on eggs - Product Code 3UA1012066"},
    ],
}


def test_the_establishment_becomes_the_firm():
    assert _firm(FAFA) == "Yasensvit Ltd, Kyiv, Ukraine"


def test_the_establishment_line_is_not_a_product():
    assert _product(FAFA) == "Raw shell-on eggs - Product Code 3UA1012066"


def test_a_named_reporting_business_still_wins():
    item = dict(FAFA, reportingBusiness={"commonName": "Acme Foods"})
    assert _firm(item) == "Acme Foods"


def test_an_ordinary_recall_is_unchanged():
    item = {"title": "Greencore recalls several products",
            "productDetails": [{"productName": "Pesto 115 g"}, {"productName": "Salad 285 g"}]}
    assert _firm(item) == "Greencore"
    assert _product(item) == "Pesto 115 g; Salad 285 g"
