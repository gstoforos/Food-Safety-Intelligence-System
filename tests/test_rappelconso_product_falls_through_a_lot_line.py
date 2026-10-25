# -*- coding: utf-8 -*-
"""RappelConso: when `modeles_ou_references` holds only lot codes, Product
takes the fiche's own heading instead.

THE ROW THAT FOUND THIS. Fiche 23905, published 2026-10-21, promoted to
Recalls on 2026-10-22, Gardel SA / Leader Price:

    modeles_ou_references  "Sachet 1Kg: C120261010A106 - Sachet 1Kg:
                            C120261010A107 - Sachet 2Kg: C120261010B306 -
                            Sachet 2Kg: C120261010B307"
    libelle                "SUCRE ROUX PURE CANNE"

PRODUCT_FIELDS tries `modeles_ou_references` first and has fallen through to
`libelle` only when the chosen field is EMPTY — and a code list is not empty.
The register published the four lot codes as the recalled food.

The gate rule written for this shape the day before,
_product_is_an_identifier_blockers_2026_10_20, asks "does Product contain a
word?" and "Sachet" and "Kg" are words, so it passed. Rather than widen the
gate, the fix walks on to the next PRODUCT_FIELDS candidate — `libelle` is
already in that tuple, so there is no new field and no new request — using
scrapers/_product_text.is_identifier_only to decide.

THE SHAPE IS NOT UNIQUE TO THIS SCRAPER: review/rappelconso_freshness.py
builds Product the same way and is covered below.
"""
from __future__ import annotations

import pytest

from scrapers._product_text import is_identifier_only


# ── The decision ──────────────────────────────────────────────────────────

def test_the_live_api_record_resolves_to_the_heading():
    """The exact API record shape, run through the scraper's own helpers."""
    from scrapers.europe_eu.rappelconso import RappelConsoScraper

    s = RappelConsoScraper()
    rec = {
        "modeles_ou_references":
            "Sachet 1Kg: C120261010A106 - Sachet 1Kg: C120261010A107 - "
            "Sachet 2Kg: C120261010B306 - Sachet 2Kg: C120261010B307",
        "libelle": "SUCRE ROUX PURE CANNE",
        "nom_de_la_marque_du_produit": "GARDEL SA",
    }
    picked, field = s._first(rec, s.PRODUCT_FIELDS)
    assert field == "modeles_ou_references", (
        "fixture drift: this test is about the FIRST field being a lot line")
    assert is_identifier_only(picked), (
        "the lot line is no longer recognised as identifiers only")

    better, better_field = s._first(
        rec, tuple(f for f in s.PRODUCT_FIELDS if f != field))
    assert better_field == "libelle"
    assert better == "SUCRE ROUX PURE CANNE"
    assert not is_identifier_only(better)


def test_a_real_denomination_is_not_overridden():
    """A populated, meaningful first field wins, as it always has.

    The fallback must not reach past a good `modeles_ou_references` to a
    `libelle` that is only the recall's headline.
    """
    from scrapers.europe_eu.rappelconso import RappelConsoScraper

    s = RappelConsoScraper()
    rec = {
        "modeles_ou_references": "Moules de bouchot STG vendues au rayon "
                                 "traditionnel",
        "libelle": "Rappel de moules — analyse non conforme",
    }
    picked, field = s._first(rec, s.PRODUCT_FIELDS)
    assert field == "modeles_ou_references"
    assert not is_identifier_only(picked), (
        "a real product name was taken for a lot line — the fallback would "
        "replace it with the recall headline")


def test_no_candidate_is_usable_and_nothing_is_invented():
    """Every candidate a code list: NOTHING is emitted as the product name.

    `_first_named_product` returns ('', '') and the emit loop sets Product
    empty, so the subcategory fallback fires or the gate refuses the row on
    "Product is empty". What must never happen is a product name appearing
    from nowhere — and, since 2026-10-24, nor may a code list be emitted as
    one.
    """
    from scrapers.europe_eu.rappelconso import RappelConsoScraper

    s = RappelConsoScraper()
    rec = {
        "modeles_ou_references": "Lot 260226",
        "libelle": "Lot n° 312",
    }
    picked, field = s._first(rec, s.PRODUCT_FIELDS)
    assert is_identifier_only(picked), "fixture drift"
    better, better_field = s._first_named_product(
        rec, tuple(f for f in s.PRODUCT_FIELDS if f != field))
    assert (better, better_field) == ("", ""), (
        "a lot line was offered as the replacement product name")


# ── IT STOPPED AT THE FIRST REPLACEMENT (found 2026-10-24) ───────────────

def test_the_walk_does_not_stop_at_a_second_lot_line():
    """Two code-shaped fields in a row must not hide the heading.

    PRODUCT_FIELDS is ordered `modeles_ou_references,
    identification_produits, libelle`. The 2026-10-22 fix called `_first` on
    the remaining candidates — which returns the first NON-EMPTY one — and
    gave up if that was itself an identifier. `identification_produits` is
    where the GTIN/DLC block lives, so on a fiche whose references field is
    also a code list the walk landed there and stopped one field short of
    the heading.

    BOTH ROWS THAT FOUND IT were published Tier 1 on 2026-10-23 with the
    GTIN block as their Product, carrying this scraper's own
    "scraper-flag: Product taken from the fiche title" stamp while the title
    had not been used. Repaired in tools/repair_2026_10_24.py.
    """
    from scrapers.europe_eu.rappelconso import RappelConsoScraper

    s = RappelConsoScraper()

    # fiche 23922 — references field is the placeholder "..", the GTIN block
    # sits in identification_produits, the heading is the food.
    rec = {
        "modeles_ou_references": "..",
        "identification_produits":
            "0207684000000; date limite de consommation; 2026-10-24",
        "libelle": "Blanquette St Jacques",
        "nom_de_la_marque_du_produit": "SK",
    }
    picked, field = s._first(rec, s.PRODUCT_FIELDS)
    assert is_identifier_only(picked), "fixture drift: first field is a name"
    better, better_field = s._first_named_product(
        rec, tuple(f for f in s.PRODUCT_FIELDS if f != field))
    assert better_field == "libelle", (
        f"walk stopped at {better_field!r} ({better[:60]!r}) instead of "
        "reaching the fiche heading")
    assert better == "Blanquette St Jacques"

    # fiche 23914 — references field is a pack list, then the GTIN block.
    rec = {
        "modeles_ou_references":
            "Barquette de 200g, Barquette de 400g, Barquette de 1 kg",
        "identification_produits":
            "3563243012829; 22/10/2026; date limite de consommation; "
            "2026-10-22; |3563243012621; |3563243014625",
        "libelle": "Noix de Saint Jacques de la Baie de Saint Brieuc "
                   "Label Rouge",
    }
    picked, field = s._first(rec, s.PRODUCT_FIELDS)
    assert is_identifier_only(picked), "fixture drift"
    better, better_field = s._first_named_product(
        rec, tuple(f for f in s.PRODUCT_FIELDS if f != field))
    assert better_field == "libelle"
    assert better.startswith("Noix de Saint Jacques")


def test_the_emit_loop_walks_rather_than_taking_the_first_non_empty():
    """The helper must be the one the emit loop calls.

    `_first` is still correct for brand, company, reason and subcategory;
    only Product needs the walk. If the emit loop goes back to `_first` for
    the replacement, the two published rows above come straight back.
    """
    import inspect

    from scrapers.europe_eu import rappelconso as mod

    src = inspect.getsource(mod)
    assert "_first_named_product" in src, (
        "scrapers/europe_eu/rappelconso.py no longer defines or calls "
        "_first_named_product — the Product fallback stops at the first "
        "non-empty candidate again")
    emit = src[src.index("product_was_identifier = False"):]
    emit = emit[:emit.index("distrib, _ = self._first")]
    assert "_first_named_product" in emit, (
        "the Product fallback in the emit loop does not use the walking "
        "helper")
    assert 'which="product-not-an-identifier"' not in emit, (
        "the emit loop still calls _first for the replacement product")


def test_the_two_rappelconso_product_paths_agree():
    """The scraper and the freshness backstop must not disagree.

    review/rappelconso_freshness.py kept the first identifier-only value as
    a last resort while the scraper emits empty, so the same fiche produced
    a code-list Product by one path and an empty one by the other. Both now
    return nothing rather than a code list.
    """
    from review import rappelconso_freshness as fresh
    from scrapers.europe_eu.rappelconso import RappelConsoScraper

    rec = {"modeles_ou_references": "Lot 260226", "libelle": "Lot n° 312"}
    assert fresh._product_from(rec) == "", (
        "the freshness backstop still falls back to a lot line")
    s = RappelConsoScraper()
    assert s._first_named_product(rec, s.PRODUCT_FIELDS) == ("", "")


# ── The fix is wired into the scraper, not only unit-tested ───────────────

def test_the_scraper_consults_the_helper():
    """A helper nobody calls is not a fix.

    2026-10-19's _has_no_letter() was reachable for Company and Brand and
    unreachable for Product, which is the whole reason the sibling test
    tests/test_a_product_name_is_not_a_reference_code.py exists. Same check
    here, at source level, because the scraper's emit loop needs a live HTTP
    response to run end to end.
    """
    import inspect

    from scrapers.europe_eu import rappelconso as mod

    src = inspect.getsource(mod)
    assert "_is_identifier_only" in src or "is_identifier_only" in src, (
        "scrapers/europe_eu/rappelconso.py does not consult "
        "scrapers/_product_text — the lot-line fallback is not wired in")
    assert "scraper-flag: Product taken from the fiche title" in src, (
        "the fallback does not stamp Notes; a heading is a weaker source "
        "than a denomination and the row must say so")


def test_the_freshness_backstop_has_the_same_fallback():
    """review/rappelconso_freshness.py builds Product from the same field.

    It read `noms_des_modeles_ou_references` or the subcategory and never the
    title, so the identical defect could arrive by that path. Swept in the
    same upload.
    """
    import inspect

    try:
        from review import rappelconso_freshness as mod
    except ImportError:                                     # pragma: no cover
        pytest.skip("review/rappelconso_freshness.py not importable here")

    src = inspect.getsource(mod)
    assert "is_identifier_only" in src, (
        "review/rappelconso_freshness.py still takes the model/reference "
        "field without asking whether it is a lot line")
