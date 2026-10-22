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
    """Every candidate a code list: Product keeps the value it had.

    The alternative — reaching for the subcategory, or leaving Product empty
    — is the subcategory-fallback path, which stamps its own flag. What must
    never happen is a product name appearing from nowhere.
    """
    from scrapers.europe_eu.rappelconso import RappelConsoScraper

    s = RappelConsoScraper()
    rec = {
        "modeles_ou_references": "Lot 260226",
        "libelle": "Lot n° 312",
    }
    picked, field = s._first(rec, s.PRODUCT_FIELDS)
    better, _ = s._first(rec, tuple(f for f in s.PRODUCT_FIELDS if f != field))
    assert is_identifier_only(picked) and is_identifier_only(better), (
        "fixture drift")
    # The scraper's rule is "use `better` only if it is NOT identifier-only",
    # so with both unusable the original value stands and the row reaches the
    # publish gate, where the identifier rules refuse it.
    assert picked == "Lot 260226"


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
