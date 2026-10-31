""""sans marque/sans modèle" is not the name of a food.

WHY THIS EXISTS (2026-10-31)
============================
Recalls held this row, promoted 2026-10-29 and published Tier 1:

    Date      2026-10-29
    Source    RappelConso (FR)
    Company   E. Leclerc Chaumont
    Brand     Unbranded
    Product   sans marque/sans modèle
    Pathogen  Listeria monocytogenes
    URL       rappel.conso.gouv.fr/fiche-rappel/23951/interne

Read on the fiche this morning: the dénomination is "Salade de riz
composée", the marque is "Sans marque", the distributor is E. Leclerc
Chaumont (fiche origin "CHAUMONDIS E.LECLERC"), lot 16102026, published
29/10/2026, category Alimentation / Plats préparés et snacks.

So the register told a dashboard reader that a Tier-1 Listeria recall was
about a thing with no brand and no model, while the real name sat in
``libelle``, a field the scraper already reads.

THIS IS THE THIRD INSTANCE OF ONE CLASS
=======================================
``_first_named_product`` exists because of the first two, both recorded in
scrapers/europe_eu/rappelconso.py:

    2026-10-22  fiche 23905  four lot codes published as the product
                             ("Sachet 1Kg: C120261010A106 - …"), real name
                             "SUCRE ROUX PURE CANNE" in libelle
    2026-10-24  fiches 23922 and 23914  the GTIN/DLC block published,
                             because the walk stopped at the first
                             replacement instead of walking on

Both were caught by ``_is_identifier_only``, which counts letters against
digits and unit words. "sans marque/sans modèle" is all letters, so it
cannot be caught that way — and it does not need to be. ``_is_no_brand``
was written for exactly this vocabulary, prefix-plus-separator rule
included (audit 2026-05-18), and it already keeps the same text out of
Brand. The walk now asks it about Product too.

WHAT IT MAY NOT COST
====================
A real food whose name merely begins with one of those words. The token
list is French regulator vocabulary for "no brand named", and the
prefix rule only honours the multi-word tokens, so "Sans-culotte saucisson"
is not a placeholder and must survive.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scrapers.europe_eu.rappelconso import (                   # noqa: E402
    RappelConsoScraper, _is_no_brand)

FIELDS = RappelConsoScraper.PRODUCT_FIELDS


def _walk(rec):
    return RappelConsoScraper.__new__(
        RappelConsoScraper)._first_named_product(rec, FIELDS)


def test_the_fiche_that_found_this_reaches_the_denomination():
    """Fiche 23951's own field shape."""
    rec = {
        "modeles_ou_references": "sans marque/sans modèle",
        "identification_produits": "Lot 16102026",
        "libelle": "Salade de riz composée",
    }
    value, field = _walk(rec)
    assert value == "Salade de riz composée", (value, field)
    assert field == "libelle"


@pytest.mark.parametrize("placeholder", [
    "sans marque/sans modèle",
    "sans marque / sans modele",
    "Sans Marque - Sans Modèle",
    "sans marque",
    "Sans marque",
    "no brand",
    "marque inconnue",
    "aucune marque",
])
def test_a_placeholder_is_walked_past(placeholder):
    rec = {"modeles_ou_references": placeholder,
           "libelle": "Rillettes de porc, 220 g"}
    value, _ = _walk(rec)
    assert value == "Rillettes de porc, 220 g", (placeholder, value)


@pytest.mark.parametrize("name", [
    "Salade de riz composée",
    "Sans-culotte saucisson sec, 200 g",
    "Sandwich jambon-beurre",
    "Boudin à la viande vendu au rayon traditionnel",
    "Tome fraîche de Cantal",
])
def test_a_real_product_name_is_taken_as_written(name):
    rec = {"modeles_ou_references": name, "libelle": "something else"}
    value, field = _walk(rec)
    assert value == name, (name, value, field)
    assert field == "modeles_ou_references"


def test_every_candidate_a_placeholder_leaves_it_empty():
    """So the subcategory fallback fires and the gate can refuse the row —
    publishing a placeholder is the one outcome never wanted."""
    rec = {"modeles_ou_references": "sans marque",
           "identification_produits": "sans modèle",
           "libelle": "no brand"}
    value, field = _walk(rec)
    assert (value, field) == ("", "")


def test_the_helper_the_walk_leans_on_still_recognises_the_pair():
    assert _is_no_brand("sans marque/sans modèle")
    assert not _is_no_brand("Salade de riz composée")


def test_no_published_row_carries_a_placeholder_product():
    rows = json.loads((ROOT / "docs" / "data" / "recalls.json")
                      .read_text(encoding="utf-8"))
    bad = [(r.get("Date"), r.get("Source"), r.get("Product"))
           for r in rows if _is_no_brand(str(r.get("Product") or "").strip())
           and str(r.get("Product") or "").strip()]
    assert not bad, bad
