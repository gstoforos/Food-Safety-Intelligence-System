# -*- coding: utf-8 -*-
"""A lot line is not a product name, even when it contains words.

pipeline/_publish_gate._product_is_an_identifier_blockers_2026_10_20 asks
"does Product contain a word?". That refuses '484565' and 'mr10ae, mr41ck'.
It does NOT refuse this, published 2026-10-22 from RappelConso fiche 23905:

    Product  'sachet 1kg: c120261010a106 - sachet 1kg: c120261010a107 -
              sachet 2kg: c120261010b306 - sachet 2kg: c120261010b307'

because "sachet" and "kg" ARE words. The packaging vocabulary satisfies the
word test while the food is still missing. The fiche's own heading says
"SUCRE ROUX PURE CANNE" — pure cane brown sugar.

scrapers/_product_text.is_identifier_only draws the distinction the word test
cannot: remove the closed set of packaging nouns, units, label keywords and
codes, and a real product name always leaves a content word behind while a
lot line leaves nothing.

WHERE IT IS WIRED. Into the SOURCE, scrapers/europe_eu/rappelconso.py, which
now walks on to the next PRODUCT_FIELDS candidate (`libelle`, the fiche
heading) when the field it picked is a bare identifier.

WHY IT IS NOT IN publish_blockers. Measured over the whole 2026-10-22
workbook, it flags 7 published rows: fiche 23905, repaired in this upload,
and six from May-July 2026 whose fiches RappelConso no longer serves —
22886 was opened on 2026-10-22 and redirects to the current category
listing. Wiring the rule into the gate would hold
test_a_product_name_is_not_a_reference_code's register-wide assertion RED,
and the only ways to green it would be to invent six product names or to
delete six published rows. Both are worse than the defect. The six are named
in tools/repair_2026_10_22.py section C and in that morning's message as an
operator decision.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from scrapers._product_text import is_identifier_only

try:
    import openpyxl
except ImportError:                                         # pragma: no cover
    openpyxl = None

XLSX = Path(__file__).resolve().parents[1] / "docs" / "data" / "recalls.xlsx"


IDENTIFIER_ONLY = [
    # fiche 23905 — the row that found this
    "sachet 1kg: c120261010a106 - sachet 1kg: c120261010a107 - "
    "sachet 2kg: c120261010b306 - sachet 2kg: c120261010b307",
    "Sachet 1Kg: C120261010A106",
    "Lot 260226",
    "Lot n° 312",
    "Lot g22014; barcode 6936489101348",
    "dss3001021-00",
    "10156-plh",
    "référence 601849",
    "mr10ae, mr41ck",
    "484565",
    "x6 330g",
    "275g",
    "2 x 125 g",
    "boite 500g lot 11061",
    "batch 11061, best before 03.01.2027",
    "ref. 3225164",
    # ── THE DATE LABEL SPELLED OUT (2026-10-24) ──────────────────────────
    # Both of these were PUBLISHED Tier 1 on 2026-10-23, verbatim, as the
    # Product of a RappelConso scallop recall. "limite" and "consommation"
    # were the content words that satisfied the sweep — the same escape
    # "Sachet" made through the publish gate's word test, one layer up.
    # fiche 23922 (heading "Blanquette St Jacques"):
    "0207684000000; date limite de consommation; 2026-10-24",
    # fiche 23914 (heading "Noix de Saint Jacques de la Baie de Saint
    # Brieuc Label Rouge"):
    "3563243012829; 22/10/2026; date limite de consommation; 2026-10-22; "
    "|3563243012621; |3563243014625",
    # The same shape in the other languages the register's scrapers read.
    # None of these has been seen in the register; they are here so the
    # next one cannot be the first.
    "Packung 125 g, mindestens haltbar bis 03.11.2026",
    "MHD 03.11.2026 — Beutel 500 g",
    "lotto 596, da consumarsi preferibilmente entro 31/12/2028",
    "lote 387/21096 1809, consumir antes de 20/11/2026",
    "te gebruiken tot 23-10-2026, zak 250 g",
]

A_REAL_NAME = [
    # The six rows this must not touch, as they will read after repair.
    "Pure cane brown sugar (SUCRE ROUX PURE CANNE), 1 kg and 2 kg bags, "
    "lots C120261010A106, C120261010A107",
    "Sardine fillet packed by the store",
    "Bouchot mussels STG sold at the service counter",
    "Ground beef prepared to order",
    "Salami, three variants, 250 g each; lot 11061",
    # Ordinary register values across scripts and languages.
    "COPPA",
    "Cheese",
    "Brie 500 grams",
    "Mt Ossa Australian Spring Water 10 L",
    "Jambon persillé (parslied ham), lot 362627, use-by 23/09/2026",
    "Beef burger patties, 5% fat, Charolais, 2 x 125 g, lots 26285-8749",
    "Rainbow Trout Fillets",
    "Chicken thigh 180 g 5 kg",
    "foie de poulet",
    "Антильский сыр",
    "Χούμους 200g",
    "ハチミツ入りヨーグルト",
    "豆腐",
    "김치",
    # A single bag of something named is a name, not a lot line.
    "sachet de sucre roux",
    "pot de rillettes 180g",
    # The two 2026-10-23 rows as they read after repair_2026_10_24.
    "Scallop blanquette (Blanquette St Jacques); GTIN 0207684000000; "
    "use-by 24/10/2026",
    "King scallops from the Bay of Saint-Brieuc, Label Rouge (Noix de "
    "Saint Jacques de la Baie de Saint Brieuc Label Rouge); 200 g, 400 g "
    "and 1 kg trays; GTINs 3563243012829, 3563243012621, 3563243014625; "
    "use-by 22/10/2026",
    # The other direction for the 2026-10-24 vocabulary: real product
    # names in the languages whose date-label words were added. Each one
    # must survive, label words and all.
    "Nautica Regenbogen Forellenfilets Natur 125 g, mindestens haltbar "
    "bis 03.11.2026",
    "Milbona High Protein Pudding Schokolade, 200 g, MHD 12.11.2026",
    "Salsa tartufata in vaso di vetro, da consumarsi entro 31/12/2028",
    "Mejillón de Chile cocido, consumir antes de 20/11/2026",
    "Plakjes ossenworst, te gebruiken tot 23-10-2026",
    "Minste houdbaarheid: Goudse jong belegen kaas 500 g",
]


@pytest.mark.parametrize("product", IDENTIFIER_ONLY)
def test_a_lot_line_is_recognised(product):
    assert is_identifier_only(product) is True, (
        f"not recognised as identifiers only: {product!r}")


@pytest.mark.parametrize("product", A_REAL_NAME)
def test_a_real_name_is_left_alone(product):
    assert is_identifier_only(product) is False, (
        f"false positive — this names a food: {product!r}")


def test_empty_is_not_an_identifier():
    """Empty is a different defect and every caller checks it separately.

    Returning True would send a caller looking for a replacement field it
    does not need, and in the scraper would skip the subcategory fallback.
    """
    for empty in ("", "   ", None):
        assert is_identifier_only(empty) is False


def test_the_word_test_alone_would_have_missed_fiche_23905():
    """The reason this module exists, pinned.

    If pipeline/_publish_gate's word test ever grows to catch this shape, this
    test fails and the comment above can be deleted. Until then it records
    that the two tests are not the same test.
    """
    from pipeline._publish_gate import (
        _product_is_an_identifier_blockers_2026_10_20 as word_test)
    lot_line = IDENTIFIER_ONLY[0]
    assert not word_test({"Product": lot_line}), (
        "the gate's word test now catches fiche 23905's shape — revisit "
        "whether scrapers/_product_text is still needed")
    assert is_identifier_only(lot_line)


@pytest.mark.parametrize("sheet", ("Recalls", "Pending", "Weekly_Review"))
def test_the_count_of_affected_live_rows_is_known(sheet):
    """No NEW row may acquire this shape.

    Pinned to the measurement taken on 2026-10-22 after the repair: the six
    unrepairable May-July rows in Recalls and nothing anywhere else. If this
    fails with a HIGHER count, a scraper has started writing lot lines into
    Product again and the source fix regressed. If it fails with a LOWER
    count, the six were repaired or removed — lower the number and say which
    in the commit message.
    """
    if openpyxl is None or not XLSX.exists():               # pragma: no cover
        pytest.skip("no workbook")
    expected = {"Recalls": 6, "Pending": 0, "Weekly_Review": 0}[sheet]
    wb = openpyxl.load_workbook(XLSX, read_only=True, data_only=True)
    if sheet not in wb.sheetnames:                          # pragma: no cover
        wb.close()
        pytest.skip(f"no {sheet} sheet")
    values = list(wb[sheet].values)
    wb.close()
    hdr = [str(h) for h in values[0]]
    rows = [dict(zip(hdr, r)) for r in values[1:] if r]
    offenders = [
        (str(r.get("Date"))[:10], str(r.get("Source")),
         str(r.get("Product"))[:46], str(r.get("URL"))[:70])
        for r in rows if is_identifier_only(str(r.get("Product") or ""))
    ]
    assert len(offenders) == expected, (
        f"{sheet}: expected {expected} row(s) whose Product is identifiers "
        f"only, found {len(offenders)}:\n  "
        + "\n  ".join(map(str, offenders[:10]))
        + "\n\nRead the food name off the notice's own heading and keep the "
          "codes in the value, with the original in Notes as "
          "[original product: …]. Do not widen "
          "scrapers/_product_text._PACKAGING_WORDS to let a row through."
    )
