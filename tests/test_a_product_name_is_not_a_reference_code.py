# -*- coding: utf-8 -*-
"""Product must name the food. A number is not a product, and neither is a
comma-separated list of carton codes.

THE RULE WAS BUILT HALF-WAY
---------------------------
``_has_no_letter()`` went into pipeline/_publish_gate.py on 2026-10-19 with
the note "a firm name is not a number", and it was wired to Company and
Brand only. The next morning the register had published, Tier 1:

    Date     2026-10-19          Source   RappelConso (FR)
    Company  'Filet de Poulet'   Product  '484565'
    URL      https://rappel.conso.gouv.fr/fiche-rappel/23842/interne

'484565' is the fiche's "Modèles ou références" value. Rule 6g asks whether
Product holds a letter **or** a digit, so a bare reference number satisfies
it, and nothing asked for a letter. The row went to subscribers saying the
recalled food was "484565". The fiche's own heading says
"POULET MARINEE THYM CETRON".

THE SECOND SHAPE, from the same morning's batch
-----------------------------------------------
The same field holding SEVERAL codes defeats both the no-letter test and the
bare-code regex in test_product_is_not_a_bare_code.py, whose pattern anchors
on one code and does not allow a comma in its tail:

    'mr10ae, mr41ck'              fiche 23850, PUBLISHED Tier 1 2026-10-19
    'cr31ad, cr30ac, cr20ai, …'   fiche 23849, Pending, 22 codes
    'x6 330g'                     fiche 22635, PUBLISHED since 2026-06-26

So the question the gate has to ask is not "is there a letter" but "is there
a WORD" — 'mr10ae, mr41ck' has eight letters in it and still names nothing.

WHY THREE LETTERS, AND WHY TWO FOR CJK
--------------------------------------
A reference code is letters and digits in short runs (MR10AE, CR31AD, PR06,
x6, 2kg, s/ATM). Every food name in the corpus has a three-letter word in
it. Japanese and Chinese do not: 豆腐 is tofu in two characters, so the
pattern takes two for a logographic script. No row needs that branch today;
it is there so the rule does not misfire on the next CAA or SAMR row.

MEASURED BEFORE WIRING IT IN (whole workbook, 2026-10-20): 3 of 2104
published rows refused — the three above, all repaired in the same upload —
1 of 27 Pending rows (fiche 23849, already refused on "Company is empty"),
6 rows already in the archives, and zero good rows. That measurement is what
earns the rule; the register-wide test below is what keeps it earned.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from pipeline._publish_gate import (
    _product_is_an_identifier_blockers_2026_10_20 as blockers,
    publish_blockers,
)

try:
    import openpyxl
except ImportError:                                         # pragma: no cover
    openpyxl = None

XLSX = Path(__file__).resolve().parents[1] / "docs" / "data" / "recalls.xlsx"


# ── the rule ───────────────────────────────────────────────────────────────

@pytest.mark.parametrize("product", [
    "484565",                                   # fiche 23842, published
    "mr10ae, mr41ck",                           # fiche 23850, published
    "x6 330g",                                  # fiche 22635, published
    "cr31ad, cr30ac, cr20ai, cr20cz",           # fiche 23849, Pending
    "16-138",                                   # FDA import-alert number
    "275g",
    "pr06 12",
    "99 / 88",
])
def test_an_identifier_is_refused(product):
    assert blockers({"Product": product}), f"not refused: {product!r}"


@pytest.mark.parametrize("product", [
    "COPPA",
    "Cheese",
    "Herta",
    "Brie 500 grams",
    "Mt Ossa Australian Spring Water 10 L",
    "Jambon persillé (parslied ham), lot 362627, use-by 23/09/2026",
    "Beef burger patties, 5% fat, Charolais (steaks hachés 5% charolais), "
    "2 x 125 g and 4 x 125 g, lots 26285-8749 and 26285-8820",
    "ESN Designer Protein Bar Dark Chocolate Salted Almond, 45 g",
    "Антильский сыр",
    "Χούμους 200g",
    "ハチミツ入りヨーグルト",
    "豆腐",                                      # two CJK characters IS a word
    "김치",
    "",
    "—",
    "Unbranded",
])
def test_a_real_product_name_passes(product):
    assert not blockers({"Product": product}), f"false positive: {product!r}"


def test_the_rule_is_wired_into_the_gate():
    """A unit-tested helper nobody calls is not a gate. 2026-10-19's
    _has_no_letter() was reachable for Company and Brand and unreachable for
    Product, which is the whole reason this file exists."""
    row = {
        "Date": "2026-10-19", "Source": "RappelConso (FR)",
        "Company": "Albi Boucherie", "Brand": "Albi Boucherie",
        "Product": "484565", "Pathogen": "Salmonella",
        "Reason": "Salmonella found in a marinated chicken preparation",
        "Class": "Voluntary", "Country": "France", "Region": "Europe",
        "Tier": 1, "Outbreak": 0,
        "URL": "https://rappel.conso.gouv.fr/fiche-rappel/23842/interne",
        "Notes": "",
    }
    problems = publish_blockers(row)
    assert any("contains no letter" in p for p in problems), problems
    row["Product"] = "mr10ae, mr41ck"
    assert any("identifier codes only" in p for p in publish_blockers(row)), \
        publish_blockers(row)


# ── the register ───────────────────────────────────────────────────────────

def _rows(sheet: str):
    wb = openpyxl.load_workbook(XLSX, read_only=True, data_only=True)
    if sheet not in wb.sheetnames:
        wb.close()
        return []
    values = list(wb[sheet].values)
    wb.close()
    if not values:
        return []
    hdr = [str(h) for h in values[0]]
    return [dict(zip(hdr, r)) for r in values[1:] if r]


@pytest.mark.parametrize("sheet", ("Recalls", "Pending", "Weekly_Review"))
def test_no_live_row_publishes_an_identifier_as_its_product(sheet):
    if openpyxl is None or not XLSX.exists():            # pragma: no cover
        pytest.skip("no workbook")
    offenders = [(str(r.get("Date"))[:10], str(r.get("Source")),
                  str(r.get("Product"))[:40], str(r.get("URL")))
                 for r in _rows(sheet) if blockers(r)]
    assert not offenders, (
        f"{len(offenders)} row(s) in {sheet} carry a reference code where the "
        f"product name belongs:\n  " + "\n  ".join(map(str, offenders[:8])) +
        "\n\nRead the food name off the notice's own heading and keep the "
        "code in Notes as [original product: …]. Do not widen the pattern to "
        "let the row through."
    )
