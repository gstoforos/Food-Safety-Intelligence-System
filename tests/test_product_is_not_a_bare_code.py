# -*- coding: utf-8 -*-
"""A published Product must name the food, not a model or lot code.

THE BUG SHAPE
-------------
RappelConso's "Modèles ou références" field carries the product name and the
manufacturer's model code in one string, separated by "/":

    Plat cuisiné : Escalope de dinde, crème de gingembre au petit épeautre
    du Ventoux / PR06

When the scraper keeps the wrong half, the register publishes the code. On
2026-10-03 eleven Recalls rows were in that state, including one published
the day before:

    pr06                 fiche 23677   (turkey escalope ready meal)
    Reference 3225164    fiche 23674   (Cokoc gummies — an Action article no.)
    Pot 300g             fiche 23590   (Demeter Egyptian sesame purée)
    Lot 362627           fiche 23465   (jambon persillé)
    dss3001021-00        fiche 22982   (9-month bone-in dry-cured ham)
    dss3005021-00        fiche 22887   (9-month boneless dry-cured ham)
    000751.001           fiche 22966   (lyonnaise beef museau salad)

The same shape was repaired once before, in isolation, when a Product was
literally "//" (row 66, 2026-09-27). Repairing one instance did not stop the
next six, which is why this test exists rather than another note in Notes.

Seven were repaired on 2026-10-03 against their own fiches. FOUR WERE NOT,
and they are pinned in UNREADABLE_FICHES below with the reason: the
RappelConso detail pages render client-side and those six requests came back
as the category listing instead. "I could not read it" is not "it is fine",
so they are recorded as known-bad rather than silently excluded — and the
pin is per-URL, so a NEW bare code cannot hide behind them.

WHAT THIS DOES NOT ASK
-----------------------
A Product may legitimately be short, or be a bare food name: "COPPA",
"Cheese", "Herta". The pattern below requires a digit run, so a word is
never flagged. It also stops at 28 characters: "Brie 500 grams" is thin but
it does name the food, and a descriptive name that happens to carry a weight
is not this bug.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

try:
    import openpyxl
except ImportError:                                         # pragma: no cover
    openpyxl = None

XLSX = Path(__file__).resolve().parents[1] / "docs" / "data" / "recalls.xlsx"

#: A Product that is nothing but an identifier: an optional label word
#: ("lot", "ref", "référence", "modèle", "article", "code", "pr"), a short
#: letter prefix, then a run of digits, then at most a short alphanumeric
#: tail. Requires at least two consecutive digits, so a bare food name
#: ("COPPA", "Cheese") can never match.
BARE_CODE = re.compile(
    r"^(?:(?:r[ée]f(?:[ée]rence|\.)?|lot|mod[eè]le"
    r"|art(?:icle)?|code|pr)\s*[:#-]?\s*)?"
    r"[A-Za-z]{0,4}[\s./-]*\d{2,}[A-Za-z0-9./ -]{0,12}$",
    re.IGNORECASE,
)

MAX_CODE_LEN = 28

#: Known-bad and LEFT ALONE on 2026-10-03 because the fiche could not be
#: read. Each one is a row to repair the next time the page renders, not a
#: shape this test accepts. Removing a line here without repairing the row
#: makes the suite fail, which is the point.
UNREADABLE_FICHES = {
    "https://rappel.conso.gouv.fr/fiche-rappel/22886/interne":
        "dss3001021-00 — 9-month dry-cured ham, Roussaly; the sibling row "
        "on fiche 22982 carries the same code and was repaired, so this one "
        "cannot be inferred from it without guessing which variant it is",
    "https://rappel.conso.gouv.fr/fiche-rappel/22715/interne":
        "Lot 260226 — fiche page returned the category listing on "
        "2026-10-03",
    "https://rappel.conso.gouv.fr/fiche-rappel/22406/interne":
        "10156-plh — fiche page returned the category listing on 2026-10-03",
    "https://rappel.conso.gouv.fr/fiche-rappel/23425/interne":
        "Brie 500 grams — names the food but not which brie; fiche page "
        "returned the category listing on 2026-10-03",
}

SHEETS = ("Recalls",)


def _rows(sheet: str):
    wb = openpyxl.load_workbook(XLSX, read_only=True, data_only=True)
    if sheet not in wb.sheetnames:
        return []
    values = list(wb[sheet].values)
    if not values:
        return []
    hdr = [str(h) for h in values[0]]
    return [dict(zip(hdr, r)) for r in values[1:] if r]


def _is_bare_code(product: str) -> bool:
    p = product.strip()
    return bool(p) and len(p) <= MAX_CODE_LEN and bool(BARE_CODE.match(p))


@pytest.mark.parametrize("sheet", SHEETS)
def test_no_published_product_is_a_bare_code(sheet):
    if openpyxl is None or not XLSX.exists():            # pragma: no cover
        pytest.skip("no workbook")
    offenders = []
    for row in _rows(sheet):
        product = str(row.get("Product") or "")
        if not _is_bare_code(product):
            continue
        url = str(row.get("URL") or "").strip().lower()
        if url in UNREADABLE_FICHES:
            continue
        offenders.append((str(row.get("Date"))[:10],
                          str(row.get("Source")), product, url))
    assert not offenders, (
        f"{len(offenders)} row(s) in {sheet} publish a model / lot / "
        f"article code as the Product:\n  " +
        "\n  ".join(map(str, offenders[:8])) +
        "\n\nRead the product name off the row's own authority page and "
        "keep the code in Notes as [original product: …]. If the page "
        "cannot be read, add the URL to UNREADABLE_FICHES with that reason "
        "rather than widening the pattern."
    )


@pytest.mark.parametrize("sheet", SHEETS)
def test_no_published_product_is_empty(sheet):
    """The degenerate case of the same bug — the "//" row had nothing left
    at all after the split."""
    if openpyxl is None or not XLSX.exists():            # pragma: no cover
        pytest.skip("no workbook")
    offenders = [(str(r.get("Date"))[:10], str(r.get("URL")))
                 for r in _rows(sheet)
                 if not str(r.get("Product") or "").strip()]
    assert not offenders, (
        f"{len(offenders)} row(s) in {sheet} publish no Product at all: "
        f"{offenders[:5]}")


def test_the_pattern_catches_what_it_was_written_for():
    for bad in ("pr06", "Reference 3225164", "Pot 300g", "Lot 362627",
                "dss3001021-00", "dss3005021-00", "000751.001",
                "10156-plh", "Lot 260226", "référence 3225164",
                "réf. 12345", "modèle AB-9981", "article 4471"):
        assert _is_bare_code(bad), f"no longer caught: {bad!r}"


def test_the_pattern_leaves_real_product_names_alone():
    for good in (
        "COPPA",
        "Cheese",
        "Herta",
        "Cut Baumkuchen (plain)",
        "Organic maple syrup, amber",
        "Butcher-style minced veal, 2x100 g, VVF, lot 73728848, use-by "
        "07/10/2026 and 08/10/2026",
        "Cokoc fried-egg shaped gummies, 80 g, Action article 3225164, "
        "best before 24/10/2027 (all lots)",
        "Jambon persillé (parslied ham), lot 362627, use-by 23/09/2026",
        "Smoked chicken quarter",
        "Mt Ossa Australian Spring Water 10 L",
        "Pilos High Protein Pudding, chocolate flavor, 200 g",
    ):
        assert not _is_bare_code(good), f"false positive on {good!r}"
