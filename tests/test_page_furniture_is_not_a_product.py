"""A page's CHROME is not the food. Pinned 2026-11-10.

THE FAULT
---------
``pipeline/_bot_wall.py`` has refused challenge-page titles since 2026-09-30:
a fetch that returns "Gcore" or "Just a moment..." told us about OUR READ of
the page, not about the product, so the value is blanked at every gate and at
the writer choke point.

The 2026-11-10 morning sweep found the sibling case, which none of those gates
caught, because the fetch SUCCEEDS. The page is the real notice; the title
extractor simply takes the wrong node — a cookie-consent heading, a navigation
heading, a listing table's header row, a share widget's CSS icon class. Sixteen
rows across the workbook carried one of those as their Product:

  * ``Ihre Datenschutzeinstellungen``  — AGES (AT) consent heading, 13 rows,
    newest 2026-11-06.
  * ``Navigation und Service``         — BVL (DE) nav heading, 3 rows.
  * ``| Date Posted | Reference # | ...`` — an FDA outbreak table HEADER, 2 rows.
  * ``telegram ... icon-calendar ...`` — an SFA (SG) share widget, 2 rows.

WHY IT COST SOMETHING
---------------------
Five of those rows sit under a URL whose recall is PUBLISHED — the notice was
read correctly later and promoted, while the furniture copy stayed in an
archive sheet saying the recall had been thrown away. Two of them, both AGES,
are exactly what turned
``tests/test_a_recall_is_not_both_published_and_rejected.py`` red on main this
morning: a silent archive row contradicting the register.

Every one of those rows also burned reviewer passes. A row whose Product is a
consent banner can never satisfy the publish gate, so reviewer 1, reviewer 2
and the confirmer each spent a fetch on it, every day, for as long as it sat
there.

THE RULE
--------
``is_not_the_notice(value)`` — a challenge-page title OR page furniture — is
the predicate every Product/Company/Brand gate uses. Furniture is kept in its
own set, NOT folded into ``WALL_TITLES``, because a consent heading means the
fetch worked: folding it in would make ``is_bot_wall_html`` discard a readable
notice.

CONSERVATIVE ON PURPOSE
-----------------------
A fixed string matches only as the WHOLE value, so "Gcore protein bar",
"Datenschutz pudding 200 g" and "Navigation biscuits" all survive. The table
shape needs a LEADING pipe and three or more pipes, so "Beef salami | halal"
survives.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from pipeline._bot_wall import (
    is_furniture_title,
    is_not_the_notice,
    is_wall_title,
    is_bot_wall_html,
    scrub_furniture_fields,
)

ROOT = Path(__file__).resolve().parents[1]
XLSX = ROOT / "docs" / "data" / "recalls.xlsx"

#: Every furniture value the 2026-11-10 sweep actually found in the workbook,
#: verbatim. Frozen here rather than read from the workbook so that removing a
#: string from FURNITURE_TITLES fires this test even after the rows are gone.
FOUND_IN_WORKBOOK = [
    "Ihre Datenschutzeinstellungen",
    "Navigation und Service",
    "| Date Posted | Reference # | Pathogen or Cause of Illness | "
    "Product(s) Linked to Illnesses (if any) | Total Case Count ",
    "telegram\n\nicon-calendar\n\n09 Oct 2026\n\nThe European Rapid Alert "
    "System for Food and Feed has issued a notification on the",
]

#: Real Product/Company values that must NOT be touched. The first three are
#: near-misses chosen to break a substring implementation.
MUST_SURVIVE = [
    "Gcore protein bar",
    "Datenschutz pudding 200 g",
    "Navigation biscuits",
    "Beef salami | halal",
    "Cocktail tomatoes",
    "Pine nuts 60g and Salad kernels mix 100g",
    "Rinder-Peperonisalami halal mit Geflügelfleisch 1000 Gramm",
    "Chestnut manju (kuri manju)",
    "Bamboo-charcoal peanuts",
]


@pytest.mark.parametrize("value", FOUND_IN_WORKBOOK)
def test_every_furniture_value_the_sweep_found_is_refused(value):
    assert is_furniture_title(value), (
        f"{value[:60]!r} was found in the workbook as a Product. It is the "
        f"page's chrome, not the food.")
    assert is_not_the_notice(value)


@pytest.mark.parametrize("value", MUST_SURVIVE)
def test_a_real_product_is_never_called_furniture(value):
    assert not is_furniture_title(value), (
        f"{value!r} is a real product value and must survive. A whole-value "
        f"match, not a substring match, is what keeps it.")
    assert not is_not_the_notice(value)


def test_furniture_is_not_folded_into_the_wall_set():
    """A consent heading means the fetch WORKED.

    If furniture were added to WALL_TITLES, is_bot_wall_html would call the
    real notice a challenge page and the fetchers would throw it away.
    """
    for value in FOUND_IN_WORKBOOK:
        assert not is_wall_title(value), (
            f"{value[:40]!r} must not be a WALL title — the fetch succeeded.")
    page = ("<html><head><title>Ihre Datenschutzeinstellungen</title></head>"
            "<body>" + ("Rückruf wegen Salmonellen. " * 200) + "</body></html>")
    assert not is_bot_wall_html(page), (
        "a readable notice whose <title> is a consent heading was discarded "
        "as a challenge page")


def test_the_writer_blanks_furniture_rather_than_publishing_it():
    row = {"Product": "Ihre Datenschutzeinstellungen",
           "Company": "Alnatura Produktions- und Handels GmbH",
           "Brand": "| a | b | c | d |"}
    hit = scrub_furniture_fields(row)
    assert sorted(hit) == ["Brand", "Product"], hit
    assert row["Product"] == "", "an empty Product holds the row at the gate"
    assert row["Brand"] == ""
    assert row["Company"] == "Alnatura Produktions- und Handels GmbH", (
        "a real Company must not be collateral damage")


def test_the_gap_finder_guard_calls_furniture_garbage():
    from pipeline._gap_finder_guards import product_is_garbage
    for value in FOUND_IN_WORKBOOK:
        assert product_is_garbage(value), (
            f"{value[:40]!r} reached Pending as a Product; the gap-finder "
            f"guard must refuse it before it costs a reviewer pass")
    for value in MUST_SURVIVE:
        assert not product_is_garbage(value), value


def test_the_writer_choke_point_still_wires_both_scrubs():
    """The code-side half, so the fix cannot be silently unwired."""
    body = (ROOT / "pipeline" / "merge_master.py").read_text(encoding="utf-8")
    assert "scrub_furniture_fields" in body, (
        "merge_master._write_sheet no longer scrubs page furniture. That is "
        "the only choke point every write route passes through.")
    assert "scrub_fields" in body, "the bot-wall scrub was removed"
    assert "page furniture removed from" in body, (
        "the furniture scrub must leave its own note, distinct from the "
        "bot-wall note: a wall means the fetch was refused, furniture means "
        "the fetch worked and the extractor read the wrong node")


def test_the_title_extractor_refuses_furniture():
    body = (ROOT / "pipeline" / "gap_finder" / "article_fetcher.py").read_text(
        encoding="utf-8")
    assert "is_not_the_notice" in body, (
        "gap_finder/article_fetcher.extract_title went back to refusing only "
        "wall titles, so a consent heading becomes a Product again")


def test_no_published_recall_carries_page_furniture():
    """The data-side half: the register itself."""
    pd = pytest.importorskip("pandas")
    if not XLSX.exists():                                   # pragma: no cover
        pytest.skip("no workbook")
    df = pd.read_excel(pd.ExcelFile(XLSX), "Recalls")
    bad = []
    for _, r in df.iterrows():
        for col in ("Product", "Company", "Brand"):
            v = r.get(col)
            if v is None or str(v) in ("nan", ""):
                continue
            if is_not_the_notice(str(v)):
                bad.append((str(r.get("Date"))[:10], col, str(v)[:50]))
    assert not bad, (
        f"{len(bad)} PUBLISHED row(s) name the page's chrome instead of the "
        f"food: {bad[:5]}")
