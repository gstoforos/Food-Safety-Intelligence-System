"""A firm whose name contains ’ still has a name.

WHY THIS EXISTS (2026-10-31)
============================
Pending held this row, collected 2026-10-30 and URL-confirmed twice:

    Date      2026-10-30
    Source    FSANZ
    Company   <empty>
    Product   Maria’s Pasta – Various Products
    Pathogen  Foreign Matter
    URL       foodstandards.gov.au/food-recalls/recall-alert/
              marias-pasta-various-products

It is a real, in-scope recall — read on the FSANZ alert this morning:
supplier "Maria's Pasta", six Bolognese pasta and sauce lines in 450 g and
500 g packs with use-by dates 30 Oct – 04 Nov 26, sold through independent
retailers including IGA in Victoria and online, Problem "The presence of
foreign matter (metal).", recall date 30 October 2026 — and it could not
publish, because `publish_blockers` refuses a row with an empty Company.

THE CAUSE IS ONE CHARACTER CLASS
================================
pipeline/official_feeds/sources/australia.py read the firm two ways and
both are regexes over the same character class:

    detail page   ([A-Z][\\w&.''\\-]*(?:\\s+[A-Z0-9][\\w&.''\\-]*){0,5})
                  \\s+is\\s+(?:recalling|conducting)
    list title    ^([A-Z][\\w &.''\\-]{1,80}?)\\s*[-–—]

That `''` is TWO ASCII APOSTROPHES — redundant inside a character class,
and plainly written where a typographic apostrophe was meant. FSANZ writes
its titles with U+2019: "Maria’s Pasta – Various Products". So the title
fallback stopped at the ’ and matched nothing, the detail page's modern
layout has a "Supplier" field rather than "<Name> is recalling" prose, and
Company came out empty.

Measured the same morning, the class is identical in ELEVEN places across
FOUR collectors — every module that reads a firm name out of an
English-language regulator's prose:

    pipeline/official_feeds/sources/australia.py     2
    pipeline/official_feeds/sources/ireland.py       2
    pipeline/official_feeds/sources/us_fsis.py       2
    pipeline/official_feeds/sources/new_zealand.py   5

FSANZ, FSAI, USDA FSIS and MPI all write typographic apostrophes, and
apostrophes are ordinary in firm names — O’Brien’s, Nature’s Own,
Maria’s Pasta, Trader Joe’s. Every one of those eleven regexes silently
dropped such a firm. The class now admits ' and ’ and ‘ in all of them.

WHY IT WAS NOT CAUGHT BY THE 2026-10-29 PASS
============================================
That pass repaired the FSANZ Bomberry row of 2026-10-28 — Company AND
Brand both set to the recall title — and fixed the cause in
scrapers/oceania/fsanz.py, whose title split has no " - " to work with.
"Bomberry Various Products - 150g" is pure ASCII, so the same shape in
pipeline/official_feeds/sources/australia.py was invisible: two collectors
read FSANZ, the repair swept one of them, and the next FSANZ recall with a
curly apostrophe landed in the other.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

COLLECTORS = (
    "pipeline/official_feeds/sources/australia.py",
    "pipeline/official_feeds/sources/ireland.py",
    "pipeline/official_feeds/sources/us_fsis.py",
    "pipeline/official_feeds/sources/new_zealand.py",
)

#: The redundant ASCII pair that was written where ’ was meant.
ASCII_ONLY_PAIR = ".''\\-"


@pytest.mark.parametrize("path", COLLECTORS)
def test_no_firm_name_class_is_ascii_apostrophe_only(path):
    src = (ROOT / path).read_text(encoding="utf-8")
    assert ASCII_ONLY_PAIR not in src, (
        f"{path} still has a firm-name character class that admits only the "
        f"ASCII apostrophe. A regulator writing \"Maria’s Pasta\" loses "
        f"its Company, the row is refused for an empty Company, and a real "
        f"recall never publishes")


@pytest.mark.parametrize("path", COLLECTORS)
def test_every_firm_name_class_admits_both_apostrophes(path):
    """Every class that names the ASCII ' also names ’."""
    src = (ROOT / path).read_text(encoding="utf-8")
    classes = re.findall(r"\[\\w[^\]]*\]", src)
    assert classes, f"{path}: no firm-name character class found"
    offenders = [c for c in classes if "'" in c and "’" not in c]
    assert not offenders, (path, offenders)


def test_the_title_that_found_this_yields_the_firm():
    """australia.py's list-title fallback, on FSANZ's own title."""
    import pipeline.official_feeds.sources.australia as au

    src = Path(au.__file__).read_text(encoding="utf-8")
    m = re.search(r'r"\^\(\[A-Z\]\[([^\]]*)\]\{1,80\}\?\)', src)
    assert m, "the list-title fallback regex moved — re-read australia.py"
    cls = m.group(1)
    pattern = re.compile(r"^([A-Z][" + cls + r"]{1,80}?)\s*[-–—]")
    got = pattern.match("Maria’s Pasta – Various Products")
    assert got and got.group(1) == "Maria’s Pasta", got


@pytest.mark.parametrize("title,firm", [
    ("Maria’s Pasta – Various Products", "Maria’s Pasta"),
    ("Bomberry Various Products - 150g", "Bomberry Various Products"),
    ("O’Brien’s Fine Foods — Hummus 200g",
     "O’Brien’s Fine Foods"),
    ("Nature‘s Own Dairy – Yoghurt 1kg", "Nature‘s Own Dairy"),
])
def test_apostrophes_of_every_shape_survive(title, firm):
    pattern = re.compile(
        r"^([A-Z][\w &.'’‘\-]{1,80}?)\s*[-–—]")
    m = pattern.match(title)
    assert m and m.group(1) == firm, (title, m.group(1) if m else None)


def test_the_collectors_still_import():
    """A character class edited by hand must not break the module."""
    import importlib
    for path in COLLECTORS:
        mod = path.replace("/", ".")[:-3]
        importlib.import_module(mod)
