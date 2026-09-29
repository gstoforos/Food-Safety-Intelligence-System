# -*- coding: utf-8 -*-
"""A date we could not read is not today's date. Swept, not spot-checked.

THE INCIDENT, AND THE MISTAKE MADE FIXING IT
============================================
On 2026-09-28 two FSAI alerts sat in Pending stamped ``Date=2026-09-28`` with
``ScrapedAt=2026-09-28T01:10:40Z`` while FSAI's own list dated both to **18
September**. ``scrapers/europe_eu/fsai.py`` read::

    d = _parse_pubdate(...) or today

That line was fixed, a test was written against that one file, and the tree
was never swept. It was on 2026-09-29 that the pattern was looked for
properly. **Three more live copies were still running**:

    scrapers/north_america/cfia.py   two copies, in the HTML-listing fallback
                                     and in the Atom feed path
    pipeline/url_guardian.py         one, on every gap-finder candidate

The CFIA pair was worse than the FSAI original. Both sites feed a ``d <
cutoff`` test whose whole job is to drop anything outside the collection
window, so a recall whose date could not be read was not merely mis-stamped —
the false ``today`` carried it **past the filter that should have dropped it**.

That is the third time in three days a defect of this shape was repaired one
instance at a time: ``verify_urls --apply`` inheriting an audit's exit code,
then a workflow running pytest without installing it, then this. Hence a
sweep. A rule that lives in one file's test is a rule the next copy does not
have to obey.

WHY THERE IS NO DATA-SIDE VERSION OF THIS TEST
==============================================
``test_a_recall_is_not_both_published_and_rejected`` briefly carried one:
*flag any row whose Date equals the date part of its own ScrapedAt.* It was
withdrawn on 2026-09-29, and the reason matters more than the test did.

That condition is **exactly as true of the system working perfectly as of the
system lying**. A regulator publishes a notice in the morning, the scraper
reads it that afternoon, Date and ScrapedAt agree — and that is the best
outcome this register can produce. On 2026-09-29 it fired on three rows, and
all three were genuine same-day captures: a GIS (PL) Listeria notice parsed
off its own listing card, RappelConso fiche 23644, and an NCC (ZA) hummus
recall. None of the three scrapers contains a today-fallback at all.

The original FSAI rows were undetectable this way for the same reason: nothing
in the workbook distinguished "published today, read today" from "date
unknown, stamped today". The evidence was never in the data. It was in the
source, one grep away, in three files — which is where this test looks.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

#: Directories that produce rows.
SEARCH_DIRS = ("scrapers", "pipeline")

#: Expressions that evaluate to "right now".
_NOW = r"(?:today|_today|today_iso|TODAY|date\.today\(\)|datetime\.utcnow\(\)|" \
       r"datetime\.now\([^)]*\)|dt\.date\.today\(\))"

#: 1. The ``X or today`` fallback idiom — the exact FSAI and url_guardian line.
_OR_FALLBACK = re.compile(r"\bor\s+" + _NOW + r"\s*[\)\],:]")

#: 2. A bare assignment of "now" to a variable that is plainly a row's DATE,
#:    not a cutoff, a stamp or a clock. ``today = date.today()`` is fine and
#:    must stay fine; ``d = today`` in a scrape loop is the bug.
_DATE_VARS = r"(?:d|dd|_d|date_val|pub_date|published|pubdate|row_date)"
_BARE_ASSIGN = re.compile(r"^\s*" + _DATE_VARS + r"\s*=\s*" + _NOW + r"\s*(?:#.*)?$")

#: Lines a human has explicitly marked as a deliberate exception. Nothing uses
#: it today; it exists so that a future genuine case is ARGUED in the source
#: rather than worked around by renaming a variable.
_ALLOW = "unknown-date-is-today: deliberate"


def _python_files():
    for d in SEARCH_DIRS:
        base = ROOT / d
        if not base.exists():                                # pragma: no cover
            continue
        for p in sorted(base.rglob("*.py")):
            if "__pycache__" in p.parts or "_attic" in p.parts:
                continue
            yield p


def _offending_lines(path: Path):
    out = []
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):                    # pragma: no cover
        return out
    for n, line in enumerate(text.splitlines(), 1):
        stripped = line.strip()
        if stripped.startswith("#"):
            continue                    # a comment explaining the bug is not the bug
        if _ALLOW in line:
            continue
        if _OR_FALLBACK.search(line) or _BARE_ASSIGN.match(line):
            out.append((n, stripped[:100]))
    return out


def test_there_is_something_to_sweep():
    """A sweep over an empty set passes for the wrong reason."""
    files = list(_python_files())
    assert len(files) > 50, f"only {len(files)} python files found — is the tree here?"


def test_no_module_substitutes_today_for_an_unknown_publication_date():
    hits = {}
    for p in _python_files():
        bad = _offending_lines(p)
        if bad:
            hits[str(p.relative_to(ROOT))] = bad
    assert not hits, (
        "A date that could not be read is being replaced with today's:\n" +
        "\n".join(f"  {f}:{n}  {src}"
                  for f, lines in sorted(hits.items()) for n, src in lines) +
        "\n\nEvery window filter in this system keys on Date — the daily "
        "sweep's in-window test, the weekly builder's report assignment, the "
        "signal detector's ISO week — and several collectors additionally "
        "compare it against a collection cutoff, so a false 'today' does not "
        "merely mis-date a row, it carries an old one past the filter meant "
        "to drop it. Emit an EMPTY Date instead: publish_blockers holds the "
        "row on 'Date is empty' until enrichment supplies the real one, and "
        "a missing value that blocks is safer than a plausible value that "
        f"lies. If a case is genuinely legitimate, write {_ALLOW!r} on the "
        "line and say why above it.")


def test_the_three_repaired_files_stay_repaired():
    """Named, because a sweep that silently covers nothing is worth nothing."""
    for rel in ("scrapers/europe_eu/fsai.py",
                "scrapers/north_america/cfia.py",
                "pipeline/url_guardian.py"):
        p = ROOT / rel
        if not p.exists():                                   # pragma: no cover
            pytest.skip(f"{rel} not present")
        assert not _offending_lines(p), f"{rel} regressed"


# --------------------------------------------------------------------------
# The other half: a date we CAN read must not be thrown away either
# --------------------------------------------------------------------------

@pytest.mark.parametrize("text,expected", [
    # The two strings that actually reached the register on 2026-09-29.
    ("September 18, 2026", "2026-09-18"),            # FDA
    ("Friday, 18 September 2026", "2026-09-18"),     # FSAI
    # The rest of the English shapes those same agencies render.
    ("18 September 2026", "2026-09-18"),
    ("Sep 18, 2026", "2026-09-18"),
    ("Sept. 18, 2026", "2026-09-18"),
    ("18 Sept 2026", "2026-09-18"),
    ("March 1st, 2026", "2026-03-01"),
    ("1 May 2026", "2026-05-01"),
])
def test_a_month_name_is_a_date(text, expected):
    """Until 2026-09-29 the listing parser could not read any of these."""
    from scrapers._listing import parse_any_date
    assert parse_any_date(text) == expected


@pytest.mark.parametrize("text", [
    "recall 12 items 2026",        # a number and a year, but no month
    "Updated Tuesday 2026",
    "Listeria 2026 recall",
    "Foobar 18, 2026",             # word-shaped, not a month
    "no date here",
    "",
])
def test_a_word_that_is_not_a_month_yields_nothing(text):
    """The month patterns match any word; only real months may resolve."""
    from scrapers._listing import parse_any_date
    assert parse_any_date(text) is None


def test_the_numeric_shapes_did_not_regress():
    from scrapers._listing import parse_any_date
    for text, expected in (("published 2026-08-28", "2026-08-28"),
                           ("28.08.2026", "2026-08-28"),
                           ("28/08/2026", "2026-08-28"),
                           ("2026.08.28", "2026-08-28"),
                           ("5.9.2026", "2026-09-05")):
        assert parse_any_date(text) == expected, text
    assert parse_any_date("31/02/2026") is None


# --------------------------------------------------------------------------
# The writer choke point
# --------------------------------------------------------------------------

def _write_and_read(tmp_path, rows):
    from openpyxl import Workbook
    import pandas as pd
    from pipeline.merge_master import _write_sheet
    wb = Workbook()
    wb.remove(wb.active)
    schema = ["Date", "Source", "Product", "URL", "Notes"]
    _write_sheet(wb, "Recalls", schema, [dict(r) for r in rows])
    f = tmp_path / "w.xlsx"
    wb.save(f)
    return pd.read_excel(f, "Recalls")


def test_the_writer_normalises_a_display_date_to_iso(tmp_path):
    """The FDA and FSAI strings, through the one choke point every sheet uses."""
    out = _write_and_read(tmp_path, [
        {"Date": "September 18, 2026", "Source": "FDA", "Product": "flour",
         "URL": "https://www.fda.gov/x", "Notes": ""},
        {"Date": "Friday, 18 September 2026", "Source": "FSAI (IE)",
         "Product": "chicken", "URL": "https://www.fsai.ie/x", "Notes": ""},
    ])
    assert list(out["Date"].astype(str)) == ["2026-09-18", "2026-09-18"]
    assert all("date-normalised" in n for n in out["Notes"].astype(str))


def test_the_writer_leaves_an_iso_date_alone(tmp_path):
    out = _write_and_read(tmp_path, [
        {"Date": "2026-09-18", "Source": "FDA", "Product": "flour",
         "URL": "https://www.fda.gov/x", "Notes": "keep me"},
    ])
    assert str(out["Date"].iloc[0])[:10] == "2026-09-18"
    assert str(out["Notes"].iloc[0]) == "keep me"


def test_an_unreadable_date_is_cleared_and_kept_in_notes(tmp_path):
    """Cleared so no filter reads it; kept so the row can be repaired by hand."""
    out = _write_and_read(tmp_path, [
        {"Date": "coming soon", "Source": "FDA", "Product": "flour",
         "URL": "https://www.fda.gov/x", "Notes": ""},
    ])
    assert str(out["Date"].iloc[0]) in ("nan", "", "NaT")
    assert "coming soon" in str(out["Notes"].iloc[0])
    assert "date-unreadable" in str(out["Notes"].iloc[0])


def test_the_writer_never_invents_a_date_for_an_empty_cell(tmp_path):
    """Empty must stay empty — that is what the publish gate holds on."""
    out = _write_and_read(tmp_path, [
        {"Date": "", "Source": "FDA", "Product": "flour",
         "URL": "https://www.fda.gov/x", "Notes": ""},
    ])
    assert str(out["Date"].iloc[0]) in ("nan", "", "NaT")


def test_a_cleared_date_is_refused_by_the_publish_gate():
    """The clearing above is only safe because the gate catches the result."""
    from pipeline._publish_gate import publish_blockers
    row = {"Date": "", "Source": "USDA FSIS", "Company": "X Foods Inc.",
           "Product": "Cheddar 200g", "Pathogen": "Listeria monocytogenes",
           "Reason": "Listeria monocytogenes detected", "Class": "Recall",
           "Country": "United States", "Region": "North America",
           "Tier": 1, "Outbreak": 0,
           "URL": "https://www.fsis.usda.gov/recalls-alerts/x-foods-cheddar"}
    assert any("Date" in b for b in publish_blockers(row))
