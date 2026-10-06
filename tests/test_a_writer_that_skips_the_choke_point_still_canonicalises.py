# -*- coding: utf-8 -*-
"""Every writer canonicalises the label, or the choke point is a fiction.

WHAT THIS CAUGHT (morning-fix 2026-10-06)
=========================================
``merge_master._write_sheet`` is documented as the writer choke point, and
it canonicalises Country and Source against ``COUNTRY_ALIASES`` and
``SOURCE_ALIASES`` on every sheet it writes. Two writers do not go through
it. Both open the workbook with openpyxl and append:

    pipeline/gap_finder/main.py::_append_rows        the 46-country fleet
    pipeline/gap_finder_gr/main.py::_append_rows     Greece

Measured on main at fa15d2e, before the fix:

    Weekly_Rejected   Source 'FSIS'     48 rows

``SOURCE_ALIASES`` has mapped ``"fsis" -> "USDA FSIS"`` since 2026-08, and
``pipeline/gap_finder/countries/us.py`` sets ``authority_short="FSIS"``.
The map was right, the config was reasonable, and the rows were written by
the one path that asked neither.
``tests/test_publish_gate.py::test_usda_fsis_source_label_is_canonical``
is what went red.

This is also, exactly, how the bare ``"Salute"`` label reached **Recalls**
on 2026-10-04 — see the 2026-10-04 block in ``SOURCE_ALIASES``, which
diagnosed it as two hand-written lists that have to agree. That was half
the story: the lists did agree by then, and the row still got through,
because this writer never consulted either of them.

WHY THIS TEST SHAPE
-------------------
Not "does us.py spell FSIS correctly" — that fixes one label on one of
nineteen configs feeding this path. Not "is the data clean" — the data
repair is a one-off and says nothing about the next row. This test takes
each bypassing writer and sends a row with a known-aliased label through
it into a real workbook, which is the only question that matters: does
what comes out the other side carry the canonical label.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

openpyxl = pytest.importorskip("openpyxl")

from pipeline.merge_master import COUNTRY_ALIASES, SOURCE_ALIASES  # noqa: E402

#: A label each map is known to alias, with what it must become.
CASES = [
    ("Source", "FSIS", "USDA FSIS"),
    ("Source", "Salute", "Ministero della Salute (IT)"),
    ("Country", "USA", "United States"),
]


def test_the_alias_maps_still_hold_the_cases_this_test_relies_on():
    """If a map entry is removed, this file must fail loudly rather than
    quietly stop testing anything."""
    for col, raw, want in CASES:
        amap = SOURCE_ALIASES if col == "Source" else COUNTRY_ALIASES
        assert amap.get(raw.lower()) == want, (
            f"{col} alias {raw!r} -> {want!r} is gone from the map; this "
            f"test was written against it")


def _row(col: str, raw: str) -> dict:
    r = {
        "Date": "2026-10-06",
        "Source": "USDA FSIS",
        "Company": "Test Co",
        "Brand": "—",
        "Product": "canonicalisation probe",
        "Pathogen": "Listeria monocytogenes",
        "Reason": "probe",
        "Class": "Recall",
        "Country": "United States",
        "URL": "https://example.invalid/canon-probe",
        "Notes": "test row",
    }
    r[col] = raw
    return r


def _written_back(xlsx: Path, sheet: str, col: str) -> list[str]:
    wb = openpyxl.load_workbook(xlsx)
    ws = wb[sheet]
    rows = list(ws.values)
    hdr = [str(h) for h in rows[0]]
    i = hdr.index(col)
    return [str(r[i]) for r in rows[1:] if r and r[i]]


@pytest.mark.parametrize("col,raw,want", CASES)
def test_fleet_writer_canonicalises(tmp_path, col, raw, want):
    from pipeline.gap_finder import main as gf

    xlsx = tmp_path / "recalls.xlsx"
    appended, _ = gf._append_rows(str(xlsx), "Weekly_Rejected",
                                  [_row(col, raw)], set())
    assert appended == 1
    assert _written_back(xlsx, "Weekly_Rejected", col) == [want], (
        f"pipeline/gap_finder/main._append_rows wrote {col} verbatim; it "
        f"does not go through merge_master._write_sheet, so it has to apply "
        f"the alias map itself")


@pytest.mark.parametrize("col,raw,want", CASES)
def test_greek_writer_canonicalises(tmp_path, col, raw, want):
    from pipeline.gap_finder_gr import main as gr

    xlsx = tmp_path / "recalls.xlsx"
    row = _row(col, raw)
    n = gr._append_rows(str(xlsx), "Weekly_Rejected", list(row.keys()), [row])
    assert n == 1
    assert _written_back(xlsx, "Weekly_Rejected", col) == [want], (
        f"pipeline/gap_finder_gr/main._append_rows wrote {col} verbatim — "
        f"same bypass as the fleet writer")


#: The second symptom of the same bypass. _EMPTY_ID_TEMPLATE in
#: merge_master has matched "not provided" since it was written, and
#: strip_empty_identifier_template says in its own docstring that it is
#: "called from `_write_sheet` AND from the writers that bypass it" — but
#: not from either gap-finder writer, so two Pending rows on main at
#: fa15d2e carried the template into live data (Italian Ministero della
#: Salute: AgriLanga Roccaverano DOP, and BMS Probios maize for popcorn).
#: The one-off repairs of 2026-10-04 and 2026-10-05 each stripped the same
#: template out of other rows by hand; this is the third recurrence.
TEMPLATE_REASONS = [
    ("Presence of Escherichia coli STEC with gene EAE (Recall ID not provided)",
     "Presence of Escherichia coli STEC with gene EAE"),
    ("Presence of tropane alkaloids above the allowed limit "
     "(Recall ID not provided)",
     "Presence of tropane alkaloids above the allowed limit"),
    ("Listeria monocytogenes detected (Recall ID: N/A)",
     "Listeria monocytogenes detected"),
]


@pytest.mark.parametrize("raw,want", TEMPLATE_REASONS)
def test_fleet_writer_strips_the_empty_identifier_template(tmp_path, raw, want):
    from pipeline.gap_finder import main as gf

    xlsx = tmp_path / "recalls.xlsx"
    row = _row("Source", "USDA FSIS")
    row["Reason"] = raw
    appended, _ = gf._append_rows(str(xlsx), "Pending", [row], set())
    assert appended == 1
    assert _written_back(xlsx, "Pending", "Reason") == [want], (
        "pipeline/gap_finder/main._append_rows wrote the extractor's "
        "empty-identifier template into live data; it must call "
        "merge_master.strip_empty_identifier_template, which has matched "
        "this text all along")


@pytest.mark.parametrize("raw,want", TEMPLATE_REASONS)
def test_greek_writer_strips_the_empty_identifier_template(tmp_path, raw, want):
    from pipeline.gap_finder_gr import main as gr

    xlsx = tmp_path / "recalls.xlsx"
    row = _row("Source", "EFET (GR)")
    row["Reason"] = raw
    n = gr._append_rows(str(xlsx), "Pending", list(row.keys()), [row])
    assert n == 1
    assert _written_back(xlsx, "Pending", "Reason") == [want], (
        "same bypass as the fleet writer")


#: The sheets the dashboard, the reports and the alert mailer read, and the
#: queue that feeds them. These are the ones a wrong label mis-counts.
#:
#: Weekly_Rejected and Rejected are deliberately NOT here. They are
#: append-only archives of what each gate refused, written by the bypassing
#: writers over months, and on main at fa15d2e they hold 244 rows with an
#: aliased label:
#:
#:     Rejected         Salute 50, EFET 34, AESAN 33, NCC 21,
#:                      Country 'South Korea' 18
#:     Weekly_Rejected  FSIS 48, Salute 28, NCC 5, AESAN 4, EFET 3
#:
#: Rewriting an archive to match a map written after it is a decision about
#: what the archive is for, not a morning fix, so it is stated here as a
#: number rather than done quietly. The 48 'FSIS' rows were repaired on
#: 2026-10-06 only because test_publish_gate's own banned-label assertion,
#: which predates this file, covers every sheet for that one agency.
LIVE_SHEETS = ("Recalls", "Pending")


def test_the_live_register_carries_no_aliased_label():
    """The data side of the same claim.

    Kept in this file rather than only in test_publish_gate so that the
    writer fix and the data repair are read together: either one alone
    leaves the register wrong again within a day.
    """
    xlsx = ROOT / "docs" / "data" / "recalls.xlsx"
    if not xlsx.exists():                                   # pragma: no cover
        pytest.skip("no workbook")
    wb = openpyxl.load_workbook(xlsx, read_only=True)
    bad = []
    for sheet in LIVE_SHEETS:
        if sheet not in wb.sheetnames:
            continue
        rows = list(wb[sheet].values)
        if not rows:
            continue
        hdr = [str(h) for h in rows[0]]
        for col, amap in (("Source", SOURCE_ALIASES),
                          ("Country", COUNTRY_ALIASES)):
            if col not in hdr:
                continue
            i = hdr.index(col)
            for r in rows[1:]:
                if not r or not r[i]:
                    continue
                raw = str(r[i]).strip()
                canon = amap.get(raw.lower())
                if canon and canon != raw:
                    bad.append((sheet, col, raw, canon))
    assert not bad, (
        f"{len(bad)} live row(s) carry a label the alias map renames: "
        f"{sorted(set(bad))[:8]}")
