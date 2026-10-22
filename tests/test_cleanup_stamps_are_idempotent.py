# -*- coding: utf-8 -*-
"""A row carries each "[needs cleanup: …]" stamp once, however many writers
touch it.

pipeline/promote_gate_passing.py's own comment, written 2026-10-21, says of
itself and merge_master._write_sheet:

    "both are idempotent, so a row promoted here and rewritten there
     carries one stamp, not two"

merge_master IS (`if _stamp not in _notes`). promote_gate_passing was not —
three bare `r["Notes"] += " [needs cleanup: …]"` with no guard. A row that
passes through that function more than once (promoted, rewritten by the
hourly Pending->Recalls merge, promoted again after an offline enrichment)
collected a stamp on each pass.

MEASURED on the 2026-10-22 workbook. Recalls row 7, RappelConso fiche 23905,
Gardel SA brown sugar, promoted that morning:

    Notes  "leader price [needs cleanup: Reason not in English]
            [offline-enrich 2026-10-22: Pathogen '' -> 'Foreign material
             (plastic)' …] [promote 2026-10-22: status 'pending_enrichment'
             ->'pending' — passes every publish-gate rule]
            [needs cleanup: Reason not in English]"

The cost is not only noise. The morning pass reads what is stamped and
discharges a stamp by removing it; a second copy left behind makes a
translated row look untranslated the next morning, and the Notes field is
capped at 300 characters by several writers, so a duplicate stamp evicts the
provenance the row is supposed to carry.
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

STAMPS = (
    "[needs cleanup: Product not in English]",
    "[needs cleanup: Company/Brand not in English]",
    "[needs cleanup: Reason not in English]",
)


def _stamp_rows_twice(row):
    """Run promote_gate_passing's stamping over a row twice.

    Calls the real function, so the test fails if the guard is removed or if
    a fourth stamp is added later without one.
    """
    from pipeline.promote_gate_passing import stamp_language_cleanup

    out = dict(row)
    stamp_language_cleanup(out)
    stamp_language_cleanup(out)
    return str(out.get("Notes") or "")


def test_the_writer_stamps_once_not_twice():
    """A French Reason and a French Product, promoted twice."""
    row = {
        "Date": "2026-10-21", "Source": "RappelConso (FR)",
        "Company": "Gardel SA", "Brand": "Gardel SA",
        "Product": "sucre roux pure canne vendu en sachet",
        "Pathogen": "Foreign material (plastic)",
        "Reason": "Suspicion de présence de corps étrangers en plastique "
                  "blanc",
        "Class": "Voluntary", "Country": "France", "Region": "Europe",
        "Tier": 3, "Outbreak": 0,
        "URL": "https://rappel.conso.gouv.fr/fiche-rappel/23905/interne",
        "Notes": "leader price", "Status": "pending_enrichment",
    }
    notes = _stamp_rows_twice(row)
    for stamp in STAMPS:
        assert notes.count(stamp) <= 1, (
            f"{stamp} appears {notes.count(stamp)} times after two passes "
            f"through the promoter:\n  {notes}")


def test_an_already_stamped_row_is_not_stamped_again():
    """The common live case: the hourly merge stamped it, the promoter runs."""
    from pipeline.promote_gate_passing import stamp_language_cleanup

    row = {
        "Date": "2026-10-21", "Source": "RappelConso (FR)",
        "Company": "Gardel SA", "Brand": "Gardel SA",
        "Product": "sucre roux pure canne",
        "Pathogen": "Foreign material (plastic)",
        "Reason": "Suspicion de présence de corps étrangers",
        "Class": "Voluntary", "Country": "France", "Region": "Europe",
        "Tier": 3, "Outbreak": 0,
        "URL": "https://rappel.conso.gouv.fr/fiche-rappel/23905/interne",
        "Notes": "leader price [needs cleanup: Reason not in English]",
        "Status": "pending_enrichment",
    }
    out = stamp_language_cleanup(dict(row))
    notes = str(out.get("Notes") or "")
    assert notes.count("[needs cleanup: Reason not in English]") == 1, notes


@pytest.mark.parametrize("sheet", ("Recalls", "Pending", "Weekly_Review"))
def test_no_live_row_carries_a_duplicated_stamp(sheet):
    if openpyxl is None or not XLSX.exists():               # pragma: no cover
        pytest.skip("no workbook")
    wb = openpyxl.load_workbook(XLSX, read_only=True, data_only=True)
    if sheet not in wb.sheetnames:                          # pragma: no cover
        wb.close()
        pytest.skip(f"no {sheet} sheet")
    values = list(wb[sheet].values)
    wb.close()
    hdr = [str(h) for h in values[0]]
    if "Notes" not in hdr:                                  # pragma: no cover
        pytest.skip("no Notes column")
    i_notes, i_url = hdr.index("Notes"), hdr.index("URL")
    offenders = []
    for r in values[1:]:
        if not r:
            continue
        notes = str(r[i_notes] or "")
        for stamp in STAMPS:
            if notes.count(stamp) > 1:
                offenders.append(
                    (str(r[i_url])[:72], stamp, notes.count(stamp)))
    assert not offenders, (
        f"{len(offenders)} row(s) in {sheet} carry the same cleanup stamp "
        f"more than once:\n  " + "\n  ".join(map(str, offenders[:8]))
        + "\n\nA writer is appending without checking. Guard it the way "
          "merge_master._write_sheet does (`if _stamp not in _notes`)."
    )


def test_every_stamping_writer_checks_before_appending():
    """Source-level guard: no bare `+=` of a cleanup stamp anywhere.

    The defect was a comment asserting idempotence that one of the two
    writers did not have. A reader of either file should not have to take
    that on trust, and the next writer added should not be able to
    reintroduce it quietly.
    """
    root = Path(__file__).resolve().parents[1]
    bare = re.compile(
        r"""\[\s*"Notes"\s*\]\s*\+=\s*["'][^"']*needs cleanup""")
    offenders = []
    for path in sorted((root / "pipeline").rglob("*.py")):
        text = path.read_text(encoding="utf-8", errors="replace")
        for n, line in enumerate(text.splitlines(), start=1):
            # Comments describing the defect are not the defect. The note in
            # promote_gate_passing.stamp_language_cleanup quotes the old
            # line verbatim, which is exactly what it should do.
            if line.lstrip().startswith("#"):
                continue
            if bare.search(line):
                offenders.append(f"{path.relative_to(root)}:{n}: "
                                 f"{line.strip()[:90]}")
    assert not offenders, (
        "a cleanup stamp is appended without checking whether it is already "
        "there:\n  " + "\n  ".join(offenders)
        + "\n\nUse a guarded helper, as promote_gate_passing._stamp_once "
          "and merge_master._write_sheet do."
    )
