"""nan is truthy, and it killed a promoter run. Pinned 2026-11-10.

WHAT HAPPENED
-------------
``pipeline.promote_gate_passing --apply`` died on 2026-11-10 while promoting
seven operator-verified rows:

    File "pipeline/merge_master.py", line 2106, in mark_rejected_with_counter
        raw = (row.get("RejectedBy") or "").strip()
    AttributeError: 'float' object has no attribute 'strip'

The promoter reads Pending with ``pd.read_excel(...).to_dict("records")``,
which gives ``float('nan')`` for every EMPTY cell. And ``nan`` is truthy, so

    (row.get("RejectedBy") or "")

never reaches its fallback: it hands a float to ``.strip()``. The run died
whole — nothing was written, and the seven rows it was in the middle of
promoting stayed in Pending. A row with an empty RejectedBy is the ORDINARY
case, so this was a crash waiting on which rows happened to pass the gate
that morning.

Six sites in merge_master.py shared the shape, all of them reading
pandas-sourced Pending rows:

    _write_sheet's dedup scan          Status
    mark_rejected_with_counter         RejectedBy, Notes
    the rejected-row sweep             Status, RejectedBy, ScrapedAt

WHY NOT JUST str()
------------------
Most of this module writes ``str(row.get(...) or "")``, which does not crash
— and for RejectedBy and Status it would be a worse bug than the crash,
because ``str(float('nan'))`` is the string ``"nan"``. Written into
RejectedBy, "nan" is a REVIEWER NAME: the delete threshold in
``mark_rejected_with_counter`` drops a row once two DIFFERENT reviewers have
rejected it, so a phantom "nan" reviewer plus one real one deletes a live
Pending row. Written into Status, "nan" is neither "rejected" nor "pending"
and the row falls through every branch.

So an empty cell reads as "" — the true statement — via ``_text()``.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from pipeline.merge_master import _text, mark_rejected_with_counter

ROOT = Path(__file__).resolve().parents[1]
NAN = float("nan")


@pytest.mark.parametrize("value,expected", [
    (None, ""),
    (NAN, ""),
    ("nan", ""),        # the string pandas' own str() would have produced
    ("NaN", ""),
    ("NaT", ""),
    ("None", ""),
    ("", ""),
    ("operator review", "operator review"),
    ("confirm-agent,url-agent", "confirm-agent,url-agent"),
    (0, "0"),           # a real zero is data, not emptiness
    (3, "3"),
])
def test_an_empty_cell_reads_as_empty_and_real_text_survives(value, expected):
    assert _text(value) == expected


def test_the_promoter_crash_is_gone():
    """The exact row shape that raised AttributeError on 2026-11-10."""
    row = {"RejectedBy": NAN, "Notes": NAN, "Status": NAN,
           "URL": "https://example.gov/notice", "Date": "2026-11-03"}
    should_delete, _ = mark_rejected_with_counter(row, "Confirmer: test")
    assert should_delete is False, (
        "one reviewer must not delete a row; the threshold is two DIFFERENT "
        "reviewers")


def test_no_phantom_nan_reviewer_is_ever_recorded():
    """The half that matters more than the crash."""
    row = {"RejectedBy": NAN, "Notes": NAN, "Status": NAN}
    mark_rejected_with_counter(row, "Confirmer: page does not mention firm")
    assert "nan" not in str(row["RejectedBy"]).lower(), (
        f"RejectedBy is {row['RejectedBy']!r} — 'nan' has been recorded as a "
        f"REVIEWER. Two reviewers delete a Pending row, so a phantom one "
        f"plus a real one silently drops a live recall.")
    assert "nan" not in str(row["Status"]).lower()
    assert "nan" not in str(row.get("Notes") or "").lower()
    # And a second, DIFFERENT reviewer must still reach the threshold.
    # The two reasons must map to two DIFFERENT canonical reviewers:
    # _reviewer_from_reason sends every unrecognised prefix to "unknown", so
    # "Confirmer:" and "Review agent:" are one reviewer to the counter.
    should_delete, _ = mark_rejected_with_counter(row, "Claude check: nope")
    assert should_delete is True, (
        "two different reviewers must still delete the row — the NaN fix "
        "must not break the counter it protects")


def test_the_counter_is_idempotent_for_one_reviewer_on_an_empty_row():
    row = {"RejectedBy": NAN, "Notes": NAN, "Status": NAN}
    assert mark_rejected_with_counter(row, "Confirmer: a")[0] is False
    assert mark_rejected_with_counter(row, "Confirmer: b")[0] is False, (
        "the same reviewer twice must not cross the two-reviewer threshold")


def test_none_of_the_six_repaired_sites_went_back_to_the_unsafe_idiom():
    """Code-side, so the fix cannot be silently reverted."""
    body = (ROOT / "pipeline" / "merge_master.py").read_text(encoding="utf-8")
    # Strip docstrings first: _text()'s own docstring quotes the broken idiom
    # deliberately, as the record of what it exists to fix.
    code = re.sub(r'"""(?:.|\n)*?"""', "", body)
    unsafe = re.findall(
        r'\((?:row|r)\.get\("(RejectedBy|Status|ScrapedAt|Notes)"\)\s*or\s*""\)',
        code)
    assert not unsafe, (
        f"{len(unsafe)} site(s) read {sorted(set(unsafe))} with the "
        f"`(x.get(...) or \"\")` idiom again. pandas gives nan for an empty "
        f"cell and nan is truthy, so the fallback never fires. Use _text().")
    assert "def _text(" in body, "the _text() helper was removed"
