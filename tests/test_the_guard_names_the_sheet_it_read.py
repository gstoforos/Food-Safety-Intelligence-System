# -*- coding: utf-8 -*-
"""A guard that quotes a verdict must say which sheet the verdict is in.

WHAT THIS CAUGHT (morning fix 2026-10-12)
=========================================
``merge_master.load_rejected_urls`` reads ``_REJECT_SHEETS`` — the permanent
``Rejected`` sheet AND the rolling ``Weekly_Rejected`` sheet. The
re-promotion guard that consumes it said, unconditionally::

    re-promotion BLOCKED <url> — already in Weekly_Rejected (<reason>)

and stamped the same wording into the row's Notes. For every row whose
verdict lives in ``Rejected``, that sentence is false in the one detail the
reader needs.

Measured live. ``promote_gate_passing --apply`` on 2026-10-12 printed::

    re-promotion BLOCKED .../fsis-issues-public-health-alert-ineligible-chicke
      — already in Weekly_Rejected (unknown: UNRESOLVED ...)

FSIS PHA-10062026-01 (Grace chicken Vienna sausages) is in ``Rejected`` and
in no weekly sheet at all. An operator following that line opens
Weekly_Rejected, does not find the row, and has to decide whether to trust
the guard — about a live Tier-2 alert that is genuinely blocked.

This is the same shape as
tests/test_a_verdict_in_the_wrong_column_is_still_a_verdict.py and
tests/test_a_guard_reads_the_whole_reason.py: a guard and the writers it
reads disagree about where something lives, and the guard states its
assumption as a fact.

WHY THE SHEET NAME IS NOT PUT IN THE REASON STRING
--------------------------------------------------
The obvious fix — prefix the stored reason with its sheet — is the defect
documented at merge_master's "NOT TRUNCATED. 2026-10-04." block. Every
re-promotion exception is a substring search over that value, and padding
it in front pushed the real defect name out of the window the searches
read, making 34 archive rows permanently unre-promotable. So provenance
travels in ``_REJECTED_URL_SHEET``, beside the reason, never inside it.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline import merge_master as M  # noqa: E402

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"


def test_the_guard_message_is_not_hardcoded_to_one_sheet():
    """The string literal is the bug. Hold it out of the source."""
    src = (ROOT / "pipeline" / "merge_master.py").read_text(encoding="utf-8")
    i = src.find("re-promotion BLOCKED")
    assert i > 0, "the guard log line moved — re-point this test"
    window = src[i:i + 400]
    assert "already in Weekly_Rejected" not in window, (
        "the re-promotion guard names Weekly_Rejected unconditionally again, "
        "while load_rejected_urls reads Rejected too. Use "
        "rejected_sheet_for() so the message names the sheet the verdict was "
        "actually read from.")


def test_the_reason_string_still_starts_with_the_reason():
    """Provenance must not be prepended — see the docstring.

    A sheet name glued onto the front of the stored reason is the
    2026-10-04 regression, so this asserts the stored value still begins
    with the rejecter/reason and not with a sheet name.
    """
    if not XLSX.exists():
        import pytest
        pytest.skip("no workbook")
    M._REJECTED_URL_CACHE.clear()
    guard = M.load_rejected_urls(XLSX)
    bad = [u for u, why in guard.items()
           if str(why).lstrip().startswith(("Rejected", "Weekly_Rejected"))
           and "no reason recorded" not in str(why)]
    assert not bad, (
        f"{len(bad)} guard entries now begin with a sheet name — that is the "
        f"prepend that buried 34 rows' defect names on 2026-10-04. Keep the "
        f"sheet in _REJECTED_URL_SHEET. First: {bad[:2]}")


def test_it_reports_the_sheet_the_url_was_read_from():
    if not XLSX.exists():
        import pytest
        pytest.skip("no workbook")
    M._REJECTED_URL_CACHE.clear()
    guard = M.load_rejected_urls(XLSX)
    if not guard:
        import pytest
        pytest.skip("no archived rejections carry a URL")
    url = next(iter(guard))
    sheet = M.rejected_sheet_for(url, XLSX)
    assert sheet in M._REJECT_SHEETS, (
        f"rejected_sheet_for returned {sheet!r}, which is not one of "
        f"{M._REJECT_SHEETS}")


def test_an_unknown_url_names_both_sheets_rather_than_guessing_one():
    """Honest uncertainty beats a confident wrong answer.

    A URL the guard has no provenance for must not be reported as sitting
    in a specific sheet — that is exactly the failure being fixed.
    """
    got = M.rejected_sheet_for("https://example.invalid/not-in-any-archive")
    for s in M._REJECT_SHEETS:
        assert s in got, got
