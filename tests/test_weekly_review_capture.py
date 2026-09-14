"""
tests/test_weekly_review_capture.py
=====================================
Tests for pipeline.weekly_review_capture.

Two functions under test:

    review_day_for(now_utc) → date
        Returns the review date a row promoted at `now_utc` belongs to.
        Cutoff: SUNDAY 17:00 Athens local time (2026-08-31; was Thursday).
          - Promoted at Sat 23:59 → THIS Sunday
          - Promoted at Sun 16:59 → THIS Sunday (still in window)
          - Promoted at Sun 17:00 → NEXT Sunday (rollover, strict ≥)
          - Promoted at Mon 09:00 → NEXT Sunday
          - Promoted at Thu 12:00 → NEXT Sunday

        The old name review_day_for is kept as an alias and returns
        the same Sunday. Nothing calls it for a Thursday any more.

    record_promotions(promoted_rows, xlsx_path, json_path) → int
        Appends each row to the Weekly_Review sheet, tagged with the
        appropriate Sunday review date. Returns count of newly-appended
        rows. Idempotent: calling twice with the same rows is a no-op.

The Sunday-17:00 cutoff matters because that's when the operator's
review email fires (sendSundayManualReview). A row promoted at 17:01
must NOT appear in that day's email — it goes to next week. If the
cutoff drifts (e.g. someone changes REVIEW_HOUR_LOCAL), the email shows
phantom rows that the operator didn't approve yet.

2026-08-31: the review day moved Thursday → Sunday together with the
reporting week becoming the ISO week. The two are the same decision.
The wipe clears the rolling review sheets and the MONDAY build reports
them; a Thursday cutoff would close the window three days before the
week it belongs to and silently drop every Friday and Saturday
promotion.

The idempotency invariant matters because weekly_review_capture is
called from inside merge_master.append_to_recalls — which runs hourly
via merge-master.yml. If the function weren't idempotent, every hourly
run would duplicate every row in Weekly_Review.

Uses freezegun to freeze time at specific UTC instants and observe the
function's response. The Athens timezone math is non-obvious (DST), so
the tests use UTC inputs and compare against specific expected Athens-
local interpretations.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest
from freezegun import freeze_time
from openpyxl import load_workbook

from pipeline.weekly_review_capture import (
    review_day_for,
    record_promotions,
    SHEET_NAME,
    SHEET_COLS,
)
from tests.conftest import RECALLS_COLS, PENDING_COLS, NEWS_COLS


# ───────────────────────────────────────────────────────────────────────
# review_day_for() — cutoff math
# ───────────────────────────────────────────────────────────────────────
class TestReviewDayFor:
    """
    Cutoff rule: Thursday 17:00 Athens local. Time arithmetic uses
    Europe/Athens which switches between EEST (UTC+3, summer) and EET
    (UTC+2, winter). Test instants are in UTC; expected output is the
    Athens-local Thursday they should resolve to.

    Reference dates (May 2026 = EEST = UTC+3):
        Wed May 13, 2026  UTC 06:00 → Athens 09:00 Wed
        Thu May 14, 2026  UTC 13:59 → Athens 16:59 Thu  (still in window)
        Thu May 14, 2026  UTC 14:00 → Athens 17:00 Thu  (rollover)
        Thu May 14, 2026  UTC 14:30 → Athens 17:30 Thu  (rollover)
        Fri May 15, 2026  UTC 09:00 → Athens 12:00 Fri  (next week)
    """

    def test_wednesday_promotion_lands_in_this_sunday(self):
        """Wed 13 May 09:00 Athens — well inside the window closing Sun 17 May."""
        now = datetime(2026, 5, 13, 6, 0, tzinfo=timezone.utc)
        assert review_day_for(now) == date(2026, 5, 17)

    def test_sunday_before_1700_lands_in_today(self):
        """Sun 17 May 16:59 Athens — one minute inside the cutoff."""
        now = datetime(2026, 5, 17, 13, 59, tzinfo=timezone.utc)
        assert review_day_for(now) == date(2026, 5, 17)

    def test_sunday_at_1700_rolls_to_next_week(self):
        """Sun 17 May 17:00 Athens exactly — strict >=, so it rolls."""
        now = datetime(2026, 5, 17, 14, 0, tzinfo=timezone.utc)
        assert review_day_for(now) == date(2026, 5, 24)

    def test_sunday_after_1700_rolls_to_next_week(self):
        """Sun 17 May 17:30 Athens — the moment the wipe runs."""
        now = datetime(2026, 5, 17, 14, 30, tzinfo=timezone.utc)
        assert review_day_for(now) == date(2026, 5, 24)

    def test_monday_lands_in_next_sunday(self):
        """Mon 18 May — first day of the new ISO week."""
        now = datetime(2026, 5, 18, 6, 0, tzinfo=timezone.utc)
        assert review_day_for(now) == date(2026, 5, 24)

    def test_friday_lands_in_the_same_sunday(self):
        """Fri 22 May. THE case the move was made for: under the Thursday
        cutoff this promotion waited a week to be reviewed."""
        now = datetime(2026, 5, 22, 12, 0, tzinfo=timezone.utc)
        assert review_day_for(now) == date(2026, 5, 24)

    def test_saturday_lands_in_the_same_sunday(self):
        """Sat 23 May — the day before the cutoff."""
        now = datetime(2026, 5, 23, 12, 0, tzinfo=timezone.utc)
        assert review_day_for(now) == date(2026, 5, 24)

    def test_thursday_lands_in_the_same_sunday(self):
        """Thu 21 May — used to be the cutoff day itself."""
        now = datetime(2026, 5, 21, 9, 0, tzinfo=timezone.utc)
        assert review_day_for(now) == date(2026, 5, 24)

    def test_winter_dst_cutoff_eet(self):
        """
        Winter (EET = UTC+2): Thu Feb 5, 2026.
          UTC 14:59 → Athens 16:59 → still in window
          UTC 15:00 → Athens 17:00 → rollover

        Tests that the cutoff respects DST — using EET offsets, not EEST.
        """
        # In window
        now = datetime(2026, 2, 8, 14, 59, tzinfo=timezone.utc)
        assert review_day_for(now) == date(2026, 2, 8)
        # Rolled over
        now = datetime(2026, 2, 8, 15, 0, tzinfo=timezone.utc)
        assert review_day_for(now) == date(2026, 2, 15)

    def test_default_now_utc_returns_some_sunday(self):
        """Calling with no argument uses current UTC. The result must
        be a Thursday in the future (or today if before 17:00 Athens
        and it's Thursday)."""
        result = review_day_for()
        # 3 = Thursday in Python's weekday() (Mon=0..Sun=6)
        assert result.weekday() == 6   # Sunday


# ───────────────────────────────────────────────────────────────────────
# record_promotions() — Weekly_Review append + idempotency
# ───────────────────────────────────────────────────────────────────────
pytestmark_slow = pytest.mark.slow


@pytestmark_slow
class TestRecordPromotions:
    """End-to-end: build a minimal xlsx, call record_promotions, inspect
    the resulting Weekly_Review sheet."""

    def test_creates_weekly_review_sheet_when_missing(
        self, tmp_xlsx_factory, tmp_path, sample_recall_row
    ):
        """First call against an xlsx WITHOUT a Weekly_Review sheet
        creates the sheet and appends rows."""
        xlsx = tmp_xlsx_factory("recalls.xlsx", {
            "Recalls": (RECALLS_COLS, []),
            "Pending": (PENDING_COLS, []),
            "NEWS":    (NEWS_COLS, []),
        })
        json_out = tmp_path / "weekly-review-latest.json"

        appended = record_promotions(
            [sample_recall_row], xlsx_path=xlsx, json_path=json_out)
        assert appended == 1

        wb = load_workbook(xlsx)
        assert SHEET_NAME in wb.sheetnames
        ws = wb[SHEET_NAME]
        # 1 header row + 1 data row
        assert ws.max_row == 2

    def test_idempotent_same_row_twice(
        self, tmp_xlsx_factory, tmp_path, sample_recall_row
    ):
        """Calling twice with the same row appends ONCE, not twice."""
        xlsx = tmp_xlsx_factory("recalls.xlsx", {
            "Recalls": (RECALLS_COLS, []),
            "Pending": (PENDING_COLS, []),
            "NEWS":    (NEWS_COLS, []),
        })
        json_out = tmp_path / "wr.json"

        first  = record_promotions([sample_recall_row], xlsx_path=xlsx,
                                   json_path=json_out)
        second = record_promotions([sample_recall_row], xlsx_path=xlsx,
                                   json_path=json_out)

        assert first == 1
        assert second == 0, "Idempotency broken: same row appended twice"

        wb = load_workbook(xlsx)
        ws = wb[SHEET_NAME]
        assert ws.max_row == 2  # header + 1 data row, NOT 3

    def test_week_added_column_set_correctly(
        self, tmp_xlsx_factory, tmp_path, sample_recall_row
    ):
        """The Week_Added column must be set to the SUNDAY review date,
        matching review_day_for(). Lock by freezing time."""
        xlsx = tmp_xlsx_factory("recalls.xlsx", {
            "Recalls": (RECALLS_COLS, []),
            "Pending": (PENDING_COLS, []),
            "NEWS":    (NEWS_COLS, []),
        })
        json_out = tmp_path / "wr.json"

        # Freeze at Wed May 13, 2026 06:00 UTC (= 09:00 Athens Wed).
        # The review window closes Sunday 17 May 2026 (2026-08-31: the
        # cutoff day moved Thursday -> Sunday with the ISO week).
        with freeze_time("2026-05-13 06:00:00"):
            record_promotions([sample_recall_row], xlsx_path=xlsx,
                              json_path=json_out)

        wb = load_workbook(xlsx)
        ws = wb[SHEET_NAME]
        headers = [c.value for c in ws[1]]
        week_added_idx = headers.index("Week_Added")
        appended_value = ws.cell(row=2, column=week_added_idx + 1).value
        assert appended_value == "2026-05-17", \
            f"Expected Week_Added=2026-05-17, got {appended_value}"

    def test_reviewed_column_starts_as_N(
        self, tmp_xlsx_factory, tmp_path, sample_recall_row
    ):
        """Every new row enters with Reviewed='N'. Manual stamp by
        operator later flips it to 'Y'."""
        xlsx = tmp_xlsx_factory("recalls.xlsx", {
            "Recalls": (RECALLS_COLS, []),
            "Pending": (PENDING_COLS, []),
            "NEWS":    (NEWS_COLS, []),
        })
        json_out = tmp_path / "wr.json"
        record_promotions([sample_recall_row], xlsx_path=xlsx,
                          json_path=json_out)

        wb = load_workbook(xlsx)
        ws = wb[SHEET_NAME]
        headers = [c.value for c in ws[1]]
        reviewed_idx = headers.index("Reviewed")
        assert ws.cell(row=2, column=reviewed_idx + 1).value == "N"

    def test_url_dedup_collapses_collisions(
        self, tmp_xlsx_factory, tmp_path, sample_recall_row
    ):
        """If the same URL+Date appears twice in one batch, it appears
        ONCE in the sheet (matches Weekly_Review dedup_key rule)."""
        xlsx = tmp_xlsx_factory("recalls.xlsx", {
            "Recalls": (RECALLS_COLS, []),
            "Pending": (PENDING_COLS, []),
            "NEWS":    (NEWS_COLS, []),
        })
        json_out = tmp_path / "wr.json"
        # Same URL, different Company — but URL+Date is the dedup key.
        row_a = dict(sample_recall_row); row_a["Company"] = "First"
        row_b = dict(sample_recall_row); row_b["Company"] = "Second"
        record_promotions([row_a, row_b], xlsx_path=xlsx, json_path=json_out)

        wb = load_workbook(xlsx)
        ws = wb[SHEET_NAME]
        assert ws.max_row == 2, "URL+Date collision should collapse to 1 row"

    def test_json_sidecar_created(
        self, tmp_xlsx_factory, tmp_path, sample_recall_row
    ):
        """record_promotions also refreshes the JSON sidecar that the
        Apps Script Thursday mailer reads."""
        xlsx = tmp_xlsx_factory("recalls.xlsx", {
            "Recalls": (RECALLS_COLS, []),
            "Pending": (PENDING_COLS, []),
            "NEWS":    (NEWS_COLS, []),
        })
        json_out = tmp_path / "wr.json"
        record_promotions([sample_recall_row], xlsx_path=xlsx,
                          json_path=json_out)

        assert json_out.exists(), "JSON sidecar not written"
        data = json.loads(json_out.read_text())
        # The shape can vary; lock the minimum invariants.
        assert isinstance(data, dict), "JSON sidecar must be a dict"

    def test_empty_promotions_list_noop(
        self, tmp_xlsx_factory, tmp_path
    ):
        """Empty list → no rows appended, but JSON sidecar still refreshed
        (defensive: stale JSON would mislead the mailer)."""
        xlsx = tmp_xlsx_factory("recalls.xlsx", {
            "Recalls": (RECALLS_COLS, []),
            "Pending": (PENDING_COLS, []),
            "NEWS":    (NEWS_COLS, []),
        })
        json_out = tmp_path / "wr.json"
        appended = record_promotions([], xlsx_path=xlsx, json_path=json_out)
        assert appended == 0

    def test_missing_xlsx_returns_zero(self, tmp_path, sample_recall_row):
        """Missing xlsx file → return 0, do NOT raise."""
        missing = tmp_path / "does-not-exist.xlsx"
        json_out = tmp_path / "wr.json"
        result = record_promotions([sample_recall_row],
                                    xlsx_path=missing,
                                    json_path=json_out)
        assert result == 0


# ──────────────────────────────────────────────────────────────────────
# AUDIT 2026-09-13 — the Sunday email that said "0 recalls added"
# ──────────────────────────────────────────────────────────────────────
# 62 rows entered Recalls in the week 7-13 Sep (35 via the confirm agent,
# 27 by operator review) and the manual-review email reported none, because
# Weekly_Review held only its header. The confirm agent calls
# promote_approved() and save_xlsx_with_pending(), mirrors EVICTIONS into
# Weekly_Rejected (fixed 2026-09-04) — and never mirrored PROMOTIONS. It is
# the only promoter left: merge_master runs in janitor mode unless
# MERGE_MASTER_PROMOTE=1, which the hourly workflow does not set.

def test_the_confirm_agent_mirrors_promotions():
    from pathlib import Path as _P
    src = (_P(__file__).resolve().parents[1] / "pipeline"
           / "recall_confirm_agent.py").read_text("utf-8")
    assert "record_promotions(" in src, (
        "the confirm agent must mirror promotions into Weekly_Review — it is "
        "the only promoter, so without this the Sunday review email reports "
        "zero however many rows were published")
    assert "weekly-review-latest.json" in src, (
        "pass json_path explicitly: the module default resolves from the "
        "package ROOT at import time, not from the workbook this run was given")


def test_both_halves_of_the_ledger_are_mirrored():
    """Rejections and promotions, in the same place, for the same reason."""
    from pathlib import Path as _P
    src = (_P(__file__).resolve().parents[1] / "pipeline"
           / "recall_confirm_agent.py").read_text("utf-8")
    assert src.index("record_rejections(") < src.index("record_promotions("), (
        "both mirrors belong immediately after save_xlsx_with_pending()")


def test_the_embedded_workflow_copy_carries_the_fix():
    """The agent runs from a heredoc copy inside its own workflow."""
    from pathlib import Path as _P
    yml = (_P(__file__).resolve().parents[1] / ".github" / "workflows"
           / "recall-confirm-agent.yml").read_text("utf-8")
    assert "record_promotions(" in yml, (
        "recall-confirm-agent.yml overwrites pipeline/recall_confirm_agent.py "
        "at runtime from its embedded copy — rebuild the pair after editing "
        "the module or the fix never runs")


# ──────────────────────────────────────────────────────────────────────
# AUDIT 2026-09-13 — the confirm agent's provenance stamp
# ──────────────────────────────────────────────────────────────────────
# The stamp was hard-coded to "pending_gap_v3 → pending" for every confirmed
# row. The lane was widened on 2026-09-04 to admit ANY pending status, and
# reviewer 2 — the only thing that sets pending_gap_v3 — has banked nothing
# since at least 2026-09-02. So the stamp asserted a reviewer-2 hand-off
# that never happened, on rows reviewer 2 never saw.

def test_the_stamp_records_the_real_prior_status():
    from pathlib import Path as _P
    src = (_P(__file__).resolve().parents[1] / "pipeline"
           / "recall_confirm_agent.py").read_text("utf-8")
    assert '_prior = str(full_pending[idx].get("Status"' in src, (
        "the confirm agent must read the row's actual prior status before "
        "overwriting it with 'pending'")
    assert '{A2_APPROVED} → pending' not in src, (
        "the stamp must not hard-code pending_gap_v3 — it is usually not "
        "the status the row held")


def test_the_embedded_copy_carries_the_stamp_fix():
    from pathlib import Path as _P
    yml = (_P(__file__).resolve().parents[1] / ".github" / "workflows"
           / "recall-confirm-agent.yml").read_text("utf-8")
    assert "_prior = str(full_pending[idx]" in yml, (
        "rebuild the recall-confirm-agent.yml heredoc pair after editing the "
        "module, or the fix never runs")


def test_the_retired_gate_is_not_watched():
    """A permanently-false OVERDUE trains operators to ignore real ones."""
    import re
    from pathlib import Path as _P
    src = (_P(__file__).resolve().parents[1] / "tools"
           / "dispatch_watchdog.py").read_text("utf-8")
    blk = src[src.index("WATCHED"):]
    names = re.findall(r'^\s*\("([^"]+)"', blk, re.M)
    assert not any("gemini" in n for n in names), (
        "gemini-url-gate was retired 2026-08-30 and its job exits at a "
        "retirement guard; watching it can only ever report OVERDUE")


def test_no_string_claims_a_reviewer_2_action_it_cannot_know():
    """The archive is read by people deciding whether a row was checked."""
    import re as _re
    from pathlib import Path as _P
    raw = (_P(__file__).resolve().parents[1] / "pipeline"
           / "recall_confirm_agent.py").read_text("utf-8")
    # Comments quote the old strings to explain why they went; only CODE
    # counts here.
    src = "\n".join(l for l in raw.splitlines()
                    if not l.lstrip().startswith("#"))
    for claim in ('"Confirmer: reviewer 2 approved but "',
                  '"Rejected by reviewer 2"'):
        assert claim not in src, (
            f"{claim} asserts an action by an agent that may never have seen "
            "the row — the lane admits any pending status, and reviewer 2 "
            "has banked nothing since 2026-09-02")
    # Name-agnostic: main calls the variable _prior, an earlier draft called
    # it _from. What matters is that the real prior status is interpolated,
    # not that a particular local name survives.
    assert _re.search(r'row was at \{_\w+\}', src), (
        "record the status the row actually held when the confirmer found it")
