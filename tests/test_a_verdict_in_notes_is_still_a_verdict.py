# -*- coding: utf-8 -*-
"""Where a rejection verdict lives — pinned. Morning fix pass 2026-11-18.

On 2026-11-18 test_a_recall_is_not_both_published_and_rejected reported
Rejected FDA/Eden Brothers 2026-10-07 as an archive row that "says nothing
about which copy wins". It says it plainly, in Notes:

    REJECTED: Confirmer: row was at pending_gap_v2 (not reviewed by
    reviewer 2) and duplicate: this URL is already published on 2026-10-07

The reason COLUMN was blank, and that test read only the column. A false
positive — and the FOURTH instance in this repo of one shape: a reader looks
at a fixed set of columns while the writers put the verdict somewhere else.
load_rejected_urls learned it about Notes on 2026-09-01,
promote_gate_passing.supersede_archived_copies on 2026-10-05 (35 of the 36
silent URLs it had missed kept their verdict in Notes), and merge_master's
re-promotion guard learned it about the Reason column the same morning,
after blocking a verified Tier-2 recall for ever.

This file holds BOTH halves of the fix, because loosening a reader is only
safe if the loosening is bounded:

  * a verdict in Notes, or in Reason, counts as a verdict;
  * a row with no verdict in ANY of the three columns is still caught.

If someone widens verdict_text() again, the second half fails.
"""
from __future__ import annotations

import pytest

from tests.test_a_recall_is_not_both_published_and_rejected import (  # noqa: E402
    RESOLVED,
    verdict_text,
)


def _is_silent(row: dict, reason_col: str = "RejectReason") -> bool:
    """True when the checker would report `row` as saying nothing."""
    return not any(m in verdict_text(row, reason_col) for m in RESOLVED)


# ── HALF ONE: a verdict counts wherever a writer put it ───────────────────

#: The real Eden Brothers row, 2026-10-07, abbreviated. Its verdict is in
#: Notes and its RejectReason was blank for six weeks.
EDEN_NOTES = ("REJECTED: Confirmer: row was at pending_gap_v2 (not reviewed "
              "by reviewer 2) and duplicate: this URL is already published "
              "on 2026-10-07")


@pytest.mark.parametrize("row,why", [
    ({"RejectReason": "", "Notes": EDEN_NOTES, "Reason": "Salmonella — outbreak"},
     "the confirmer's duplicate verdict, in Notes, column blank"),
    ({"RejectReason": "duplicate_already_approved — FSA-PRIN-41-2026",
      "Notes": "", "Reason": ""},
     "a verdict in the column itself still reads first"),
    ({"RejectReason": "nan", "Notes": "nan",
      "Reason": "No matching hazard category — SUPERSEDED 2026-10-05"},
     "the gap-finder fleet writes its refusal into the Reason column"),
    ({"RejectReason": "", "Notes": "[UNRESOLVED CONTRADICTION (morning fix "
                                   "2026-11-18): not the same recall]",
      "Reason": ""},
     "an explicit unresolved flag is a verdict too"),
])
def test_a_verdict_is_found_wherever_a_writer_put_it(row, why):
    assert not _is_silent(row), (
        f"a row that states its verdict was reported as silent — {why}. "
        f"verdict_text() must read the reason column, Notes and Reason, "
        f"because all three are written to in production.")


# ── HALF TWO: silence is still silence ────────────────────────────────────

@pytest.mark.parametrize("row,why", [
    ({"RejectReason": "", "Notes": "Discovered via news: ilmessaggero.it",
      "Reason": ""},
     "the two Ministero della Salute rows of 2026-11-14 before they were "
     "flagged — a discovery trail is not a verdict"),
    ({"RejectReason": "nan", "Notes": "nan", "Reason": "nan"},
     "an empty row"),
    ({"RejectReason": "no-authority-url: no official Salute press-release "
                      "URL in the source article (news-only discovery)",
      "Notes": "Discovered via news: ilmessaggero.it", "Reason": ""},
     "a REFUSAL GROUND is not a statement about which copy wins: this row "
     "explains why IT was refused and says nothing about the published "
     "recall sharing its URL"),
    ({"RejectReason": "", "Notes": "Listeria monocytogenes",
      "Reason": "Presence of Listeria monocytogenes"},
     "a hazard in Reason is not a verdict — this is the noise the "
     "2026-10-05 note predicted folding Reason in would add, and it must "
     "stay harmless"),
])
def test_a_row_with_no_verdict_is_still_caught(row, why):
    assert _is_silent(row), (
        f"a row with no verdict in any column was accepted — {why}. "
        f"Folding Notes and Reason into the haystack must not make the "
        f"check vacuous; RESOLVED is a list of verdict words, not a list of "
        f"words that happen to appear on archive rows.")


def test_the_marker_list_is_the_one_the_checker_uses():
    """RESOLVED is imported, not copied — so the two cannot drift."""
    assert "duplicate" in RESOLVED
    assert "superseded" in RESOLVED
    assert "unresolved contradiction" in RESOLVED
