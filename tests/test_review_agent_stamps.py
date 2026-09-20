# -*- coding: utf-8 -*-
"""Reviewer 2 must leave a mark on every row it approves.

THE GAP (audit 2026-09-19)
--------------------------
Advancing a row's status and recording that a reviewer read it are two
different facts. They were fused: the ``[review-agent …]`` stamp was
written inside ``if cur in _ADVANCE_FROM``, and that set is

    {"pending_gap_v1", "pending_gap_v2", "pending_retry", "pending_enrichment"}

— it does not contain plain ``"pending"``.

Measured against the live workbook on 2026-09-19. Before the run, Pending
held **15 rows at "pending"**, 10 at "pending_enrichment" and 2 at
"pending_gap_v2". The run promoted 15 rows (Recalls 1736 → 1751) and
wrote **zero** review-agent stamps.

Those rows are in a worse state than the ones carrying *"reviewer 2 did
NOT review this row"*. That flag is true and auditable. These carry no
stamp and no flag, so nothing in the register separates a row Qwen read
line by line from one nobody looked at — which is the single thing the
two-reviewer architecture exists to make visible.

A second, quieter bug rode along: the stamp was appended and the result
truncated to 1000 characters. On a row whose Notes were already long, the
truncation ate the stamp it had just been asked to record.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

SRC = (ROOT / "pipeline" / "recall_review_agent.py").read_text(encoding="utf-8")

#: The block that applies an approval to a Pending row.
BLOCK = SRC[SRC.index("_ADVANCE_FROM = {"):SRC.index("# Rejects → rejected_flags")]


def test_the_stamp_is_not_conditional_on_advancing_the_status():
    """The regression itself: a stamp only for rows that change status."""
    # Every assignment of the review-agent tag must be reachable whether or
    # not `cur in _ADVANCE_FROM`, i.e. there must be an else-branch tag.
    assert "else:" in BLOCK, (
        "the approval block has no else-branch — a row whose status does not "
        "advance is approved and promoted with no reviewer stamp, which is "
        "exactly the 2026-09-19 defect")
    tags = re.findall(r'tag = \(f"\[review-agent', BLOCK)
    assert len(tags) >= 2, (
        "expected a stamp on both the advancing and the non-advancing path, "
        "found %d" % len(tags))


def test_plain_pending_is_still_not_an_advancing_status():
    """Guards the premise. If "pending" were added to _ADVANCE_FROM the fix
    above would become dead code and the test would silently stop meaning
    anything."""
    m = re.search(r"_ADVANCE_FROM = \{(.*?)\}", BLOCK, re.S)
    assert m
    states = {s.strip().strip('"\'') for s in m.group(1).split(",") if s.strip()}
    assert "pending" not in states, (
        "if plain 'pending' advances, re-read this test's premise")
    assert "pending_enrichment" in states


def test_the_truncation_cannot_eat_the_stamp():
    """Truncate the OLD notes, never the tag that was just added."""
    assert "_room = 1000 - len(tag)" in BLOCK, (
        "the stamp must be appended with room reserved for it; the old code "
        "did (notes + tag)[:1000], which drops the stamp on a long row")
    assert "notes[:_room]" in BLOCK


# --------------------------------------------------------------------------
# behaviour, exercised rather than read
# --------------------------------------------------------------------------

def _apply(cur_status: str, notes: str) -> dict:
    """Run the approval block's logic over one row.

    The real function needs a live workbook and a model, so this mirrors the
    branch under test exactly as written in the source. Kept honest by
    test_the_stamp_is_not_conditional_on_advancing_the_status above, which
    asserts against the source itself.
    """
    _ADVANCE_FROM = {"pending_gap_v1", "pending_gap_v2", "pending_retry",
                     "pending_enrichment"}
    _A2 = "pending_gap_v3"
    today = "2026-09-19"
    row = {"Status": cur_status, "Notes": notes}
    cur = str(row.get("Status", "")).strip()
    notes = str(row.get("Notes", "")).strip()
    if cur in _ADVANCE_FROM:
        row["Status"] = _A2
        tag = (f"[review-agent {today}: {cur} → {_A2} (Qwen verified; "
               f"awaiting reviewer 3 confirmation)]")
    else:
        tag = (f"[review-agent {today}: reviewed and approved at status "
               f"{cur!r}; fields verified against the source, status left "
               f"unchanged]")
    _room = 1000 - len(tag) - 1
    row["Notes"] = ((notes[:_room].rstrip() + " " + tag).strip()
                    if _room > 0 else tag[:1000])
    return row


@pytest.mark.parametrize("status", [
    "pending",              # the 15 that went unstamped
    "pending_gap_v1",
    "pending_gap_v2",
    "pending_retry",
    "pending_enrichment",
    "",                     # a row with no status at all
])
def test_every_approved_status_gets_a_stamp(status):
    out = _apply(status, "carrefour grenoble meylan uniquement")
    assert "[review-agent 2026-09-19:" in out["Notes"], (
        "status %r was approved without a reviewer stamp" % status)


def test_an_advancing_row_still_advances():
    out = _apply("pending_enrichment", "")
    assert out["Status"] == "pending_gap_v3"
    assert "pending_gap_v3" in out["Notes"]


def test_a_non_advancing_row_keeps_its_status():
    out = _apply("pending", "")
    assert out["Status"] == "pending"
    assert "status left unchanged" in out["Notes"]


def test_a_very_long_note_keeps_the_stamp():
    out = _apply("pending", "x" * 3000)
    assert len(out["Notes"]) <= 1000
    assert out["Notes"].endswith("]")
    assert "[review-agent 2026-09-19:" in out["Notes"], (
        "the old code appended then truncated, so a long row lost the stamp")
