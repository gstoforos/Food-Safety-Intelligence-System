# -*- coding: utf-8 -*-
"""A rejection must name the reviewer that made it, and reject for a reason
that reviewer was actually authorised to use.

TWO DEFECTS, ONE SHEET (audit 2026-09-22)
=========================================

**1. "unknown" on every current-agent rejection.**

``_REVIEWER_TAG_RE`` in ``weekly_rejected_capture`` listed
``claude-check``, ``openrouter-check`` and ``gemini-check`` — the
reviewers that existed BEFORE the three-agent chain was built. Nothing
updated it when reviewer 1 / 2 / 3 replaced them, so no stamp any current
agent writes could match, ``rejected_by`` stayed empty, and the fallback
wrote "an unnamed reviewer".

Measured on the live sheet: **10 of the 11** stamped rows in
Weekly_Rejected carried ``RejectedBy = "unknown"`` while their own Reason
column began ``"URL agent:"``. The sheet knew which agent had spoken and
had no way to say so.

That is also the true origin of the line escalated over Yotvata:

    NO_REASON_RECORDED — rejected by an unnamed reviewer from CFIA,
    status pending. The writer supplied no verdict…

The writer *had* supplied a verdict. Nothing in this module could read it.
The bug was hunted as a reviewer failing to record a reason; it was a
regex that had not heard of the reviewer.

**2. A criterion the contract never granted.**

One live rejection reads::

    URL agent: No specific outbreak for the given product and hazard found

Reviewer 1 is authorised to reject on hazard type and on source. Nowhere
is it authorised to reject for the absence of an OUTBREAK — most recalls
are precautionary and no illness is ever linked to them. The model
invented the criterion, and the deliberately narrow reject-guard
(``_url_guard.reject_refusal``) does not catch it, because it is not a
reachability claim. The fix belongs in the prompt contract, and this file
asserts the contract now says so.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline.weekly_rejected_capture import (          # noqa: E402
    _extract_rejection_metadata as extract)


def _row(notes: str, **kw):
    return {"Notes": notes, "Source": "FSAI (IE)", **kw}


# --------------------------------------------------------------------------
# 1. attribution — the live note shapes, verbatim
# --------------------------------------------------------------------------

LIVE = [
    ("REJECTED: URL agent: No official recall page found | FSAI HTML "
     "listing fallback || unknown: Arrived already marked rejected",
     "reviewer 1 (url-agent)"),
    ("REJECTED: Confirmer: row was at pending_gap (not reviewed by "
     "reviewer 2) and page could not be verified",
     "reviewer 3 (confirm-agent)"),
    ("[url-agent 2026-09-18: pending→pending_gap_v2 URL confirmed]",
     "reviewer 1 (url-agent)"),
    ("[review-agent 2026-09-18: pending_gap_v2 → pending_gap_v3]",
     "reviewer 2 (review-agent)"),
    ("[confirm-agent 2026-09-20: published]",
     "reviewer 3 (confirm-agent)"),
]


@pytest.mark.parametrize("notes,expect", LIVE,
                         ids=[e for _, e in LIVE])
def test_the_current_agents_are_named(notes, expect):
    by, _ = extract(_row(notes))
    assert by == expect, (
        f"got {by!r} — an unattributed rejection cannot be audited, and "
        f"this is the shape 10 of 11 live rows carried")


def test_none_of_the_live_shapes_come_back_unknown():
    for notes, _ in LIVE:
        by, why = extract(_row(notes))
        assert by and by != "unknown"
        assert "an unnamed reviewer" not in why


def test_the_old_reviewers_still_parse():
    """Rows written before the three-agent chain must keep their names."""
    for tag in ("claude-check", "openrouter-check", "gemini-check"):
        by, _ = extract(_row(f"[{tag} 2026-05-09: fail; not a recall]"))
        assert by == tag


def test_the_reason_is_still_extracted_alongside_the_name():
    by, why = extract(_row("[claude-check 2026-05-09: fail; not a recall]"))
    assert by == "claude-check"
    assert why == "not a recall"


def test_a_genuinely_unstamped_row_is_still_labelled_unaudited():
    """The fallback must survive. Widening the regex must not make an
    unattributed row silently look attributed."""
    by, why = extract(_row("some free text with no stamp whatsoever"))
    assert by == ""
    assert "NO_REASON_RECORDED" in why
    assert "UNAUDITED" in why


def test_an_explicit_RejectedBy_field_still_wins():
    by, _ = extract(_row("[url-agent 2026-09-18: x]", RejectedBy="operator"))
    assert by == "operator"


def test_the_name_is_bounded():
    by, _ = extract(_row("[url-agent 2026-09-18: " + "x" * 500 + "]"))
    assert len(by) <= 80


# --------------------------------------------------------------------------
# 2. the prompt contract
# --------------------------------------------------------------------------

AGENT = (ROOT / "pipeline" / "recall_url_agent.py").read_text(encoding="utf-8")

#: Whitespace-collapsed copy. The prompt is a wrapped triple-quoted string,
#: so a phrase that reads as one line on screen is split across two in the
#: source. Searching the raw text for it reports a rule as absent when it is
#: present — a trap this repo has already been caught by once.
AGENT_FLAT = " ".join(AGENT.split())


def test_the_contract_forbids_rejecting_for_no_outbreak():
    assert "AN OUTBREAK IS NOT REQUIRED" in AGENT
    assert "no specific outbreak for the given product and hazard" in AGENT_FLAT.lower(), (
        "quote the live rejection verbatim — a rule the model can match "
        "against its own past output is easier to follow than an abstraction")
    assert "precautionary" in AGENT


def test_the_contract_separates_unreachable_from_nonexistent():
    assert '"COULD NOT FETCH" IS NOT "DOES NOT EXIST"' in AGENT
    assert "403" in AGENT
    i = AGENT.index('"COULD NOT FETCH" IS NOT "DOES NOT EXIST"')
    block = AGENT[i:i + 900]
    assert "retry" in block and "not \"reject\"" in block, (
        "the contract must name the verdict to use instead, or the model "
        "has a prohibition and no alternative")


def test_the_new_rules_are_inside_the_prompt_not_a_comment():
    """A rule in a Python comment is a rule the model never sees."""
    start = AGENT.index("5. Decide:")
    end = AGENT.index("Return ONLY this JSON:")
    prompt = AGENT[start:end]
    assert "AN OUTBREAK IS NOT REQUIRED" in prompt
    assert '"COULD NOT FETCH" IS NOT "DOES NOT EXIST"' in prompt


# --------------------------------------------------------------------------
# the heredoc pair — a contract that is not embedded is never sent
# --------------------------------------------------------------------------

def test_the_workflow_copy_is_byte_identical():
    wf = (ROOT / ".github" / "workflows" / "recall-url-agent.yml"
          ).read_text(encoding="utf-8").split("\n")
    s = next(i for i, l in enumerate(wf) if "PYEOF_A1'" in l)
    e = next(i for i, l in enumerate(wf) if l.strip() == "PYEOF_A1" and i > s)
    embedded = "\n".join(l[10:] if l.startswith(" " * 10) else l
                         for l in wf[s + 1:e])
    assert embedded.rstrip("\n") == AGENT.rstrip("\n"), (
        "recall-url-agent.yml writes reviewer 1 from this heredoc; a prompt "
        "change that lands only in the .py is never sent to the model")
