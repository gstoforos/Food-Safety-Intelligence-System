# -*- coding: utf-8 -*-
"""Benjamini-Hochberg must be corrected over the tests, not the survivors.

THE DEFECT (audit 2026-09-21)
=============================
``pipeline/signal_detector.py`` called::

    keep = benjamini_hochberg([c.p_value for c in candidates], FDR_Q)

``candidates`` holds only the strata that already cleared
``prop_hit or count_hit``; everything else hit ``continue`` further up and
was never appended. So BH's denominator was the count of things ALREADY
FOUND SIGNIFICANT.

On the 2026-09-14/20 run that was 3, giving thresholds 0.033 / 0.067 /
0.100. Every survivor of an alpha = 0.01 screen clears those by
construction, and the published meta recorded exactly that::

    "candidates": 3, "after_fdr": 3

Selecting on the outcome and then correcting for multiplicity over the
selected set controls nothing. BH's guarantee is over the whole family of
tests, so ``m`` is ``strata_tested`` — the strata that received a p-value
at all — which was 15.

Redone honestly on that week::

    k=1  p=0.00426  <=  1*0.10/15 = 0.00667   PASS   STEC - France
    k=2  p=0.00690  <=  2*0.10/15 = 0.01333   PASS   Salmonella - France
    k=3  p=0.02516  <=  3*0.10/15 = 0.02000   FAIL   Aflatoxin - Europe

The published page showed all three as ``FDR: pass``.

THE SAME MISTAKE, ALREADY CAUGHT ONCE
-------------------------------------
``signal_detector.py`` carries this comment from 2026-09-02:

    "the meta field `strata_tested` reported len(candidates) — the number
     that ALARMED"

It was fixed in the REPORTING and left in the STATISTIC. That is the
reason this file asserts against both.

A SECOND SCOPE FIX RODE ALONG
-----------------------------
The page says BH "controls the expected false-discovery proportion among
share-channel hits". A count-only candidate carries the SHARE test's
p-value even though the share test is not what fired it, so those
p-values do not belong in the family. Including them could only loosen
the threshold for the rows the family does cover. BH now runs over the
proportion channel alone, and every row publishes an ``fdr_status`` that
names which test the verdict came from.

WHAT DOES *NOT* CHANGE: count-only signals are still published. That
bypass is deliberate and the page states it outright. What changes is the
label — ``n/a · count`` instead of ``pass``.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline.signal_detector import benjamini_hochberg as bh  # noqa: E402

#: The three p-values published for 2026-09-14/2026-09-20.
LIVE_P = [0.004255644364634047, 0.00689941948066981, 0.0251599919607981]
LIVE_LABELS = ["STEC · France", "Salmonella · France", "Aflatoxin · Europe"]
LIVE_TESTED = 15
Q = 0.1


# --------------------------------------------------------------------------
# the incident
# --------------------------------------------------------------------------

def test_the_shipped_denominator_passed_everything():
    """Reproduce the defect, so the fix below is measured against it."""
    assert bh(LIVE_P, Q) == [True, True, True], (
        "with m = len(pvals) every survivor of the alpha screen passes; "
        "this is the behaviour that published Aflatoxin as FDR-pass")


def test_the_honest_denominator_drops_the_third():
    assert bh(LIVE_P, Q, m=LIVE_TESTED) == [True, True, False], (
        "over the 15 strata that were actually tested, the step-up rejects "
        "the first two only")


def test_the_arithmetic_is_reproducible_by_hand():
    """A reader must be able to redo this without running the code."""
    ordered = sorted(LIVE_P)
    thresholds = [(k / LIVE_TESTED) * Q for k in (1, 2, 3)]
    assert ordered[0] <= thresholds[0]
    assert ordered[1] <= thresholds[1]
    assert ordered[2] > thresholds[2], (
        "0.02516 must exceed 3*0.1/15 = 0.02 — if this ever flips, the "
        "worked example in every docstring above is wrong")


# --------------------------------------------------------------------------
# the function
# --------------------------------------------------------------------------

def test_m_defaults_to_len_for_a_caller_that_tested_everything():
    p = [0.001, 0.5]
    assert bh(p, Q) == bh(p, Q, m=2)


def test_a_denominator_below_n_is_refused():
    """m < n would make the correction WEAKER than no correction.

    Not an assertion error — a silent anti-conservative mask is worse than
    a loud one, so the function clamps rather than trusting the caller.
    """
    assert bh(LIVE_P, Q, m=1) == bh(LIVE_P, Q, m=3)


def test_a_larger_family_is_strictly_harder():
    """Monotone in m: more tests can never make a rejection easier."""
    prev = 99
    for m in (3, 15, 50, 500):
        n = sum(bh(LIVE_P, Q, m=m))
        assert n <= prev, f"m={m} rejected more than a smaller family"
        prev = n


def test_nothing_survives_an_enormous_family():
    assert bh(LIVE_P, Q, m=100000) == [False, False, False]


def test_an_empty_family_is_not_an_error():
    assert bh([], Q, m=15) == []


def test_bh_is_step_up_not_step_down():
    """A large p between two small ones must not break the run.

    Step-DOWN would stop at the first failure and lose the third; step-up
    takes the LARGEST passing rank and rejects everything below it.
    """
    p = [0.001, 0.09, 0.002]          # unsorted on purpose
    assert bh(p, 0.5, m=3) == [True, True, True]


# --------------------------------------------------------------------------
# the call site — reading the source, because the defect was in the call
# --------------------------------------------------------------------------

SRC = (ROOT / "pipeline" / "signal_detector.py").read_text(encoding="utf-8-sig")


def test_the_call_site_passes_the_tested_count():
    assert "benjamini_hochberg([c.p_value for c in share_c], FDR_Q, m=tested)" in SRC, (
        "the detector must correct over `tested`, not over len(candidates) "
        "— see this module's docstring for what that cost")


def test_the_old_call_is_gone():
    assert "benjamini_hochberg([c.p_value for c in candidates], FDR_Q)" not in SRC


def test_bh_runs_over_the_share_channel_only():
    assert 'share_c = [c for c in candidates if c.channel == "proportion"]' in SRC, (
        "a count-only row carries the share test's p-value but was not "
        "fired by it; those hypotheses are not in the family the page "
        "claims to control")


def test_the_denominator_is_published():
    assert '"fdr_m": tested,' in SRC, (
        "a reader cannot redo the step-up from the published board unless "
        "the board says what m was")


def test_count_only_rows_are_still_published():
    """The bypass is deliberate and documented. Do not quietly remove it."""
    assert 'c.fdr_pass or c.channel == "count-only"' in SRC, (
        "count-only signals bypass FDR by design and the page says so; "
        "this fix changes the LABEL, not which rows are reported")


def test_every_row_says_which_test_its_verdict_came_from():
    assert 'c.fdr_status = ("not-applicable" if c.channel == "count-only"' in SRC


# --------------------------------------------------------------------------
# the page must not claim more than the code does
# --------------------------------------------------------------------------

PAGE = (ROOT / "docs" / "signals.html").read_text(encoding="utf-8")


def test_the_page_no_longer_says_several_hundred():
    assert "Several hundred strata per run" not in PAGE, (
        "484 strata exist, 15 were tested, 3 entered BH — 'several hundred' "
        "was true of none of those numbers")


def test_the_page_states_the_real_denominator():
    assert "${meta.strata_tested||0} strata received a p-value" in PAGE
    assert "${meta.fdr_m||meta.strata_tested||0}" in PAGE


def test_the_page_renders_the_status_not_the_boolean():
    assert "s.fdr_pass?'pass':'—'" not in PAGE, (
        "rendering fdr_pass beside a count-only row prints a verdict from "
        "a test that row did not take")
    assert "s.fdr_status==='not-applicable'?'n/a · count'" in PAGE


# --------------------------------------------------------------------------
# live board sanity — skipped when the board predates the fix
# --------------------------------------------------------------------------

def test_the_live_board_agrees_with_an_independent_recompute():
    import json
    bp = ROOT / "docs" / "data" / "signals-board.json"
    if not bp.exists():
        pytest.skip("no board")
    d = json.loads(bp.read_text(encoding="utf-8-sig"))
    meta = d.get("meta") or {}
    if "fdr_m" not in meta:
        pytest.skip("board predates the fix — regenerate with signal_detector")
    share = [s for s in (d.get("signals") or [])
             if s.get("channel") == "proportion"]
    expect = bh([s["p_value"] for s in share], float(meta["fdr_q"]),
                m=int(meta["fdr_m"]))
    got = [bool(s.get("fdr_pass")) for s in share]
    assert got == expect, (
        "the published FDR verdicts do not survive an independent recompute")
