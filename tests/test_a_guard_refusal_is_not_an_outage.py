# -*- coding: utf-8 -*-
"""A guard refusal is a verdict. It must never be filed as an outage.

THE LIVELOCK (2026-09-29)
=========================
Reviewer 1 failed every scheduled run with exit code 3. The log:

    [searx] 0 result(s) for '...'
    [searx] EMPTY — the model is told not to reword; if this repeats, check SEARX_URL.
    [url-guard] reject refused: the row already carries an official regulator
                URL (https://rappel.conso.gouv.fr/fiche-rappel/23644/interne)
    ... x4
    confirm: 0  reject: 0  retry (infra, left in Pending): 4
    *** NO REVIEW PERFORMED — all 4 rows returned retry. ***
    Nothing was written. The queue is untouched.

Every step of that is a component behaving correctly except one.

Searx returned nothing, so the model had nothing to cite and fell back to "No
official regulator URL found" on every row. The url-guard refused all four —
correctly: each row already carries a per-recall notice on its regulator's own
host, and "no official URL found" is a claim about REACHABILITY, not about
existence. Several regulator hosts return 403 to datacentre traffic for every
page they serve.

The defect was that a refusal was counted as ``retry``. ``retry`` means the
infrastructure failed and NO verdict exists. So a run in which every row was
refused looked identical to a total llama outage, main() took the all-retry
branch, and **returned 3 before the write-back** — discarding the refusal note
it had just written into that row's Notes.

That closed the loop. ``refusal_count()`` reads those notes. With them thrown
away it always returned 0, so ``_prior >= 1`` was never true, so the
escalation added on 2026-09-25 — the mechanism whose entire purpose is to stop
a row being asked the same question forever — could never fire. Ten rows were
doing exactly that when the escalation was written; it had been dead ever
since, because the two changes were made a week apart and neither author
looked at the other.

WHAT THIS FILE SWEEPS
---------------------
All three reviewers, not just the one that broke. Reviewer 2 turned out NOT to
have the bug — its guard refusals live in the ``reject`` bucket and are
handled after the abort, so its retry counter really is infra-only — and that
is worth pinning rather than assuming, because the next person to read the two
identical-looking abort blocks will assume they are identical.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

REVIEWERS = {
    "reviewer 1 (url agent)": "pipeline/recall_url_agent.py",
    "reviewer 2 (review agent)": "pipeline/recall_review_agent.py",
    "reviewer 3 (confirm agent)": "pipeline/recall_confirm_agent.py",
}


def _live_source(rel: str) -> str:
    """Source with `#` comment lines removed.

    The comments explaining this incident necessarily quote the removed line.
    A test that read them as code would fail on its own explanation — which
    has happened in this repo five times.
    """
    p = ROOT / rel
    if not p.exists():                                       # pragma: no cover
        pytest.skip(f"{rel} not present")
    return "\n".join(l for l in p.read_text(encoding="utf-8").splitlines()
                     if not l.strip().startswith("#"))


def test_there_are_reviewers_to_sweep():
    assert all((ROOT / r).exists() for r in REVIEWERS.values())


@pytest.mark.parametrize("name,rel", sorted(REVIEWERS.items()))
def test_no_reviewer_counts_a_guard_refusal_as_an_infra_retry(name, rel):
    src = _live_source(rel)
    marker = "[url-guard] reject refused"
    if marker not in src:
        pytest.skip(f"{name} has no url-guard refusal branch")
    start = src.index(marker)
    # End the branch at the first thing that is definitely outside it. The
    # per-run SUMMARY legitimately prints counts["retry"], so a window that
    # runs past the branch flags the very line that reports the fix.
    ends = [src.find(m, start) for m in (
        'rejected_flags[idx] = f"URL agent:',
        "rejected_flags[idx] = f\"Review agent:",
        "\n    if _stopped_early",
        "\n        print(f\"  [{n}/",
    )]
    ends = [e for e in ends if e > start]
    region = src[start:min(ends)] if ends else src[start:start + 1200]
    assert not re.search(r'counts\[[\'"]retry[\'"]\]', region), (
        f"{name} increments the RETRY counter inside its url-guard refusal "
        f"branch. retry means 'the infrastructure failed and no verdict "
        f"exists'; a refusal is the opposite — the guard reached a verdict. "
        f"Conflating them makes an all-refused run look like an outage, and "
        f"the all-retry abort then returns before the write-back, discarding "
        f"the refusal note that the escalation counts.")


@pytest.mark.parametrize("name,rel", sorted(REVIEWERS.items()))
def test_the_all_retry_abort_sits_above_the_write_back(name, rel):
    """Not a complaint — a fact this file depends on, pinned so it stays true.

    The abort returning early is deliberate: a genuine outage must not write.
    It is exactly because it sits above the write-back that misclassifying a
    row as retry destroys data.
    """
    src = _live_source(rel)
    if "NO REVIEW PERFORMED" not in src:
        pytest.skip(f"{name} has no all-retry abort")
    abort = src.index("NO REVIEW PERFORMED")
    assert "save_xlsx_with_pending" in src[abort:], (
        f"{name}'s write-back is no longer below the abort — if it moved "
        f"above, re-read this test's docstring before changing anything")


def test_reviewer_one_distinguishes_the_two():
    src = _live_source("pipeline/recall_url_agent.py")
    assert '"refused"' in src, (
        "reviewer 1 must carry a separate refusal counter")
    _abort = src.index("NO REVIEW PERFORMED")
    # From the run summary to the banner: the abort condition and
    # whatever it is computed from.
    guard = src[src.rindex("print(f\"{'='*60}\")", 0, _abort):_abort]
    assert "refused" in guard, (
        "the abort must consult the refusal count, or a run of nothing but "
        "refusals is still mistaken for an outage")


def test_reviewer_two_retry_is_infra_only():
    """Verified, not assumed: reviewer 2 never had this bug.

    Its only source of a retry verdict is the `infra` dict, and its guard
    refusals are handled in the reject bucket AFTER the abort. Pinned so that
    the next person reading two identical-looking abort blocks does not
    'fix' the one that is already correct — or, worse, assume the one that
    is broken is fine because this one is.
    """
    src = _live_source("pipeline/recall_review_agent.py")
    verdicts = re.findall(r'["\']verdict["\']\s*:\s*["\']retry["\']', src)
    assert len(verdicts) == 1, (
        f"reviewer 2 now has {len(verdicts)} places that mint a retry verdict; "
        f"when it was one, that one was the infra dict. Check the new ones "
        f"are not guard refusals wearing an outage's name.")
    i = src.rindex("infra = {", 0, src.index('"verdict": "retry"') + 1) \
        if 'infra = {' in src else -1
    assert i >= 0 and src.index('"verdict": "retry"') - i < 200, (
        "the single retry verdict is no longer the one inside `infra` — that "
        "is the shape reviewer 1's bug had")


# --------------------------------------------------------------------------
# the other half: the probe that let it happen
# --------------------------------------------------------------------------

SEARX_WORKFLOWS = ("recall-url-agent.yml", "recall-review-agent.yml")


@pytest.mark.parametrize("wf", SEARX_WORKFLOWS)
def test_the_searx_probe_requires_a_non_empty_result_set(wf):
    """Reachable is not useful — the same invariant as the lying 404.

    The probe passed on a grep for the string "results", which an EMPTY
    "results": [] satisfies. Searx was up and returning nothing; the check
    went green and the reviewer spent its slot producing verdicts that had to
    be refused. A dependency that answers is not a dependency that works.
    """
    p = ROOT / ".github" / "workflows" / wf
    if not p.exists():                                       # pragma: no cover
        pytest.skip(f"{wf} not present")
    body = p.read_text(encoding="utf-8")
    assert "Health-check Searx" in body, f"{wf} no longer probes Searx"
    assert """grep -q '"results"'""" not in body, (
        f"{wf} is back to grepping for the string \"results\" — an empty "
        f"results array passes that, which is how a useless Searx was "
        f"declared reachable on 2026-09-29")
    assert "-gt 0" in body, (
        f"{wf}'s Searx probe does not require at least one result")


@pytest.mark.parametrize("wf", SEARX_WORKFLOWS)
def test_the_searx_probe_is_a_single_line_of_python(wf):
    """A multi-line `python3 -c` literal dedents to column 0 and ends the
    YAML block scalar early — a workflow that does not parse at all. That is
    how the first attempt at this fix broke both files."""
    p = ROOT / ".github" / "workflows" / wf
    if not p.exists():                                       # pragma: no cover
        pytest.skip(f"{wf} not present")
    # The property is that the `-c` literal CLOSES on the line it opens on.
    # (It used to test what the line ends with, which rejects a valid
    # `python3 -c '...' 2>/dev/null) || n=-1` on one line.)
    import re as _re
    for line in p.read_text(encoding="utf-8").splitlines():
        if "python3 -c" in line:
            assert _re.search(r"""python3 -c (?:'[^']*'|"[^"]*")""", line), (
                f"{wf}: the python probe spills onto another line")


@pytest.mark.parametrize("wf", SEARX_WORKFLOWS)
def test_the_reviewer_stops_when_searx_is_down(wf):
    p = ROOT / ".github" / "workflows" / wf
    if not p.exists():                                       # pragma: no cover
        pytest.skip(f"{wf} not present")
    body = p.read_text(encoding="utf-8")
    assert "Stop early when a dependency is down" in body
    assert "steps.searx.outputs.reachable != 'true'" in body, (
        f"{wf} probes Searx and then runs anyway — the probe is decoration")
