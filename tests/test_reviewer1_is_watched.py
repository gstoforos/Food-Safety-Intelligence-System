# -*- coding: utf-8 -*-
"""Reviewer 1 must sign its own commits, or nothing can tell it stopped.

THE BLIND SPOT (audit 2026-09-23)
=================================
``tools/dispatch_watchdog.py`` is the safety net for a fleet whose
scheduling lives outside version control (FsisScheduler.gs). It watches
COMMIT PREFIXES and shouts OVERDUE when a workflow goes quiet.

``recall-url-agent.yml`` — reviewer 1 — committed as::

    Recall review agent: <date> verified promotions

which is reviewer 2's prefix. One watchdog line therefore covered two
agents, and reviewer 2 alone could hold it green.

It did. Measured on 2026-09-23:

* no ``[url-agent …]`` stamp anywhere in the Pending sheet
* the ``[url-guard …]`` notes on nine held rows all dated **2026-09-22**
* every ``FSIS daily update`` commit reading ``+0 approved``
* Recalls flat at 1764 for the whole day
* and the dispatch watchdog reporting nothing

The watchdog's own comment records this exact fix being applied to the
CONFIRM agent on 2026-09-04 — *"the confirm agent now signs its own
commits"*. Reviewer 1 was never given the same treatment, so the repo
carried a documented fix for a documented bug in one of the two places it
occurred.

WHAT IS ASSERTED
----------------
Each of the three review-chain agents commits under a prefix of its own,
and the watchdog has a line for each. A shared prefix is the defect.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

WF = ROOT / ".github" / "workflows"
WATCHDOG = ROOT / "tools" / "dispatch_watchdog.py"

#: workflow file -> the prefix it must commit under
CHAIN = {
    "recall-url-agent.yml": "Recall URL agent:",
    "recall-review-agent.yml": "Recall review agent:",
    "recall-confirm-agent.yml": "Recall confirm agent:",
}


def _commit_step_code(wf_name: str) -> str:
    """The commit step's CODE, with comment lines stripped.

    Two passes are needed and both were learned the hard way:

    * read from the commit step only — recall-url-agent.yml embeds the
      whole reviewer-1 source in a heredoc, and that source names other
      agents in its own comments
    * drop `#` lines — the very comment that explains this fix quotes the
      old prefix, and the first version of this test failed against its
      own fix because of it
    """
    src = (WF / wf_name).read_text(encoding="utf-8")
    step = src[src.index("Commit xlsx changes"):]
    return "\n".join(l for l in step.split("\n")
                     if not l.lstrip().startswith("#"))


def _commit_messages(wf_name: str) -> list[str]:
    return re.findall(r'"(Recall [^"$]*?):', _commit_step_code(wf_name))


@pytest.mark.parametrize("wf,prefix", sorted(CHAIN.items()))
def test_each_agent_signs_its_own_commits(wf, prefix):
    if not (WF / wf).exists():
        pytest.skip(f"{wf} not present")
    msgs = _commit_messages(wf)
    assert msgs, f"{wf} has no recognisable commit message"
    want = prefix.rstrip(":")
    for m in msgs:
        assert m == want, (
            f"{wf} commits as {m!r}, not {want!r} — dispatch_watchdog "
            f"watches prefixes, so a shared one lets a live agent hold a "
            f"dead agent's line green")


def test_no_two_chain_workflows_share_a_prefix():
    seen = {}
    for wf, prefix in CHAIN.items():
        if not (WF / wf).exists():
            continue
        assert prefix not in seen, (
            f"{wf} and {seen[prefix]} both commit {prefix!r} — that is the "
            f"2026-09-23 blind spot")
        seen[prefix] = wf


@pytest.mark.parametrize("prefix", sorted(CHAIN.values()))
def test_the_watchdog_has_a_line_for_each(prefix):
    src = WATCHDOG.read_text(encoding="utf-8")
    pat = "^" + prefix.replace(" ", r"\s") if False else None  # noqa: F841
    esc = prefix.replace("(", r"\(")
    assert re.search(r'r"\^' + re.escape(prefix), src), (
        f"dispatch_watchdog.py does not watch {prefix!r} — an agent nobody "
        f"watches can stop without anything going red")


def test_reviewer_1_is_watched_at_all():
    src = WATCHDOG.read_text(encoding="utf-8")
    assert "Recall URL agent:" in src
    m = re.search(r'\("recall url agent[^"]*",\s*r"\^Recall URL agent:",\s*(\d+)\)', src)
    assert m, "the reviewer 1 watchdog entry is not in the expected shape"
    hours = int(m.group(1))
    assert 24 <= hours <= 48, (
        f"threshold {hours}h — reviewer 1 is dispatched twice a day, so "
        f"under 24h cries wolf on one slip and over 48h hides a whole day")


def test_the_old_shared_prefix_is_gone_from_reviewer_1():
    assert '"Recall review agent:' not in _commit_step_code(
        "recall-url-agent.yml"), (
        "reviewer 1 still commits under reviewer 2's prefix")


def test_the_embedded_agent_is_still_byte_identical():
    """The commit step is OUTSIDE the heredoc, so this change must not have
    disturbed it. Cheap to assert, and the pair has drifted before."""
    py = ROOT / "pipeline" / "recall_url_agent.py"
    if not py.exists():
        pytest.skip("no reviewer 1 source")
    wf = (WF / "recall-url-agent.yml").read_text(encoding="utf-8").split("\n")
    s = next(i for i, l in enumerate(wf) if "PYEOF_A1'" in l)
    e = next(i for i, l in enumerate(wf) if l.strip() == "PYEOF_A1" and i > s)
    embedded = "\n".join(l[10:] if l.startswith(" " * 10) else l
                         for l in wf[s + 1:e])
    assert embedded.rstrip("\n") == py.read_text(encoding="utf-8").rstrip("\n")
