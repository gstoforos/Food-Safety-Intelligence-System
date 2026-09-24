# -*- coding: utf-8 -*-
"""Every path handed to safe_push.sh must be one that can exist.

THE BUG THIS EXISTS TO PREVENT (2026-09-23)
===========================================
Three gap finders passed a REGION-named directory to safe_push.sh:

    scandinavian_gap_finder.yml   docs/data/gap_finder_nordic/
    east_eu_gap_finder.yml        docs/data/gap_finder_easteu/
    central_eu_gap_finder.yml     docs/data/gap_finder_centraleu/

The pipeline never creates such a directory. CountryConfig.data_dir is
f"docs/data/gap_finder_{code}", so only PER-COUNTRY directories ever
exist. africa_gap_finder.yml, doing the same multi-country job, listed
its countries properly and worked.

`git add` is ATOMIC across pathspecs. One pathspec matching nothing aborts
the whole command and stages NOTHING — including docs/data/recalls.xlsx,
which held the run's recalls. safe_push.sh had `git add ... || true`, so
the error was swallowed; `git diff --cached --quiet` then saw an empty
index and the script printed "No changes to commit." and exited 0.

Result: the workflow ran, did the work, discarded it, and went GREEN.
Daily. For months.

Measured on 2026-09-23, from the run logs of that day:

    se  10 rejected rows          cz  5 rejected rows
    no  2 Pending + 13 rejected   md  7 rejected rows
    is  2 rejected rows           hr  1 rejected row

and ZERO rows dated 2026-09-23 in the register from any of them. Norway's
run included four ACCEPTED Listeria recalls.

Nothing in the repository connected a workflow's push paths to the paths
the pipeline actually writes. This file does.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

WORKFLOWS = ROOT / ".github" / "workflows"
SCRIPT = ROOT / "scripts" / "safe_push.sh"

#: docs/data/gap_finder_<code>/ — the only shape the pipeline writes.
GAP_DIR_RE = re.compile(r"^docs/data/gap_finder_([a-z_]+)/?$")

#: A shell variable or expression. Cannot be checked statically; the
#: script's own missing-path handling covers these at run time.
DYNAMIC_RE = re.compile(r"[$`]|\$\{\{")


def _registered_codes() -> set[str]:
    from pipeline.gap_finder.countries import all_codes
    return set(all_codes())


def _safe_push_calls():
    """[(workflow_name, [path, ...])] for every safe_push.sh invocation."""
    calls = []
    for wf in sorted(WORKFLOWS.glob("*.yml")):
        text = wf.read_text(encoding="utf-8", errors="replace")
        # Join continuations so a multi-line invocation is one string.
        joined = text.replace("\\\n", " ")
        for line in joined.splitlines():
            if "safe_push.sh" not in line or line.strip().startswith("#"):
                continue
            after = line.split("safe_push.sh", 1)[1]
            # Drop the quoted commit message, keep the path arguments.
            after = re.sub(r'"[^"]*"', " ", after)
            paths = [t for t in after.split() if "/" in t or t.endswith(".xlsx")]
            if paths:
                calls.append((wf.name, paths))
    return calls


CALLS = _safe_push_calls()


def test_some_workflows_actually_use_safe_push():
    assert CALLS, "found no safe_push.sh invocations — has the parser broken?"


@pytest.mark.parametrize("wf,paths", CALLS, ids=[c[0] for c in CALLS])
def test_every_push_path_can_exist(wf, paths):
    codes = _registered_codes()
    bad = []
    for p in paths:
        if DYNAMIC_RE.search(p):
            continue                       # resolved at run time
        if (ROOT / p.rstrip("/")).exists():
            continue                       # plainly fine
        m = GAP_DIR_RE.match(p)
        if m and m.group(1) in codes:
            continue                       # a country that has not run yet
        bad.append(p)
    assert not bad, (
        f"{wf} passes a path to safe_push.sh that the pipeline can never "
        f"create: {bad}\n"
        f"  The pipeline writes docs/data/gap_finder_<cc>/ per COUNTRY — "
        f"CountryConfig.data_dir is f'docs/data/gap_finder_{{code}}'. A "
        f"region name like gap_finder_nordic/ never exists.\n"
        f"  git add is atomic: one bad pathspec stages NOTHING, including "
        f"recalls.xlsx, and the run's work is discarded with a green tick. "
        f"List the countries individually instead.")


def _script_code() -> str:
    """The script with comment lines stripped.

    The fix's own comment quotes the broken line verbatim, so a naive
    substring search finds it in the explanation and fails. (Third time
    this pattern has bitten in this repo — the dispatch watchdog and the
    reviewer-prefix test both did it.) Check the CODE, not the prose.
    """
    out = []
    for line in SCRIPT.read_text(encoding="utf-8").splitlines():
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue
        out.append(line.split(" #", 1)[0] if " #" in line else line)
    return "\n".join(out)


def test_safe_push_no_longer_swallows_a_failed_add():
    """`git add ... || true` is what made the bug invisible."""
    s = _script_code()
    assert 'git add "${PATHS[@]}" || true' not in s, (
        "safe_push.sh is back to swallowing a failed git add. One bad "
        "pathspec then stages nothing and the script reports success.")
    assert "stage_existing" in s, "the missing-path guard is gone"


def test_safe_push_fails_when_work_would_be_dropped():
    """The canary: xlsx dirty, index empty — that is work about to be lost,
    not 'nothing to commit'. They used to print the same message."""
    s = _script_code()
    assert "assert_not_silently_dropping_work" in s
    assert "git status --porcelain" in s, (
        "the canary no longer checks whether the xlsx actually changed")


@pytest.mark.parametrize("wf,paths", CALLS, ids=[c[0] for c in CALLS])
def test_the_xlsx_is_the_first_path(wf, paths):
    """safe_push.sh's own contract: the first path must be recalls.xlsx,
    because that is the file it row-unions on a push conflict. Anything
    else there loses rows on every concurrent push."""
    concrete = [p for p in paths if not DYNAMIC_RE.search(p)]
    if not concrete:
        pytest.skip("all paths are dynamic")
    assert concrete[0] == "docs/data/recalls.xlsx", (
        f"{wf}: first safe_push path is {concrete[0]!r}, not "
        f"docs/data/recalls.xlsx — see the script's USAGE block")
