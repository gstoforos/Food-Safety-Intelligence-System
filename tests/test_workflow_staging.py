"""A workflow that regenerates a file must also commit it.

Found 2026-08-27. `docs/data/weekly-review-latest.json` is the slice the
Thursday 17:00 mailer fetches. Sixteen workflows regenerate it — and exactly
one stages it for commit: the Thursday wipe, whose job is to EMPTY it. Every
workflow that filled it discarded the result when its runner was destroyed,
so the only version ever pushed was an emptied one. The mailer therefore sent
"0 recalls added" for a week with 32 promotions, reading a file frozen at
2026-08-13T14:40:19Z.

Nothing crashed. No workflow failed. The file was written correctly every
hour and thrown away. These tests exist because that failure mode is
invisible to every other kind of check.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
WF_DIR = ROOT / ".github" / "workflows"

# Module invoked -> files it writes that MUST reach the repo.
# Keep this table honest: an entry here is a promise the module keeps.
WRITES = {
    "pipeline.merge_master": [
        "docs/data/recalls.xlsx",
        "docs/data/recalls.json",
        "docs/data/weekly-review-latest.json",
        "docs/data/weekly-rejected-latest.json",
    ],
    "pipeline.claude_check": [
        "docs/data/recalls.xlsx",
        "docs/data/recalls.json",
        "docs/data/weekly-review-latest.json",
        "docs/data/weekly-rejected-latest.json",
    ],
}


def _workflows():
    if not WF_DIR.is_dir():                                # pragma: no cover
        pytest.skip("no .github/workflows in this checkout")
    return sorted(p for p in WF_DIR.glob("*.yml"))


def _staged_paths(text: str) -> str:
    """Everything this workflow ever passes to `git add`, as one blob.

    Shell line continuations are folded first: a `git add` whose paths run
    over several backslash-continued lines is the normal way to stage more
    than two files, and a matcher that stopped at the newline would report
    those files as unstaged.
    """
    folded = re.sub(r"\\\s*\n\s*", " ", text)
    return "\n".join(re.findall(r"git\s+add\s+([^\n|&;]+)", folded))


def _commits(text: str) -> bool:
    return bool(re.search(r"git\s+commit", text))


@pytest.mark.parametrize("wf", _workflows(), ids=lambda p: p.name)
def test_workflow_stages_what_it_regenerates(wf: Path):
    text = wf.read_text(encoding="utf-8")
    if not _commits(text):
        pytest.skip("does not commit")
    staged = _staged_paths(text)
    if not staged.strip():
        pytest.skip("commits nothing via git add (uses another mechanism)")
    # `git add -A` stages the whole tree, which covers every output by
    # definition. Narrower is better practice, but it is not this bug.
    if re.search(r"git\s+add\s+(-A|--all|\.)(\s|$)", text):
        pytest.skip("stages the whole tree with `git add -A`")

    missing = []
    for module, outputs in WRITES.items():
        if not re.search(rf"python\s+-m\s+{re.escape(module)}\b", text):
            continue
        for out in outputs:
            if out not in staged:
                missing.append(f"{module} writes {out}")

    assert not missing, (
        f"{wf.name} runs a module that regenerates files it never stages, so "
        f"the work is discarded when the runner exits:\n  "
        + "\n  ".join(missing))


def test_the_mailer_slice_is_staged_by_a_producer_not_only_the_wipe():
    """The bug in one assertion.

    If the only workflow committing the slice is the one that empties it,
    the published file can only ever be empty.
    """
    target = "docs/data/weekly-review-latest.json"
    producers, wipers = [], []
    for wf in _workflows():
        text = wf.read_text(encoding="utf-8")
        if target not in _staged_paths(text):
            continue
        (wipers if "wipe" in wf.name else producers).append(wf.name)

    assert producers, (
        f"{target} is staged only by {wipers or 'nothing'} — no workflow that "
        f"FILLS it ever commits it, so the mailer can only ever read an "
        f"emptied file")
