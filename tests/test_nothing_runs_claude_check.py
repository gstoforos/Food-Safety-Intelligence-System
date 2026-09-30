"""Nothing runs claude_check (operator ruling, 2026-09-30).

"We have it there, the claude_check, but it must not be in any code — we
have our own agents." pipeline/claude_check.py stays in the repository as
history. No workflow may execute it, and no live pipeline module may import
it — review is done by recall_url_agent, recall_review_agent and
recall_confirm_agent.

openrouter_check.py and gemini_check.py are wrappers that import
claude_check and swap its model; they are listed for deletion and must not
be run either. This sweeps every workflow and every pipeline module, so a
new caller fails here rather than being found later.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
WF = sorted((ROOT / ".github" / "workflows").glob("*.yml"))
BANNED_MODULES = ("claude_check", "openrouter_check", "gemini_check")
# The module itself and its two retired wrappers (pending deletion).
EXEMPT = {"claude_check.py", "openrouter_check.py", "gemini_check.py"}


def _live(text: str) -> str:
    return "\n".join(l for l in text.splitlines()
                     if not l.strip().startswith("#"))


@pytest.mark.parametrize("wf", WF, ids=lambda p: p.name)
def test_no_workflow_runs_it(wf):
    live = _live(wf.read_text(encoding="utf-8"))
    for m in BANNED_MODULES:
        assert not re.search(rf"python3?\s+(-m\s+)?pipeline[./]{m}\b", live), (
            f"{wf.name} runs pipeline.{m}")
        assert f"pipeline/{m}.py" not in live, (
            f"{wf.name} patches or copies pipeline/{m}.py")


@pytest.mark.parametrize("py", sorted((ROOT / "pipeline").rglob("*.py")),
                         ids=lambda p: str(p.relative_to(ROOT)))
def test_no_live_module_imports_it(py):
    if py.name in EXEMPT:
        return
    live = _live(py.read_text(encoding="utf-8"))
    assert not re.search(
        r"^\s*(from\s+pipeline(\.claude_check|\s+import\s+claude_check)"
        r"|import\s+(pipeline\.)?claude_check)", live, re.M), (
        f"{py.relative_to(ROOT)} imports claude_check")


def test_the_file_is_kept():
    assert (ROOT / "pipeline" / "claude_check.py").exists()
