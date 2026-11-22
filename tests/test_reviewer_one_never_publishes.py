# -*- coding: utf-8 -*-
"""Reviewer 1 verifies and tags. It never writes a row into Recalls.

WHY THIS FILE EXISTS (morning fix 2026-11-22)
=============================================
The rule is locked since 2026-04-30 and written down twice:

  * merge_master.promote_approved's own docstring —
        "Reviewer 1 = Gemini URL gate (pipeline/url_gate_gemini.py)
           • NEVER promotes to Recalls (verify-and-tag-only rule)"
  * pipeline/url_gate_gemini.py, which ends its run with
        new_approved = []           # nothing promoted from this gate
        final_approved = sort_rows(approved)   # Recalls untouched by this gate

pipeline/recall_url_agent.py is the Gemini-free replacement for that agent.
THE REWRITE DROPPED BOTH LINES. It called promote_approved with
``archive_immediately=False`` — which only softens the ARCHIVING half — and
then saved ``approved_existing + new_approved`` straight into Recalls.

MEASURED over its last 14 runs (2026-11-17 → 2026-11-22): it published 14
rows into Recalls on 5 of them, every one before reviewer 2 or reviewer 3
had seen the row.

THE ROW THAT FOUND IT. On 2026-11-20 at 13:48 UTC reviewer 1 published the
NCC (ZA) "Deli Hummus range" media statement. The NCC's own page prints
"Date Posted: October 3, 2024"; the row reached the register with
Date 2026-11-20, Tier 1, inside the open week W47 — over an operator verdict
of 2026-11-06 archiving that exact URL as "TERMINAL, UPHELD … roughly 22
months stale". The operator's note had predicted it in as many words:
"every morning the only thing between it and publication is merge_master's
re-promotion guard".

This test reads the agent's source, because the behaviour is a write and the
agent's main() needs a workbook, a llama endpoint and a network. The three
things it pins are the three the rewrite lost.
"""
from __future__ import annotations

import re
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

AGENT = ROOT / "pipeline" / "recall_url_agent.py"
WORKFLOW = ROOT / ".github" / "workflows" / "recall-url-agent.yml"
HEREDOC = "cat > pipeline/recall_url_agent.py <<'PYEOF_A1'"


def _source() -> str:
    return AGENT.read_text(encoding="utf-8")


class TestReviewerOneNeverPublishes(unittest.TestCase):
    def test_recalls_is_saved_unchanged(self):
        src = _source()
        self.assertNotIn(
            "sort_rows(approved_existing + new_approved)", src,
            "reviewer 1 is writing promote_approved's new_approved into "
            "Recalls again — that is the verify-and-tag-only rule broken, "
            "and it is how the 2024 NCC Deli Hummus statement was published "
            "on 2026-11-20")
        self.assertIn(
            "final_approved = sort_rows(approved_existing)", src,
            "reviewer 1 must save Recalls exactly as it read it")

    def test_rows_it_would_have_promoted_stay_in_pending(self):
        src = _source()
        self.assertIn(
            "sort_rows(remaining + new_approved)", src,
            "rows reviewer 1 would have promoted must go BACK into Pending "
            "for reviewer 3, not be dropped")

    def test_it_still_archives_softly(self):
        self.assertIn("archive_immediately=False", _source(),
                      "reviewer 1 must still stamp rather than hard-evict")


class TestTheWorkflowCopyIsTheSameFile(unittest.TestCase):
    """The workflow REWRITES the agent from a heredoc before running it.

    A fix applied only to pipeline/recall_url_agent.py is discarded at
    runtime, so the two must stay byte-identical.
    """

    def test_heredoc_matches_the_repo_file(self):
        if not WORKFLOW.exists():                       # pragma: no cover
            self.skipTest("workflow not in this checkout")
        lines = WORKFLOW.read_text(encoding="utf-8").split("\n")
        starts = [i for i, l in enumerate(lines) if HEREDOC in l]
        self.assertEqual(1, len(starts),
                         "expected exactly one agent heredoc in the workflow")
        start = starts[0]
        indent = len(lines[start]) - len(lines[start].lstrip())
        end = next(j for j in range(start + 1, len(lines))
                   if lines[j].strip() == "PYEOF_A1")
        body = "\n".join(
            (l[indent:] if l.startswith(" " * indent) else "")
            for l in lines[start + 1:end]) + "\n"
        self.assertEqual(
            _source(), body,
            "the heredoc in .github/workflows/recall-url-agent.yml has "
            "drifted from pipeline/recall_url_agent.py — the workflow "
            "overwrites the file before running it, so the repo copy is not "
            "what runs")

    def test_the_heredoc_also_never_publishes(self):
        if not WORKFLOW.exists():                       # pragma: no cover
            self.skipTest("workflow not in this checkout")
        y = WORKFLOW.read_text(encoding="utf-8")
        self.assertFalse(
            re.search(r"sort_rows\(approved_existing \+ new_approved\)", y),
            "the workflow's own copy of reviewer 1 still publishes")


if __name__ == "__main__":                              # pragma: no cover
    unittest.main()
