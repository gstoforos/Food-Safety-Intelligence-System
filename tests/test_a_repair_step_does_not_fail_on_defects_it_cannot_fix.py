"""An audit may fail a build. A repair may not fail it for unrelated defects.

THE INCIDENT (2026-09-28)
=========================
`offline-enrich-and-promote.yml` ran its third step, `verify_urls --apply`,
and exited 1. GitHub killed the job there. Everything after it —
`promote_gate_passing`, the weekly rebuild, the signals rebuild and the commit
— never executed. The one part of this system that keeps working while the
Llama box is down was being stopped on every single run.

Nothing was wrong with the repair. `verify_urls` returned `1 if flagged`, and
the register carries 8 structural defects that cannot be fixed offline: the
EFET press-release landing page, a CFIA row citing hortidaily.com, and six BVL
rows citing produktwarnung.eu — a private aggregator, not the German
regulator. The repair step was failing the build because an audit it also
performs found defects it was never asked to fix.

THE RULE
--------
`--apply` is a repair action: its exit code reports whether the repair worked.
The bare invocation is an audit: exit 1 for "problems found" is correct there
and a workflow may gate on it. This file pins both halves, and pins that the
workflow still reaches its commit.
"""
from __future__ import annotations

import subprocess, sys
from pathlib import Path

import pytest, yaml

ROOT = Path(__file__).resolve().parents[1]
WF = ROOT / ".github" / "workflows" / "offline-enrich-and-promote.yml"


def _run(*args):
    return subprocess.run([sys.executable, "-m", "pipeline.verify_urls", *args],
                          cwd=ROOT, capture_output=True, text=True, timeout=600)


@pytest.mark.slow
def test_apply_exits_zero_even_when_unfixable_defects_exist():
    r = _run("--apply")
    assert r.returncode == 0, (
        "verify_urls --apply exited "
        f"{r.returncode}. A repair step must not fail the build because the "
        "audit found defects it cannot fix — that killed every run of the "
        "offline loop on 2026-09-28, before promote, the rebuilds and the "
        "commit.")


@pytest.mark.slow
def test_the_bare_audit_still_reports_problems_with_exit_one():
    r = _run()
    assert r.returncode == 1, (
        "the bare audit should still exit 1 while the 8 known structural "
        "defects exist; exit 0 here would mean the audit stopped detecting "
        "them, which is worse than the build break it replaced.")


def test_the_workflow_always_reaches_its_commit():
    if not WF.exists():                                     # pragma: no cover
        pytest.skip("workflow not present")
    d = yaml.safe_load(WF.read_text(encoding="utf-8"))
    steps = list(d["jobs"].values())[0]["steps"]
    commit = [s for s in steps if str(s.get("name", "")).strip().lower() == "commit"]
    assert commit, "the workflow has no Commit step"
    cond = str(commit[0].get("if", ""))
    assert "always()" in cond, (
        "the Commit step is conditional on every previous step succeeding. A "
        "promotion that worked must not be discarded because the weekly "
        "builder failed after it: the rebuilds retry next run, but an "
        "unstaged workbook change is overwritten by the next hourly job and "
        "lost for good.")
