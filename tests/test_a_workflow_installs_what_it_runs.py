"""A workflow that runs pytest must install pytest.

THE INCIDENT (2026-09-29)
=========================
`offline-enrich-and-promote.yml` died with:

    /opt/hostedtoolcache/Python/3.11.16/x64/bin/python: No module named pytest
    Error: Process completed with exit code 1.

It installed `requirements.txt` and then ran the register guard tests.
`pytest` is not in `requirements.txt` and never should be — that file is
deliberately the RUNTIME-ONLY set, so workflows which merely run the pipeline
do not pull a test stack. `pytest` lives in `requirements-dev.txt`.

The guard step sits between the promotion and the commit, so the job went red
on every run while the work itself had already succeeded. (The commit still
happened — that step became `if: always()` on 2026-09-28 for exactly this
class of failure — but a permanently red job is a job nobody reads, and the
next real failure hides inside it.)

This is the second workflow bug of this shape in two days: the first was
`verify_urls --apply` inheriting the AUDIT exit code and killing the run for
defects it could not fix. Both share a root: a step failing for a reason
unrelated to the work it guards.
"""
from __future__ import annotations

from pathlib import Path

import pytest, yaml

ROOT = Path(__file__).resolve().parents[1]
WF = ROOT / ".github" / "workflows"


def _workflows_running_pytest():
    out = []
    for p in sorted(WF.glob("*.yml")):
        t = p.read_text(encoding="utf-8", errors="ignore")
        if "pytest" in t and "pip install" in t:
            out.append(p)
    return out


def test_there_is_something_to_check():
    assert _workflows_running_pytest(), (
        "no workflow both installs dependencies and runs pytest — the "
        "detection here has drifted, not the workflows")


@pytest.mark.parametrize("wf", _workflows_running_pytest(), ids=lambda p: p.name)
def test_a_workflow_that_runs_pytest_installs_the_dev_requirements(wf):
    body = "\n".join(l for l in wf.read_text(encoding="utf-8").splitlines()
                     if not l.strip().startswith("#"))
    if "pytest" not in body:
        pytest.skip("pytest appears only in comments here")
    assert "requirements-dev.txt" in body, (
        f"{wf.name} runs pytest but never installs requirements-dev.txt. "
        f"pytest is NOT in requirements.txt by design — that file is the "
        f"runtime-only set. Without the dev set the step dies with "
        f"'No module named pytest' and the job is red on every run.")


def test_requirements_txt_stays_runtime_only():
    """The other half of the contract: do not 'fix' this by moving pytest.

    Adding pytest to requirements.txt would make every pipeline workflow
    install a test stack it never uses. The split is deliberate; the fix
    belongs in the workflow that needs the tests.
    """
    req = ROOT / "requirements.txt"
    if not req.exists():                                    # pragma: no cover
        pytest.skip("no requirements.txt")
    body = "\n".join(l for l in req.read_text(encoding="utf-8").splitlines()
                     if not l.strip().startswith("#"))
    assert "pytest" not in body.lower(), (
        "pytest has been added to requirements.txt. That is the wrong fix: "
        "requirements.txt is the runtime-only set, and every pipeline "
        "workflow would start installing a test stack. Install "
        "requirements-dev.txt in the workflow that runs the tests instead.")
