# -*- coding: utf-8 -*-
"""Every ``run:`` block in every workflow must be valid shell.

WHY THIS EXISTS
---------------
On 2026-09-16 the "Stop early when a dependency is down" step was added to
both reviewer workflows. Its whole purpose was to replace an uninformative
red build with a named cause: which of llama / llama-tools / Searx was
down, printed as a GitHub annotation and a step summary table.

It contained one stray character::

    echo '```'"

The trailing double-quote opened a string that was never closed. Bash
parses a script in full before executing any of it, so the step did not
merely lose its summary table — **nothing in it ran at all**, including
the ``::error title=Reviewer dependency down::`` annotation on its first
line. The step exited 2 with "Process completed with exit code 2", and
the failure email said nothing about why.

The diagnostic step built to end silent failures failed silently. It ran
that way for a day.

A syntax error of this kind is invisible to every check the repo had:
YAML parses fine (the shell is just a string), Python tests never touch
it, and the only feedback is a red run in production. That is exactly the
class of problem this repo keeps paying for — something that looks
deployed, is deployed, and does nothing.

WHAT THIS CHECKS
----------------
``bash -n`` on every ``run:`` block: parse, do not execute. It cannot
tell you the step does the right thing; it tells you the step will run at
all, which is the part that was missing.

``${{ ... }}`` expressions are replaced with a literal token first. They
are substituted by the Actions runner before bash sees them, so leaving
them in would produce false failures on ``${{ }}`` inside quotes — and,
worse, would hide real errors behind noise.
"""

from __future__ import annotations

import re
import subprocess
import tempfile
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"

#: Actions substitutes these before the shell runs. Replaced with a bare
#: word so the shell sees something syntactically inert.
_EXPR = re.compile(r"\$\{\{[^}]*\}\}")


def _run_blocks():
    """(workflow, job, step name, script) for every shell run: block."""
    out = []
    for f in sorted(WORKFLOWS.glob("*.y*ml")):
        try:
            doc = yaml.safe_load(f.read_text(encoding="utf-8"))
        except yaml.YAMLError as exc:                     # noqa: PERF203
            out.append((f.name, "-", "<the YAML itself>", None, str(exc)))
            continue
        for job_name, job in (doc.get("jobs") or {}).items():
            if not isinstance(job, dict):
                continue
            for idx, step in enumerate(job.get("steps") or []):
                if not isinstance(step, dict):
                    continue
                script = step.get("run")
                if not isinstance(script, str):
                    continue
                # Only shells bash -n understands. A `shell: python` block
                # is a different language and not this test's business.
                shell = str(step.get("shell") or "bash")
                if shell not in ("bash", "sh"):
                    continue
                name = step.get("name") or "step #%d" % (idx + 1)
                out.append((f.name, job_name, name, script, None))
    return out


BLOCKS = _run_blocks()


def test_there_are_workflows_to_check():
    """A zero-length list would make every test below pass vacuously."""
    assert WORKFLOWS.is_dir(), "no .github/workflows directory"
    assert len(BLOCKS) > 50, (
        "only %d run: blocks found — the collector is probably broken, and a "
        "broken collector is a green suite that checks nothing" % len(BLOCKS))


@pytest.mark.parametrize(
    "wf,job,step,script,yaml_err",
    BLOCKS,
    ids=["%s::%s" % (b[0], b[2])[:90] for b in BLOCKS],
)
def test_run_block_is_parseable_shell(wf, job, step, script, yaml_err):
    if yaml_err is not None:
        pytest.fail("%s does not parse as YAML: %s" % (wf, yaml_err))

    with tempfile.NamedTemporaryFile("w", suffix=".sh", encoding="utf-8",
                                     delete=False) as fh:
        fh.write(_EXPR.sub("SUBST", script))
        path = fh.name

    proc = subprocess.run(["bash", "-n", path], capture_output=True, text=True)
    Path(path).unlink(missing_ok=True)

    assert proc.returncode == 0, (
        "%s :: job %s :: step %r is not valid shell, so NOTHING in it will "
        "run — bash parses the whole script before executing the first line:"
        "\n\n%s" % (wf, job, step, proc.stderr.strip()))
