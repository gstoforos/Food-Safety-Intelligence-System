"""A comment line inside a backslash-continued shell command ends it.

MEASURED, 2026-11-01. .github/workflows/afts-monthly-report.yml, step
"Wire pdf_url into monthly-index.json", carried five comment lines between
`--docs-dir docs \\` and `--site-url ...` (added 2026-08-11). bash joins the
continued line with the comment, the comment swallows the rest, and the
command runs without --site-url: set_pdf_urls exits 2, the job fails, and
the "Commit outputs" step after it never runs. The October 2026 monthly
report was built on the runner and discarded; August and September had to
be uploaded by hand.

This holds the shape across EVERY workflow: no line ending in a backslash
may be followed by a comment line in the same run block.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = sorted((ROOT / ".github" / "workflows").glob("*.yml"))


def _offenders(run: str):
    lines = run.split("\n")
    for i in range(len(lines) - 1):
        if lines[i].rstrip().endswith("\\") and lines[i + 1].lstrip().startswith("#"):
            yield i + 1, lines[i + 1].strip()


@pytest.mark.parametrize("wf", WORKFLOWS, ids=lambda p: p.name)
def test_no_comment_line_continues_a_command(wf):
    doc = yaml.safe_load(wf.read_text(encoding="utf-8")) or {}
    bad = []
    for job_name, job in (doc.get("jobs") or {}).items():
        for step in job.get("steps") or []:
            run = step.get("run")
            if not isinstance(run, str):
                continue
            for n, text in _offenders(run):
                bad.append(f"{job_name} / {step.get('name')!r} line {n}: {text[:70]}")
    assert not bad, "comment inside a continued command (ends it early):\n" + "\n".join(bad)


def test_the_detector_sees_the_2026_11_01_shape():
    run = "python -m x \\\n  --a 1 \\\n# note\n  --b 2\n"
    assert list(_offenders(run)) == [(2, "# note")]


def test_the_monthly_pdf_url_step_passes_site_url():
    doc = yaml.safe_load((ROOT / ".github/workflows/afts-monthly-report.yml").read_text())
    step = next(s for s in doc["jobs"]["build"]["steps"] if s["name"].startswith("Wire pdf_url"))
    cmd = step["run"][step["run"].index("python -m pipeline.set_pdf_urls"):]
    joined = cmd.replace("\\\n", " ")
    first = joined.split("\n")[0]
    assert "--site-url" in first and "--index" in first and "--docs-dir" in first
