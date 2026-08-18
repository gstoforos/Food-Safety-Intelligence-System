"""Repo-wide undefined-name guard.

WHY
===
On 2026-08-18 the UK collector died at 02:10 UTC with

    NameError: name 'date' is not defined. Did you mean: 'data'?

Two names — `date` and `since` — were used in pipeline/official_feeds/
sources/uk.py and defined nowhere in it. The file imported cleanly, the
syntax was valid, and the 484-test suite was green, because nothing ever
executed that function. A whole class of shipped-broken changes looks
exactly like this.

A scan for undefined names catches it in under a second, with no test
data, no fixtures and no network. Running it on the day it was written
also found two more that nobody had noticed:

    pipeline/sunday_gemini_qa.py:1087  api_fixes
    pipeline/sunday_gemini_qa.py:1088  gemini_url_fixes

Both sat inside `if not dry_run:` immediately after `wb.save(XLSX_PATH)`,
so every real Sunday QA run saved its fixes and then raised NameError
before rebuilding a single daily brief.

This is a floor, not a substitute for exercising the code — see
test_official_feeds_uk.py for that. It only proves every name resolves.

pyflakes is imported unconditionally on purpose. `pytest.importorskip`
would turn a missing dependency into a silent skip, which is the same
green-while-broken shape this test exists to remove. It is pinned in
requirements-dev.txt.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pyflakes  # noqa: F401  — hard requirement; see docstring

ROOT = Path(__file__).resolve().parents[1]

# Directories that ship code the pipeline actually runs.
SCANNED = ("pipeline", "scrapers", "tools", "enrichment", "tests")

# Files at the repo root that are entry points, not scratch.
ROOT_FILES = ("build_weekly_report_afts.py", "build_monthly_report_afts.py")


def _targets() -> list[str]:
    out = [str(ROOT / d) for d in SCANNED if (ROOT / d).is_dir()]
    out += [str(ROOT / f) for f in ROOT_FILES if (ROOT / f).is_file()]
    return out


def test_no_undefined_names_anywhere():
    proc = subprocess.run(
        [sys.executable, "-m", "pyflakes", *_targets()],
        capture_output=True, text=True,
    )
    bad = [ln for ln in proc.stdout.splitlines()
           if "undefined name" in ln or "undefined local" in ln]
    assert not bad, (
        "Undefined names — these are NameErrors waiting for the code path "
        "to run:\n  " + "\n  ".join(bad))


def test_no_star_imports_hiding_undefined_names():
    """`from x import *` makes the check above unable to see what is
    defined, so pyflakes downgrades every unknown name to a maybe. One
    star import anywhere blinds the guard for that whole file."""
    proc = subprocess.run(
        [sys.executable, "-m", "pyflakes", *_targets()],
        capture_output=True, text=True,
    )
    bad = [ln for ln in proc.stdout.splitlines()
           if "unable to detect undefined names" in ln]
    assert not bad, (
        "Star imports blind the undefined-name guard:\n  " + "\n  ".join(bad))
