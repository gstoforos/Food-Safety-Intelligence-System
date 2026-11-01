"""The monthly social PNGs need a rasteriser the workflow actually installs.

MEASURED, 2026-11-01. The afts-monthly-report run that committed October 2026
(a235f20b) wrote docs/social/2026-M10-{linkedin,instagram,x}.pdf and NO .png.
pipeline.build_monthly_social.rasterise_png imports pypdfium2, falls back to
pdf2image, and returns None quietly when neither imports — and neither was in
requirements.txt, the only file the workflow pip-installs. Instagram and X
accept images only, so the cards that matter most were the ones skipped.
"""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _requirements() -> set[str]:
    names = set()
    for line in (ROOT / "requirements.txt").read_text(encoding="utf-8").splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            names.add(re.split(r"[<>=!~\[ ;]", line, 1)[0].lower())
    return names


def test_a_rasteriser_is_installed_by_the_monthly_workflow():
    wf = (ROOT / ".github/workflows/afts-monthly-report.yml").read_text(encoding="utf-8")
    assert "pip install -r requirements.txt" in wf
    src = (ROOT / "pipeline/build_monthly_social.py").read_text(encoding="utf-8")
    used = [m for m in ("pypdfium2", "pdf2image") if f"import {m}" in src
            or f"from {m}" in src]
    assert used, "build_monthly_social no longer names a rasteriser — re-read it"
    assert _requirements() & set(used), (
        f"requirements.txt installs none of {used}: the day-1 workflow writes "
        "the social PDFs and silently skips every PNG")
