"""The dashboard counts the sources the pipeline MONITORS.

    "also number of sources...40? we have add more and more" — operator, 2026-10-01

The Sources tile counted distinct Source values among published rows, so a
regulator scraped daily but with no in-scope recall yet did not count; the
header carried a hand-typed "66". tools/monitored_sources.py is now the one
list, written to docs/data/sources.json, and both read from it.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools.monitored_sources import (  # noqa: E402
    NOT_A_SOURCE_PREFIXES, SOURCES, payload,
)


def test_no_source_is_listed_twice():
    labels = [l for l, _, _ in SOURCES]
    assert len(labels) == len(set(labels))


def test_every_gap_finder_country_is_counted():
    domains = {h for _, _, d in SOURCES for h in d.split()}
    missing = []
    for p in sorted((ROOT / "pipeline" / "gap_finder" / "countries").glob("*.py")):
        m = re.search(r'authority_domain="([^"]+)"', p.read_text(encoding="utf-8"))
        if m and m.group(1) not in domains:
            missing.append((p.stem, m.group(1)))
    assert not missing, f"monitored but not counted: {missing}"


def test_every_published_source_is_counted():
    rows = json.loads((ROOT / "docs" / "data" / "recalls.json")
                      .read_text(encoding="utf-8"))
    labels = {l for l, _, _ in SOURCES}
    missing = sorted({str(r.get("Source")) for r in rows
                      if str(r.get("Source")) not in labels
                      and not str(r.get("Source")).startswith(NOT_A_SOURCE_PREFIXES)})
    assert not missing, f"add to tools/monitored_sources.py: {missing}"


def test_the_published_json_is_in_step():
    on_disk = json.loads((ROOT / "docs" / "data" / "sources.json")
                         .read_text(encoding="utf-8"))
    assert on_disk == payload(), "run: python -m tools.monitored_sources"


def test_the_dashboard_reads_the_list_not_the_rows():
    html = (ROOT / "docs" / "index.html").read_text(encoding="utf-8")
    assert "data/sources.json" in html
    assert "Sources monitored" in html
    assert "66 sources" not in html
