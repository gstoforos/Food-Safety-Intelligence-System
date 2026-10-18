"""The writer's bilingual split must fire on a RASFF subject that carries a
complete English half (2026-10-18). It used to be gated behind
looks_non_english(), which is False for exactly those strings, so it never
fired; ten published RASFF rows carried both languages."""
from __future__ import annotations

import json
from pathlib import Path

from pipeline._language import split_bilingual

ROOT = Path(__file__).resolve().parent.parent

CASES = [
    ("przekroczenie NDP ochratoksyny A w mące żytniej TYP 720 pochodzącej z Polski "
     "///exceedance of the maximum permitted level (MPL) for ochratoxin A in type 720 "
     "rye flour originating from Poland", "exceedance of the maximum permitted level"),
    ("Presenza di E Coli STEC in formaggio a latte crudo // Presence of STEC E. coli in "
     "raw milk cheese", "Presence of STEC"),
]


def test_the_split_keeps_the_english_half():
    for raw, want in CASES:
        got = split_bilingual(raw)
        assert got and got.startswith(want), (raw, got)


def test_the_writer_no_longer_requires_a_foreign_looking_whole():
    src = (ROOT / "pipeline" / "merge_master.py").read_text(encoding="utf-8")
    assert "not isinstance(_v, str) or not _looks_non_english(_v)" not in src


def test_no_published_rasff_row_is_still_bilingual():
    rows = json.loads((ROOT / "docs" / "data" / "recalls.json").read_text(encoding="utf-8-sig"))
    bad = [(r["Date"], r["Product"][:60]) for r in rows
           if r.get("Source") == "RASFF (EU)"
           and any((split_bilingual(r.get(c) or "") or r.get(c)) != r.get(c)
                   for c in ("Product", "Reason") if r.get(c))]
    assert not bad, bad
