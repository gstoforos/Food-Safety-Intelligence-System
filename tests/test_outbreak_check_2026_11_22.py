# -*- coding: utf-8 -*-
"""Outbreak check 2026-11-22: two FSA alerts were published twice.

1. promote_gate_passing never ran recall_confirm_agent.duplicate_problems,
   so the FSA article/<n> copy of an alert already published at its
   fsa-prin URL went straight to Recalls (Hotel Chocolat, article/7000).
2. _is_terminal_rejection read any "superseded" as "not a verdict", so a
   duplicate verdict written beside that word could not stop re-promotion
   (Greencore, article/6920, back on 2026-11-20).
"""
from pipeline.merge_master import _is_terminal_rejection
from pipeline.recall_confirm_agent import duplicate_problems

CANON = {"Date": "2026-11-21", "Source": "FSA (UK)", "Company": "Hotel Chocolat",
         "Pathogen": "Salmonella",
         "URL": "https://alerts.food.gov.uk/news-alerts/alert/fsa-prin-54-2026"}
COPY = dict(CANON, URL=("https://alerts.food.gov.uk/article/7000/Hotel-Chocolat-"
                        "recalls-several-chocolate-products-because-of-the-"
                        "possible-presence-of-Salmonella"))


def test_article_copy_of_published_fsa_alert_is_a_duplicate():
    assert duplicate_problems(COPY, [CANON])


def test_promote_gate_passing_asks_the_duplicate_rule():
    from pathlib import Path
    src = (Path(__file__).resolve().parent.parent / "pipeline"
           / "promote_gate_passing.py").read_text(encoding="utf-8")
    assert "duplicate_problems(r, rec)" in src


def test_duplicate_verdict_is_terminal_even_beside_superseded():
    d = ("outbreak check: Removed — duplicate_of_published_recall — same alert "
         "| [re-promotion allowed: the archive row is explicitly marked SUPERSEDED]")
    assert _is_terminal_rejection(d) is True


def test_plain_superseded_row_still_releases():
    assert _is_terminal_rejection(
        "[SUPERSEDED 2026-10-11 — defect repaired] allergen: out of scope") is False
