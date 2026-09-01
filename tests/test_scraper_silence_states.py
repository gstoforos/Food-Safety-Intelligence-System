"""Regression: "OK_EMPTY" must not absorb silently-broken scrapers.

pipeline/scraper_health_audit.py exists because 36 of 66 scrapers were
failing silently and the orchestrator's summary masked it. It fixed that
for HARD failures (404/403/DNS/SSL/timeout) and then grew a softer blind
spot of its own: any scraper reporting "[DONE] ... 0" that could not be
matched to a failure line was bucketed OK_EMPTY, which the docstring
described as a "legitimately empty window".

That description was an assumption, not a measurement. Measured on
2026-09-01: 40 of 55 scrapers were OK_EMPTY on the same day, and 26 of
them had never written a single row to the register in its entire
history. "Legitimately empty" cannot describe a scraper that has never
produced anything.

These tests pin the three-way split and, critically, the thin-history
path — the first cut of the refinement required 3 rows before judging a
source and therefore flagged NOTHING, hiding AGES Austria (113 days
silent against a 23-day cadence) behind an OK_EMPTY.
"""
from __future__ import annotations

import json
from datetime import date, timedelta

import pytest

from pipeline.scraper_health_audit import (
    _display_to_source_key,
    refine_empty_states,
    register_activity,
)


def _write(tmp_path, rows):
    p = tmp_path / "recalls.json"
    p.write_text(json.dumps(rows), encoding="utf-8")
    return p


def test_display_to_source_key_is_exact_not_fuzzy():
    """A fuzzy match once collapsed FDA (PH), FDA (GH), NAFDAC (NG) and
    TFDA (TW) onto the US FDA's rows and produced four wrong numbers."""
    assert _display_to_source_key("AESAN (ES)/Spain") == "AESAN (ES)"
    assert _display_to_source_key("FDA/USA") == "FDA"
    assert _display_to_source_key("FDA (PH)/Philippines") == "FDA (PH)"
    assert _display_to_source_key("FDA (GH)/Ghana") != "FDA"


def test_never_produced_is_not_called_empty(tmp_path):
    rj = _write(tmp_path, [{"Source": "FDA", "Date": "2026-08-29"}])
    act = register_activity(rj)
    refined, ev = refine_empty_states({"KEBS (KE)/Kenya": "OK_EMPTY"}, act)
    assert refined["KEBS (KE)/Kenya"] == "NEVER_PRODUCED"
    assert ev["KEBS (KE)/Kenya"]["rows"] == 0


def test_recent_producer_stays_ok_empty(tmp_path):
    today = date.today()
    rows = [{"Source": "FDA", "Date": str(today - timedelta(days=d))}
            for d in (1, 3, 6, 9, 12)]
    rj = _write(tmp_path, rows)
    act = register_activity(rj)
    refined, _ = refine_empty_states({"FDA/USA": "OK_EMPTY"}, act)
    assert refined["FDA/USA"] == "OK_EMPTY"


def test_source_silent_beyond_its_own_cadence_is_flagged(tmp_path):
    """AGES Austria: a steady cadence, then 113 days of nothing."""
    base = date.today() - timedelta(days=113)
    rows = [{"Source": "AGES (AT)", "Date": str(base - timedelta(days=d))}
            for d in (0, 20, 40, 60)]
    rj = _write(tmp_path, rows)
    act = register_activity(rj)
    refined, ev = refine_empty_states({"AGES (AT)/Austria": "OK_EMPTY"}, act)
    assert refined["AGES (AT)/Austria"] == "SILENT_STALE"
    assert ev["AGES (AT)/Austria"]["threshold_basis"].startswith("2x")


def test_thin_history_source_is_still_judged(tmp_path):
    """A single row 237 days ago has no cadence of its own. The first cut
    of this code SKIPPED such sources and so flagged nothing at all."""
    today = date.today()
    rows = [{"Source": "ANMAT (AR)", "Date": str(today - timedelta(days=237))}]
    # pooled gaps must exist for the fallback to be computable
    rows += [{"Source": "FDA", "Date": str(today - timedelta(days=d))}
             for d in (1, 4, 9, 30, 90)]
    rj = _write(tmp_path, rows)
    act = register_activity(rj)
    refined, ev = refine_empty_states({"ANMAT (AR)/Argentina": "OK_EMPTY"}, act)
    assert refined["ANMAT (AR)/Argentina"] == "SILENT_STALE"
    assert "pooled" in ev["ANMAT (AR)/Argentina"]["threshold_basis"]


def test_hard_failures_are_never_reclassified(tmp_path):
    rj = _write(tmp_path, [{"Source": "FDA", "Date": "2026-08-29"}])
    act = register_activity(rj)
    states = {"AESAN (ES)/Spain": "FAIL_404", "MoH (IL)/Israel": "FAIL_403"}
    refined, _ = refine_empty_states(states, act)
    assert refined == states


def test_missing_register_degrades_quietly(tmp_path):
    act = register_activity(tmp_path / "does-not-exist.json")
    assert act == {}
    refined, _ = refine_empty_states({"X/Y": "OK_EMPTY"}, act)
    assert refined["X/Y"] == "NEVER_PRODUCED"
