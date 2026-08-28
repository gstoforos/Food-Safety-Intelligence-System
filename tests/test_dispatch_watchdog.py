"""The scheduler watchdog must notice silence, and must not cry wolf.

Every workflow that collects, reviews or publishes is dispatched by an Apps
Script outside this repository. When it stops, nothing in GitHub goes red.
These tests pin the two behaviours that make the watchdog worth having:
a workflow that has never committed is overdue, and a workflow that
committed recently is not.
"""
from datetime import datetime, timedelta, timezone

import pytest

from tools import dispatch_watchdog as DW


def _fake_log(entries):
    """entries: list of (hours_ago, subject)."""
    now = datetime.now(timezone.utc)
    return [(now - timedelta(hours=h), s) for h, s in entries]


def test_a_workflow_that_never_committed_is_overdue(monkeypatch):
    monkeypatch.setattr(DW, "_log", lambda days: [])
    for r in DW.check():
        assert r["overdue"], r["label"]
        assert r["count"] == 0
        assert r["last"] is None


def test_a_recent_commit_clears_the_workflow(monkeypatch):
    monkeypatch.setattr(DW, "_log", lambda days: _fake_log([
        (1, "FSIS daily update 2026-08-28 (+0 approved, 4 pending)"),
    ]))
    got = {r["label"]: r for r in DW.check()}
    assert got["scrape fleet (run_all)"]["overdue"] is False
    assert got["scrape fleet (run_all)"]["count"] == 1


def test_an_old_commit_does_not_clear_the_workflow(monkeypatch):
    """The failure being guarded against is a fleet that ran last week and
    has been silent since — not one that has never run."""
    monkeypatch.setattr(DW, "_log", lambda days: _fake_log([
        (200, "FSIS daily update 2026-08-20 (+0 approved, 4 pending)"),
    ]))
    got = {r["label"]: r for r in DW.check()}
    assert got["scrape fleet (run_all)"]["overdue"] is True


def test_the_analytical_sweep_is_watched():
    """Nothing else writes the fourteen analytical columns. If the sweep
    stops, the register keeps growing and the statistical corpus does not,
    and no other check in the repo would see it."""
    labels = [w[0] for w in DW.WATCHED]
    assert "analytical schema sweep" in labels


def test_report_exit_code_follows_overdue(monkeypatch, capsys):
    monkeypatch.setattr(DW, "_log", lambda days: [])
    assert DW.report(DW.check()) == 1
    out = capsys.readouterr().out
    assert "OVERDUE" in out


def test_a_message_that_merely_contains_the_phrase_does_not_count(monkeypatch):
    """Patterns are anchored at the start of the subject so an operator
    commit that mentions a workflow by name cannot mark it as alive."""
    monkeypatch.setattr(DW, "_log", lambda days: _fake_log([
        (1, "fix: stop AFTS Weekly Report build from double-counting"),
    ]))
    got = {r["label"]: r for r in DW.check()}
    assert got["weekly report"]["overdue"] is True


def test_the_never_run_workflows_found_by_the_2026_08_28_audit_are_watched():
    """Three things had gone quiet for months with nothing noticing: the
    monthly report build (no commit since May, across three Day-1
    dispatches), the AI synthesis writers (never scheduled, never run), and
    the analytical schema sweep (written but wired to nothing)."""
    labels = [w[0] for w in DW.WATCHED]
    for want in ("monthly report", "ai synthesis", "analytical schema sweep"):
        assert want in labels, want
