"""A green run during an LLM outage must not read as a healthy one.

WHY THIS EXISTS (2026-09-25)
===========================
The Llama box died mid-request that evening. The reviewer chain reported it
loudly and correctly — exit code 3, "*** NO REVIEW PERFORMED — all 12 rows
returned retry ***", workflow red. A gap finder in the same outage behaves
completely differently:

  * extractor.extract_one catches the LlamaError and routes the row to
    REJECTED with RejectionReason "llm-extraction-failed: ...". That handling
    is right and deliberate — a half-parsed row must never reach Pending, and
    the recall is preserved for manual re-extraction rather than dropped.
  * main() then returns `1 if state.errors else 0`, and an LLM failure never
    reaches state.errors. Exit 0.
  * _write_run_log records status="completed".
  * tools/audit_freshness.check_gap_finders grades on the run log's AGE, which
    an outage does not affect, and reports "candidates=14 verified=9" — OK.

So the country ran, found candidates, verified them, wrote rows and committed,
and every real recall went to Rejected. Five are already there: South Africa's
Deli Hummus Listeria recall twice (2026-08-09 and 2026-09-21), Czechia
2026-09-15, Poland 2026-09-19 and 2026-09-21.

This is the DEFAULT_TIMEOUT=45 incident's exact signature — "killed exactly
the rows that classified ACCEPTED and routed them to Rejected" — with the box
DOWN instead of slow. Raising the timeout from 45s to 300s could not help when
nothing is listening on the port.

WHAT IS DELIBERATELY NOT CHANGED: the exit code. That is a decision from
2026-08-24, recorded in every regional finder's workflow — "a VPS outage is
not a workflow failure. Every regional finder hard-exited here, so one
unreachable box turned the whole fleet red and buried the real cause." The
decision stands. The clause that did not hold is the one that followed it,
"the finder itself reports honestly when the model is unavailable": it
reported to stderr and nowhere a machine would look.
"""
from __future__ import annotations

import json

import pytest

from pipeline.gap_finder.main import RunState


LLM_FAIL_REASON = (
    "llm-extraction-failed: Llama timeout or circuit-open; title-only "
    "fallback could not produce clean fields. Not a content verdict — "
    "re-extract when the box is up.")
CONTENT_FAIL_REASON = "not-a-recall: press release about a training programme"


class TestTheCounterExists:

    def test_run_state_carries_it(self):
        assert RunState().llm_extraction_failures == 0

    def test_it_reaches_the_run_log(self):
        """_write_run_log does asdict(state), so a new field lands in the log
        automatically. That is the only durable record a watcher has."""
        from dataclasses import asdict
        rec = asdict(RunState(country_code="se", llm_extraction_failures=3))
        assert rec["llm_extraction_failures"] == 3

    def test_only_llm_failures_are_counted_not_content_rejections(self):
        """The whole point is telling the two apart. A row the RULES rejected
        is the system working; a row the BOX lost is not."""
        rows = [
            {"RejectionReason": LLM_FAIL_REASON},
            {"RejectionReason": LLM_FAIL_REASON},
            {"RejectionReason": CONTENT_FAIL_REASON},
            {"RejectionReason": ""},
            {},
        ]
        counted = sum(1 for r in rows
                      if str(r.get("RejectionReason") or "")
                      .startswith("llm-extraction-failed"))
        assert counted == 2


class TestTheFreshnessAuditGradesOnUsefulness:
    """check_gap_finders used to grade on age alone, and age is precisely what
    an outage leaves untouched."""

    @staticmethod
    def _grade(tmp_path, monkeypatch, record):
        import tools.audit_freshness as A
        d = tmp_path / "gap_finder_se"
        d.mkdir(parents=True)
        (d / "run_log.jsonl").write_text(json.dumps(record) + "\n",
                                         encoding="utf-8")
        monkeypatch.setattr(A, "DATA", tmp_path)
        from datetime import date
        rows = A.check_gap_finders(date.fromisoformat("2026-09-25"))
        assert len(rows) == 1
        return rows[0]

    BASE = dict(country_code="se", started_at="2026-09-25T06:00:00+00:00",
                candidates_found=14, verified_count=9, status="completed")

    def test_a_healthy_run_is_still_ok(self, tmp_path, monkeypatch):
        import tools.audit_freshness as A
        row = self._grade(tmp_path, monkeypatch,
                          dict(self.BASE, extracted_accepted=9,
                               llm_extraction_failures=0))
        assert row["verdict"] == A.OK

    def test_losing_every_accepted_row_is_stale(self, tmp_path, monkeypatch):
        """Fresh timestamp, nine verified candidates, nothing usable
        produced. This is the case that used to read OK."""
        import tools.audit_freshness as A
        row = self._grade(tmp_path, monkeypatch,
                          dict(self.BASE, extracted_accepted=0,
                               llm_extraction_failures=9))
        assert row["verdict"] == A.STALE
        assert "PRODUCED NOTHING USABLE" in row["detail"]
        assert "Llama" in row["detail"]

    def test_losing_some_is_a_warn(self, tmp_path, monkeypatch):
        import tools.audit_freshness as A
        row = self._grade(tmp_path, monkeypatch,
                          dict(self.BASE, extracted_accepted=6,
                               llm_extraction_failures=3))
        assert row["verdict"] == A.WARN
        assert "3 of 9" in row["detail"]

    def test_the_detail_says_these_are_not_content_rejections(self, tmp_path,
                                                              monkeypatch):
        """An operator reading the audit must not conclude the rules refused
        these rows. The distinction is the entire finding."""
        row = self._grade(tmp_path, monkeypatch,
                          dict(self.BASE, extracted_accepted=0,
                               llm_extraction_failures=4))
        assert "NOT content" in row["detail"]

    def test_age_still_dominates_when_it_is_worse(self, tmp_path, monkeypatch):
        """A partial LLM loss must not UPGRADE a genuinely stale country."""
        import tools.audit_freshness as A
        row = self._grade(tmp_path, monkeypatch,
                          dict(self.BASE, started_at="2026-08-01T06:00:00+00:00",
                               extracted_accepted=6, llm_extraction_failures=3))
        assert row["verdict"] == A.STALE


class TestTheExitCodePolicyIsUnchanged:
    """2026-08-24: a VPS outage must not turn the whole fleet red. This test
    exists so the next person changing the visibility does not also quietly
    change the exit contract."""

    def test_main_still_exits_on_state_errors_only(self):
        import inspect
        from pipeline.gap_finder import main as M
        src = inspect.getsource(M.main)
        assert "return 1 if state.errors else 0" in src, (
            "the exit condition moved — if that was deliberate, update the "
            "2026-08-24 note in every regional finder workflow too")

    def test_but_it_raises_an_annotation(self):
        import inspect
        from pipeline.gap_finder import main as M
        src = inspect.getsource(M.main)
        assert "::error title=LLM unavailable" in src
        assert "GITHUB_STEP_SUMMARY" in src
