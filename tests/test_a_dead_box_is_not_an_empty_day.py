# -*- coding: utf-8 -*-
"""A gap-finder run that extracted NOTHING must not read as a quiet day.

THE INCIDENT — 2026-11-19, THE WHOLE FLEET
==========================================
Every gap finder dispatched this morning reported a clean, completed run and
produced not one row. Measured on docs/data/gap_finder_*/run_log.jsonl:

    country  candidates  verified  accepted  rejected  llm_failures  errors
    de              175        45         0         0             0      []
    at               95        19         0         0             0      []
    se               66        31         0         0             0      []
    be               16        14         0         0             0      []
    nl               12        10         0         0             0      []
    pl               17        12         0         0             0      []
    dk               20         7         0         0             0      []
    no               11         7         0         0             0      []
    hu                5         5         0         0             0      []
    ch                4         3         0         0             0      []
    is                3         3         0         0             0      []
    fi                2         2         0         0             0      []

Twelve of twelve countries with verified candidates. 158 verified candidates,
every one dropped. status "completed", exit 0, errors [], and
tools/audit_freshness.check_gap_finders graded all twelve OK, because it keys
on llm_extraction_failures and that was zero. Zero runs of this shape on any
of the eight preceding days — de, at, se, be, nl and ch each extracted rows
on 11-16, 11-17 and 11-18.

THE CAUSE
---------
pipeline/gap_finder/main.stage_extractor opened with

    if not client.health():
        print("  ERROR: cannot reach llama-server at ...", file=sys.stderr)
        return [], []

so when the Llama box was down BEFORE the first request, Stage 3 returned
before touching a record. Nothing classified, nothing rejected, nothing to
count, no state.error.

This is the 2026-09-25 incident
(tests/test_an_llm_outage_is_not_a_clean_run.py) with the box down before the
run instead of during it. The counter written that day —
llm_extraction_failures, derived from rejected rows carrying
"llm-extraction-failed" — cannot see this case, because there are no rejected
rows. The 2026-09-25 note ends "the finder itself reports honestly when the
model is unavailable"; on this path the honest report went to stderr and
nowhere a machine looks.

THE FIX THIS FILE PINS
----------------------
1. The loss is RECORDED: RunState gains llm_unreachable and
   llm_lost_before_first_request, both of which reach run_log.jsonl through
   asdict(), and main() folds the lost count into llm_extraction_failures —
   the one field the freshness audit already grades as STALE.
2. Per-record exceptions in Stage 3 are counted too (extraction_errors). The
   loop around extract_one prints "ERROR record <i>" and continues, so a
   defect throwing on every record produced the same all-zero log.
3. audit_freshness grades the SHAPE, not just the counter: verified > 0 with
   nothing produced either way is STALE however fresh the timestamp.
4. The exit code is UNCHANGED. 2026-08-24 stands: a VPS outage must not turn
   the whole fleet red.
"""
from __future__ import annotations

import inspect
import json
from dataclasses import asdict
from datetime import date

import pytest

from pipeline.gap_finder.main import RunState


# ── 1. THE LOSS IS RECORDED ───────────────────────────────────────────────

class TestTheCountersExist:

    def test_run_state_carries_them(self):
        s = RunState()
        assert s.llm_unreachable is False
        assert s.llm_lost_before_first_request == 0
        assert s.extraction_errors == 0

    def test_they_reach_the_run_log(self):
        """_write_run_log does asdict(state), so a field lands in the log
        automatically. That log is the only durable record a watcher has."""
        rec = asdict(RunState(country_code="de", llm_unreachable=True,
                              llm_lost_before_first_request=45,
                              extraction_errors=3))
        assert rec["llm_unreachable"] is True
        assert rec["llm_lost_before_first_request"] == 45
        assert rec["extraction_errors"] == 3
        json.dumps(rec)          # the log is JSONL; it has to serialise


class TestStageExtractorRecordsAnUnreachableBox:

    def test_the_health_check_branch_sets_the_counters(self, monkeypatch):
        """The exact 2026-11-19 path: health() False, verified non-empty."""
        from pipeline.gap_finder import main as M

        class _DeadClient:
            base_url = "http://box:8080"
            model = "m"

            @staticmethod
            def reset_breaker():
                pass

            def health(self):
                return False

        monkeypatch.setattr(M.extractor, "LlamaClient", _DeadClient)
        state = RunState(country_code="de")
        verified = [{"efet_title": f"t{i}"} for i in range(45)]
        pending, rejected = M.stage_extractor(
            verified, cfg=object(), state=state)

        assert (pending, rejected) == ([], [])
        assert state.llm_unreachable is True
        assert state.llm_lost_before_first_request == 45

    def test_state_is_optional(self, monkeypatch):
        """The parameter must not be required, or every other caller breaks."""
        from pipeline.gap_finder import main as M

        class _DeadClient:
            base_url = "http://box:8080"
            model = "m"

            @staticmethod
            def reset_breaker():
                pass

            def health(self):
                return False

        monkeypatch.setattr(M.extractor, "LlamaClient", _DeadClient)
        assert M.stage_extractor([{"efet_title": "t"}], cfg=object()) == ([], [])

    def test_per_record_exceptions_are_counted(self, monkeypatch):
        """The other silent path out of Stage 3: `except ...: continue`."""
        from pipeline.gap_finder import main as M

        class _LiveClient:
            base_url = "http://box:8080"
            model = "m"

            @staticmethod
            def reset_breaker():
                pass

            def health(self):
                return True

        def _boom(v, client, cfg, verbose=False):
            raise RuntimeError("extractor defect")

        monkeypatch.setattr(M.extractor, "LlamaClient", _LiveClient)
        monkeypatch.setattr(M.extractor, "extract_one", _boom)
        monkeypatch.setattr(M.extractor, "write_jsonl",
                            lambda rows, path: None)

        class _Cfg:
            pending_path = "/dev/null"
            rejected_path = "/dev/null"

        state = RunState(country_code="de")
        pending, rejected = M.stage_extractor(
            [{"efet_title": "a"}, {"efet_title": "b"}], cfg=_Cfg(),
            state=state)
        assert (pending, rejected) == ([], [])
        assert state.extraction_errors == 2
        # and not mistaken for a model outage
        assert state.llm_unreachable is False

    def test_main_folds_the_loss_into_the_graded_counter(self):
        """llm_extraction_failures is the field audit_freshness grades, so
        main() must add the pre-flight loss to it — otherwise the recording
        above is true and still invisible."""
        src = inspect.getsource(
            __import__("pipeline.gap_finder.main", fromlist=["main"]).main)
        assert ("state.llm_extraction_failures += "
                "state.llm_lost_before_first_request") in src, (
            "main() no longer folds llm_lost_before_first_request into "
            "llm_extraction_failures. If that moved, point this test at the "
            "new path — do not delete it: the fold is what makes the "
            "freshness audit see a box that was down before the run.")


# ── 2. THE EXIT CONTRACT IS UNCHANGED ─────────────────────────────────────

def test_the_exit_code_policy_still_stands():
    """2026-08-24: one unreachable box must not turn the whole fleet red."""
    from pipeline.gap_finder import main as M
    assert "return 1 if state.errors else 0" in inspect.getsource(M.main)


def test_an_unreachable_box_is_not_a_state_error():
    """It must not reach state.errors, which is what the exit code reads."""
    from pipeline.gap_finder import main as M
    src = inspect.getsource(M.stage_extractor)
    head = src[:src.index("pending_rows: list[dict] = []")]
    assert "add_error" not in head, (
        "stage_extractor now records a state error before classifying — that "
        "turns the whole fleet red on a VPS outage, which 2026-08-24 "
        "explicitly decided against.")


# ── 3. THE AUDIT GRADES THE SHAPE, NOT JUST THE COUNTER ───────────────────

class TestTheFreshnessAuditSeesTheShape:
    """The backstop. main() folds the loss into llm_extraction_failures, but
    the audit must also catch the SHAPE arriving any other way — an older
    finder, a future early return, a per-record defect."""

    BASE = dict(country_code="de", started_at="2026-11-19T03:43:17+00:00",
                candidates_found=175, status="completed")

    @staticmethod
    def _grade(tmp_path, monkeypatch, record):
        import tools.audit_freshness as A
        d = tmp_path / "gap_finder_de"
        d.mkdir(parents=True)
        (d / "run_log.jsonl").write_text(json.dumps(record) + "\n",
                                         encoding="utf-8")
        monkeypatch.setattr(A, "DATA", tmp_path)
        rows = A.check_gap_finders(date.fromisoformat("2026-11-19"))
        assert len(rows) == 1
        return rows[0]

    def test_the_exact_2026_11_19_log_is_stale(self, tmp_path, monkeypatch):
        """Fresh timestamp, 45 verified candidates, nothing produced, no
        counted failure. This is the log that read OK for twelve countries."""
        import tools.audit_freshness as A
        row = self._grade(tmp_path, monkeypatch,
                          dict(self.BASE, verified_count=45,
                               extracted_accepted=0, extracted_rejected=0,
                               llm_extraction_failures=0, errors=[]))
        assert row["verdict"] == A.STALE
        assert "PRODUCED NOTHING USABLE" in row["detail"]
        assert "NOT content" in row["detail"], (
            "an operator reading this must not conclude the rules refused "
            "these rows")

    def test_a_healthy_run_is_still_ok(self, tmp_path, monkeypatch):
        import tools.audit_freshness as A
        row = self._grade(tmp_path, monkeypatch,
                          dict(self.BASE, verified_count=45,
                               extracted_accepted=2, extracted_rejected=43,
                               llm_extraction_failures=0))
        assert row["verdict"] == A.OK

    def test_rejections_alone_are_still_a_working_run(self, tmp_path,
                                                      monkeypatch):
        """Most countries accept nothing on most days — se, nl and be
        rejected every verified row on 11-16, 11-17 and 11-18 and were
        working. Producing only rejections must stay OK."""
        import tools.audit_freshness as A
        row = self._grade(tmp_path, monkeypatch,
                          dict(self.BASE, verified_count=31,
                               extracted_accepted=0, extracted_rejected=31,
                               llm_extraction_failures=0))
        assert row["verdict"] == A.OK

    def test_a_country_that_verified_nothing_is_not_stale_for_it(
            self, tmp_path, monkeypatch):
        """lu, jp, hr, ee, eg and mk all run with verified_count 0 on a
        normal day. Nothing to extract is not the same as losing everything."""
        import tools.audit_freshness as A
        row = self._grade(tmp_path, monkeypatch,
                          dict(self.BASE, candidates_found=0,
                               verified_count=0, extracted_accepted=0,
                               extracted_rejected=0,
                               llm_extraction_failures=0))
        assert row["verdict"] == A.OK

    def test_the_counted_outage_keeps_its_own_wording(self, tmp_path,
                                                      monkeypatch):
        """The 2026-09-25 path must not be swallowed by the new one: when
        rows WERE routed to Rejected, the detail has to keep saying so."""
        import tools.audit_freshness as A
        row = self._grade(tmp_path, monkeypatch,
                          dict(self.BASE, verified_count=9,
                               extracted_accepted=0, extracted_rejected=9,
                               llm_extraction_failures=9))
        assert row["verdict"] == A.STALE
        assert "Llama" in row["detail"]

    def test_an_unreachable_box_is_named_in_the_detail(self, tmp_path,
                                                       monkeypatch):
        row = self._grade(tmp_path, monkeypatch,
                          dict(self.BASE, verified_count=45,
                               extracted_accepted=0, extracted_rejected=0,
                               llm_extraction_failures=0,
                               llm_unreachable=True))
        assert "health check" in row["detail"]

    def test_per_record_errors_are_named_in_the_detail(self, tmp_path,
                                                       monkeypatch):
        row = self._grade(tmp_path, monkeypatch,
                          dict(self.BASE, verified_count=45,
                               extracted_accepted=0, extracted_rejected=0,
                               llm_extraction_failures=0,
                               extraction_errors=45))
        assert "45 per-record error" in row["detail"]


# ── 4. THE OTHER FINDERS' HEALTH GATES ────────────────────────────────────

@pytest.mark.parametrize("rel", [
    "pipeline/extractor.py",
    "pipeline/official_feeds/extractor.py",
    "pipeline/gap_finder/extractor.py",
    "pipeline/gap_finder_gr/extractor.py",
])
def test_the_standalone_extractor_clis_still_fail_loudly(rel):
    """These four each have their own health gate in a CLI main(). They
    `return 1`, which is loud, and that is why none of them was part of the
    2026-11-19 incident. Pinned so a later edit does not quietly make one of
    them return 0 or an empty result the way stage_extractor did.

    Read as TEXT, not imported: two of the four resolve their rules module
    through the sys.path their workflow sets, so importing them here fails on
    the import line and tells us nothing about the health gate.
    """
    from pathlib import Path
    src = (Path(__file__).resolve().parents[1] / rel).read_text(
        encoding="utf-8")
    i = src.index("cannot reach llama-server")
    tail = src[i:i + 400]
    assert "return 1" in tail, (
        f"{rel}'s health gate no longer returns a failing status. A silent "
        f"return is the 2026-11-19 defect: pipeline/gap_finder/main."
        f"stage_extractor returned ([], []) there and the whole fleet "
        f"reported twelve clean runs with 158 candidates dropped.")


def test_the_greek_finder_still_raises():
    """pipeline/gap_finder_gr/main.py raises LlamaError at its own health
    gate, which is why Greece could not hit this. Recorded so the two
    finders' different answers to the same question stay a deliberate
    difference rather than a surprise."""
    from pathlib import Path
    src = (Path(__file__).resolve().parents[1]
           / "pipeline" / "gap_finder_gr" / "main.py").read_text(
        encoding="utf-8")
    i = src.index("llama-server unreachable at")
    assert "raise LlamaError" in src[max(0, i - 200):i], (
        "the Greek finder's health gate no longer raises. If it was aligned "
        "with the parametric finder on purpose, it must set the same "
        "counters instead — not return quietly.")
