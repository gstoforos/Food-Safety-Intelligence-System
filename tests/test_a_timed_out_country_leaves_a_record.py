"""A gap-finder country killed by the fleet's per-country cap must say so.

WHY (morning fix 2026-11-01)
============================
pipeline/gap_finder/main._write_run_log's docstring promises that "every
exit writes a record", after the 2026-09-16 audit found fourteen countries
reading as dark because a crash unwound past the append. The promise held
for every exit Python controls. It did not hold for the one the fleet
workflow actually uses:

    if timeout 12m python -m pipeline.gap_finder.main --country "$cc"

``timeout`` sends SIGTERM, whose default disposition kills the interpreter
outright — no exception, no unwinding, no ``except BaseException``, no
append. The shell logs "TIMEOUT $cc" into a job log with a 14-day
retention, and run_log.jsonl, the only durable record, stays silent.

MEASURED. ``us`` last wrote a record on 2026-10-14 and was on
tools/fleet_shard.py's catch-up list — hence dispatched — on all eight
days from 2026-10-24 to 2026-10-31. Eight dispatches, zero records, and
on 2026-11-01 tests/test_no_country_goes_dark.py failed with the only
reading the register allowed: "'us' last ran 2026-10-14, 17 days ago".

These tests hold the two halves of the fix: the signal becomes an
exception the guard can see, and the workflow still guarantees the process
dies even if the handler does not return.
"""

from __future__ import annotations

import re
import signal
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
MAIN = ROOT / "pipeline" / "gap_finder" / "main.py"
FLEET = ROOT / ".github" / "workflows" / "gap_finder_fleet.yml"


def _main_module():
    import importlib
    sys.path.insert(0, str(ROOT))
    return importlib.import_module("pipeline.gap_finder.main")


def test_terminated_is_not_catchable_as_a_plain_exception():
    """Eighteen stages catch ``Exception``. If a termination were one of
    them, the kill would read as a clean empty pass — worse than silence,
    because it would be recorded as success."""
    m = _main_module()
    assert issubclass(m._Terminated, BaseException)
    assert not issubclass(m._Terminated, Exception)


def test_the_guard_installs_a_sigterm_handler():
    m = _main_module()
    before = signal.getsignal(signal.SIGTERM)
    try:
        m._install_termination_handlers()
        after = signal.getsignal(signal.SIGTERM)
        assert callable(after), (
            "SIGTERM is still on its default disposition — `timeout` would "
            "kill the interpreter with no run_log record")
        assert after is not before or callable(before)
        with pytest.raises(m._Terminated):
            after(signal.SIGTERM, None)
    finally:
        signal.signal(signal.SIGTERM, before)


def test_a_termination_is_recorded_as_timed_out_not_crashed(monkeypatch):
    """The status must distinguish the two. A country the cap keeps killing
    needs a bigger cap or a faster stage; a country that crashes needs a
    bug fixed. "crashed" for both is a diagnosis thrown away."""
    m = _main_module()
    seen = {}

    class _Cfg:
        code = "zz"
        run_log_path = "/dev/null"

    def fake_main():
        # Exactly what the handler does, from inside a stage frame.
        def stage_news_scraper():
            raise m._Terminated("SIGTERM")
        cfg = _Cfg()                     # noqa: F841 - read back off the traceback
        state = m.RunState(country_code="zz", country_name="Zedland",
                           started_at="2026-11-01T00:00:00+00:00")
        _ = state
        stage_news_scraper()

    def fake_write(cfg, state, status, failed_stage="", error=""):
        seen.update(status=status, failed_stage=failed_stage, error=error)

    monkeypatch.setattr(m, "main", fake_main)
    monkeypatch.setattr(m, "_write_run_log", fake_write)

    rc = m._main_guarded()

    assert rc == 2
    assert seen.get("status") == "timed_out", (
        f"a SIGTERM must be recorded as timed_out, got {seen.get('status')!r}")
    assert seen.get("failed_stage") == "stage_news_scraper", (
        "the record must name the stage the cap interrupted — that is the "
        "whole diagnostic value")


def test_an_ordinary_crash_is_still_recorded_as_crashed(monkeypatch):
    """The ratchet in the other direction: adding the timeout status must
    not relabel real crashes."""
    m = _main_module()
    seen = {}

    class _Cfg:
        code = "zz"
        run_log_path = "/dev/null"

    def fake_main():
        def stage_extractor():
            raise RuntimeError("boom")
        cfg = _Cfg()                     # noqa: F841
        state = m.RunState(country_code="zz", country_name="Zedland",
                           started_at="2026-11-01T00:00:00+00:00")
        _ = state
        stage_extractor()

    monkeypatch.setattr(m, "main", fake_main)
    monkeypatch.setattr(m, "_write_run_log",
                        lambda cfg, state, status, failed_stage="", error="":
                        seen.update(status=status, failed_stage=failed_stage))

    assert m._main_guarded() == 2
    assert seen.get("status") == "crashed"
    assert seen.get("failed_stage") == "stage_extractor"


def test_every_per_country_timeout_in_the_fleet_sends_sigterm_then_sigkill():
    """A handler that hangs would hold the shard forever and reintroduce
    the silent exit from the other side, so the cap must still be able to
    kill outright."""
    assert FLEET.exists(), FLEET
    text = FLEET.read_text(encoding="utf-8")
    calls = re.findall(r"timeout\s+(?:(-k\s*\S+)\s+)?(\d+[smh])\s", text)
    assert calls, "the fleet workflow no longer caps each country at all"
    for kill_after, cap in calls:
        assert kill_after, (
            f"`timeout {cap}` has no -k: if the SIGTERM handler added on "
            f"2026-11-01 ever hangs, nothing kills the process and the "
            f"shard stalls with no record")


def test_the_guard_is_what_the_module_actually_runs():
    """A record-on-every-exit guard that __main__ does not call is
    decoration. Asserted on the source so it cannot be mocked true."""
    src = MAIN.read_text(encoding="utf-8")
    tail = src[src.rindex('if __name__ == "__main__":'):]
    assert "_main_guarded()" in tail, (
        "__main__ must dispatch through _main_guarded, not main")
    assert "_install_termination_handlers()" in src
    guard = src[src.index("def _main_guarded"):]
    assert guard.index("_install_termination_handlers()") < guard.index("return main()"), (
        "the handlers must be installed BEFORE main() runs — a country "
        "killed during stage 1 is the common case")
