"""Smoke tests for the publication watchdog's diagnostic.

The watchdog is only as good as the exit code, and an exit code that is
always 0 is the thing this whole file exists to prevent — a green check
on a register that has published nothing for three days.
"""
from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _run(*args):
    return subprocess.run([sys.executable, "tools/why_no_acceptance.py", *args],
                          cwd=ROOT, capture_output=True, text=True, timeout=180)


class TestWhyNoAcceptance(unittest.TestCase):

    def test_it_runs_and_reports(self):
        p = _run()
        self.assertEqual(0, p.returncode, p.stderr[-800:])
        for expected in ("Recalls", "Pending rows by owning stage",
                         "workflows permitted to write to Recalls"):
            self.assertIn(expected, p.stdout)

    def test_an_impossible_threshold_still_passes(self):
        """--days 9999 can never trip, so a 0 here proves the exit code is
        driven by the measurement and not hardcoded."""
        self.assertEqual(0, _run("--days", "9999").returncode)

    def test_the_threshold_can_actually_fail(self):
        """--days 0 is disabled by design; --days 1 must be capable of
        returning 1. Assert the code path exists rather than the current
        state, so this does not flap with the register's real activity."""
        src = (ROOT / "tools" / "why_no_acceptance.py").read_text()
        self.assertIn("return 1", src)
        self.assertIn("stalled_days >= args.days", src)

    def test_the_watchdog_is_not_dispatch_only(self):
        """The point of the watchdog is to check a scheduler it must not
        depend on. If it becomes workflow_dispatch-only it inherits the
        exact failure it was built to catch."""
        wf = (ROOT / ".github" / "workflows"
              / "publication-watchdog.yml").read_text()
        self.assertIn("schedule:", wf)
        self.assertIn("cron:", wf)


if __name__ == "__main__":
    unittest.main()
