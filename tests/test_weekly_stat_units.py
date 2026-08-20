"""`total` and `tier1` must count the same unit.

WHY (2026-08-20)
================
`total` was switched to count INCIDENTS on 2026-08-15 so that one event
producing many regulator notices stops inflating the week. `tier1` was
left counting ROWS. The two units met on the dashboard card as

    W34    total 43    Tier-1 51

— more critical items than items. The Leclerc Dinan cluster is twenty
Listeria notices from one failed chiller: it adds 1 to `total` and added
20 to `tier1`.

An incident is Tier 1 if ANY of its notices is — a cluster's severity is
its worst hazard, not an average.

THE SAFETY PROPERTY
-------------------
_incident_id.derive() returns an id only for rows a human tagged
[incident:<id>]. Untagged rows are their own incident. So for every week
that predates incident tagging, incident counts equal row counts and no
published figure moves. The tests below pin that both ways.
"""
from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "docs"))

from build_weekly_report_afts import compute_stats  # noqa: E402

TAG = "[incident:test:one-chiller]"


def _row(tier=1, outbreak=0, notes="", date="2026-08-15"):
    return {"Date": date, "Tier": tier, "Outbreak": outbreak, "Notes": notes,
            "Pathogen": "Listeria monocytogenes", "Source": "RappelConso (FR)",
            "Company": "X", "Product": "Y", "URL": "https://example.invalid/1"}


class TestUnitsMatch(unittest.TestCase):

    def test_tier1_never_exceeds_total(self):
        wr = [_row(notes=TAG) for _ in range(20)] + [_row() for _ in range(3)]
        st = compute_stats(wr, [])
        self.assertLessEqual(st["tier1"], st["total"],
                             f"tier1={st['tier1']} > total={st['total']}")

    def test_a_tagged_cluster_counts_once_in_both(self):
        wr = [_row(notes=TAG) for _ in range(20)]
        st = compute_stats(wr, [])
        self.assertEqual(1, st["total"])
        self.assertEqual(1, st["tier1"])

    def test_untagged_rows_are_unchanged(self):
        """The historical-safety property: no tags, no movement."""
        wr = [_row(tier=1) for _ in range(7)] + [_row(tier=2) for _ in range(4)]
        st = compute_stats(wr, [])
        self.assertEqual(11, st["total"])
        self.assertEqual(7, st["tier1"])

    def test_a_cluster_is_tier1_if_any_notice_is(self):
        """Severity of a cluster is its worst hazard, not an average."""
        wr = ([_row(tier=3, notes=TAG) for _ in range(5)]
              + [_row(tier=1, notes=TAG)])
        st = compute_stats(wr, [])
        self.assertEqual(1, st["total"])
        self.assertEqual(1, st["tier1"])

    def test_a_wholly_tier2_cluster_is_not_tier1(self):
        wr = [_row(tier=2, notes=TAG) for _ in range(5)]
        st = compute_stats(wr, [])
        self.assertEqual(1, st["total"])
        self.assertEqual(0, st["tier1"])


class TestThePublishedIndex(unittest.TestCase):

    def test_no_week_reports_more_tier1_than_total(self):
        idx = ROOT / "docs" / "data" / "weekly-index.json"
        if not idx.exists():                               # pragma: no cover
            self.skipTest("weekly-index.json not present")
        entries = json.loads(idx.read_text(encoding="utf-8"))
        bad = [f"W{e['week_num']}: tier1={e['tier1']} total={e['total']}"
               for e in entries if e.get("tier1", 0) > e.get("total", 0)]
        self.assertEqual([], bad, f"unit mismatch on {bad}")

    def test_the_week_just_built_is_listed(self):
        """The index used to drop any week whose Friday had not arrived —
        which is every report built on the operator's Thursday close."""
        idx = ROOT / "docs" / "data" / "weekly-index.json"
        latest = ROOT / "docs" / "data" / "weekly-summary-latest.json"
        if not (idx.exists() and latest.exists()):         # pragma: no cover
            self.skipTest("index files not present")
        entries = json.loads(idx.read_text(encoding="utf-8"))
        pointer = json.loads(latest.read_text(encoding="utf-8"))
        names = {e["filename"] for e in entries}
        self.assertIn(pointer["filename"], names,
                      "weekly-summary-latest points at a report the index "
                      "does not list")


if __name__ == "__main__":
    unittest.main()
