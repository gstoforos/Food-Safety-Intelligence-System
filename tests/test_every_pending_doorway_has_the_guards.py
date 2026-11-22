# -*- coding: utf-8 -*-
"""One batch queues one row per recall, and the fleet's door has the guards.

WHY THIS FILE EXISTS (morning fix 2026-11-22)
=============================================
TWO ROWS, ONE BATCH
-------------------
merge_master.append_to_pending builds ``existing_urls`` and
``pending_by_key`` ONCE, from the workbook, before its loop — and never
added a row it had just queued. So two rows in the SAME call that name the
same recall both passed every check and both landed.

Measured on the 2026-11-22 workbook: Pending held AESAN alert 2026_86
twice, as ``www.aesan.gob.es/alertas/2026_86`` and
``aesan.gob.es/alertas/2026_86``, both with ScrapedAt
``2026-11-21T08:41:32Z`` — the same append call. Reproduced directly
against the function: two rows with the BYTE-IDENTICAL URL also both
landed. ``_normalize_url_for_dedup`` collapses either pair correctly; it
was simply never asked about a row queued a moment earlier.

The cost is the one tests/test_pending_holds_one_row_per_url names:
reviewer 2 indexes Pending as ``url_to_idx[url] = i``, one slot per URL,
last write wins, so every copy but the last is invisible to it and can
never be corrected, promoted or refused.

THE SIDE DOOR
-------------
pipeline.gap_finder.main.write_pending_rows writes the Pending sheet
directly. It called neither validate_pending_row nor anything else in
merge_master, so the entire gap-finder fleet reached Pending with no
terminal-rejection guard and with a RAW-STRING URL comparison — the one
that sees "https://www.x/y" and "https://x/y" as two different recalls.

THE ROW THAT FOUND IT. At 11:53 UTC on 2026-11-20 the fleet ("Gap finder
fleet: 2026-11-20 shard of 8 countries") wrote the NCC (ZA) "Deli Hummus
range" media statement into Pending, at Status "pending". An operator had
archived that exact URL on 2026-11-06 as "TERMINAL, UPHELD … roughly 22
months stale", after eight earlier refusals. Reviewer 1 published it just
under two hours later.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.gap_finder.main import (                          # noqa: E402
    refuse_terminally_rejected,
)
from pipeline.merge_master import append_to_pending             # noqa: E402

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"

BASE = {
    "Date": "2026-11-19",
    "Source": "AESAN (ES)",
    "Company": "Acme Foods SL",
    "Brand": "Acme",
    "Product": "Hen eggs, class A, 12-pack, lot 2611",
    "Pathogen": "Salmonella",
    "Reason": "Possible presence of Salmonella Enteritidis in eggs",
    "Class": "Recall",
    "Country": "Spain",
    "Region": "Europe",
    "Tier": 1,
    "Outbreak": 0,
    "Notes": "test fixture",
}

HUMMUS_URL = ("https://thencc.org.za/media-statement-deli-hummus-range-"
              "product-safety-recall/")


def _queue(*urls, **over):
    rows = [dict(BASE, URL=u, **over) for u in urls]
    return append_to_pending([], [], rows, "2026-11-22T06:00:00Z")


class TestOneBatchQueuesOneRowPerRecall(unittest.TestCase):
    def test_a_www_variant_in_the_same_batch_does_not_queue_twice(self):
        out = _queue("https://www.aesan.gob.es/alertas/2026_86",
                     "https://aesan.gob.es/alertas/2026_86")
        self.assertEqual(
            1, len(out),
            "one recall queued twice in one batch, one 'www.' apart — the "
            "exact pair found in Pending on 2026-11-22")

    def test_the_identical_url_twice_does_not_queue_twice(self):
        out = _queue("https://www.aesan.gob.es/alertas/2026_86",
                     "https://www.aesan.gob.es/alertas/2026_86")
        self.assertEqual(1, len(out),
                         "a byte-identical URL queued twice in one batch")

    def test_two_different_recalls_both_still_queue(self):
        out = _queue("https://www.aesan.gob.es/alertas/2026_86",
                     "https://www.aesan.gob.es/alertas/2026_87")
        self.assertEqual(2, len(out),
                         "the fix must not collapse two distinct alerts")


class TestTheFleetDoorHasTheTerminalGuard(unittest.TestCase):
    def test_a_terminally_rejected_url_is_refused(self):
        if not XLSX.exists():                           # pragma: no cover
            self.skipTest("no workbook")
        rows = [dict(BASE, Source="NCC (ZA)", Product="Deli Hummus range",
                     URL=HUMMUS_URL),
                dict(BASE, URL="https://www.aesan.gob.es/alertas/2026_99")]
        keep, refused = refuse_terminally_rejected(rows, str(XLSX))
        self.assertEqual(1, len(refused),
                         "the fleet door let a terminally rejected URL "
                         "through")
        self.assertIn("thencc.org.za", refused[0]["URL"])
        self.assertEqual(1, len(keep), "an ordinary row was refused too")

    def test_the_refusal_is_stamped_not_silent(self):
        if not XLSX.exists():                           # pragma: no cover
            self.skipTest("no workbook")
        _, refused = refuse_terminally_rejected(
            [dict(BASE, Source="NCC (ZA)", Product="Deli Hummus range",
                  URL=HUMMUS_URL)], str(XLSX))
        r = refused[0]
        self.assertTrue(
            any("terminal" in str(r.get(c) or "").lower()
                for c in ("RejectionReason", "RejectReason", "Notes")),
            "a dropped row with no verdict is indistinguishable from a "
            "scraper that stopped working")


if __name__ == "__main__":                              # pragma: no cover
    unittest.main()
