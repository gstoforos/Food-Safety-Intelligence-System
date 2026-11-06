# -*- coding: utf-8 -*-
"""Every route into Pending applies the stale-alert-number refusal.

WHY THIS FILE EXISTS (morning fix 2026-11-06)
=============================================
``pipeline.merge_master.validate_pending_row`` has refused a row whose
alert number names a year its Date contradicts since 2026-10-24. Its own
comment there says what it was for:

    "the Nigeria gap finder re-queued that one May-2022 alert into Pending
     again and again — already archived three times — where it sat with
     Date 2026-10-23 holding
     tests/test_alert_number_year_matches_date[Pending] red."

The rule went into the wrong doorway. The gap finders do not reach Pending
through ``validate_pending_row``; they reach it through their OWN writers
(``pipeline/gap_finder/main.py::write_pending_rows`` and
``pipeline/gap_finder_gr/main.py::write_pending_rows_to_xlsx``), whose only
test was "is this URL already in Pending or Recalls". So the fix of
2026-10-24 could never apply to the collector it was written for, and on
2026-11-06 the same test was red again with three NAFDAC rows that the
Nigeria gap finder had written to Pending at 18:44-18:46 the night before:

    2026-11-03  public-alert-no-014-2020  Liqui Fruit Red Grape Juice
    2026-11-01  public-alert-no-35-2025   Amoxicillin suspensions 125mg/5ml
    2026-10-28  public-alert-no-003-2020  Hand Sanitizers

This test holds the shape in BOTH writers, and it asserts the source-level
property as well as the behaviour: a writer that stops calling the helper
is the defect, and a passing unit test on the helper alone would not see
it. That is the same lesson as
tests/test_pending_gate_refuses_an_old_alert_number.py, which checks the
merge_master call site by reading its source.
"""
from __future__ import annotations

import inspect
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

GOOD = {
    "Date": "2026-11-05",
    "Source": "RappelConso (FR)",
    "Company": "La Cabane à Jus",
    "Product": "Apple and cherry juice, 1 L bottle",
    "Pathogen": "Patulin",
    "Reason": "Juice with a patulin level above the authorised limits.",
    "URL": "https://rappel.conso.gouv.fr/fiche-rappel/24022/interne",
    "Notes": "",
}
STALE = {
    "Date": "2026-11-03",
    "Source": "NAFDAC (NG)",
    "Company": "South Africa Foods Company",
    "Product": "Liqui Fruit Red Grape Juice",
    "Reason": ("Recall of Liquid Fruit Grape Juice product code 27327 due "
               "to potential contamination"),
    "URL": ("https://nafdac.gov.ng/public-alert-no-014-2020-recall-of-"
            "liquid-fruit-grape-juice-product-code-27327-by-south-africa-"
            "foods-company"),
    "Notes": "Discovered via news: dailytrust.com",
}


class TestTheHelper(unittest.TestCase):
    def test_the_three_writers_share_one_implementation(self):
        """Three copies of a rule is how the collector tables drifted."""
        from pipeline.gap_finder.main import refuse_stale_alert_numbers as a
        from pipeline.gap_finder_gr.main import refuse_stale_alert_numbers as b
        for fn in (a, b):
            keep, refused = fn([GOOD, STALE])
            self.assertEqual(1, len(keep), fn.__module__)
            self.assertEqual(1, len(refused), fn.__module__)
            self.assertEqual(GOOD["URL"], keep[0]["URL"], fn.__module__)
            self.assertEqual(STALE["URL"], refused[0]["URL"], fn.__module__)

    def test_the_refusal_is_recorded_on_the_row(self):
        """An archived row with no reason is an archive nobody can read."""
        from pipeline.gap_finder.main import refuse_stale_alert_numbers
        _keep, refused = refuse_stale_alert_numbers([STALE])
        notes = str(refused[0]["Notes"])
        self.assertIn("alert_number_year_mismatch", notes)
        self.assertIn("public-alert-no-014-2020", notes)
        self.assertIn("Discovered via news: dailytrust.com", notes,
                      "the row's existing Notes must be kept, not replaced")

    def test_the_input_rows_are_not_mutated(self):
        from pipeline.gap_finder.main import refuse_stale_alert_numbers
        before = dict(STALE)
        refuse_stale_alert_numbers([STALE])
        self.assertEqual(before, STALE)

    def test_the_rule_is_imported_not_copied(self):
        """Both gates must read the same regex, or they drift."""
        from pipeline.gap_finder import main as gf
        src = inspect.getsource(gf.refuse_stale_alert_numbers)
        self.assertIn("from pipeline._publish_gate import "
                      "alert_number_year_mismatch", src)


class TestTheWriters(unittest.TestCase):
    def test_write_pending_rows_calls_the_refusal(self):
        from pipeline.gap_finder import main as gf
        src = inspect.getsource(gf.write_pending_rows)
        self.assertIn("refuse_stale_alert_numbers", src,
                      "pipeline/gap_finder/main.py::write_pending_rows is "
                      "how every country gap finder reaches Pending; a "
                      "refusal it does not call does not exist")

    def test_greek_writer_calls_the_refusal(self):
        from pipeline.gap_finder_gr import main as gr
        src = inspect.getsource(gr.write_pending_rows_to_xlsx)
        self.assertIn("refuse_stale_alert_numbers", src,
                      "the Greek collector has its own writer; the hole "
                      "survives in whichever writer nobody looked at")

    def test_refused_rows_are_archived_not_dropped(self):
        """Silently dropping a row is how a real recall disappears."""
        from pipeline.gap_finder import main as gf
        src = inspect.getsource(gf.write_pending_rows)
        self.assertIn("write_rejected_rows", src)


if __name__ == "__main__":                              # pragma: no cover
    unittest.main()
