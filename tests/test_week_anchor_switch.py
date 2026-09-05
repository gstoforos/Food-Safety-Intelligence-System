"""The reporting week moved from Friday-rule to ISO at 2026-W36.

Weeks up to W35 stay anchored on their Friday (so a rebuild keeps the header
they were published with); W36 onward is anchored on its Sunday. W36 is the
ten-day bridge, 28 Aug - 6 Sep 2026. On Monday 7 Sep the workflow must hand
the builders a date inside W36, never Friday 4 Sep as a Friday-rule week.
"""
import importlib.util, sys, unittest
from datetime import date
sys.path.insert(0, ".")


def _builder():
    spec = importlib.util.spec_from_file_location("wb", "docs/build_weekly_report_afts.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestAnchorFor(unittest.TestCase):
    def test_old_weeks_keep_friday_new_weeks_take_sunday(self):
        from pipeline.build_missing_weekly_reports import anchor_for
        self.assertEqual(date(2026, 8, 28), anchor_for(date(2026, 8, 30)))   # W35 Sunday -> its Friday
        self.assertEqual(date(2026, 8, 28), anchor_for(date(2026, 8, 24)))   # W35 Monday -> its Friday
        self.assertEqual(date(2026, 9, 6), anchor_for(date(2026, 9, 4)))     # W36 Friday -> its Sunday
        self.assertEqual(date(2026, 9, 6), anchor_for(date(2026, 9, 6)))
        self.assertEqual(date(2026, 9, 13), anchor_for(date(2026, 9, 7)))    # Monday belongs to the NEXT week

    def test_builder_and_gap_fill_agree(self):
        from pipeline.build_missing_weekly_reports import anchor_for as a1
        a2 = _builder().anchor_for
        for d in (date(2026, 4, 17), date(2026, 8, 21), date(2026, 8, 30),
                  date(2026, 9, 1), date(2026, 9, 6), date(2026, 12, 31)):
            self.assertEqual(a1(d), a2(d), d)

    def test_candidate_sequence_crosses_the_switch(self):
        from pipeline.build_missing_weekly_reports import iter_week_ends
        got = iter_week_ends(date(2026, 8, 14), date(2026, 9, 13))
        self.assertEqual([date(2026, 8, 14), date(2026, 8, 21), date(2026, 8, 28),
                          date(2026, 9, 6), date(2026, 9, 13)], got)


class TestBridgeWeek(unittest.TestCase):
    def _rows(self):
        base = {"Source": "RappelConso (FR)", "Company": "x", "Product": "y",
                "Pathogen": "Listeria monocytogenes", "Tier": 1, "Outbreak": 0,
                "Country": "France", "URL": "https://rappel.conso.gouv.fr/fiche-rappel/1/interne"}
        # 28-30 Aug were stamped W36 under the Friday rule before the switch;
        # 31 Aug - 6 Sep are stamped W36 under ISO. Both must land in W36.
        return [dict(base, Date=d, report_week="W36") for d in
                ("2026-08-28", "2026-08-29", "2026-08-31", "2026-09-03", "2026-09-06")] + \
               [dict(base, Date="2026-08-27", report_week="W35")]

    def test_w36_holds_ten_days_and_says_so(self):
        b = _builder()
        rows = self._rows()
        wr = b.filter_week(rows, date(2026, 9, 6))
        self.assertEqual(5, len(wr))
        ws, we = b._display_window(date(2026, 9, 6), wr)
        self.assertEqual((date(2026, 8, 28), date(2026, 9, 6)), (ws, we))
        self.assertEqual(10, (we - ws).days + 1)

    def test_w35_rebuilt_on_its_friday_keeps_its_header(self):
        b = _builder()
        rows = self._rows()
        wr = b.filter_week(rows, date(2026, 8, 28))
        self.assertEqual(1, len(wr))
        ws, we = b._display_window(date(2026, 8, 28), wr)
        self.assertEqual((date(2026, 8, 27), date(2026, 8, 27)), (ws, we))  # single row tightens the Fri-Thu window

    def test_workflow_hands_over_a_sunday(self):
        src = open(".github/workflows/afts-weekly-report.yml", encoding="utf-8").read()
        self.assertIn("days_since_sunday", src)
        self.assertNotIn("days_since_friday = (t.weekday() - 4) % 7", src)


if __name__ == "__main__":
    unittest.main()
