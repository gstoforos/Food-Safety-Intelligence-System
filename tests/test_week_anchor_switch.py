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


# ──────────────────────────────────────────────────────────────────────
# AUDIT 2026-09-13 — the prior-week comparison
# ──────────────────────────────────────────────────────────────────────
# `total` is an INCIDENT count; `prev_total` was `len(pr)`, a NOTICE count.
# Identical for every week before incident tagging, so the mismatch was
# invisible until W36 collapsed 109 notices into 83 incidents. Without the
# fix the W37 issue compares 52 incidents against 109 notices and prints
# -52% for a real change of -37%.

def test_prev_total_is_an_incident_count_not_a_notice_count():
    import importlib.util
    from pathlib import Path as _P
    root = _P(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location(
        "_b", root / "docs" / "build_weekly_report_afts.py")
    b = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(b)

    def _row(i, inc=None):
        n = f"[incident:{inc}]" if inc else ""
        return {"Date": "2026-09-01", "Source": "RappelConso (FR)",
                "Company": f"Co{i}", "Brand": "", "Product": "p",
                "Pathogen": "Listeria monocytogenes", "Reason": "Listeria",
                "Class": "Recall", "Country": "France", "Region": "Europe",
                "Tier": 1, "Outbreak": 0, "Notes": n,
                "URL": f"https://rappel.conso.gouv.fr/fiche-rappel/{9000+i}/interne"}

    # prior week: four notices that a human tagged as ONE incident
    prev = [_row(i, "fr:test-cluster") for i in range(4)]
    cur = [_row(100 + i) for i in range(3)]
    st = b.compute_stats(cur, prev)
    assert st["prev_total"] == 1, (
        f"prior week must count 1 incident, not 4 notices (got "
        f"{st['prev_total']})")
    assert st["total"] == 3
    assert st["delta"] == 2


def test_an_untagged_prior_week_is_unchanged():
    """No tags means notices == incidents; no historical figure may move."""
    import importlib.util
    from pathlib import Path as _P
    root = _P(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location(
        "_b2", root / "docs" / "build_weekly_report_afts.py")
    b = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(b)
    mk = lambda i: {"Date": "2026-09-01", "Source": "FDA", "Company": f"C{i}",
                    "Brand": "", "Product": "p", "Pathogen": "Salmonella",
                    "Reason": "Salmonella", "Class": "Recall",
                    "Country": "United States", "Region": "North America",
                    "Tier": 1, "Outbreak": 0, "Notes": "",
                    "URL": f"https://www.fda.gov/x/{i}"}
    st = b.compute_stats([mk(9), mk(8)], [mk(i) for i in range(5)])
    assert st["prev_total"] == 5


# ──────────────────────────────────────────────────────────────────────
# AUDIT 2026-09-14 — the email and the page disagreed in print
# ──────────────────────────────────────────────────────────────────────
# Monday's subscriber email said the week fell 37%; the page it links to
# said "-10% per day (52 in 7 days vs 83 in 10)". Both are arithmetically
# right — W36 covered ten days, W37 seven — but the JSON the mailer reads
# carried no per-day figure and no prior-window length, so the mailer had
# nothing else it could print.

def _mod():
    import importlib.util
    from pathlib import Path as _P
    root = _P(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location(
        "_wb", root / "docs" / "build_weekly_report_afts.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def _rows(n, d="2026-09-08"):
    return [{"Date": d, "Source": "FDA", "Company": f"C{i}", "Brand": "",
             "Product": "p", "Pathogen": "Salmonella", "Reason": "Salmonella",
             "Class": "Recall", "Country": "United States",
             "Region": "North America", "Tier": 1, "Outbreak": 0, "Notes": "",
             "URL": f"https://www.fda.gov/x/{d}/{i}"} for i in range(n)]


def test_the_summary_json_carries_the_prior_window(tmp_path):
    """So the mailer can say exactly what the page says."""
    from datetime import date
    m = _mod()
    import json
    stats = {"total": 52, "tier1": 37, "outbreaks": 1, "delta": -31,
             "delta_pct": -37, "prev_total": 83,
             "top_pathogen": ("Salmonella spp.", 20)}
    m.write_weekly_summary_json(date(2026, 9, 13), _rows(3), stats, tmp_path,
                                prev_week_rows=_rows(4, "2026-08-28"))
    d = json.loads((tmp_path / "weekly-summary-latest.json").read_text("utf-8"))
    assert d["prev_total"] == 83
    assert "prev_span_days" in d and "delta_per_day_pct" in d


def test_equal_windows_leave_the_per_day_figure_unset(tmp_path):
    """A normal week: delta_pct is already the honest comparison."""
    from datetime import date
    import json
    m = _mod()
    stats = {"total": 10, "tier1": 5, "outbreaks": 0, "delta": 2,
             "delta_pct": 25, "prev_total": 8,
             "top_pathogen": ("Salmonella spp.", 4)}
    m.write_weekly_summary_json(date(2026, 9, 13), _rows(3), stats, tmp_path,
                                prev_week_rows=_rows(4, "2026-09-01"))
    d = json.loads((tmp_path / "weekly-summary-latest.json").read_text("utf-8"))
    assert d["delta_per_day_pct"] is None, (
        "when both windows are the same length the mailer should keep using "
        "delta_pct")
