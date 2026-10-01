"""A Top-N table lists INCIDENTS, not rows (2026-09-30).

September 2026's Top 10 — and the social cards built from it, which are
immutable once posted — carried the Evergreen sprouts outbreak twice (FDA
recall + CDC investigation page) and the Frutas y Hortalizas blueberry
outbreak twice. Two of ten slots were repeats.
"""
from __future__ import annotations

from pathlib import Path

from pipeline._outbreak_id import one_row_per_event

ROOT = Path(__file__).resolve().parents[1]


def _r(src, url, company, product, patho="Salmonella", date="2026-09-10"):
    return {"Source": src, "URL": url, "Company": company, "Product": product,
            "Pathogen": patho, "Reason": patho, "Date": date, "Outbreak": 1}


FDA = _r("FDA", "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/"
         "evergreen-fresh-sprouts-llc-recalls-broccoli-sprouts", "Evergreen Fresh Sprouts, LLC.",
         "Broccoli Sprouts")
CDC = _r("CDC", "https://www.cdc.gov/salmonella/outbreaks/broccoli-sprouts-09-26/index.html",
         "Evergreen Fresh Sprouts, LLC", "Broccoli sprouts", date="2026-09-09")
OTHER = _r("CFIA", "https://recalls-rappels.canada.ca/en/alert-recall/x-raspberries",
           "New Alasko", "Frozen raspberries", patho="Norovirus")


def test_the_cdc_page_does_not_take_a_second_slot():
    out = one_row_per_event([CDC, FDA, OTHER])
    assert FDA in out and CDC not in out and OTHER in out


def test_order_is_kept():
    assert one_row_per_event([OTHER, FDA, CDC]) == [OTHER, FDA]


def test_rows_without_an_outbreak_are_untouched():
    plain = [dict(FDA, Outbreak=0), dict(CDC, Outbreak=0)]
    assert one_row_per_event(plain) == plain


def test_every_top_list_uses_it():
    m = (ROOT / "docs" / "build_monthly_report_afts.py").read_text("utf-8")
    w = (ROOT / "docs" / "build_weekly_report_afts.py").read_text("utf-8")
    assert m.count("_one_per_event(_ranked_") == 2, "monthly HTML Top 10 + summary JSON top10"
    assert w.count("_one_per_event(rank_top_recalls(") == 2, "weekly HTML Top 5 + summary threats"


def test_the_outbreak_count_agrees_with_the_table():
    """September 2026 printed 8 outbreaks for 7 events: the slugless FDA
    Evergreen recall and its CDC page were counted separately."""
    from pipeline._outbreak_id import count_events
    assert count_events([FDA, CDC, OTHER]) == 2
