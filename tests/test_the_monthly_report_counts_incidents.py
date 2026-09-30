"""The monthly report counts what the weekly report counts (2026-09-30).

The weekly builder has counted INCIDENTS since 2026-08-15 and outbreak
EVENTS since 2026-08-14; the monthly builder never got either fix. August
2026 printed "250 food-safety hazard recall incidents" when 20 of them were
one Leclerc Dinan cold-chain failure, and "6 outbreak-associated incidents"
when two were one pumpkin-seed cluster. Its trend series was also row-based,
so the same report printed -24% in the KPI banner and -18% in §02.

And the failure-mode paragraph was hard-coded to Listeria: June 2026 read
"For a Salmonella spp.-dominated month, the relevant failure modes are
Listeria persistence on food-contact surfaces".
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "docs"))

import build_monthly_report_afts as monthly  # noqa: E402


def _row(i, **kw):
    r = {"Date": "2026-08-15", "Source": "RappelConso (FR)", "Company": f"Co{i}",
         "Brand": "", "Product": f"P{i}", "Pathogen": "Listeria monocytogenes",
         "Reason": "Listeria", "Country": "France", "Tier": 1, "Outbreak": 0,
         "URL": f"https://rappel.conso.gouv.fr/fiche-rappel/{9000 + i}/Interne",
         "Notes": ""}
    r.update(kw)
    return r


def test_tagged_notices_count_once():
    rows = [_row(i, Notes="[incident:leclerc-dinan-test]") for i in range(5)]
    rows += [_row(10), _row(11)]
    s = monthly.compute_month_stats(rows, [])
    assert s["total"] == 3, "5 tagged notices + 2 untagged = 3 incidents"
    assert s["tier1"] == 3, "Tier-1 counts the same unit as total"
    assert sum(c for _, c in s["pathogen_counts"]) == 3, (
        "the distribution table must count the same unit as the total")


def test_untagged_rows_are_unchanged():
    rows = [_row(i) for i in range(4)]
    assert monthly.compute_month_stats(rows, [])["total"] == 4


def test_mom_uses_incidents_on_both_sides():
    cur = [_row(i, Notes="[incident:x]") for i in range(10)] + [_row(20)]
    prv = [_row(30 + i, Date="2026-07-10") for i in range(4)]
    s = monthly.compute_month_stats(cur, prv)
    assert (s["total"], s["prev_total"], s["delta_pct"]) == (2, 4, -50)


def test_the_trend_series_is_built_from_incidents():
    src = (ROOT / "docs" / "build_monthly_report_afts.py").read_text("utf-8")
    assert "monthly_count_history = [(ym, _ci(c)) for ym, c in cohorts]" in src


def test_cluster_event_count_is_outbreak_events():
    """docs/monthly_stats.py is the copy the builder imports (docs/ is on
    its sys.path). docs/data/monthly_stats.py is an unused duplicate."""
    src = (ROOT / "docs" / "monthly_stats.py").read_text("utf-8")
    assert '"event_count":   _event_count' in src


@pytest.mark.parametrize("top,must,must_not", [
    ("Salmonella spp.", "kill step", "Listeria"),
    ("Listeria monocytogenes", "Listeria persistence", "kill step"),
    ("Bacillus cereus / Cereulide", "cereulide", "Listeria"),
    ("STEC / Shiga-toxin E. coli", "irrigation", "Listeria"),
    ("Aflatoxin", "storage", "Listeria"),
    ("Foreign material (metal)", "detection", "Listeria"),
    ("Norovirus", "food handlers", "Listeria"),
])
def test_failure_modes_follow_the_leading_hazard(top, must, must_not):
    txt = monthly._failure_modes_for(top)
    assert must in txt and must_not not in txt, (top, txt)


def test_the_updates_check_compares_incidents():
    src = (ROOT / "pipeline" / "build_monthly_updates_check.py").read_text("utf-8")
    assert "count_incidents as _ci" in src
