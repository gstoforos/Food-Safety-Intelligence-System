# -*- coding: utf-8 -*-
"""A gap finder's rejection must reach the sheet with its reason intact.

THE GAP (found 2026-09-21, re-measured daily since)
===================================================
Rows in Weekly_Rejected carrying a BLANK ``Week_Added`` and a BLANK
``RejectionReason``::

    2026-09-21   14 of 40
    2026-09-22   29 of 40
    2026-09-23   49 of 78     it · ng · pt · es · gr · za

About +16 a day, from six gap finders.

Nothing is lost in transit. The fields are never written, because two
modules name the same two facts differently::

    built by the gap finder      the live sheet column
    -----------------------      ---------------------
    RejectReason                 RejectionReason
    RejectedAt                   (no such column)
    (nothing)                    Week_Added

``_append_rows`` writes ``row.get(h, "")`` per header — BY NAME. That is
why nothing is misaligned and why this went unseen: a key the sheet does
not name is dropped, a column the row lacks lands empty.

* a blank ``Week_Added`` matches no review window, so the row appears in
  **no** Sunday operator email — rejected in silence
* a blank ``RejectionReason`` is what an audit reports as
  "NO_REASON_RECORDED … the writer supplied no verdict"

FOUR modules define ``build_rejected_row`` and all four had the defect. A
first pass fixed only the two ``gap_finder`` copies and moved nothing
measurable, because every live blank row came through the authority-URL
gate in the other two.
"""

from __future__ import annotations

import sys
import types
from dataclasses import dataclass
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
# pipeline/extractor.py and official_feeds/extractor.py fall back to a bare
# `from gap_finder.rules import ...`, which resolves only with pipeline/ on
# the path — how they are launched in production.
sys.path.insert(0, str(ROOT / "pipeline"))

from pipeline.weekly_rejected_capture import review_day_for  # noqa: E402

MUST_BE_SET = ("Week_Added", "RejectionReason", "RejectedBy")


@dataclass
class _C:
    category: str = "allergen"
    rule: str = "milk not declared on label"
    matched_term: str = "milk"


_V = {"efet_date_iso": "2026-09-20", "efet_title": "Richiamo di formaggio",
      "efet_url": "https://example.test/richiamo-1",
      "news_source_domain": "ansa.it"}


def _cfg(s, n, c):
    return types.SimpleNamespace(authority_short=s, name_en=n, code=c)


def _it():
    from pipeline.gap_finder import extractor as ex
    return ex.build_rejected_row(_V, _C(), _cfg("Salute", "Italy", "it"))


def _gr():
    from pipeline.gap_finder_gr import extractor as ex
    return ex.build_rejected_row(_V, _C())


def _pl():
    from pipeline import extractor as ex
    return ex.build_rejected_row(_V, _C(), _cfg("AESAN", "Spain", "es"))


def _of():
    from pipeline.official_feeds import extractor as ex
    return ex.build_rejected_row(_V, _C(), _cfg("EFET", "Greece", "gr"))


def _gate_refusal():
    """The gate-refusal doorway, added 2026-11-07.

    refuse_stale_alert_numbers (2026-11-06) and refuse_non_food_appliances
    (2026-11-07) archive to Weekly_Rejected WITHOUT going through
    build_rejected_row — the row they archive is a PENDING row dict, and
    _append_rows writes a blank for every header it finds no key for. The
    first row that path archived (NAFDAC public alert 027/2022, Japata
    Alcoholic Bitters, 2026-10-24) therefore landed with Week_Added,
    RejectedBy, RejectionReason and Reviewed all empty, which held
    test_only_dateless_rows_remain_unstamped_on_the_live_sheet below red
    the next morning.

    It is NOT in BUILDERS: a gate refusal keeps the row's own Reason and
    Pathogen, which build_rejected_row overwrites with the rule and the
    matched term. Only the four MUST_BE_SET columns are the shared
    contract, so only those are parametrised over it, below.
    """
    from pipeline.gap_finder.main import _stamp_refusal
    return _stamp_refusal(
        {"Date": "2026-09-20", "Source": "Salute",
         "Product": "Richiamo di formaggio",
         "Pathogen": "Listeria monocytogenes",
         "Reason": "Presence of Listeria monocytogenes",
         "URL": "https://example.test/richiamo-1",
         "Notes": "Discovered via news: ansa.it"},
        "non_food_appliance",
        "the row's own text names a durable consumer good "
        "('electric shaver')")


BUILDERS = [("pipeline.extractor", _pl), ("official_feeds.extractor", _of),
            ("gap_finder", _it), ("gap_finder_gr", _gr)]
#: Every path that appends a row to Weekly_Rejected, builders plus the gate
#: refusals. These four columns are what the Sunday email and an auditor
#: read, and no path may land them blank.
APPENDERS = BUILDERS + [("gap_finder.main._stamp_refusal", _gate_refusal)]
MODULES = ["pipeline.extractor", "pipeline.official_feeds.extractor",
           "pipeline.gap_finder.extractor", "pipeline.gap_finder_gr.extractor"]


def _src(mod):
    return (ROOT / (mod.replace(".", "/") + ".py")).read_text(encoding="utf-8")


@pytest.mark.parametrize("name,build", APPENDERS,
                         ids=[b[0] for b in APPENDERS])
@pytest.mark.parametrize("col", MUST_BE_SET)
def test_the_column_is_not_blank(name, build, col):
    assert str(build().get(col, "")).strip(), (
        f"{name} still writes a blank {col} — the rejection lands on the "
        f"sheet with nothing to identify or explain it")


@pytest.mark.parametrize("col", ("Week_Added", "RejectionReason",
                                 "RejectedBy", "Reviewed", "Status",
                                 "RejectReason", "RejectedAt"))
def test_a_gate_refusal_fills_every_archive_column(col):
    """The NAFDAC row of 2026-10-24 landed with four of these blank."""
    assert str(_gate_refusal().get(col, "")).strip(), (
        f"a gate refusal still lands a blank {col} on Weekly_Rejected")


def test_a_gate_refusal_keeps_the_rows_own_recall_fields():
    """A refusal records a verdict; it does not rewrite the recall.

    build_rejected_row puts the rule in Reason and the matched term in
    Pathogen because it builds a row from a classification. A gate refusal
    starts from a row that already HAS those fields, and overwriting them
    would destroy the only record of what was refused.
    """
    row = _gate_refusal()
    assert row["Reason"] == "Presence of Listeria monocytogenes"
    assert row["Pathogen"] == "Listeria monocytogenes"
    assert row["Product"] == "Richiamo di formaggio"
    assert row["URL"] == "https://example.test/richiamo-1"


def test_a_gate_refusal_says_which_rule_fired_and_why():
    row = _gate_refusal()
    assert "non_food_appliance" in row["RejectionReason"]
    assert "durable consumer good" in row["RejectionReason"], (
        "the category alone does not tell an auditor why THIS row went")
    assert "non_food_appliance" in str(row["RejectedBy"])
    assert "Discovered via news: ansa.it" in row["Notes"], (
        "the refusal note must be appended to the row's Notes, not replace "
        "them")
    assert "[gap-finder refusal" in row["Notes"]


def test_a_gate_refusal_takes_its_week_from_the_capture_module():
    assert _gate_refusal()["Week_Added"] == review_day_for().isoformat()


def test_both_gate_refusals_go_through_the_one_stamp():
    """Two refusal rules, one shape on the sheet."""
    src = _src("pipeline.gap_finder.main")
    for fn in ("refuse_stale_alert_numbers", "refuse_non_food_appliances"):
        body = src[src.index(f"def {fn}("):]
        body = body[:body.index("\ndef ", 1)]
        assert "_stamp_refusal(" in body, (
            f"{fn} builds its archive row by hand again — that is how the "
            f"four columns came to be blank")


@pytest.mark.parametrize("name,build", BUILDERS, ids=[b[0] for b in BUILDERS])
def test_the_reason_survives_the_rename(name, build):
    row = build()
    assert row["RejectReason"] == "allergen", "the old key must still work"
    assert "allergen" in row["RejectionReason"]
    assert "milk not declared on label" in row["RejectionReason"], (
        "the rule that fired is the useful half; the category alone does "
        "not tell an auditor why THIS row went")


@pytest.mark.parametrize("name,build", BUILDERS, ids=[b[0] for b in BUILDERS])
def test_the_week_stamp_comes_from_the_capture_module(name, build):
    assert build()["Week_Added"] == review_day_for().isoformat()


@pytest.mark.parametrize("name,build", BUILDERS, ids=[b[0] for b in BUILDERS])
def test_the_week_stamp_is_a_sunday(name, build):
    from datetime import date
    y, m, d = (int(x) for x in build()["Week_Added"].split("-"))
    assert date(y, m, d).weekday() == 6


@pytest.mark.parametrize("name,build", BUILDERS, ids=[b[0] for b in BUILDERS])
def test_the_row_is_unchanged_otherwise(name, build):
    """The fix is additive. Nothing the row already carried may move.

    ONE FIELD MOVED DELIBERATELY, 2026-09-27: Status was pinned here as
    "Rejected" with a capital R, which is what these four builders wrote. All
    three reviewers selected their lane with `.strip()` and no `.lower()`
    against lowercase status sets, so a capitalised status matched no lane and
    the row was invisible to every reviewer — 86 rows across eight countries
    (Spain, Greece, Portugal, Nigeria, South Africa, Italy, Poland, Norway).
    The builders now write the canonical lowercase value and the reviewers
    lowercase before comparing; see
    tests/test_no_country_is_invisible_to_the_reviewers.py.

    The capitalisation was pinned here incidentally, alongside Source and URL,
    rather than because anything depended on it. The assertion is kept — this
    test's job is to notice when a field moves — and now states which value is
    correct and why.
    """
    row = build()
    assert row["Source"] in ("Salute", "EFET", "AESAN")
    assert row["URL"] == "https://example.test/richiamo-1"
    assert row["Status"] == "rejected", (
        "must be the canonical lowercase status — a capitalised one is "
        "invisible to every reviewer's lane")
    assert row["Reason"] == "milk not declared on label"
    assert row["Pathogen"] == "milk"


@pytest.mark.parametrize("mod", MODULES)
def test_the_helper_does_not_recompute_the_date(mod):
    src = _src(mod)
    h = src[src.index("def _review_week"):]
    h = h[:h.index("\ndef ", 1)] if "\ndef " in h[1:] else h
    assert "review_day_for" in h
    assert "timedelta" not in h, (
        "a second copy of the review-Sunday math is exactly the drift that "
        "made every Sunday email report 0 recalls")


@pytest.mark.parametrize("mod", MODULES)
def test_every_reason_override_has_a_matching_reason(mod):
    """The authority-URL gate and the LLM-failure path both REPLACE Reason
    after the builder runs. Left alone, RejectionReason would report the
    classifier's category for a row the classifier never reached."""
    src = _src(mod)
    o = src.count('rejected["Reason"] = (')
    p = src.count('rejected["RejectionReason"] = (')
    assert p >= o, (
        f"{mod} overrides Reason {o}x but sets RejectionReason only {p}x — "
        f"a rejection path still reports a category it never computed")


def test_nothing_the_live_header_names_lands_empty():
    openpyxl = pytest.importorskip("openpyxl")
    xlsx = ROOT / "docs" / "data" / "recalls.xlsx"
    if not xlsx.exists():
        pytest.skip("no workbook")
    wb = openpyxl.load_workbook(xlsx, read_only=True)
    if "Weekly_Rejected" not in wb.sheetnames:
        pytest.skip("no sheet")
    headers = [str(c.value or "") for c in wb["Weekly_Rejected"][1]]
    for name, build in BUILDERS:
        row = build()
        blank = [h for h in MUST_BE_SET
                 if h in headers and not str(row.get(h, "")).strip()]
        assert not blank, f"{name} lands blank in {blank} on the live sheet"


def test_only_dateless_rows_remain_unstamped_on_the_live_sheet():
    """The backfill covers every row that HAS a Date.

    Three rows resist it, all from gap_finder/pt, and they are a different
    defect: Date = None AND Company = None. A review week cannot be
    reconstructed from a row with no date, and inventing one would put a
    rejection in an arbitrary Sunday email. They stay blank and visible.
    """
    openpyxl = pytest.importorskip("openpyxl")
    xlsx = ROOT / "docs" / "data" / "recalls.xlsx"
    if not xlsx.exists():
        pytest.skip("no workbook")
    ws = openpyxl.load_workbook(xlsx, read_only=True)["Weekly_Rejected"]
    h = [str(c.value or "") for c in ws[1]]
    iw, idt = h.index("Week_Added"), h.index("Date")
    bad = [r for r in ws.iter_rows(min_row=2, values_only=True)
           if not str(r[iw] or "").strip() and str(r[idt] or "").strip()]
    assert not bad, (
        f"{len(bad)} row(s) have a Date but no Week_Added — the backfill "
        f"missed them")
