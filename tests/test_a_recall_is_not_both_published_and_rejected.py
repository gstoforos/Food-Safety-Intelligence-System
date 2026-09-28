"""Two faults the 2026-09-28 morning sweep found, pinned.

FAULT 1 — A RECALL CANNOT BE BOTH PUBLISHED AND THROWN AWAY
-----------------------------------------------------------
The sweep counted **31 URLs that are simultaneously published and rejected** —
21 shared between Recalls and Weekly_Rejected, 10 between Recalls and Rejected
— and stated the consequence exactly: "whichever copy a reader hits first
decides what the register says."

Twelve of them were mine. On 2026-09-27 I added a narrow exception to
merge_master's re-promotion guard so a row archived for a REPAIRABLE defect
("Pathogen is empty") could return once the defect was repaired. It worked —
eleven real recalls came back, five Tier 1 — but it only ever wrote to
Recalls. The Weekly_Rejected row saying the recall had been discarded stayed
put. Fixing the promotion without retiring the rejection is half a fix.

NOT EVERY SHARED URL IS A CONTRADICTION, and this test must not pretend
otherwise:

  * **Duplicates.** Seven rows are the RASFF copies archived on 2026-09-27.
    Once their malformed reference URLs were repaired, the archived copy and
    the kept copy legitimately share one address — they were always the same
    notification.
  * **Corrected re-publications.** Two USDA FSIS rows were rejected as
    "fabricated_pathogen_and_out_of_scope" for carrying Pathogen "Hepatitis A
    virus" on recalls whose real hazard was production without inspection. The
    rows published under those URLs now carry the correct "Uninspected product
    (hazard not assessed)". The rejection is the audit trail of a fabrication;
    the published row is its repair.

So the rule is not "no shared URLs". It is: **every shared URL must SAY which
copy wins** — SUPERSEDED, a duplicate note, or an explicit unresolved flag. A
shared URL with a silent archive row is the fault.

FAULT 2 — A SCRAPE DATE IS NOT A PUBLISH DATE
----------------------------------------------
Two FSAI rows sat in Pending stamped ``Date=2026-09-28`` with
``ScrapedAt=2026-09-28T01:10:40Z`` while FSAI's own list dated both alerts to
**18 September**. The listing fallback defaulted an unparsable date to
``today``.

Every window filter in this system keys on Date — the daily sweep's in-window
test, the weekly builder's report assignment, the signal detector's ISO week.
Defaulting to today makes a ten-day-old alert look new to all three at once,
and leaves no way to tell an undated row from a genuinely-today one. An
unknown date is now left EMPTY, where the publish gate blocks it, because a
missing value that blocks is safer than a plausible value that lies.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
XLSX = ROOT / "docs" / "data" / "recalls.xlsx"

pd = pytest.importorskip("pandas")

#: Markers that make a shared URL self-explaining rather than contradictory.
RESOLVED = ("superseded", "duplicate", "unresolved contradiction",
            "broken provenance", "already_approved")


#: Sheets where Date DRIVES BEHAVIOUR: the daily sweep's in-window test, the
#: weekly builder's report assignment, the signal detector's ISO week, and the
#: promotion queue feeding all three. `Rejected` is a TERMINAL archive — no
#: filter, report or detector reads it — so a bad Date there is untidy rather
#: than harmful, and mass-editing settled audit rows to satisfy a test would
#: cost more than it buys. It is excluded deliberately, not because it passes.
LIVE_SHEETS = ("Recalls", "Pending", "Weekly_Review", "Weekly_Rejected")


def _sheets():
    if not XLSX.exists():                                   # pragma: no cover
        pytest.skip("no workbook")
    x = pd.ExcelFile(XLSX)
    return x, {s: pd.read_excel(x, s) for s in x.sheet_names}


def _norm(series):
    return series.astype(str).str.strip().str.lower()


@pytest.mark.parametrize("sheet,reason_col",
                         [("Weekly_Rejected", "RejectionReason"),
                          ("Rejected", "RejectReason")])
def test_every_shared_url_says_which_copy_wins(sheet, reason_col):
    _, sh = _sheets()
    if sheet not in sh or "Recalls" not in sh:
        pytest.skip(f"{sheet} not present")
    published = set(_norm(sh["Recalls"]["URL"])) - {"", "nan"}
    arch = sh[sheet]
    if reason_col not in arch.columns:
        pytest.skip(f"{sheet} has no {reason_col}")

    silent = []
    for _, r in arch.iterrows():
        if str(r.get("URL") or "").strip().lower() not in published:
            continue
        reason = str(r.get(reason_col) or "").lower()
        if not any(m in reason for m in RESOLVED):
            silent.append((str(r.get("Date"))[:10], str(r.get("Product"))[:40],
                           reason[:60]))
    assert not silent, (
        f"{len(silent)} row(s) in {sheet} share a URL with a PUBLISHED recall "
        f"and say nothing about which copy wins:\n  " +
        "\n  ".join(map(str, silent[:6])) +
        "\n\nA recall cannot be both published and thrown away. Mark the "
        "archive row SUPERSEDED (the defect was repaired), note it as a "
        "duplicate, or flag it as an unresolved contradiction — but it must "
        "not sit there silently contradicting the register.")


def test_no_row_carries_its_own_scrape_time_as_a_publish_date():
    """Date == the date part of ScrapedAt, on a row that names no other date."""
    _, sh = _sheets()
    bad = []
    for name, df in sh.items():
        if name not in LIVE_SHEETS:
            continue
        if "Date" not in df.columns or "ScrapedAt" not in df.columns:
            continue
        for _, r in df.iterrows():
            d = str(r.get("Date") or "")[:10]
            s = str(r.get("ScrapedAt") or "")[:10]
            if not d or d in ("nan", "NaT") or not s or s in ("nan", "NaT"):
                continue
            notes = str(r.get("Notes") or "").lower()
            if d == s and "date-cleared" not in notes and "confirmed" not in notes:
                bad.append((name, d, str(r.get("Source"))[:14],
                            str(r.get("Product"))[:38]))
    assert not bad, (
        f"{len(bad)} row(s) carry their own scrape date as the publish date:\n  " +
        "\n  ".join(map(str, bad[:6])) +
        "\n\nEvery window filter keys on Date — the daily sweep, the weekly "
        "builder and the signal detector. A scrape date here makes an old "
        "alert look like today's to all three. Leave an unknown date EMPTY; "
        "the publish gate will hold the row until it is enriched.")


def test_the_fsai_fallback_does_not_default_a_date_to_today():
    """The code-side half, so the data-side fix cannot silently regress."""
    f = ROOT / "scrapers" / "europe_eu" / "fsai.py"
    if not f.exists():                                      # pragma: no cover
        pytest.skip("fsai scraper not present")
    body = "\n".join(l for l in f.read_text(encoding="utf-8").splitlines()
                     if not l.strip().startswith("#"))
    assert not re.search(r"_parse_pubdate\([^)]*\)\s+or\s+today", body), (
        "scrapers/europe_eu/fsai.py still falls back to `today` when it cannot "
        "parse a listing date. That is what stamped two 18-September alerts "
        "with 2026-09-28.")
    assert "if d else" in body or "if d is not None" in body, (
        "the fallback no longer defaults to today, but nothing guards the "
        "emit site — d may be None when Date is written.")


def test_no_date_field_is_non_iso():
    """'27 January 2022' sorts and filters as nothing at all."""
    _, sh = _sheets()
    bad = []
    for name, df in sh.items():
        if name not in LIVE_SHEETS:
            continue
        if "Date" not in df.columns:
            continue
        for v in df["Date"]:
            v = str(v)
            if v in ("nan", "", "NaT"):
                continue
            if not re.match(r"^\d{4}-\d{2}-\d{2}", v):
                bad.append((name, v))
    assert not bad, (
        f"{len(bad)} non-ISO Date value(s): {bad[:5]}. Every window filter "
        f"does a string or datetime comparison on this column.")
