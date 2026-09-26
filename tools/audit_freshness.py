#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Which parts of the register are actually awake?

    python3 tools/audit_freshness.py
    python3 tools/audit_freshness.py --json out.json
    python3 tools/audit_freshness.py --fail-on-stale     # for CI

WHY
---
"Did the weekly report run?" is not answerable by looking at git. Every
artefact in docs/data/ gets rewritten by something most days, so file
timestamps say "yes" for things that are in fact producing nothing.

The 2026-09-14 audit found the sharpest version of this. Every regional
gap finder is dispatched daily by the Apps Script scheduler, every run
goes green, and every run commits a message like

    "Greek gap finder: 2026-09-14 auto-update (merge attempt 1)"

That commit changed recalls.xlsx by 809 bytes and nothing else. The
Greek finder's own run log says its last real run was **2026-07-08**.
The Nordic five last ran 2026-05-31; the Central EU eight, 2026-06-14.
Three months dark, green the whole time, with a daily commit apiece
saying otherwise.

The mechanism is a deliberate one: the VPS health-check step is
``continue-on-error: true`` (added 2026-08-24, because one unreachable
box was turning the whole fleet red and burying the real cause). That
was the right call for the fleet. What it left behind is a run that
cannot fail — so the only honest signal is what each component last
WROTE, not whether its job exited zero.

So this reads each component's own record of its last real output:
a run log's last entry, a summary's generated_utc, a register row's
DateAdded. Not the commit date.

WHAT "STALE" MEANS HERE
-----------------------
Each check carries its own expectation, because a weekly report is not
late at four days old and a daily brief is. A check reports STALE only
past its own threshold, and every line prints the age so a judgement
call stays possible.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "docs" / "data"

OK, WARN, STALE, MISSING = "OK", "WARN", "STALE", "MISSING"


def _age_days(when: str | None, today: dt.date) -> int | None:
    if not when:
        return None
    try:
        return (today - dt.date.fromisoformat(str(when)[:10])).days
    except ValueError:
        return None


def _verdict(age: int | None, warn_days: int, stale_days: int) -> str:
    if age is None:
        return MISSING
    if age >= stale_days:
        return STALE
    if age >= warn_days:
        return WARN
    return OK


def _load_json(path: Path):
    """utf-8-sig: at least one file in docs/data/ carries a BOM."""
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:                                    # noqa: BLE001
        return None


# ---------------------------------------------------------------------------
# checks
# ---------------------------------------------------------------------------

def check_register(today):
    """Rows reaching the register, by their own DateAdded stamp."""
    from openpyxl import load_workbook
    xlsx = DATA / "recalls.xlsx"
    if not xlsx.exists():
        return [dict(component="register (xlsx)", verdict=MISSING,
                     detail="docs/data/recalls.xlsx not found")]

    wb = load_workbook(xlsx, read_only=True, data_only=True)
    rows = list(wb["Recalls"].iter_rows(values_only=True))
    hdr = [str(c or "") for c in rows[0]]
    recs = [dict(zip(hdr, r)) for r in rows[1:]]
    added = [str(r.get("DateAdded") or "")[:10] for r in recs]
    last = max((a for a in added if a), default=None)
    recent = sum(1 for a in added if a and _age_days(a, today) is not None
                 and _age_days(a, today) <= 7)

    out = [dict(component="register (xlsx)", last=last,
                age=_age_days(last, today),
                verdict=_verdict(_age_days(last, today), 2, 4),
                detail="%d rows total, %d added in the last 7 days"
                       % (len(recs), recent))]

    js = DATA / "recalls.json"
    if js.exists():
        pub = _load_json(js) or []
        drift = len(recs) - len(pub)
        out.append(dict(
            component="public mirror (json)", last=last, age=0,
            verdict=OK if drift == 0 else STALE,
            detail="json %d rows vs xlsx %d — %s"
                   % (len(pub), len(recs),
                      "in sync" if drift == 0 else
                      "DRIFT of %d; the site is serving a different register "
                      "from the workbook" % drift)))

    pend = list(wb["Pending"].iter_rows(values_only=True))
    out.append(dict(component="pending queue", last=None, age=None,
                    verdict=OK if len(pend) - 1 < 25 else WARN,
                    detail="%d row(s) waiting on review" % (len(pend) - 1)))
    wb.close()
    return out


def check_summaries(today):
    out = []
    for name, label, warn, stale in (
        ("weekly-summary-latest.json", "weekly report", 8, 10),
        ("monthly-summary-latest.json", "monthly report", 35, 40),
        ("weekly-review-latest.json", "Sunday review capture", 8, 10),
        ("scraper-health.json", "scraper health", 8, 10),
        ("signals-latest.json", "signals board", 3, 7),
    ):
        p = DATA / name
        if not p.exists():
            out.append(dict(component=label, verdict=MISSING, detail=str(p)))
            continue
        d = _load_json(p) or {}
        # signals-latest.json nests its stamp under "meta"; the others put it
        # at the top level. Look in both rather than reporting a false MISSING.
        meta = d.get("meta") if isinstance(d.get("meta"), dict) else {}
        when = (d.get("generated_utc") or d.get("generated")
                or d.get("generated_at") or d.get("built_utc")
                or meta.get("generated_utc") or meta.get("generated")
                or meta.get("generated_at") or meta.get("week_end"))
        age = _age_days(when, today)
        out.append(dict(component=label, last=str(when or "")[:19], age=age,
                        verdict=_verdict(age, warn, stale),
                        detail=d.get("filename") or ""))
    return out


def check_gap_finders(today):
    """Each finder's own run log — the one thing a green no-op cannot fake."""
    out = []
    for d in sorted(DATA.glob("gap_finder_*")):
        cc = d.name.replace("gap_finder_", "")
        log = d / "run_log.jsonl"
        if not log.exists():
            out.append(dict(component="gap finder %s" % cc, verdict=MISSING,
                            detail="no run_log.jsonl"))
            continue
        lines = [l for l in log.read_text(encoding="utf-8-sig").splitlines() if l.strip()]
        if not lines:
            out.append(dict(component="gap finder %s" % cc, verdict=MISSING,
                            detail="run_log is empty"))
            continue
        try:
            rec = json.loads(lines[-1])
        except Exception:                                # noqa: BLE001
            out.append(dict(component="gap finder %s" % cc, verdict=MISSING,
                            detail="run_log tail is not JSON"))
            continue
        when = rec.get("started_at") or rec.get("ts")
        age = _age_days(when, today)
        verdict = _verdict(age, 3, 7)
        detail = ("candidates=%s verified=%s"
                  % (rec.get("candidates_found"), rec.get("verified_count")))

        # ── A fresh run is not the same as a useful one (2026-09-25) ─────
        #
        # This check graded on AGE alone, and age is exactly what an LLM
        # outage does not affect. When the Llama box is unreachable, the
        # extractor routes every row that classified ACCEPTED to Rejected,
        # the finder exits 0, the run log says status="completed", and this
        # line reported "candidates=14 verified=9" and a verdict of OK.
        # Five real recalls are in Rejected from exactly that: South
        # Africa's Deli Hummus Listeria recall twice (08-09, 09-21),
        # Czechia 09-15, Poland 09-19 and 09-21.
        #
        # llm_extraction_failures was added to the run log the same day. A
        # run that lost EVERY accepted row to the box is STALE whatever its
        # timestamp says — it produced nothing. A run that lost some is a
        # WARN, unless age already makes it worse.
        llm_lost = int(rec.get("llm_extraction_failures") or 0)
        if llm_lost:
            accepted = int(rec.get("extracted_accepted") or 0) + llm_lost
            detail += ("; %d of %d accepted rows lost to an unreachable "
                       "Llama box — routed to Rejected, NOT content "
                       "rejections" % (llm_lost, accepted))
            if accepted and llm_lost >= accepted:
                verdict = STALE
                detail = ("PRODUCED NOTHING USABLE: " + detail)
            elif verdict == OK:
                verdict = WARN

        out.append(dict(
            component="gap finder %s" % cc, last=str(when or "")[:16], age=age,
            verdict=verdict, detail=detail))
    return out


def check_daily(today):
    pages = sorted(p.stem for p in (ROOT / "docs" / "daily").glob("*.html")) \
        if (ROOT / "docs" / "daily").exists() else []
    newest = pages[-1] if pages else None
    out = [dict(component="daily briefs", last=newest,
                age=_age_days(newest, today),
                verdict=_verdict(_age_days(newest, today), 2, 3),
                detail="%d page(s) on disk" % len(pages))]

    live = ROOT / "docs" / "daily-index.json"
    idx = _load_json(live) or {}
    entries = idx.get("entries") if isinstance(idx, dict) else idx
    entries = entries or []
    dates = sorted(str(e.get("date", ""))[:10] for e in entries
                   if isinstance(e, dict))
    newest_idx = dates[-1] if dates else None
    missing = [p for p in pages if p not in dates]
    out.append(dict(
        component="daily index", last=newest_idx,
        age=_age_days(newest_idx, today),
        verdict=STALE if missing else _verdict(_age_days(newest_idx, today), 2, 3),
        detail="%d entr(ies)%s" % (
            len(dates),
            "; %d page(s) NOT indexed — those briefs are unreachable from the "
            "dashboard" % len(missing) if missing else "")))

    # The orphan. docs/data/daily-index.json is read by nothing; it shadows
    # the live file above and has its own, wronger, contents.
    orphan = DATA / "daily-index.json"
    if orphan.exists():
        out.append(dict(
            component="daily index (orphan copy)", verdict=WARN,
            last=None, age=None,
            detail="docs/data/daily-index.json exists and is read by nothing. "
                   "The live file is docs/daily-index.json. Two files, one "
                   "name, different contents — delete this one."))
    return out


def check_encoding(today):
    """Files carrying a UTF-8 BOM.

    Not a freshness problem — a loading one, and it belongs in the same
    report because it has the same shape: the file looks present and
    current, and the consumer falls over. Plain ``json.load`` raises
    "Unexpected UTF-8 BOM" on these; only a reader that passes
    ``encoding="utf-8-sig"`` survives. Found on docs/data/signals-latest.json
    and docs/data/source-coverage.json during the 2026-09-14 audit.
    """
    out = []
    for p in sorted(DATA.rglob("*.json")):
        try:
            head = p.open("rb").read(3)
        except OSError:
            continue
        if head == b"\xef\xbb\xbf":
            out.append(dict(
                component="BOM: %s" % p.name, verdict=WARN, last=None, age=None,
                detail="starts with a UTF-8 BOM; json.load() raises unless the "
                       "reader passes encoding='utf-8-sig'"))
    return out


CHECKS = (check_register, check_summaries, check_gap_finders, check_daily,
          check_encoding)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--json", metavar="PATH")
    ap.add_argument("--fail-on-stale", action="store_true")
    ap.add_argument("--today", default=None, help="override for testing")
    args = ap.parse_args()

    today = dt.date.fromisoformat(args.today) if args.today else dt.date.today()

    rows = []
    for fn in CHECKS:
        try:
            rows.extend(fn(today))
        except Exception as exc:                         # noqa: BLE001
            rows.append(dict(component=fn.__name__, verdict=MISSING,
                             detail="check itself failed: %s" % exc))

    order = {STALE: 0, MISSING: 1, WARN: 2, OK: 3}
    rows.sort(key=lambda r: (order.get(r.get("verdict"), 9),
                             -(r.get("age") or 0)))

    print("\nFSIS freshness — %s\n" % today)
    print("%-8s %-26s %-18s %5s  %s" % ("", "COMPONENT", "LAST OUTPUT", "AGE", "DETAIL"))
    print("-" * 108)
    for r in rows:
        age = r.get("age")
        print("%-8s %-26s %-18s %5s  %s" % (
            r.get("verdict", "?"), r.get("component", "")[:26],
            str(r.get("last") or "-")[:18],
            ("%dd" % age) if age is not None else "-",
            str(r.get("detail") or "")[:56]))

    stale = [r for r in rows if r.get("verdict") in (STALE, MISSING)]
    if stale:
        print("\nNot producing:")
        for r in stale:
            print("  %-26s last output %s%s"
                  % (r["component"], r.get("last") or "never",
                     "  (%dd ago)" % r["age"] if r.get("age") is not None else ""))
        print("\nA green workflow is not evidence here. The regional gap "
              "finders' health-check step is continue-on-error, so a run with "
              "no model behind it\nstill exits zero and still commits an "
              "'auto-update' message. Read the run log, not the commit log.")

    if args.json:
        Path(args.json).write_text(json.dumps(rows, ensure_ascii=False, indent=1),
                                   encoding="utf-8")
        print("\nwrote %s" % args.json)

    return 1 if (args.fail_on_stale and stale) else 0


if __name__ == "__main__":
    raise SystemExit(main())
