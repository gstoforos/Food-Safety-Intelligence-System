"""
backfill_recent_briefs.py
=========================
One-off repair: rebuild the last few days of daily briefs from recalls.xlsx
data, so the dashboard has something to show right now instead of waiting
for tomorrow's 10:00 Athens scheduled run.

Why this is needed:
  - On Apr 22 2026 the daily_recall_search.py run returned zero recalls
    (prompt too strict about date matching). The old retention-1 policy
    then wiped Apr 21's legitimate brief from the dashboard.
  - The new retention-7 policy (now in daily_recall_search.py) prevents
    this from happening again going forward, but it doesn't bring back
    the briefs that were already deleted.
  - This script reads docs/data/recalls.xlsx Recalls sheet, synthesizes
    HTML briefs for each of the last N days from rows already in that
    sheet, and updates daily-index.json so the dashboard renders them.

Usage:
    cd <repo root>
    python backfill_recent_briefs.py           # dry run — shows plan
    python backfill_recent_briefs.py --apply   # actually write files
    python backfill_recent_briefs.py --apply --days 5   # last 5 days

Source of truth: docs/data/recalls.xlsx Recalls sheet. If a recall isn't
in there, it won't appear in a backfilled brief. For the days in question
(Apr 18-22 2026), the earlier manual edits already added the missed
Listeria/STEC/histamine/Alternaria rows to Recalls, so a backfill will
pick them up.

Safe: dry-run by default. --apply actually writes to docs/daily/ and
docs/daily-index.json. No commits — you commit after reviewing.
"""
from __future__ import annotations
import argparse
import json
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

# Import from the shipped module
from pipeline.daily_recall_search import (  # noqa: E402
    render_daily_html, DAILY_DIR, DAILY_INDEX,
)
from pipeline.merge_master import load_existing  # noqa: E402
from scrapers._models import Recall  # noqa: E402


XLSX_PATH = ROOT / "docs" / "data" / "recalls.xlsx"


def row_to_recall(row: dict) -> Recall:
    """Convert a Recalls-sheet row dict (as returned by load_existing) → Recall."""
    # load_existing() returns rows with capitalized keys matching the xlsx columns
    return Recall(
        Date=str(row.get("Date") or "")[:10],
        Source=str(row.get("Source") or ""),
        Company=str(row.get("Company") or ""),
        Brand=str(row.get("Brand") or "—"),
        Product=str(row.get("Product") or ""),
        Pathogen=str(row.get("Pathogen") or ""),
        Reason=str(row.get("Reason") or ""),
        Class=str(row.get("Class") or "Recall"),
        Country=str(row.get("Country") or ""),
        Region=str(row.get("Region") or ""),
        Tier=int(row.get("Tier") or 2),
        Outbreak=int(row.get("Outbreak") or 0),
        URL=str(row.get("URL") or ""),
        Notes=str(row.get("Notes") or ""),
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=3,
                    help="Backfill the last N days (default 3)")
    ap.add_argument("--apply", action="store_true",
                    help="Actually write files (default: dry run)")
    ap.add_argument("--from-date", type=str, default=None,
                    help="End date YYYY-MM-DD (default: yesterday Athens)")
    args = ap.parse_args()

    # Target date anchor (yesterday Athens time, unless --from-date given)
    if args.from_date:
        end_date = date.fromisoformat(args.from_date)
    else:
        try:
            from zoneinfo import ZoneInfo
            from datetime import datetime
            now_athens = datetime.now(ZoneInfo("Europe/Athens"))
            end_date = (now_athens - timedelta(days=1)).date()
        except Exception:
            end_date = date.today() - timedelta(days=1)

    start_date = end_date - timedelta(days=args.days - 1)

    mode = "APPLY" if args.apply else "DRY RUN"
    print(f"=== Backfill daily briefs — {mode} ===\n")
    print(f"Window:   {start_date} to {end_date}  ({args.days} days)")
    print(f"Source:   {XLSX_PATH}")
    print(f"Outputs:  {DAILY_DIR}/ + {DAILY_INDEX}\n")

    if not XLSX_PATH.exists():
        print(f"ERROR: {XLSX_PATH} not found.")
        return 1

    # Load every recall from the xlsx
    raw_rows = load_existing(XLSX_PATH)
    print(f"Loaded {len(raw_rows)} total rows from Recalls sheet")

    # Bucket rows by Date (YYYY-MM-DD)
    by_date: dict = {}
    for r in raw_rows:
        d = str(r.get("Date", ""))[:10]
        if not d:
            continue
        by_date.setdefault(d, []).append(r)

    # Load existing daily-index entries so we don't overwrite days we shouldn't
    existing_index = []
    if DAILY_INDEX.exists():
        try:
            existing_index = json.loads(DAILY_INDEX.read_text()).get("entries", [])
        except Exception:
            pass
    existing_dates = {e.get("date") for e in existing_index}

    # Walk the window
    plan = []
    d = start_date
    while d <= end_date:
        iso = d.isoformat()
        rows = by_date.get(iso, [])
        recalls = [row_to_recall(r) for r in rows]
        # Regions summary
        region_counts: dict = {}
        for r in recalls:
            region_counts[r.Region or "Other"] = region_counts.get(r.Region or "Other", 0) + 1
        plan.append({
            "date": iso,
            "count": len(recalls),
            "tier1": sum(1 for r in recalls if r.Tier == 1),
            "outbreak": sum(1 for r in recalls if r.Outbreak == 1),
            "regions": region_counts,
            "recalls": recalls,
            "already_in_index": iso in existing_dates,
        })
        d += timedelta(days=1)

    print(f"Plan:")
    for p in plan:
        reg = ", ".join(f"{k}={v}" for k, v in p["regions"].items()) or "—"
        flag = "(exists)" if p["already_in_index"] else "(new)"
        print(f"  {p['date']}  {p['count']:>3} recalls  T1={p['tier1']}  "
              f"OB={p['outbreak']}  regions: {reg}  {flag}")

    if not args.apply:
        print("\n(dry run — nothing written. Re-run with --apply to commit.)")
        return 0

    # Apply — write HTML + update index
    print("\nApplying…")
    DAILY_DIR.mkdir(parents=True, exist_ok=True)

    # Build new index entries for the window
    window_entries = []
    for p in plan:
        iso = p["date"]
        html = render_daily_html(
            date.fromisoformat(iso), p["recalls"], 5
        )
        html_path = DAILY_DIR / f"{iso}.html"
        html_path.write_text(html, encoding="utf-8")
        print(f"  Wrote {html_path.relative_to(ROOT)}")
        window_entries.append({
            "date": iso,
            "url": f"daily/{iso}.html",
            "total": p["count"],
            "tier1": p["tier1"],
            "outbreak": p["outbreak"],
            "by_region": p["regions"],
            "generated": "BACKFILLED " + date.today().isoformat(),
        })

    # Merge with any existing entries outside the backfill window, sort
    # newest-first, trim to 7 days total.
    window_dates = {e["date"] for e in window_entries}
    merged = (
        [e for e in existing_index if e.get("date") not in window_dates]
        + window_entries
    )
    merged.sort(key=lambda e: e.get("date", ""), reverse=True)
    cutoff = (end_date - timedelta(days=6)).isoformat()
    merged = [e for e in merged if e.get("date", "") >= cutoff]

    DAILY_INDEX.write_text(json.dumps({"entries": merged}, indent=2))
    print(f"  Wrote {DAILY_INDEX.relative_to(ROOT)} ({len(merged)} entries)")

    print("\nDone. Review the changes, then:")
    print("  git add docs/daily/ docs/daily-index.json")
    print("  git commit -m 'Backfill recent daily briefs from recalls.xlsx'")
    print("  git push")
    return 0


if __name__ == "__main__":
    sys.exit(main())
