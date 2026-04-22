"""
cleanup_daily_briefs.py
=======================
One-off cleanup. Run once from the repo root after pulling the updated
daily_recall_search.py to fix the current state of docs/daily-index.json
and docs/daily/*.html.

What it does:
  1. Reads docs/daily-index.json
  2. Drops every entry whose corresponding daily/YYYY-MM-DD.html file
     doesn't actually exist on disk (fixes "Open brief opens nothing"
     because the sample index I gave earlier referenced HTML files
     that were never generated).
  3. Keeps only the MOST-RECENT valid entry (matches the new
     retention-1 policy: dashboard shows exactly one card = yesterday).
  4. Deletes every other daily/*.html file on disk so stale briefs
     don't accumulate.
  5. Writes the trimmed daily-index.json back.

After this runs, the Daily tab will show either:
  - Exactly one card (yesterday's brief), if the HTML file exists
  - "No daily briefs yet" empty state, if no valid HTML file exists

Either state is correct — tomorrow morning the 10:00 Athens workflow
generates a fresh daily/<yesterday>.html + updates the index to point
to it, and this retention logic kicks in on every subsequent run.

Usage:
    cd <repo root>
    python cleanup_daily_briefs.py          # dry-run preview
    python cleanup_daily_briefs.py --apply  # actually delete + rewrite

Safe: prints everything it would do in preview mode before you commit
to --apply. Writes go to docs/daily-index.json and docs/daily/*.html
only; nothing else is touched.
"""
from __future__ import annotations
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DAILY_DIR = ROOT / "docs" / "daily"
DAILY_INDEX = ROOT / "docs" / "daily-index.json"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true",
                    help="Actually perform the cleanup (default: dry run)")
    args = ap.parse_args()

    mode = "APPLY" if args.apply else "DRY RUN"
    print(f"=== Daily Briefs Cleanup — {mode} ===\n")

    # Load current state
    if not DAILY_INDEX.exists():
        print(f"No {DAILY_INDEX} — nothing to clean.")
        return 0

    try:
        data = json.loads(DAILY_INDEX.read_text())
    except Exception as e:
        print(f"ERROR: could not parse {DAILY_INDEX}: {e}")
        return 1

    entries = data.get("entries", [])
    print(f"Found {len(entries)} entries in daily-index.json")

    # Partition into valid (HTML exists) and invalid (orphan entries)
    valid, orphan = [], []
    for e in entries:
        iso = e.get("date", "")
        if not iso:
            orphan.append(e)
            continue
        html_path = DAILY_DIR / f"{iso}.html"
        if html_path.exists():
            valid.append(e)
        else:
            orphan.append(e)

    print(f"  {len(valid)} entries have matching HTML files")
    print(f"  {len(orphan)} entries are orphans (no HTML file)")
    for e in orphan:
        print(f"    ORPHAN  {e.get('date', '???')}  (→ {e.get('url', '?')} missing)")

    # Sort newest-first; keep only the most recent valid entry
    valid.sort(key=lambda e: e.get("date", ""), reverse=True)
    keep = valid[:1]
    drop = valid[1:]

    print()
    if keep:
        k = keep[0]
        print(f"KEEP: {k['date']}  ({k.get('total',0)} recalls, "
              f"{k.get('tier1',0)} Tier-1, {k.get('outbreak',0)} outbreak)")
    else:
        print("KEEP: (nothing — no valid entry to keep, dashboard will show empty state)")
    for e in drop:
        print(f"DROP (too old):     {e.get('date','???')}  ({e.get('total',0)} recalls)")

    keep_dates = {e["date"] for e in keep}

    # Find HTML files to delete
    html_to_delete = []
    if DAILY_DIR.exists():
        for f in sorted(DAILY_DIR.glob("*.html")):
            stem = f.stem
            # Only touch YYYY-MM-DD.html files
            if len(stem) == 10 and stem[4] == "-" and stem[7] == "-":
                if stem not in keep_dates:
                    html_to_delete.append(f)

    print()
    print(f"HTML files to delete from docs/daily/: {len(html_to_delete)}")
    for f in html_to_delete:
        print(f"  DELETE  {f.name}")

    # Apply phase
    if not args.apply:
        print("\n(dry run — no changes made. Re-run with --apply to commit.)")
        return 0

    print("\nApplying…")
    # Delete orphan + too-old HTML files
    for f in html_to_delete:
        try:
            f.unlink()
            print(f"  Deleted  {f.name}")
        except Exception as e:
            print(f"  FAILED   {f.name}: {e}")

    # Write trimmed daily-index.json
    DAILY_INDEX.write_text(json.dumps({"entries": keep}, indent=2))
    print(f"  Wrote    {DAILY_INDEX} ({len(keep)} entry)")

    print("\nDone. Commit the changes:")
    print("    git add docs/daily-index.json docs/daily/")
    print("    git commit -m 'Clean up daily briefs: retain yesterday only'")
    print("    git push")
    return 0


if __name__ == "__main__":
    sys.exit(main())
