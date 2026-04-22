"""
cleanup_stale_daily_briefs.py
=============================
One-off cleanup. Run ONCE at repo root to delete every HTML file in
docs/daily/ and empty docs/daily-index.json.

Why: the previous daily_recall_search.py wrote HTML briefs directly
from OpenAI's raw JSON, which polluted docs/daily/ with entries that
never existed in the Recalls sheet (Nestle Colombia cereulide,
VFA Vietnam HiPP, Greenstorm Peanut allergen leak, etc.). The new
daily_recall_search.py renders briefs from the Recalls sheet only, so
the next 10:00 Athens run will generate a correct brief.

Running this script before deploying the new pipeline prevents the
dashboard from showing a mix of old (bogus) and new (correct) briefs.

Usage:
    cd <repo root>
    python cleanup_stale_daily_briefs.py          # dry run
    python cleanup_stale_daily_briefs.py --apply  # actually delete
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
                    help="Actually delete files (default: dry run)")
    args = ap.parse_args()

    mode = "APPLY" if args.apply else "DRY RUN"
    print(f"=== Stale daily briefs cleanup — {mode} ===\n")

    html_files: list = []
    if DAILY_DIR.exists():
        for f in sorted(DAILY_DIR.glob("*.html")):
            html_files.append(f)
            print(f"  Will delete: {f.relative_to(ROOT)}")
    else:
        print(f"  (no {DAILY_DIR.relative_to(ROOT)} directory — nothing to clean)")

    if DAILY_INDEX.exists():
        print(f"  Will reset:  {DAILY_INDEX.relative_to(ROOT)} (empty entries)")

    if not args.apply:
        print("\n(dry run — no files touched. Re-run with --apply to commit.)")
        return 0

    print("\nApplying…")
    for f in html_files:
        try:
            f.unlink()
            print(f"  Deleted {f.name}")
        except Exception as e:
            print(f"  FAILED  {f.name}: {e}")

    if DAILY_INDEX.exists():
        DAILY_INDEX.write_text(json.dumps({"entries": []}, indent=2))
        print(f"  Reset   {DAILY_INDEX.relative_to(ROOT)}")

    print("\nDone. Commit the cleanup:")
    print("    git add docs/daily/ docs/daily-index.json")
    print("    git commit -m 'Clear stale daily briefs before pipeline rewrite'")
    print("    git push")
    print("\nThen deploy pipeline/daily_recall_search.py, and the next 10:00")
    print("Athens run will produce a clean brief from the Recalls sheet.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
