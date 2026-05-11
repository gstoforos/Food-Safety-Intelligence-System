#!/usr/bin/env python3
"""
tools/unstick_pending_enrichment.py — clear stranded pending_enrichment rows.

USAGE:
    python -m tools.unstick_pending_enrichment [--dry-run] [--no-commit]

REQUIRES: tools.patch_claude_check_pending_enrichment must have been
run first. This script reuses the patched scope-check logic.

Background — audit 2026-05-11
-----------------------------
Two stacked bugs in claude_check.py + url_gate_gemini.py caused
pending_enrichment rows to pile up in Pending indefinitely:

  Bug 1: After claude_check returned "pass" for a pending_enrichment
         row, the Status field was never flipped to plain "pending".
         promote_approved skips any pending_enrichment row (it's in
         NON_PROMOTABLE_STATUSES). Result: silent infinite loop.

  Bug 2: The clean-row shortcut in claude_check.check_row() returned
         "pass" without enforcing Tier-1 pathogen scope. Gemini's
         enricher writes any page-shaped string into Pathogen
         (Asbestos, Electrical shock, Nitrosamines, Mechanical hazard,
         phthalates...) without scope validation. So if Bug 1 were
         fixed alone, ~15 non-food RappelConso recalls (cars,
         toys, electrical appliances) would have promoted to Recalls.

The patch script tools.patch_claude_check_pending_enrichment fixes both
bugs forward. THIS script handles the already-stranded rows that
accumulated before the patch landed.

What it does
------------
For every Pending row where Status=='pending_enrichment':

  A) Check pathogen against FSIS Tier-1 scope (is_tier1_pathogen).
     OUT-OF-SCOPE → archive to Rejected sheet + Weekly_Rejected,
                    remove from Pending. RejectedBy='unstick-2026-05-11'.

  B) IN-SCOPE → flip Status to plain "pending" so the very next
                merge_master run promotes the row to Recalls.

  C) NEITHER (empty Pathogen still) → leave alone. The patched
     claude_check.py will pick these up on the next normal run.

After flipping (B) rows, the script runs merge_master.promote_approved
to push them into Recalls and rebuild the json mirror in one shot, so
the dashboard reflects the fix immediately. Set --no-commit to skip
the git commit step.

Run with --dry-run first to preview the verdicts row-by-row.

Files modified:
  docs/data/recalls.xlsx (Pending, Recalls, Rejected, Weekly_Rejected)
  docs/data/recalls.json (mirror — rebuilt from Recalls sheet)
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.merge_master import (  # noqa: E402
    STATUS_PENDING, STATUS_PENDING_ENRICHMENT, STATUS_REJECTED,
    load_existing, load_pending,
    promote_approved,
    save_xlsx_with_pending, mirror_json_from_xlsx,
    sort_rows,
)
from pipeline._pathogen_scope import is_in_scope as is_tier1_pathogen  # noqa: E402

XLSX_PATH = ROOT / "docs" / "data" / "recalls.xlsx"
JSON_PATH = ROOT / "docs" / "data" / "recalls.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("unstick-pending-enrichment")

TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")
TODAY_TS = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--dry-run", action="store_true",
                   help="Print verdicts without modifying files")
    p.add_argument("--no-commit", action="store_true",
                   help="Skip git commit step (just write files)")
    p.add_argument("--no-promote", action="store_true",
                   help="Flip Status only; don't run promote_approved "
                        "(leaves the actual Recalls insert to the next "
                        "scheduled merge_master run)")
    p.add_argument("--xlsx", default=str(XLSX_PATH))
    p.add_argument("--json", default=str(JSON_PATH))
    return p.parse_args()


def _classify(row: Dict[str, Any]) -> Tuple[str, str]:
    """
    Return (verdict, reason) for one pending_enrichment row.

    verdicts:
      'unlock'     — Pathogen is Tier-1, ready to promote
      'reject'     — Pathogen present but out of Tier-1 scope
      'leave'      — Pathogen still empty / needs more enrichment passes
    """
    pathogen = str(row.get("Pathogen") or "").strip()
    if not pathogen:
        return "leave", "Pathogen still empty — Gemini enrichment incomplete"
    if is_tier1_pathogen(pathogen):
        return "unlock", f"Tier-1 pathogen: {pathogen!r}"
    return "reject", f"pathogen_out_of_scope: {pathogen!r} (not Tier-1)"


def _stamp_note(row: Dict[str, Any], tag: str) -> None:
    notes = (row.get("Notes") or "").strip()
    row["Notes"] = (notes + " " + tag).strip()[:1000]


def main() -> int:
    args = _parse_args()
    xlsx_path = Path(args.xlsx)
    json_path = Path(args.json)
    if not xlsx_path.exists():
        log.error("xlsx not found: %s", xlsx_path)
        return 1

    # Load via merge_master so headers / typing / NEWS sheet survive
    approved = load_existing(xlsx_path)
    pending = load_pending(xlsx_path)
    log.info("Loaded: %d approved, %d pending", len(approved), len(pending))

    targets = [
        (i, r) for i, r in enumerate(pending)
        if (r.get("Status") or "").strip() == STATUS_PENDING_ENRICHMENT
    ]
    log.info("Pending_enrichment rows found: %d", len(targets))
    if not targets:
        log.info("Nothing to do.")
        return 0

    # Classify each
    unlock_idx: List[int] = []
    reject_flags: Dict[int, str] = {}
    leave_idx: List[int] = []

    log.info("─" * 60)
    for idx, row in targets:
        verdict, reason = _classify(row)
        url_tail = str(row.get("URL") or "")[-55:]
        log.info("[%-6s] row=%d  Source=%-18s  %s",
                 verdict, idx,
                 str(row.get("Source") or "")[:18],
                 reason[:90])
        log.info("         URL=...%s", url_tail)
        if verdict == "unlock":
            unlock_idx.append(idx)
        elif verdict == "reject":
            reject_flags[idx] = (
                f"unstick-2026-05-11 ({TODAY_TS}): {reason}"
            )
        else:
            leave_idx.append(idx)
    log.info("─" * 60)
    log.info("Summary: unlock=%d, reject=%d, leave=%d",
             len(unlock_idx), len(reject_flags), len(leave_idx))

    if args.dry_run:
        log.info("--dry-run: no files modified")
        return 0

    # Apply unlocks first (mutate pending rows in place)
    for idx in unlock_idx:
        row = pending[idx]
        row["Status"] = STATUS_PENDING
        _stamp_note(
            row,
            f"[unstick {TODAY}: pending_enrichment → pending "
            f"(scope-verified Tier-1: {str(row.get('Pathogen') or '')[:40]})]",
        )

    if args.no_promote:
        # Just save with the flipped statuses. Don't push to Recalls;
        # don't archive rejects. Operator can call merge_master separately.
        log.info("--no-promote: status flips applied but no promotion "
                 "performed. Run merge_master next.")
        # Still apply rejections? Operator probably wants them out either way.
        # Mark them with Status=rejected so next reviewer evicts them.
        for idx, reason in reject_flags.items():
            row = pending[idx]
            row["Status"] = STATUS_REJECTED
            row["RejectedBy"] = "unstick-2026-05-11"
            _stamp_note(row, f"[unstick {TODAY}: rejected — {reason[:120]}]")
        save_xlsx_with_pending(approved, pending, xlsx_path)
        log.info("Saved xlsx (flips + rejected stamps only): Pending=%d",
                 len(pending))
        return 0

    # Full path: run promote_approved as a SECOND-REVIEWER call.
    # This mirrors what claude_check.main() does at the end of its run:
    #   - unlock_idx rows now have Status='pending' → promoted
    #   - reject_flags rows → archived to Rejected, removed from Pending
    #   - leave_idx rows → stay as-is (still pending_enrichment)
    new_approved, remaining, archived_rejected = promote_approved(
        pending=pending,
        approved_existing=approved,
        rejected_flags=reject_flags,
        archive_immediately=True,
    )
    log.info("promote_approved: +%d to Recalls, %d remain in Pending, "
             "%d archived to Rejected",
             len(new_approved), len(remaining), len(archived_rejected))

    final_approved = sort_rows(approved + new_approved)
    save_xlsx_with_pending(
        final_approved, remaining, xlsx_path,
        newly_rejected_rows=archived_rejected,
    )
    mirror_json_from_xlsx(xlsx_path, json_path)
    log.info("Saved xlsx: Recalls=%d, Pending=%d", len(final_approved), len(remaining))

    # Mirror archived to Weekly_Rejected so the Thursday review sees them.
    if archived_rejected:
        try:
            from pipeline.weekly_rejected_capture import record_rejections
            record_rejections(archived_rejected, xlsx_path=xlsx_path)
            log.info("Weekly_Rejected: appended %d row(s)", len(archived_rejected))
        except Exception as e:
            log.warning("Could not mirror to Weekly_Rejected: %s", e)

    if not args.no_commit:
        try:
            from pipeline.commit_github import git_commit_and_push
            msg = (f"FSIS unstick pending_enrichment {TODAY} "
                   f"(+{len(new_approved)} promoted, "
                   f"{len(archived_rejected)} rejected, "
                   f"{len(leave_idx)} left for next enrichment)")
            files = [str(xlsx_path.relative_to(ROOT)),
                     str(json_path.relative_to(ROOT))]
            git_commit_and_push(ROOT, files, msg)
            log.info("Pushed: %s", msg)
        except Exception as e:
            log.warning("Git commit failed: %s (files still saved locally)", e)

    log.info("Done. The %d unlocked rows are now in Recalls; the dashboard "
             "should reflect them after the next build.", len(new_approved))
    return 0


if __name__ == "__main__":
    sys.exit(main())
