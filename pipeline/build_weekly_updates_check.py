"""pipeline/build_weekly_updates_check.py — Wednesday weekly-updates check.

Rebuilds the last 4 Friday-ending weeks if their underlying recall counts
have changed since first publication, and writes a manifest of which
weeks actually changed for the Apps Script subscriber mailer to read.

WHY THIS EXISTS
===============
Weekly reports publish Friday with whatever recalls have been promoted into
the Recalls sheet at that point. During the following days, Pending rows
continue to be reviewed and promoted by the dual-reviewer gate. By the
next Wednesday, prior weeks may have additional verified recalls that
should be reflected in the historical weekly view.

This script reuses the existing `refresh_stale_weeks` logic from the
Friday weekly builder (`docs/build_weekly_report_afts.py`) — same
"compare baked-in HTML count vs. current dataset count" gate — but
walks back N weeks (default 4) and writes a structured manifest the
mailer can read.

OUTPUT
======
docs/data/weekly-updates-pending.json with shape:

  {
    "generated_utc": "2026-05-13T07:00:00+00:00",
    "checked_back_weeks": 4,
    "anchor_friday": "2026-05-08",
    "updated_weeks": [
      {
        "year": 2026,
        "week_num": 17,
        "filename": "2026-W17.html",
        "report_url": "https://gstoforos.github.io/.../2026-W17.html",
        "week_start": "2026-04-18",
        "week_end":   "2026-04-24",
        "old_total":  12,
        "new_total":  15,
        "delta":      3
      }
    ]
  }

The manifest is ALWAYS written (even with empty updated_weeks) so the
Apps Script mailer can distinguish "checked, nothing changed" from
"never checked, manifest missing".

USAGE
=====
    python -m pipeline.build_weekly_updates_check

EXIT CODES
==========
    0 = success (manifest written, with or without updates)
    1 = error (xlsx missing, builder import failed, etc.)
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
# build_weekly_report_afts.py lives in docs/ — add to path so we can import it
sys.path.insert(0, str(ROOT / "docs"))

from build_weekly_report_afts import (  # noqa: E402
    load_recalls,
    filter_week,
    build_html,
    compute_stats,
    _extract_total_from_html,
    _extract_published_from_html,
    _extract_label_from_html,
    update_dashboard_data,
    write_weekly_summary_json,
)

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
MANIFEST = ROOT / "docs" / "data" / "weekly-updates-pending.json"
REPORT_URL_BASE = "https://gstoforos.github.io/Food-Safety-Intelligence-System"
LOOKBACK_WEEKS = 4

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("weekly-updates-check")


def _last_friday(today: "date | None" = None):
    """Return the most recent Friday strictly before today.

    On Wednesday: returns the Friday 5 days ago (last week's published Friday).
    On Friday before this Wednesday's run: still returns the prior Friday.
    """
    if today is None:
        today = datetime.now(timezone.utc).date()
    # Monday=0, ..., Friday=4, ..., Sunday=6
    days_back = (today.weekday() - 4) % 7
    if days_back == 0:
        # today IS Friday — we want the previous Friday's report, not today's
        days_back = 7
    return today - timedelta(days=days_back)


def main() -> int:
    log.info("Wednesday weekly-updates check starting")

    if not XLSX.exists():
        log.error("XLSX not found: %s", XLSX)
        return 1

    all_recalls = load_recalls(XLSX)
    log.info("Loaded %d recalls from %s", len(all_recalls), XLSX)

    anchor_friday = _last_friday()
    log.info("Anchor Friday (most recent published week): %s", anchor_friday)

    updated: list[dict] = []

    # Walk back from the anchor Friday (offset 0 = anchor itself).
    # The anchor week was just built on Friday — but it can also pick up
    # late promotions over the weekend, so we include it in the check.
    for offset in range(0, LOOKBACK_WEEKS):
        week_end = anchor_friday - timedelta(days=7 * offset)
        wnum = week_end.isocalendar()[1]
        year = week_end.year
        week_start = week_end - timedelta(days=6)
        filename = f"{year}-W{wnum:02d}.html"
        report_path = ROOT / "docs" / filename

        dataset_recalls = filter_week(all_recalls, week_end)
        dataset_total = len(dataset_recalls)
        existing_total = _extract_total_from_html(report_path)
        existing_label = _extract_label_from_html(report_path)

        # ── Decision branch (audit 2026-05-09) ──────────────────────────
        # 1. No existing HTML            → fresh build (label="PUBLISHED")
        # 2. Count differs               → rebuild         (label="UPDATED")
        # 3. Count matches + UPDATED     → genuinely unchanged, skip
        # 4. Count matches + PUBLISHED   → STUCK LABEL: rebuild once to
        #    flip "PUBLISHED" → "UPDATED". This handles the legacy state
        #    where a previous rebuild updated the count from N to N+k but
        #    pre-dated this label-flip code, so the label was overwritten
        #    with "PUBLISHED" rather than transitioning to "UPDATED".
        #    After this one-shot fix, label="UPDATED" and the unchanged
        #    branch (case 3) will skip on subsequent runs.
        label_flip_only = False
        if existing_total is None:
            log.info("W%02d: no existing report at %s — building fresh",
                     wnum, report_path)
            old_total = 0
        elif existing_total == dataset_total:
            if existing_label == "PUBLISHED":
                log.info("W%02d (%s): count matches (%d) but label is "
                         "PUBLISHED — flipping to UPDATED",
                         wnum, week_end, dataset_total)
                old_total = existing_total
                label_flip_only = True
            else:
                log.info("W%02d (%s): unchanged — %d recalls (label=%s)",
                         wnum, week_end, dataset_total,
                         existing_label or "—")
                continue
        else:
            log.info("W%02d (%s): STALE — was %d, now %d (Δ %+d)",
                     wnum, week_end, existing_total, dataset_total,
                     dataset_total - existing_total)
            old_total = existing_total

        # Rebuild: same call signature as build_weekly_report_afts.refresh_stale_weeks
        prev_prev_end = week_end - timedelta(days=7)
        prev_week_recalls = filter_week(all_recalls, prev_prev_end)
        # Preserve original publish date so the rebuilt HTML stamps
        # "(updated <today>)" rather than silently overwriting the
        # PUBLISHED line. None on fresh builds (existing_total was None).
        orig_pub = _extract_published_from_html(report_path) if existing_total is not None else None
        html, stats = build_html(week_end, dataset_recalls, prev_week_recalls,
                                 original_published=orig_pub)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(html, encoding="utf-8")
        log.info("W%02d rebuilt -> %s (%d recalls, original_pub=%r)",
                 wnum, report_path, dataset_total, orig_pub)

        # Refresh per-week summary JSON used by dashboard / mailer.
        # update_dashboard_data() handles the all-weeks index — called
        # once after the loop. write_weekly_summary_json is per-week.
        try:
            write_weekly_summary_json(
                week_end, dataset_recalls, stats,
                ROOT / "docs" / "data",
            )
        except Exception as e:
            log.warning("write_weekly_summary_json failed for W%02d: %s",
                        wnum, e)

        # Skip manifest append on label-flip-only rebuilds — count didn't
        # change, so the Wednesday subscriber email shouldn't list this
        # week as "revised". The HTML, dashboard JSON, and summary JSON
        # all get the new UPDATED label, but no email goes out about it.
        if label_flip_only:
            log.info("W%02d: label flip applied (no manifest entry)", wnum)
            continue

        updated.append({
            "year": year,
            "week_num": wnum,
            "filename": filename,
            "report_url": f"{REPORT_URL_BASE}/{filename}",
            "week_start": week_start.isoformat(),
            "week_end": week_end.isoformat(),
            "old_total": int(old_total),
            "new_total": int(dataset_total),
            "delta": int(dataset_total - old_total),
        })

    # ── Unconditional JSON refresh (audit 2026-05-09, hardened twice) ───
    # The dashboard reads docs/data/weekly-index.json; the Friday weekly
    # mailer reads docs/data/weekly-summary-latest.json. BOTH can drift
    # out of sync with the dataset between Friday-builds — e.g. a row
    # promotes to Recalls over the weekend, the prior week's count
    # changes, but if no HTML rebuild fires (because someone manually
    # ran build_html earlier and the HTML count already matches), the
    # JSONs stay frozen at last Friday's snapshot.
    #
    # Solution: regenerate BOTH files on EVERY Wednesday run, regardless
    # of whether HTML reports needed rebuilding. Costs one xlsx scan +
    # two small JSON writes — cheap, idempotent, removes the drift gap.
    #
    # weekly-index.json     → all weeks, totals/tier1/outbreaks per week.
    # weekly-summary-latest → anchor (most-recent Friday) week's full
    #                         summary with leading_pathogen + top_threats,
    #                         shape consumed by Friday Apps Script mailer.
    try:
        most_recent_we = (
            max(datetime.strptime(w["week_end"], "%Y-%m-%d").date()
                for w in updated)
            if updated else anchor_friday
        )
        update_dashboard_data(most_recent_we, stats={}, all_recalls=all_recalls)
        log.info("Dashboard JSON refreshed: docs/data/weekly-index.json")
    except Exception as e:
        log.warning("update_dashboard_data failed (non-fatal): %s", e)

    try:
        # Anchor-week summary — what the Friday weekly mailer reads.
        # Always emit for anchor_friday (most recent Friday-ending week)
        # using the CURRENT dataset, so the mailer's Friday digest sees
        # late-promoted recalls even when the W##.html hasn't itself
        # been rebuilt this run.
        anchor_recalls = filter_week(all_recalls, anchor_friday)
        prev_anchor_recalls = filter_week(
            all_recalls, anchor_friday - timedelta(days=7)
        )
        anchor_stats = compute_stats(anchor_recalls, prev_anchor_recalls)
        write_weekly_summary_json(
            anchor_friday, anchor_recalls, anchor_stats,
            ROOT / "docs" / "data",
        )
        log.info("Summary JSON refreshed: docs/data/weekly-summary-latest.json "
                 "(anchor=%s, total=%d)",
                 anchor_friday, len(anchor_recalls))
    except Exception as e:
        log.warning("weekly-summary-latest.json refresh failed (non-fatal): %s", e)

    # Sort oldest week first for natural reading order in the email
    updated.sort(key=lambda w: w["week_end"])

    # Apps Script mailer hint (audit 2026-05-09):
    # The Wednesday email's primary CTA button (currently labeled
    # "VISIT THE DASHBOARD →") should link to the rebuilt weekly report
    # when exactly ONE week was updated, NOT to the index dashboard.
    # When multiple weeks updated, fall back to the index dashboard with
    # a hash anchor pointing at the WEEKLY tab. Empty manifest → no email
    # so this field is unused.
    INDEX_DASHBOARD_URL = "https://www.advfood.tech/#weekly"
    if len(updated) == 1:
        primary_cta = {
            "label": "VIEW REFRESHED REPORT",
            "url": updated[0]["report_url"],
        }
    elif len(updated) > 1:
        primary_cta = {
            "label": "VIEW WEEKLY DASHBOARD",
            "url": INDEX_DASHBOARD_URL,
        }
    else:
        primary_cta = None

    manifest = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "checked_back_weeks": LOOKBACK_WEEKS,
        "anchor_friday": anchor_friday.isoformat(),
        "updated_weeks": updated,
        "primary_cta": primary_cta,
    }
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    if updated:
        total_delta = sum(w["delta"] for w in updated)
        log.info("Wrote manifest: %d week(s) changed, +%d total recalls -> %s",
                 len(updated), total_delta, MANIFEST)
    else:
        log.info("Wrote manifest: 0 weeks changed (no email will be sent) -> %s",
                 MANIFEST)

    return 0


if __name__ == "__main__":
    sys.exit(main())
