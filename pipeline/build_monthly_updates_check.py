"""
AFTS FSIS — Monthly updates check.
====================================

Runs every month on the 8th at 09:00 Athens time (via
.github/workflows/monthly-updates-check.yml). Walks back N closed months
(default 3) and, for each, decides whether the previously-built monthly
HTML report needs to be rebuilt to reflect new data that has arrived
since the original 1st-of-month build:

  1. NO existing HTML       → build fresh           (label="PUBLISHED")
  2. Count differs           → REBUILD (data drift)  (label="UPDATED")
  3. Count matches + UPDATED → skip (genuinely unchanged)
  4. Count matches + PUBLISHED → REBUILD (label flip only)
       — handles the legacy state where a previous rebuild updated the
       count but pre-dated the label-flip code, so the masthead is
       still "PUBLISHED · 1 May 2026" even though the report has been
       silently revised since first publication.

For every month that gets rebuilt, this script also rebuilds the public
marketing one-pager PDF (the lead magnet that hub.html links via
monthly-index.json's `pdf_url`). For the LATEST closed month (the
"anchor"), the rebuild also refreshes:

  - docs/data/monthly-summary-latest.json  (consumed by the Apps Script
    Friday digest mailer for the most-recent monthly issue)

For OLDER months (not the anchor), per-month summary JSONs are written
to /tmp so they don't pollute monthly-summary-latest.json — only the
marketing PDF and monthly-index.json entry are persisted.

UNCONDITIONAL refresh (regardless of any rebuilds):
  - docs/data/monthly-index.json                 (dashboard cards)
  - docs/data/monthly-summary-latest.json        (mailer source) — for
    the anchor month only, using the CURRENT dataset count.

Output manifest:
  - docs/data/monthly-updates-pending.json       (subscriber email source
    for cumulative monthly-update notifications)

Mirrors the design and audit semantics of pipeline/build_weekly_updates_check.py.
"""
from __future__ import annotations

import calendar
import json
import logging
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("build_monthly_updates_check")

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "docs"))

# Imports from the monthly builder live in docs/. Path is set above.
from build_monthly_report_afts import (  # noqa: E402
    _extract_total_from_html_monthly,
    _extract_published_from_html_monthly,
    _extract_label_from_html_monthly,
    update_monthly_index_json,
    write_monthly_summary_json,
    bucket_by_month,
    filter_month,
    month_bounds,
    compute_month_stats,
)
import build_weekly_report_afts as weekly  # noqa: E402

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
LOOKBACK_MONTHS = 3       # check anchor + 2 priors
SITE_URL_BASE = "https://gstoforos.github.io/Food-Safety-Intelligence-System"
DASHBOARD_URL = "https://www.advfood.tech/fsis-recalls"
INDEX_DASHBOARD_URL = "https://www.advfood.tech/#monthly"

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
INDEX_JSON = ROOT / "docs" / "data" / "monthly-index.json"
SUMMARY_LATEST = ROOT / "docs" / "data" / "monthly-summary-latest.json"
MANIFEST = ROOT / "docs" / "data" / "monthly-updates-pending.json"
MARKETING_DIR = ROOT / "docs" / "marketing"

BUILDER_PY = ROOT / "docs" / "build_monthly_report_afts.py"

# ---------------------------------------------------------------------------
# DATE HELPERS
# ---------------------------------------------------------------------------
def _last_day_of_month(year: int, month: int) -> date:
    return date(year, month, calendar.monthrange(year, month)[1])


def _month_minus(d: date, months: int) -> date:
    """Return last day of the month that is `months` months before d's month."""
    y, m = d.year, d.month - months
    while m <= 0:
        m += 12
        y -= 1
    return _last_day_of_month(y, m)


def _anchor_month_end(today: Optional[date] = None) -> date:
    """The most recent CLOSED month's last day.

    The monthly-updates-check is scheduled for the 8th of each month, so
    on May 8th the anchor is April 30th (April closed at end of last
    month). Using the day-1 of "today" minus 1 day reliably gives last
    month's last day regardless of which day of the current month it is.
    """
    today = today or datetime.now(timezone.utc).date()
    first_of_this_month = today.replace(day=1)
    return first_of_this_month - timedelta(days=1)


# ---------------------------------------------------------------------------
# CHILD INVOCATIONS
# ---------------------------------------------------------------------------
def _run_monthly_builder(month_end: date, summary_path: Path) -> Tuple[bool, str]:
    """Subprocess docs/build_monthly_report_afts.py for one specific month.

    The builder auto-detects rebuild vs fresh publish from the existence
    of the output HTML on disk (see _extract_published_from_html_monthly
    in build_monthly_report_afts.py), so this caller doesn't need to
    pass any "rebuild" flag — just the standard --month-end + --xlsx +
    --summary-json args.
    """
    cmd = [
        sys.executable,
        str(BUILDER_PY),
        "--month-end", month_end.isoformat(),
        "--xlsx", str(XLSX),
        "--summary-json", str(summary_path),
        "--site-url", SITE_URL_BASE,
        "--dashboard-url", DASHBOARD_URL,
        "--monthly-index", str(INDEX_JSON),
    ]
    log.info("→ subprocess: %s", " ".join(cmd))
    try:
        out = subprocess.run(
            cmd, check=True, capture_output=True, text=True,
            cwd=ROOT,
        )
        return True, out.stdout
    except subprocess.CalledProcessError as e:
        log.error("Builder failed for %s\nstdout:\n%s\nstderr:\n%s",
                  month_end, e.stdout, e.stderr)
        return False, e.stderr


def _run_marketing_pdf(summary_path: Path) -> bool:
    """Subprocess pipeline.build_monthly_marketing for the given summary.

    The marketing builder writes <month_tag>-marketing.pdf into
    --out-dir. month_tag comes from the summary JSON's "month_tag" field
    (e.g. "2026-M04"), so this works for any month's summary, not just
    the latest.
    """
    cmd = [
        sys.executable, "-m", "pipeline.build_monthly_marketing",
        "--summary", str(summary_path),
        "--out-dir", str(MARKETING_DIR),
    ]
    log.info("→ subprocess: %s", " ".join(cmd))
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True, cwd=ROOT)
        return True
    except subprocess.CalledProcessError as e:
        log.warning("Marketing PDF build failed (non-fatal): %s\n%s",
                    e, e.stderr)
        return False


def _run_set_pdf_urls() -> bool:
    """Subprocess pipeline.set_pdf_urls so monthly-index.json's pdf_url
    fields stay aligned with the marketing PDFs we just regenerated.
    Idempotent — leaves manually-set legacy pdf_urls alone."""
    cmd = [
        sys.executable, "-m", "pipeline.set_pdf_urls",
        "--index", str(INDEX_JSON),
        "--docs-dir", str(ROOT / "docs"),
        "--site-url", SITE_URL_BASE,
    ]
    log.info("→ subprocess: %s", " ".join(cmd))
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True, cwd=ROOT)
        return True
    except subprocess.CalledProcessError as e:
        log.warning("set_pdf_urls failed (non-fatal): %s\n%s", e, e.stderr)
        return False


# ---------------------------------------------------------------------------
# UNCONDITIONAL JSON REFRESH
# ---------------------------------------------------------------------------
# Note on monthly-summary-latest.json:
#   The rebuild loop above already writes a fresh monthly-summary-latest.json
#   whenever the anchor month is rebuilt (data drift or stuck-label flip).
#   The "skip" branch (count matches + label=UPDATED) only fires when the
#   anchor's HTML is in sync with the dataset — and since the HTML and
#   summary JSON are written together by the builder, the summary is
#   guaranteed current too. No standalone refresh needed.
#
#   monthly-index.json IS refreshed unconditionally below: the dashboard
#   cards depend on per-month entries that aren't co-written with the HTML
#   on every run, so they CAN drift independently of HTML and warrant a
#   per-call upsert against the current dataset.
# ---------------------------------------------------------------------------
def _refresh_index_json(all_recalls: List[Dict], months_back: int = 12) -> bool:
    """Update docs/data/monthly-index.json so every entry's total/tier1/
    outbreaks/top_pathogen reflect the CURRENT dataset, not whatever was
    baked in on the original 1st-of-month build.

    Walks back `months_back` months to keep the call cheap; older months
    are stable (regulators don't issue recall amendments years later)
    and don't need recurring refresh.

    Same auto-update-or-append semantics as update_monthly_index_json
    (called per-month here).
    """
    today = datetime.now(timezone.utc).date()
    anchor = _anchor_month_end(today)
    for i in range(months_back):
        me = _month_minus(anchor, i)
        ms, me_full = month_bounds(me.year, me.month)
        month_recalls = filter_month(all_recalls, ms, me_full)
        if not month_recalls:
            continue   # don't create an entry for a month with zero recalls
        prior = _month_minus(me, 1)
        ps, pe = month_bounds(prior.year, prior.month)
        prior_recalls = filter_month(all_recalls, ps, pe)
        stats = compute_month_stats(month_recalls, prior_recalls)
        try:
            update_monthly_index_json(ms, me_full, stats, INDEX_JSON)
        except Exception as e:
            log.warning("update_monthly_index_json failed for %s: %s", me, e)
    log.info("Monthly index JSON refreshed via per-month upsert (last %d months)",
             months_back)
    return True


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main() -> int:
    log.info("Monthly updates check starting")
    today = datetime.now(timezone.utc).date()
    anchor = _anchor_month_end(today)
    log.info("Anchor month-end (most recent closed month): %s", anchor)

    # Load dataset
    all_recalls = weekly.load_recalls(XLSX)
    log.info("Loaded %d recalls from %s", len(all_recalls), XLSX)

    # Walk back LOOKBACK_MONTHS months (anchor + N-1 priors)
    months_to_check: List[date] = [_month_minus(anchor, i) for i in range(LOOKBACK_MONTHS)]
    log.info("Checking %d months back: %s",
             LOOKBACK_MONTHS, [m.isoformat() for m in months_to_check])

    updated: List[Dict[str, Any]] = []
    label_flips: List[str] = []   # for log/manifest separation

    for me in months_to_check:
        ms, me_full = month_bounds(me.year, me.month)
        year_m = f"{me.year}-M{me.month:02d}"
        report_path = ROOT / "docs" / f"{year_m}.html"

        month_recalls = filter_month(all_recalls, ms, me_full)
        dataset_total = len(month_recalls)
        existing_total = _extract_total_from_html_monthly(report_path)
        existing_label = _extract_label_from_html_monthly(report_path)

        # ── Decision branch (mirrors weekly) ────────────────────────────
        # 1. No HTML                       → fresh build (PUBLISHED)
        # 2. Count differs                 → rebuild (UPDATED)
        # 3. Count matches + UPDATED       → skip (genuinely unchanged)
        # 4. Count matches + PUBLISHED     → STUCK label, force rebuild
        # 5. Count matches + NO label      → LEGACY build, force rebuild
        #     (HTML predates label-flip code, has no <strong>...</strong>
        #      marker — treat same as case 4 to migrate to UPDATED label)
        is_fresh = False
        is_label_flip = False
        if existing_total is None:
            log.info("%s: no existing report — building fresh", year_m)
            old_total = 0
            is_fresh = True
        elif existing_total == dataset_total:
            if existing_label == "UPDATED":
                log.info("%s: unchanged — %d recalls (label=UPDATED)",
                         year_m, dataset_total)
                continue
            else:
                # PUBLISHED, or legacy with no label at all — flip to UPDATED.
                src = existing_label or "<legacy: no label>"
                log.info("%s: count matches (%d) but label is %s — flipping",
                         year_m, dataset_total, src)
                old_total = existing_total
                is_label_flip = True
        else:
            log.info("%s: STALE — was %d, now %d (Δ %+d)",
                     year_m, existing_total, dataset_total,
                     dataset_total - existing_total)
            old_total = existing_total

        # ── Rebuild via subprocess ──────────────────────────────────────
        # Anchor month writes to monthly-summary-latest.json (so the
        # mailer sees the anchor's current stats). Older months write to
        # /tmp so we don't clobber the anchor's summary file.
        is_anchor = (me == anchor)
        if is_anchor:
            summary_path = SUMMARY_LATEST
        else:
            summary_path = Path("/tmp") / f"monthly-summary-{year_m}.json"

        ok, out = _run_monthly_builder(me, summary_path)
        if not ok:
            log.error("%s: rebuild failed — skipping marketing PDF", year_m)
            continue

        # Rebuild marketing one-pager PDF for this month
        _run_marketing_pdf(summary_path)

        if is_label_flip:
            # Cosmetic-only — don't surface in the manifest's updated_weeks
            # list; subscribers shouldn't be emailed about a label change.
            label_flips.append(year_m)
            continue

        updated.append({
            "year_m":      year_m,
            "year":        me.year,
            "month_num":   me.month,
            "month_name":  ms.strftime("%B %Y"),
            "month_start": ms.isoformat(),
            "month_end":   me_full.isoformat(),
            "filename":    f"{year_m}.html",
            "report_url":  f"{SITE_URL_BASE}/{year_m}.html",
            "old_total":   int(old_total),
            "new_total":   int(dataset_total),
            "delta":       int(dataset_total - old_total),
            "first_publish": is_fresh,
        })

    # ── Unconditional JSON refresh (audit 2026-05-09) ───────────────────
    # See comment block on _refresh_index_json: the dashboard's monthly
    # tab reads monthly-index.json. Refresh every month-entry's stats from
    # the current dataset, regardless of whether the loop above rebuilt
    # any HTML. This closes the drift gap where a recall promotes into
    # an older month but no HTML rebuild fires.
    _refresh_index_json(all_recalls, months_back=12)

    # Wire pdf_url onto any month entries whose marketing PDF we just
    # rebuilt (or that had been missing one).
    _run_set_pdf_urls()

    # ── Build CTA hint for the Apps Script monthly mailer ───────────────
    # (Mirrors weekly's primary_cta logic.)
    if len(updated) == 1:
        primary_cta = {
            "label": "VIEW REFRESHED REPORT",
            "url":   updated[0]["report_url"],
        }
    elif len(updated) > 1:
        primary_cta = {
            "label": "VIEW MONTHLY DASHBOARD",
            "url":   INDEX_DASHBOARD_URL,
        }
    else:
        primary_cta = None

    # ── Write manifest ──────────────────────────────────────────────────
    manifest = {
        "generated_utc":      datetime.now(timezone.utc).isoformat(),
        "checked_back_months": LOOKBACK_MONTHS,
        "anchor_month_end":   anchor.isoformat(),
        "updated_months":     updated,
        "label_flips":        label_flips,
        "primary_cta":        primary_cta,
    }
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    log.info("Wrote manifest: %d months changed (label-flips: %d, no email "
             "for those) -> %s", len(updated), len(label_flips), MANIFEST)
    return 0


if __name__ == "__main__":
    sys.exit(main())
