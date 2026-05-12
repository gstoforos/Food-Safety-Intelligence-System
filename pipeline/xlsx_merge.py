"""
pipeline/xlsx_merge.py — safe xlsx union-merge for push-retry conflicts.

THE BUG THIS FIXES
==================
news-feed.yml and merge-master.yml both had a "binary-safe push retry" that
copied the loser's stale local xlsx to /tmp, pulled the winner's remote, then
OVERWROTE the freshly-pulled remote with the stale /tmp copy. Result: any
rows the remote-winner had just added (e.g. an operator's manual upload of
recalls.xlsx with +11 new rows) got silently destroyed on the next news
or merge tick.

THE FIX
=======
On push-retry, instead of overwriting, do a row-level union of:
  - Recalls : remote ∪ ours; never shrinks. Remote wins on collision.
  - Pending : remote ∪ ours, minus any row whose dedup_key already
              appears in the merged Recalls.
  - NEWS    : remote ∪ ours by NEWS dedup key.

Identity = the same _dedup_key used by merge_master:
URL primary, fallback to Date+Company+Pathogen.

Used by:
  pipeline/commit_github.py  (when called from url_gate_gemini etc.)
  .github/workflows/news-feed.yml      (via inline Python in retry block)
  .github/workflows/merge-master.yml   (same pattern)
"""
from __future__ import annotations
import logging
import re
import sys
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Tuple

from openpyxl import Workbook, load_workbook

log = logging.getLogger(__name__)


def _dedup_key(row: Dict[str, Any]) -> str:
    """Same logic as pipeline.merge_master._dedup_key."""
    url = (row.get("URL") or "").strip().lower()
    if url:
        return url
    co = unicodedata.normalize("NFD", str(row.get("Company") or "")) \
        .encode("ascii", "ignore").decode().lower()
    co = re.sub(r"[^a-z0-9]", "", co)[:30]
    return f"{row.get('Date','')}|{co}|{(str(row.get('Pathogen','')) or '')[:30]}"


def _news_dedup_key(row: Dict[str, Any]) -> str:
    """Same logic as pipeline.merge_master._news_dedup_key."""
    link = (row.get("Link") or "").strip().lower()
    if link:
        return link
    title = (str(row.get("Title") or "")).strip().lower()[:120]
    return f"{row.get('Published (UTC)','')}|{title}"


def _wr_dedup_key(row: Dict[str, Any]) -> str:
    """Weekly_Review dedup key — URL+Date, matching the rule used by
    pipeline.weekly_review_capture._existing_keys.

    A row is uniquely identified by its (URL, Date) pair within the
    Weekly_Review sheet. If two pushes both wrote the same recall to
    Weekly_Review (different timestamps, identical URL+Date), they
    collapse to one row in the merge — preserving the earlier-written
    one's Week_Added, Reviewed, and audit-trail fields.
    """
    url = (str(row.get("URL", "")) or "").strip().lower()
    d = str(row.get("Date", ""))[:10]
    return f"{url}|{d}" if url else f"||{d}|{row.get('Pathogen','')}"


def _read_sheet(xlsx_path: Path, sheet: str) -> Tuple[List[str], List[Dict[str, Any]]]:
    if not xlsx_path.exists():
        return [], []
    wb = load_workbook(xlsx_path, read_only=True, data_only=True)
    if sheet not in wb.sheetnames:
        wb.close()
        return [], []
    ws = wb[sheet]
    headers = [c.value for c in ws[1] if c.value is not None]
    rows: List[Dict[str, Any]] = []
    for tup in ws.iter_rows(min_row=2, values_only=True):
        if all(v in (None, "") for v in tup):
            continue
        rec = {h: (v if v is not None else "") for h, v in zip(headers, tup)}
        rows.append(rec)
    wb.close()
    return headers, rows


def _write_sheet(ws, headers: List[str], rows: List[Dict[str, Any]]) -> None:
    for c, h in enumerate(headers, start=1):
        ws.cell(1, c, h)
    for r_idx, row in enumerate(rows, start=2):
        for c_idx, h in enumerate(headers, start=1):
            ws.cell(r_idx, c_idx, row.get(h, ""))


def _merge_unique(
    remote_rows: List[Dict[str, Any]],
    ours_rows: List[Dict[str, Any]],
    key_fn,
) -> List[Dict[str, Any]]:
    """Union by key_fn; remote wins on collision."""
    seen: Dict[str, Dict[str, Any]] = {}
    out: List[Dict[str, Any]] = []
    for r in remote_rows:
        k = key_fn(r)
        if k and k not in seen:
            seen[k] = r
            out.append(r)
    for r in ours_rows:
        k = key_fn(r)
        if k and k not in seen:
            seen[k] = r
            out.append(r)
    return out


def merge_xlsx_with_remote(
    remote_path: Path,
    ours_path: Path,
    out_path: Path,
) -> Dict[str, int]:
    """Merge our local xlsx with the just-pulled remote, write to out_path.
    Returns counts dict for logging.
    """
    rec_headers, rec_remote = _read_sheet(remote_path, "Recalls")
    _, rec_ours = _read_sheet(ours_path, "Recalls")
    if not rec_headers:
        rec_headers = (
            ["Date", "Source", "Company", "Brand", "Product", "Pathogen",
             "Reason", "Class", "Country", "Region", "Tier", "Outbreak",
             "URL", "Notes"]
        )
    rec_merged = _merge_unique(rec_remote, rec_ours, _dedup_key)
    rec_keys = {_dedup_key(r) for r in rec_merged}

    pen_headers, pen_remote = _read_sheet(remote_path, "Pending")
    _, pen_ours = _read_sheet(ours_path, "Pending")
    if not pen_headers:
        # Audit 2026-05-06 — added RejectedBy column (Phase A counter
        # support). Without this, push-retry conflicts would silently
        # strip the RejectedBy column, breaking the 2-reviewer-rejection
        # delete logic in merge_master.cleanup_orphan_rejected.
        pen_headers = rec_headers + ["ScrapedAt", "Status", "RejectedBy"]
    pen_merged_raw = _merge_unique(pen_remote, pen_ours, _dedup_key)
    pen_merged = [r for r in pen_merged_raw if _dedup_key(r) not in rec_keys]

    news_headers, news_remote = _read_sheet(remote_path, "NEWS")
    _, news_ours = _read_sheet(ours_path, "NEWS")
    if not news_headers:
        news_headers = (
            ["Published (UTC)", "Pathogen", "Event", "Source",
             "Title", "Link", "Retrieved (UTC)"]
        )
    news_merged = _merge_unique(news_remote, news_ours, _news_dedup_key)

    # ── Weekly_Review (audit 2026-05-06) ──────────────────────────────
    # Pre-2026-05-06 this module silently dropped the Weekly_Review sheet
    # on every push-retry conflict. The xlsx_merge.merge_xlsx_with_remote
    # path created a fresh workbook with only Recalls/Pending/NEWS, so any
    # operator-built or mailer-state Weekly_Review tab was destroyed on
    # the next news-feed or merge-master tick.
    #
    # Now we union both sides by URL+Date dedup (matches the rule that
    # weekly_review_capture.record_promotions uses). Headers come from
    # remote if present, else from ours, else from the Phase B canonical
    # template.
    wr_headers, wr_remote = _read_sheet(remote_path, "Weekly_Review")
    _, wr_ours = _read_sheet(ours_path, "Weekly_Review")
    if not wr_headers:
        _, wr_ours_headers_attempt = wr_ours, None
        # fall back to ours' headers if available
        wr_headers_local, _ = _read_sheet(ours_path, "Weekly_Review")
        if wr_headers_local:
            wr_headers = wr_headers_local
        else:
            # Canonical schema used by pipeline/weekly_review_capture.py
            wr_headers = rec_headers + [
                "DateAdded", "LastUpdated", "LastChecked",
                "Week_Added", "Reviewed",
            ]
    wr_merged = _merge_unique(wr_remote, wr_ours, _wr_dedup_key)

    # ── Weekly_Rejected (audit 2026-05-12) ────────────────────────────
    # Pre-2026-05-12 this module silently dropped the Weekly_Rejected
    # sheet on every push-retry conflict — same bug class as the
    # 2026-05-06 Weekly_Review case, missed because Weekly_Rejected was
    # added later (audit 2026-05-09) and never propagated here.
    # Observed in production: every gap-finder cascade / news-feed run
    # that hit a non-fast-forward and fell back to xlsx_merge ended with
    # a fresh workbook containing only Recalls/Pending/Weekly_Review/NEWS.
    # The 30-row Weekly_Rejected sheet that claude-check had written
    # minutes earlier was gone after the merge, breaking the operator's
    # Thursday review email.
    #
    # Same union logic and dedup_key as Weekly_Review (URL+Date), since
    # passes and rejects share the same identity rule.
    wj_headers, wj_remote = _read_sheet(remote_path, "Weekly_Rejected")
    _, wj_ours = _read_sheet(ours_path, "Weekly_Rejected")
    if not wj_headers:
        wj_headers_local, _ = _read_sheet(ours_path, "Weekly_Rejected")
        if wj_headers_local:
            wj_headers = wj_headers_local
        else:
            # Canonical schema used by pipeline/weekly_rejected_capture.py
            wj_headers = rec_headers + [
                "DateAdded", "LastUpdated", "LastChecked",
                "Week_Added", "RejectedBy", "RejectionReason", "Reviewed",
            ]
    wj_merged = _merge_unique(wj_remote, wj_ours, _wr_dedup_key)

    wb = Workbook()
    rec_ws = wb.active
    rec_ws.title = "Recalls"
    _write_sheet(rec_ws, rec_headers, rec_merged)
    pen_ws = wb.create_sheet("Pending")
    _write_sheet(pen_ws, pen_headers, pen_merged)
    if wr_merged or wr_remote or wr_ours:
        # Only create Weekly_Review if at least one side had data.
        # If both sides had ZERO Weekly_Review rows AND no Weekly_Review
        # sheet existed, don't create an empty one — record_promotions
        # will create it on the next promotion.
        wr_ws = wb.create_sheet("Weekly_Review")
        _write_sheet(wr_ws, wr_headers, wr_merged)
    if wj_merged or wj_remote or wj_ours:
        # Same gating logic as Weekly_Review — only materialize the sheet
        # if at least one side had a row. Empty Weekly_Rejected sheets
        # are recreated by weekly_rejected_capture.record_rejections.
        wj_ws = wb.create_sheet("Weekly_Rejected")
        _write_sheet(wj_ws, wj_headers, wj_merged)
    news_ws = wb.create_sheet("NEWS")
    _write_sheet(news_ws, news_headers, news_merged)
    wb.save(out_path)

    counts = {
        "recalls_remote": len(rec_remote),
        "recalls_ours":   len(rec_ours),
        "recalls_merged": len(rec_merged),
        "pending_remote": len(pen_remote),
        "pending_ours":   len(pen_ours),
        "pending_merged": len(pen_merged),
        "weekly_review_remote": len(wr_remote),
        "weekly_review_ours":   len(wr_ours),
        "weekly_review_merged": len(wr_merged),
        "weekly_rejected_remote": len(wj_remote),
        "weekly_rejected_ours":   len(wj_ours),
        "weekly_rejected_merged": len(wj_merged),
        "news_remote":    len(news_remote),
        "news_ours":      len(news_ours),
        "news_merged":    len(news_merged),
    }
    log.info("xlsx merge: %s", counts)
    return counts


# ---------------------------------------------------------------------------
# CLI for use from inside YAML retry blocks:
#     python -m pipeline.xlsx_merge <remote_path> <ours_path> <out_path>
# ---------------------------------------------------------------------------
def _cli() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    if len(sys.argv) != 4:
        print("usage: python -m pipeline.xlsx_merge <remote> <ours> <out>",
              file=sys.stderr)
        return 2
    counts = merge_xlsx_with_remote(
        Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3]),
    )
    print(f"merged: Recalls={counts['recalls_merged']} "
          f"Pending={counts['pending_merged']} NEWS={counts['news_merged']}")
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
