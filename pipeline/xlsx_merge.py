"""
pipeline/xlsx_merge.py — safe xlsx union-merge for push-retry.

When two workflows both modify docs/data/recalls.xlsx and one hits a
non-fast-forward push, the OLD retry logic in commit_github overwrote
the freshly-pulled remote xlsx with the loser's stale copy — destroying
any rows the winner had just added.

This module provides `merge_xlsx_with_remote()` which produces a union:
  • Recalls : union of remote + ours; never shrinks.
  • Pending : union, minus rows whose dedup_key is in merged Recalls.
  • NEWS    : union by NEWS dedup key.

Identity = the same _dedup_key used by merge_master:
URL primary, fallback to Date+Company+Pathogen.

Used by commit_github.git_commit_and_push retry path.
"""
from __future__ import annotations
import logging
import re
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
    co = unicodedata.normalize("NFD", row.get("Company") or "") \
        .encode("ascii", "ignore").decode().lower()
    co = re.sub(r"[^a-z0-9]", "", co)[:30]
    return f"{row.get('Date','')}|{co}|{(row.get('Pathogen','') or '')[:30]}"


def _news_dedup_key(row: Dict[str, Any]) -> str:
    """Same logic as pipeline.merge_master._news_dedup_key."""
    link = (row.get("Link") or "").strip().lower()
    if link:
        return link
    title = (row.get("Title") or "").strip().lower()[:120]
    return f"{row.get('Published (UTC)','')}|{title}"


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
    seen = {}
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
        pen_headers = rec_headers + ["ScrapedAt", "Status"]
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

    wb = Workbook()
    rec_ws = wb.active
    rec_ws.title = "Recalls"
    _write_sheet(rec_ws, rec_headers, rec_merged)
    pen_ws = wb.create_sheet("Pending")
    _write_sheet(pen_ws, pen_headers, pen_merged)
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
        "news_remote":    len(news_remote),
        "news_ours":      len(news_ours),
        "news_merged":    len(news_merged),
    }
    log.info("xlsx merge: %s", counts)
    return counts
