"""
Master merge logic:
- Load existing recalls.xlsx (your 197 seed rows)
- Apply new scraped rows (after enrichment + review)
- Dedup by URL (primary) + Date+Company+Pathogen (fallback)
- Sort newest first
- Save back to xlsx (Recalls + NEWS sheets preserved)
- Also write recalls.json
"""
from __future__ import annotations
import json
import logging
from pathlib import Path
from typing import List, Dict, Any
from openpyxl import load_workbook
from openpyxl.styles import Font

from scrapers._models import Recall

log = logging.getLogger(__name__)

SCHEMA = ["Date", "Source", "Company", "Brand", "Product", "Pathogen", "Reason",
          "Class", "Country", "Region", "Tier", "Outbreak", "URL", "Notes"]


def load_existing(xlsx_path: Path) -> List[Dict[str, Any]]:
    """Read existing Recalls sheet -> list of dicts."""
    wb = load_workbook(xlsx_path)
    if "Recalls" not in wb.sheetnames:
        return []
    ws = wb["Recalls"]
    headers = [c.value for c in ws[1]]
    out = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        rec = {h: (v if v is not None else "") for h, v in zip(headers, row)}
        out.append(rec)
    log.info("Loaded %d existing rows from %s", len(out), xlsx_path)
    return out


def _dedup_key(row: Dict[str, Any]) -> str:
    """URL primary, fallback to date+company+pathogen."""
    url = (row.get("URL") or "").strip().lower()
    if url:
        return url
    import re, unicodedata
    co = unicodedata.normalize("NFD", row.get("Company") or "").encode("ascii", "ignore").decode().lower()
    co = re.sub(r"[^a-z0-9]", "", co)[:30]
    return f"{row.get('Date','')}|{co}|{(row.get('Pathogen','') or '')[:30]}"


def merge_new(existing: List[Dict[str, Any]], new_recalls: List[Recall]) -> List[Dict[str, Any]]:
    """
    Merge new scraped rows into existing, deduping.
    - Existing rows are TRUSTED (already cleaned manually)
    - New rows are appended only if their dedup key isn't already present
    """
    existing_keys = {_dedup_key(r) for r in existing}
    appended = 0
    merged = list(existing)
    for r in new_recalls:
        d = r.to_dict()
        # Ensure all schema columns present
        for col in SCHEMA:
            d.setdefault(col, "" if col not in ("Tier", "Outbreak") else 0)
        k = _dedup_key(d)
        if k in existing_keys:
            continue
        existing_keys.add(k)
        merged.append(d)
        appended += 1
    log.info("Merged: %d existing + %d new (after dedup) = %d total", len(existing), appended, len(merged))
    return merged


def sort_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Sort newest first by Date string (YYYY-MM-DD sorts lexically)."""
    return sorted(rows, key=lambda r: (r.get("Date") or ""), reverse=True)


def save_xlsx(rows: List[Dict[str, Any]], xlsx_path: Path) -> None:
    """Save merged rows back to xlsx, preserving NEWS sheet."""
    # Load existing to keep NEWS sheet
    if xlsx_path.exists():
        wb = load_workbook(xlsx_path)
        if "Recalls" in wb.sheetnames:
            del wb["Recalls"]
    else:
        from openpyxl import Workbook
        wb = Workbook()
        wb.remove(wb.active)

    ws = wb.create_sheet("Recalls", 0)  # insert as first sheet
    # Write headers
    for i, h in enumerate(SCHEMA, 1):
        c = ws.cell(row=1, column=i, value=h)
        c.font = Font(bold=True)
    # Write data
    for r_idx, row in enumerate(rows, 2):
        for c_idx, col in enumerate(SCHEMA, 1):
            v = row.get(col, "")
            if col in ("Tier", "Outbreak"):
                try:
                    v = int(v) if v not in ("", None) else 0
                except (ValueError, TypeError):
                    v = 0
            ws.cell(row=r_idx, column=c_idx, value=v)
    ws.freeze_panes = "A2"

    # Ensure NEWS sheet exists
    if "NEWS" not in wb.sheetnames:
        news = wb.create_sheet("NEWS")
        news_headers = ["Published (UTC)", "Pathogen", "Event", "Source", "Title", "Link", "Retrieved (UTC)"]
        for i, h in enumerate(news_headers, 1):
            c = news.cell(row=1, column=i, value=h)
            c.font = Font(bold=True)
        news.freeze_panes = "A2"

    xlsx_path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(xlsx_path)
    log.info("Saved %d rows to %s", len(rows), xlsx_path)


def save_json(rows: List[Dict[str, Any]], json_path: Path) -> None:
    """Mirror to recalls.json."""
    json_path.parent.mkdir(parents=True, exist_ok=True)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=1, default=str)
    log.info("Saved %d rows to %s", len(rows), json_path)
