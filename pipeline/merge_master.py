"""
Master merge logic (Pending-sheet architecture).

recalls.xlsx holds THREE sheets:
  - Recalls  : approved, published data (consumed by the weekly report)
  - Pending  : freshly scraped rows awaiting validation + review
  - NEWS     : unrelated news-feed sheet, preserved as-is

Daily pipeline flow:
  1. Scrapers write to Pending (via append_to_pending)
  2. Enrichment + URL validation + AI review run against Pending
  3. promote_approved() moves rows that pass all checks into Recalls
  4. Rejected rows stay in Pending with a rejection reason stored in Notes
     (prefixed "REJECTED: <reason> | <original notes>") so a human can triage.

Dedup:
  - Primary key: URL (lowercased, stripped)
  - Fallback:    date + company + pathogen
  - Dedup applies within Pending and across Pending->Recalls promotion.
"""
from __future__ import annotations
import json
import logging
import re
import unicodedata
from pathlib import Path
from typing import List, Dict, Any, Tuple
from openpyxl import load_workbook, Workbook
from openpyxl.styles import Font, PatternFill

from scrapers._models import Recall

log = logging.getLogger(__name__)

SCHEMA = ["Date", "Source", "Company", "Brand", "Product", "Pathogen", "Reason",
          "Class", "Country", "Region", "Tier", "Outbreak", "URL", "Notes"]

# Pending sheet has the same columns plus two tracking columns.
PENDING_SCHEMA = SCHEMA + ["ScrapedAt", "Status"]

NEWS_HEADERS = ["Published (UTC)", "Pathogen", "Event", "Source", "Title",
                "Link", "Retrieved (UTC)"]

# Status values used in the Pending sheet
STATUS_PENDING = "pending"
STATUS_APPROVED = "approved"   # transient — promoted rows are removed from Pending
STATUS_REJECTED = "rejected"


# ---------------------------------------------------------------------------
# Dedup
# ---------------------------------------------------------------------------
def _dedup_key(row: Dict[str, Any]) -> str:
    """URL primary, fallback to date+company+pathogen."""
    url = (row.get("URL") or "").strip().lower()
    if url:
        return url
    co = unicodedata.normalize("NFD", row.get("Company") or "").encode("ascii", "ignore").decode().lower()
    co = re.sub(r"[^a-z0-9]", "", co)[:30]
    return f"{row.get('Date','')}|{co}|{(row.get('Pathogen','') or '')[:30]}"


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------
def _load_sheet(xlsx_path: Path, sheet: str, schema: List[str]) -> List[Dict[str, Any]]:
    if not xlsx_path.exists():
        return []
    wb = load_workbook(xlsx_path)
    if sheet not in wb.sheetnames:
        return []
    ws = wb[sheet]
    headers = [c.value for c in ws[1]]
    out = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        if all(v in (None, "") for v in row):
            continue
        rec = {h: (v if v is not None else "") for h, v in zip(headers, row)}
        # Backfill any schema cols missing from the sheet (schema evolution safety)
        for col in schema:
            rec.setdefault(col, "" if col not in ("Tier", "Outbreak") else 0)
        out.append(rec)
    return out


def load_existing(xlsx_path: Path) -> List[Dict[str, Any]]:
    """Read approved Recalls sheet -> list of dicts."""
    out = _load_sheet(xlsx_path, "Recalls", SCHEMA)
    log.info("Loaded %d approved rows from Recalls", len(out))
    return out


def load_pending(xlsx_path: Path) -> List[Dict[str, Any]]:
    """Read Pending sheet -> list of dicts. Empty if sheet doesn't exist yet."""
    out = _load_sheet(xlsx_path, "Pending", PENDING_SCHEMA)
    log.info("Loaded %d rows from Pending", len(out))
    return out


# ---------------------------------------------------------------------------
# Merge scraped rows into Pending
# ---------------------------------------------------------------------------
def append_to_pending(
    existing_pending: List[Dict[str, Any]],
    approved: List[Dict[str, Any]],
    new_recalls: List[Recall],
    scraped_at: str,
) -> List[Dict[str, Any]]:
    """
    Take new scraped+enriched Recall objects and append them to the pending list.

    Dedup rules:
      - If the key is already approved in Recalls  -> skip silently
      - If the key is currently in Pending with Status='pending'    -> skip (waiting)
      - If the key is currently in Pending with Status='rejected'   -> DELETE the
        old rejected row and insert the freshly scraped row for re-validation.
        This gives the source a chance to fix broken links / fill missing fields
        before the next run, and prevents rejected rows from being silently
        re-skipped forever.
      - Otherwise (brand new key) -> insert as Status='pending'.
    """
    keys_in_approved = {_dedup_key(r) for r in approved}

    # Index existing pending by key so we can drop rejected duplicates in place.
    # Multiple rows with the same key shouldn't happen, but if they do keep them
    # all (one will match; the others are untouched).
    pending_by_key: Dict[str, List[int]] = {}
    for i, r in enumerate(existing_pending):
        pending_by_key.setdefault(_dedup_key(r), []).append(i)

    # Decide which existing-pending rows to drop (rejected rows being re-scraped).
    indices_to_drop: set = set()
    fresh_rows: List[Dict[str, Any]] = []
    retried = 0
    appended = 0
    already_pending = 0
    already_approved = 0

    for r in new_recalls:
        d = r.to_dict() if isinstance(r, Recall) else dict(r)
        for col in SCHEMA:
            d.setdefault(col, "" if col not in ("Tier", "Outbreak") else 0)
        k = _dedup_key(d)

        if k in keys_in_approved:
            already_approved += 1
            continue

        if k in pending_by_key:
            # Look at the FIRST matching row's status (practically there's only one)
            existing_idx = pending_by_key[k][0]
            existing_status = (existing_pending[existing_idx].get("Status") or "").lower()
            if existing_status == STATUS_REJECTED:
                # Drop the old rejected row, re-queue the fresh scrape
                indices_to_drop.add(existing_idx)
                d["ScrapedAt"] = scraped_at
                d["Status"] = STATUS_PENDING
                fresh_rows.append(d)
                retried += 1
            else:
                # Still pending from a prior run — leave it alone
                already_pending += 1
            continue

        # Brand new key
        d["ScrapedAt"] = scraped_at
        d["Status"] = STATUS_PENDING
        fresh_rows.append(d)
        appended += 1

    # Assemble output: existing pending minus dropped + new/retried
    kept = [r for i, r in enumerate(existing_pending) if i not in indices_to_drop]
    out = kept + fresh_rows

    log.info(
        "Pending: kept %d (dropped %d rejected for retry), +%d new, +%d retried "
        "(skipped: %d already-pending, %d already-approved) = %d total",
        len(kept), len(indices_to_drop), appended, retried,
        already_pending, already_approved, len(out),
    )
    return out


# ---------------------------------------------------------------------------
# Promotion: Pending -> Recalls
# ---------------------------------------------------------------------------
def promote_approved(
    pending: List[Dict[str, Any]],
    approved_existing: List[Dict[str, Any]],
    rejected_flags: Dict[int, str],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Split the Pending list into (new_approved_rows_for_Recalls, rows_to_keep_in_Pending).

    - `rejected_flags` maps pending-row-index -> rejection reason string.
      Rows listed here are marked Status='rejected' and KEPT in Pending
      (with reason written into Notes as 'REJECTED: <reason> | <orig notes>').
    - Rows NOT in rejected_flags (and whose current Status is 'pending') are
      treated as approved and moved to Recalls, deduped against approved_existing.
    - Rows already marked 'rejected' in a prior run stay in Pending untouched.
    """
    approved_keys = {_dedup_key(r) for r in approved_existing}

    new_approved: List[Dict[str, Any]] = []
    kept_in_pending: List[Dict[str, Any]] = []

    for idx, row in enumerate(pending):
        # Strip runtime-only fields (e.g. _url_check) before persisting
        clean = {k: v for k, v in row.items() if not k.startswith("_")}

        # Previously-rejected rows: leave alone, don't re-promote, don't re-reject
        if clean.get("Status") == STATUS_REJECTED and idx not in rejected_flags:
            kept_in_pending.append(clean)
            continue

        if idx in rejected_flags:
            reason = rejected_flags[idx]
            orig_notes = (clean.get("Notes") or "").strip()
            if not orig_notes.startswith("REJECTED:"):
                clean["Notes"] = f"REJECTED: {reason}" + (f" | {orig_notes}" if orig_notes else "")
            clean["Status"] = STATUS_REJECTED
            kept_in_pending.append(clean)
            continue

        # Approved row: dedup against existing Recalls
        k = _dedup_key(clean)
        if k in approved_keys:
            # Already published — drop silently from Pending
            continue
        approved_keys.add(k)

        # Strip pending-only tracking columns before inserting into Recalls
        approved_row = {col: clean.get(col, "" if col not in ("Tier", "Outbreak") else 0)
                        for col in SCHEMA}
        new_approved.append(approved_row)

    rejected_kept = sum(1 for r in kept_in_pending if r.get("Status") == STATUS_REJECTED)
    log.info("Promotion: %d approved -> Recalls, %d kept in Pending (%d rejected)",
             len(new_approved), len(kept_in_pending), rejected_kept)
    return new_approved, kept_in_pending


# ---------------------------------------------------------------------------
# Sort / Save
# ---------------------------------------------------------------------------
def sort_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Sort newest first by Date string (YYYY-MM-DD sorts lexically)."""
    return sorted(rows, key=lambda r: (r.get("Date") or ""), reverse=True)


def _write_sheet(wb: Workbook,
                 sheet_name: str,
                 schema: List[str],
                 rows: List[Dict[str, Any]],
                 header_fill: PatternFill = None) -> None:
    """(Re)create a sheet with given schema + rows."""
    if sheet_name in wb.sheetnames:
        del wb[sheet_name]
    ws = wb.create_sheet(sheet_name)
    for i, h in enumerate(schema, 1):
        c = ws.cell(row=1, column=i, value=h)
        c.font = Font(bold=True)
        if header_fill is not None:
            c.fill = header_fill
    for r_idx, row in enumerate(rows, 2):
        for c_idx, col in enumerate(schema, 1):
            v = row.get(col, "")
            if col in ("Tier", "Outbreak"):
                try:
                    v = int(v) if v not in ("", None) else 0
                except (ValueError, TypeError):
                    v = 0
            ws.cell(row=r_idx, column=c_idx, value=v)
    ws.freeze_panes = "A2"


def save_xlsx_with_pending(
    approved_rows: List[Dict[str, Any]],
    pending_rows: List[Dict[str, Any]],
    xlsx_path: Path,
) -> None:
    """
    Save BOTH sheets (Recalls + Pending), preserving NEWS sheet if present.
    Sheet order: Recalls (0), Pending (1), NEWS (last).
    """
    if xlsx_path.exists():
        wb = load_workbook(xlsx_path)
    else:
        wb = Workbook()
        if wb.active and wb.active.max_row == 1 and wb.active.max_column == 1:
            wb.remove(wb.active)

    # Write Recalls (approved published data)
    _write_sheet(wb, "Recalls", SCHEMA, approved_rows)

    # Write Pending (amber-ish header fill to make the tab visually distinct)
    pending_fill = PatternFill(start_color="FFF3CD", end_color="FFF3CD", fill_type="solid")
    _write_sheet(wb, "Pending", PENDING_SCHEMA, pending_rows, header_fill=pending_fill)

    # Ensure NEWS sheet exists (empty if it wasn't there before)
    if "NEWS" not in wb.sheetnames:
        news = wb.create_sheet("NEWS")
        for i, h in enumerate(NEWS_HEADERS, 1):
            c = news.cell(row=1, column=i, value=h)
            c.font = Font(bold=True)
        news.freeze_panes = "A2"

    # Reorder: Recalls, Pending, (others), NEWS last
    ordered = ["Recalls", "Pending"]
    others = [s for s in wb.sheetnames if s not in ("Recalls", "Pending", "NEWS")]
    ordered += others
    if "NEWS" in wb.sheetnames:
        ordered.append("NEWS")
    wb._sheets = [wb[s] for s in ordered]

    xlsx_path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(xlsx_path)
    log.info("Saved xlsx: Recalls=%d, Pending=%d -> %s",
             len(approved_rows), len(pending_rows), xlsx_path)


def save_json(rows: List[Dict[str, Any]], json_path: Path) -> None:
    """Mirror approved Recalls to recalls.json (weekly-report + dashboard consumer)."""
    json_path.parent.mkdir(parents=True, exist_ok=True)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=1, default=str)
    log.info("Saved %d approved rows to %s", len(rows), json_path)


# ---------------------------------------------------------------------------
# Back-compat shims (kept so legacy call sites don't break)
# ---------------------------------------------------------------------------
def save_xlsx(rows: List[Dict[str, Any]], xlsx_path: Path) -> None:
    """DEPRECATED: single-sheet save. Kept for any legacy caller."""
    log.warning("save_xlsx (single-sheet) is deprecated — use save_xlsx_with_pending")
    existing_pending = load_pending(xlsx_path)
    save_xlsx_with_pending(rows, existing_pending, xlsx_path)


def merge_new(existing: List[Dict[str, Any]], new_recalls: List[Recall]) -> List[Dict[str, Any]]:
    """
    DEPRECATED: direct merge into Recalls (pre-Pending-sheet behavior).
    Kept for any back-compat call; new code should use append_to_pending +
    promote_approved instead.
    """
    existing_keys = {_dedup_key(r) for r in existing}
    merged = list(existing)
    appended = 0
    for r in new_recalls:
        d = r.to_dict() if isinstance(r, Recall) else dict(r)
        for col in SCHEMA:
            d.setdefault(col, "" if col not in ("Tier", "Outbreak") else 0)
        k = _dedup_key(d)
        if k in existing_keys:
            continue
        existing_keys.add(k)
        merged.append(d)
        appended += 1
    log.info("merge_new (legacy): %d existing + %d new = %d total",
             len(existing), appended, len(merged))
    return merged
