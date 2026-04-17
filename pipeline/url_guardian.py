"""
URL Guardian — the every-4-hour integrity pass over docs/data/recalls.xlsx.

ARCHITECTURE RULE: The guardian touches ONLY the Pending sheet.
Recalls is the published, approved dataset — NEVER modified here.

What it does, in order:
  1. Load recalls.xlsx (all three sheets: Recalls, Pending, NEWS)
  2. Validate URLs for Pending rows in the last N days (default 14)
  3. Blank URLs on Pending rows that are truly broken (404/410/5xx) or
     generic landing pages.  Keep 403s from known gov domains (bot-blocks).
  4. Ask OpenAI's find_missing_recalls() what recalls may have been missed
     per major agency over the last M days (default 3). Append novel rows
     to Pending with Status='pending' — they go through the normal
     Claude URL gate (07:30 UTC) before promotion to Recalls.
  5. Optionally ask Claude Haiku to spot-check Tier-1 Pending rows for
     pathogen/outbreak mis-classification.
  6. Write back: xlsx only (Recalls unchanged, Pending updated, NEWS kept).
     JSON is NOT written here — run_all.py is the single authority for
     mirroring Recalls → recalls.json.
  7. Return a summary dict for logging.

Usage:
    from pipeline.url_guardian import guardian_run
    summary = guardian_run("docs/data/recalls.xlsx")
    print(summary)

Environment variables:
    OPENAI_API_KEY     — required for gap-finder
    ANTHROPIC_API_KEY  — optional, used for Tier-1 spot checks
    GUARDIAN_SINCE_DAYS     — URL-check window (default 14)
    GUARDIAN_GAP_DAYS       — gap-finder window (default 3)
    GUARDIAN_SKIP_AI        — "true" skips gap-finder and Tier-1 review
"""
from __future__ import annotations
import os
import json
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Any, Tuple

from openpyxl import load_workbook, Workbook
from openpyxl.styles import Font, PatternFill

# Imports from the review package (sibling modules in the user's repo)
try:
    from review.url_validator import validate_all, should_blank_url, is_generic_url
except ImportError:
    from url_validator import validate_all, should_blank_url, is_generic_url  # type: ignore

try:
    from review.openai_client import find_missing_recalls
except ImportError:
    from openai_client import find_missing_recalls  # type: ignore

try:
    from review.claude_client import review_tier1
except ImportError:
    try:
        from claude_client import review_tier1  # type: ignore
    except ImportError:
        review_tier1 = None  # optional

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Agencies the gap-finder should ask about.
GAP_FINDER_TARGETS: List[Tuple[str, str]] = [
    ("United States", "FDA"),
    ("United States", "USDA FSIS"),
    ("Canada",        "CFIA"),
    ("European Union", "RASFF"),
    ("United Kingdom", "FSA"),
    ("France",        "RappelConso"),
    ("Germany",       "BVL"),
    ("Australia",     "FSANZ"),
]

RECALLS_SCHEMA = ["Date", "Source", "Company", "Brand", "Product", "Pathogen",
                  "Reason", "Class", "Country", "Region", "Tier", "Outbreak",
                  "URL", "Notes"]

PENDING_SCHEMA = RECALLS_SCHEMA + ["ScrapedAt", "Status"]

NEWS_HEADERS = ["Published (UTC)", "Pathogen", "Event", "Source", "Title",
                "Link", "Retrieved (UTC)"]


# --- IO helpers -------------------------------------------------------------
def _load_sheet(wb, sheet_name: str, schema: List[str]) -> List[Dict[str, Any]]:
    """Load a single sheet -> list of dicts.  Returns [] if sheet missing."""
    if sheet_name not in wb.sheetnames:
        return []
    ws = wb[sheet_name]
    headers = [c.value for c in ws[1]]
    out = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        if all(v in (None, "") for v in row):
            continue
        rec = {h: (v if v is not None else "") for h, v in zip(headers, row)}
        for col in schema:
            rec.setdefault(col, "" if col not in ("Tier", "Outbreak") else 0)
        out.append(rec)
    return out


def _load_all(xlsx_path: Path):
    """Load all three sheets. Returns (recalls, pending, news_rows, news_headers)."""
    wb = load_workbook(xlsx_path)
    recalls = _load_sheet(wb, "Recalls", RECALLS_SCHEMA)
    pending = _load_sheet(wb, "Pending", PENDING_SCHEMA)
    news_rows = []
    news_headers = NEWS_HEADERS
    if "NEWS" in wb.sheetnames:
        nw = wb["NEWS"]
        news_headers = [c.value for c in nw[1]]
        news_rows = [dict(zip(news_headers, r))
                     for r in nw.iter_rows(min_row=2, values_only=True)]
    return recalls, pending, news_rows, news_headers


def _write_sheet(wb: Workbook, sheet_name: str, schema: List[str],
                 rows: List[Dict[str, Any]], header_fill: PatternFill = None):
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


def _save_xlsx(xlsx_path: Path,
               recalls: List[Dict[str, Any]],
               pending: List[Dict[str, Any]],
               news_rows: List[Dict[str, Any]],
               news_headers: List[str]):
    """Save all three sheets.  Recalls is written UNCHANGED."""
    wb = Workbook()
    if wb.active and wb.active.max_row == 1 and wb.active.max_column == 1:
        wb.remove(wb.active)

    _write_sheet(wb, "Recalls", RECALLS_SCHEMA, recalls)

    pending_fill = PatternFill(start_color="FFF3CD", end_color="FFF3CD",
                               fill_type="solid")
    _write_sheet(wb, "Pending", PENDING_SCHEMA, pending, header_fill=pending_fill)

    if news_rows:
        _write_sheet(wb, "NEWS", news_headers, news_rows)
    else:
        nw = wb.create_sheet("NEWS")
        for i, h in enumerate(NEWS_HEADERS, 1):
            c = nw.cell(row=1, column=i, value=h)
            c.font = Font(bold=True)
        nw.freeze_panes = "A2"

    # Sheet order: Recalls, Pending, NEWS
    ordered = []
    for name in ["Recalls", "Pending"]:
        if name in wb.sheetnames:
            ordered.append(wb[name])
    for s in wb.sheetnames:
        if s not in ("Recalls", "Pending", "NEWS"):
            ordered.append(wb[s])
    if "NEWS" in wb.sheetnames:
        ordered.append(wb["NEWS"])
    wb._sheets = ordered

    wb.save(xlsx_path)


# --- core logic -------------------------------------------------------------
def _in_window(row: Dict[str, Any], cutoff: date) -> bool:
    d = str(row.get("Date") or "")[:10]
    if not d:
        return False
    try:
        return datetime.strptime(d, "%Y-%m-%d").date() >= cutoff
    except ValueError:
        return False


def _url_health_pass(pending: List[Dict[str, Any]], since_days: int) -> Dict[str, int]:
    """Validate URLs on recent PENDING rows. Blank ones that are truly broken."""
    cutoff = date.today() - timedelta(days=since_days)
    targets = [(i, r) for i, r in enumerate(pending) if _in_window(r, cutoff)]
    log.info("URL health: checking %d of %d pending rows (last %d days)",
             len(targets), len(pending), since_days)

    rows_to_check = [r for _, r in targets]
    validated = validate_all(rows_to_check, max_workers=10)

    stats = {"checked": len(validated), "ok": 0, "bot_blocked": 0,
             "blanked_generic": 0, "blanked_404": 0, "blanked_5xx": 0, "kept_403": 0}
    for (idx, _), vrow in zip(targets, validated):
        check = vrow.get("_url_check", {})
        reason = check.get("reason", "")
        if reason == "ok":
            stats["ok"] += 1
            continue
        if reason == "bot_blocked":
            stats["bot_blocked"] += 1
            continue
        if should_blank_url(check):
            original = pending[idx].get("URL", "")
            if reason == "generic":
                stats["blanked_generic"] += 1
            elif 400 <= check.get("status", 0) < 500:
                stats["blanked_404"] += 1
            else:
                stats["blanked_5xx"] += 1
            pending[idx]["URL"] = ""
            notes = str(pending[idx].get("Notes") or "")
            audit = f"[URL-guardian {date.today().isoformat()}: blanked {reason} {original[:60]}]"
            pending[idx]["Notes"] = (notes + " " + audit).strip()[:500]
        else:
            stats["kept_403"] += 1
    return stats


def _signature(r: Dict[str, Any]) -> str:
    """De-dup key: date | company | pathogen | country, lowercased."""
    d = str(r.get("Date") or "")[:10]
    c = (str(r.get("Company") or "")).strip().lower()
    p = (str(r.get("Pathogen") or "")).strip().lower()
    co = (str(r.get("Country") or "")).strip().lower()
    return f"{d}|{c}|{p}|{co}"


def _gap_finder_pass(recalls: List[Dict[str, Any]],
                     pending: List[Dict[str, Any]],
                     gap_days: int) -> Dict[str, int]:
    """Ask OpenAI what recalls we may have missed.

    Append novel rows to PENDING (not Recalls).
    Dedup checks against both Recalls and existing Pending.
    """
    if os.getenv("OPENAI_API_KEY", "").strip() == "":
        log.info("Gap-finder: OPENAI_API_KEY missing, skipping")
        return {"suggested": 0, "added": 0, "dupes_rejected": 0}

    existing_sigs = {_signature(r) for r in recalls}
    existing_sigs.update(_signature(r) for r in pending)

    added = 0
    suggested = 0
    dupes = 0
    today = date.today().isoformat()
    scraped_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for country, agency in GAP_FINDER_TARGETS:
        try:
            candidates = find_missing_recalls(country, agency, since_days=gap_days) or []
        except Exception as e:
            log.warning("Gap-finder %s/%s failed: %s", country, agency, e)
            continue
        suggested += len(candidates)
        for c in candidates:
            url = (c.get("URL") or "").strip()
            if not url or is_generic_url(url):
                continue
            new_row = {
                "Date":      (c.get("Date") or today)[:10],
                "Source":    f"{agency} (gap-finder)",
                "Company":  c.get("Company", ""),
                "Brand":    "",
                "Product":  c.get("Product", ""),
                "Pathogen": c.get("Pathogen", ""),
                "Reason":   c.get("Reason", ""),
                "Class":    "",
                "Country":  country,
                "Region":   "",
                "Tier":     2,
                "Outbreak": 0,
                "URL":      url,
                "Notes":    f"[gap-finder {today} via OpenAI]",
                "ScrapedAt": scraped_at,
                "Status":   "pending",
            }
            sig = _signature(new_row)
            if sig in existing_sigs:
                dupes += 1
                continue
            existing_sigs.add(sig)
            pending.append(new_row)
            added += 1
    log.info("Gap-finder: suggested=%d added=%d dupes_rejected=%d", suggested, added, dupes)
    return {"suggested": suggested, "added": added, "dupes_rejected": dupes}


def _tier1_spot_check(pending: List[Dict[str, Any]], since_days: int) -> Dict[str, int]:
    """Optional Claude Haiku review of recent Tier-1 PENDING rows."""
    if review_tier1 is None:
        return {"flags": 0, "reviewed": 0}
    if os.getenv("ANTHROPIC_API_KEY", "").strip() == "":
        return {"flags": 0, "reviewed": 0}
    cutoff = date.today() - timedelta(days=since_days)
    recent_tier1 = [r for r in pending
                    if int(r.get("Tier") or 2) == 1 and _in_window(r, cutoff)]
    if not recent_tier1:
        return {"flags": 0, "reviewed": 0}
    flags = review_tier1(recent_tier1) or []
    log.info("Claude Tier-1 spot-check: reviewed=%d flags=%d", len(recent_tier1), len(flags))
    for f in flags:
        if f.get("severity") == "high":
            idx = f.get("row_index", -1)
            if 0 <= idx < len(recent_tier1):
                row = recent_tier1[idx]
                note = f" [Claude-flag {date.today().isoformat()}: {f.get('issue','')[:80]}]"
                row["Notes"] = (str(row.get("Notes") or "") + note).strip()[:500]
    return {"flags": len(flags), "reviewed": len(recent_tier1)}


# --- entry point ------------------------------------------------------------
def guardian_run(xlsx_path: str = "docs/data/recalls.xlsx",
                 json_path: str = None,
                 since_days: int = None,
                 gap_days: int = None,
                 skip_ai: bool = False) -> Dict[str, Any]:
    """Main 4-hour guardian pass.  Modifies ONLY the Pending sheet.

    json_path is accepted for CLI back-compat but IGNORED — the guardian
    never writes recalls.json.  The daily pipeline (run_all.py) is the
    single authority for mirroring Recalls → JSON.
    """
    since_days = int(since_days if since_days is not None
                     else os.getenv("GUARDIAN_SINCE_DAYS", "14"))
    gap_days = int(gap_days if gap_days is not None
                   else os.getenv("GUARDIAN_GAP_DAYS", "3"))
    skip_ai = skip_ai or os.getenv("GUARDIAN_SKIP_AI", "false").lower() == "true"

    xp = Path(xlsx_path)
    if not xp.exists():
        log.error("xlsx not found: %s", xp)
        return {"ok": False, "error": "xlsx not found"}

    recalls, pending, news_rows, news_headers = _load_all(xp)
    log.info("Loaded %d recalls (READ-ONLY), %d pending, %d news from %s",
             len(recalls), len(pending), len(news_rows), xp)

    pending_before = len(pending)

    # 1. URL health pass on PENDING only
    try:
        url_stats = _url_health_pass(pending, since_days)
    except Exception as e:
        log.warning("URL health pass failed (non-fatal): %s", e)
        url_stats = {"checked": 0, "error": str(e)}

    # 2. Gap-finder + Tier-1 spot-check on PENDING only
    if skip_ai:
        gap_stats = {"suggested": 0, "added": 0, "dupes_rejected": 0, "skipped": True}
        tier1_stats = {"flags": 0, "reviewed": 0, "skipped": True}
    else:
        try:
            gap_stats = _gap_finder_pass(recalls, pending, gap_days)
        except Exception as e:
            log.warning("Gap-finder pass failed (non-fatal): %s", e)
            gap_stats = {"suggested": 0, "added": 0, "error": str(e)}
        try:
            tier1_stats = _tier1_spot_check(pending, since_days)
        except Exception as e:
            log.warning("Tier-1 spot-check failed (non-fatal): %s", e)
            tier1_stats = {"flags": 0, "reviewed": 0, "error": str(e)}

    # 3. Sort pending newest-first
    pending.sort(key=lambda r: str(r.get("Date") or ""), reverse=True)

    # 4. Write back xlsx — Recalls UNCHANGED, Pending updated, NEWS preserved
    _save_xlsx(xp, recalls, pending, news_rows, news_headers)
    log.info("Saved xlsx: Recalls=%d (unchanged), Pending=%d",
             len(recalls), len(pending))

    summary = {
        "ok": True,
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "recalls_count": len(recalls),
        "pending_before": pending_before,
        "pending_after": len(pending),
        "pending_delta": len(pending) - pending_before,
        "url_health": url_stats,
        "gap_finder": gap_stats,
        "tier1_review": tier1_stats,
    }
    log.info("Guardian summary: %s", json.dumps(summary, indent=2))
    return summary


if __name__ == "__main__":
    import sys
    import traceback
    import argparse
    ap = argparse.ArgumentParser(description="FSIS URL Guardian — 4-hour integrity pass")
    ap.add_argument("--xlsx", default="docs/data/recalls.xlsx")
    ap.add_argument("--json", default="docs/data/recalls.json",
                    help="(ignored — guardian does not write JSON)")
    ap.add_argument("--since-days", type=int, default=None)
    ap.add_argument("--gap-days", type=int, default=None)
    ap.add_argument("--skip-ai", action="store_true")
    args = ap.parse_args()

    log.info("=" * 70)
    log.info("FSIS URL Guardian starting")
    log.info("Python:   %s", sys.version.replace("\n", " "))
    log.info("CWD:      %s", Path.cwd())
    log.info("xlsx arg: %s", args.xlsx)
    xp = Path(args.xlsx)
    log.info("xlsx exists: %s  (size=%s bytes)",
             xp.exists(),
             xp.stat().st_size if xp.exists() else "n/a")
    log.info("OPENAI_API_KEY:    %s", "set" if os.getenv("OPENAI_API_KEY") else "NOT SET")
    log.info("ANTHROPIC_API_KEY: %s", "set" if os.getenv("ANTHROPIC_API_KEY") else "NOT SET")
    log.info("NOTE: recalls.json is NOT written by the guardian")
    log.info("=" * 70)

    if not xp.exists():
        log.error("FATAL: xlsx file not found at %s", xp.absolute())
        log.error("CWD contents: %s", sorted(p.name for p in Path.cwd().iterdir())[:20])
        sys.exit(1)

    try:
        result = guardian_run(args.xlsx, args.json,
                              args.since_days, args.gap_days, args.skip_ai)
        if not isinstance(result, dict) or not result.get("ok"):
            log.error("Guardian run returned failure: %s", result)
            sys.exit(1)
        log.info("Guardian completed successfully.")
        sys.exit(0)
    except Exception as exc:
        log.error("FATAL uncaught exception in guardian_run: %s: %s",
                  type(exc).__name__, exc)
        traceback.print_exc()
        sys.exit(1)
