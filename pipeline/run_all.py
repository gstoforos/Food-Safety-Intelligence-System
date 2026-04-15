"""
Master orchestrator — runs the full daily pipeline.

Workflow:
  1. Discover and instantiate all scraper classes
  2. Run them in parallel batches (rate-limit friendly)
  3. Enrich raw rows with Gemini (free tier)
  4. Merge into existing recalls.xlsx (deduped against your 197 seed)
  5. Validate URLs (HEAD check)
  6. Review with OpenAI (paid, full pass) + Claude (free tier, Tier-1 only)
  7. Apply review fixes
  8. Save recalls.xlsx + recalls.json
  9. Git commit + push

Triggered by GitHub Actions cron at 5 PM UTC daily.
"""
from __future__ import annotations
import os
import sys
import json
import logging
import importlib
import pkgutil
import inspect
from pathlib import Path
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

# Make repo root importable
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scrapers._base import BaseScraper, make_session
from scrapers._models import Recall
from enrichment.enrich_rows import enrich_recalls
from review.url_validator import validate_all
from review.openai_client import review_batch as openai_review
from review.claude_client import review_tier1 as claude_review
from pipeline.merge_master import load_existing, merge_new, sort_rows, save_xlsx, save_json
from pipeline.commit_github import git_commit_and_push

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("orchestrator")

DATA_DIR = ROOT / "data"
XLSX_PATH = DATA_DIR / "recalls.xlsx"
JSON_PATH = DATA_DIR / "recalls.json"
SINCE_DAYS = int(os.getenv("SINCE_DAYS", "30"))
MAX_PARALLEL = int(os.getenv("MAX_PARALLEL", "8"))
SKIP_AI = os.getenv("SKIP_AI", "").lower() in ("1", "true", "yes")
SKIP_REVIEW = os.getenv("SKIP_REVIEW", "").lower() in ("1", "true", "yes")
SKIP_COMMIT = os.getenv("SKIP_COMMIT", "").lower() in ("1", "true", "yes")


def discover_scrapers() -> List[BaseScraper]:
    """Auto-discover all BaseScraper subclasses in scrapers/* packages."""
    found: List[BaseScraper] = []
    import scrapers
    pkgs = ["north_america", "europe_eu", "europe_non_eu", "eu_wide",
            "asia", "oceania", "africa", "latam", "middle_east"]
    for region in pkgs:
        try:
            pkg = importlib.import_module(f"scrapers.{region}")
        except ImportError as e:
            log.warning("Failed to import scrapers.%s: %s", region, e)
            continue
        for _, modname, _ in pkgutil.iter_modules(pkg.__path__):
            try:
                mod = importlib.import_module(f"scrapers.{region}.{modname}")
            except Exception as e:
                log.warning("Failed to import %s.%s: %s", region, modname, e)
                continue
            for _, cls in inspect.getmembers(mod, inspect.isclass):
                if (issubclass(cls, BaseScraper) and cls is not BaseScraper
                        and cls.__module__ == mod.__name__ and cls.AGENCY):
                    found.append(cls())
    log.info("Discovered %d scrapers", len(found))
    return found


def run_one_scraper(scraper: BaseScraper, since_days: int) -> List[Recall]:
    name = f"{scraper.AGENCY}/{scraper.COUNTRY}"
    try:
        log.info("[START] %s", name)
        rows = scraper.scrape(since_days=since_days)
        log.info("[DONE]  %s -> %d recalls", name, len(rows))
        return rows
    except Exception as e:
        log.error("[FAIL]  %s: %s", name, e)
        return []


def run_all_scrapers(scrapers: List[BaseScraper], since_days: int) -> List[Recall]:
    """Run all scrapers in parallel batches."""
    all_rows: List[Recall] = []
    with ThreadPoolExecutor(max_workers=MAX_PARALLEL) as ex:
        futs = {ex.submit(run_one_scraper, s, since_days): s for s in scrapers}
        for f in as_completed(futs):
            all_rows.extend(f.result())
    log.info("Total scraped (raw): %d", len(all_rows))
    return all_rows


def apply_reviews(rows: List[Dict[str, Any]],
                  openai_issues: List[Dict[str, Any]],
                  claude_flags: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Apply suggested fixes from OpenAI + Claude."""
    out = [dict(r) for r in rows]
    fixed = 0
    for issue in openai_issues:
        idx = issue.get("row_index")
        if idx is None or idx >= len(out):
            continue
        fixes = issue.get("suggested_fixes", {}) or {}
        for k, v in fixes.items():
            if k in out[idx] and v:
                out[idx][k] = v
                fixed += 1
    for flag in claude_flags:
        idx = flag.get("row_index")
        if idx is None or idx >= len(out):
            continue
        fixes = flag.get("suggested_fix", {}) or {}
        for k, v in fixes.items():
            if k in out[idx] and v:
                out[idx][k] = v
                fixed += 1
    log.info("Applied %d field fixes from review", fixed)
    return out


def main() -> int:
    t0 = datetime.now(timezone.utc)
    log.info("=" * 60)
    log.info("FSIS daily run started: %s UTC", t0.isoformat())
    log.info("Config: since_days=%d, parallel=%d, ai=%s, review=%s, commit=%s",
             SINCE_DAYS, MAX_PARALLEL,
             not SKIP_AI, not SKIP_REVIEW, not SKIP_COMMIT)

    # 1) Load existing seed (your 197 records)
    existing = load_existing(XLSX_PATH) if XLSX_PATH.exists() else []
    log.info("Seed: %d existing rows", len(existing))

    # 2) Discover and run all scrapers
    scrapers = discover_scrapers()
    raw_recalls = run_all_scrapers(scrapers, since_days=SINCE_DAYS)

    if not raw_recalls:
        log.warning("No recalls scraped this run")

    # 3) Enrich with Gemini (free tier, deterministic + AI fill-in)
    enriched = enrich_recalls(raw_recalls, use_ai=not SKIP_AI)

    # 4) Merge into existing (dedup against 197 seed)
    merged = merge_new(existing, enriched)
    merged = sort_rows(merged)

    # 5) Review pipeline (URL validation + AI review)
    if not SKIP_REVIEW:
        log.info("URL validation pass...")
        validated = validate_all(merged, max_workers=10)
        # Strip _url_check from rows that pass (only flag bad ones for review)
        bad_url_rows = [r for r in validated if not r.get("_url_check", {}).get("ok")]
        log.info("URL validation: %d failed", len(bad_url_rows))

        # Only review NEW rows (rows not in original 197) to save API budget
        existing_keys = {(r.get("URL") or "").lower().strip() for r in existing}
        new_rows = [r for r in merged if (r.get("URL") or "").lower().strip() not in existing_keys]
        log.info("Review pass on %d new rows", len(new_rows))

        openai_issues = openai_review(new_rows) if new_rows else []
        claude_flags = claude_review(new_rows) if new_rows else []

        # Apply fixes back to merged
        # Build index map: dedup_key -> position in merged
        idx_map = {(r.get("URL") or "").lower().strip(): i for i, r in enumerate(merged)}
        # Translate OpenAI/Claude row_index (which refers to new_rows) -> merged index
        for issue in openai_issues:
            ni = issue.get("row_index", -1)
            if 0 <= ni < len(new_rows):
                key = (new_rows[ni].get("URL") or "").lower().strip()
                if key in idx_map:
                    issue["row_index"] = idx_map[key]
        for flag in claude_flags:
            ni = flag.get("row_index", -1)
            if 0 <= ni < len(new_rows):
                key = (new_rows[ni].get("URL") or "").lower().strip()
                if key in idx_map:
                    flag["row_index"] = idx_map[key]

        merged = apply_reviews(merged, openai_issues, claude_flags)
        merged = sort_rows(merged)

    # 6) Save
    save_xlsx(merged, XLSX_PATH)
    save_json(merged, JSON_PATH)

    # 7) Commit
    if not SKIP_COMMIT:
        ok = git_commit_and_push(
            repo_dir=ROOT,
            files=["data/recalls.xlsx", "data/recalls.json"],
            message=f"FSIS daily update {t0.strftime('%Y-%m-%d')} ({len(merged)} records)",
        )
        if not ok:
            log.error("Git push failed")
            return 1

    elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
    log.info("=" * 60)
    log.info("DONE in %.1fs | total records: %d | new this run: %d",
             elapsed, len(merged), len(merged) - len(existing))
    return 0


if __name__ == "__main__":
    sys.exit(main())
