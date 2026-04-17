"""
Master orchestrator — runs the full daily pipeline with Pending-sheet architecture.

Triggered by GitHub Actions cron at 17:00 UTC daily.

Flow:
  1. Discover and instantiate all scraper classes
  2. Run them in parallel batches (rate-limit friendly)
     - Each GenericGeminiScraper: Gemini first, Claude Haiku fallback on 0-row pages
  3. Enrich raw rows with Gemini (pathogen / country / class / tier normalization)
  4. Append enriched rows to the Pending sheet (deduped vs Pending + Recalls)
  5. URL validation (HEAD/GET) against Pending rows only
  6. AI review of newly-scraped pending rows: OpenAI (full pass) + Claude (Tier-1 only)
  7. Collect rejection reasons from validation + review
  8. Promote approved rows Pending -> Recalls; keep rejected in Pending with reason
  9. Save docs/data/recalls.xlsx (Recalls + Pending + NEWS) and docs/data/recalls.json
 10. Git commit + push

Approval policy (what counts as a rejection):
  - URL is generic / landing-page / 404 / 410 / 5xx  ->  rejected
    (bot-blocked 403 on known gov domains is NOT a rejection — likely valid)
  - Missing required field (Date, Company, Product, Pathogen, URL)  ->  rejected
  - OpenAI reviewer flags with high-severity issue codes              ->  rejected
  Otherwise the row is approved and promoted to Recalls.

Environment flags:
  SKIP_AI       : skip Gemini enrichment (keep deterministic normalization only)
  SKIP_REVIEW   : skip URL validation + OpenAI/Claude review (auto-approve everything)
  SKIP_COMMIT   : don't git push (useful for local dry-runs)
  SINCE_DAYS    : how many days back to scrape (default 30)
  MAX_PARALLEL  : thread pool size for scrapers (default 8)
"""
from __future__ import annotations
import os
import sys
import logging
import importlib
import pkgutil
import inspect
from pathlib import Path
from typing import List, Dict, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

# Make repo root importable
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scrapers._base import BaseScraper, make_session  # noqa: E402
from scrapers._models import Recall                    # noqa: E402
from enrichment.enrich_rows import enrich_recalls      # noqa: E402
from review.url_validator import validate_all, should_blank_url  # noqa: E402
from review.openai_client import review_batch as openai_review   # noqa: E402
from review.claude_client import review_tier1 as claude_review   # noqa: E402
from pipeline.merge_master import (                    # noqa: E402
    load_existing, load_pending,
    append_to_pending, promote_approved,
    sort_rows, save_xlsx_with_pending, save_json,
    STATUS_PENDING, STATUS_REJECTED,
)
from pipeline.commit_github import git_commit_and_push  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("orchestrator")

# ---------------------------------------------------------------------------
# Paths — everything lives in docs/data/ now (root /data/ is gone).
# ---------------------------------------------------------------------------
DATA_DIR = ROOT / "docs" / "data"
XLSX_PATH = DATA_DIR / "recalls.xlsx"
JSON_PATH = DATA_DIR / "recalls.json"

SINCE_DAYS = int(os.getenv("SINCE_DAYS", "30"))
MAX_PARALLEL = int(os.getenv("MAX_PARALLEL", "8"))
SKIP_AI = os.getenv("SKIP_AI", "").lower() in ("1", "true", "yes")
SKIP_REVIEW = os.getenv("SKIP_REVIEW", "").lower() in ("1", "true", "yes")
SKIP_COMMIT = os.getenv("SKIP_COMMIT", "").lower() in ("1", "true", "yes")

# High-severity review codes that cause promotion to be withheld.
REJECTION_CODES = {"URL_INVALID", "URL_MISMATCH", "MISSING_FIELD"}

# Required fields for a row to be promotable at all.
REQUIRED_FIELDS = ("Date", "Company", "Product", "Pathogen", "URL")


# ---------------------------------------------------------------------------
# Scraper discovery
# ---------------------------------------------------------------------------
def discover_scrapers() -> List[BaseScraper]:
    """Auto-discover all BaseScraper subclasses in scrapers/* packages."""
    found: List[BaseScraper] = []
    import scrapers  # noqa: F401
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


# ---------------------------------------------------------------------------
# Validation + review helpers
# ---------------------------------------------------------------------------
def _missing_required(row: Dict[str, Any]) -> List[str]:
    """Return list of missing required-field names."""
    missing = []
    for f in REQUIRED_FIELDS:
        v = row.get(f)
        if v is None or (isinstance(v, str) and not v.strip()) or v == "—":
            missing.append(f)
    return missing


def apply_reviewer_fixes(rows: List[Dict[str, Any]],
                         openai_issues: List[Dict[str, Any]],
                         claude_flags: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Apply non-destructive suggested_fixes from reviewers back onto rows.
    Does NOT reject — rejection is decided separately in compute_rejections().
    row_index in issues/flags is assumed to be aligned to `rows` (caller's job).
    """
    out = [dict(r) for r in rows]
    fixed = 0
    for issue in openai_issues:
        idx = issue.get("row_index")
        if idx is None or idx >= len(out) or idx < 0:
            continue
        fixes = issue.get("suggested_fixes", {}) or {}
        for k, v in fixes.items():
            if k in out[idx] and v:
                out[idx][k] = v
                fixed += 1
    for flag in claude_flags:
        idx = flag.get("row_index")
        if idx is None or idx >= len(out) or idx < 0:
            continue
        fixes = flag.get("suggested_fix", {}) or {}
        for k, v in fixes.items():
            if k in out[idx] and v:
                out[idx][k] = v
                fixed += 1
    log.info("Applied %d field fixes from review", fixed)
    return out


def compute_rejections(
    pending: List[Dict[str, Any]],
    new_indices: List[int],
    openai_issues: List[Dict[str, Any]],
) -> Dict[int, str]:
    """
    Decide which pending rows should be rejected.
    Returns {pending_row_index: reason_string}.

    Only NEW rows (those just scraped this run) are evaluated; previously-pending
    or already-rejected rows are left alone.
    """
    rejections: Dict[int, str] = {}

    new_index_set = set(new_indices)

    # 1) URL-validation failures (check._url_check was written in-place earlier)
    for idx in new_indices:
        row = pending[idx]
        check = row.get("_url_check") or {}
        if should_blank_url(check):
            reason = check.get("error") or check.get("reason") or "bad URL"
            rejections[idx] = f"URL check: {reason}"

    # 2) Missing required fields
    for idx in new_indices:
        if idx in rejections:
            continue
        missing = _missing_required(pending[idx])
        if missing:
            rejections[idx] = f"Missing required: {', '.join(missing)}"

    # 3) OpenAI high-severity issues.
    #    openai_issues row_index is relative to new_indices ordering; translate it.
    for issue in openai_issues:
        rel_idx = issue.get("row_index")
        if rel_idx is None or rel_idx < 0 or rel_idx >= len(new_indices):
            continue
        pending_idx = new_indices[rel_idx]
        if pending_idx in rejections:
            continue
        codes = set(issue.get("issues") or [])
        hit = codes & REJECTION_CODES
        if hit:
            rejections[pending_idx] = f"Reviewer: {', '.join(sorted(hit))}"

    log.info("Rejections computed: %d / %d new rows",
             len(rejections), len(new_index_set))
    return rejections


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    t0 = datetime.now(timezone.utc)
    scraped_at = t0.strftime("%Y-%m-%dT%H:%M:%SZ")
    log.info("=" * 60)
    log.info("FSIS daily run started: %s", scraped_at)
    log.info("Config: since_days=%d, parallel=%d, ai=%s, review=%s, commit=%s",
             SINCE_DAYS, MAX_PARALLEL,
             not SKIP_AI, not SKIP_REVIEW, not SKIP_COMMIT)
    log.info("Data dir: %s", DATA_DIR)

    # ---- 1. Load state: approved (Recalls) + existing Pending --------------
    approved = load_existing(XLSX_PATH) if XLSX_PATH.exists() else []
    existing_pending = load_pending(XLSX_PATH) if XLSX_PATH.exists() else []
    log.info("State: %d approved + %d existing pending", len(approved), len(existing_pending))

    # ---- 2. Discover + run scrapers ---------------------------------------
    scrapers = discover_scrapers()
    raw_recalls = run_all_scrapers(scrapers, since_days=SINCE_DAYS)
    if not raw_recalls:
        log.warning("No recalls scraped this run")

    # ---- 3. Enrich raw recalls --------------------------------------------
    enriched = enrich_recalls(raw_recalls, use_ai=not SKIP_AI)

    # ---- 4. Append to Pending (dedup vs Pending + Recalls) ----------------
    pending_before = existing_pending
    pending = append_to_pending(
        existing_pending=pending_before,
        approved=approved,
        new_recalls=enriched,
        scraped_at=scraped_at,
    )
    # Indices of rows that are NEW this run (everything after the existing pending length)
    new_indices = list(range(len(pending_before), len(pending)))
    log.info("New pending rows this run: %d", len(new_indices))

    # ---- 5. URL validation on the NEW pending rows only ------------------
    if not SKIP_REVIEW and new_indices:
        log.info("URL validation on %d new pending rows...", len(new_indices))
        subset = [pending[i] for i in new_indices]
        validated = validate_all(subset, max_workers=10)
        # Copy _url_check results back into pending in-place
        for i, idx in enumerate(new_indices):
            pending[idx]["_url_check"] = validated[i].get("_url_check", {})

    # ---- 6. AI review on the NEW pending rows only -----------------------
    openai_issues: List[Dict[str, Any]] = []
    claude_flags: List[Dict[str, Any]] = []
    if not SKIP_REVIEW and new_indices:
        new_rows = [pending[i] for i in new_indices]
        log.info("AI review pass on %d new rows", len(new_rows))
        openai_issues = openai_review(new_rows) if new_rows else []
        claude_flags = claude_review(new_rows) if new_rows else []

        # Apply non-destructive fixes to the new rows, then write them back
        fixed_new = apply_reviewer_fixes(new_rows, openai_issues, claude_flags)
        for i, idx in enumerate(new_indices):
            # Preserve _url_check / ScrapedAt / Status on the original row
            for k, v in fixed_new[i].items():
                if k.startswith("_"):
                    continue
                pending[idx][k] = v

    # ---- 7. Decide rejections --------------------------------------------
    rejections: Dict[int, str] = {}
    if not SKIP_REVIEW and new_indices:
        rejections = compute_rejections(pending, new_indices, openai_issues)

    # ---- 8. Promote approved rows -> Recalls -----------------------------
    new_approved, pending_after = promote_approved(
        pending=pending,
        approved_existing=approved,
        rejected_flags=rejections,
    )
    final_approved = sort_rows(approved + new_approved)
    final_pending = sort_rows(pending_after)

    log.info("Summary: +%d approved (total %d), Pending now %d (rejected kept: %d)",
             len(new_approved), len(final_approved), len(final_pending),
             sum(1 for r in final_pending if r.get("Status") == STATUS_REJECTED))

    # ---- 9. Save ---------------------------------------------------------
    save_xlsx_with_pending(final_approved, final_pending, XLSX_PATH)
    save_json(final_approved, JSON_PATH)

    # ---- 10. Commit ------------------------------------------------------
    if not SKIP_COMMIT:
        ok = git_commit_and_push(
            repo_dir=ROOT,
            files=["docs/data/recalls.xlsx", "docs/data/recalls.json"],
            message=(f"FSIS daily update {t0.strftime('%Y-%m-%d')} "
                     f"(+{len(new_approved)} approved, {len(final_pending)} pending)"),
        )
        if not ok:
            log.error("Git push failed")
            return 1

    elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
    log.info("=" * 60)
    log.info("DONE in %.1fs | approved total: %d | pending: %d | new approved: %d",
             elapsed, len(final_approved), len(final_pending), len(new_approved))
    return 0


if __name__ == "__main__":
    sys.exit(main())
