"""
AFTS Food Safety Intelligence — Gap Finder
Orchestrator (parametric — supports any country via --country flag).

Wires the 4 pipeline stages end-to-end:
  Stage 1: news_scraper.collect_rss + collect_google_news → candidates.jsonl
  Stage 2: search_verifier.build_index + match_in_index   → verified.jsonl
  Stage 3: extractor.extract_one (rules + LLM)            → pending.jsonl + rejected.jsonl
  Stage 4: append rows to docs/data/recalls.xlsx          (idempotent, dedupe by URL)

All paths come from the CountryConfig. The xlsx file is shared across countries
(single AFTS knowledge base); the per-country JSONL outputs are isolated.

CLI:
    python -m pipeline.gap_finder.main --country gr
    python -m pipeline.gap_finder.main --country it --verbose
    python -m pipeline.gap_finder.main --country it --dry-run     # no xlsx writes
    python -m pipeline.gap_finder.main --country it --skip-news   # reuse existing JSONL
"""

from __future__ import annotations
import argparse
import json
import sys
import time
import traceback
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

try:
    from . import news_scraper, article_fetcher, extractor
    from .countries import get as get_country
    from .countries.base import CountryConfig
except ImportError:
    from gap_finder import news_scraper, article_fetcher, extractor    # type: ignore
    from gap_finder.countries import get as get_country                # type: ignore
    from gap_finder.countries.base import CountryConfig                # type: ignore


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_XLSX = "docs/data/recalls.xlsx"
PENDING_SHEET = "Pending"
RECALLS_SHEET = "Recalls"
REJECTED_SHEET = "Weekly_Rejected"


# ─────────────────────────────────────────────────────────────────────────────
# RUN STATE — tallies for the final summary
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class RunState:
    country_code: str = ""
    country_name: str = ""
    started_at: str = ""
    candidates_found: int = 0
    verified_count: int = 0
    unmatched_count: int = 0
    extracted_accepted: int = 0
    extracted_rejected: int = 0
    appended_pending: int = 0
    appended_rejected: int = 0
    skipped_dupe_pending: int = 0
    skipped_dupe_recalls: int = 0
    skipped_dupe_rejected: int = 0
    errors: list[dict] = field(default_factory=list)

    def add_error(self, stage: str, exc: Exception) -> None:
        self.errors.append({
            "stage": stage, "type": type(exc).__name__, "msg": str(exc),
            "traceback": traceback.format_exc()[:500],
        })


# ─────────────────────────────────────────────────────────────────────────────
# XLSX I/O (idempotent append, dedupe by URL)
# ─────────────────────────────────────────────────────────────────────────────

def _load_existing_urls(xlsx_path: str, sheet_name: str) -> set[str]:
    """Read every URL already in the given sheet."""
    if not Path(xlsx_path).exists():
        return set()
    try:
        from openpyxl import load_workbook
    except ImportError:
        print("  [WARN] openpyxl missing — cannot dedupe", file=sys.stderr)
        return set()
    try:
        wb = load_workbook(xlsx_path, read_only=True, data_only=True)
        if sheet_name not in wb.sheetnames:
            return set()
        ws = wb[sheet_name]
        headers = [c.value for c in next(ws.iter_rows(min_row=1, max_row=1))]
        try:
            url_col = headers.index("URL")
        except ValueError:
            return set()
        urls: set[str] = set()
        for row in ws.iter_rows(min_row=2, values_only=True):
            if url_col < len(row) and row[url_col]:
                urls.add(str(row[url_col]).strip())
        wb.close()
        return urls
    except Exception as e:
        print(f"  [WARN] dedupe read failed for {sheet_name}: {e}", file=sys.stderr)
        return set()


def _append_rows(
    xlsx_path: str, sheet_name: str, rows: list[dict], dedupe_against: set[str],
) -> tuple[int, int]:
    """Append rows to xlsx sheet, deduplicating by URL. Returns (appended, skipped)."""
    if not rows:
        return 0, 0
    try:
        from openpyxl import load_workbook, Workbook
    except ImportError:
        print("  [ERROR] openpyxl missing — cannot write xlsx", file=sys.stderr)
        return 0, 0

    p = Path(xlsx_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if p.exists():
        wb = load_workbook(xlsx_path)
    else:
        wb = Workbook()
        # Remove default sheet
        if "Sheet" in wb.sheetnames:
            del wb["Sheet"]

    if sheet_name not in wb.sheetnames:
        ws = wb.create_sheet(sheet_name)
        # Headers = union of all row keys, with stable ordering
        headers = list(rows[0].keys())
        ws.append(headers)
    else:
        ws = wb[sheet_name]
        headers = [c.value for c in next(ws.iter_rows(min_row=1, max_row=1))]

    appended = skipped = 0
    for row in rows:
        url = (row.get("URL") or "").strip()
        if url and url in dedupe_against:
            skipped += 1
            continue
        ws.append([row.get(h, "") for h in headers])
        if url:
            dedupe_against.add(url)
        appended += 1

    wb.save(xlsx_path)
    return appended, skipped


def write_pending_rows(rows: list[dict], xlsx_path: str) -> tuple[int, int, int]:
    """Write to Pending sheet. Skip URLs already in Pending OR Recalls.
    Returns (appended, skipped_pending_dupe, skipped_recalls_dupe)."""
    pending_urls = _load_existing_urls(xlsx_path, PENDING_SHEET)
    recalls_urls = _load_existing_urls(xlsx_path, RECALLS_SHEET)

    pre_recalls_skip = sum(1 for r in rows if (r.get("URL") or "").strip() in recalls_urls)
    rows_not_in_recalls = [r for r in rows if (r.get("URL") or "").strip() not in recalls_urls]

    appended, pending_skipped = _append_rows(
        xlsx_path, PENDING_SHEET, rows_not_in_recalls, pending_urls
    )
    return appended, pending_skipped, pre_recalls_skip


def write_rejected_rows(rows: list[dict], xlsx_path: str) -> tuple[int, int]:
    """Write to Weekly_Rejected sheet. Dedupe by URL."""
    rejected_urls = _load_existing_urls(xlsx_path, REJECTED_SHEET)
    return _append_rows(xlsx_path, REJECTED_SHEET, rows, rejected_urls)


# ─────────────────────────────────────────────────────────────────────────────
# JSONL I/O
# ─────────────────────────────────────────────────────────────────────────────

def read_jsonl(path: str) -> list[dict]:
    p = Path(path)
    if not p.exists():
        return []
    out: list[dict] = []
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out


def append_jsonl(record: dict, path: str) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE STAGES
# ─────────────────────────────────────────────────────────────────────────────

def stage_news_scraper(cfg: CountryConfig, verbose: bool = False) -> list[dict]:
    print("\n=== Stage 1: News Scraper ===", file=sys.stderr)
    rss = news_scraper.collect_rss(cfg, verbose=verbose)
    gn = news_scraper.collect_google_news(cfg, verbose=verbose)
    all_cands = rss + gn
    deduped = news_scraper.deduplicate(all_cands)
    print(f"  RSS: {len(rss)}, Google News: {len(gn)}, deduped: {len(deduped)}",
          file=sys.stderr)
    news_scraper.write_jsonl(deduped, cfg.candidates_path)
    return [c.to_dict() for c in deduped]


def stage_article_fetch(
    candidates: list[dict], cfg: CountryConfig, verbose: bool = False,
) -> tuple[list[dict], list[dict]]:
    """Stage 2: fetch news article bodies in parallel. Replaces the old
    search-engine-based authority verifier (which was DDG-throttle-limited).

    Each candidate gets its article body fetched + cleaned + truncated.
    Output is schema-compatible with the old verified.jsonl so downstream
    extractor.py works unchanged.
    """
    print(f"\n=== Stage 2: News Article Fetcher ===", file=sys.stderr)
    if not candidates:
        return [], []

    enriched, failed = article_fetcher.enrich_all(candidates, cfg, verbose=verbose)
    print(f"  enriched: {len(enriched)}, failed/empty: {len(failed)}",
          file=sys.stderr)

    verified_dicts = [r.to_dict() for r in enriched]
    article_fetcher.write_jsonl(verified_dicts, cfg.verified_path)
    article_fetcher.write_jsonl(failed, cfg.unmatched_path)
    return verified_dicts, failed


def stage_extractor(
    verified: list[dict], cfg: CountryConfig, verbose: bool = False,
) -> tuple[list[dict], list[dict]]:
    print("\n=== Stage 3: Extractor ===", file=sys.stderr)
    if not verified:
        return [], []

    client = extractor.LlamaClient()
    print(f"  LlamaClient: {client.base_url}  model={client.model}",
          file=sys.stderr)
    if not client.health():
        print(f"  ERROR: cannot reach llama-server at {client.base_url}",
              file=sys.stderr)
        return [], []

    pending_rows: list[dict] = []
    rejected_rows: list[dict] = []

    for i, v in enumerate(verified, 1):
        if verbose:
            print(f"  [{i}/{len(verified)}] {v.get('efet_title', '')[:60]}",
                  file=sys.stderr)
        try:
            pending, rejected = extractor.extract_one(v, client, cfg, verbose=verbose)
        except Exception as e:
            print(f"  ERROR record {i}: {e}", file=sys.stderr)
            continue
        if pending:
            pending_rows.append(pending)
        if rejected:
            rejected_rows.append(rejected)

    extractor.write_jsonl(pending_rows, cfg.pending_path)
    extractor.write_jsonl(rejected_rows, cfg.rejected_path)

    print(f"  accepted: {len(pending_rows)}, rejected: {len(rejected_rows)}",
          file=sys.stderr)
    return pending_rows, rejected_rows


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description="AFTS Gap Finder — Orchestrator (parametric)"
    )
    parser.add_argument("--country", required=True,
                        help="ISO2 country code: gr, it, ...")
    parser.add_argument("--xlsx", default=DEFAULT_XLSX,
                        help=f"Path to recalls.xlsx (default: {DEFAULT_XLSX})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Run pipeline but skip xlsx writes")
    parser.add_argument("--skip-news", action="store_true",
                        help="Skip Stage 1 — reuse existing candidates.jsonl")
    parser.add_argument("--skip-verify", action="store_true",
                        help="Skip Stage 2 — reuse existing verified.jsonl")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    cfg = get_country(args.country)
    state = RunState(
        country_code=cfg.code, country_name=cfg.name_en,
        started_at=datetime.now(timezone.utc).isoformat(),
    )

    print(f"=== AFTS Gap Finder ({cfg.code}/{cfg.name_en}) "
          f"started {state.started_at} ===", file=sys.stderr)
    print(f"  authority: {cfg.authority_short} ({cfg.authority_domain})",
          file=sys.stderr)
    print(f"  xlsx:      {args.xlsx}", file=sys.stderr)
    print(f"  dry-run:   {args.dry_run}", file=sys.stderr)

    # ── Stage 1: News ───────────────────────────────────────────────────────
    if args.skip_news:
        candidates = read_jsonl(cfg.candidates_path)
        print(f"\n[Stage 1] SKIPPED — loaded {len(candidates)} candidates",
              file=sys.stderr)
    else:
        try:
            candidates = stage_news_scraper(cfg, verbose=args.verbose)
        except Exception as e:
            state.add_error("news_scraper", e)
            print(f"  ERROR: {e}", file=sys.stderr)
            candidates = []
    state.candidates_found = len(candidates)

    # ── Stage 2: Article Fetch ──────────────────────────────────────────────
    if args.skip_verify:
        verified = read_jsonl(cfg.verified_path)
        unmatched = read_jsonl(cfg.unmatched_path)
        print(f"\n[Stage 2] SKIPPED — loaded {len(verified)} verified, "
              f"{len(unmatched)} unmatched", file=sys.stderr)
    else:
        try:
            verified, unmatched = stage_article_fetch(candidates, cfg,
                                                     verbose=args.verbose)
        except Exception as e:
            state.add_error("article_fetcher", e)
            print(f"  ERROR: {e}", file=sys.stderr)
            verified, unmatched = [], []
    state.verified_count = len(verified)
    state.unmatched_count = len(unmatched)

    # ── Stage 3: Extract ────────────────────────────────────────────────────
    try:
        pending_rows, rejected_rows = stage_extractor(verified, cfg, verbose=args.verbose)
    except Exception as e:
        state.add_error("extractor", e)
        print(f"  ERROR: {e}", file=sys.stderr)
        pending_rows, rejected_rows = [], []
    state.extracted_accepted = len(pending_rows)
    state.extracted_rejected = len(rejected_rows)

    # ── Stage 4: Write xlsx ─────────────────────────────────────────────────
    print(f"\n=== Stage 4: Write to {args.xlsx} ===", file=sys.stderr)
    if args.dry_run:
        print(f"  DRY RUN — no xlsx writes performed", file=sys.stderr)
        print(f"  would append: {len(pending_rows)} pending, "
              f"{len(rejected_rows)} rejected", file=sys.stderr)
    else:
        try:
            app_p, skip_pending, skip_recalls = write_pending_rows(
                pending_rows, args.xlsx
            )
            state.appended_pending = app_p
            state.skipped_dupe_pending = skip_pending
            state.skipped_dupe_recalls = skip_recalls

            app_r, skip_rej = write_rejected_rows(rejected_rows, args.xlsx)
            state.appended_rejected = app_r
            state.skipped_dupe_rejected = skip_rej
        except Exception as e:
            state.add_error("write_xlsx", e)
            print(f"  ERROR writing xlsx: {e}", file=sys.stderr)

    # ── Summary ─────────────────────────────────────────────────────────────
    print("\n" + "=" * 70, file=sys.stderr)
    print("RUN SUMMARY", file=sys.stderr)
    print("=" * 70, file=sys.stderr)
    print(f"  country:                 {cfg.code} ({cfg.name_en})", file=sys.stderr)
    print(f"  candidates found:        {state.candidates_found}", file=sys.stderr)
    print(f"  verified (matched):      {state.verified_count}", file=sys.stderr)
    print(f"  unmatched:               {state.unmatched_count}", file=sys.stderr)
    print(f"  extracted accepted:      {state.extracted_accepted}", file=sys.stderr)
    print(f"  extracted rejected:      {state.extracted_rejected}", file=sys.stderr)
    print(f"  → appended Pending:      {state.appended_pending}", file=sys.stderr)
    print(f"  → appended Rejected:     {state.appended_rejected}", file=sys.stderr)
    print(f"  skipped (dupe Pending):  {state.skipped_dupe_pending}", file=sys.stderr)
    print(f"  skipped (dupe Recalls):  {state.skipped_dupe_recalls}", file=sys.stderr)
    print(f"  skipped (dupe Rejected): {state.skipped_dupe_rejected}", file=sys.stderr)
    print(f"  errors:                  {len(state.errors)}", file=sys.stderr)
    print("=" * 70, file=sys.stderr)

    # ── Run log ─────────────────────────────────────────────────────────────
    log_record = asdict(state)
    log_record["finished_at"] = datetime.now(timezone.utc).isoformat()
    append_jsonl(log_record, cfg.run_log_path)

    return 1 if state.errors else 0


if __name__ == "__main__":
    sys.exit(main())
