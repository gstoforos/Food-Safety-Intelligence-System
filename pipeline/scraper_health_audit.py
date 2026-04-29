"""
Scraper health audit — categorizes per-scraper failures from a run_all
log and writes a JSON file the dashboard can render.

Audit 2026-04-29: A run_all execution returned 0 recalls scraped while
36 of 66 scrapers were silently failing (24 × HTTP 404, 5 × 403, 2 ×
DNS, 2 × SSL, 3 × timeout). The orchestrator's final summary line
("DONE in 161.1s | approved total: 271 | pending: 4 | new approved: 0")
masked the wholesale collapse. There was no signal that broad coverage
had been lost.

This module fixes the visibility gap. It:
  1. Parses the orchestrator's stderr/stdout log
  2. Buckets each scraper into one of seven states:
       OK              — scraper ran, returned >0 recalls
       OK_EMPTY        — scraper ran, returned 0 (legitimately empty window)
       FAIL_404        — regulator URL is dead (URL changed)
       FAIL_403        — regulator is blocking us (User-Agent issue)
       FAIL_DNS        — DNS resolution failed (regulator site down or transient)
       FAIL_SSL        — TLS handshake failed (DH_KEY_TOO_SMALL, expired cert)
       FAIL_TIMEOUT    — connection or read timeout (regulator slow)
       FAIL_OTHER      — anything else
  3. Writes docs/data/scraper-health.json with timestamp + breakdown
  4. Exits non-zero if more than HEALTH_FAIL_THRESHOLD scrapers failed,
     so the GitHub Actions run is marked as a real failure that you'll
     see in the email summary.

USAGE
-----
    # Pipe a run_all log into this script:
    python -m pipeline.run_all 2>&1 | python -m pipeline.scraper_health_audit

    # Or read a captured log file:
    python -m pipeline.scraper_health_audit --log /path/to/run.log

    # Or, integrated into run_all's workflow YAML:
    python -m pipeline.run_all 2>&1 | tee /tmp/run.log
    python -m pipeline.scraper_health_audit --log /tmp/run.log

The threshold defaults to: fail the run if MORE THAN 33% of scrapers
hard-failed. Override with SCRAPER_FAIL_PCT_THRESHOLD env var.
"""
from __future__ import annotations
import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

ROOT = Path(__file__).resolve().parent.parent
HEALTH_PATH = ROOT / "docs" / "data" / "scraper-health.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("scraper-health")

DEFAULT_FAIL_PCT = float(os.getenv("SCRAPER_FAIL_PCT_THRESHOLD", "33"))

# ──────────────────────────────────────────────────────────────────────
# Log line patterns
# ──────────────────────────────────────────────────────────────────────
RE_DONE = re.compile(
    r"orchestrator: \[DONE\]\s+(?P<name>\S+(?:\s\([A-Z]{2}\))?(?:/\S+)?)\s+->\s+(?P<count>\d+)\s+recalls"
)
RE_404 = re.compile(r"scraper\.(?P<scraper>[\w_]+):\s+skip\s+\S+\s+\(status=404\)")
RE_403 = re.compile(r"scraper\.(?P<scraper>[\w_]+):\s+skip\s+\S+\s+\(status=403\)")
RE_NORESP = re.compile(r"scraper\.(?P<scraper>[\w_]+):\s+skip\s+\S+\s+\(status=no-response\)")
RE_DNS = re.compile(
    r"NameResolutionError|Failed to resolve|Name or service not known"
)
RE_SSL = re.compile(
    r"SSLError|CERTIFICATE_VERIFY_FAILED|DH_KEY_TOO_SMALL"
)
RE_TIMEOUT = re.compile(
    r"ReadTimeoutError|ConnectTimeoutError|Read timed out|Connection to .* timed out"
)
RE_RAPPELCONSO_FAIL = re.compile(
    r"All RappelConso datasets failed or empty"
)


def categorize_run(log_text: str) -> Tuple[Dict[str, str], Dict[str, int]]:
    """Walk the log, return (per_scraper_status, summary_counts).

    per_scraper_status maps display_name → state code (OK / OK_EMPTY /
    FAIL_404 / FAIL_403 / FAIL_DNS / FAIL_SSL / FAIL_TIMEOUT / FAIL_OTHER).

    summary_counts is a flat dict of state → count.
    """
    # Track which scrapers reached the [DONE] line + their result count
    done: Dict[str, int] = {}
    for m in RE_DONE.finditer(log_text):
        name = m.group("name")
        count = int(m.group("count"))
        done[name] = count

    # Map scraper-internal names (e.g. "aesan_es") to the display name
    # used at [DONE] (e.g. "AESAN (ES)/Spain"). The log uses both —
    # we'll do best-effort matching.
    scraper_failures: Dict[str, str] = {}  # internal_name → state
    for m in RE_404.finditer(log_text):
        scraper_failures.setdefault(m.group("scraper"), "FAIL_404")
    for m in RE_403.finditer(log_text):
        scraper_failures.setdefault(m.group("scraper"), "FAIL_403")
    for m in RE_NORESP.finditer(log_text):
        # No-response could be DNS, SSL, or timeout. Look at the
        # preceding lines to classify, but if we can't tell, mark as
        # FAIL_OTHER so we still capture it.
        scraper_failures.setdefault(m.group("scraper"), "FAIL_OTHER")

    # Categorise FAIL_OTHER more precisely by scanning context windows.
    # For each "no-response" scraper, look ~15 lines before the skip
    # message for DNS/SSL/timeout signatures.
    lines = log_text.splitlines()
    for i, line in enumerate(lines):
        m = RE_NORESP.search(line)
        if not m:
            continue
        scraper = m.group("scraper")
        ctx = "\n".join(lines[max(0, i - 15):i])
        if RE_DNS.search(ctx):
            scraper_failures[scraper] = "FAIL_DNS"
        elif RE_SSL.search(ctx):
            scraper_failures[scraper] = "FAIL_SSL"
        elif RE_TIMEOUT.search(ctx):
            scraper_failures[scraper] = "FAIL_TIMEOUT"

    # Build per-display-name status. Iterate over [DONE] entries. If
    # ANY internal-scraper failure name appears as a substring of the
    # display name (case-insensitive), that scraper is failed.
    per_scraper: Dict[str, str] = {}
    for display_name, count in done.items():
        # Normalize display name for matching
        display_lower = display_name.lower().replace(" ", "_").replace("(", "").replace(")", "")
        matched_failure = None
        for internal, state in scraper_failures.items():
            internal_norm = internal.lower()
            if internal_norm in display_lower or display_lower.startswith(internal_norm):
                matched_failure = state
                break
        if matched_failure:
            per_scraper[display_name] = matched_failure
        elif count > 0:
            per_scraper[display_name] = "OK"
        else:
            per_scraper[display_name] = "OK_EMPTY"

    # RappelConso special case — it doesn't fail with an HTTP-skip
    # message, it logs "All RappelConso datasets failed or empty".
    # Detect that and override its OK_EMPTY → FAIL_OTHER.
    if RE_RAPPELCONSO_FAIL.search(log_text):
        for display_name in per_scraper:
            if "rappelconso" in display_name.lower() or "france" in display_name.lower():
                per_scraper[display_name] = "FAIL_OTHER"
                break

    # Summary counts
    summary: Dict[str, int] = {
        "OK": 0, "OK_EMPTY": 0,
        "FAIL_404": 0, "FAIL_403": 0, "FAIL_DNS": 0,
        "FAIL_SSL": 0, "FAIL_TIMEOUT": 0, "FAIL_OTHER": 0,
    }
    for state in per_scraper.values():
        summary[state] = summary.get(state, 0) + 1

    return per_scraper, summary


def write_health_report(per_scraper: Dict[str, str],
                        summary: Dict[str, int],
                        out_path: Path = HEALTH_PATH) -> None:
    total = sum(summary.values())
    failed = sum(v for k, v in summary.items() if k.startswith("FAIL_"))
    ok = summary.get("OK", 0) + summary.get("OK_EMPTY", 0)
    fail_pct = (100.0 * failed / total) if total else 0.0

    report = {
        "generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "totals": {
            "scrapers": total,
            "ok": ok,
            "ok_returning_data": summary.get("OK", 0),
            "ok_empty_window": summary.get("OK_EMPTY", 0),
            "failed": failed,
            "fail_pct": round(fail_pct, 1),
        },
        "by_state": summary,
        "per_scraper": per_scraper,
        "fail_threshold_pct": DEFAULT_FAIL_PCT,
        "exceeded_threshold": fail_pct > DEFAULT_FAIL_PCT,
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False),
                        encoding="utf-8")
    log.info("Wrote %s", out_path)


def print_summary(per_scraper: Dict[str, str], summary: Dict[str, int]) -> None:
    total = sum(summary.values())
    print()
    print("=" * 60)
    print(" SCRAPER HEALTH AUDIT")
    print("=" * 60)
    print(f" Total scrapers:           {total}")
    print(f"   OK (returned data):     {summary.get('OK', 0)}")
    print(f"   OK (empty window):      {summary.get('OK_EMPTY', 0)}")
    failed = sum(v for k, v in summary.items() if k.startswith('FAIL_'))
    print(f"   FAILED:                 {failed}")
    print(f"     • HTTP 404 (URL gone):  {summary.get('FAIL_404', 0)}")
    print(f"     • HTTP 403 (blocked):   {summary.get('FAIL_403', 0)}")
    print(f"     • DNS unreachable:      {summary.get('FAIL_DNS', 0)}")
    print(f"     • SSL/TLS error:        {summary.get('FAIL_SSL', 0)}")
    print(f"     • Timeout:              {summary.get('FAIL_TIMEOUT', 0)}")
    print(f"     • Other:                {summary.get('FAIL_OTHER', 0)}")
    print()
    if failed:
        print(" FAILING SCRAPERS:")
        for name, state in sorted(per_scraper.items(),
                                  key=lambda kv: (kv[1], kv[0])):
            if state.startswith("FAIL_"):
                print(f"   [{state:14}] {name}")
    print("=" * 60)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--log", type=Path, default=None,
                    help="Read log from file instead of stdin")
    ap.add_argument("--no-fail", action="store_true",
                    help="Always exit 0 even if threshold exceeded "
                         "(report-only mode for ad-hoc audits)")
    args = ap.parse_args()

    if args.log:
        log_text = args.log.read_text(encoding="utf-8", errors="replace")
    else:
        log_text = sys.stdin.read()

    if not log_text.strip():
        log.error("No log content received (stdin was empty and no --log given)")
        return 1

    per_scraper, summary = categorize_run(log_text)
    write_health_report(per_scraper, summary)
    print_summary(per_scraper, summary)

    total = sum(summary.values())
    failed = sum(v for k, v in summary.items() if k.startswith("FAIL_"))
    fail_pct = (100.0 * failed / total) if total else 0.0

    if fail_pct > DEFAULT_FAIL_PCT and not args.no_fail:
        log.error("Scraper health: %.1f%% of scrapers failed "
                  "(%d of %d) — exceeds %.0f%% threshold",
                  fail_pct, failed, total, DEFAULT_FAIL_PCT)
        log.error("Run is marked FAILED so the next operator notices.")
        return 1

    log.info("Scraper health: %.1f%% failed — within threshold (%.0f%%)",
             fail_pct, DEFAULT_FAIL_PCT)
    return 0


if __name__ == "__main__":
    sys.exit(main())
