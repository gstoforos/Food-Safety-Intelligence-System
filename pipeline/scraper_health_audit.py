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
       OK_EMPTY        — returned 0, and this source HAS produced rows
                         within its own historical publication cadence
       SILENT_STALE    — returned 0, has produced before, but has been
                         silent far beyond its own p90 gap
       NEVER_PRODUCED  — returned 0, and has never produced a single row
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
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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


# ── REGISTER CROSS-CHECK (audit 2026-09-01) ────────────────────────────────
# This module was written because 36 of 66 scrapers were failing silently and
# the orchestrator's summary line masked it. It fixed that for HARD failures.
# It then created a softer blind spot of its own: any scraper reporting
# "[DONE] ... 0" that could not be matched to a failure line was bucketed
# OK_EMPTY and DESCRIBED IN THE DOCSTRING as a "legitimately empty window".
# That description is an assumption, not a measurement. A scraper whose parser
# silently returns [] is indistinguishable from a regulator that published
# nothing.
#
# Measured on 2026-09-01: 40 of 55 scrapers were OK_EMPTY on the same day, and
# 36 of 55 have never written a single row to the register under their own
# Source label in its entire history. "Legitimately empty" cannot describe
# that.
#
# The register itself is the evidence. For each source we already know how
# often it has historically produced rows, so silence can be judged against
# that source's OWN cadence rather than against a guessed global threshold.
# A source needs at least two dated rows before an inter-row gap exists.
_MIN_ROWS_FOR_CADENCE = 2
# Never flag a source quiet for less than this, whatever its cadence says.
_STALE_FLOOR_DAYS = 21
# Fallback for sources too thin to have a cadence of their own: the POOLED
# p99 inter-row gap measured across every source in the register. Computed at
# runtime, not hardcoded, so it tracks the corpus. On 2026-09-01 the pooled
# distribution over 491 gaps was p50=2, p75=6, p90=15, p95=27, p99=91, max=113
# days — so a thin-history source silent beyond ~91 days has exceeded almost
# anything the register has ever seen, which is evidence rather than a guess.
_POOLED_FALLBACK_QUANTILE = 0.99


def _display_to_source_key(display_name: str) -> str:
    """'AESAN (ES)/Spain' -> 'AESAN (ES)';  'FDA/USA' -> 'FDA'.

    Exact prefix before the country suffix. Deliberately NOT fuzzy: an
    earlier ad-hoc substring match in analysis collapsed FDA (PH), FDA (GH),
    NAFDAC (NG) and TFDA (TW) onto the US FDA's 80 rows and produced four
    confidently wrong numbers. An unmatched name stays unmatched.
    """
    return display_name.split("/")[0].strip()


def register_activity(recalls_json: Path) -> Dict[str, Dict[str, Any]]:
    """Per Source label: row count, last row date, and p90 inter-row gap."""
    try:
        rows = json.loads(Path(recalls_json).read_text(encoding="utf-8"))
    except Exception as exc:                                # noqa: BLE001
        log.warning("Register cross-check unavailable (%s: %s)",
                    type(exc).__name__, str(exc)[:80])
        return {}
    by_src: Dict[str, list] = {}
    for r in rows:
        src = str(r.get("Source") or "").strip()
        d = str(r.get("Date") or "")[:10]
        if not src or len(d) != 10:
            continue
        by_src.setdefault(src, []).append(d)
    today = datetime.now(timezone.utc).date()
    out: Dict[str, Dict[str, Any]] = {}
    pooled: list = []
    for src, dates in by_src.items():
        ds = sorted({d for d in dates})
        try:
            parsed = [date.fromisoformat(d) for d in ds]
        except ValueError:
            continue
        gaps = [(b - a).days for a, b in zip(parsed, parsed[1:])]
        pooled.extend(gaps)
        gaps.sort()
        p90 = gaps[int(len(gaps) * 0.9)] if gaps else None
        out[src] = {
            "rows": len(dates),
            "last_row": ds[-1],
            "days_since_last_row": (today - parsed[-1]).days,
            "p90_gap_days": p90,
        }
    pooled.sort()
    fallback = (pooled[int(len(pooled) * _POOLED_FALLBACK_QUANTILE)]
                if pooled else None)
    for v in out.values():
        v["pooled_p99_gap_days"] = fallback
    return out


def refine_empty_states(per_scraper: Dict[str, str],
                        activity: Dict[str, Dict[str, Any]]
                        ) -> tuple:
    """Split OK_EMPTY into OK_EMPTY / SILENT_STALE / NEVER_PRODUCED.

    Only OK_EMPTY entries are touched. A hard failure keeps its state.
    Returns (refined_states, per_scraper_evidence).
    """
    refined = dict(per_scraper)
    evidence: Dict[str, Dict[str, Any]] = {}
    for display, state in per_scraper.items():
        if state != "OK_EMPTY":
            continue
        key = _display_to_source_key(display)
        act = activity.get(key)
        if act is None:
            refined[display] = "NEVER_PRODUCED"
            evidence[display] = {"source_key": key, "rows": 0,
                                 "note": "no rows under this Source label"}
            continue
        ev = dict(act); ev["source_key"] = key
        evidence[display] = ev
        # Thin history: judge against the pooled corpus gap rather than
        # skipping. Skipping is what hid AGES Austria — 113 days silent on a
        # 23-day cadence — behind an OK_EMPTY on 2026-09-01.
        if act["rows"] < _MIN_ROWS_FOR_CADENCE or act["p90_gap_days"] is None:
            fb = act.get("pooled_p99_gap_days")
            if fb is None:
                continue
            ev["stale_threshold_days"] = fb
            ev["threshold_basis"] = "pooled p99 (thin history)"
            if act["days_since_last_row"] > fb:
                refined[display] = "SILENT_STALE"
            continue
        threshold = max(_STALE_FLOOR_DAYS, 2 * act["p90_gap_days"])
        ev["threshold_basis"] = "2x this source's own p90 gap"
        ev["stale_threshold_days"] = threshold
        if act["days_since_last_row"] > threshold:
            refined[display] = "SILENT_STALE"
    return refined, evidence


def write_health_report(per_scraper: Dict[str, str],
                        summary: Dict[str, int],
                        out_path: Path = HEALTH_PATH,
                        evidence: Optional[Dict[str, Dict[str, Any]]] = None
                        ) -> None:
    total = sum(summary.values())
    failed = sum(v for k, v in summary.items() if k.startswith("FAIL_"))
    # A scraper that has never produced a row, or has gone silent well past
    # its own cadence, is not "ok". Counting it as ok is what let 40 of 55
    # read as healthy on 2026-09-01.
    ok = summary.get("OK", 0) + summary.get("OK_EMPTY", 0)
    silent = summary.get("SILENT_STALE", 0) + summary.get("NEVER_PRODUCED", 0)
    fail_pct = (100.0 * failed / total) if total else 0.0

    report = {
        "generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "totals": {
            "scrapers": total,
            "ok": ok,
            "ok_returning_data": summary.get("OK", 0),
            "ok_empty_window": summary.get("OK_EMPTY", 0),
            # 2026-09-01: silence is now reported, not absorbed into "ok".
            "silent_stale": summary.get("SILENT_STALE", 0),
            "never_produced": summary.get("NEVER_PRODUCED", 0),
            "failed": failed,
            "fail_pct": round(fail_pct, 1),
        },
        "silent": silent,
        "by_state": summary,
        "per_scraper": per_scraper,
        "silence_evidence": evidence or {},
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
    # Cross-check every "empty" scraper against the register's own history
    # before calling it healthy — see refine_empty_states().
    activity = register_activity(HEALTH_PATH.parent / "recalls.json")
    per_scraper, evidence = refine_empty_states(per_scraper, activity)
    summary = {}
    for st in per_scraper.values():
        summary[st] = summary.get(st, 0) + 1
    write_health_report(per_scraper, summary, evidence=evidence)
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
