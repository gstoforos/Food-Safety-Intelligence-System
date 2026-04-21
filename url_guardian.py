"""
Claude final URL check — daily gatekeeper before the weekly report.

Runs at 07:30 UTC, after:
  - 17:00 daily scrape (previous evening)
  - 06:00 OpenAI gap-finder (~90 minutes earlier)

And before:
  - 08:00 Friday weekly report (~30 minutes later)

Scope: ALL rows currently in Pending (including previously-rejected ones — the
retry logic in append_to_pending already re-queues them when the scraper / gap-
finder picks them up again, but any remaining rejected rows still get one more
URL check here in case the previous URL is now live).

Gate logic per pending row:

  1. Run URL validator (same HEAD/GET logic used in the 17:00 pipeline).
     - If URL is DEAD (404 / 410 / 5xx / generic landing page) -> DELETE the
       row entirely. Dead URL = nothing to audit, nothing to publish.
     - If URL is OK (or 403 on a known bot-hostile gov domain)  -> continue.

  2. Ask Claude Haiku to do a final "does this row look publishable?" check:
     - Complete required fields (Date, Company, Product, Pathogen, URL)
     - URL looks like a specific recall page, not a category
     - Pathogen name matches the reason text
     Claude returns pass/fail per row.

  3. If Claude says pass  -> promote to Recalls
     If Claude says fail  -> keep in Pending as rejected with the reason
     If Claude is disabled (no API key) -> promote anyway (URL check alone is
     the minimum bar; Haiku is a nice-to-have second opinion).

After all promotions: save xlsx, commit, push. The 08:00 weekly report then
reads a clean Recalls tab.
"""
from __future__ import annotations
import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pipeline.merge_master import (  # noqa: E402
    load_existing, load_pending,
    promote_approved, sort_rows,
    save_xlsx_with_pending, mirror_json_from_xlsx,
    STATUS_REJECTED, STATUS_PENDING,
)
from pipeline.commit_github import git_commit_and_push  # noqa: E402
from review.url_validator import check_url, should_blank_url  # noqa: E402
from review.claude_client import _call_claude, _strip_fences, ENABLED as CLAUDE_ENABLED  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("claude-url-check")

DATA_DIR = ROOT / "docs" / "data"
XLSX_PATH = DATA_DIR / "recalls.xlsx"
JSON_PATH = DATA_DIR / "recalls.json"

SKIP_COMMIT = os.getenv("SKIP_COMMIT", "").lower() in ("1", "true", "yes")

REQUIRED_FIELDS = ("Date", "Company", "Product", "Pathogen", "URL")

# Max pending rows to send Claude in a single batch request
CLAUDE_BATCH_SIZE = 20


# ---------------------------------------------------------------------------
# Step 1: URL validator on every Pending row
# ---------------------------------------------------------------------------
def validate_urls(pending: List[Dict[str, Any]]) -> Tuple[List[int], List[int]]:
    """
    Returns (indices_to_delete, indices_still_alive).
    Dead = should_blank_url() says yes OR URL is missing entirely.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    delete_idx: List[int] = []
    alive_idx: List[int] = []

    def _check(i: int) -> Tuple[int, Dict[str, Any]]:
        url = pending[i].get("URL", "") or ""
        return i, check_url(url)

    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = [ex.submit(_check, i) for i in range(len(pending))]
        for f in as_completed(futs):
            i, check = f.result()
            pending[i]["_url_check"] = check
            url = pending[i].get("URL", "") or ""
            if not url or should_blank_url(check):
                delete_idx.append(i)
            else:
                alive_idx.append(i)

    log.info("URL check: %d alive, %d to delete (dead URLs)",
             len(alive_idx), len(delete_idx))
    return sorted(delete_idx), sorted(alive_idx)


# ---------------------------------------------------------------------------
# Step 2: Claude second-opinion on the survivors
# ---------------------------------------------------------------------------
CLAUDE_GATE_SYSTEM = (
    "You are the final reviewer before a food-safety recall is published on a "
    "public dashboard. Return ONLY strict JSON — no markdown, no prose."
)

CLAUDE_GATE_PROMPT = """Each row below is a pathogen food-recall record proposed for publication.
Its URL has already been confirmed reachable. Your job: decide publishable or not.

Reject a row if ANY of the following:
- A required field is missing or "—": Date, Company, Product, Pathogen, URL
- The URL looks like a homepage, category listing, search page, or tag page
  (not a specific recall page)
- The Pathogen field doesn't match the Reason text (e.g. Pathogen="Listeria"
  but Reason mentions Salmonella)
- The row is obvious low-quality / spam / clearly duplicated data

Return strict JSON:
{{"decisions": [{{"row_index": <int>, "pass": true|false, "reason": "<short>"}}]}}

Include EVERY input row in decisions, not only the failures.

Rows:
{rows_json}
"""


def _missing_required(row: Dict[str, Any]) -> List[str]:
    missing = []
    for f in REQUIRED_FIELDS:
        v = row.get(f)
        if v is None or (isinstance(v, str) and not v.strip()) or v == "—":
            missing.append(f)
    return missing


def claude_gate(rows: List[Dict[str, Any]]) -> Dict[int, Tuple[bool, str]]:
    """
    Ask Claude to pass/fail each row. Returns {input_index: (pass, reason)}.
    Falls back to deterministic required-field check if Claude is disabled
    or the call fails.
    """
    decisions: Dict[int, Tuple[bool, str]] = {}

    # Deterministic fallback = required-field check. Always compute it so we
    # have something even if Claude is disabled.
    det_fail: Dict[int, str] = {}
    for i, r in enumerate(rows):
        miss = _missing_required(r)
        if miss:
            det_fail[i] = f"Missing required: {', '.join(miss)}"

    if not CLAUDE_ENABLED:
        log.info("Claude disabled — using deterministic required-field check only")
        for i in range(len(rows)):
            if i in det_fail:
                decisions[i] = (False, det_fail[i])
            else:
                decisions[i] = (True, "ok (claude disabled)")
        return decisions

    # Batch-call Claude
    for start in range(0, len(rows), CLAUDE_BATCH_SIZE):
        chunk = rows[start:start + CLAUDE_BATCH_SIZE]
        # Claude sees row_index 0..N-1 relative to this batch
        batch_view = [{"row_index": j,
                       "Date": r.get("Date", ""),
                       "Source": r.get("Source", ""),
                       "Company": r.get("Company", ""),
                       "Brand": r.get("Brand", ""),
                       "Product": r.get("Product", ""),
                       "Pathogen": r.get("Pathogen", ""),
                       "Reason": r.get("Reason", ""),
                       "Class": r.get("Class", ""),
                       "Country": r.get("Country", ""),
                       "URL": r.get("URL", "")}
                      for j, r in enumerate(chunk)]
        prompt = CLAUDE_GATE_PROMPT.format(
            rows_json=json.dumps(batch_view, ensure_ascii=False, indent=2),
        )
        txt = _call_claude(prompt, max_tokens=4000, system=CLAUDE_GATE_SYSTEM)
        got_response = False
        if txt:
            try:
                data = json.loads(_strip_fences(txt))
                for d in data.get("decisions", []):
                    j = d.get("row_index")
                    if j is None or j < 0 or j >= len(chunk):
                        continue
                    real_idx = start + j
                    decisions[real_idx] = (bool(d.get("pass")),
                                           str(d.get("reason") or "")[:300])
                got_response = True
            except json.JSONDecodeError as e:
                log.warning("Claude gate JSON parse failed: %s | text=%s", e, txt[:200])

        # Fill any rows Claude didn't cover with the deterministic fallback
        for j in range(len(chunk)):
            real_idx = start + j
            if real_idx in decisions:
                continue
            if real_idx in det_fail:
                decisions[real_idx] = (False, det_fail[real_idx] + " (claude unreached)")
            elif not got_response:
                # Claude call failed entirely — conservatively pass URL-confirmed rows
                decisions[real_idx] = (True, "ok (claude unreached, url verified)")
            else:
                # Claude responded but skipped this row — conservative pass
                decisions[real_idx] = (True, "ok (claude silent, url verified)")

    passes = sum(1 for v in decisions.values() if v[0])
    log.info("Claude gate: %d pass, %d fail across %d rows",
             passes, len(decisions) - passes, len(rows))
    return decisions


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    t0 = datetime.now(timezone.utc)
    log.info("=" * 60)
    log.info("Claude URL check (final gatekeeper) started: %s",
             t0.strftime("%Y-%m-%dT%H:%M:%SZ"))

    approved = load_existing(XLSX_PATH) if XLSX_PATH.exists() else []
    pending = load_pending(XLSX_PATH) if XLSX_PATH.exists() else []
    log.info("State: %d approved + %d pending", len(approved), len(pending))

    if not pending:
        log.info("Nothing in Pending — nothing to do. Exiting cleanly.")
        return 0

    # ---- Step 1: URL check, delete dead URLs ----------------------------
    delete_idx, alive_idx = validate_urls(pending)
    if delete_idx:
        log.info("Deleting %d rows with dead URLs", len(delete_idx))
    alive_rows = [pending[i] for i in alive_idx]

    # ---- Step 2: Claude second-opinion on the survivors ----------------
    if not alive_rows:
        log.info("No surviving rows to gate")
        final_pending_out = []
        new_approved: List[Dict[str, Any]] = []
    else:
        # Any row whose URL is now LIVE is eligible for re-evaluation, even if
        # it was marked 'rejected' in a prior run. Reset its Status so the
        # promote_approved guard doesn't auto-keep it as rejected.
        for row in alive_rows:
            if row.get("Status") == STATUS_REJECTED:
                row["Status"] = STATUS_PENDING
                # Strip the old REJECTED: prefix — this is a fresh evaluation
                notes = (row.get("Notes") or "").strip()
                if notes.startswith("REJECTED:"):
                    # Remove "REJECTED: <reason> | " prefix, keep any trailing original notes
                    tail = notes.split(" | ", 1)
                    row["Notes"] = tail[1] if len(tail) > 1 else ""

        decisions = claude_gate(alive_rows)

        # Translate decisions (keyed by alive_rows index) into rejected_flags
        # for promote_approved. That helper takes `pending` (rows to stay
        # considered) plus rejected_flags keyed by pending-list index. We hand
        # it alive_rows directly; dead rows have already been dropped.
        rejected_flags: Dict[int, str] = {}
        for j, row in enumerate(alive_rows):
            passed, reason = decisions.get(j, (True, "ok"))
            if not passed:
                rejected_flags[j] = f"Claude gate: {reason}"

        new_approved, final_pending_out = promote_approved(
            pending=alive_rows,
            approved_existing=approved,
            rejected_flags=rejected_flags,
        )

    # ---- Assemble final state + save ------------------------------------
    final_approved = sort_rows(approved + new_approved)
    final_pending = sort_rows(final_pending_out)

    save_xlsx_with_pending(final_approved, final_pending, XLSX_PATH)
    # json = strict mirror of xlsx Recalls sheet (architecture rule)
    mirror_json_from_xlsx(XLSX_PATH, JSON_PATH)

    # ---- Commit ---------------------------------------------------------
    if not SKIP_COMMIT:
        ok = git_commit_and_push(
            repo_dir=ROOT,
            files=["docs/data/recalls.xlsx", "docs/data/recalls.json"],
            message=(f"FSIS URL-gate {t0.strftime('%Y-%m-%d')} "
                     f"(+{len(new_approved)} approved, "
                     f"-{len(delete_idx)} dead, "
                     f"{len(final_pending)} pending)"),
        )
        if not ok:
            log.error("Git push failed")
            return 1

    elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
    log.info("=" * 60)
    log.info("DONE in %.1fs | +%d approved | -%d dead | %d pending remaining",
             elapsed, len(new_approved), len(delete_idx), len(final_pending))
    return 0


if __name__ == "__main__":
    sys.exit(main())
