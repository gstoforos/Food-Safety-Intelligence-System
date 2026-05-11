"""
pipeline/gemini_check.py — second-pass review using Gemini direct API.

Drop-in substitute for pipeline/openrouter_check.py. Same monkey-patch
pattern, same audit-tag scheme, same workflow contract. Different
backend: direct Google Gemini API instead of OpenRouter.

DESIGN
======
This module IMPORTS pipeline.claude_check and MONKEY-PATCHES its
_verify_with_claude function to call Gemini instead of Anthropic.

All other logic (prompt, verdict-fix-upgrade, corrections handling,
audit stamps, xlsx writes, git commit, cooldown tracking) is REUSED
from claude_check.py unchanged. Any future improvement to claude_check
(better prompt, new corrections, etc.) automatically propagates here.

This is the cleanest pattern for a "dissent reviewer" — same task,
different model — and matches what openrouter_check.py did before it
broke.

WHY THIS REPLACES openrouter_check.py
=====================================
Production logs from 2026-05-09 and 2026-05-10 showed every OpenRouter
reviewer call returning HTTP 404 because the configured DeepSeek free-
tier model (deepseek/deepseek-chat-v3.1:free) was retired in DeepSeek's
V4 transition. The configured paid fallback (deepseek/deepseek-chat-v3.1)
was also retired in the same transition. Net effect: every Pending row
SKIPped at the dissent reviewer stage → 0 promotions to Recalls for
two weeks regardless of how many real recalls the scrapers found.

Switching to direct Gemini API removes the OpenRouter dependency
entirely. Google manages model lifecycle on stable cycles (model IDs
remain valid for ~12-24 months with deprecation announcements months
in advance). The May 2026 incident cannot recur with this design.

WHEN IT RUNS
============
Athens 20:00 — after the 19:00 second-pass URL gate. Catches errors
in rows scraped at 18:00 daily-scrape that a single morning chain
might have missed (or disputes Claude morning-verdicts that look
uncertain).

To swap from OpenRouter to Gemini in production, change the GitHub
Actions workflow command:
    OLD: python -m pipeline.openrouter_check
    NEW: python -m pipeline.gemini_check
Nothing else changes.

COST
====
Free path: $0/run on Gemini 2.5 Flash free tier (250 req/day per key,
5-key rotation = 1250/day pool capacity, reviewer uses ~50/day).
Falls back to Gemini 2.0 Flash (1500/day per key = 7500/day pool) if
primary key pool is exhausted.

There is no paid fallback configured by default. If both Gemini models'
key pools are simultaneously exhausted (extreme — would require an
unrelated scraper script to burn through all keys), reviewer calls
return None and rows SKIP for retry next run, same behavior as the
OpenRouter client's contract.

ENV
===
  GEMINI_API_KEY_1 .. GEMINI_API_KEY_5  — at least one required
  GEMINI_API_KEY                        — fallback if numbered absent
  GEMINI_REVIEWER_MODEL                 — optional, default gemini-2.5-flash
  GEMINI_REVIEWER_FALLBACK_MODEL        — optional, default gemini-2.0-flash

TAGS
====
All audit notes written by this module are prefixed
"[gemini-check ...]" so they're easy to distinguish from
"[claude-check ...]" tags. The Thursday review email surfaces both,
keeping the morning-Claude / evening-Gemini dual-pass dissent record
intact for operator review.
"""
from __future__ import annotations

import os
import sys
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Import claude_check so we can patch and reuse its main()
from pipeline import claude_check  # noqa: E402
from review.gemini_reviewer import (  # noqa: E402
    _call_gemini, _clean_response, ENABLED as GEMINI_ENABLED,
    MODEL as GEMINI_MODEL,
)

log = logging.getLogger("gemini-check")


# ============================================================
# Patched _verify_with_X — calls Gemini, returns same JSON shape
# ============================================================
def _verify_with_gemini(row: Dict[str, Any],
                       page_text: str) -> Optional[Dict[str, Any]]:
    """
    Drop-in replacement for claude_check._verify_with_claude.

    Uses direct Google Gemini API instead of Anthropic. Returns the
    parsed JSON dict, or None on failure (same contract as the
    original — caller SKIPs the row on None and retries next run).
    """
    if not GEMINI_ENABLED:
        return None

    prompt = claude_check.CLAUDE_CHECK_PROMPT.format(
        Date=row.get("Date", ""),
        Source=row.get("Source", ""),
        Country=row.get("Country", ""),
        Company=row.get("Company", ""),
        Brand=row.get("Brand", "—"),
        Product=row.get("Product", ""),
        Pathogen=row.get("Pathogen", ""),
        Class=row.get("Class", ""),
        Tier=row.get("Tier", ""),
        Outbreak=row.get("Outbreak", ""),
        Reason=row.get("Reason", ""),
        URL=row.get("URL", ""),
        page_text=page_text,
    )
    try:
        raw = _call_gemini(
            prompt=prompt,
            system=claude_check.CLAUDE_CHECK_SYSTEM,
            # Audit 2026-05-11: bumped from 4000 → 8192. The reviewer
            # JSON is short (~30 fields) but on gemini-2.5-flash-lite
            # the model was truncating mid-string at ~350-400 chars
            # during the 2026-05-11 20:00 Athens run (3 rows SKIPped
            # with "Unterminated string" JSON parse errors). The cap
            # had nothing to do with the JSON size — flash-lite was
            # apparently rendering reasoning tokens against the same
            # budget despite responseMimeType=application/json. The
            # _extract_text helper now also detects MAX_TOKENS finish
            # reasons explicitly so the cause is visible in logs.
            max_tokens=8192,
        )
    except Exception as exc:
        log.warning("Gemini call failed: %s: %s",
                    type(exc).__name__, str(exc)[:120])
        return None
    if not raw:
        return None
    try:
        # _clean_response is defensive — Gemini's responseMimeType=
        # application/json should give us clean JSON, but the helper
        # also handles ```json fences (if a future model swap emits
        # them) and <think> blocks (if a future reasoning model is
        # configured via GEMINI_REVIEWER_MODEL env var).
        return json.loads(_clean_response(raw))
    except json.JSONDecodeError as exc:
        log.warning("Gemini JSON parse failed: %s | %s", exc, raw[:200])
        return None


# ============================================================
# Patch claude_check at runtime + override audit tag
# ============================================================
# Override the verifier so claude_check.check_row() goes through Gemini
claude_check._verify_with_claude = _verify_with_gemini

# Override CLAUDE_ENABLED (used as a guard in check_row)
claude_check.CLAUDE_ENABLED = GEMINI_ENABLED

# Override CHECKER_TAG so audit stamps written to Notes read
# "[gemini-check 2026-05-11: pass; ...]" instead of "[claude-check ...]".
# This single override drives BOTH the stamp-writing format string AND
# the prior-stamp regex inside claude_check.main(), AND the cooldown
# regex in _was_recently_verified — so gemini-check re-verifies rows
# that claude-check stamped this morning, and vice versa (independent
# dissent reviewers, as designed).
claude_check.CHECKER_TAG = "gemini-check"

_orig_main = claude_check.main


def main():
    """Run the Gemini dissent review."""
    if not GEMINI_ENABLED:
        log.error("No GEMINI_API_KEY* set in env. Aborting.")
        return 1

    log.info("=" * 60)
    log.info("FSIS Gemini check (dissent reviewer)")
    log.info("Model: %s", GEMINI_MODEL)
    log.info("Audit tag: [%s ...]", claude_check.CHECKER_TAG)
    log.info("=" * 60)

    return _orig_main()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[logging.StreamHandler()],
    )
    sys.exit(main())
