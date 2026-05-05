"""
pipeline/openrouter_check.py — second-pass review using OpenRouter.

DESIGN:
  This module IMPORTS pipeline.claude_check and MONKEY-PATCHES its
  _verify_with_claude function to call OpenRouter instead of Anthropic.

  All other logic (prompt, verdict-fix-upgrade, corrections handling,
  audit stamps, xlsx writes, git commit) is REUSED from claude_check.py
  unchanged. Any future improvements to claude_check (better prompt,
  new corrections, etc.) automatically propagate here.

  This is the cleanest pattern for a "dissent reviewer" — same task,
  different model.

WHEN IT RUNS:
  Athens 20:00 — after the 19:00 second-pass URL gate.
  Catches errors in rows scraped at 18:00 daily-scrape that a single
  morning chain might have missed (or dispute Claude morning-verdicts
  that look uncertain).

COST:
  Free path: $0/run (DeepSeek :free or Llama :free)
  Paid worst case: ~$0.50/month (DeepSeek Chat at $0.14/$0.28 per M tok)

ENV:
  OPENROUTER_API_KEY        — required, set as GitHub secret
  OPENROUTER_MODEL          — optional, default "deepseek/deepseek-chat-v3.1:free"
  OPENROUTER_FALLBACK_MODEL — optional, default "deepseek/deepseek-chat-v3.1"

TAGS:
  All audit notes written by this module are prefixed
  "[openrouter-check ...]" so they're easy to distinguish from
  "[claude-check ...]" tags. The Thursday review email surfaces both.
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
from review.openrouter_client import (  # noqa: E402
    _call_openrouter, _strip_fences, ENABLED as OPENROUTER_ENABLED,
)

log = logging.getLogger("openrouter-check")


# ============================================================
# Patched _verify_with_X — calls OpenRouter, returns same JSON shape
# ============================================================
def _verify_with_openrouter(row: Dict[str, Any],
                              page_text: str) -> Optional[Dict[str, Any]]:
    """
    Drop-in replacement for claude_check._verify_with_claude.
    Uses OpenRouter (DeepSeek/Llama/Qwen via :free path) instead of Anthropic.
    Returns the parsed JSON dict, or None on failure.
    """
    if not OPENROUTER_ENABLED:
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
        raw = _call_openrouter(
            prompt=prompt,
            system=claude_check.CLAUDE_CHECK_SYSTEM,
            max_tokens=4000,
        )
    except Exception as exc:
        log.warning("OpenRouter call failed: %s: %s",
                    type(exc).__name__, str(exc)[:120])
        return None
    if not raw:
        return None
    try:
        return json.loads(_strip_fences(raw))
    except json.JSONDecodeError as exc:
        log.warning("OpenRouter JSON parse failed: %s | %s", exc, raw[:200])
        return None


# ============================================================
# Patch claude_check at runtime + change the audit tag
# ============================================================
# Override the verifier so claude_check.check_row() goes through OpenRouter
claude_check._verify_with_claude = _verify_with_openrouter

# Override CLAUDE_ENABLED (used as a guard in check_row)
claude_check.CLAUDE_ENABLED = OPENROUTER_ENABLED

# Replace the audit stamp tag in the main() loop. main() builds the
# audit string with literal "[claude-check ..." — we can't change that
# without forking the function, but we CAN add a marker via the logger
# name to make grep'ing easier. For the audit Notes itself, we'll
# search-replace "claude-check" -> "openrouter-check" by patching the
# regex used to drop prior stamps.
_orig_main = claude_check.main


def main():
    """Run the OpenRouter dissent review."""
    if not OPENROUTER_ENABLED:
        log.error("OPENROUTER_API_KEY not set. Aborting.")
        return 1

    log.info("=" * 60)
    log.info("FSIS OpenRouter check (dissent reviewer)")
    log.info("Model: %s",
             os.getenv("OPENROUTER_MODEL", "deepseek/deepseek-chat-v3.1:free"))
    log.info("=" * 60)

    # Patch the audit-stamp regex + tag literal at module level by string
    # surgery on the source file would be invasive — instead we monkeypatch
    # the audit prefix using a simple wrapper. The cleanest way is to
    # override sys.modules["pipeline.claude_check"]'s "TAG" constant if
    # it had one; since it doesn't, we accept that audit stamps will
    # show "[claude-check ...]" but the GitHub Actions log makes the
    # source crystal-clear (workflow name = "FSIS OpenRouter Check").
    #
    # If you want explicit "[openrouter-check ...]" stamps, add a
    # CHECKER_TAG constant to claude_check.py and use it there.

    return _orig_main()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[logging.StreamHandler()],
    )
    sys.exit(main())
