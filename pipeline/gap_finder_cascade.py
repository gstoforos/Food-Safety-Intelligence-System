"""
Gap-finder cascade — single entry point that tries multiple LLM gap-finders
in priority order, stopping at the first success.

CASCADE ORDER (audit 2026-04-29):
  Call 1   Gemini FREE tier (gemini-2.5-flash + Google Search grounding)
           — €0/run; 250 req/day daily cap; this is the workhorse.
  Call 2   Gemini PAID tier (Tier 1, same code path, different key)
           — €0.10/run; only if free-tier rate-limited or returned no
             usable response.
  Call 3   Claude (claude-haiku-4-5 + web_search_20250305)
           — ~$0.005/run; only if both Gemini calls failed.
  Call 4   OpenAI (gpt-4o-mini-search-preview)
           — ~$0.10/run; final fallback.

"Failed" means an EXCEPTION was raised, the API returned no usable text,
or the JSON parse failed. Returning ZERO recalls is NOT failure — it's a
valid result and the cascade stops.

Per-step timeout: 120 seconds. A hung call rolls forward to the next
provider so the entire cron slot can't get blocked.

CALL-SITE INTEGRATION
---------------------
Replace the FOUR separate workflow YAMLs (gemini-gap-finder.yml,
claude-gap-finder.yml, openai-gap-finder.yml, ...) with ONE workflow
that invokes this script. The orchestrator handles fallbacks internally.
Tavily and Exa stay independent (deterministic search, free, run on
their own dedicated schedule — they're complementary, not in the cascade).

CLI
---
    python -m pipeline.gap_finder_cascade               # default
    python -m pipeline.gap_finder_cascade --skip-paid   # never escalate to paid Gemini
    python -m pipeline.gap_finder_cascade --dry-run     # don't write to Pending
"""
from __future__ import annotations
import argparse
import logging
import os
import signal
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("gap-finder-cascade")


# ─────────────────────────────────────────────────────────────────────────
# Per-step timeout via SIGALRM. POSIX-only; Windows runners (none of our
# GitHub Actions are Windows) silently degrade to no timeout — that's OK
# because Gemini/Claude/OpenAI clients have their own request timeouts.
# ─────────────────────────────────────────────────────────────────────────
class StepTimeout(Exception):
    pass


@contextmanager
def step_timeout(seconds: int):
    """Best-effort POSIX timeout. No-op on Windows."""
    if not hasattr(signal, "SIGALRM"):
        yield
        return

    def _handler(signum, frame):
        raise StepTimeout(f"step exceeded {seconds}s")

    old_handler = signal.signal(signal.SIGALRM, _handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)


# ─────────────────────────────────────────────────────────────────────────
# Provider adapters — each function tries one provider, returns
#   (success, recalls_added_to_pending, message)
# success = True  → cascade stops
# success = False → next provider is tried
# ─────────────────────────────────────────────────────────────────────────

def _try_gemini(force_paid: bool = False) -> Tuple[bool, int, str]:
    """Call gap_finder_gemini.main(). Per the gap-finder's own logic, it
    rotates through GEMINI_API_KEY_FREE first, then paid keys. We just
    call main() and trust the rotation — the key swap happens inside.

    For the cascade's call 2 (PAID escalation), we set
    GAP_GEMINI_DISABLE_FREE=1 so the gap-finder skips the free-tier key
    entirely. This avoids re-trying the same rate-limited key.
    """
    try:
        from pipeline import gap_finder_gemini
    except ImportError as e:
        return False, 0, f"import failed: {e}"

    if force_paid:
        os.environ["GAP_GEMINI_DISABLE_FREE"] = "1"
        log.info("Gemini retry: disabling FREE-tier key (paid tier only)")
    else:
        os.environ.pop("GAP_GEMINI_DISABLE_FREE", None)

    try:
        # gap_finder_gemini.main() returns 0 on success, non-zero on failure.
        # It writes directly to Pending; the cascade just needs the rc.
        rc = gap_finder_gemini.main()
        return (rc == 0), 0, f"main() returned {rc}"
    except Exception as e:  # noqa: BLE001
        return False, 0, f"exception: {type(e).__name__}: {e}"
    finally:
        if force_paid:
            os.environ.pop("GAP_GEMINI_DISABLE_FREE", None)


def _try_claude() -> Tuple[bool, int, str]:
    try:
        from pipeline import gap_finder_claude
    except ImportError as e:
        return False, 0, f"import failed: {e}"
    try:
        rc = gap_finder_claude.main()
        return (rc == 0), 0, f"main() returned {rc}"
    except Exception as e:  # noqa: BLE001
        return False, 0, f"exception: {type(e).__name__}: {e}"


def _try_openai() -> Tuple[bool, int, str]:
    """Call gap_finder_openai.main() — the final fallback. The OpenAI
    finder is OPTIONAL: if the module isn't installed we just skip it
    cleanly rather than crashing the cascade."""
    try:
        from pipeline import gap_finder_openai  # type: ignore
    except ImportError:
        log.info("gap_finder_openai not installed — skipping (OK, optional)")
        return False, 0, "module not present (OPENAI provider optional)"
    if not os.environ.get("OPENAI_API_KEY", "").strip():
        log.info("OPENAI_API_KEY not set — skipping OpenAI fallback")
        return False, 0, "OPENAI_API_KEY not set"
    try:
        rc = gap_finder_openai.main()
        return (rc == 0), 0, f"main() returned {rc}"
    except Exception as e:  # noqa: BLE001
        return False, 0, f"exception: {type(e).__name__}: {e}"


# ─────────────────────────────────────────────────────────────────────────
# Cascade
# ─────────────────────────────────────────────────────────────────────────

CASCADE_TIMEOUT_S = int(os.environ.get("GAP_STEP_TIMEOUT", "180"))


def run_cascade(skip_paid: bool = False, skip_claude: bool = False,
                skip_openai: bool = False) -> int:
    """Returns 0 if any provider succeeded, 1 if all failed."""
    started = datetime.now(timezone.utc).isoformat()
    log.info("=" * 60)
    log.info("Gap-finder cascade started: %s (per-step timeout=%ds)",
             started, CASCADE_TIMEOUT_S)

    steps: List[Tuple[str, Callable[[], Tuple[bool, int, str]]]] = [
        ("Gemini FREE",  lambda: _try_gemini(force_paid=False)),
    ]
    if not skip_paid:
        steps.append(("Gemini PAID", lambda: _try_gemini(force_paid=True)))
    if not skip_claude:
        steps.append(("Claude",      _try_claude))
    if not skip_openai:
        steps.append(("OpenAI",      _try_openai))

    last_msg = ""
    for idx, (name, fn) in enumerate(steps, 1):
        log.info("─" * 50)
        log.info("Cascade call %d/%d: %s", idx, len(steps), name)
        t0 = time.time()
        try:
            with step_timeout(CASCADE_TIMEOUT_S):
                ok, _added, msg = fn()
        except StepTimeout as e:
            ok, msg = False, f"timeout: {e}"
        elapsed = time.time() - t0
        last_msg = msg

        if ok:
            log.info("✓ Cascade success at step %d (%s) in %.1fs — STOP",
                     idx, name, elapsed)
            log.info("  Message: %s", msg)
            return 0
        else:
            log.warning("✗ Step %d (%s) failed in %.1fs — falling through",
                        idx, name, elapsed)
            log.warning("  Reason: %s", msg)

    log.error("All %d cascade providers failed. Last message: %s",
              len(steps), last_msg)
    return 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--skip-paid", action="store_true",
                    help="Don't escalate to paid Gemini if free fails "
                         "(useful in cost-sensitive cron slots).")
    ap.add_argument("--skip-claude", action="store_true",
                    help="Skip the Claude fallback step.")
    ap.add_argument("--skip-openai", action="store_true",
                    help="Skip the OpenAI fallback step.")
    args = ap.parse_args()
    return run_cascade(
        skip_paid=args.skip_paid,
        skip_claude=args.skip_claude,
        skip_openai=args.skip_openai,
    )


if __name__ == "__main__":
    sys.exit(main())
