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


# ─────────────────────────────────────────────────────────────────────────
# Pending-row-count helper (audit 2026-05-06).
#
# The cascade's success rule used to be just `rc == 0`. That treated a
# legitimate "Gemini ran but found zero new recalls" as success and stopped
# the cascade, never trying Claude/OpenAI as a backup. Production data
# (2026-05-04 → 2026-05-06) shows this misses real fresh recalls regularly
# — Gemini's grounded search returns [] when its search budget is
# exhausted or when Google hasn't yet indexed the recall page.
#
# The new rule:
#     rc == 0 AND added > 0  → real success, stop
#     rc == 0 AND added == 0 → fall-through (try next provider)
#     rc != 0                → fall-through (existing behavior)
#
# Empty result with EVERY cascade step failing is logged loudly so
# operators can tell "nothing new today" apart from "all providers
# silently no-op'd".
# ─────────────────────────────────────────────────────────────────────────
_XLSX_PATH = Path(os.environ.get(
    "FSIS_XLSX_PATH",
    str(Path(__file__).resolve().parent.parent / "docs" / "data" / "recalls.xlsx"),
))


def _pending_count() -> int:
    """Read the current Pending sheet row count. Used to detect whether a
    cascade step actually wrote anything new. Resilient to xlsx-missing
    (returns 0) and to load failure (returns -1, caller treats as 'unknown').
    """
    try:
        if not _XLSX_PATH.exists():
            return 0
        import openpyxl
        wb = openpyxl.load_workbook(_XLSX_PATH, read_only=True, data_only=True)
        if "Pending" not in wb.sheetnames:
            wb.close()
            return 0
        ws = wb["Pending"]
        # max_row counts header row too — subtract 1
        n = max(0, ws.max_row - 1)
        wb.close()
        return n
    except Exception as e:
        log.warning("_pending_count: failed to read xlsx (%s) — "
                    "treating as unknown", e)
        return -1


def run_cascade(skip_paid: bool = False, skip_claude: bool = False,
                skip_openai: bool = False) -> int:
    """Run the cascade. Returns 0 if any provider added rows, 1 otherwise.

    Audit 2026-05-06 — success rule tightened. Pre-fix: rc==0 stopped
    the cascade unconditionally, even when Gemini "succeeded" with zero
    rows. Post-fix: a step counts as success ONLY if it (a) returned
    rc==0 AND (b) actually wrote rows to Pending. Empty-result success
    falls through to the next provider.
    """
    started = datetime.now(timezone.utc).isoformat()
    log.info("=" * 60)
    log.info("Gap-finder cascade started: %s (per-step timeout=%ds)",
             started, CASCADE_TIMEOUT_S)

    steps: List[Tuple[str, Callable[[], Tuple[bool, int, str]]]] = [
        ("Gemini (free→paid)", lambda: _try_gemini(force_paid=False)),
    ]
    if not skip_paid:
        steps.append(("Gemini (paid only)", lambda: _try_gemini(force_paid=True)))
    if not skip_claude:
        steps.append(("Claude",      _try_claude))
    if not skip_openai:
        steps.append(("OpenAI",      _try_openai))

    # Snapshot Pending count BEFORE the cascade — every per-step delta is
    # measured against this. If a step fails (rc!=0 or exception), Pending
    # is unchanged and the next step still measures from the original
    # baseline. If a step succeeds with rows, the next step's "added"
    # measures only its own contribution.
    initial_pending = _pending_count()
    log.info("Cascade baseline: Pending has %d rows", initial_pending)

    last_msg = ""
    total_added = 0
    cascade_summary: List[str] = []  # provider-by-provider for final log

    for idx, (name, fn) in enumerate(steps, 1):
        log.info("─" * 50)
        log.info("Cascade call %d/%d: %s", idx, len(steps), name)
        before = _pending_count()
        t0 = time.time()
        try:
            with step_timeout(CASCADE_TIMEOUT_S):
                ok, _added, msg = fn()
        except StepTimeout as e:
            ok, msg = False, f"timeout: {e}"
        elapsed = time.time() - t0
        last_msg = msg
        after = _pending_count()
        # added = how many rows this step contributed. -1 means we couldn't
        # measure (e.g. xlsx load failed); treat as 0 for accounting but
        # log a warning so operators know the metric is unreliable.
        if before < 0 or after < 0:
            added_this_step = 0
            log.warning("Cascade row-count metric unreliable (before=%d after=%d)",
                        before, after)
        else:
            added_this_step = max(0, after - before)
        total_added += added_this_step

        # Step-level success classification:
        #   rc==0 AND added>0  → STOP (real success)
        #   rc==0 AND added==0 → fall through (empty-result; try next)
        #   rc!=0              → fall through (failure)
        if ok and added_this_step > 0:
            log.info("✓ Cascade success at step %d (%s) in %.1fs — STOP",
                     idx, name, elapsed)
            log.info("  Message: %s", msg)
            log.info("  Added: %d new rows to Pending", added_this_step)
            cascade_summary.append(f"{name}=+{added_this_step}")
            log.info("Cascade summary: %s | total +%d rows",
                     " → ".join(cascade_summary), total_added)
            return 0
        elif ok and added_this_step == 0:
            log.warning("⊘ Step %d (%s) ran successfully in %.1fs but added "
                        "ZERO rows — falling through to next provider",
                        idx, name, elapsed)
            log.warning("  Message: %s", msg)
            cascade_summary.append(f"{name}=0(empty)")
        else:
            log.warning("✗ Step %d (%s) failed in %.1fs — falling through",
                        idx, name, elapsed)
            log.warning("  Reason: %s", msg)
            cascade_summary.append(f"{name}=fail")

    # All steps exhausted. Determine outcome:
    if total_added > 0:
        # Some intermediate step added rows but its rc said 'success';
        # subsequent steps still ran. This shouldn't happen with the
        # current logic (a successful step returns immediately) but is
        # defensible if a step writes rows then returns rc!=0 by accident.
        log.info("✓ Cascade completed with +%d total rows (across %d steps)",
                 total_added, len(steps))
        log.info("Cascade summary: %s", " → ".join(cascade_summary))
        return 0
    log.error("All %d cascade providers ran but added ZERO rows total.",
              len(steps))
    log.error("Cascade summary: %s", " → ".join(cascade_summary))
    log.error("Last message: %s", last_msg)
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
