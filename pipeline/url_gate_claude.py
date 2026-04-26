"""
DEPRECATED — pipeline.url_gate_claude has been retired.

Why
---
Claude was pattern-matching from training data and hallucinating recall
URLs (e.g. fabricated RappelConso fiche-rappel IDs 17394, 17399, 17400).
Gemini 2.5 Flash with native Google Search grounding fires real searches,
so every URL it accepts/proposes appeared in a real search result.

What this shim does
-------------------
For backwards compatibility with any pinned cron / external invocation,
this module transparently delegates to `pipeline.url_gate_gemini.main()`.
Any `ANTHROPIC_API_KEY` env var is ignored. Set `GEMINI_API_KEY` (or the
rotation slots `GEMINI_API_KEY_1` … `GEMINI_API_KEY_10`).

Migration
---------
Update callers from:
    python -m pipeline.url_gate_claude
to:
    python -m pipeline.url_gate_gemini

Original Claude implementation preserved at:
    pipeline/url_gate_claude.py.deprecated.bak
"""
from __future__ import annotations
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("url-gate-claude-shim")


def main() -> int:
    log.warning(
        "DEPRECATED: pipeline.url_gate_claude is a shim. "
        "Delegating to pipeline.url_gate_gemini. "
        "Update callers to invoke url_gate_gemini directly."
    )
    from pipeline.url_gate_gemini import main as _gemini_main  # noqa: E402
    return _gemini_main()


if __name__ == "__main__":
    sys.exit(main())
