"""
DEPRECATED — pipeline.url_gate_claude has been retired.

The URL gate moved to Gemini 2.5 Flash with native Google Search grounding,
because Claude (without grounded search) was pattern-matching URLs from
training data and producing non-existent recall pages.

For backwards compatibility with any pinned external invocation, this module
delegates to `pipeline.url_gate_gemini.main()`. ANTHROPIC_API_KEY is ignored;
configure GEMINI_API_KEY (or rotation slots GEMINI_API_KEY_1 … _10) instead.

Migration:
    python -m pipeline.url_gate_claude   →   python -m pipeline.url_gate_gemini

The original Claude-based implementation is archived at:
    pipeline/url_gate_claude.py.deprecated
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
