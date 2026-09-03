#!/usr/bin/env python3
"""
build_signals_page.py — refresh the inline snapshot in docs/signals.html
=======================================================================

docs/signals.html is the full-page view of the aberration detector. When it
is served from the site it fetches docs/data/signals-board.json live. It also
carries an inline copy of that JSON in

    <script id="sg-data" type="application/json">…</script>

so the file renders when opened directly — from a download, an email
attachment, or a file:// URL where fetch() is not allowed. This script
replaces the contents of that block with the current board. It touches
nothing else in the page.

Run after pipeline.signal_detector --board:

    python -m pipeline.build_signals_page
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PAGE = ROOT / "docs" / "signals.html"
BOARD = ROOT / "docs" / "data" / "signals-board.json"
REVIEW = ROOT / "docs" / "data" / "signals-review.json"

_BLOCK = re.compile(
    r'(<script id="sg-data" type="application/json">)(.*?)(</script>)',
    re.S,
)
_RBLOCK = re.compile(
    r'(<script id="sg-review" type="application/json">)(.*?)(</script>)',
    re.S,
)


def main() -> int:
    if not BOARD.exists():
        print(f"no board at {BOARD}; run pipeline.signal_detector --board first")
        return 2
    data = json.loads(BOARD.read_text(encoding="utf-8"))
    # "</script>" inside a JSON string would end the block early. There is
    # none in the detector's output, but a note field is free text, so escape
    # the sequence anyway; the browser's JSON.parse sees the original.
    inline = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    inline = inline.replace("</", "<\\/")
    html = PAGE.read_text(encoding="utf-8")
    if not _BLOCK.search(html):
        print("sg-data block not found in docs/signals.html")
        return 3
    new = _BLOCK.sub(lambda m: m.group(1) + inline + m.group(3), html, count=1)
    if REVIEW.exists() and _RBLOCK.search(new):
        rv = json.dumps(json.loads(REVIEW.read_text(encoding="utf-8")),
                        ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")
        new = _RBLOCK.sub(lambda m: m.group(1) + rv + m.group(3), new, count=1)
    if new == html:
        print("signals.html snapshot already current")
        return 0
    PAGE.write_text(new, encoding="utf-8")
    print(f"signals.html snapshot refreshed: week {data.get('meta', {}).get('week')}, "
          f"{len(inline):,} bytes inline")
    return 0


if __name__ == "__main__":
    sys.exit(main())
