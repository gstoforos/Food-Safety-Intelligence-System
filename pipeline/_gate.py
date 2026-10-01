"""The subscriber sign-in tag every subscriber page carries (2026-10-01).

    "visitor not signed must see the sign in message and blur" — operator

docs/fsis-gate.js blurs the page and shows the sign-in box until the Google
script (FsisAccess.gs) accepts the session. It must be in <head>, before the
body paints, so the page never flashes unblurred.

Every generator that writes a subscriber page calls add_gate() on the HTML it
writes: the monthly report and its -all companion, the weekly report, the
daily brief. Hand-maintained pages (index.html, signals.html, alerts.html)
carry the tag directly. tests/test_subscriber_pages_are_gated.py fails if a
page under docs/ is missing it, and lists the pages that are public on
purpose (the preview, the hub, the marketing material).
"""
from __future__ import annotations

import re

GATE_TAG = '<script src="/fsis-gate.js"></script>'

_HEAD = re.compile(r"<head(\s[^>]*)?>", re.IGNORECASE)


def add_gate(html: str) -> str:
    """Insert the sign-in tag right after <head>. Idempotent."""
    if "fsis-gate.js" in html:
        return html
    m = _HEAD.search(html)
    if not m:
        raise ValueError("add_gate: no <head> in a subscriber page")
    return html[:m.end()] + "\n" + GATE_TAG + html[m.end():]
