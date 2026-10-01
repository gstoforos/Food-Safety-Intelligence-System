"""Every subscriber page opens behind the sign-in (operator 2026-10-01).

    "visitor not signed must see the sign in message and blur"

docs/fsis-gate.js blurs the page and shows the sign-in box until the Google
script (FsisAccess.gs) accepts the session. It is loaded from <head> so
nothing paints unblurred. The generators add it through pipeline/_gate.py.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
sys.path.insert(0, str(ROOT))

from pipeline._gate import GATE_TAG, add_gate  # noqa: E402

#: Public on purpose: the free preview and the Wix hub of monthly cards.
PUBLIC = {"index-promo.html", "hub.html"}
#: Operator / methodology pages rebuilt by other tools, not subscriber content.
PUBLIC_DIRS = ("data", "reports", "marketing", "social", "audits")


def _subscriber_pages():
    for p in sorted(DOCS.rglob("*.html")):
        rel = p.relative_to(DOCS)
        if rel.as_posix() in PUBLIC or rel.parts[0] in PUBLIC_DIRS:
            continue
        yield rel.as_posix(), p


def test_every_subscriber_page_carries_the_gate_in_head():
    missing, late = [], []
    for rel, p in _subscriber_pages():
        html = p.read_text(encoding="utf-8", errors="replace")
        if "fsis-gate.js" not in html:
            missing.append(rel)
            continue
        head_end = re.search(r"</head>", html, re.I)
        if head_end and html.find("fsis-gate.js") > head_end.start():
            late.append(rel)
    assert not missing, f"{len(missing)} subscriber page(s) open without sign-in: {missing[:8]}"
    assert not late, f"gate loaded after <head> (page paints unblurred first): {late[:8]}"


def test_the_public_pages_stay_public():
    for name in PUBLIC:
        p = DOCS / name
        if p.exists():
            assert "fsis-gate.js" not in p.read_text(encoding="utf-8"), name


def test_the_gate_script_exists_and_points_at_the_router():
    js = (DOCS / "fsis-gate.js").read_text(encoding="utf-8")
    assert "fsis_signin" in js and "fsis_check" in js and "fsis_recover" in js
    alerts = (DOCS / "alerts.html").read_text(encoding="utf-8")
    url = re.search(r"https://script\.google\.com/macros/s/[A-Za-z0-9_-]+/exec", js).group(0)
    assert url in alerts, "fsis-gate.js must call the same deployment alerts.html uses (Router.gs)"
    assert "history.replaceState(null, '', '/')" in js, "the report URL must be cut back to the root"


def test_add_gate_is_idempotent_and_goes_first_in_head():
    html = "<!DOCTYPE html><html><head><title>x</title></head><body>y</body></html>"
    once = add_gate(html)
    assert add_gate(once) == once
    assert once.index(GATE_TAG) < once.index("<title>")


@pytest.mark.parametrize("path,needle", [
    ("docs/build_monthly_report_afts.py", "_add_gate(html)"),
    ("docs/build_monthly_report_afts.py", "_add_gate(all_html)"),
    ("docs/build_weekly_report_afts.py", "_gate_html(html)"),
    ("pipeline/daily_recall_search.py", "add_gate(html)"),
])
def test_every_generator_writes_the_gate(path, needle):
    assert needle in (ROOT / path).read_text(encoding="utf-8")
