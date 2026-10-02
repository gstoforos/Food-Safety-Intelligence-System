"""Every subscriber page loads fsis-gate.js (operator 2026-10-01).

The dashboard (index) asks for sign-in; every other page only has its
address cut back to the root so the report link is never shared.

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
    # Only the dashboard asks for sign-in (operator 2026-10-01): reports open
    # straight away and only have their address cut back.
    assert "var IS_INDEX" in js and "if (!IS_INDEX) return;" in js
    assert "if (IS_INDEX) root.classList.add('fsis-locked');" in js
    assert "fsis-remember" in js, "Remember me keeps name, email and token on the device"
    assert "fsis_signout" in js and "id=\"fsis-signout\"" in js, "Sign out at the top of the dashboard"


def test_the_preview_ends_on_the_plans():
    """The preview runs inside Wix, which it cannot navigate: when the 30 s
    countdown ends it must SHOW the plans, with their checkout links."""
    html = (DOCS / "index-promo.html").read_text(encoding="utf-8")
    assert "_promoShowPlans();" in html and "function _promoShowPlans()" in html
    assert html.count("pricing-plans/checkout-1") >= 2


def test_the_register_is_a_google_sheet_not_a_download():
    """Operator 2026-10-02: "Sheet only". No page builds a file of the
    register any more; the dashboard opens the view-only Google Sheet whose
    link the sign-in returns (FsisRegisterSheet.gs)."""
    for page in ("index.html", "index-promo.html"):
        html = (DOCS / page).read_text(encoding="utf-8")
        assert "function downloadRecallsXlsx" not in html, page
        assert "function downloadRecallsJson" not in html, page
        assert "XLSX.writeFile" not in html, page
        assert 'onclick="openRegisterSheet()">▦ SHEET</button>' in html, page
    html = (DOCS / "index.html").read_text(encoding="utf-8")
    assert "function openRegisterSheet()" in html
    js = (DOCS / "fsis-gate.js").read_text(encoding="utf-8")
    assert js.count("sheet: r.sheet || ''") == 2 and "if (r.sheet) s.sheet = r.sheet;" in js


def test_remember_me_keeps_the_token_through_sign_out():
    """Operator 2026-10-01: "Remember me does not remember the token". Sign
    out used to wipe it. It now keeps it and only stops the automatic
    sign-in; the token field is a password field so the browser's password
    manager saves it too (survives blocked storage in the Wix frame)."""
    js = (DOCS / "fsis-gate.js").read_text(encoding="utf-8")
    assert "rem.token = ''" not in js
    assert "rem.signedOut = true" in js and "rem.signedOut) { showSignin(); return; }" in js
    assert 'autocomplete="current-password"' in js


def test_the_dashboard_filters_by_country():
    html = (DOCS / "index.html").read_text(encoding="utf-8")
    assert '<select id="f-ctry"><option value="">All Countries</option></select>' in html
    assert "if(ctry&&r.country!==ctry)return false;" in html


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
