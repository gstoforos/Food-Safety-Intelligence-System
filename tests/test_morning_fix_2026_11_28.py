# -*- coding: utf-8 -*-
"""Morning fix pass 2026-11-28 — the two code defects found, pinned.

DEFECT 1 — THE ENGLISH DETECTOR COULD NOT SEE 'filet de hareng'.
Fiche 24197 (E.Leclerc, histamine, Tier 2) was published on 2026-11-27 with
the Product 'filet de hareng (conditionné sous-vide)' and NO
"needs cleanup: Product not in English" stamp — only the Reason stamp. The
morning pass reads what is stamped, so an unstamped French Product is
invisible to it; the only reason this one was caught is that the pass also
reads every Product published in the last 26 h by hand.

This is the FOURTH measurement of the same hole ('pâté aux olives'
2026-10-27, 'sandwich jambon beurre' 2026-11-24, 'choux chantilly'
2026-11-26) and it fails for the same reason each time: every word in the
value defeats a different rule. 'hareng' and 'conditionné' were in no list
at all; 'filet' and 'sous vide' are English culinary words; 'de' is
deliberately name-safe; and one accent is under looks_non_english()'s
two-accent floor. Fixed by adding the two French-only words to
_FRENCH_FOOD_WORDS in pipeline/_language.py.

DEFECT 2 — BRAZIL'S DIÁRIO OFICIAL COLLAPSED TO ONE DEDUP KEY.
tests/test_a_query_param_recall_id_is_never_dropped.py was RED on main:

    pesquisa.in.gov.br/imprensa/jsp/visualiza/index.jsp
        ?jornal=515&pagina=106&data=17/11/2026
        ?jornal=515&pagina=197&data=25/11/2026

Two distinct ANVISA resolutions, one dedup key. The DOU serves every page of
every edition off one index.jsp and the identity is the (jornal, pagina,
data) triple, so every Brazilian notice after the first was droppable as a
duplicate in silence. Both URLs are in Weekly_Rejected today, so nothing
published was wrong — this is the fourth recurrence of a bug shape that HAS
cost published rows before (FDA 2026-07-26, CAA/MFDS/Thai FDA 2026-10-01,
USDA FSIS/BeaconBio 2026-10-05), so it is fixed before it costs another.

The same sweep, widened to "a query string ALL of whose params are
stripped", also found szpi.gov.cz/en/docDetail.aspx?docid — SZPI addresses a
document by `docid` alone. It has no collision partner yet (the one URL in
the workbook carries a valueless docid), so no test fires on it; it is fixed
here because the next SZPI docDetail URL would collide with it.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline._language import product_needs_english          # noqa: E402
from pipeline.merge_master import _normalize_url_for_dedup     # noqa: E402

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"


# ── DEFECT 1 ────────────────────────────────────────────────────────────────

def test_the_fiche_24197_product_is_seen_as_french():
    """The exact value published on 2026-11-27, which returned False."""
    assert product_needs_english("filet de hareng (conditionné sous-vide)")


@pytest.mark.parametrize("value", [
    "filet de hareng",
    "filets de hareng fumé",
    "hareng doux conditionné sous-vide",
    "saumon conditionné sous vide",
])
def test_the_two_added_words_fire_on_their_own(value):
    assert product_needs_english(value), value


@pytest.mark.parametrize("value", [
    # The translation this pass wrote, and the shapes it must not flag.
    "Herring fillet (vacuum-packed)",
    "Herring fillet, vacuum-packed, sold at the fish counter",
    # A kept French name inside an English description still passes: that is
    # the whole point of requiring both English vocabularies to be empty.
    "Herring fillet (filet de hareng), vacuum-packed",
    "Smoked herring fillets, 200 g pack",
])
def test_the_english_translations_are_not_flagged(value):
    assert not product_needs_english(value), value


def test_the_addition_flags_nothing_else_in_the_register():
    """The file's own convention: measure the change over every published
    Product and show it fires on the one defective row and nothing else.

    Run against the live workbook, so it keeps holding as rows are added:
    any FUTURE row these two words flag is a row that genuinely needs
    translating, and the morning pass will have translated it — so the only
    values this test may see are English ones.
    """
    openpyxl = pytest.importorskip("openpyxl")
    if not XLSX.exists():
        pytest.skip("workbook not present")
    wb = openpyxl.load_workbook(XLSX, read_only=True, data_only=True)
    ws = wb["Recalls"]
    rows = list(ws.iter_rows(values_only=True))
    hdr = list(rows[0])
    if "Product" not in hdr:
        pytest.skip("no Product column")
    i = hdr.index("Product")
    offenders = [
        str(r[i]) for r in rows[1:]
        if i < len(r) and r[i] and product_needs_english(str(r[i]))
    ]
    assert offenders == [], (
        "published Products that read as non-English:\n  "
        + "\n  ".join(repr(o) for o in offenders[:8]))


# ── DEFECT 2 ────────────────────────────────────────────────────────────────

DOU = "https://pesquisa.in.gov.br/imprensa/jsp/visualiza/index.jsp"


def test_two_dou_editions_do_not_collapse():
    a = _normalize_url_for_dedup(f"{DOU}?jornal=515&pagina=106&data=17/11/2026")
    b = _normalize_url_for_dedup(f"{DOU}?jornal=515&pagina=197&data=25/11/2026")
    assert a != b, "two distinct ANVISA notices share one dedup key"


def test_two_pages_of_one_dou_edition_do_not_collapse():
    """`pagina` alone is the difference on consecutive-page notices."""
    a = _normalize_url_for_dedup(f"{DOU}?jornal=515&pagina=106&data=17/11/2026")
    b = _normalize_url_for_dedup(f"{DOU}?jornal=515&pagina=107&data=17/11/2026")
    assert a != b


def test_the_dou_identity_triple_survives_normalisation():
    key = _normalize_url_for_dedup(
        f"{DOU}?jornal=515&pagina=106&data=17/11/2026&utm_source=news")
    for part in ("jornal=515", "pagina=106", "data=17/11/2026"):
        assert part in key, f"{part} was stripped from {key}"
    assert "utm_source" not in key, "utm_source must still be stripped"


def test_one_dou_url_is_still_its_own_duplicate():
    """The keepers must not break the http/https/www collapse."""
    a = _normalize_url_for_dedup(f"{DOU}?jornal=515&pagina=106&data=17/11/2026")
    b = _normalize_url_for_dedup(
        f"http://www.pesquisa.in.gov.br/imprensa/jsp/visualiza/index.jsp"
        f"?jornal=515&pagina=106&data=17/11/2026")
    assert a == b


def test_szpi_docid_is_identity():
    a = _normalize_url_for_dedup("https://www.szpi.gov.cz/en/docDetail.aspx?docid=1234")
    b = _normalize_url_for_dedup("https://www.szpi.gov.cz/en/docDetail.aspx?docid=5678")
    c = _normalize_url_for_dedup("https://www.szpi.gov.cz/en/docDetail.aspx?docid")
    assert len({a, b, c}) == 3, "SZPI documents share a dedup key"


@pytest.mark.parametrize("host_param", [
    # These keepers are host-scoped and must not leak onto other hosts.
    "https://example.gov/recalls?jornal=515",
    "https://example.gov/recalls?pagina=106",
    "https://example.gov/recalls?data=17/11/2026",
    "https://example.gov/recalls?docid=1234",
])
def test_the_new_keepers_are_host_scoped(host_param):
    assert "?" not in _normalize_url_for_dedup(host_param), (
        f"{host_param} kept a param it should have stripped")
