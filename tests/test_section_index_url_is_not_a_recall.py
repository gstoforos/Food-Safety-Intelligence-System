# -*- coding: utf-8 -*-
"""A regulator's own SECTION INDEX is never a recall notice.

MORNING FIX 2026-11-23
----------------------
At 00:14 UTC the DDGS gap finder queued, and the offline promoter published
into Recalls at 04:27 UTC, this row:

    Date      2026-11-13
    Source    Ministero della Salute (IT)
    Company   'Ministero della Salute'
    Product   'Norme e atti'
    Pathogen  'Listeria monocytogenes'
    Tier      1
    URL       https://www.salute.gov.it/new/it/sezione/norme-e-atti/

"Norme e atti" is Italian for "Rules and acts" — the ministry's LEGISLATION
section index. The document its Notes quote is guidance on applying
Reg. (EC) 2073/2005 to Listeria in ready-to-eat foods: a law, not a recall.
R2 (a real regulatory action) and R3 (the regulator's own per-recall URL)
both fail, and the row reached Tier 1 of the public register and week 46.

The four generic-URL lists already knew this shape in Spanish
(/subseccion/<x>.htm) and on produktwarnung.eu (/rubrik/<x>/). The Italian,
Portuguese, German, Polish, Croatian and English spellings were missing —
nafdac.gov.ng/category/recalls-and-alerts/ is the same page in English and
sits in Rejected four times over, caught further downstream each time.

This test holds all four lists, because a pattern that lives in one of them
is a pattern three gates do not have.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


#: The row that reached the register, plus the same page in other languages.
SECTION_INDEXES = [
    "https://www.salute.gov.it/new/it/sezione/norme-e-atti/",
    "https://www.salute.gov.it/new/it/sezione/norme-e-atti",
    "https://nafdac.gov.ng/category/recalls-and-alerts/",
    "https://thencc.org.za/category/product-recalls/",
    "https://www.hapih.hr/kategorija/obavijesti-za-potrosace/",
    "https://example.gov/seccion/alertas-alimentarias/",
    "https://example.gov/sección/alertas-alimentarias/",
    "https://example.gov/seção/alertas/",
    "https://example.gov/kategorie/lebensmittel/",
    "https://example.gov/rubrique/alertes/",
]

#: Real per-recall notices. A gate that refuses these is worse than the bug.
REAL_NOTICES = [
    "https://rappel.conso.gouv.fr/fiche-rappel/24125/interne",
    "https://www.ages.at/mensch/produktwarnungen-produktrueckrufe/produkt/"
    "gurkerl-yutto-pinienkerne-50-g",
    "https://www.aesan.gob.es/alertas/2026_87",
    "https://alerts.food.gov.uk/news-alerts/alert/fsa-prin-54-2026",
    "https://www.salute.gov.it/portale/richiami/dettaglioRichiamo.jsp?id=23769",
    "https://recalls-rappels.canada.ca/en/alert-recall/"
    "cadbury-brand-dairy-milk-oreo-candy-treat-size-bar-recalled-due-"
    "undeclared-pistachio",
    "https://www.recall.caa.go.jp/result/detail.php?rcl=00000036166&screenkbn=01",
    "https://www.foodstandards.gov.au/food-recalls/recall-alert/"
    "woodbridge-smokehouse-smoked-salmon-spread-150g",
    "https://www.mattilsynet.no/tilbakekallinger/hewad-mohmand-kaller-tilbake-"
    "rosiner-etter-funn-av-plantevernmidler-i-produktet",
    "https://www.lebensmittelwarnung.de/___lebensmittelwarnung.de/Meldungen/"
    "2026/11_November/261120_21_NI_Wildsalami/261120_21_NI_Wildsalami.html",
]


def _callers():
    """(name, predicate) for every gate that owns a generic-URL list."""
    from pipeline._url_filters import is_generic_url as f_filters
    from pipeline._gap_finder_guards import is_generic_url as f_guards
    from review.url_validator import is_generic_url as f_review
    from pipeline.merge_master import _GENERIC_URL_PATTERNS as MM

    def f_merge(url: str) -> bool:
        return any(re.search(p, url, re.IGNORECASE) for p in MM)

    return [
        ("pipeline._url_filters", f_filters),
        ("pipeline._gap_finder_guards", f_guards),
        ("review.url_validator", f_review),
        ("pipeline.merge_master", f_merge),
    ]


@pytest.mark.parametrize("url", SECTION_INDEXES)
def test_every_gate_refuses_a_section_index(url):
    missed = [name for name, fn in _callers() if not fn(url)]
    assert not missed, (
        "%s is a regulator section index, not a recall notice, but these "
        "gates would let it through: %s" % (url, missed))


@pytest.mark.parametrize("url", REAL_NOTICES)
def test_no_gate_refuses_a_real_per_recall_notice(url):
    wrong = [name for name, fn in _callers() if fn(url)]
    assert not wrong, (
        "%s is a real per-recall notice but these gates call it generic: %s"
        % (url, wrong))


#: Published rows whose URL the gates already call generic, which this pass
#: did NOT remove, and why. The set may only ever SHRINK: a new entry means a
#: listing page reached the register again.
#:
#: beaconbio.org — Recalls row 2265, Source "BeaconBio (TW)", a 2026-04-06
#: Salmonella outbreak cluster (157 cases, Kaohsiung). Its own Notes have
#: carried a reviewer's flag since June: "beaconbio.org IS on the locked
#: news_mirror_blocklist — pipeline gate failure; consider DROP from Recalls
#: or swap to TFDA Taiwan regulator URL if available". The URL is page 2 of
#: an aggregator's report listing, so R3 fails. It is left in place on
#: 2026-11-23 because removing it changes week 15 and the CLOSED month
#: 2026-M04, whose report tools.rebase_and_verify reports but does not
#: rebuild — the swap-or-drop decision is the operator's, not this pass's.
#: salute.gov.it/new/it/avvisi/avvisi-e-richiami-di-prodotti-alimentari —
#: Recalls row 2163, 2026-04-14, La Vacje Salumerie Snc, "Salame Nostrano
#: (~800 g, lot L6CCTD)", Listeria monocytogenes, Tier 1. A real recall; the
#: URL is the ministry's LISTING index, put there by a "URL FIX 2026-05-01"
#: that swapped a news-mirror URL (ilfattoalimentare.it) for the portal
#: index because the specific avviso PDF could not be reached. The index
#: joined KNOWN_REGULATOR_LANDINGS today so the Italian gap finder stops
#: re-queueing it into the archives every week; this row then shows up here,
#: which is the point — it was always wrong and was never visible. It is NOT
#: removed on 2026-11-23: the recall is genuine, the month 2026-M04 is
#: closed and tools.rebase_and_verify reports closed months without
#: rebuilding them, and the real repair is the ministry's own avviso URL,
#: which this sandbox cannot fetch (salute.gov.it answers with a Gcore
#: JavaScript challenge). Swap-or-drop is the operator's call.
KNOWN_UNFIXED = (
    "beaconbio.org/en/report/",
    "salute.gov.it/new/it/avvisi/avvisi-e-richiami-di-prodotti-alimentari",
)


def test_no_published_row_cites_a_section_index():
    """The register itself must hold no NEW section-index URL."""
    openpyxl = pytest.importorskip("openpyxl")
    xlsx = ROOT / "docs" / "data" / "recalls.xlsx"
    if not xlsx.exists():                                # pragma: no cover
        pytest.skip("workbook not present in this checkout")
    from pipeline._url_filters import is_generic_url

    wb = openpyxl.load_workbook(xlsx, read_only=True, data_only=True)
    try:
        offenders = []
        for sheet in ("Recalls", "Weekly_Review"):
            if sheet not in wb.sheetnames:
                continue
            ws = wb[sheet]
            rows = ws.iter_rows(values_only=True)
            head = [str(c) for c in next(rows)]
            if "URL" not in head:
                continue
            iu = head.index("URL")
            for n, row in enumerate(rows, start=2):
                url = str(row[iu] or "") if iu < len(row) else ""
                if not url or not is_generic_url(url):
                    continue
                if any(k in url for k in KNOWN_UNFIXED):
                    continue
                offenders.append("%s r%d: %s" % (sheet, n, url))
    finally:
        wb.close()
    assert not offenders, (
        "published rows whose URL is a listing / section index, not a "
        "per-recall notice: %s\nIf one of these is a deliberate exception, "
        "it belongs in KNOWN_UNFIXED above with the reason." % offenders)


def test_known_unfixed_entries_are_still_present():
    """An exception that no longer matches a row must be deleted, not kept."""
    openpyxl = pytest.importorskip("openpyxl")
    xlsx = ROOT / "docs" / "data" / "recalls.xlsx"
    if not xlsx.exists():                                # pragma: no cover
        pytest.skip("workbook not present in this checkout")
    wb = openpyxl.load_workbook(xlsx, read_only=True, data_only=True)
    try:
        ws = wb["Recalls"]
        rows = ws.iter_rows(values_only=True)
        head = [str(c) for c in next(rows)]
        iu = head.index("URL")
        urls = [str(r[iu] or "") for r in rows if iu < len(r)]
    finally:
        wb.close()
    stale = [k for k in KNOWN_UNFIXED if not any(k in u for u in urls)]
    assert not stale, (
        "KNOWN_UNFIXED names rows that are no longer published — remove "
        "these entries: %s" % stale)
