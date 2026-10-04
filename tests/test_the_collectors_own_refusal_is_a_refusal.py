""""no-authority-url" is a not-found claim, whoever wrote it.

WHY THIS EXISTS (2026-10-04)
============================
_url_guard._NOT_FOUND_REASON decides whether an archived rejection was a
statement about REACHABILITY (which merge_master may look past when the row
holds a URL on the authority's own domain and passes the full publish gate)
or a verdict on the recall (permanent). Every alternative in it required a
past participle — "found", "located", "available", "identified" — or a
"could not / unable to" opener.

Three extractors write neither. pipeline/extractor.py,
pipeline/gap_finder/extractor.py and pipeline/official_feeds/extractor.py
all emit, for every country in the fleet:

    "no-authority-url: no official {AUTHORITY} press-release URL in the
     source article (news-only discovery)"

so the pattern missed the refusal the pipeline produces most. Measured on
the 2026-10-04 workbook: 535 archive rows carry it, 42 of them while
HOLDING a URL on the authority's own host, and all 42 were barred for ever.

The clause is also specifically about where the collector LOOKED — "in the
source article" — not about whether the notice exists. Italy is the
clearest case: the Ministero della Salute publishes recalls as "cartello di
richiamo" / "modulo richiamo" PDFs under
salute.gov.it/.../avvisi_sicurezza_alimentare/ and not as press releases at
all, so the thing the collector went looking for does not exist for that
regulator while the notice itself plainly does. Eight rows were already
published on exactly that URL shape.

THE ROW THAT FOUND IT. Salumificio F.lli Costantini, salame stagionato al
tartufo, Listeria monocytogenes, Tier 1, Ministero della Salute notice of
2026-09-22 — barred twelve days while holding the ministry's own PDF.

WHAT THIS MAY NOT COST. A content verdict must still pass straight through,
and the two brakes on the far side are untouched: merge_master still
requires the row to pass the FULL publish gate, which is what keeps a
regulator LISTING page out (publish-gate rule 6).
"""
from __future__ import annotations

import pytest

from pipeline._url_guard import _NOT_FOUND_REASON, reject_refusal

SALUTE_PDF = ("https://www.salute.gov.it/new/sites/default/files/"
              "external_data/avvisi_sicurezza_alimentare/"
              "Modulo%20richiamo%20conforme_1790069655.pdf")

ROW = {
    "Date": "2026-09-22", "Source": "Ministero della Salute (IT)",
    "Company": "Salumificio F.lli Costantini Srl",
    "Product": "Aged salami with truffle",
    "Pathogen": "Listeria monocytogenes",
    "Reason": "Presence of Listeria monocytogenes",
    "Class": "Recall", "Country": "Italy", "Region": "Europe",
    "Tier": 1, "Outbreak": 0, "URL": SALUTE_PDF,
}

COLLECTOR_REFUSALS = [
    "no-authority-url: no official Salute press-release URL in the source "
    "article (news-only discovery)",
    "no-authority-url: no official NÉBIH press-release URL in the source "
    "article (news-only discovery)",
    "no-authority-url: no official AGES press-release URL",
    "URL agent: No official regulator page found for this recall",
    "could not confirm official recall: Unable to find the official "
    "regulator URL through the search query",
    "HTTP 403 error on official regulator page",
    "Official URL is unreachable (bot_wall)",
]

CONTENT_VERDICTS = [
    "unknown: No matching hazard category — defer to manual review.",
    "pet_food_out_of_scope — AFTS-FSIS is a HUMAN-food register. Product "
    "reads 'Chicken Chips for Dogs (6 oz, lot 24045) - PET FOOD'.",
    "Undeclared allergen — out of scope (Rule B reject).",
    "out_of_scope_import_reinspection",
    "out_of_scope_not_food — a compounded sterile injectable",
    "Confirmer: row was at pending_gap_v2 (not reviewed by reviewer 2) and "
    "Date is empty",
    "duplicate of RASFF notification 874035",
    "REJECTED: labelling — Capri-Sun Orange multipacks",
    "no illnesses reported in the notice",
    "URL agent: Hazard is undeclared allergen (gluten, arachide, moutarde) "
    "not a microbial pathogen",
]


@pytest.mark.parametrize("why", COLLECTOR_REFUSALS)
def test_a_collector_refusal_is_recognised(why):
    assert _NOT_FOUND_REASON.search(why), why


@pytest.mark.parametrize("why", CONTENT_VERDICTS)
def test_a_content_verdict_is_not_a_reachability_refusal(why):
    assert not _NOT_FOUND_REASON.search(why), (
        f"{why!r} is a verdict on the recall and must stay permanent")
    assert not reject_refusal(ROW, why), why


def test_the_row_that_found_this_is_no_longer_barred():
    why = ("no-authority-url: no official Salute press-release URL in the "
           "source article (news-only discovery) [backfilled 2026-09-23; "
           "writer gap_finder/it/rules.py wrote it to RejectReason, a column "
           "this sheet does not have]")
    excuse = reject_refusal(ROW, why)
    assert excuse, (
        "a Tier-1 Listeria recall holding the Ministero della Salute's own "
        "PDF was barred for ever on a claim about what a news article "
        "contained")
    assert "reachability" in excuse


def test_a_news_host_still_proves_nothing():
    row = dict(ROW, URL="https://ilfattoalimentare.it/richiamo-salame.html")
    assert not reject_refusal(row, COLLECTOR_REFUSALS[0])


def test_a_regulator_listing_page_is_still_refused_downstream():
    """The reachability excuse is not a pass — the publish gate still runs."""
    from pipeline._publish_gate import publish_blockers
    row = dict(ROW, URL="https://nafdac.gov.ng/category/recalls-and-alerts/")
    assert reject_refusal(row, COLLECTOR_REFUSALS[0]), (
        "the host is an authority, so the excuse applies")
    assert publish_blockers(row), (
        "but a category listing page is not a recall notice and the publish "
        "gate must still refuse it — that is the second brake")
