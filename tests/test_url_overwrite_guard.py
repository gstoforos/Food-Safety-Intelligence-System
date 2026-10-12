# -*- coding: utf-8 -*-
"""Reviewer 1 may not replace a regulator's own href with its own guess.

THE INCIDENT (2026-09-18)
-------------------------
A fabricated URL was published and emailed to a subscriber.

FSAI truncates its slugs. ``pipeline/official_feeds/sources/ireland.py``
took the real href out of FSAI's listing markup —

    /news-and-alerts/food-alerts/recall-of-specific-batch-of-various-macroom-buffal

— and stamped the identical slug into Notes as ``source_id=FSAI-<slug>``.
Reviewer 1 was then asked for "the confirmed official regulator URL". It
found the page, read the full TITLE, and wrote a slug from the title
rather than copying the href:

    …macroom-buffalo-cheese-products-due-to-the-presence-of-listeria-monocytogenes/

The real slug plus 63 characters of headline. Well-formed, plausible, and
not a page — FSAI serves the alerts LISTING for it, which is the general
page the subscriber landed on.

Every defence passed. ``row["URL"] = res["official_url"]`` was
unconditional. The provenance check then fetched the fabricated URL,
landed on the listing, and found "macroom" and "buffalo" in it, because a
listing page names every alert it links to. A listing corroborated a row
it was not about.

WHAT IS ASSERTED HERE
---------------------
A URL the collector read out of a regulator's markup is evidence; the
model's opinion of what it should look like is not. Reviewer 1 still
confirms, rejects and enriches — it just stops rewriting hrefs it did not
find. Gap-finder rows, whose URLs ARE model-proposed, are untouched by
this: they carry no ``source_id``.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline.recall_url_agent import url_overwrite_refusal  # noqa: E402

REAL = ("https://www.fsai.ie/news-and-alerts/food-alerts/"
        "recall-of-specific-batch-of-various-macroom-buffal")
FABRICATED = ("https://www.fsai.ie/news-and-alerts/food-alerts/"
              "recall-of-specific-batch-of-various-macroom-buffalo-cheese-"
              "products-due-to-the-presence-of-listeria-monocytogenes/")

COLLECTOR_ROW = {
    "Source": "FSAI (IE)",
    "Company": "Macroom Buffalo Cheese Products Ltd.",
    "Product": "Mozzarella, Bocconcini and Burrata",
    "URL": REAL,
    "Notes": ("source_id=FSAI-recall-of-specific-batch-of-various-macroom-buffal "
              "[via official-feed collector]"),
}

#: A gap-finder row: the URL IS a model proposal, so reviewer 1 must be
#: free to correct it. No source_id, no collector marker.
GAP_ROW = {
    "Source": "RappelConso (FR)",
    "Company": "SARL la Gare du Terroir",
    "Product": "Graines de courge",
    "URL": "https://rappel.conso.gouv.fr/fiche-rappel/23512/Interne",
    "Notes": "[gap finder fr 2026-09-15] candidate URL from search",
}


# --------------------------------------------------------------------------
# the incident
# --------------------------------------------------------------------------

def test_the_exact_fabricated_url_is_refused():
    why = url_overwrite_refusal(COLLECTOR_ROW, FABRICATED)
    assert why, "this is the URL that was published and emailed"
    assert "official-feed collector" in why


def test_it_is_refused_even_without_the_collector_marker():
    """The slug-extension shape is independently disqualifying.

    Belt and braces: if the collector marker is ever dropped from Notes,
    the fabrication signature alone must still catch it.
    """
    row = dict(COLLECTOR_ROW,
               Notes="source_id=FSAI-recall-of-specific-batch-of-various-macroom-buffal")
    row["Notes"] = row["Notes"].replace("[via official-feed collector]", "")
    why = url_overwrite_refusal(row, FABRICATED)
    assert why and "past source_id" in why


def test_a_listing_page_is_refused():
    """The listing is what the fabricated URL actually resolved to."""
    why = url_overwrite_refusal(
        GAP_ROW, "https://www.fsai.ie/news-and-alerts/food-alerts")
    assert why and "listing" in why


@pytest.mark.parametrize("listing", [
    "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts",
    "https://www.mattilsynet.no/tilbakekallinger",
    "https://www.gov.pl/web/gis/ostrzezenia",
    "https://www.fsis.usda.gov/recalls",
])
def test_other_agency_listings_are_refused_too(listing):
    assert url_overwrite_refusal(GAP_ROW, listing)


def test_a_host_change_is_refused():
    """A row does not migrate to another domain on a model's say-so."""
    why = url_overwrite_refusal(
        GAP_ROW, "https://www.foodsafetynews.com/2026/09/some-story/")
    assert why and "host" in why


# --------------------------------------------------------------------------
# what must still be allowed — a guard that blocks everything is not a guard
# --------------------------------------------------------------------------

def test_a_gap_finder_url_may_still_be_corrected():
    """Reviewer 1's actual job. Model-proposed URLs need checking."""
    better = "https://rappel.conso.gouv.fr/fiche-rappel/23999/Interne"
    assert url_overwrite_refusal(GAP_ROW, better) == ""


def test_no_change_is_never_a_refusal():
    assert url_overwrite_refusal(COLLECTOR_ROW, REAL) == ""
    assert url_overwrite_refusal(COLLECTOR_ROW, REAL + "/") == ""


def test_an_empty_proposal_is_not_a_refusal():
    assert url_overwrite_refusal(COLLECTOR_ROW, "") == ""
    assert url_overwrite_refusal(COLLECTOR_ROW, None) == ""


def test_a_row_with_no_url_can_be_given_one():
    """Nothing to protect, so the model's URL is better than nothing."""
    row = dict(GAP_ROW, URL="")
    assert url_overwrite_refusal(
        row, "https://rappel.conso.gouv.fr/fiche-rappel/23512/Interne") == ""


def test_a_deeper_path_on_the_same_slug_is_allowed():
    """Anchors and query strings are presentation, not fabrication."""
    assert url_overwrite_refusal(COLLECTOR_ROW, REAL + "#details") == ""
    assert url_overwrite_refusal(COLLECTOR_ROW, REAL + "?lang=en") == ""


# ==========================================================================
# THE SAME SHAPE, A SECOND ROUTE IN (morning fix 2026-10-12)
# ==========================================================================
# url_overwrite_refusal refused the Macroom shape on two signals: a Notes
# stamp from the official-feed collector, and a `source_id=` the proposal
# could be measured against. A scraper's HTML LISTING FALLBACK has neither,
# and it reads hrefs out of the regulator's own markup just as the collector
# does — so the shape went through again, on a live Tier-1 recall:
#
#   Pending, FSAI (IE), SuperValu Smoked Salmon
#     row's href  .../food-alerts/recall-of-specific-batches-of-various-supervalu-sm
#     proposal    + "oked-salmon-products-due-to-the-presence-of-listeria-monocytogenes"
#     Notes       "FSAI HTML listing fallback — ..." then "URL confirmed"
#
# FSAI's own alert listing truncates its slugs at 49 characters — verified
# 2026-10-12 against https://www.fsai.ie/news-and-alerts/all-alerts, where
# the link reads "...various-SuperValu-Sm", and that 98-character URL serves
# the alert (11 October 2026, Listeria monocytogenes). The long slug is not
# a page. It produced a duplicate Pending row, which the confirmer then
# rejected for an empty Date.

_FSAI_REAL = ("https://www.fsai.ie/news-and-alerts/food-alerts/"
              "recall-of-specific-batches-of-various-supervalu-sm")
_FSAI_FABRICATED = ("https://www.fsai.ie/news-and-alerts/food-alerts/"
                    "recall-of-specific-batches-of-various-supervalu-smoked-"
                    "salmon-products-due-to-the-presence-of-listeria-"
                    "monocytogenes")


def test_a_listing_fallback_href_is_not_overwritten():
    row = {"URL": _FSAI_REAL,
           "Notes": "FSAI HTML listing fallback — claude-check please enrich "
                    "Pathogen+Date+Reason from detail page"}
    why = url_overwrite_refusal(row, _FSAI_FABRICATED)
    assert why, "the live 2026-10-12 overwrite must be refused"
    assert "regulator's own markup" in why


def test_the_cfia_listing_fallback_is_covered_too():
    """The other scraper with this route in — same stamp, different agency."""
    row = {"URL": "https://recalls-rappels.canada.ca/en/alert-recall/some-slug",
           "Notes": "CFIA HTML listing fallback — claude-check needs to enrich"}
    assert url_overwrite_refusal(
        row, "https://recalls-rappels.canada.ca/en/alert-recall/some-slug"
             "-recalled-due-to-listeria-monocytogenes-contamination")


def test_a_path_extension_is_refused_with_no_notes_at_all():
    """The general statement of the defect.

    Rules keyed on Notes only refuse rows that happen to carry the right
    stamp. The evidence is in the URL column: a proposal that keeps the
    whole of the row's path and runs on past it is the headline glued on.
    """
    why = url_overwrite_refusal({"URL": _FSAI_REAL, "Notes": ""},
                                _FSAI_FABRICATED)
    assert why and "runs" in why, why


def test_a_listing_page_may_still_be_replaced_by_a_notice_under_it():
    """The move the reviewer exists to make must stay allowed.

    A row sitting on an agency index SHOULD be moved to the notice, and
    that is a path extension. Refusing it would break the reviewer.
    """
    row = {"URL": "https://www.fsai.ie/news-and-alerts/food-alerts",
           "Notes": ""}
    assert url_overwrite_refusal(row, _FSAI_REAL) == ""


def test_a_sibling_notice_is_not_a_path_extension():
    """Replacing one notice with a different one is not this defect."""
    row = {"URL": _FSAI_REAL, "Notes": ""}
    assert url_overwrite_refusal(
        row, "https://www.fsai.ie/news-and-alerts/food-alerts/"
             "recall-of-a-batch-of-goats-milk-kefir") == ""


def test_adding_a_slash_or_query_is_still_not_an_overwrite():
    """Guards the new rule against the oldest false positive here."""
    row = {"URL": _FSAI_REAL, "Notes": ""}
    assert url_overwrite_refusal(row, _FSAI_REAL + "/") == ""
    assert url_overwrite_refusal(row, _FSAI_REAL + "?utm=1") == ""
