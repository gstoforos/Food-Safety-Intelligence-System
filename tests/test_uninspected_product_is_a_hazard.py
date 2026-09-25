"""An FSIS recall of uninspected meat must reach the register.

WHY THIS EXISTS (2026-09-25)
===========================
The daily global sweep reported this as absent from the register:

    Star Meat Delivery Inc., Lucama NC, ~167,639 lb of raw pork, beef and
    goat, produced without the benefit of federal inspection and bearing a
    FALSE inspection mark, "EST. 1363"  — FSIS recall 022-2026

It was absent, and USDA FSIS had shown no new row since 2026-09-08. The
scraper was not broken. scrapers/north_america/usda_fsis.py migrated to
pathogens("en") on 2026-07-29 — it is still the ONLY scraper that has, the
other eleven use for_languages("en") — so an FSIS record must match a named
hazard to survive. No term in the vocabulary could match "produced without
the benefit of federal inspection", so the record was dropped at
n_skipped_no_pathogen. Reproduced against the live scraper with the record
rebuilt in the FSIS API's own field shape:

    USDA FSIS: 1 pathogen recalls in 30-day window (3 records scanned,
    skipped: ... no_pathogen=2 ...)

This was an omission, not a scope decision. Allergen-only recalls ARE
deliberately excluded — NON_PATHOGEN_REJECTS lists "undeclared milk",
"allergen labelling" and their kin, _models._NON_PATHOGEN_MARKERS enforces
it, and only 16 of 1780 published rows mention an allergen at all, every one
of those incidentally. Uninspected product appeared on no list in either
direction. And of the two categories, unassessed is the worse one to be
blind to: an allergen recall names its hazard and affects a known subset of
consumers, while uninspected meat means nobody looked.

So the two halves of this file are equally load-bearing:
  * uninspected product gets in, with a truthful Pathogen and a tier that
    is not the floor;
  * allergen-only recalls stay out, because widening the gate to catch Star
    Meat must not quietly widen it to catch everything.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from scrapers._models import assign_tier, normalize_pathogen
from scrapers._pathogen_vocab import (NON_PATHOGEN_REJECTS, pathogens,
                                      recall_signals)
from scrapers.north_america.usda_fsis import USDAFSISScraper

CANON = "Uninspected product (hazard not assessed)"


def _ago(days: int) -> str:
    return (datetime.now(timezone.utc)
            - timedelta(days=days)).strftime("%Y-%m-%d")


class _MockResponse:
    def __init__(self, payload, status_code: int = 200):
        self._payload, self.status_code, self.text = payload, status_code, ""

    def json(self):
        return self._payload


# The real record, in the API's own shape. Note field_product_items is a
# LIST — FSIS emits arrays for multi-value fields, which is the 2026-08-13
# parse bug, so the fixture keeps that shape on purpose.
STAR_MEAT = {
    "field_title": ("Star Meat Delivery Inc. Recalls Raw Pork, Beef and Goat "
                    "Products Produced Without Benefit of Inspection"),
    "field_summary": ("<p>Star Meat Delivery Inc., a Lucama, N.C. "
                      "establishment, is recalling approximately 167,639 "
                      "pounds of raw pork, beef and goat products that were "
                      "produced without the benefit of federal inspection "
                      "and bore a false inspection mark, \"EST. 1363\".</p>"),
    "field_product_items": ["ground pork", "chorizo", "pork chops",
                            "goat cuts", "menudo mix"],
    "field_recall_reason": "Produced Without Benefit of Inspection",
    "field_recall_date": None,          # filled per-test
    "field_archive_recall": "False",
    "langcode": "English",
    "field_recall_url": ("/recalls-alerts/star-meat-delivery-inc--recalls-raw-"
                         "pork-beef-and-goat-products-produced-without"),
}

ALLERGEN_ONLY = {
    "field_title": ("Acme Foods Recalls Chicken Products Due to Misbranding "
                    "and Undeclared Allergens"),
    "field_summary": ("<p>The product contains milk, a known allergen, which "
                      "is not declared on the label.</p>"),
    "field_product_items": "frozen chicken entrees",
    "field_recall_reason": "Misbranding and Undeclared Allergen",
    "field_recall_date": None,
    "field_archive_recall": "False",
    "langcode": "English",
    "field_recall_url": "/recalls-alerts/acme-foods-undeclared-milk",
}

LISTERIA = {
    "field_title": ("Example Co. Recalls Ready-To-Eat Meat Due to Possible "
                    "Listeria monocytogenes Contamination"),
    "field_summary": "<p>Possible Listeria monocytogenes contamination.</p>",
    "field_product_items": "RTE deli meat",
    "field_recall_reason": "Listeria monocytogenes",
    "field_recall_date": None,
    "field_archive_recall": "False",
    "langcode": "English",
    "field_recall_url": "/recalls-alerts/example-co-listeria",
}


def _dated(rec, days):
    out = dict(rec)
    out["field_recall_date"] = _ago(days)
    return out


def _scrape(records):
    s = USDAFSISScraper()
    with patch("scrapers.north_america.usda_fsis.fetch",
               return_value=_MockResponse(records)):
        return s.scrape(since_days=30)


class TestTheScraperKeepsIt:

    def test_star_meat_survives_the_hazard_gate(self):
        out = _scrape([_dated(STAR_MEAT, 2)])
        assert len(out) == 1, (
            "an uninspected-meat recall was dropped at the hazard gate — "
            "this is FSIS recall 022-2026, 167,639 lb")

    def test_its_pathogen_says_what_is_known_and_no_more(self):
        row = _scrape([_dated(STAR_MEAT, 2)])[0]
        assert row.Pathogen == CANON
        low = row.Pathogen.lower()
        for invented in ("salmonella", "listeria", "e. coli", "coli"):
            assert invented not in low, (
                "nobody tested this product — naming a pathogen would be "
                "fabrication (R5)")

    def test_it_is_not_filed_at_the_lowest_tier(self):
        row = _scrape([_dated(STAR_MEAT, 2)])[0]
        assert row.Tier == 2, (
            "an unassessed hazard is unknown, not mild. Tier 3 here was the "
            "first version of this fix, caused by _fda_framework_tier not "
            "reading _TIERS — see the comment beside the canonical in that "
            "function's tier_2_pathogens set")

    def test_the_relative_url_is_resolved_not_dropped(self):
        row = _scrape([_dated(STAR_MEAT, 2)])[0]
        assert row.URL.startswith("https://www.fsis.usda.gov/recalls-alerts/")
        assert "star-meat-delivery-inc" in row.URL


class TestTheGateDidNotJustOpen:
    """Widening the vocabulary to catch Star Meat must not widen it to
    everything. These are the rows that must still be refused."""

    def test_an_allergen_only_recall_is_still_dropped(self):
        assert _scrape([_dated(ALLERGEN_ONLY, 3)]) == []

    def test_the_allergen_rejects_are_untouched(self):
        for term in ("undeclared milk", "undeclared peanut",
                     "allergen labelling", "labeling error"):
            assert term in NON_PATHOGEN_REJECTS

    @pytest.mark.parametrize("raw", [
        "Misbranding and Undeclared Allergen", "undeclared milk",
        "undeclared peanut", "allergen labelling", "label error",
    ])
    def test_the_normaliser_still_refuses_allergens(self, raw):
        assert normalize_pathogen(raw) == ""

    def test_a_recall_verb_is_still_not_a_hazard(self):
        """The 2026-07-29 regression: every FSIS title contains "Recalls",
        so a vocabulary carrying recall verbs matched everything and stamped
        Pathogen="recall" on misbranding rows. The new terms must not
        reintroduce that."""
        vocab = set(pathogens("en"))
        assert vocab.isdisjoint(set(recall_signals("en")))
        for verb in ("recall", "recalls", "alert", "warning", "withdrawal"):
            assert verb not in vocab

    def test_the_mixed_batch_keeps_two_of_three(self):
        out = _scrape([_dated(STAR_MEAT, 2), _dated(ALLERGEN_ONLY, 3),
                       _dated(LISTERIA, 4)])
        assert {r.Pathogen for r in out} == {CANON, "Listeria monocytogenes"}


class TestTheCanonicalAndItsTiers:

    @pytest.mark.parametrize("raw", [
        "Produced Without Benefit of Inspection",
        "produced without the benefit of federal inspection",
        "without benefit of inspection",
        "bore a false inspection mark, \"EST. 1363\"",
        "uninspected product",
    ])
    def test_every_fsis_phrasing_normalises(self, raw):
        assert normalize_pathogen(raw) == CANON

    @pytest.mark.parametrize("raw", [
        "Ineligible Imported",
        "not presented for import re-inspection",
        "ineligible for importation",
    ])
    def test_import_violations_are_deliberately_not_included(self, raw):
        """Arguably the same unassessed class — product from outside the
        inspection system — but a SEPARATE scope line with its own existing
        test (test_usda_fsis_scraper.py::test_import_violation_dropped),
        and that line has not been ruled on.

        The first version of this fix did include them, and that test still
        passed — only because its fixture says "Ineligible Imported" while
        the vocabulary term was "ineligible for importation". A wording
        accident, not agreement. This test exists so the next person
        changes the line on purpose rather than discovering they already
        have."""
        assert normalize_pathogen(raw) == ""

    def test_the_regulators_own_class_still_wins(self):
        """Step 1 of the hybrid framework. The Tier-2 fallback applies only
        when FSIS gives no Class, which is why it is a fallback."""
        assert assign_tier(CANON, 0, "Class I", "raw pork") == 1
        assert assign_tier(CANON, 0, "Class II", "raw pork") == 2

    def test_no_class_falls_back_to_two(self):
        assert assign_tier(CANON, 0, "", "raw pork, beef and goat") == 2

    def test_an_outbreak_still_bumps(self):
        assert assign_tier(CANON, 1, "", "raw pork") == 1

    def test_an_unknown_hazard_is_still_tier_three(self):
        """The floor must stay where it was for things we genuinely do not
        recognise — otherwise this change has moved every default."""
        assert assign_tier("something we do not track", 0, "", "x") == 3

    def test_listeria_is_unaffected(self):
        assert assign_tier("Listeria monocytogenes", 0, "", "RTE deli") == 1
