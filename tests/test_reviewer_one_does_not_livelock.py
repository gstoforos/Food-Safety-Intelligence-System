"""A refused rejection has to change something.

WHY THIS EXISTS (2026-09-25)
===========================
pipeline/_url_guard.reject_refusal stops reviewer 1 discarding a row whose
URL is already on a regulator's own domain, because "I could not fetch the
page" is a statement about a datacentre IP, not about whether the notice
exists. It has fired 15 times and was right every time.

But it changed nothing else. The row stayed at its input status, so the
next pass asked the same model the same question about the same page, got
the same answer, and the guard refused again — three passes a day. On
2026-09-25 ten rows were doing this in Pending, the oldest since
2026-09-18: about twenty-one wasted reviewer passes on one RappelConso row,
and a reviewer-1 queue a freshness audit reads as STALE while the stage is
in fact running perfectly and being overruled.

The verdict was inside the refusal all along. Reviewer 1's job is to
CONFIRM THE OFFICIAL URL, and the guard can only refuse once it has
established, deterministically, that the row carries one. So the second
refusal escalates instead of waiting.

These tests hold the two brakes on that escalation, because promoting a row
on the strength of a negative is only safe with both:
  1. it must be a PER-RECALL url, not a board index — otherwise this
     becomes the Switzerland/Germany "landing page reached Recalls" defect
     by a new route;
  2. not on the first refusal — one refusal can be a transient fetch
     failure, and one more pass costs nothing.
"""
from __future__ import annotations

import pytest

from pipeline._url_guard import (url_is_self_evidently_official,
                                 refusal_count, reject_refusal)


FR_NOTICE = "https://rappel.conso.gouv.fr/fiche-rappel/23605/interne"
FR_LISTING = "https://rappel.conso.gouv.fr/categorie/1"
FSIS_NOTICE = ("https://www.fsis.usda.gov/recalls-alerts/star-meat-delivery-"
               "inc--recalls-raw-pork-beef-and-goat-products-produced-without")
FSIS_LISTING = "https://www.fsis.usda.gov/recalls-alerts"
DE_ROOT = "https://www.lebensmittelwarnung.de/"
NEWS = "https://www.foodsafetynews.com/2026/09/some-story/"


class TestWhatCountsAsSelfEvident:

    @pytest.mark.parametrize("url", [FR_NOTICE, FSIS_NOTICE])
    def test_a_per_recall_notice_on_the_authority_host_is_self_evident(self, url):
        assert url_is_self_evidently_official(url)

    @pytest.mark.parametrize("url", [FR_LISTING, FSIS_LISTING, DE_ROOT])
    def test_a_listing_or_root_is_never_self_evident(self, url):
        """Brake 1. These are on the authority's own host, which is exactly
        why the host check alone is not enough."""
        assert url_is_self_evidently_official(url) == ""

    @pytest.mark.parametrize("url", [NEWS, "", "   ", "not a url"])
    def test_anything_off_the_authority_host_is_never_self_evident(self, url):
        assert url_is_self_evidently_official(url) == ""

    def test_a_country_with_a_config_is_held_to_its_own_regex(self):
        """FSIS has a us.py config, so the per-recall pattern — not the
        generic two-segment fallback — is what governs. A URL on the right
        host in the wrong shape must fail."""
        assert url_is_self_evidently_official(
            "https://www.fsis.usda.gov/wps/portal/fsis/topics/whatever") == ""

    def test_a_country_without_a_config_still_gets_a_listing_check(self):
        """France has no CountryConfig at all — RappelConso is a native
        scraper. The fallback must still refuse a category page."""
        assert url_is_self_evidently_official(FR_NOTICE)
        assert url_is_self_evidently_official(FR_LISTING) == ""


class TestTheRefusalCounter:

    def test_a_fresh_row_has_no_refusals(self):
        assert refusal_count("") == 0
        assert refusal_count("leclerc thouars") == 0

    def test_it_counts_the_real_note_shape(self):
        """Verbatim from the live sheet on 2026-09-25."""
        note = ("magasins u [url-guard 2026-09-24: reviewer 1 tried to reject "
                "this row as 'No official regulator page found for the given "
                "product and hazard'; refused — the official URL is on the row.]")
        assert refusal_count(note) == 1

    def test_it_counts_two(self):
        note = ("x [url-guard 2026-09-22: reviewer 1 tried to reject this row "
                "as 'a'] [url-guard 2026-09-24: reviewer 1 tried to reject "
                "this row as 'b']")
        assert refusal_count(note) == 2

    def test_the_escalation_note_is_itself_counted(self):
        """The escalation tag keeps the same 'tried to reject' wording on
        purpose, so a row's history stays countable from the sheet alone."""
        note = ("[url-guard 2026-09-25: reviewer 1 tried to reject this row as "
                "'x' for the 2nd time; refused AND escalated — the URL is on "
                "the authority's own domain]")
        assert refusal_count(note) == 1


class TestTheEscalationCondition:
    """The condition as reviewer 1 applies it: prior >= 1 AND self-evident."""

    @staticmethod
    def escalates(notes: str, url: str) -> bool:
        return bool(refusal_count(notes) >= 1
                    and url_is_self_evidently_official(url))

    ONE = ("[url-guard 2026-09-24: reviewer 1 tried to reject this row as "
           "'no official regulator URL found']")

    def test_not_on_the_first_refusal(self):
        """Brake 2."""
        assert self.escalates("", FR_NOTICE) is False

    def test_yes_on_the_second(self):
        assert self.escalates(self.ONE, FR_NOTICE) is True

    def test_never_for_a_listing_however_many_times(self):
        many = self.ONE * 6
        assert self.escalates(many, FR_LISTING) is False

    def test_never_for_a_news_host_however_many_times(self):
        many = self.ONE * 6
        assert self.escalates(many, NEWS) is False


class TestTheGuardStillOnlyBlocksReachability:
    """The escalation must not widen what the guard refuses. A CONTENT
    rejection — not food, out of scope, not a recall — is reviewer 1 doing
    its job and has to pass straight through, even on an authority URL.
    Otherwise the dexmedetomidine injection row in Pending on 2026-09-25
    could be escalated into the register."""

    def test_a_content_rejection_is_not_refused(self):
        row = {"URL": FSIS_NOTICE}
        for why in ("not a food product — a drug",
                    "out of scope: medical device",
                    "this is guidance, not a recall",
                    "pre-2026"):
            assert reject_refusal(row, why) == "", why

    def test_a_reachability_rejection_is_refused(self):
        row = {"URL": FSIS_NOTICE}
        for why in ("No official regulator URL found",
                    "could not find the page",
                    "unable to reach the site",
                    "HTTP 404"):
            assert reject_refusal(row, why) != "", why
