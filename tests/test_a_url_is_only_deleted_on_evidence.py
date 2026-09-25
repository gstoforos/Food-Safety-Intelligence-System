"""A URL leaves a row only when something that can read the page says it is gone.

WHY THIS EXISTS (2026-09-25)
===========================
On 2026-09-25 the register held four FDA rows, all stamped
"REJECTED: http_error", and one of them — Galil Importing Corp's Lior
ground cinnamon, elevated lead — had had its URL deleted outright:

    [URL-guardian 2026-09-25: blanked http_error
     https://www.fda.gov/safety/recalls-market-withdrawals-s...]

The URL was correct. www.fda.gov sits behind Akamai bot detection, which
answers plain `requests` with HTTP 404 for every page, real notices
included. review/url_validator.py consulted its BOT_HOSTILE_DOMAINS
tolerance list for status 403 only, so the lying 404 walked past it into
should_blank_url, which blanks on 404.

Two consequences, both live in the sheet that morning:
  * the row can never be published again — a row with no URL cannot pass
    the authority-URL gate, which is the register's whole promise;
  * there were suddenly TWO Pending rows for one recall, because
    _dedup_key is URL-primary, so blanking a URL creates a new row
    identity rather than editing an existing one.

98 register rows sit on the four Akamai hosts. Every one of them was one
guardian pass from this. pipeline/_provenance.py had written the mechanism
down on 2026-09-02, including the sentence "the same blindness would
instead REJECT every real FDA/FSIS row" — 23 days before it did.

These tests are about the RULE, not about the four hostnames: a status
code is never enough to delete a URL.
"""
from __future__ import annotations

import pytest

from review.url_validator import (should_blank_url, BOT_HOSTILE_DOMAINS,
                                  _akamai_host, is_generic_url)


class TestBlankingNeedsEvidence:

    def test_a_lying_404_from_an_akamai_host_does_not_blank(self):
        """The exact shape that deleted the Galil URL."""
        check = {"url": "https://www.fda.gov/safety/recalls-market-withdrawals-"
                        "safety-alerts/galil-importing-corp-recalls-lior-"
                        "cinnamon-ground-seasoning-due-elevated-lead-levels",
                 "status": 404, "ok": True, "generic": False,
                 "reason": "bot_blocked", "verified_dead": False}
        assert should_blank_url(check) is False

    def test_a_404_confirmed_through_impersonation_does_blank(self):
        """The tolerance must not become blanket. A notice that a browser-
        equivalent client also cannot find really is gone."""
        check = {"status": 404, "ok": False, "reason": "http_error",
                 "verified_dead": True}
        assert should_blank_url(check) is True

    def test_an_empty_url_still_blanks(self):
        assert should_blank_url({"reason": "empty",
                                 "verified_dead": True}) is True

    @pytest.mark.parametrize("reason,status", [
        ("bot_blocked", 403), ("bot_blocked", 404), ("bot_blocked", 500),
        ("tls_error", 0), ("network", 0), ("generic", 0),
    ])
    def test_nothing_inconclusive_ever_blanks(self, reason, status):
        assert should_blank_url({"reason": reason, "status": status,
                                 "verified_dead": False}) is False

    def test_a_check_from_before_the_invariant_does_not_blank(self):
        """A caller holding a dict built by the old code has no
        verified_dead key. Refuse rather than guess — the cost of keeping a
        dead link is one bad click; of guessing wrong, a destroyed row."""
        assert should_blank_url({"reason": "http_error", "status": 404}) is False
        assert should_blank_url({"reason": "http_error", "status": 500}) is False

    def test_the_status_code_alone_decides_nothing(self):
        """The property the old code violated, stated directly: two checks
        that differ ONLY in verified_dead must reach opposite verdicts."""
        for status in (404, 410, 500, 503):
            live = {"reason": "http_error", "status": status,
                    "verified_dead": False}
            dead = {"reason": "http_error", "status": status,
                    "verified_dead": True}
            assert should_blank_url(live) is False, status
            assert should_blank_url(dead) is True, status


class TestTheAkamaiHostsAreRecognised:

    @pytest.mark.parametrize("url", [
        "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/x",
        "https://www.fsis.usda.gov/recalls-alerts/star-meat-delivery-inc--recalls-raw-pork",
        "https://www.fda.gov.ph/fda-advisory-no-2026-1234/",
        "https://www.gov.il/en/pages/food-recall-1",
    ])
    def test_the_four_known_liars_route_to_impersonation(self, url):
        assert _akamai_host(url) is True

    @pytest.mark.parametrize("url", [
        "https://rappel.conso.gouv.fr/fiche-rappel/23605/interne",
        "https://www.lebensmittelwarnung.de/bvl-lmw-de/detail/lebensmittel/123",
        "https://recalls-rappels.canada.ca/en/alert-recall/x",
    ])
    def test_other_hosts_keep_the_cheaper_path(self, url):
        assert _akamai_host(url) is False

    def test_the_akamai_hosts_are_also_on_the_tolerance_list(self):
        """Belt and braces: even if the impersonation route is removed, a
        4xx from these hosts must still be classified bot_blocked."""
        for host in ("www.fda.gov", "www.fsis.usda.gov"):
            assert host in BOT_HOSTILE_DOMAINS


class TestTheGenericCheckStillWorks:
    """The blanking change must not loosen the listing check, which is a
    separate gate and the one that keeps landing pages out of Recalls."""

    @pytest.mark.parametrize("url", [
        "https://www.fsis.usda.gov/recalls-alerts",
        "https://rappel.conso.gouv.fr/categorie/1",
        "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts",
    ])
    def test_listings_are_still_generic(self, url):
        assert is_generic_url(url) is True

    def test_a_real_notice_is_not_generic(self):
        assert is_generic_url(
            "https://www.fsis.usda.gov/recalls-alerts/star-meat-delivery-inc--"
            "recalls-raw-pork-beef-and-goat-products-produced-without") is False
