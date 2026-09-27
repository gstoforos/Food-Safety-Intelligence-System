"""A lookalike host is not the regulator.

WHY THIS EXISTS (2026-09-27)
===========================
The publish gate proves a row's URL belongs to the regulator its Source names.
It tested that with a three-way disjunction: equality, a suffix test, and a
bare SUBSTRING containment test.

The first two clauses are right. The third is the defect, so an
FDA-labelled row citing

    https://fda.gov.evil.example/recall/123

passed the one gate whose entire job is to establish that the named regulator
published the notice — because "fda.gov" is a substring of
"fda.gov.evil.example".

Replaced by host_matches_domain(), the single canonical host test: exact
domain or genuine subdomain, both sides normalised for case, port, trailing
dot and a leading "www." (the allow-list mixes "fda.gov" with
"www.fsis.usda.gov", and a host should match either spelling of its own
domain).

Measured before and after on the 1,794 published rows: 16 rows fail the gate
either way. No published row was relying on the substring clause, so the
tightening costs nothing and closes the hole.

Same defect, same week, opposite direction: review/url_validator matched
hostnames EXACTLY where it needed a suffix test, which made every
alerts.food.gov.uk alert look dead (2026-09-26). Both are suffix tests now.
"""
from __future__ import annotations

import pytest

from pipeline._publish_gate import host_matches_domain as m


class TestTheLookalikeIsRefused:

    @pytest.mark.parametrize("host", [
        "fda.gov.evil.example",          # the reproduced case
        "fsis.usda.gov.attacker.test",
        "notfda.gov",
        "myfda.gov.co",
        "xfda.gov",
        "fda.gov1.example",
    ])
    def test_a_host_that_merely_contains_the_domain(self, host):
        assert m(host, "fda.gov") is False or m(host, "fsis.usda.gov") is False

    def test_the_exact_reproduction(self):
        assert m("fda.gov.evil.example", "fda.gov") is False

    def test_a_path_containing_the_domain_is_not_a_host(self):
        assert m("evil.example/fda.gov", "fda.gov") is False


class TestTheRealRegulatorStillPasses:

    @pytest.mark.parametrize("host,domain", [
        ("fda.gov", "fda.gov"),
        ("www.fda.gov", "fda.gov"),
        ("fda.gov", "www.fda.gov"),
        ("www.fsis.usda.gov", "fsis.usda.gov"),
        ("fsis.usda.gov", "www.fsis.usda.gov"),
        # genuine subdomains, which is where regulators actually publish
        ("alerts.food.gov.uk", "food.gov.uk"),
        ("webgate.ec.europa.eu", "ec.europa.eu"),
        ("rappel.conso.gouv.fr", "conso.gouv.fr"),
        ("recalls-rappels.canada.ca", "canada.ca"),
    ])
    def test_exact_or_subdomain(self, host, domain):
        assert m(host, domain) is True

    @pytest.mark.parametrize("host", [
        "FDA.GOV", "www.FDA.gov.", "www.fda.gov:443", "  fda.gov  ",
        "https://www.fda.gov/safety/recalls",
    ])
    def test_normalisation(self, host):
        """Case, trailing dot, port, whitespace and a full URL all resolve to
        the same host. The allow-list is hand-maintained and inconsistent
        about www, so both spellings must work."""
        assert m(host, "fda.gov") is True


class TestDegenerateInput:

    @pytest.mark.parametrize("host,domain", [
        ("", "fda.gov"), ("fda.gov", ""), ("", ""), (None, "fda.gov"),
        ("fda.gov", None),
    ])
    def test_nothing_matches_nothing(self, host, domain):
        assert m(host, domain) is False


class TestTheGateUsesIt:
    """A canonical validator that the gate does not call is decoration."""

    def test_the_substring_clause_is_gone(self):
        """NOTE: this scans the module source for the removed clause, so
        neither this file nor _publish_gate.py may quote it verbatim — both
        docstrings describe it in words instead. Quoting it is how this test
        first failed, which is a fair demonstration that it works."""
        import inspect
        from pipeline import _publish_gate as G
        src = inspect.getsource(G)
        assert "or a in _h" not in src, (
            "the substring clause is back — a lookalike host passes again")
        assert "host_matches_domain(_h, a)" in src, (
            "the gate must call the canonical validator")

    def test_an_fda_row_on_a_lookalike_host_is_blocked(self):
        from pipeline._publish_gate import publish_blockers
        row = dict(Date="2026-09-25", Source="FDA", Company="Acme Foods",
                   Brand="Acme", Product="frozen peas 500g",
                   Pathogen="Listeria monocytogenes",
                   Reason="Possible Listeria monocytogenes contamination",
                   Class="Class I", Country="United States", Tier=1,
                   URL="https://fda.gov.evil.example/recall/123")
        blocks = publish_blockers(row)
        assert any("does not belong to Source" in b for b in blocks), blocks
