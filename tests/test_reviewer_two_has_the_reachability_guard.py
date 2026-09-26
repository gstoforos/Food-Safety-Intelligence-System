"""Reviewer 2 may not archive a recall because a datacentre IP got a 403.

WHY THIS EXISTS (2026-09-26)
===========================
An operator opened https://alerts.food.gov.uk/news-alerts/alert/fsa-prin-47-2026-update-1
in a browser and asked whether the register had it. It did — twice, both
rejected:

    [review-agent 2026-09-25: REJECTED — Review agent: URL is dead and no
     official page found]
    [review-agent 2026-09-25: REJECTED — Review agent: URL not found and no
     official page found]

The URL is not dead. alerts.food.gov.uk is robots-disallowed to automated
clients, which is a statement about the client, not about the alert. The row
was World of Sweets / Alyan Dubai Style chocolate — **Salmonella, Tier 1** —
and both the original alert and its update-1 were archived to
Weekly_Rejected.

pipeline/_url_guard.reject_refusal has existed since 2026-09-21 for exactly
this, and returns the right answer for both of those reason strings. It was
wired into reviewer 1 only. Reviewer 2 is the reviewer that ARCHIVES, so its
mistakes leave Pending altogether — the worse place for the guard to be
missing.

Measured on the 2026-09-26 workbook before wiring it in: TWELVE rows rejected
on a not-found reason while holding a URL on the regulator's own domain, six
distinct recalls, three Tier 1 —

    World of Sweets / Alyan Dubai chocolate   Salmonella   Tier 1  (x2 URLs)
    International Sprout Holdings             Salmonella   Tier 1  www.fda.gov
    Kilbride Classic Cuisine                  Listeria     Tier 1  fsai.ie
    RASFF Portugal                            Aflatoxin    Tier 2
    Hong Kong CFS                             E. coli      Tier 2
    Dunnes Stores                                          Tier 3

Every one of those hosts answers 403, a robots refusal, or a lying 404 to
automated traffic.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from pipeline._url_guard import host_is_authority, reject_refusal

ROOT = Path(__file__).resolve().parent.parent
AGENT2 = (ROOT / "pipeline" / "recall_review_agent.py").read_text(encoding="utf-8")

FSA = "https://alerts.food.gov.uk/news-alerts/alert/fsa-prin-47-2026-update-1"
FDA = ("https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/"
       "international-sprout-holdings")
FSAI = "https://www.fsai.ie/news-and-alerts/food-alerts/recall-of-a-batch"
RASFF = "https://webgate.ec.europa.eu/rasff-window/screen/notification/579466"
CFS = "https://www.cfs.gov.hk/english/whatsnew/whatsnew_fa/2026_625.html"
NEWS = "https://www.foodsafetynews.com/2026/09/a-story/"


class TestTheGuardIsWiredIn:

    def test_reviewer_two_consults_it_before_flagging_a_reject(self):
        assert "reject_refusal" in AGENT2, (
            "reviewer 2 archives rows; it must ask the guard first")
        i = AGENT2.index("Rejects → rejected_flags")
        j = AGENT2.index('rejected_flags[idx] = f"Review agent:', i)
        assert "_reject_refusal(_row, _why)" in AGENT2[i:j], (
            "the guard must be consulted BEFORE the row is flagged rejected")

    def test_a_refused_reject_leaves_the_row_in_pending(self):
        i = AGENT2.index("[url-guard] reject refused")
        j = AGENT2.index('rejected_flags[idx] = f"Review agent:', i)
        region = AGENT2[i:j]
        assert "continue" in region, (
            "a refused reject must skip the rejected_flags write entirely")
        assert 'Status' not in region, (
            "reviewer 2's refusal must not change status — that is reviewer "
            "1's escalation, and reviewer 2 judges content, not URLs")

    def test_the_refusal_is_written_into_notes(self):
        assert "reviewer 2 tried to reject this" in AGENT2
        assert "_room = 1000 - len(_tag) - 1" in AGENT2, (
            "same truncation rule as every other stamp")

    def test_there_is_a_failsafe_when_the_guard_module_is_missing(self):
        """Both reviewers are written to disk from their own heredoc, so the
        import can fail. Failing open means silently archiving recalls."""
        i = AGENT2.index("def _reject_refusal(row, reason):")
        stub = AGENT2[i:AGENT2.index("for merged, review in results", i)]
        assert "not importable" in stub
        assert "is\\s+dead" in stub or "is_dead" in stub or "dead" in stub


class TestWhatTheGuardRefuses:
    """The reason strings reviewer 2 actually produced, against the hosts it
    actually produced them for."""

    @pytest.mark.parametrize("url", [FSA, FDA, FSAI, RASFF, CFS])
    def test_all_six_hosts_are_authority_hosts(self, url):
        assert host_is_authority(url)

    @pytest.mark.parametrize("why", [
        "Review agent: URL is dead and no official page found",
        "Review agent: URL not found and no official page found",
        "could not reach the official page",
        "HTTP 403 on the regulator page",
        "unable to locate an official notice",
    ])
    def test_a_reachability_reason_is_refused(self, why):
        assert reject_refusal({"URL": FSA}, why) != "", why

    @pytest.mark.parametrize("why", [
        "not a food product — a veterinary drug",
        "out of scope: allergen labelling only",
        "this is guidance, not a recall",
        "duplicate of a published row",
        "pre-2026",
    ])
    def test_a_content_reason_passes_straight_through(self, why):
        """The guard must not blunt reviewer 2's actual job. A content verdict
        on an authority URL is the reviewer working."""
        assert reject_refusal({"URL": FSA}, why) == "", why

    def test_a_news_host_proves_nothing_and_is_not_protected(self):
        assert reject_refusal({"URL": NEWS},
                              "no official page found") == ""

    def test_a_row_with_no_url_is_not_protected(self):
        assert reject_refusal({"URL": ""}, "no official page found") == ""


class TestTheHeredocsAreInSync:
    """recall-review-agent.yml AND recallreviewagent.yml both write reviewer 2
    to disk from a PYEOF_AGENT heredoc, so a fix landing only in the .py never
    executes. That is the 2026-09-02 failure, and reviewer 2 has TWO copies to
    keep in step rather than one."""

    @pytest.mark.parametrize("wf", ["recall-review-agent.yml",
                                    "recallreviewagent.yml"])
    def test_the_embedded_copy_is_byte_identical(self, wf):
        lines = (ROOT / ".github" / "workflows" / wf).read_text(
            encoding="utf-8").split("\n")
        s = next(i for i, l in enumerate(lines) if "PYEOF_AGENT'" in l)
        e = next(i for i, l in enumerate(lines)
                 if l.strip() == "PYEOF_AGENT" and i > s)
        indent = 0
        for l in lines[s + 1:e]:
            if l.strip():
                indent = len(l) - len(l.lstrip())
                break
        embedded = "\n".join(l[indent:] if l.startswith(" " * indent) else l
                             for l in lines[s + 1:e])
        assert embedded.rstrip("\n") == AGENT2.rstrip("\n"), (
            f"{wf} writes reviewer 2 from this heredoc — a fix that lands "
            f"only in the .py never runs")

    @pytest.mark.parametrize("wf", ["recall-review-agent.yml",
                                    "recallreviewagent.yml"])
    def test_the_embedded_copy_compiles(self, wf):
        import ast
        lines = (ROOT / ".github" / "workflows" / wf).read_text(
            encoding="utf-8").split("\n")
        s = next(i for i, l in enumerate(lines) if "PYEOF_AGENT'" in l)
        e = next(i for i, l in enumerate(lines)
                 if l.strip() == "PYEOF_AGENT" and i > s)
        indent = 0
        for l in lines[s + 1:e]:
            if l.strip():
                indent = len(l) - len(l.lstrip())
                break
        ast.parse("\n".join(l[indent:] if l.startswith(" " * indent) else l
                            for l in lines[s + 1:e]))


class TestToleranceMatchesOnSuffix:
    """My own 2026-09-25 fix had this hole: BOT_HOSTILE_DOMAINS was compared
    with an exact hostname match while _url_guard.host_is_authority has always
    matched on suffix. food.gov.uk was tolerated; alerts.food.gov.uk, where
    every UK FSA alert actually lives, was not — so a failed check against it
    counted as evidence and would have deleted the URL."""

    @pytest.mark.parametrize("url", [
        "https://alerts.food.gov.uk/news-alerts/alert/fsa-prin-47-2026",
        "https://www.food.gov.uk/news-alerts",
        "https://food.gov.uk/x",
        "https://webgate.ec.europa.eu/rasff-window/screen/notification/1",
        "https://www.cfs.gov.hk/english/whatsnew/whatsnew_fa/2026_625.html",
    ])
    def test_a_subdomain_of_a_tolerated_regulator_is_tolerated(self, url):
        from review.url_validator import _is_tolerated_host
        assert _is_tolerated_host(url) is True

    @pytest.mark.parametrize("url", [
        "https://www.foodsafetynews.com/x",
        # Suffix matching must not be fooled by a lookalike.
        "https://food.gov.uk.evil.example/x",
        "https://notfood.gov.uk.example/x",
        "",
    ])
    def test_a_lookalike_or_news_host_is_not(self, url):
        from review.url_validator import _is_tolerated_host
        assert _is_tolerated_host(url) is False

    def test_a_four_hundred_from_a_tolerated_host_is_not_verified_dead(self):
        """The whole consequence: an unreadable regulator page must never set
        verified_dead, because that is the flag that deletes a URL."""
        from review.url_validator import should_blank_url
        assert should_blank_url({"reason": "bot_blocked", "status": 404,
                                 "verified_dead": False}) is False
