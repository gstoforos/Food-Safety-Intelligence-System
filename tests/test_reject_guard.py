# -*- coding: utf-8 -*-
"""Reviewer 1 may not discard a row whose official URL is on the row.

THE MIRROR IMAGE (audit 2026-09-21)
===================================
``url_overwrite_refusal`` stops reviewer 1 REPLACING a regulator's own
href with a guess. That was the Macroom incident — a fabricated URL
published and emailed to a subscriber.

This is the same mistake pointing the other way, and it has been quietly
costing rows the whole time. Reviewer 1's contract lets it reject for "no
official page findable". It decides *findable* by searching and fetching,
and it never looks at the URL already sitting on the row. So a collector
can hand it the regulator's own href — the strongest evidence that
exists — and if a GitHub runner cannot fetch that page, the model reports
that the notice does not exist and the row is thrown away.

MEASURED, from the live ``Weekly_Rejected`` sheet on 2026-09-21. Each was
rejected as "no official page", with the official page in its own URL
column:

    FSAI (IE)  Dunnes Stores Potato Waffles      rejected TWICE
    FSAI (IE)  prepared Roast Chicken and Gravy
    FDA        Gf Blends

``fda.gov``, ``fsis.usda.gov``, ``fda.gov.ph`` and ``gov.il`` are already
documented in this repo as returning HTTP 403 to datacentre traffic for
*every* URL. "I could not fetch it" and "it does not exist" are different
facts, and the agent was publishing the second when it only knew the
first.

WHAT THIS GUARD DOES NOT DO
---------------------------
It is not a licence to publish. A refused reject leaves the row in
Pending for a later run — it does not confirm it. And it only touches the
reachability class: every content verdict reviewer 1 is *supposed* to
make — out of scope, undeclared allergen, foreign body, pre-2026, not a
recall, non-food — passes straight through, on an authority host or any
other.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline._url_guard import (host_is_authority,          # noqa: E402
                                 reject_refusal)

# The three rows this cost, verbatim from the sheet.
DUNNES = {"Source": "FSAI (IE)", "Company": "Dunnes Stores Potato Waffles",
          "URL": ("https://www.fsai.ie/news-and-alerts/food-alerts/"
                  "recall-of-a-batch-of-dunnes-stores-potato-waffles")}
ROAST = {"Source": "FSAI (IE)", "Company": "prepared Roast Chicken and Gravy",
         "URL": ("https://www.fsai.ie/news-and-alerts/food-alerts/"
                 "recall-of-branded-roast-chicken-and-gravy-produced")}
GFBLENDS = {"Source": "FDA", "Company": "Gf Blends",
            "URL": ("https://www.fda.gov/safety/recalls-market-withdrawals-"
                    "safety-alerts/gf-blends-recalls-truly-aip-all-purpose-fl")}
FROSTY = {"Source": "RappelConso (FR)", "Company": "Frosty Pockets",
          "URL": "https://rappel.conso.gouv.fr/fiche-rappel/23546/interne"}
NEWS = {"Source": "news", "Company": "x",
        "URL": "https://www.foodsafetynews.com/2026/09/some-story/"}


# --------------------------------------------------------------------------
# the incident
# --------------------------------------------------------------------------

@pytest.mark.parametrize("row,reason", [
    (DUNNES, "No official recall page found"),
    (ROAST, "No official regulator page found for this recall"),
    (GFBLENDS, "No official regulator page found for this recall"),
])
def test_the_live_false_rejections_are_refused(row, reason):
    why = reject_refusal(row, reason)
    assert why, f"{row['Company']} would still be discarded"
    assert "reachability" in why


def test_the_refusal_names_the_url_it_is_defending():
    why = reject_refusal(DUNNES, "No official recall page found")
    assert DUNNES["URL"] in why, (
        "an operator reading the audit trail must see WHICH url the guard "
        "thought was good enough")


@pytest.mark.parametrize("reason", [
    "no official page found",
    "No official regulator URL found for this recall",
    "could not find the official page",
    "Unable to locate an official regulator page",
    "page not found",
    "the page returned 403",
    "fetch failed with 404",
])
def test_the_whole_not_found_family_is_caught(reason):
    assert reject_refusal(DUNNES, reason)


# --------------------------------------------------------------------------
# what must STILL be rejected — a guard that blocks everything is not a guard
# --------------------------------------------------------------------------

@pytest.mark.parametrize("reason", [
    "non-food product (glycérol)",
    "undeclared allergen (milk)",
    "foreign body (plastic)",
    "notice published 2024-11-02, out of scope",
    "not a recall — a market withdrawal notice",
    "pet food",
    "duplicate of an existing row",
    "labelling error, no microbial pathogen",
    "chemical contamination (ethylene oxide)",
])
def test_content_verdicts_pass_straight_through(reason):
    assert reject_refusal(DUNNES, reason) == "", (
        "this is reviewer 1 doing its job; the guard must not touch it")


def test_an_out_of_contract_reject_is_NOT_this_guard_s_problem():
    """A separate defect, found the same day, deliberately left alone.

    One live rejection reads:

        "URL agent: No specific outbreak for the given product and hazard
         found"

    That is not a reachability claim, so this guard correctly ignores it.
    It is worse than a reachability claim: reviewer 1's contract never
    authorises rejecting a recall for the absence of an OUTBREAK. A
    recall does not require one. The model invented a criterion.

    The fix for that belongs in the prompt contract, not here, and
    silently widening this regex to swallow it would hide a prompt bug
    behind a URL guard.
    """
    assert reject_refusal(
        DUNNES, "No specific outbreak for the given product and hazard "
                "found") == "", (
        "if this ever starts being refused, the guard has been widened to "
        "cover a defect it does not fix — read the docstring")



def test_the_glycerol_reject_still_stands():
    """A real content call on an authority URL, from the same day."""
    assert reject_refusal(FROSTY, "non-food product (glycérol)") == ""


def test_a_news_host_proves_nothing():
    assert reject_refusal(NEWS, "No official regulator page found") == ""


def test_a_row_with_no_url_can_still_be_rejected():
    assert reject_refusal({"URL": ""}, "No official page found") == ""
    assert reject_refusal({}, "No official page found") == ""


def test_an_empty_reason_is_not_refused():
    assert reject_refusal(DUNNES, "") == ""
    assert reject_refusal(DUNNES, None) == ""


# --------------------------------------------------------------------------
# host recognition
# --------------------------------------------------------------------------

@pytest.mark.parametrize("url", [
    "https://www.fsai.ie/news-and-alerts/food-alerts/x",
    "https://www.fda.gov/safety/recalls/x",
    "https://rappel.conso.gouv.fr/fiche-rappel/23546/interne",
    "https://www.fsis.usda.gov/recalls/x",
    "https://recalls-rappels.canada.ca/en/alert-recall/x",
    "https://www.mattilsynet.no/x",
    "https://www.gov.pl/web/gis/x",
    "https://webgate.ec.europa.eu/rasff-window/screen/notification/1",
    "https://www.salute.gov.it/x",
    "HTTPS://WWW.FDA.GOV/x",
])
def test_authority_hosts_are_recognised(url):
    assert host_is_authority(url)


@pytest.mark.parametrize("url", [
    "https://www.foodsafetynews.com/x",
    "https://news.google.com/x",
    "https://www.fda.gov.evil.example/x",   # suffix-match must not be fooled
    "https://notfda.gov/x",
    "", "not a url", "javascript:void(0)",
])
def test_non_authority_hosts_are_not(url):
    assert not host_is_authority(url)


# --------------------------------------------------------------------------
# the call site
# --------------------------------------------------------------------------

AGENT = (ROOT / "pipeline" / "recall_url_agent.py").read_text(encoding="utf-8")


def test_the_agent_consults_the_guard_before_rejecting():
    assert "_ref = reject_refusal(row, _why)" in AGENT
    i = AGENT.index("elif dec == \"reject\":")
    j = AGENT.index("rejected_flags[idx] = f\"URL agent:", i)
    assert "reject_refusal" in AGENT[i:j], (
        "the guard must be consulted BEFORE the row is flagged rejected")


def test_a_refused_reject_becomes_a_retry_not_a_confirm():
    """The row goes back to the queue. It is not published."""
    assert 'counts["retry"] = counts.get("retry", 0) + 1' in AGENT
    assert "row[\"Status\"]" not in AGENT[
        AGENT.index("[url-guard] reject refused"):
        AGENT.index("else:", AGENT.index("[url-guard] reject refused"))], (
        "a refused reject must not change the row's status")


def test_the_refusal_is_written_into_notes():
    assert "[url-guard {today}: reviewer 1 tried to reject" in AGENT
    assert "_room = 1000 - len(tag) - 1" in AGENT, (
        "same truncation rule as every other stamp — reserve room for the "
        "tag rather than appending and cutting it off")


def test_there_is_a_failsafe_when_the_guard_module_is_missing():
    """Each agent is written to disk from its own heredoc; the import can
    fail. Failing open here means silently discarding rows again."""
    i = AGENT.index("def reject_refusal(row, reason):")
    stub = AGENT[i:AGENT.index("def review_url", i)]
    assert "not importable" in stub
    assert "404|403" in stub or "404" in stub


# --------------------------------------------------------------------------
# the heredoc pair — a fix that is not embedded never runs
# --------------------------------------------------------------------------

def test_the_workflow_copy_is_byte_identical():
    wf = (ROOT / ".github" / "workflows" / "recall-url-agent.yml"
          ).read_text(encoding="utf-8").split("\n")
    s = next(i for i, l in enumerate(wf) if "PYEOF_A1'" in l)
    e = next(i for i, l in enumerate(wf) if l.strip() == "PYEOF_A1" and i > s)
    embedded = "\n".join(l[10:] if l.startswith(" " * 10) else l
                         for l in wf[s + 1:e])
    assert embedded.rstrip("\n") == AGENT.rstrip("\n"), (
        "recall-url-agent.yml writes reviewer 1 to disk from this heredoc, "
        "so a fix that lands only in the .py never executes — that is the "
        "2026-09-02 failure, repeated")
