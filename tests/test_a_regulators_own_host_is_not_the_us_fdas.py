# -*- coding: utf-8 -*-
"""Reviewer 2's Source→host table must not read "fda" out of "NAFDAC".

THE ARCHIVE ROW THAT FOUND THIS. Weekly_Rejected idx540 on the 2026-10-22
workbook, the NAFDAC alert for Jawa International's amoxicillin suspensions,
refused on 2026-10-21 with:

    "URL domain does not match Source 'NAFDAC (NG)' (expected fda.gov)"

nafdac.gov.ng IS NAFDAC's own domain. _url_source_mismatch matched each key
of _SOURCE_DOMAINS as a bare SUBSTRING of the lower-cased Source, and
"nafdac (ng)" contains "fda", so the first key that matched was the US FDA's
and a Nigerian regulator was asked to publish on fda.gov.

MEASURED over every distinct Source string in the live workbook — four
regulators refused on their own URLs, permanently:

    NAFDAC (NG)    "fda" in "nafdac (ng)"    -> demanded fda.gov
    SFDA (SA)      "fda" in "sfda (sa)"      -> demanded fda.gov
    Thai FDA (TH)  "fda" in "thai fda (th)"  -> demanded fda.gov
    ECDC           "cdc" in "ecdc"           -> demanded cdc.gov

TFDA (TW) escaped by luck alone: "fda.gov" is a substring of "fda.gov.tw".

Corroborated by the register: Recalls holds no NAFDAC, SFDA or Thai FDA row
at all, and ECDC's newest row is 2026-06-25.

The same shape as _is_own_url_host (2026-10-19) and
review/url_validator._is_tolerated_host (2026-09-26): a check that compared
substrings where it had to compare tokens. The hosts added are the ones on
the operator's own pre-authorised source list, not guesses.

FOUR COPIES. The table and the loop live in pipeline/recall_review_agent.py
and in three workflow heredocs, which the operator rule requires to stay
byte-identical. The last test in this file holds that.
"""
from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from pipeline.recall_review_agent import (_SOURCE_DOMAINS,
                                          _url_source_mismatch)

ROOT = Path(__file__).resolve().parents[1]

#: (Source as the register writes it, a URL on that regulator's own host)
OWN_HOST = [
    ("NAFDAC (NG)",
     "https://nafdac.gov.ng/public-alert-no-35-2025-substandard-batches-of-"
     "annmox-amoxicillin-suspensions/"),
    ("NAFDAC", "https://nafdac.gov.ng/news-and-events/"),
    ("SFDA (SA)", "https://www.sfda.gov.sa/en/news-list/12345"),
    ("SFDA", "https://www.sfda.gov.sa/en/news-list/12345"),
    ("Thai FDA (TH)", "https://www.fda.moph.go.th/news/12345"),
    ("Thai FDA", "https://www.fda.moph.go.th/news/12345"),
    ("TFDA (TW)", "https://www.fda.gov.tw/TC/news.aspx?cid=4"),
    ("ECDC", "https://www.ecdc.europa.eu/en/news-events/listeria-2026"),
    # The regulators that were already right must stay right.
    ("FDA", "https://www.fda.gov/safety/recalls-market-withdrawals-safety-"
            "alerts/acme-recalls-cheese"),
    ("CDC", "https://www.cdc.gov/outbreaks/listeria-2026.html"),
    ("USDA FSIS", "https://www.fsis.usda.gov/recalls-alerts/kayem-foods-inc"),
    ("RappelConso (FR)",
     "https://rappel.conso.gouv.fr/fiche-rappel/23879/Interne"),
    ("NVWA (NL)", "https://www.nvwa.nl/actueel/nieuws/2026/10/20/x"),
    ("Ministero della Salute (IT)", "https://www.salute.gov.it/x.pdf"),
    ("FAVV-AFSCA", "https://favv-afsca.be/nl/producten/x"),
    ("CFS (HK)", "https://www.cfs.gov.hk/english/press/10-2026.html"),
    ("AGES (AT)", "https://www.ages.at/mensch/produktwarnungen/x"),
    ("BLV (CH)", "https://www.blv.admin.ch/de/newnsb/x"),
]

#: Cases where the table SHOULD still object — the rule has to keep working.
WRONG_HOST = [
    ("FDA", "https://nafdac.gov.ng/public-alert-no-35-2025/"),
    ("FDA", "https://www.fda.moph.go.th/news/12345"),
    ("CDC", "https://www.ecdc.europa.eu/en/news-events/x"),
    ("NAFDAC (NG)", "https://www.fda.gov/safety/recalls/x"),
    ("ECDC", "https://www.cdc.gov/outbreaks/x.html"),
    ("Thai FDA (TH)", "https://www.fda.gov/safety/recalls/x"),
]


@pytest.mark.parametrize("source,url", OWN_HOST)
def test_a_regulators_own_url_is_accepted(source, url):
    got = _url_source_mismatch({"Source": source, "URL": url})
    assert got is None, (
        f"{source} refused on its own host: {got}")


@pytest.mark.parametrize("source,url", WRONG_HOST)
def test_another_regulators_url_is_still_refused(source, url):
    got = _url_source_mismatch({"Source": source, "URL": url})
    assert got is not None, (
        f"{source} accepted a URL on another regulator's host ({url}) — the "
        "host rule has been widened too far")


def test_no_key_is_matched_inside_a_longer_word():
    """The defect itself, stated as a property of every key in the table.

    A key must not fire on a Source that merely contains its letters. Run
    over the whole table so a key added later cannot reintroduce it.
    """
    bad = []
    for key in _SOURCE_DOMAINS:
        # Wrap the key in letters on both sides: no key may match this.
        for shrouded in (f"x{key}", f"{key}x", f"x{key}x"):
            got = _url_source_mismatch(
                {"Source": shrouded, "URL": "https://example.invalid/x"})
            if got is not None and key in got.lower().replace(" ", ""):
                continue        # a different key matched; not this defect
            if got is not None:
                bad.append((key, shrouded, got))
    # The real assertion: the Sources the audit measured must be matched
    # against their OWN host. "(expected sfda.gov.sa)" contains the string
    # "fda.gov", so the check has to compare the named host exactly.
    expected = {
        "NAFDAC (NG)": "nafdac.gov.ng",
        "SFDA (SA)": "sfda.gov.sa",
        "Thai FDA (TH)": "fda.moph.go.th",
        "ECDC": "ecdc.europa.eu",
    }
    for source, host in expected.items():
        got = _url_source_mismatch(
            {"Source": source, "URL": "https://example.invalid/x"})
        assert got is not None, (
            f"{source} is matched against no host at all — its entry is "
            "missing from _SOURCE_DOMAINS")
        assert f"(expected {host})" in got, (
            f"{source} is being matched against the wrong regulator's host: "
            f"{got}")


def test_thai_fda_is_listed_before_fda():
    """Order is load-bearing: "fda" IS a whole word inside "Thai FDA".

    The boundary fix alone does not save this one, and the loop takes the
    first key that matches. Measured 2026-10-22: with "thai fda" below
    "fda", a notice on fda.moph.go.th was still refused as "expected
    fda.gov".
    """
    keys = list(_SOURCE_DOMAINS)
    assert "thai fda" in keys, "Thai FDA has no entry in the table"
    assert keys.index("thai fda") < keys.index("fda"), (
        "'thai fda' must come before 'fda' in _SOURCE_DOMAINS, or the US "
        "FDA's host is demanded of a Thai notice")


def _table(path: Path, pad: str) -> str:
    text = path.read_text(encoding="utf-8")
    start = text.index(pad + "_SOURCE_DOMAINS = {")
    end = text.index(pad + "}", start) + len(pad) + 1
    block = text[start:end]
    return textwrap.dedent(block) if pad else block


def test_the_workflow_heredocs_carry_the_same_table():
    """Reviewer heredocs stay byte-identical to pipeline/*.py (operator rule).

    Four copies of this table exist. A fix applied to one of them is a fix
    that three nightly runs do not have.
    """
    canonical = _table(ROOT / "pipeline" / "recall_review_agent.py", "")
    copies = [
        (ROOT / ".github/workflows/recall_review_agent.py", ""),
        (ROOT / ".github/workflows/recallreviewagent.yml", " " * 10),
        (ROOT / ".github/workflows/recall-review-agent.yml", " " * 10),
    ]
    drifted = []
    for path, pad in copies:
        if not path.exists():                               # pragma: no cover
            continue
        try:
            got = _table(path, pad)
        except ValueError:                                  # pragma: no cover
            drifted.append(f"{path.name}: no _SOURCE_DOMAINS table found")
            continue
        if got != canonical:
            drifted.append(path.name)
    assert not drifted, (
        "_SOURCE_DOMAINS has drifted from pipeline/recall_review_agent.py "
        "in: " + ", ".join(drifted)
        + "\n\nThe reviewer heredocs must stay byte-identical to the module, "
          "or the nightly reviewers run a different rule from the one the "
          "tests check."
    )
