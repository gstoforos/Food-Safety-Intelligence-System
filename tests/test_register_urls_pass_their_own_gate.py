# -*- coding: utf-8 -*-
"""The register is the oracle. Point the configs at it.

WHY THIS FILE EXISTS (audit 2026-09-23)
=======================================
Fourteen country configs were written that morning from URL patterns
verified by web search. Every one passed its unit tests. Then the same
configs were run against the URLs ALREADY IN THE REGISTER, and four were
wrong:

  hk  — took /press/ only. The register held 6 Food Alert pages
        (/whatsnew/whatsnew_fa/<year>_<n>.html) and 14 Food Incident Post
        PDFs (/rc/subject/files/<date>_<n>.pdf). 20 of 22 rejected.
        Hong Kong's own live Pending row was the counter-example.
  mx  — took /cofepris/<section>/<slug>. BOTH published Mexican rows are
        CMS alert PDFs. 2 of 2 rejected.
  co  — took /biblioteca/preview/<id>. The ONE published Colombian row is
        a press-room article. 1 of 1 rejected.
  hu  — named its own host, so search_verifier could never match it.

Each would have run daily, found candidates, rejected all of them at the
authority gate, and reported success. Web search told me what a regulator's
recall page looks like. The register told me what it ACTUALLY publishes,
and the two differed for three countries out of fourteen.

So this check does not get done once by hand. Every authority URL the
register already holds must pass its own country's item gate — because if
it does not, that country cannot re-find the recalls it has already found.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from urllib.parse import urlparse

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline.gap_finder.countries import all_codes, get  # noqa: E402

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"

#: Written 2026-09-23 and corrected against the register the same day.
#: These must stay at 100%: a rejected URL here is a recall the country
#: has already found once and could not find again.
NEW_2026_09_23 = {
    "sg", "hk", "kr", "jp", "tw", "ph", "id", "vn",
    "br", "mx", "co", "cl", "sa", "ae",
}

#: Older configs whose regex has since been rewritten against this same
#: evidence, and which are therefore held to the same 100% bar.
#:
#: ch — read r"(warnung|rappel|richiamo|news|aktuell)", a word match on the
#:      whole URL, wrong in both directions at once. It refused all six
#:      /rueckrufe/ recall notices (the word "Rückruf" is in the FILENAME,
#:      not the path) while accepting /fr/mises-en-garde-et-rappels-aliments
#:      — the BLV LANDING PAGE, on the strength of "rappel". 9 of 14 Swiss
#:      URLs in Recalls were refused by their own country's gate.
CORRECTED = {"ch"}

MUST_BE_CLEAN = NEW_2026_09_23 | CORRECTED

#: Sheets holding rows the system STANDS BEHIND. Rejected/Weekly_Rejected
#: are excluded on purpose: a rejected row's URL is often a listing page,
#: and the gate refusing it is the gate working.
TRUSTED_SHEETS = ("Recalls", "Pending")


def _authority_urls():
    """{code: {(sheet, url)}} for every register URL on a configured
    authority domain."""
    openpyxl = pytest.importorskip("openpyxl")
    if not XLSX.exists():
        pytest.skip("no register in this checkout")

    dom2code: dict[str, str] = {}
    for code in all_codes():
        cfg = get(code)
        # Primary only. The *_extra domains are accepted as CONTEXT —
        # linkable from an article — and are explicitly not required to
        # satisfy the item regex.
        dom2code.setdefault(cfg.authority_domain.lower(), code)

    found: dict[str, set] = {}
    wb = openpyxl.load_workbook(XLSX, read_only=True)
    for sheet in TRUSTED_SHEETS:
        if sheet not in wb.sheetnames:
            continue
        ws = wb[sheet]
        head = [str(c.value or "") for c in ws[1]]
        if "URL" not in head:
            continue
        iu = head.index("URL")
        for row in ws.iter_rows(min_row=2, values_only=True):
            url = str(row[iu] or "").strip()
            if not url.startswith("http"):
                continue
            host = urlparse(url).netloc.lower()
            for dom, code in dom2code.items():
                if host == dom or host.endswith("." + dom):
                    found.setdefault(code, set()).add((sheet, url))
    return found


@pytest.fixture(scope="module")
def register_urls():
    return _authority_urls()


def _rejects(code, urls):
    """URLs this country's own gate would refuse, checked against BOTH
    strings the pipeline builds."""
    rx = re.compile(get(code).authority_item_url_regex, re.IGNORECASE)
    out = []
    for sheet, url in sorted(urls):
        p = urlparse(url)
        pq = f"{p.path}?{p.query}" if p.query else p.path
        if not (rx.search(url) and rx.search(pq)):
            out.append((sheet, url))
    return out


@pytest.mark.parametrize("code", sorted(MUST_BE_CLEAN))
def test_a_new_config_accepts_every_url_the_register_already_holds(
        code, register_urls):
    urls = register_urls.get(code)
    if not urls:
        pytest.skip(f"{code}: no register URL on {get(code).authority_domain} yet")
    bad = _rejects(code, urls)
    assert not bad, (
        f"{code}: {len(bad)} of {len(urls)} authority URLs already in the "
        f"register are REFUSED by this country's own item regex:\n"
        + "\n".join(f"    [{s}] {u}" for s, u in bad[:8])
        + f"\n  regex: {get(code).authority_item_url_regex}\n"
        f"These are recalls the system has already found once. If the gate "
        f"refuses them, it will refuse them again — the country runs, finds "
        f"candidates, accepts none, and reports success. Widen the regex to "
        f"the board this URL lives on (and add a listing page for that "
        f"board to the negatives in test_country_config_conformance.py), or "
        f"establish that these rows should not have been published.")


def test_report_coverage_for_the_older_configs(register_urls):
    """Reported, not asserted.

    The pre-2026-09-23 configs have rejects too, and some of them are the
    gate doing its job — a listing page that reached Recalls before the
    item regexes existed SHOULD be refused now. Failing on those would
    mean asserting that past mistakes are correct. So this prints, and a
    number that grows is the finding.
    """
    older = sorted(set(register_urls) - MUST_BE_CLEAN)
    print("\n  older configs, authority URLs in Recalls/Pending:")
    for code in older:
        urls = register_urls[code]
        bad = _rejects(code, urls)
        flag = "" if not bad else f"   <-- {len(bad)} refused"
        print(f"    {code:4} {len(urls) - len(bad):3}/{len(urls):<3} accepted{flag}")
        for s, u in bad[:3]:
            print(f"           [{s}] {u[:96]}")


def test_the_new_countries_are_all_still_registered():
    missing = sorted(NEW_2026_09_23 - set(all_codes()))
    assert not missing, (
        f"configs written 2026-09-23 are gone from the registry: {missing}")
