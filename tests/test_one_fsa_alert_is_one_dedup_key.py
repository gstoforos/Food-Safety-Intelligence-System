"""One FSA alert is one dedup key, whichever food.gov.uk host serves it. 2026-11-08.

THE BUG
-------
The UK Food Standards Agency publishes a single alert at three addresses:

    alerts.food.gov.uk/news-alerts/alert/fsa-prin-38-2026     the public page
    data.food.gov.uk/food-alerts/id/FSA-PRIN-38-2026          the API record
    food.gov.uk/news-alerts/alert/fsa-prin-02-2026            the bare host

``merge_master._normalize_url_for_dedup`` keyed on the host, so these were
three recalls. This is the same identity-split the function already fixes for
admin.ch (one federal release per language and department host) and for AESAN
(one Spanish alert at up to seven language addresses) — and on the UK agency
it had already happened six times in the live workbook.

MEASURED on the workbook of 2026-11-08, sweeping every food.gov.uk URL in all
five sheets through the function and grouping by notation: 8 of 28 notations
had more than one dedup key, and six of them — fsa-prin-28, 38, 39, 40, 41 and
42 of 2026 — were PUBLISHED under one host form while an independently-
reviewed copy of the same alert sat REJECTED under another. Each copy had
taken its own trip through the reviewers and come back with its own verdict on
one recall.

On the morning of 2026-11-08 a third copy of fsa-prin-38-2026 (Greencore,
three pasta products, Listeria monocytogenes, published 2026-08-07) reached the
front of the publish gate as the only Pending row passing every check. It would
have been promoted as a seventh row for a recall the register has carried since
August — and the monthly report for a closed month would have moved with it.

WHAT MUST NOT COLLAPSE
----------------------
The ``-update-N`` suffix. FSA does not amend an alert in place; it republishes
at its own slug, and this register publishes those as separate rows
(fsa-prin-02-2026 plus update-1 and update-2; 28 plus update-1; 47 plus
update-1). Whether an update is a duplicate or an extension of its parent is a
real question, but it is not one a URL normalizer may answer by itself, and
collapsing them would start discarding extensions in silence. So the suffix is
part of the key, and this test pins that.

The ``/article/<n>/<slug>`` form carries no notation. It cannot be mapped to
one without fetching the page, so the function leaves it alone — pinned below
so that nobody later maps it by guessing at the slug.
"""
from __future__ import annotations

import collections
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
XLSX = ROOT / "docs" / "data" / "recalls.xlsx"

from pipeline.merge_master import _normalize_url_for_dedup as N  # noqa: E402

#: Every notation shape the register holds, with the host forms FSA serves it
#: at. Each pair is one alert and must therefore be one key.
_SAME = [
    ("https://alerts.food.gov.uk/news-alerts/alert/fsa-prin-38-2026",
     "http://data.food.gov.uk/food-alerts/id/FSA-PRIN-38-2026"),
    ("https://www.food.gov.uk/news-alerts/alert/fsa-prin-28-2026",
     "https://alerts.food.gov.uk/news-alerts/alert/fsa-prin-28-2026"),
    ("https://alerts.food.gov.uk/news-alerts/alert/fsa-prin-41-2026",
     "https://data.food.gov.uk/food-alerts/id/FSA-PRIN-41-2026"),
    # Case and a trailing slash are not identity either.
    ("https://ALERTS.food.gov.uk/news-alerts/alert/FSA-PRIN-42-2026/",
     "https://data.food.gov.uk/food-alerts/id/fsa-prin-42-2026"),
    # Allergy alerts and "food fraud" alerts use the same notation scheme.
    ("https://www.food.gov.uk/news-alerts/alert/fsa-aa-13-2026",
     "https://data.food.gov.uk/food-alerts/id/FSA-AA-13-2026"),
]

#: Pairs that are two different things and must keep two keys.
_DIFFERENT = [
    # Two different alerts.
    ("https://alerts.food.gov.uk/news-alerts/alert/fsa-prin-38-2026",
     "https://alerts.food.gov.uk/news-alerts/alert/fsa-prin-39-2026"),
    # An alert and its republished update: a question for a human, not for
    # this function.
    ("https://alerts.food.gov.uk/news-alerts/alert/fsa-prin-02-2026",
     "https://alerts.food.gov.uk/news-alerts/alert/fsa-prin-02-2026-update-1"),
    ("https://alerts.food.gov.uk/news-alerts/alert/fsa-prin-02-2026-update-1",
     "https://alerts.food.gov.uk/news-alerts/alert/fsa-prin-02-2026-update-2"),
    # A PRIN and an AA that share a number are not the same alert.
    ("https://www.food.gov.uk/news-alerts/alert/fsa-prin-13-2026",
     "https://www.food.gov.uk/news-alerts/alert/fsa-aa-13-2026"),
]

_NOTATION = re.compile(r"(fsa-(?:prin|aa|fafa)-\d+-\d{4}(?:-update-\d+)?)",
                       re.I)


@pytest.mark.parametrize("a,b", _SAME)
def test_one_alert_on_two_hosts_is_one_key(a, b):
    assert N(a) == N(b), (
        f"{a}\nand\n{b}\nare the SAME FSA alert — data.food.gov.uk returns the "
        f"PRIN notation as the record's own `notation` and both addresses carry "
        f"the same title, company, hazard and product list. Two keys means the "
        f"alert gets reviewed twice and can end up published under one form and "
        f"rejected under the other, which is what the live workbook showed six "
        f"times on 2026-11-08.")


@pytest.mark.parametrize("a,b", _DIFFERENT)
def test_two_alerts_keep_two_keys(a, b):
    assert N(a) != N(b), (
        f"{a}\nand\n{b}\nare not the same alert and must not share a dedup key. "
        f"A republished `-update-N` alert in particular is published by this "
        f"register as its own row; collapsing it onto its parent would discard "
        f"extensions as duplicates.")


def test_the_notation_survives_in_the_key():
    """The collapse must not throw away the identifier along with the host."""
    for url in [u for pair in _SAME for u in pair]:
        key = N(url)
        want = _NOTATION.search(url).group(1).lower()
        assert key == "food.gov.uk/food-alerts/" + want, (
            f"{url} -> {key}: the notation {want} is the identity and must be "
            f"the whole of the key after the host is dropped.")


def test_the_article_form_is_left_alone():
    """No notation on the page address — so no guessing at one."""
    article = ("https://alerts.food.gov.uk/article/6816/Greencore-recalls-"
               "three-pasta-products-because-of-possible-contamination-with-"
               "Listeria-monocytogenes")
    assert N(article) != "food.gov.uk/food-alerts/fsa-prin-38-2026", (
        "The /article/<n>/<slug> form carries no PRIN notation. It happens to "
        "be fsa-prin-38-2026, but only the page or the API says so, and a "
        "normalizer that guessed it from the slug would be guessing. That copy "
        "is handled as data, in tools/repair_2026_11_08.py.")
    assert "6816" in N(article), (
        "The article number is the only identifier this form carries and must "
        "survive, or every /article/ URL on the host collapses to one key.")


def test_a_food_gov_uk_page_with_no_notation_is_untouched():
    """The rule is anchored on the notation, not on the host."""
    for url in ("https://www.food.gov.uk/news-alerts",
                "https://www.food.gov.uk/business-guidance/food-allergies",
                "https://data.food.gov.uk/food-alerts/id.html"):
        assert not N(url).startswith("food.gov.uk/food-alerts/fsa-"), (
            f"{url} is not an alert notice and must not be rewritten into the "
            f"alert key space.")


def test_every_notation_in_the_workbook_has_exactly_one_key():
    """The shape of the bug, measured against the register itself."""
    pd = pytest.importorskip("pandas")
    if not XLSX.exists():                                   # pragma: no cover
        pytest.skip("no workbook")

    by_notation = collections.defaultdict(set)
    x = pd.ExcelFile(XLSX)
    for sheet in x.sheet_names:
        df = pd.read_excel(x, sheet)
        if "URL" not in df.columns:
            continue
        for v in df["URL"]:
            u = str(v).strip()
            if not u or u.lower() == "nan" or "food.gov.uk" not in u.lower():
                continue
            m = _NOTATION.search(u)
            if m:
                by_notation[m.group(1).lower()].add(N(u))

    split = {n: sorted(k) for n, k in by_notation.items() if len(k) > 1}
    assert not split, (
        f"{len(split)} FSA notation(s) in the register still carry more than "
        f"one dedup key, so the same alert can be reviewed twice and reach two "
        f"different verdicts:\n" +
        "\n".join(f"  {n}\n" + "\n".join(f"      {k}" for k in keys)
                  for n, keys in sorted(split.items())))
