# -*- coding: utf-8 -*-
"""A regulator's recall board is not a notice, in any of its languages.

THE ROW THAT FOUND THIS (morning fix 2026-11-04, main at 8eefd23e)
==================================================================
Pending held, at status "pending_gap" since 2026-10-29:

    Source    BLV
    Country   Switzerland
    Company   "Mises en garde et rappels"
    Product   "aliments"
    Pathogen  "Listeria monocytogenes"   Tier 1
    URL       https://www.blv.admin.ch/fr/mises-en-garde-et-rappels-aliments

Company and Product are the two halves of the page title "Mises en garde et
rappels — aliments". The page is the BLV's recall BOARD; it lists every
Swiss warning and names no single recall.

`pipeline/promote_gate_passing` reported that row as passing the FULL
publish gate — "Pending 10 rows — pass the full publish gate: 1". The only
thing that kept it out of the register was an earlier copy of the same
listing page already sitting in Rejected, so merge_master's re-promotion
guard refused it as "already in Rejected". That is a coincidence of the
archive, not a gate: delete that archive row and a listing page publishes.

THE GATE HAD ALREADY BEEN TOLD, ONCE, IN THE WRONG PLACE
========================================================
pipeline/gap_finder/countries/ch.py has refused this exact URL since
2026-09-23, and its comment says why in so many words:

    TOO WIDE. "rappel" matched /fr/mises-en-garde-et-rappels-aliments,
    the BLV LANDING PAGE — which is how a row reached Recalls with
    Company = Brand = the page title and Product = "aliments".

The fix was made in the gap finder's intake regex only. The publish gate —
the thing that decides what reaches subscribers — never learned it.

WHY THE LANGUAGE SEGMENT, NOT JUST THIS PATH
============================================
`_LANDING_PATHS` is a hand-kept set of exact paths, and three of its
entries are already language-prefixed forms of other entries
("/en/food-alerts", "/fr/rappels", "/nl/terugroepingen"), each added after
a row cited that one spelling. BLV serves this board at /de/, /fr/ AND
/it/; a list built one language at a time can only ever hold the ones that
have already cost a row. So the gate strips one leading two-letter segment
before the membership test — which can only ever match a path whose
remainder is ALREADY an acknowledged board, and therefore cannot widen the
rule onto a notice slug.
"""
from __future__ import annotations

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

from pipeline._publish_gate import (  # noqa: E402
    _LANDING_PATHS, _is_landing_path, publish_blockers,
)


# ---------------------------------------------------------------------------
# The row that found it
# ---------------------------------------------------------------------------

BLV_BOARD_ROW = {
    "Date": "2026-10-29",
    "Source": "BLV",
    "Company": "Mises en garde et rappels",
    "Brand": "Mises en garde et rappels",
    "Product": "aliments",
    "Pathogen": "Listeria monocytogenes",
    "Reason": "Listeria monocytogenes",
    "Class": "Recall",
    "Country": "Switzerland",
    "Region": "Europe",
    "Tier": 1,
    "Outbreak": 0,
    "URL": "https://www.blv.admin.ch/fr/mises-en-garde-et-rappels-aliments",
    "Notes": "Wander AG procède au rappel du produit « Ovo Crunchy Ice "
             "Mini » en raison de la présence de corps étrangers en bois "
             "— 29.10.2026",
    "Status": "pending_gap",
}


def test_the_blv_recall_board_is_refused_by_the_publish_gate():
    blockers = publish_blockers(BLV_BOARD_ROW)
    assert any("landing page" in b for b in blockers), (
        "the BLV recall board passes the publish gate. On 2026-11-04 the "
        "offline promoter listed this row as the ONE row of ten that passed "
        "every rule; only an archive coincidence kept it unpublished. "
        f"blockers={blockers!r}")


@pytest.mark.parametrize("lang", ("de", "fr", "it", "en"))
def test_every_language_of_the_board_is_refused(lang):
    row = dict(BLV_BOARD_ROW)
    row["URL"] = (f"https://www.blv.admin.ch/{lang}/"
                  f"mises-en-garde-et-rappels-aliments")
    assert any("landing page" in b for b in publish_blockers(row)), (
        f"the /{lang}/ spelling of the board is accepted. BLV serves this "
        f"page in three languages and a per-language list only ever holds "
        f"the ones that have already cost a row.")


# ---------------------------------------------------------------------------
# The generalisation must not reach past a known board
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("path", (
    "/de/newnsb/oBmdo4JjUtwF",                       # a BLV news notice
    "/dam/blv/de/dokumente/rueckrufe/rr-eberle.pdf",  # a BLV recall document
    "/fr/produits/rappel-de-bio-planet-2",            # AFSCA notice
    "/en/alert-recall/various-brands-organic-cheeses-recalled",
    "/fiche-rappel/23987/interne",
    "/fr/mises-en-garde-et-rappels-aliments/une-fiche",  # one level deeper
    "/fr/rappels-de-saucisse-de-morteau",             # a slug that STARTS
))                                                    # like a listed board
def test_a_real_notice_is_not_a_landing_page(path):
    assert not _is_landing_path(path.lower()), (
        f"{path} is a notice, not a board. The language-segment rule must "
        f"only ever match a path whose remainder is EXACTLY an entry of "
        f"_LANDING_PATHS — a prefix match would refuse real recalls, which "
        f"is the failure mode _LANDING_PATHS' own comment warns about "
        f"('a gate that cries wolf on good rows gets switched off').")


def test_the_rule_refuses_nothing_the_register_already_publishes():
    """The measurement that makes the change safe to ship.

    A gate rule is only as good as its false-positive count on the rows
    already in Recalls. A narrower variant of this fix — consulting each
    country config's own `authority_item_url_regex` — was measured first
    and REFUSED 85 published URLs (every Mattilsynet, NVWA, AGES and AESAN
    notice whose config is not held to the register at 100%), so it was not
    shipped. This one refuses exactly the rows that were already refused.
    """
    pd = pytest.importorskip("pandas")
    xlsx = ROOT / "docs" / "data" / "recalls.xlsx"
    if not xlsx.exists():                                   # pragma: no cover
        pytest.skip("no workbook")
    df = pd.read_excel(xlsx, "Recalls")

    newly_refused = []
    for _, r in df.iterrows():
        url = str(r.get("URL") or "").strip()
        if not url or url == "nan":
            continue
        tail = url.split("?", 1)[0].split("#", 1)[0]
        after_host = tail.split("://", 1)[-1]
        path = (("/" + after_host.split("/", 1)[1])
                if "/" in after_host else "").rstrip("/").lower()
        if not _is_landing_path(path):
            continue
        # Refused by the literal set too? Then this rule did not add it.
        if path in _LANDING_PATHS:
            continue
        from pipeline._publish_gate import _INDEX_PATH_PREFIX
        if _INDEX_PATH_PREFIX.match(path):
            continue
        newly_refused.append((str(r.get("Date"))[:10], url[:96]))

    assert not newly_refused, (
        f"{len(newly_refused)} published recall(s) are refused as a landing "
        f"page ONLY because of the language-segment rule:\n  " +
        "\n  ".join(map(str, newly_refused[:10])) +
        "\n\nEach is a recall the register already holds and could not "
        "re-promote. Narrow the rule or list the page explicitly.")
