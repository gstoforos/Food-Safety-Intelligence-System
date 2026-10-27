"""The same firm under two legal forms is the same firm.

WHAT WAS WRONG (audit 2026-10-27)
=================================
One MPI (NZ) recall reached Recalls TWICE on 2026-10-24:

    Hutchinsons Ltd      Arla Protein Pudding Vanilla Cookie (200g)
      .../news/media-releases/arla-protein-pudding-recalled-due-to-possible-…
    Hutchinsons Limited  Arla Protein Pudding Vanilla Cookie (200g)
      .../food-safety-home/…/recalled-food-products/arla-protein-pudding-…

Same regulator, same date, same product string, same hazard (glass
fragments) — two of MPI's own pages for one recall. That is exactly the
"same recall, different URL" case merge_master's near-dup index was built
for, and it missed, because _near_dup_key strips punctuation but not the
CORPORATE FORM: "hutchinsonsltd" and "hutchinsonslimited" are different
strings, so the two rows had different keys.

WHY THE COMPANY KEY ITSELF WAS NOT LOOSENED
===========================================
Stripping the legal form inside _near_dup_key was tried first and MEASURED
on all 2184 published rows. It catches this pair AND suppresses two real
recalls: three separate RappelConso fiches from one Chevenet STEC incident,

    fiche 23016  CHEVENET SARL  Satonnay 140 g (cheese)      2026-07-31
    fiche 23033  CHEVENET SARL  Fresh goat cheese            2026-07-29
    fiche 23018  Chevenet       Tonnelet (small cask)         2026-07-28

three different products, three regulatory actions. _near_dup_key omits
Product deliberately, so loosening its company half makes a blunt rule
blunter and costs real notices.

THE REPAIR
==========
A SECOND key, strictly additive, that pays for the looser company match by
requiring Product to agree too: (source, legal-form-insensitive company,
pathogen, product). Measured on the same 2184 rows it matches the
Hutchinsons pair and nothing else — the Chevenet fiches differ in Product
and are untouched.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline.merge_master import (                             # noqa: E402
    NEAR_DUP_WINDOW_DAYS, _build_near_dup_index, _company_base,
    _is_near_duplicate, _near_dup_key, _same_firm_same_product_key,
)

MPI_A = {"Source": "MPI (NZ)", "Company": "Hutchinsons Ltd",
         "Product": "Arla Protein Pudding Vanilla Cookie (200g)",
         "Pathogen": "Glass fragments", "Date": "2026-10-24",
         "URL": "https://www.mpi.govt.nz/news/media-releases/arla-protein-"
                "pudding-recalled-due-to-possible-glass-fragments"}
MPI_B = {**MPI_A, "Company": "Hutchinsons Limited",
         "URL": "https://www.mpi.govt.nz/food-safety-home/food-recalls-and-"
                "complaints/recalled-food-products/arla-protein-pudding-"
                "vanilla-cookie"}


# ── the firm-name normaliser ───────────────────────────────────────────────

@pytest.mark.parametrize("a,b", [
    ("Hutchinsons Ltd", "Hutchinsons Limited"),
    ("Hutchinsons Ltd.", "Hutchinsons Limited"),
    ("Sabato Limited", "Sabato Ltd"),
    ("Boucherie Viau inc.", "Boucherie Viau Incorporated"),
    ("Arla Foods Ltd", "Arla Foods Limited"),
    ("CHEVENET SARL", "Chevenet"),
])
def test_two_legal_forms_of_one_name_agree(a, b):
    assert _company_base(a) == _company_base(b)
    assert _company_base(a)


@pytest.mark.parametrize("a,b", [
    ("Hutchinsons Ltd", "Hutchison Ltd"),
    ("Arla Foods Mayer Australia", "Arla Foods Ltd"),
    ("Greencore", "Greenyard"),
])
def test_two_different_firms_still_differ(a, b):
    assert _company_base(a) != _company_base(b)


def test_a_name_that_is_only_a_legal_form_makes_no_key():
    """"Ltd" on its own normalises to nothing. The key must then be empty
    rather than "", "", which would match every other such row."""
    for name in ("Ltd", "LLC", "GmbH", "Limited", "  Inc.  "):
        assert _company_base(name) == "", (name, _company_base(name))
        assert _same_firm_same_product_key(
            {"Source": "X", "Company": name, "Pathogen": "p",
             "Product": "q"}) == "", name


def test_an_abbreviation_split_by_dots_is_not_stripped():
    """"S.A." tokenises to ("s", "a"), neither of which is a legal-form
    token, so it survives as "sa". That is deliberate: the stripper only
    removes a WHOLE trailing token, so it can never eat part of a name."""
    assert _company_base("S.A.") == "sa"
    assert _company_base("Gardel SA") == "gardel"
    assert _company_base("Prestige de la Sarthe") == "prestigedelasarthe"


# ── the two keys cannot collide ────────────────────────────────────────────

def test_the_two_keys_live_in_one_index_without_colliding():
    a, b = _near_dup_key(MPI_A), _same_firm_same_product_key(MPI_A)
    assert a and b and a != b
    assert a.count("|") == 2 and b.count("|") == 3


# ── the defect itself ──────────────────────────────────────────────────────

def test_the_second_mpi_page_is_a_near_duplicate():
    idx = _build_near_dup_index([MPI_A])
    is_dup, matched = _is_near_duplicate(MPI_B, idx)
    assert is_dup, "one recall, two of the regulator's own pages, two rows"
    assert matched == "2026-10-24"


def test_it_works_in_either_order():
    idx = _build_near_dup_index([MPI_B])
    assert _is_near_duplicate(MPI_A, idx)[0]


def test_the_three_chevenet_fiches_are_not_suppressed():
    """Different products from one firm in one incident are separate
    regulatory actions and must all publish."""
    rows = [
        {"Source": "RappelConso (FR)", "Company": "CHEVENET SARL",
         "Product": "Satonnay 140 g (cheese)", "Date": "2026-07-31",
         "Pathogen": "Shiga toxin-producing E. coli (STEC)"},
        {"Source": "RappelConso (FR)", "Company": "Chevenet",
         "Product": "Tonnelet (small cask)", "Date": "2026-07-28",
         "Pathogen": "Shiga toxin-producing E. coli (STEC)"},
        {"Source": "RappelConso (FR)", "Company": "CHEVENET SARL",
         "Product": "Fresh goat cheese", "Date": "2026-07-29",
         "Pathogen": "Shiga toxin-producing E. coli (STEC)"},
    ]
    # The additive key alone must not merge them. (_near_dup_key DOES match
    # two of them — it always did, and that behaviour is unchanged here.)
    keys = {_same_firm_same_product_key(r) for r in rows}
    assert len(keys) == 3, keys


def test_a_recurrence_outside_the_window_is_not_a_duplicate():
    idx = _build_near_dup_index([MPI_A])
    later = {**MPI_B, "Date": "2026-12-24"}
    assert not _is_near_duplicate(later, idx)[0]
    assert NEAR_DUP_WINDOW_DAYS == 30


def test_the_published_register_holds_no_such_pair():
    """Measured assertion over the register.

    Only pairs the ADDITIVE key catches are asserted on. Pairs that
    _near_dup_key already matches are pre-existing and out of scope here —
    RASFF has many, because its Company is "Origin: X | Notifying: Y" and
    its Product is the notification subject, so two notifications about one
    commodity flow legitimately agree on both. Those were already caught by
    the three-field key before this one existed and are not new duplicates;
    asserting on them would make this test about RASFF's row format rather
    than about the defect it was written for.
    """
    import json
    from collections import defaultdict
    from datetime import datetime

    rows = json.loads((ROOT / "docs" / "data" / "recalls.json")
                      .read_text(encoding="utf-8"))

    def _pairs(keyfn):
        idx = defaultdict(list)
        for i, r in enumerate(rows):
            k, d = keyfn(r), str(r.get("Date") or "")[:10]
            if k and d:
                idx[k].append((i, d))
        out = set()
        for items in idx.values():
            for a in range(len(items)):
                for b in range(a + 1, len(items)):
                    try:
                        da = datetime.strptime(items[a][1], "%Y-%m-%d").date()
                        db = datetime.strptime(items[b][1], "%Y-%m-%d").date()
                    except ValueError:
                        continue
                    if abs((da - db).days) <= NEAR_DUP_WINDOW_DAYS:
                        out.add((items[a][0], items[b][0]))
        return out

    new = _pairs(_same_firm_same_product_key) - _pairs(_near_dup_key)
    assert not new, [
        (rows[a].get("Source"), rows[a].get("Company"),
         rows[b].get("Company"), str(rows[a].get("Product"))[:40],
         rows[a].get("URL"), rows[b].get("URL")) for a, b in sorted(new)[:4]]
