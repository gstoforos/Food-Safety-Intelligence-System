# -*- coding: utf-8 -*-
"""A scraper may not fall back to the product or the title for Company.

FOUND 2026-10-29, twice, from two different angles.

1. Recalls  2026-10-28  FSANZ (AU)
       Company = Brand = Product = "Bomberry Various Products – 150g"
   The published register named a company that does not exist. FSANZ
   titles are "Company - Product", this one was "Product – size" with an
   en dash and no company part at all, so the " - " split left `company`
   empty and scrapers/oceania/fsanz.py did:

       Company=company or product[:120]

   The page's own supplier is "Atomic Changes Pty Ltd t/a Bomberry", and
   it is in the body, not the title.

2. Pending  FDA
       Company = "Wedgewood Pharmacy Issues Recall Certain Compounded
                  Veterinary Medications Within Expiry Due"
       Company = "Tonight We Ride Llc Issues Voluntary Recall Ultimate
                  Forza Male Supplement Due Presence Undeclared"
   scrapers/north_america/fda_press.py did:

       Company=title.split(" Recalls ")[0][:100]

   FDA writes the firm first and the verb second, but " Recalls " is only
   one of a dozen verb phrases it uses. On any other, the split returned
   the whole title.

Both are the same defect: when the firm cannot be parsed, invent one from
text that is not a firm name. Fabricating a field value is the one
unforgivable error in this register, and an empty Company is already
handled — the publish gate refuses it ("Company is empty") and the row
waits in Pending for a reviewer to read the page. A day's delay is the
correct price; a fake firm name is not.

This test reads the SOURCE, not the data, because the data only shows the
instances that happen to have been collected. It is deliberately narrow:
it fires on a Company= keyword whose value falls back to a
product/title/summary/headline/description expression, and it allows a
fallback to a brand or a firm-ish name, which is a real name of a real
party. The ALLOWED set is for a scraper whose source genuinely names the
firm in a field this test cannot tell apart; an entry needs a reason and
a date.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRAPERS = ROOT / "scrapers"

#: Words that mark an expression as NOT a firm name.
BAD_WORDS = ("product", "title", "summary", "headline", "description",
             "subject", "reason", "text", "name_en", "item_name")

#: A fallback to one of these is fine — they are names of real parties.
OK_WORDS = ("company", "firm", "brand", "co", "_co", "supplier",
            "manufacturer", "distributor", "recalling", "operator")

#: path -> reason + date. Empty on purpose.
ALLOWED: dict[str, str] = {}


def _source_files():
    return sorted(p for p in SCRAPERS.rglob("*.py")
                  if "__pycache__" not in p.parts and "_attic" not in p.parts)


def _leaf_names(node: ast.AST) -> list[str]:
    """Every identifier-ish token in an expression, lowercased."""
    out: list[str] = []
    for sub in ast.walk(node):
        if isinstance(sub, ast.Name):
            out.append(sub.id.lower())
        elif isinstance(sub, ast.Attribute):
            out.append(sub.attr.lower())
        elif isinstance(sub, ast.Constant) and isinstance(sub.value, str):
            out.append(sub.value.lower())
    return out


def _looks_like_a_firm(node: ast.AST) -> bool:
    names = _leaf_names(node)
    if any(any(w == n or n.endswith("_" + w) or n.startswith(w + "_")
               for w in OK_WORDS) for n in names):
        return True
    return not any(bad in n for n in names for bad in BAD_WORDS)


def _offending_fallbacks(tree: ast.AST) -> list[tuple[int, str]]:
    """Company=<x> or <y> where <y> is product/title-ish."""
    bad: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.keyword) or node.arg != "Company":
            continue
        value = node.value
        # Company=(a or b)[:120] — unwrap the slice first.
        while isinstance(value, ast.Subscript):
            value = value.value
        if not isinstance(value, ast.BoolOp) or not isinstance(value.op,
                                                               ast.Or):
            continue
        for alt in value.values[1:]:
            if _looks_like_a_firm(alt):
                continue
            bad.append((getattr(node, "lineno", 0),
                        ast.unparse(node.value)[:120]))
            break
    return bad


@pytest.mark.parametrize("path", _source_files(),
                         ids=lambda p: str(p.relative_to(ROOT)))
def test_company_never_falls_back_to_the_product(path: Path):
    rel = str(path.relative_to(ROOT))
    if rel in ALLOWED:
        pytest.skip("%s: %s" % (rel, ALLOWED[rel]))
    tree = ast.parse(path.read_text(encoding="utf-8"))
    bad = _offending_fallbacks(tree)
    assert not bad, (
        "%s falls back to the product/title for Company:\n%s\n\n"
        "A firm name that cannot be parsed must be left EMPTY. The publish "
        "gate refuses an empty Company and the row waits in Pending for a "
        "reviewer to read the page — which is the design. Copying the "
        "product, the recall title or the page summary into Company "
        "publishes a firm that does not exist."
        % (rel, "\n".join("  line %d: Company=%s" % b for b in bad)))


def test_fda_press_firm_parsing():
    """The FDA title shapes that produced the two Pending rows."""
    import sys
    if str(ROOT) not in sys.path:                           # pragma: no cover
        sys.path.insert(0, str(ROOT))
    from scrapers.north_america.fda_press import _firm_from_title

    cases = {
        "Haifa Smoked Fish Recalls Cold Smoked Sprats Due Potential "
        "Health Risk": "Haifa Smoked Fish",
        "Wedgewood Pharmacy Issues Recall Certain Compounded Veterinary "
        "Medications Within Expiry Due": "Wedgewood Pharmacy",
        "Tonight We Ride Llc Issues Voluntary Recall Ultimate Forza Male "
        "Supplement Due Presence Undeclared": "Tonight We Ride Llc",
        "Acme Foods Announces Nationwide Voluntary Recall Of Frozen Peas":
            "Acme Foods",
        "Acme Foods Expands Recall Of Frozen Peas": "Acme Foods",
        "Acme Foods Voluntarily Recalls Frozen Peas": "Acme Foods",
        "Acme Foods Issues Public Health Alert For Frozen Peas":
            "Acme Foods",
    }
    for title, firm in cases.items():
        assert _firm_from_title(title) == firm, title

    # No verb boundary at all: EMPTY, never the title.
    for title in ("Outbreak Investigation Of Salmonella In Pistachio Cream",
                  "Frozen Peas 500G", "", "Recalls"):
        assert _firm_from_title(title) == "", title
