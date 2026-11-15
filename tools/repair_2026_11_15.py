# -*- coding: utf-8 -*-
"""Morning fix pass 2026-11-15. Base 8be2eb4b.

THE FINDING: A PRESS-RELEASE HEADLINE WAS PUBLISHED AS A FIRM AND A FOOD
=======================================================================
Recalls row 2, added by the DDGS gap-finder at 00:14 UTC today, promoted at
02:41 and published Tier 1 by 04:53:

    Date     2026-11-14                 Source   USDA FSIS
    Company  'FSIS Issues Public Health'
    Brand    'FSIS Issues Public Health'
    Product  'for Ready-to-Eat Meat ...'
    Pathogen 'Listeria monocytogenes'
    URL      fsis.usda.gov/recalls-alerts/fsis-issues-public-health-alert-
             ready-eat-meat-products-containing-biscuits-recalled

There is no firm called "FSIS Issues Public Health" and no food called "for
Ready-to-Eat Meat ...". The notice's own headline is "FSIS Issues Public
Health Alert for Ready-to-Eat Meat Products Containing Biscuits Recalled …".

WHERE IT CAME FROM. gap_finder_tavily._TITLE_SPLIT_RX looks for a recall VERB
in the title and cuts there. _RECALL_VERBS contains the bare word "alert",
which sits in the MIDDLE of every agency headline of the form "<AGENCY>
Issues Public Health Alert for <X>". Everything left of it became Company;
that module then sets `Brand=company`; everything right of it became Product.
The terminator alternation does contain "for", but the Product group is
`.+?` and needs at least one character first, so a "for" in first position
cannot end the group and the whole remainder was taken.

The 2026-09-16 audit removed the "whole headline as Company" FALLBACK for
this exact failure mode (the Sjømathuset row) and did not consider that a
MATCH splitting in the wrong place produces the same row by another route.
Nothing checked the match. It does now, in two places:

  * pipeline/gap_finder_tavily._split_is_a_headline — a cut whose left side
    has the agency as its sentence subject, or whose right side opens with a
    bare function word, is discarded and the row goes on with no Company and
    no Product, which the gate then refuses. One choke point: exa, ddgs,
    brave and daily_recall_search all import this function.
  * pipeline/_publish_gate._name_field_blockers_2026_10_19 — three new
    refusals of the same family (agency-subject headline in Company/Brand,
    function-word opener in Product, truncation ellipsis anywhere in a name
    field), so a row of this shape cannot be published whatever wrote it.

Both rules were measured against the whole corpus before being written.
Across 2,404 published rows, 16 Pending, 68 Weekly_Review, 730
Weekly_Rejected and 3,644 Rejected they match this row, its Weekly_Review
mirror, one more published row of the same family (fault B below), and in
the Rejected archive only debris already correctly refused. No false
positive.

WHAT THIS SCRIPT CAN AND CANNOT RESTORE. fsis.usda.gov will not open from
this sandbox: the alert page, /recalls and /food-safety/alerts each returned
a client error through the agent proxy, and the rules forbid routing around
a blocked fetch. So the firm is NOT invented here. Product is restored from
the row's OWN captured text, which quotes the release verbatim —

    "WASHINGTON, Nov. 14, 2026 – The U.S. Department of Agriculture's Food
     Safety and Inspection Service (FSIS) is issuing a public health alert
     for ready-to-eat meat products containing FDA-regulated biscuits that
     may be contaminated with Listeria monocytogenes (Lm). Additional
     downstream pr…"

— and Company takes the register's existing "not specified" sentinel shape
(Recalls row 121, "(not specified in AESAN alert 2026_79)"; row 467, "Not
named on the notice (importer; brand Grace)"). Brand is emptied: the row's
own text names no brand. The firm is left for the pipeline's own FSIS
scraper, which reaches the site from GitHub Actions, or for a reader who can
open the page. The recall itself is real, in scope and Tier 1, so the row
stays published rather than being archived over a field this pass could not
read.

B. THE SAME ELLIPSIS, PUBLISHED SINCE JUNE
   The new truncation rule found a second live instance on its first run.
   Recalls row 1453 (2026-06-24, AFSCA (BE), aflatoxin):

       Company  'Bestar Europe'
       Brand    'Bestar Europe | Agence fédérale pour la sécurité de ...'

   A past pass cleaned the regulator boilerplate out of Company — its Notes
   say so — and left the identical debris in Brand. _SITE_SUFFIX_RX strips a
   trailing "| Site Name" only when the tail is 40 characters or fewer, and
   "Agence fédérale pour la sécurité de ..." is longer, so the strip never
   fired. Brand is set to the firm's own name, which is in this row's
   Company and in its own URL slug (rappel-de-bestar-europe). Nothing else
   on the row is touched: its Product is vague but asserts no false fact,
   and the June notice is not something this pass read.

C. A GERMAN PRODUCT PUBLISHED THIS MORNING
   Recalls row 18 (2026-11-13, BLV (CH), Listeria, Aldi Suisse AG) was
   published with Product 'Mini-Hähnchenbrust-Filetstücke 150g'. It carried
   no "[needs cleanup: Product not in English]" stamp — the detector needs
   two non-English word hits and this is one hyphenated compound plus a
   weight. Translated from the row's own captured FSVO title,
   "Öffentliche Warnung: Listerien in Mini-Hähnchenbrust-Filetstücken 150g,
   verkauft bei ALDI SUISSE AG". The original is kept in Notes.

D. A CONSUMER-ADVICE ARTICLE SITTING IN PENDING AS "trusted"
   Pending row 6 (ANVISA (BR)) is
   gov.br/anvisa/en/updates/to-disinfect-produce-use-water-and-a-proper-
   solution-specifically-designed-for-food — an advice article, not a
   recall. It is already stamped Status='rejected' and the confirmer will
   evict it on its next --commit run; it is archived here so the workbook is
   consistent today. Idempotent: if main's confirm agent got there first
   this is a no-op.

   That row is also why tests/test_register_urls_pass_their_own_gate.py was
   RED on [br] this morning. That file excludes the Rejected sheets on
   purpose — "a rejected row's URL is often a listing page, and the gate
   refusing it is the gate working" — but counted a PENDING row already
   stamped rejected, which is the same thing. Fixed in the test, which is
   the guard that holds after this row is gone.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
TODAY = "2026-11-15"
BY = "morning fix pass 2026-11-15"

FSIS_URL = ("https://www.fsis.usda.gov/recalls-alerts/fsis-issues-public-"
            "health-alert-ready-eat-meat-products-containing-biscuits-"
            "recalled")
BESTAR_URL = "https://favv-afsca.be/fr/produits/rappel-de-bestar-europe"
BLV_URL = "https://www.blv.admin.ch/de/newnsb/BRX4VP3-xxIP"
ANVISA_URL = ("https://www.gov.br/anvisa/en/updates/to-disinfect-produce-"
              "use-water-and-a-proper-solution-specifically-designed-for-"
              "food")


# ── helpers ────────────────────────────────────────────────────────────────

def _hdr(ws):
    return [str(c.value).strip() if c.value is not None else ""
            for c in next(ws.iter_rows(min_row=1, max_row=1))]


def _find(ws, url):
    """Row numbers whose URL is `url`, trailing slash and case ignored."""
    hdr = _hdr(ws)
    if "URL" not in hdr:
        return []
    col = hdr.index("URL") + 1
    want = url.rstrip("/").lower()
    return [r for r in range(2, ws.max_row + 1)
            if str(ws.cell(row=r, column=col).value or "").strip()
            .rstrip("/").lower() == want]


def _get(ws, row, field):
    hdr = _hdr(ws)
    if field not in hdr:
        return ""
    v = ws.cell(row=row, column=hdr.index(field) + 1).value
    return "" if v is None else str(v)


def _set(ws, row, field, value):
    hdr = _hdr(ws)
    if field not in hdr:
        return False
    cell = ws.cell(row=row, column=hdr.index(field) + 1)
    if str(cell.value or "") == str(value):
        return False
    cell.value = value
    return True


def _note(ws, row, text):
    old = _get(ws, row, "Notes")
    if text in old:
        return False
    return _set(ws, row, "Notes", (old + " " + text).strip())


def _same(a, b) -> bool:
    """Field compare, case- and whitespace-insensitive."""
    return " ".join(str(a or "").split()).casefold() == \
           " ".join(str(b or "").split()).casefold()


def _row_dict(ws, row):
    return {h: _get(ws, row, h) for h in _hdr(ws) if h}


def _archive(wb, row: dict, reason: str) -> None:
    """Append to Rejected. The archive is append-only; nothing is deleted."""
    arch = wb["Rejected"]
    out = []
    for h in _hdr(arch):
        if h in ("RejectReason", "RejectionReason"):
            out.append(reason)
        elif h == "RejectedBy":
            out.append(BY)
        elif h == "RejectedAt":
            out.append(TODAY)
        elif h == "Status":
            out.append("rejected")
        else:
            v = row.get(h, "")
            out.append("" if v is None else v)
    arch.append(out)


# ── A — the FSIS press-release headline, in Recalls and Weekly_Review ──────

_FSIS_BAD_NAME = "FSIS Issues Public Health"
_FSIS_BAD_PRODUCT = "for Ready-to-Eat Meat ..."
_FSIS_COMPANY = "(not named in the FSIS text held on this row)"
_FSIS_PRODUCT = "Ready-to-eat meat products containing FDA-regulated biscuits"
_FSIS_NOTE = (
    "[morning fix pass 2026-11-15: this row was published with Company and "
    "Brand 'FSIS Issues Public Health' and Product 'for Ready-to-Eat Meat "
    "...' — the two halves of the notice's own headline, cut by "
    "gap_finder_tavily._TITLE_SPLIT_RX on the bare word 'Alert' inside it. "
    "Product is restored from THIS ROW'S OWN captured text, which quotes the "
    "release: 'is issuing a public health alert for ready-to-eat meat "
    "products containing FDA-regulated biscuits that may be contaminated "
    "with Listeria monocytogenes (Lm)'. Brand is emptied: that text names no "
    "brand. COMPANY IS NOT KNOWN AND IS NOT GUESSED — fsis.usda.gov would "
    "not open from this pass (the alert page, /recalls and "
    "/food-safety/alerts each returned a client error through the agent "
    "proxy), so the recalling firm has not been read by anyone. The "
    "pipeline's own FSIS scraper reaches the site from GitHub Actions; the "
    "firm should be filled from the notice. The hazard, the date and the "
    "authority URL are the row's own and are unchanged.]")


def repair_the_fsis_headline_row(wb) -> int:
    n = 0
    for sheet in ("Recalls", "Weekly_Review"):
        if sheet not in wb.sheetnames:
            continue
        ws = wb[sheet]
        for r in _find(ws, FSIS_URL):
            if _same(_get(ws, r, "Company"), _FSIS_BAD_NAME):
                _set(ws, r, "Company", _FSIS_COMPANY)
                n += 1
            if _same(_get(ws, r, "Brand"), _FSIS_BAD_NAME):
                _set(ws, r, "Brand", "")
                n += 1
            if _same(_get(ws, r, "Product"), _FSIS_BAD_PRODUCT):
                _set(ws, r, "Product", _FSIS_PRODUCT)
                n += 1
            if n:
                _note(ws, r, _FSIS_NOTE)
            print("  %s row %d: Company=%r Brand=%r Product=%r"
                  % (sheet, r, _get(ws, r, "Company"), _get(ws, r, "Brand"),
                     _get(ws, r, "Product")))
    return n


# ── B — the Belgian brand with the agency name and an ellipsis in it ───────

_BESTAR_BAD = "Bestar Europe | Agence fédérale pour la sécurité de ..."
_BESTAR_NOTE = (
    "[morning fix pass 2026-11-15: Brand held 'Bestar Europe | Agence "
    "fédérale pour la sécurité de ...' — the firm's name, the FAVV/AFSCA "
    "site-name suffix the CMS appends to its <title>, and a truncation "
    "ellipsis. _SITE_SUFFIX_RX strips such a suffix only when it is 40 "
    "characters or fewer and this one is longer, so it survived; the same "
    "debris was cleaned out of Company by an earlier pass and left here. "
    "Brand set to the firm's own name, which is this row's Company and its "
    "own URL slug (rappel-de-bestar-europe). No other field is touched: the "
    "June notice was not read by this pass. "
    "[original brand: Bestar Europe | Agence fédérale pour la sécurité de "
    "...]]")


def repair_the_bestar_brand(wb) -> int:
    ws = wb["Recalls"]
    n = 0
    for r in _find(ws, BESTAR_URL):
        if _same(_get(ws, r, "Brand"), _BESTAR_BAD):
            _set(ws, r, "Brand", "Bestar Europe")
            _note(ws, r, _BESTAR_NOTE)
            n += 1
        print("  Recalls row %d: Brand=%r" % (r, _get(ws, r, "Brand")))
    return n


# ── C — the German Product published this morning ──────────────────────────

_BLV_DE = "Mini-Hähnchenbrust-Filetstücke 150g"
_BLV_EN = "Mini chicken breast fillet pieces, 150 g"
_BLV_NOTE = (
    "[morning fix pass 2026-11-15: Product was published in German and "
    "carried no '[needs cleanup: Product not in English]' stamp — the "
    "detector needs two non-English word hits and this is one hyphenated "
    "compound plus a weight, which is the short-name blind spot the standing "
    "instruction to read every Product published in the last 26 h exists "
    "for. Translated from this row's own captured FSVO title, 'Öffentliche "
    "Warnung: Listerien in Mini-Hähnchenbrust-Filetstücken 150g, verkauft "
    "bei ALDI SUISSE AG'. The weight is the notice's own; no lot, species or "
    "pack detail is added. [original product: "
    "Mini-Hähnchenbrust-Filetstücke 150g]]")


def translate_the_blv_product(wb) -> int:
    n = 0
    for sheet in ("Recalls", "Weekly_Review"):
        if sheet not in wb.sheetnames:
            continue
        ws = wb[sheet]
        for r in _find(ws, BLV_URL):
            if _same(_get(ws, r, "Product"), _BLV_DE):
                _set(ws, r, "Product", _BLV_EN)
                _note(ws, r, _BLV_NOTE)
                n += 1
            print("  %s row %d: Product=%r"
                  % (sheet, r, _get(ws, r, "Product")))
    return n


# ── C2 — the OTHER German Product, published two days ago ─────────────────

BLV2_URL = "https://www.recallswiss.admin.ch/customer-access/#Recalls/1191"
_BLV2_DE = ("Mini-Hähnchenbrust-Filetstücke; Mini Klassik, Mini Paprika und "
            "Mini Curry")
_BLV2_EN = ("Mini chicken breast fillet pieces, in the Mini Klassik, Mini "
            "Paprika and Mini Curry varieties")
_BLV2_NOTE = (
    "[morning fix pass 2026-11-15: a second German Product on the same Aldi "
    "Suisse Listeria recall, this one the FSVO's Recalls/1191 record, "
    "published 2026-11-13 and so OUTSIDE the 26-hour window the standing "
    "instruction names — which is how the by-hand read missed it too. It "
    "surfaced because the 13 November daily brief still rendered it in "
    "German. Translated from this row's own captured FSVO title, 'Aldi "
    "Suisse AG ruft das Produkt «Mini-Hähnchenbrust-Filetstücke» in den "
    "Sorten «Mini Klassik, Mini Paprika und Mini Curry» wegen Nachweis auf "
    "Listerien zurück'. The three variety names are the notice's own and are "
    "kept as written. pipeline/_language.product_needs_english returned "
    "False on this string — its Latin-script detectors were French-only — "
    "and is widened to Germanic and Nordic food words in the same pass, "
    "measured at one hit and no false positive across all 2,404 published "
    "Products. [original product: Mini-Hähnchenbrust-Filetstücke; Mini "
    "Klassik, Mini Paprika und Mini Curry]]")


def translate_the_second_blv_product(wb) -> int:
    n = 0
    for sheet in ("Recalls", "Weekly_Review"):
        if sheet not in wb.sheetnames:
            continue
        ws = wb[sheet]
        for r in _find(ws, BLV2_URL):
            if _same(_get(ws, r, "Product"), _BLV2_DE):
                _set(ws, r, "Product", _BLV2_EN)
                _note(ws, r, _BLV2_NOTE)
                n += 1
            print("  %s row %d: Product=%r"
                  % (sheet, r, _get(ws, r, "Product")))
    return n


# ── D — the ANVISA consumer-advice article in Pending ──────────────────────

_ANVISA_REASON = (
    "Not a recall: gov.br/anvisa/en/updates/to-disinfect-produce-use-water-"
    "and-a-proper-solution-specifically-designed-for-food is a consumer-"
    "advice article ('To disinfect produce, use water and a proper solution "
    "specifically designed for food'). It names no product, no firm and no "
    "regulatory action, and it was already stamped Status='rejected' in "
    "Pending by a reviewer. Archived by the morning fix pass 2026-11-15 so "
    "the workbook does not carry a row the system does not stand behind in a "
    "sheet that means it does; the confirmer evicts such rows on its next "
    "--commit run and this is the same eviction, made today.")


def archive_the_anvisa_advice_row(wb) -> int:
    ws = wb["Pending"]
    rows = _find(ws, ANVISA_URL)
    if not rows:
        print("  Pending: the ANVISA advice row is already gone — no-op")
        return 0
    n = 0
    for r in sorted(rows, reverse=True):
        status = _get(ws, r, "Status").strip().lower()
        if status != "rejected":
            print("  Pending row %d: Status is %r, not 'rejected' — left "
                  "alone, this pass only finalises a verdict already made"
                  % (r, status))
            continue
        _archive(wb, _row_dict(ws, r), _ANVISA_REASON)
        ws.delete_rows(r, 1)
        n += 1
        print("  Pending row %d archived to Rejected" % r)
    return n


# ── main ───────────────────────────────────────────────────────────────────

def main() -> int:
    import openpyxl

    wb = openpyxl.load_workbook(XLSX)
    print("A. the FSIS press-release headline published as a firm and a food")
    repair_the_fsis_headline_row(wb)
    print("B. the Belgian brand carrying the agency name and an ellipsis")
    repair_the_bestar_brand(wb)
    print("C. the German Product published this morning")
    translate_the_blv_product(wb)
    print("C2. the second German Product, on the FSVO's other record")
    translate_the_second_blv_product(wb)
    print("D. the ANVISA consumer-advice article in Pending")
    archive_the_anvisa_advice_row(wb)
    wb.save(XLSX)
    print("saved %s" % XLSX)

    # E. THE DASHBOARD READS THE MIRROR, NOT THE WORKBOOK.
    # advfood.tech/fsis-recalls reads docs/data/recalls.json. Every repair
    # script in tools/ ends with this call and this one did not, which was
    # measured before it was fixed: after A-D above, recalls.json still
    # carried 'FSIS Issues Public Health' three times and the German
    # Product — eight field differences on the three repaired rows, with
    # the whole suite green. tools/rebase_and_verify.rebuild_register
    # claims in its own docstring to regenerate this file and does not;
    # that is fixed in the same pass, and tests/test_the_dashboard_mirror_
    # agrees_with_the_register.py now makes the drift visible instead of
    # leaving it to each script's memory.
    from pipeline.merge_master import mirror_json_from_xlsx
    n = mirror_json_from_xlsx(XLSX, XLSX.parent / "recalls.json")
    print("recalls.json re-mirrored: %d rows" % n)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
