# -*- coding: utf-8 -*-
"""Morning fix, 2026-10-12: six Pending rows, one of them a fabricated URL.

Run on a FRESH clone of main, by tools/rebase_and_verify.py, which then
runs pipeline.promote_gate_passing --apply behind it.

NOTHING LEAVES Recalls. Every removal here is from Pending, and every
removed Pending row is written into the append-only Rejected archive with
a reason and a rejecter first. One row is filled and will publish; four are
archived; one gets a Brand it already stated and stays in Pending because
nothing names its firm.

Every fact below was read on an authority page on 2026-10-12, and each
row's Notes say which page.


A. FSAI (IE) SUPERVALU SMOKED SALMON — A REAL TIER-1 RECALL, AND A URL
   NOBODY COULD READ
=======================================================================
Two Pending rows, same recall, two URLs::

  98 chars  .../food-alerts/recall-of-specific-batches-of-various-supervalu-sm
 164 chars  .../food-alerts/recall-of-specific-batches-of-various-supervalu-
            smoked-salmon-products-due-to-the-presence-of-listeria-monocytogenes

The long one carries "[url-agent 2026-10-12: pending_enrichment→
pending_gap_v2 URL confirmed]". It is not a page.

FSAI's own alert listing publishes its slugs TRUNCATED AT 49 CHARACTERS.
Read on https://www.fsai.ie/news-and-alerts/all-alerts (2026-10-12), the
link for this alert reads::

    11 Oct  Recall of specific batches of SuperValu Smoked Salmon products
            /News-and-Alerts/Food-Alerts/Recall-of-specific-batches-of-various-SuperValu-Sm

and the two alerts above it are truncated the same way
("...-of-various-ready-to-eat-products", "...-Wrights-of-Mario-Oak-Sliced-S"
— FSAI's own URL says "Mario" where its link text says "Marino"). A
search for the long slug returns the 98-character page. So the short URL
is the regulator's own href and the canonical address; the long one is the
headline glued back on by reviewer 1.

That is the Macroom shape, which pipeline/_url_guard.py already refuses on
two signals — a collector Notes stamp, and a `source_id=` to measure the
proposal against. An FSAI HTML-listing-fallback row carries neither, so it
went through. Both holes are closed in code in this same upload
(_url_guard rules 1 and 2b, tests/test_url_overwrite_guard.py).

The 98-character page was read in full on 2026-10-12. Everything written
below is on it:

    title      Recall of specific batches of SuperValu Smoked Salmon
               products due to the presence of Listeria monocytogenes
    published  Sunday, 11 October 2026
    brand      SuperValu
    products   SuperValu Smoked Salmon 40 g; SuperValu Smoked Salmon 150 g
    approval   IE DN0012 EU
    batches    1027992 1 0 (40 g); 1027989 6 0 (150 g)
    use-by     17/10/26
    hazard     the presence of Listeria monocytogenes

Company and Product on the row were both the listing TITLE — "SuperValu
Smoked Salmon products" — which is why the gate read a product as a firm.
Company and Brand become SuperValu (the alert names no separate producer;
the approval number IE DN0012 EU is recorded in Notes, not guessed into a
firm name). Date, Pathogen, Product and Reason are filled from the page.


B. THE FABRICATED COPY
======================
The 164-character row is removed from Pending and archived. Its copy in
Weekly_Rejected — archived by the confirmer for "row was at pending_gap_v2
(not reviewed by reviewer 2) and Date is empty", which was true but not the
real reason — gets the URL verdict appended so the archive does not read as
a date problem.


C. LIVSMEDELSVERKET (SE) — A PRESS RELEASE, NOT A RECALL
========================================================
Pending since 2026-10-12 05:13 with Company, Brand, Product, Pathogen and
Reason ALL empty and Date 2026-09-30. Its URL is::

    https://www.livsmedelsverket.se/om-oss/press/pressmeddelanden/
    mer-livsmedelskontroll-under-2025/

"Mer livsmedelskontroll under 2025" — more food inspection during 2025. A
corporate press release about inspection volumes, discovered via news
("Discovered via news: livsmedelsverket.se"), on the /om-oss/press/
path rather than /aterkallade-varor. R2 fails: there is no regulatory
action on a product. Archived.


D. FDA VIA GOOGLE NEWS — PASTA — THE URL IS A CLOSED 2025 OUTBREAK
==================================================================
Pending with Date 2026-10-08, Company empty, and Product and Reason both
the news headline "Pasta sold in NY recalled over possible Listeria
contamination: FDA - PIX11". Its URL is::

    https://www.fda.gov/food/outbreaks-foodborne-illness/
    outbreak-investigation-listeria-monocytogenes-prepared-pasta-meals-june-2025

That is the FDA's JUNE 2025 prepared-pasta-meals investigation, which was
declared over in February 2026. It is an FDA page, which is why the
url-guard rightly refused to let reviewer 1 throw the row away for "No
official regulator URL found" — and it is not the notice for an October
2026 New York recall. The row has been round the lane twice
(pending_gap→v1→v2, "URL confirmed" both times) and the publish gate holds
it on an empty Company, so it cannot leave Pending in either direction.

Archived on the URL, not on the Company: R3 wants the regulator's own
per-recall notice and this is a different recall's page. No replacement row
is written — no FDA notice for an October 2026 New York pasta recall was
found on fda.gov, and a headline is not a recall.


E. FDA VIA GOOGLE NEWS — FROZEN BERRIES — ALREADY PUBLISHED
===========================================================
Pending with Date 2026-10-06 and Product/Reason the headline "Frozen fruit
recall expands: E.coli risk for Walmart Great Value & Trader Joe's
berries. Check your freezer! - Facebook". The recall itself is in Recalls
already::

    2026-10-02  FDA  Frutas y Hortalizas del Sur S.A.
                Organic Blueberries, Triple Berry Blend and Mixed Berry Blend
                Shiga toxin-producing E. coli (STEC)
                fda.gov/safety/recalls-market-withdrawals-safety-alerts/
                frutas-y-hortalizas-del-sur-sa-expands-recalls-...

Confirmed on the FDA's own outbreak page (read 2026-10-12, last updated
2026-10-05): the 2 October 2026 expansion is Great Value Organic Triple
Berry Blend, Great Value Organic Blueberries and Trader Joe's Organic
Mixed Berry Blend, all from Frutas y Hortalizas del Sur S.A.

So this is a duplicate that dedup could not see, because the published row
is keyed on the FDA's per-recall notice and this one on the FDA's outbreak
investigation page — two URLs, one recall. Archived as a duplicate naming
the published row. The row it duplicates is untouched.


F. USDA FSIS PHA-10062026-01 — BRAND FROM ITS OWN TEXT, COMPANY LEFT EMPTY
==========================================================================
Pending with Company empty, Brand "—", and Product::

    •  140-g. cans of "GRACE READY TO EAT CHICKEN VIENNA SAUSAGES."
    •  140-g. cans of "GRACE READY TO EAT CHICKEN VIENNA SAUSAGES HOT & SPICY."

Brand is on the row's own text — GRACE — and is filled. COMPANY IS LEFT
EMPTY ON PURPOSE. The alert's own Reason, as the FSIS API gave it, is an
import-eligibility action: product "illegally imported from Jamaica, a
country that is not eligible to export meat or poultry products to the
United States. FSIS is continuing to investigate how these products
entered the country." These alerts routinely name no recalling firm, and
www.fsis.usda.gov refuses reads from this sandbox (Akamai; the pipeline
reaches it from Actions via scrapers/_akamai_fetch.py), so the page could
not be checked for one. Writing "Grace Foods" would be inventing a field.

The row therefore stays in Pending, blocked on Company, which is the
honest state and a decision for the operator: whether the publish gate
should accept an empty Company for an import-eligibility alert whose Brand
is known. Its copy in Rejected — archived with RejectedBy "unknown", no
RejectedAt and no RejectReason — is annotated UNRESOLVED so the archive
stops reading as a verdict nobody made.
"""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "recalls.json"

TODAY = "2026-10-12"
BY = "morning fix " + TODAY
OP = f"[morning fix {TODAY}:"

# ── the six rows, keyed on URL ─────────────────────────────────────────────

FSAI_REAL = ("https://www.fsai.ie/news-and-alerts/food-alerts/"
             "recall-of-specific-batches-of-various-supervalu-sm")
FSAI_FABRICATED = ("https://www.fsai.ie/news-and-alerts/food-alerts/"
                   "recall-of-specific-batches-of-various-supervalu-smoked-"
                   "salmon-products-due-to-the-presence-of-listeria-"
                   "monocytogenes")
SE_PRESS = ("https://www.livsmedelsverket.se/om-oss/press/pressmeddelanden/"
            "mer-livsmedelskontroll-under-2025/")
FDA_PASTA = ("https://www.fda.gov/food/outbreaks-foodborne-illness/"
             "outbreak-investigation-listeria-monocytogenes-prepared-pasta-"
             "meals-june-2025")
FDA_BERRIES = ("https://www.fda.gov/food/outbreaks-foodborne-illness/"
               "outbreak-investigation-e-coli-o145h28-frozen-blueberries-"
               "july-2026")
FSIS_PHA = ("https://www.fsis.usda.gov/recalls-alerts/fsis-issues-public-"
            "health-alert-ineligible-chicken-vienna-style-sausage-products")

#: A. Fields written onto the real FSAI row. A trailing "!" means "write it
#: even though the cell is not empty" — used only where the cell holds
#: scrape debris the authority page contradicts (Company and Product were
#: both the listing title).
FSAI_FILL = {
    "Date": "2026-10-11",
    "Company!": "SuperValu",
    "Brand!": "SuperValu",
    "Product!": ("SuperValu Smoked Salmon 40 g and SuperValu Smoked Salmon "
                 "150 g — batch codes 1027992 1 0 (40 g) and 1027989 6 0 "
                 "(150 g), use-by 17/10/26"),
    "Pathogen": "Listeria monocytogenes",
    "Reason!": "Presence of Listeria monocytogenes",
    "note": ("read in full on the FSAI alert page 2026-10-11 "
             "(fsai.ie/News-and-Alerts/Food-Alerts/Recall-of-specific-"
             "batches-of-various-SuperValu-Sm): published Sunday 11 October "
             "2026; brand SuperValu; SuperValu Smoked Salmon 40 g and 150 g; "
             "approval number IE DN0012 EU; batches 1027992 1 0 and "
             "1027989 6 0; use-by 17/10/26; hazard the presence of Listeria "
             "monocytogenes. Company and Product previously held the listing "
             "title 'SuperValu Smoked Salmon products' — a product read as a "
             "firm. The alert names no producer other than the brand, so "
             "Company is the brand and the approval number is recorded here "
             "rather than guessed into a firm name. FSAI publishes its own "
             "alert slugs truncated at 49 characters, so this 98-character "
             "URL is the canonical page]"),
}

#: B–E. Pending rows archived, with the reason the archive will carry.
ARCHIVE = {
    FSAI_FABRICATED: (
        "fabricated_url: the slug is the alert HEADLINE appended to FSAI's own "
        "truncated slug and is not a page. FSAI publishes its alert slugs cut "
        "at 49 characters — verified 2026-10-12 on "
        "fsai.ie/news-and-alerts/all-alerts, which links this alert as "
        "'...-of-various-SuperValu-Sm', and a search for the long slug returns "
        "that 98-character page. Written by reviewer 1 on 2026-10-12 "
        "('URL confirmed') over the href the FSAI HTML listing fallback had "
        "read from the regulator's markup. The recall itself is real and is "
        "published from the correct URL: " + FSAI_REAL + " . Guard holes "
        "closed in pipeline/_url_guard.py (rules 1 and 2b) in the same "
        "upload."),
    SE_PRESS: (
        "not_a_recall: 'Mer livsmedelskontroll under 2025' (more food "
        "inspection during 2025) is a Livsmedelsverket PRESS RELEASE about "
        "inspection volumes, on the /om-oss/press/pressmeddelanden/ path, not "
        "a product action on /aterkallade-varor. Discovered via news with "
        "Company, Brand, Product, Pathogen and Reason all empty — there is no "
        "recall on the page to fill them from. R2 (a real regulatory action) "
        "fails."),
    FDA_PASTA: (
        "wrong_url: the row is an October 2026 news headline ('Pasta sold in "
        "NY recalled over possible Listeria contamination: FDA - PIX11', "
        "Date 2026-10-08) attached to the FDA's JUNE 2025 prepared-pasta-meals "
        "Listeria outbreak investigation, which was declared over in February "
        "2026. An FDA page, which is why the url-guard refused to let "
        "reviewer 1 reject it as 'No official regulator URL found' — but not "
        "this recall's notice. R3 (the regulator's own per-recall URL) fails. "
        "No FDA notice for an October 2026 New York pasta recall was found on "
        "fda.gov on 2026-10-12, so no replacement row is written: a headline "
        "is not a recall."),
    FDA_BERRIES: (
        "duplicate: already published as Recalls 2026-10-02, FDA, 'Frutas y "
        "Hortalizas del Sur S.A.', 'Organic Blueberries, Triple Berry Blend "
        "and Mixed Berry Blend', Shiga toxin-producing E. coli (STEC), from "
        "the FDA's own per-recall notice "
        "(fda.gov/safety/recalls-market-withdrawals-safety-alerts/"
        "frutas-y-hortalizas-del-sur-sa-expands-recalls-...). This copy is "
        "keyed on the FDA OUTBREAK INVESTIGATION page instead, so dedup saw "
        "two URLs for one recall. Confirmed on that outbreak page (read "
        "2026-10-12, last updated 2026-10-05): the 2 October 2026 expansion "
        "is Great Value Organic Triple Berry Blend, Great Value Organic "
        "Blueberries and Trader Joe's Organic Mixed Berry Blend, all Frutas y "
        "Hortalizas del Sur S.A. Product and Reason on this row are a "
        "Facebook-sourced headline. The published row is untouched."),
}

#: F. Brand from the row's own Product text. Company stays empty.
FSIS_FILL = {
    "Brand!": "Grace",
    "note": ("Brand taken from the row's own Product text ('GRACE READY TO "
             "EAT CHICKEN VIENNA SAUSAGES'). COMPANY DELIBERATELY LEFT "
             "EMPTY: PHA-10062026-01 is an import-eligibility alert — "
             "product 'illegally imported from Jamaica, a country that is "
             "not eligible to export meat or poultry products to the United "
             "States. FSIS is continuing to investigate how these products "
             "entered the country' — and such alerts routinely name no "
             "recalling firm. www.fsis.usda.gov refused reads from the fix "
             "sandbox on 2026-10-12 (Akamai; the pipeline reaches it from "
             "Actions via scrapers/_akamai_fetch.py), so the page could not "
             "be checked for one. The row stays in Pending blocked on "
             "Company rather than carry an invented firm — operator decision "
             "pending on whether the gate should accept an empty Company for "
             "an import-eligibility alert with a known Brand]"),
}

#: F. The reasonless archive copy of the same alert.
FSIS_ARCHIVE_NOTE = (
    " | UNRESOLVED (morning fix 2026-10-12): this archive row carries "
    "RejectedBy 'unknown' with no RejectedAt and no RejectReason — a verdict "
    "nobody recorded. The alert is live (FSIS PHA-10062026-01, 2026-10-06) "
    "and sits in Pending blocked on an empty Company, which the alert itself "
    "may not name. Neither published nor refused; do not read this row as a "
    "rejection.")


# ── helpers ────────────────────────────────────────────────────────────────

def _head(ws):
    return [str(c.value) if c.value is not None else "" for c in ws[1]]


def _archive(wb, row: dict, reason: str) -> None:
    """Append to Rejected. The archive is append-only; nothing is deleted."""
    arch = wb["Rejected"]
    out = []
    for h in _head(arch):
        if h in ("RejectReason", "RejectionReason"):
            out.append(reason)
        elif h == "RejectedBy":
            out.append(BY)
        elif h == "RejectedAt":
            out.append(TODAY)
        elif h == "Status":
            out.append("rejected")
        else:
            out.append(row.get(h, ""))
    arch.append(out)


def _fill(ws, url: str, spec: dict) -> int:
    """Write `spec` onto the Pending row whose URL is `url`.

    A key ending in "!" overwrites a non-empty cell; anything else is
    written only into an empty one. Tier is recomputed, never hand-set.
    """
    head = _head(ws)
    col = {name: i for i, name in enumerate(head)}
    for row in ws.iter_rows(min_row=2):
        if str(row[col["URL"]].value or "").strip() != url:
            continue
        changed = []
        for key, val in spec.items():
            if key == "note":
                continue
            force = key.endswith("!")
            field = key.rstrip("!")
            cell = row[col[field]]
            cur = str(cell.value or "").strip()
            if force or not cur or cur in ("None", "nan", "—"):
                if str(cell.value or "") != val:
                    cell.value = val
                    changed.append(field)
        if not changed:
            print(f"  {url[:60]}…: already filled, left alone")
            return 0
        from scrapers._models import assign_tier
        try:
            outbreak = 1 if int(row[col["Outbreak"]].value or 0) else 0
        except (TypeError, ValueError):
            outbreak = 0
        tier = assign_tier(str(row[col["Pathogen"]].value or ""), outbreak)
        if str(row[col["Tier"]].value) != str(tier):
            row[col["Tier"]].value = tier
            changed.append("Tier")
        row[col["Notes"]].value = (str(row[col["Notes"]].value or "")
                                   + f" {OP} filled {', '.join(changed)} — "
                                   + spec["note"]).strip()
        print(f"  {url[:60]}…: {', '.join(changed)}")
        return 1
    print(f"  {url[:60]}…: not in Pending (already handled upstream)")
    return 0


# ── sections ───────────────────────────────────────────────────────────────

def fill_fsai(wb) -> int:
    """A. The real FSAI row, from the page read on 2026-10-12."""
    return _fill(wb["Pending"], FSAI_REAL, FSAI_FILL)


def fill_fsis_brand(wb) -> int:
    """F. Brand only. Company stays empty — see the module docstring."""
    return _fill(wb["Pending"], FSIS_PHA, FSIS_FILL)


def archive_pending(wb) -> int:
    """B–E. Four Pending rows out, four Rejected rows in. Recalls untouched."""
    ws = wb["Pending"]
    head = _head(ws)
    ucol = head.index("URL") + 1
    n = 0
    for r in range(ws.max_row, 1, -1):
        url = str(ws.cell(r, ucol).value or "").strip()
        reason = ARCHIVE.get(url)
        if not reason:
            continue
        row = {h: ws.cell(r, i + 1).value for i, h in enumerate(head)}
        print(f"  Pending row {r}: {row.get('Source')} "
              f"{str(row.get('Product'))[:48]!r} -> Rejected "
              f"({reason.split(':', 1)[0]})")
        _archive(wb, row, reason)
        ws.delete_rows(r, 1)
        n += 1
    return n


def annotate_weekly_rejected(wb) -> int:
    """B. The fabricated URL's archive copy reads as a date problem."""
    if "Weekly_Rejected" not in wb.sheetnames:
        return 0
    ws = wb["Weekly_Rejected"]
    head = _head(ws)
    if "URL" not in head or "RejectionReason" not in head:
        return 0
    iu, ir = head.index("URL"), head.index("RejectionReason")
    n = 0
    for row in ws.iter_rows(min_row=2):
        if str(row[iu].value or "").strip() != FSAI_FABRICATED:
            continue
        cur = str(row[ir].value or "")
        if "fabricated_url" in cur:
            continue
        row[ir].value = (cur + " | " + ARCHIVE[FSAI_FABRICATED]).strip()
        n += 1
    return n


def annotate_fsis_archive(wb) -> int:
    """F. An archive row with no reason and no rejecter is not a verdict."""
    ws = wb["Rejected"]
    head = _head(ws)
    iu = head.index("URL")
    ir = head.index("RejectReason") if "RejectReason" in head else None
    if ir is None:
        return 0
    n = 0
    for row in ws.iter_rows(min_row=2):
        if str(row[iu].value or "").strip() != FSIS_PHA:
            continue
        cur = str(row[ir].value or "")
        if cur.strip().lower() in ("", "none", "nan"):
            cur = ""
        if "UNRESOLVED (morning fix 2026-10-12)" in cur:
            continue
        row[ir].value = (cur + FSIS_ARCHIVE_NOTE).strip(" |")
        n += 1
    return n


def main() -> int:
    import openpyxl
    if not XLSX.exists():
        print("no workbook at", XLSX)
        return 1

    wb = openpyxl.load_workbook(XLSX)
    before = wb["Recalls"].max_row - 1
    pending_before = wb["Pending"].max_row - 1

    print("A — FSAI SuperValu smoked salmon filled from its own alert page")
    n_a = fill_fsai(wb)
    print(f"  {n_a} row(s) filled\n")

    print("F — USDA FSIS PHA-10062026-01: Brand from its own text")
    n_f = fill_fsis_brand(wb)
    print(f"  {n_f} row(s) filled\n")

    print("B–E — four Pending rows archived (Recalls untouched)")
    n_arch = archive_pending(wb)
    print(f"  {n_arch} row(s) archived\n")

    print("B — Weekly_Rejected copy of the fabricated URL annotated")
    n_wr = annotate_weekly_rejected(wb)
    print(f"  {n_wr} row(s) annotated\n")

    print("F — reasonless Rejected copy of PHA-10062026-01 marked UNRESOLVED")
    n_fr = annotate_fsis_archive(wb)
    print(f"  {n_fr} row(s) annotated\n")

    touched = n_a + n_f + n_arch + n_wr + n_fr
    if touched:
        wb.save(XLSX)
    after_pending = wb["Pending"].max_row - 1
    wb.close()

    if touched:
        from pipeline.merge_master import mirror_json_from_xlsx
        print(f"Pending {pending_before} -> {after_pending}; "
              f"Recalls unchanged at {before}")
        print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    # Recalls is never shrunk by this script. The one row that WILL enter
    # Recalls is the FSAI recall, and it enters through
    # pipeline.promote_gate_passing --apply, which rebase_and_verify runs
    # after this script — the same gate every scraped row passes.
    print("ROWS_REMOVED=0")
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
