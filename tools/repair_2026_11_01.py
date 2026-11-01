# -*- coding: utf-8 -*-
"""Morning fix pass 2026-11-01.

Every value set below was read this morning on the regulator's own page, or
is already on the row it is written to. NO ROW LEAVES Recalls, so the commit
message carries no `remove-rows` token.

A. ONE STALE "needs cleanup" STAMP (Recalls, RappelConso fiche 23953).

   Row: RappelConso (FR), 2026-10-29, "Produit à la Ferme - le Goût du Vrai",
   Product "Farmhouse fromage blanc", Listeria monocytogenes.
     rappel.conso.gouv.fr/fiche-rappel/23953/interne

   The morning pass of 2026-10-31 translated this Product from the fiche's
   own French ("fromage blanc fermier") and recorded the original in Notes,
   but did not remove the `[needs cleanup: Product not in English]` stamp
   reviewer 3 had put there. The stamp is the work queue for this very step
   ("translate every Recalls row whose Notes carry [needs cleanup: ...]"), so
   a stamp left on a row that has already been translated makes every
   following morning re-read the same row. Dropped, nothing else touched.

B. THE FDA HAIFA SPRAT RECALL — Date and Pathogen filled, Product de-headlined.

   Pending row, Source FDA, status pending_gap_v2, Date and Pathogen EMPTY,
   Notes "FDA HTML fallback — claude-check needs to enrich Date+Pathogen":
     fda.gov/safety/recalls-market-withdrawals-safety-alerts/
     haifa-smoked-fish-recalls-cold-smoked-sprats-due-potential-health-risk

   claude_check.py is retired and may not run, so the row was never going to
   be enriched by the thing its own Notes were waiting for. Both missing
   fields are published on the page itself, read this morning:

     press release date .... October 27, 2026 (content current as of
                             10/27/2026)
     recalling firm ........ "D&M Smoked Fish Inc. dba Haifa Smoked Fish
                             Inc.", Queens, NY; the FDA inspection was at
                             D&M Smoked Fish Inc. "of Jamaica, NY"
     product ............... "Cold Smoked Sprat Fish", packed loose in bulk
                             cardboard boxes labelled "Cold Smoked Sprats",
                             shipped refrigerated, Brand Name "No Brand"
     lots .................. "All Lots are included in the recall";
                             approximately 560 lbs total
     hazard ................ potential contamination with Clostridium
                             botulinum
     distribution .......... New Jersey, New York and Pennsylvania, retail
     illnesses ............. "No illnesses have been reported to date."

   Also cross-checked against the FDA "Recalls, Market Withdrawals, & Safety
   Alerts" table, which carries the same row dated 10/27/2026 with Product
   Description "Cold Smoked Sprat Fish" and reason "Potential contamination
   with Clostridium botulinum".

   THIS IS NOT A DUPLICATE of Recalls' 2026-10-23 row for the same firm and
   product:
     fda.gov/food/alerts-advisories-safety-information/
     fda-advises-not-eat-sell-or-serve-cold-smoked-sprats-sold-haifa-
     smoked-fish-inc
   That row is the ADVISORY FDA issued on 2026-10-23 because it had
   recommended a recall and "the firm declined" — its Class is "Alert" and
   its own Notes say "No recall was issued". The 10/27 notice is the firm
   doing what it had refused four days earlier: a recall of all lots. Two
   dated regulatory actions, two authority URLs, one hazard. The published
   row's Notes get a forward reference and this row gets the back reference,
   so neither reads as an unexplained near-duplicate of the other.

   Product is replaced because what is in the field is the press release
   HEADLINE ("Haifa Smoked Fish Recalls Cold Smoked Sprats Due Potential
   Health Risk"), not a food — the same defect class as the one-word
   "Unbranded" found on fiche 23552 on 2026-10-30. FDA's own Product
   Description is used instead.

C. THREE Pending ROWS ARCHIVED — each refused on a rule, with the reason read.

   C1. NOT A FOOD (R1). FDA, Wedgewood Pharmacy, LLC:
         fda.gov/safety/recalls-market-withdrawals-safety-alerts/
         wedgewood-pharmacy-issues-recall-certain-compounded-veterinary-
         medications-within-expiry-due
       The FDA recalls table, read this morning, gives this row as
       "10/21/2026 | Wedgewood Pharmacy, LLC. | Compounded veterinary
       medications (No Brand Name) | Potential potency issues". Compounded
       veterinary medicines are not food, and "potency" is not a hazard in
       this register's scope — the row had no Pathogen for exactly that
       reason. Weekly_Rejected already holds a copy at this URL.

   C2. A FOOD-CONTACT ARTICLE, NOT A FOOD (R1). GIS (PL), 2026-10-28:
         gov.pl/web/gis/ostrzezenie-publiczne-dotyczace-wyrobu-do-kontaktu-
         z-zywnoscia-migracja-pierwszorzedowych-amin-aromatycznych-z-
         zestawu-przyborow-kuchennych
       The recalled article is a KITCHEN UTENSIL SET. GIS's own title says
       so: "Ostrzeżenie publiczne dotyczące wyrobu do kontaktu z żywnością:
       Migracja pierwszorzędowych amin aromatycznych z zestawu przyborów
       kuchennych" — "Public warning concerning a food-contact article:
       migration of primary aromatic amines from a kitchen utensil set".
       No food is recalled, so R1 refuses it, and "packaging" is named in
       the out-of-scope list. Recorded rather than quietly left in Pending,
       because the blocker the promoter reported ("no hazard term in the
       row's own text") invites the wrong repair: primary aromatic amines
       ARE a chemical contaminant, and adding them to the hazard vocabulary
       to clear this row would have started publishing food-contact
       articles as food recalls. The scope rule is what refuses it, not the
       vocabulary. The Polish title's translation is recorded on the row.

   C3. ALREADY PUBLISHED FROM THE REGULATOR'S OWN NOTICE. CDC, 2026-10-30:
         cdc.gov/salmonella/outbreaks/oysters-10-26/index.html
       Read this morning: posted October 19 2026, updated October 30 2026;
       28 illnesses in 13 states, 5 hospitalisations; harvester "Little
       Wicomico Oyster Company"; product whole (shellstock) oysters
       harvested 18 September – 8 October 2026 from Little Wicomico River,
       VA and Little Wicomico River Lower, VA; "On October 9, 2026, the
       Little Wicomico Oyster Company voluntarily recalled oysters"; the
       Virginia Department of Health closed the harvest area.
       Recalls already carries this recall at the FDA safety alert for the
       same event, dated 2026-10-19, Company "Little Wicomico Oyster
       Company". So the Pending row is a second observer of a published
       recall and is archived as a duplicate — an accepted refusal, not a
       defect.
       REVIEWER 2's STATED REASON WAS WRONG AND IS CORRECTED ON THE ROW.
       It rejected with "Date, Company, and Brand cannot be confirmed from
       the page". The page names the company in the row's own extracted
       Product text and carries both an "Oct. 30, 2026" page date and the
       October 9 recall date; only Brand is genuinely absent, because the
       oysters are shellstock and carry none. A right outcome reached on a
       false reading is still a reviewer finding, and it is recorded so the
       next pass does not trust the stated reason.

D. THE DUPLICATE FSAI ROW (Pending) — one of two rows at one URL.

   Pending held TWO rows for
     fsai.ie/news-and-alerts/food-alerts/recall-of-several-supervalu-and-
     inspired-by-centra
   with the same ScrapedAt (2026-10-31T03:12:21Z) and the same source_id,
   one enriched by reviewer 2 to pending_gap_v3 (Company "Supervalu", Brand
   "Supervalu, Inspired By Centra", Product "Smoked Salmon, Mackerel, Prawn
   Cocktail") and one still carrying the page headline as its Product, no
   Company, and a reviewer-3 hold reading "no recalling firm on the row
   (Brand ''); not published and not rejected — reviewer 2 to read the firm
   from the notice".

   Reviewer 2 could not read the firm onto that row however many times it
   ran, because pipeline/recall_review_agent.py builds its row index as

       url_to_idx: Dict[str, int] = {}
       for i, pr in enumerate(full_pending):
           url_to_idx[u] = i

   — one slot per URL, last write wins. With two rows at one URL the
   earlier one is invisible to reviewer 2 for the rest of its life, while
   reviewer 3 keeps holding it and the promoter keeps listing it. The stale
   copy is archived as the duplicate it is; the enriched copy promotes
   normally. tests/test_pending_holds_one_row_per_url.py now fails the
   moment a second row lands at a URL, so the pair cannot re-form silently
   while the collector-side cause is still open (reported, not fixed here —
   it needs the collector run that produced it, which this pass cannot
   reproduce).

E. The published 2026-10-23 Haifa advisory row gets the forward reference
   described in B. No field but Notes is touched on it.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "recalls.json"

TODAY = "2026-11-01"
BY = "morning fix pass " + TODAY

SPRAT_RECALL_URL = ("https://www.fda.gov/safety/recalls-market-withdrawals-"
                    "safety-alerts/haifa-smoked-fish-recalls-cold-smoked-"
                    "sprats-due-potential-health-risk")
SPRAT_ALERT_URL = ("https://www.fda.gov/food/alerts-advisories-safety-"
                   "information/fda-advises-not-eat-sell-or-serve-cold-"
                   "smoked-sprats-sold-haifa-smoked-fish-inc")
FSAI_URL = ("https://www.fsai.ie/news-and-alerts/food-alerts/"
            "recall-of-several-supervalu-and-inspired-by-centra")

# ── A ──────────────────────────────────────────────────────────────────────
RECALLS_FIXES = [
    {
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/23953/interne",
        "set": {"Product": "Farmhouse fresh white cheese"},
        "drop_stamps": ["[needs cleanup: Product not in English]"],
        "note": (f"[{BY}: the stamp was NOT stale and this pass first read "
                 f"it as such — corrected before upload. The pass of "
                 f"2026-10-31 translated \"fermier\" to \"Farmhouse\" and "
                 f"left \"fromage blanc\" in French, so the published "
                 f"Product read \"Farmhouse fromage blanc\" and reviewer "
                 f"3's stamp was still true: "
                 f"tests/test_product_and_firm_names_are_english.py and "
                 f"tests/test_a_short_french_product_name_is_caught.py both "
                 f"go red on it, which is how the half-translation was "
                 f"found. Fiche 23953 names the product \"fromage blanc "
                 f"fermier\"; fromage blanc is a fresh unripened white "
                 f"cheese, so the Product is now \"Farmhouse fresh white "
                 f"cheese\" and the stamp is dropped on a row that is "
                 f"actually in English. The French is already preserved in "
                 f"these Notes as [original product: fromage blanc "
                 f"fermier].]"),
    },
    # ── E ──
    {
        "url": SPRAT_ALERT_URL,
        "set": {},
        "note": (f"[{BY}: cross-reference — the firm issued the recall FDA "
                 f"says it declined here. FDA press release of 27 October "
                 f"2026, \"HAIFA SMOKED FISH Recalls Cold Smoked Sprats Due "
                 f"to Potential Health Risk\": all lots, approximately 560 "
                 f"lbs, New Jersey, New York and Pennsylvania, no illnesses "
                 f"reported. That notice is a separate dated regulatory "
                 f"action on its own authority URL "
                 f"(.../haifa-smoked-fish-recalls-cold-smoked-sprats-due-"
                 f"potential-health-risk) and is queued from Pending today; "
                 f"this row remains the 2026-10-23 ADVISORY and keeps Class "
                 f"\"Alert\".]"),
    },
]

# ── B / G ─────────────────────────────────────────────────────────────────
PENDING_FIXES = [
    # ── G. ONE FIRM, ONE SPELLING ──
    #
    # Reviewer 2 read the firm off FSAI alert 2026.67 as "Supervalu" and the
    # brands as "Supervalu, Inspired By Centra". The register has carried
    # this retailer since 2026-08-05 as "SuperValu" (capital V), and the
    # previous FSAI alert for the same two brands, 2026.66 of 2026-10-14,
    # as Brand "SuperValu; Inspired By Centra". Promoting reviewer 2's
    # spelling put two spellings of one firm and one brand pair in the
    # register at once and turned
    # tests/test_firm_names_are_uniform.py::test_the_register_has_one_
    # spelling_per_name red — found by the upload gate before this ever
    # reached main. The trade name is styled "SuperValu"; the row is
    # brought onto the register's own spelling, and the alert's wording is
    # recorded so nothing is lost.
    {
        "url": FSAI_URL,
        "status": "pending_gap_v3",
        "set": {
            "Company": "SuperValu",
            "Brand": "SuperValu; Inspired By Centra",
        },
        "note": (f"[{BY}: Company \"Supervalu\" -> \"SuperValu\" and Brand "
                 f"\"Supervalu, Inspired By Centra\" -> \"SuperValu; "
                 f"Inspired By Centra\", to the spelling and separator the "
                 f"register has used for this retailer since 2026-08-05 and "
                 f"for this brand pair on FSAI alert 2026.66 (2026-10-14). "
                 f"FSAI's own alert title for THIS recall writes it "
                 f"\"Supervalu and Inspired By Centra\"; no fact is changed, "
                 f"only the spelling, so one firm is not two rows in every "
                 f"count.]"),
    },
    {
        "url": SPRAT_RECALL_URL,
        "set": {
            "Date": "2026-10-27",
            "Pathogen": "Clostridium botulinum",
            "Product": ("Cold smoked sprat fish, packed loose in bulk "
                        "cardboard boxes labelled \"Cold Smoked Sprats\", "
                        "all lots (approximately 560 lbs)"),
            "Brand": "—",
            "Reason": ("Cold smoked sprat fish recalled because it may be "
                       "contaminated with Clostridium botulinum. The firm "
                       "recalled all lots after FDA advised on 23 October "
                       "2026 that the product should not be eaten, sold or "
                       "served; no illnesses have been reported to date."),
            "Class": "Recall",
            "Tier": 1,
        },
        "note": (f"[{BY}: Date and Pathogen were EMPTY and the row's own "
                 f"Notes said \"claude-check needs to enrich Date+Pathogen\" "
                 f"— claude_check.py is retired and never runs, so nothing "
                 f"was ever going to fill them. Both read on the FDA press "
                 f"release at this row's own URL this morning: press release "
                 f"date 27 October 2026 (content current as of 10/27/2026); "
                 f"recalling firm \"D&M Smoked Fish Inc. dba Haifa Smoked "
                 f"Fish Inc.\", Queens, NY, FDA inspection at D&M Smoked "
                 f"Fish Inc. \"of Jamaica, NY\"; product \"Cold Smoked Sprat "
                 f"Fish\" packed loose in bulk cardboard boxes labelled "
                 f"\"Cold Smoked Sprats\", shipped refrigerated, Brand Name "
                 f"\"No Brand\"; \"All Lots are included in the recall\", "
                 f"approximately 560 lbs; hazard \"potential contamination "
                 f"with Clostridium botulinum\"; distributed in New Jersey, "
                 f"New York and Pennsylvania through retail stores; \"No "
                 f"illnesses have been reported to date.\" Same figures in "
                 f"FDA's own recalls table (10/27/2026, Product Description "
                 f"\"Cold Smoked Sprat Fish\"). Product replaced because the "
                 f"field held the press-release HEADLINE, not a food. NOT a "
                 f"duplicate of the published 2026-10-23 row at "
                 f".../fda-advises-not-eat-sell-or-serve-cold-smoked-sprats-"
                 f"sold-haifa-smoked-fish-inc: that is the advisory FDA "
                 f"issued when the firm DECLINED to recall (Class \"Alert\", "
                 f"\"No recall was issued\"); this is the recall the firm "
                 f"then issued four days later. No best-before is published.]"
                 ),
    },
]

# ── C / D ─────────────────────────────────────────────────────────────────
PENDING_ARCHIVE = [
    {
        "url": ("https://www.fda.gov/safety/recalls-market-withdrawals-"
                "safety-alerts/wedgewood-pharmacy-issues-recall-certain-"
                "compounded-veterinary-medications-within-expiry-due"),
        "status": None,
        "reason": (f"Refused by the {BY} on R1 (food only): compounded "
                   f"VETERINARY MEDICATIONS are not food. FDA's own recalls "
                   f"table, read {TODAY}, gives this notice as \"10/21/2026 "
                   f"| Wedgewood Pharmacy, LLC. | Compounded veterinary "
                   f"medications (No Brand Name) | Potential potency "
                   f"issues\" — and potency is not a hazard in this "
                   f"register's scope, which is why the row carried no "
                   f"Pathogen. Not a vocabulary gap; a scope refusal."),
    },
    {
        "url": ("https://www.gov.pl/web/gis/ostrzezenie-publiczne-"
                "dotyczace-wyrobu-do-kontaktu-z-zywnoscia-migracja-"
                "pierwszorzedowych-amin-aromatycznych-z-zestawu-"
                "przyborow-kuchennych"),
        "status": None,
        "reason": (f"Refused by the {BY} on R1 (food only): the recalled "
                   f"article is a KITCHEN UTENSIL SET, not a food. GIS's "
                   f"own title, on the row: \"Ostrzeżenie publiczne "
                   f"dotyczące wyrobu do kontaktu z żywnością: Migracja "
                   f"pierwszorzędowych amin aromatycznych z zestawu "
                   f"przyborów kuchennych\" = \"Public warning concerning a "
                   f"food-contact article: migration of primary aromatic "
                   f"amines from a kitchen utensil set\". No food is "
                   f"recalled, and packaging/articles are named in the "
                   f"out-of-scope list. The promoter's blocker (\"no hazard "
                   f"term in the row's own text\") must NOT be cleared by "
                   f"adding primary aromatic amines to the hazard "
                   f"vocabulary: PAAs are a real chemical contaminant, and "
                   f"teaching the gate that term would start publishing "
                   f"food-contact articles as food recalls. Scope refuses "
                   f"the row; vocabulary is not the defect."),
    },
    {
        "url": "https://www.cdc.gov/salmonella/outbreaks/oysters-10-26/index.html",
        "status": None,
        "reason": (f"Refused by the {BY} as a DUPLICATE of a published "
                   f"recall: Recalls already carries the Little Wicomico "
                   f"Oyster Company shellstock-oyster recall at FDA's own "
                   f"safety alert, dated 2026-10-19. Read on this CDC page "
                   f"{TODAY}: posted 19 October 2026, updated 30 October "
                   f"2026; 28 illnesses, 13 states, 5 hospitalisations; "
                   f"harvester \"Little Wicomico Oyster Company\"; whole "
                   f"(shellstock) oysters harvested 18 September – 8 October "
                   f"2026 from Little Wicomico River, VA and Little "
                   f"Wicomico River Lower, VA; \"On October 9, 2026, the "
                   f"Little Wicomico Oyster Company voluntarily recalled "
                   f"oysters\"; harvest area closed by the Virginia "
                   f"Department of Health. An accepted near-duplicate "
                   f"refusal. NOTE ON REVIEWER 2: its stated reason, "
                   f"\"Date, Company, and Brand cannot be confirmed from the "
                   f"page\", is WRONG — the company is in the row's own "
                   f"extracted Product text and the page carries both an "
                   f"\"Oct. 30, 2026\" date and the 9 October recall date. "
                   f"Only Brand is genuinely absent (shellstock oysters "
                   f"carry none). Right outcome, false reading."),
    },
    {
        "url": FSAI_URL,
        "status": "pending_gap_v2",
        # Carries the SUPERSEDED marker from birth, deliberately. This row
        # records the removal of one COPY; it is not a verdict on the
        # recall, and load_rejected_urls would otherwise read it as one and
        # block the surviving copy at the same URL. See section F.
        "reason": (f"[SUPERSEDED {TODAY} — this archive row records the "
                   f"removal of a duplicate COPY at this URL, not a verdict "
                   f"on the recall. The enriched copy is the one that "
                   f"publishes.] Refused by the {BY} as the DUPLICATE of two Pending "
                   f"rows at one URL. The other copy (same ScrapedAt "
                   f"2026-10-31T03:12:21Z, same source_id) was enriched by "
                   f"reviewer 2 to pending_gap_v3 with Company "
                   f"\"Supervalu\", Brand \"Supervalu, Inspired By Centra\" "
                   f"and Product \"Smoked Salmon, Mackerel, Prawn "
                   f"Cocktail\", and promotes today. This copy still held "
                   f"the FSAI page headline as its Product and no Company, "
                   f"and could never be fixed in place: "
                   f"pipeline/recall_review_agent.py indexes Pending as "
                   f"url_to_idx[url] = i, one slot per URL, last write "
                   f"wins — so with two rows at one URL the earlier one is "
                   f"invisible to reviewer 2 forever, while reviewer 3 "
                   f"holds it (\"no recalling firm on the row (Brand '')\") "
                   f"and the offline promoter keeps listing it. The "
                   f"collector-side cause of the duplicate pair is NOT "
                   f"fixed here; tests/test_pending_holds_one_row_per_url.py "
                   f"now fails the moment a second row lands at a URL."),
    },
]


# --------------------------------------------------------------------------

def _head(ws):
    return [c.value for c in ws[1]]


def _rowdict(ws, head, r):
    return {h: ws.cell(r, i + 1).value for i, h in enumerate(head)}


def _set(ws, head, r, field, value):
    ws.cell(r, head.index(field) + 1).value = value


def _key(u):
    from pipeline.merge_master import _normalize_url_for_dedup
    return _normalize_url_for_dedup(str(u or "").strip())


def _apply_notes(ws, head, r, row, note, drop_stamps):
    notes = str(row.get("Notes") or "").strip()
    for stamp in drop_stamps or ():
        notes = notes.replace(stamp, "").strip()
    notes = re.sub(r"[ \t]{2,}", " ", (notes + " " + note).strip())
    _set(ws, head, r, "Notes", notes)


def _fix_sheet(wb, sheet, specs) -> int:
    ws = wb[sheet]
    head = _head(ws)
    done = 0
    for spec in specs:
        target = _key(spec["url"])
        hit = False
        for r in range(2, ws.max_row + 1):
            row = _rowdict(ws, head, r)
            if _key(row.get("URL")) != target:
                continue
            want = spec.get("status")
            if want is not None and str(row.get("Status") or "").strip() != want:
                continue
            for field, value in spec["set"].items():
                if field not in head:
                    continue
                _set(ws, head, r, field, value)
            _apply_notes(ws, head, r, row, spec["note"],
                         spec.get("drop_stamps"))
            done += 1
            hit = True
            print(f"  {sheet}: fixed {row.get('Source')!r} "
                  f"{spec['url'][-54:]}")
        if not hit:
            raise SystemExit(
                f"{sheet}: no row at {spec['url']} — main has moved under "
                f"this repair script; re-read the sheet before re-running")
    return done


def _archive(arch, src_row, reason, rejected_by):
    out = []
    for h in _head(arch):
        if h in ("RejectReason", "RejectionReason"):
            out.append(reason)
        elif h == "RejectedBy":
            out.append(rejected_by)
        elif h == "RejectedAt":
            out.append(TODAY)
        elif h == "Status":
            out.append("rejected")
        else:
            out.append(src_row.get(h, ""))
    arch.append(out)


def _archive_pending(wb) -> int:
    """Pending -> Rejected, with the verdict in the archive's own reason
    column. Pending is a queue, not an archive, so a row refused on a rule
    leaves it; Rejected is append-only and nothing is deleted from it.

    Matched on the writer's own dedup key AND, where two rows share a URL,
    on Status — section D removes one of a pair and must not take both.
    """
    ws = wb["Pending"]
    head = _head(ws)
    arch = wb["Rejected"]
    done = 0
    for spec in PENDING_ARCHIVE:
        target = _key(spec["url"])
        want = spec.get("status")
        hit = False
        for r in range(ws.max_row, 1, -1):
            row = _rowdict(ws, head, r)
            if _key(row.get("URL")) != target:
                continue
            if want is not None and str(row.get("Status") or "").strip() != want:
                continue
            _archive(arch, row, spec["reason"], BY)
            ws.delete_rows(r, 1)
            done += 1
            hit = True
            print(f"  Pending -> Rejected: {row.get('Source')!r} "
                  f"{str(row.get('Product'))[:48]!r}")
        if not hit:
            raise SystemExit(
                f"Pending: no row at {spec['url']} "
                f"(status={want!r}) — main has moved under this repair "
                f"script; re-read the sheet before re-running")
    return done


#: Section F. Archive rows whose stated verdict was about a TRANSIENT STATE
#: of the row and not about the recall, and which therefore must stop
#: blocking re-promotion now that the state is cured.
SUPERSEDE = [
    {
        "url": FSAI_URL,
        "note": (
            f" [SUPERSEDED {TODAY} by the {BY}: this verdict names a "
            f"transient state of the row (\"row was at pending_enrichment / "
            f"pending (not reviewed by reviewer 2) and Date is empty\"), not "
            f"a property of the recall. Both conditions are cured: reviewer "
            f"2 verified the row to pending_gap_v3 on 2026-10-31 (Company "
            f"\"Supervalu\", Brand \"Supervalu, Inspired By Centra\") and "
            f"Date is 2026-10-20, read from FSAI alert 2026.67 "
            f"(\"Tuesday, 20 October 2026\", Category 1: For Action) quoted "
            f"on the row's own Reason. The row passes every publish-gate "
            f"rule. Nothing is deleted — this annotation only stops a cured "
            f"defect being an unappealable verdict, per the 2026-10-11 "
            f"SUPERSEDED rule in merge_master.load_rejected_urls.]"),
    },
    {
        "url": SPRAT_RECALL_URL,
        "note": (
            f" [SUPERSEDED {TODAY} by the {BY}: this verdict names a "
            f"transient state of the row (\"row was at pending_gap_v2 (not "
            f"reviewed by reviewer 2) and Date is empty\"), not a property "
            f"of the recall. Date 2026-10-27 and Pathogen \"Clostridium "
            f"botulinum\" were read this morning on the FDA press release at "
            f"the row's own URL, and the row now passes every publish-gate "
            f"rule. The empty Date was never going to be filled by the thing "
            f"the row was waiting for: its Notes said \"claude-check needs to "
            f"enrich Date+Pathogen\" and claude_check.py is retired. Nothing "
            f"is deleted.]"),
    },
]


def _supersede_archive(wb) -> int:
    """Mark a stale archive verdict SUPERSEDED, in the sheet's own reason
    column — never delete it. Archives are append-only.

    "[SUPERSEDED <date> — …]" is the marker merge_master.load_rejected_urls
    has honoured since 2026-10-11 and the marker
    tests/test_a_recall_is_not_both_published_and_rejected.py reads. Without
    it a confirmer line about a row's STATE ("Date is empty") becomes a
    permanent verdict on the RECALL, and the offline promoter reports only
    "re-promotion BLOCKED", which is indistinguishable from a correct
    scope refusal.
    """
    done = 0
    for spec in SUPERSEDE:
        target = _key(spec["url"])
        hit = False
        for sheet in ("Weekly_Rejected", "Rejected"):
            if sheet not in wb.sheetnames:
                continue
            ws = wb[sheet]
            head = _head(ws)
            why = ("RejectionReason" if "RejectionReason" in head
                   else "RejectReason" if "RejectReason" in head else None)
            if why is None or "URL" not in head:
                continue
            for r in range(2, ws.max_row + 1):
                row = _rowdict(ws, head, r)
                if _key(row.get("URL")) != target:
                    continue
                existing = str(row.get(why) or "").strip()
                if existing.lower() in ("", "nan", "none"):
                    existing = ""
                hit = True
                if "superseded" in existing.lower():
                    continue
                _set(ws, head, r, why, (existing + spec["note"]).strip())
                done += 1
                print(f"  {sheet}:{r}: verdict marked SUPERSEDED — "
                      f"{spec['url'][-48:]}")
        if not hit:
            raise SystemExit(
                f"no archive row found at {spec['url']} — the promoter "
                f"reported one as blocking; re-read the sheets")
    return done


def main() -> int:
    if not XLSX.exists():
        raise SystemExit(f"workbook not found: {XLSX}")

    wb = openpyxl.load_workbook(XLSX)
    recalls_fixed = _fix_sheet(wb, "Recalls", RECALLS_FIXES)
    pending_fixed = _fix_sheet(wb, "Pending", PENDING_FIXES)
    archived = _archive_pending(wb)
    # AFTER the archive write, so the duplicate row this pass adds at the
    # FSAI URL is annotated too — otherwise it would itself block the copy
    # it was written to make way for.
    superseded = _supersede_archive(wb)
    wb.save(XLSX)
    wb.close()

    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    print(f"RECALLS_FIELDS_FIXED={recalls_fixed}")
    print(f"PENDING_FIELDS_FIXED={pending_fixed}")
    print(f"PENDING_ARCHIVED={archived}")
    print(f"ARCHIVE_VERDICTS_SUPERSEDED={superseded}")
    print("ROWS_QUEUED=0")
    print("ROWS_REMOVED=0")
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
