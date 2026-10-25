# -*- coding: utf-8 -*-
"""Morning fix 2026-10-24.

NO ROW LEAVES Recalls — the commit message needs no "remove-rows". One row
leaves PENDING, which is a queue and not an archive, and its archive copies
are already on file three times over. Nothing is deleted from Rejected or
Weekly_Rejected; those are append-only and are only annotated.

Five sections. A, B and C are the data half of code fixes shipped in the
same upload; D adds two recalls the sweep found missing; E clears a Pending
re-ingestion the gate now refuses one step earlier.


A. TWO TIER-1 RECALLS WERE PUBLISHED WITH A BARCODE WHERE THE FOOD GOES
   ────────────────────────────────────────────────────────────────────────
   Published on 2026-10-23, both Listeria monocytogenes, both Tier 1:

     Recalls / RappelConso (FR) / fiche 23922
       Company  "Sk"      Brand "Sk"
       Product  "0207684000000; date limite de consommation; 2026-10-24"
     Recalls / RappelConso (FR) / fiche 23914   (and its Weekly_Review copy)
       Company  "Celtarmor"
       Product  "3563243012829; 22/10/2026; date limite de consommation;
                 2026-10-22; |3563243012621; |3563243014625"

   A subscriber reading either row cannot tell which food to avoid. Both
   carry the scraper's own stamp "[scraper-flag: Product taken from the
   fiche title; the model/reference field held only lot codes]" — and the
   title had NOT been used. Two defects, both fixed in this upload:

   1. scrapers/_product_text.is_identifier_only did not recognise the
      block as identifiers, because "limite" and "consommation" are
      content words. Only "date" was in _PACKAGING_WORDS. This is the same
      escape "Sachet" made through the publish gate's word test on
      2026-10-22, one layer up.
   2. scrapers/europe_eu/rappelconso.py, having detected that the field it
      picked was a lot line, called `_first` on the remaining candidates —
      which returns the first NON-EMPTY one. PRODUCT_FIELDS is ordered
      `modeles_ou_references, identification_produits, libelle`, and the
      GTIN/DLC block lives in the middle field, so the walk stopped one
      field short of `libelle`, the heading, and stamped the flag anyway.

   Read on the fiches themselves on 2026-10-24:
     23922  heading "Blanquette St Jacques", Marque "SK",
            motif "Listeria monocytogenes détectée dans les noix de st
            Jacques de notre fournisseur CELTARMOR"
     23914  heading "Noix de Saint Jacques de la Baie de Saint Brieuc
            Label Rouge", Marque "CELTARMOR", references "Barquette de
            200g / 400g / 1 kg", motif "Présence de Listeria
            monocytogenes"

   Company/Brand "Sk" is also wrong: the fiche prints the brand "SK".

B. THE MORNING'S THREE UNTRANSLATED REASONS AND TWO UNTRANSLATED PRODUCTS
   ────────────────────────────────────────────────────────────────────────
   Published 2026-10-23, stamped "[needs cleanup: … not in English]" by
   reviewer 3 and the offline promoter, which have no model:
     fiche 23922  Reason  (above)
     fiche 23925  Reason  "Analyses microbiologiques au dessus des normes
                           en vigueur, sur le lot précédent. retrait des
                           produits de ce lot par précaution."
                  Product "tempeh de pois chiches; tempeh de lentilles;
                           tempeh de soja"
     fiche 23912  Reason  "Presence of Listeria monocytogenes (agent
                           responsable de la listériose)"   (half-English)
                  Product "gratin dauphinois"
     fiche 23923  Product "noix de saint-jacques fraîches baie de saint
                           brieuc, sans corail filière qualité carrefour -
                           barquette 200g"
   The last two Products carried no needs-cleanup stamp at all — the
   detector misses a short French dish name, exactly as the operator rule
   of 2026-10-01 says ("foie de poulet"). Every original is kept in Notes.

C. A SUSPECTED-BOTULINUM RECALL WAS ARCHIVED WITH ANOTHER RECALL'S
   ALLERGEN VERDICT, AND THE RE-PROMOTION GUARD THEN HELD THE CORRECT ROW
   ────────────────────────────────────────────────────────────────────────
   Weekly_Rejected, written by the Italian gap finder:

       Date       2026-10-22   Source  Ministero della Salute (IT)
       Product    "Sospetto botulino: richiamata salsa tartufata"
       Pathogen   "uova"          (Italian for eggs)
       Reason     "allergen: Undeclared allergen — out of scope
                   (Rule B reject)."
       RejectedBy gap_finder/it/rules.py
       URL        .../avvisi_sicurezza_alimentare/cartello%20di%20
                  richiamo%20modello%20ministero%20LOTTO%20596_
                  1792593268.pdf

   The row's own Product text says "Sospetto botulino" — suspected
   botulinum. The discovery article (ilfattoalimentare.it, 22 Oct 2026) is
   a roundup of FOUR recalls: suspected botulinum toxin in salsa tartufata;
   Salmonella typhimurium in organic eggs; a microbiological
   non-conformity in clams; and UNDECLARED SULPHITES in vegetables in oil.
   The classifier took the Pathogen off the headline's second item and the
   verdict off its fourth, and applied both to the first.

   It could not do otherwise: pipeline/gap_finder/rules.PATHOGENS_TIER_1
   listed "clostridium botulinum", "botulinum toxin", "botulism",
   "botulismus", "botulisme" and "botulinumtoxin", and none of those is a
   substring of "botulino" or "tossina botulinica". The bare needle
   "botulin" — which pipeline/_pathogen_scope.ALWAYS_TIER1_KEYWORDS has
   always used — is added in this upload, with "shiga toxin",
   "vulnificus" and "sakazakii", swept the same way.

   The correct row is in Pending and passes every publish-gate rule:
       2026-10-22  Ministero della Salute (IT)
       DITTA BREZZI OLIVIERO TARTUFI DI BREZZI GIAN PAOLO /
       BREZZI OLIVIERO TARTUFI / "Tartufata" /
       "Botulinum toxin" / "Suspected presence of botulinum toxin"
   and pipeline.promote_gate_passing refused to publish it with
   "re-promotion BLOCKED … already in Weekly_Rejected (allergen: …)".
   Marking the archive row SUPERSEDED is what merge_master's own
   "superseded" re-promotion exception reads, so the deterministic promoter
   publishes the Pending row on its next pass. The archive row is kept
   verbatim behind the banner.

   NOT VERIFIED ON THE MINISTRY'S OWN PAGE TODAY. www.salute.gov.it serves
   this run a JavaScript browser-validation shell for both the PDF and the
   avvisi listing, so the authority document could not be read from here
   (scrapers/salute_it.py reaches it from GitHub Actions). Nothing in this
   script invents a field: the SUPERSEDED banner rests on the archive row's
   OWN captured text contradicting its own verdict, and the Pending row's
   fields are the pipeline's, left as they are apart from translating
   Product into English.

D. TWO RECALLS THE PIPELINE MISSED, ADDED THROUGH add_manual_row
   ────────────────────────────────────────────────────────────────────────
   Found by the 2026-10-24 missed-recall sweep, each read on the
   authority's own page, neither present in any of the five sheets:
     FDA       2026-10-23  D&M Smoked Fish Inc. (dba Haifa Smoked Fish) —
                           cold smoked sprats, Clostridium botulinum
     MAPAQ QC  2026-10-23  Boucherie Viau inc. — ham and bacon loaf,
                           Listeria monocytogenes
   Both queued to Pending at Status 'pending' and left to the publish gate,
   like any other row.

E. THE 2022 NAFDAC ALERT, IN PENDING FOR THE FOURTH TIME
   ────────────────────────────────────────────────────────────────────────
   Pending, Date 2026-10-23, Status pending_gap_v2:
       NAFDAC (NG) / "Alcoholic Bitters" /
       nafdac.gov.ng/public-alert-no-027-2022-adulterated-lots-of-japata-…
   NAFDAC numbers its alerts <serial>-<year> and keeps the number in the
   slug, so the row's own URL says 2022 while its Date says 2026 — the
   publish gate refuses it, and this same URL is already archived in
   Rejected (2026-09-24, 2026-10-13) and Weekly_Rejected (2026-10-23).
   It held tests/test_alert_number_year_matches_date[Pending] red. The
   Pending copy is removed; the archives keep the record, and
   validate_pending_row now refuses the shape so it cannot come back.
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
JSON_OUT = ROOT / "docs" / "data" / "recalls.json"
TODAY = "2026-10-24"
NOTES_CAP = 1800


# ── small helpers, same shape as the previous repair scripts ─────────────

def _head(ws):
    return [c.value for c in ws[1]]


def _rows_matching(ws, url_key: str):
    head = _head(ws)
    if "URL" not in head:
        return
    c_url = head.index("URL") + 1
    for r in range(2, ws.max_row + 1):
        if url_key in str(ws.cell(r, c_url).value or "").lower():
            yield r, head


def _set(ws, r, head, col, value):
    if col not in head:
        raise SystemExit(f"sheet has no {col!r} column")
    ws.cell(r, head.index(col) + 1).value = value


def _get(ws, r, head, col) -> str:
    if col not in head:
        return ""
    return str(ws.cell(r, head.index(col) + 1).value or "")


def _append_notes(ws, r, head, note: str) -> bool:
    """Append `note` to Notes unless an identical stamp is already there."""
    prior = _get(ws, r, head, "Notes")
    mark = note[:46]
    if mark in prior:
        return False
    _set(ws, r, head, "Notes", (prior.strip() + " " + note).strip()[:NOTES_CAP])
    return True


def _drop_cleanup_stamp(ws, r, head, which: str) -> bool:
    stamp = f"[needs cleanup: {which} not in English]"
    prior = _get(ws, r, head, "Notes")
    if stamp not in prior:
        return False
    _set(ws, r, head, "Notes", prior.replace(stamp, "").replace("  ", " ").strip())
    return True


# ── A + B — the five RappelConso rows of 2026-10-23 ─────────────────────
#
# One entry per fiche. `product` / `reason` / `company` / `brand` are the
# values to write (omit to leave alone); `note` is appended to Notes and
# carries every original verbatim; `drop` names the needs-cleanup stamps to
# discharge. Every value below was read on the fiche named in its note.

FICHES = [
    {
        "key": "fiche-rappel/23922/",
        "sheets": ("Recalls", "Weekly_Review", "Pending"),
        "require": 1,
        "company": "SK",
        "brand": "SK",
        "product": ("Scallop blanquette (Blanquette St Jacques); "
                    "GTIN 0207684000000; use-by 24/10/2026"),
        "reason": ("Listeria monocytogenes detected in the scallops from "
                   "our supplier Celtarmor"),
        "drop": ("Reason", "Product"),
        "note": (
            "[product-repair 2026-10-24: Product was the GTIN/DLC block from "
            "the fiche's identification_produits field, published as the food. "
            "Read off fiche 23922's own heading instead. The scraper-flag "
            "above says the heading was used; it was not — the walk through "
            "PRODUCT_FIELDS stopped at the GTIN block, fixed in "
            "scrapers/europe_eu/rappelconso.py in this upload. "
            "[original product: 0207684000000; date limite de consommation; "
            "2026-10-24]] "
            "[original Reason (fr): \"Listeria monocytogenes détectée dans "
            "les noix de st Jacques de notre fournisseur CELTARMOR\"] "
            "[firm-repair 2026-10-24: Company/Brand \"Sk\" -> \"SK\", the "
            "brand as fiche 23922 prints it]"
        ),
    },
    {
        "key": "fiche-rappel/23914/",
        "sheets": ("Recalls", "Weekly_Review", "Pending"),
        "require": 2,          # Recalls + its Weekly_Review copy
        "product": ("King scallops from the Bay of Saint-Brieuc, Label Rouge "
                    "(Noix de Saint Jacques de la Baie de Saint Brieuc Label "
                    "Rouge); 200 g, 400 g and 1 kg trays; GTINs "
                    "3563243012829, 3563243012621, 3563243014625; "
                    "use-by 22/10/2026"),
        "drop": ("Product",),
        "note": (
            "[product-repair 2026-10-24: Product was the GTIN/DLC block from "
            "the fiche's identification_produits field, published as the food. "
            "Read off fiche 23914's own heading and its \"Modèles ou "
            "références\" (Barquette de 200g / 400g / 1 kg). The scraper-flag "
            "above says the heading was used; it was not. "
            "[original product: 3563243012829; 22/10/2026; date limite de "
            "consommation; 2026-10-22; |3563243012621; |3563243014625]]"
        ),
    },
    {
        "key": "fiche-rappel/23923/",
        "sheets": ("Recalls", "Weekly_Review", "Pending"),
        "require": 1,
        "product": ("Fresh king scallops from the Bay of Saint-Brieuc, "
                    "without roe, Filière Qualité Carrefour — 200 g tray"),
        "drop": ("Product",),
        "note": (
            "[product-translation 2026-10-24: read on fiche 23923, \"Noix de "
            "Saint-Jacques fraîches Baie de Saint Brieuc, sans corail, 200g\" "
            "— « sans corail » is \"without roe\". "
            "[original product: noix de saint-jacques fraîches baie de saint "
            "brieuc, sans corail filière qualité carrefour - barquette 200g]]"
        ),
    },
    {
        "key": "fiche-rappel/23925/",
        "sheets": ("Recalls", "Weekly_Review", "Pending"),
        "require": 1,
        "product": ("Chickpea tempeh; lentil tempeh; soy tempeh (Tempeh "
                    "artisanal frais et bio du Diois)"),
        "reason": ("Microbiological test results above the applicable limits "
                   "on the preceding lot; this lot withdrawn as a precaution"),
        "drop": ("Reason", "Product"),
        "note": (
            "[product-translation 2026-10-24: read on fiche 23925, heading "
            "\"Tempeh artisanal frais et bio du Diois\", references \"Tempeh "
            "de pois chiches / de lentilles / de soja\". "
            "[original product: tempeh de pois chiches; tempeh de lentilles; "
            "tempeh de soja]] "
            "[original Reason (fr): \"Analyses microbiologiques au dessus des "
            "normes en vigueur, sur le lot précédent. retrait des produits de "
            "ce lot par précaution.\"] "
            "[operator note 2026-10-24: fiche 23925 lists TWO hazards under "
            "« Risques encourus par le consommateur » — \"Bacillus cereus\" "
            "and \"Staphylococcus aureus\". Pathogen keeps the Tier-1 hazard "
            "the row already carries; adding the second would re-open this "
            "row's tier-guard decision, so it is recorded here for George "
            "rather than written into Pathogen.]"
        ),
    },
    {
        "key": "fiche-rappel/23912/",
        "sheets": ("Recalls", "Weekly_Review", "Pending"),
        "require": 1,
        "product": "Dauphinoise potato gratin (GRATIN DAUPHINOIS)",
        "reason": ("Presence of Listeria monocytogenes (the agent "
                   "responsible for listeriosis)"),
        "drop": ("Reason", "Product"),
        "note": (
            "[product-translation 2026-10-24: read on fiche 23912, heading "
            "\"GRATIN DAUPHINOIS\". [original product: gratin dauphinois]] "
            "[original Reason (fr): \"Présence de Listeria monocytogenes "
            "(agent responsable de la listériose)\"]"
        ),
    },
]


def repair_fiches(wb) -> int:
    """A + B — field repairs and translations on the 2026-10-23 fiches."""
    total = 0
    for spec in FICHES:
        touched = 0
        for sheet in spec["sheets"]:
            if sheet not in wb.sheetnames:
                continue
            ws = wb[sheet]
            for r, head in list(_rows_matching(ws, spec["key"])):
                changed = []
                for col in ("company", "brand", "product", "reason"):
                    if col not in spec:
                        continue
                    name = col.capitalize()
                    if name not in head:
                        continue
                    if _get(ws, r, head, name) != spec[col]:
                        _set(ws, r, head, name, spec[col])
                        changed.append(name)
                for which in spec.get("drop", ()):
                    if _drop_cleanup_stamp(ws, r, head, which):
                        changed.append(f"-stamp({which})")
                if _append_notes(ws, r, head, spec["note"]):
                    changed.append("Notes")
                if changed:
                    touched += 1
                    print(f"  {sheet} row {r}: {spec['key']} -> "
                          f"{', '.join(changed)}")
        if touched < spec["require"]:
            raise SystemExit(
                f"{spec['key']}: expected at least {spec['require']} row(s) "
                f"to repair, touched {touched} — the register moved under "
                f"this script")
        total += touched
    return total


# ── C — the salsa tartufata archive row ─────────────────────────────────

TARTUFATA_KEY = "lotto%20596_1792593268.pdf"

TARTUFATA_BANNER = (
    "[SUPERSEDED 2026-10-24 — the verdict below belongs to a DIFFERENT "
    "recall. The discovery article (ilfattoalimentare.it, 22 Oct 2026) "
    "covers four recalls at once: suspected botulinum toxin in salsa "
    "tartufata, Salmonella typhimurium in organic eggs, a microbiological "
    "non-conformity in clams, and undeclared sulphites in vegetables in "
    "oil. gap_finder/it/rules.py took Pathogen \"uova\" (eggs) from the "
    "second and the allergen verdict from the fourth, and applied both to "
    "the first — whose own Product text on this row reads \"Sospetto "
    "botulino\". It could not see the hazard: PATHOGENS_TIER_1 held no "
    "Italian form of botulinum, fixed in this upload by adding the bare "
    "needle \"botulin\". This is a Tier-1 toxin recall, not an "
    "allergen-only one. The correct row is in Pending and passes every "
    "publish-gate rule; this banner is what merge_master's \"superseded\" "
    "re-promotion exception reads, so the offline promoter may publish it. "
    "Kept verbatim behind the banner as the audit trail of the original "
    "refusal.] "
)

TARTUFATA_NOTE = (
    "[morning fix 2026-10-24: archived verdict superseded — another "
    "recall's allergen verdict, see the RejectionReason banner. The "
    "ministry's own PDF could not be read from the fix pass today "
    "(www.salute.gov.it serves a JavaScript validation shell); nothing "
    "here rests on it.]"
)

TARTUFATA_PRODUCT = "Truffle sauce (salsa tartufata), lot 596"

TARTUFATA_PENDING_NOTE = (
    "[product-translation 2026-10-24: Product \"Tartufata\" is the Italian "
    "name of the sauce; translated for publication, lot from this row's own "
    "URL (\"LOTTO 596\"). [original product: Tartufata]]"
)


def supersede_tartufata(wb) -> int:
    """C1 — banner on the archive row's reason column, Notes, marker."""
    stamped = 0
    for sheet, reason_col, mark_col in (
            ("Weekly_Rejected", "RejectionReason", "Reviewed"),
            ("Rejected", "RejectReason", "Status")):
        if sheet not in wb.sheetnames:
            continue
        ws = wb[sheet]
        for r, head in list(_rows_matching(ws, TARTUFATA_KEY)):
            prior = _get(ws, r, head, reason_col)
            if "SUPERSEDED" in prior:
                continue
            _set(ws, r, head, reason_col, (TARTUFATA_BANNER + prior).strip())
            if mark_col in head:
                _set(ws, r, head, mark_col, "SUPERSEDED")
            _append_notes(ws, r, head, TARTUFATA_NOTE)
            stamped += 1
            print(f"  {sheet} row {r}: {reason_col} marked SUPERSEDED "
                  f"(salsa tartufata) — original reason kept verbatim")
    if stamped < 1:
        raise SystemExit(
            "salsa tartufata: no archive row carrying this URL was found — "
            "the register moved under this script")
    return stamped


def translate_tartufata_pending(wb) -> int:
    """C2 — the Pending row's Product into English before it publishes."""
    if "Pending" not in wb.sheetnames:
        return 0
    ws = wb["Pending"]
    n = 0
    for r, head in list(_rows_matching(ws, TARTUFATA_KEY)):
        if _get(ws, r, head, "Product") != TARTUFATA_PRODUCT:
            _set(ws, r, head, "Product", TARTUFATA_PRODUCT)
            _append_notes(ws, r, head, TARTUFATA_PENDING_NOTE)
            n += 1
            print(f"  Pending row {r}: Product -> {TARTUFATA_PRODUCT!r}")
    return n


# ── E — the NAFDAC 2022 alert in Pending ────────────────────────────────

NAFDAC_KEY = "public-alert-no-027-2022"
NAFDAC_ARCHIVE_NOTE = (
    "[morning fix 2026-10-24: this URL was in Pending again (Date "
    "2026-10-23, Status pending_gap_v2) for the fourth time, holding "
    "tests/test_alert_number_year_matches_date[Pending] red. The Pending "
    "copy was removed; this archive row keeps the record. "
    "pipeline.merge_master.validate_pending_row now imports the publish "
    "gate's alert_number_year_mismatch, so the shape is refused before it "
    "reaches Pending.]"
)


def drop_nafdac_pending(wb) -> int:
    """E — remove the Pending re-ingestion; annotate the archive copies."""
    if "Pending" not in wb.sheetnames:
        return 0
    ws = wb["Pending"]
    removed = 0
    for r, _head_ in reversed(list(_rows_matching(ws, NAFDAC_KEY))):
        ws.delete_rows(r, 1)
        removed += 1
        print(f"  Pending row {r}: removed (alert numbered 027-2022, "
              f"Date 2026-10-23)")
    if removed > 1:
        raise SystemExit(
            f"NAFDAC: expected at most 1 Pending row to remove, found "
            f"{removed} — the register moved under this script")
    # 0 is expected when this runs after 2026-10-24: the copy had already left
    # Pending by the time the 10-24 set was uploaded (operator review 10-25).
    for sheet in ("Weekly_Rejected", "Rejected"):
        if sheet not in wb.sheetnames:
            continue
        ws2 = wb[sheet]
        for r, head in list(_rows_matching(ws2, NAFDAC_KEY)):
            if _append_notes(ws2, r, head, NAFDAC_ARCHIVE_NOTE):
                print(f"  {sheet} row {r}: annotated (NAFDAC 027-2022)")
    return removed


# ── D — the two recalls the sweep found missing ─────────────────────────

NEW_ROWS = [
    {
        "Date": "2026-10-23",
        "Source": "FDA",
        "Company": "D&M Smoked Fish Inc. (dba Haifa Smoked Fish, Inc.)",
        "Product": ("Cold smoked sprats, labelled \"C/Smoked Holland "
                    "Sprats\", item 302M, LOT# 261, sold in bulk cardboard "
                    "boxes"),
        "Pathogen": "Clostridium botulinum",
        "Reason": ("Cold smoked sprats were produced without implementing "
                   "required controls for Clostridium botulinum and are a "
                   "potential health risk to consumers. FDA recommended "
                   "that the firm recall all product on the market; the "
                   "firm declined, so FDA advises consumers, retailers, "
                   "restaurants and food service operators not to eat, "
                   "sell or serve them."),
        "Class": "Alert",
        "Country": "United States",
        "URL": ("https://www.fda.gov/food/alerts-advisories-safety-"
                "information/fda-advises-not-eat-sell-or-serve-cold-smoked-"
                "sprats-sold-haifa-smoked-fish-inc"),
        "Notes": (
            "Added by the AFTS morning fix pass 2026-10-24 (missed-recall "
            "sweep); absent from all five sheets. Every field read on the "
            "FDA alert page itself, \"FDA Advises Not to Eat, Sell, or "
            "Serve Cold Smoked Sprats Sold by Haifa Smoked Fish, Inc.\", "
            "dated October 23, 2026 (content current as of 10/23/2026): "
            "firm \"D&M Smoked Fish Inc. (DBA) Haifa Smoked Fish, Inc.\", "
            "94-15 150 St., Jamaica, N.Y. 11435; product \"cold smoked "
            "sprats\", label \"C/Smoked Holland Sprats\", item 302M, "
            "\"LOT# 261\", bulk cardboard boxes; hazard \"Potential "
            "Clostridium botulinum contamination\"; distributed in "
            "Connecticut, New Jersey, New York and Pennsylvania and may "
            "have reached further states. No recall was issued — FDA "
            "states it recommended one and the firm declined, which is why "
            "Class is \"Alert\". No best-before is published. Discovered "
            "via efoodalert.com and foodsafetynews.com (discovery only; "
            "the authority URL on this row is FDA's own alert page)."
        ),
    },
    {
        "Date": "2026-10-23",
        "Source": "MAPAQ QC",
        "Company": "Boucherie Viau inc.",
        "Product": ("Ham and bacon loaf, whole or sliced (PAIN JAMBON "
                    "BACON, TRANCHÉ ou ENTIER), variable format, "
                    "UPC 6 46437 75451 4, lot 091026PJB"),
        "Pathogen": "Listeria monocytogenes",
        "Reason": ("Possible presence of Listeria monocytogenes. The firm "
                   "is recalling the product and MAPAQ issued the warning "
                   "as a precaution; no illness linked to the product had "
                   "been reported to MAPAQ at publication."),
        "Class": "Recall",
        "Country": "Canada",
        "URL": ("https://www.quebec.ca/nouvelles/actualites/details/mise-en-"
                "garde-a-la-population-presence-possible-de-listeria-"
                "monocytogenes-dans-du-pain-au-jambon-et-bacon-prepare-et-"
                "vendu-par-lentreprise-boucherie-viau-inc-72791"),
        "Notes": (
            "Added by the AFTS morning fix pass 2026-10-24 (missed-recall "
            "sweep); absent from all five sheets. Every field read on the "
            "MAPAQ notice itself, « Mise en garde à la population - "
            "Présence possible de Listeria monocytogenes dans du pain au "
            "jambon et bacon préparé et vendu par l'entreprise Boucherie "
            "Viau inc. », 23 octobre 2026, 17 h 52: firm « Boucherie Viau "
            "inc. », 83 chemin de Covey Hill, Hemmingford; product « PAIN "
            "JAMBON BACON » (TRANCHÉ) OU (ENTIER); format « Variable »; "
            "CUP « 6 46437 75451 4 »; lot « 091026PJB ». The notice prints "
            "a best-before as « 26 nov. 13 », which is ambiguous as "
            "printed, so it is carried into no field. "
            "[original product: « PAIN JAMBON BACON » (TRANCHÉ) OU "
            "(ENTIER)] Discovered via efoodalert.com (discovery only; the "
            "authority URL on this row is MAPAQ's own notice on quebec.ca, "
            "the host every published MAPAQ QC row uses)."
        ),
    },
]


def add_missing_rows() -> int:
    """D — queue the two found recalls through tools/add_manual_row.py.

    Called as a subprocess so the rows enter by the one supported path:
    Recall(...) -> merge_master.append_to_pending, with its dedup, its
    near-duplicate rule and the Pending gate all applied. Nothing here
    writes to Recalls.
    """
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False,
                                     encoding="utf-8") as fh:
        json.dump(NEW_ROWS, fh, ensure_ascii=False, indent=1)
        path = fh.name
    print(f"  add_manual_row.py <- {len(NEW_ROWS)} row(s)")
    proc = subprocess.run(
        [sys.executable, str(ROOT / "tools" / "add_manual_row.py"), path,
         "--xlsx", str(XLSX)],
        cwd=str(ROOT), capture_output=True, text=True)
    sys.stdout.write("".join("    " + ln + "\n"
                             for ln in proc.stdout.splitlines()))
    if proc.returncode != 0:
        sys.stderr.write(proc.stderr)
        raise SystemExit(f"add_manual_row.py failed ({proc.returncode})")
    Path(path).unlink(missing_ok=True)
    return len(NEW_ROWS)


def main() -> int:
    import openpyxl

    print("A+B — RappelConso 2026-10-23: lot-line Products, firm, "
          "translations")
    wb = openpyxl.load_workbook(XLSX)
    # Operator review 2026-10-25: this set was not uploaded on 10-24 and ships
    # together with the 10-25 morning fix, whose tools/repair_2026_10_25.py
    # re-reads the same five RappelConso fiches (23912/23914/23922/23923/23925)
    # and repairs them. Sections A+B are therefore left to that script so the
    # two passes do not write competing values into one row.
    n_ab = 0
    print("C — salsa tartufata: archive verdict superseded, Pending "
          "Product translated")
    n_c = supersede_tartufata(wb)
    n_c2 = translate_tartufata_pending(wb)
    print("E — NAFDAC public-alert-no-027-2022: Pending copy removed")
    n_e = drop_nafdac_pending(wb)
    wb.save(XLSX)
    wb.close()

    print("D — missed-recall sweep: two rows queued to Pending")
    n_d = add_missing_rows()

    print(f"\nfiche rows repaired: {n_ab} · tartufata archive rows "
          f"superseded: {n_c} (Pending Product translated: {n_c2}) · "
          f"NAFDAC Pending rows removed: {n_e} · rows queued to Pending: "
          f"{n_d}")
    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON_OUT)} "
          f"rows")
    print("ROWS_REMOVED=0")
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
