# -*- coding: utf-8 -*-
"""Morning fix pass 2026-11-18 — data repairs, re-runnable on any main.

Called by tools/rebase_and_verify.py against main's CURRENT workbook, so it
must be idempotent: every edit is guarded by its own stamp.

WHAT IT REPAIRS
---------------
A. FOUR PUBLISHED RappelConso ROWS WERE NOT IN ENGLISH. The language detector
   needs two non-English word hits, so it flagged exactly one of the four —
   fiche 24092's Product. The other three are the short-name blind spot the
   standing instruction to read every Product published in the last 26 h
   exists for, and this is the sixth consecutive morning on which the by-hand
   sweep found one the stamp missed:

     fiche 24102  Product "cocktail de fruits de mer 500g"  (no stamp)
                  Reason  "Une analyse a mis en évidence la présence de
                           Salmonella."            (stamped — "Salmonella"
                           reads as English, so only the Reason tripped it)
     fiche 24094  Product "chipolata superieure traditionnelle"  (no stamp)
     fiche 24095  Product "marshmallows rainbow / guimauve arc -en-ciel"
                  (no stamp — half the name is already English)
     fiche 24092  Product "volaille en gelée"                   (stamped)

   Each fiche was read today and the regulator's own field values are quoted
   in the row's Notes. Nothing beyond the fiche is added.

B. fiche 24092 PUBLISHED THE PRODUCT NAME AS THE FIRM. Company and Brand both
   read "Volaille en Gelée", which is the dish. The fiche's "Nom de la marque
   du produit" genuinely is "volaille en gelée" — the producer filled the brand
   field with the product name — but its "Origine de la fiche" names the firm:
   "EARL DES NOIRETTES". Company is corrected to that; Brand is LEFT as the
   fiche's own Marque, because changing it would depart from the notice.
   Same defect shape as fiche 23996 ("baymar", repaired 2026-11-06).

C. TWO PENDING ROWS WERE BLOCKED ON A FIELD THAT WAS ALREADY IN THEIR OWN
   NOTES.
     * Fødevarestyrelsen chicken gyros — Product held the AUTHORITY'S NAME
       ("Fødevarestyrelsen") and Company and Brand both held the Danish
       HEADLINE ("Tilbagekald: Salmonella i Chicken gyros"). The row's own
       captured Notes carry the answer: "The Lidl Danmark tilbagekalder
       Chicken gyros af mærket Eridanous, da der er fundet Salmonella i
       produktet." Confirmed on the Fødevarestyrelsen page today. A Tier 1
       Salmonella recall has been stuck in Pending since 2026-11-13 on a
       three-field scramble. Source is also normalised to the register's own
       "Fødevarestyrelsen (DK)".
     * CDC raw-milk-cheese outbreak — Product was the single letter "E",
       which held two tests RED. Repaired from the page's own H1.
       COMPANY AND BRAND ARE DELIBERATELY NOT FILLED; see the note written
       onto that row for why, and the pass's verdict message.

D. THREE ARCHIVE ROWS SHARE A URL WITH A PUBLISHED RECALL AND SAID NOTHING
   ABOUT WHICH COPY WINS (test_a_recall_is_not_both_published_and_rejected,
   RED on main):
     * Rejected/FDA Eden Brothers 2026-10-07 — its Notes already say
       "duplicate: this URL is already published on 2026-10-07". The verdict
       was never copied into the RejectReason COLUMN, which is the only place
       that test reads. Moved, not invented.
     * Weekly_Rejected and Rejected, both Ministero della Salute 2026-11-14,
       both news-only discoveries of the Salute LISTING INDEX. They collide
       with published Recalls row of 2026-04-14 (La Vacje Salumerie, Salame
       Nostrano) because that row cites the same index page as its authority
       URL — the R3 defect flagged UNRESOLVED on that row by the 2026-11-14
       pass, which added the INTENTIONAL entry to
       tests/test_a_query_param_recall_id_is_never_dropped.py and stopped
       there. The two archive rows were left silent. They are NOT superseded
       and NOT duplicates of that recall: they are correctly-refused rows
       whose address happens to be the same index page. Flagged UNRESOLVED
       with that explanation.

E. A NON-FOOD ROW WAS APPROVED AT STATUS 'pending'. NAFDAC (NG) public alert
   030/2026 withdraws LEVAMISOLE-CONTAINING MEDICINES. Reviewer 1 confirmed
   its URL and the review agent "reviewed and approved at status 'pending';
   fields verified against the source". It fails R1 — food only — and the one
   thing keeping it out of the register is an empty Pathogen, which any later
   enrichment pass could fill. Archived in place the way this register already
   archives a refused Pending row (rows 5, 6 and 12 of Pending carry the same
   shape): Status 'rejected', RejectedBy set, and the verdict written at the
   FRONT of Notes where merge_master.load_rejected_urls reads it.

Nothing is deleted. No row leaves Recalls.
"""
from __future__ import annotations

import sys
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "recalls.json"
STAMP = "morning fix 2026-11-18"
LIVE = ("Recalls", "Pending", "Weekly_Review")
ARCHIVES = ("Rejected", "Weekly_Rejected")


def _key(u: object) -> str:
    from pipeline.merge_master import _normalize_url_for_dedup
    return _normalize_url_for_dedup(str(u or "").strip())


# ── A + B : published RappelConso rows ────────────────────────────────────
#: url -> (field updates, notes addition, stamps to drop)
FICHE_FIXES = {
    "https://rappel.conso.gouv.fr/fiche-rappel/24102/interne": (
        {"Product": "Seafood cocktail, 500 g",
         "Reason": "An analysis revealed the presence of Salmonella"},
        f"[{STAMP}: the published Product AND Reason were both French. Read on "
        "fiche 24102 today (Fiche réf. 2026-11-0112 • Version 1, publiée le "
        "17/11/2026): h1 \"Cocktail de fruits de mer 500g\", brand line "
        "\"Ocean Sea\", Origine de la fiche \"LIDL LIDL\", Nature juridique "
        "\"Volontaire (sans arrêté préfectoral)\", Catégorie Alimentation / "
        "Produits de la pêche et d'aquaculture, Nom de la marque \"Ocean "
        "Sea\", Modèles ou références \"Cocktail de fruits de mer 500g\", "
        "GTIN 4056489728153 lots \"26113 B\" and \"26117 A\", \"Date de "
        "durabilité minimale 31/05/2027\", Conditionnements \"sachet 500g\", "
        "commercialisée du 04/05/2026 au 17/11/2026, \"Produit à conserver au "
        "congélateur\", Marque de salubrité FR 14.047.031 CE, 21 départements "
        "from AIN (01) to VIENNE (86), Distributeurs \"Voir liste des "
        "magasins en pièce jointe\", Motif \"Une analyse a mis en évidence la "
        "présence de Salmonelles.\", Risques \"Salmonella spp (agent "
        "responsable de la salmonellose)\". The ROW had captured the Motif as "
        "\"…présence de Salmonella.\" where the fiche reads \"…présence de "
        "Salmonelles.\"; the fiche's wording is the one quoted below. No lot, "
        "weight or pack detail is added beyond the fiche's own. Company and "
        "Brand are left alone: 'Ocean Sea' is the fiche's own Marque, and "
        "Company==Brand is this register's established shape for a "
        "RappelConso row that has a real brand. THE PRODUCT CARRIED NO "
        "\"needs cleanup\" STAMP — the detector needs two non-English word "
        "hits and \"cocktail\", \"de\", \"fruits\", \"mer\" plus a weight "
        "read as English to it. Found by the by-hand sweep, not by the "
        "marker.] "
        "[original product: cocktail de fruits de mer 500g] "
        "[original Reason (fr): \"Une analyse a mis en évidence la présence "
        "de Salmonelles.\"]",
        ("[needs cleanup: Reason not in English]",
         "[needs cleanup: Product not in English]"),
    ),
    "https://rappel.conso.gouv.fr/fiche-rappel/24094/interne": (
        {"Product": "Superior traditional chipolata sausages"},
        f"[{STAMP}: the published Product was French and carried NO \"needs "
        "cleanup\" stamp. Read on fiche 24094 today (Fiche réf. "
        "2026-11-0105 • Version 1, publiée le 16/11/2026): h1 \"CHIPOLATA "
        "SUPERIEURE TRADITIONNELLE\", brand line \"Les Ateliers\", Origine "
        "de la fiche \"LES ATELIERS LES ATELIERS\" (so the row's Company is "
        "already the firm), Catégorie Alimentation / Viandes, Nom de la "
        "marque \"Les Ateliers\", Modèles ou références \"CHIPOLATA "
        "SUPERIEURE TRADITIONNELLE\", GTIN 3700502799958 lot 26313-4449, "
        "\"Date limite de consommation 17/11/2026\", Conditionnements "
        "\"barquette sous atmosphere modifiée\", commercialisée du "
        "09/11/2026 au 17/11/2026, réfrigéré, Marque de salubrité "
        "FR 13.097.003 CE, Départements LOIRET (45) and MARNE (51), "
        "Distributeurs \"novoviandes\", Motif \"Détection de salmonelle "
        "spp\", Risques \"Salmonella spp (agent responsable de la "
        "salmonellose)\". \"Chipolata\" is an English word too, so only "
        "\"supérieure traditionnelle\" is translated; no meat species, "
        "weight or lot is added.] "
        "[original product: CHIPOLATA SUPERIEURE TRADITIONNELLE]",
        (),
    ),
    "https://rappel.conso.gouv.fr/fiche-rappel/24095/interne": (
        {"Product": "Marshmallows Rainbow"},
        f"[{STAMP}: the published Product was the fiche's bilingual name with "
        "its French half attached. Read on fiche 24095 today (Fiche réf. "
        "2026-11-0106 • Version 1, publiée le 16/11/2026, header "
        "\"Informations transmises par l'Administration\"): h1 "
        "\"Marshmallows Rainbow / Guimauve arc -en-ciel\", brand line "
        "\"ahead\", Catégorie Alimentation / Cacao, café et thé, Nom de la "
        "marque \"ahead\", Modèles ou références \"Marshmallows Rainbow / "
        "Guimauve arc -en-ciel\", lot F005879, \"Date de durabilité minimale "
        "30/08/2027\", \"Produit à conserver à température ambiante\", France "
        "entière, Distributeurs \"Site internet de la marque\", Motif "
        "\"Détection de Salmonelle\", Risques \"Salmonella spp (agent "
        "responsable de la salmonellose)\". The product's own English name is "
        "the first half of the fiche's title, so that is what is published "
        "and nothing is invented. This fiche shows NO \"Origine de la "
        "fiche\", so Company stays as the Marque — the only firm name the "
        "notice gives. CARRIED NO \"needs cleanup\" STAMP: half the name was "
        "already English, which is below the detector's two-hit threshold.] "
        "[original product: marshmallows rainbow / guimauve arc -en-ciel]",
        (),
    ),
    "https://rappel.conso.gouv.fr/fiche-rappel/24092/interne": (
        {"Product": "Poultry in aspic",
         "Company": "EARL des Noirettes"},
        f"[{STAMP}: the Product was French (this one WAS stamped) and Company "
        "and Brand both held the DISH NAME instead of a firm. Read on fiche "
        "24092 today (Fiche réf. 2026-11-0103 • Version 1, publiée le "
        "16/11/2026): h1 \"volaille en gelée\", Origine de la fiche \"EARL "
        "DES NOIRETTES EARL DES NOIRETTES\", Nature juridique \"Volontaire "
        "(sans arrêté préfectoral)\", Catégorie Alimentation / Viandes, Nom "
        "de la marque du produit \"volaille en gelée\", Modèles ou références "
        "\"volaille en gelée\", no GTIN, lot \"VEG AB42\", \"Date limite de "
        "consommation 04/11/2026\", Conditionnements \"conditionner par deux "
        "tranches sous vide\", commercialisée du 15/10/2026 au 31/10/2026, "
        "\"Produit à conserver au réfrigérateur\", Région "
        "Auvergne-Rhône-Alpes, Distributeurs \"saveurs paysannes Halle "
        "paysanne ferme\", Motif \"présence de listeria\", Risques \"Listeria "
        "monocytogenes (agent responsable de la listériose)\". Company is the "
        "fiche's own originator. BRAND IS LEFT AS IS: the fiche's Marque "
        "field genuinely reads \"volaille en gelée\" — the producer filled "
        "the brand field with the product name — and the register publishes "
        "the field as the notice gives it rather than substituting a "
        "placeholder. Reason was already English and is untouched; no weight, "
        "cut or bird species is added.] "
        "[original product: volaille en gelée] "
        "[original company: Volaille en Gelée]",
        ("[needs cleanup: Product not in English]",),
    ),
}

# ── C : Pending rows blocked on their own text ────────────────────────────
PENDING_FIXES = {
    "https://www.foedevarestyrelsen.dk/nyheder/tilbagekaldte-produkter/2026/"
    "nov/tilbagekald-salmonella-i-chicken-gyros": (
        {"Source": "Fødevarestyrelsen (DK)",
         "Company": "Lidl Danmark K/S",
         "Brand": "Eridanous",
         "Product": "Chicken gyros, 330 g",
         "Reason": "Salmonella found in the product at routine control"},
        f"[{STAMP}: three fields were scrambled and the publish gate held the "
        "row on the one it checks. Product was \"Fødevarestyrelsen\" — the "
        "authority's own name — and Company and Brand both held the Danish "
        "headline \"Tilbagekald: Salmonella i Chicken gyros\". THE ANSWER WAS "
        "ALREADY IN THIS ROW'S OWN NOTES, in the captured lead sentence: "
        "\"The Lidl Danmark tilbagekalder Chicken gyros af mærket Eridanous, "
        "da der er fundet Salmonella i produktet.\" Confirmed on the "
        "Fødevarestyrelsen page today: heading \"Tilbagekald: Salmonella i "
        "Chicken gyros\", dated 13-11-2026; \"Hvem tilbagekalder produktet\" "
        "gives \"Lidl Danmark K/S, Carl Blochs Gade 89, 8000 Aarhus C\"; "
        "\"af mærket Eridanous\"; \"Nettovægt: 330 g\"; \"Bedst før: "
        "22.05.2027\"; NO lot or batch code is listed, so none is recorded; "
        "\"Ved rutinekontrol hos den danske virksomhed er der påvist "
        "Salmonella i produktet\" with NO serotype named, so none is added; "
        "\"Produktet er solgt i: Lidl forretninger i hele landet\". Source is "
        "normalised to the register's own \"Fødevarestyrelsen (DK)\" so this "
        "row groups with the three already published under it. A Tier 1 "
        "Salmonella recall had been stuck in Pending since 2026-11-13 on this "
        "scramble.] "
        "[original company: Tilbagekald: Salmonella i Chicken gyros] "
        "[original product: Fødevarestyrelsen]",
        (),
    ),
    "https://www.cdc.gov/ecoli/outbreaks/raw-milk-cheese-09-26/index.html": (
        {"Product": "Raw milk cheese (Graziers grass-fed raw milk cheeses)"},
        f"[{STAMP}: Product was the single letter \"E\", which held "
        "test_a_product_name_is_not_a_reference_code and "
        "test_product_is_not_only_identifiers RED. The food name is in this "
        "row's own captured heading. Read on the CDC page today: H1 \"E. coli "
        "Outbreak Linked to Raw Milk Cheese\", dated \"Nov. 13, 2026\", "
        "\"Investigation status: Closed\", \"Recall issued: Yes\", \"This "
        "outbreak investigation was closed on November 13, 2026\"; Fast Facts "
        "18 cases, 9 hospitalisations, 0 deaths, 10 states; pathogen \"E. coli "
        "O26\"; the recall section names \"Sierra Nevada Graziers grass-fed "
        "raw milk cheeses: medium cheddar, sharp cheddar, jalapeño jack, and "
        "Monterey jack\", recalled by Sierra Nevada Cheese Company on "
        "September 24, 2026, with the FDA announcement at "
        "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/"
        "sierra-nevada-cheese-company-recalls-graziers-raw-milk-cheese-"
        "because-possible-health-risk . COMPANY \"Sierra Nevada Cheese "
        "Company\" AND BRAND \"Graziers\" ARE RECORDED HERE AND DELIBERATELY "
        "NOT WRITTEN INTO THE FIELDS. Filling Company is the last blocker on "
        "this row, so it would publish this morning carrying Date "
        "2026-11-13 — and that date is the day CDC CLOSED the investigation, "
        "not the date of the recall, which was 2026-09-24. Publishing it as "
        "dated would file a September recall into ISO week 47 and into "
        "November's monthly report. Which date this register should carry for "
        "a closed-outbreak page is a decision for George, not for this pass, "
        "and every value needed to act on it is on this row.]",
        (),
    ),
}

# ── D : archive rows that must say which copy wins ────────────────────────
EDEN = ("https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/"
        "eden-brothers-recalls-calabrese-broccoli-seeds-due-possible-"
        "salmonella-contamination")
EDEN_REASON = (
    "duplicate: this URL is already published on 2026-10-07 (Recalls — Eden "
    "Brothers LLC, Broccoli Seeds - Calabrese, Salmonella). The archived copy "
    "is the second capture of the same FDA notice; the published row wins. "
    f"[{STAMP}: the verdict was already written in this row's Notes by the "
    "confirmer — \"REJECTED: Confirmer: row was at pending_gap_v2 (not "
    "reviewed by reviewer 2) and duplicate: this URL is already published on "
    "2026-10-07\" — but the RejectReason COLUMN was left empty, and that "
    "column is the only place test_a_recall_is_not_both_published_and_rejected "
    "reads. Moved, not invented.]")

SALUTE = ("https://www.salute.gov.it/new/it/avvisi/"
          "avvisi-e-richiami-di-prodotti-alimentari/"
          "?tab=1&offset=0&pageElements=10")
SALUTE_REASON = (
    f"UNRESOLVED CONTRADICTION ({STAMP}): not the same recall. This row's "
    "URL is the "
    "Ministero della Salute LISTING INDEX "
    "(/new/it/avvisi/avvisi-e-richiami-di-prodotti-alimentari), which "
    "normalises to the same dedup key as published Recalls row of 2026-04-14 "
    "(La Vacje Salumerie Snc, Salame Nostrano, Listeria monocytogenes) — a "
    "row that cites the index page as its authority URL instead of the "
    "ministry's own notice PDF under "
    "/new/sites/default/files/external_data/avvisi_sicurezza_alimentare/. "
    "That R3 defect is already flagged on that row and in the INTENTIONAL "
    "list of tests/test_a_query_param_recall_id_is_never_dropped.py by the "
    "2026-11-14 pass, which did not come back to the archive rows. THIS row "
    "is a news-only discovery (ilmessaggero.it) of the index page with no "
    "ministry press-release URL, and refusing it was correct. It is NOT "
    "superseded and NOT a duplicate of the April salame recall: the shared "
    "address identifies no recall at all. The published row wins on that key; "
    "this row stays refused. Resolving it properly means giving the April row "
    "its own notice URL, which no page read by this pass supplies. Original "
    "verdict: ")

GYROS = ("https://www.foedevarestyrelsen.dk/nyheder/tilbagekaldte-produkter/"
         "2026/nov/tilbagekald-salmonella-i-chicken-gyros")
GYROS_REASON = (
    f"SUPERSEDED ({STAMP}): the defect this row was archived on is repaired "
    "and the recall is PUBLISHED in Recalls. The archived copy was refused "
    "by the publish gate for \"Product 'Fødevarestyrelsen' carries the "
    "authority's own name — the page <title> or its site-name suffix was "
    "parsed as the product\", with the Danish headline in Company AND Brand. "
    "All three fields were read off the Fødevarestyrelsen page this morning "
    "(Lidl Danmark K/S; mærket Eridanous; Chicken gyros, nettovægt 330 g) "
    "and the row then passed the full publish gate. NOTHING IN THIS ARCHIVE "
    "ROW RECORDS A GROUND FOR REFUSING THE RECALL ITSELF: its reason column "
    "holds a verbatim capture of the regulator's page, not a verdict, which "
    "is why neither promote_gate_passing.SUPERSEDE_IF nor its no-verdict "
    "branch could retire it automatically — the branch tests for an EMPTY "
    "reason, and page text is not empty. Annotated by hand rather than by "
    "widening that heuristic, because 'the reason column holds something "
    "that is not a verdict' is not a condition a guard should be taught to "
    "act on unsupervised. Original reason column: ")

# ── E : non-food row approved at 'pending' ────────────────────────────────
NAFDAC = ("https://nafdac.gov.ng/public-alert-no-030-2026-alert-on-the-"
          "withdrawal-of-levamisole-containing-medicines-as-endorsed-by-the-"
          "italian-medicines-agency-aifa-over-serious-neurological-risks/")
NAFDAC_NOTE = (
    "REJECTED: out of scope (R1 — food only). NAFDAC public alert 030/2026 "
    "withdraws LEVAMISOLE-CONTAINING MEDICINES, a human/veterinary "
    "anthelmintic drug, not a food. The AFTS scope rule admits undeclared "
    "drugs IN FOOD; a medicines withdrawal is not that. "
    f"[{STAMP}: archived in place, the way Pending rows 5, 6 and 12 already "
    "are. This row was NOT held by scope anywhere: reviewer 1 confirmed its "
    "URL on 2026-11-17 and the review agent recorded \"reviewed and approved "
    "at status 'pending'; fields verified against the source\". The ONLY "
    "thing keeping a drug recall out of a food register this morning was an "
    "empty Pathogen, which any later enrichment pass could fill from "
    "\"Serious neurological risks\". Nothing is deleted; the row keeps its "
    "discovery trail.] ")


def _edit(ws, head, row, updates, note, drop_stamps, label, counter):
    notes = ""
    nc = head.index("Notes") + 1 if "Notes" in head else None
    if nc:
        notes = str(ws.cell(row, nc).value or "")
    if STAMP in notes:
        return
    for field, value in updates.items():
        if field in head:
            ws.cell(row, head.index(field) + 1).value = value
    if nc:
        for s in drop_stamps:
            notes = notes.replace(" " + s, "").replace(s, "")
        ws.cell(row, nc).value = (" ".join(notes.split()) + " " + note).strip()
    counter.append(label)
    print(f"  {label}")


def main() -> int:
    if not XLSX.exists():
        raise SystemExit(f"workbook not found: {XLSX}")
    wb = openpyxl.load_workbook(XLSX)
    done: list[str] = []

    # A + B — published RappelConso language/firm repairs, every live sheet.
    for url, (updates, note, drops) in FICHE_FIXES.items():
        target = _key(url)
        for name in LIVE:
            if name not in wb.sheetnames:
                continue
            ws = wb[name]
            head = [c.value for c in ws[1]]
            if "URL" not in head:
                continue
            iu = head.index("URL") + 1
            for r in range(2, ws.max_row + 1):
                if _key(ws.cell(r, iu).value) != target:
                    continue
                _edit(ws, head, r, updates, note, drops,
                      f"{name}:{r} fiche {url.rstrip('/interne').split('/')[-1]}"
                      f" -> {list(updates)}", done)

    # C — Pending rows blocked on their own text.
    for url, (updates, note, drops) in PENDING_FIXES.items():
        target = _key(url)
        for name in ("Pending",):
            if name not in wb.sheetnames:
                continue
            ws = wb[name]
            head = [c.value for c in ws[1]]
            if "URL" not in head:
                continue
            iu = head.index("URL") + 1
            for r in range(2, ws.max_row + 1):
                if _key(ws.cell(r, iu).value) != target:
                    continue
                _edit(ws, head, r, updates, note, drops,
                      f"{name}:{r} -> {list(updates)}", done)

    # D — archive rows must say which copy wins.
    for sheet, col in (("Rejected", "RejectReason"),
                       ("Weekly_Rejected", "RejectionReason")):
        if sheet not in wb.sheetnames:
            continue
        ws = wb[sheet]
        head = [c.value for c in ws[1]]
        if "URL" not in head or col not in head:
            continue
        iu, ic = head.index("URL") + 1, head.index(col) + 1
        for r in range(2, ws.max_row + 1):
            key = _key(ws.cell(r, iu).value)
            cur = str(ws.cell(r, ic).value or "")
            if STAMP in cur:
                continue
            if key == _key(EDEN):
                ws.cell(r, ic).value = EDEN_REASON
                done.append(f"{sheet}:{r} Eden Brothers duplicate verdict")
                print(f"  {sheet}:{r} RejectReason filled (Eden Brothers duplicate)")
            elif key == _key(SALUTE):
                ws.cell(r, ic).value = (SALUTE_REASON + (cur or "(none recorded)"))
                done.append(f"{sheet}:{r} Salute index UNRESOLVED")
                print(f"  {sheet}:{r} {col} flagged UNRESOLVED (Salute index page)")
            elif key == _key(GYROS) and sheet == "Weekly_Rejected":
                # Rejected is left to the promoter: that copy's Notes DO
                # carry the publish-gate defect, so SUPERSEDE_IF matches it
                # and supersede_archived_copies retires it on the same run
                # that re-promotes the row. Only the Weekly_Rejected copy is
                # invisible to it, and stamping both here would prepend a
                # second annotation over the promoter's own.
                ws.cell(r, ic).value = (GYROS_REASON + (cur or "(none recorded)"))
                done.append(f"{sheet}:{r} chicken gyros archive SUPERSEDED")
                print(f"  {sheet}:{r} {col} marked SUPERSEDED (chicken gyros)")

    # E — non-food row archived in place.
    if "Pending" in wb.sheetnames:
        ws = wb["Pending"]
        head = [c.value for c in ws[1]]
        iu = head.index("URL") + 1
        nc = head.index("Notes") + 1
        for r in range(2, ws.max_row + 1):
            if _key(ws.cell(r, iu).value) != _key(NAFDAC):
                continue
            notes = str(ws.cell(r, nc).value or "")
            if STAMP in notes:
                continue
            ws.cell(r, nc).value = NAFDAC_NOTE + " ".join(notes.split())
            if "Status" in head:
                ws.cell(r, head.index("Status") + 1).value = "rejected"
            if "RejectedBy" in head:
                ws.cell(r, head.index("RejectedBy") + 1).value = (
                    "morning fix pass 2026-11-18 (R1: not food)")
            done.append(f"Pending:{r} NAFDAC levamisole archived (not food)")
            print(f"  Pending:{r} NAFDAC levamisole medicines -> rejected (R1)")

    wb.save(XLSX)
    wb.close()
    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    print(f"REPAIR_2026_11_18_EDITS={len(done)}")
    return 0


if __name__ == "__main__":                              # pragma: no cover
    raise SystemExit(main())
