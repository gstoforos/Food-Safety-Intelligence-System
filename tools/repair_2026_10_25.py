# -*- coding: utf-8 -*-
"""Morning fix pass 2026-10-25. Base 9e2626da.

WHAT WAS WRONG
==============

1. remove-rows: ONE recall published TWICE. RappelConso fiche 2026-10-0245,
   "TETE DE VEAU EN GELEE", Ferme de Larcher, 23.10.2026, is in Recalls at
   both of the addresses RappelConso serves it from:

       .../static/fiches/d71d11bb-67a0-40c6-914d-894deea2471f.html  (10-24)
       .../fiche-rappel/23918/interne                               (10-25)

   Byte-identical Reason (including its "toxi- infections" spacing),
   identical brand, date and distributor list. The canonical id-bearing row
   is kept; the static print-mirror row is removed and archived, which is
   also what pipeline/_url_identity.row_rank prefers.
   Root cause fixed in pipeline/_url_identity.NO_STABLE_ID_PATHS and the
   third dedup axis in pipeline/merge_master.

2. A PUBLICATION DATE SEVEN DAYS IN THE FUTURE. BLV (CH), Danforel A/S,
   "Forellenfilets geräuchert - Nature", Listeria monocytogenes, Tier 1:
   Date 2026-11-01, report_week W44. 01.11.2026 is the product's
   Mindesthaltbarkeitsdatum, printed inside the FSVO listing's own link
   text. FSVO published the recall on 23.10.2026 — read on blv.admin.ch's
   own public listing of warnings and recalls, where it is filed under
   Rückrufe with that date. So a Tier-1 Listeria recall was missing from
   W43 and filed into a November week nobody has built.
   Root cause fixed in scrapers/europe_non_eu/blv_ch.py (take the TRAILING
   date, strip shelf-life-labelled dates first) and netted in
   pipeline/merge_master (no Date beyond tomorrow).

3. SIX ROWS NOT IN ENGLISH, five of them carrying the reviewers' own
   "[needs cleanup: ...]" stamp. Four tests were RED on main at 9e2626da
   because of them: test_language_policy, test_reason_is_not_wholly_foreign,
   test_alert_vocab (ASR reachable by no subscriber word) and
   test_curator_scope_2026_09_09 (ASR classified as no hazard at all).

4. PRODUCT FIELDS HOLDING NO PRODUCT. Three RappelConso rows published a
   barcode string, a use-by date or the company's own name in Product,
   because the scraper fell back to the fiche's "Modèles ou références"
   field, which on these fiches holds exactly that. Each product name was
   read from the fiche itself:

       23922  "Blanquette St Jacques"                     (fiche 2026-10-0249)
       23914  "Noix de Saint Jacques de la Baie de
               Saint Brieuc Label Rouge"                  (fiche 2026-10-0242)
       23918  "TETE DE VEAU EN GELEE"                     (fiche 2026-10-0245)

5. A VERIFIED RECALL BLOCKED ON A FIELD ITS OWN NOTES CARRY. Pending,
   FSANZ, 2026-10-24, Arla protein puddings, glass fragments, reviewer-1 and
   reviewer-2 passed, status pending_gap_v2 — held out of Recalls because
   "Company is empty", while its Notes hold the FSANZ page verbatim:

       "Arla Protein Salted Caramel Flavour Pudding 200g & Arla Protein
        Chocolate Flavour Pudding 200g | Food Standards Australia New
        Zealand ... Arla Foods Mayer Australia ... Recall published 24
        October 2026"

   The company was in Product, the product was in Notes, and Product held
   the company. Also Reason "Glass fragments — outbreak" with Outbreak=1:
   glass in a pudding is not an outbreak, and the flag feeds the outbreak
   count on every weekly and monthly report.

6. FOUR ROWS MARKED REJECTED AND NEVER ARCHIVED. Pending has carried rows
   with Status='rejected' since 2026-10-23/24 through a dozen hourly
   merges. One of them is why test_alert_number_year_matches_date[Pending]
   was RED: NAFDAC "public-alert-no-0013-2018" — a 2018 alert for rabies
   VACCINES (not food) carrying Date 2026-10-24, the day it was discovered.
   All four are archived to Rejected with their existing rejection reason.

NOTHING IS INVENTED. Every English value below is a translation of text
already on the row, or a product name read off the regulator's own fiche;
the original is kept in Notes as [original ...] so the dashboard can show
and search it.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "recalls.json"
TODAY = "2026-10-25"

DUP_URL = ("rappel.conso.gouv.fr/static/fiches/"
           "d71d11bb-67a0-40c6-914d-894deea2471f.html")
DUP_REASON = (
    "REJECTED — duplicate address for one recall. RappelConso fiche "
    "2026-10-0245 (Ferme de Larcher, TETE DE VEAU EN GELEE, published "
    "23.10.2026) is served both at /fiche-rappel/23918/interne and at this "
    "/static/fiches/<uuid>.html print mirror, and both landed in Recalls. "
    "The uuid is not the fiche number, so the URL dedup key could not "
    "collapse the pair. The canonical id-bearing row is kept (see "
    "pipeline/_url_identity.row_rank). Morning fix 2026-10-25.")

ASR_PATHOGEN = "Sulphite-reducing anaerobes (ASR)"

# ── Field repairs, keyed by a distinctive substring of the row's URL ───────
#
# "set" values replace the field. "note" is appended to Notes. "drop_stamps"
# removes a reviewer cleanup stamp that this pass has actioned.
REPAIRS = [
    # 1. BLV (CH) Danforel — best-before date published as the recall date.
    {
        "url": "recallswiss.admin.ch/customer-access/#Recalls/1168",
        "sheets": ("Recalls", "Weekly_Review"),
        "expect": {"Date": "2026-11-01"},
        "set": {
            "Date": "2026-10-23",
            "report_week": "W43",
            "Product": "Smoked trout fillets, plain",
        },
        "note": ("[original product: Forellenfilets geräuchert - Nature] "
                 "[morning-fix 2026-10-25: Date corrected 2026-11-01 -> "
                 "2026-10-23 and report_week W44 -> W43. 01.11.2026 is the "
                 "Mindesthaltbarkeitsdatum printed in the FSVO listing's own "
                 "link text, not the publication date; FSVO's public listing "
                 "of warnings and recalls files this notice under Rückrufe "
                 "with the date 23.10.2026. Scraper fixed at source in "
                 "scrapers/europe_non_eu/blv_ch.py.]"),
        "report_week_sheets": ("Recalls",),
    },
    # 2. RappelConso 23922 — brand mis-cased, barcode string in Product,
    #    French Reason.
    {
        "url": "rappel.conso.gouv.fr/fiche-rappel/23922/",
        "sheets": ("Recalls", "Weekly_Review"),
        "expect": {"Company": "Sk"},
        "set": {
            "Company": "SK",
            "Brand": "SK",
            "Product": ("Scallop blanquette, barcode 0207684000000, "
                        "use-by 2026-10-24"),
            "Reason": ("Listeria monocytogenes detected in the scallops from "
                       "our supplier Celtarmor"),
        },
        "note": ("[original product: Blanquette St Jacques] "
                 "[original Reason (fr): \"Listeria monocytogenes détectée "
                 "dans les noix de st jacques de notre fournisseur "
                 "celtarmor\"] "
                 "[morning-fix 2026-10-25: Product held only a barcode and a "
                 "use-by date; the product name is the fiche title on "
                 "rappel.conso.gouv.fr/fiche-rappel/23922/interne (fiche "
                 "2026-10-0249), whose Nom de la marque is \"SK\" and whose "
                 "Modèles ou références field is empty (\"..\").]"),
        "drop_stamps": ("[needs cleanup: Reason not in English]",),
    },
    # 3. RappelConso 23923 — French Product.
    {
        "url": "rappel.conso.gouv.fr/fiche-rappel/23923/",
        "sheets": ("Recalls", "Weekly_Review"),
        "set": {
            "Product": ("Fresh scallop meat without roe, Bay of "
                        "Saint-Brieuc, Filière Qualité Carrefour — 200 g "
                        "tray"),
        },
        "note": ("[original product: noix de saint-jacques fraîches baie de "
                 "saint brieuc, sans corail filière qualité carrefour - "
                 "barquette 200g] "
                 "[morning-fix 2026-10-25: Product translated; brand name "
                 "kept as published.]"),
        "drop_stamps": ("[needs cleanup: Product not in English]",),
    },
    # 4. RappelConso 23925 — French Product and French Reason.
    {
        "url": "rappel.conso.gouv.fr/fiche-rappel/23925/",
        "sheets": ("Recalls", "Weekly_Review"),
        "set": {
            "Product": "Chickpea tempeh; lentil tempeh; soy tempeh",
            "Reason": ("Microbiological test results above the applicable "
                       "limits on the preceding lot; products from that lot "
                       "withdrawn as a precaution"),
        },
        "note": ("[original product: tempeh de pois chiches; tempeh de "
                 "lentilles; tempeh de soja] "
                 "[original Reason (fr): \"Analyses microbiologiques au "
                 "dessus des normes en vigueur, sur le lot précédent. "
                 "retrait des produits de ce lot par précaution.\"]"),
        "drop_stamps": ("[needs cleanup: Reason not in English]",),
    },
    # 5. RappelConso 23912 — half-translated Reason, French Product.
    {
        "url": "rappel.conso.gouv.fr/fiche-rappel/23912/",
        "sheets": ("Recalls", "Weekly_Review"),
        "set": {
            "Product": "Potato gratin (gratin dauphinois)",
            "Reason": ("Presence of Listeria monocytogenes (the agent "
                       "responsible for listeriosis)"),
        },
        "note": ("[original product: gratin dauphinois] "
                 "[original Reason (fr): \"Presence of Listeria "
                 "monocytogenes (agent responsable de la listériose)\"]"),
        "drop_stamps": ("[needs cleanup: Reason not in English]",),
    },
    # 6. RappelConso 23914 — barcodes and a use-by date in Product.
    {
        "url": "rappel.conso.gouv.fr/fiche-rappel/23914/",
        "sheets": ("Recalls", "Weekly_Review"),
        "set": {
            "Product": ("Scallop meat, Bay of Saint-Brieuc, Label Rouge — "
                        "200 g, 400 g and 1 kg trays; barcodes "
                        "3563243012829, 3563243012621, 3563243014625; "
                        "use-by 2026-10-22"),
        },
        "note": ("[original product: 3563243012829; 22/10/2026; date limite "
                 "de consommation; 2026-10-22; |3563243012621; "
                 "|3563243014625] "
                 "[morning-fix 2026-10-25: Product held only barcodes and a "
                 "use-by date. The product name and the three tray sizes are "
                 "the fiche title and Modèles ou références on "
                 "rappel.conso.gouv.fr/fiche-rappel/23914/interne (fiche "
                 "2026-10-0242). \"Label Rouge\" is a protected French "
                 "quality mark and is kept as written.]"),
    },
    # 7. RappelConso 23918 — the surviving Ferme de Larcher row: company
    #    name in Product, unnormalised Pathogen, French Reason.
    {
        "url": "rappel.conso.gouv.fr/fiche-rappel/23918/",
        "sheets": ("Recalls", "Weekly_Review"),
        "set": {
            "Product": "Jellied calf's head",
            "Pathogen": ASR_PATHOGEN,
            "Reason": ("Risk of foodborne toxi-infection caused by the "
                       "presence of sulphite-reducing anaerobes (ASR), "
                       "presenting as abdominal cramps and pain, and "
                       "diarrhoea"),
        },
        "note": ("[original product: TETE DE VEAU EN GELEE] "
                 "[original Reason (fr): \"Risque de toxi- infections "
                 "alimentaires causées par la présence d’asr se traduisent "
                 "par des crampes et douleurs abdominales, et une "
                 "diarrhée.\"] "
                 "[morning-fix 2026-10-25: Product held the brand name, "
                 "which is verbatim what the fiche's Modèles ou références "
                 "field says (\"FERME DE LARCHER\"); the product name is the "
                 "fiche title on rappel.conso.gouv.fr/fiche-rappel/23918/"
                 "interne (fiche 2026-10-0245). Pathogen 'asr' normalised to "
                 "the full label — ASR is anaérobies sulfito-réducteurs, and "
                 "the fiche's Risques encourus reads \"Autres contaminants "
                 "biologiques\". Vocabulary added to "
                 "pipeline/_publish_gate.HAZARD_CLASS_KEYWORDS and "
                 "tools/alert_vocab.py so the hazard is classified and "
                 "reachable by a subscriber alert.]"),
        "drop_stamps": (
            "[needs cleanup: Reason not in English]",
        ),
    },
]

# ── Pending repair: the FSANZ Arla row, from its own Notes ────────────────
PENDING_FIX_URL = ("foodstandards.gov.au/food-recalls/recall-alert/"
                   "arla-protein-salted-caramel-flavour-pudding-200g-"
                   "arla-protein-chocolate")
PENDING_FIX = {
    "Company": "Arla Foods Mayer Australia",
    "Product": ("Arla Protein Salted Caramel Flavour Pudding 200 g; Arla "
                "Protein Chocolate Flavour Pudding 200 g"),
    "Reason": "Glass fragments",
    "Outbreak": 0,
}
# ── One alert, pending AND rejected on the same URL ──────────────────────
#
# FSAI (IE), 20 Oct 2026, "Recall of several Supervalu and Inspired by Centra
# Smoked Salmon, Mackerel and Prawn Cocktail products", Listeria
# monocytogenes. Pending holds TWO rows on this one URL:
#
#   dated 2026-10-20, reviewer 1 confirmed twice, reviewer 2 (Qwen)
#       verified, status pending_gap_v3 — and it passes the full publish gate
#   Date empty, Company/Product/Reason all holding the page heading,
#       status pending_gap_v2
#
# Weekly_Rejected carries a rejection on the same URL whose reason is
# "row was at pending_enrichment (not reviewed by reviewer 2) and Date is
# empty" — which describes the SECOND row. pipeline.promote_gate_passing's
# re-promotion guard is keyed on the URL, so that rejection blocks the FIRST
# row too, and has done for five days:
#
#   re-promotion BLOCKED https://www.fsai.ie/news-and-alerts/food-alerts/
#   recall-of-several-supervalu-and-inspired-b — already in Weekly_Rejected
#
# The archive is append-only and the guard is a re-ingestion loop-breaker,
# so neither is touched here. The rejection is ANNOTATED as UNRESOLVED,
# naming which row it was about, and the publication decision is the
# operator's. Widening the guard to re-admit a URL after a shape-defect
# rejection is a change to a loop-breaker and wants evidence, not a Sunday.
FSAI_UNRESOLVED_URL = ("fsai.ie/news-and-alerts/food-alerts/"
                       "recall-of-several-supervalu-and-inspired-by-centra")
FSAI_UNRESOLVED_NOTE = (
    " [UNRESOLVED 2026-10-25, morning fix: this rejection is keyed on a URL "
    "that TWO Pending rows share. Its reason (\"row was at "
    "pending_enrichment (not reviewed by reviewer 2) and Date is empty\") "
    "describes the undated copy, whose Company, Product and Reason all hold "
    "the FSAI page heading. The other copy on this URL is dated 2026-10-20, "
    "was URL-confirmed twice by reviewer 1 and verified by reviewer 2, sits "
    "at pending_gap_v3 and PASSES the full publish gate — but "
    "promote_gate_passing's re-promotion guard is URL-keyed, so this "
    "rejection has blocked it since 2026-10-20. Nothing is deleted and the "
    "guard is not widened. OPERATOR DECISION: publish the dated row (an "
    "FSAI Listeria recall of smoked salmon, mackerel and prawn cocktail at "
    "Supervalu and Centra), or confirm it should stay out.]")

PENDING_FIX_NOTE = (
    " [morning-fix 2026-10-25: Company and Product taken from this row's own "
    "Notes, which hold the FSANZ recall-alert page verbatim — heading \"Arla "
    "Protein Salted Caramel Flavour Pudding 200g & Arla Protein Chocolate "
    "Flavour Pudding 200g\", firm \"Arla Foods Mayer Australia\", \"Recall "
    "published 24 October 2026\". Product had held the company name and "
    "Company was empty, which is the only thing the publish gate was "
    "refusing. Reason \"Glass fragments — outbreak\" and Outbreak=1 "
    "corrected: foreign material in a pudding is not an outbreak-associated "
    "incident, and the flag feeds the outbreak count on every weekly and "
    "monthly report.]")


def _head(ws):
    return [c.value for c in ws[1]]


def _rowdict(ws, head, r):
    return {h: ws.cell(r, i + 1).value for i, h in enumerate(head)}


def _set(ws, head, r, field, value):
    ws.cell(r, head.index(field) + 1).value = value


def _append_note(ws, head, r, note, drop_stamps=()):
    if "Notes" not in head:
        return
    cur = str(_rowdict(ws, head, r).get("Notes") or "")
    for stamp in drop_stamps:
        cur = cur.replace(stamp, "")
    # The promoter stamps a longer variant of the same complaint.
    cur = cur.replace(
        "[needs cleanup: Reason not in English and no verified "
        "translation on file]", "")
    cur = " ".join(cur.split())
    _set(ws, head, r, "Notes", (cur + " " + note).strip())


def _archive(arch, head_src_row, reason, rejected_by):
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
            out.append(head_src_row.get(h, ""))
    arch.append(out)


def main() -> int:
    import openpyxl
    wb = openpyxl.load_workbook(XLSX)
    rejected = wb["Rejected"]
    removed = 0
    fixed = 0

    # ── 1. remove-rows: the duplicate static-fiche row ───────────────────
    for sheet in ("Recalls", "Weekly_Review", "Pending"):
        if sheet not in wb.sheetnames:
            continue
        ws = wb[sheet]
        head = _head(ws)
        if "URL" not in head:
            continue
        for r in range(ws.max_row, 1, -1):
            row = _rowdict(ws, head, r)
            if DUP_URL not in str(row.get("URL") or "").lower():
                continue
            if sheet == "Recalls":
                _archive(rejected, row, DUP_REASON,
                         "morning fix " + TODAY)
                removed += 1
                print(f"  Recalls -> Rejected: {row.get('Company')!r} / "
                      f"{row.get('Product')!r} (duplicate address)")
            else:
                print(f"  {sheet}: duplicate copy deleted")
            ws.delete_rows(r, 1)
    if removed != 1:
        raise SystemExit(
            f"expected exactly 1 Recalls row to remove, found {removed}")

    # ── 2. field repairs ─────────────────────────────────────────────────
    for spec in REPAIRS:
        hits = 0
        for sheet in spec["sheets"]:
            if sheet not in wb.sheetnames:
                continue
            ws = wb[sheet]
            head = _head(ws)
            if "URL" not in head:
                continue
            for r in range(2, ws.max_row + 1):
                row = _rowdict(ws, head, r)
                if spec["url"].lower() not in str(row.get("URL") or "").lower():
                    continue
                for field, want in (spec.get("expect") or {}).items():
                    got = str(row.get(field) or "")
                    if got != want:
                        raise SystemExit(
                            f"{spec['url']}: expected {field}={want!r}, "
                            f"found {got!r} — main has changed under this "
                            f"repair; re-read the row before re-running")
                for field, value in spec["set"].items():
                    if field == "report_week" and sheet not in spec.get(
                            "report_week_sheets", spec["sheets"]):
                        continue
                    if field in head:
                        _set(ws, head, r, field, value)
                _append_note(ws, head, r, spec["note"],
                             spec.get("drop_stamps", ()))
                if "LastUpdated" in head:
                    _set(ws, head, r, "LastUpdated", TODAY)
                hits += 1
                print(f"  {sheet}: repaired {spec['url']}")
        if not hits:
            raise SystemExit(f"{spec['url']}: no row matched — refusing to "
                             f"pretend this repair landed")
        fixed += hits

    # ── 3. the FSANZ Arla row in Pending ─────────────────────────────────
    ws = wb["Pending"]
    head = _head(ws)
    arla = 0
    for r in range(2, ws.max_row + 1):
        row = _rowdict(ws, head, r)
        if PENDING_FIX_URL.lower() not in str(row.get("URL") or "").lower():
            continue
        for field, value in PENDING_FIX.items():
            if field in head:
                _set(ws, head, r, field, value)
        cur = str(row.get("Notes") or "")
        _set(ws, head, r, "Notes", cur + PENDING_FIX_NOTE)
        arla += 1
        print("  Pending: FSANZ Arla row — Company/Product filled from its "
              "own Notes, outbreak flag cleared")
    if arla != 1:
        raise SystemExit(f"expected exactly 1 FSANZ Arla Pending row, "
                         f"found {arla}")

    # ── 4. archive the Pending rows already marked rejected ──────────────
    head = _head(ws)
    stale = 0
    for r in range(ws.max_row, 1, -1):
        row = _rowdict(ws, head, r)
        if str(row.get("Status") or "").strip().lower() != "rejected":
            continue
        prior = str(row.get("Notes") or "")
        reason = prior.split("|")[0].strip() or "REJECTED (reason on the row)"
        reason = (f"{reason} [archived by the morning fix of {TODAY}: this "
                  f"row has carried Status='rejected' in Pending since its "
                  f"reviewer rejection and was never moved to the archive.]")
        _archive(rejected, row, reason,
                 str(row.get("RejectedBy") or "") or "reviewer (archived "
                 "by morning fix " + TODAY + ")")
        ws.delete_rows(r, 1)
        stale += 1
        print(f"  Pending -> Rejected: {row.get('Source')!r} / "
              f"{str(row.get('Product'))[:45]!r} (already rejected)")
    print(f"  {stale} previously-rejected Pending row(s) archived")

    # ── 5. annotate the FSAI rejection that blocks a gate-passing row ────
    annotated = 0
    for sheet in ("Weekly_Rejected", "Rejected"):
        if sheet not in wb.sheetnames:
            continue
        ws2 = wb[sheet]
        head2 = _head(ws2)
        if "URL" not in head2 or "Notes" not in head2:
            continue
        for r in range(2, ws2.max_row + 1):
            row = _rowdict(ws2, head2, r)
            if FSAI_UNRESOLVED_URL.lower() not in str(
                    row.get("URL") or "").lower():
                continue
            cur = str(row.get("Notes") or "")
            if "UNRESOLVED 2026-10-25" in cur:
                continue
            _set(ws2, head2, r, "Notes", cur + FSAI_UNRESOLVED_NOTE)
            annotated += 1
            print(f"  {sheet}: FSAI Supervalu rejection annotated UNRESOLVED")
    if not annotated:
        raise SystemExit("expected at least 1 archived FSAI Supervalu row to "
                         "annotate, found none")

    wb.save(XLSX)
    wb.close()

    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    print(f"ROWS_REMOVED={removed}")
    print(f"ROWS_REPAIRED={fixed}")
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
