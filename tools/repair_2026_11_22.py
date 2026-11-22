# -*- coding: utf-8 -*-
"""Morning fix 2026-11-22 — two rows out of Recalls, two French rows repaired,
two stale stamps cleared, one duplicate Pending row collapsed, two verified
missing recalls queued.

REMOVED FROM Recalls (archived to Rejected; nothing is deleted from an archive)
------------------------------------------------------------------------------
1. NCC (ZA) "Deli Hummus range", https://thencc.org.za/media-statement-deli-
   hummus-range-product-safety-recall/ — published 2026-11-20 at 13:48 UTC by
   reviewer 1, Tier 1, with Date 2026-11-20 and report_week W47.

   The NCC's own page, read on 2026-11-22, prints "Date Posted: October 3,
   2024" (article:published_time 2024-10-03T11:00:37+00:00) and says "On
   16 September 2024, Shoprite Checkers notified the Consumer Commission
   (NCC) of a product recall"; the sell-by dates it lists run 10 September to
   8 October 2024. So the Date on the published row was the scrape day, and
   the recall is 25 months old.

   An operator had already settled this URL on 2026-11-06, archiving it from
   Pending as "TERMINAL, UPHELD … the recall is real and roughly 22 months
   stale", after eight earlier refusals. Two defects let it back in; both are
   fixed in code in the same change:
     * pipeline/gap_finder/main.write_pending_rows writes the Pending sheet
       directly and never asked the terminal-rejection record, so the fleet
       re-queued it at 11:53 UTC on 2026-11-20;
     * pipeline/recall_url_agent.py (reviewer 1) saved promote_approved's
       new_approved into Recalls, though the verify-and-tag-only rule locked
       on 2026-04-30 says reviewer 1 never publishes.

   The six archive copies stamped "[SUPERSEDED 2026-11-20 …]" were stamped
   BECAUSE of that publication, so each gets an append-only correction note
   saying the publication was itself the defect and the operator's verdict of
   2026-11-06 stands. The operator's own row (Rejected, Date 2026-11-03) was
   never superseded and is left exactly as it is.

2. NKFH (HU) "Enzym cleaner", brand Bactador,
   https://nkfh.gov.hu/hirek/termekvisszahivas-szag-es-folteltavolito-
   tisztitoszer-visszahivasa-mikrobiologiai-fertozes-veszelye-miatt-1 —
   published into Recalls on 2026-11-22. The NKFH notice, read today, gives
   "Termék megnevezése: Geruchs- und Fleckenentferner Enzymreiniger 750 ml
   (Enzimes szag- és folteltávolító tisztítószer háziállatok utáni
   takarításhoz)" in a "750 ml-es műanyag szórófejes flakon": a 750 ml spray
   bottle of enzyme cleaner for cleaning up after pets, reported through the
   EU Safety Gate. Not a food. R1.

REPAIRED IN PLACE (read on the fiche today)
-------------------------------------------
3. RappelConso fiche 24121 (Fiche réf. 2026-11-0130 • Version 1, 19/11/2026).
   Company and Brand both held the fiche's own 119-character sentence,
   because "Nom de la marque du produit" on that fiche IS that sentence — the
   recall is a prefectoral blocking order with no brand. "Origine de la fiche"
   names COMITE REGIONAL DE LA CONCHYLICULTURE DE LA MEDITERRANEE. Product
   and Reason were French.
4. RappelConso fiche 24108 (Fiche réf. 2026-11-0118 • Version 1, 19/11/2026),
   LE CABRICHOU. Product and Reason were French. "Sous-catégorie de produit:
   Lait et produits laitiers".

STALE STAMPS CLEARED
--------------------
5. Fiche 24099 (Grillo Party) and BLV (CH) Aldi Suisse mini chicken breast
   fillet pieces still carried "[needs cleanup: Product not in English]"
   although both Products were translated on earlier passes. The stamp is a
   work order; leaving it on a finished row sends every later pass back to it.

PENDING
-------
6. AESAN alert 2026_86 was queued twice in one append call on 2026-11-21
   (ScrapedAt 08:41:32Z for both), one "www." apart. The copy reviewer 1 has
   advanced (pending_gap_v2, www. form) is kept; the stale pending_gap copy
   is dropped, with its history folded into the kept row's Notes.
   merge_master.append_to_pending no longer allows the shape.
7. Two verified missing recalls are queued through tools/add_manual_row.py —
   see MANUAL_ROWS below.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON_PATH = ROOT / "docs" / "data" / "recalls.json"
TODAY = "2026-11-22"
STAMP = f"morning fix {TODAY}"

HUMMUS = "thencc.org.za/media-statement-deli-hummus-range-product-safety-recall"
CLEANER = "nkfh.gov.hu/hirek/termekvisszahivas-szag-es-folteltavolito-tisztitoszer"

REMOVE_FROM_RECALLS = [
    (HUMMUS,
     "removed from Recalls — the NCC's own page is dated 3 October 2024 "
     "(\"Date Posted: October 3, 2024\"; it says Shoprite Checkers notified "
     "the NCC on 16 September 2024 and lists sell-by dates of 10/09/2024 to "
     "08/10/2024), so the published Date of 2026-11-20 was the scrape day "
     "and the recall is 25 months stale. An operator archived this exact URL "
     "on 2026-11-06 as TERMINAL, UPHELD. It reached Recalls at 13:48 UTC on "
     "2026-11-20 through two defects fixed in code today: the gap-finder "
     "fleet writes Pending without asking the terminal-rejection record, and "
     "reviewer 1 was saving promote_approved's new_approved into Recalls "
     "although the verify-and-tag-only rule says it never publishes. "
     "TERMINAL — the operator's verdict of 2026-11-06 stands."),
    (CLEANER,
     "removed from Recalls — not a food. The NKFH notice reads \"Termék "
     "megnevezése: Geruchs- und Fleckenentferner Enzymreiniger 750 ml "
     "(Enzimes szag- és folteltávolító tisztítószer háziállatok utáni "
     "takarításhoz)\", packaged as a \"750 ml-es műanyag szórófejes flakon\" "
     "— a spray bottle of enzyme cleaner for cleaning up after pets, "
     "notified through the EU Safety Gate. R1: food only. The publish gate "
     "gained _non_food_household_product_blockers today so the shape cannot "
     "return. TERMINAL."),
]

#: Archive rows stamped SUPERSEDED on 2026-11-20 because of a publication that
#: was itself the defect. Append-only correction; nothing is rewritten.
UNSUPERSEDE_NOTE = (
    f" [{STAMP}: the SUPERSEDED marker of 2026-11-20 on this row was written "
    "by promote_gate_passing because the same URL had just been published in "
    "Recalls. That publication was the defect — the NCC page is dated "
    "3 October 2024 — and the Recalls row was removed on 2026-11-22. The "
    "operator's verdict of 2026-11-06 (TERMINAL, UPHELD) stands. This row's "
    "original refusal is still the register's answer for this URL.]")

REPAIRS = {
    "rappel.conso.gouv.fr/fiche-rappel/24121": {
        "Company": "Comité Régional de la Conchyliculture de Méditerranée",
        "Brand": "Unbranded",
        "Product": ("Shellfish harvested in the Étang de Thau from "
                    "16 November 2026 (all pack formats: wooden baskets, "
                    "loose, trays); health marks beginning FR 34."),
        "Reason": ("PSP marine biotoxins (paralytic shellfish poisoning "
                   "toxins) — harvesting blocked as a precaution pending "
                   "analysis results"),
        "_note": (
            "read on fiche 24121 today (Fiche réf. 2026-11-0130 • Version 1, "
            "19/11/2026, Catégorie \"Alimentation\", Sous-catégorie "
            "\"Produits de la pêche et d'aquaculture\"). Company and Brand "
            "both held the fiche's own subtitle sentence, which is what its "
            "\"Nom de la marque du produit\" field contains: this is a "
            "prefectoral blocking order (\"Nature juridique du rappel: "
            "Imposé par arrêté préfectoral\") with no brand. Company is now "
            "\"Origine de la fiche\" as printed (COMITE REGIONAL DE LA "
            "CONCHYLICULTURE DE LA MEDITERRANEE) and Brand is Unbranded. "
            "Product translated from the H1; Reason translated from "
            "\"Motif du rappel: Biotoxines PSP par mesure de précaution\" "
            "and \"Risques: Biotoxines marines PSP (toxines paralysantes)\". "
            "[original company/brand: \"En attente des résultats d'analyses, "
            "blocage des coquillages récoltés dans l'étang de Thau à partir "
            "du 16 novembre 2026\"] [original product: \"par mesure de "
            "précaution, blocage des coquillages récoltés dans l'étang de "
            "Thau à partir du 16 novembre 2026\"] [original Reason (fr): "
            "\"Biotoxines PSP par mesure de précaution\"]"),
    },
    "rappel.conso.gouv.fr/fiche-rappel/24108": {
        "Product": ("Fresh plain salt-free crottin cheese, x 6, reference "
                    "CRSS6CA, lot 24432, best before 17/11/2026"),
        "Reason": "Suspected presence of Salmonella",
        "_note": (
            "read on fiche 24108 today (Fiche réf. 2026-11-0118 • Version 1, "
            "19/11/2026, Sous-catégorie \"Lait et produits laitiers\", "
            "\"Noms des modèles ou références: Crottin frais nature sans sel "
            "x 6 référence CRSS6CA\", lot 24432, DDM 17/11/2026). "
            "[original product: \"crottin frais nature sans sel x 6; "
            "référence crss6ca\"] [original Reason (fr): \"Suspicion de "
            "présence de Salmonelles\"]"),
    },
}

#: Rows whose Product was translated on an earlier pass but whose work-order
#: stamp was never removed.
CLEAR_STAMPS = [
    ("rappel.conso.gouv.fr/fiche-rappel/24099",
     "[needs cleanup: Product not in English]"),
    ("blv.admin.ch/de/newnsb/BRX4VP3-xxIP",
     "[needs cleanup: Product not in English]"),
]

#: (keep_url_fragment, drop_url_fragment) — one recall, two queued rows.
PENDING_COLLAPSE = [("https://www.aesan.gob.es/alertas/2026_86",
                     "https://aesan.gob.es/alertas/2026_86")]

MANUAL_ROWS = [
    {
        "Date": "2026-11-19",
        "Source": "RappelConso (FR)",
        # The register already spells this firm "Metro France" and this
        # brand "Metro Chef" (3 and 5 rows respectively); the fiche shouts
        # them as "METRO FRANCE" / "Metro chef". One spelling per name —
        # tests/test_firm_names_are_uniform. The fiche's own wording is
        # kept verbatim in Notes below.
        "Company": "Metro France",
        "Brand": "Metro Chef",
        "Product": ("Linseed (graine de lin), 650 g, Metro Chef — GTIN "
                    "4337182082293, lot P227085, best before 01/09/2028"),
        "Pathogen": "Hydrocyanic acid (acide cyanhydrique) — chemical hazard",
        "Reason": "Hydrocyanic acid content above the maximum permitted level",
        "Class": "Voluntary",
        "Country": "France",
        "URL": "https://rappel.conso.gouv.fr/fiche-rappel/24119/interne",
        "Notes": (
            "[morning fix 2026-11-22: missed-recall sweep. Found on the "
            "RappelConso food listing (categorie/0/3) and read on fiche "
            "24119 itself today: Fiche réf. 2026-11-0128 • Version 1, date "
            "19/11/2026, Catégorie \"Alimentation\", Sous-catégorie \"Noix "
            "et graines\", H1 \"GRAINE DE LIN 650G METRO CHEF\", \"Nom de la "
            "marque du produit: Metro chef\", \"Origine de la fiche: METRO "
            "FRANCE\", \"Distributeurs: METRO France\", \"Risques encourus "
            "par le consommateur: Autres contaminants chimiques\", \"Motif "
            "du rappel: Acide cyanhydrique supérieur à la teneur maximale\", "
            "identification GTIN 4337182082293 / lot P227085 / DDM "
            "01/09/2028, \"Zone géographique de vente: France entière\", "
            "\"Nature juridique du rappel: Volontaire (sans arrêté "
            "préfectoral)\". Present in none of the five sheets. Same hazard "
            "and the same wording as Recalls rows 126 and 127 (fiches 24053 "
            "and 24054, 10/11/2026), whose Pathogen value is copied here. "
            "[original product: \"GRAINE DE LIN 650G METRO CHEF\"] "
            "[original Reason (fr): \"Acide cyanhydrique supérieur à la "
            "teneur maximale\"]]"),
    },
    {
        "Date": "2026-11-19",
        "Source": "RappelConso (FR)",
        "Company": "GAEC La Louisa",
        "Brand": "Unbranded",
        "Product": ("Reblochon de Savoie made with raw milk, lots 1409, "
                    "1509, 1609, 1709, 1809, 2109, 2209, 2309 and 2809 "
                    "(recommended consumption dates 14/11/2026 to "
                    "28/11/2026)"),
        "Pathogen": "Salmonella",
        "Reason": ("Withdrawal as a precaution in a context of suspected "
                   "Salmonella"),
        "Class": "Voluntary",
        "Country": "France",
        "URL": "https://rappel.conso.gouv.fr/fiche-rappel/24111/interne",
        "Notes": (
            "[morning fix 2026-11-22: re-queued. The confirm agent moved "
            "this row to Weekly_Rejected this morning (Week_Added "
            "2026-11-22) with RejectionReason \"NO_REASON_RECORDED — "
            "rejected by confirm-agent from RappelConso (FR), status "
            "pending. The writer supplied no verdict and none could be "
            "parsed from Notes; treat as UNAUDITED, not as a confirmed "
            "rejection.\" Its Company and Brand both read \"/\", which is "
            "RappelConso's own no-brand marker and what the fiche prints in "
            "\"Nom de la marque du produit\". Read on fiche 24111 today: "
            "Fiche réf. 2026-11-0120 • Version 1, date 19/11/2026, H1 "
            "\"REBLOCHON DE SAVOIE au lait cru\", \"Origine de la fiche: "
            "GAEC LA LOUISA\", \"Risques encourus par le consommateur: "
            "Salmonella spp (agent responsable de la salmonellose)\", "
            "\"Motif du rappel: retrait par mesure de précaution dans un "
            "contexte suspicion salmonelle\", \"Zone géographique de vente: "
            "Départements : HAUTE-SAVOIE (74)\", nine lots with recommended "
            "consumption dates 14/11/2026 to 28/11/2026, \"Nature juridique "
            "du rappel: Volontaire (sans arrêté préfectoral)\". Company is "
            "now the firm the fiche names; Brand is Unbranded because the "
            "fiche prints \"/\". [original product: \"reblochon de savoie au "
            "lait cru\"] [original Reason (fr): \"Retrait par mesure de "
            "précaution dans un contexte suspicion Salmonella\"]]"),
    },
]


def _key(u) -> str:
    s = str(u or "").strip().lower()
    s = s.split("#")[0]
    for p in ("https://", "http://"):
        if s.startswith(p):
            s = s[len(p):]
    if s.startswith("www."):
        s = s[4:]
    return s.rstrip("/")


def _head(ws):
    return [str(c.value) for c in ws[1]]


def _append_note(ws, row, head, note):
    if "Notes" not in head:
        return
    c = ws.cell(row, head.index("Notes") + 1)
    cur = str(c.value or "")
    if note.strip()[:60] in cur:
        return
    c.value = (cur + note)[:2000]


def _remove_from_recalls(wb) -> int:
    rec = wb["Recalls"]
    head = _head(rec)
    iu = head.index("URL") + 1
    arch = wb["Rejected"]
    ah = _head(arch)
    gone = 0
    for frag, why in REMOVE_FROM_RECALLS:
        rows = [r for r in range(2, rec.max_row + 1)
                if frag in _key(rec.cell(r, iu).value)]
        for r in rows:
            vals = {h: rec.cell(r, i + 1).value for i, h in enumerate(head)}
            out = []
            for h in ah:
                if h == "RejectReason":
                    out.append(f"{STAMP}: {why}")
                elif h == "RejectedBy":
                    out.append(STAMP)
                elif h in ("RejectedAt", "ScrapedAt"):
                    out.append(TODAY)
                elif h == "Status":
                    out.append("rejected")
                else:
                    out.append(vals.get(h, ""))
            arch.append(out)
        for r in reversed(rows):
            rec.delete_rows(r, 1)
            gone += 1
        print(f"  Recalls: removed {len(rows)} row(s) — {frag[:70]}")
        # the Sunday review slice carries its own copy
        if "Weekly_Review" in wb.sheetnames:
            wr = wb["Weekly_Review"]
            wh = _head(wr)
            wiu = wh.index("URL") + 1
            wrows = [r for r in range(2, wr.max_row + 1)
                     if frag in _key(wr.cell(r, wiu).value)]
            for r in reversed(wrows):
                wr.delete_rows(r, 1)
            if wrows:
                print(f"  Weekly_Review: removed {len(wrows)} copy/copies")
    return gone


def _correct_supersede_marks(wb) -> int:
    n = 0
    for sheet, col in (("Rejected", "RejectReason"),
                       ("Weekly_Rejected", "RejectionReason")):
        if sheet not in wb.sheetnames:
            continue
        ws = wb[sheet]
        head = _head(ws)
        if "URL" not in head or col not in head:
            continue
        iu = head.index("URL") + 1
        ic = head.index(col) + 1
        for r in range(2, ws.max_row + 1):
            if HUMMUS not in _key(ws.cell(r, iu).value):
                continue
            cur = str(ws.cell(r, ic).value or "")
            if "SUPERSEDED 2026-11-20" not in cur:
                continue
            _append_note(ws, r, head, UNSUPERSEDE_NOTE)
            ws.cell(r, ic).value = (
                f"[CORRECTED {TODAY} — the SUPERSEDED marker below was "
                "written because the URL had just been published in Recalls; "
                "that publication was the defect and the Recalls row is "
                "removed. The operator's TERMINAL verdict of 2026-11-06 "
                "stands.] " + cur)[:2000]
            if "Status" in head:
                ws.cell(r, head.index("Status") + 1).value = "rejected"
            if "Reviewed" in head:
                ws.cell(r, head.index("Reviewed") + 1).value = "Y"
            n += 1
    print(f"  archives: corrected {n} SUPERSEDED-2026-11-20 row(s)")
    return n


def _repair(wb) -> int:
    n = 0
    for sheet in ("Recalls", "Weekly_Review"):
        if sheet not in wb.sheetnames:
            continue
        ws = wb[sheet]
        head = _head(ws)
        if "URL" not in head:
            continue
        iu = head.index("URL") + 1
        for r in range(2, ws.max_row + 1):
            k = _key(ws.cell(r, iu).value)
            for frag, fields in REPAIRS.items():
                if frag not in k:
                    continue
                for col, val in fields.items():
                    if col.startswith("_") or col not in head:
                        continue
                    ws.cell(r, head.index(col) + 1).value = val
                notes_c = ws.cell(r, head.index("Notes") + 1)
                cur = str(notes_c.value or "")
                for stamp in ("[needs cleanup: Product not in English]",
                              "[needs cleanup: Reason not in English]",
                              "[needs cleanup: Company/Brand not in English]"):
                    cur = cur.replace(stamp, "")
                notes_c.value = (
                    cur.strip() + f" [{STAMP}: {fields['_note']}]")[:2000]
                if sheet == "Recalls" and "LastUpdated" in head:
                    ws.cell(r, head.index("LastUpdated") + 1).value = TODAY
                n += 1
                print(f"  {sheet}: repaired row {r} — {frag[-5:]}")
    return n


def _clear_stamps(wb) -> int:
    n = 0
    for sheet in ("Recalls", "Weekly_Review"):
        if sheet not in wb.sheetnames:
            continue
        ws = wb[sheet]
        head = _head(ws)
        if "URL" not in head or "Notes" not in head:
            continue
        iu = head.index("URL") + 1
        ic = head.index("Notes") + 1
        for r in range(2, ws.max_row + 1):
            k = _key(ws.cell(r, iu).value)
            for frag, stamp in CLEAR_STAMPS:
                if _key(frag) not in k:
                    continue
                cur = str(ws.cell(r, ic).value or "")
                if stamp not in cur:
                    continue
                ws.cell(r, ic).value = (
                    cur.replace(stamp, "").strip()
                    + f" [{STAMP}: the Product on this row was already "
                      "translated on an earlier pass; the stale "
                      "needs-cleanup stamp is removed so later passes do "
                      "not re-open a finished row.]")[:2000]
                n += 1
                print(f"  {sheet}: cleared stale stamp on row {r}")
    return n


def _collapse_pending(wb) -> int:
    if "Pending" not in wb.sheetnames:
        return 0
    ws = wb["Pending"]
    head = _head(ws)
    iu = head.index("URL") + 1
    ic = head.index("Notes") + 1 if "Notes" in head else None
    n = 0
    for keep_u, drop_u in PENDING_COLLAPSE:
        kk, dk = _key(keep_u), _key(drop_u)
        keep_rows = [r for r in range(2, ws.max_row + 1)
                     if str(ws.cell(r, iu).value or "").strip() == keep_u]
        drop_rows = [r for r in range(2, ws.max_row + 1)
                     if str(ws.cell(r, iu).value or "").strip() == drop_u]
        if not keep_rows or not drop_rows:
            continue
        if ic:
            kc = ws.cell(keep_rows[0], ic)
            kc.value = (str(kc.value or "").strip()
                        + f" [{STAMP}: AESAN alert 2026_86 was queued twice "
                          "in one append call on 2026-11-21 (both rows "
                          "ScrapedAt 08:41:32Z), one \"www.\" apart, because "
                          "merge_master.append_to_pending built its dedup "
                          "sets once before the loop and never added a row it "
                          "had just queued. Reviewer 2 indexes Pending as "
                          "url_to_idx[url] = i, so the second copy was "
                          "invisible to it. The other copy (status "
                          "pending_gap, bare host) is dropped; this row, which "
                          "reviewer 1 has advanced, is the one kept. The "
                          "function no longer allows the shape.]")[:2000]
        for r in reversed(drop_rows):
            ws.delete_rows(r, 1)
            n += 1
        print(f"  Pending: dropped {len(drop_rows)} duplicate row(s) of {kk}")
    return n


def _queue_manual_rows() -> int:
    path = ROOT / "tools" / f"manual_rows_{TODAY.replace('-', '_')}.json"
    path.write_text(json.dumps(MANUAL_ROWS, ensure_ascii=False, indent=2),
                    encoding="utf-8")
    print(f"  wrote {path.name}")
    r = subprocess.run(
        [sys.executable, str(ROOT / "tools" / "add_manual_row.py"), str(path)],
        cwd=str(ROOT), capture_output=True, text=True)
    sys.stdout.write(r.stdout)
    sys.stderr.write(r.stderr)
    if r.returncode != 0:
        print(f"  WARNING: add_manual_row exited {r.returncode} — "
              "see the per-row verdict lines above")
    return r.returncode


def main() -> int:
    if not XLSX.exists():
        raise SystemExit(f"workbook not found: {XLSX}")
    wb = openpyxl.load_workbook(XLSX)
    gone = _remove_from_recalls(wb)
    corrected = _correct_supersede_marks(wb)
    repaired = _repair(wb)
    cleared = _clear_stamps(wb)
    collapsed = _collapse_pending(wb)
    wb.save(XLSX)
    wb.close()
    rc = _queue_manual_rows()
    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"  recalls.json re-mirrored: "
          f"{mirror_json_from_xlsx(XLSX, JSON_PATH)} rows")
    print(f"REMOVED={gone} CORRECTED={corrected} REPAIRED={repaired} "
          f"STAMPS_CLEARED={cleared} PENDING_COLLAPSED={collapsed} "
          f"MANUAL_RC={rc}")
    return 0


if __name__ == "__main__":                              # pragma: no cover
    raise SystemExit(main())
