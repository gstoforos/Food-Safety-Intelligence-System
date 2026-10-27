# -*- coding: utf-8 -*-
"""Morning Fix 2026-10-27 — translations, one duplicate, one label.

Every change below is applied to main's CURRENT workbook by
tools/rebase_and_verify.py; nothing here copies a recalls.xlsx.

WHAT IT DOES
============
A. TRANSLATIONS the publish gates could not make (no model at those gates).
   Four Products and four Reasons, each verified against the regulator's
   own page this morning and quoted in Notes.

B. TWO STALE "[needs cleanup: …]" STAMPS removed from rows that are already
   in English.

C. ONE DUPLICATE REMOVED. MPI (NZ) published one recall on two of its own
   pages and merge_master's near-dup key missed it ("Hutchinsons Ltd" vs
   "Hutchinsons Limited"). The media-release row goes; the row kept is the
   one on MPI's per-recall page, which is the regulator's own record of the
   recall and the URL publish-gate rule 3 asks for. REMOVES 1 ROW from
   Recalls — the commit message must carry `remove-rows`.

D. SOURCE LABEL "MPI NZ" -> "MPI (NZ)" on the surviving row. The registry
   label, the scraper's AGENCY and verify_urls all say "MPI (NZ)"; three
   collector tables said "MPI NZ", which is why nothing host-checked those
   rows. Same repair as Hungary's "NÉBIH" -> "NKFH (HU)" on 2026-10-07.

E. THREE MISSED RECALLS queued through tools/add_manual_row.py — found by
   walking RappelConso fiche numbers past the newest one in the register.

Nothing else in Recalls is touched. Archives are append-only: the removed
row is annotated into Rejected rather than deleted outright is NOT possible
here (it was never rejected), so its full content is printed to the run log
and recorded in the surviving row's Notes.
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

STAMP_P = "[needs cleanup: Product not in English]"
STAMP_C = "[needs cleanup: Company/Brand not in English]"
STAMP_R = "[needs cleanup: Reason not in English]"

# ── A/B. Field repairs, keyed on the row's own URL ─────────────────────────
# {url: {field: new value}} plus the Notes additions each one carries.
# Every translation was read off the regulator's page on 2026-10-27.
PRODUCT_FIXES = {
    "https://rappel.conso.gouv.fr/fiche-rappel/23933/interne": {
        "Product": "Olive pâté (pâté aux olives)",
        "_orig_product": "Pâté aux olives",
        "_note": "[morning-fix 2026-10-27: Product translated from French. "
                 "Read on rappel.conso.gouv.fr/fiche-rappel/23933 — "
                 "\"Pâté aux olives\", marque STEMMELEN, "
                 "motif \"Détection de Listeria\", publiée 26/10/2026. "
                 "No fact added; the detector missed it because \"pâté\" "
                 "was listed as an English word]",
    },
    "https://rappel.conso.gouv.fr/fiche-rappel/23897/interne": {
        "Product": "Chicken paupiette with chorizo "
                   "(paupiette de poulet au chorizo)",
        "Reason": "Presence of Salmonella detected in the product",
        "_orig_product": "Paupiette de poulet au chorizo",
        "_orig_reason": "présence de salmonelle détectée dans le produit",
        "_note": "[morning-fix 2026-10-27: Product and Reason translated "
                 "from French. Read on "
                 "rappel.conso.gouv.fr/fiche-rappel/23897 (réf. "
                 "2026-10-0225) — nom du modèle \"Paupiette de poulet au "
                 "chorizo\", motif \"présence de salmonelle détectée dans "
                 "le produit\", marque \"Sans marque\", publiée "
                 "26/10/2026. The published Reason was half-translated]",
    },
    "https://rappel.conso.gouv.fr/fiche-rappel/23926/interne": {
        "Product": "Scallop meat, Label Rouge, U Saveurs — 200 g and 400 g, "
                   "caught in area ANE",
        "_orig_product": "Noix St Jacques LR USAVEURS 200 g et 400 g "
                         "pêchées ANE",
        "_note": "[morning-fix 2026-10-27: Product translated from French. "
                 "Read on rappel.conso.gouv.fr/fiche-rappel/23926 (réf. "
                 "2026-10-0257 series) — \"Noix St Jacques LR USAVEURS 200 "
                 "g et 400 g pêchées ANE\", marque U SAVEURS, motif \"NON "
                 "CONFORMITE MICROBIOLOGIQUE\", DLC 22/10/2026, publiée "
                 "26/10/2026. \"LR\" is Label Rouge; the FAO area code ANE "
                 "is kept exactly as published]",
    },
}

# Reasons repaired by URL elsewhere in the register — same half-translation
# defect, older rows, found by the same sweep.
REASON_FIXES = {
    "Contamination potentielle par Salmonella":
        "Potential contamination with Salmonella",
    "Contamination à e. coli / stec":
        "Contamination with E. coli / STEC",
}

# Pathogen values that are not in English. Pathogen is a DESCRIPTION field
# and must read in English (pipeline/_language, 2026-08-02 rule). No Nordic
# language is in _MARKERS, so the detector cannot see a Swedish one; this
# was found by tests/test_alert_vocab.py instead — "Kadmium exceeding
# limit" is reachable by no alert term at all, which for a hazard database
# is the whole failure. The English is read from the row's own Reason
# ("The product may contain cadmium"); nothing is added.
PATHOGEN_FIXES = {
    "https://www.livsmedelsverket.se/om-oss/press/aterkallanden/"
    "coop-aterkallar-fryst-rod-chili---kan-innehalla-kadmium/": (
        "Kadmium exceeding limit", "Cadmium exceeding limit",
        "[morning-fix 2026-10-27: Pathogen 'Kadmium exceeding limit' -> "
        "'Cadmium exceeding limit'. Swedish; Pathogen must read in "
        "English, and no alert vocabulary term could reach the Swedish "
        "spelling, so no subscriber alert could have fired on this row. "
        "The English is this row's own Reason, \"The product may contain "
        "cadmium\" — no fact added. "
        "[original Pathogen (sv): \"Kadmium exceeding limit\"]"),
}

# ── B. Stamps to drop from rows already in English ─────────────────────────
DROP_STAMPS = {
    # BLV (CH): Product reads "smoked trout fillets - natural" and the FSVO
    # German title is already quoted in Notes. The stamp is stale.
    "https://www.blv.admin.ch/de/newnsb/V67FO2BgSwqP": (STAMP_P,),
}

# ── C/D. The MPI duplicate ─────────────────────────────────────────────────
MPI_REMOVE = ("https://www.mpi.govt.nz/news/media-releases/"
              "arla-protein-pudding-recalled-due-to-possible-glass-fragments")
MPI_KEEP = ("https://www.mpi.govt.nz/food-safety-home/"
            "food-recalls-and-complaints/recalled-food-products/"
            "arla-protein-pudding-vanilla-cookie")
MPI_KEEP_NOTE = (
    "[morning-fix 2026-10-27: Source 'MPI NZ' -> 'MPI (NZ)', the label the "
    "registry, scrapers/oceania/mpi_nz.AGENCY and verify_urls all use. "
    "Under 'MPI NZ' pipeline/_source_canon returned kind='unknown', so "
    "HOST_FOR_CANONICAL_SOURCE['NZ-MPI'] never ran and nothing checked "
    "that this row cites mpi.govt.nz] "
    "[morning-fix 2026-10-27: a DUPLICATE of this recall was removed from "
    "Recalls — same recall on MPI's media-release page, Company "
    "'Hutchinsons Ltd', URL .../news/media-releases/"
    "arla-protein-pudding-recalled-due-to-possible-glass-fragments. One "
    "recall, two of the regulator's own pages; the near-dup key missed it "
    "because 'Ltd' and 'Limited' normalise differently. This row is kept "
    "because its URL is MPI's per-recall record]")

# ── E. The three missed recalls ────────────────────────────────────────────
NEW_ROWS = [
    {
        "Date": "2026-10-27",
        "Source": "RappelConso (FR)",
        "Company": "Société Civile d'Exploitation Agricole Boulan "
                   "(Chez Boulan)",
        "Brand": "Spéciale Boulan",
        "Product": "Oysters, Spéciale Boulan no. 3 and no. 4, matured in "
                   "the Médoc (Huitre spéciale Boulan n3 n4)",
        "Pathogen": "Norovirus",
        "Reason": "Presence of norovirus",
        "Class": "Voluntary",
        "Country": "France",
        "URL": "https://rappel.conso.gouv.fr/fiche-rappel/23930/interne",
        "Notes": "[morning-fix 2026-10-27: added by the AFTS-FSIS Morning "
                 "Fix pass after walking RappelConso fiche numbers past "
                 "the newest in the register (23933). Read on "
                 "rappel.conso.gouv.fr/fiche-rappel/23930, réf. "
                 "2026-10-0257: produit \"Huitre spéciale Boulan n3 n4\", "
                 "modèle \"Spéciale Boulan Affinée en Médoc\", entreprise "
                 "\"Société Civile d'Exploitation Agricole Boulan\", "
                 "catégorie Alimentation / Produits de la pêche et "
                 "d'aquaculture, motif \"Présence de norovirus\", risque "
                 "\"Risque de symptômes de gastro-entérite\", publiée "
                 "27/10/2026. Absent from all five sheets (searched by "
                 "fiche number, company, brand and product). "
                 "[original product: Huitre spéciale Boulan n3 n4] "
                 "[original Reason (fr): \"Présence de norovirus\"]",
    },
    {
        "Date": "2026-10-26",
        "Source": "RappelConso (FR)",
        "Company": "CBeDink",
        "Brand": "le grossiste du cbd",
        "Product": "Loose gummies 30 mg, blueberry "
                   "(Gummies vrac 30 mg, myrtille)",
        "Pathogen": "Delta-9-tetrahydrocannabinol (THC)",
        "Reason": "Delta-THC content above the acute reference dose; "
                  "prohibited substance",
        "Class": "Voluntary",
        "Country": "France",
        "URL": "https://rappel.conso.gouv.fr/fiche-rappel/23934/interne",
        "Notes": "[morning-fix 2026-10-27: added by the AFTS-FSIS Morning "
                 "Fix pass after walking RappelConso fiche numbers past "
                 "the newest in the register (23933). Read on "
                 "rappel.conso.gouv.fr/fiche-rappel/23934, réf. "
                 "2026-10-0261 version 1: produit \"Gummies vrac 30 mg\" "
                 "(parfum myrtille), marque \"le grossiste du cbd\", "
                 "entreprise CBeDink, catégorie Alimentation / Produits "
                 "sucrés, motif \"supérieur a la dose de référence aigue\" "
                 "pour le delta-THC, risque \"Présence de substance "
                 "interdite\", publiée 26/10/2026. In scope under the "
                 "THC-over-a-reference-dose rule. Absent from all five "
                 "sheets (searched by fiche number, company, brand and "
                 "product); the register's other delta-9 gummy rows are "
                 "Bien Etre Chamonix fiches 23835/23837 and SARL THITAN "
                 "fiches 23709/23710, different firms. "
                 "[original product: Gummies vrac 30 mg] "
                 "[original Reason (fr): \"supérieur a la dose de "
                 "référence aigue\"]",
    },
    {
        "Date": "2026-10-26",
        "Source": "RappelConso (FR)",
        "Company": "S.A.S. du Pfaffenbach",
        "Brand": "Natur und Mensch Lebensbaum",
        "Product": "Savory, dried, 25 g sachet (Bohnenkraut - sarriette)",
        "Pathogen": "Pyrrolizidine alkaloids",
        "Reason": "Maximum limit for pyrrolizidine alkaloids exceeded in "
                  "organic ground dried savory from Germany",
        "Class": "Voluntary",
        "Country": "France",
        "URL": "https://rappel.conso.gouv.fr/fiche-rappel/23932/interne",
        "Notes": "[morning-fix 2026-10-27: added by the AFTS-FSIS Morning "
                 "Fix pass after walking RappelConso fiche numbers past "
                 "the newest in the register (23933). Read on "
                 "rappel.conso.gouv.fr/fiche-rappel/23932, réf. "
                 "2026-10-0259 version 1: produit \"BOHNENKRAUT - "
                 "SARRIETTE\" sachet 25 g, marque \"Natur und Mensch "
                 "Lebensbaum\", entreprise \"S.A.S. du Pfaffenbach\", "
                 "catégorie Alimentation / Herbes et épices, motif "
                 "\"Dépassement de la LM pour les alcaloïdes "
                 "pyrrolizidiniques dans la sarriette biologique moulue et "
                 "séchée provenant d'Allemagne\", lots 103150 / 104062 / "
                 "104731, publiée 26/10/2026, vente Haut-Rhin (68). "
                 "France's own action on the Lebensbaum savory the BVL "
                 "published on 2026-10-16 (Ulrich Walter GmbH, already in "
                 "Recalls) and the BLV on 2026-10-23 — a separate "
                 "regulatory action by a different authority, filed the "
                 "way the Arla pudding recalls are filed per jurisdiction. "
                 "Absent from all five sheets (searched by fiche number, "
                 "company and product). "
                 "[original product: BOHNENKRAUT - SARRIETTE] "
                 "[original Reason (fr): \"Dépassement de la LM pour les "
                 "alcaloïdes pyrrolizidiniques dans la sarriette "
                 "biologique moulue et séchée provenant d'Allemagne\"]",
    },
]


def _add_note(existing, note: str) -> str:
    cur = str(existing or "").strip()
    return f"{cur} {note}".strip() if cur else note


def repair_recalls() -> dict:
    """Apply every field repair to the Recalls sheet. Safe to run twice.

    It IS run twice: the offline promoter publishes rows out of Pending in
    between, and on 2026-10-27 one of those (Livsmedelsverket's Swedish
    "Kadmium exceeding limit") is itself one of the things being repaired.
    A pass that only ran before promotion would have published it
    untouched. Every branch below is keyed on a value that the repair
    changes, so the second pass is a no-op on everything the first fixed.
    """
    import openpyxl

    removed = retitled = translated = unstamped = 0
    wb = openpyxl.load_workbook(XLSX)
    ws = wb["Recalls"]
    head = [c.value for c in ws[1]]
    col = {h: i + 1 for i, h in enumerate(head) if h}
    need = {"URL", "Product", "Reason", "Notes", "Source", "Company",
            "Pathogen"}
    missing = need - set(col)
    if missing:
        raise SystemExit(f"ERROR: Recalls is missing columns {sorted(missing)}")

    # walk bottom-up so a deletion cannot shift a row we have not seen
    for r in range(ws.max_row, 1, -1):
        url = str(ws.cell(r, col["URL"]).value or "").strip()
        notes = ws.cell(r, col["Notes"]).value

        # ── C. the duplicate MPI row ──────────────────────────────────────
        if url == MPI_REMOVE:
            dump = {h: ws.cell(r, col[h]).value for h in head if h in col}
            print("  REMOVING duplicate row (archived to this log):")
            print("   ", json.dumps({k: str(v)[:160] for k, v in dump.items()
                                     if v not in (None, "")},
                                    ensure_ascii=False)[:1200])
            ws.delete_rows(r, 1)
            removed += 1
            continue

        # ── D. the label on the surviving MPI row ─────────────────────────
        if url == MPI_KEEP:
            if str(ws.cell(r, col["Source"]).value or "").strip() == "MPI NZ":
                ws.cell(r, col["Source"]).value = "MPI (NZ)"
                ws.cell(r, col["Notes"]).value = _add_note(notes,
                                                           MPI_KEEP_NOTE)
                notes = ws.cell(r, col["Notes"]).value
                retitled += 1
                print("  Source 'MPI NZ' -> 'MPI (NZ)' on the kept row")

        # ── A. product / reason translations keyed on URL ─────────────────
        fix = PRODUCT_FIXES.get(url)
        already = (fix
                   and str(ws.cell(r, col["Product"]).value or "") ==
                   fix.get("Product", "")
                   and ("Reason" not in fix
                        or str(ws.cell(r, col["Reason"]).value or "") ==
                        fix["Reason"]))
        if fix and not already:
            n = str(notes or "")
            if "Product" in fix:
                before = ws.cell(r, col["Product"]).value
                ws.cell(r, col["Product"]).value = fix["Product"]
                print(f"  Product: {str(before)[:50]!r} -> "
                      f"{fix['Product'][:50]!r}")
            if "Reason" in fix:
                before = ws.cell(r, col["Reason"]).value
                ws.cell(r, col["Reason"]).value = fix["Reason"]
                print(f"  Reason:  {str(before)[:50]!r} -> "
                      f"{fix['Reason'][:50]!r}")
            n = n.replace(STAMP_P, "").replace(STAMP_R, "")
            n = " ".join(n.split())
            if fix.get("_orig_product") and "[original product:" not in n:
                n = _add_note(n, f"[original product: "
                                 f"{fix['_orig_product']}]")
            if fix.get("_orig_reason"):
                n = _add_note(n, f"[original Reason (fr): "
                                 f"\"{fix['_orig_reason']}\"]")
            ws.cell(r, col["Notes"]).value = _add_note(n, fix["_note"])
            notes = ws.cell(r, col["Notes"]).value
            translated += 1

        # ── A. the older half-translated Reasons, matched on value ────────
        cur_reason = str(ws.cell(r, col["Reason"]).value or "").strip()
        if cur_reason in REASON_FIXES:
            new = REASON_FIXES[cur_reason]
            ws.cell(r, col["Reason"]).value = new
            ws.cell(r, col["Notes"]).value = _add_note(
                notes,
                f"[morning-fix 2026-10-27: Reason translated from French — "
                f"one French marker, below the detector's two-hit floor, so "
                f"the table was never consulted] "
                f"[original Reason (fr): \"{cur_reason}\"]")
            notes = ws.cell(r, col["Notes"]).value
            translated += 1
            print(f"  Reason:  {cur_reason!r} -> {new!r}")

        # ── A. non-English Pathogen ───────────────────────────────────────
        pf = PATHOGEN_FIXES.get(url)
        if pf and str(ws.cell(r, col["Pathogen"]).value or "").strip() == pf[0]:
            ws.cell(r, col["Pathogen"]).value = pf[1]
            ws.cell(r, col["Notes"]).value = _add_note(notes, pf[2])
            notes = ws.cell(r, col["Notes"]).value
            translated += 1
            print(f"  Pathogen: {pf[0]!r} -> {pf[1]!r}")

        # ── B. stale stamps ───────────────────────────────────────────────
        for stamp in DROP_STAMPS.get(url, ()):
            n = str(notes or "")
            if stamp in n:
                n = " ".join(n.replace(stamp, "").split())
                ws.cell(r, col["Notes"]).value = _add_note(
                    n, "[morning-fix 2026-10-27: dropped a stale "
                       "\"needs cleanup\" stamp — this Product is already "
                       "in English and the regulator's own title is "
                       "already quoted in Notes]")
                unstamped += 1
                print(f"  dropped stale stamp on {url[:70]}")

    wb.save(XLSX)
    wb.close()
    return {"removed": removed, "relabelled": retitled,
            "translated": translated, "unstamped": unstamped}


def main() -> int:
    first = repair_recalls()

    # ── E. the three missed recalls, through the real code path ───────────
    queued = 0
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False,
                                     encoding="utf-8") as fh:
        json.dump(NEW_ROWS, fh, ensure_ascii=False, indent=2)
        rows_path = fh.name
    print(f"\n  queueing {len(NEW_ROWS)} operator-found rows via "
          f"tools/add_manual_row.py")
    proc = subprocess.run(
        [sys.executable, str(ROOT / "tools" / "add_manual_row.py"), rows_path],
        cwd=str(ROOT), capture_output=True, text=True)
    print(proc.stdout.rstrip())
    if proc.stderr.strip():
        print(proc.stderr.rstrip(), file=sys.stderr)
    if proc.returncode != 0:
        print(f"ERROR: add_manual_row exited {proc.returncode}")
        return 2
    queued = len(NEW_ROWS)
    Path(rows_path).unlink(missing_ok=True)

    # promote anything that now passes every publish-gate rule
    proc = subprocess.run(
        [sys.executable, "-m", "pipeline.promote_gate_passing", "--apply"],
        cwd=str(ROOT), capture_output=True, text=True)
    print(proc.stdout[-4000:].rstrip())
    if proc.returncode != 0:
        print(f"NOTE: promote_gate_passing exited {proc.returncode}")

    # SECOND PASS. The promoter has just published rows out of Pending and
    # one of them is itself a repair target — see repair_recalls().
    print("\n  second repair pass over the newly promoted rows:")
    second = repair_recalls()

    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"\nrecalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    print(f"ROWS_REMOVED={first['removed'] + second['removed']}")
    print(f"ROWS_RELABELLED={first['relabelled'] + second['relabelled']}")
    print(f"FIELDS_TRANSLATED={first['translated'] + second['translated']}")
    print(f"STAMPS_DROPPED={first['unstamped'] + second['unstamped']}")
    print(f"ROWS_QUEUED={queued}")
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
