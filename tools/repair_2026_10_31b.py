"""Morning fix 2026-10-31, part b — the 2026-10-30 hand-off, folded in.

The 2026-10-30 zips were never uploaded. Most of that day's data work is
redone, against today's main, by tools/repair_2026_10_31.py (NAFDAC removal,
ANVISA Source, French translations, fiches 23937/23956/23951, cassoulet
23952, AGES grapes, Maria's Pasta, Glenmark archive). The url-guard on main
now stops reviewer 1 dropping Pending rows, so the 2026-10-30 restore of
the rows that pass lost is no longer needed. What is left, and only that:

  * fiche 23552 — the published Product was the placeholder "Unbranded";
  * CFIA grated carrots (RA-82881) — in Pending since the daily search
    queued it, with no Company and a non-canonical Pathogen;
  * CAA (JP) SBK Toyama, calcium pantothenate over the additive standard —
    in no sheet.

Every value was read on the authority's own page on 2026-10-30.
Runs after tools/repair_2026_10_31.py (sorted order). Idempotent.
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "recalls.json"

RECALLS_FIXES = [
    {
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/23552/interne",
        "set": {
            "Product": ("Plain chicken breast fillet; Tex-Mex marinated "
                        "chicken breast fillet; cornflake-breaded chicken "
                        "breast escalope"),
        },
        "note": (
            "[morning-fix 2026-10-30: the published Product was the single "
            "word \"Unbranded\", which is a brand field, not a food. Read "
            "on fiche 23552 (réf. 2026-09-0173) today: \"Dénomination du "
            "produit: FILET DE POULET NATURE - FILET DE POULET MARINE TEX "
            "MEX - ESCALOPE DE FILET DE POULET CHAPELURE CORNFLAKE\", "
            "catégorie Alimentation / Viandes, Marque \"sans marque\", "
            "Modèles ou références \"sans marque\", Motif \"Détection de "
            "Salmonella Thyphimurium\" (the fiche's own spelling), Risques "
            "\"Salmonella spp (agent responsable de la salmonellose)\", "
            "distributeur \"Grand Frais\", publiée le 18/09/2026. Found by "
            "sweeping every sheet for placeholder-shaped Products after "
            "fiche 23951 turned up the same defect on a row published "
            "yesterday — this one had been live since September and carried "
            "no \"needs cleanup\" stamp, because a one-word English "
            "placeholder is exactly what the language detector cannot "
            "see.] [original product: FILET DE POULET NATURE - FILET DE "
            "POULET MARINE TEX MEX - ESCALOPE DE FILET DE POULET CHAPELURE "
            "CORNFLAKE]"),
    },
]

CARROTS_URL = ("https://recalls-rappels.canada.ca/en/alert-recall/"
               "certain-grated-carrots-recalled-due-pieces-metal")
PENDING_FIXES = [
    {
        "url": CARROTS_URL,
        "set": {
            "Company": "J. G. Fruits & Légumes Inc.",
            "Product": "Grated carrots: 1 kg (UPC 00650746298553); 5 x 1.82 kg "
                       "(UPC 8926930012574 and 10620868203022); 1/16 100 x 2.27 kg; "
                       "all best before 28 October 2026",
            "Pathogen": "Foreign material (metal)",
            "Reason": "Certain grated carrots recalled due to pieces of metal "
                      "(CFIA category: Food - Extraneous Material). Sold in "
                      "Quebec.",
        },
        "note": ("[morning-fix 2026-10-31 (from the unuploaded 2026-10-30 "
                 "pass): Company was EMPTY and Pathogen the one-off string "
                 "\"Pieces Of Metal\". Read on the CFIA notice: recall date "
                 "2026-10-27, original published date 2026-10-29, RA-82881, "
                 "Class 2, recalling firm J. G. Fruits & Légumes Inc., brand "
                 "None on every product, distribution Quebec. Brand left "
                 "empty: the notice lists none. Foreign material is Tier 3.]"),
    },
]

NEW_ROWS = [
    {
        "Date": "2026-10-29",
        "Source": "CAA (JP)",
        "Company": "SBK Toyama",
        "Product": "Pantothenic acid 300 mg",
        "Pathogen": "Unauthorised food additive use (calcium pantothenate)",
        "Reason": ("Violation of the food-additive usage standard for "
                   "calcium pantothenate."),
        "Class": "Recall",
        "Country": "Japan",
        "Outbreak": 0,
        "URL": ("https://www.recall.caa.go.jp/result/detail.php?"
                "rcl=00000036069&screenkbn=01"),
        "Notes": (
            "Added by the morning fix pass 2026-10-30. Read on the CAA "
            "recall-register detail page itself, opened today, and on the "
            "CAA food listing where it is 掲載日 2026/10/29 with 対応 "
            "\"返金／回収\" (refund / recall). 事由 "
            "\"パントテン酸カルシウムの食品添加物使用基準違反\" — a breach "
            "of the food-additive usage standard for calcium pantothenate. "
            "対応開始日 2026年10月28日; the detail page shows no 掲載日 of "
            "its own, so the Date is the listing's 掲載日, and 対応方法 is "
            "blank on the detail page so Class comes from the listing's own "
            "対応 column. Company エスビーケー・トヤマ is katakana "
            "spelling out S-B-K followed by Toyama; the romanisation adds "
            "nothing. Brand is left EMPTY: the page has no brand field. "
            "The page does NOT say what the violation was — not the "
            "quantity, not the food category — so the Reason says only "
            "what the page says. "
            "[original company: エスビーケー・トヤマ] "
            "[original product: パントテン酸300㎎] "
            "[original Reason (ja): "
            "\"パントテン酸カルシウムの食品添加物使用基準違反\"] "
            "[scope: additives over a permitted limit are in AFTS scope. "
            "This is the register's FIRST additive-standard hazard, so "
            "nothing in tools/alert_vocab.py could reach it — no pathogen "
            "term matched the string at all, which "
            "tests/test_alert_vocab.py::test_every_row_matches_at_least_"
            "one_pathogen_term would have caught only after publication. "
            "The term \"Unauthorised food additive\" is added in this same "
            "upload, and it also picks up the already-published and until "
            "now unreachable \"Chlorpyrifos (pesticide residue) and "
            "unauthorized dye E 122\".]"),
    },
]


def _head(ws):
    return [c.value for c in ws[1]]


def _norm(u):
    return str(u or "").strip().rstrip("/").lower()


def _fix(wb, sheet, specs) -> int:
    ws = wb[sheet]
    head = _head(ws)
    done = 0
    for spec in specs:
        hit = False
        for r in range(2, ws.max_row + 1):
            if _norm(ws.cell(r, head.index("URL") + 1).value) != _norm(spec["url"]):
                continue
            hit = True
            notes_c = ws.cell(r, head.index("Notes") + 1)
            notes = str(notes_c.value or "")
            if spec["note"][:60] in notes:
                continue
            for k, v in spec["set"].items():
                if k in head:
                    ws.cell(r, head.index(k) + 1).value = v
            notes_c.value = re.sub(r"[ \t]{2,}", " ", (notes + " " + spec["note"]).strip())
            done += 1
            print(f"  {sheet}: fixed {spec['url'][-52:]}")
        if not hit:
            print(f"  {sheet}: no row at {spec['url'][-52:]} (already moved on)")
    return done


def _present(wb, url) -> bool:
    for ws in wb.worksheets:
        head = _head(ws)
        if "URL" not in head:
            continue
        i = head.index("URL") + 1
        for r in range(2, ws.max_row + 1):
            if _norm(ws.cell(r, i).value) == _norm(url):
                return True
    return False


def main() -> int:
    wb = openpyxl.load_workbook(XLSX)
    fixed = _fix(wb, "Recalls", RECALLS_FIXES)
    pfixed = _fix(wb, "Pending", PENDING_FIXES)
    rows = [r for r in NEW_ROWS if not _present(wb, r["URL"])]
    wb.save(XLSX)
    wb.close()
    if rows:
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False,
                                         encoding="utf-8") as fh:
            json.dump(rows, fh, ensure_ascii=False, indent=2)
            path = fh.name
        try:
            proc = subprocess.run([sys.executable, str(ROOT / "tools" / "add_manual_row.py"), path],
                                  cwd=str(ROOT), text=True, capture_output=True)
            print(proc.stdout.rstrip())
            if proc.stderr.strip():
                print(proc.stderr.rstrip(), file=sys.stderr)
        finally:
            Path(path).unlink(missing_ok=True)
        if proc.returncode != 0:
            raise SystemExit(f"add_manual_row refused a row ({proc.returncode})")
    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    print(f"RECALLS_FIELDS_FIXED={fixed}")
    print(f"PENDING_FIELDS_FIXED={pfixed}")
    print(f"ROWS_QUEUED={len(rows)}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
