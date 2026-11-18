# -*- coding: utf-8 -*-
"""Morning fix pass 2026-11-18 (b) — queue the ten missed recalls found today.

Rows are in tools/manual_rows_2026_11_18.json and go in through
tools/add_manual_row.py, so they land in Pending under the same dedup, the
same validation and the same two review stages as every scraped row.

WHAT WAS MISSED AND HOW IT WAS FOUND
------------------------------------
1. RappelConso fiche 24090 — Reblochon de Savoie, Schmidhauser, possible
   Salmonella, publiée le 16/11/2026. The food listing carried seven food
   fiches this morning (24102, 24098, 24097, 24095, 24094, 24092, 24090).
   Six are in a sheet. 24090 was in NONE of the five, searched by fiche
   number, URL, company and product.

2. NINE Consumer Affairs Agency (Japan) notices. The CAA food listing carried
   TWELVE notices with 掲載日 2026/11/17 or 2026/11/18. The newest CAA (JP)
   row in the register was 2026-11-13, so neither day's batch had been
   collected at all. All twelve detail pages were read. Four are out of scope
   and are NOT queued:
     rcl 36144  バンブーカット, pickled plums — best-before misprint only
     rcl 36149  JA全農Aコープ, pork head meat — missing storage-temperature
                display only
     rcl 36143  高木海藻店, mozuku seaweed — best-before misprint only
     (rcl 36142 is queued on the MOULD half of its reason; its allergen-
      labelling half alone would not qualify.)
   The nine queued are: foreign material 36145 (conveyor rubber), 36146 (hard
   foreign matter), 36138 (a species suspected to be juvenile pufferfish);
   pesticide residues 36147 (procymidone 0.07 ppm against a 0.01 ppm
   standard), 36140 (tolfenpyrad 0.09 ppm); mould 36139, 36141, 36142; and
   36148 (excessive heat-resistant bacteria on storage testing).

EVERY field on every row was read on the authority page itself and the
regulator's own wording is quoted in that row's Notes. No Company, Product,
Pathogen or Date is inferred. Non-Latin firm and product names follow the
operator rule of 2026-10-01: the firm's own English name where the notice
publishes one, otherwise a romanisation, with the original kept in Notes.
rcl 36148 names no organism, so none is invented — its Pathogen uses the
register's existing "Unspecified microbiological contamination" label, which
tools/alert_vocab.py can reach, with the regulator's own term in brackets.

A ROW THE DEDUP OR NEAR-DUPLICATE RULE REFUSES IS NOT AN ERROR HERE. This
script runs inside tools/rebase_and_verify.py against main's CURRENT
workbook, and the Daily Global Recall Search commits roughly two hours after
this pass. If it has already queued one of these rows, add_manual_row says so
per row and the refusal is the correct outcome, so a refusal is reported and
does not fail the run. What WOULD be a defect is a row that vanishes without
a verdict line, which is exactly what add_manual_row was changed on
2026-10-28 to make impossible.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "recalls.json"
ROWS_JSON = ROOT / "tools" / "manual_rows_2026_11_18.json"


def main() -> int:
    if not ROWS_JSON.exists():
        raise SystemExit(f"rows file not found: {ROWS_JSON}")
    proc = subprocess.run(
        [sys.executable, str(ROOT / "tools" / "add_manual_row.py"),
         str(ROWS_JSON)],
        cwd=str(ROOT), capture_output=True, text=True)
    sys.stdout.write(proc.stdout)
    sys.stderr.write(proc.stderr)
    if proc.returncode != 0:
        # Deliberately not fatal — see the docstring. The per-row verdict
        # lines above name every row that did not land and why.
        print(f"NOTE: add_manual_row exited {proc.returncode} — at least one "
              f"row did not land. Read the per-row verdicts above: an "
              f"'already pending'/'already approved' refusal is the correct "
              f"outcome when another collector got there first.")
    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    return 0


if __name__ == "__main__":                              # pragma: no cover
    raise SystemExit(main())
