"""Regression tests for the writer-level Class normalisation.

Part A below is HISTORY: the url-gate it describes (url_gate_gemini) was
removed on 2026-09-30 with every Gemini path, so its tests went with it.
Part B — the writer normalising Class — is still live and still tested.

WHY THIS FILE EXISTS (audit 2026-07-30)
=======================================
A. CROSS-ROW CONTAMINATION.
   url_gate_gemini sends rows to Gemini in batches and maps each returned
   decision back with `real_idx = start + d["row_index"]`. Every check around
   that line validated whether the decision was PLAUSIBLE — confidence,
   date_match, brand_match, bare-domain, JS artifacts — and none validated
   whether it belonged to that row. `row_index` is self-reported by the
   model, so a shifted index passed all of them: the content is genuine, it
   just describes a different recall.

   Found in production: ten RappelConso rows carrying a neighbour's Reason.
   Each shares its exact Reason string with a row from another source whose
   Pathogen matches that Reason correctly, e.g.

       Recalls row 740, fiche 22205, Pathogen "Listeria monocytogenes"
         Reason "Aflatoxins in mini corn wafers from Slovakia, raw material
                 from Hungary.; risk: serious; category: ..."
       Recalls row 745, RASFF (EU), Pathogen "Aflatoxin"
         Reason  <identical string>

   That "; risk: serious; category: ..." shape is verbatim RASFF
   notification text; RappelConso never emits it.

B. CLASS LANGUAGE BYPASS.
   41 rows reached the published Recalls sheet holding raw French Class
   values ("volontaire (sans arrete prefectoral)", "impose par arrete
   prefectoral") despite Recall.__post_init__ and promote_approved both
   normalising. All 41 carry a [url-gate ...] note — they were updated in
   place after promotion, so neither earlier gate ran again.

Run:  python -m pytest tests/test_url_gate_identity.py -v
"""
from __future__ import annotations
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pipeline.merge_master import _write_sheet, SCHEMA  # noqa: E402
from scrapers._models import _normalize_class_language  # noqa: E402


FICHE = "https://rappel.conso.gouv.fr/fiche-rappel/{}/Interne"


class TestWriterNormalisesClass(unittest.TestCase):
    """B: the writer is the one gate that cannot be bypassed."""

    def test_normaliser_maps_the_production_values(self):
        self.assertEqual(
            "Voluntary",
            _normalize_class_language("volontaire (sans arrêté préfectoral)"))
        self.assertEqual(
            "Mandatory",
            _normalize_class_language("imposé par arrêté préfectoral"))

    def test_write_sheet_normalises_raw_french_class(self):
        from openpyxl import Workbook
        wb = Workbook()
        rows = [
            {"Date": "2026-07-28", "Source": "RappelConso (FR)",
             "Company": "E.Leclerc Outreau", "Product": "x",
             "Pathogen": "Listeria monocytogenes",
             "Class": "imposé par arrêté préfectoral",
             "URL": FICHE.format(23008), "Tier": 1, "Outbreak": 0},
            {"Date": "2026-07-28", "Source": "RappelConso (FR)",
             "Company": "Bienheureux", "Product": "y",
             "Pathogen": "Listeria monocytogenes",
             "Class": "volontaire (sans arrêté préfectoral)",
             "URL": FICHE.format(22980), "Tier": 1, "Outbreak": 0},
        ]
        _write_sheet(wb, "Recalls", SCHEMA, rows)
        ws = wb["Recalls"]
        col = SCHEMA.index("Class") + 1
        written = [ws.cell(r, col).value for r in (2, 3)]
        self.assertEqual(["Mandatory", "Voluntary"], written,
                         "raw French Class reached the sheet — the writer "
                         "guard is not running")

    def test_workbook_has_no_raw_french_class(self):
        try:
            import openpyxl
        except ImportError:                       # pragma: no cover
            self.skipTest("openpyxl not installed")
        xlsx = ROOT / "docs" / "data" / "recalls.xlsx"
        if not xlsx.exists():                     # pragma: no cover
            self.skipTest("recalls.xlsx not present")
        wb = openpyxl.load_workbook(xlsx, read_only=True)
        offenders = []
        for sheet in wb.sheetnames:
            rows = list(wb[sheet].values)
            if not rows or "Class" not in [str(h) for h in rows[0]]:
                continue
            ic = [str(h) for h in rows[0]].index("Class")
            for n, r in enumerate(rows[1:], 2):
                if not r:
                    continue
                raw = str(r[ic] or "")
                if raw and _normalize_class_language(raw) != raw:
                    offenders.append((sheet, n, raw))
        self.assertEqual([], offenders,
                         f"un-normalised Class values in the workbook: "
                         f"{offenders[:10]}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
