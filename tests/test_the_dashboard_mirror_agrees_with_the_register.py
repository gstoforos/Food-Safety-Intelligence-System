# -*- coding: utf-8 -*-
"""docs/data/recalls.json is the register, or it is a lie on the dashboard.

WHY THIS FILE EXISTS (morning fix pass 2026-11-15)
==================================================
advfood.tech/fsis-recalls reads docs/data/monthly-index.json and
docs/data/recalls.json. It does not read recalls.xlsx. merge_master says so
in as many words:

    "This is the ONLY sanctioned way to produce recalls.json. It guarantees
     that json can never drift from xlsx"                — mirror_json_from_xlsx

The guarantee holds only for code that CALLS it, and nothing checked that
anything had. Measured on this morning's pass, after its repair script had
corrected three published rows and before this test existed:

    row 2     Company  xlsx '(not named in the FSIS text held on this row)'
                       json 'FSIS Issues Public Health'
              Product  xlsx 'Ready-to-eat meat products containing FDA-
                             regulated biscuits'
                       json 'for Ready-to-Eat Meat ...'
    row 18    Product  xlsx 'Mini chicken breast fillet pieces, 150 g'
                       json 'Mini-Hähnchenbrust-Filetstücke 150g'
    row 1453  Brand    xlsx 'Bestar Europe'
                       json 'Bestar Europe | Agence fédérale pour la
                             sécurité de ...'

Eight field differences, 5,365 tests green, and the upload gate's own
REBASE-REPORT.md reporting the repair as done. George would have uploaded a
corrected register and watched the dashboard keep showing the old rows.

Two code paths were repaired alongside this test — the pass's repair script
re-mirrors like every other script in tools/, and
tools/rebase_and_verify.rebuild_register now regenerates the file its
docstring has always claimed it regenerates — but a derived file that
drifts silently needs a check, not two more call sites to remember. This is
the check.
"""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "recalls.json"

#: Fields a drift in would be visible to a subscriber or a dashboard reader.
#: Notes is excluded: it is long, free-text, carries the audit trail, and is
#: not what the dashboard renders in a row. Everything a reader acts on is
#: here.
PUBLIC_FIELDS = ("Date", "Source", "Company", "Brand", "Product", "Pathogen",
                 "Reason", "Class", "Country", "Region", "Tier", "Outbreak",
                 "URL")


def _expected():
    from pipeline.merge_master import load_existing, RECALLS_INTERNAL_COLUMNS
    out = []
    for r in load_existing(XLSX):
        rec = {}
        for k, v in r.items():
            if k in RECALLS_INTERNAL_COLUMNS:
                continue
            if hasattr(v, "isoformat"):
                rec[k] = v.isoformat()[:10]
            elif v is None:
                rec[k] = ""
            else:
                rec[k] = v
        out.append(rec)
    return out


class TestTheMirrorAgrees(unittest.TestCase):

    def setUp(self):
        if not (XLSX.exists() and JSON.exists()):          # pragma: no cover
            self.skipTest("register or mirror not present in this checkout")

    def test_the_row_counts_agree(self):
        got = json.loads(JSON.read_text(encoding="utf-8"))
        self.assertEqual(
            len(_expected()), len(got),
            "recalls.json holds a different number of rows than the "
            "Recalls sheet — the dashboard is showing a different register")

    def test_no_public_field_has_drifted(self):
        want = _expected()
        got = json.loads(JSON.read_text(encoding="utf-8"))
        bad = []
        for i, (w, g) in enumerate(zip(want, got), start=2):
            for f in PUBLIC_FIELDS:
                if str(w.get(f, "")) != str(g.get(f, "")):
                    bad.append(
                        f"  row {i} {f}: xlsx {str(w.get(f, ''))[:70]!r} "
                        f"!= json {str(g.get(f, ''))[:70]!r}")
        self.assertEqual(
            [], bad,
            f"{len(bad)} field(s) differ between the register and the "
            "mirror the dashboard reads. Re-run "
            "merge_master.mirror_json_from_xlsx after any workbook edit:\n"
            + "\n".join(bad[:20]))

    def test_an_internal_column_never_reaches_the_mirror(self):
        from pipeline.merge_master import RECALLS_INTERNAL_COLUMNS
        got = json.loads(JSON.read_text(encoding="utf-8"))
        leaked = sorted({k for row in got for k in row}
                        & set(RECALLS_INTERNAL_COLUMNS))
        self.assertEqual([], leaked,
                         f"internal columns leaked to the public mirror: "
                         f"{leaked}")


class TestTheGateRegeneratesIt(unittest.TestCase):
    """The upload gate must not merely promise this in a docstring."""

    def test_rebuild_register_calls_the_mirror(self):
        src = (ROOT / "tools" / "rebase_and_verify.py").read_text(
            encoding="utf-8")
        body = src.split("def rebuild_register", 1)[1].split("\n    # 5", 1)[0]
        code = "\n".join(ln.split("#", 1)[0] for ln in body.splitlines())
        self.assertIn(
            "mirror_json_from_xlsx", code,
            "rebase_and_verify.rebuild_register regenerates sources.json "
            "and the public xlsx but not recalls.json, which is the file "
            "the dashboard actually reads — and which classify() refuses "
            "to copy out of a data zip because it is DERIVED. Rebuilt by "
            "nothing and copied by nothing.")


if __name__ == "__main__":
    unittest.main()
