# -*- coding: utf-8 -*-
"""A row that has been translated must not still look stamped.

THE SHAPE
---------
Reviewer 3 and the offline promoter mark a row whose Product or
Company/Brand is not in English by writing a stamp into Notes:

    [needs cleanup: Product not in English]
    [needs cleanup: Company/Brand not in English]

The morning fix pass translates such a row and removes the stamp. It then
appends a note saying what it read and, when the detector had MISSED the
row, that no stamp was ever applied.

On 2026-10-13 that explanation quoted the stamp verbatim:

    [detector-miss 2026-10-13: reviewer 3 did NOT stamp this row
     '[needs cleanup: Product not in English]' — ...]

so the row still matched a search for the stamp. Every consumer that finds
work by searching Notes for it — the next morning's pass, reviewer 3, the
offline promoter — saw three fully translated rows (RappelConso fiches
23742, 23767 and 23768) as outstanding. The stamp count decayed from
"three rows to translate" into "three rows that can never be cleared",
which is worse than a missing stamp: it is work that looks real and is not.

THE INVARIANT
-------------
A stamp in Notes means "this row still needs translating". So a row whose
Notes also carry this pass's own markers — "[translated " or
"[detector-miss " — must NOT carry a stamp: either it was translated (no
stamp) or it was not (no translation marker). Describe the stamp in prose;
never quote it.

WHY THIS IS A DATA TEST AND NOT A SOURCE SCAN
---------------------------------------------
The first version of this file also grepped tools/repair_*.py for a quoted
stamp. It flagged the STAMPS constant's own continuation line and the
docstrings of three earlier passes — all legitimate — so the scan was
dropped. The register is where the damage shows and the register is what
is asserted here: if a future note quotes the stamp again, the row it is
written on fails this test on the next run.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

STAMPS = ("[needs cleanup: Product not in English]",
          "[needs cleanup: Company/Brand not in English]")
PASS_MARKERS = ("[translated ", "[detector-miss ")

RECALLS_JSON = ROOT / "docs" / "data" / "recalls.json"


def _rows():
    if not RECALLS_JSON.exists():
        return []
    return json.loads(RECALLS_JSON.read_text("utf-8"))


def test_no_published_row_is_both_translated_and_stamped():
    offenders = []
    for row in _rows():
        notes = str(row.get("Notes") or "")
        if not any(s in notes for s in STAMPS):
            continue
        if not any(m in notes for m in PASS_MARKERS):
            continue        # a genuine, untranslated stamp — fine
        offenders.append((row.get("Date"), row.get("Source"),
                          str(row.get("Product") or "")[:60]))
    assert not offenders, (
        "%d published row(s) carry a needs-cleanup stamp AND a "
        "translation marker:\n    %s\n\n"
        "A stamp means the row still needs translating, so these rows are "
        "permanently outstanding work that is already done. The usual "
        "cause is a note that QUOTES the stamp while explaining that the "
        "detector never applied it. Describe the stamp, do not quote it — "
        "tools/repair_2026_10_14.strip_quoted_cleanup_stamps clears the "
        "rows that already carry the old wording."
        % (len(offenders), "\n    ".join(map(str, offenders))))
