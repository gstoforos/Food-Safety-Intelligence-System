"""The Class column is a controlled vocabulary, not free text.

WHY
===
`Class` is listed in MUST_BE_ENGLISH, and the heuristic detector in
test_language_policy.py is supposed to keep it English. On 2026-08-18 a
sweep of the published workbook found two values it had never flagged:

    "Richiamo precauzionale"                    (IT, published 2026-07-14)
    "Veiligheidswaarschuwing (Safety Warning)"  (NL, published 2026-05-16)

Neither contains a marker word the detector knows, and neither is long
enough for its statistical path to fire. That is not a bug in the
detector so much as the wrong tool: `Class` is not prose. It is a
regulator's notice type, drawn from a small closed set, and the right
guard for a closed set is the set itself.

A whitelist also catches things a language detector never could — a
Reason accidentally written into Class, a raw notice title, an empty
string dressed up as a value.

WHEN THIS TEST FAILS
====================
A new value appeared. That is not automatically wrong — new regulators
join the register. Check two things and then add it to ALLOWED:

  1. Is it English? ("Richiamo precauzionale" was not.)
  2. Is it a NOTICE TYPE, not a reason, a product or a title?

Do not widen this by pattern. Add the literal string.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Every Class value published as of 2026-08-18, after the two non-English
# stragglers above were translated. Case variants ("Border Rejection" /
# "Border rejection") are listed as they appear rather than normalised —
# normalising here would hide that the register carries both.
ALLOWED = {
    "Administrative action",
    "Advisory",
    "Alert",
    "Alert notification",
    "Border Rejection",
    "Border rejection",
    "Border rejection notification",
    "Class 1",
    "Class 2",
    "Class 3",
    "Class I",
    "Class II",
    "Food Alert",
    "Food recall warning",
    "Food recall warning (Class 1)",
    "Information",
    "Information notification for attention",
    "Mandatory",
    "Notification",
    "Outbreak",
    "Outbreak investigation",
    "PHA",
    "Precautionary recall",          # was "Richiamo precauzionale" (IT)
    "Preventive withdrawal",
    "Product Recall Information Notice",
    "Public Health Alert",
    "Public Health Alert (Product Contamination)",
    "Public advisory",
    "Public health alert",
    "Public health warning",
    "Rapid Outbreak Assessment / Public Health Alert",
    "Recall",
    "Recall (Voluntary)",
    "Regional public warning",
    "Safety warning",                # was "Veiligheidswaarschuwing (...)" (NL)
    "Sanitary alert",
    "Sanitary alert (update)",
    "Voluntary",
    "Voluntary recall",
}

# Values that must never come back, with the reason they left.
BANNED = {
    "Richiamo precauzionale": "Italian; use 'Precautionary recall'",
    "Veiligheidswaarschuwing (Safety Warning)": "Dutch; use 'Safety warning'",
}


def _rows(sheet="Recalls"):
    import openpyxl
    xlsx = ROOT / "docs" / "data" / "recalls.xlsx"
    wb = openpyxl.load_workbook(xlsx, read_only=True)
    rows = list(wb[sheet].values)
    hdr = [str(h) for h in rows[0]]
    return [dict(zip(hdr, r)) for r in rows[1:] if r]


class TestClassVocabulary(unittest.TestCase):

    def test_every_published_class_is_in_the_vocabulary(self):
        seen = {}
        for row in _rows():
            v = str(row.get("Class") or "").strip()
            if v and v not in ALLOWED:
                seen.setdefault(v, []).append(
                    f"{str(row.get('Date'))[:10]} {row.get('Source')}")
        self.assertEqual(
            {}, seen,
            "Class values outside the controlled vocabulary — check each is "
            "English AND is a notice type, then add the literal string to "
            f"ALLOWED: { {k: v[:2] for k, v in seen.items()} }")

    def test_the_translated_values_do_not_come_back(self):
        published = {str(r.get("Class") or "").strip() for r in _rows()}
        back = {v: why for v, why in BANNED.items() if v in published}
        self.assertEqual({}, back,
                         f"Non-English Class values re-published: {back}")

    def test_no_class_value_is_a_sentence(self):
        """A Reason or a notice title landing in Class is the failure a
        language detector cannot see. Notice types are short."""
        long = [(str(r.get("Date"))[:10], str(r.get("Class")))
                for r in _rows()
                if len(str(r.get("Class") or "")) > 60]
        self.assertEqual([], long,
                         f"Class values too long to be a notice type: {long}")

    def test_the_vocabulary_has_no_dead_entries(self):
        """Keep ALLOWED honest: an entry nothing uses is either a typo or a
        value that was removed without the whitelist being updated."""
        published = {str(r.get("Class") or "").strip() for r in _rows()}
        dead = sorted(ALLOWED - published)
        self.assertEqual([], dead,
                         f"ALLOWED lists Class values no row carries: {dead}")


if __name__ == "__main__":
    unittest.main()
