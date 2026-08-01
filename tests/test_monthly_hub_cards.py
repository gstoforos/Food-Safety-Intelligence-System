"""Regression tests for the hub.html monthly carousel.

WHY THIS FILE EXISTS (audit 2026-08-01)
=======================================
hub.html showed no months at all.

`renderCard()` bails out on its very first line when a month has no download
link:

    docs/hub.html:256
        if (!lnk.href) return '';  // no PDF yet → omit card entirely

and `linkFor()` has exactly two sources for that href:

    var url = entry.pdf_url || LEGACY_PDF[key] || null;

`LEGACY_PDF` holds two hard-coded Wix URLs, for 2026-M01 and 2026-M02 only.
So for every month from M03 onwards the card exists if and only if
`monthly-index.json` carries a non-null `pdf_url`.

`update_monthly_index_json()` in docs/build_monthly_report_afts.py rebuilt each
entry from scratch on every run and never carried `pdf_url` forward. Any
monthly rebuild therefore blanked it and the entire carousel went empty —
which is exactly what happened when M03–M07 were rebuilt.
`pipeline/set_pdf_urls.py` exists to repopulate the field, but a rebuild that
forgets to re-run it silently takes the hub down, and nothing failed loudly.

Run:  python -m pytest tests/test_monthly_hub_cards.py -v
"""
from __future__ import annotations
import json
import re
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

INDEX = ROOT / "docs" / "data" / "monthly-index.json"
HUB = ROOT / "docs" / "hub.html"
DOCS = ROOT / "docs"

# The only months hub.html can render without a pdf_url, via its LEGACY_PDF map.
LEGACY_KEYS = {"2026-M01", "2026-M02"}


def _entries():
    raw = json.loads(INDEX.read_text(encoding="utf-8"))
    return raw if isinstance(raw, list) else raw.get("reports", [])


def _key(e):
    return "{}-M{:02d}".format(e.get("year"), int(e.get("month_num")))


class TestEveryMonthRendersACard(unittest.TestCase):

    def setUp(self):
        if not INDEX.exists():                    # pragma: no cover
            self.skipTest("monthly-index.json not present")

    def test_index_is_not_empty(self):
        self.assertTrue(_entries(), "monthly-index.json has no entries — the "
                                    "hub carousel would render nothing at all")

    def test_every_month_has_a_download_link(self):
        """Mirrors hub.html linkFor(): pdf_url, else LEGACY_PDF, else no card."""
        invisible = [
            _key(e) for e in _entries()
            if not e.get("pdf_url") and _key(e) not in LEGACY_KEYS
        ]
        self.assertEqual(
            [], invisible,
            f"these months would be OMITTED from hub.html — no pdf_url and no "
            f"LEGACY_PDF entry: {invisible}")

    def test_every_pdf_url_resolves_to_a_real_file(self):
        missing = []
        for e in _entries():
            url = e.get("pdf_url")
            if not url:
                continue
            rel = url.split("fsis.advfood.tech/")[-1].lstrip("/")
            if not (DOCS / rel).exists():
                missing.append((_key(e), rel))
        self.assertEqual([], missing,
                         f"pdf_url points at a file that does not exist: {missing}")

    def test_index_carries_the_stats_the_cards_display(self):
        """renderCard() only draws the stat block when all three are present."""
        bad = [_key(e) for e in _entries()
               if e.get("total") is None or e.get("tier1") is None
               or e.get("outbreaks") is None]
        self.assertEqual([], bad, f"months whose stat block would be blank: {bad}")


class TestRebuildDoesNotBlankPdfUrl(unittest.TestCase):
    """The actual bug: a monthly rebuild wiped pdf_url and emptied the hub."""

    def test_builder_preserves_pdf_url(self):
        src = (ROOT / "docs" / "build_monthly_report_afts.py").read_text(
            encoding="utf-8")
        self.assertIn("_prev_pdf_url", src,
                      "update_monthly_index_json() no longer carries pdf_url "
                      "forward — the next monthly rebuild will empty the hub")
        self.assertIn("Preserved pdf_url", src)


class TestHubContractUnchanged(unittest.TestCase):
    """If hub.html's link logic changes, these tests must be revisited."""

    def setUp(self):
        if not HUB.exists():                      # pragma: no cover
            self.skipTest("hub.html not present")

    def test_hub_still_omits_cards_without_a_link(self):
        src = HUB.read_text(encoding="utf-8")
        self.assertIn("if (!lnk.href) return ''", src)

    def test_hub_still_reads_the_data_scoped_index(self):
        src = HUB.read_text(encoding="utf-8")
        self.assertIn("data/monthly-index.json", src)

    def test_legacy_pdf_map_still_covers_only_m01_m02(self):
        src = HUB.read_text(encoding="utf-8")
        block = re.search(r"var LEGACY_PDF = \{(.*?)\}", src, re.S)
        self.assertIsNotNone(block)
        keys = set(re.findall(r"'(\d{4}-M\d{2})'", block.group(1)))
        self.assertEqual(
            LEGACY_KEYS, keys,
            "LEGACY_PDF changed — update LEGACY_KEYS in this test to match")


if __name__ == "__main__":
    unittest.main(verbosity=2)
