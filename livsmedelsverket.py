"""
Offline smoke test for scrapers.north_america.usda_fsis.

Exercises the hazard-scope logic with synthetic FSIS API records drawn from
real public releases (March 2026 White Oak Pastures metal PHA, recent
Listeria recalls, excluded-reason misbranding cases, retractions, etc.).

Run:  python -m pytest tests/test_usda_fsis_scraper.py -v
  or: python tests/test_usda_fsis_scraper.py
"""
from __future__ import annotations
import json
import sys
import unittest
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scrapers.north_america.usda_fsis import USDAFSISScraper  # noqa: E402


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _days_ago(n: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=n)).strftime("%Y-%m-%d")


# Synthetic but realistic FSIS API payloads — based on actual recent releases.
SAMPLE_PAYLOAD = [
    {
        # Should PASS — the case that made us rewrite this scraper.
        # Based on the March 23 2026 White Oak Pastures PHA for ground beef
        # contaminated with metal. Previously dropped by the narrow keyword
        # filter; now captured via "Metal fragments" canonical hazard.
        "field_recall_number": "PHA-03232026-01",
        "field_title": "FSIS Issues Public Health Alert for Ground Beef Products Due "
                       "to Possible Foreign Matter Contamination (Metal)",
        "field_recall_reason": "Foreign Matter Contamination",
        "field_recall_type": "Public Health Alert",
        "field_recall_classification": "",
        "field_related_to_outbreak": "False",
        "field_summary": "<p>WASHINGTON, March 23, 2026 - Public health alert for ground "
                        "beef product that may be contaminated with foreign material, "
                        "specifically metal fragments. A recall was not requested.</p>",
        "field_establishment": "White Oak Pastures",
        "field_product_items": "16oz (1 lb.) packages of WHITE OAK PASTURES GRASSFED "
                              "GROUND BEEF",
        "field_states": "Nationwide",
        "field_recall_date": _days_ago(7),
        "field_recall_url": "https://www.fsis.usda.gov/recalls-alerts/white-oak-pastures-pha",
    },
    {
        # Should PASS — Listeria recall. Baseline case.
        "field_recall_number": "010-2026",
        "field_title": "ACME Deli Recalls RTE Meat Products Due to Possible "
                       "Listeria Contamination",
        "field_recall_reason": "Listeria monocytogenes",
        "field_recall_type": "Recall",
        "field_recall_classification": "Class I",
        "field_related_to_outbreak": "True",
        "field_summary": "<p>One illness reported. ACME Deli, Chicago IL...</p>",
        "field_establishment": "ACME Deli",
        "field_product_items": "RTE roast beef, 1-lb packages",
        "field_states": "IL, IN, WI",
        "field_recall_date": _days_ago(10),
        "field_recall_url": "https://www.fsis.usda.gov/recalls-alerts/acme-deli",
    },
    {
        # Should PASS — STEC / E. coli O157:H7.
        "field_recall_number": "008-2026",
        "field_title": "Processor Recalls Ground Beef Due to Possible E. coli O157:H7",
        "field_recall_reason": "E. coli O157:H7",
        "field_recall_type": "Recall",
        "field_recall_classification": "Class I",
        "field_related_to_outbreak": "False",
        "field_summary": "<p>Routine testing positive for E. coli O157:H7.</p>",
        "field_establishment": "Big Beef Co.",
        "field_product_items": "Ground beef chubs 10-lb",
        "field_states": "Multistate",
        "field_recall_date": _days_ago(3),
        "field_recall_url": "https://www.fsis.usda.gov/recalls-alerts/big-beef",
    },
    {
        # Should DROP — misbranding/allergen. Explicit exclusion.
        "field_recall_number": "009-2026",
        "field_title": "Deli Recalls Chicken Salad Due to Misbranding",
        "field_recall_reason": "Misbranding, Unreported Allergens",
        "field_recall_type": "Recall",
        "field_recall_classification": "Class I",
        "field_related_to_outbreak": "False",
        "field_summary": "<p>Undeclared wheat.</p>",
        "field_establishment": "Test Deli",
        "field_product_items": "Chicken salad kits",
        "field_states": "CA",
        "field_recall_date": _days_ago(2),
        "field_recall_url": "https://www.fsis.usda.gov/recalls-alerts/test-deli",
    },
    {
        # Should DROP — produced without inspection (not a hazard category).
        "field_recall_number": "PHA-03252026-01",
        "field_title": "Firm Recalls Product Produced Without Benefit of Inspection",
        "field_recall_reason": "Produced Without Benefit of Inspection",
        "field_recall_type": "Public Health Alert",
        "field_recall_classification": "",
        "field_related_to_outbreak": "False",
        "field_summary": "<p>No inspection.</p>",
        "field_establishment": "Unknown Processor",
        "field_product_items": "Various",
        "field_states": "TX",
        "field_recall_date": _days_ago(25),
        "field_recall_url": "https://www.fsis.usda.gov/recalls-alerts/no-inspection",
    },
    {
        # Should DROP — retraction.
        "field_recall_number": "PHA-04062026-RETRACTION",
        "field_title": "FSIS Retracts Public Health Alert for Walmart Great Value "
                       "Fully Cooked Dino Shaped Chicken Breast Nuggets",
        "field_recall_reason": "Retraction",
        "field_recall_type": "Retraction",
        "field_recall_classification": "",
        "field_related_to_outbreak": "False",
        "field_summary": "<p>Retracting earlier alert.</p>",
        "field_establishment": "Walmart",
        "field_product_items": "Great Value Dino Nuggets",
        "field_states": "Nationwide",
        "field_recall_date": _days_ago(13),
        "field_recall_url": "https://www.fsis.usda.gov/recalls-alerts/great-value-retraction",
    },
    {
        # Should DROP — stale (older than since_days=30).
        "field_recall_number": "001-2026",
        "field_title": "Old Listeria Recall",
        "field_recall_reason": "Listeria monocytogenes",
        "field_recall_type": "Recall",
        "field_recall_classification": "Class I",
        "field_related_to_outbreak": "False",
        "field_summary": "<p>Old.</p>",
        "field_establishment": "Old Co.",
        "field_product_items": "Something old",
        "field_states": "CA",
        "field_recall_date": _days_ago(90),
        "field_recall_url": "https://www.fsis.usda.gov/recalls-alerts/old",
    },
    {
        # Should PASS — Salmonella in date range, US-format date parsing.
        "field_recall_number": "007-2026",
        "field_title": "Chicken Firm Recalls Products Due to Possible Salmonella",
        "field_recall_reason": "Salmonella spp.",
        "field_recall_type": "Recall",
        "field_recall_classification": "Class I",
        "field_related_to_outbreak": "True",
        "field_summary": "<p>15 illnesses reported.</p>",
        "field_establishment": "Chicken Inc.",
        "field_product_items": "Frozen raw chicken",
        "field_states": "Nationwide",
        # Intentionally use US format to exercise _parse_fsis_date fallback
        "field_recall_date": (datetime.now(timezone.utc) - timedelta(days=5))
                             .strftime("%m/%d/%Y"),
        "field_recall_url": "https://www.fsis.usda.gov/recalls-alerts/chicken-inc",
    },
    {
        # Should PASS — glass fragments in a PHA.
        "field_recall_number": "PHA-04012026-01",
        "field_title": "Frozen Chicken Patty Products Recalled Due to Possible "
                       "Foreign Matter (Glass) Contamination",
        "field_recall_reason": "Foreign Matter Contamination",
        "field_recall_type": "Recall",
        "field_recall_classification": "Class I",
        "field_related_to_outbreak": "False",
        "field_summary": "<p>Possible glass fragments.</p>",
        "field_establishment": "Foster Farms",
        "field_product_items": "Frozen chicken patties, various sizes",
        "field_states": "AZ, CA, CO, UT, WA",
        "field_recall_date": _days_ago(18),
        "field_recall_url": "https://www.fsis.usda.gov/recalls-alerts/foster-farms-glass",
    },
    {
        # Should DROP — import violation (excluded).
        "field_recall_number": "006-2026",
        "field_title": "Firm Recalls Products Due to Ineligible Imported Source",
        "field_recall_reason": "Ineligible Imported",
        "field_recall_type": "Recall",
        "field_recall_classification": "Class II",
        "field_related_to_outbreak": "False",
        "field_summary": "<p>Ineligible import.</p>",
        "field_establishment": "Import Co.",
        "field_product_items": "Various",
        "field_states": "NY",
        "field_recall_date": _days_ago(8),
        "field_recall_url": "https://www.fsis.usda.gov/recalls-alerts/import-violation",
    },
]


class _MockResponse:
    def __init__(self, payload):
        self._payload = payload
    def json(self):
        return self._payload


class TestUSDAFSISScraper(unittest.TestCase):

    def setUp(self):
        self.scraper = USDAFSISScraper()

    def _run_with_payload(self, payload, since_days=30):
        with patch("scrapers.north_america.usda_fsis.fetch",
                   return_value=_MockResponse(payload)):
            return self.scraper.scrape(since_days=since_days)

    def test_metal_pha_captured(self):
        """The White Oak Pastures metal PHA — the whole reason for the rewrite."""
        out = self._run_with_payload(SAMPLE_PAYLOAD)
        metals = [r for r in out if "Metal" in (r.Pathogen or "")]
        self.assertEqual(len(metals), 1, f"Expected 1 metal hazard, got {len(metals)}: {[r.Pathogen for r in out]}")
        r = metals[0]
        self.assertEqual(r.Company, "White Oak Pastures")
        self.assertEqual(r.Class, "Public Health Alert")
        self.assertIn("PHA-03232026-01", r.Notes)

    def test_glass_captured(self):
        out = self._run_with_payload(SAMPLE_PAYLOAD)
        glass = [r for r in out if "Glass" in (r.Pathogen or "")]
        self.assertEqual(len(glass), 1)

    def test_listeria_captured_with_outbreak(self):
        out = self._run_with_payload(SAMPLE_PAYLOAD)
        li = [r for r in out if "Listeria" in (r.Pathogen or "")]
        self.assertEqual(len(li), 1)
        self.assertEqual(li[0].Outbreak, 1)
        self.assertEqual(li[0].Class, "Class I")

    def test_ecoli_captured(self):
        out = self._run_with_payload(SAMPLE_PAYLOAD)
        ec = [r for r in out if "O157" in (r.Pathogen or "")]
        self.assertEqual(len(ec), 1)

    def test_salmonella_us_date_format(self):
        """Exercises the MM/DD/YYYY fallback in _parse_fsis_date."""
        out = self._run_with_payload(SAMPLE_PAYLOAD)
        sa = [r for r in out if "Salmonella" in (r.Pathogen or "")]
        self.assertEqual(len(sa), 1)

    def test_misbranding_dropped(self):
        out = self._run_with_payload(SAMPLE_PAYLOAD)
        mis = [r for r in out if r.Notes and "009-2026" in r.Notes]
        self.assertEqual(mis, [])

    def test_without_inspection_dropped(self):
        out = self._run_with_payload(SAMPLE_PAYLOAD)
        noi = [r for r in out if r.Notes and "PHA-03252026-01" in r.Notes]
        self.assertEqual(noi, [])

    def test_retraction_dropped(self):
        out = self._run_with_payload(SAMPLE_PAYLOAD)
        ret = [r for r in out if "RETRACTION" in (r.Notes or "").upper()]
        self.assertEqual(ret, [])

    def test_stale_dropped(self):
        out = self._run_with_payload(SAMPLE_PAYLOAD)
        old = [r for r in out if r.Notes and "001-2026" in r.Notes]
        self.assertEqual(old, [])

    def test_import_violation_dropped(self):
        out = self._run_with_payload(SAMPLE_PAYLOAD)
        imp = [r for r in out if r.Notes and "006-2026" in r.Notes]
        self.assertEqual(imp, [])

    def test_expected_total_count(self):
        """Of 10 payload records, 5 should pass: metal PHA, glass, Listeria,
        STEC, Salmonella."""
        out = self._run_with_payload(SAMPLE_PAYLOAD)
        self.assertEqual(len(out), 5,
            f"Expected 5 in-scope rows, got {len(out)}: "
            f"{[(r.Notes.split(';')[0], r.Pathogen) for r in out]}")

    def test_html_stripped_from_reason(self):
        payload = [{
            **SAMPLE_PAYLOAD[1],  # Listeria base
            "field_recall_reason": "<p>Listeria monocytogenes</p>",
            "field_recall_number": "999-2026",
            "field_recall_date": _days_ago(1),
        }]
        out = self._run_with_payload(payload)
        self.assertEqual(len(out), 1)
        self.assertIn("Listeria", out[0].Pathogen)

    def test_api_failure_returns_empty(self):
        with patch("scrapers.north_america.usda_fsis.fetch", return_value=None):
            out = self.scraper.scrape(since_days=30)
        self.assertEqual(out, [])

    def test_non_list_response_returns_empty(self):
        with patch("scrapers.north_america.usda_fsis.fetch",
                   return_value=_MockResponse({"error": "something"})):
            out = self.scraper.scrape(since_days=30)
        self.assertEqual(out, [])

    def test_dedup_by_recall_number(self):
        """API sometimes emits the same recall twice after edits."""
        dup = [SAMPLE_PAYLOAD[1], dict(SAMPLE_PAYLOAD[1])]
        out = self._run_with_payload(dup)
        self.assertEqual(len(out), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
