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

from scrapers.north_america.usda_fsis import (  # noqa: E402
    USDAFSISScraper,
    _canonicalise_fsis_url,
)


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
    """Stand-in for the requests.Response that scrapers._base.fetch returns.

    AUDIT 2026-07-29 — this class previously defined only ``json()``. The
    scraper checks ``r.status_code != 200`` before parsing, so every test in
    this module died with AttributeError: 14 of 15 were failing. Nobody saw
    it because pytest.ini sets ``testpaths = tests`` and this file lived in
    ``scrapers/`` — it was never collected by CI. That is the reason the URL
    gate could zero out FSIS for three months unnoticed. File moved to
    tests/ and the mock completed.
    """

    def __init__(self, payload, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code
        self.text = ""

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
        # Case-insensitive: normalize_pathogen leaves non-microbial hazards
        # in the vocabulary's own lower-case canonical form ("metal fragment").
        metals = [r for r in out if "metal" in (r.Pathogen or "").lower()]
        self.assertEqual(len(metals), 1, f"Expected 1 metal hazard, got {len(metals)}: {[r.Pathogen for r in out]}")
        r = metals[0]
        self.assertEqual(r.Company, "White Oak Pastures")
        self.assertEqual(r.Class, "Public Health Alert")
        self.assertIn("PHA-03232026-01", r.Notes)

    def test_glass_captured(self):
        out = self._run_with_payload(SAMPLE_PAYLOAD)
        glass = [r for r in out if "glass" in (r.Pathogen or "").lower()]
        self.assertEqual(len(glass), 1)

    def test_listeria_captured_with_outbreak(self):
        out = self._run_with_payload(SAMPLE_PAYLOAD)
        li = [r for r in out if "Listeria" in (r.Pathogen or "")]
        self.assertEqual(len(li), 1)
        self.assertEqual(li[0].Outbreak, 1)
        self.assertEqual(li[0].Class, "Class I")

    def test_ecoli_captured(self):
        out = self._run_with_payload(SAMPLE_PAYLOAD)
        # The stored value is the CANONICAL pathogen, not the raw serotype
        # string: normalize_pathogen("E. coli O157:H7") ->
        # "Shiga toxin-producing E. coli (STEC)". Asserting on "O157" was
        # asserting on un-normalised text.
        ec = [r for r in out if "STEC" in (r.Pathogen or "")]
        self.assertEqual(len(ec), 1, [r.Pathogen for r in out])
        # Regression guard for the first-vocabulary-hit bug: this must NOT
        # collapse to the generic coli tier.
        self.assertNotIn("generic", ec[0].Pathogen.lower())

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


# ---------------------------------------------------------------------------
# Regression: the URL-gate blackout (audit 2026-07-29)
#
# Every fixture above hard-codes an absolute https://www.fsis.usda.gov/... URL.
# The live API does not emit those. Production therefore dropped 100% of
# in-window FSIS recalls at the prefix check and reported it as
# "0 pathogen recalls" — indistinguishable from a quiet week — while this
# module stayed green. (It stayed green in name only: it lived in scrapers/,
# pytest.ini sets testpaths = tests, so CI never collected it at all.)
#
# These tests pin the real-world URL shapes so that cannot recur.
# ---------------------------------------------------------------------------
class TestFsisUrlCanonicalisation(unittest.TestCase):

    def test_site_relative_path_resolves(self):
        self.assertEqual(
            _canonicalise_fsis_url(
                "/recalls-alerts/maple-leaf-foods-inc--recalls-not-ready-eat"),
            "https://www.fsis.usda.gov/recalls-alerts/"
            "maple-leaf-foods-inc--recalls-not-ready-eat")

    def test_relative_without_leading_slash_resolves(self):
        self.assertEqual(
            _canonicalise_fsis_url("recalls-alerts/acme-deli"),
            "https://www.fsis.usda.gov/recalls-alerts/acme-deli")

    def test_bare_http_upgraded(self):
        self.assertEqual(
            _canonicalise_fsis_url("http://www.fsis.usda.gov/recalls-alerts/x"),
            "https://www.fsis.usda.gov/recalls-alerts/x")

    def test_missing_www_normalised(self):
        self.assertEqual(
            _canonicalise_fsis_url("https://fsis.usda.gov/recalls-alerts/x"),
            "https://www.fsis.usda.gov/recalls-alerts/x")

    def test_protocol_relative_resolved(self):
        self.assertEqual(
            _canonicalise_fsis_url("//www.fsis.usda.gov/recalls-alerts/x"),
            "https://www.fsis.usda.gov/recalls-alerts/x")

    def test_already_canonical_unchanged(self):
        u = "https://www.fsis.usda.gov/recalls-alerts/acme-deli"
        self.assertEqual(_canonicalise_fsis_url(u), u)

    def test_foreign_host_never_laundered(self):
        """Canonicalisation must NOT rewrite a third-party link into an FSIS
        one, or the prefix gate stops being a safety check."""
        for u in ("https://example.com/recalls-alerts/fake",
                  "https://fsis.usda.gov.evil.tld/recalls-alerts/fake",
                  "mailto:fsis.webmaster@usda.gov"):
            self.assertEqual(_canonicalise_fsis_url(u), u, u)

    def test_empty_stays_empty(self):
        self.assertEqual(_canonicalise_fsis_url(""), "")
        self.assertEqual(_canonicalise_fsis_url(None), "")


class TestFsisRelativeUrlPayload(unittest.TestCase):
    """End-to-end: a payload shaped like the LIVE API must still yield rows."""

    def setUp(self):
        self.scraper = USDAFSISScraper()

    def _run(self, payload, since_days=30):
        with patch("scrapers.north_america.usda_fsis.fetch",
                   return_value=_MockResponse(payload)):
            return self.scraper.scrape(since_days=since_days)

    def test_relative_urls_no_longer_zero_the_scrape(self):
        payload = []
        for rec in SAMPLE_PAYLOAD:
            r = dict(rec)
            # Strip the origin — reproduce what the live API actually returns.
            r["field_recall_url"] = r.get("field_recall_url", "").replace(
                "https://www.fsis.usda.gov", "")
            payload.append(r)

        out = self._run(payload)
        self.assertEqual(
            len(out), 5,
            "Relative API URLs must survive the prefix gate. Got "
            f"{len(out)} rows — the 2026-07-29 blackout has regressed.")
        for r in out:
            self.assertTrue(
                r.URL.startswith("https://www.fsis.usda.gov/"),
                f"URL not canonicalised: {r.URL!r}")

    def test_foreign_url_still_rejected(self):
        r = dict(SAMPLE_PAYLOAD[1])          # the Listeria baseline row
        r["field_recall_url"] = "https://example.com/not-fsis"
        self.assertEqual(self._run([r]), [])

    def test_press_release_field_used_as_fallback(self):
        """field_recall_url is undocumented; field_en_press_release is the
        field the published FSIS API schema actually carries."""
        r = dict(SAMPLE_PAYLOAD[1])
        r.pop("field_recall_url", None)
        r["field_en_press_release"] = "/recalls-alerts/acme-deli-press"
        out = self._run([r])
        self.assertEqual(len(out), 1)
        self.assertEqual(
            out[0].URL,
            "https://www.fsis.usda.gov/recalls-alerts/acme-deli-press")


class TestFsisRecallVerbNotAPathogen(unittest.TestCase):
    """Regression: PATHOGEN_KEYWORDS must exclude recall_signals.

    Every FSIS title contains the word "Recalls". With the union vocabulary
    the pathogen gate matched 100% of records and stamped Pathogen="recall"
    on misbranding / retraction / import-violation rows.
    """

    def setUp(self):
        self.scraper = USDAFSISScraper()

    def test_recall_verbs_absent_from_vocabulary(self):
        kws = set(self.scraper.PATHOGEN_KEYWORDS)
        for verb in ("recall", "recalled", "recalls", "recalling",
                     "withdrawal", "withdrawn", "withdraws",
                     "alert", "warning"):
            self.assertNotIn(verb, kws)

    def test_real_hazards_still_present(self):
        kws = set(self.scraper.PATHOGEN_KEYWORDS)
        for hazard in ("listeria", "salmonella", "metal fragment",
                       "glass fragment"):
            self.assertIn(hazard, kws)

    def test_no_row_is_tagged_with_a_recall_verb(self):
        with patch("scrapers.north_america.usda_fsis.fetch",
                   return_value=_MockResponse(SAMPLE_PAYLOAD)):
            out = self.scraper.scrape(since_days=30)
        for r in out:
            self.assertNotIn((r.Pathogen or "").strip().lower(),
                             {"recall", "recalls", "alert", "warning"},
                             f"recall verb stored as a pathogen: {r.Pathogen!r}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
