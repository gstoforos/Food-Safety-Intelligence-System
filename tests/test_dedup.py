"""
tests/test_dedup.py
====================
Tests for merge_master._dedup_key + _normalize_url_for_dedup.

These two functions are the IDENTITY rule for the entire pipeline. If a
row's dedup key changes between runs (or if two truly-different rows
collide on the same key), recalls either get duplicated on the dashboard
or get silently dropped.

The most expensive production bug this catches:

  2026-05-06 INCIDENT — A Tavily-sourced http://www.fsis.usda.gov/... URL
  slipped past the gate even though https://fsis.usda.gov/... was already
  in Recalls. The old dedup was `url.strip().lower()` so the two protocols
  produced different keys. Operator had to manually dedupe.

  Fix (audit 2026-05-06): _normalize_url_for_dedup strips protocol, www.,
  trailing /, fragment, and tracking query params. Now both http/https
  variants collapse to the same key.

A second bug class this locks down:

  Tracking query params (utm_*, gclid, fbclid) appended by social
  redirects must NOT change the dedup key. The same recall surfaced
  via different referrers should collapse. Identifier-style params
  (permalink, id, fiche, ref, recall_id) MUST be preserved because
  they ARE the recall identifier on some sites.
"""
from __future__ import annotations

import pytest

from pipeline.merge_master import _dedup_key, _normalize_url_for_dedup


# ───────────────────────────────────────────────────────────────────────
# _normalize_url_for_dedup — the function that drives the dedup key
# ───────────────────────────────────────────────────────────────────────
class TestNormalizeUrl:
    """The URL normalization rule that the entire pipeline relies on."""

    def test_empty_url_returns_empty(self):
        assert _normalize_url_for_dedup("") == ""
        assert _normalize_url_for_dedup(None) == ""

    def test_http_and_https_collapse(self):
        # THE MAY-6 BUG. http://www.fsis.usda.gov/ and https://fsis.usda.gov/
        # must produce IDENTICAL normalized keys.
        a = _normalize_url_for_dedup("http://www.fsis.usda.gov/recalls/foo")
        b = _normalize_url_for_dedup("https://fsis.usda.gov/recalls/foo")
        assert a == b
        assert a == "fsis.usda.gov/recalls/foo"

    def test_uppercase_lowercased(self):
        assert _normalize_url_for_dedup(
            "HTTPS://WWW.EXAMPLE.COM/Recalls"
        ) == "example.com/recalls"

    def test_trailing_slash_stripped(self):
        # /recalls/foo and /recalls/foo/ are the same resource.
        a = _normalize_url_for_dedup("https://example.com/recalls/foo")
        b = _normalize_url_for_dedup("https://example.com/recalls/foo/")
        assert a == b

    def test_fragment_stripped(self):
        # https://example.com/foo#section and #other are the same page.
        a = _normalize_url_for_dedup("https://example.com/foo#section")
        b = _normalize_url_for_dedup("https://example.com/foo#other")
        c = _normalize_url_for_dedup("https://example.com/foo")
        assert a == b == c == "example.com/foo"

    def test_utm_params_stripped(self):
        # Tracking params must not change dedup identity.
        bare = _normalize_url_for_dedup("https://example.com/recalls/foo")
        utm  = _normalize_url_for_dedup(
            "https://example.com/recalls/foo?utm_source=newsletter&utm_medium=email")
        assert bare == utm

    def test_multiple_tracking_params_stripped(self):
        # gclid, fbclid, ref-as-tracking-source all stripped.
        bare = _normalize_url_for_dedup("https://example.com/foo")
        tagged = _normalize_url_for_dedup(
            "https://example.com/foo?gclid=ABC&fbclid=XYZ&utm_campaign=q4")
        # _normalize only KEEPS permalink/id/fiche/ref/recall_id, so
        # the URL above with gclid + fbclid + utm_campaign drops them all.
        assert bare == tagged

    def test_identifier_query_params_preserved_permalink(self):
        # WordPress sites identify posts by ?permalink=NNN — that MUST be
        # part of the dedup key.
        a = _normalize_url_for_dedup("https://example.com/?permalink=12345")
        b = _normalize_url_for_dedup("https://example.com/?permalink=99999")
        assert a != b, "permalink query param must be preserved"
        assert "permalink=12345" in a
        assert "permalink=99999" in b

    def test_identifier_query_params_preserved_id(self):
        # Normalization lowercases everything, including param values.
        a = _normalize_url_for_dedup("https://example.com/page?id=A1")
        b = _normalize_url_for_dedup("https://example.com/page?id=B2")
        assert a != b
        assert "id=a1" in a    # already lowercased by _normalize_url_for_dedup

    def test_identifier_query_params_preserved_fiche(self):
        # RappelConso uses ?fiche=NNN on some endpoints.
        a = _normalize_url_for_dedup(
            "https://rappel.conso.gouv.fr/page?fiche=2026-04-0149")
        assert "fiche=2026-04-0149" in a

    def test_identifier_params_keep_tracking_strips(self):
        # Mixed params: preserve permalink, strip utm.
        result = _normalize_url_for_dedup(
            "https://example.com/?permalink=42&utm_source=newsletter")
        assert "permalink=42" in result
        assert "utm" not in result

    def test_www_stripped(self):
        a = _normalize_url_for_dedup("https://www.example.com/foo")
        b = _normalize_url_for_dedup("https://example.com/foo")
        assert a == b

    def test_real_rappelconso_url_stable(self):
        # Regression: real production URL must produce a stable, expected key.
        url = "https://rappel.conso.gouv.fr/fiche-rappel/2026-04-0149/interne"
        assert _normalize_url_for_dedup(url) == \
            "rappel.conso.gouv.fr/fiche-rappel/2026-04-0149/interne"


# ───────────────────────────────────────────────────────────────────────
# _dedup_key — the actual row identity rule
# ───────────────────────────────────────────────────────────────────────
class TestDedupKey:
    """
    _dedup_key returns the normalized URL when present, else falls back
    to "Date|company-slug|pathogen-slug". The fallback exists for rows
    that were ingested without a URL (rare — happens for some legacy
    Tavily / news-feed entries).
    """

    def test_url_dominates_when_present(self, sample_recall_row):
        row = dict(sample_recall_row)
        row["URL"] = "https://www.fsis.usda.gov/recalls/foo"
        key = _dedup_key(row)
        assert key == "fsis.usda.gov/recalls/foo"

    def test_protocol_difference_same_key(self, sample_recall_row):
        # The May 6 bug — same recall, different protocols, must dedupe.
        a = dict(sample_recall_row); a["URL"] = "http://www.fsis.usda.gov/recalls/foo"
        b = dict(sample_recall_row); b["URL"] = "https://fsis.usda.gov/recalls/foo"
        assert _dedup_key(a) == _dedup_key(b)

    def test_missing_url_falls_back_to_date_company_pathogen(self,
                                                              sample_recall_row):
        row = dict(sample_recall_row)
        row["URL"] = ""
        key = _dedup_key(row)
        # Format: "YYYY-MM-DD|companysluglower|pathogenfirst30"
        assert key.startswith("2026-05-10|")
        assert "acmefoodsinc" in key
        assert "listeria" in key.lower()

    def test_company_unicode_normalized(self, sample_recall_row):
        # Accented chars must round-trip to ASCII via NFD normalization.
        row = dict(sample_recall_row)
        row["URL"] = ""
        row["Company"] = "Süßwaren Müller GmbH"
        key = _dedup_key(row)
        # ß → ss, ü → u, German umlauts stripped
        # The slug is lowercased + non-alphanumeric stripped + truncated to 30.
        assert "uwarenmullergmbh" in key.lower() or \
               "ssussenmullergmbh" in key.lower() or \
               "swarenmullergmbh" in key.lower(), \
               f"unexpected slug: {key}"

    def test_company_truncated_to_30_chars(self, sample_recall_row):
        # Very long company names get truncated to 30 chars in the slug.
        row = dict(sample_recall_row)
        row["URL"] = ""
        row["Company"] = "A" * 100
        key = _dedup_key(row)
        # The slug part (between two `|`) must be ≤ 30 chars.
        parts = key.split("|")
        assert len(parts[1]) <= 30, f"company slug too long: {len(parts[1])}"

    def test_pathogen_truncated_to_30_chars(self, sample_recall_row):
        # Pathogen names sometimes include very long Latin binomials.
        row = dict(sample_recall_row)
        row["URL"] = ""
        row["Pathogen"] = "X" * 100
        key = _dedup_key(row)
        parts = key.split("|")
        assert len(parts[2]) <= 30

    def test_two_different_rows_different_keys(self, sample_recall_row):
        # Sanity: distinct rows must NOT collide.
        a = dict(sample_recall_row); a["URL"] = "https://example.com/a"
        b = dict(sample_recall_row); b["URL"] = "https://example.com/b"
        assert _dedup_key(a) != _dedup_key(b)
