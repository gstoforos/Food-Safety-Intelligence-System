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


# ---------------------------------------------------------------------------
# 2026-07-26 INCIDENT — FSAI "Butchers Selection" promoted for the 5th time
# ---------------------------------------------------------------------------
# The same FSAI alert entered Recalls twice. Both rows carry the same URL,
# differing only in case, and the same date, pathogen and product:
#
#   Dunnes Stores       .../News-and-Alerts/Food-Alerts/Recall-of-a-specific-batch-of-Butchers-Selection-I
#   Butchers Selection  .../news-and-alerts/food-alerts/recall-of-a-specific-batch-of-butchers-selection-i
#
# _normalize_url_for_dedup collapses those to one string, so the URL axis
# catches it. But fsai.ie is registered in _url_identity as a host with no
# stable alert identifier, so _dedup_key routes it to a CONTENT key — and the
# two rows disagree on Company (retailer vs own-brand), producing different
# keys. promote_approved checked the content key only, so it promoted again.
#
# The content-key rule is deliberate (it stops fabricated FSAI URLs — the
# 2026-07-09 Horgans case) and must stay. These tests pin the invariant that
# promote_approved checks BOTH axes, so neither failure mode can return.

_FSAI = "https://www.fsai.ie"
_ROW_224 = {
    "Date": "2026-06-30", "Source": "FSAI", "Company": "Dunnes Stores",
    "Brand": "Butchers Selection",
    "Product": "Butchers Selection Irish Turkey Burgers Mediterranean Style",
    "Pathogen": "Salmonella", "Reason": "Salmonella", "Class": "Recall",
    "Country": "Ireland", "Region": "Europe", "Tier": 1, "Outbreak": 0,
    "URL": f"{_FSAI}/News-and-Alerts/Food-Alerts/Recall-of-a-specific-batch-of-Butchers-Selection-I",
    "Notes": "",
}
_ROW_233 = {
    **_ROW_224, "Company": "Butchers Selection",
    "Product": "Butchers Selection Irish Turkey Burgers Mediterranean Style, 400g",
    "URL": f"{_FSAI}/news-and-alerts/food-alerts/recall-of-a-specific-batch-of-butchers-selection-i",
    "Status": "pending",
}


class TestFsaiButchersSelectionDuplicate:
    def test_url_normalizer_collapses_the_case_variants(self):
        assert (_normalize_url_for_dedup(_ROW_224["URL"])
                == _normalize_url_for_dedup(_ROW_233["URL"]))

    def test_content_key_alone_does_not_catch_it(self):
        # Documents WHY the URL axis is required. If this ever starts
        # failing, _url_identity's host registry changed and the comment
        # block in promote_approved needs revisiting.
        assert _dedup_key(_ROW_224) != _dedup_key(_ROW_233)

    def test_promote_approved_rejects_the_duplicate(self):
        from pipeline.merge_master import promote_approved
        new_approved, _kept, _arch = promote_approved(
            [dict(_ROW_233)], [dict(_ROW_224)], {})
        assert new_approved == [], (
            "FSAI Butchers Selection duplicate promoted again — promote_approved "
            "must dedup on normalized URL as well as content key")

    def test_promote_approved_still_admits_a_genuinely_new_fsai_row(self):
        from pipeline.merge_master import promote_approved
        other = {**_ROW_233, "Company": "Western Brand",
                 "Product": "Sage & Onion Cook in Bag Whole Chicken",
                 "URL": f"{_FSAI}/news-and-alerts/food-alerts/recall-of-specific-batches-of-western-brand-sage-o"}
        new_approved, _kept, _arch = promote_approved(
            [dict(other)], [dict(_ROW_224)], {})
        assert len(new_approved) == 1

    def test_case_variant_caught_for_a_url_keyed_host_too(self):
        from pipeline.merge_master import promote_approved
        a = {**_ROW_224, "Source": "RappelConso (FR)", "Country": "France",
             "URL": "https://rappel.conso.gouv.fr/fiche-rappel/22960/Interne"}
        b = {**a, "Company": "Someone Else", "Status": "pending",
             "URL": "http://www.rappel.conso.gouv.fr/Fiche-Rappel/22960/interne/"}
        new_approved, _kept, _arch = promote_approved([dict(b)], [dict(a)], {})
        assert new_approved == []


# ---------------------------------------------------------------------------
# 2026-07-26 — normalizer over-collapse: distinct recalls sharing a base URL
# ---------------------------------------------------------------------------
class TestNormalizerDoesNotOverCollapse:
    def test_fda_search_param_is_identity_bearing(self):
        base = "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts"
        keys = {_normalize_url_for_dedup(f"{base}?search_api_fulltext=H-{n}-2026")
                for n in ("0700", "0699", "0698")}
        assert len(keys) == 3, "FDA recall numbers must not collapse to one key"

    def test_pdf_fragment_is_identity_bearing(self):
        pdf = ("https://www.salute.gov.it/new/sites/default/files/external_data/"
               "avvisi_sicurezza_alimentare/cartello%20richiamo_1781203550.pdf")
        assert _normalize_url_for_dedup(pdf) != _normalize_url_for_dedup(pdf + "#tuma-san-giorz")

    def test_fragment_still_stripped_on_ordinary_pages(self):
        page = "https://www.fsai.ie/news-and-alerts/food-alerts/some-recall"
        assert _normalize_url_for_dedup(page) == _normalize_url_for_dedup(page + "#main-content")

    def test_tracking_params_still_stripped(self):
        assert _normalize_url_for_dedup(
            "https://example.com/recall?utm_source=x&utm_campaign=y") == "example.com/recall"

    def test_protocol_and_www_still_collapse(self):
        assert (_normalize_url_for_dedup("http://www.fsis.usda.gov/a/")
                == _normalize_url_for_dedup("https://fsis.usda.gov/a"))
