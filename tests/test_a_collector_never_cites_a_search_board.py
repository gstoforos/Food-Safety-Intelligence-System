"""
AUDIT 2026-10-23 — THE FSIS COLLECTOR MANUFACTURED A URL ITS OWN GATE REFUSES
=============================================================================

pipeline/official_feeds/sources/us_fsis.py built this when the API gave it
no press-release link:

    url = f"https://www.fsis.usda.gov/recalls-alerts?search={rn}"

Two places in this repo already say that string cannot be published:

  * pipeline/_publish_gate.py — "A SEARCH RESULTS PAGE IS NOT A RECALL
    NOTICE" (_SEARCH_QUERY_URL, audit 2026-08-13), naming
    `fsis.usda.gov/recalls-alerts?search=015-2026` by example; and
  * pipeline/gap_finder/countries/us.py — labels the same shape LISTING and
    requires a slug after the slash, "all three of its forms are already
    sitting in the Rejected sheet, which is the gate doing its job".

So every row the fallback produced was dead on arrival. Worse, main.py
dedups on the URL string, so the manufactured key never matched the
per-recall URL the core FSIS platform had already published the same recall
under — and the row came back.

FSIS recall 026-2026, Kayem Foods Inc., temperature abuse, is PUBLISHED at
Recalls 2026-10-17 on FSIS's own per-recall URL. The search-URL copy was
archived by an operator on 2026-10-20 (tools/repair_2026_10_20.py) and
removed from the queue again by the morning fix pass on 2026-10-22
(tools/repair_2026_10_22.py). This collector re-queued it both times — the
second at 12:24 UTC on 2026-10-22, about three hours after the removal was
uploaded. Each time it held
tests/test_register_urls_pass_their_own_gate.py[us] RED, and a red suite
blocks every upload through tools/rebase_and_verify.py.

Removing the row a third time without this fix would have been the third
removal of a row the collector rebuilds every morning.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline._publish_gate import _SEARCH_QUERY_URL          # noqa: E402
from pipeline.gap_finder.countries import get as get_country  # noqa: E402
from pipeline.official_feeds.sources import us_fsis           # noqa: E402


# ──────────────────────────────────────────────────────────────────────
# The URL the collector offers must be one the gate can accept
# ──────────────────────────────────────────────────────────────────────

def test_no_recall_number_becomes_a_search_url():
    """The fallback is gone, not widened.

    A recall number is not a citation. With no press-release link there is
    no per-recall URL to give, and guessing the slug is forbidden (rule 3),
    so the honest answer is no URL at all.
    """
    src = (ROOT / "pipeline" / "official_feeds" / "sources"
           / "us_fsis.py").read_text("utf-8")
    code = "\n".join(l for l in src.splitlines()
                     if not l.lstrip().startswith("#"))
    assert "recalls-alerts?search=" not in code, (
        "us_fsis.py still builds a /recalls-alerts?search= URL in running "
        "code. pipeline/_publish_gate.py refuses that string by name, so "
        "every row carrying it is a Pending zombie that holds the suite red.")


def test_a_record_without_a_press_release_carries_no_url():
    rec = us_fsis._record_from_api({
        "field_recall_number": "026-2026",
        "field_title": "Kayem Foods Inc. Recalls Meat and Poultry Products "
                       "Due to Possible Temperature Abuse Processing Defect",
        "field_recall_reason": "Possible Temperature Abuse",
        "field_recall_date": "Oct 17, 2026",
        "field_en_press_release": "",
    })
    assert rec.url == "", (
        f"expected no URL, got {rec.url!r} — the publish gate will refuse "
        f"anything search-shaped and the row will never leave Pending")
    assert rec.raw.get("_pending_no_auth_url") is True, (
        "the row must be flagged so main.py stamps it pending_no_auth_url "
        "— the status that puts it in front of a reviewer instead of in "
        "front of a gate that refuses it forever")
    assert rec.source_id == "026-2026", (
        "the recall number is still recorded; it is just not pretending "
        "to be a URL")


def test_the_gate_would_have_refused_the_old_fallback():
    """Proof the two halves of the system disagreed, not an opinion."""
    old = "https://www.fsis.usda.gov/recalls-alerts?search=026-2026"
    assert _SEARCH_QUERY_URL.search(old), (
        "the publish gate's own rule must still match the string the "
        "collector used to build")
    item_rx = re.compile(get_country("us").authority_item_url_regex, re.I)
    assert not item_rx.search(old), (
        "and the us country config must still refuse it as an item URL")


# ──────────────────────────────────────────────────────────────────────
# The shapes FSIS actually sends the press-release field in
# ──────────────────────────────────────────────────────────────────────

SLUG = ("kayem-foods-inc--recalls-meat-and-poultry-products-due-possible-"
        "temperature-abuse")
WANT = "https://www.fsis.usda.gov/recalls-alerts/" + SLUG


def test_an_absolute_press_release_url_is_used_as_is():
    assert us_fsis._press_release_url(WANT) == WANT


def test_a_site_relative_press_release_is_resolved():
    """`if press.startswith("http")` threw this away."""
    assert us_fsis._press_release_url("/recalls-alerts/" + SLUG) == WANT


def test_a_markup_wrapped_press_release_is_unwrapped():
    """_field() returns the raw value and _strip_html was never applied to
    this one field, so an anchor was discarded too."""
    anchor = f'<a href="/recalls-alerts/{SLUG}">Kayem Foods Inc.</a>'
    assert us_fsis._press_release_url(anchor) == WANT
    assert us_fsis._press_release_url(f'<a href="{WANT}">notice</a>') == WANT


def test_recovering_the_field_puts_the_real_url_on_the_record():
    rec = us_fsis._record_from_api({
        "field_recall_number": "026-2026",
        "field_title": "Kayem Foods Inc. Recalls Meat and Poultry Products",
        "field_recall_date": "Oct 17, 2026",
        "field_en_press_release": f'<a href="/recalls-alerts/{SLUG}">x</a>',
    })
    assert rec.url == WANT
    assert not rec.raw.get("_pending_no_auth_url"), (
        "a row that HAS the regulator's notice must not be flagged for "
        "manual URL resolution")


def test_a_search_or_listing_url_in_that_field_is_still_refused():
    """The recovery must not become a back door for the refused shape."""
    for bad in (
        "https://www.fsis.usda.gov/recalls-alerts?search=026-2026",
        "/recalls-alerts?search=026-2026",
        "https://www.fsis.usda.gov/recalls",
        "https://www.fsis.usda.gov/recalls-alerts",
        "https://example.com/recalls-alerts/" + SLUG,   # not FSIS's host
        "mailto:press@fsis.usda.gov",
        "",
    ):
        assert us_fsis._press_release_url(bad) == "", (
            f"{bad!r} is not a per-recall notice and must not be accepted")


# ──────────────────────────────────────────────────────────────────────
# A row with no URL must not be re-appended every morning
# ──────────────────────────────────────────────────────────────────────

def test_the_writer_has_a_second_dedup_key_for_rows_without_a_url():
    """The URL set cannot dedup a row that has no URL.

    Without a second key, the fix above would trade one permanent zombie
    for a Pending sheet that grows by one Kayem row a day.
    """
    src = (ROOT / "pipeline" / "official_feeds" / "main.py").read_text("utf-8")
    assert "_SOURCE_ID_RE" in src and "existing_src_ids" in src, (
        "pipeline/official_feeds/main.py must dedup on (Source, source_id) "
        "as well as URL, or a no-URL record is appended on every run")


def test_the_source_id_pattern_reads_back_what_the_writer_wrote():
    """_row_for() writes `source_id=<id>`; the dedup must parse that."""
    from pipeline.official_feeds.main import _SOURCE_ID_RE
    notes = "source_id=026-2026 [via official-feed collector]"
    m = _SOURCE_ID_RE.search(notes)
    assert m and m.group(1) == "026-2026"
    assert _SOURCE_ID_RE.search("no identifier here") is None
