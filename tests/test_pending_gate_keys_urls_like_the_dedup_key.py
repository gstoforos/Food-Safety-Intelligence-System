# -*- coding: utf-8 -*-
"""The Pending gate must key URLs exactly as the dedup key does.

THE FAULT (audit 2026-11-13)
----------------------------
``merge_master.validate_pending_row`` rejects a candidate whose URL is
already in Recalls or Pending. Until this morning it did so with a bare
``url.rstrip("/").lower()``, on both sides of the comparison, while
``_dedup_key`` — the register's definition of "same recall" everywhere
else — used ``_normalize_url_for_dedup``. Every normalisation that
function performs was therefore lost at this one gate:

  * protocol (http:// vs https://)
  * a leading "www."
  * the fragment
  * non-canonical query strings
  * the admin.ch, AESAN and FSA language / host variants, each of which
    was added to the normaliser precisely because one notice is reachable
    at several addresses

Measured on the workbook of 2026-11-13, Pending held AESAN alert 2026_81
(Listeria monocytogenes in Priégola dairy products, Ref. ES2026/678)
TWICE, one "www." apart:

    https://www.aesan.gob.es/en/alertas/2026_81   (pending_gap_v2)
    https://aesan.gob.es/en/alertas/2026_81       (pending_gap)

Reviewer 2 indexes Pending as ``url_to_idx[url] = i`` — one slot per URL,
last write wins — so the first copy was invisible to it: it could never be
corrected, never promoted and never refused, while reviewer 3 held it and
the offline promoter reported it every morning.
tests/test_pending_holds_one_row_per_url.py was RED on plain main.

That test names the symptom in the data. This one holds the cause in the
code: the gate and the dedup key must agree, for every variant the
normaliser knows about, or the register grows a second copy of a recall it
already has.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest  # noqa: E402

from pipeline.merge_master import (  # noqa: E402
    _dedup_key,
    _normalize_url_for_dedup,
    validate_pending_row,
)


def _row(url: str) -> dict:
    """A row that passes every gate rule except, possibly, the dedup one."""
    return {
        "Date": "2026-11-10",
        "Source": "AESAN (ES)",
        "Company": "Priégola",
        "Brand": "Priégola",
        "Product": "Leche Fresca Entera Priégola 1 Litro",
        "Pathogen": "Listeria monocytogenes",
        "Reason": "Possible presence of Listeria monocytogenes",
        "Class": "Recall",
        "Country": "Spain",
        "URL": url,
        "Notes": "",
    }


#: Each pair addresses ONE recall. The left member stands for the row
#: already in Recalls or Pending; the right is the candidate arriving at
#: the gate. Every pair is a variant _normalize_url_for_dedup already
#: folds, so the gate must refuse the right member as a duplicate.
SAME_RECALL = [
    # The 2026-11-13 incident itself.
    ("https://www.aesan.gob.es/en/alertas/2026_81",
     "https://aesan.gob.es/en/alertas/2026_81"),
    # AESAN language variants (normaliser, audit 2026-10-28).
    ("https://www.aesan.gob.es/alertas/2026_81",
     "https://www.aesan.gob.es/en/alertas/2026_81"),
    # A bare "www." difference, no language segment.
    ("https://www.fsai.ie/news-and-alerts/food-alerts/example-2026",
     "https://fsai.ie/news-and-alerts/food-alerts/example-2026"),
    # Protocol (the 2026-05-06 audit that created the normaliser).
    ("https://www.fsis.usda.gov/recalls-alerts/example-2026",
     "http://www.fsis.usda.gov/recalls-alerts/example-2026"),
    # Trailing slash.
    ("https://www.ages.at/mensch/produkt/example-2026",
     "https://www.ages.at/mensch/produkt/example-2026/"),
    # A presentation-only fragment on a non-PDF path.
    ("https://www.ages.at/mensch/produkt/example-2026",
     "https://www.ages.at/mensch/produkt/example-2026#content"),
    # admin.ch newsroom language segments (normaliser, audit 2026-09-07).
    ("https://www.blv.admin.ch/de/newnsb/V67FO2BgSwqP",
     "https://www.blv.admin.ch/fr/newnsb/V67FO2BgSwqP"),
]


@pytest.mark.parametrize("already,candidate", SAME_RECALL)
def test_the_gate_refuses_every_variant_the_dedup_key_folds(already, candidate):
    """The two halves of the gate must reach the same verdict."""
    key_already = _normalize_url_for_dedup(already)
    key_candidate = _normalize_url_for_dedup(candidate)
    assert key_already == key_candidate, (
        "fixture is wrong: _normalize_url_for_dedup does not fold these two "
        f"addresses ({already!r} -> {key_already!r}, "
        f"{candidate!r} -> {key_candidate!r})"
    )

    existing = {key_already}
    ok, why = validate_pending_row(_row(candidate), existing)
    assert not ok, (
        f"The Pending gate accepted {candidate!r} while the register already "
        f"holds {already!r}. _dedup_key folds them to {key_already!r}, so a "
        "second row for one recall has just been queued. This is the AESAN "
        "2026_81 fault of 2026-11-13: build existing_urls and compare with "
        "_normalize_url_for_dedup, not with url.rstrip('/').lower()."
    )
    assert "Duplicate URL" in why, why


@pytest.mark.parametrize("already,candidate", SAME_RECALL)
def test_the_dedup_key_agrees_with_the_gate(already, candidate):
    """_dedup_key is the other half; it must fold the same pairs."""
    assert _dedup_key(_row(already)) == _dedup_key(_row(candidate))


def test_a_genuinely_different_recall_still_passes():
    """The fold must not swallow a distinct notice on the same host.

    The normaliser preserves identifier-style query params and the
    per-recall fragment of a shared PDF for exactly this reason; the gate
    must inherit that too, or it starts dropping real recalls as
    duplicates — the over-collapse half of the 2026-07-26 audit.
    """
    existing = {_normalize_url_for_dedup(
        "https://www.aesan.gob.es/alertas/2026_81")}
    ok, why = validate_pending_row(
        _row("https://www.aesan.gob.es/alertas/2026_82"), existing)
    assert ok, f"a different AESAN alert was refused as a duplicate: {why}"
