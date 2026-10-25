"""One fiche served at two addresses is one recall.

INCIDENT 2026-10-24/25, found by the morning fix pass of 2026-10-25.

RappelConso serves every fiche twice:

    https://rappel.conso.gouv.fr/fiche-rappel/23918/interne          (id)
    https://rappel.conso.gouv.fr/static/fiches/<uuid>.html           (no id)

The second is the static print mirror of the first. The uuid is not the fiche
number and cannot be derived from it, so the two URLs share no path, no
parameter and no digit — `_normalize_url_for_dedup` can never collapse them.

THE ROWS. Fiche 2026-10-0245, Ferme de Larcher, "TETE DE VEAU EN GELEE",
published 23.10.2026, was in Recalls twice:

    .../static/fiches/d71d11bb-67a0-40c6-914d-894deea2471f.html   DateAdded 10-24
    .../fiche-rappel/23918/interne                                DateAdded 10-25

with byte-identical Reason (including its "toxi- infections" spacing),
identical brand, date and distributor list. The static form also appears on
five archived rows, so this is the shape of the source, not a one-off.

WHY THE EXISTING AXES BOTH MISSED IT. `_dedup_key` gives each row ONE key: a
URL key when the URL carries an id, a CONTENT key when it does not. So:

    approved   .../fiche-rappel/23918/interne   -> key = the URL
    candidate  .../static/fiches/<uuid>.html    -> key = the content

Two keys in two different key spaces. This is the 2026-07-26 FSAI lesson one
axis further along — that audit found the content rule had REPLACED the URL
axis instead of adding to it, and the same sentence applies here.

THE FIX. Two narrow pieces:
  1. `_url_identity.NO_STABLE_ID_PATHS` — per-PATH registration, because
     registering the whole host would discard a perfectly good fiche number
     on every other RappelConso URL.
  2. A third axis in the promote gate: every approved row also contributes
     its content key, indexed BY HOST, and a candidate whose own URL has no
     id is refused when that key is already present for its host. Scoped by
     host on purpose — two regulators announcing one event on one day is a
     cross-source question pipeline/daily_review_agent.py already owns.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from openpyxl import load_workbook                       # noqa: E402
import pytest                                            # noqa: E402

from pipeline import merge_master as mm                  # noqa: E402
from pipeline._url_identity import (                     # noqa: E402
    content_key, has_stable_id, row_rank,
)

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"

CANON = "https://rappel.conso.gouv.fr/fiche-rappel/23918/interne"
STATIC = ("https://rappel.conso.gouv.fr/static/fiches/"
          "d71d11bb-67a0-40c6-914d-894deea2471f.html")


def _row(url, **kw):
    row = {
        "Date": "2026-10-23",
        "Source": "RappelConso (FR)",
        "Company": "Ferme de Larcher",
        "Brand": "Ferme de Larcher",
        "Product": "Jellied calf's head",
        "Pathogen": "Sulphite-reducing anaerobes (ASR)",
        "Reason": ("Risk of foodborne toxi-infection caused by the presence "
                   "of sulphite-reducing anaerobes (ASR)"),
        "Class": "Voluntary",
        "Country": "France",
        "Tier": 3,
        "Outbreak": 0,
        "URL": url,
        "Notes": "",
    }
    row.update(kw)
    return row


# ── the registry ──────────────────────────────────────────────────────────

def test_the_static_mirror_carries_no_alert_id():
    assert not has_stable_id(STATIC)


def test_the_canonical_fiche_still_does():
    assert has_stable_id(CANON)


def test_the_rest_of_the_host_is_untouched():
    """Per-PATH, not per-host — the point of NO_STABLE_ID_PATHS."""
    for url in ("https://rappel.conso.gouv.fr/categorie/0/2",
                "https://rappel.conso.gouv.fr/recherche",
                "https://rappel.conso.gouv.fr/fiche-rappel/23922/interne",
                "https://rappel.conso.gouv.fr/static/"):
        assert has_stable_id(url), url


def test_another_host_serving_a_static_html_path_is_untouched():
    assert has_stable_id("https://example.gov/static/fiches/abcdef12.html")


def test_the_two_addresses_share_a_content_identity():
    assert content_key(_row(STATIC)) == content_key(_row(CANON))


def test_the_canonical_row_is_the_one_preferred():
    """Which copy survives is already decided by row_rank; the removal in
    tools/repair_2026_10_25.py follows it rather than inventing a rule."""
    assert row_rank(_row(CANON)) < row_rank(_row(STATIC))


# ── the promote gate ──────────────────────────────────────────────────────

def _promote(pending, approved):
    """Run the gate and report which pending rows reached Recalls."""
    new_approved, _kept, _archived = mm.promote_approved(
        pending, approved, {})
    return new_approved


def test_the_static_mirror_is_refused_when_the_canonical_is_published():
    got = _promote([_row(STATIC)], [_row(CANON)])
    assert not got, (
        "the static print mirror of an already-published fiche was promoted "
        "again: %s" % ([r.get("URL") for r in got],))


def test_the_canonical_is_refused_when_the_static_mirror_is_published():
    """The pair arrived in this order on 2026-10-24 — the mirror first. The
    axis has to work both ways round or it only ever catches one of two
    orderings."""
    if not hasattr(mm, "promote_approved"):
        pytest.skip("promote_approved not exposed")
    got = _promote([_row(CANON)], [_row(STATIC)])
    assert not got, (
        "a fiche already published at its static address was promoted again "
        "at its canonical one: %s" % ([r.get("URL") for r in got],))


def test_a_different_recall_on_the_same_host_still_promotes():
    """RappelConso publishes one fiche per SKU, same company, same pathogen,
    same day — merge_master's own comment on _near_dup_key says so. The
    narrow axis must not start refusing those."""
    if not hasattr(mm, "promote_approved"):
        pytest.skip("promote_approved not exposed")
    other = _row("https://rappel.conso.gouv.fr/fiche-rappel/23919/interne",
                 Product="Jellied calf's head, 500 g jar")
    got = _promote([other], [_row(CANON)])
    assert [r.get("URL") for r in got] == [other["URL"]], (
        "a second fiche of the same recall day was wrongly refused")


def test_a_static_mirror_of_an_unpublished_recall_still_promotes():
    """The axis refuses a REPEAT, not the shape. A static-fiche URL is a
    legitimate RappelConso address and must still be publishable when it is
    the only row for that recall."""
    got = _promote([_row(STATIC)], [])
    assert [r.get("URL") for r in got] == [STATIC]


# ── the register ──────────────────────────────────────────────────────────

def _recalls():
    if not XLSX.exists():
        pytest.skip("no recalls.xlsx")
    wb = load_workbook(XLSX, read_only=True, data_only=True)
    try:
        ws = wb["Recalls"]
        it = ws.iter_rows(values_only=True)
        head = list(next(it))
        return [dict(zip(head, r)) for r in it
                if any(v is not None and str(v).strip() for v in r)]
    finally:
        wb.close()


def test_no_published_rappelconso_recall_appears_at_both_addresses():
    rows = _recalls()
    id_keys = {}
    for r in rows:
        u = str(r.get("URL") or "")
        if "rappel.conso.gouv.fr" not in u or not has_stable_id(u):
            continue
        id_keys.setdefault(content_key(r), []).append(u)
    clashes = []
    for r in rows:
        u = str(r.get("URL") or "")
        if "rappel.conso.gouv.fr" not in u or has_stable_id(u):
            continue
        hit = id_keys.get(content_key(r))
        if hit:
            clashes.append((u, hit))
    assert not clashes, (
        "RappelConso fiche(s) published at BOTH their canonical and their "
        "static address — one recall, two rows:\n    %s"
        % ("\n    ".join("%s  ==  %s" % (a, b) for a, b in clashes),))
