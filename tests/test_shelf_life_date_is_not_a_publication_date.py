"""A best-before date in a regulator's headline is not the recall's date.

INCIDENT 2026-10-24, found by the morning fix pass of 2026-10-25. The row and
its cost are written up in tests/test_no_row_is_dated_in_the_future.py; this
file holds the two source-level fixes.

The FSVO listing renders each recall's publication date at the END of the
link text and, when there is one, the product's best-before date in the
MIDDLE of it:

    "DANFOREL A/S ruft das Produkt «Forellenfilets geräuchert - Nature»
     mit dem Mindesthaltbarkeitsdatum 01.11.2026» wegen Listerien
     zurück — 23.10.2026"

scrapers/europe_non_eu/blv_ch.py read that with `parse_date`, which returns
the FIRST date in a string, so the Danforel Listeria recall was stored as
2026-11-01.

Two fixes, both narrow:

  1. scrapers/_listing.strip_shelf_life_dates removes a date ONLY where the
     text itself labels it as a shelf-life or lot date. The label must be
     present; nothing is inferred. A publication date is never introduced by
     one of those words, so no caller can lose one.
  2. blv_ch takes the TRAILING date of the link text, which is the
     convention that listing actually uses.

The stripper is also applied to `nearest_date`'s FORWARD fallback, which is
the only path in the shared listing parser that reads a card's own title.
The behind-path reads the listing's date column and is untouched.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scrapers._listing import (                 # noqa: E402
    nearest_date, parse_any_date, strip_shelf_life_dates,
)
from scrapers.europe_non_eu.blv_ch import parse_listing   # noqa: E402

DANFOREL = ("DANFOREL A/S ruft das Produkt «Forellenfilets geräuchert - "
            "Nature» mit dem Mindesthaltbarkeitsdatum 01.11.2026» wegen "
            "Listerien zurück — 23.10.2026")


# ── the stripper ──────────────────────────────────────────────────────────

def test_the_live_title_yields_the_publication_date():
    assert parse_any_date(DANFOREL) == "2026-11-01", (
        "the unstripped title still reads best-before-first; if this "
        "changes, the regression this file guards has moved")
    assert parse_any_date(strip_shelf_life_dates(DANFOREL)) == "2026-10-23"


def test_every_label_the_fleet_prints_is_recognised():
    cases = [
        ("Mindesthaltbarkeitsdatum 01.11.2026", "de"),
        ("MHD 05.11.2026", "de"),
        ("Verbrauchsdatum 02.11.2026", "de"),
        ("date limite de consommation 2026-10-24", "fr"),
        ("à consommer jusqu'au 24/10/2026", "fr"),
        ("DLC 24/10/2026", "fr"),
        ("best before 28/04/2027", "en"),
        ("use-by 2026-10-24", "en"),
        ("expiry date 11/05/2027", "en"),
        ("da consumarsi entro 30/11/2026", "it"),
        ("fecha de caducidad 30/11/2026", "es"),
        ("tenminste houdbaar tot 30-11-2026", "nl"),
        ("parasta ennen 30.11.2026", "fi"),
    ]
    for text, lang in cases:
        assert parse_any_date(strip_shelf_life_dates(text)) is None, (
            "%s label left its date behind: %r -> %r"
            % (lang, text, strip_shelf_life_dates(text)))


def test_two_shelf_life_dates_joined_by_and_both_go():
    """FSANZ prints one per batch: "best before 28/04/2027 and 09/05/2027"."""
    t = "Arla Protein Pudding, best before 28/04/2027 and 09/05/2027"
    assert parse_any_date(strip_shelf_life_dates(t)) is None


def test_a_publication_date_beside_a_shelf_life_date_survives():
    t = "Arla Protein Pudding, best before 26.04.2027, recalled 24/10/2026"
    assert parse_any_date(strip_shelf_life_dates(t)) == "2026-10-24"


def test_text_with_no_shelf_life_label_is_returned_unchanged():
    for t in ("Recall published 24 October 2026",
              "published 23.10.2026",
              "Öffentliche Warnung: Listerien in Forellenfilets",
              "Rückruf vom 20.10.2026",
              ""):
        assert strip_shelf_life_dates(t) == t, t


def test_the_label_itself_is_kept():
    """A reader of the title must still see that a best-before was named —
    only the date goes."""
    out = strip_shelf_life_dates("mit dem Mindesthaltbarkeitsdatum 01.11.2026")
    assert "Mindesthaltbarkeitsdatum" in out
    assert "01.11.2026" not in out


def test_a_lot_code_date_goes_too():
    t = "lot code 12/11/2026"
    assert parse_any_date(strip_shelf_life_dates(t)) is None


# ── the BLV listing parse ─────────────────────────────────────────────────

LISTING = """<ul>
<li>14.10.2026</li>
<li><a href="https://www.recallswiss.admin.ch/customer-access/#Recalls/1168"
>DANFOREL A/S ruft das Produkt &laquo;Forellenfilets ger&auml;uchert -
Nature&raquo; mit dem Mindesthaltbarkeitsdatum 01.11.2026&raquo; wegen
Listerien zur&uuml;ck &mdash; 23.10.2026</a></li>
<li><a href="https://www.recallswiss.admin.ch/customer-access/#Recalls/1167"
>Phag GmbH ruft das Produkt &laquo;Bohnenkraut&raquo; wegen
Pyrrolizidinalkaloiden zur&uuml;ck &mdash; 23.10.2026</a></li>
<li>20. September 2026</li>
<li><a href="/de/newnsb/V67FO2BgSwqP">&Ouml;ffentliche Warnung: Listerien in
ger&auml;ucherten Forellenfilets</a></li>
</ul>"""


def _by_url(entries):
    return {e["url"]: e for e in entries}


def test_the_danforel_entry_gets_the_publication_date():
    e = _by_url(parse_listing(LISTING))
    got = e["https://www.recallswiss.admin.ch/customer-access/#Recalls/1168"]
    assert got["date"] == "2026-10-23", (
        "the best-before date is back in Date: %r" % (got["date"],))


def test_a_recall_with_no_shelf_life_date_is_unaffected():
    e = _by_url(parse_listing(LISTING))
    got = e["https://www.recallswiss.admin.ch/customer-access/#Recalls/1167"]
    assert got["date"] == "2026-10-23"


def test_a_warning_still_takes_the_date_rendered_above_it():
    """The behind-path: warnings render "20. September 2026" above the
    heading and carry no date in the link text at all."""
    e = _by_url(parse_listing(LISTING))
    got = e["https://www.blv.admin.ch/de/newnsb/V67FO2BgSwqP"]
    assert got["date"] == "2026-09-20"


def test_no_blv_entry_is_dated_after_the_listing_it_came_from():
    """The property, rather than the three cases: nothing on this page may
    be dated later than the newest date the page itself renders."""
    dates = [e["date"] for e in parse_listing(LISTING) if e["date"]]
    assert dates
    assert max(dates) <= "2026-10-23"


# ── the shared forward fallback ───────────────────────────────────────────

def test_nearest_date_forward_fallback_ignores_a_shelf_life_date():
    html = ('<a href="/x">Recall of smoked trout, best before 01.11.2026, '
            'issued 23.10.2026</a>')
    pos = html.index("Recall")
    assert nearest_date(html, pos) == "2026-10-23"


def test_nearest_date_behind_path_is_unchanged():
    html = '<span class="date">23.10.2026</span><a href="/x">Recall</a>'
    assert nearest_date(html, html.index("<a")) == "2026-10-23"
