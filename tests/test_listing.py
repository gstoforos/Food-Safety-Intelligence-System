# -*- coding: utf-8 -*-
"""scrapers/_listing.py — the deterministic listing parse.

This is the GIS (PL) method generalised so the other 56 ten-line scrapers
can opt in with one line. It runs as a floor under the LLM path, so its
job is to be dull and correct: find the dated entries, pair each with its
own date, and refuse to invent rows when it cannot.
"""

from __future__ import annotations

import re
import sys
from datetime import date
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scrapers._listing import (  # noqa: E402
    extract_links,
    looks_like_listing,
    nearest_date,
    parse_any_date,
    within_window,
)


# --------------------------------------------------------------------------
# date parsing
# --------------------------------------------------------------------------

@pytest.mark.parametrize("text,expected", [
    ("published 2026-08-28", "2026-08-28"),          # ISO
    ("28.08.2026", "2026-08-28"),                    # day-first, EU
    ("28/08/2026", "2026-08-28"),
    ("28-08-2026", "2026-08-28"),
    ("2026.08.28", "2026-08-28"),                    # East-Asian dotted
    ("2026/08/28", "2026-08-28"),
    ("5.9.2026", "2026-09-05"),                      # unpadded
])
def test_regulator_date_shapes_parse(text, expected):
    assert parse_any_date(text) == expected


def test_iso_wins_over_a_day_first_reading():
    """2026-08-28 must never be read as day 2026."""
    assert parse_any_date("2026-08-28") == "2026-08-28"


def test_an_impossible_date_is_rejected_not_coerced():
    """31/02 is a parse error, not the 2nd of some month."""
    assert parse_any_date("31.02.2026") is None


def test_ambiguous_slash_dates_are_read_day_first():
    """03/04/2026 is genuinely ambiguous.

    Day-first is chosen because 62 of the 66 agencies are outside the US,
    and the two US scrapers are hardened ones that never reach this module.
    The point of the test is that the choice is deliberate and visible.
    """
    assert parse_any_date("03/04/2026") == "2026-04-03"


def test_no_date_returns_none():
    assert parse_any_date("brak daty") is None
    assert parse_any_date("") is None


# --------------------------------------------------------------------------
# pairing a date with the right entry
# --------------------------------------------------------------------------

CARDS = (
    '<li><span class="date">10.09.2026</span>'
    '<a href="/web/x/alert-one">First</a></li>'
    '<li><span class="date">09.09.2026</span>'
    '<a href="/web/x/alert-two">Second</a></li>'
    '<li><span class="date">28.08.2026</span>'
    '<a href="/web/x/alert-three">Third</a></li>'
)
BASE = "https://example.gov/web/x/list"
DETAIL = re.compile(r"/web/x/alert-")


def test_each_entry_gets_its_own_date_not_the_neighbours():
    got = extract_links(CARDS, BASE, DETAIL)
    assert [e["date"] for e in got] == ["2026-09-10", "2026-09-09", "2026-08-28"]


def test_the_date_before_the_anchor_wins():
    """Listings render date-then-title; the nearest date BEHIND is this
    card's, not the previous card's."""
    pos = CARDS.index('href="/web/x/alert-two"')
    assert nearest_date(CARDS, pos) == "2026-09-09"


def test_a_date_after_the_anchor_is_used_when_nothing_is_behind():
    html = '<a href="/web/x/alert-one">T</a><span>28.08.2026</span>'
    assert extract_links(html, BASE, DETAIL)[0]["date"] == "2026-08-28"


def test_titles_are_stripped_of_markup():
    html = '<a href="/web/x/alert-one"><span>  Two   words </span></a>'
    assert extract_links(html, BASE, DETAIL)[0]["title"] == "Two words"


# --------------------------------------------------------------------------
# what it refuses to do
# --------------------------------------------------------------------------

def test_no_pattern_means_no_rows():
    """A generic every-link parse would turn nav chrome into recalls.

    This is the single most important line in the module: the fallback is
    opt-in, so switching it on cannot silently start inventing rows for
    fifty-six scrapers at once.
    """
    assert extract_links(CARDS, BASE, None) == []


def test_links_to_other_hosts_are_skipped_by_default():
    html = ('<a href="https://elsewhere.example/web/x/alert-nine">x</a>'
            '<a href="/web/x/alert-one">y</a>')
    got = extract_links(html, BASE, DETAIL)
    assert len(got) == 1
    assert got[0]["url"].startswith("https://example.gov/")


def test_duplicate_hrefs_collapse():
    assert len(extract_links(CARDS + CARDS, BASE, DETAIL)) == 3


def test_relative_urls_are_made_absolute():
    got = extract_links(CARDS, BASE, DETAIL)
    assert all(e["url"].startswith("https://example.gov/web/x/") for e in got)


def test_empty_html_is_not_an_error():
    assert extract_links("", BASE, DETAIL) == []


def test_undated_entries_are_kept_not_dropped():
    """A missing date is review's problem. Dropping the row loses a recall."""
    html = '<a href="/web/x/alert-one">No date here</a>'
    got = extract_links(html, BASE, DETAIL)
    assert len(got) == 1 and got[0]["date"] == ""


# --------------------------------------------------------------------------
# looks_like_listing — the check that would have caught GIS on day one
# --------------------------------------------------------------------------

def test_a_real_listing_is_recognised():
    v = looks_like_listing(CARDS, BASE, DETAIL)
    assert v["is_listing"] is True
    assert v["dated_links"] == 3


def test_an_article_page_is_not_mistaken_for_a_listing():
    """The GIS failure exactly: a 200 response from the wrong slug."""
    article = ('<h1>Ostrzeżenie publiczne</h1>'
               '<p>Data publikacji 28.08.2026</p>'
               '<p>Treść ostrzeżenia…</p>')
    v = looks_like_listing(article, BASE, DETAIL)
    assert v["is_listing"] is False
    assert "does not look like a listing" in v["reason"]


def test_a_stale_pattern_is_named_as_such():
    """Dates present, no href matches: the pattern moved, not the page."""
    html = ('<li>10.09.2026 <a href="/new-path/alert-one">a</a></li>'
            '<li>09.09.2026 <a href="/new-path/alert-two">b</a></li>'
            '<li>08.09.2026 <a href="/new-path/alert-three">c</a></li>')
    v = looks_like_listing(html, BASE, DETAIL)
    assert v["is_listing"] is False
    assert "pattern is probably stale" in v["reason"]


def test_changed_card_markup_is_named_as_such():
    """Links match, dates gone: the markup changed, not the URL."""
    html = ('<a href="/web/x/alert-one">a</a>'
            '<a href="/web/x/alert-two">b</a>'
            '<a href="/web/x/alert-three">c</a>')
    v = looks_like_listing(html, BASE, DETAIL)
    assert v["is_listing"] is False
    assert "none carry a date" in v["reason"]


def test_a_verdict_without_a_pattern_says_it_is_weak():
    v = looks_like_listing(CARDS, BASE, None)
    assert "weak signal" in v["reason"]


# --------------------------------------------------------------------------
# the scrape window
# --------------------------------------------------------------------------

def test_window_keeps_recent_and_drops_old():
    cutoff = date(2026, 8, 15)
    assert within_window("2026-08-28", cutoff) is True
    assert within_window("2026-08-15", cutoff) is True       # inclusive edge
    assert within_window("2026-08-14", cutoff) is False


def test_window_keeps_undated_rows():
    """Deliberate: better to review a dateless row than drop a recall."""
    assert within_window("", date(2026, 8, 15)) is True
    assert within_window("not-a-date", date(2026, 8, 15)) is True
