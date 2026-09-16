# -*- coding: utf-8 -*-
"""Deterministic listing parse — the GIS (PL) method, generalised.

WHY THIS MODULE EXISTS
======================
On 2026-09-14 the GIS (PL) scraper was found to have produced zero rows in
eight months while reporting no error. It was ten lines wrapping
``GenericGeminiScraper``, aimed at ``/web/gis/ostrzezenia-publiczne-
dotyczace-zywnosci`` — the singular *article* slug — while GIS publishes
its dated list at ``/web/gis/ostrzezenia``. The fetch succeeded, so health
never showed a failure; the LLM had nothing to extract; an empty list came
back, month after month. The cost was a Tier-1 botulism recall (Łowicz
pesto, 28.08.2026) sitting unseen for seventeen days.

It is not one scraper's problem. **45 of the 73 live scrapers are the same
ten-line shape**, and the FSAI (IE) audit of 2026-05-06 wrote up the same
four silent failure modes before replacing that one by hand: the LLM
rate-limited, the LLM deciding it saw no recalls, markup changing, a
network blip. Every one returns ``[]``.

So rather than rewrite forty-five files, the method that fixed GIS lives
here and any scraper can opt into it in one line.

WHAT IT DOES
============
``extract_links(html, …)`` finds the entries on a regulator listing page
without an LLM: hrefs matching a per-agency pattern, each paired with the
date rendered beside it. That is all a listing page is.

Two properties matter more than cleverness:

1. **It is a floor, not a replacement.** Callers merge its output into the
   LLM's by URL. A partial LLM answer is otherwise indistinguishable from
   a complete one — which is the failure that hid GIS.
2. **It is honest about finding nothing.** ``looks_like_listing()`` answers
   the question the old code could not: *is the configured URL even a
   listing?* A page with no dated links under a detail pattern is a page
   nobody should be pointing a scraper at, and it says so rather than
   returning an empty list that reads like a quiet week.

WHAT IT DELIBERATELY DOES NOT DO
================================
It does not guess a detail pattern when none is configured. A generic
"every link on the page" parse produces navigation chrome, cookie notices
and language switchers, and those become review burden and false rows.
An agency that has not declared ``DETAIL_URL_RE`` gets the probe (is this
a listing at all?) but no emitted rows.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Dict, List, Optional, Sequence
from urllib.parse import urljoin, urlparse

__all__ = [
    "DATE_PATTERNS",
    "extract_links",
    "looks_like_listing",
    "nearest_date",
    "parse_any_date",
]

#: Date shapes that regulator listings actually render, with the parse each
#: needs. Order matters: the unambiguous ISO form is tried first, then
#: day-first (Europe, most of this register), then month-first (US), then
#: the East-Asian dotted form. A listing that renders 03/04/2026 is
#: genuinely ambiguous; day-first wins because 62 of the 66 agencies here
#: are outside the United States, and the two US scrapers are hardened ones
#: that never reach this module.
DATE_PATTERNS: Sequence[tuple] = (
    (re.compile(r"\b(20\d{2})-(\d{1,2})-(\d{1,2})\b"), ("y", "m", "d")),
    (re.compile(r"\b(20\d{2})\.(\d{1,2})\.(\d{1,2})\b"), ("y", "m", "d")),
    (re.compile(r"\b(20\d{2})/(\d{1,2})/(\d{1,2})\b"), ("y", "m", "d")),
    (re.compile(r"\b(\d{1,2})\.(\d{1,2})\.(20\d{2})\b"), ("d", "m", "y")),
    (re.compile(r"\b(\d{1,2})/(\d{1,2})/(20\d{2})\b"), ("d", "m", "y")),
    (re.compile(r"\b(\d{1,2})-(\d{1,2})-(20\d{2})\b"), ("d", "m", "y")),
)

#: How far either side of an anchor to look for its date. Regulator cards
#: put the date in a sibling element; 600 characters spans typical card
#: markup without reaching the next entry. Verified against gov.pl.
DATE_WINDOW = 600

_TAGS = re.compile(r"<[^>]+>")
_WS = re.compile(r"\s+")
_HREF = re.compile(r'href\s*=\s*["\']([^"\'#][^"\']*)["\']', re.I)


def _clean(text: str) -> str:
    return _WS.sub(" ", _TAGS.sub(" ", text)).strip()


def parse_any_date(text: str) -> Optional[str]:
    """First date in `text` as ISO, or None. Rejects impossible dates."""
    for pattern, order in DATE_PATTERNS:
        m = pattern.search(text or "")
        if not m:
            continue
        parts = dict(zip(order, m.groups()))
        try:
            d = date(int(parts["y"]), int(parts["m"]), int(parts["d"]))
        except (ValueError, KeyError):
            continue        # 31/02, or a day-first read of a month-first date
        return d.isoformat()
    return None


def nearest_date(html: str, pos: int, window: int = DATE_WINDOW) -> Optional[str]:
    """ISO date rendered nearest to `pos`.

    Looks BEHIND first — every regulator listing checked renders the date
    before the title — and takes the last match before the anchor, which is
    the one belonging to this card rather than the previous one. Falls
    forward only when nothing is behind, so the first entry on a page still
    resolves.
    """
    behind = html[max(0, pos - window):pos]
    best = None
    for pattern, order in DATE_PATTERNS:
        for m in pattern.finditer(behind):
            if best is None or m.start() > best[0]:
                best = (m.start(), m.group(0))
    if best is not None:
        got = parse_any_date(best[1])
        if got:
            return got
    return parse_any_date(html[pos:pos + window])


def extract_links(
    html: str,
    base_url: str,
    detail_re: Optional[re.Pattern] = None,
    same_host_only: bool = True,
    window: int = DATE_WINDOW,
) -> List[Dict[str, str]]:
    """Entries on a listing page: ``url``, ``title``, ``date`` (ISO or "").

    De-duplicated by URL, in page order — which for every regulator listing
    checked is newest first. Pure string handling: no network, so it is
    testable from a fixture.

    `detail_re` matches the href as written in the markup (before joining
    against `base_url`), so a pattern can anchor on the site's own path.
    Without it nothing is emitted — see the module docstring on why a
    generic every-link parse is worse than no parse.
    """
    if detail_re is None:
        return []

    host = urlparse(base_url).netloc.lower()
    out: List[Dict[str, str]] = []
    seen = set()

    for m in _HREF.finditer(html or ""):
        href = m.group(1).strip()
        if not detail_re.search(href):
            continue
        absolute = urljoin(base_url, href)
        if same_host_only and urlparse(absolute).netloc.lower() != host:
            continue
        key = absolute.rstrip("/")
        if key in seen:
            continue
        seen.add(key)

        # The href match ends at the closing quote, still INSIDE the <a ...>
        # tag, so the title slice has to start after that tag closes.
        # Without this the title comes back as "> Real title" and every
        # hazard lookup is done against a string with a stray bracket on it.
        open_end = html.find(">", m.end())
        start = open_end + 1 if 0 <= open_end - m.end() < 600 else m.end()
        close = html.find("</a>", start)
        title = _clean(html[start:close]) if 0 <= close - start < 4000 else ""
        out.append({
            "url": absolute,
            "title": title,
            "date": nearest_date(html, m.start(), window) or "",
        })
    return out


def looks_like_listing(
    html: str,
    base_url: str,
    detail_re: Optional[re.Pattern] = None,
    min_entries: int = 3,
) -> Dict[str, object]:
    """Is the page at `base_url` a listing of dated entries?

    This is the check that would have caught GIS on day one. A fetch
    returning 200 says nothing; a listing has many links and many dates,
    an article has few of both.

    Returns a verdict dict — ``is_listing``, ``links``, ``dated_links``,
    ``dates_on_page``, ``reason`` — rather than a bare bool, because the
    numbers are what tell an operator *how* it is wrong: an article slug
    scores near zero, a listing whose markup changed keeps its links and
    loses its dates.
    """
    total_links = len(set(_HREF.findall(html or "")))
    dates_on_page = sum(len(p.findall(html or "")) for p, _ in DATE_PATTERNS)

    entries = extract_links(html, base_url, detail_re) if detail_re else []
    dated = [e for e in entries if e["date"]]

    if detail_re is None:
        is_listing = total_links >= 10 and dates_on_page >= min_entries
        reason = ("no DETAIL_URL_RE configured — judged on raw link and date "
                  "counts only, which is a weak signal")
    elif len(dated) >= min_entries:
        is_listing = True
        reason = "ok"
    elif entries and not dated:
        is_listing = False
        reason = ("%d detail link(s) found but none carry a date — the card "
                  "markup probably changed" % len(entries))
    elif not entries and dates_on_page >= min_entries:
        is_listing = False
        reason = ("page has %d date(s) but no href matches DETAIL_URL_RE — "
                  "the pattern is probably stale" % dates_on_page)
    else:
        is_listing = False
        reason = ("only %d dated entr(ies) found — this does not look like a "
                  "listing page at all. Check the URL: a regulator's article "
                  "slug and its listing slug are often one word apart."
                  % len(dated))

    return {
        "url": base_url,
        "is_listing": is_listing,
        "links": total_links,
        "detail_links": len(entries),
        "dated_links": len(dated),
        "dates_on_page": dates_on_page,
        "reason": reason,
    }


def within_window(iso_date: str, cutoff: date) -> bool:
    """True when the row is inside the scrape window, or undated.

    Undated rows are KEPT deliberately: the pipeline would rather review a
    row with a missing date than silently drop a recall because a listing
    stopped rendering one.
    """
    if not iso_date:
        return True
    try:
        return datetime.strptime(iso_date[:10], "%Y-%m-%d").date() >= cutoff
    except (ValueError, TypeError):
        return True
