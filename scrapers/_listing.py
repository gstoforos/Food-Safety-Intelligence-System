# -*- coding: utf-8 -*-
"""Deterministic listing parse — the GIS (PL) method, generalised.

WHY THIS MODULE EXISTS
======================
On 2026-09-14 the GIS (PL) scraper was found to have produced zero rows in
eight months while reporting no error. It was ten lines wrapping
``GenericLLMScraper``, aimed at ``/web/gis/ostrzezenia-publiczne-
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
    "strip_shelf_life_dates",
]

#: Date shapes that regulator listings actually render, with the parse each
#: needs. Order matters: the unambiguous ISO form is tried first, then
#: day-first (Europe, most of this register), then month-first (US), then
#: the East-Asian dotted form. A listing that renders 03/04/2026 is
#: genuinely ambiguous; day-first wins because 62 of the 66 agencies here
#: are outside the United States, and the two US scrapers are hardened ones
#: that never reach this module.
#: English month names, full and abbreviated, lower-cased. Only English is
#: listed, deliberately. The agencies whose listings render a month NAME
#: rather than digits are the English-language ones — FDA, FSAI, FSA (UK),
#: FSANZ, MPI NZ, CFIA's English side — and a month table that guessed at
#: other languages would collide ("mai" is May in German and French but
#: March in Estonian; "juni"/"juli" differ by one letter across four
#: languages). An unrecognised word yields None, which is the honest answer.
MONTH_NAMES = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}

DATE_PATTERNS: Sequence[tuple] = (
    (re.compile(r"\b(20\d{2})-(\d{1,2})-(\d{1,2})\b"), ("y", "m", "d")),
    (re.compile(r"\b(20\d{2})\.(\d{1,2})\.(\d{1,2})\b"), ("y", "m", "d")),
    (re.compile(r"\b(20\d{2})/(\d{1,2})/(\d{1,2})\b"), ("y", "m", "d")),
    (re.compile(r"\b(\d{1,2})\.(\d{1,2})\.(20\d{2})\b"), ("d", "m", "y")),
    (re.compile(r"\b(\d{1,2})/(\d{1,2})/(20\d{2})\b"), ("d", "m", "y")),
    (re.compile(r"\b(\d{1,2})-(\d{1,2})-(20\d{2})\b"), ("d", "m", "y")),
    # ── Month-NAME forms (added 2026-09-29) ────────────────────────────────
    # Until today this module could not read a month name at all, so the
    # single largest English-language date shape parsed as nothing. Two rows
    # reached the register carrying the regulator's display string verbatim
    # in the Date column — FDA's "September 18, 2026" and FSAI's "Friday,
    # 18 September 2026". Neither could ever publish (the gate refuses a
    # non-YYYY-MM-DD Date, correctly) but both sorted and filtered as
    # nothing at all, which is how a real 18-September recall becomes
    # invisible to the weekly builder rather than merely unpublished.
    #
    # An optional weekday prefix is not matched here: it does not need to
    # be. "Friday, 18 September 2026" contains "18 September 2026", and \b
    # lets the pattern start there.
    (re.compile(r"\b(\d{1,2})\s+([A-Za-z]{3,9})\.?,?\s+(20\d{2})\b"),
     ("d", "M", "y")),
    (re.compile(r"\b([A-Za-z]{3,9})\.?\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(20\d{2})\b"),
     ("M", "d", "y")),
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


# ── SHELF-LIFE DATES ARE NOT PUBLICATION DATES (audit 2026-10-25) ─────────
#
# A regulator's own headline often carries the product's best-before or
# use-by date: the FSVO writes "DANFOREL A/S ruft das Produkt «Forellen-
# filets geräuchert - Nature» mit dem Mindesthaltbarkeitsdatum 01.11.2026»
# wegen Listerien zurück". Any date-finder pointed at that text picks up
# 01.11.2026, and a shelf-life date is always in the FUTURE — so the row is
# filed into a report week that has not happened yet. That is what the
# Danforel Listeria recall did: published 23.10.2026, stored as 2026-11-01,
# stamped report_week W44, and absent from the week it belongs to.
#
# This removes ONLY a date that the text itself labels as a shelf-life or
# lot date. The label has to be there; nothing is guessed. A publication
# date is never introduced by one of these words, so no caller loses one.
_SHELF_LIFE_LABELS = (
    # German (BVL, BLV/FSVO, AGES)
    "mindesthaltbarkeitsdatum", "haltbarkeitsdatum", "verbrauchsdatum",
    "mhd", "zu verbrauchen bis", "verbrauchen bis", "haltbar bis",
    # French (RappelConso, FAVV, Luxembourg)
    "date limite de consommation", "date de durabilite minimale",
    "date de durabilité minimale", "a consommer jusqu",
    "à consommer jusqu", "a consommer de preference avant",
    "à consommer de préférence avant", "dlc", "dduo",
    # English (FSA, FSAI, FSANZ, MPI, FDA, FSIS)
    "best before", "best-before", "best by", "use by", "use-by",
    "sell by", "sell-by", "expiration date", "expiry date", "exp date",
    # Italian / Spanish / Portuguese
    "termine minimo di conservazione", "da consumarsi entro",
    "da consumarsi preferibilmente entro", "tmc",
    "fecha de caducidad", "consumo preferente", "fecha de consumo",
    "data de validade",
    # Dutch / Nordic
    "tenminste houdbaar tot", "ten minste houdbaar tot",
    "te gebruiken tot", "tht", "tgt",
    "siste forbruksdag", "best for", "sista forbrukningsdag",
    "sista förbrukningsdag", "parasta ennen",
    # lot / batch markings that carry a date
    "lot number", "batch code", "lot code", "chargennummer",
)

#: A label, then up to this many characters of slack, then a date. The slack
#: covers "best before dates between", "à consommer jusqu'au" and "MHD /
#: Charge" style wording while staying short enough that the next sentence's
#: date is out of reach. NON-DIGIT characters only, so the slack can never
#: skip OVER a date to reach a later one.
_SHELF_LIFE_SLACK = 40

_ANY_DATE = "|".join(p.pattern for p, _ in DATE_PATTERNS)

#: A label, slack, a date — then any further dates joined to it by "and" /
#: "to" / a comma or slash, because a notice naming two batches prints two
#: shelf-life dates ("best before 28/04/2027 and 09/05/2027").
_SHELF_LIFE_RE = re.compile(
    r"(?P<label>%s)(?:[^\d]{0,%d}?(?:%s))"
    r"(?:\s*(?:and|und|et|e|y|o|to|bis|or|ou|&|,|/|;)\s*(?:%s))*" % (
        "|".join(re.escape(lbl) for lbl in _SHELF_LIFE_LABELS),
        _SHELF_LIFE_SLACK,
        _ANY_DATE,
        _ANY_DATE,
    ),
    re.IGNORECASE,
)


def strip_shelf_life_dates(text: str) -> str:
    """`text` with every date that it labels as a shelf-life/lot date removed.

    Only a date introduced by one of `_SHELF_LIFE_LABELS` is touched, and the
    label survives verbatim — just the date after it goes, replaced by a
    space so word boundaries hold. Text with no such label comes back
    unchanged, character for character.
    """
    if not text:
        return text or ""
    return _SHELF_LIFE_RE.sub(lambda m: m.group("label") + " ", text)


def parse_any_date(text: str) -> Optional[str]:
    """First date in `text` as ISO, or None. Rejects impossible dates.

    Every match of each pattern is tried, not only the first. Before
    2026-09-29 a pattern that matched something impossible ("31/02/2026") or
    something that only looks like a date ("12 items 2026") gave up on that
    shape entirely and moved to the next pattern, so a perfectly good date
    later in the same string was never reached. That mattered the moment
    month names were added below, because ``[A-Za-z]{3,9}`` matches any
    word and the first word-shaped match in a card is frequently not a
    month.
    """
    text = text or ""
    for pattern, order in DATE_PATTERNS:
        for m in pattern.finditer(text):
            parts = dict(zip(order, m.groups()))
            try:
                if "M" in parts:            # month rendered as a name
                    month = MONTH_NAMES[parts["M"].strip(". ").lower()]
                else:
                    month = int(parts["m"])
                d = date(int(parts["y"]), month, int(parts["d"]))
            except (ValueError, KeyError):
                # 31/02, a day-first read of a month-first date, or a word
                # that is not a month. Try the next match of this shape.
                continue
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
    # The forward fallback reads the card's own title, which is where a
    # regulator prints the product's best-before date (audit 2026-10-25 —
    # see strip_shelf_life_dates). The BEHIND path above is untouched: it
    # reads the listing's own date column, which carries no shelf-life text.
    return parse_any_date(strip_shelf_life_dates(html[pos:pos + window]))


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
