"""GIS (PL) Poland — public food warnings scraper.

WHY THIS REPLACES THE PREVIOUS VERSION (audit 2026-09-14)
=========================================================
Production data: **1 row in 8 months**, and that row
(``2026-06-03`` tuna/egg salad, Listeria) did not come from this scraper
at all — its Notes read ``Discovered via news: wp.pl``. So the honest
count is zero. ``docs/data/scraper-health.json`` has been reporting
``SILENT_STALE`` for Poland with ``days_since_last_row: 102``.

The previous version was ten lines wrapping ``GenericGeminiScraper``::

    class GISScraper(GenericGeminiScraper):
        AGENCY = "GIS (PL)"
        INDEX_URLS = ['https://www.gov.pl/web/gis/ostrzezenia-publiczne-dotyczace-zywnosci']

This is the same shape, and the same four silent failure modes, that the
FSAI (IE) audit of 2026-05-06 documented and replaced — see
``scrapers/europe_eu/fsai.py``. Gemini rate-limited, Gemini deciding it
saw no recalls, markup changing, a network blip: every one of them
returns an empty list and nothing anywhere says so.

**On top of that, the configured URL was not the warnings listing.**
GIS publishes its public food warnings at

    https://www.gov.pl/web/gis/ostrzezenia

That page carries the dated list — verified 2026-09-14, ten entries
spanning 07.08.2026 to 10.09.2026, each linking to a
``/web/gis/ostrzezenie-publiczne-dotyczace-zywnosci…`` detail page. The
slug the scraper was pointed at is the singular *article* form, not the
plural listing, which is why a fetch could succeed (health never showed
``FAIL_404``) while extraction returned nothing, month after month.

WHAT THIS COST
--------------
The warning that exposed it:

    28.08.2026  "Możliwa obecność toksyny botulinowej w jednej partii
                 zielonego pesto"
    Łowicz Pesto alla Genovese 180 g, lot L 605192FE I, best before
    08.2027, MW FOOD Sp. z o.o. (Maspex) — botulinum toxin, ntnh gene
    detected during an epidemiological investigation, two human cases.

Tier 1, outbreak-linked, and RASFF-notified onward to the Netherlands.
It sat unseen for seventeen days and was finally noticed only because
Food Safety News wrote about it on 11.09. The listing above shows this
is not an isolated miss: Listeria ×3, Bacillus cereus and cereulide
warnings in the same five weeks never reached the register either.

THIS VERSION
============
- Points at the real listing, and at nothing else. The 2013-2019
  ``---archiwum`` page is an index of years, not warnings; it is
  deliberately not listed.
- **Deterministic fallback.** When the LLM path yields nothing, the
  listing is parsed directly for GIS's stable warning-slug pattern and
  the ``DD.MM.YYYY`` date rendered beside each entry. Detail-page
  enrichment (producer, lot, best-before) is left to claude-check, as
  with FSAI.
- The fallback is also a **floor**, not just a backup: if the LLM path
  returns fewer rows than the deterministic parse found, the missing
  ones are merged in by URL. A partial Gemini answer used to be
  indistinguishable from a complete one.
- Loud logging at every step, prefixed ``GIS:`` so a workflow log can be
  grepped for which path ran and how many rows each produced, and a
  warning when a live listing yields zero rows.

The Polish hazard words GIS actually uses ("toksyna botulinowa",
"cereulidyna", "alkaloidy pirolizydynowe", "fragmenty szkła") are in
``scrapers/_pathogen_vocab.py``; several were missing and were added in
the same audit.
"""
from __future__ import annotations

import logging
import re
from typing import Dict, List, Optional

from scrapers._base import GenericGeminiScraper, fetch
from scrapers._models import Recall

log = logging.getLogger(__name__)

#: The listing page, verified live 2026-09-14.
GIS_LISTING = "https://www.gov.pl/web/gis/ostrzezenia"

#: GIS warning detail pages all live under this slug stem. Both the
#: "Ostrzeżenie publiczne dotyczące żywności…" articles and the numbered
#: duplicates ("…-pesto2") match. Anchored to /web/gis/ so the many
#: regional station copies (/web/wsse-…, /web/psse-…) are not picked up —
#: they are the same warning republished and would duplicate every row.
_WARNING_HREF = re.compile(
    r'href="((?:https?://www\.gov\.pl)?/web/gis/'
    r'ostrzezenie-publiczne[^"#?]*)"',
    re.I,
)

_DATE = re.compile(r"\b(\d{2})\.(\d{2})\.(20\d{2})\b")

#: How far either side of an anchor to look for the date rendered with it.
#: gov.pl puts the date in a sibling element; 600 characters comfortably
#: spans the card markup without reaching the neighbouring entry.
_DATE_WINDOW = 600

_TAGS = re.compile(r"<[^>]+>")
_WS = re.compile(r"\s+")


def _clean(text: str) -> str:
    return _WS.sub(" ", _TAGS.sub(" ", text)).strip()


def _nearest_date(html: str, pos: int) -> Optional[str]:
    """ISO date rendered nearest to `pos`, or None.

    Looks behind first: gov.pl renders the date before the title in the
    listing card. Falls forward only if nothing is behind, so the last
    entry on a page still resolves.
    """
    behind = html[max(0, pos - _DATE_WINDOW):pos]
    m = None
    for m in _DATE.finditer(behind):
        pass                      # keep the LAST match before the anchor
    if m is None:
        m = _DATE.search(html[pos:pos + _DATE_WINDOW])
    if m is None:
        return None
    day, month, year = m.groups()
    return f"{year}-{month}-{day}"


def _anchor_text(html: str, end: int) -> str:
    """Visible text of the anchor whose href ended at `end`."""
    close = html.find("</a>", end)
    if close < 0 or close - end > 4000:
        return ""
    return _clean(html[end:close])


#: Polish hazard wording as GIS writes it, mapped to a term
#: normalize_pathogen() already understands. The Latin binomials
#: ("Listeria monocytogenes", "Bacillus cereus") appear verbatim in GIS
#: titles and are handled by the shared CORE vocabulary; what needs
#: translating is the wording GIS uses when it does NOT name the organism.
_PL_HAZARDS = (
    ("botulin", "", "Clostridium botulinum"),            # "toksyny botulinowej"
    # Polish declines: jad / jadu / jadem, kiełbasiany / kiełbasianego /
    # kiełbasianym. Match the adjective stem, which survives every case,
    # and carry the diacritic-free spelling some CMS exports produce.
    ("kiełbasian", "", "Clostridium botulinum"),
    ("kielbasian", "", "Clostridium botulinum"),
    ("cereulid", "", "Cereulide"),                       # "cereulidyny"
    ("alkaloidy pirolizydynowe", "", "Pyrrolizidine alkaloids"),
    ("szk", "fragment", "Foreign material (glass)"),     # "fragmentów szkła"
    ("ciało obce", "", "Foreign material"),
    ("ciala obce", "", "Foreign material"),
    ("ciał obcych", "", "Foreign material"),
    ("pleśń", "", "Mold"),
    ("plesn", "", "Mold"),
    ("trutka na szczury", "", "Rodenticide (rat poison)"),
    ("histamin", "", "Histamine / scombrotoxin"),
    ("aflatoksyn", "", "Aflatoxin"),
    ("ochratoksyn", "", "Ochratoxin"),
    ("patulin", "", "Patulin"),
    ("salmonell", "", "Salmonella"),
    ("listerio", "", "Listeria monocytogenes"),
)


def pathogen_from_title(title: str) -> str:
    """Best-effort hazard label from a GIS warning title.

    ``_PL_HAZARDS`` is consulted FIRST because it yields labels
    ``normalize_pathogen`` already canonicalises. The shared CORE
    vocabulary is a fallback for Latin binomials GIS writes verbatim.

    Order matters and is load-bearing. CORE holds the bare stem
    ``"botulin"``, which matches "toksyny botulinowej" — but
    ``normalize_pathogen("botulin")`` does not resolve (its pattern wants
    "botulinum", "botulism" or "botulinum toxin"), so the row would be
    stamped Tier 3. For the one hazard in this register that kills
    without a dose response, that is the wrong way to be wrong.

    An empty string when nothing is recognised — review enriches it
    rather than the scraper guessing.
    """
    from scrapers._pathogen_vocab import CORE

    low = (title or "").lower()
    for first, second, label in _PL_HAZARDS:
        if first in low and (not second or second in low):
            return label
    for kw in CORE:
        k = kw.strip()
        if len(k) > 4 and k in low:
            return k
    return ""


def parse_listing(html: str, listing_url: str = GIS_LISTING) -> List[Dict[str, str]]:
    """Deterministic parse of the GIS warnings listing.

    Returns one dict per warning with ``url``, ``title`` and ``date``
    (ISO, or "" when the page rendered none). De-duplicated by URL,
    preserving page order — newest first, as GIS renders it.

    Pure string handling, no network: tests/test_gis_scraper.py drives it
    from a fixture built from the live page.
    """
    out: List[Dict[str, str]] = []
    seen = set()
    for m in _WARNING_HREF.finditer(html):
        href = m.group(1)
        if href.startswith("/"):
            href = "https://www.gov.pl" + href
        if href in seen:
            continue
        seen.add(href)
        out.append({
            "url": href,
            "title": _anchor_text(html, m.end()),
            "date": _nearest_date(html, m.start()) or "",
        })
    return out


class GISScraper(GenericGeminiScraper):
    AGENCY = "GIS (PL)"
    COUNTRY = "Poland"
    INDEX_URLS = [GIS_LISTING]
    LANGUAGE = "pl"

    EXTRACTION_HINTS = (
        "This is the Polish Chief Sanitary Inspectorate (Główny Inspektorat "
        "Sanitarny) public food warnings listing.\n"
        "- Each entry is titled 'Ostrzeżenie publiczne dotyczące żywności: …' "
        "and the hazard is named in the title itself.\n"
        "- Dates render as DD.MM.YYYY and are the date GIS published the "
        "warning. Emit them as YYYY-MM-DD.\n"
        "- Polish hazard wording to recognise: 'toksyna botulinowa' / 'jad "
        "kiełbasiany' = Clostridium botulinum; 'cereulidyna' = cereulide; "
        "'alkaloidy pirolizydynowe' = pyrrolizidine alkaloids; 'fragmenty "
        "szkła' = glass fragments; 'ciało obce' = foreign body; 'pleśń' = "
        "mould; 'wycofanie' = withdrawal/recall.\n"
        "- 'w jednej partii X' means 'in one batch of X'; X is the product.\n"
        "- The producer, lot number and best-before date are usually on the "
        "linked detail page rather than the listing. Leave them empty rather "
        "than guessing; they are enriched later."
    )

    def scrape(self, since_days: int = 30) -> List[Recall]:
        rows = super().scrape(since_days=since_days)
        log.info("GIS: LLM path returned %d row(s)", len(rows))

        found = self._deterministic(since_days)
        if not found:
            if not rows:
                log.warning(
                    "GIS: listing %s yielded NO warnings by either path — "
                    "the page markup or the URL has probably changed. This "
                    "scraper was silent for 102 days once before; check it.",
                    GIS_LISTING,
                )
            return rows

        have = {str(r.URL or "").rstrip("/") for r in rows}
        merged = list(rows)
        for rec in found:
            if str(rec.URL or "").rstrip("/") not in have:
                merged.append(rec)

        if len(merged) > len(rows):
            log.warning(
                "GIS: deterministic parse recovered %d warning(s) the LLM "
                "path missed (%d -> %d). A partial extraction is otherwise "
                "indistinguishable from a complete one.",
                len(merged) - len(rows), len(rows), len(merged),
            )
        return merged

    # ------------------------------------------------------------------
    def _deterministic(self, since_days: int) -> List[Recall]:
        """Parse the listing without the LLM. Never raises."""
        try:
            resp = fetch(self.session, GIS_LISTING)
        except Exception as exc:                      # noqa: BLE001
            log.warning("GIS: fallback fetch raised: %s", exc)
            return []
        if resp is None or not getattr(resp, "ok", False):
            log.warning(
                "GIS: fallback fetch of %s failed (status=%s)",
                GIS_LISTING, getattr(resp, "status_code", "no-response"),
            )
            return []

        try:
            items = parse_listing(resp.text)
        except Exception as exc:                      # noqa: BLE001
            log.exception("GIS: fallback parse raised: %s", exc)
            return []

        log.info("GIS: deterministic parse found %d warning(s)", len(items))

        from datetime import datetime, timedelta, timezone
        cutoff = (datetime.now(timezone.utc) - timedelta(days=since_days)).date()

        rows: List[Recall] = []
        for it in items:
            if it["date"]:
                try:
                    if datetime.strptime(it["date"], "%Y-%m-%d").date() < cutoff:
                        continue
                except ValueError:
                    pass          # unparseable date: keep, let review catch it
            rows.append(self._new_recall(
                Date=it["date"],
                Product=it["title"],
                Pathogen=pathogen_from_title(it["title"]),
                Reason=it["title"],
                URL=it["url"],
                Notes="GIS listing (deterministic parse); "
                      "producer, lot and best-before are on the detail page",
            ))
        return rows
