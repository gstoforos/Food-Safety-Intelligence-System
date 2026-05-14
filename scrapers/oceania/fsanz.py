"""FSANZ (Food Standards Australia New Zealand) — per-recall-page scraper.

Rewrite notes (May 2026):
    The April 2026 version selected items from the /food-recalls listing
    using broad CSS selectors and treated each match as a recall. That
    conflated three URL classes:
      - /food-recalls/recall-alert/<slug>   — canonical recall records
      - /news/<slug>                        — umbrella news articles that
                                              link to MULTIPLE recalls
      - /food-recalls/                      — listing pages

    Symptoms of the conflation:
      * One umbrella news article like
        /news/cereulide-toxin-infant-formula-products would yield ONE row
        instead of the TWO underlying recalls (Sanulac/Alula +
        Nestlé/Alfamino).
      * Dates often missing because the listing snippet for a card
        doesn't always include 'Published DD Month YYYY'.
      * Company/Product fields contained image alt-text fragments
        because the listing card's first <a href> was sometimes the
        image link wrapping the card.

    This version:
      * Treats the listing pages as URL DISCOVERY surfaces only — no
        per-row data is extracted from them.
      * Categorizes discovered URLs into two streams:
          1) /food-recalls/recall-alert/<slug>  → fetched directly
          2) /news/<slug>                       → fetched, then its
                                                  /food-recalls/recall-alert/
                                                  child links are queued
      * Each recall-alert page is fetched and parsed individually,
        giving canonical Company/Product (from <h1>), Date (from
        'Published DD Month YYYY' in page body), and Pathogen (from the
        structured 'Problem:' section + page text run through
        normalize_pathogen).
      * Result: one Recall row per actual recall, with correct dates and
        no image-alt-text contamination.

    Cost: one extra HTTP request per recall (typically 10-20 for a
    30-day window). The listing page now serves as a URL index, not a
    data source.

    Locked audit rules respected:
      * Outbreak=1 requires explicit case counts (number + illness/case
        word). Generic "may cause illness" boilerplate does NOT flip
        Outbreak to 1.
      * Pathogen is taxonomy-canonical only (no raw-title fallback). If
        normalize_pathogen returns nothing for a page, the row is
        dropped, not stored with a sketchy hazard string.
"""
from __future__ import annotations
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Set
import logging
import re

from scrapers._base import BaseScraper, fetch
from scrapers._models import Recall, normalize_pathogen

log = logging.getLogger(__name__)


# ─── Hazard keyword gate ──────────────────────────────────────────────
_HAZARD_GATE = (
    "listeria", "salmonella", "e. coli", "stec", "o157", "shiga",
    "botulin", "hepatitis", "hav", "norovirus", "campylobacter",
    "cyclospora", "vibrio", "cronobacter", "cereulide", "bacillus cereus",
    "biotoxin", "histamine", "shellfish", "aflatoxin", "ochratoxin",
    "patulin", "yersinia", "shigella", "mycotoxin",
    # Mycotoxins (April 2026+ scope: Alternaria + Fusarium + ergot)
    "fumonisin", "zearalenone", "deoxynivalenol", "nivalenol",
    "alternaria", "alternariol", "tenuazonic",
    "t-2 toxin", "ht-2 toxin", "citrinin",
    "ergot", "claviceps", "fusarium",
    "ocratoxin", "ocratossin", "mykotoxin", "micotoxin",
    "micotossin", "mutterkorn",
    # Chemical tampering / heavy metals / foreign-body (April 2026+ scope)
    "rodenticide", "rat poison", "bromadiolon",
    "lead contamin", "cadmium", "arsenic", "mercury",
    "glass fragm", "metal fragm", "plastic fragm", "foreign matter",
    "foreign body",
)


_MONTH_NAMES = (
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
)


def _extract_pathogen(text: str) -> str:
    """Canonical Pathogen via the shared taxonomy. Empty when nothing matched."""
    if not text:
        return ""
    canonical = normalize_pathogen(text)
    # normalize_pathogen falls through to raw text when nothing matched;
    # accept a value only if it's a real rule-hit (short and different
    # from the input).
    if canonical and canonical != text and len(canonical) < 80:
        return canonical
    return ""


def _parse_published_date(text: str) -> Optional[datetime]:
    """Find 'Published DD Month YYYY' in text. Returns None if absent."""
    if not text:
        return None
    m = re.search(
        r"published\s+(\d{1,2})\s+(" + "|".join(_MONTH_NAMES) + r")\s+(\d{4})",
        text, re.I,
    )
    if not m:
        return None
    try:
        return datetime.strptime(
            f"{m.group(1)} {m.group(2).title()} {m.group(3)}",
            "%d %B %Y",
        ).replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _normalize_url(href: str, base: str) -> str:
    """Absolute URL, no fragment, trimmed."""
    if not href:
        return ""
    href = href.split("#", 1)[0].strip()
    if href.startswith("/"):
        href = base + href
    return href


# Strict check: only flip Outbreak to 1 when there's a number AND an
# illness/case word on the same page. Avoids the boilerplate trap
# ("Food products containing X may cause illness/injury if consumed").
_OUTBREAK_PATTERN = re.compile(
    r"\b\d+\s+(?:case|cases|people|illness(?:es)?|sick|hospitali[sz]ation"
    r"|hospitali[sz]ed|reported\s+ill|fell\s+ill)",
    re.IGNORECASE,
)


class FSANZScraper(BaseScraper):
    AGENCY = "FSANZ (AU)"
    COUNTRY = "Australia"

    BASE_URL = "https://www.foodstandards.gov.au"
    LIST_URL = "https://www.foodstandards.gov.au/food-recalls"
    RECALLS_ALT_URL = "https://www.foodstandards.gov.au/food-recalls/recalls"

    RECALL_ALERT_PREFIX = "/food-recalls/recall-alert/"
    NEWS_PREFIX = "/news/"

    # Boilerplate URLs that must NEVER be treated as a recall record.
    _LISTING_URLS = {
        "https://www.foodstandards.gov.au/food-recalls",
        "https://www.foodstandards.gov.au/food-recalls/",
        "https://www.foodstandards.gov.au/food-recalls/recalls",
        "https://www.foodstandards.gov.au/food-recalls/recalls/",
        "https://www.foodstandards.gov.au/food-recalls/recall-alert",
        "https://www.foodstandards.gov.au/food-recalls/recall-alert/",
    }

    # Safety caps to bound network usage when a listing page returns
    # an abnormally large set of links.
    MAX_NEWS_FOLLOWED = 10
    MAX_RECALL_ALERTS_PER_RUN = 60

    def scrape(self, since_days: int = 30) -> List[Recall]:
        from bs4 import BeautifulSoup

        # ── Step 1: discover recall-alert URLs from the listing pages ──
        alert_urls: Set[str] = set()
        news_urls:  Set[str] = set()

        for listing_url in (self.LIST_URL, self.RECALLS_ALT_URL):
            r = fetch(self.session, listing_url)
            if not r:
                log.warning("FSANZ: list-page fetch returned None for %s",
                            listing_url)
                continue
            soup = BeautifulSoup(r.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = _normalize_url(a["href"], self.BASE_URL)
                if not href.startswith(self.BASE_URL):
                    continue
                path = href[len(self.BASE_URL):]
                if path.startswith(self.RECALL_ALERT_PREFIX) \
                        and href not in self._LISTING_URLS:
                    alert_urls.add(href)
                elif path.startswith(self.NEWS_PREFIX):
                    news_urls.add(href)

        # ── Step 2: follow news umbrella articles ───────────────────
        # News pages may link to multiple /food-recalls/recall-alert/*
        # children. The 2026-01-23 cereulide post is the canonical
        # example: one news article, two underlying recalls (Sanulac
        # and Nestlé). Without this step we'd capture only one.
        news_followed = 0
        for news_url in list(news_urls):
            if news_followed >= self.MAX_NEWS_FOLLOWED:
                break
            r = fetch(self.session, news_url)
            if not r:
                continue
            news_followed += 1
            soup = BeautifulSoup(r.text, "html.parser")
            body_lower = soup.get_text(" ", strip=True).lower()
            # Skip news articles that aren't related to a hazard our
            # taxonomy covers — they won't yield recall-alert links
            # we'd want to process anyway.
            if not any(g in body_lower for g in _HAZARD_GATE):
                continue
            for a in soup.find_all("a", href=True):
                href = _normalize_url(a["href"], self.BASE_URL)
                if not href.startswith(self.BASE_URL):
                    continue
                path = href[len(self.BASE_URL):]
                if path.startswith(self.RECALL_ALERT_PREFIX) \
                        and href not in self._LISTING_URLS:
                    alert_urls.add(href)

        log.info(
            "FSANZ: discovered %d recall-alert URL(s) "
            "(followed %d news umbrella(s))",
            len(alert_urls), news_followed,
        )

        if not alert_urls:
            return []

        # ── Step 3: fetch + parse each recall-alert page ────────────
        cutoff = datetime.now(timezone.utc) - timedelta(days=since_days)
        out: List[Recall] = []
        stats = {"no_date": 0, "stale": 0, "no_hazard": 0,
                 "fetch_failed": 0, "no_title": 0}

        for url in sorted(alert_urls)[:self.MAX_RECALL_ALERTS_PER_RUN]:
            try:
                recall = self._parse_recall_page(url, cutoff, stats)
                if recall is not None:
                    out.append(recall)
            except Exception as e:
                log.warning("FSANZ recall-alert parse failed (%s): %s", url, e)

        log.info(
            "FSANZ: %d recalls captured "
            "(%d no-date, %d stale, %d no-hazard, %d fetch-failed, "
            "%d no-title)",
            len(out),
            stats["no_date"], stats["stale"], stats["no_hazard"],
            stats["fetch_failed"], stats["no_title"],
        )
        return out

    def _parse_recall_page(
        self, url: str, cutoff: datetime, stats: dict,
    ) -> Optional[Recall]:
        """Fetch and parse one /food-recalls/recall-alert/* page.

        Returns a Recall on success, or None when the page should be
        dropped. Increments the appropriate counter in `stats` for any
        drop, so the caller can produce a useful summary log.
        """
        from bs4 import BeautifulSoup

        r = fetch(self.session, url)
        if not r:
            stats["fetch_failed"] += 1
            return None

        soup = BeautifulSoup(r.text, "html.parser")
        body_text = soup.get_text(" ", strip=True)

        # Hazard gate first (cheap pre-filter; avoids parsing date on
        # pages that clearly have no relevant hazard).
        if not any(g in body_text.lower() for g in _HAZARD_GATE):
            stats["no_hazard"] += 1
            return None

        # Required: a published date.
        published = _parse_published_date(body_text)
        if not published:
            stats["no_date"] += 1
            return None
        if published < cutoff:
            stats["stale"] += 1
            return None

        # Title from <h1>; fall back to <title> minus the site suffix.
        h1_el = soup.find("h1")
        title = h1_el.get_text(" ", strip=True) if h1_el else ""
        if not title:
            t = soup.find("title")
            if t:
                title = t.get_text(" ", strip=True)
                title = re.sub(
                    r"\s*\|\s*Food Standards Australia New Zealand\s*$",
                    "", title,
                )
        if not title:
            stats["no_title"] += 1
            return None

        # FSANZ recall-alert titles follow "Company - Product" almost
        # universally. Use the first " - " as the split.
        if " - " in title:
            company, product = title.split(" - ", 1)
        else:
            company, product = "", title
        company = company.strip()[:200]
        product = product.strip()[:400]

        # Pathogen: try the structured "Problem:" section first, then
        # the title, then the whole body text. The Problem section is
        # short and explicit ("This recall is due to the potential
        # presence of toxin (cereulide) contamination, …") so it's the
        # cleanest source when present.
        problem_text = self._extract_section(soup, "Problem")
        pathogen = (
            _extract_pathogen(problem_text)
            or _extract_pathogen(title)
            or _extract_pathogen(body_text)
        )
        if not pathogen:
            stats["no_hazard"] += 1
            return None

        # Outbreak: explicit case-count only (locked audit rule).
        outbreak = 1 if _OUTBREAK_PATTERN.search(body_text) else 0

        reason = (problem_text or body_text)[:400].strip()

        return self._new_recall(
            Date=published.strftime("%Y-%m-%d"),
            Company=company or product[:120],
            Brand="—",  # left for downstream enrichment / claude-check
            Product=product,
            Pathogen=pathogen,
            Reason=reason,
            Class="Recall",
            URL=url,
            Outbreak=outbreak,
            Notes="",
        )

    @staticmethod
    def _extract_section(soup, header_name: str) -> str:
        """Return the text of the section whose <h2>/<h3>/<h4> matches
        header_name (e.g. 'Problem' → text under <h2>Problem:</h2>).

        FSANZ recall-alert pages use a flat heading hierarchy: a heading
        followed by <p> siblings until the next heading. We collect
        those intervening sibling text nodes.
        """
        target = header_name.lower()
        for tag in soup.find_all(["h2", "h3", "h4"]):
            t = tag.get_text(" ", strip=True).rstrip(":").strip().lower()
            if t == target:
                chunks = []
                for sib in tag.next_siblings:
                    name = getattr(sib, "name", None)
                    if name in ("h2", "h3", "h4"):
                        break
                    if name is None:  # NavigableString
                        s = str(sib).strip()
                        if s:
                            chunks.append(s)
                    else:
                        s = sib.get_text(" ", strip=True)
                        if s:
                            chunks.append(s)
                return " ".join(chunks)[:1000]
        return ""
