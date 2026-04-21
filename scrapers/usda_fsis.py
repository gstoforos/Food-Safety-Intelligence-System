"""
Base class for RSS/Atom food safety news scrapers.

Each subclass sets FEED_URLS, SOURCE_NAME, and optionally PATHOGEN_KEYWORDS.
The base class handles HTTP fetch, XML parse, pathogen detection, and
dedup. Produces NewsItem dicts matching the NEWS sheet schema:
  Published (UTC), Pathogen, Event, Source, Title, Link, Retrieved (UTC)
"""
from __future__ import annotations
import logging
import re
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional
from email.utils import parsedate_to_datetime

from scrapers._base import make_session, fetch
from scrapers._models import normalize_pathogen, PATHOGEN_RULES

log = logging.getLogger(__name__)


# Keywords that signal a food-safety-relevant news item even if no
# specific pathogen name appears (e.g. "recall", "outbreak", "contamination").
FOOD_SAFETY_CONTEXT = re.compile(
    r"recall|outbreak|contaminat|food.?borne|food.?poison|withdraw|"
    r"advisory|alert|warning|illness|hospitali[sz]|pathogen|"
    r"surveillance|inspection|violation|adulterat",
    re.IGNORECASE,
)

# Build a combined regex from all PATHOGEN_RULES patterns for fast scanning
_all_pathogen_patterns = "|".join(f"(?:{pat})" for _, pat in PATHOGEN_RULES)
PATHOGEN_RE = re.compile(_all_pathogen_patterns, re.IGNORECASE)


class NewsItem:
    """Single news article matching the NEWS sheet schema."""
    __slots__ = ("published", "pathogen", "event", "source", "title", "link", "retrieved")

    def __init__(self, published: str, pathogen: str, event: str,
                 source: str, title: str, link: str):
        self.published = published
        self.pathogen = pathogen
        self.event = event
        self.source = source
        self.title = title
        self.link = link
        self.retrieved = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def to_dict(self) -> Dict[str, str]:
        return {
            "Published (UTC)": self.published,
            "Pathogen": self.pathogen,
            "Event": self.event,
            "Source": self.source,
            "Title": self.title,
            "Link": self.link,
            "Retrieved (UTC)": self.retrieved,
        }


class BaseNewsScraper(ABC):
    """Abstract RSS/Atom news scraper."""

    SOURCE_NAME: str = ""
    FEED_URLS: List[str] = []

    # If True, only keep items that mention a known pathogen.
    # If False, also keep items with food safety context keywords.
    PATHOGEN_STRICT: bool = False

    def __init__(self, session=None):
        self.session = session or make_session(timeout=20)

    def scrape_news(self, since_days: int = 7) -> List[NewsItem]:
        """Fetch all feeds and return pathogen-related news items."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=since_days)
        items: List[NewsItem] = []
        seen_links: set = set()

        for feed_url in self.FEED_URLS:
            try:
                raw_items = self._fetch_feed(feed_url, cutoff)
                for item in raw_items:
                    link = item.get("link", "").strip()
                    if not link or link in seen_links:
                        continue
                    seen_links.add(link)

                    title = item.get("title", "").strip()
                    description = item.get("description", "").strip()
                    pub_date = item.get("pub_date", "")
                    text = f"{title} {description}"

                    # Detect pathogen from title + description
                    pathogen = self._detect_pathogen(text)

                    # Filter: must mention a pathogen or food safety context
                    if not pathogen and self.PATHOGEN_STRICT:
                        continue
                    if not pathogen and not FOOD_SAFETY_CONTEXT.search(text):
                        continue

                    # Classify event type from text
                    event = self._classify_event(text)

                    items.append(NewsItem(
                        published=pub_date,
                        pathogen=pathogen,
                        event=event,
                        source=self.SOURCE_NAME,
                        title=title[:200],
                        link=link,
                    ))
            except Exception as e:
                log.warning("Feed %s failed: %s", feed_url, e)

        log.info("[NEWS] %s: %d items from %d feeds",
                 self.SOURCE_NAME, len(items), len(self.FEED_URLS))
        return items

    def _fetch_feed(self, url: str, cutoff: datetime) -> List[Dict[str, str]]:
        """Fetch and parse an RSS/Atom feed. Returns raw item dicts."""
        resp = fetch(self.session, url)
        if not resp:
            return []

        items: List[Dict[str, str]] = []
        try:
            root = ET.fromstring(resp.content)
        except ET.ParseError as e:
            log.warning("XML parse failed for %s: %s", url, e)
            return []

        # Handle RSS 2.0
        ns = {"atom": "http://www.w3.org/2005/Atom",
              "dc": "http://purl.org/dc/elements/1.1/",
              "content": "http://purl.org/rss/1.0/modules/content/"}

        for item_el in root.iter("item"):
            entry = self._parse_rss_item(item_el, ns, cutoff)
            if entry:
                items.append(entry)

        # Handle Atom
        for entry_el in root.iter("{http://www.w3.org/2005/Atom}entry"):
            entry = self._parse_atom_entry(entry_el, ns, cutoff)
            if entry:
                items.append(entry)

        return items

    def _parse_rss_item(self, el: ET.Element, ns: dict, cutoff: datetime) -> Optional[Dict[str, str]]:
        title = (el.findtext("title") or "").strip()
        link = (el.findtext("link") or "").strip()
        desc = (el.findtext("description") or "").strip()
        pub = (el.findtext("pubDate") or el.findtext("dc:date", namespaces=ns) or "").strip()

        pub_dt = self._parse_pub_date(pub)
        if pub_dt and pub_dt < cutoff:
            return None

        pub_iso = pub_dt.strftime("%Y-%m-%dT%H:%M:%SZ") if pub_dt else ""

        # Strip HTML tags from description
        desc = re.sub(r"<[^>]+>", " ", desc)
        desc = re.sub(r"\s+", " ", desc).strip()[:500]

        return {"title": title, "link": link, "description": desc, "pub_date": pub_iso}

    def _parse_atom_entry(self, el: ET.Element, ns: dict, cutoff: datetime) -> Optional[Dict[str, str]]:
        atom = "http://www.w3.org/2005/Atom"
        title = (el.findtext(f"{{{atom}}}title") or "").strip()
        link_el = el.find(f"{{{atom}}}link[@rel='alternate']")
        if link_el is None:
            link_el = el.find(f"{{{atom}}}link")
        link = (link_el.get("href", "") if link_el is not None else "").strip()
        summary = (el.findtext(f"{{{atom}}}summary") or
                   el.findtext(f"{{{atom}}}content") or "").strip()
        pub = (el.findtext(f"{{{atom}}}published") or
               el.findtext(f"{{{atom}}}updated") or "").strip()

        pub_dt = self._parse_pub_date(pub)
        if pub_dt and pub_dt < cutoff:
            return None

        pub_iso = pub_dt.strftime("%Y-%m-%dT%H:%M:%SZ") if pub_dt else ""
        summary = re.sub(r"<[^>]+>", " ", summary)
        summary = re.sub(r"\s+", " ", summary).strip()[:500]

        return {"title": title, "link": link, "description": summary, "pub_date": pub_iso}

    @staticmethod
    def _parse_pub_date(text: str) -> Optional[datetime]:
        """Parse RFC 2822, ISO 8601, or common date formats."""
        if not text:
            return None
        # RFC 2822 (common in RSS)
        try:
            return parsedate_to_datetime(text)
        except (ValueError, TypeError):
            pass
        # ISO 8601
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            pass
        return None

    @staticmethod
    def _detect_pathogen(text: str) -> str:
        """Scan text for known pathogens. Returns canonical name or ''."""
        m = PATHOGEN_RE.search(text)
        if not m:
            return ""
        return normalize_pathogen(m.group(0))

    @staticmethod
    def _classify_event(text: str) -> str:
        """Classify the event type from text content."""
        low = text.lower()
        if re.search(r"outbreak|cluster|case.?count|hospitali[sz]|death", low):
            return "Outbreak"
        if re.search(r"recall|withdraw|pull.?from|remov.?from.?market", low):
            return "Recall"
        if re.search(r"alert|warning|advisory|notification", low):
            return "Alert"
        if re.search(r"inspect|violat|enforce|fine|penalt|shut.?down", low):
            return "Enforcement"
        if re.search(r"study|research|report|survey|surveillance|finding", low):
            return "Research"
        return "News"
