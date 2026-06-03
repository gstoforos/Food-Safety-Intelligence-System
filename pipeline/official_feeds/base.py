"""
Base types for the official-feed collector: the normalized Record that every
source fetcher must emit, plus a simple source registry.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Optional


@dataclass
class Record:
    """
    A normalized recall record. Every source fetcher maps its native API/RSS
    fields into this shape, so downstream classify/dedup/xlsx code is uniform.
    """
    source_id: str          # authority's unique notation, e.g. "FSA-PRIN-05-2018"
    country_code: str        # ISO2: 'gb', 'sct', 'ie', ...
    country_name: str        # 'United Kingdom', 'Scotland', 'Ireland'
    authority: str           # 'FSA', 'FSS', 'FSAI'
    title: str               # full alert title
    company: str = ""        # recalling company / brand
    product: str = ""        # product name
    hazard: str = ""         # hazard/reason text (fed to classifier)
    alert_type: str = ""     # 'recall' | 'allergy' | 'action' | ''
    region: str = ""         # 'Europe', 'North America', ...
    recall_class: str = ""   # authority class verbatim, e.g. 'Class I' (US/CA)
    outbreak: int = 0        # 1 if authority flags outbreak-linked
    published: Optional[datetime] = None  # publication date (UTC)
    url: str = ""            # canonical alert URL
    raw: dict = field(default_factory=dict)  # original payload (debug)

    def hazard_blob(self) -> str:
        """Combined text for the tier classifier."""
        return " ".join(p for p in (self.title, self.hazard, self.product) if p)

    def age_days(self, now: Optional[datetime] = None) -> Optional[float]:
        if self.published is None:
            return None
        now = now or datetime.now(timezone.utc)
        pub = self.published
        if pub.tzinfo is None:
            pub = pub.replace(tzinfo=timezone.utc)
        return (now - pub).total_seconds() / 86400.0


# ─── Source registry ─────────────────────────────────────────────────────────

@dataclass
class FeedSource:
    code: str                       # 'uk', 'scotland', 'ireland', ...
    name_en: str
    authority_short: str
    fetcher: Callable[..., list]    # callable returning list[Record]
    region: str = ""                # 'Europe', 'North America', 'Oceania', ...
    timezone: str = "UTC"
    run_local_hour: int = 9
    cron_utc_offsets: tuple = (8, 9)
    # Google News supplement config (insurance against flaky official feeds)
    gnews_authority: str = ""        # display name in queries, e.g. "FSA" / "FDA"
    gnews_terms: tuple = ()          # pathogen/hazard terms for evergreen queries
    gnews_hl: str = "en-US"
    gnews_gl: str = "US"
    gnews_ceid: str = "US:en"
    gnews_days_back: int = 3
    # Country-scope filter for GNews results — prevents cross-border bleed
    # (e.g. US recalls surfacing in the AU collector's output). If non-empty,
    # an article is kept only if its title contains one of the keywords OR
    # its URL contains one of the domain suffixes. Empty = no filter (legacy
    # sources keep their permissive behaviour).
    gnews_country_keywords: tuple = ()
    gnews_country_domains: tuple = ()
    # Title-keyword denylist — articles whose title contains any of these
    # are dropped unconditionally, even if they passed the country-scope
    # filter. Use for unambiguous foreign-country signals (e.g. "FDA",
    # "Walmart" on an AU collector — that's a US recall syndicated by an
    # AU news outlet, not an AU recall).
    gnews_block_title_keywords: tuple = ()
    # If True, the GNews hazard text is "<title> || <description>" instead of
    # title alone. Useful for sources whose headlines often omit the pathogen
    # name but whose article snippets contain it ("X chocolates recalled" in
    # headline, "due to possible Salmonella" in description). Default False —
    # legacy sources (UK, IE, US FDA/FSIS, CA, AU, NZ) keep title-only
    # behaviour unchanged.
    gnews_use_description: bool = False
    # EFET-style approach for regulators whose own websites block GitHub
    # Actions IPs (Philippines FDA, Korea MFDS, Vietnam VFA, Japan CAA).
    # Each alias is fed into the query builder as if it were an additional
    # authority — so a single source can search for "Philippines recall
    # salmonella" AND "FDA Philippines recall salmonella" AND "Philippine
    # FDA recall salmonella", surfacing articles that name the regulator
    # explicitly (which we treat as government-verified per EFET pattern).
    # Default empty — no change for existing sources.
    gnews_authority_aliases: tuple = ()
    # EFET URL-resolution config — port of the gap_finder pattern. When set,
    # any GNews-surfaced record gets a post-classification step: fetch the
    # news article, scan for hrefs to authority_domain, return the first
    # matching authority URL as rec.url. This puts the REGULATOR'S OWN
    # PAGE in Pending — not a news outlet URL.
    # authority_domain: substring to find in news article hrefs (e.g.
    # "fda.gov", "cfs.gov.hk", "anvisa.gov.br"). Empty = no resolution
    # (legacy behaviour — news URL stored as-is).
    authority_domain: str = ""
    # authority_url_pattern: regex applied to path+query of authority-domain
    # URLs. Drops nav/portal pages, keeps individual recall pages. Example
    # for FDA: r"safety/recalls-market-withdrawals-safety-alerts/[^/]+"
    # Empty = no filter (any URL on authority_domain passes).
    authority_url_pattern: str = ""
    # EFET-style URL resolution — same architecture as the gap_finder
    # pipeline that has worked for EFET, ASAE, IT, ES, DE etc. for months.
    # 5 broad DDG queries built ONCE per source run (not per record),
    # results indexed in memory, news titles matched against the index by
    # Jaccard token overlap. WAF-immune: we never touch the regulator's
    # own site, only the search engine's snapshot.
    # Example:
    #   bulk_index_queries=(
    #       "site:fda.gov recalls 2026 salmonella",
    #       "site:fda.gov recalls 2026 listeria",
    #       "site:fda.gov recalls market withdrawals 2026",
    #       "site:fda.gov press release recall 2026",
    #       "site:fda.gov recalls market withdrawals safety alerts",
    #   )
    bulk_index_queries: tuple = ()


_REGISTRY: dict[str, FeedSource] = {}


def register(src: FeedSource) -> None:
    _REGISTRY[src.code.lower()] = src


def get(code: str) -> FeedSource:
    code = code.lower()
    if code not in _REGISTRY:
        from .sources import (uk, scotland, ireland,  # noqa: F401
                              us_fda, us_fsis, canada,
                              australia, new_zealand,
                              singapore, hong_kong, taiwan,
                              south_korea, japan,
                              philippines, indonesia,
                              thailand, vietnam,
                              brazil, mexico, argentina,
                              chile, colombia, peru)
    if code not in _REGISTRY:
        raise KeyError(f"Unknown feed source {code!r}. Registered: {sorted(_REGISTRY)}")
    return _REGISTRY[code]


def all_codes() -> list:
    if not _REGISTRY:
        from .sources import (uk, scotland, ireland,  # noqa: F401
                              us_fda, us_fsis, canada,
                              australia, new_zealand,
                              singapore, hong_kong, taiwan,
                              south_korea, japan,
                              philippines, indonesia,
                              thailand, vietnam,
                              brazil, mexico, argentina,
                              chile, colombia, peru)
    return sorted(_REGISTRY.keys())
