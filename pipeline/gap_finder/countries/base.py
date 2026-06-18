"""
AFTS Food Safety Intelligence — Gap Finder
Country configuration base class.

Each country's gap finder is described by a single CountryConfig instance
(in countries/<cc>.py). All pipeline modules (news_scraper, search_verifier,
extractor, main) read from CountryConfig instead of having country-specific
logic hardcoded.

This is the contract that lets us ship Italy/Spain/Portugal/Switzerland/
Belgium/Germany/Austria/Netherlands by writing one ~80-line config file each.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class RssSource:
    """One RSS feed source: a domain + list of candidate feed URLs (try in order)."""
    domain: str
    feeds: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class CountryConfig:
    # ── Identity ────────────────────────────────────────────────────────────
    code: str                       # ISO2 lowercase: "gr", "it", "es"
    name_en: str                    # "Greece", "Italy" — for English xlsx Country col
    name_local: str                 # "Ελλάδα", "Italia" — for logs/prompts

    # ── Food safety authority ───────────────────────────────────────────────
    authority_short: str            # "EFET", "Salute", "AESAN"
    authority_full: str             # "Ενιαίος Φορέας Ελέγχου Τροφίμων" / "Ministero della Salute"
    authority_domain: str           # "efet.gr", "salute.gov.it"
    # Regex (within netloc+path) identifying a REAL recall item (filters portal/category pages)
    authority_item_url_regex: str   # e.g. r"anakleiseis-cat/item/\d+" for EFET

    # ── News sources ────────────────────────────────────────────────────────
    rss_sources: list[RssSource]            # tried first (often broken; we tolerate failures)
    google_news_domains: list[str]          # used for site:-restricted Google News queries
    google_news_keywords: list[str]         # search terms used per domain via Google News

    # ── Search-engine bulk index (EFET WAF bypass pattern) ─────────────────
    # 4-6 broad DDG queries that surface the authority's recent recall list.
    # site: operator restricts to authority_domain.
    bulk_index_queries: list[str]

    # ── LLM extraction prompt context ───────────────────────────────────────
    language_name: str              # "Greek", "Italian" — used in LLM system prompt
    language_code: str              # "el", "it" — used in DDG kl= parameter
    # Localized hint about how brand names are usually written
    brand_handling_note: str        # e.g. "Keep Greek-only brands in Greek..."

    # ── Title-relevance prefilter (drops 'πολιτικός' news etc.) ──────────────
    # Lowercase normalized substrings; news titles must contain at least one
    # of these to survive into the candidate set.
    recall_signal_terms: list[str]

    # ── Scheduling (per AFTS rule — local time, EVER UTC) ───────────────────
    timezone: str                   # "Europe/Athens", "Europe/Rome"
    run_local_hour: int             # 21 (run at 21:00 local time)
    # Two UTC cron lines — one per DST offset. Generated at workflow-build time.
    # Provided here so the workflow YAML can reference them by name.
    cron_utc_offsets: tuple[int, int]   # e.g. (18, 19) for Athens (EEST, EET)

    # ── Authority recall-index URL (optional) ───────────────────────────────
    # The authority's own page that LISTS recent recall press releases. Used
    # as the Tier-2 resolver: fetch this index (curl_cffi clears the Joomla
    # 409), parse the item/<num>-<slug> links, and match the recall by its
    # product keywords. Empty string disables Tier-2 for that country.
    authority_index_url: str = ""

    # ── Aggregator front-end (optional) ─────────────────────────────────────
    # Some countries have no clean authority index, but a reliable third-party
    # aggregator mirrors the authority with per-item pages that each LINK to the
    # official authority report (e.g. Poland: oalert.pl → gov.pl/web/gis/...).
    # When set, the Tier-2 resolver fetches authority_index_url on this
    # index_domain, matches the recall there, then opens the matched item and
    # extracts the link back to authority_domain — so the Pending record still
    # points at the official authority URL (authority-pure), while matching
    # benefits from the aggregator's clean, structured rows.
    #   index_domain         : host of the aggregator index/items (e.g. "oalert.pl")
    #   index_item_url_regex : path pattern of the aggregator's per-item pages
    # Leave empty to disable (normal direct-authority Tier-2).
    index_domain: str = ""
    index_item_url_regex: str = ""

    # ── Output paths (per-country to avoid collisions) ──────────────────────
    @property
    def data_dir(self) -> str:
        return f"docs/data/gap_finder_{self.code}"

    @property
    def candidates_path(self) -> str:
        return f"{self.data_dir}/candidates.jsonl"

    @property
    def verified_path(self) -> str:
        return f"{self.data_dir}/verified.jsonl"

    @property
    def unmatched_path(self) -> str:
        return f"{self.data_dir}/unmatched.jsonl"

    @property
    def index_path(self) -> str:
        return f"{self.data_dir}/{self.authority_short.lower()}_index.jsonl"

    @property
    def pending_path(self) -> str:
        return f"{self.data_dir}/pending_candidates.jsonl"

    @property
    def rejected_path(self) -> str:
        return f"{self.data_dir}/rejected_records.jsonl"

    @property
    def run_log_path(self) -> str:
        return f"{self.data_dir}/run_log.jsonl"


# Registry — populated by countries/gr.py, countries/it.py, etc. on import
_REGISTRY: dict[str, CountryConfig] = {}


def register(cfg: CountryConfig) -> CountryConfig:
    """Each country module calls this at import to make itself loadable by code."""
    _REGISTRY[cfg.code] = cfg
    return cfg


def get(code: str) -> CountryConfig:
    """Look up a country config by ISO2 code. Raises KeyError if not registered."""
    code = code.lower()
    if code not in _REGISTRY:
        # Lazy-import all countries to populate registry.
        # Note: Iceland's ISO2 code is 'is' but Python 'is' is reserved,
        # so the module file is named iceland.py (still registers with code='is').
        from . import (gr, it, es, pt,
                       de, at, ch, be, nl, lu, pl, hu,
                       se, no, dk, fi, iceland,
                       cz, hr, ee, mk, md, ba)  # noqa: F401
    if code not in _REGISTRY:
        raise KeyError(
            f"Unknown country code {code!r}. "
            f"Registered: {sorted(_REGISTRY.keys())}"
        )
    return _REGISTRY[code]


def all_codes() -> list[str]:
    return sorted(_REGISTRY.keys())
