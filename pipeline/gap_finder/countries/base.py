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
import sys as _sys
from pathlib import Path
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
    # Regex identifying a REAL recall item, filtering out portal/category pages.
    #
    # MATCHED AGAINST TWO DIFFERENT STRINGS — write it for both:
    #   * the full URL, in authority_url_finder and extractor
    #   * "path?query" WITH THE NETLOC STRIPPED, in search_verifier's
    #     bulk-index filter
    # so a regex that names the host matches at the first two sites and
    # silently fails at the third, dropping every bulk-index hit as a portal
    # page. (Corrected 2026-09-23; this comment used to read "within
    # netloc+path", which is how br/hk/mx/hu came to name their hosts.)
    #
    # Scope by PATH, and prefix with the host-optional group when the host is
    # unavoidable:
    #   r"anakleiseis-cat/item/\d+"                          (EFET — path only)
    #   r"^(?:https?://[^/]+)?/cofepris/(?:articulos|prensa)/[a-z0-9\-]+"
    # The query string IS included where present, so authorities that carry
    # the recall id as a parameter can require it:
    #   r"newsContent\.aspx\?(?:.*&)?id=[a-z]?\d+"           (Taiwan TFDA)
    # Host-level filtering is done separately, against authority_domain.
    authority_item_url_regex: str

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

    # ── Additional accepted authority domains (optional) ────────────────────
    # Most countries have a single authority host. A few legitimately publish
    # recalls across more than one official domain — e.g. Czechia: the SZPI
    # regulator site (szpi.gov.cz, per-recall /clanek/ and /en/article/ pages)
    # AND its public "food pillory" database (potravinynapranyri.cz,
    # /Detail.aspx?id=N). A recall URL on ANY of these is authority-valid.
    # authority_domain stays the PRIMARY; these are accepted in addition for the
    # gate, the article-HTML scan, and the "candidate URL is itself authority"
    # Tier-0 check. Each entry is a bare host ("potravinynapranyri.cz").
    authority_domains_extra: list[str] = field(default_factory=list)

    # ── News-authority mode (portal-less countries) ─────────────────────────
    # DEFAULT FALSE — every country with a real per-recall authority portal
    # (GR, ZA, NG, GH, all EU) MUST keep this False so the authority-URL gate
    # stays absolute (no news URLs ever enter Recalls; multi-outlet duplicates
    # are blocked at the gate).
    #
    # A few countries have NO per-recall authority portal at all: the regulator
    # issues recalls only as press statements / letters / Arabic SharePoint
    # pages with no linkable per-recall slug (e.g. Egypt NFSA, Kenya KEBS,
    # Zambia ZNPHI). For those — and ONLY those — set this True. The gate then
    # accepts the NEWS article URL as the record URL when no authority URL
    # exists, but STRICTLY: (a) the outlet must be on this country's curated
    # google_news_domains whitelist, and (b) the record must still classify at
    # tier 1 or 2. Cross-outlet duplicates collapse downstream via
    # dedupe_by_recall_identity (company + pathogen), so two outlets covering
    # one recall fold into a single row. This is a deliberate, scoped
    # relaxation of the authority-pure guarantee for portal-less countries.
    news_authority_mode: bool = False

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
        _import_all_countries()
    if code not in _REGISTRY:
        raise KeyError(
            f"Unknown country code {code!r}. "
            f"Registered: {sorted(_REGISTRY.keys())}"
        )
    return _REGISTRY[code]


def _import_all_countries() -> None:
    """Import every country module in this package, so each registers.

    DISCOVERED, NOT LISTED (audit 2026-09-23)
    -----------------------------------------
    This used to be a hand-written import tuple naming all 28 modules. A
    new country config was therefore invisible until someone remembered to
    add it in a second place — and the failure was silent, because `get()`
    only raises for the code you asked about, so 27 countries kept working
    while the 28th did not exist.

    That is the same shape as every other outage found this week: a
    hand-maintained list beside the thing it is supposed to describe. The
    gap-finder fleet already reads this registry rather than a list; the
    registry now reads the directory rather than a list.

    Iceland is why the naive version of this needs care: its ISO2 code is
    "is", a Python keyword, so its module is iceland.py and registers
    itself with code="is". Walking the directory handles that without
    anyone having to know it.
    """
    import importlib
    import pkgutil

    # This directory, not the package's __path__. The first version of this
    # read `__path__ if "__path__" in dir() else [str(Path(__file__).parent)]`,
    # which was wrong twice over: dir() inside a function returns LOCAL names,
    # never module globals, so the test was always False and the fallback
    # always taken. It worked, by accident, because the fallback is the right
    # answer. pyflakes caught it as an undefined name (tests/
    # test_no_undefined_names.py) — the guard was hiding a bug rather than
    # preventing one. The directory is what we actually mean here, so say so.
    here = [str(Path(__file__).parent)]

    for _, modname, _ in pkgutil.iter_modules(here):
        if modname.startswith("_") or modname == "base":
            continue
        try:
            importlib.import_module(f"{__package__}.{modname}")
        except Exception as exc:                             # noqa: BLE001
            # One broken config must not take the other 27 down with it.
            # Loud, because a country that fails to import is a country
            # that silently stops being covered.
            print(f"[countries] WARNING: {modname} did not import: "
                  f"{type(exc).__name__}: {exc}", file=_sys.stderr)


def all_codes() -> list[str]:
    """Every registered code. Triggers discovery if nothing is loaded yet."""
    if not _REGISTRY:
        _import_all_countries()
    return sorted(_REGISTRY.keys())
