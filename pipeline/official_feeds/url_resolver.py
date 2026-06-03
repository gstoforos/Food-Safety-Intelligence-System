"""
URL resolver — reuses the proven gap_finder.search_verifier (EFET-style).

How it works (the SAME way EFET, ASAE, Italian, German etc. have worked
in production for months):

  1. ONCE per source run, fire 5 broad DDG queries like:
        site:fda.gov recalls 2026 salmonella
        site:fda.gov recalls market withdrawals 2026
        ...
     7-11s delays between queries (DDG-tolerant), Mojeek fallback.

  2. Build in-memory index of authority URLs from results, filtered by
     authority_item_url_regex.

  3. For each accepted GNews record, match its title against the index
     by Jaccard token overlap + date-window constraint.

  4. Best match above MATCH_THRESHOLD (10%) → return authority URL.
     Otherwise None → caller keeps news URL + sets pending_no_auth_url.

Network budget per source run: 5 DDG calls (~45-55 seconds with delays).
Cost: $0 — no API keys, no signups. Same code path as gap_finder.

NOTE: This wraps the existing pipeline.gap_finder.search_verifier so we
don't duplicate the DDG/Mojeek logic, polite-sleep tuning, Jaccard
matching, or stopword tables. ALL the hard work lives there already.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

# Import the proven gap_finder primitives directly. The shape we need:
#   - CountryConfig-like object with bulk_index_queries + authority_domain +
#     authority_item_url_regex
#   - build_index(cfg) returns list[AuthorityAnnouncement]
#   - match_in_index(news_title, news_published_iso, index) returns (ann, score)
try:
    from ..gap_finder.search_verifier import (
        build_index, match_in_index, AuthorityAnnouncement)
except ImportError:
    # Fallback when run as a module from a different layout
    from pipeline.gap_finder.search_verifier import (   # type: ignore
        build_index, match_in_index, AuthorityAnnouncement)


# Process-level cache so multiple records in one run share one index build.
# Key: tuple(bulk_index_queries). Value: list[AuthorityAnnouncement].
_INDEX_CACHE: dict = {}


@dataclass
class _ResolverCfg:
    """Minimal duck-typed shim that quacks like CountryConfig for the
    gap_finder search_verifier. Includes every cfg attribute that
    search_verifier.py touches (verified via grep on the upstream module)."""
    # Used by build_index() + ddg_search()
    authority_short: str
    authority_domain: str
    authority_item_url_regex: str
    bulk_index_queries: list
    # Used by _ddg_headers() for Accept-Language and logging
    code: str = "us"
    name_en: str = ""
    language_code: str = "en"
    # I/O paths — only used by search_verifier's CLI mode (not from our
    # in-process call) but referenced via getattr in some places, so we
    # provide empty strings rather than risk AttributeError.
    candidates_path: str = ""
    unmatched_path: str = ""
    verified_path: str = ""


def _get_index(authority_short: str,
               authority_domain: str,
               item_url_regex: str,
               bulk_index_queries: tuple,
               verbose: bool = True) -> list:
    """Lazily build (and cache) the authority's URL index via gap_finder.
    The build is shared across all records in one source run."""
    key = tuple(bulk_index_queries)
    if key in _INDEX_CACHE:
        return _INDEX_CACHE[key]

    cfg = _ResolverCfg(
        authority_short=authority_short,
        authority_domain=authority_domain,
        authority_item_url_regex=item_url_regex,
        bulk_index_queries=list(bulk_index_queries),
        # Defaults for the optional fields are fine — we're not using the
        # candidates/unmatched/verified file I/O paths from our in-process call.
        code=authority_short.lower()[:2] or "us",
        name_en=authority_short,
        language_code="en",
    )
    try:
        index = build_index(cfg, verbose=verbose)
    except Exception as e:   # noqa: BLE001
        print(f"  [WARN] search_verifier.build_index failed: {e}")
        index = []
    _INDEX_CACHE[key] = index
    print(f"  [authority index] {authority_short}: {len(index)} URLs indexed")
    return index


def resolve_authority_url(news_url: str,
                          title: str,
                          authority_domain: str,
                          authority_url_pattern: Optional[re.Pattern] = None,
                          bulk_index_queries: tuple = (),
                          authority_short: str = "",
                          published_iso: str = "",
                          ) -> Optional[str]:
    """
    Find the regulator's URL for a recall via the EFET-style index match.

    Args:
        news_url:               GNews redirector (unused in this method)
        title:                  GNews article title — drives the match
        authority_domain:       e.g. "fda.gov", "cfs.gov.hk"
        authority_url_pattern:  compiled regex (used as item_url_regex for
                                 the index filter)
        bulk_index_queries:     5 broad DDG queries to build the index. If
                                 empty, returns None (no fallback — we want
                                 deterministic behaviour).
        authority_short:        for log readability, e.g. "FDA", "CFS"
        published_iso:          ISO-8601 date string for the article — used
                                 for the 30-day date-window constraint in
                                 the match function

    Returns:
        Authority URL string, or None.
    """
    if not title or not authority_domain or not bulk_index_queries:
        return None

    item_url_regex = (authority_url_pattern.pattern
                       if authority_url_pattern else r".+")
    auth_short = authority_short or authority_domain.split(".")[0].upper()

    index = _get_index(auth_short, authority_domain, item_url_regex,
                        bulk_index_queries)
    if not index:
        return None

    ann, score = match_in_index(title, published_iso or "", index)
    if not ann:
        return None
    return ann.url
