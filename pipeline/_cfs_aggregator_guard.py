"""HK CFS "Food Incident Post" aggregator guard — added 2026-07-14.

The Hong Kong Centre for Food Safety (cfs.gov.hk) publishes "Food Incident
Post" PDFs that RE-POST recalls issued by other jurisdictions' regulators
(France RappelConso, US FDA, UK FSA, NZ MPI, AU FSANZ, SG SFA, ...). When
the underlying recall is a *foreign* one, the CFS post is a cross-source
DUPLICATE of the upstream regulator's recall — which FSIS already ingests
directly. These duplicates were surfacing in alert emails (e.g. AOSTE ham
FR, Emborg Emmentaler NZ, Drayton Harbor oysters US) all sourced from
cfs.gov.hk with a non-Hong-Kong Country.

CFS is, however, the SOLE/primary source when the recall's origin IS Hong
Kong (local HK recalls). Those must be kept.

Rule: reject an incoming cfs.gov.hk row UNLESS its Country/origin is Hong
Kong. This is intentionally conditional (unlike the flat news-mirror
blocklist) so genuine HK-origin CFS rows still ingest.

Operator instruction (2026-07-14, verbatim intent): "Block that web site
from arriving in repo if not the product origin of ... Hong Kong."
"""
from __future__ import annotations

import re
import unicodedata

# Host(s) that publish the CFS aggregator PDFs.
CFS_HOSTS = ("cfs.gov.hk",)

# Accepted spellings/encodings of Hong Kong as an origin country. Anything
# NOT in this set (for a cfs.gov.hk URL) is treated as a foreign re-post.
_HONG_KONG_ALIASES = {
    "hongkong",
    "hongkongsar",
    "hongkongchina",
    "hk",
    "hksar",
    "chinahongkong",
    "chinahongkongsar",
}


def _norm_country(country: str) -> str:
    """Fold accents/case/punctuation for robust country comparison."""
    if not country:
        return ""
    s = unicodedata.normalize("NFD", str(country)).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z]", "", s.lower())


def is_cfs_host(url: str) -> bool:
    """True if the URL is a CFS aggregator host."""
    if not url:
        return False
    u = str(url).lower()
    return any(h in u for h in CFS_HOSTS)


def is_hong_kong_origin(country: str) -> bool:
    """True if the Country field denotes Hong Kong (any common spelling)."""
    return _norm_country(country) in _HONG_KONG_ALIASES


def is_foreign_cfs_repost(url: str, country: str) -> bool:
    """True if this is a cfs.gov.hk row whose origin country is NOT Hong Kong.

    These are cross-source duplicates of an upstream regulator's recall and
    must be blocked at ingest. HK-origin CFS rows return False (kept).
    """
    if not is_cfs_host(url):
        return False
    return not is_hong_kong_origin(country)
