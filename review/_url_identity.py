"""
URL identity — canonical-URL rules, soft-404 detection, and content-based
dedup keys for regulators whose URLs carry no stable alert identifier.

WHY THIS MODULE EXISTS (root-cause, verified 2026-07-09)
─────────────────────────────────────────────────────────
The Ireland (FSAI) duplication bug had four coinciding causes. Three of them
are properties of fsai.ie that no amount of prompt-tuning can fix, so they are
encoded here as data:

  1. FSAI publishes no RSS/JSON feed, so the URL slug IS the alert identifier.
     Every other regulator embeds a stable id (FSA-PRIN-33-2026,
     notification/856435, fiche-rappel/22757).

  2. The FSAI CMS hard-caps the slug at 50 characters, and its slug generator
     is off-by-one: the live alert
         .../food-alerts/ecall-of-specific-batches-of-horgans-and-m-s-truff
     is title_slug[1:51] — the leading "R" of "Recall" is dropped. The
     canonical slug is therefore NOT derivable from the alert title. Any
     agent that reconstructs a "clean" full-length URL produces a wrong one.

  3. The retired /news_centre/ URL tree catch-all REDIRECTS to a live 200
     page (verified: /news_centre/food_alerts.html?page=2 -> /news?page=2).
     A wrong fsai.ie URL therefore never returns 404, so an HTTP liveness
     probe can never mark it dead.

  4. Every FSAI page renders a global "Food Alerts" sidebar listing the most
     recent alerts. A content-verifying reviewer that lands on the redirect
     target sees the alert title and date on the page and "confirms" it.
     (Both fabricated Horgans rows carried a passing reviewer verdict whose
     note read "date 2026-07-06 matches page listing".)

Net effect: a fabricated FSAI URL resolves, verifies, and — because the dedup
key is the URL string — lands as a brand-new row on every gap-finder pass.

DESIGN CONSTRAINTS
──────────────────
  • Pure functions. No network, no I/O, no side effects. Safe to import
    anywhere in the pipeline.
  • Host-scoped. Nothing here changes behaviour for a host that is not
    explicitly registered. Adding a rule for one regulator must never alter
    another regulator's outcome.
  • Conservative. Every reject rule below is backed by an observed live
    example; none is a general heuristic that could nuke a good row.
"""
from __future__ import annotations

import re
import unicodedata
import urllib.parse
from typing import Any, Dict, Optional

# ─────────────────────────────────────────────────────────────────────────
# Host registry
# ─────────────────────────────────────────────────────────────────────────
# Regulators whose URLs contain NO stable alert identifier. For these hosts
# the dedup key must be derived from row CONTENT, never from the URL string,
# because two URLs can address the same alert.
#
# Keep this list minimal and evidence-backed. Adding a host here changes how
# its rows are deduplicated.
NO_STABLE_ID_HOSTS = (
    "fsai.ie",
)

# Paths that are LISTING / INDEX pages, per host. If a request for a
# detail-shaped URL ends up (after redirects) at one of these paths, the
# original URL did not address a real alert page: it is a soft 404.
#
# Matching is prefix-based on the lowercased path.
LISTING_PATHS: Dict[str, tuple] = {
    "fsai.ie": (
        "/news",              # covers /news and /news?page=N (redirect target)
        "/news-alerts",
        "/news-and-alerts",   # bare section index (no trailing slug)
        "/home",
    ),
    # NB: /news_centre is deliberately NOT listed. It is a *requested* legacy
    # path, never a redirect target. Listing it here would make is_soft_404()
    # treat the bad URL as "already a listing" and exclude it from detection.
}

# FSAI canonical detail-page shape.
#   • Section is /news-and-alerts/<something>-alerts/
#   • Slug is a single path segment of at most 50 characters
# Verified against 10/10 live hrefs on fsai.ie/news (2026-07-09): the longest
# observed slug is exactly 50 chars; none exceeds it.
FSAI_SLUG_MAX = 50
_FSAI_DETAIL_RE = re.compile(
    r"^/news-and-alerts/[a-z-]+-alerts/(?P<slug>[^/]{1,%d})/?$" % FSAI_SLUG_MAX,
    re.IGNORECASE,
)
# Retired URL trees. These 302 to a live listing page, so they look healthy to
# an HTTP probe. They are never canonical.
_FSAI_LEGACY_RE = re.compile(r"^/news[_-]centre/", re.IGNORECASE)


# ─────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────
def _host(url: str) -> str:
    """Lowercased hostname with a leading 'www.' removed.

    NOTE: str.lstrip("www.") strips a CHARACTER SET, not a prefix — it would
    turn 'webgate.ec.europa.eu' into 'ebgate.ec.europa.eu'. Use an explicit
    prefix strip.
    """
    try:
        h = (urllib.parse.urlparse(url).netloc or "").lower()
    except Exception:
        return ""
    h = h.split("@")[-1].split(":")[0]
    if h.startswith("www."):
        h = h[4:]
    return h


def _path(url: str) -> str:
    try:
        return urllib.parse.urlparse(url).path or "/"
    except Exception:
        return "/"


def _host_matches(host: str, registered: str) -> bool:
    return host == registered or host.endswith("." + registered)


def _registered_host(url: str, registry) -> Optional[str]:
    """Return the registry key matching this URL's host, else None."""
    h = _host(url)
    if not h:
        return None
    keys = registry if isinstance(registry, (tuple, list)) else registry.keys()
    for key in keys:
        if _host_matches(h, key):
            return key
    return None


def has_stable_id(url: str) -> bool:
    """False when the host encodes no stable alert id in its URLs."""
    return _registered_host(url, NO_STABLE_ID_HOSTS) is None


# ─────────────────────────────────────────────────────────────────────────
# Soft-404 detection
# ─────────────────────────────────────────────────────────────────────────
def is_listing_path(url: str) -> bool:
    """True if `url` points at a known listing/index page for its host."""
    key = _registered_host(url, LISTING_PATHS)
    if not key:
        return False
    p = _path(url).rstrip("/").lower() or "/"
    for prefix in LISTING_PATHS[key]:
        if p == prefix or p.startswith(prefix + "/") or p.startswith(prefix + "?"):
            return True
    return False


def is_soft_404(requested_url: str, final_url: str) -> bool:
    """
    True when a request for a detail page silently landed on a listing page.

    This is the check that a plain HTTP status probe cannot make: fsai.ie
    redirects retired/unknown alert URLs to /news with HTTP 200, so the row
    survives every liveness gate while pointing at the wrong page.

    Deliberately narrow — ALL of the following must hold:
      • host is registered in LISTING_PATHS (unknown hosts are never touched)
      • the requested URL was NOT itself a listing page (we don't punish a
        row that legitimately links to an index)
      • the final URL, after redirects, IS a listing page
      • host did not change (an off-site redirect is a different problem)
    """
    if not requested_url or not final_url:
        return False
    if _registered_host(requested_url, LISTING_PATHS) is None:
        return False
    if _host(requested_url) != _host(final_url):
        return False
    if is_listing_path(requested_url):
        return False
    return is_listing_path(final_url)


# ─────────────────────────────────────────────────────────────────────────
# FSAI canonical-URL rules
# ─────────────────────────────────────────────────────────────────────────
def is_fsai_url(url: str) -> bool:
    return _host_matches(_host(url), "fsai.ie")


def is_fsai_canonical(url: str) -> bool:
    """True if `url` has the exact shape FSAI publishes for an alert page."""
    if not is_fsai_url(url):
        return False
    return bool(_FSAI_DETAIL_RE.match(_path(url)))


def fsai_url_problem(url: str) -> Optional[str]:
    """
    Return a short reason string if this fsai.ie URL cannot be canonical,
    else None.

    Only three conditions are rejected, each backed by a live observation:

      • retired /news_centre/ or /news-centre/ tree  -> 302s to a listing
      • a .html suffix                               -> retired CMS only
      • an alert slug longer than 50 characters      -> CMS caps at 50

    Anything else on fsai.ie is left alone. In particular a slug that looks
    "cut off mid-word" (…cooked-ham-p) is CORRECT and must never be repaired:
    that is what the agency actually publishes.
    """
    if not is_fsai_url(url):
        return None
    p = _path(url)
    if _FSAI_LEGACY_RE.match(p):
        return "retired /news_centre/ tree (redirects to listing; never canonical)"
    if p.lower().endswith(".html"):
        return "retired .html CMS path"
    m = re.match(r"^/news-and-alerts/[a-z-]+-alerts/(?P<slug>[^/]+)/?$", p, re.IGNORECASE)
    if m and len(m.group("slug")) > FSAI_SLUG_MAX:
        return f"slug {len(m.group('slug'))} chars > FSAI cap of {FSAI_SLUG_MAX} (reconstructed, not published)"
    return None


# ─────────────────────────────────────────────────────────────────────────
# Dedup keys
# ─────────────────────────────────────────────────────────────────────────
_UPDATE_RE = re.compile(r"\bupdate\b|\bupdated\b", re.IGNORECASE)


def _ascii_tokens(value: str, limit: int = 0) -> str:
    """Stable, order-independent company fingerprint.

    All significant tokens are used. An earlier draft truncated to the first
    4 sorted tokens, which made 'Brady Family; Deluxe; Tesco Finest' and
    'Brady Family / Deluxe / Tesco' hash differently — a truncation artifact,
    not a real distinction. `limit=0` means no truncation.

    LIMITATION (by design): this is an EXACT key, so it cannot merge rows whose
    operator is named differently, e.g. 'Butchers Selection' vs
    'Dunnes Stores (Butchers Selection)'. Content dedup is defence-in-depth
    only. The primary defence is fsai_url_problem(), which deletes the
    reconstructed URL at the Pending gate before it can ever reach Recalls.
    """
    s = unicodedata.normalize("NFD", str(value or "")).encode("ascii", "ignore").decode()
    s = s.lower()
    stop = {
        "ltd", "limited", "the", "and", "company", "co", "plc", "inc", "llc",
        "sa", "bv", "gmbh", "srl", "stores", "store", "food", "foods", "group",
    }
    toks = sorted({t for t in re.findall(r"[a-z0-9]{3,}", s) if t not in stop})
    if limit:
        toks = toks[:limit]
    return "-".join(toks)[:80]


def _is_update(row: Dict[str, Any]) -> bool:
    """
    Distinguish 'Update to recall of X' from the original recall of X.
    Without this, an update issued on the same date as its original would
    collapse into it.
    """
    blob = " ".join(str(row.get(k) or "") for k in ("Product", "Notes", "URL", "Class"))
    return bool(_UPDATE_RE.search(blob))


def content_key(row: Dict[str, Any]) -> str:
    """
    Identity of a recall derived from its content rather than its URL.

    Uses only fields that a scraper and a search agent must agree on:
    the date, the pathogen, and the operator. Free-text fields (Product,
    Brand, Class) are deliberately excluded — agent-written rows paraphrase
    them, so they never match the scraped row.
    """
    date = str(row.get("Date") or "")[:10]
    pathogen = str(row.get("Pathogen") or "").strip().lower()[:30]
    company = _ascii_tokens(row.get("Company"))
    suffix = "|upd" if _is_update(row) else ""
    return f"content|{date}|{company}|{pathogen}{suffix}"


def dedup_key(row: Dict[str, Any]) -> str:
    """
    Canonical dedup key for a recall row.

    Behaviour by host:
      • Host with a stable alert id in the URL (the default, i.e. every host
        NOT in NO_STABLE_ID_HOSTS): key on the normalized URL, exactly as
        before. Unchanged semantics.
      • Host in NO_STABLE_ID_HOSTS (fsai.ie): key on CONTENT. Two URLs can
        address the same alert, so the URL string cannot be the identity.
      • No URL at all: fall back to content, as before.

    The caller supplies `normalize_url` behaviour via `normalized_url`; this
    keeps merge_master's existing normalization the single source of truth
    for protocol/www/query handling.
    """
    url = str(row.get("URL") or "").strip()
    if url and has_stable_id(url):
        return ""  # signal: caller keeps its existing URL-based key
    return content_key(row)


# ─────────────────────────────────────────────────────────────────────────
# Row preference (which duplicate survives)
# ─────────────────────────────────────────────────────────────────────────
def row_rank(row: Dict[str, Any]) -> tuple:
    """
    Sort key: LOWER is better. Use to pick the survivor among duplicates.

    Priority:
      1. Canonical URL beats non-canonical.
      2. Official-feed row beats an agent-discovered row. The feed reads the
         agency's own anchor href verbatim, so it is ground truth; an agent
         reconstructs the URL and, for FSAI, provably cannot get it right.
      3. Earlier DateAdded wins (the original, not the re-discovery).
    """
    url = str(row.get("URL") or "")
    notes = str(row.get("Notes") or "").lower()
    canonical = 0 if (is_fsai_canonical(url) or has_stable_id(url)) else 1
    from_feed = 0 if "official-feed" in notes else 1
    added = str(row.get("DateAdded") or "9999")[:10]
    return (canonical, from_feed, added)


__all__ = [
    "NO_STABLE_ID_HOSTS",
    "LISTING_PATHS",
    "FSAI_SLUG_MAX",
    "has_stable_id",
    "is_listing_path",
    "is_soft_404",
    "is_fsai_url",
    "is_fsai_canonical",
    "fsai_url_problem",
    "content_key",
    "dedup_key",
    "row_rank",
]
