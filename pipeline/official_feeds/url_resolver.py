"""
URL resolver — port of the EFET / gap_finder URL verification pattern to
official_feeds.

The problem this solves:
    GNews surfaces news articles ABOUT recalls (e.g. "FDA Announces Nationwide
    Recall on Frozen Pizza" from Yahoo News). The recall's authoritative URL
    lives on the regulator's site (fda.gov/safety/recalls-...). Without
    resolution, Pending would store the Yahoo URL — bad for B2B subscribers
    who expect to click through to the regulator.

How it works:
    1. Accept a news article URL (Google News redirector or publisher URL)
    2. Fetch the article HTML (follow redirects)
    3. Scan all <a href> links for one matching authority_domain
    4. Filter to URLs matching authority_url_pattern (drops nav/portal pages,
       keeps individual recall pages)
    5. Return the authority URL — or None if no match in article body

When None is returned, caller MUST fall back to the news URL and set
Status='pending_no_auth_url' so the operator can manually find the
regulator's link during review.

This works even when the regulator's website blocks GitHub Actions IPs
(PH/KR/VN/JP), because we never fetch the regulator — we only extract the
URL string from the news article's body. The subscriber's browser will
fetch it later from a different IP.
"""

from __future__ import annotations

import re
from typing import Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from .fetch import DEFAULT_HEADERS


_RESOLVE_TIMEOUT = 10           # seconds per article fetch
_MAX_RESOLVE_PER_RUN = 50       # safety cap (avoid runaway HTTP)


def resolve_authority_url(news_url: str,
                          authority_domain: str,
                          authority_url_pattern: Optional[re.Pattern] = None,
                          ) -> Optional[str]:
    """
    Fetch the news article at `news_url` and try to extract an authority URL
    from its body.

    Args:
        news_url: The Google News redirector or publisher article URL.
        authority_domain: Substring to look for in href attributes
            (e.g. "fda.gov", "cfs.gov.hk", "anvisa.gov.br"). Match is on
            the full URL, so subdomains and paths under the bare domain
            both work.
        authority_url_pattern: Optional compiled regex applied to the matched
            URL's path+query. If set, the URL must match the pattern to be
            returned — drops authority-domain nav pages (e.g. /home,
            /recalls listing page) and keeps individual recall pages
            (e.g. /safety/recalls-market-withdrawals-safety-alerts/<slug>).

    Returns:
        The authority URL string if a matching link was found in the
        article body, else None.
    """
    if not news_url or not authority_domain:
        return None
    try:
        resp = requests.get(news_url, headers=DEFAULT_HEADERS,
                            timeout=_RESOLVE_TIMEOUT, allow_redirects=True)
        if resp.status_code != 200:
            return None
    except Exception:   # noqa: BLE001  (network, timeout, SSL, etc.)
        return None

    soup = BeautifulSoup(resp.content, "html.parser")
    # Some news outlets put outbound links in <a>; some put them in
    # <link rel="canonical"> or JSON-LD. Scan all <a> first (most common),
    # then fall back to canonical/source attributions.
    candidates: list[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # Resolve relative URLs against the publisher base.
        full = urljoin(resp.url, href)
        if authority_domain in full:
            candidates.append(full)

    # Also check canonical and og:url (regulators sometimes link via
    # social-share meta when news outlets republish)
    for tag, attr in (("link", "href"),
                       ("meta", "content")):
        for el in soup.find_all(tag):
            v = el.get(attr, "")
            if v and authority_domain in v:
                candidates.append(v)

    # Dedup, preserve order
    seen: set[str] = set()
    deduped: list[str] = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            deduped.append(c)

    if not deduped:
        return None

    if authority_url_pattern is None:
        # No pattern — just return the first match on authority domain.
        return deduped[0]

    # Filter to URLs that match the pattern (path+query). This drops nav
    # pages like fda.gov/safety/recalls-market-withdrawals-safety-alerts
    # (the listing page) and keeps the individual recall page.
    for url in deduped:
        parsed = urlparse(url)
        path_q = f"{parsed.path}?{parsed.query}" if parsed.query else parsed.path
        if authority_url_pattern.search(path_q):
            return url

    # No URL matched the pattern. Don't return the listing/nav URL; let
    # the caller fall back to the news URL with a flag.
    return None
