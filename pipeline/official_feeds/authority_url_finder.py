"""
AFTS Gap Finder — authority-URL finder.

PURPOSE
-------
The news-based gap finder discovers recalls via news articles (Google News →
publisher page). But a recall record must point at the OFFICIAL regulator
press release (e.g. efet.gr), NOT the news outlet that reported it. Two news
outlets covering the same recall would otherwise create two Recalls rows with
two different news URLs — exactly the duplicate problem seen in production
(news247.gr + kathimerini.gr mirrors of one EFET notice).

This module scans a fetched news-article's HTML for a link to the authority's
official recall page, using the per-country `authority_domain` +
`authority_item_url_regex` already defined in the CountryConfig.

CONTRACT
--------
find_authority_url(html, cfg) -> str
    Returns the official authority recall URL if the article links to one,
    else "". The caller treats "" as "no official source found" and must NOT
    fall back to the news URL for a Recalls record.

WHY SCAN THE ARTICLE
--------------------
Greek (and most EU) food-recall news articles cite/link the regulator notice
directly — "Όπως ανακοίνωσε ο ΕΦΕΤ … (link to efet.gr)". Extracting that link
gives the canonical URL deterministically, with no LLM and no guessing.
"""

from __future__ import annotations

import re
from html import unescape
from urllib.parse import urljoin, urlparse

try:
    from .countries.base import CountryConfig
except ImportError:                                              # pragma: no cover
    from gap_finder.countries.base import CountryConfig          # type: ignore


def _all_links(html: str) -> list[str]:
    """Every href in the HTML, unescaped. Cheap regex scan (no bs4 dependency)."""
    if not html:
        return []
    hrefs = re.findall(r'href\s*=\s*["\']([^"\']+)["\']', html, flags=re.IGNORECASE)
    return [unescape(h) for h in hrefs]


def find_authority_url(html: str, cfg: CountryConfig) -> str:
    """Find the official authority recall URL referenced in a news article.

    Strategy (in priority order):
      1. A link whose host is the authority domain AND whose path matches the
         authority's item-URL regex (e.g. efet.gr/.../anakleiseis-cat/item/5396).
         This is the canonical per-recall press-release URL.
      2. Any link to the authority domain that looks like a recall page
         (path contains the recall section keyword), as a softer fallback.
    Returns "" if the article references no authority link.
    """
    domain = (cfg.authority_domain or "").lower().lstrip("www.")
    if not domain:
        return ""

    item_re = None
    if getattr(cfg, "authority_item_url_regex", ""):
        try:
            item_re = re.compile(cfg.authority_item_url_regex, re.IGNORECASE)
        except re.error:
            item_re = None

    links = _all_links(html)

    def _host_matches(u: str) -> bool:
        try:
            host = urlparse(u).netloc.lower().lstrip("www.")
        except Exception:
            return False
        return host == domain or host.endswith("." + domain)

    # Pass 1: authority-domain link matching the precise item-URL regex.
    if item_re is not None:
        for u in links:
            if _host_matches(u) and item_re.search(u):
                return _clean(u)

    # Pass 2: any authority-domain link that looks like a recall page.
    # Section hints cover the common regulator recall-section path words.
    section_hints = ("anakl", "recall", "rappel", "richiamo", "alerta",
                     "retirada", "deltia-typou", "warnung", "tilbakekalling",
                     "aterkallelse", "fiche-rappel")
    for u in links:
        if _host_matches(u) and any(h in u.lower() for h in section_hints):
            return _clean(u)

    # Pass 3: bare authority-domain link (last resort — better than a news URL,
    # but only if it's not the site root / generic homepage).
    for u in links:
        if _host_matches(u):
            path = urlparse(u).path.strip("/")
            if path and path not in ("index.php", "home", "el", "en"):
                return _clean(u)

    return ""


def _clean(u: str) -> str:
    """Normalize a found URL: strip tracking fragments, ensure scheme."""
    u = u.strip()
    # Drop fragment + common tracking query noise.
    u = u.split("#", 1)[0]
    if u.startswith("//"):
        u = "https:" + u
    return u



# ──────────────────────────────────────────────────────────────────────────
# Tier 2 — resolve the authority URL by searching the authority site directly
# ──────────────────────────────────────────────────────────────────────────
#
# Greek (and many EU) news outlets cite the regulator by NAME but do not
# hyperlink the press release, so scanning the article HTML (Tier 1) finds
# nothing. Tier 2 instead asks Google News for the authority's OWN page on
# this recall: `site:efet.gr <key terms> when:21d`. The first result that
# matches the authority item-URL pattern is the canonical press release.
#
# Uses gnews_resolver to decode the Google News redirect → real efet.gr URL.

import re as _re2
from urllib.parse import quote_plus as _quote_plus

try:
    from . import gnews_resolver as _gnr
    from .news_scraper import _http_get as _ns_http_get, parse_rss as _parse_rss
except ImportError:                                              # pragma: no cover
    from gap_finder import gnews_resolver as _gnr                # type: ignore
    from gap_finder.news_scraper import (                        # type: ignore
        _http_get as _ns_http_get, parse_rss as _parse_rss,
    )

_GN_BASE = "https://news.google.com/rss/search"

# Greek stopwords + outlet names to strip from the title before searching,
# so the query keeps only distinctive recall terms (product/company/brand).
_TITLE_NOISE = {
    "efet", "εφετ", "ο", "η", "το", "του", "της", "των", "και", "λογω",
    "λόγω", "ανακληση", "ανάκληση", "ανακαλει", "ανακαλεί", "ανακαλειται",
    "ανακαλείται", "προσοχη", "προσοχή", "γνωστη", "γνωστή", "μην", "την",
    "protothema", "kathimerini", "news", "24", "7", "skai", "in", "gr",
    "παθογονου", "παθογόνου", "μικροοργανισμου", "μικροοργανισμού",
    "βρεθηκε", "βρέθηκε",
}


def _title_keywords(title: str, max_words: int = 6) -> str:
    """Distinctive words from the news title for a site:efet.gr search."""
    words = _re2.findall(r"[0-9A-Za-zΑ-Ωα-ωΆ-Ώά-ώ]+", title or "")
    kept = []
    for w in words:
        wl = w.lower()
        # accent-fold for stopword test
        import unicodedata as _ud
        folded = "".join(c for c in _ud.normalize("NFD", wl)
                          if _ud.category(c) != "Mn")
        if folded in _TITLE_NOISE or len(w) < 3:
            continue
        kept.append(w)
        if len(kept) >= max_words:
            break
    return " ".join(kept)


def resolve_authority_url_via_search(news_title: str, cfg: CountryConfig,
                                     verbose: bool = False) -> str:
    """Find the authority's own recall page via `site:<domain> <terms>`.

    Returns the canonical authority item URL, or "" if none matches.
    """
    domain = (cfg.authority_domain or "").lower().lstrip("www.")
    if not domain:
        return ""
    terms = _title_keywords(news_title)
    if not terms:
        return ""

    q = f"site:{domain} {terms} when:30d"
    lang = getattr(cfg, "language_code", "el")
    code = cfg.code.upper()
    url = (f"{_GN_BASE}?q={_quote_plus(q)}"
           f"&hl={lang}-{code}&gl={code}&ceid={code}:{lang}")

    xml = _ns_http_get(url, verbose=False)
    if not xml:
        return ""

    item_re = None
    if getattr(cfg, "authority_item_url_regex", ""):
        try:
            item_re = _re2.compile(cfg.authority_item_url_regex, _re2.IGNORECASE)
        except _re2.error:
            item_re = None

    for item in _parse_rss(xml):
        link = item.get("link", "")
        # Google News wraps the link; decode → real efet.gr URL.
        real = _gnr.resolve(link) if _gnr.is_google_news_url(link) else link
        if not real:
            continue
        host = ""
        try:
            host = urlparse(real).netloc.lower().lstrip("www.")
        except Exception:
            pass
        if host != domain and not host.endswith("." + domain):
            continue
        if item_re is None or item_re.search(real):
            if verbose:
                print(f"    [authority-search] matched {real[:70]}",
                      file=__import__("sys").stderr)
            return _clean(real)
    return ""

if __name__ == "__main__":
    # Tiny self-test with a synthetic EFET-linking article.
    import sys
    sys.path.insert(0, ".")
    try:
        from pipeline.gap_finder.countries import get as get_country
    except ImportError:
        from gap_finder.countries import get as get_country  # type: ignore
    cfg = get_country("gr")

    sample = '''
      <article>
        Ο ΕΦΕΤ ανακοίνωσε την ανάκληση. Δείτε το
        <a href="https://www.efet.gr/index.php/el/enimerosi/deltia-typou/anakleiseis-cat/item/5396-anakliseis-pestrofa">δελτίο τύπου</a>.
        Σχετικά: <a href="https://www.news247.gr/some-other-article">άλλο άρθρο</a>
      </article>'''
    print("found:", find_authority_url(sample, cfg))

    no_efet = '<article>Recall news with no official link <a href="https://www.kathimerini.gr/x">more</a></article>'
    print("none :", repr(find_authority_url(no_efet, cfg)))
