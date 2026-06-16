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
# Tier 2 — resolve the authority URL from the authority's OWN recall index
# ──────────────────────────────────────────────────────────────────────────
#
# Greek news outlets cite EFET by name but don't hyperlink the press release,
# so the article-HTML scan (Tier 1) finds nothing. EFET press releases are
# also NOT in Google News (it's a government portal, not a news publisher),
# so a site:efet.gr Google-News query returns nothing either.
#
# What works: fetch EFET's OWN recall-index page
# (cfg.authority_index_url) with curl_cffi (Chrome 131 clears the Joomla 409
# that blocks plain requests), parse every item/<num>-<slug> link, and match
# the one whose slug best overlaps the recall's product keywords. The index
# lists each press release with its title in the link slug, e.g.
#   .../anakleiseis-cat/item/5396-deltio-typou-anaklisi-...-pestrofas-...
# so keyword overlap on the slug reliably picks the right item.

import re as _re2

# NOTE: fetch_html is imported LAZILY inside resolve_authority_url_via_search
# to avoid a circular import (article_fetcher imports this module).

# Greek stopwords + outlet names to strip from the title before matching,
# leaving only distinctive recall terms (product/company/brand/pathogen).
_TITLE_NOISE = {
    "efet", "εφετ", "ο", "η", "το", "του", "της", "των", "και", "λογω",
    "ανακληση", "ανακαλει", "ανακαλειται", "προσοχη", "γνωστη", "μην",
    "την", "protothema", "kathimerini", "news", "skai", "in", "gr",
    "παθογονου", "μικροοργανισμου", "βρεθηκε", "deltio", "typou",
    "anaklisi", "anakliseis", "mi", "asfaloys", "asfalous", "proiontos",
    "logo", "parousias", "tou", "tis", "ton",
}


# Greek → Latin transliteration map. EFET URL slugs romanize the Greek
# title (πέστροφας → pestrofas, καπνιστής → kapnistis), but news titles are
# in Greek script. To match title keywords against slug keywords we
# transliterate Greek to the SAME Latin scheme EFET/Joomla uses.
_GR2LAT = {
    "α":"a","β":"v","γ":"g","δ":"d","ε":"e","ζ":"z","η":"i","θ":"th",
    "ι":"i","κ":"k","λ":"l","μ":"m","ν":"n","ξ":"x","ο":"o","π":"p",
    "ρ":"r","σ":"s","ς":"s","τ":"t","υ":"y","φ":"f","χ":"x","ψ":"ps",
    "ω":"o",
}
# Common digraphs Joomla collapses (ου→ou, αι→ai, ει→ei, ντ→nt, μπ→b, γκ→g)
_GR_DIGRAPHS = [
    ("ου","ou"), ("αι","ai"), ("ει","ei"), ("οι","oi"), ("αυ","af"),
    ("ευ","ef"), ("ντ","nt"), ("μπ","mp"), ("γκ","gk"), ("τσ","ts"),
    ("τζ","tz"), ("γγ","ng"),
]


def _translit_gr(w: str) -> str:
    """Transliterate a Greek-script word to EFET-slug Latin."""
    for gr, lat in _GR_DIGRAPHS:
        w = w.replace(gr, lat)
    return "".join(_GR2LAT.get(c, c) for c in w)


def _fold(w: str) -> str:
    import unicodedata as _ud
    w = w.lower()
    # strip accents first
    w = "".join(c for c in _ud.normalize("NFD", w)
                if _ud.category(c) != "Mn")
    # if it contains Greek letters, transliterate to Latin to match slugs
    if any("\u0370" <= c <= "\u03ff" for c in w):
        w = _translit_gr(w)
    return w


def _keywords(text: str) -> set[str]:
    """Distinctive accent-folded words (>=4 chars, non-stopword) from text."""
    out = set()
    for w in _re2.findall(r"[0-9A-Za-zΑ-Ωα-ωΆ-Ώά-ώ]+", text or ""):
        f = _fold(w)
        if len(f) >= 4 and f not in _TITLE_NOISE:
            out.add(f)
    return out


def resolve_authority_url_via_search(news_title: str, cfg: CountryConfig,
                                     verbose: bool = False) -> str:
    """Find the authority's canonical recall URL from its recall-index page.

    Fetches cfg.authority_index_url (curl_cffi), parses item links, and
    returns the item whose slug best overlaps the news title's keywords.
    Returns "" if no index URL configured or no confident match.
    """
    import sys as _sys
    index_url = getattr(cfg, "authority_index_url", "") or ""
    if not index_url:
        return ""

    # Lazy import to avoid circular dependency with article_fetcher.
    try:
        from .article_fetcher import fetch_html as _fetch_html
    except ImportError:                                          # pragma: no cover
        from gap_finder.article_fetcher import fetch_html as _fetch_html  # type: ignore

    domain = (cfg.authority_domain or "").lower().lstrip("www.")
    item_re = None
    if getattr(cfg, "authority_item_url_regex", ""):
        try:
            item_re = _re2.compile(cfg.authority_item_url_regex, _re2.IGNORECASE)
        except _re2.error:
            item_re = None

    # Fetch the index page (curl_cffi clears the Joomla 409).
    try:
        _resolved, html, status = _fetch_html(index_url, cfg)
    except Exception as e:
        if verbose:
            print(f"    [authority-index] fetch error: {e}", file=_sys.stderr)
        return ""
    if not html:
        if verbose:
            print(f"    [authority-index] empty fetch (status={status})",
                  file=_sys.stderr)
        return ""

    # Collect candidate item URLs (absolute) from the index.
    items: list[str] = []
    for href in _all_links(html):
        u = href
        if u.startswith("/"):
            u = f"https://www.{domain}{u}"
        elif u.startswith("//"):
            u = "https:" + u
        # must be an authority item URL
        try:
            host = urlparse(u).netloc.lower().lstrip("www.")
        except Exception:
            continue
        if (host == domain or host.endswith("." + domain)) and \
           (item_re is None or item_re.search(u)):
            items.append(_clean(u))

    if not items:
        if verbose:
            print(f"    [authority-index] no item links found in index",
                  file=_sys.stderr)
        return ""

    # Match: score each item slug by keyword overlap with the news title.
    def _stems(words: set[str]) -> set[str]:
        # 6-char prefixes absorb Greek inflection endings after translit
        # (salata/salatas, stathi/stathis, pestrofa/pestrofas).
        return {w[:6] for w in words if len(w) >= 4}

    want = _stems(_keywords(news_title))
    if not want:
        return ""
    best_url, best_score = "", 0
    for u in items:
        slug = u.rsplit("/", 1)[-1]
        have = _stems(_keywords(slug.replace("-", " ")))
        score = len(want & have)
        if score > best_score:
            best_score, best_url = score, u

    # Require at least 2 overlapping distinctive keywords to accept a match,
    # so we don't grab an unrelated recall on a single common word.
    if best_score >= 2:
        if verbose:
            print(f"    [authority-index] matched (score={best_score}) "
                  f"{best_url[:75]}", file=_sys.stderr)
        return best_url

    if verbose:
        print(f"    [authority-index] no confident match "
              f"(best score={best_score})", file=_sys.stderr)
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
