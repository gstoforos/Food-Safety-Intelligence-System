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


def _all_links_with_text(html: str) -> list[tuple[str, str]]:
    """Every (href, anchor_text) pair in the HTML.

    Authority index rows (e.g. FAVV /nl/producten) put the full recall
    description inside the <a> element — company, brand, product, hazard,
    date — not just in the URL slug. Matching against this visible text is
    far more reliable than matching against the slug alone. The anchor's
    title="" attribute is folded in too (FAVV repeats the company there).
    Tags inside the anchor are stripped so we keep only readable text.
    """
    if not html:
        return []
    out: list[tuple[str, str]] = []
    for m in re.finditer(
        r'<a\b([^>]*?)>(.*?)</a>', html, flags=re.IGNORECASE | re.DOTALL
    ):
        attrs, inner = m.group(1), m.group(2)
        hm = re.search(r'href\s*=\s*["\']([^"\']+)["\']', attrs, re.IGNORECASE)
        if not hm:
            continue
        href = unescape(hm.group(1))
        tm = re.search(r'title\s*=\s*["\']([^"\']+)["\']', attrs, re.IGNORECASE)
        title_attr = unescape(tm.group(1)) if tm else ""
        text = re.sub(r"<[^>]+>", " ", inner)          # strip nested tags
        text = unescape(re.sub(r"\s+", " ", text)).strip()
        combined = (text + " " + title_attr).strip()
        out.append((href, combined))
    return out


def _authority_domains(cfg: CountryConfig) -> list[str]:
    """All accepted authority hosts: primary + any authority_domains_extra."""
    out = []
    primary = (getattr(cfg, "authority_domain", "") or "").lower().lstrip("www.")
    if primary:
        out.append(primary)
    for d in (getattr(cfg, "authority_domains_extra", None) or []):
        d = (d or "").lower().lstrip("www.")
        if d and d not in out:
            out.append(d)
    return out


def host_is_authority(url: str, cfg: CountryConfig) -> bool:
    """True if url's host equals or is a subdomain of ANY accepted authority host."""
    try:
        host = urlparse(url).netloc.lower().lstrip("www.")
    except Exception:
        return False
    if not host:
        return False
    for d in _authority_domains(cfg):
        if host == d or host.endswith("." + d):
            return True
    return False


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
        return host_is_authority(u, cfg)

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


def _norm_for_index_cmp(u: str) -> str:
    """Lowercase + strip scheme / www. / query / fragment / trailing slash, so
    two spellings of the same index page compare equal."""
    u = (u or "").strip().lower()
    u = u.split("#", 1)[0].split("?", 1)[0]
    u = re.sub(r"^https?://", "", u)
    if u.startswith("www."):
        u = u[4:]
    return u.rstrip("/")


def url_is_index(url: str, cfg: CountryConfig) -> bool:
    """True if `url` IS the authority's recall INDEX/listing page.

    An index lists many recalls and carries no single-recall data (product,
    hazard, lot), so it must never be treated as a per-recall authority item.
    This matters for authorities like ANSA (md) whose item regex
    ``(rechemare|retragere)…`` also matches the bare index
    ``/media/rechemare-retragere`` — without this guard the index (≈377 chars of
    listing teasers) gets fetched as if it were a recall, classifies as
    unknown, and the real recall page is never reached. (audit 2026-06-30)
    """
    idx = getattr(cfg, "authority_index_url", "") or ""
    if not idx:
        return False
    return _norm_for_index_cmp(url) == _norm_for_index_cmp(idx)


def url_is_authority_item(url: str, cfg: CountryConfig) -> str:
    """If `url` is ITSELF an authority per-recall page, return it cleaned; else "".

    Some authorities (e.g. SZPI's potravinynapranyri.cz) publish each recall as
    its own page that Google News indexes directly — so the discovered candidate
    URL is already the canonical authority URL. In that case there is no separate
    "authority link in the article" to find and no index to search: the candidate
    URL is the answer. This check short-circuits both Tier-1 (HTML scan) and
    Tier-2 (index resolve), which would otherwise fail for such sites and reject
    a perfectly valid authority recall page.

    Matches when the host is the authority domain (with/without www / subdomain)
    AND the path+query matches the configured authority_item_url_regex.
    """
    domain = (cfg.authority_domain or "").lower().lstrip("www.")
    if not domain or not url:
        return ""
    if not host_is_authority(url, cfg):
        return ""
    # The bare recall INDEX is on the authority domain and can match the item
    # regex, but it is a listing, not a single recall — never accept it here.
    if url_is_index(url, cfg):
        return ""
    item_rx = getattr(cfg, "authority_item_url_regex", "")
    if not item_rx:
        return ""
    try:
        if re.compile(item_rx, re.IGNORECASE).search(url):
            return _clean(url)
    except re.error:
        return ""
    return ""



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


# Cyrillic → Latin transliteration map. FVA (North Macedonia) and similar
# authorities romanize Cyrillic titles into their URL slugs
# (БИО КАКАО → bio-kakao, отповикување → otpovikuvanje, афлатоксин →
# aflatoksin). News titles and anchor rows are in Cyrillic, so to match
# title keywords against slug keywords we transliterate Cyrillic to the same
# Latin scheme. Covers Macedonian + Bulgarian/Serbian Cyrillic letters.
_CYR2LAT = {
    "а":"a","б":"b","в":"v","г":"g","д":"d","ѓ":"gj","е":"e","ж":"z",
    "з":"z","ѕ":"dz","и":"i","ј":"j","к":"k","л":"l","љ":"lj","м":"m",
    "н":"n","њ":"nj","о":"o","п":"p","р":"r","с":"s","т":"t","ќ":"kj",
    "у":"u","ф":"f","х":"h","ц":"c","ч":"c","џ":"dz","ш":"s",
    # Bulgarian / Serbian extras
    "й":"i","ъ":"a","ь":"","э":"e","ю":"ju","я":"ja","ы":"y","щ":"st",
    "ё":"e","ћ":"c","ђ":"dj",
}


def _translit_cyr(w: str) -> str:
    """Transliterate a Cyrillic-script word to authority-slug Latin."""
    return "".join(_CYR2LAT.get(c, c) for c in w)


def _fold(w: str) -> str:
    import unicodedata as _ud
    w = w.lower()
    # strip accents first
    w = "".join(c for c in _ud.normalize("NFD", w)
                if _ud.category(c) != "Mn")
    # if it contains Greek letters, transliterate to Latin to match slugs
    if any("\u0370" <= c <= "\u03ff" for c in w):
        w = _translit_gr(w)
    # if it contains Cyrillic letters, transliterate to Latin to match slugs
    if any("\u0400" <= c <= "\u04ff" for c in w):
        w = _translit_cyr(w)
    return w


def _keywords(text: str) -> set[str]:
    """Distinctive accent-folded words (>=4 chars, non-stopword) from text."""
    out = set()
    for w in _re2.findall(r"[0-9A-Za-zΑ-Ωα-ωΆ-Ώά-ώ\u0400-\u04ff]+", text or ""):
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

    authority_domain = (cfg.authority_domain or "").lower().lstrip("www.")
    # If an aggregator front-end is configured (e.g. PL: oalert.pl → gov.pl),
    # items live on index_domain and we follow through to authority_domain
    # afterwards. Otherwise index IS the authority site.
    index_domain = (getattr(cfg, "index_domain", "") or "").lower().lstrip("www.")
    match_domain = index_domain or authority_domain
    domain = match_domain  # used by item-collection host filter below

    item_re = None
    _item_pat = (getattr(cfg, "index_item_url_regex", "") if index_domain
                 else getattr(cfg, "authority_item_url_regex", ""))
    if _item_pat:
        try:
            item_re = _re2.compile(_item_pat, _re2.IGNORECASE)
        except _re2.error:
            item_re = None

    # Fetch the index page (curl_cffi clears the Joomla 409).
    try:
        # Authority index pages (FAVV, AGES) can be large/slow; give the
        # single per-run index fetch a longer timeout than article fetches.
        _resolved, html, status = _fetch_html(index_url, cfg, timeout=30)
    except Exception as e:
        if verbose:
            print(f"    [authority-index] fetch error: {e}", file=_sys.stderr)
        return ""
    if not html:
        if verbose:
            print(f"    [authority-index] empty fetch (status={status})",
                  file=_sys.stderr)
        return ""

    # Collect candidate items as (url, anchor_text). Authority index rows carry
    # the full recall description (company/brand/product/hazard) in the link
    # text — matching against that is far more reliable than the slug alone.
    items: list[tuple[str, str]] = []
    seen: set[str] = set()
    # Resolve relative hrefs against the URL we actually fetched (_resolved),
    # not a hardcoded www. prefix — some hosts (e.g. oalert.pl) serve without
    # www and "www." + host fails to fetch, breaking the follow-through.
    base_url = _resolved or index_url
    for href, atext in _all_links_with_text(html):
        u = href
        if u.startswith("//"):
            u = "https:" + u
        elif u.startswith("/"):
            u = urljoin(base_url, u)
        # must be an authority item URL
        try:
            host = urlparse(u).netloc.lower().lstrip("www.")
        except Exception:
            continue
        if (host == domain or host.endswith("." + domain)) and \
           (item_re is None or item_re.search(u)):
            # The index links to itself (and to category/listing pages that also
            # match the item regex) — never collect the index as a recall item,
            # or the matcher will score it on the news title's recall verbs and
            # return a contentless listing page. (audit 2026-06-30)
            if url_is_index(u, cfg):
                continue
            cu = _clean(u)
            if cu in seen:
                continue
            seen.add(cu)
            items.append((cu, atext))

    if not items:
        if verbose:
            print(f"    [authority-index] no item links found in index",
                  file=_sys.stderr)
        return ""

    # Match: score each item slug by keyword overlap with the news title.
    # 5-char prefixes absorb inflection across languages (Greek translit
    # salata/salatas; German Rueckruf/Rueckrufe; French rappel/rappels)
    # while staying long enough to avoid spurious short-word collisions.
    def _stems(words: set[str]) -> set[str]:
        return {w[:5] for w in words if len(w) >= 4}

    want_kw = _keywords(news_title)
    want = _stems(want_kw)
    if not want:
        return ""
    # Distinctive long keywords (>=6 chars, usually a brand/company/product)
    # — a single one of these overlapping is strong enough to accept.
    want_strong = {w[:5] for w in want_kw if len(w) >= 7}

    best_url, best_score, best_strong = "", 0, False
    for u, atext in items:
        slug = u.rsplit("/", 1)[-1]
        # Score against slug AND the row's anchor text (company/brand/product/
        # hazard). The anchor text is the authority's own published row, so a
        # terse news title like "Jumbo ... worsten" still matches the row that
        # says "Waarschuwing van Jumbo BBQ Pittige Worsten ... Soja".
        have_kw = _keywords(slug.replace("-", " ")) | _keywords(atext)
        have = _stems(have_kw)
        overlap = want & have
        score = len(overlap)
        strong = bool(overlap & want_strong)
        # prefer higher score; break ties toward a strong-keyword hit
        if score > best_score or (score == best_score and strong and not best_strong):
            best_score, best_url, best_strong = score, u, strong

    # Accept if >=2 keywords overlap (original rule), OR exactly 1 overlaps
    # but it is a long distinctive keyword (brand/company) — this rescues
    # German/French authorities whose news titles are terse.
    if best_score >= 2 or (best_score >= 1 and best_strong):
        if verbose:
            print(f"    [authority-index] matched (score={best_score}) "
                  f"{best_url[:75]}", file=_sys.stderr)
        # Aggregator follow-through: when the match is on an aggregator index
        # (index_domain set, e.g. oalert.pl), the matched page itself links to
        # the official authority report (e.g. gov.pl/web/gis/...). Fetch the
        # matched item and return THAT authority URL so the Pending record
        # stays authority-pure and the Stage-3 gate (host==authority_domain)
        # passes. If no authority link is found, reject rather than store an
        # aggregator URL the gate would refuse anyway.
        if index_domain and authority_domain and authority_domain != index_domain:
            try:
                _r2, item_html, _st2 = _fetch_html(best_url, cfg, timeout=30)
            except Exception as e:
                item_html = ""
                if verbose:
                    print(f"    [authority-index] item fetch error: {e}",
                          file=_sys.stderr)
            auth_url = ""
            if item_html:
                for u in _all_links(item_html):
                    cu = u
                    if cu.startswith("//"):
                        cu = "https:" + cu
                    try:
                        host = urlparse(cu).netloc.lower().lstrip("www.")
                    except Exception:
                        continue
                    if host == authority_domain or host.endswith("." + authority_domain):
                        auth_url = _clean(cu)
                        break
            if auth_url:
                if verbose:
                    print(f"    [authority-index] followed → {auth_url[:75]}",
                          file=_sys.stderr)
                return auth_url
            if verbose:
                print(f"    [authority-index] no {authority_domain} link on "
                      f"aggregator item — rejecting", file=_sys.stderr)
            return ""
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
