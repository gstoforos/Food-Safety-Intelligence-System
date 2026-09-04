"""
_provenance.py — does the cited page actually describe THIS row?

WHY THIS EXISTS
    Audited 2026-09-01. Of the three review agents:

        recall_url_agent      fetches the page, content-match checks: 0
        recall_review_agent   fetches the page, content-match checks: 0
        recall_confirm_agent  does not fetch at all

    All three verify that a URL is well-formed, on the regulator's own domain,
    and resolves. None of them verify that the page is about the recall the
    row claims. Only pipeline/agents/curator.py does, and the curator only
    sees proposals from the outbreak-intel agent — it never touches the rows
    the three reviewers publish.

    That gap is not theoretical. RappelConso fiche 22230 reached the register
    as a Brie/Listeria row; the fiche is a SHEIN plush toy recall. The URL was
    well-formed, on rappel.conso.gouv.fr, and returned HTTP 200. Every check
    the reviewers run passed it. The Rejected sheet holds the same class:
    an O'Brien ham Listeria row whose URL points at supplement.ge, a Georgian
    supplement blog, and an NCC hummus row sourced from iol.co.za.

    A domain check cannot catch these. Only reading the page can.

WHAT IT DOES
    Fetches the URL and requires the page to mention at least one distinctive
    token the row claims — pathogen, company, brand or product. Returns a list
    of problems; empty means the page corroborates the row.

WHAT IT DELIBERATELY DOES NOT DO
    * It does not require ALL tokens to match. Regulators paraphrase, translate
      and abbreviate; demanding a full match would reject good rows.
    * It does not treat an unreachable URL as proof of a bad row. A timeout is
      "could not confirm", not "wrong" — the caller decides whether that
      blocks. Several regulators 403 datacentre traffic, and rejecting on that
      would discard real recalls for an infrastructure reason.
    * It does not parse or correct anything. It answers one question.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any, Dict, List, Tuple

# Tokens too generic to prove a page is about a given row. "listeria" appears
# on every Listeria notice a regulator has ever published, so matching it
# proves nothing about WHICH recall the page describes.
_GENERIC = {
    "shiga", "toxin", "producing", "coli", "listeria", "monocytogenes",
    "salmonella", "escherichia", "bacillus", "cereus", "clostridium",
    "aflatoxin", "ochratoxin", "norovirus", "hepatitis", "vibrio",
    "campylobacter", "cronobacter", "staphylococcus", "presence",
    "product", "products", "produit", "produits", "recall", "rappel",
    "unbranded", "various", "marque", "sans", "aucune", "brand",
    "limited", "company", "foods", "food", "group", "france", "united",
    "kingdom", "ireland", "belgium", "unknown", "none",
}

_UA = "AFTS-FSIS/1.0 (Food Safety Intelligence System; +https://fsis.advfood.tech)"


#: Statuses from fetch_text() that prove the regulator has no such page.
#: 404 Not Found and 410 Gone are assertions by the server. Everything else
#: fetch_text can return — http_403, http_5xx, unreachable:* — is a statement
#: about our access, not about the page.
_DEAD_STATUSES = ("http_404", "http_410")

#: Hosts whose notice pages cannot be read by any fetcher we have and must
#: not be fetched at all (operator rule 4, 2026-09-02). The RASFF portal
#: serves an EMPTY JavaScript shell with HTTP 200 — no product, no country,
#: no text — so a content match against it fails for every real
#: notification. From 2026-09-01 to 2026-09-04 that blocked every RASFF
#: scraper row at reviewer 3, and because the block was never archived the
#: rows were silently deleted and re-scraped daily. Rows on these hosts are
#: verified by their scraper's structured fields, not by this module.
_NO_FETCH_HOSTS = ("webgate.ec.europa.eu",)

#: Fewer visible characters than this after tag-stripping means the server
#: sent an application shell, not a notice. That is "could not read", never
#: "does not mention".
_MIN_PAGE_CHARS = 300


def is_dead_status(status: str) -> bool:
    """True only when the server said the page does not exist.

    Deliberately excludes 403 and every timeout/proxy failure. See the note
    in check() — conflating the two rejects real recalls on the several
    regulators that refuse datacentre traffic for all of their URLs.
    """
    return str(status or "") in _DEAD_STATUSES


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s or ""))
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", s.lower())


def anchors_for(row: Dict[str, Any], limit: int = 10) -> List[str]:
    """Distinctive tokens the page must corroborate.

    Company / Brand / Product first: those name THIS recall. Pathogen last and
    only when nothing else is available, because a pathogen name appears on
    every notice about that organism and cannot distinguish one from another.
    """
    out: List[str] = []
    for field in ("Company", "Brand", "Product"):
        for tok in re.findall(r"[A-Za-zÀ-ÿ]{5,}", str(row.get(field, "") or "")):
            t = _norm(tok)
            if t and t not in _GENERIC:
                out.append(t)
    if not out:
        for tok in re.findall(r"[A-Za-z]{5,}", str(row.get("Pathogen", "") or "")):
            t = _norm(tok)
            if t and t not in _GENERIC:
                out.append(t)
    return list(dict.fromkeys(out))[:limit]


def _fetch_response(url: str, timeout: int):
    """One GET, routed by host. Returns (response_or_None, failure_status).

    AKAMAI HOSTS (audit 2026-09-02). www.fda.gov, www.fsis.usda.gov,
    www.fda.gov.ph and www.gov.il sit behind Akamai bot detection, which
    fingerprints the TLS handshake. Plain `requests` gets HTTP 404 from
    them for EVERY page — real notices included (claude_check.py recorded
    this on 2026-05-20; the real Mama Cozzi's FSIS PHA carries
    "fetch failed (HTTP 404)" stamps from 2026-06-14; the URL guardian
    blanked three real FDA URLs on 2026-09-01 the same way).

    claude_check.py has routed those hosts through curl_cffi Chrome
    impersonation since 2026-05-20. When the Qwen reviewers replaced it on
    2026-08-01 this module kept plain `requests`, so for FDA and FSIS rows
    the page check has been passing vacuously ever since — an invented slug
    and a real notice both came back "unreadable", and unreadable is not a
    defect. Three Gemini re-mints of already-published FDA recalls reached
    the register on 2026-09-02 through exactly that hole. With the 404 guard
    in check() now live, the same blindness would instead REJECT every real
    FDA/FSIS row. Either way the fix is the same: read the page the way the
    scrapers do.

    Everything not on the Akamai list keeps the plain `requests` path.
    """
    try:
        from scrapers._akamai_fetch import is_akamai_host, fetch_via_curl_cffi
    except Exception:                                        # noqa: BLE001
        is_akamai_host, fetch_via_curl_cffi = (lambda _u: False), None
    if fetch_via_curl_cffi is not None and is_akamai_host(url):
        try:
            r = fetch_via_curl_cffi(url, timeout=timeout, allow_redirects=True)
        except Exception as e:                               # noqa: BLE001
            return None, f"unreachable:{type(e).__name__}"
        if r is None:
            return None, "unreachable:curl_cffi"
        return r, ""
    try:
        import requests
    except Exception:                                        # noqa: BLE001
        return None, "requests_unavailable"
    try:
        r = requests.get(url, timeout=timeout, allow_redirects=True,
                         headers={"User-Agent": _UA})
    except Exception as e:                                   # noqa: BLE001
        return None, f"unreachable:{type(e).__name__}"
    return r, ""


def fetch_text(url: str, timeout: int = 25) -> Tuple[str, str]:
    """Return (visible_text, status). status is 'ok' or a reason string."""
    r, fail = _fetch_response(url, timeout)
    if r is None:
        return "", fail
    if r.status_code >= 400:
        return "", f"http_{r.status_code}"
    text = re.sub(r"<script.*?</script>", " ", r.text, flags=re.S | re.I)
    text = re.sub(r"<style.*?</style>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = _norm(text)
    if len(text.strip()) < _MIN_PAGE_CHARS:
        return "", "js_shell"          # readable HTTP, unreadable page
    return text, "ok"


def check(row: Dict[str, Any], page_text: str = None,
          treat_unreachable_as_problem: bool = False) -> List[str]:
    """Does the cited page describe this row?

    Pass page_text when the caller already fetched it, so the page is not
    downloaded twice in one review.
    """
    url = str(row.get("URL", "") or "").strip()
    if not url:
        return ["row has no URL — provenance cannot be established"]
    if any(h in url.lower() for h in _NO_FETCH_HOSTS):
        return []          # rule 4: never fetch; not adjudicable from the page

    status = "ok"
    if page_text is None:
        page_text, status = fetch_text(url)

    if status != "ok" or not page_text:
        # DEAD vs BLOCKED (audit 2026-09-02). These are not the same fact and
        # must not share a branch.
        #
        #   404 / 410  the regulator has no such page. For a URL a model
        #              proposed, that is the signature of an invented slug.
        #              It is a defect of fact and always blocks.
        #   403 / timeout / proxy error
        #              WE could not read it. Says nothing about the row.
        #              fsis.usda.gov, the RASFF portal and salute.gov.it
        #              (Gcore) refuse datacentre traffic for every URL they
        #              serve, valid ones included, so blocking here would
        #              discard real recalls wholesale.
        #
        # An earlier version of this audit's fix passed
        # treat_unreachable_as_problem=True for model-sourced rows and so
        # rejected on 403 as well. That was wrong: it would have failed every
        # gap-finder row on those three sources. The flag now governs ONLY
        # the unreadable case; a dead page blocks regardless of it.
        if is_dead_status(status):
            return [f"the cited page does not exist ({status}) — the "
                    f"regulator publishes no such notice at this URL"]
        if treat_unreachable_as_problem:
            return [f"could not read the cited page ({status}) — "
                    f"provenance unconfirmed"]
        return []          # infrastructure, not a data defect

    toks = anchors_for(row)
    if not toks:
        return []          # nothing distinctive to test; silence is honest

    hits = [t for t in toks if t in page_text]
    if hits:
        return []
    return [f"page does not mention any of {toks[:6]} — the cited URL "
            f"describes something other than this row"]


def matched_tokens(row: Dict[str, Any], page_text: str) -> List[str]:
    """Which anchors the page corroborated — for the audit trail."""
    return [t for t in anchors_for(row) if t in (page_text or "")]
