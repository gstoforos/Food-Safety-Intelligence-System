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


def fetch_text(url: str, timeout: int = 25) -> Tuple[str, str]:
    """Return (visible_text, status). status is 'ok' or a reason string."""
    try:
        import requests
    except Exception:                                        # noqa: BLE001
        return "", "requests_unavailable"
    try:
        r = requests.get(url, timeout=timeout, allow_redirects=True,
                         headers={"User-Agent": _UA})
    except Exception as e:                                   # noqa: BLE001
        return "", f"unreachable:{type(e).__name__}"
    if r.status_code >= 400:
        return "", f"http_{r.status_code}"
    text = re.sub(r"<script.*?</script>", " ", r.text, flags=re.S | re.I)
    text = re.sub(r"<style.*?</style>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    return _norm(text), "ok"


def check(row: Dict[str, Any], page_text: str = None,
          treat_unreachable_as_problem: bool = False) -> List[str]:
    """Does the cited page describe this row?

    Pass page_text when the caller already fetched it, so the page is not
    downloaded twice in one review.
    """
    url = str(row.get("URL", "") or "").strip()
    if not url:
        return ["row has no URL — provenance cannot be established"]

    status = "ok"
    if page_text is None:
        page_text, status = fetch_text(url)
    if status != "ok" or not page_text:
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
