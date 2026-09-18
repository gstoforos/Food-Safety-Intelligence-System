# -*- coding: utf-8 -*-
"""May a reviewer replace the URL on a row?

Shared by reviewer 1 (recall_url_agent) and reviewer 2
(recall_review_agent). It lives in its own module because both agents are
written to disk by their workflows from a heredoc, independently — a
helper defined inside one of them is not importable from the other.


"""

from __future__ import annotations

import re
from typing import Any, Dict
from urllib.parse import urlparse


_COLLECTOR_SOURCED = re.compile(r"\[via official-feed collector\]", re.I)

#: Paths that are an agency INDEX, never one notice. A proposed URL landing
#: on one of these is the failure above, whatever its anchors match.
_LISTING_PATH = re.compile(
    r"^/?("
    r"news-and-alerts/food-alerts|news-alerts|recalls?(-alerts)?|"
    r"safety/recalls-market-withdrawals-safety-alerts|"
    r"tilbakekallinger|web/gis/ostrzezenia\w*|fiche-rappel"
    r")/?$", re.I)


def url_overwrite_refusal(row: Dict[str, Any], proposed: str) -> str:
    """"" if reviewer 1 may set this URL, else the reason it may not."""
    cur = str(row.get("URL") or "").strip()
    new = str(proposed or "").strip()

    def _same(a: str, b: str) -> bool:
        """Equal once a trailing slash, a #fragment and a ?query are set
        aside. Those are presentation, not a different document, so
        adding one is not an overwrite and must not be refused."""
        strip = lambda s: re.split(r"[?#]", s.strip().lower(), 1)[0].rstrip("/")
        return strip(a) == strip(b)

    if not new or _same(new, cur):
        return ""                       # no change proposed

    notes = str(row.get("Notes") or "")

    # 1. The collector read this href out of the regulator's own markup.
    if cur and _COLLECTOR_SOURCED.search(notes):
        return ("row came from the official-feed collector, whose URL is the "
                "regulator's own href — a reviewer does not overwrite it")

    # 2. The proposed URL extends past the recorded identifier: a truncated
    #    slug with the headline glued back on.
    m = re.search(r"source_id=([^\s\]]+)", notes)
    if m:
        slug = re.sub(r"^[A-Z]{2,6}-", "", m.group(1)).lower()
        low = new.lower()
        if len(slug) >= 12 and slug in low:
            tail = re.sub(r"^[?#].*$", "", low.split(slug, 1)[1].strip("/"))
            if len(tail) > 8:
                return ("proposed URL runs %d characters past source_id %r — "
                        "a truncated regulator slug with the headline appended "
                        "is not a page" % (len(tail), slug[-36:]))

    # 3. An agency index is never one notice.
    try:
        if _LISTING_PATH.match((urlparse(new).path or "/").strip("/")):
            return ("proposed URL is the agency's listing page, not a notice "
                    "— a listing names every alert it links to, so anchor "
                    "checks pass on it")
    except Exception:                                        # noqa: BLE001
        pass

    # 4. Never move a row to a different host than the one it came from.
    try:
        if cur:
            h_old = (urlparse(cur).netloc or "").lower().lstrip("www.")
            h_new = (urlparse(new).netloc or "").lower().lstrip("www.")
            if h_old and h_new and h_old != h_new:
                return ("proposed URL changes host from %r to %r" % (h_old, h_new))
    except Exception:                                        # noqa: BLE001
        pass

    return ""


