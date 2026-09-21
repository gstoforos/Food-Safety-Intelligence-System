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

# ==========================================================================
# THE REJECT SIDE (audit 2026-09-21)
# ==========================================================================
# url_overwrite_refusal above stops reviewer 1 REPLACING a regulator's own
# href with a guess. That was the Macroom incident. This is its mirror
# image, and it has been quietly costing rows ever since:
#
# Reviewer 1's contract lets it reject for "no official page findable". It
# decides "findable" by SEARCHING and FETCHING — and it does not look at
# the URL already sitting on the row. So when a collector has supplied the
# regulator's own href, which is the strongest evidence available, and the
# page happens to be unreachable from a GitHub runner, the model concludes
# the page does not exist and throws the row away.
#
# MEASURED 2026-09-21, from the live Weekly_Rejected sheet — every one of
# these was rejected as "no official page", with the official page in its
# own URL column:
#
#   FSAI (IE)  Dunnes Stores Potato Waffles   (rejected TWICE)
#     https://www.fsai.ie/news-and-alerts/food-alerts/recall-of-a-batch-of-
#     dunnes-stores-potato-waffles
#   FSAI (IE)  prepared Roast Chicken and Gravy
#   FDA        Gf Blends
#     https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/
#     gf-blends-recalls-truly-aip-all-purpose-fl
#
# fda.gov, fsis.usda.gov, fda.gov.ph and gov.il are already known to return
# HTTP 403 to datacentre traffic for EVERY url — documented in this repo.
# "I could not fetch it" and "it does not exist" are different facts and
# the agent was reporting the second when it meant the first.
#
# This is not a licence to publish. The row goes back to the queue for a
# human or a later run; it is simply not DISCARDED on the strength of a
# fetch that a datacentre IP was never going to complete.

#: Hosts whose own domain is proof the page is official. Suffix match, so
#: a subdomain counts. Keep in step with the collectors in
#: pipeline/official_feeds/sources/.
_AUTHORITY_HOSTS = (
    "fsai.ie", "fda.gov", "fsis.usda.gov", "cfia-acia.canada.ca",
    "inspection.canada.ca", "recalls-rappels.canada.ca", "food.gov.uk",
    "rappel.conso.gouv.fr", "agriculture.gouv.fr", "mattilsynet.no",
    "livsmedelsverket.se", "ruokavirasto.fi", "nvwa.nl", "favv-afsca.be",
    "blv.admin.ch", "ages.at", "bvl.bund.de", "lebensmittelwarnung.de",
    "gov.pl", "efet.gr", "salute.gov.it", "aesan.gob.es", "asae.gov.pt",
    "webgate.ec.europa.eu", "ec.europa.eu", "foodstandards.gov.au",
    "mpi.govt.nz", "sfa.gov.sg", "cfs.gov.hk", "mfds.go.kr",
    "fda.gov.ph", "gov.il", "nafdac.gov.ng", "sahpra.org.za",
)

#: Reject reasons that are a claim about REACHABILITY, not about content.
#: Anything here is answerable by the URL already on the row.
_NOT_FOUND_REASON = re.compile(
    r"no\s+(official|specific)?\s*(regulator|recall|agency)?\s*"
    r"(page|url|link|source)?\s*(was\s+)?(found|findable|located|"
    r"available|identified)"
    r"|could\s+not\s+(be\s+)?(find|locate|reach|access|verify)"
    r"|unable\s+to\s+(find|locate|reach|access|verify)"
    r"|(page|url|link)\s+(not\s+found|unreachable|inaccessible)"
    r"|404|403",
    re.I)


def host_is_authority(url: str) -> bool:
    """Is this URL on a regulator's own domain?"""
    try:
        host = urlparse(str(url or "").strip()).netloc.lower()
    except ValueError:
        return False
    if not host:
        return False
    host = host.split(":")[0]
    if host.startswith("www."):
        host = host[4:]
    return any(host == a or host.endswith("." + a) for a in _AUTHORITY_HOSTS)


def reject_refusal(row: Dict[str, Any], reason: str) -> str:
    """"" if reviewer 1 may reject on this reason, else why it may not.

    ONLY blocks the "I could not find the page" class. A reject on
    content — out of scope, allergen, foreign body, pre-2026, not a
    recall, non-food — is reviewer 1 doing its job and passes straight
    through, even on an authority URL.
    """
    why = str(reason or "").strip()
    if not why:
        return ""
    if not _NOT_FOUND_REASON.search(why):
        return ""          # a content verdict; not this guard's business
    url = str(row.get("URL") or "").strip()
    if not url:
        return ""          # nothing on the row to contradict the model
    if not host_is_authority(url):
        return ""          # a news or unknown host proves nothing
    return (f"the row already carries an official regulator URL ({url}); "
            f"'{why[:90]}' is a statement about reachability, not about "
            f"whether the notice exists. Several regulator hosts return "
            f"403 to datacentre traffic for every page.")
