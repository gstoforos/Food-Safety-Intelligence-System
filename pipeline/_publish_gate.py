"""Deterministic publish gate — zero tokens, zero network.

WHY THIS EXISTS (audit 2026-08-01)
==================================
A subscriber alert went out on 2026-07-31 carrying this row:

    Date      2026-07-27
    Source    NCC
    Company   BM Foods (a member of Sea Harvest Group)
    Product   Deli Hummus range
    Pathogen  Listeria Monocytogenes
    Reason    Recall ID 842632
    Class     <empty>
    Region    Not specified in the article
    Notes     Discovered via news: timeslive.co.za
    URL       https://thencc.org.za/media-statement-deli-hummus-range-...

The recall is real. It is also from **16 September 2024** — the NCC media
statement is dated then, the best-before dates run 10 Sep to 08 Oct 2024, and
the timeslive article the row itself cites as its source is dated 2024-10-04.
It was published as a 2026-07-27 recall and emailed to subscribers, roughly
22 months stale. `_MIN_VALID_DATE` did not catch it because the fabricated
date is inside 2026.

That row carried SIX defects. Not one of them needed a language model to
spot:

    Reason is only a reference number ......... 'Recall ID 842632'
    Class empty ............................... ''
    Region not in the controlled vocabulary ... 'Not specified in the article'
    Source not the canonical label ............ 'NCC'  (established: 'NCC (ZA)')
    Brand holds the retailer, not the brand ... 'Shoprite Checkers'
    No review trail at all .................... 'Discovered via news: ...'

The same sweep found more of the same shape already published:

    8 rows with NO Pathogen in a pathogen-recall database, including
      'mg3, mg3 hybrid+ voiture de tourisme'  — a passenger CAR, recalled for
          seat movement in a collision (RappelConso fiche 49461)
      'jouet de baignoire à pulvérisation'    — a bath toy, choking hazard
      'petrole lampant'                        — lamp oil
      'mento sport bottle'                     — a drinks bottle
    1 row whose URL is the AESAN HOMEPAGE, with Company, Reason, Pathogen and
      Class all empty
    7 rows whose entire Reason is 'Recall ID <n>' — 5 of them the same number,
      leaked from the extractor prompt's own worked example

The lesson is not "the LLM reviewer failed". The lesson is that these rows
should never have reached an LLM reviewer at all. A schema gate rejects every
one of them for free, and keeps working when the Gemini quota is exhausted,
when the API is down, and when rappel.conso.gouv.fr refuses a TLS handshake.

Use `publish_blockers(row)` — it returns a list of human-readable reasons, one
per violated rule, or an empty list when the row is publishable.
"""
from __future__ import annotations
import re
from typing import Any, Dict, List

# Controlled vocabularies. A value outside these is an extraction artifact,
# not a regional judgement call.
VALID_REGIONS = frozenset({
    "Europe", "North America", "Latin America", "Asia", "Africa",
    "Oceania", "Middle East", "Unknown",
})

# Free-prose tells: an LLM answering the question instead of filling the field.
_PROSE_MARKERS = (
    "not specified", "not stated", "not mentioned", "not provided",
    "not available", "unknown origin", "no information", "n/a in the",
    "in the article", "not found in", "could not determine", "unclear",
)

# A Reason that is nothing but a reference number.
_ID_ONLY_REASON = re.compile(
    r"^\s*(?:recall\s*id|id|allerta|pratica|ref(?:erence)?)\s*[:#]?\s*[0-9]{3,}\s*$",
    re.IGNORECASE,
)

# Regulator landing pages — never a specific recall notice.
#
# DELIBERATELY NARROW. The first draft of this rule tested "path ends with /"
# and "path contains /product-recalls", which flagged 20+ perfectly good
# notices whose slug happens to end in a slash
# (thencc.org.za/product-safety-recall-nutricia-aptamil-nutribiotik-2-.../).
# A gate that cries wolf on good rows gets switched off. A URL is only a
# landing page when its path, once the trailing slash is removed, is EMPTY or
# is exactly one of these listing paths — never on a substring match.
_LANDING_PATHS = frozenset({
    "",
    "/home",
    "/index.htm", "/index.html", "/index.php",
    "/aecosan/web/home/aecosan_inicio.htm",
    "/recalls", "/product-recalls", "/category/product-recalls",
    "/food-alerts", "/news-and-alerts", "/avisos", "/warnungen", "/rappels",
    "/index.php/el/enimerosi/deltia-typou",
    "/en/food-alerts", "/fr/rappels", "/nl/terugroepingen",
})

_PLACEHOLDER_VALUES = frozenset({
    "", "none", "null", "n/a", "na", "-", "—", "tbd", "unknown", "nan",
})


def _blank(value: Any) -> bool:
    return str(value or "").strip().lower() in _PLACEHOLDER_VALUES


def publish_blockers(row: Dict[str, Any]) -> List[str]:
    """Return every reason this row must not be published. Empty == publishable.

    Deterministic and offline by construction: no network, no model, no quota.
    Each rule below corresponds to a defect actually found in published data.
    """
    problems: List[str] = []

    # 1. Pathogen — this is a pathogen/hazard recall database. A row without
    #    one is either out of scope or a failed extraction. This alone rejects
    #    the car, the bath toy, the lamp oil and the sports bottle.
    if _blank(row.get("Pathogen")):
        problems.append(
            "Pathogen is empty — a hazard database row must name its hazard "
            "(this rule rejects non-food consumer-product recalls that arrive "
            "through RappelConso's other categories)")

    # 2. Reason must describe the hazard, never be a bare identifier.
    reason = str(row.get("Reason") or "").strip()
    if _blank(reason):
        problems.append("Reason is empty")
    elif _ID_ONLY_REASON.match(reason):
        problems.append(
            f"Reason is only a reference number ({reason!r}) — the hazard is "
            "not described. 'Recall ID 842632' was the extractor prompt's own "
            "worked example and leaked onto unrelated recalls")

    # 3. Controlled vocabularies, and free prose leaking into them.
    region = str(row.get("Region") or "").strip()
    if region and region not in VALID_REGIONS:
        problems.append(
            f"Region {region!r} is not one of {sorted(VALID_REGIONS)}")
    # Prose leakage is only a blocker in Region, which is a CONTROLLED
    # vocabulary. In Company an honest "(not specified in FAVV notice)" is
    # better than an invented name — that is a disclosure, not a defect, and
    # blocking it would push the enricher back toward fabricating.
    region_lc = region.lower()
    if region_lc and any(marker in region_lc for marker in _PROSE_MARKERS):
        problems.append(
            f"Region contains free prose rather than a value "
            f"({region[:60]!r})")

    # 4. Required identity fields.
    for field in ("Company", "Product", "Class", "Date", "Source", "URL"):
        if _blank(row.get(field)):
            problems.append(f"{field} is empty")

    # 5. Date shape. Value-range checks stay with merge_master's
    #    _MIN_VALID_DATE; this only catches malformed dates.
    date = str(row.get("Date") or "").strip()
    if date and not re.match(r"^\d{4}-\d{2}-\d{2}$", date):
        problems.append(f"Date {date!r} is not YYYY-MM-DD")

    # 6. URL must point at a specific notice, not a regulator landing page.
    url = str(row.get("URL") or "").strip()
    if url:
        if not url.startswith(("http://", "https://")):
            problems.append(f"URL {url[:60]!r} is not absolute")
        else:
            tail = url.split("?", 1)[0].split("#", 1)[0]
            after_host = tail.split("://", 1)[-1]
            path = ("/" + after_host.split("/", 1)[1]) if "/" in after_host else ""
            path = path.rstrip("/").lower()
            if path in _LANDING_PATHS:
                problems.append(
                    f"URL is a regulator landing page, not a recall notice "
                    f"({url[:80]!r})")

    return problems


def is_publishable(row: Dict[str, Any]) -> bool:
    """Convenience wrapper — True when the row violates no rule."""
    return not publish_blockers(row)
