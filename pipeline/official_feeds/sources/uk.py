"""
UK source — Food Standards Agency (FSA) Food Alerts API.

Endpoint: https://data.food.gov.uk/food-alerts/id  (JSON, OGL v3.0)
Covers England, Wales, Northern Ireland (Scotland handled separately in
scotland.py per AFTS spec, using country=GB-SCT).

Alert types:
  PRIN  = Product Recall Information Notice  → alert_type 'recall'
  AA    = Allergy Alert                      → alert_type 'allergy'
  FAFA  = Food Alert For Action              → alert_type 'action'

JSON item shape (from API reference + CSV export):
  @id, type, title, shortTitle, status, notation, created, modified, url,
  alertAuthor (company), problem[] {reason, allergen.label, pathogenRisk.label,
  hazardCategory.label}, productDetails[] {productName}, country[]
"""

from __future__ import annotations

from ..base import Record, FeedSource, register
from ..fetch import get_json, parse_iso

API = "https://data.food.gov.uk/food-alerts/id"

_TYPE_MAP = {
    "PRIN": "recall",
    "AA": "allergy",
    "FAFA": "action",
}


def _extract_type(item: dict) -> str:
    """item['type'] is a list of URIs like ['.../def/Alert', '.../def/PRIN']."""
    types = item.get("type", [])
    if isinstance(types, str):
        types = [types]
    for t in types:
        tail = str(t).rstrip("/").split("/")[-1]
        if tail in _TYPE_MAP:
            return _TYPE_MAP[tail]
    return ""


def _first(lst):
    if isinstance(lst, list) and lst:
        return lst[0]
    return lst if lst else {}


def _join_label(node, *keys) -> str:
    """Pull .label or text from a possibly-list node for given keys."""
    if isinstance(node, list):
        node = node[0] if node else {}
    parts = []
    for k in keys:
        v = node.get(k) if isinstance(node, dict) else None
        if isinstance(v, dict):
            v = v.get("label") or v.get("@id", "")
        if isinstance(v, list):
            v = " ".join(str(x.get("label", x) if isinstance(x, dict) else x) for x in v)
        if v:
            parts.append(str(v))
    return " ".join(parts)


def _hazard_text(item: dict) -> str:
    prob = _first(item.get("problem", []))
    if not isinstance(prob, dict):
        return ""
    bits = []
    for key in ("reason", "allergen", "pathogenRisk", "hazardCategory"):
        v = prob.get(key)
        if isinstance(v, dict):
            v = v.get("label") or ""
        elif isinstance(v, list):
            v = " ".join(
                (x.get("label", "") if isinstance(x, dict) else str(x)) for x in v
            )
        if v:
            bits.append(str(v))
    return " ".join(bits)


def _product(item: dict) -> str:
    pd = item.get("productDetails", [])
    if isinstance(pd, list):
        names = []
        for p in pd:
            if isinstance(p, dict) and p.get("productName"):
                names.append(str(p["productName"]))
        return "; ".join(names)
    if isinstance(pd, dict):
        return str(pd.get("productName", ""))
    return ""


def _country_codes(item: dict) -> list[str]:
    c = item.get("country", [])
    if isinstance(c, dict):
        c = [c]
    out = []
    for entry in c:
        if isinstance(entry, dict):
            cid = entry.get("@id", "")
        else:
            cid = str(entry)
        tail = cid.rstrip("/").split("/")[-1]  # e.g. GB-ENG
        if tail:
            out.append(tail)
    return out


def _derive_country(codes: list[str], include_scotland: bool):
    """Map FSA country[] codes → (country_code, country_name, authority).

    The FSA API tags each alert with GB-ENG / GB-WLS / GB-NIR / GB-SCT.
    Previously the record hardcoded GB / United Kingdom / FSA for every
    non-Scotland alert, which mislabelled cross-border notices (e.g. an
    FSAI Irish recall the FSA cross-notifies for Northern Ireland was
    written as 'United Kingdom / FSA' with an fsa-prin URL, duplicating
    the FSAI primary). Derive the jurisdiction from the actual codes.
    """
    up = {c.upper() for c in (codes or [])}
    if include_scotland or up == {"GB-SCT"}:
        return "sct", "Scotland", "FSS"
    # Any England/Wales/Northern-Ireland tag (alone or mixed) → UK/FSA.
    if up & {"GB-ENG", "GB-WLS", "GB-NIR", "GB"}:
        return "gb", "United Kingdom", "FSA"
    # Untagged alerts default to UK/FSA (the API's own scope).
    return "gb", "United Kingdom", "FSA"


def fetch(limit: int = 50, include_scotland: bool = False,
          lookback_days: int = 90) -> list[Record]:
    """
    Fetch recent FSA alerts. By default EXCLUDES Scotland-only alerts
    (those are emitted by scotland.py). An alert tagged for multiple
    countries including England/Wales/NI is kept here.
    """
    # ---------------------------------------------------------------------
    # SILENT SCRAPER FAILURE (found 2026-08-07).
    #
    # This call used to read:
    #     get_json(API, params={"_limit": limit, "_sort": "-created"})
    #
    # The FSA endpoint is an Epimorphics Linked Data API. It accepts NEITHER
    # of those parameters and ignores both silently — no error, no warning,
    # HTTP 200. What comes back is the FIRST page of the collection in its
    # natural order: fifty alerts from January-April 2018.
    #
    # So the scraper ran green every day, returned fifty rows every day, and
    # every one of them was eight years old — dropped downstream by the
    # minimum-date rule. Zero output, zero errors. "FSA (UK)" does not appear
    # in scraper-health.json either, so nothing else flagged it.
    #
    # Cost: no FSA (UK) row entered the database between 2026-07-08 and
    # 2026-08-07. Three in-scope recalls were missed —
    #   FSA-PRIN-36-2026 (21 Jul) Waitrose brioche rolls, hard plastic + metal
    #   FSA-PRIN-37-2026 (22 Jul) Graham's Family Dairy milk, veterinary
    #                             medicines including penicillin
    #   FSA-PRIN-38-2026 (07 Aug) Greencore, three pasta products, Listeria
    #                             monocytogenes
    #
    # The parameter this API actually honours is `min-created`. Verified by
    # hand: min-created=2026-07-01 returns twelve 2026 alerts, while
    # _sort=-created returns FSA-AA-01-2018. A rolling date window is used
    # rather than a page count so the query cannot drift back in time again.
    # ---------------------------------------------------------------------
    from datetime import date, timedelta
    since = (date.today() - timedelta(days=lookback_days)).isoformat()
    data = get_json(API, params={"min-created": since, "_pageSize": limit})
    items = data.get("items", [])

    # A window this wide is never legitimately empty — the FSA publishes
    # several alerts a week. Empty means the query stopped working again,
    # and that has to be loud rather than green.
    if not items:
        raise RuntimeError(
            f"FSA food-alerts returned 0 items for min-created={since} "
            f"(_pageSize={limit}). The FSA publishes several alerts a week, "
            f"so an empty {lookback_days}-day window means the query is "
            f"being ignored — which is exactly how a month of UK recalls was "
            f"lost in July 2026. Check the parameter names against "
            f"https://data.food.gov.uk/food-alerts/ before concluding it was "
            f"a quiet month.")

    # The endpoint ignores unknown parameters rather than rejecting them, so
    # a working filter must be confirmed from the DATA, not the HTTP status.
    newest = max((str(i.get("created") or "") for i in items), default="")
    if newest and newest[:10] < since:
        raise RuntimeError(
            f"FSA food-alerts ignored min-created={since}: the newest item "
            f"returned is {newest[:10]}. This is the July 2026 failure mode "
            f"— the API served its oldest page and the scraper reported "
            f"success.")
    records: list[Record] = []
    for item in items:
        notation = item.get("notation") or item.get("@id", "").split("/")[-1]
        countries = _country_codes(item)
        is_scotland_only = countries == ["GB-SCT"]
        if include_scotland:
            keep = "GB-SCT" in countries
        else:
            keep = (not countries) or (not is_scotland_only)
        if not keep:
            continue

        company = item.get("alertAuthor") or item.get("sender") or ""
        if isinstance(company, dict):
            company = company.get("label", "") or company.get("name", "")

        cc, cname, auth = _derive_country(countries, include_scotland)

        # Prefer the public news-alerts URL over the data.food.gov.uk JSON @id.
        pub_url = item.get("url") or item.get("@id", "")

        rec = Record(
            source_id=notation,
            country_code=cc,
            country_name=cname,
            authority=auth,
            title=item.get("title", ""),
            company=str(company),
            product=_product(item),
            hazard=_hazard_text(item),
            alert_type=_extract_type(item),
            published=parse_iso(item.get("created", "")),
            url=pub_url,
            raw=item,
        )
        records.append(rec)
    return records


UK = FeedSource(
    code="uk",
    name_en="United Kingdom",
    authority_short="FSA",
    fetcher=fetch,
    region="Europe",
    timezone="Europe/London",
    run_local_hour=9,
    cron_utc_offsets=(8, 9),  # 09:00 London = 08:00 UTC (BST) / 09:00 UTC (GMT)
    gnews_authority="FSA Food Standards Agency",
    gnews_terms=("salmonella", "listeria", "E. coli", "botulism",
                 "undeclared allergen"),
    gnews_hl="en-GB", gnews_gl="GB", gnews_ceid="GB:en",
    gnews_days_back=3,
)

register(UK)
