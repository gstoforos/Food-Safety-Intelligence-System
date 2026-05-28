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


def fetch(limit: int = 50, include_scotland: bool = False) -> list[Record]:
    """
    Fetch recent FSA alerts. By default EXCLUDES Scotland-only alerts
    (those are emitted by scotland.py). An alert tagged for multiple
    countries including England/Wales/NI is kept here.
    """
    data = get_json(API, params={"_limit": limit, "_sort": "-created"})
    items = data.get("items", [])
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

        rec = Record(
            source_id=notation,
            country_code="sct" if include_scotland else "gb",
            country_name="Scotland" if include_scotland else "United Kingdom",
            authority="FSS" if include_scotland else "FSA",
            title=item.get("title", ""),
            company=str(company),
            product=_product(item),
            hazard=_hazard_text(item),
            alert_type=_extract_type(item),
            published=parse_iso(item.get("created", "")),
            url=item.get("url") or item.get("@id", ""),
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
