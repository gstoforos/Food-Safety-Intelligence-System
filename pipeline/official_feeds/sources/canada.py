"""
Canada source — CFIA / Health Canada Recalls & Safety Alerts open data.

Endpoint (English open data, ALL recall categories):
  https://recalls-rappels.canada.ca/sites/default/files/opendata-donneesouvertes/HCRSAMOpenData.json

The file contains every recall category (food, consumer product, health
product, vehicle, ...). We filter to FOOD recalls only. CFIA food titles name
the hazard directly ("X brand Y recalled due to Listeria monocytogenes"), so
title-based classification works well. Google News (hl=en-CA) is the hybrid
backstop.

The open-data schema uses bilingual / suffixed keys that have changed over
time, so parsing is intentionally defensive: every field is resolved by trying
several key variants and unwrapping {en/fr} dicts.
"""

from __future__ import annotations

from datetime import datetime, timezone

from ..base import Record, FeedSource, register
from ..fetch import get_json, parse_iso

API = ("https://recalls-rappels.canada.ca/sites/default/files/"
       "opendata-donneesouvertes/HCRSAMOpenData.json")


def _val(rec: dict, *keys):
    """Resolve a field by trying multiple key variants; unwrap en/fr dicts."""
    low = {str(k).lower(): v for k, v in rec.items()}
    for k in keys:
        v = low.get(k.lower())
        if v is None:
            continue
        if isinstance(v, dict):
            for lang in ("en", "english", "En", "EN"):
                if v.get(lang):
                    return str(v[lang])
            # first non-empty value
            for vv in v.values():
                if vv:
                    return str(vv)
        elif isinstance(v, list):
            parts = [str(x) for x in v if x]
            if parts:
                return " ".join(parts)
        elif v:
            return str(v)
    return ""


def _is_food(category: str, title: str) -> bool:
    blob = f"{category} {title}".lower()
    if "aliment" in blob or "food" in blob:
        return True
    return False


def fetch(limit: int = 200) -> list[Record]:
    records: list[Record] = []
    try:
        data = get_json(API)
    except Exception as e:  # noqa: BLE001
        print(f"  [WARN] CFIA open-data fetch failed: {e}")
        return records

    # Find the record list (schema varies: list, or dict with results/records)
    rows = None
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        for key in ("results", "records", "data", "items"):
            if isinstance(data.get(key), list):
                rows = data[key]
                break
        if rows is None:
            for v in data.values():
                if isinstance(v, list) and v and isinstance(v[0], dict):
                    rows = v
                    break
    if not rows:
        print("  [WARN] CFIA: could not locate record list in JSON")
        return records

    # IMPORTANT: the open-data file is ordered oldest-first. We must parse the
    # WHOLE list and sort by date desc, otherwise slicing the first N hits the
    # historical archive (which is why 298/300 were "too old" in v1).
    for it in rows:
        if not isinstance(it, dict):
            continue
        category = _val(it, "category", "Category - En", "Category", "recallCategory",
                        "category_en", "Type - En", "type")
        title = _val(it, "title", "Title - En", "Title", "title_en",
                     "recallTitle", "Product - En")
        if not title:
            continue
        if not _is_food(category, title):
            continue

        url = _val(it, "url", "Url - En", "URL", "link", "url_en",
                   "Web link - En")
        date_s = _val(it, "datePublished", "Date published", "date_published",
                      "Last updated", "publishedDate", "date")
        published = parse_iso(date_s)
        nid = _val(it, "recallId", "NID", "nid", "id", "recallNumber")

        records.append(Record(
            source_id=f"CFIA-{nid}" if nid else f"CFIA-{abs(hash(url or title))%10**10}",
            country_code="ca",
            country_name="Canada",
            authority="CFIA",
            title=title,
            company="",
            product="",
            hazard=title,                 # CFIA titles name the hazard
            alert_type="recall",
            region="North America",
            recall_class=_val(it, "recallClass", "Recall class - En", "class"),
            outbreak=0,
            published=published,
            url=url,
            raw=it,
        ))

    # Sort newest first; records without a date go to the end
    from datetime import datetime, timezone
    sentinel = datetime(1970, 1, 1, tzinfo=timezone.utc)
    records.sort(key=lambda r: r.published or sentinel, reverse=True)
    return records[:limit]


CANADA = FeedSource(
    code="canada",
    name_en="Canada",
    authority_short="CFIA",
    fetcher=fetch,
    region="North America",
    timezone="America/Toronto",
    run_local_hour=9,
    cron_utc_offsets=(13, 14),
    gnews_authority="CFIA Canada food",
    gnews_terms=("salmonella", "listeria", "E. coli", "botulism",
                 "undeclared allergen"),
    gnews_hl="en-CA", gnews_gl="CA", gnews_ceid="CA:en",
    gnews_days_back=3,
    authority_domain="inspection.canada.ca",
    authority_url_pattern=r"(recall-alert|food-recall)/[a-z0-9-]{10,}",
    authority_index_urls=(
        "https://recalls-rappels.canada.ca/en/search/site?f%5B0%5D=type%3Arecall_food",
        "https://inspection.canada.ca/food-safety-for-industry/food-recalls-and-allergy-alerts/eng/1351519587174/1351519588221",
    ),
)

register(CANADA)
