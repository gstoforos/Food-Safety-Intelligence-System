"""
Canada source — CFIA / Health Canada Recalls & Safety Alerts open data.

Endpoint (English open data, ALL recall categories, updated daily):
  https://recalls-rappels.canada.ca/sites/default/files/opendata-donneesouvertes/HCRSAMOpenData.json

The file contains every recall category (food, consumer product, health
product, vehicle, ...). We filter to FOOD recalls only. CFIA food titles name
the hazard directly ("X brand Y recalled due to Listeria monocytogenes"), so
title-based classification works well. Google News (hl=en-CA) is the hybrid
backstop.

DATE HANDLING (audit 2026-06-21): the open-data schema uses bilingual /
suffixed / renamed date keys that have drifted over time. The previous version
read the FIRST matching key, which on the current schema resolved to a stale
field — so sorting put old records on top and the genuinely-recent food recalls
(e.g. 2026-06-19) got sliced out of the top-N before the age filter ever saw
them (symptom: "200 official records, all too old"). We now compute the MOST
RECENT parseable date across every date-like field of each record, so sorting
and the downstream age filter use the recall's real publication/update date.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

from ..base import Record, FeedSource, register
from ..fetch import get_json, parse_iso

API = ("https://recalls-rappels.canada.ca/sites/default/files/"
       "opendata-donneesouvertes/HCRSAMOpenData.json")

_DATE_KEY_HINT = ("date", "published", "publish", "updated", "issued",
                  "posted", "modified")
_ISO_RE = re.compile(r"\b20\d{2}[-/]\d{1,2}[-/]\d{1,2}\b")


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


def _best_date(rec: dict):
    """Most recent parseable date across all date-like fields of the record.

    Recalls carry several dates (published / last-updated / created); the recall
    becomes public at the latest of them, and 'is this recent?' is best answered
    by the max. Robust to whichever key the current schema uses.
    """
    best = None

    def consider(s):
        nonlocal best
        dt = parse_iso(str(s))
        if dt is not None and (best is None or dt > best):
            best = dt

    for k, v in rec.items():
        if not any(h in str(k).lower() for h in _DATE_KEY_HINT):
            continue
        if isinstance(v, dict):
            for vv in v.values():
                consider(vv)
        elif isinstance(v, list):
            for vv in v:
                consider(vv)
        else:
            consider(v)

    # Fallback: scan all string values for an ISO-ish date substring.
    if best is None:
        for v in rec.values():
            if isinstance(v, str):
                m = _ISO_RE.search(v)
                if m:
                    consider(m.group(0))
    return best


def _is_food(category: str, title: str) -> bool:
    blob = f"{category} {title}".lower()
    return "aliment" in blob or "food" in blob


def fetch(limit: int = 200) -> list[Record]:
    records: list[Record] = []
    try:
        data = get_json(API)
    except Exception as e:  # noqa: BLE001
        print(f"  [WARN] CFIA open-data fetch failed: {e}")
        return records

    # Locate the record list (schema varies: list, or dict with
    # results/records, possibly nested under results.ALL / EN).
    rows = None
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        for key in ("results", "records", "data", "items"):
            v = data.get(key)
            if isinstance(v, list):
                rows = v
                break
            if isinstance(v, dict):
                for sub in ("ALL", "All", "all", "EN", "En", "en"):
                    if isinstance(v.get(sub), list):
                        rows = v[sub]
                        break
                if rows is not None:
                    break
        if rows is None:
            for v in data.values():
                if isinstance(v, list) and v and isinstance(v[0], dict):
                    rows = v
                    break
    if not rows:
        print("  [WARN] CFIA: could not locate record list in JSON")
        return records

    for it in rows:
        if not isinstance(it, dict):
            continue
        category = _val(it, "category", "Category - En", "Category",
                        "recallCategory", "category_en", "Type - En", "type")
        title = _val(it, "title", "Title - En", "Title", "title_en",
                     "recallTitle", "Product - En")
        if not title:
            continue
        if not _is_food(category, title):
            continue

        url = _val(it, "url", "Url - En", "URL", "link", "url_en",
                   "Web link - En")
        published = _best_date(it)
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

    # Sort newest first; records without a date go to the end.
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
    bulk_index_queries=(
        "site:inspection.canada.ca recall food 2026 salmonella",
        "site:inspection.canada.ca recall food 2026 listeria",
        "site:recalls-rappels.canada.ca food 2026",
        "site:inspection.canada.ca food recall warning 2026",
        "site:recalls-rappels.canada.ca recall food",
    ),
)

register(CANADA)
