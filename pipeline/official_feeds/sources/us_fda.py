"""
US source — FDA openFDA Food Enforcement API.

Endpoint: https://api.fda.gov/food/enforcement.json
Covers all FDA-regulated food (seafood, produce, dairy, packaged, supplements)
— i.e. everything USDA FSIS does NOT cover (FSIS = meat/poultry/egg only).
This is the real US value-add. No API key needed. Updated weekly.

Result fields: recalling_firm, product_description, reason_for_recall,
classification ("Class I"/"Class II"/"Class III"), recall_initiation_date
(YYYYMMDD), report_date (YYYYMMDD), status, distribution_pattern, city, state,
country, recall_number, voluntary_mandated.
"""

from __future__ import annotations

from datetime import datetime, timezone

from ..base import Record, FeedSource, register
from ..fetch import get_json

API = "https://api.fda.gov/food/enforcement.json"


def _parse_fda_date(s: str):
    s = (s or "").strip()
    if len(s) == 8 and s.isdigit():            # YYYYMMDD
        try:
            return datetime(int(s[:4]), int(s[4:6]), int(s[6:8]),
                            tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


def fetch(limit: int = 100) -> list[Record]:
    records: list[Record] = []
    try:
        data = get_json(API, params={"sort": "report_date:desc",
                                     "limit": limit})
    except Exception as e:  # noqa: BLE001
        print(f"  [WARN] openFDA fetch failed: {e}")
        return records

    for it in data.get("results", []):
        rn = it.get("recall_number", "")
        published = (_parse_fda_date(it.get("recall_initiation_date", ""))
                     or _parse_fda_date(it.get("report_date", "")))
        product = (it.get("product_description", "") or "")[:300]
        rec = Record(
            source_id=rn or it.get("event_id", ""),
            country_code="us",
            country_name="United States",
            authority="FDA",
            title=product[:120] if product else (it.get("reason_for_recall", "")[:120]),
            company=it.get("recalling_firm", ""),
            product=product,
            hazard=it.get("reason_for_recall", ""),
            alert_type="recall",
            region="North America",
            recall_class=it.get("classification", ""),   # "Class I" etc.
            outbreak=0,
            published=published,
            url=(f"https://api.fda.gov/food/enforcement.json?search="
                 f"recall_number.exact:%22{rn}%22") if rn else "",
            raw=it,
        )
        records.append(rec)
    return records


US_FDA = FeedSource(
    code="us_fda",
    name_en="United States",
    authority_short="FDA",
    fetcher=fetch,
    region="North America",
    timezone="America/New_York",
    run_local_hour=9,
    cron_utc_offsets=(13, 14),  # 09:00 ET = 13:00 UTC (EDT) / 14:00 UTC (EST)
    gnews_authority="FDA US food",
    gnews_terms=("salmonella", "listeria", "E. coli", "botulism",
                 "cyclospora", "undeclared allergen"),
    gnews_hl="en-US", gnews_gl="US", gnews_ceid="US:en",
    gnews_days_back=3,
)

register(US_FDA)
