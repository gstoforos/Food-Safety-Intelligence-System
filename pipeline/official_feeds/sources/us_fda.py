"""
US source — FDA food recalls via the openFDA Food Enforcement API.

  https://api.fda.gov/food/enforcement.json   (no API key; structured)

WHY openFDA ONLY (audit 2026-06-22): every free FDA real-time feed is either
WAF-blocked or dead from a cloud runner —
  • www.fda.gov RSS / recalls page  → Akamai bot-defense (redirects to an
    abuse-detection page); blocked even with Chrome-TLS impersonation.
  • recalls.gov FDA feed reader      → frozen (stale > 1 year).
  • healthdata.gov Socrata mirror    → stale.
  • foodsafety.gov widget            → JS-rendered, no machine feed.
api.fda.gov itself is NOT blocked, so openFDA is the reliable structured base.
It lags (official weekly batch), so REAL-TIME coverage comes from the Google
News supplement in main.py, and the resolver agent (Stage 3b) finds the
official fda.gov URL for each accepted headline. This mirrors the documented
fallback: take the recall list from FDA's structured data, then search for the
authority URL.
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
        data = get_json(API, params={"sort": "report_date:desc", "limit": limit})
    except Exception as e:  # noqa: BLE001
        print(f"  [WARN] openFDA fetch failed: {e}")
        return records

    for it in data.get("results", []):
        rn = it.get("recall_number", "")
        published = (_parse_fda_date(it.get("recall_initiation_date", ""))
                     or _parse_fda_date(it.get("report_date", "")))
        product = (it.get("product_description", "") or "")[:300]
        records.append(Record(
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
            # openFDA rows have no public page; Stage-3b resolves the fda.gov URL.
            url=(f"https://api.fda.gov/food/enforcement.json?search="
                 f"recall_number.exact:%22{rn}%22") if rn else "",
            raw=it,
        ))

    sentinel = datetime(1970, 1, 1, tzinfo=timezone.utc)
    records.sort(key=lambda r: r.published or sentinel, reverse=True)
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
    authority_domain="fda.gov",
    authority_url_pattern=r"safety/recalls-market-withdrawals-safety-alerts/[a-z0-9-]{30,}",
    # ─── AFTS North America Recall Agent (Phase 1: FDA) ────────────────
    # Stage 3b routes through pipeline/official_feeds/agents/north_america.py
    # instead of the legacy DDG resolver. Same key as gap_finder_claude.py.
    market_agent="north_america",
    regulator_code="FDA",
    bulk_index_queries=(
        "site:fda.gov recalls 2026 salmonella",
        "site:fda.gov recalls 2026 listeria",
        "site:fda.gov recalls market withdrawals 2026",
        "site:fda.gov recalls 2026 cheese pizza",
        "site:fda.gov press release recall 2026",
    ),
)

register(US_FDA)
