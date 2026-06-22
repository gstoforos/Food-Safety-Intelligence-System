"""
US source — FDA food recalls, from TWO complementary official feeds:

1. openFDA Food Enforcement API (structured, authoritative, but LAGS by weeks):
     https://api.fda.gov/food/enforcement.json
   Gives classification ("Class I/II/III"), recalling firm, reason. No API key.

2. FDA Food-Safety Recalls RSS (REAL-TIME press releases, audit 2026-06-21):
     https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/food-safety/rss.xml
   This is what the live "Recalls, Market Withdrawals & Safety Alerts" page
   publishes. Each item already carries the official fda.gov press-release URL,
   so RSS-sourced rows arrive WITH their authority URL and skip the Stage-3b
   resolver entirely (no agent fuzzy-cache mis-resolution — that's what put a
   2025 Listeria-pasta URL on a 2026 Alfredo-Salmonella recall).

openFDA covers everything FSIS does NOT (FSIS = meat/poultry/egg only). The two
feeds are merged and de-duplicated; Google News (hl=en-US) remains the hybrid
backstop in main.py.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

from ..base import Record, FeedSource, register
from ..fetch import get_json, get_rss

API = "https://api.fda.gov/food/enforcement.json"
RSS_URL = ("https://www.fda.gov/about-fda/contact-fda/stay-informed/"
           "rss-feeds/food-safety/rss.xml")

# Hazard cues to lift a Tier-relevant hazard out of an RSS title/description.
_HAZARD_RE = re.compile(
    r"(listeria(?:\s+monocytogenes)?|salmonella|botulism|clostridium\s+botulinum|"
    r"botulinum|e\.?\s*coli|escherichia\s+coli|cronobacter|hepatitis\s*a|"
    r"norovirus|cyclospora|staphylococc\w*|bacillus\s+cereus|cereulide|"
    r"aflatoxin|undeclared\s+\w+|foreign\s+(?:material|matter)|"
    r"lead|cesium|metal)",
    re.IGNORECASE)


def _parse_fda_date(s: str):
    s = (s or "").strip()
    if len(s) == 8 and s.isdigit():            # YYYYMMDD (openFDA)
        try:
            return datetime(int(s[:4]), int(s[4:6]), int(s[6:8]),
                            tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


def _fetch_openfda(limit: int) -> list[Record]:
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
    return records


def _fetch_rss() -> list[Record]:
    """Real-time FDA food-safety recalls. Each item already has its fda.gov URL.

    NOTE (audit 2026-06-22): www.fda.gov sits behind Akamai bot defense that
    redirects automated requests to /apology_objects/abuse-detection-apology.html
    even with Chrome-TLS impersonation, so this feed is often unavailable from
    cloud runners (api.fda.gov is NOT blocked, so openFDA still works). We try
    ONCE and degrade quietly — openFDA + Google News + the resolver agent carry
    FDA coverage when the RSS is blocked.
    """
    records: list[Record] = []
    items = get_rss(RSS_URL, retries=1)
    if not items:
        print("  [INFO] FDA food-safety RSS unavailable (WAF) — "
              "using openFDA + Google News")
        return records
    for it in items:
        title = (it.get("title") or "").strip()
        link = (it.get("link") or "").strip()
        if not title or "fda.gov" not in link:
            continue
        desc = (it.get("description") or "").strip()
        m = _HAZARD_RE.search(f"{title} {desc}")
        hazard = m.group(0) if m else title
        records.append(Record(
            source_id=link,                      # URL is the stable identity
            country_code="us",
            country_name="United States",
            authority="FDA",
            title=title[:160],
            company="",
            product=title[:300],
            hazard=hazard,
            alert_type="recall",
            region="North America",
            recall_class="",
            outbreak=0,
            published=it.get("published"),
            url=link,                            # OFFICIAL fda.gov press release
            raw={"rss_title": title, "rss_desc": desc, "rss_link": link},
        ))
    return records


def fetch(limit: int = 100) -> list[Record]:
    """Merge real-time RSS + structured openFDA, de-duplicated.

    RSS rows win on duplicates because they carry the official fda.gov URL.
    Dedup key = normalized title (first 60 chars) and, when present, the
    openFDA recall number.
    """
    rss = _fetch_rss()
    api = _fetch_openfda(limit)

    out: list[Record] = []
    seen: set[str] = set()

    def norm(t: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", (t or "").lower())[:60]

    for rec in rss + api:          # RSS first → preferred on collision
        keys = {norm(rec.title)}
        if rec.source_id:
            keys.add(f"id:{rec.source_id.lower()}")
        if any(k in seen for k in keys):
            continue
        seen |= keys
        out.append(rec)

    # Newest first; undated to the end.
    sentinel = datetime(1970, 1, 1, tzinfo=timezone.utc)
    out.sort(key=lambda r: r.published or sentinel, reverse=True)
    return out


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
