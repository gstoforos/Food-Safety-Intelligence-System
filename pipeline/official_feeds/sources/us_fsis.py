"""
US source — USDA FSIS Recall API (meat, poultry, egg products).

Endpoint: https://www.fsis.usda.gov/fsis/api/recall/v/1
Returns a JSON array of recall objects with Drupal field names.

NOTE: FSIS is already ingested by the AFTS core FSIS platform. This source is
included for architectural completeness and as a cross-check; the URL-based
dedup in main.py skips anything the core pipeline already wrote to
Pending/Recalls, so no duplication occurs.

Fields: field_title, field_recall_number, field_recall_date,
field_recall_classification ("Class I"/"Class II"/"Class III"/"Public Health
Alert"), field_recall_reason, field_summary (HTML), field_product_items,
field_establishment, field_related_to_outbreak ("0"/"1"), field_active_notice,
field_states, field_year, field_en_press_release.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

from ..base import Record, FeedSource, register
from ..fetch import get_json

API = "https://www.fsis.usda.gov/fsis/api/recall/v/1"

_TAG_RE = re.compile(r"<[^>]+>")


def _strip_html(s: str) -> str:
    return _TAG_RE.sub(" ", s or "").replace("&nbsp;", " ").strip()


def _truthy(v) -> bool:
    return str(v).strip().lower() in ("1", "true", "yes")


def _parse_fsis_date(s: str):
    s = _strip_html(s)
    if not s:
        return None
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _field(it: dict, key: str) -> str:
    """FSIS fields are sometimes scalars, sometimes lists of dicts."""
    v = it.get(key)
    if v is None:
        return ""
    if isinstance(v, list):
        parts = []
        for x in v:
            if isinstance(x, dict):
                parts.append(str(x.get("name", x.get("value", ""))))
            else:
                parts.append(str(x))
        return " ".join(p for p in parts if p)
    if isinstance(v, dict):
        return str(v.get("name", v.get("value", "")))
    return str(v)


def fetch(limit: int = 100) -> list[Record]:
    records: list[Record] = []
    try:
        data = get_json(API)
    except Exception as e:  # noqa: BLE001
        print(f"  [WARN] FSIS fetch failed: {e}")
        return records

    rows = data if isinstance(data, list) else data.get("results", data.get("data", []))
    for it in rows[:limit] if isinstance(rows, list) else []:
        rn = _field(it, "field_recall_number")
        title = _strip_html(_field(it, "field_title"))
        reason = _strip_html(_field(it, "field_recall_reason"))
        summary = _strip_html(_field(it, "field_summary"))[:600]
        product = _strip_html(_field(it, "field_product_items"))[:300]
        cls = _strip_html(_field(it, "field_recall_classification"))
        published = _parse_fsis_date(_field(it, "field_recall_date"))
        outbreak = 1 if _truthy(_field(it, "field_related_to_outbreak")) else 0

        company = _strip_html(_field(it, "field_establishment"))
        if not company:
            m = re.search(r"^([\w&.''\- ]{2,60}?)\s+Recalls\b", title)
            if m:
                company = m.group(1).strip()

        press = _field(it, "field_en_press_release")
        url = ""
        if press.startswith("http"):
            url = press
        elif rn:
            url = f"https://www.fsis.usda.gov/recalls-alerts?search={rn}"

        alert = "action" if "alert" in cls.lower() else "recall"

        rec = Record(
            source_id=rn or title[:40],
            country_code="us",
            country_name="United States",
            authority="USDA FSIS",
            title=title,
            company=company,
            product=product,
            hazard=" ".join(p for p in (title, reason, summary) if p),
            alert_type=alert,
            region="North America",
            recall_class=cls,
            outbreak=outbreak,
            published=published,
            url=url,
            raw=it,
        )
        records.append(rec)
    return records


US_FSIS = FeedSource(
    code="us_fsis",
    name_en="United States",
    authority_short="USDA FSIS",
    fetcher=fetch,
    region="North America",
    timezone="America/New_York",
    run_local_hour=9,
    cron_utc_offsets=(13, 14),
    gnews_authority="USDA FSIS meat poultry",
    gnews_terms=("salmonella", "listeria", "E. coli O157", "undeclared allergen"),
    gnews_hl="en-US", gnews_gl="US", gnews_ceid="US:en",
    gnews_days_back=3,
)

register(US_FSIS)
