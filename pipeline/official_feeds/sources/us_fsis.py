"""
US source — USDA FSIS Recall API + HTML scrape fallback.

Endpoint: https://www.fsis.usda.gov/fsis/api/recall/v/1
Fallback: https://www.fsis.usda.gov/recalls (HTML scrape) when the API is
blocked by FSIS's WAF (returns 403 from cloud / GitHub Actions IPs).

NOTE: FSIS is already ingested by the AFTS core FSIS platform. This source is
included for architectural completeness; the URL-based dedup in main.py skips
anything the core pipeline already wrote to Pending/Recalls, so no duplication.

API fields: field_title, field_recall_number, field_recall_date,
field_recall_classification, field_recall_reason, field_summary,
field_product_items, field_establishment, field_related_to_outbreak,
field_active_notice, field_en_press_release.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

from ..base import Record, FeedSource, register
from ..fetch import get_json, DEFAULT_HEADERS, TIMEOUT

API = "https://www.fsis.usda.gov/fsis/api/recall/v/1"
SCRAPE_URL = "https://www.fsis.usda.gov/recalls"
SCRAPE_BASE = "https://www.fsis.usda.gov"

_TAG_RE = re.compile(r"<[^>]+>")


def _strip_html(s: str) -> str:
    return _TAG_RE.sub(" ", s or "").replace("&nbsp;", " ").strip()


def _truthy(v) -> bool:
    return str(v).strip().lower() in ("1", "true", "yes")


def _parse_fsis_date(s: str):
    s = _strip_html(s)
    if not s:
        return None
    # Pull a date substring out of free text if needed
    m = re.search(
        r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s+\d{1,2},?\s+\d{4}",
        s)
    if m:
        s = m.group(0)
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%b %d %Y", "%B %d %Y",
                "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _field(it: dict, key: str) -> str:
    """FSIS API fields are sometimes scalars, sometimes lists of dicts."""
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


def _record_from_api(it: dict) -> Record:
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

    return Record(
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


# ─── HTML scrape fallback ────────────────────────────────────────────────────

def _scrape_fsis_recalls(limit: int = 50) -> list[Record]:
    """
    Defensive HTML scrape of https://www.fsis.usda.gov/recalls.
    Used only when the API 403s. Parses the recall listing into Records.
    """
    print(f"  [INFO] Falling back to HTML scrape of {SCRAPE_URL}")
    try:
        r = requests.get(SCRAPE_URL, headers=DEFAULT_HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
    except Exception as e:  # noqa: BLE001
        print(f"  [WARN] FSIS HTML scrape failed: {e}")
        return []

    soup = BeautifulSoup(r.content, "html.parser")
    records: list[Record] = []
    seen = set()

    # FSIS uses Drupal Views; recall rows commonly carry class 'views-row'.
    # Fall back to any link pointing at /recalls-alerts/<slug> if not found.
    items = soup.select("article.recall, .views-row, tr.recall-row, div.recall")
    if not items:
        anchors = soup.find_all("a", href=re.compile(r"/recalls-alerts/[^?#]+"))
        # Group by anchor; each anchor's row is its parent
        items = [a.find_parent(["article", "tr", "li", "div"]) or a for a in anchors]

    _CLASS_TOKENS = ("Public Health Alert", "Class III", "Class II", "Class I")

    for el in items:
        link = el.find("a", href=re.compile(r"/recalls-alerts/[^?#]+")) \
            if hasattr(el, "find") else None
        if link is None and getattr(el, "name", "") == "a":
            link = el
        href = (link.get("href") if link else "") or ""
        if href.startswith("/"):
            href = SCRAPE_BASE + href

        title_el = el.find(["h2", "h3", "h4"]) if hasattr(el, "find") else None
        title = (title_el.get_text(" ", strip=True) if title_el else "")
        if not title and link is not None:
            title = link.get_text(" ", strip=True)
        title = re.sub(r"\s+", " ", title).strip()
        if not title or len(title) < 15:
            continue

        slug = href.rsplit("/", 1)[-1] if href else title[:60]
        if slug in seen:
            continue
        seen.add(slug)

        # Surrounding text often carries date + class label
        ctx = el.get_text(" ", strip=True) if hasattr(el, "get_text") else title
        cls = ""
        for tok in _CLASS_TOKENS:
            if tok in ctx:
                cls = tok
                break
        published = _parse_fsis_date(ctx)

        # Company before "Recalls", reason after "Due to"
        company = ""
        m = re.search(r"^([\w&.''\- ]{2,60}?)\s+Recalls\b", title)
        if m:
            company = m.group(1).strip()
        hazard = title
        m2 = re.search(r"\bDue to (.+?)(?:\.|$)", title)
        if m2:
            hazard = title + " — " + m2.group(1).strip()

        # FSIS recall numbers like "021-2026" appear near the title
        m_rn = re.search(r"\b(\d{3}-\d{4}|FSIS[-\s]\w[-\w]*)\b", ctx)
        rn = m_rn.group(1) if m_rn else ""

        alert = "action" if "Public Health Alert" in cls else "recall"

        records.append(Record(
            source_id=rn or slug,
            country_code="us",
            country_name="United States",
            authority="USDA FSIS",
            title=title,
            company=company,
            product="",
            hazard=hazard,
            alert_type=alert,
            region="North America",
            recall_class=cls,
            outbreak=0,
            published=published,
            url=href,
            raw={"source": "html_scrape", "context": ctx[:200]},
        ))
        if len(records) >= limit:
            break
    print(f"  [INFO] HTML scrape returned {len(records)} records")
    return records


def fetch(limit: int = 100) -> list[Record]:
    # Try API first
    try:
        data = get_json(API)
        records: list[Record] = []
        rows = data if isinstance(data, list) else \
            data.get("results", data.get("data", []))
        if isinstance(rows, list):
            for it in rows[:limit]:
                records.append(_record_from_api(it))
        if records:
            return records
        print("  [INFO] FSIS API returned no records — trying HTML fallback")
    except Exception as e:  # noqa: BLE001
        print(f"  [WARN] FSIS API failed: {e}")

    # HTML scrape fallback
    return _scrape_fsis_recalls(limit=limit)


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
