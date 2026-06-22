"""
Canada source — CFIA / Health Canada Recalls & Safety Alerts.

PRIMARY (audit 2026-06-22): scrape the LIVE listing at
  https://recalls-rappels.canada.ca/en?page=,N
This page is current (the bulk open-data JSON lagged — its food records were all
>14 days old) and canada.ca does not bot-block like FDA. Every row carries a
category icon (icon-food.svg / icon-product.svg / icon-health.svg), a link to
its own /alert-recall/<slug> page, and a date. We keep ONLY food rows — the
page lists every category (food, consumer product, health product, vehicle) and
we want food only. The /alert-recall/<slug> link is the authority URL, so food
rows arrive WITH their CFIA URL (no resolver needed).

FALLBACK: the open-data bulk JSON (HCRSAMOpenData.json), used only if the live
listing yields nothing (e.g. markup change).

CFIA food titles name the hazard directly ("X recalled due to Listeria
monocytogenes"), so title-based classification works. Google News (hl=en-CA)
remains the hybrid backstop in main.py.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

from bs4 import BeautifulSoup

from ..base import Record, FeedSource, register
from ..fetch import get_json, get_text, parse_iso

LISTING = "https://recalls-rappels.canada.ca/en"
API = ("https://recalls-rappels.canada.ca/sites/default/files/"
       "opendata-donneesouvertes/HCRSAMOpenData.json")

_DATE_KEY_HINT = ("date", "published", "publish", "updated", "issued",
                  "posted", "modified")
_ISO_RE = re.compile(r"\b20\d{2}[-/]\d{1,2}[-/]\d{1,2}\b")
_DATE_IN_TEXT = re.compile(r"(20\d{2}-\d{2}-\d{2})")


# ── PRIMARY: live listing scrape ───────────────────────────────────────────
def _is_food_row(icon_src: str, cat_text: str) -> bool:
    """A row is food if its icon is the food icon, or the category text names a
    food category. CFIA food rows use icon-food.svg; categories include
    'Food recall warning', 'Food safety', and food 'Notification'."""
    if "icon-food" in (icon_src or "").lower():
        return True
    c = (cat_text or "").lower()
    return "food recall" in c or "food safety" in c


def _scrape_listing(pages: int = 6) -> list[Record]:
    records: list[Record] = []
    seen: set[str] = set()
    for p in range(pages):
        url = f"{LISTING}?page=%2C{p}" if p else LISTING
        html = get_text(url)
        if not html:
            break
        soup = BeautifulSoup(html, "html.parser")
        found_here = 0
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/alert-recall/" not in href:
                continue
            title = a.get_text(" ", strip=True)
            if not title:
                continue
            if href.startswith("/"):
                href = "https://recalls-rappels.canada.ca" + href
            if href in seen:
                continue

            # Walk up to the row container holding the icon + category + date.
            container = a
            cat_text = ""
            for _ in range(5):
                container = container.parent
                if container is None:
                    break
                txt = container.get_text(" ", strip=True)
                if container.find("img") and _DATE_IN_TEXT.search(txt):
                    cat_text = txt
                    break
            icon_src = ""
            if container is not None:
                img = container.find("img")
                if img:
                    icon_src = img.get("src", "")

            if not _is_food_row(icon_src, cat_text):
                continue

            m = _DATE_IN_TEXT.search(cat_text)
            published = parse_iso(m.group(1)) if m else None

            seen.add(href)
            found_here += 1
            records.append(Record(
                source_id=href,
                country_code="ca",
                country_name="Canada",
                authority="CFIA",
                title=title,
                company="",
                product="",
                hazard=title,            # CFIA titles name the hazard
                alert_type="recall",
                region="North America",
                recall_class="",
                outbreak=0,
                published=published,
                url=href,                # OFFICIAL CFIA /alert-recall page
                raw={"icon": icon_src, "cat": cat_text},
            ))
        # Stop paginating once a page yields no food rows (older pages only).
        if found_here == 0 and p >= 1:
            break
    return records


# ── FALLBACK: open-data bulk JSON ──────────────────────────────────────────
def _val(rec: dict, *keys):
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
    best = None

    def consider(s):
        nonlocal best
        dt = parse_iso(str(s))
        if dt is None:
            m = _ISO_RE.search(str(s))
            if m:
                dt = parse_iso(m.group(0))
        if dt is not None and (best is None or dt > best):
            best = dt

    for k, v in rec.items():
        hinted = any(h in str(k).lower() for h in _DATE_KEY_HINT)
        if isinstance(v, dict):
            for vv in v.values():
                if hinted or (isinstance(vv, str) and _ISO_RE.search(vv)):
                    consider(vv)
        elif isinstance(v, list):
            for vv in v:
                if hinted or (isinstance(vv, str) and _ISO_RE.search(vv)):
                    consider(vv)
        elif hinted or (isinstance(v, str) and _ISO_RE.search(v)):
            consider(v)
    return best


def _is_food(category: str, title: str) -> bool:
    blob = f"{category} {title}".lower()
    return "aliment" in blob or "food" in blob


def _fetch_bulk_json(limit: int) -> list[Record]:
    records: list[Record] = []
    try:
        data = get_json(API)
    except Exception as e:  # noqa: BLE001
        print(f"  [WARN] CFIA open-data fetch failed: {e}")
        return records

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
        if not title or not _is_food(category, title):
            continue
        url = _val(it, "url", "Url - En", "URL", "link", "url_en",
                   "Web link - En")
        nid = _val(it, "recallId", "NID", "nid", "id", "recallNumber")
        records.append(Record(
            source_id=f"CFIA-{nid}" if nid else f"CFIA-{abs(hash(url or title))%10**10}",
            country_code="ca", country_name="Canada", authority="CFIA",
            title=title, company="", product="", hazard=title,
            alert_type="recall", region="North America",
            recall_class=_val(it, "recallClass", "Recall class - En", "class"),
            outbreak=0, published=_best_date(it), url=url, raw=it,
        ))
    sentinel = datetime(1970, 1, 1, tzinfo=timezone.utc)
    records.sort(key=lambda r: r.published or sentinel, reverse=True)
    return records[:limit]


def fetch(limit: int = 200) -> list[Record]:
    records = _scrape_listing()
    if records:
        sentinel = datetime(1970, 1, 1, tzinfo=timezone.utc)
        records.sort(key=lambda r: r.published or sentinel, reverse=True)
        print(f"  [CFIA] live listing: {len(records)} food rows")
        return records[:limit]
    print("  [WARN] CFIA live listing yielded no food rows — "
          "falling back to open-data JSON")
    return _fetch_bulk_json(limit)


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
    authority_domain="recalls-rappels.canada.ca",
    authority_url_pattern=r"/alert-recall/[a-z0-9-]{10,}",
    bulk_index_queries=(
        "site:inspection.canada.ca recall food 2026 salmonella",
        "site:inspection.canada.ca recall food 2026 listeria",
        "site:recalls-rappels.canada.ca food 2026",
        "site:inspection.canada.ca food recall warning 2026",
        "site:recalls-rappels.canada.ca recall food",
    ),
)

register(CANADA)
