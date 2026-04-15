"""
news.py — Food Safety News Scraper
Fetches RSS from dedicated food safety publishers.
Last 7 days only. Pathogen mentions only. No AI. No duplicates.
Writes to NEWS sheet in docs/data/recalls.xlsx.
Runs via GitHub Actions every 4 hours.
"""

import re
import hashlib
import logging
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse
import xml.etree.ElementTree as ET

import requests
import openpyxl
from openpyxl import load_workbook

logger = logging.getLogger(__name__)

# ── RSS Sources ────────────────────────────────────────────────────────────────
SOURCES = [
    # Dedicated food safety publishers (highest quality)
    {"name": "Food Safety News",        "url": "https://www.foodsafetynews.com/feed/"},
    {"name": "Food Poisoning Bulletin",  "url": "https://www.foodpoisoningbulletin.com/feed/"},
    {"name": "Outbreak News Today",      "url": "https://outbreaknewstoday.com/feed/"},
    {"name": "Food Safety Magazine",     "url": "https://www.food-safety.com/rss/topic/296-recalls-and-alerts"},
    {"name": "BarfBlog",                 "url": "https://barfblog.com/feed/"},
    # Official regulator RSS
    {"name": "FDA Recalls",              "url": "https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/food-safety-recalls/rss.xml"},
    {"name": "USDA FSIS",               "url": "https://www.fsis.usda.gov/rss/recalls.xml"},
    {"name": "CFIA Canada",             "url": "https://recalls-rappels.canada.ca/en/rss.xml"},
    {"name": "FSA UK",                  "url": "https://www.food.gov.uk/news-alerts/rss/alerts"},
    {"name": "EFSA",                    "url": "https://www.efsa.europa.eu/en/rss/rss.xml"},
    {"name": "CDC Foodborne",           "url": "https://tools.cdc.gov/api/v2/resources/media/277909.rss"},
    {"name": "FSANZ Australia",         "url": "https://www.foodstandards.gov.au/media/rss/pages/food-recalls.aspx"},
    {"name": "MPI New Zealand",         "url": "https://www.mpi.govt.nz/rss/news/"},
    {"name": "RASFF Portal",            "url": "https://webgate.ec.europa.eu/rasff-window/api/notification/listnewrss"},
    {"name": "eFoodAlert",              "url": "https://efoodalert.com/feed/"},
]

# ── Pathogen detection ─────────────────────────────────────────────────────────
PATHOGEN_PATTERNS = [
    (r"listeria",                               "Listeria monocytogenes"),
    (r"salmonella",                             "Salmonella"),
    (r"e\.?\s*coli|stec|o157|shiga.toxin",      "E. coli / STEC"),
    (r"clostridium.botulinum|botulism",         "Clostridium botulinum"),
    (r"clostridium.perfringens",                "Clostridium perfringens"),
    (r"norovirus",                              "Norovirus"),
    (r"hepatitis.a",                            "Hepatitis A"),
    (r"aflatoxin",                              "Aflatoxin"),
    (r"cereulide|bacillus.cereus",              "Cereulide / Bacillus cereus"),
    (r"campylobacter",                          "Campylobacter"),
    (r"vibrio",                                 "Vibrio"),
    (r"cronobacter|sakazakii",                  "Cronobacter"),
    (r"cyclospora",                             "Cyclospora"),
    (r"brucell",                                "Brucella"),
    (r"yersinia",                               "Yersinia"),
    (r"ochratoxin",                             "Ochratoxin A"),
    (r"patulin|mycotoxin",                      "Mycotoxin"),
    (r"saxitoxin|paralytic.shellfish|psp|dsp|asp|lipophilic.biotoxin", "Shellfish toxins"),
]

# ── Event type detection ───────────────────────────────────────────────────────
EVENT_PATTERNS = [
    (r"outbreak",       "Outbreak"),
    (r"recall",         "Recall"),
    (r"advisory|alert", "Advisory"),
    (r"investigation",  "Investigation"),
    (r"warning",        "Warning"),
]

# ── NEWS sheet columns ─────────────────────────────────────────────────────────
NEWS_HEADERS = [
    "Published (UTC)", "Pathogen", "Event", "Source", "Title", "Link", "Retrieved (UTC)"
]

WINDOW_DAYS = 7


def detect_pathogen(text: str) -> str:
    t = text.lower()
    for pattern, canonical in PATHOGEN_PATTERNS:
        if re.search(pattern, t):
            return canonical
    return ""


def detect_event(text: str) -> str:
    t = text.lower()
    for pattern, label in EVENT_PATTERNS:
        if re.search(pattern, t):
            return label
    return "Alert"


def canonical_url(url: str) -> str:
    """Strip query/fragment for dedup purposes."""
    try:
        p = urlparse(url)
        return f"{p.scheme}://{p.netloc}{p.path}".rstrip("/").lower()
    except Exception:
        return url.lower().strip()


def title_hash(title: str) -> str:
    clean = re.sub(r"[^a-z0-9]", "", title.lower())
    return hashlib.md5(clean.encode()).hexdigest()[:12]


def parse_date(entry) -> datetime | None:
    """Try multiple date fields from RSS entry."""
    for field in ["published_parsed", "updated_parsed"]:
        val = getattr(entry, field, None)
        if val:
            try:
                import calendar
                ts = calendar.timegm(val)
                return datetime.fromtimestamp(ts, tz=timezone.utc)
            except Exception:
                pass
    # Try raw string fields
    for field in ["published", "updated"]:
        raw = getattr(entry, field, "")
        if raw:
            for fmt in [
                "%a, %d %b %Y %H:%M:%S %z",
                "%a, %d %b %Y %H:%M:%S GMT",
                "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%dT%H:%M:%SZ",
            ]:
                try:
                    dt = datetime.strptime(raw.strip(), fmt)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt
                except Exception:
                    pass
    return None


def fetch_feed(source: dict, cutoff: datetime) -> list[dict]:
    """Fetch one RSS feed, return list of news row dicts."""
    import feedparser
    rows = []
    try:
        resp = requests.get(source["url"], timeout=15,
                            headers={"User-Agent": "AFTS-FSIS-NewsBot/2.0"})
        if resp.status_code != 200:
            logger.warning(f"[{source['name']}] HTTP {resp.status_code}")
            return []
        feed = feedparser.parse(resp.content)
    except Exception as e:
        logger.warning(f"[{source['name']}] fetch error: {e}")
        return []

    for entry in feed.entries:
        pub = parse_date(entry)
        if pub is None or pub < cutoff:
            continue

        title = getattr(entry, "title", "").strip()
        link  = getattr(entry, "link",  "").strip()
        summary = getattr(entry, "summary", "") or getattr(entry, "description", "") or ""

        combined = f"{title} {summary}"
        pathogen = detect_pathogen(combined)
        if not pathogen:
            continue  # not a pathogen event

        # Skip pure allergen/foreign material items with no pathogen keyword
        if re.search(r"undeclared|allergen|foreign.material|mislabel", combined.lower()):
            if not pathogen:
                continue

        event = detect_event(combined)

        rows.append({
            "published": pub.strftime("%Y-%m-%d %H:%M UTC"),
            "pathogen":  pathogen,
            "event":     event,
            "source":    source["name"],
            "title":     title,
            "link":      link,
        })

    logger.info(f"[{source['name']}] {len(rows)} pathogen items")
    return rows


def load_existing_keys(ws) -> tuple[set, set]:
    """Return sets of (canonical_url, title_hash) already in sheet."""
    urls, titles = set(), set()
    for row in ws.iter_rows(min_row=2, values_only=True):
        if not row or not row[0]:
            continue
        link = str(row[5] or "")
        title = str(row[4] or "")
        urls.add(canonical_url(link))
        titles.add(title_hash(title))
    return urls, titles


def run(xlsx_path: str):
    """Main entry point — update NEWS sheet in xlsx_path."""
    import feedparser  # noqa: confirm installed

    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=WINDOW_DAYS)
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Load workbook
    try:
        wb = load_workbook(xlsx_path)
    except FileNotFoundError:
        logger.error(f"recalls.xlsx not found at {xlsx_path}")
        raise

    # Ensure NEWS sheet exists
    if "NEWS" not in wb.sheetnames:
        ws = wb.create_sheet("NEWS")
        ws.append(NEWS_HEADERS)
    else:
        ws = wb["NEWS"]
        # Ensure header
        if ws.max_row < 1 or ws.cell(1, 1).value != "Published (UTC)":
            ws.insert_rows(1)
            for i, h in enumerate(NEWS_HEADERS, 1):
                ws.cell(1, i).value = h

    existing_urls, existing_titles = load_existing_keys(ws)

    # Fetch all sources
    new_rows = []
    for source in SOURCES:
        rows = fetch_feed(source, cutoff)
        for r in rows:
            cu = canonical_url(r["link"])
            th = title_hash(r["title"])
            if cu in existing_urls or th in existing_titles:
                continue  # duplicate
            existing_urls.add(cu)
            existing_titles.add(th)
            new_rows.append(r)

    if not new_rows:
        logger.info("No new pathogen news items.")
        return 0

    # Sort newest first
    new_rows.sort(key=lambda x: x["published"], reverse=True)

    # Append to sheet (insert after header so newest stays on top)
    insert_at = 2
    ws.insert_rows(insert_at, amount=len(new_rows))
    for i, r in enumerate(new_rows):
        row_num = insert_at + i
        ws.cell(row_num, 1).value = r["published"]
        ws.cell(row_num, 2).value = r["pathogen"]
        ws.cell(row_num, 3).value = r["event"]
        ws.cell(row_num, 4).value = r["source"]
        ws.cell(row_num, 5).value = r["title"]
        ws.cell(row_num, 6).value = r["link"]
        ws.cell(row_num, 7).value = now_str

    # Prune rows older than WINDOW_DAYS (keep sheet lean)
    rows_to_delete = []
    for row in ws.iter_rows(min_row=2):
        pub_val = row[0].value
        if not pub_val:
            continue
        try:
            pub_dt = datetime.strptime(str(pub_val)[:16], "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
            if pub_dt < cutoff:
                rows_to_delete.append(row[0].row)
        except Exception:
            pass

    for row_num in reversed(rows_to_delete):
        ws.delete_rows(row_num)

    wb.save(xlsx_path)
    logger.info(f"NEWS sheet updated: +{len(new_rows)} new, -{len(rows_to_delete)} expired")
    return len(new_rows)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else "docs/data/recalls.xlsx"
    added = run(path)
    print(f"Done. Added {added} new news items.")
