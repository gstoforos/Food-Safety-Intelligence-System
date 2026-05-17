"""
FSIS scraper diagnostic — FDA + CFIA live-endpoint probe.

Run from repo root:
    python diagnose_scrapers.py

What it does
------------
Hits every endpoint the FDA + CFIA scrapers depend on, using the exact
HTTP headers those scrapers use (Akamai-bypass for FDA, plain UA for
CFIA). Reports for each endpoint:
  * HTTP status
  * Response size + elapsed time
  * Whether the body contains the three target recall slugs:
       - Blackstone Products Parmesan Ranch (FDA, May 15, Salmonella)
       - Shirreza brand Tahini Halva (CFIA, May 15, Salmonella)
       - Kyan Culture / Farm Boy microgreens (CFIA, May 15, E. coli)
  * For CFIA open-data JSON: count of CFIA pathogen rows in the
    last 7 days, and whether Shirreza + Kyan are present as
    structured records.
  * Verdict: OK / EMPTY / BLOCKED / 404 / SCHEMA-DRIFT / NETWORK

No pipeline imports. No env vars required. No state written.
Standalone — works even if the rest of the repo is broken.
"""
from __future__ import annotations
import json
import sys
import time
from datetime import datetime, timedelta, timezone

try:
    import requests
except ImportError:
    print("ERROR: `requests` not installed. Install with: pip install requests")
    sys.exit(2)


# ── Target slugs we expect to find in healthy endpoints ─────────────────
TARGETS = {
    "Blackstone (FDA, May 15)": "blackstone-products-recalls-parmesan-ranch-seasoning",
    "Shirreza   (CFIA, May 15)": "shirreza-brand-tahini-halva-date-sap",
    "Kyan       (CFIA, May 15)": "kyan-culture-brand-and-farm-boy",
}


# ── HTTP headers ────────────────────────────────────────────────────────
# Plain Chrome 127 UA for non-FDA endpoints (CFIA, openFDA).
PLAIN_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/127.0.0.0 Safari/537.36"
)
PLAIN_HEADERS = {"User-Agent": PLAIN_UA}

# FDA Akamai-bypass headers — must match scrapers/north_america/fda_press.py
# _AKAMAI_BYPASS_HEADERS exactly. Akamai cross-checks UA, sec-ch-ua-*, and
# sec-fetch-* together; missing any one yields 404 from AkamaiNetStorage.
AKAMAI_HEADERS_HTML = {
    "User-Agent": PLAIN_UA,
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "sec-ch-ua": '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
    "Cache-Control": "max-age=0",
}
AKAMAI_HEADERS_RSS = dict(AKAMAI_HEADERS_HTML)
AKAMAI_HEADERS_RSS["Accept"] = (
    "application/rss+xml, application/xml;q=0.9, text/xml;q=0.8, */*;q=0.5"
)
AKAMAI_HEADERS_JSON = dict(AKAMAI_HEADERS_HTML)
AKAMAI_HEADERS_JSON["Accept"] = "application/json, */*;q=0.5"


# ── Endpoint catalogue ──────────────────────────────────────────────────
ENDPOINTS = [
    # name, url, headers, kind, owning_scraper
    (
        "FDA press RSS (food-safety-recalls)",
        "https://www.fda.gov/about-fda/contact-fda/stay-informed/"
        "rss-feeds/food-safety-recalls/rss.xml",
        AKAMAI_HEADERS_RSS,
        "rss",
        "scrapers/north_america/fda_press.py",
    ),
    (
        "FDA datatables endpoint #1 (?_format=json on listing)",
        "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts?_format=json",
        AKAMAI_HEADERS_JSON,
        "json",
        "scrapers/north_america/fda_datatables.py",
    ),
    (
        "FDA datatables endpoint #2 (datatables/json)",
        "https://www.fda.gov/datatables/json/recalls-market-withdrawals-safety-alerts",
        AKAMAI_HEADERS_JSON,
        "json",
        "scrapers/north_america/fda_datatables.py",
    ),
    (
        "FDA datatables endpoint #3 (datatables-data)",
        "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/"
        "datatables-data?_format=json",
        AKAMAI_HEADERS_JSON,
        "json",
        "scrapers/north_america/fda_datatables.py",
    ),
    (
        "openFDA enforcement (last 14 days)",
        (
            "https://api.fda.gov/food/enforcement.json"
            "?search=recall_initiation_date:["
            + (datetime.utcnow() - timedelta(days=14)).strftime("%Y%m%d")
            + "+TO+99991231]&limit=100"
        ),
        PLAIN_HEADERS,
        "json",
        "scrapers/north_america/fda.py",
    ),
    (
        "CFIA open-data JSON (HCRSAMOpenData)",
        "https://recalls-rappels.canada.ca/sites/default/files/"
        "opendata-donneesouvertes/HCRSAMOpenData.json",
        PLAIN_HEADERS,
        "json",
        "scrapers/north_america/cfia.py  (L1)",
    ),
    (
        "CFIA RSS — canonical feed slug",
        "https://recalls-rappels.canada.ca/en/feed/cfia-alerts-recalls",
        PLAIN_HEADERS,
        "rss",
        "scrapers/north_america/cfia.py  (L2)",
    ),
    (
        "CFIA listing HTML",
        "https://recalls-rappels.canada.ca/en?page=%2C1",
        PLAIN_HEADERS,
        "html",
        "scrapers/north_america/cfia.py  (L3)",
    ),
]


# ── Helpers ─────────────────────────────────────────────────────────────
def human_size(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    if n < 1024 * 1024:
        return f"{n/1024:.1f} KB"
    return f"{n/1024/1024:.2f} MB"


def probe(url: str, headers: dict, timeout: int = 30):
    """Hit url, return (status, elapsed_s, body_bytes, err)."""
    t0 = time.time()
    try:
        r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        return r.status_code, time.time() - t0, r.content, None
    except requests.exceptions.RequestException as e:
        return None, time.time() - t0, b"", str(e)


def find_targets(body: str) -> dict:
    """For each TARGET slug, return True/False whether it appears in body (case-insensitive)."""
    lo = body.lower()
    return {label: slug in lo for label, slug in TARGETS.items()}


def cfia_jsonl_analysis(body: bytes):
    """Parse CFIA HCRSAMOpenData.json, extract pathogen-recall stats."""
    try:
        data = json.loads(body.decode("utf-8", errors="replace"))
    except Exception as e:
        return {"error": f"JSON parse failed: {e}"}
    if not isinstance(data, list):
        return {"error": f"unexpected schema, top-level type={type(data).__name__}"}

    pathogen_tokens = (
        "listeria", "salmonella", "e. coli", "e.coli", "escherichia coli",
        "stec", "clostridium", "botulin", "staphylococcus", "bacillus cereus",
        "cereulide", "cronobacter", "norovirus", "hepatitis", "vibrio",
        "cyclospora", "shigella", "campylobacter", "yersinia",
        "marine biotoxin", "biotoxin", "microbiological",
    )

    cutoff = datetime.utcnow().date() - timedelta(days=7)
    total = 0
    cfia_total = 0
    cfia_active = 0
    cfia_pathogen_active = 0
    recent_pathogen_rows = []
    shirreza_row = None
    kyan_row = None

    for row in data:
        total += 1
        org = (row.get("Organization") or "").strip()
        if org != "CFIA":
            continue
        cfia_total += 1
        if str(row.get("Archived", "0")).strip() != "0":
            continue
        cfia_active += 1
        issue = (row.get("Issue") or "").strip().lower()
        if not any(t in issue for t in pathogen_tokens):
            continue
        cfia_pathogen_active += 1
        last_updated_s = (row.get("Last updated") or "").strip()
        try:
            d = datetime.strptime(last_updated_s, "%Y-%m-%d").date()
        except ValueError:
            continue
        if d >= cutoff:
            recent_pathogen_rows.append((d, row.get("Title") or "", row.get("URL") or ""))
        url_lo = (row.get("URL") or "").lower()
        if "shirreza-brand-tahini-halva" in url_lo:
            shirreza_row = row
        if "kyan-culture-brand-and-farm-boy" in url_lo:
            kyan_row = row

    recent_pathogen_rows.sort(key=lambda x: x[0], reverse=True)
    return {
        "total_records": total,
        "cfia_total": cfia_total,
        "cfia_active": cfia_active,
        "cfia_pathogen_active": cfia_pathogen_active,
        "recent_pathogen_count_7d": len(recent_pathogen_rows),
        "recent_pathogen_sample": recent_pathogen_rows[:10],
        "shirreza_present": shirreza_row is not None,
        "shirreza_row": shirreza_row,
        "kyan_present": kyan_row is not None,
        "kyan_row": kyan_row,
    }


def verdict_for(status, elapsed, body, err, kind):
    """Classify each probe."""
    if err is not None:
        return f"NETWORK ({err.splitlines()[0][:80]})"
    if status is None:
        return "NETWORK (no response)"
    if status == 404:
        return "404 (endpoint moved / Akamai 404 cloak — needs investigation)"
    if status == 403:
        return "BLOCKED (403 — bot detection or geo)"
    if status >= 500:
        return f"SERVER ERROR ({status})"
    if status >= 400:
        return f"HTTP {status}"
    if not body:
        return "EMPTY (200 OK but no body)"
    if kind == "json":
        try:
            json.loads(body.decode("utf-8", errors="replace"))
            return f"OK (200, JSON valid)"
        except Exception:
            return f"SCHEMA-DRIFT (200 OK, body is not JSON; {human_size(len(body))})"
    if kind == "rss":
        bs = body[:400].lower()
        if b"<rss" in bs or b"<feed" in bs or b"<?xml" in bs:
            return "OK (200, looks like XML/RSS)"
        return f"SCHEMA-DRIFT (200 OK, not XML; first bytes: {body[:60]!r})"
    if kind == "html":
        if b"<html" in body[:2000].lower() or b"<!doctype html" in body[:200].lower():
            return "OK (200, HTML)"
        return "SCHEMA-DRIFT (200 OK, not HTML)"
    return f"OK ({status})"


def section(title: str):
    print()
    print("─" * 80)
    print(title)
    print("─" * 80)


# ── Main ─────────────────────────────────────────────────────────────────
def main():
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print("=" * 80)
    print(f" FSIS scraper diagnostic — {now_utc}")
    print(f" Targets: Blackstone (FDA), Shirreza (CFIA), Kyan Culture (CFIA)")
    print("=" * 80)

    summary = []
    cfia_json_body = None

    for name, url, headers, kind, owner in ENDPOINTS:
        section(f"{name}\n  owner: {owner}\n  url:   {url}")
        status, elapsed, body, err = probe(url, headers)
        v = verdict_for(status, elapsed, body, err, kind)
        print(f"  HTTP:    {status}    elapsed: {elapsed:.2f}s    size: {human_size(len(body))}")
        print(f"  VERDICT: {v}")

        if body and not err and (status or 0) < 400:
            text = body.decode("utf-8", errors="replace")
            hits = find_targets(text)
            for label, present in hits.items():
                marker = "YES" if present else "no"
                print(f"  contains {label}: {marker}")
            # Stash the CFIA JSON body for structured analysis below
            if "HCRSAMOpenData" in url:
                cfia_json_body = body

        summary.append((name, status, v))

    # ── Deep dive on CFIA open-data JSON (L1, the canonical layer) ──
    if cfia_json_body is not None:
        section("CFIA open-data JSON — structured analysis")
        stats = cfia_jsonl_analysis(cfia_json_body)
        if "error" in stats:
            print(f"  ERROR: {stats['error']}")
        else:
            print(f"  total records in feed:           {stats['total_records']:>6,}")
            print(f"  CFIA total:                      {stats['cfia_total']:>6,}")
            print(f"  CFIA active (Archived=0):        {stats['cfia_active']:>6,}")
            print(f"  CFIA active pathogen recalls:    {stats['cfia_pathogen_active']:>6,}")
            print(f"  CFIA pathogen recalls in last 7d:{stats['recent_pathogen_count_7d']:>6,}")
            print()
            print(f"  Shirreza present as structured row: "
                  f"{'YES' if stats['shirreza_present'] else 'NO'}")
            if stats["shirreza_row"]:
                r = stats["shirreza_row"]
                print(f"    Last updated: {r.get('Last updated')}   "
                      f"Issue: {r.get('Issue')}   Archived: {r.get('Archived')}")
                print(f"    URL: {r.get('URL')}")
            print(f"  Kyan Culture present as structured row: "
                  f"{'YES' if stats['kyan_present'] else 'NO'}")
            if stats["kyan_row"]:
                r = stats["kyan_row"]
                print(f"    Last updated: {r.get('Last updated')}   "
                      f"Issue: {r.get('Issue')}   Archived: {r.get('Archived')}")
                print(f"    URL: {r.get('URL')}")
            print()
            print("  Most recent CFIA pathogen recalls in feed (up to 10):")
            for d, title, link in stats["recent_pathogen_sample"]:
                print(f"    {d}  {title[:80]}")

    # ── Final summary ──
    section("SUMMARY")
    for name, status, v in summary:
        print(f"  [{status if status is not None else 'ERR':>3}] {name:55s}  {v}")

    print()
    print("Done.")


if __name__ == "__main__":
    main()
