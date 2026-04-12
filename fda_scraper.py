"""
FSIS Agent – FDA Scraper
Uses the openFDA Food Enforcement JSON API (no HTML scraping needed).
Endpoint: https://api.fda.gov/food/enforcement.json
"""
import os
import time
import logging
from datetime import datetime, timedelta
from urllib.parse import urlencode

import requests

from config import (
    FDA_API_BASE, FDA_PAGE_LIMIT, FDA_LOOKBACK_DAYS, PATHOGENS
)
from database import init_db, upsert_recall, log_run, get_conn

log = logging.getLogger(__name__)


# ── Date helpers ──────────────────────────────────────────────────────────────

def _iso_to_fda(iso: str) -> str:
    """2025-01-15  →  20250115"""
    return iso.replace("-", "")


def _fda_to_iso(fda: str) -> str:
    """20250115  →  2025-01-15"""
    if len(fda) == 8:
        return f"{fda[:4]}-{fda[4:6]}-{fda[6:]}"
    return fda


def _last_stored_date() -> str | None:
    """Return the most recent recall_date already in DB, or None."""
    conn = get_conn()
    row = conn.execute(
        "SELECT MAX(recall_date) FROM recalls"
    ).fetchone()
    conn.close()
    return row[0] if row and row[0] else None


# ── Pathogen filter ───────────────────────────────────────────────────────────

def _is_pathogen_recall(reason: str) -> bool:
    r = reason.lower()
    return any(p.lower() in r for p in PATHOGENS)


# ── openFDA fetcher ───────────────────────────────────────────────────────────

def _build_url(date_from: str, date_to: str, skip: int = 0) -> str:
    fda_from = _iso_to_fda(date_from)
    fda_to   = _iso_to_fda(date_to)
    search   = f'recall_initiation_date:[{fda_from}+TO+{fda_to}]'

    params = {
        "search": search,
        "limit":  FDA_PAGE_LIMIT,
        "skip":   skip,
    }
    api_key = os.environ.get("OPENFDA_API_KEY", "").strip()
    if api_key:
        params["api_key"] = api_key

    # urlencode but preserve the raw + in the date range
    base = f"{FDA_API_BASE}?search={search}&limit={FDA_PAGE_LIMIT}&skip={skip}"
    if api_key:
        base += f"&api_key={api_key}"
    return base


def _parse_record(raw: dict) -> dict | None:
    """Normalize one openFDA record. Returns None if not pathogen-related."""
    reason = raw.get("reason_for_recall", "")
    if not _is_pathogen_recall(reason):
        return None

    recall_id = raw.get("recall_number", "").strip()
    if not recall_id:
        return None

    fda_date = raw.get("recall_initiation_date", "")
    iso_date = _fda_to_iso(fda_date) if fda_date else ""

    return {
        "id":             recall_id,
        "recall_date":    iso_date,
        "firm":           raw.get("recalling_firm", ""),
        "product":        raw.get("product_description", "")[:500],
        "reason":         reason[:1000],
        "classification": raw.get("classification", ""),
        "status":         raw.get("status", ""),
        "distribution":   raw.get("distribution_pattern", "")[:500],
        "quantity":       raw.get("product_quantity", "")[:200],
        "city":           raw.get("city", ""),
        "state":          raw.get("state", ""),
        "country":        raw.get("country", "US"),
        "raw":            raw,
    }


def fetch_fda(date_from: str = None, date_to: str = None) -> tuple[int, int]:
    """
    Pull FDA food enforcement recalls in [date_from, date_to].
    Returns (total_fetched, new_inserted).
    """
    today = datetime.utcnow().date()
    if date_to is None:
        date_to = str(today)
    if date_from is None:
        last = _last_stored_date()
        if last:
            # resume from day after last stored
            dt = datetime.strptime(last, "%Y-%m-%d").date() + timedelta(days=1)
            date_from = str(dt)
        else:
            date_from = str(today - timedelta(days=FDA_LOOKBACK_DAYS))

    log.info(f"FDA fetch: {date_from} → {date_to}")

    fetched = 0
    new     = 0
    skip    = 0

    while True:
        url = _build_url(date_from, date_to, skip)
        try:
            resp = requests.get(url, timeout=30)
        except requests.RequestException as e:
            log.error(f"Request error at skip={skip}: {e}")
            break

        if resp.status_code == 404:
            # openFDA returns 404 when no results in range
            log.info("No results in date range (404) – done.")
            break
        if resp.status_code == 429:
            log.warning("Rate limited – sleeping 60s")
            time.sleep(60)
            continue
        if resp.status_code != 200:
            log.error(f"HTTP {resp.status_code}: {resp.text[:200]}")
            break

        data    = resp.json()
        results = data.get("results", [])
        total   = data.get("meta", {}).get("results", {}).get("total", 0)

        if not results:
            break

        for raw in results:
            rec = _parse_record(raw)
            if rec and upsert_recall(rec):
                new += 1
        fetched += len(results)

        log.info(f"  skip={skip} | page={len(results)} | total_api={total} | new={new}")

        skip += FDA_PAGE_LIMIT
        if skip >= total:
            break

        time.sleep(0.5)   # polite delay

    log.info(f"FDA fetch complete: {fetched} fetched, {new} new pathogen records.")
    return fetched, new


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    init_db()
    fetched, new = fetch_fda()
    log_run("FDA", fetched, new)
    print(f"Done – {fetched} fetched, {new} new.")
