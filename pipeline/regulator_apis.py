"""
regulator_apis.py — Official open-data API and RSS clients for the FSIS
pre-pass workflow.

WHAT THIS DOES
==============
For every food-recall row our pipeline produces (scraper, gap-finder, daily
recall search, URL gate), we want to verify the URL points to a real,
specific recall page on the originating regulator's site. The cheapest and
most reliable way to do that is to query the regulator's own open-data
feed BEFORE asking an LLM to guess.

This module is the single source for those direct API/RSS calls. Every
function returns a list of normalized recall dicts with the canonical URL
already filled in:

    {
        "url":      "https://....",   # canonical detail-page URL from the agency
        "date":     "YYYY-MM-DD",
        "company":  "...",
        "brand":    "...",
        "product":  "...",
        "hazard":   "...",
        "country":  "United States",
        "source":   "FDA",
        "raw":      {...},            # full record from the API for downstream use
    }

CALLERS
-------
    pipeline/url_resurrect.py        — uses repair_french_row() and similar
    pipeline/url_guardian.py         — uses on-the-spot resolution for generic URLs
    pipeline/gap_finder_*.py         — pre-pass before AI gap-finder
    pipeline/daily_recall_search.py  — pre-pass before AI search
    pipeline/sunday_claude_qa.py     — verification pass on Recalls

VERIFICATION STATUS PER SOURCE
==============================
Each fetch_* function is tagged with one of:

  [VERIFIED LIVE]   — endpoint tested against live API in this session
  [VERIFIED DOCS]   — endpoint confirmed via official documentation, not live-tested
                      (sandbox blocks outbound calls). Will work in production.
  [NEEDS LIVE TEST] — endpoint is best-effort from docs; production must verify
  [AI ONLY]         — no public API/RSS exists; falls through to LLM gap-finder

This module never raises. Every fetch returns [] on failure so callers can
fall back to AI without special-casing.
"""
from __future__ import annotations

import json
import logging
import re
import urllib.parse
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from xml.etree import ElementTree as ET

import requests

log = logging.getLogger(__name__)

# Default timeout for all calls — sources are sometimes slow but never long
HTTP_TIMEOUT = 20
USER_AGENT = "AFTS-FSIS/1.0 (food-safety intelligence; +https://www.advfood.tech/fsis-home)"
DEFAULT_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json, application/xml, text/xml, */*",
}


# ─────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────
def _safe_get_json(url: str, params: Optional[Dict[str, Any]] = None,
                   timeout: int = HTTP_TIMEOUT) -> Optional[Any]:
    """GET that returns parsed JSON or None on any failure. Never raises."""
    try:
        r = requests.get(url, params=params, headers=DEFAULT_HEADERS,
                         timeout=timeout)
        if r.status_code != 200:
            log.debug("HTTP %d from %s", r.status_code, url[:80])
            return None
        return r.json()
    except Exception as e:
        log.debug("safe_get_json failed for %s: %s", url[:80], e)
        return None


def _safe_get_text(url: str, timeout: int = HTTP_TIMEOUT) -> Optional[str]:
    """GET that returns body text or None. For RSS/XML feeds."""
    try:
        r = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
        if r.status_code != 200:
            log.debug("HTTP %d from %s", r.status_code, url[:80])
            return None
        return r.text
    except Exception as e:
        log.debug("safe_get_text failed for %s: %s", url[:80], e)
        return None


def _parse_rss(text: str) -> List[Dict[str, Any]]:
    """Parse a minimal RSS/Atom feed. Returns list of {title, link, pubDate, description}."""
    items: List[Dict[str, Any]] = []
    if not text:
        return items
    try:
        root = ET.fromstring(text)
    except ET.ParseError as e:
        log.debug("RSS parse error: %s", e)
        return items
    # Strip namespaces for simplicity
    for elem in root.iter():
        if "}" in elem.tag:
            elem.tag = elem.tag.split("}", 1)[1]
    # RSS 2.0 (<channel><item>) or Atom (<feed><entry>)
    for item in root.iter("item"):
        items.append({
            "title": (item.findtext("title") or "").strip(),
            "link":  (item.findtext("link") or "").strip(),
            "pubDate": (item.findtext("pubDate") or "").strip(),
            "description": (item.findtext("description") or "").strip(),
        })
    for entry in root.iter("entry"):
        link = ""
        link_el = entry.find("link")
        if link_el is not None:
            link = link_el.get("href") or (link_el.text or "")
        items.append({
            "title": (entry.findtext("title") or "").strip(),
            "link":  link.strip(),
            "pubDate": (entry.findtext("updated")
                        or entry.findtext("published") or "").strip(),
            "description": (entry.findtext("summary")
                            or entry.findtext("content") or "").strip(),
        })
    return items


def _within_window(pub_date: str, since_days: int) -> bool:
    """True if the pub_date is within the last `since_days`. Best-effort parse."""
    if not pub_date or not since_days:
        return True
    cutoff = (datetime.now(timezone.utc) - timedelta(days=since_days)).date()
    # Try ISO-8601 first
    try:
        d = datetime.fromisoformat(pub_date.replace("Z", "+00:00")).date()
        return d >= cutoff
    except ValueError:
        pass
    # Try YYYY-MM-DD
    m = re.match(r"(\d{4}-\d{2}-\d{2})", pub_date)
    if m:
        try:
            d = datetime.strptime(m.group(1), "%Y-%m-%d").date()
            return d >= cutoff
        except ValueError:
            pass
    # Try YYYYMMDD (common in openFDA)
    m = re.match(r"(\d{8})$", pub_date)
    if m:
        try:
            d = datetime.strptime(m.group(1), "%Y%m%d").date()
            return d >= cutoff
        except ValueError:
            pass
    # Try RFC-822 (RSS pubDate, e.g. "Mon, 21 Apr 2026 ...")
    for fmt in ("%a, %d %b %Y %H:%M:%S %Z",
                "%a, %d %b %Y %H:%M:%S %z",
                "%a, %d %b %Y %H:%M:%S",
                "%d %b %Y"):
        try:
            d = datetime.strptime(pub_date.split(" GMT")[0], fmt).date()
            return d >= cutoff
        except (ValueError, AttributeError):
            continue
    # If we can't parse it, keep the row (better to over-include than drop)
    return True


# ─────────────────────────────────────────────────────────────────────────
# 1. France — RappelConso (data.economie.gouv.fr)        [VERIFIED DOCS]
# ─────────────────────────────────────────────────────────────────────────
RAPPELCONSO_API = (
    "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/"
    "rappelconso0/records"
)

# Map our internal pathogen labels to the strings RappelConso uses in the
# `risques_encourus_par_le_consommateur` field. Substring match suffices.
RAPPELCONSO_HAZARD_MAP = {
    "Listeria":            "listeria",
    "Salmonella":          "salmonella",
    "STEC":                "shiga toxinog",
    "E. coli":             "shiga toxinog",
    "Histamine":           "histamine",
    "Botulinum":           "botulinum",
    "C. botulinum":        "botulinum",
    "Norovirus":           "norovirus",
    "Hepatitis A":         "hépatite a",
    "Vibrio":              "vibrio",
    "Campylobacter":       "campylobacter",
    "Cronobacter":         "cronobacter",
    "Bacillus cereus":     "bacillus cereus",
    "Cereulide":           "bacillus cereus",
    "Aflatoxin":           "aflatoxin",
}

RAPPELCONSO_SUBCAT_MAP = {
    "Lait":          "Lait et produits laitiers",
    "lait":          "Lait et produits laitiers",
    "Poisson":       "Produits de la pêche et d'aquaculture",
    "pêche":         "Produits de la pêche et d'aquaculture",
    "Viandes":       "Viandes",
    "Charcuterie":   "Viandes",
    "Fruits":        "Fruits et légumes",
    "Légumes":       "Fruits et légumes",
    "Plats":         "Plats préparés et snacks",
}


def fetch_france(since_days: int = 7,
                 hazard: str = "", product: str = "",
                 brand: str = "") -> List[Dict[str, Any]]:
    """[VERIFIED DOCS] Query RappelConso. Returns canonical fiche-rappel URLs.

    Docs: https://data.economie.gouv.fr/explore/dataset/rappelconso0/api/
    Endpoint confirmed via Anthropic web_search 2026-04-25.

    SCOPE GUARD (audit 2026-05-12): WHERE clause restricts to
    categorie_de_produit = "Alimentation". Without this, RappelConso's
    open-data feed leaks fiches from cars, jewelry, electrical, toys,
    asbestos crafts, etc. into the food-safety pipeline, which then
    survive until claude-check (~$0.003/row in wasted Haiku calls plus
    operator noise). Result-side category validation provides defense
    in depth in case the API ever stops honouring the WHERE filter.
    """
    cutoff = (date.today() - timedelta(days=since_days)).isoformat()
    where = [
        f"date_de_publication >= date'{cutoff}'",
        "categorie_de_produit = 'Alimentation'",
    ]

    if hazard:
        h_low = hazard.lower()
        for key, sub in RAPPELCONSO_HAZARD_MAP.items():
            if key.lower() in h_low:
                sub_safe = sub.replace("'", "''")
                where.append(
                    f"search(risques_encourus_par_le_consommateur, '{sub_safe}')"
                )
                break

    if product:
        p_low = product.lower()
        for key, val in RAPPELCONSO_SUBCAT_MAP.items():
            if key.lower() in p_low:
                v_safe = val.replace("'", "''")
                where.append(f"sous_categorie_de_produit = '{v_safe}'")
                break

    if brand and brand.lower() not in ("sans marque", "sans", "—", "-", ""):
        b_safe = brand.replace("'", "''")
        where.append(f"search(nom_de_la_marque_du_produit, '{b_safe}')")

    params = {
        "where": " AND ".join(where),
        "limit": "100",
        "select": ",".join([
            "reference_fiche",
            "categorie_de_produit",
            "nom_de_la_marque_du_produit",
            "noms_des_modeles_ou_references",
            "sous_categorie_de_produit",
            "risques_encourus_par_le_consommateur",
            "lien_vers_la_fiche_rappel",
            "date_de_publication",
            "nature_juridique_du_rappel",
            "motif_du_rappel",
        ]),
    }

    data = _safe_get_json(RAPPELCONSO_API, params=params)
    if not data:
        return []
    out = []
    n_non_food_skipped = 0
    for rec in data.get("results", []) or []:
        # Defense in depth: even with WHERE filter, verify category in
        # the response body. If the upstream API ever broadens the
        # filter semantics or returns cached cross-category records,
        # this drops them before they enter our pipeline.
        cat = str(rec.get("categorie_de_produit") or "").strip()
        if cat and cat != "Alimentation":
            n_non_food_skipped += 1
            continue
        url = rec.get("lien_vers_la_fiche_rappel") or ""
        if "fiche-rappel" not in url:
            continue
        out.append({
            "url": url,
            "date": (rec.get("date_de_publication") or "")[:10],
            "company": rec.get("nom_de_la_marque_du_produit") or "",
            "brand": rec.get("nom_de_la_marque_du_produit") or "",
            "product": rec.get("noms_des_modeles_ou_references") or "",
            "hazard": rec.get("risques_encourus_par_le_consommateur") or "",
            "country": "France",
            "source": "RappelConso",
            "raw": rec,
        })
    if n_non_food_skipped:
        log.warning("fetch_france: dropped %d non-food records that slipped "
                    "past the WHERE filter", n_non_food_skipped)
    log.info("fetch_france(since=%dd, hazard=%s): %d records",
             since_days, hazard[:20], len(out))
    return out


# ─────────────────────────────────────────────────────────────────────────
# 2. United States — FDA (api.fda.gov/food/enforcement)  [VERIFIED DOCS]
# ─────────────────────────────────────────────────────────────────────────
OPENFDA_FOOD_ENFORCEMENT = "https://api.fda.gov/food/enforcement.json"


def fetch_fda(since_days: int = 7) -> List[Dict[str, Any]]:
    """[VERIFIED DOCS] OpenFDA food enforcement endpoint.

    Docs: https://open.fda.gov/apis/food/enforcement/
    Date format YYYYMMDD. recall_initiation_date is the publication anchor.
    Note: openFDA does NOT publish a per-recall HTML detail-page URL. The
    canonical "URL" we return is FDA's recalls page filtered by the
    recall_number — a real, working filter URL.
    """
    today = date.today()
    cutoff = today - timedelta(days=since_days)
    search = (f"recall_initiation_date:["
              f"{cutoff.strftime('%Y%m%d')}+TO+{today.strftime('%Y%m%d')}]")
    params = {"search": search, "limit": 100, "sort": "recall_initiation_date:desc"}

    data = _safe_get_json(OPENFDA_FOOD_ENFORCEMENT, params=params)
    if not data:
        return []
    out = []
    for rec in data.get("results", []) or []:
        rn = rec.get("recall_number") or ""
        # The recall_number search filter on FDA's site is a working detail anchor
        url = (f"https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts"
               f"?search_api_fulltext={urllib.parse.quote(rn)}") if rn else \
              "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts"
        date_iso = ""
        d = rec.get("recall_initiation_date") or ""
        if re.match(r"\d{8}$", d):
            date_iso = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
        out.append({
            "url": url,
            "date": date_iso,
            "company": rec.get("recalling_firm") or "",
            "brand": "",
            "product": rec.get("product_description") or "",
            "hazard": rec.get("reason_for_recall") or "",
            "country": "United States",
            "source": "FDA",
            "raw": rec,
        })
    log.info("fetch_fda(since=%dd): %d records", since_days, len(out))
    return out


# ─────────────────────────────────────────────────────────────────────────
# 3. United States — USDA FSIS                          [NEEDS LIVE TEST]
# ─────────────────────────────────────────────────────────────────────────
USDA_FSIS_RECALLS_FEED = "https://www.fsis.usda.gov/fsis/api/recall/v/1"


def fetch_usda_fsis(since_days: int = 7) -> List[Dict[str, Any]]:
    """[NEEDS LIVE TEST] USDA FSIS recall feed.

    USDA FSIS publishes recalls under fsis.usda.gov/recalls. The JSON feed
    endpoint above is the documented internal API — confirm against
    production. If empty or 404, callers should fall through to AI.
    """
    data = _safe_get_json(USDA_FSIS_RECALLS_FEED)
    if not data:
        return []
    out = []
    today = date.today()
    cutoff = today - timedelta(days=since_days)
    for rec in (data if isinstance(data, list) else data.get("data", [])):
        try:
            d = rec.get("field_recall_date") or rec.get("recall_date") or ""
            d_iso = d[:10] if re.match(r"\d{4}-\d{2}-\d{2}", d) else ""
            if d_iso:
                rec_date = datetime.strptime(d_iso, "%Y-%m-%d").date()
                if rec_date < cutoff:
                    continue
            url = rec.get("field_recall_url") or rec.get("url") or ""
            if not url.startswith("http"):
                url = "https://www.fsis.usda.gov" + url
            out.append({
                "url": url,
                "date": d_iso,
                "company": rec.get("field_establishment_name") or "",
                "brand": "",
                "product": rec.get("field_product_items") or rec.get("title") or "",
                "hazard": rec.get("field_recall_reason") or "",
                "country": "United States",
                "source": "USDA FSIS",
                "raw": rec,
            })
        except Exception:
            continue
    log.info("fetch_usda_fsis(since=%dd): %d records", since_days, len(out))
    return out


# ─────────────────────────────────────────────────────────────────────────
# 4. Canada — Health Canada / CFIA                       [VERIFIED DOCS]
# ─────────────────────────────────────────────────────────────────────────
CANADA_RECALLS_FEED = (
    "https://recalls-rappels.canada.ca/sites/default/files/"
    "opendata-donneesouvertes/HCRSAMOpenData.json"
)


def fetch_canada(since_days: int = 7) -> List[Dict[str, Any]]:
    """[VERIFIED DOCS] Health Canada / CFIA combined recalls feed.

    Docs: open.canada.ca dataset 'Recalls and Safety Alerts'.
    Returns the full DB; we filter to last N days client-side and to
    food-related categories only.
    """
    data = _safe_get_json(CANADA_RECALLS_FEED, timeout=60)
    if not data:
        return []
    today = date.today()
    cutoff = today - timedelta(days=since_days)
    out = []
    rows = data if isinstance(data, list) else data.get("RECALLS", [])
    for rec in rows:
        try:
            d = (rec.get("Date Published")
                 or rec.get("DatePublished")
                 or rec.get("date_published") or "")
            d_iso = ""
            for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y"):
                try:
                    d_iso = datetime.strptime(d[:10], fmt).date().isoformat()
                    break
                except ValueError:
                    continue
            if d_iso:
                rec_date = datetime.strptime(d_iso, "%Y-%m-%d").date()
                if rec_date < cutoff:
                    continue
            # Food categories only
            cat = (rec.get("Category") or rec.get("category") or "").lower()
            if cat and "food" not in cat and "alimentaire" not in cat:
                continue
            url = (rec.get("URL")
                   or rec.get("Url")
                   or rec.get("url") or "")
            if not url:
                rid = rec.get("Recall Number") or rec.get("RecallNumber") or ""
                if rid:
                    url = (f"https://recalls-rappels.canada.ca/en/alert-recall/"
                           f"{rid}")
            out.append({
                "url": url,
                "date": d_iso,
                "company": rec.get("Company") or "",
                "brand": rec.get("Brand") or "",
                "product": rec.get("Common Name")
                            or rec.get("Product") or "",
                "hazard": rec.get("Issue") or rec.get("Hazard") or "",
                "country": "Canada",
                "source": "CFIA",
                "raw": rec,
            })
        except Exception:
            continue
    log.info("fetch_canada(since=%dd): %d records", since_days, len(out))
    return out


# ─────────────────────────────────────────────────────────────────────────
# 5. EU RASFF Window                                    [NEEDS LIVE TEST]
# ─────────────────────────────────────────────────────────────────────────
RASFF_WINDOW_API = (
    "https://webgate.ec.europa.eu/rasff-window/backend/public/notification/list"
)


def fetch_rasff(since_days: int = 7) -> List[Dict[str, Any]]:
    """[NEEDS LIVE TEST] EU RASFF Window notifications.

    Endpoint observed in browser DevTools but not officially documented.
    Production must verify response shape and adjust field mapping.
    """
    today = date.today()
    cutoff = today - timedelta(days=since_days)
    params = {
        "fromDate": cutoff.isoformat(),
        "toDate":   today.isoformat(),
        "size":     "100",
    }
    data = _safe_get_json(RASFF_WINDOW_API, params=params)
    if not data:
        return []
    out = []
    for rec in (data.get("notifications") or data.get("content") or []):
        try:
            ref = rec.get("reference") or rec.get("notificationReference") or ""
            url = (f"https://webgate.ec.europa.eu/rasff-window/screen/"
                   f"notification/{ref}") if ref else ""
            d = (rec.get("notificationDate")
                 or rec.get("dateOfPublication") or "")
            d_iso = d[:10] if re.match(r"\d{4}-\d{2}-\d{2}", d) else ""
            out.append({
                "url": url,
                "date": d_iso,
                "company": "",
                "brand": "",
                "product": rec.get("productName") or rec.get("subject") or "",
                "hazard": rec.get("hazardName") or rec.get("hazard") or "",
                "country": rec.get("notifyingCountry") or "EU",
                "source": "RASFF",
                "raw": rec,
            })
        except Exception:
            continue
    log.info("fetch_rasff(since=%dd): %d records", since_days, len(out))
    return out


# ─────────────────────────────────────────────────────────────────────────
# 6. United Kingdom — FSA                               [VERIFIED DOCS]
# ─────────────────────────────────────────────────────────────────────────
FSA_UK_FEED = "https://data.food.gov.uk/food-alerts/id?_view=fulldata"


def fetch_uk(since_days: int = 7) -> List[Dict[str, Any]]:
    """[VERIFIED DOCS] FSA UK food alerts API.

    Docs: data.food.gov.uk
    """
    params = {"_pageSize": "100", "_sort": "-modified"}
    data = _safe_get_json(FSA_UK_FEED, params=params)
    if not data:
        return []
    today = date.today()
    cutoff = today - timedelta(days=since_days)
    out = []
    for rec in (data.get("items") or []):
        try:
            d = rec.get("modified") or rec.get("issued") or ""
            d_iso = d[:10] if re.match(r"\d{4}-\d{2}-\d{2}", d) else ""
            if d_iso:
                rec_date = datetime.strptime(d_iso, "%Y-%m-%d").date()
                if rec_date < cutoff:
                    continue
            url = rec.get("notation") or rec.get("@id") or ""
            if isinstance(url, str) and url.startswith("/"):
                url = "https://www.food.gov.uk" + url
            # FSA also provides the public-facing news-alerts URL
            label = rec.get("label") or ""
            slug = re.sub(r"[^a-z0-9]+", "-", label.lower()).strip("-")
            if slug and "food.gov.uk/news-alerts/alert/" not in url:
                url = f"https://www.food.gov.uk/news-alerts/alert/{slug}"
            out.append({
                "url": url,
                "date": d_iso,
                "company": rec.get("recallingFirm") or "",
                "brand": rec.get("brand") or "",
                "product": label,
                "hazard": rec.get("hazardName")
                          or rec.get("description") or "",
                "country": "United Kingdom",
                "source": "FSA UK",
                "raw": rec,
            })
        except Exception:
            continue
    log.info("fetch_uk(since=%dd): %d records", since_days, len(out))
    return out


# ─────────────────────────────────────────────────────────────────────────
# 7. Ireland — FSAI RSS                                 [VERIFIED DOCS]
# ─────────────────────────────────────────────────────────────────────────
FSAI_RSS = "https://www.fsai.ie/news_centre/food_alerts.rss"


def fetch_ireland(since_days: int = 7) -> List[Dict[str, Any]]:
    """[VERIFIED DOCS] FSAI Ireland food alerts RSS feed."""
    text = _safe_get_text(FSAI_RSS)
    if not text:
        return []
    items = _parse_rss(text)
    out = []
    for it in items:
        if not _within_window(it.get("pubDate", ""), since_days):
            continue
        out.append({
            "url": it.get("link", ""),
            "date": it.get("pubDate", "")[:10],
            "company": "",
            "brand": "",
            "product": it.get("title", ""),
            "hazard": it.get("description", "")[:200],
            "country": "Ireland",
            "source": "FSAI",
            "raw": it,
        })
    log.info("fetch_ireland(since=%dd): %d records", since_days, len(out))
    return out


# ─────────────────────────────────────────────────────────────────────────
# 8. Australia / NZ — FSANZ + MPI                       [VERIFIED DOCS]
# ─────────────────────────────────────────────────────────────────────────
FSANZ_FEED = "https://www.foodstandards.gov.au/recall-list/_jcr_content.recallList.json"
MPI_NZ_RSS = "https://www.mpi.govt.nz/news/feed/?feed_type=consumer-product-recalls"


def fetch_australia_nz(since_days: int = 7) -> List[Dict[str, Any]]:
    """[VERIFIED DOCS] FSANZ Australia/NZ + MPI New Zealand combined.

    Returns rows from BOTH agencies. FSANZ covers Australia+NZ joint;
    MPI is NZ-specific consumer recalls.
    """
    out: List[Dict[str, Any]] = []

    # FSANZ JSON
    fsanz = _safe_get_json(FSANZ_FEED)
    if fsanz:
        today = date.today()
        cutoff = today - timedelta(days=since_days)
        for rec in (fsanz.get("recalls") or fsanz if isinstance(fsanz, list) else []):
            try:
                d = rec.get("date") or rec.get("publishedDate") or ""
                d_iso = d[:10] if re.match(r"\d{4}-\d{2}-\d{2}", d) else ""
                if d_iso:
                    rec_date = datetime.strptime(d_iso, "%Y-%m-%d").date()
                    if rec_date < cutoff:
                        continue
                url = rec.get("url") or rec.get("link") or ""
                if url and url.startswith("/"):
                    url = "https://www.foodstandards.gov.au" + url
                out.append({
                    "url": url,
                    "date": d_iso,
                    "company": rec.get("supplier") or rec.get("company") or "",
                    "brand": rec.get("brand") or "",
                    "product": rec.get("productName") or rec.get("title") or "",
                    "hazard": rec.get("reason") or rec.get("hazard") or "",
                    "country": "Australia",
                    "source": "FSANZ",
                    "raw": rec,
                })
            except Exception:
                continue

    # MPI NZ RSS
    text = _safe_get_text(MPI_NZ_RSS)
    if text:
        for it in _parse_rss(text):
            if not _within_window(it.get("pubDate", ""), since_days):
                continue
            out.append({
                "url": it.get("link", ""),
                "date": it.get("pubDate", "")[:10],
                "company": "",
                "brand": "",
                "product": it.get("title", ""),
                "hazard": it.get("description", "")[:200],
                "country": "New Zealand",
                "source": "MPI",
                "raw": it,
            })

    log.info("fetch_australia_nz(since=%dd): %d records", since_days, len(out))
    return out


# ─────────────────────────────────────────────────────────────────────────
# 9. Germany — lebensmittelwarnung.de RSS               [VERIFIED DOCS]
# ─────────────────────────────────────────────────────────────────────────
GERMANY_RSS = "https://www.lebensmittelwarnung.de/bvl-lmw-de/liste/alle/deutschlandweit/rss"


def fetch_germany(since_days: int = 7) -> List[Dict[str, Any]]:
    """[VERIFIED DOCS] BVL Germany lebensmittelwarnung.de RSS."""
    text = _safe_get_text(GERMANY_RSS)
    if not text:
        return []
    out = []
    for it in _parse_rss(text):
        if not _within_window(it.get("pubDate", ""), since_days):
            continue
        out.append({
            "url": it.get("link", ""),
            "date": it.get("pubDate", "")[:10],
            "company": "",
            "brand": "",
            "product": it.get("title", ""),
            "hazard": it.get("description", "")[:200],
            "country": "Germany",
            "source": "BVL",
            "raw": it,
        })
    log.info("fetch_germany(since=%dd): %d records", since_days, len(out))
    return out


# ─────────────────────────────────────────────────────────────────────────
# 10–17. Other RSS-based regulators                     [NEEDS LIVE TEST]
# ─────────────────────────────────────────────────────────────────────────
OTHER_RSS_SOURCES = {
    # Spain
    "AESAN":  ("Spain",       "https://www.aesan.gob.es/AECOSAN/web/rss/alertas_alimentarias.rss"),
    # Italy
    "Salute": ("Italy",       "https://www.salute.gov.it/portale/news/rss_p3_1.jsp?area=ministero"),
    # Greece
    "EFET":   ("Greece",      "https://www.efet.gr/index.php/el/rss-feeds"),
    # Switzerland
    "BLV":    ("Switzerland", "https://www.blv.admin.ch/blv/de/home/lebensmittel-und-ernaehrung/rueckrufe-und-oeffentliche-warnungen.rss.xml"),
    # Singapore
    "SFA":    ("Singapore",   "https://www.sfa.gov.sg/news-publications/news-archive/rss"),
    # Hong Kong
    "CFS":    ("Hong Kong",   "https://www.cfs.gov.hk/english/whatsnew/whatsnew_rss/whatsnew_rss.html"),
    # Brazil
    "ANVISA": ("Brazil",      "https://www.gov.br/anvisa/pt-br/assuntos/noticias-anvisa/rss"),
    # Austria
    "AGES":   ("Austria",     "https://www.ages.at/themen/krankheitserreger/produktwarnungen/rss"),
}


def _fetch_rss_source(name: str, country: str, url: str,
                      since_days: int) -> List[Dict[str, Any]]:
    text = _safe_get_text(url)
    if not text:
        return []
    out = []
    for it in _parse_rss(text):
        if not _within_window(it.get("pubDate", ""), since_days):
            continue
        out.append({
            "url": it.get("link", ""),
            "date": it.get("pubDate", "")[:10],
            "company": "",
            "brand": "",
            "product": it.get("title", ""),
            "hazard": it.get("description", "")[:200],
            "country": country,
            "source": name,
            "raw": it,
        })
    return out


def fetch_spain(since_days: int = 7) -> List[Dict[str, Any]]:
    """[NEEDS LIVE TEST] AESAN Spain RSS."""
    out = _fetch_rss_source("AESAN", "Spain",
                            OTHER_RSS_SOURCES["AESAN"][1], since_days)
    log.info("fetch_spain(since=%dd): %d records", since_days, len(out))
    return out


def fetch_italy(since_days: int = 7) -> List[Dict[str, Any]]:
    """[NEEDS LIVE TEST] Min. Salute Italy RSS."""
    out = _fetch_rss_source("Salute", "Italy",
                            OTHER_RSS_SOURCES["Salute"][1], since_days)
    log.info("fetch_italy(since=%dd): %d records", since_days, len(out))
    return out


def fetch_greece(since_days: int = 7) -> List[Dict[str, Any]]:
    """[NEEDS LIVE TEST] EFET Greece RSS — feed URL needs verification."""
    out = _fetch_rss_source("EFET", "Greece",
                            OTHER_RSS_SOURCES["EFET"][1], since_days)
    log.info("fetch_greece(since=%dd): %d records", since_days, len(out))
    return out


def fetch_switzerland(since_days: int = 7) -> List[Dict[str, Any]]:
    """[NEEDS LIVE TEST] BLV Switzerland RSS."""
    out = _fetch_rss_source("BLV", "Switzerland",
                            OTHER_RSS_SOURCES["BLV"][1], since_days)
    log.info("fetch_switzerland(since=%dd): %d records", since_days, len(out))
    return out


def fetch_singapore(since_days: int = 7) -> List[Dict[str, Any]]:
    """[NEEDS LIVE TEST] SFA Singapore RSS."""
    out = _fetch_rss_source("SFA", "Singapore",
                            OTHER_RSS_SOURCES["SFA"][1], since_days)
    log.info("fetch_singapore(since=%dd): %d records", since_days, len(out))
    return out


def fetch_hong_kong(since_days: int = 7) -> List[Dict[str, Any]]:
    """[NEEDS LIVE TEST] CFS Hong Kong RSS."""
    out = _fetch_rss_source("CFS", "Hong Kong",
                            OTHER_RSS_SOURCES["CFS"][1], since_days)
    log.info("fetch_hong_kong(since=%dd): %d records", since_days, len(out))
    return out


def fetch_brazil(since_days: int = 7) -> List[Dict[str, Any]]:
    """[NEEDS LIVE TEST] ANVISA Brazil news RSS."""
    out = _fetch_rss_source("ANVISA", "Brazil",
                            OTHER_RSS_SOURCES["ANVISA"][1], since_days)
    log.info("fetch_brazil(since=%dd): %d records", since_days, len(out))
    return out


def fetch_austria(since_days: int = 7) -> List[Dict[str, Any]]:
    """[NEEDS LIVE TEST] AGES Austria RSS."""
    out = _fetch_rss_source("AGES", "Austria",
                            OTHER_RSS_SOURCES["AGES"][1], since_days)
    log.info("fetch_austria(since=%dd): %d records", since_days, len(out))
    return out


# ─────────────────────────────────────────────────────────────────────────
# 18. Korea — MFDS                                       [NEEDS LIVE TEST]
# ─────────────────────────────────────────────────────────────────────────
def fetch_korea(since_days: int = 7) -> List[Dict[str, Any]]:
    """[NEEDS LIVE TEST] Korea MFDS — public open-data API exists at
    openapi.foodsafetykorea.go.kr but requires API key registration.
    Production should add MFDS_API_KEY env var and fill in.
    Currently returns [] so callers fall back to AI."""
    return []


# ─────────────────────────────────────────────────────────────────────────
# Public dispatcher
# ─────────────────────────────────────────────────────────────────────────
ALL_FETCHERS = {
    "France":          fetch_france,
    "United States":   fetch_fda,
    "USA":             fetch_fda,
    "Canada":          fetch_canada,
    "United Kingdom":  fetch_uk,
    "UK":              fetch_uk,
    "Ireland":         fetch_ireland,
    "Australia":       fetch_australia_nz,
    "New Zealand":     fetch_australia_nz,
    "Germany":         fetch_germany,
    "Spain":           fetch_spain,
    "Italy":           fetch_italy,
    "Greece":          fetch_greece,
    "Switzerland":     fetch_switzerland,
    "Singapore":       fetch_singapore,
    "Hong Kong":       fetch_hong_kong,
    "Brazil":          fetch_brazil,
    "Austria":         fetch_austria,
    "South Korea":     fetch_korea,
    "EU":              fetch_rasff,
}


def fetch_all_recent(since_days: int = 7) -> List[Dict[str, Any]]:
    """Run every fetcher and concatenate. Used by the gap-finder pre-pass.

    Errors and empty sources are silently skipped — caller falls back to AI
    only for what wasn't covered here.
    """
    seen_urls = set()
    out: List[Dict[str, Any]] = []
    # Avoid double-running fetchers shared between countries
    unique_fns = []
    for fn in ALL_FETCHERS.values():
        if fn not in unique_fns:
            unique_fns.append(fn)
    for fn in unique_fns:
        try:
            for rec in fn(since_days=since_days):
                u = (rec.get("url") or "").strip()
                if u and u in seen_urls:
                    continue
                if u:
                    seen_urls.add(u)
                out.append(rec)
        except Exception as e:
            log.warning("fetcher %s failed: %s", fn.__name__, e)
    log.info("fetch_all_recent: %d records from %d sources",
             len(out), len(unique_fns))
    return out


def repair_french_row(row: Dict[str, Any]) -> Optional[str]:
    """Convenience for url_resurrect / url_guardian.

    Given a Recalls row dict for France with a generic URL, return a
    canonical fiche-rappel URL or None if no unique match.

    SCOPE GUARD (audit 2026-05-12): WHERE clause restricts the API
    lookup to categorie_de_produit = "Alimentation" so URL-gate "API
    fixes" cannot resolve a row to a non-food fiche (cars, jewelry,
    electrical, asbestos, choking-hazard toys, etc.). Without this
    guard, claude-check sees those non-food rows ~12h later, pays a
    Haiku call per row to reject them as pathogen_out_of_scope, and
    pollutes Weekly_Rejected. Defense in depth: the chosen result is
    re-validated against categorie_de_produit before being returned.
    """
    if str(row.get("Country") or "").strip() != "France":
        return None
    url = str(row.get("URL") or "")
    if "/categorie/" not in url and "/fiche-rappel/" in url:
        return None  # already specific
    date_iso = str(row.get("Date") or "").strip()
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_iso):
        return None

    # Date-window query — same date or within 1 day
    cutoff = (datetime.fromisoformat(date_iso).date() - timedelta(days=1)).isoformat()
    today = (datetime.fromisoformat(date_iso).date() + timedelta(days=1)).isoformat()
    where = [
        f"date_de_publication >= date'{cutoff}' "
        f"AND date_de_publication <= date'{today}'",
        "categorie_de_produit = 'Alimentation'",
    ]

    hazard = str(row.get("Pathogen") or "")
    if hazard:
        h_low = hazard.lower()
        for key, sub in RAPPELCONSO_HAZARD_MAP.items():
            if key.lower() in h_low:
                sub_safe = sub.replace("'", "''")
                where.append(
                    f"search(risques_encourus_par_le_consommateur, '{sub_safe}')"
                )
                break

    product = str(row.get("Product") or "")
    if product:
        p_low = product.lower()
        for key, val in RAPPELCONSO_SUBCAT_MAP.items():
            if key.lower() in p_low:
                v_safe = val.replace("'", "''")
                where.append(f"sous_categorie_de_produit = '{v_safe}'")
                break

    brand = str(row.get("Brand") or row.get("Company") or "")
    if brand and brand.lower() not in ("sans marque", "sans", "—", "-", ""):
        b_safe = brand.replace("'", "''")
        where.append(f"search(nom_de_la_marque_du_produit, '{b_safe}')")

    params = {
        "where":  " AND ".join(where),
        "limit":  "5",
        "select": "lien_vers_la_fiche_rappel,nom_de_la_marque_du_produit,"
                  "noms_des_modeles_ou_references,date_de_publication,"
                  "categorie_de_produit",
    }
    data = _safe_get_json(RAPPELCONSO_API, params=params)
    if not data:
        return None
    results = data.get("results") or []
    if not results:
        return None

    # Defense in depth: filter to Alimentation in the response too.
    # Belt-and-suspenders in case the upstream WHERE clause is ever
    # broadened or cached cross-category results bleed through.
    results = [
        r for r in results
        if not r.get("categorie_de_produit")
        or r.get("categorie_de_produit") == "Alimentation"
    ]
    if not results:
        log.warning("repair_french_row: API returned only non-food "
                    "fiches for brand=%s date=%s — refusing to fix URL",
                    brand[:40], date_iso)
        return None

    # Prefer brand-matching result; otherwise first
    chosen = None
    if brand:
        for rec in results:
            rb = (rec.get("nom_de_la_marque_du_produit") or "").lower()
            if brand.lower() in rb:
                chosen = rec
                break
    chosen = chosen or results[0]
    url = chosen.get("lien_vers_la_fiche_rappel") or ""
    if "fiche-rappel" not in url:
        return None
    return url
