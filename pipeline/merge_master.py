"""
Master merge logic (Pending-sheet architecture).

recalls.xlsx holds THREE sheets:
  - Recalls  : approved, published data (consumed by the weekly report)
  - Pending  : freshly scraped rows awaiting validation + review
  - NEWS     : unrelated news-feed sheet, preserved as-is

Daily pipeline flow:
  1. Scrapers write to Pending (via append_to_pending)
  2. Enrichment + URL validation + AI review run against Pending
  3. promote_approved() moves rows that pass all checks into Recalls
  4. Rejected rows stay in Pending with a rejection reason stored in Notes
     (prefixed "REJECTED: <reason> | <original notes>") so a human can triage.

Dedup:
  - Primary key: URL (lowercased, stripped)
  - Fallback:    date + company + pathogen
  - Dedup applies within Pending and across Pending->Recalls promotion.
"""
from __future__ import annotations
import json
import logging
import re
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Tuple
from urllib.parse import urlparse
from openpyxl import load_workbook, Workbook
from openpyxl.styles import Font, PatternFill

from scrapers._models import Recall

log = logging.getLogger(__name__)

SCHEMA = ["Date", "Source", "Company", "Brand", "Product", "Pathogen", "Reason",
          "Class", "Country", "Region", "Tier", "Outbreak", "URL", "Notes"]

# ── Internal-only tracking columns (Recalls sheet only) ──────────────────
# These columns are appended to the Recalls sheet for internal bookkeeping
# and are EXCLUDED from the public-facing recalls.json that feeds the
# dashboard. See mirror_json_from_xlsx() for the filtering.
#
# DateAdded   — date the row was first promoted to Recalls (set once,
#               never changed). Used to distinguish original publication
#               date (Date) from when FSIS captured it.
# LastUpdated — date the row was last modified (any field changed). Set
#               by promote_approved on insert and by audit/fix code paths
#               that touch existing rows.
# LastChecked — date a URL gate (Gemini grounded check or url_guardian
#               reachability check) last validated this row's URL. Used
#               by url_guardian to skip rows checked recently and avoid
#               redundant Gemini grounded calls.
RECALLS_INTERNAL_COLUMNS = ["DateAdded", "LastUpdated", "LastChecked"]
RECALLS_SCHEMA = SCHEMA + RECALLS_INTERNAL_COLUMNS

# Pending sheet has the same columns plus two tracking columns.
PENDING_SCHEMA = SCHEMA + ["ScrapedAt", "Status"]

NEWS_HEADERS = ["Published (UTC)", "Pathogen", "Event", "Source", "Title",
                "Link", "Retrieved (UTC)"]

# Status values used in the Pending sheet
STATUS_PENDING = "pending"
STATUS_APPROVED = "approved"   # transient — promoted rows are removed from Pending
STATUS_REJECTED = "rejected"


# ---------------------------------------------------------------------------
# Dedup
# ---------------------------------------------------------------------------
def _dedup_key(row: Dict[str, Any]) -> str:
    """URL primary, fallback to date+company+pathogen."""
    url = (row.get("URL") or "").strip().lower()
    if url:
        return url
    co = unicodedata.normalize("NFD", row.get("Company") or "").encode("ascii", "ignore").decode().lower()
    co = re.sub(r"[^a-z0-9]", "", co)[:30]
    return f"{row.get('Date','')}|{co}|{(row.get('Pathogen','') or '')[:30]}"


# ---------------------------------------------------------------------------
# Near-duplicate detection (catches same-recall-different-URL cases)
# ---------------------------------------------------------------------------
# Why this exists: regulators sometimes publish the same recall under two
# URL formats (FDA's canonical company-slug URL vs. their share-link
# "?permalink=<hash>" wrapper). The OpenAI/search-based gap finders find
# the wrapper URL while the direct scraper finds the canonical URL —
# string dedup can't catch this because the URLs are entirely different
# paths. Even the date+company+pathogen fallback fails when the gap-finder
# discovers the recall N days after the scraper did (different Date field).
#
# The near-dup index keys on (source, normalized_company, pathogen) and
# stores a list of dates. A new row is rejected if there's already an
# entry with the same key dated within NEAR_DUP_WINDOW_DAYS days. This
# blocks rediscovery duplicates without blocking legitimate same-company
# recurring recalls (which are usually months apart).
NEAR_DUP_WINDOW_DAYS = 30


def _near_dup_key(row: Dict[str, Any]) -> str:
    """Normalized (source, company, pathogen) tuple — date-independent."""
    src = (row.get("Source") or "").strip().lower()
    co = unicodedata.normalize("NFD", row.get("Company") or "").encode("ascii", "ignore").decode().lower()
    co = re.sub(r"[^a-z0-9]", "", co)[:30]
    pa = (row.get("Pathogen") or "").strip().lower()[:50]
    if not (src and co and pa):
        return ""  # missing any of the three — can't make a meaningful match
    return f"{src}|{co}|{pa}"


def _build_near_dup_index(rows: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    """Return {near_dup_key: [date_str, ...]} for all rows with valid keys."""
    idx: Dict[str, List[str]] = {}
    for r in rows:
        k = _near_dup_key(r)
        d = str(r.get("Date") or "")[:10]
        if k and d:
            idx.setdefault(k, []).append(d)
    return idx


def _is_near_duplicate(
    row: Dict[str, Any], near_dup_index: Dict[str, List[str]],
) -> Tuple[bool, str]:
    """Check if `row` is a near-dup of anything in the index. Returns (is_dup, match_date)."""
    k = _near_dup_key(row)
    new_date_str = str(row.get("Date") or "")[:10]
    if not (k and new_date_str and k in near_dup_index):
        return False, ""
    try:
        new_date = datetime.strptime(new_date_str, "%Y-%m-%d").date()
    except ValueError:
        return False, ""
    for old_date_str in near_dup_index[k]:
        try:
            old_date = datetime.strptime(old_date_str, "%Y-%m-%d").date()
            if abs((new_date - old_date).days) <= NEAR_DUP_WINDOW_DAYS:
                return True, old_date_str
        except ValueError:
            continue
    return False, ""


# ---------------------------------------------------------------------------
# Pending-row validation gate
# ---------------------------------------------------------------------------
# This is the SINGLE chokepoint that blocks garbage from EVERY source
# (scrapers, gap finders, manual injects). Every row added to Pending must
# pass validate_pending_row() — see PIPELINE_FIX_SPEC.md.

# Generic / non-detail URL patterns we never want in Pending. These are
# regulator landing/listing/transparency pages, not specific recall fiches.
_GENERIC_URL_PATTERNS = (
    r"vertexaisearch\.cloud\.google",        # Gemini grounding redirect
    r"fsai\.ie/news-alerts/food\?page=",      # FSAI paginated listing
    r"rasff-window/screen/list\?",            # RASFF list page
    r"quebec\.ca/.*/listeriosis",             # Quebec disease info
    r"quebec\.ca/.*/animal-disease",          # Quebec animal disease info
    r"quebec\.ca/.*/food-recalls$",           # Quebec generic recalls page
    r"regulatory-transparency-and-openness",  # CFIA transparency pages
    r"food-safety-investigations/$",          # CFIA investigation index
    r"/categorie/[\d/]+/?$",                  # RappelConso category index
    r"/rubrik/[^/]+/?$",                      # produktwarnung.eu rubrik
    r"/news-and-alerts/food-alerts/?$",       # FSAI alerts root
    r"/safety/recalls-market-withdrawals-safety-alerts/?$",  # FDA root
    r"/animal-veterinary/news-events/outbreaks-and-advisories/?$",  # FDA pet root
    # CFIA recalls landing page (any locale path or bare host). The CFIA
    # scraper finds specific recall slugs at recalls-rappels.canada.ca/<lang>/<slug>;
    # the bare /fr or /en URL is the listing page itself, never a recall.
    # Triggered by the audit 2026-04-28 leak where the French landing page
    # entered Recalls with the page H1 ("Trouvez des rappels...") as Company.
    r"recalls-rappels\.canada\.ca/(?:fr|en)/?$",
    r"recalls-rappels\.canada\.ca/?$",
    # FDA share-link wrapper format used by their "voluntary-recall" template.
    # Functionally a SPA route — same recall is also published at the
    # canonical /safety/recalls-market-withdrawals-safety-alerts/<slug> URL,
    # which the FDA scraper always finds. Reject the wrapper to prevent
    # duplicates from search-based gap finders (Tavily/Exa/OpenAI) that
    # return whichever URL Google indexed first.
    r"/safety/recalls-market-withdrawals-safety-alerts/voluntary-recall\?permalink=",
)

# Company-field strings that the scraper has clearly bungled (they're page
# titles, section headers, or page text — never legitimate company names).
#
# IMPORTANT — what does NOT belong in this set:
#   • "Various brands" / "Various producers" / "Multiple brands"  → legit
#     descriptor when one recall covers many SKUs from different producers
#     (e.g. BLV Salmonellen-Weichkäse, RASFF multi-country alerts).
#   • "Unbranded" / "—" / "sans marque" / "No brand"              → legit
#     descriptor for RappelConso "sans marque" entries, generic raw products,
#     bulk commodity recalls.
#   • "Consult Food …", "Various Foods Ltd", etc.                 → real
#     company names that happen to start with normally-suspect words.
# Company-field cleanup beyond clear scraper bugs is the URL gate's job +
# downstream Claude review's job — not this gate's job.
_GARBAGE_COMPANIES = {
    "list of",                                       # FSAI/CFIA listing-page H1
    "food alerts",                                   # FSAI navigation
    "food alert",                                    # FSAI navigation
    "listeriosis",                                   # disease name as company
    "animals can catch and transmit salmonellosis",  # CFIA page text
    "food safety investigation:",                    # CFIA section header
    "timeline of events:",                           # CFIA section header
    "recall of",                                     # FSAI page-title leak (prefix)
}

# Hard cutoff: nothing dated before this enters Pending.
_MIN_VALID_DATE = "2026-01-01"


# News-outlet hosts. Any URL whose host (or parent domain) matches lands in
# the NEWS sheet via scrapers/news.py — never in Recalls. Gap-finders
# (Tavily/Exa/Gemini) sometimes surface news-article URLs while searching for
# recall content; without this blocklist they would slip into Pending and
# get promoted to Recalls with the article <title> tag scraped as Company.
# Triggered by the audit 2026-04-28 leak (foodsafetynews.com articles
# appearing in Recalls).
_NEWS_HOSTS = frozenset({
    "foodsafetynews.com",
    "foodpoisonjournal.com",
    "foodpoisoningbulletin.com",
    "outbreaknewstoday.com",
    "cidrap.umn.edu",
    "food-safety.com",
    "barfblog.com",
    "foodbusinessnews.net",
    "foodnavigator.com",
    "foodnavigator-usa.com",
    "just-food.com",
    "foodmanufacture.co.uk",
    "foodprocessing.com",
    "foodengineeringmag.com",
    "fooddive.com",
    "reuters.com",
    "apnews.com",
    "bbc.com",
    "bbc.co.uk",
    "bloomberg.com",
    "theguardian.com",
    "nytimes.com",
    "washingtonpost.com",
    "medicalxpress.com",
    "sciencedaily.com",
    "yahoo.com",
    "msn.com",
    "news.google.com",
})


def _host_is_news_outlet(url: str) -> bool:
    """True if the URL host (or any parent of it) is in _NEWS_HOSTS."""
    if not url:
        return False
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        return False
    if host.startswith("www."):
        host = host[4:]
    if host in _NEWS_HOSTS:
        return True
    # Subdomain match (e.g. recalls.reuters.com endswith .reuters.com)
    for h in _NEWS_HOSTS:
        if host.endswith("." + h):
            return True
    return False


def validate_pending_row(
    row: Dict[str, Any],
    existing_urls: set,
) -> Tuple[bool, str]:
    """
    Return (is_valid, rejection_reason). Reject garbage before it enters
    Pending. Called by append_to_pending() for every candidate row, no
    matter the source (scrapers, gap finders, manual injects).

    Rules — see PIPELINE_FIX_SPEC.md for rationale:
      • REJECT vertexaisearch redirect URLs (Gemini grounding artifacts)
      • REJECT generic / category / paginated-listing pages
      • REJECT URLs that aren't http/https
      • REJECT garbage Company fields ("List of", "Food Alerts", etc.)
      • REJECT duplicate URLs already in Recalls or Pending
      • REJECT dates before 2026-01-01

    `existing_urls` is a set of already-seen lowercased URLs (Recalls +
    current Pending). Pass an empty set to skip the dedup check.

    NOTE on near-duplicate detection: helpers _near_dup_key /
    _build_near_dup_index / _is_near_duplicate exist above for use by
    standalone audit scripts. They are NOT wired into this gate because
    European regulators (especially RappelConso) routinely publish one
    fiche per SKU — same company, same pathogen, same day, different
    fiche IDs. A naive same-source/company/pathogen+30d gate would
    falsely reject those legitimate per-SKU rows. Use the helpers only
    when you've verified the URL host context makes near-dup detection
    safe (e.g. for FDA where wrapper URLs duplicate canonical URLs).
    """
    url = str(row.get("URL", "") or "").strip()
    company = str(row.get("Company", "") or "").strip()
    date_str = str(row.get("Date", "") or "")[:10]

    # ── REJECT: vertexaisearch redirect URLs (Gemini grounding artifacts) ──
    if "vertexaisearch.cloud.google" in url:
        return False, "Gemini grounding redirect URL, not a real recall"

    # ── REJECT: URL host is a news outlet, not a regulator ──────────────
    # News articles belong in the NEWS sheet (populated by scrapers/news.py).
    # Gap-finders sometimes surface news-article URLs while searching for
    # recall content; reject them at the gate before they reach Recalls.
    if _host_is_news_outlet(url):
        return False, f"News outlet URL — belongs in NEWS sheet, not Recalls: {url[:60]}"

    # ── REJECT: generic / informational / listing pages ─────────────────
    for pat in _GENERIC_URL_PATTERNS:
        if re.search(pat, url, re.IGNORECASE):
            return False, f"Generic/info page URL: matches {pat}"

    # ── REJECT: URL is not http/https ───────────────────────────────────
    if url and not url.lower().startswith(("http://", "https://")):
        return False, f"Invalid URL scheme: {url[:30]}"

    # ── REJECT: company field is clearly a page title, not a company ────
    co_low = company.lower()
    if co_low in _GARBAGE_COMPANIES:
        return False, f'Company field is not a company: "{company}"'
    # Substring check for "Recall of …" / "List of …" leakage
    for bad in _GARBAGE_COMPANIES:
        if co_low.startswith(bad + " ") or co_low.startswith(bad + ":"):
            return False, f'Company field starts with garbage prefix "{bad}"'
    # Article-title leak: scraped <title> tags from news pages contain a
    # pipe + outlet name (e.g. "Salmonella outbreak ... | Food Safety News")
    # or HTML/JS fragments. Real company names never contain these.
    if " | " in company and re.search(
        r"\|\s*(food\s*safety\s*news|food\s*poison|outbreak\s*news|cidrap|"
        r"reuters|bbc|bloomberg|guardian)\b", company, re.I):
        return False, f'Company field is a news article <title> tag: "{company[:60]}"'
    if re.search(r"window\.\w+|document\.querySelector|<\s*script\b|"
                 r"\{socials\b|addEventListener\(", company, re.I):
        return False, f'Company field contains HTML/JS fragment: "{company[:60]}"'

    # ── REJECT: duplicate URL already in Recalls or Pending ─────────────
    url_norm = url.rstrip("/").lower()
    if url_norm and url_norm in existing_urls:
        return False, "Duplicate URL already exists"

    # ── REJECT: date is before 2026-01-01 ───────────────────────────────
    if date_str and date_str < _MIN_VALID_DATE:
        return False, f"Date before {_MIN_VALID_DATE}: {date_str}"

    return True, "OK"


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------
def _load_sheet(xlsx_path: Path, sheet: str, schema: List[str]) -> List[Dict[str, Any]]:
    if not xlsx_path.exists():
        return []
    wb = load_workbook(xlsx_path)
    if sheet not in wb.sheetnames:
        return []
    ws = wb[sheet]
    headers = [c.value for c in ws[1]]
    out = []
    # Defensive: openpyxl may return Date cells as datetime objects when a
    # cell was manually edited and Excel auto-typed it. Every downstream
    # consumer (gate, sort, dedup, JSON mirror) expects YYYY-MM-DD strings,
    # so coerce here at the single source of truth.
    from datetime import datetime as _dt, date as _dt_date
    for row in ws.iter_rows(min_row=2, values_only=True):
        if all(v in (None, "") for v in row):
            continue
        rec = {h: (v if v is not None else "") for h, v in zip(headers, row)}
        # Normalise Date column
        d = rec.get("Date")
        if isinstance(d, (_dt, _dt_date)):
            try:
                rec["Date"] = d.strftime("%Y-%m-%d")
            except (TypeError, ValueError):
                rec["Date"] = ""
        elif d not in (None, "") and not isinstance(d, str):
            # Excel serial number or other unexpected type
            rec["Date"] = str(d)[:10]
        # Backfill any schema cols missing from the sheet (schema evolution safety)
        for col in schema:
            rec.setdefault(col, "" if col not in ("Tier", "Outbreak") else 0)
        out.append(rec)
    return out


def load_existing(xlsx_path: Path) -> List[Dict[str, Any]]:
    """Read approved Recalls sheet -> list of dicts."""
    out = _load_sheet(xlsx_path, "Recalls", RECALLS_SCHEMA)
    log.info("Loaded %d approved rows from Recalls", len(out))
    return out


def load_pending(xlsx_path: Path) -> List[Dict[str, Any]]:
    """Read Pending sheet -> list of dicts. Empty if sheet doesn't exist yet."""
    out = _load_sheet(xlsx_path, "Pending", PENDING_SCHEMA)
    log.info("Loaded %d rows from Pending", len(out))
    return out


# ---------------------------------------------------------------------------
# Merge scraped rows into Pending
# ---------------------------------------------------------------------------
def append_to_pending(
    existing_pending: List[Dict[str, Any]],
    approved: List[Dict[str, Any]],
    new_recalls: List[Recall],
    scraped_at: str,
) -> List[Dict[str, Any]]:
    """
    Take new scraped+enriched Recall objects and append them to the pending list.

    Dedup rules:
      - If the key is already approved in Recalls  -> skip silently
      - If the key is currently in Pending with Status='pending'    -> skip (waiting)
      - If the key is currently in Pending with Status='rejected'   -> DELETE the
        old rejected row and insert the freshly scraped row for re-validation.
        This gives the source a chance to fix broken links / fill missing fields
        before the next run, and prevents rejected rows from being silently
        re-skipped forever.
      - Otherwise (brand new key) -> insert as Status='pending'.
    """
    keys_in_approved = {_dedup_key(r) for r in approved}

    # ── Near-duplicate index (audit 2026-04-29) ──────────────────────────
    # Catches hallucinated-URL gap-finder duplicates that string dedup
    # can't see. Example: scraper has Listeria/LES ATELIERS DE SEBASTIEN/
    # 2026-04-28 at /fiche-rappel/22142/Interne (real). Gemini gap-finder
    # then hallucinates the same recall at /fiche-rappel/22185/Interne
    # (fake). _dedup_key() compares URLs → no match → both land in
    # Pending → URL gate runs once a day at 07:00 → in the meantime
    # merge_master promotes the hallucinated one to Recalls.
    #
    # _is_near_duplicate keys on (source, normalized_company, pathogen)
    # within a 30-day window — so the same (source, company, pathogen)
    # appearing at a different URL within 30 days is rejected as a near-
    # dup before it ever lands in Pending. This catches the leak at
    # ingest time, not promotion time.
    near_dup_index = _build_near_dup_index(approved + existing_pending)

    # Build set of all URLs already present (Recalls + current Pending) for
    # the validation gate's dedup check. Lowercased + trailing-slash-stripped
    # to match validate_pending_row()'s normalisation.
    existing_urls: set = set()
    for r in approved:
        u = str(r.get("URL", "") or "").strip().rstrip("/").lower()
        if u:
            existing_urls.add(u)
    for r in existing_pending:
        u = str(r.get("URL", "") or "").strip().rstrip("/").lower()
        if u:
            existing_urls.add(u)

    # Index existing pending by key so we can drop rejected duplicates in place.
    # Multiple rows with the same key shouldn't happen, but if they do keep them
    # all (one will match; the others are untouched).
    pending_by_key: Dict[str, List[int]] = {}
    for i, r in enumerate(existing_pending):
        pending_by_key.setdefault(_dedup_key(r), []).append(i)

    # Decide which existing-pending rows to drop (rejected rows being re-scraped).
    indices_to_drop: set = set()
    fresh_rows: List[Dict[str, Any]] = []
    retried = 0
    appended = 0
    already_pending = 0
    already_approved = 0
    rejected_by_gate = 0

    for r in new_recalls:
        d = r.to_dict() if isinstance(r, Recall) else dict(r)
        for col in SCHEMA:
            d.setdefault(col, "" if col not in ("Tier", "Outbreak") else 0)

        # ── HARD GATE: validate before any other logic. Blocks garbage from
        # ── ALL sources (scrapers, gap-finders, manual injects).
        ok, why = validate_pending_row(d, existing_urls)
        if not ok:
            log.warning(
                "Pending gate REJECT: %s | url=%s | company=%s",
                why, str(d.get("URL", ""))[:100], str(d.get("Company", ""))[:50],
            )
            rejected_by_gate += 1
            continue

        k = _dedup_key(d)

        if k in keys_in_approved:
            already_approved += 1
            continue

        # ── Near-duplicate check (audit 2026-04-29) ─────────────────────
        # Reject if a recall with the same (source, normalized_company,
        # pathogen) was already approved or pended within the last 30
        # days. This is the gate that stops gap-finder URL hallucinations
        # from ever entering Pending — even when their hallucinated URL
        # is structurally valid (5-digit fiche ID in the right range).
        is_near, match_date = _is_near_duplicate(d, near_dup_index)
        if is_near:
            log.warning(
                "Pending near-dup REJECT: same (source, company, pathogen) "
                "already exists dated %s | new url=%s | company=%s",
                match_date, str(d.get("URL", ""))[:100],
                str(d.get("Company", ""))[:50],
            )
            rejected_by_gate += 1
            continue

        if k in pending_by_key:
            # Look at the FIRST matching row's status (practically there's only one)
            existing_idx = pending_by_key[k][0]
            existing_status = (existing_pending[existing_idx].get("Status") or "").lower()
            if existing_status == STATUS_REJECTED:
                # Drop the old rejected row, re-queue the fresh scrape
                indices_to_drop.add(existing_idx)
                d["ScrapedAt"] = scraped_at
                d["Status"] = STATUS_PENDING
                fresh_rows.append(d)
                retried += 1
            else:
                # Still pending from a prior run — leave it alone
                already_pending += 1
            continue

        # Brand new key
        d["ScrapedAt"] = scraped_at
        d["Status"] = STATUS_PENDING
        fresh_rows.append(d)
        appended += 1

    # Assemble output: existing pending minus dropped + new/retried
    kept = [r for i, r in enumerate(existing_pending) if i not in indices_to_drop]
    out = kept + fresh_rows

    log.info(
        "Pending: kept %d (dropped %d rejected for retry), +%d new, +%d retried "
        "(skipped: %d already-pending, %d already-approved, %d gate-rejected) = %d total",
        len(kept), len(indices_to_drop), appended, retried,
        already_pending, already_approved, rejected_by_gate, len(out),
    )
    return out


# ---------------------------------------------------------------------------
# Promotion: Pending -> Recalls
# ---------------------------------------------------------------------------
def promote_approved(
    pending: List[Dict[str, Any]],
    approved_existing: List[Dict[str, Any]],
    rejected_flags: Dict[int, str],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Split the Pending list into (new_approved_rows_for_Recalls, rows_to_keep_in_Pending).

    - `rejected_flags` maps pending-row-index -> rejection reason string.
      Rows listed here are marked Status='rejected' and KEPT in Pending
      (with reason written into Notes as 'REJECTED: <reason> | <orig notes>').
    - Rows NOT in rejected_flags (and whose current Status is 'pending') are
      treated as approved and moved to Recalls, deduped against approved_existing.
    - Rows already marked 'rejected' in a prior run stay in Pending untouched.
    """
    approved_keys = {_dedup_key(r) for r in approved_existing}

    new_approved: List[Dict[str, Any]] = []
    kept_in_pending: List[Dict[str, Any]] = []

    for idx, row in enumerate(pending):
        # Strip runtime-only fields (e.g. _url_check) before persisting
        clean = {k: v for k, v in row.items() if not k.startswith("_")}

        # Previously-rejected rows: leave alone, don't re-promote, don't re-reject
        if clean.get("Status") == STATUS_REJECTED and idx not in rejected_flags:
            kept_in_pending.append(clean)
            continue

        if idx in rejected_flags:
            reason = rejected_flags[idx]
            orig_notes = (clean.get("Notes") or "").strip()
            if not orig_notes.startswith("REJECTED:"):
                clean["Notes"] = f"REJECTED: {reason}" + (f" | {orig_notes}" if orig_notes else "")
            clean["Status"] = STATUS_REJECTED
            kept_in_pending.append(clean)
            continue

        # Approved row: dedup against existing Recalls
        k = _dedup_key(clean)
        if k in approved_keys:
            # Already published — drop silently from Pending
            continue
        approved_keys.add(k)

        # Strip pending-only tracking columns before inserting into Recalls.
        # Fill RECALLS_SCHEMA, including the internal tracking columns:
        #   DateAdded   = today (when row first promoted to Recalls)
        #   LastUpdated = today (initial insert counts as an update)
        #   LastChecked = "" (no URL re-validation has happened yet —
        #                     url_guardian/url_gate will fill this later)
        from datetime import date as _today_fn
        _today = _today_fn.today().isoformat()
        approved_row = {col: clean.get(col, "" if col not in ("Tier", "Outbreak") else 0)
                        for col in SCHEMA}
        approved_row["DateAdded"] = _today
        approved_row["LastUpdated"] = _today
        approved_row["LastChecked"] = ""
        new_approved.append(approved_row)

    rejected_kept = sum(1 for r in kept_in_pending if r.get("Status") == STATUS_REJECTED)
    log.info("Promotion: %d approved -> Recalls, %d kept in Pending (%d rejected)",
             len(new_approved), len(kept_in_pending), rejected_kept)
    return new_approved, kept_in_pending


# ---------------------------------------------------------------------------
# Sort / Save
# ---------------------------------------------------------------------------
def sort_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Sort newest first by Date string (YYYY-MM-DD sorts lexically).

    Defensive: rows can land here with Date as a datetime/date object when
    a cell in the xlsx was manually edited and Excel auto-typed it as a
    date. Coerce every key to a YYYY-MM-DD string before comparing so the
    sort never crashes on mixed types.
    """
    def _key(r: Dict[str, Any]) -> str:
        d = r.get("Date")
        if d is None or d == "":
            return ""
        # datetime / date object → ISO string
        if hasattr(d, "strftime"):
            try:
                return d.strftime("%Y-%m-%d")
            except (TypeError, ValueError):
                return ""
        # Anything else → string (truncate to first 10 chars to drop time)
        return str(d)[:10]
    return sorted(rows, key=_key, reverse=True)


def _write_sheet(wb: Workbook,
                 sheet_name: str,
                 schema: List[str],
                 rows: List[Dict[str, Any]],
                 header_fill: PatternFill = None) -> None:
    """(Re)create a sheet with given schema + rows.

    Defensive: Date cells are forced to YYYY-MM-DD strings with General
    number_format so an upstream datetime never gets written back as a
    typed-date cell (which would re-introduce the Excel-serial-leak bug).
    """
    from datetime import datetime as _dt, date as _dt_date
    if sheet_name in wb.sheetnames:
        del wb[sheet_name]
    ws = wb.create_sheet(sheet_name)
    for i, h in enumerate(schema, 1):
        c = ws.cell(row=1, column=i, value=h)
        c.font = Font(bold=True)
        if header_fill is not None:
            c.fill = header_fill
    for r_idx, row in enumerate(rows, 2):
        for c_idx, col in enumerate(schema, 1):
            v = row.get(col, "")
            if col in ("Tier", "Outbreak"):
                try:
                    v = int(v) if v not in ("", None) else 0
                except (ValueError, TypeError):
                    v = 0
            elif col == "Date":
                # Force every Date cell to a string + General format
                if isinstance(v, (_dt, _dt_date)):
                    try:
                        v = v.strftime("%Y-%m-%d")
                    except (TypeError, ValueError):
                        v = ""
                elif v not in (None, "") and not isinstance(v, str):
                    v = str(v)[:10]
            cell = ws.cell(row=r_idx, column=c_idx, value=v)
            if col == "Date":
                cell.number_format = "General"
    ws.freeze_panes = "A2"


def save_xlsx_with_pending(
    approved_rows: List[Dict[str, Any]],
    pending_rows: List[Dict[str, Any]],
    xlsx_path: Path,
) -> None:
    """
    Save BOTH sheets (Recalls + Pending), preserving NEWS sheet if present.
    Sheet order: Recalls (0), Pending (1), NEWS (last).
    """
    if xlsx_path.exists():
        wb = load_workbook(xlsx_path)
    else:
        wb = Workbook()
        if wb.active and wb.active.max_row == 1 and wb.active.max_column == 1:
            wb.remove(wb.active)

    # Write Recalls (approved published data)
    _write_sheet(wb, "Recalls", RECALLS_SCHEMA, approved_rows)

    # Write Pending (amber-ish header fill to make the tab visually distinct)
    pending_fill = PatternFill(start_color="FFF3CD", end_color="FFF3CD", fill_type="solid")
    _write_sheet(wb, "Pending", PENDING_SCHEMA, pending_rows, header_fill=pending_fill)

    # Ensure NEWS sheet exists (empty if it wasn't there before)
    if "NEWS" not in wb.sheetnames:
        news = wb.create_sheet("NEWS")
        for i, h in enumerate(NEWS_HEADERS, 1):
            c = news.cell(row=1, column=i, value=h)
            c.font = Font(bold=True)
        news.freeze_panes = "A2"

    # Reorder: Recalls, Pending, (others), NEWS last
    ordered = ["Recalls", "Pending"]
    others = [s for s in wb.sheetnames if s not in ("Recalls", "Pending", "NEWS")]
    ordered += others
    if "NEWS" in wb.sheetnames:
        ordered.append("NEWS")
    wb._sheets = [wb[s] for s in ordered]

    xlsx_path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(xlsx_path)
    log.info("Saved xlsx: Recalls=%d, Pending=%d -> %s",
             len(approved_rows), len(pending_rows), xlsx_path)


def save_json(rows: List[Dict[str, Any]], json_path: Path) -> None:
    """
    DEPRECATED — writes recalls.json from an in-memory list of rows.

    Using this is an architectural violation: recalls.json MUST mirror what's
    on the Recalls sheet of recalls.xlsx, not an arbitrary in-memory list.
    If the xlsx write fails or gets interrupted, the json would diverge from
    the file that's actually committed.

    Use `mirror_json_from_xlsx(xlsx_path, json_path)` instead. Kept here only
    so legacy callers don't crash outright during the transition.
    """
    log.warning("save_json (in-memory) is deprecated; "
                "use mirror_json_from_xlsx for guaranteed xlsx->json sync")
    json_path.parent.mkdir(parents=True, exist_ok=True)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=1, default=str)
    log.info("Saved %d approved rows to %s", len(rows), json_path)


def mirror_json_from_xlsx(xlsx_path: Path, json_path: Path) -> int:
    """
    Write recalls.json as a strict mirror of the Recalls sheet in recalls.xlsx.

    This is the ONLY sanctioned way to produce recalls.json. It guarantees
    that json can never drift from xlsx: we read the file that was just
    committed to disk, normalise types (dates to ISO strings), and serialise.

    INTERNAL columns (DateAdded, LastUpdated, LastChecked) are STRIPPED
    here so they don't leak into the public-facing dashboard. Only the
    14 SCHEMA columns make it to recalls.json.

    Returns the number of rows written.
    """
    rows = load_existing(xlsx_path)
    out = []
    for r in rows:
        rec = {}
        for k, v in r.items():
            # Skip internal-only tracking columns — public consumers never see these
            if k in RECALLS_INTERNAL_COLUMNS:
                continue
            if hasattr(v, "isoformat"):
                rec[k] = v.isoformat()[:10]
            elif v is None:
                rec[k] = ""
            else:
                rec[k] = v
        out.append(rec)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=1, default=str)
    log.info("Mirrored %d rows from xlsx -> %s", len(out), json_path)
    return len(out)


# ---------------------------------------------------------------------------
# Daily-brief rebuild helper (used by url_gate, claude_check, merge_master CLI)
# ---------------------------------------------------------------------------
def rebuild_daily_briefs_for_promoted(
    new_approved: List[Dict[str, Any]],
    full_approved: List[Dict[str, Any]],
) -> Tuple[List[str], List[str]]:
    """
    After Pending → Recalls promotion, rebuild the per-date daily-brief HTML
    for every date that gained at least one new row. Without this, the
    dashboard's DAILY tab and rolling 7-day display stay stale until the
    next scheduled daily-recall-search run.

    Args:
        new_approved : rows just promoted to Recalls this run
        full_approved: the FULL Recalls sheet AFTER promotion (so we render
                       the complete day, not just the newly-added rows)

    Returns:
        (rebuilt_brief_paths, files_to_commit) — caller is responsible for
        adding `files_to_commit` to its git_commit_and_push call. Both lists
        are empty when nothing was promoted or the brief renderer module
        is unavailable.
    """
    files_to_commit: List[str] = []
    rebuilt_briefs: List[str] = []
    if not new_approved:
        return rebuilt_briefs, files_to_commit

    try:
        from pipeline.daily_recall_search import (  # noqa: WPS433
            render_daily_html, update_daily_index,
        )
        from scrapers._models import Recall as _Recall  # noqa: WPS433
    except ImportError as ie:
        log.warning("Cannot import brief renderer (%s) — skipping daily "
                    "brief rebuild", ie)
        return rebuilt_briefs, files_to_commit

    from collections import defaultdict
    from datetime import date as _date

    # Group newly-promoted rows by Date
    by_date: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in new_approved:
        d = str(r.get("Date") or "").strip()[:10]
        if d:
            by_date[d].append(r)

    # Fast-lookup of ALL Recalls by date so we render the full day, not
    # only the newly-promoted rows.
    full_by_date: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in full_approved:
        d = str(r.get("Date") or "").strip()[:10]
        if d:
            full_by_date[d].append(r)

    for date_str in sorted(by_date.keys(), reverse=True):
        try:
            y, m, d = date_str.split("-")
            target = _date(int(y), int(m), int(d))
        except (ValueError, AttributeError):
            log.warning("Skip brief rebuild — bad date '%s'", date_str)
            continue

        day_rows = full_by_date.get(date_str, [])
        recalls_objs = []
        for row in day_rows:
            try:
                recalls_objs.append(_Recall(**{
                    k: (v if v is not None else "")
                    for k, v in row.items()
                    if k in _Recall.__annotations__
                }))
            except Exception as cce:  # noqa: BLE001
                log.debug("skip row coerce: %s", cce)

        try:
            render_daily_html(target, recalls_objs)
            update_daily_index(target, recalls_objs)
            brief_path = f"docs/daily/{date_str}.html"
            rebuilt_briefs.append(brief_path)
            files_to_commit.append(brief_path)
            log.info("Rebuilt daily brief for %s (%d rows)",
                     date_str, len(recalls_objs))
        except Exception as rerr:  # noqa: BLE001
            log.warning("Brief rebuild failed for %s: %s", date_str, rerr)

    if rebuilt_briefs:
        files_to_commit.append("docs/daily-index.json")

    return rebuilt_briefs, files_to_commit


# ---------------------------------------------------------------------------
# Back-compat shims (kept so legacy call sites don't break)
# ---------------------------------------------------------------------------
def save_xlsx(rows: List[Dict[str, Any]], xlsx_path: Path) -> None:
    """DEPRECATED: single-sheet save. Kept for any legacy caller."""
    log.warning("save_xlsx (single-sheet) is deprecated — use save_xlsx_with_pending")
    existing_pending = load_pending(xlsx_path)
    save_xlsx_with_pending(rows, existing_pending, xlsx_path)


def merge_new(existing: List[Dict[str, Any]], new_recalls: List[Recall]) -> List[Dict[str, Any]]:
    """
    DEPRECATED: direct merge into Recalls (pre-Pending-sheet behavior).
    Kept for any back-compat call; new code should use append_to_pending +
    promote_approved instead.
    """
    existing_keys = {_dedup_key(r) for r in existing}
    merged = list(existing)
    appended = 0
    for r in new_recalls:
        d = r.to_dict() if isinstance(r, Recall) else dict(r)
        for col in SCHEMA:
            d.setdefault(col, "" if col not in ("Tier", "Outbreak") else 0)
        k = _dedup_key(d)
        if k in existing_keys:
            continue
        existing_keys.add(k)
        merged.append(d)
        appended += 1
    log.info("merge_new (legacy): %d existing + %d new = %d total",
             len(existing), appended, len(merged))
    return merged


# ---------------------------------------------------------------------------
# NEWS sheet merge (for RSS news feed scrapers)
# ---------------------------------------------------------------------------
def _news_dedup_key(row: Dict[str, Any]) -> str:
    """Dedup key for a NEWS row: link URL (lowered, stripped)."""
    link = (row.get("Link") or row.get("link") or "").strip().lower()
    if link:
        return link
    title = (row.get("Title") or row.get("title") or "").strip().lower()[:80]
    return f"{row.get('Published (UTC)', '')}|{title}"


def load_news(xlsx_path: Path) -> List[Dict[str, str]]:
    """Load existing NEWS rows from the xlsx."""
    if not xlsx_path.exists():
        return []
    try:
        wb = load_workbook(xlsx_path, read_only=True, data_only=True)
        if "NEWS" not in wb.sheetnames:
            wb.close()
            return []
        ws = wb["NEWS"]
        headers = [c.value for c in next(ws.iter_rows(min_row=1, max_row=1))]
        rows = []
        for row in ws.iter_rows(min_row=2, values_only=True):
            d = {}
            for i, v in enumerate(row):
                if i < len(headers) and headers[i]:
                    d[headers[i]] = str(v) if v is not None else ""
            if d.get("Title") or d.get("Link"):
                rows.append(d)
        wb.close()
        return rows
    except Exception as e:
        log.warning("Failed to load NEWS sheet: %s", e)
        return []


def append_news_to_xlsx(
    xlsx_path: Path,
    new_items: List[Dict[str, str]],
) -> int:
    """
    Append new NEWS items to the NEWS sheet, deduped against existing rows.
    Returns count of actually-appended items.
    """
    if not new_items:
        return 0

    existing = load_news(xlsx_path)
    seen_keys = {_news_dedup_key(r) for r in existing}

    to_add = []
    for item in new_items:
        k = _news_dedup_key(item)
        if k in seen_keys:
            continue
        seen_keys.add(k)
        to_add.append(item)

    if not to_add:
        log.info("NEWS merge: 0 new items (all duplicates)")
        return 0

    # Open the workbook and append rows to the NEWS sheet
    wb = load_workbook(xlsx_path)
    if "NEWS" not in wb.sheetnames:
        ws = wb.create_sheet("NEWS")
        for i, h in enumerate(NEWS_HEADERS, 1):
            c = ws.cell(row=1, column=i, value=h)
            c.font = Font(bold=True)
        ws.freeze_panes = "A2"
    else:
        ws = wb["NEWS"]

    for item in to_add:
        row_vals = [item.get(h, "") for h in NEWS_HEADERS]
        ws.append(row_vals)

    wb.save(xlsx_path)
    log.info("NEWS merge: appended %d new items (total now %d)",
             len(to_add), len(existing) + len(to_add))
    return len(to_add)



# =========================================================================
# CLI entry point — run by the hourly merge-master workflow.
#
# AUDIT 2026-04-29 — promotion semantics tightened:
#   The hourly CLI used to call promote_approved() with rejected_flags driven
#   only by review/url_validator (HTTP HEAD/GET reachability). That meant
#   ANY row with a 200-returning URL was promoted to Recalls — including
#   hallucinated RappelConso fiches (e.g. /fiche-rappel/22180/Interne) that
#   render a soft-200 page even when the fiche ID doesn't exist. This let
#   ~7 hallucinated Gemini gap-finder rows leak from the 07:34 Exa run into
#   Recalls before the once-daily 07:00 Gemini URL gate could catch them.
#
#   New rule (per George 2026-04-29):
#     "Only data from URL Gemini followed by Claude check must be allowed
#      1 or two times per day."
#
#   The hourly CLI is now a JANITOR ONLY — it cleans malformed URLs from
#   Pending and removes Pending rows whose URL has been confirmed dead,
#   but it NEVER promotes to Recalls. Promotion happens exclusively via:
#     1. pipeline/url_gate_gemini.py     (07:00 Athens — Gemini URL gate)
#     2. pipeline/claude_check.py        (07:45 Athens — Claude content check)
#
#   To override (e.g. backfill, manual catch-up), set MERGE_MASTER_PROMOTE=1.
# =========================================================================
if __name__ == "__main__":
    import sys, os
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    from pathlib import Path
    ROOT = Path(__file__).resolve().parent.parent
    XLSX = ROOT / "docs" / "data" / "recalls.xlsx"

    if not XLSX.exists():
        log.info("No recalls.xlsx found — nothing to merge.")
        sys.exit(0)

    approved = load_existing(XLSX)
    pending = load_pending(XLSX)
    log.info("State: %d approved, %d pending", len(approved), len(pending))

    if not pending:
        log.info("No Pending rows — nothing to do.")
        sys.exit(0)

    # ── URL reachability check (cheap janitor pass) ─────────────────────
    # We still validate URLs at every hourly run so dead URLs can be
    # marked rejected in Pending. We do NOT use the result to drive
    # promotion — that would re-create the leak this CLI is here to
    # prevent.
    from review.url_validator import validate_all
    log.info("Validating %d Pending URLs (janitor pass — no promotion)...",
             len(pending))
    validated = validate_all(pending, max_workers=5)

    # Mark broken-URL rows as rejected so they don't promote later.
    # Anything that's currently 'pending' AND has a confirmed bad URL
    # gets stamped REJECTED so the next gate pass skips it.
    rejected_flags = {}
    for idx, row in enumerate(validated):
        check = row.get("_url_check", {})
        # Only reject if the URL check says it's BROKEN. Reachable URLs
        # stay 'pending' until the URL gate validates them properly.
        if not check.get("ok", False):
            rejected_flags[idx] = check.get("reason", "URL check failed")

    # Strip the runtime _url_check field before persisting
    clean_pending = [{k: v for k, v in row.items() if k != "_url_check"}
                     for row in validated]

    # ── Promotion gate ──────────────────────────────────────────────────
    # OFF by default — only the once-daily Gemini URL gate (07:00) and
    # Claude check (07:45) workflows are permitted to promote rows to
    # Recalls. The hourly CLI just stamps rejections and exits.
    promote_enabled = os.environ.get("MERGE_MASTER_PROMOTE", "").strip() in (
        "1", "true", "yes")

    if promote_enabled:
        log.info("MERGE_MASTER_PROMOTE=1 — promotion ENABLED for this run "
                 "(use only for backfill/manual catch-up)")
        new_approved, remaining = promote_approved(
            clean_pending, approved, rejected_flags,
        )
        if new_approved:
            log.info("Promoted %d rows Pending → Recalls", len(new_approved))
            final_approved = sort_rows(approved + new_approved)
        else:
            log.info("No rows promoted this run.")
            final_approved = sort_rows(approved)
    else:
        log.info("Janitor mode (default): NOT promoting. Only the daily "
                 "Gemini URL gate + Claude check are allowed to advance "
                 "rows from Pending → Recalls.")
        # We still update Pending with rejection stamps for broken URLs.
        # Walk clean_pending; for any idx in rejected_flags, copy the
        # rejection note onto the row so the next gate pass skips it.
        for idx, row in enumerate(clean_pending):
            if idx in rejected_flags and row.get("Status") != STATUS_REJECTED:
                reason = rejected_flags[idx]
                orig_notes = (row.get("Notes") or "").strip()
                if not orig_notes.startswith("REJECTED:"):
                    row["Notes"] = f"REJECTED: {reason}" + (
                        f" | {orig_notes}" if orig_notes else "")
                row["Status"] = STATUS_REJECTED
        new_approved = []
        remaining = clean_pending
        final_approved = sort_rows(approved)
        if rejected_flags:
            log.info("Marked %d Pending row(s) as rejected (broken URL)",
                     len(rejected_flags))

    save_xlsx_with_pending(final_approved, sort_rows(remaining), XLSX)
    mirror_json_from_xlsx(XLSX, ROOT / "docs" / "data" / "recalls.json")

    # Rebuild daily briefs for any date that gained newly-promoted rows.
    # Without this, the dashboard's rolling 7-day display stays stale.
    # In janitor mode (no promotion), new_approved is empty — this is a
    # cheap no-op.
    rebuilt_briefs, brief_files = rebuild_daily_briefs_for_promoted(
        new_approved, final_approved,
    )
    if rebuilt_briefs:
        log.info("Rebuilt %d daily brief(s)", len(rebuilt_briefs))

    log.info("Done. Recalls=%d, Pending=%d", len(final_approved), len(remaining))
