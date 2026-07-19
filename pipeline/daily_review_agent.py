"""pipeline/daily_review_agent.py — FSIS Daily Review Agent (Phase 1).

Runs once per day AFTER the morning scrape + merge have settled and performs
the integrity review that was previously done by hand. It is deliberately
split into two lanes with very different risk profiles:

  LANE A — SAFE deterministic fixes, applied automatically:
      • tier-1 enforcement          (pipeline._pathogen_scope.enforce_tier1)
      • blank DateAdded stamp        (legacy rows get today's Athens date)
      • "Homepage -" scrape-artifact strip on Product/Company/Brand
      • blank-Date recovery from the URL slug
      • resort (merge_master.sort_rows) + mirror_json_from_xlsx
      • flag weekly/monthly reports whose baked count drifted, so the
        workflow's rebuild step re-issues them with an UPDATED masthead.

  LANE B — RISKY removals, DETECTED and QUEUED only, NEVER executed here:
      • duplicate URLs / fiches (exact normalised-URL collisions)
      • CFIA FR/EN pairs (same recall at distinct /fr/ vs /en/ URLs)
      • HK CFS foreign re-posts   (pipeline._cfs_aggregator_guard)
      • cross-source duplicates   (same content_key, different URL/Source)
    Every queued item carries evidence + a confidence grade and is written to
    docs/data/daily_review_report.json / daily_review_digest.md for the
    Phase-2 approval mailer. NOTHING is deleted. Deletion + archival to
    Weekly_Rejected happens only after the operator approves an item.

Outputs (all under docs/data/):
    daily_review_report.json   machine-readable summary (report + queue)
    daily_review_digest.md     human-readable digest for the approval email
    review_ledger.json         resolved-item ledger; resolved items are never
                               re-proposed (created empty on first live run)

CLI:
    python -m pipeline.daily_review_agent            # live: Lane A applied
    python -m pipeline.daily_review_agent --dry-run  # read-only: report only

House rules respected: complete file (no diffs); LF/no-BOM; Athens time for
every date stamp; the in-progress week is NEVER published (W29 stays
unpublished until its Friday closes — mirrors filter_week/refresh semantics);
rebuilt reports read UPDATED, never PUBLISHED; the Apps Script mailer is never
touched; nothing is auto-deleted; every future removal must archive with a
reason.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import logging
import os
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from pipeline import merge_master as mm
from pipeline._pathogen_scope import (
    enforce_tier1,
    is_always_tier1,
    is_empty_pathogen,
)
from pipeline._url_identity import (
    content_key,
    has_stable_id,
    row_rank,
)
from pipeline._cfs_aggregator_guard import is_foreign_cfs_repost

log = logging.getLogger("daily_review_agent")

# ── Paths ────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"
DATA = DOCS / "data"
XLSX = DATA / "recalls.xlsx"
RECALLS_JSON = DATA / "recalls.json"
REPORT_JSON = DATA / "daily_review_report.json"
DIGEST_MD = DATA / "daily_review_digest.md"
LEDGER_JSON = DATA / "review_ledger.json"

ATHENS = ZoneInfo("Europe/Athens")

# More than this many proposed deletions in one day escalates to a single
# "review manually" email instead of N per-item Approve/Reject links.
DEFAULT_MAX_LINKS = 25

# Fields whose blankness is a genuine integrity problem worth reporting.
REQUIRED_FIELDS = ("Date", "Company", "Product", "URL", "Pathogen")

# ── Lane-A helpers ───────────────────────────────────────────────────────
# "Homepage -" scrape artifact. Sources such as lebensmittelwarnung.de emit a
# breadcrumb prefix ("lebensmittelwarnung.de - Homepage - <real product>")
# that leaks into Product/Company. Strip everything up to and including the
# "Homepage -" token, but only when a non-empty remainder survives so we never
# blank a field.
_HOMEPAGE_RE = re.compile(
    r"^\s*(?:[^\n]{0,80}?[-–—]\s*)?homepage\s*[-–—]\s*",
    re.IGNORECASE,
)

# ISO-ish date embedded in a URL slug: 2026-07-07, 2026/07/07, or 20260707.
_URL_DATE_RE = re.compile(
    r"(20\d{2})[-/_]?(0[1-9]|1[0-2])[-/_]?(0[1-9]|[12]\d|3[01])"
)

# CFIA / inspection.canada.ca language token in the path, e.g. /en/ or /fr/.
_LANG_SEG_RE = re.compile(r"/(en|fr)(?=/|$)", re.IGNORECASE)


def _athens_today() -> date:
    return datetime.now(ATHENS).date()


def _athens_now_iso() -> str:
    return datetime.now(ATHENS).replace(microsecond=0).isoformat()


def _strip_homepage(value: str) -> str:
    """Remove a leading '... - Homepage -' breadcrumb artifact.

    Conservative: returns the original value unchanged unless the artifact is
    present AND stripping it leaves a non-empty remainder.
    """
    if not value:
        return value
    m = _HOMEPAGE_RE.match(str(value))
    if not m:
        return value
    remainder = str(value)[m.end():].strip()
    return remainder if remainder else value


def _recover_date_from_url(url: str) -> str:
    """Return a YYYY-MM-DD date embedded in the URL slug, or '' if none/invalid."""
    if not url:
        return ""
    m = _URL_DATE_RE.search(str(url))
    if not m:
        return ""
    y, mo, d = m.groups()
    try:
        return date(int(y), int(mo), int(d)).isoformat()
    except ValueError:
        return ""


def _url_lang(url: str) -> str:
    """'en' / 'fr' from a canada.ca recall URL's leading language segment, else ''."""
    m = _LANG_SEG_RE.search(mm._normalize_url_for_dedup(url))
    return m.group(1).lower() if m else ""


def _is_cfia_url(url: str) -> bool:
    u = (url or "").lower()
    return ("recalls-rappels.canada.ca" in u
            or "inspection.canada.ca" in u
            or "inspection.gc.ca" in u)


# CFIA recall identity. The open-data scraper stashes the stable numeric recall
# id in Notes as "NID=<n>"; when present it is the durable identity. When it is
# absent (gap-finder rows), fall back to a normalised slug stem so the SAME
# recall republished under a drifted slug / company / "Last updated" date is
# still recognised as one recall.
_CFIA_NID_RE = re.compile(r"\bNID=(\d+)")
_CFIA_SLUG_STOP = {
    "brand", "brands", "recalled", "recall", "due", "and", "of", "the", "to",
    "certain", "various", "multiple", "products", "product", "made", "contains",
    "containing", "possible", "contamination", "recalls", "inc", "ltd", "co",
    "company", "foods", "food", "canada", "en", "fr", "alert",
    # pathogen / hazard words — a slug's pathogen suffix is not its identity
    "listeria", "monocytogenes", "salmonella", "coli", "ecoli", "e", "o157",
    "o26", "o121", "o104", "stec", "norovirus", "botulism", "cronobacter",
    "hepatitis", "bacillus", "cereus", "cereulide", "pathogenic", "glass",
    "pieces", "h7", "sap",
}


def _cfia_nid(row: Dict[str, Any]) -> str:
    m = _CFIA_NID_RE.search(str(row.get("Notes") or ""))
    return m.group(1) if m else ""


def _cfia_slug_tokens(url: str) -> frozenset:
    """Distinctive token set of a CFIA recall slug (stop/pathogen words removed)."""
    norm = mm._normalize_url_for_dedup(url)
    m = re.search(r"/alert-recall/([^/?]+)", norm) or re.search(r"/rappel-avis/([^/?]+)", norm)
    slug = m.group(1) if m else norm.rsplit("/", 1)[-1]
    toks = {t for t in re.split(r"[^a-z0-9]+", slug.lower())
            if t and not t.isdigit() and t not in _CFIA_SLUG_STOP and len(t) > 1}
    return frozenset(toks)


def _cfia_same_recall(a: Dict[str, Any], b: Dict[str, Any]) -> Optional[str]:
    """Return a reason string if a and b are the same CFIA recall, else None.

    Identity, in order: (1) equal NID; (2) same-date (±21d) slug-stem match
    where one distinctive token set is a subset of the other, or their Jaccard
    overlap is >= 0.7 with >= 3 shared tokens.
    """
    na, nb = _cfia_nid(a), _cfia_nid(b)
    if na and nb:
        return f"same CFIA recall id (NID={na})" if na == nb else None
    ta, tb = _cfia_slug_tokens(a.get("URL", "")), _cfia_slug_tokens(b.get("URL", ""))
    if len(ta) < 3 or len(tb) < 3:
        return None
    da = str(a.get("Date") or "")[:10]
    db = str(b.get("Date") or "")[:10]
    try:
        gap = abs((datetime.strptime(da, "%Y-%m-%d").date()
                   - datetime.strptime(db, "%Y-%m-%d").date()).days)
    except ValueError:
        gap = 999
    if gap > 21:
        return None
    inter = ta & tb
    union = ta | tb
    if ta <= tb or tb <= ta:
        return (f"CFIA slug variants of one recall "
                f"(shared stem: {'-'.join(sorted(inter))[:60]})")
    if len(inter) >= 3 and len(inter) / len(union) >= 0.7:
        return (f"CFIA near-identical slug/date "
                f"(shared: {'-'.join(sorted(inter))[:60]})")
    return None


def _row_view(row: Dict[str, Any]) -> Dict[str, Any]:
    """Compact, JSON-safe subset of a row for the report/digest."""
    return {
        "Date": str(row.get("Date") or ""),
        "Source": str(row.get("Source") or ""),
        "Company": str(row.get("Company") or ""),
        "Product": str(row.get("Product") or ""),
        "Pathogen": str(row.get("Pathogen") or ""),
        "Country": str(row.get("Country") or ""),
        "Tier": row.get("Tier", ""),
        "URL": str(row.get("URL") or ""),
    }


def _item_id(category: str, row: Dict[str, Any]) -> str:
    """Stable id for a proposed deletion (survives across runs / ledgering)."""
    basis = "|".join([
        category,
        mm._normalize_url_for_dedup(str(row.get("URL") or "")),
        str(row.get("Date") or "")[:10],
        content_key(row),
    ])
    return hashlib.sha1(basis.encode("utf-8")).hexdigest()[:16]


# ── Report-builder imports (for authoritative stale detection) ───────────
def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, str(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


def _week_friday(tag: str) -> Optional[date]:
    """Friday date for a 'W{nn}' tag in 2026, or None if unparseable."""
    m = re.fullmatch(r"[Ww](\d{1,2})", tag or "")
    if not m:
        return None
    try:
        return date.fromisocalendar(2026, int(m.group(1)), 5)
    except ValueError:
        return None


# ── Integrity report (read-only diagnostics) ─────────────────────────────
def build_integrity_report(rows: List[Dict[str, Any]], today: date) -> Dict[str, Any]:
    # Duplicate URLs (normalised) among URL-keyed rows.
    url_groups: Dict[str, List[int]] = {}
    for i, r in enumerate(rows):
        raw = str(r.get("URL") or "").strip()
        if not raw or not has_stable_id(raw):
            continue
        url_groups.setdefault(mm._normalize_url_for_dedup(raw), []).append(i)
    duplicate_urls = []
    shared_nonspecific_urls = []
    for k, v in sorted(url_groups.items()):
        if len(v) <= 1:
            continue
        entry = {"url": k, "count": len(v), "rows": [_row_view(rows[i]) for i in v]}
        # A group whose rows have >1 distinct content identity is NOT a set of
        # duplicates — it is one non-specific URL (an agency listing/search page)
        # shared by DIFFERENT recalls. Deleting any of them would lose a real
        # recall, so it is reported as an integrity defect, never queued.
        if len({content_key(rows[i]) for i in v}) > 1:
            shared_nonspecific_urls.append(entry)
        else:
            duplicate_urls.append(entry)

    # Blank required fields.
    blank_fields = []
    for r in rows:
        missing = [
            f for f in REQUIRED_FIELDS
            if not str(r.get(f) or "").strip()
            or (f == "Pathogen" and is_empty_pathogen(str(r.get("Pathogen") or "")))
        ]
        if missing:
            v = _row_view(r)
            v["missing"] = missing
            blank_fields.append(v)

    # Mis-tiered pathogens: always-Tier-1 pathogen sitting at a lower tier.
    mistiered = []
    for r in rows:
        pathogen = str(r.get("Pathogen") or "")
        if not is_always_tier1(pathogen):
            continue
        try:
            cur = int(r.get("Tier") or 0)
        except (ValueError, TypeError):
            cur = 0
        if cur != 1:
            v = _row_view(r)
            v["current_tier"] = cur if cur else "unset"
            mistiered.append(v)

    # Date sanity: blank, unparseable, or future-dated.
    date_issues = []
    for r in rows:
        ds = str(r.get("Date") or "").strip()
        if not ds:
            v = _row_view(r); v["issue"] = "blank Date"; date_issues.append(v); continue
        try:
            d = datetime.strptime(ds[:10], "%Y-%m-%d").date()
        except ValueError:
            v = _row_view(r); v["issue"] = "unparseable Date"; date_issues.append(v); continue
        if d > today:
            v = _row_view(r); v["issue"] = f"future Date ({ds[:10]} > {today.isoformat()})"
            date_issues.append(v)

    return {
        "duplicate_urls": duplicate_urls,
        "shared_nonspecific_urls": shared_nonspecific_urls,
        "blank_fields": blank_fields,
        "mistiered": mistiered,
        "date_issues": date_issues,
    }


# ── Lane A (safe, deterministic) ─────────────────────────────────────────
def compute_lane_a(rows: List[Dict[str, Any]], today: date) -> List[Dict[str, Any]]:
    """Return a list of change records without mutating `rows`."""
    today_iso = today.isoformat()
    changes: List[Dict[str, Any]] = []
    for i, r in enumerate(rows):
        # 1. tier-1 enforcement
        pathogen = str(r.get("Pathogen") or "")
        if is_always_tier1(pathogen):
            try:
                cur = int(r.get("Tier") or 0)
            except (ValueError, TypeError):
                cur = 0
            if cur != 1:
                changes.append({"row": i, "field": "Tier", "fix": "tier1_enforce",
                                "before": r.get("Tier", ""), "after": 1,
                                "reason": f"{pathogen.strip()} is always Tier 1"})
        # 2. blank DateAdded stamp
        if not str(r.get("DateAdded") or "").strip():
            changes.append({"row": i, "field": "DateAdded", "fix": "stamp_dateadded",
                            "before": "", "after": today_iso,
                            "reason": "blank DateAdded stamped with today (Athens)"})
        # 3. Homepage- artifact strip (Product, Company, Brand)
        for field in ("Product", "Company", "Brand"):
            val = str(r.get(field) or "")
            stripped = _strip_homepage(val)
            if stripped != val:
                changes.append({"row": i, "field": field, "fix": "strip_homepage",
                                "before": val, "after": stripped,
                                "reason": "removed 'Homepage -' scrape artifact"})
        # 4. blank-Date recovery from URL slug
        if not str(r.get("Date") or "").strip():
            recovered = _recover_date_from_url(str(r.get("URL") or ""))
            if recovered:
                changes.append({"row": i, "field": "Date", "fix": "recover_date",
                                "before": "", "after": recovered,
                                "reason": "recovered Date from URL slug"})
    return changes


def apply_lane_a(rows: List[Dict[str, Any]], changes: List[Dict[str, Any]],
                 today: date) -> None:
    """Apply Lane-A changes in place and stamp LastUpdated on touched rows."""
    touched = set()
    today_iso = today.isoformat()
    for c in changes:
        r = rows[c["row"]]
        if c["fix"] == "tier1_enforce":
            enforce_tier1(r)               # sets Tier=1 + provenance note
        elif c["fix"] == "recover_date":
            r["Date"] = c["after"]
            note = str(r.get("Notes") or "")
            stamp = (f"[date-fix {today_iso}: Date was blank; recovered "
                     f"{c['after']} from URL slug]")
            r["Notes"] = (note + " " + stamp).strip() if note else stamp
            if not str(r.get("report_week") or "").strip():
                r["report_week"] = mm.compute_report_week(c["after"])
        else:
            r[c["field"]] = c["after"]
        touched.add(c["row"])
    for i in touched:
        rows[i]["LastUpdated"] = today_iso


# ── Lane B (detect + queue only) ─────────────────────────────────────────
def _best_survivor(indices: List[int], rows: List[Dict[str, Any]]) -> int:
    """Index of the row to KEEP among duplicates (lowest row_rank wins)."""
    return min(indices, key=lambda i: row_rank(rows[i]))


def detect_lane_b(rows: List[Dict[str, Any]], resolved: set) -> List[Dict[str, Any]]:
    proposed: List[Dict[str, Any]] = []
    claimed: set = set()  # row indices already proposed (one proposal per row)

    def _queue(idx: int, survivor: Optional[int], category: str,
               confidence: str, reason: str):
        if idx in claimed:
            return
        item = {
            "id": _item_id(category, rows[idx]),
            "category": category,
            "confidence": confidence,
            "reason": reason,
            "row": _row_view(rows[idx]),
            "duplicate_of": _row_view(rows[survivor]) if survivor is not None else None,
        }
        if item["id"] in resolved:
            return  # already approved/rejected earlier — never re-propose
        claimed.add(idx)
        proposed.append(item)

    # 1. HK CFS foreign re-posts (highest certainty — guard rule).
    for i, r in enumerate(rows):
        if is_foreign_cfs_repost(str(r.get("URL") or ""), str(r.get("Country") or "")):
            _queue(i, None, "cfs_foreign_repost", "high",
                   "cfs.gov.hk row whose origin Country is not Hong Kong — "
                   "cross-source re-post of an upstream regulator's recall")

    # 2. CFIA FR/EN pairs: the SAME recall published on recalls-rappels.canada.ca
    #    in both official languages. EN and FR use TRANSLATED slugs
    #    (/en/alert-recall/... vs /fr/rappel-avis/...), so nothing in the URL
    #    string links the pair — only matching content plus the /en/ vs /fr/
    #    language segment does. Group CFIA rows by content identity; when a
    #    group carries both languages, keep the English row and queue the French.
    cfia_groups: Dict[str, List[int]] = {}
    for i, r in enumerate(rows):
        u = str(r.get("URL") or "")
        if _is_cfia_url(u) and _url_lang(u):
            cfia_groups.setdefault(content_key(r), []).append(i)
    for key, idxs in cfia_groups.items():
        langs = {_url_lang(str(rows[i].get("URL") or "")) for i in idxs}
        if not ({"en", "fr"} <= langs):
            continue  # need at least one EN and one FR to be a language pair
        en_idxs = [i for i in idxs if _url_lang(str(rows[i].get("URL") or "")) == "en"]
        survivor = _best_survivor(en_idxs, rows)  # prefer the English page
        for i in idxs:
            if _url_lang(str(rows[i].get("URL") or "")) == "fr":
                _queue(i, survivor, "cfia_fr_en", "high",
                       "CFIA FR/EN pair — same recall published on "
                       "recalls-rappels.canada.ca in both languages; French "
                       "duplicate of the English row")

    # 2b. CFIA same-recall duplicates (NID or drifted slug/company/date).
    #     Catches the class where CFIA republishes one recall under a new slug
    #     or its "Last updated" date moves, so exact-URL and date-window dedup
    #     both miss it (e.g. the Charlevoisienne & Joe smoked-meat Listeria pair
    #     and the ongoing pistachio investigation rows).
    cfia_idxs = [i for i, r in enumerate(rows) if _is_cfia_url(str(r.get("URL") or ""))]
    for a_pos in range(len(cfia_idxs)):
        i = cfia_idxs[a_pos]
        if i in claimed:
            continue
        for b_pos in range(a_pos + 1, len(cfia_idxs)):
            j = cfia_idxs[b_pos]
            if j in claimed:
                continue
            reason = _cfia_same_recall(rows[i], rows[j])
            if not reason:
                continue
            survivor = _best_survivor([i, j], rows)
            drop = j if survivor == i else i
            _queue(drop, survivor, "cfia_duplicate", "high", reason)

    # 3. Duplicate URLs / fiches (exact normalised-URL collisions).
    url_groups: Dict[str, List[int]] = {}
    for i, r in enumerate(rows):
        raw = str(r.get("URL") or "").strip()
        if not raw or not has_stable_id(raw):
            continue
        url_groups.setdefault(mm._normalize_url_for_dedup(raw), []).append(i)
    for key, idxs in url_groups.items():
        if len(idxs) <= 1:
            continue
        survivor = _best_survivor(idxs, rows)
        survivor_ck = content_key(rows[survivor])
        for i in idxs:
            if i == survivor:
                continue
            # Only queue rows that are the SAME recall as the survivor. Rows
            # sharing a non-specific listing/search URL but with a different
            # content identity are a data-quality defect (reported under
            # integrity.shared_nonspecific_urls), not a duplicate to delete.
            if content_key(rows[i]) != survivor_ck:
                continue
            _queue(i, survivor, "duplicate_url", "high",
                   f"duplicate of an identical normalised URL "
                   f"({len(idxs)} rows share {key})")

    # 4. Cross-source duplicates: same content identity reported by DIFFERENT
    #    sources. A recall's identity is its stable-ID URL (fiche, notification,
    #    permalink), so two DISTINCT stable-ID URLs from the SAME regulator are
    #    two DISTINCT recalls even when date+company+pathogen coincide (e.g.
    #    RappelConso fiche 22794 vs 22798 — same bakery, same day, two recalls).
    #    We therefore only flag a content_key group that spans 2+ sources, and
    #    only queue a row whose survivor is from a different source than itself.
    content_groups: Dict[str, List[int]] = {}
    for i, r in enumerate(rows):
        # content_key is meaningful only with a real date + company.
        if str(r.get("Date") or "").strip() and str(r.get("Company") or "").strip():
            content_groups.setdefault(content_key(r), []).append(i)
    for ck, idxs in content_groups.items():
        if len(idxs) < 2:
            continue
        sources = {str(rows[i].get("Source") or "").strip().lower() for i in idxs}
        if len(sources) < 2:
            continue  # same-source rows with distinct stable IDs are distinct recalls
        survivor = _best_survivor(idxs, rows)
        survivor_src = str(rows[survivor].get("Source") or "").strip().lower()
        for i in idxs:
            if i == survivor:
                continue
            if str(rows[i].get("Source") or "").strip().lower() == survivor_src:
                continue  # keep same-source siblings; only cross-source is a dup
            _queue(i, survivor, "cross_source_dup", "medium",
                   "same content identity (date + company + pathogen) as a row "
                   f"from a different source ({survivor_src or 'unknown'})")

    return proposed


# ── Stale-report detection (excludes the in-progress week/month) ─────────
def detect_stale_reports(rows: List[Dict[str, Any]], today: date) -> Dict[str, List[str]]:
    weeklies: List[str] = []
    monthlies: List[str] = []

    # Weeklies — authoritative via the builder's own filter_week + extractor.
    try:
        bw = _load_module(DOCS / "build_weekly_report_afts.py", "bw_stale")
        tags = sorted({str(r.get("report_week") or "").strip()
                       for r in rows if str(r.get("report_week") or "").strip()},
                      key=lambda t: int(t[1:]) if t[1:].isdigit() else 0)
        for tag in tags:
            friday = _week_friday(tag)
            if friday is None:
                continue
            if friday > today:
                continue  # in-progress week — NEVER published until it closes
            html = DOCS / f"{friday.year}-W{friday.isocalendar()[1]:02d}.html"
            if not html.exists():
                continue  # never issued; not our job to first-publish here
            baked = bw._extract_total_from_html(html)
            dataset = len(bw.filter_week(rows, friday))
            if baked is not None and baked != dataset:
                weeklies.append(tag)
    except Exception as e:  # pragma: no cover - defensive
        log.warning("weekly stale detection skipped: %s", e)

    # Monthlies — closed calendar months only (current month is in progress).
    try:
        bm = _load_module(DOCS / "build_monthly_report_afts.py", "bm_stale")
        cur_month_start = today.replace(day=1)
        for mo in range(1, 13):
            m_start = date(2026, mo, 1)
            if m_start >= cur_month_start:
                break  # current + future months are in progress
            m_end = (date(2026, mo + 1, 1) - timedelta(days=1)) if mo < 12 else date(2026, 12, 31)
            tag = f"M{mo:02d}"
            html = DOCS / f"2026-{tag}.html"
            if not html.exists():
                continue
            baked = bm._extract_total_from_html_monthly(html)
            dataset = len(bm.filter_month(rows, m_start, m_end))
            if baked is not None and baked != dataset:
                monthlies.append(tag)
    except Exception as e:  # pragma: no cover - defensive
        log.warning("monthly stale detection skipped: %s", e)

    return {"weeklies": weeklies, "monthlies": monthlies}


# ── Asset / report integrity (catches orphaned + broken deliverables) ────
# Months that ship a marketing PDF. M01/M02 are HTML-only by standing rule.
_MARKETING_MONTHS = ("M03", "M04", "M05", "M06", "M07", "M08",
                     "M09", "M10", "M11", "M12")


def _monthly_pdf_urls() -> Dict[str, Optional[str]]:
    """month tag -> pdf_url from docs/data/monthly-index.json ('' if absent)."""
    idx = DATA / "monthly-index.json"
    out: Dict[str, Optional[str]] = {}
    if not idx.exists():
        return out
    try:
        data = json.loads(idx.read_text(encoding="utf-8"))
    except (ValueError, OSError):
        return out
    items = data if isinstance(data, list) else (
        data.get("months") or data.get("reports") or data.get("items") or [])
    for m in items:
        if not isinstance(m, dict):
            continue
        fn = str(m.get("filename") or m.get("month") or m.get("id") or "")
        mm_tag = re.search(r"(M\d{2})", fn)
        if mm_tag:
            out[mm_tag.group(1)] = m.get("pdf_url")
    return out


def build_asset_integrity() -> List[Dict[str, Any]]:
    """Flag orphaned/broken/rule-violating report deliverables.

    • orphaned_marketing_pdf : PDF exists on disk but its month has no pdf_url
    • broken_pdf_link        : pdf_url is set but the PDF file is missing
    • unexpected_pdf_link    : M01/M02 carry a pdf_url (should be HTML-only)
    • missing_all_edition    : a monthly main edition has no -all sibling
    """
    issues: List[Dict[str, Any]] = []
    pdf_urls = _monthly_pdf_urls()
    marketing = DOCS / "marketing"
    for tag in _MARKETING_MONTHS:
        pdf = marketing / f"2026-{tag}-marketing.pdf"
        link = pdf_urls.get(tag)
        if pdf.exists() and not link:
            issues.append({"kind": "orphaned_marketing_pdf", "month": tag,
                           "detail": f"{pdf.name} exists but monthly-index pdf_url is null"})
        if link and not pdf.exists():
            issues.append({"kind": "broken_pdf_link", "month": tag,
                           "detail": f"pdf_url set but {pdf.name} is missing on disk"})
    for tag in ("M01", "M02"):
        if pdf_urls.get(tag):
            issues.append({"kind": "unexpected_pdf_link", "month": tag,
                           "detail": f"{tag} has a pdf_url but is HTML-only by rule"})
    for main in sorted(DOCS.glob("2026-M??.html")):
        alled = main.with_name(main.stem + "-all.html")
        if not alled.exists():
            issues.append({"kind": "missing_all_edition", "month": main.stem,
                           "detail": f"{main.name} has no {alled.name} sibling"})
    return issues


# ── Ledger ───────────────────────────────────────────────────────────────
def load_ledger() -> Dict[str, Any]:
    if LEDGER_JSON.exists():
        try:
            data = json.loads(LEDGER_JSON.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                data.setdefault("resolved", {})
                return data
        except (ValueError, OSError) as e:
            log.warning("review_ledger.json unreadable (%s); treating as empty", e)
    return {"resolved": {}}


# ── Digest (human-readable) ──────────────────────────────────────────────
def render_digest(report: Dict[str, Any]) -> str:
    integ = report["integrity"]
    lane_a = report["lane_a"]
    lane_b = report["lane_b"]
    stale = report["stale_reports"]
    lines: List[str] = []
    lines.append(f"# FSIS Daily Review — {report['date']} (Athens)")
    lines.append("")
    lines.append(f"Mode: **{report['mode']}** · generated {report['generated']}")
    lines.append(f"Recalls rows reviewed: **{report['counts']['recalls_rows']}** · "
                 f"in-progress week (never published): **{report['in_progress_week']}**")
    lines.append("")

    lines.append("## 1. Integrity")
    lines.append(f"- Duplicate URL groups: **{len(integ['duplicate_urls'])}**")
    lines.append(f"- Non-specific URLs shared by different recalls: "
                 f"**{len(integ.get('shared_nonspecific_urls', []))}**")
    lines.append(f"- Rows with blank required fields: **{len(integ['blank_fields'])}**")
    lines.append(f"- Mis-tiered (always-Tier-1) rows: **{len(integ['mistiered'])}**")
    lines.append(f"- Date-sanity issues: **{len(integ['date_issues'])}**")
    lines.append("")

    lines.append("## 2. Lane A — safe auto-fixes "
                 + ("(APPLIED)" if lane_a["applied"] else "(would apply)"))
    if lane_a["summary"]:
        for fix, n in sorted(lane_a["summary"].items()):
            lines.append(f"- {fix}: **{n}**")
    else:
        lines.append("- none")
    lines.append("")

    lines.append("## 3. Lane B — proposed deletions (QUEUED, nothing deleted)")
    if lane_b["escalate"]:
        lines.append(f"> ⚠️ **{len(lane_b['proposed_deletions'])} proposed deletions "
                     f"exceed the {lane_b['max_links']}/day link limit — this digest "
                     f"escalates to a single manual review instead of per-item links.**")
        lines.append("")
    if not lane_b["proposed_deletions"]:
        lines.append("- none")
    for it in lane_b["proposed_deletions"]:
        r = it["row"]
        lines.append(f"- **[{it['category']} · {it['confidence']}]** "
                     f"{r['Date']} · {r['Company']} · {r['Pathogen']} — {r['URL']}")
        lines.append(f"  - Evidence: {it['reason']}")
        if it["duplicate_of"]:
            lines.append(f"  - Duplicate of: {it['duplicate_of']['URL']}")
        lines.append(f"  - id `{it['id']}` — Approve: ⟨token⟩ · Reject: ⟨token⟩  "
                     f"_(Phase 2 fills signed links)_")
    lines.append("")

    lines.append("## 4. Reports flagged stale (rebuild → UPDATED masthead)")
    lines.append(f"- Weeklies: {', '.join(stale['weeklies']) or 'none'}")
    lines.append(f"- Monthlies: {', '.join(stale['monthlies']) or 'none'}")
    lines.append("")

    lines.append("## 5. Asset / deliverable integrity")
    assets = report.get("asset_issues", [])
    if not assets:
        lines.append("- none")
    for a in assets:
        lines.append(f"- **[{a['kind']}]** {a['month']}: {a['detail']}")
    lines.append("")
    lines.append("_Lane A applies automatically. Lane B removals require your "
                 "approval and are archived to Weekly_Rejected on execution — "
                 "nothing is ever silently deleted._")
    lines.append("")
    return "\n".join(lines)


# ── Orchestration ────────────────────────────────────────────────────────
def run(dry_run: bool = False, max_links: int = DEFAULT_MAX_LINKS) -> Dict[str, Any]:
    today = _athens_today()
    rows = mm.load_existing(XLSX)

    integrity = build_integrity_report(rows, today)

    lane_a_changes = compute_lane_a(rows, today)
    lane_a_summary: Dict[str, int] = {}
    for c in lane_a_changes:
        lane_a_summary[c["fix"]] = lane_a_summary.get(c["fix"], 0) + 1

    ledger = load_ledger()
    resolved = set(ledger.get("resolved", {}).keys())
    proposed = detect_lane_b(rows, resolved)
    escalate = len(proposed) > max_links

    stale = detect_stale_reports(rows, today)
    asset_issues = build_asset_integrity()
    in_progress = mm.compute_report_week(today.isoformat())

    applied = False
    if not dry_run:
        apply_lane_a(rows, lane_a_changes, today)
        ordered = mm.sort_rows(rows)
        mm.save_xlsx_with_pending(ordered, mm.load_pending(XLSX), XLSX)
        mm.mirror_json_from_xlsx(XLSX, RECALLS_JSON)
        applied = True
        if not LEDGER_JSON.exists():
            LEDGER_JSON.write_text(
                json.dumps({"resolved": {}}, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )

    report = {
        "generated": _athens_now_iso(),
        "date": today.isoformat(),
        "mode": "dry-run" if dry_run else "live",
        "in_progress_week": in_progress,
        "counts": {
            "recalls_rows": len(rows),
            "duplicate_url_groups": len(integrity["duplicate_urls"]),
            "shared_nonspecific_urls": len(integrity["shared_nonspecific_urls"]),
            "blank_field_rows": len(integrity["blank_fields"]),
            "mistiered_rows": len(integrity["mistiered"]),
            "date_issue_rows": len(integrity["date_issues"]),
            "lane_a_changes": len(lane_a_changes),
            "proposed_deletions": len(proposed),
            "ledger_resolved": len(resolved),
            "asset_issues": len(asset_issues),
        },
        "integrity": integrity,
        "asset_issues": asset_issues,
        "lane_a": {"applied": applied, "summary": lane_a_summary, "changes": lane_a_changes},
        "lane_b": {"proposed_deletions": proposed, "escalate": escalate,
                   "max_links": max_links},
        "stale_reports": stale,
    }

    # The report + digest are non-destructive artifacts and are written in
    # BOTH modes so a dry-run still produces reviewable output.
    DATA.mkdir(parents=True, exist_ok=True)
    REPORT_JSON.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    DIGEST_MD.write_text(render_digest(report), encoding="utf-8")

    return report


def _print_summary(report: Dict[str, Any]) -> None:
    c = report["counts"]
    print("=" * 68)
    print(f"FSIS Daily Review Agent — {report['date']} Athens — mode={report['mode']}")
    print("=" * 68)
    print(f"Recalls rows reviewed        : {c['recalls_rows']}")
    print(f"In-progress week (unpublished): {report['in_progress_week']}")
    print("-- Integrity --")
    print(f"  duplicate URL groups       : {c['duplicate_url_groups']}")
    print(f"  shared non-specific URLs   : {c['shared_nonspecific_urls']}")
    print(f"  rows with blank fields     : {c['blank_field_rows']}")
    print(f"  mis-tiered (always-Tier-1) : {c['mistiered_rows']}")
    print(f"  date-sanity issues         : {c['date_issue_rows']}")
    print("-- Lane A (safe auto-fixes) --", "APPLIED" if report["lane_a"]["applied"] else "detected only")
    for fix, n in sorted(report["lane_a"]["summary"].items()):
        print(f"  {fix:22s} : {n}")
    if not report["lane_a"]["summary"]:
        print("  (none)")
    print("-- Lane B (proposed deletions, QUEUED — nothing deleted) --")
    by_cat: Dict[str, int] = {}
    for it in report["lane_b"]["proposed_deletions"]:
        by_cat[it["category"]] = by_cat.get(it["category"], 0) + 1
    for cat, n in sorted(by_cat.items()):
        print(f"  {cat:22s} : {n}")
    if not by_cat:
        print("  (none)")
    print(f"  escalate-to-manual         : {report['lane_b']['escalate']} "
          f"(> {report['lane_b']['max_links']}/day)")
    print("-- Stale reports to rebuild (UPDATED masthead) --")
    print(f"  weeklies : {', '.join(report['stale_reports']['weeklies']) or 'none'}")
    print(f"  monthlies: {', '.join(report['stale_reports']['monthlies']) or 'none'}")
    print("-- Asset / deliverable integrity --")
    if report["asset_issues"]:
        for a in report["asset_issues"]:
            print(f"  [{a['kind']}] {a['month']}: {a['detail']}")
    else:
        print("  (none)")
    print("-- Outputs --")
    print(f"  {REPORT_JSON.relative_to(ROOT)}")
    print(f"  {DIGEST_MD.relative_to(ROOT)}")
    print("=" * 68)


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="FSIS Daily Review Agent (Phase 1)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Report + queue only; apply no Lane-A fixes and write "
                         "nothing to recalls.xlsx / recalls.json.")
    ap.add_argument("--max-links", type=int, default=DEFAULT_MAX_LINKS,
                    help=f"Proposed-deletion count above which the digest escalates "
                         f"to a single manual-review email (default {DEFAULT_MAX_LINKS}).")
    ap.add_argument("--verbose", "-v", action="store_true",
                    help="Verbose (DEBUG-level) logging.")
    args = ap.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    report = run(dry_run=args.dry_run, max_links=args.max_links)
    _print_summary(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
