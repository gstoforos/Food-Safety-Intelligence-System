"""
Sunday Gemini QA — weekly deep audit of the Recalls sheet.

Replaces sunday_claude_qa.py. Claude was pattern-matching from training data
and hallucinating fiche IDs. Gemini 2.5 Flash with native Google Search
grounding fires real searches, so every URL it proposes is one that actually
appeared in a search result.

Runs every Sunday 23:00 Athens via FsisScheduler dispatch.

THREE-LAYER AUDIT
=================

Layer 1 — Deterministic fixes (no AI, no cost):
  • Country → Region normalization to canonical 6-region taxonomy
  • Pathogen → Tier consistency (STEC/Listeria/Botulinum/cereulide → Tier-1)
  • Missing Company / placeholder fills for known multi-producer alerts
  • Auto-append /Interne to RappelConso URLs missing it

Layer 2 — API pre-pass via regulator_apis.py (no AI, no cost):
  • Every France row with /categorie/ → query RappelConso open-data API for
    canonical fiche-rappel/NNNNN/Interne
  • Other API-having countries: cross-check URL against official feed

Layer 3 — Gemini AI verification with Google Search grounding (paid):
  • Only for rows that survived Layer 1+2 with structural concerns
  • The 4 ChatGPT-discovered rules baked into the system prompt:
      Rule 1 — France: hallucinated IDs, must Google-verify
      Rule 2 — Greece EFET: hallucinated IDs/slugs, must Google-verify
      Rule 3 — USA FDA/FSIS + Ireland FSAI: DO NOT TOUCH truncated URLs
      Rule 4 — Others: verify, don't aggressive-rewrite

OUTPUTS
=======
  • Deterministic + API fixes applied IN-PLACE to docs/data/recalls.xlsx
  • Markdown QA report committed to docs/data/sunday-qa/<date>.md
  • Git commit + push at end

COST
====
  ~$0.05–$0.15/run (Gemini 2.5 Flash, ~50–80 rows in 14-day window).
  Runs once per week → < $1/month. On Gemini free tier: $0.

INVOCATION
==========
  python -m pipeline.sunday_gemini_qa
  python -m pipeline.sunday_gemini_qa --days 30
  python -m pipeline.sunday_gemini_qa --dry-run
  python -m pipeline.sunday_gemini_qa --no-ai     # deterministic only

Disabled cleanly when no GEMINI_API_KEY env var is present.
"""
from __future__ import annotations
import argparse
import json
import logging
import os
import re
import sys
import urllib.parse
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

import openpyxl
import requests

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pipeline.commit_github import git_commit_and_push  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("sunday-gemini-qa")

# ───────────────────────────────────────────────────────────────────────
# Configuration
# ───────────────────────────────────────────────────────────────────────
XLSX_PATH = ROOT / "docs" / "data" / "recalls.xlsx"
QA_DIR = ROOT / "docs" / "data" / "sunday-qa"

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

DEFAULT_AUDIT_DAYS = 14

# Canonical FSIS taxonomy (per User Guide § Daily briefs)
CANONICAL_REGIONS = {
    "Europe", "North America", "Latin America",
    "Asia-Pacific", "Middle East & Africa", "Oceania",
}

COUNTRY_TO_REGION = {
    # Europe
    "France": "Europe", "Germany": "Europe", "Italy": "Europe",
    "Spain": "Europe", "Greece": "Europe", "United Kingdom": "Europe",
    "Switzerland": "Europe", "Ireland": "Europe", "Austria": "Europe",
    "Czech Republic": "Europe", "Slovakia": "Europe", "Belgium": "Europe",
    "Netherlands": "Europe", "Portugal": "Europe", "Sweden": "Europe",
    "Norway": "Europe", "Denmark": "Europe", "Finland": "Europe",
    "Poland": "Europe", "Hungary": "Europe", "Romania": "Europe",
    "Bulgaria": "Europe", "Croatia": "Europe", "Turkey": "Europe",
    "Slovenia": "Europe", "Estonia": "Europe", "Latvia": "Europe",
    "Lithuania": "Europe", "Luxembourg": "Europe", "Cyprus": "Europe",
    "Malta": "Europe", "Iceland": "Europe",
    # North America
    "USA": "North America", "United States": "North America",
    "Canada": "North America",
    # Latin America (Mexico per User Guide convention)
    "Mexico": "Latin America", "Brazil": "Latin America",
    "Colombia": "Latin America", "Argentina": "Latin America",
    "Chile": "Latin America", "Peru": "Latin America",
    "Venezuela": "Latin America", "Ecuador": "Latin America",
    "Uruguay": "Latin America", "Paraguay": "Latin America",
    "Bolivia": "Latin America", "Costa Rica": "Latin America",
    "Panama": "Latin America", "Guatemala": "Latin America",
    "Dominican Republic": "Latin America", "Cuba": "Latin America",
    # Asia-Pacific
    "Japan": "Asia-Pacific", "South Korea": "Asia-Pacific",
    "China": "Asia-Pacific", "Singapore": "Asia-Pacific",
    "Hong Kong": "Asia-Pacific", "Taiwan": "Asia-Pacific",
    "Vietnam": "Asia-Pacific", "Thailand": "Asia-Pacific",
    "Malaysia": "Asia-Pacific", "Indonesia": "Asia-Pacific",
    "Philippines": "Asia-Pacific", "India": "Asia-Pacific",
    "Pakistan": "Asia-Pacific", "Bangladesh": "Asia-Pacific",
    # Oceania
    "Australia": "Oceania", "New Zealand": "Oceania",
    "Fiji": "Oceania", "Papua New Guinea": "Oceania",
    # Middle East & Africa
    "South Africa": "Middle East & Africa", "Kenya": "Middle East & Africa",
    "Kenya / Comesa": "Middle East & Africa", "Egypt": "Middle East & Africa",
    "Morocco": "Middle East & Africa", "Nigeria": "Middle East & Africa",
    "Saudi Arabia": "Middle East & Africa", "UAE": "Middle East & Africa",
    "Israel": "Middle East & Africa", "Lebanon": "Middle East & Africa",
    "Jordan": "Middle East & Africa", "Tunisia": "Middle East & Africa",
    "Algeria": "Middle East & Africa", "Ghana": "Middle East & Africa",
    "Ethiopia": "Middle East & Africa", "Tanzania": "Middle East & Africa",
    "Uganda": "Middle East & Africa", "Zimbabwe": "Middle East & Africa",
    "Botswana": "Middle East & Africa", "Namibia": "Middle East & Africa",
}

TIER1_PATHOGENS = {
    "listeria", "listeria monocytogenes",
    "stec", "e. coli stec", "e. coli o157", "shiga",
    "c. botulinum", "clostridium botulinum", "botulinum",
    "cereulide", "bacillus cereus (cereulide)",
    "hepatitis a", "hepatitis e",
    "vibrio", "vibrio parahaemolyticus", "vibrio cholerae",
    "cronobacter",
    "rodenticide", "rat poison", "bromadiolone",
    "heavy metal", "lead", "cadmium", "arsenic", "mercury",
}

# Domains where the agency CMS enforces strict slug length — DO NOT touch
# URLs that look truncated mid-word (FDA, FSIS, FSAI).
TRUNCATED_BUT_VALID_DOMAINS = (
    "fda.gov",
    "fsis.usda.gov",
    "fsai.ie",
)

RAPPELCONSO_DOMAIN = "rappel.conso.gouv.fr"


# ───────────────────────────────────────────────────────────────────────
# URL helpers — same logic as url_gate_gemini.py for consistency
# ───────────────────────────────────────────────────────────────────────
def _is_structurally_bad(url: str) -> Optional[str]:
    """Return rejection reason if URL is structurally bad, else None."""
    if not url or not url.startswith("http"):
        return "not a URL"
    bad_patterns = [
        "/categorie/", "/rubrik/", "/tag/", "/category/",
        "/search?", "/recherche?",
    ]
    for pat in bad_patterns:
        if pat in url:
            return f"listing/category URL ({pat})"
    p = urllib.parse.urlparse(url)
    segs = [s for s in (p.path or "").split("/") if s]
    if len(segs) == 0:
        return "domain only, no path"
    return None


def _is_truncation_protected(url: str) -> bool:
    """True if URL is on FDA/FSIS/FSAI — the truncated-but-valid agencies."""
    if not url:
        return False
    p = urllib.parse.urlparse(url)
    host = (p.netloc or "").lower()
    return any(host.endswith(d) for d in TRUNCATED_BUT_VALID_DOMAINS)


def _strip_fences(txt: str) -> str:
    t = txt.strip()
    if t.startswith("```"):
        t = re.sub(r"^```[a-zA-Z]*\s*\n", "", t)
        t = re.sub(r"\n```\s*$", "", t)
    return t.strip()


def _collect_gemini_keys() -> List[str]:
    keys: List[str] = []
    legacy = os.getenv("GEMINI_API_KEY")
    if legacy:
        keys.append(legacy.strip())
    for i in range(1, 11):
        v = os.getenv(f"GEMINI_API_KEY_{i}")
        if v and v.strip() not in keys:
            keys.append(v.strip())
    return keys


# ───────────────────────────────────────────────────────────────────────
# Layer 1: Deterministic fixers
# ───────────────────────────────────────────────────────────────────────
def fix_region(country: str, current_region: str) -> Optional[str]:
    """Return a corrected region or None if unchanged/unknown."""
    if not country:
        return None
    expected = COUNTRY_TO_REGION.get(country.strip())
    if expected and expected != current_region:
        return expected
    if current_region not in CANONICAL_REGIONS and expected:
        return expected
    return None


def fix_tier(pathogen: str, current_tier: Any) -> Optional[int]:
    """Return corrected tier or None if unchanged."""
    if not pathogen:
        return None
    p_lower = str(pathogen).lower().strip()
    is_tier1 = any(t in p_lower for t in TIER1_PATHOGENS)
    try:
        cur = int(current_tier) if current_tier is not None else 0
    except (TypeError, ValueError):
        cur = 0
    if is_tier1 and cur != 1:
        return 1
    return None


def fix_rappelconso_interne(url: str) -> Optional[str]:
    """Append /Interne to RappelConso fiche-rappel URLs missing it (Rule 1)."""
    if not url or RAPPELCONSO_DOMAIN not in url:
        return None
    if "/fiche-rappel/" not in url:
        return None
    if url.rstrip("/").endswith("/Interne"):
        return None
    # Extract the numeric ID. The scraper sometimes injects a bad /YYYY/
    # prefix (e.g. /fiche-rappel/2026/17370) — find the LAST numeric segment
    # following /fiche-rappel/ to get the real ID.
    after = url.split("/fiche-rappel/", 1)[1]
    nums = re.findall(r"\d+", after)
    if not nums:
        return None
    fiche_id = nums[-1]  # last numeric segment is the actual ID
    return f"https://{RAPPELCONSO_DOMAIN}/fiche-rappel/{fiche_id}/Interne"


# ───────────────────────────────────────────────────────────────────────
# Gemini call helper — with Google Search grounding
# ───────────────────────────────────────────────────────────────────────
def _call_gemini_grounded(prompt: str, system: str,
                           max_tokens: int = 4000) -> Optional[str]:
    """Single Gemini call with Google Search grounding. Returns text or None."""
    try:
        import google.generativeai as genai  # type: ignore
    except ImportError:
        log.warning("google-generativeai not installed; Gemini QA disabled")
        return None

    keys = _collect_gemini_keys()
    if not keys:
        return None

    tool_configs = [
        [{"google_search": {}}],
        [{"google_search_retrieval": {}}],
        "google_search_retrieval",
    ]

    last_error: Optional[Exception] = None
    for api_key in keys:
        for tool_cfg in tool_configs:
            try:
                genai.configure(api_key=api_key)
                model = genai.GenerativeModel(
                    GEMINI_MODEL, system_instruction=system,
                )
                resp = model.generate_content(
                    prompt,
                    tools=tool_cfg,
                    generation_config={"max_output_tokens": max_tokens,
                                       "temperature": 0.1},
                )
                text = (getattr(resp, "text", None) or "").strip()
                if text:
                    try:
                        gm = resp.candidates[0].grounding_metadata
                        queries = getattr(gm, "web_search_queries", []) or []
                        if queries:
                            log.debug("Gemini ran %d Google searches",
                                      len(queries))
                    except Exception:
                        pass
                    return text
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                log.debug("Gemini attempt failed: %s", str(exc)[:150])
                continue

    log.warning("Gemini QA: all keys/configs failed. Last error: %s", last_error)
    return None


# ───────────────────────────────────────────────────────────────────────
# Layer 3: Gemini AI verification — strict prompt with the 4 rules
# ───────────────────────────────────────────────────────────────────────
QA_SYSTEM_PROMPT = """You are the weekly URL & data integrity auditor for the \
FSIS food-safety dashboard. You have Google Search. For every flagged row, \
run real searches and verify URLs against the actual recall facts.

CANONICAL REGIONS (one of exactly these six):
  Europe, North America, Latin America, Asia-Pacific, Middle East & Africa, Oceania

TIER CLASSIFICATION:
  Tier 1 = Listeria monocytogenes, STEC/E. coli O157, C. botulinum,
           cereulide / Bacillus cereus, Cronobacter, Hepatitis A,
           rodenticides, heavy metals
  Tier 2 = Salmonella, Campylobacter, Norovirus, histamine, mycotoxins
  Tier 3 = lower-severity contaminants

NEVER guess fiche/recall IDs (they are chronological across all categories,
not topical). NEVER fabricate a URL. If verification fails, flag the row
'needs_manual_review' rather than proposing a wrong URL.

Return STRICT JSON, no preamble."""


QA_PROMPT_TEMPLATE = """Audit this food-recall row and propose corrections if needed.

═══ ROW ═══
  Date:     {date}
  Source:   {source}
  Company:  {company}
  Brand:    {brand}
  Product:  {product}
  Pathogen: {pathogen}
  Reason:   {reason}
  Country:  {country}
  Region:   {region}
  Tier:     {tier}
  URL:      {url}
  Notes:    {notes}

═══ MANDATORY URL VERIFICATION WORKFLOW ═══

STEP 1 — Compose Google query: "<agency> <brand-or-company> <date> <pathogen>"
         Example: "RappelConso Belle Henriette 2026-04-23 Listeria"
STEP 2 — Run web_search.
STEP 3 — Find the result on the AGENCY'S OFFICIAL DOMAIN whose snippet
         matches date + brand + hazard.
STEP 4 — Verify all four:
         ✓ date_match (page date == row Date, ±1 day)
         ✓ brand_match (page mentions the brand or company)
         ✓ hazard_match (page mentions the pathogen)
         ✓ is_detail_page (URL is NOT /categorie/, /rubrik/, /tag/,
           /search?, or a bare domain)

═══ AGENCY-SPECIFIC RULES ═══

[RULE 1 — France RappelConso]
  Domain: rappel.conso.gouv.fr
  • URL MUST end with /Interne (e.g. /fiche-rappel/22114/Interne)
  • Scraper frequently hallucinates sequential IDs (17394, 17399, 17400, 17401).
    Do NOT trust the existing ID — verify it via Google every time.
  • Reject any URL containing /categorie/.

[RULE 2 — Greece EFET]
  Domain: efet.gr
  • Numeric item IDs and Greek-to-English slugs are frequently hallucinated.
  • Always Google-verify by product name.
  • Correct URL pattern:
    https://www.efet.gr/index.php/el/enimerosi/deltia-typou/anakleiseis-cat/item/NNNN-...

[RULE 3 — USA FDA / USDA FSIS / Ireland FSAI — DO NOT TOUCH]
  Domains: fda.gov, fsis.usda.gov, fsai.ie
  • These agency CMSes enforce strict URL-slug length.
  • URLs that LOOK truncated mid-word (ending in "-because", "-due",
    "-possible-health-risk", "-gr") are the OFFICIAL working links.
  • If a URL is on these domains, set verification.is_detail_page=true and
    do NOT propose a "fix" unless the URL is structurally a category page.
  • Pass these through with confidence=0.95 and strategy="agency-cms-truncated".

[RULE 4 — All other agencies]
  • Verify URL is on the agency's official domain
  • Verify URL is a specific recall page, not an index/listing
  • If URL fails verification, propose a corrected URL ONLY if Google
    Search returned a verified match
  • If no verified match, set pass=false with reason "needs manual review"
  • NEVER guess, NEVER fabricate

═══ OUTPUT (STRICT JSON) ═══

{{
  "issues": ["short description of each issue, or empty list"],
  "url_corrected": "<canonical URL or null if no fix>",
  "fixes": {{
    "Pathogen": "<corrected value or null>",
    "Region":   "<corrected value or null>",
    "Tier":     <1|2|3 or null>,
    "Country":  "<corrected value or null>"
  }},
  "verification": {{
    "date_match":     true|false,
    "brand_match":    true|false,
    "hazard_match":   true|false,
    "is_detail_page": true|false
  }},
  "google_query_used": "<the query you actually ran>",
  "confidence": 0.0-1.0,
  "strategy": "agency-cms-truncated | api-lookup | search-verified | failed"
}}
"""


def gemini_audit_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Ask Gemini (with Google Search grounding) to audit one row."""
    prompt = QA_PROMPT_TEMPLATE.format(
        date=row.get("Date", ""),
        source=row.get("Source", ""),
        company=row.get("Company", ""),
        brand=row.get("Brand", ""),
        product=row.get("Product", ""),
        pathogen=row.get("Pathogen", ""),
        reason=str(row.get("Reason") or "")[:400],
        country=row.get("Country", ""),
        region=row.get("Region", ""),
        tier=row.get("Tier", ""),
        url=row.get("URL", ""),
        notes=str(row.get("Notes") or "")[:200],
    )
    raw = _call_gemini_grounded(prompt, system=QA_SYSTEM_PROMPT,
                                 max_tokens=2000)
    if not raw:
        return {"issues": [], "url_corrected": None, "fixes": {},
                "verification": {}, "confidence": 0.0, "strategy": "failed"}
    try:
        return json.loads(_strip_fences(raw))
    except json.JSONDecodeError as e:
        log.debug("Gemini QA JSON parse failed: %s | raw=%s", e, raw[:200])
        return {"issues": [], "url_corrected": None, "fixes": {},
                "verification": {}, "confidence": 0.0, "strategy": "failed"}


# ───────────────────────────────────────────────────────────────────────
# Main audit loop
# ───────────────────────────────────────────────────────────────────────
def audit(days: int = DEFAULT_AUDIT_DAYS, dry_run: bool = False,
          ai_check: bool = True) -> Dict[str, Any]:
    """Run the Sunday QA. Writes fixes back to xlsx + report to sunday-qa/."""
    if not XLSX_PATH.exists():
        log.error("Recalls file missing: %s", XLSX_PATH)
        return {"error": "no xlsx"}

    wb = openpyxl.load_workbook(XLSX_PATH)
    ws = wb["Recalls"]
    H = {ws.cell(1, c).value: c for c in range(1, ws.max_column + 1)}

    today = date.today()
    cutoff = (today - timedelta(days=days)).isoformat()

    findings: List[Dict[str, Any]] = []
    det_fixes = {"region": 0, "tier": 0, "company_filled": 0,
                 "interne_appended": 0}
    api_fixes_count = 0
    ai_flags: List[Dict[str, Any]] = []

    # ── Layer 1+2: Deterministic + API pre-pass ─────────────────────────
    try:
        from pipeline.regulator_apis import repair_french_row
        api_available = True
    except ImportError:
        log.warning("regulator_apis not importable — API pre-pass skipped")
        api_available = False
        repair_french_row = None  # type: ignore

    rows_in_window: List[Tuple[int, Dict[str, Any]]] = []
    for r in range(2, ws.max_row + 1):
        row_date = str(ws.cell(r, H["Date"]).value or "")
        if row_date < cutoff:
            continue

        row = {k: ws.cell(r, H[k]).value for k in H}
        rows_in_window.append((r, row))

        # Layer 1.A — Region fix
        new_region = fix_region(
            str(row.get("Country") or ""),
            str(row.get("Region") or ""),
        )
        if new_region:
            findings.append({
                "row": r, "date": row_date, "field": "Region",
                "old": row.get("Region"), "new": new_region,
                "kind": "deterministic",
            })
            if not dry_run:
                ws.cell(r, H["Region"]).value = new_region
            det_fixes["region"] += 1

        # Layer 1.B — Tier fix
        new_tier = fix_tier(str(row.get("Pathogen") or ""), row.get("Tier"))
        if new_tier:
            findings.append({
                "row": r, "date": row_date, "field": "Tier",
                "old": row.get("Tier"), "new": new_tier,
                "kind": "deterministic",
                "note": f"pathogen '{row.get('Pathogen')}' is Tier-1",
            })
            if not dry_run:
                ws.cell(r, H["Tier"]).value = new_tier
            det_fixes["tier"] += 1

        # Layer 1.C — Missing Company for known multi-producer alerts
        company = str(row.get("Company") or "").strip()
        if company in ("", "None", "nan", "—", "-"):
            src = str(row.get("Source") or "")
            if "BLV" in src:
                if not dry_run:
                    ws.cell(r, H["Company"]).value = (
                        "(BLV public warning — multiple producers)"
                    )
                det_fixes["company_filled"] += 1

        # Layer 1.D — Append /Interne to RappelConso URLs (Rule 1)
        url = str(row.get("URL") or "")
        new_url = fix_rappelconso_interne(url)
        if new_url:
            findings.append({
                "row": r, "date": row_date, "field": "URL",
                "old": url, "new": new_url, "kind": "interne_appended",
            })
            if not dry_run:
                ws.cell(r, H["URL"]).value = new_url
            det_fixes["interne_appended"] += 1
            row["URL"] = new_url  # update local view for downstream layers

        # Layer 2 — API pre-pass (France only for now)
        if (api_available
                and str(row.get("Country") or "").strip() == "France"
                and "/categorie/" in str(row.get("URL") or "")):
            try:
                api_url = repair_french_row(row)  # type: ignore
                if api_url and api_url != row.get("URL"):
                    findings.append({
                        "row": r, "date": row_date, "field": "URL",
                        "old": row.get("URL"), "new": api_url,
                        "kind": "api-prepass",
                        "note": "RappelConso open-data API",
                    })
                    if not dry_run:
                        ws.cell(r, H["URL"]).value = api_url
                    api_fixes_count += 1
                    row["URL"] = api_url
            except Exception as e:
                log.debug("API pre-pass failed for row %d: %s", r, e)

    # ── Layer 3: Gemini AI verification on remaining concerns ───────────
    gemini_keys_present = bool(_collect_gemini_keys())

    if ai_check and gemini_keys_present:
        for r, row in rows_in_window:
            url = str(row.get("URL") or "")

            # Skip Layer-3 for FDA/FSIS/FSAI (Rule 3 — DO NOT TOUCH)
            if _is_truncation_protected(url):
                continue

            # Only call Gemini on rows that look problematic
            looks_problematic = (
                _is_structurally_bad(url) is not None
                or "needs specific url" in str(row.get("Notes") or "").lower()
                or "needs manual review" in str(row.get("Notes") or "").lower()
                or "needs api" in str(row.get("Notes") or "").lower()
                or "efet.gr" in url  # Greece — always verify per Rule 2
            )

            if not looks_problematic:
                continue

            result = gemini_audit_row(row)
            v = result.get("verification") or {}
            confidence = float(result.get("confidence", 0.0) or 0.0)
            url_fix = result.get("url_corrected") or None

            # Local verification gate — same as url_gate_gemini
            all_v_ok = all([
                v.get("date_match"), v.get("brand_match"),
                v.get("hazard_match"), v.get("is_detail_page"),
            ])

            if url_fix and confidence >= 0.6 and all_v_ok:
                # Reject structurally-bad fix
                if (not _is_truncation_protected(url_fix)
                        and _is_structurally_bad(url_fix)):
                    ai_flags.append({
                        "row": r, "date": str(row.get("Date") or ""),
                        "company": str(row.get("Company") or "")[:40],
                        "issues": [f"Gemini proposed structurally bad URL: {url_fix}"],
                        "confidence": confidence,
                    })
                else:
                    findings.append({
                        "row": r, "date": str(row.get("Date") or ""),
                        "field": "URL", "old": url, "new": url_fix,
                        "kind": "gemini-verified",
                        "note": f"conf={confidence:.2f}, query={result.get('google_query_used','')[:60]}",
                    })
                    if not dry_run:
                        ws.cell(r, H["URL"]).value = url_fix
            else:
                if result.get("issues") or url:
                    ai_flags.append({
                        "row": r, "date": str(row.get("Date") or ""),
                        "company": str(row.get("Company") or "")[:40],
                        "issues": result.get("issues", [])[:3] or [
                            "verification failed"],
                        "confidence": confidence,
                    })

    if not dry_run:
        wb.save(XLSX_PATH)
        log.info("Saved fixes to %s", XLSX_PATH)

    # ── Write Markdown report ──────────────────────────────────────────
    QA_DIR.mkdir(parents=True, exist_ok=True)
    report_path = QA_DIR / f"{today.isoformat()}.md"
    report_lines = [
        f"# FSIS Sunday QA Report — {today.isoformat()}",
        "",
        f"**Audit window:** last {days} days (since {cutoff})",
        f"**Engine:** Gemini 2.5 Flash with Google Search grounding",
        f"**Total findings:** {len(findings)} fixes, {len(ai_flags)} AI flags",
        "",
        f"**Layer 1 (deterministic):** "
        f"region={det_fixes['region']}, tier={det_fixes['tier']}, "
        f"company_filled={det_fixes['company_filled']}, "
        f"/Interne appended={det_fixes['interne_appended']}",
        f"**Layer 2 (API pre-pass):** {api_fixes_count} URL fixes",
        f"**Layer 3 (Gemini grounded):** "
        f"{sum(1 for f in findings if f.get('kind')=='gemini-verified')} URL fixes verified",
        "",
        "## All applied fixes",
        "",
    ]
    if findings:
        report_lines.append("| Row | Date | Field | Old | New | Note |")
        report_lines.append("|---|---|---|---|---|---|")
        for f in findings:
            note = f.get("note", f.get("kind", ""))
            old_short = str(f.get("old", ""))[:60]
            new_short = str(f.get("new", ""))[:60]
            report_lines.append(
                f"| {f['row']} | {f['date']} | {f['field']} "
                f"| {old_short} | {new_short} | {note} |"
            )
    else:
        report_lines.append("_No fixes applied._")

    report_lines += ["", "## AI flags (manual review needed)", ""]
    if ai_flags:
        report_lines.append("| Row | Date | Company | Issues | Confidence |")
        report_lines.append("|---|---|---|---|---|")
        for f in ai_flags:
            issues = "; ".join(f.get("issues") or [])[:100]
            report_lines.append(
                f"| {f['row']} | {f['date']} | {f['company']} "
                f"| {issues} | {f.get('confidence', 0):.2f} |"
            )
    else:
        report_lines.append("_No AI flags._")

    report_lines += [
        "",
        "---",
        f"_Generated by `pipeline.sunday_gemini_qa` at "
        f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}_",
    ]

    if not dry_run:
        report_path.write_text("\n".join(report_lines), encoding="utf-8")
        log.info("Wrote report: %s", report_path)

    summary = {
        "audit_date": today.isoformat(),
        "audit_days": days,
        "deterministic_fixes": det_fixes,
        "api_fixes": api_fixes_count,
        "gemini_url_fixes": sum(1 for f in findings
                                if f.get("kind") == "gemini-verified"),
        "ai_flags": len(ai_flags),
        "report_path": str(report_path.relative_to(ROOT)),
    }
    log.info("QA summary: %s", json.dumps(summary, indent=2))
    return summary


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=DEFAULT_AUDIT_DAYS,
                    help=f"Audit window in days (default {DEFAULT_AUDIT_DAYS})")
    ap.add_argument("--dry-run", action="store_true", help="Don't write changes")
    ap.add_argument("--no-ai", action="store_true",
                    help="Skip Gemini AI checks (deterministic + API only)")
    args = ap.parse_args()

    summary = audit(days=args.days, dry_run=args.dry_run,
                    ai_check=not args.no_ai)

    if "error" in summary:
        return 1

    # Commit if anything changed
    if not args.dry_run:
        any_fix = (any(summary["deterministic_fixes"].values())
                   or summary["api_fixes"] > 0
                   or summary["gemini_url_fixes"] > 0)
        if any_fix or summary["ai_flags"] > 0:
            paths = [str(XLSX_PATH), summary["report_path"]]
            msg = (
                f"Sunday Gemini QA {summary['audit_date']}: "
                f"{summary['deterministic_fixes']['region']} region, "
                f"{summary['deterministic_fixes']['tier']} tier, "
                f"{summary['deterministic_fixes']['interne_appended']} /Interne, "
                f"{summary['api_fixes']} API, "
                f"{summary['gemini_url_fixes']} Gemini, "
                f"{summary['ai_flags']} flags"
            )
            git_commit_and_push(ROOT, paths, msg)

    return 0


if __name__ == "__main__":
    sys.exit(main())
