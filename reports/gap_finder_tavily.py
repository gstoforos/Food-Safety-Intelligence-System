"""
Tavily gap-finder — uses Tavily's AI-optimized search API to find pathogen
recalls the other gap-finders missed.

Tavily free tier: 1,000 searches/month. At once-daily with ~2-3 targeted
searches per run, monthly spend = ~60-90 calls = ~8% of free quota.

Why Tavily as a 4th gap-finder:
  - Different search index than Google (used by Gemini) → catches results
    Google's SERP didn't rank highly
  - Designed for LLM consumption — returns clean snippets + URLs ranked
    by relevance-to-query, not by traditional SEO signals
  - Cheaper than wiring another LLM with a paid web_search tool

Architecture:
  1. Run 3 targeted Tavily searches (recent recalls, global illness,
     RASFF/regulator notices from past 7 days)
  2. Extract URLs from results
  3. Filter to regulator domains (whitelist)
  4. Use Gemini Flash (free tier) to extract structured recall fields from
     each candidate URL's snippet — zero paid LLM calls
  5. Write to Pending, same as every other gap-finder

Cost: $0 (Tavily free tier + Gemini free tier). Runs once per day at 14:00
Athens (after the morning scrapers, Claude/OpenAI/Gemini gap-finders, and
the URL gate have all had their turn — Tavily's job is filling remaining gaps).
"""
from __future__ import annotations
import os
import sys
import json
import logging
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse

import requests

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scrapers._models import (  # noqa: E402
    Recall, normalize_pathogen, normalize_country, infer_region, assign_tier,
)
from pipeline.merge_master import (  # noqa: E402
    load_existing, load_pending,
    append_to_pending, sort_rows, save_xlsx_with_pending,
)
from pipeline.commit_github import git_commit_and_push  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("gap-finder-tavily")

DATA_DIR = ROOT / "docs" / "data"
XLSX_PATH = DATA_DIR / "recalls.xlsx"

SINCE_DAYS = int(os.getenv("GAP_SINCE_DAYS", "7"))
SKIP_COMMIT = os.getenv("SKIP_COMMIT", "").lower() in ("1", "true", "yes")
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY", "").strip()
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")

TAVILY_ENDPOINT = "https://api.tavily.com/search"

# ---------------------------------------------------------------------------
# Regulator domain whitelist — only accept Tavily results from these sites.
# Any URL not on this list is dropped before the Gemini extraction step.
# ---------------------------------------------------------------------------
REGULATOR_DOMAINS = {
    # North America
    "fda.gov", "fsis.usda.gov", "cdc.gov", "accessdata.fda.gov",
    "inspection.canada.ca", "recalls-rappels.canada.ca",
    "quebec.ca",
    # EU & UK
    "food.gov.uk", "foodstandards.gov.scot", "fsai.ie",
    "rappelconso.gouv.fr", "agriculture.gouv.fr",
    "aesan.gob.es", "lebensmittelwarnung.de", "bvl.bund.de", "bfr.bund.de",
    "ages.at", "salute.gov.it", "nvwa.nl", "favv-afsca.be", "afsca.be",
    "foedevarestyrelsen.dk", "livsmedelsverket.se", "mattilsynet.no",
    "ruokavirasto.fi", "ruokavirasto.fi", "gis.gov.pl",
    "nebih.gov.hu", "ansvsa.ro", "bfsa.bg", "szpi.gov.cz", "svps.sk",
    "mast.is", "webgate.ec.europa.eu", "food.ec.europa.eu",
    "efsa.europa.eu", "ecdc.europa.eu",
    "blv.admin.ch", "admin.ch",
    # Asia-Pacific
    "foodstandards.gov.au", "mpi.govt.nz", "cfs.gov.hk",
    "mfds.go.kr", "mhlw.go.jp", "samr.gov.cn", "sfa.gov.sg",
    "fda.gov.ph", "fssai.gov.in", "tfda.gov.tw", "bpom.go.id",
    "moh.gov.my", "fda.moph.go.th",
    # LatAm
    "argentina.gob.ar", "gov.br", "anvisa.gov.br", "gob.mx",
    "arcsa.gob.ec", "digesa.gob.pe", "invima.gov.co", "ispch.cl",
    "msp.gub.uy",
    # Middle East / Africa
    "moccae.gov.ae", "health.gov.il", "moph.gov.qa", "sfda.gov.sa",
    "tarimorman.gov.tr", "nafdac.gov.ng", "kebs.org",
    "nfsa.gov.eg", "onssa.gov.ma", "ncc.org.za",
    # News outlets the gap-finders sometimes cite (these go to Pending
    # with the News domain noted — URL gate still validates them before
    # promotion to Recalls)
    "foodsafetynews.com", "food-safety.com", "outbreaknewstoday.com",
}


def _is_regulator_url(url: str) -> bool:
    """Is this URL from a whitelisted regulator or food-safety outlet?"""
    try:
        host = urlparse(url).netloc.lower().lstrip("www.")
    except Exception:
        return False
    if not host:
        return False
    # Strip leading 'www.' if any
    host = host[4:] if host.startswith("www.") else host
    # Check exact match OR subdomain match against the whitelist
    for dom in REGULATOR_DOMAINS:
        if host == dom or host.endswith("." + dom):
            return True
    return False


# ---------------------------------------------------------------------------
# Tavily search
# ---------------------------------------------------------------------------

def _tavily_search(query: str, max_results: int = 10, days: int = 7) -> List[Dict[str, Any]]:
    """Single Tavily search. Returns raw result dicts or [] on failure."""
    if not TAVILY_API_KEY:
        log.error("TAVILY_API_KEY not set — skipping search")
        return []
    body = {
        "api_key": TAVILY_API_KEY,
        "query": query,
        "search_depth": "advanced",     # better recall coverage than "basic"
        "include_answer": False,        # we parse raw results ourselves
        "max_results": max_results,
        "days": days,                   # restrict to recent N days
        "topic": "news",                # recall announcements are news
    }
    try:
        r = requests.post(TAVILY_ENDPOINT, json=body, timeout=30)
        if r.status_code != 200:
            log.warning("Tavily %d: %s", r.status_code, r.text[:200])
            return []
        data = r.json()
        return data.get("results", []) or []
    except Exception as e:
        log.warning("Tavily call failed: %s", e)
        return []


def _run_tavily_queries(since_days: int) -> List[Dict[str, Any]]:
    """Run the 3 canonical gap-finder queries and dedup results."""
    queries = [
        'food recall salmonella OR listeria OR "e. coli" OR botulism OR campylobacter',
        'food recall mould OR mold OR "foreign material" OR glass OR "ethylene oxide"',
        'RASFF notification food alert withdrawal',
        'site:recalls-rappels.canada.ca food recall',
        'site:food.gov.uk food alert recall',
        'site:fsai.ie food recall alert',
        'site:rappel.conso.gouv.fr rappel alimentaire',
        'site:fda.gov food recall pathogen',
        'site:foodstandards.gov.au food recall',
    ]
    all_results: Dict[str, Dict[str, Any]] = {}  # URL -> result (dedup)
    for q in queries:
        log.info("Tavily search: %s", q)
        results = _tavily_search(q, max_results=10, days=since_days)
        log.info("  -> %d raw results", len(results))
        for r in results:
            url = (r.get("url") or "").strip()
            if not url:
                continue
            if not _is_regulator_url(url):
                continue
            # First occurrence wins (skip duplicates from other queries)
            if url not in all_results:
                all_results[url] = r
    log.info("Regulator-whitelisted results: %d unique URLs", len(all_results))
    return list(all_results.values())


# ---------------------------------------------------------------------------
# Gemini-powered field extraction (FREE)
# ---------------------------------------------------------------------------

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


EXTRACT_PROMPT = """The following is a title + snippet + URL from a Google-News-style search result for a recent food recall or public-health alert.

Your job: if it describes a food recall involving a PATHOGEN, MOULD, MYCOTOXIN, BIOTOXIN, FOREIGN MATERIAL, PEST CONTAMINATION, or CHEMICAL HAZARD (Salmonella, Listeria, E. coli / STEC, Botulinum, Norovirus, Hepatitis A, Campylobacter, Cronobacter, mould/mold, aflatoxin, histamine, glass/metal/plastic fragments, rodent, ethylene oxide, heavy metals, dioxins, pesticide residues, etc.), extract the structured fields below.

If the item is NOT about a food safety hazard (e.g. allergen-only, policy news, labeling error, unrelated), return: {{"skip": true}}.

Return ONLY strict JSON — no markdown fences, no prose.

Schema when extracting:
{{
  "Date": "YYYY-MM-DD",           // publication date
  "Source": "...",                 // regulator short name (e.g. "FDA", "RappelConso", "FSAI")
  "Company": "...",
  "Brand": "—",                    // use "—" if not stated
  "Product": "...",
  "Pathogen": "...",               // e.g. "Salmonella"
  "Reason": "...",                 // short cause text
  "Class": "Recall",
  "Country": "...",
  "Outbreak": 0,                   // 1 if illnesses/cases/deaths mentioned
  "Notes": "..."
}}

INPUT:
URL: {url}
Title: {title}
Snippet: {snippet}
Published (approx): {published}
"""


def _claude_extract_fields(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Use Claude Haiku to extract structured Recall fields from a search result.

    Switched from Gemini (free tier) because Gemini's daily quota gets
    exhausted by ~10am Athens from the other pipeline steps, leaving
    Tavily with nothing at 22:00. Claude Haiku costs ~€0.0015/call
    (~€0.08/run, ~€2.5/month) — cheap insurance for reliability.
    """
    try:
        import anthropic  # type: ignore
    except ImportError:
        log.error("anthropic SDK not installed — cannot extract fields")
        return None

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        log.error("ANTHROPIC_API_KEY not set — cannot extract fields")
        return None

    prompt = EXTRACT_PROMPT.format(
        url=item.get("url", ""),
        title=item.get("title", ""),
        snippet=(item.get("content") or item.get("snippet") or "")[:1500],
        published=item.get("published_date", "") or "unknown",
    )
    try:
        client = anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}],
        )
        # Collect text from response blocks
        text = ""
        for block in resp.content:
            if getattr(block, "type", "") == "text":
                text += getattr(block, "text", "")
        text = text.strip()
        if not text:
            return None
        # Strip ```json fences if present
        if text.startswith("```"):
            text = re.sub(r"^```[a-zA-Z]*\s*\n", "", text)
            text = re.sub(r"\n```\s*$", "", text)
            text = text.strip()
        data = json.loads(text)
        if isinstance(data, dict):
            if data.get("skip"):
                return None
            return data
    except Exception as e:
        log.debug("Claude extract failed for %s: %s", item.get("url", "?"), e)
    return None


# Keep the old name as an alias so callers don't break if imported elsewhere
_gemini_extract_fields = _claude_extract_fields


# ---------------------------------------------------------------------------
# Result -> Recall
# ---------------------------------------------------------------------------

def results_to_recalls(items: List[Dict[str, Any]]) -> List[Recall]:
    out: List[Recall] = []
    for item in items:
        url = (item.get("url") or "").strip()
        if not url:
            continue
        fields = _claude_extract_fields(item)
        if not fields:
            continue
        try:
            pathogen = normalize_pathogen(fields.get("Pathogen", "") or "")
            country = normalize_country(fields.get("Country", "") or "")
            outbreak = int(fields.get("Outbreak", 0) or 0)
            rec = Recall(
                Date=(fields.get("Date") or "")[:10],
                Source=fields.get("Source", "") or "Tavily-gap",
                Company=fields.get("Company", "") or "",
                Brand=fields.get("Brand", "") or "—",
                Product=fields.get("Product", "") or "",
                Pathogen=pathogen,
                Reason=fields.get("Reason", "") or "",
                Class=fields.get("Class", "") or "Recall",
                Country=country,
                Region=infer_region(country) if country else "",
                Tier=assign_tier(pathogen, outbreak),
                Outbreak=outbreak,
                URL=url,
                Notes=(fields.get("Notes", "") or "") + "  [via Tavily gap-finder]",
            )
            rec = rec.normalize()
            if not rec.Pathogen or rec.Pathogen in ("—", ""):
                continue
            if not rec.URL.lower().startswith(("http://", "https://")):
                continue
            out.append(rec)
        except Exception as e:
            log.warning("Skipping malformed Tavily row: %s", e)
            continue
    log.info("Tavily gap-finder: %d items -> %d valid Recall objects",
             len(items), len(out))
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    t0 = datetime.now(timezone.utc)
    scraped_at = t0.strftime("%Y-%m-%dT%H:%M:%SZ")
    log.info("=" * 60)
    log.info("Tavily gap-finder run: %s", scraped_at)

    if not TAVILY_API_KEY:
        log.error("TAVILY_API_KEY not set — cannot run")
        return 1

    if not XLSX_PATH.exists():
        log.error("recalls.xlsx not found at %s", XLSX_PATH)
        return 1

    approved = load_existing(XLSX_PATH)
    pending = load_pending(XLSX_PATH)
    log.info("Loaded %d approved + %d pending rows", len(approved), len(pending))

    # 1. Run Tavily searches, filter to whitelisted regulator domains
    items = _run_tavily_queries(SINCE_DAYS)
    if not items:
        log.info("Tavily: no regulator-whitelisted results this run.")
        return 0

    # 2. Extract structured fields via Gemini (free)
    recalls = results_to_recalls(items)
    if not recalls:
        log.info("Tavily gap-finder: all results filtered out.")
        return 0

    # 3. Dedup-append to Pending
    new_pending = append_to_pending(
        pending=pending,
        new_recalls=recalls,
        approved_existing=approved,
        scraped_at=scraped_at,
    )
    added = len(new_pending) - len(pending)
    log.info("Tavily gap-finder: added %d new rows to Pending", added)

    save_xlsx_with_pending(
        xlsx_path=XLSX_PATH,
        approved_rows=sort_rows(approved),
        pending_rows=sort_rows(new_pending),
    )

    if added > 0 and not SKIP_COMMIT:
        msg = f"Tavily gap-finder: +{added} rows to Pending ({scraped_at})"
        git_commit_and_push(ROOT, [str(XLSX_PATH)], msg)
        log.info("Committed and pushed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
