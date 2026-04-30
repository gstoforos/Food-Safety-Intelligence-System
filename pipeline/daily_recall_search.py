"""
Daily Recall Search — OpenAI-powered global recall finder (Pending-first).

REPLACES pipeline/gap_finder_openai.py.

Correct pipeline flow — EXCEL IS THE SOURCE OF TRUTH:

  1. Read existing Recalls + Pending from docs/data/recalls.xlsx
  2. Run 5 region sweeps via gpt-4o-mini-search-preview, looking for
     pathogen/biotoxin/mycotoxin/foreign-material/pest/chemical recalls
     from any regulator worldwide in a 3-day window (yesterday ±1).
     Allergen-only recalls are rejected by a strict in-code filter.
  3. Dedup returned rows vs Recalls AND Pending by URL + fingerprint.
     Survivors are appended to the **Pending** sheet — not Recalls.
  4. The URL guardian (separate workflow, already deployed) later
     validates those Pending URLs and promotes them to Recalls.
  5. Writes docs/daily/YYYY-MM-DD.html by RE-READING the Recalls sheet
     for the target date. The brief ALWAYS matches what's in the xlsx.
  6. Updates docs/daily-index.json so the dashboard card reflects the
     Recalls-sheet counts, not the OpenAI-raw counts.

Why Pending-first:
  Before this version, OpenAI results went straight into Recalls and the
  brief rendered from the raw API response. That created two problems:
    (a) unverified URLs polluted the Recalls sheet
    (b) the brief didn't match the xlsx — dashboard and Recalls diverged
  Now OpenAI is a *proposer* (writes Pending), not a publisher. The URL
  guardian remains the sole promoter to Recalls. The brief renders from
  Recalls only, so by construction it always matches.

Budget controls:
  - Hard cap per run: €1.00 (config HARD_CAP_EUR_PER_RUN)
  - Hard cap per week: €7.00 (HARD_CAP_EUR_PER_WEEK) — persisted in
    docs/data/.spend_ledger.json so a bad run can't blow the budget.
  - Current per-run expected cost ≈ €0.002 (gpt-4o-mini-search-preview only,
    token costs at $0.15/M in and $0.60/M out, no separate search fee).

Invocation:
  python -m pipeline.daily_recall_search
  python -m pipeline.daily_recall_search --date 2026-04-21
  python -m pipeline.daily_recall_search --regions Europe,NorthAmerica
"""
from __future__ import annotations
import argparse
import json
import logging
import os
import re
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

import requests

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scrapers._models import (  # noqa: E402
    Recall, normalize_pathogen, normalize_country, infer_region, assign_tier,
)
from pipeline.merge_master import (  # noqa: E402
    load_existing, load_pending, sort_rows,
    save_xlsx_with_pending, append_to_pending,
)
from pipeline.commit_github import git_commit_and_push  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("daily-recall-search")

DATA_DIR = ROOT / "docs" / "data"
XLSX_PATH = DATA_DIR / "recalls.xlsx"
DAILY_DIR = ROOT / "docs" / "daily"
DAILY_INDEX = ROOT / "docs" / "daily-index.json"
SPEND_LEDGER = DATA_DIR / ".spend_ledger.json"
# Status file — read by daily_recall_search_exa.py to decide whether to fall back.
STATUS_FILE = DATA_DIR / ".daily_search_status.json"

# ---------------------------------------------------------------------------
# Budget
# ---------------------------------------------------------------------------
HARD_CAP_EUR_PER_RUN = float(os.getenv("DAILY_BUDGET_EUR_PER_RUN", "1.00"))
HARD_CAP_EUR_PER_WEEK = float(os.getenv("DAILY_BUDGET_EUR_PER_WEEK", "7.00"))
USD_TO_EUR = 0.92  # static — close enough; doesn't need to be exact

# Tavily free tier: 1,000 searches/month. ~5 queries × 5 regions = 25 per run.
# At 1 run/day → 750/mo, ~75% of free quota. Fits with margin for retries.
# Cost is tracked as query-count (€0 in free tier; ledger kept for parity
# with the legacy OpenAI version so spend-cap logic still works).
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY", "").strip()
TAVILY_ENDPOINT = "https://api.tavily.com/search"
TAVILY_MAX_RESULTS_PER_QUERY = int(os.getenv("TAVILY_MAX_RESULTS", "10"))
TAVILY_FRESHNESS_DAYS       = int(os.getenv("TAVILY_DAYS", "3"))

SKIP_COMMIT = os.getenv("SKIP_COMMIT", "").lower() in ("1", "true", "yes")


# ---------------------------------------------------------------------------
# Regions — 5 sweeps per run covers the globe
# ---------------------------------------------------------------------------
REGION_SPECS: List[Dict[str, Any]] = [
    {
        "region": "Europe",
        "agencies": (
            "RappelConso (France, rappel.conso.gouv.fr), BVL / "
            "lebensmittelwarnung.de (Germany), AGES (Austria), "
            "FSAI (Ireland, fsai.ie), FSA UK (food.gov.uk), "
            "FSS Scotland (foodstandards.gov.scot), Livsmedelsverket "
            "(Sweden), Fødevarestyrelsen (Denmark), Mattilsynet (Norway), "
            "Ruokavirasto (Finland), ŠVPS (Slovakia, svps.sk), SZPI "
            "(Czech, szpi.gov.cz), AESAN (Spain), Min. Salute "
            "(salute.gov.it Italy), NVWA (Netherlands, nvwa.nl), AFSCA/FAVV "
            "(Belgium, favv-afsca.be), ASAE (Portugal), RASFF (EU, "
            "webgate.ec.europa.eu/rasff-window), EFSA, BLV (Switzerland), "
            "EFET (Greece, efet.gr), GIS (Poland), PVD (Latvia), "
            "VTA (Estonia), VMVT (Lithuania), MAST (Iceland)"
        ),
    },
    {
        "region": "NorthAmerica",
        "agencies": (
            "FDA (fda.gov/safety/recalls-market-withdrawals-safety-alerts), "
            "USDA FSIS (fsis.usda.gov/recalls), CDC, CFIA Canada "
            "(recalls-rappels.canada.ca), MAPAQ Quebec, state departments "
            "of agriculture where nationally reported"
        ),
    },
    {
        "region": "LATAM",
        "agencies": (
            "ANVISA Brazil (gov.br/anvisa), COFEPRIS Mexico (gob.mx/cofepris), "
            "ANMAT Argentina (argentina.gob.ar/anmat), ISP Chile "
            "(ispch.cl), INVIMA Colombia (invima.gov.co), DIGESA Peru, "
            "ARCSA Ecuador, MSP Uruguay"
        ),
    },
    {
        "region": "AsiaPacific",
        "agencies": (
            "FSANZ Australia (foodstandards.gov.au/food-recalls), MPI NZ "
            "(mpi.govt.nz/food-safety-home/food-recalls), MFDS Korea "
            "(mfds.go.kr), MHLW Japan (mhlw.go.jp), CFS Hong Kong "
            "(cfs.gov.hk), SFA Singapore (sfa.gov.sg), FSSAI India "
            "(fssai.gov.in), FDA Philippines (fda.gov.ph), BPOM "
            "Indonesia (pom.go.id), MoH Malaysia (moh.gov.my), TFDA "
            "Taiwan (fda.gov.tw), Thai FDA (fda.moph.go.th), VFA Vietnam, "
            "SAMR China (samr.gov.cn)"
        ),
    },
    {
        "region": "MiddleEastAfrica",
        "agencies": (
            "SFDA Saudi Arabia (sfda.gov.sa), MoCCAE UAE (moccae.gov.ae), "
            "MoH Israel (gov.il), MoPH Qatar (moph.gov.qa), TGTHB Turkey "
            "(tarimorman.gov.tr), NAFDAC Nigeria (nafdac.gov.ng), NCC "
            "South Africa (thencc.org.za), NFSA Egypt, ONSSA Morocco, "
            "KEBS Kenya, FDA Ghana"
        ),
    },
]


# ---------------------------------------------------------------------------
# The prompt — battle-tested phrasing
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = (
    "You are a senior food-safety operations analyst for AFTS, a Greek "
    "food-process-engineering consultancy. You report every morning on "
    "YESTERDAY'S official food recalls published by regulators worldwide. "
    "You NEVER make up URLs or facts. If you cannot verify a recall via a "
    "live regulator page, you omit it. You return ONLY strict JSON — no "
    "markdown fences, no prose. The URL field MUST be a URL you actually "
    "saw in a web search result."
)


def build_user_prompt(target_date: date, region: str, agencies: str) -> str:
    # Provide the date in multiple formats so the model doesn't need to
    # reformat — this eliminates the silent-empty failure mode where the
    # model saw "20/04/2026" on a RappelConso page, compared to
    # "2026-04-20" in the prompt, and returned nothing because the string
    # didn't match even though it was the same date.
    #
    # Window widened from ±1 to ±2 days (5 acceptable dates total) because
    # regulators commonly publish recalls dated 2-3 days in the past — the
    # recall happens, internal review takes a few days, then the agency
    # posts. The narrow ±1 window was missing those late posts.
    yday = target_date
    d_minus_2 = target_date - timedelta(days=2)
    prev = target_date - timedelta(days=1)
    nxt  = target_date + timedelta(days=1)
    d_plus_2 = target_date + timedelta(days=2)
    fmt_variants = (
        f"{yday.isoformat()} (ISO) | "
        f"{yday.strftime('%d/%m/%Y')} (EU DD/MM/YYYY as used by RappelConso, "
        f"EU sites) | "
        f"{yday.strftime('%m/%d/%Y')} (US MM/DD/YYYY as used by FDA, FSIS) | "
        f"{yday.strftime('%B %d, %Y')} (US long form) | "
        f"{yday.strftime('%d %B %Y')} (EU long form) | "
        f"{yday.strftime('%A %B %d %Y')} (weekday form)"
    )
    return (
        f"TASK: Using your web_search tool, find every food recall, public "
        f"health alert, food safety notice, or product withdrawal officially "
        f"published by a food regulator in {region} within a 5-day window "
        f"centered on yesterday. The target day is:\n\n"
        f"  TARGET DAY (yesterday Athens time): {fmt_variants}\n"
        f"  ACCEPTABLE PUBLISH DATES: {d_minus_2.isoformat()} | "
        f"{prev.isoformat()} | {yday.isoformat()} | "
        f"{nxt.isoformat()} | {d_plus_2.isoformat()}\n\n"
        f"Include recalls whose publication/issue date falls on ANY of "
        f"those five dates. Regulator sites often show dates as DD/MM/YYYY "
        f"or localized text — normalize them and include if they match one "
        f"of the five acceptable ISO dates above.\n\n"
        f"REGULATORS TO COVER (search each by name + recent recalls): {agencies}\n\n"
        f"SEARCH STRATEGY:\n"
        f"  - For each regulator, search '<agency name> recall "
        f"{yday.strftime('%B %Y')}' and '<agency name> recent recalls'\n"
        f"  - Then open the 3-4 most recent recall notices from each agency "
        f"and check their publication date against the 5-day window\n"
        f"  - If you see phrases like 'today', 'this week', 'yesterday', "
        f"'hier', 'aujourd'hui', 'heute', translate them using the fact "
        f"that the current date is {nxt.isoformat()}\n\n"
        f"IN SCOPE — include recalls where the hazard is:\n"
        f"  • Pathogens: Listeria, Salmonella, E. coli / STEC / O157, "
        f"Clostridium botulinum, Norovirus, Hepatitis A, Campylobacter, "
        f"Cronobacter, Bacillus cereus, Cyclospora, Shigella, Vibrio, "
        f"Yersinia\n"
        f"  • Biotoxins: histamine/scombrotoxin, marine biotoxins "
        f"(DSP/PSP/ASP, domoic acid, saxitoxin, ciguatera), cereulide\n"
        f"  • Mycotoxins: aflatoxin, ochratoxin, patulin, Alternaria "
        f"(alternariol/AOH/AME, tenuazonic acid), Fusarium toxins "
        f"(fumonisin, zearalenone, deoxynivalenol/DON, nivalenol, "
        f"T-2, HT-2), citrinin, ergot alkaloids (Claviceps)\n"
        f"  • Foreign material: glass, metal, plastic, wood, stone\n"
        f"  • Rodent / insect / pest contamination (physical hazard)\n"
        f"  • Chemical: heavy metals (lead, cadmium, mercury, arsenic) "
        f"over legal limit, pesticide residues over MRL, unauthorized "
        f"substances (rodenticide, DMAE, novel food ingredients)\n\n"
        f"OUT OF SCOPE — EXCLUDE:\n"
        f"  • Allergen-only recalls (undeclared milk, egg, nuts, soy, "
        f"wheat, gluten, sulphite, fish, shellfish, sesame, celery — even "
        f"if severe, they are allergens-only unless combined with one of "
        f"the in-scope hazards above)\n"
        f"  • Labeling errors (language, country-of-origin, wrong nutrition "
        f"panel, undeclared organic claims)\n"
        f"  • Quality issues (taste, appearance, texture)\n"
        f"  • Non-food products (cosmetics, toys, medical devices, "
        f"supplements unless food-borne)\n\n"
        f"OUTPUT — strict JSON, no fences, no commentary:\n"
        f'{{"date": "{yday.isoformat()}", "region": "{region}", '
        f'"recalls": [ROW, ROW, ...]}}\n\n'
        f"Each ROW object has these fields (always all of them):\n"
        f'  "date": "YYYY-MM-DD" — the actual publication date you found '
        f'(must be one of {prev.isoformat()}, {yday.isoformat()}, '
        f'{nxt.isoformat()})\n'
        f'  "country": English country name\n'
        f'  "source": regulator short name, e.g. "FDA", "RappelConso (FR)", '
        f'"BVL (DE)", "FSANZ (AU)"\n'
        f'  "company": firm/producer name\n'
        f'  "brand": brand name or "—"\n'
        f'  "product": full product description with size/pack\n'
        f'  "hazard_type": one of PATHOGEN, BIOTOXIN, MYCOTOXIN, '
        f"FOREIGN_MATERIAL, PEST_CONTAMINATION, CHEMICAL\n"
        f'  "pathogen": specific agent (e.g. "Listeria monocytogenes", '
        f'"Salmonella Enteritidis", "glass fragments", "rodent"), or "—"\n'
        f'  "reason": short cause description in English\n'
        f'  "class": recall class if stated (Class I/II/III, Volontaire, '
        f'Alert, Recall) — or "Recall"\n'
        f'  "outbreak": 1 if ANY illness/hospitalisation/death mentioned, '
        f"else 0\n"
        f'  "url": DEEP-LINK URL to the specific recall detail page on the '
        f"regulator's official domain. Must be a URL you actually retrieved "
        f"via web_search. NEVER a category or homepage. If you cannot find "
        f"a specific deep-link page, use the category page URL as fallback — "
        f"do NOT omit the recall.\n"
        f'  "notes": distribution area, lot codes, illness count, or any '
        f"extra context worth capturing.\n\n"
        f"CRITICAL RULES:\n"
        f"1. If a regulator genuinely published nothing in the 3-day "
        f'window in {region}, skip that regulator — but do not skip a '
        f"regulator just because you're unsure about the date. When unsure, "
        f"INCLUDE the recall and let the structured date field speak.\n"
        f"2. NEVER invent URLs. Every URL must appear verbatim in a real "
        f"search result you ran. A category-page fallback is fine.\n"
        f"3. NEVER include allergen-only recalls.\n"
        f"4. The publication date must fall within "
        f"[{prev.isoformat()}, {nxt.isoformat()}]. Reject older.\n"
        f"5. Return ONLY the JSON object, nothing else — no markdown, "
        f"no prose."
    )


# ---------------------------------------------------------------------------
# Budget ledger (persisted across runs in docs/data/.spend_ledger.json)
# ---------------------------------------------------------------------------
def load_ledger() -> Dict[str, Any]:
    if SPEND_LEDGER.exists():
        try:
            return json.loads(SPEND_LEDGER.read_text())
        except Exception:
            pass
    return {"entries": []}


def save_ledger(ledger: Dict[str, Any]) -> None:
    SPEND_LEDGER.parent.mkdir(parents=True, exist_ok=True)
    SPEND_LEDGER.write_text(json.dumps(ledger, indent=2))


def current_week_spend_eur(ledger: Dict[str, Any]) -> float:
    """Sum of spend in entries within the last 7 days."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    total = 0.0
    for e in ledger.get("entries", []):
        try:
            ts = datetime.fromisoformat(e["ts"].replace("Z", "+00:00"))
            if ts >= cutoff:
                total += float(e["eur"])
        except Exception:
            continue
    return total


def record_spend(ledger: Dict[str, Any], eur: float, region: str,
                 in_tok: int, out_tok: int) -> None:
    ledger.setdefault("entries", []).append({
        "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "eur": round(eur, 4),
        "region": region,
        "input_tokens": in_tok,
        "output_tokens": out_tok,
    })
    # Keep only last 60 days to stop unbounded growth
    cutoff = datetime.now(timezone.utc) - timedelta(days=60)
    ledger["entries"] = [
        e for e in ledger["entries"]
        if datetime.fromisoformat(e["ts"].replace("Z", "+00:00")) >= cutoff
    ]


# ---------------------------------------------------------------------------
# Tavily search (replaces gpt-4o-mini-search-preview, April 2026 cost cut)
# ---------------------------------------------------------------------------
# Reuse deterministic helpers from gap_finder_tavily.py — same domain
# whitelist, same pathogen/outbreak/company/product extractors, same date
# parser. Zero LLM calls in this pipeline.
from pipeline.gap_finder_tavily import (  # noqa: E402
    HOST_TO_SOURCE,
    _lookup_source as _gf_lookup_source,
    _detect_pathogen as _gf_detect_pathogen,
    _detect_outbreak as _gf_detect_outbreak,
    _extract_company_product as _gf_extract_company_product,
    _parse_date as _gf_parse_date,
    _is_generic_url as _gf_is_generic_url,
)


# Mycotoxin / chemical / physical hazard markers — used to bucket the
# detected pathogen text into a hazard_type code that is_in_scope() expects.
# Order matters (most specific first).
_HAZARD_TYPE_RULES: List[Tuple[str, "re.Pattern[str]"]] = [
    ("MYCOTOXIN", re.compile(
        r"\b(?:aflatox|ochratox|ocratoxin|ocratossin|patulin|alternaria|"
        r"alternariol|tenuazonic|fumonisin|zearalenon|deoxynivalenol|"
        r"nivalenol|t[\s\-]?2[\s\-]?toxin|ht[\s\-]?2[\s\-]?toxin|"
        r"citrinin|ergot|claviceps|mutterkorn|"
        r"mycotox|mykotoxin|micotoxin|micotossin)\b", re.I)),
    ("BIOTOXIN", re.compile(
        r"\b(?:biotoxin|histamine|scombro|domoic|saxitox|tetrodotox|"
        r"ciguatera|dsp|psp|asp|cereulide)\b", re.I)),
    ("FOREIGN_MATERIAL", re.compile(
        r"\b(?:glass|metal|plastic|wood|stone|rubber|bone)\s+"
        r"(?:fragment|shard|particle|piece)|"
        r"foreign\s+(?:body|material|object|matter)\b", re.I)),
    ("PEST_CONTAMINATION", re.compile(
        r"\b(?:rodenticid|rat\s+poison|rodent|insect|pest)\b", re.I)),
    ("CHEMICAL", re.compile(
        r"\b(?:heavy\s+metal|cadmium|lead\s+contamin|mercury\s+contamin|"
        r"arsenic\s+contamin|pesticid|chlorpyrifos|glyphosate|"
        r"unauthoris(?:ed|ized)\s+substance|ethylene\s+oxide|"
        r"chlorate|sudan|melamine|mineral\s+oil|dioxin)\b", re.I)),
    ("PATHOGEN", re.compile(
        r"\b(?:listeria|salmonell|e\.?\s*coli|stec|o157|shiga|botulin|"
        r"norovirus|hepatit|campylobact|cyclospor|vibrio|cronobact|"
        r"bacillus\s*cereus|shigella|yersinia|brucell|staphyloc)\b", re.I)),
]


def _infer_hazard_type(text: str) -> str:
    """Bucket a pathogen/reason blob into the codes is_in_scope() accepts."""
    if not text:
        return ""
    for code, rx in _HAZARD_TYPE_RULES:
        if rx.search(text):
            return code
    return ""


# Per-region Tavily query templates. We use site:queries against the
# strongest regulators in each region, plus one generic recent-recall sweep.
# Each region runs ~5 queries — total per run ≈ 25, well within free tier.
_REGION_QUERIES: Dict[str, List[str]] = {
    "Europe": [
        'site:rappel.conso.gouv.fr OR site:rappelconso.gouv.fr rappel alimentaire',
        'site:food.gov.uk OR site:foodstandards.gov.scot food alert recall',
        'site:fsai.ie food recall alert',
        'site:nvwa.nl OR site:favv-afsca.be food recall',
        'RASFF notification food alert pathogen withdrawal',
    ],
    "NorthAmerica": [
        'site:fda.gov/safety/recalls food recall pathogen',
        'site:fsis.usda.gov/recalls-alerts food recall',
        'site:recalls-rappels.canada.ca food recall',
        'site:inspection.canada.ca food recall',
        'site:cdc.gov food outbreak investigation',
    ],
    "LATAM": [
        'site:gov.br/anvisa recall alimento',
        'site:gob.mx/cofepris alerta alimento',
        'site:argentina.gob.ar/anmat retiro alimento',
        'site:ispch.cl OR site:invima.gov.co alerta alimentaria',
        'recall alimento Brasil OR Mexico OR Argentina patogeno',
    ],
    "AsiaPacific": [
        'site:foodstandards.gov.au food recall',
        'site:mpi.govt.nz food recall',
        'site:cfs.gov.hk OR site:sfa.gov.sg food recall alert',
        'site:fssai.gov.in OR site:fda.gov.ph food recall',
        'site:mfds.go.kr OR site:mhlw.go.jp food recall',
    ],
    "MiddleEastAfrica": [
        'site:sfda.gov.sa food recall',
        'site:moccae.gov.ae OR site:gov.il food recall',
        'site:nafdac.gov.ng food recall alert',
        'site:fda.gov.gh OR site:thencc.org.za food recall',
        'food recall withdrawal Africa OR "Middle East" pathogen',
    ],
}


# Run-level statistics for the status file
_RUN_STATS: Dict[str, Any] = {
    "tavily_queries_attempted": 0,
    "tavily_queries_succeeded": 0,
    "tavily_results_total":     0,
    "tavily_rate_limited":      False,
    "tavily_auth_error":        False,
    "regions_attempted":        [],
    "regions_with_results":     [],
}


def _tavily_search_one(query: str) -> Tuple[List[Dict[str, Any]], str]:
    """Single Tavily search. Returns (results, error_code).

    error_code:
      ""           — success (results may still be empty)
      "no_key"     — TAVILY_API_KEY missing
      "rate_limit" — HTTP 429 or quota message
      "auth"       — HTTP 401/403
      "http"       — other non-200
      "exception"  — request raised
    """
    if not TAVILY_API_KEY:
        log.error("TAVILY_API_KEY not set — skipping search")
        return [], "no_key"
    body = {
        "api_key":      TAVILY_API_KEY,
        "query":        query,
        "search_depth": "advanced",
        "include_answer": False,
        "max_results":  TAVILY_MAX_RESULTS_PER_QUERY,
        "days":         TAVILY_FRESHNESS_DAYS,
        "topic":        "news",
    }
    try:
        r = requests.post(TAVILY_ENDPOINT, json=body, timeout=30)
    except Exception as e:
        log.warning("Tavily call failed: %s", e)
        return [], "exception"

    if r.status_code == 429:
        log.warning("Tavily 429 — rate limit / quota exceeded for: %s", query)
        return [], "rate_limit"
    if r.status_code in (401, 403):
        log.warning("Tavily %d — auth failure for: %s", r.status_code, query)
        return [], "auth"
    if r.status_code != 200:
        # Tavily returns 432/usage errors when free credits are exhausted
        body_low = (r.text or "").lower()
        if r.status_code == 432 or "usage limit" in body_low or "quota" in body_low:
            log.warning("Tavily %d — quota/usage limit for: %s", r.status_code, query)
            return [], "rate_limit"
        log.warning("Tavily %d: %s", r.status_code, r.text[:200])
        return [], "http"
    try:
        data = r.json()
    except Exception as e:
        log.warning("Tavily JSON parse failed: %s", e)
        return [], "http"
    return data.get("results", []) or [], ""


def call_tavily_search(target_date: date, region: str, agencies: str,
                       ledger: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Run the per-region Tavily query set and extract recalls deterministically.

    Returns dict {"recalls": [row_dict, ...]} matching the legacy OpenAI shape,
    or None if Tavily is unavailable / hit rate-limit and produced nothing.
    The `agencies` arg is unused (kept for signature parity with the legacy
    OpenAI path); region-specific queries are picked from _REGION_QUERIES.
    """
    queries = _REGION_QUERIES.get(region, [])
    if not queries:
        log.warning("No Tavily query template for region %r — skipping", region)
        return None

    _RUN_STATS["regions_attempted"].append(region)

    # Aggregate results across queries, dedup by URL
    seen_urls: Dict[str, Dict[str, Any]] = {}
    region_rate_limited = False
    region_auth = False
    for q in queries:
        _RUN_STATS["tavily_queries_attempted"] += 1
        results, err = _tavily_search_one(q)
        if err == "rate_limit":
            _RUN_STATS["tavily_rate_limited"] = True
            region_rate_limited = True
            # Stop early — further queries will also fail
            break
        if err == "auth":
            _RUN_STATS["tavily_auth_error"] = True
            region_auth = True
            break
        if err:
            continue  # other errors: skip this query, try next
        _RUN_STATS["tavily_queries_succeeded"] += 1
        _RUN_STATS["tavily_results_total"] += len(results)
        for r in results:
            url = (r.get("url") or "").strip()
            if not url:
                continue
            if _gf_lookup_source(url) is None:  # not whitelisted
                continue
            if _gf_is_generic_url(url):
                continue
            if url not in seen_urls:
                seen_urls[url] = r

    log.info("  [%s] tavily: %d queries → %d unique whitelisted URLs%s",
             region, len(queries), len(seen_urls),
             " (rate-limited)" if region_rate_limited else
             " (auth)" if region_auth else "")

    if seen_urls:
        _RUN_STATS["regions_with_results"].append(region)

    # Deterministic extraction → row dicts matching the OpenAI output shape
    accept_dates = {
        (target_date - timedelta(days=1)).isoformat(),
        target_date.isoformat(),
        (target_date + timedelta(days=1)).isoformat(),
    }
    rows: List[Dict[str, Any]] = []
    for url, item in seen_urls.items():
        title   = (item.get("title") or "").strip()
        content = (item.get("content") or "").strip()
        blob    = title + "  " + content

        pathogen_raw = _gf_detect_pathogen(blob)
        if not pathogen_raw:
            continue
        hazard_type = _infer_hazard_type(blob) or "PATHOGEN"

        outbreak = _gf_detect_outbreak(blob)
        company, product = _gf_extract_company_product(title, content)
        if not product:
            product = (content.split(". ", 1)[0])[:200]

        date_str = _gf_parse_date(item)
        if not date_str:
            continue
        # Date-window guard — must match the OpenAI-era ±1 day window
        if date_str not in accept_dates:
            continue

        src_lookup = _gf_lookup_source(url) or ("Tavily-daily", "")
        source_label, country_guess = src_lookup

        rows.append({
            "date":        date_str,
            "source":      source_label or "Tavily-daily",
            "company":     company or "",
            "brand":       company or "—",
            "product":     product or "",
            "pathogen":    pathogen_raw,
            "reason":      pathogen_raw + (" — outbreak" if outbreak else ""),
            "class":       "Recall",
            "country":     country_guess or "",
            "outbreak":    outbreak,
            "url":         url,
            "notes":       (content[:300] +
                            "  [via Tavily daily search, deterministic extract]"),
            "hazard_type": hazard_type,
        })

    # Record a nominal "spend" of €0 — keeps the ledger schema intact so
    # downstream commit messages and summaries still work. Tavily free-tier
    # = €0 per query.
    record_spend(ledger, 0.0, region, 0, 0)
    return {"recalls": rows}


def write_status_file(ok: bool, recalls_count: int, regions_done: int,
                      target_date: date) -> None:
    """Drop a JSON status file the Exa fallback reads to decide whether to run.

    Schema:
      {
        "ts": "<ISO UTC>",
        "target_date": "YYYY-MM-DD",
        "ok": bool,                  # primary search produced results without quota error
        "should_fallback": bool,     # true if Exa should run
        "recalls_count": N,
        "regions_attempted": [...],
        "regions_with_results": [...],
        "tavily_rate_limited": bool,
        "tavily_auth_error": bool,
        "tavily_queries_attempted": N,
        "tavily_queries_succeeded": N,
      }
    """
    # Fallback should fire if (a) Tavily hit rate-limit/auth, OR (b) zero
    # recalls were extracted from any region — the latter usually means the
    # free tier silently degraded result quality or all queries returned 0.
    should_fallback = (
        _RUN_STATS["tavily_rate_limited"]
        or _RUN_STATS["tavily_auth_error"]
        or recalls_count == 0
    )
    payload = {
        "ts":                       datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "target_date":              target_date.isoformat(),
        "ok":                       ok,
        "should_fallback":          should_fallback,
        "recalls_count":            recalls_count,
        "regions_attempted":        _RUN_STATS["regions_attempted"],
        "regions_with_results":     _RUN_STATS["regions_with_results"],
        "tavily_rate_limited":      _RUN_STATS["tavily_rate_limited"],
        "tavily_auth_error":        _RUN_STATS["tavily_auth_error"],
        "tavily_queries_attempted": _RUN_STATS["tavily_queries_attempted"],
        "tavily_queries_succeeded": _RUN_STATS["tavily_queries_succeeded"],
    }
    try:
        STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
        STATUS_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        log.info("Status file written: %s (should_fallback=%s)",
                 STATUS_FILE, should_fallback)
    except Exception as e:
        log.warning("Failed to write status file: %s", e)


# ---------------------------------------------------------------------------
# Filtering + dedup
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Filter — reject allergen-only, labeling, quality issues
# ---------------------------------------------------------------------------
# Allergen keywords — if any of these appear as the "pathogen" value OR in
# the reason/notes, the row is allergen-only (non-pathogen) and must be
# rejected. Previous version only matched "undeclared <word>" in the reason
# field, so a row with pathogen="Peanut" reason="Undeclared peanut allergen"
# slipped through because "allergen" wasn't literally in the pathogen value.
ALLERGEN_KEYWORDS = (
    "peanut", "tree nut", "almond", "cashew", "hazelnut", "walnut",
    "pecan", "pistachio", "macadamia", "brazil nut",
    "milk allergen", "egg allergen", "dairy allergen",
    "soy allergen", "soya allergen",
    "wheat allergen", "gluten", "celiac", "coeliac",
    "sulphite", "sulfite",
    "fish allergen", "shellfish", "crustacean", "mollusc", "mollusk",
    "sesame", "celery allergen", "mustard allergen",
    "lupin", "lupine",
)

# If any of these appear in the raw reason/notes alongside "undeclared" or
# "unlabeled" or "contains", it's almost certainly allergen-only even if the
# model mislabeled hazard_type.
ALLERGEN_REASON_MARKERS = re.compile(
    r"(?:undeclared|unlabeled|not\s+declared|contains?\s+(?:undeclared\s+)?|"
    r"presence\s+of\s+(?:undeclared\s+)?)\s*"
    r"(?:milk|egg|peanut|tree\s*nut|almond|cashew|hazelnut|walnut|pecan|"
    r"pistachio|soy|soya|wheat|gluten|sulphite|sulfite|fish|shellfish|"
    r"sesame|celery|mustard|lupin|crustacean|mollusc|mollusk)\b",
    re.I,
)

# In-scope hazard indicators — at least one must appear in pathogen+reason
# for a row to pass. This is a positive check that complements the negative
# allergen filter: even if the allergen filter misses something, the row
# still needs to affirmatively look like a real hazard to pass.
#
# NOTE: opening \b requires a word boundary at the start, but no trailing
# boundary — the patterns are stems (e.g. 'salmonell' matches both
# 'salmonella' and 'salmonellae'). A trailing \b would reject stems like
# 'salmonell' when the real word is 'salmonella'.
IN_SCOPE_MARKERS = re.compile(
    r"\b(?:listeria|salmonell|e\.?\s*coli|stec|o157|shiga|botulin|"
    r"norovirus|hepatit|campylobact|cyclospor|vibrio|cronobact|"
    r"bacillus\s*cereus|cereulide|shigella|yersinia|"
    r"histamine|scombro|biotoxin|dsp|psp|asp|domoic|saxitox|ciguatera|"
    r"aflatox|ochratox|ocratoxin|ocratossin|patulin|alternaria|alternariol|"
    r"tenuazonic|fumonisin|zearalenon|deoxynivalenol|nivalenol|"
    r"\bt[\s\-]?2[\s\-]?toxin|\bht[\s\-]?2[\s\-]?toxin|"
    r"citrinin|ergot[\s\-]+alkaloid|claviceps|mutterkorn|"
    r"mycotox|mykotoxin|micotoxin|micotossin|"
    r"glass\s+(?:fragment|shard|particle)|glass\s+in\s+product|"
    r"metal\s+(?:fragment|shard|particle)|"
    r"plastic\s+(?:fragment|shard|particle)|"
    r"wood\s+(?:fragment|shard|particle)|"
    r"stone\s+(?:fragment|particle)|"
    r"foreign\s+(?:body|material|object|matter)|"
    r"rodent|rat\s+poison|rodenticid|insect|pest\s+contamination|"
    r"heavy\s+metal|lead\s+contamination|cadmium|mercury\s+contamination|"
    r"arsenic\s+contamination|"
    r"pesticid|unauthoris(?:ed|ized)\s+substance|"
    r"chlorpyrifos|glyphosate|dmae|novel\s+food)",
    re.I,
)


def is_in_scope(row: Dict[str, Any]) -> bool:
    """
    Return True only if the row is a real in-scope hazard recall.

    Two-sided check:
      1. NEGATIVE: reject allergen keywords in pathogen OR allergen markers
         in reason/notes. This catches the "pathogen=Peanut" leak.
      2. POSITIVE: require at least one in-scope hazard keyword in
         pathogen+reason+notes. If nothing in IN_SCOPE_MARKERS matches,
         the row can't be proven in-scope — reject it.
    """
    pathogen = (row.get("pathogen") or "").lower().strip()
    reason   = (row.get("reason")   or "").lower()
    notes    = (row.get("notes")    or "").lower()
    hazard_type = (row.get("hazard_type") or "").upper().strip()
    blob = f"{pathogen} {reason} {notes}"

    valid_hazards = {"PATHOGEN", "BIOTOXIN", "MYCOTOXIN",
                     "FOREIGN_MATERIAL", "PEST_CONTAMINATION", "CHEMICAL"}
    if hazard_type not in valid_hazards:
        return False

    # Empty / placeholder pathogen → not a real hazard
    if not pathogen or pathogen in ("—", "-", "none", "n/a"):
        return False

    # NEGATIVE: allergen keyword in pathogen field → reject
    # (e.g. pathogen="Peanut", pathogen="tree nut", pathogen="gluten")
    for kw in ALLERGEN_KEYWORDS:
        if kw in pathogen:
            return False

    # Bare allergen names (without "allergen" suffix) in the pathogen field
    # are also rejection-worthy — covers "peanut", "milk", "egg" etc. on
    # their own. Be careful not to block legit hazards like "E. coli" which
    # contains no allergen keyword. Simple word-boundary match.
    bare_allergen_re = re.compile(
        r"^\s*(?:peanut|tree\s*nut|almond|cashew|hazelnut|walnut|pecan|"
        r"pistachio|macadamia|milk|egg|soy|soya|wheat|sesame|mustard|"
        r"lupin|lupine|celery|fish|shellfish|crustacean|mollusc|mollusk|"
        r"gluten|sulphite|sulfite)\s*(?:allergen)?\s*$", re.I
    )
    if bare_allergen_re.match(pathogen):
        return False

    # NEGATIVE: allergen markers in reason/notes → reject
    if ALLERGEN_REASON_MARKERS.search(reason) or ALLERGEN_REASON_MARKERS.search(notes):
        # Exception: if both a pathogen AND an allergen appear (rare but
        # possible, e.g. Listeria + undeclared milk), let it through.
        if not IN_SCOPE_MARKERS.search(blob):
            return False

    # POSITIVE: must have at least one in-scope hazard keyword somewhere
    if not IN_SCOPE_MARKERS.search(blob):
        return False

    return True


def normalize_key(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").lower().strip())


def is_duplicate(row: Dict[str, Any],
                 existing_urls: set,
                 existing_signatures: set) -> bool:
    """Dedup by URL (primary) or (date, company, product fingerprint)."""
    url = normalize_key(row.get("url", ""))
    if url and url in existing_urls:
        return True
    sig = (
        row.get("date", "")[:10],
        normalize_key(row.get("company", ""))[:60],
        normalize_key(row.get("product", ""))[:60],
    )
    if sig in existing_signatures:
        return True
    return False


# ---------------------------------------------------------------------------
# Convert a raw dict → Recall
# ---------------------------------------------------------------------------
def to_recall(row: Dict[str, Any]) -> Optional[Recall]:
    try:
        country = normalize_country(row.get("country", "") or "")
        pathogen = row.get("pathogen", "") or ""
        outbreak = int(row.get("outbreak", 0) or 0)
        # For non-microbial hazards normalize_pathogen may return empty — keep
        # whatever the model gave us
        path_norm = normalize_pathogen(pathogen) or pathogen
        rec = Recall(
            Date=(row.get("date") or "")[:10],
            Source=row.get("source", "") or "Tavily-daily",
            Company=(row.get("company") or "")[:200],
            Brand=(row.get("brand") or "—")[:100],
            Product=(row.get("product") or "")[:400],
            Pathogen=path_norm[:200],
            Reason=(row.get("reason") or "")[:400],
            Class=(row.get("class") or "Recall")[:80],
            Country=country,
            Region=infer_region(country) if country else "",
            Tier=assign_tier(path_norm, outbreak),
            Outbreak=outbreak,
            URL=(row.get("url") or "").strip(),
            Notes=((row.get("notes") or "") +
                   "  [via Tavily daily search, deterministic extract]")[:500],
        )
        rec = rec.normalize()
        if not rec.URL.lower().startswith(("http://", "https://")):
            return None
        if not rec.Date:
            return None
        return rec
    except Exception as e:
        log.warning("Failed to build Recall: %s", e)
        return None


# ---------------------------------------------------------------------------
# Daily HTML report
# ---------------------------------------------------------------------------
DAILY_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>Daily Recall Brief — {DATE_PRETTY}</title>
<meta name="description" content="Global food-recall brief for {DATE_PRETTY}. Official regulator data only.">
<style>
*{{box-sizing:border-box}}
html,body{{margin:0;padding:0;background:#0a0f0d;color:#e8efe9;
  font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif;
  -webkit-font-smoothing:antialiased;line-height:1.5;font-size:16px}}
.wrap{{max-width:780px;margin:0 auto;padding:18px 18px 60px}}
.brand{{font-size:11px;letter-spacing:.18em;color:#75937d;text-transform:uppercase;
  font-family:ui-monospace,"SF Mono",Menlo,monospace;margin-bottom:8px}}
h1{{font-size:22px;font-weight:800;margin:0 0 4px;color:#fff;letter-spacing:-.01em;line-height:1.2}}
.sub{{color:#9cb3a2;font-size:13px;margin-bottom:18px}}
.kpi{{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:20px}}
.kpi-cell{{background:#12201a;border:1px solid #1f3329;border-radius:8px;padding:10px 12px}}
.kpi-v{{font-size:22px;font-weight:800;color:#00ff88;font-family:ui-monospace,monospace;line-height:1}}
.kpi-v.red{{color:#ff5a5a}}.kpi-v.orange{{color:#ffb24d}}
.kpi-l{{font-size:9px;color:#75937d;letter-spacing:.14em;text-transform:uppercase;margin-top:6px}}
.section{{margin:24px 0 12px}}
.section h2{{font-size:14px;font-weight:700;color:#00ff88;letter-spacing:.06em;
  text-transform:uppercase;margin:0 0 10px;padding-bottom:6px;border-bottom:1px solid #1f3329}}
.card{{background:#12201a;border:1px solid #1f3329;border-left:3px solid #00ff88;
  border-radius:6px;padding:13px 14px;margin-bottom:10px;font-size:14px}}
.card.t1{{border-left-color:#ff5a5a}}.card.ob{{border-left-color:#ffb24d}}
.c-hd{{display:flex;justify-content:space-between;gap:8px;flex-wrap:wrap;margin-bottom:6px}}
.c-src{{font-family:ui-monospace,monospace;font-size:10px;color:#75937d;
  letter-spacing:.08em;text-transform:uppercase}}
.c-tier{{font-family:ui-monospace,monospace;font-size:9px;font-weight:700;
  padding:2px 7px;border-radius:3px;letter-spacing:.1em}}
.c-tier.t1{{background:rgba(255,90,90,.15);color:#ff7a7a;border:1px solid rgba(255,90,90,.35)}}
.c-tier.t2{{background:rgba(255,178,77,.12);color:#ffb24d;border:1px solid rgba(255,178,77,.3)}}
.c-co{{font-weight:700;color:#fff;margin-bottom:3px;font-size:15px}}
.c-pr{{color:#c9d5cc;font-size:13px;margin-bottom:6px}}
.c-why{{font-size:12px;color:#9cb3a2;margin-bottom:8px}}
.c-why strong{{color:#e8efe9}}
.c-link{{display:inline-block;font-family:ui-monospace,monospace;font-size:10px;
  color:#00ff88;text-decoration:none;padding:6px 10px;border:1px solid #1f3329;
  border-radius:3px;letter-spacing:.06em;background:#0a0f0d;min-height:30px;
  line-height:18px}}
.c-link:active{{background:#182c23}}
.empty{{background:#12201a;border:1px solid #1f3329;border-radius:8px;
  padding:18px;text-align:center;color:#75937d;font-size:13px}}
.foot{{margin-top:40px;padding-top:18px;border-top:1px solid #1f3329;
  font-family:ui-monospace,monospace;font-size:10px;color:#5a7561;
  letter-spacing:.06em;line-height:1.6}}
.foot a{{color:#75937d;text-decoration:none}}
a.back{{display:inline-block;margin-bottom:16px;font-family:ui-monospace,monospace;
  font-size:10px;color:#75937d;text-decoration:none;letter-spacing:.1em;
  text-transform:uppercase}}
a.back:hover{{color:#00ff88}}
@media(max-width:480px){{h1{{font-size:20px}}.kpi-v{{font-size:18px}}.card{{padding:11px 12px}}.c-co{{font-size:14px}}}}
</style></head>
<body><div class="wrap">
<a class="back" href="https://www.advfood.tech/fsis-recalls">← Dashboard</a>
<div class="brand">AFTS · Food Safety Intelligence · Daily Brief</div>
<h1>Global recalls — {DATE_PRETTY}</h1>
<div class="sub">Official regulator sources only · Generated {GENERATED_AT} Athens · {REGIONS_SCANNED} regions scanned</div>

<div class="kpi">
  <div class="kpi-cell"><div class="kpi-v">{TOTAL}</div><div class="kpi-l">Total</div></div>
  <div class="kpi-cell"><div class="kpi-v red">{T1}</div><div class="kpi-l">Tier 1</div></div>
  <div class="kpi-cell"><div class="kpi-v orange">{OUTBREAK}</div><div class="kpi-l">Outbreak</div></div>
</div>
{BODY}
<div class="foot">
Pathogens + biotoxins + mycotoxins + foreign material + pest + chemical hazards only.<br>
Allergen-only, labeling, quality issues excluded per AFTS scope.<br>
<a href="https://www.advfood.tech/fsis-recalls">Back to dashboard</a> · <a href="../daily-index.json">JSON archive</a>
</div>
</div></body></html>
"""


def load_recalls_for_date(xlsx_path: Path, target: date) -> List[Recall]:
    """
    Read the Recalls sheet and return Recall objects whose Date == target.

    This is the authoritative source for the daily brief — the HTML file
    under docs/daily/ is a rendering of whatever is in the Recalls sheet
    on the target date at the moment of this call. If a row is in Pending
    but not yet promoted, it does NOT appear in the brief. That's the
    whole point: brief = Recalls sheet, always.
    """
    if not xlsx_path.exists():
        log.warning("Recalls xlsx not found at %s — brief will be empty",
                    xlsx_path)
        return []
    raw_rows = load_existing(xlsx_path)
    iso = target.isoformat()
    out: List[Recall] = []
    for row in raw_rows:
        row_date = str(row.get("Date") or "")[:10]
        if row_date != iso:
            continue
        try:
            out.append(Recall(
                Date=row_date,
                Source=str(row.get("Source") or ""),
                Company=str(row.get("Company") or ""),
                Brand=str(row.get("Brand") or "—"),
                Product=str(row.get("Product") or ""),
                Pathogen=str(row.get("Pathogen") or ""),
                Reason=str(row.get("Reason") or ""),
                Class=str(row.get("Class") or "Recall"),
                Country=str(row.get("Country") or ""),
                Region=str(row.get("Region") or ""),
                Tier=int(row.get("Tier") or 2),
                Outbreak=int(row.get("Outbreak") or 0),
                URL=str(row.get("URL") or ""),
                Notes=str(row.get("Notes") or ""),
            ))
        except (ValueError, TypeError) as e:
            log.warning("Skipping malformed Recalls row for %s: %s",
                        row.get("Company"), e)
    return out


def render_daily_html(target_date: date, recalls: List[Recall],
                      regions_scanned: int) -> str:
    t1 = sum(1 for r in recalls if r.Tier == 1)
    outbreak = sum(1 for r in recalls if r.Outbreak == 1)
    pretty = target_date.strftime("%A, %d %B %Y")

    if not recalls:
        body = ('<div class="empty">No pathogen / biotoxin / foreign-material / '
                'chemical recalls published by monitored regulators this day.'
                '<br>(Allergen-only recalls are excluded per AFTS scope.)</div>')
    else:
        # Group by region
        by_region: Dict[str, List[Recall]] = {}
        for r in recalls:
            by_region.setdefault(r.Region or "Other", []).append(r)

        parts = []
        for region in sorted(by_region.keys()):
            parts.append(f'<div class="section"><h2>{region}</h2>')
            for r in sorted(by_region[region],
                            key=lambda x: (-x.Outbreak, x.Tier, x.Country)):
                cls = "card"
                if r.Tier == 1:
                    cls += " t1"
                elif r.Outbreak == 1:
                    cls += " ob"
                tier_badge = (f'<span class="c-tier t{r.Tier}">TIER {r.Tier}</span>'
                              if r.Tier in (1, 2) else "")
                ob_note = " · OUTBREAK" if r.Outbreak == 1 else ""
                url_display = r.URL
                if len(url_display) > 55:
                    url_display = url_display[:52] + "…"
                parts.append(
                    f'<div class="{cls}">'
                    f'<div class="c-hd">'
                    f'<span class="c-src">{r.Source} · {r.Country}{ob_note}</span>'
                    f'{tier_badge}</div>'
                    f'<div class="c-co">{_h(r.Company)}</div>'
                    f'<div class="c-pr">{_h(r.Product)}</div>'
                    f'<div class="c-why"><strong>{_h(r.Pathogen)}</strong> — '
                    f'{_h(r.Reason)}</div>'
                    f'<a class="c-link" href="{_h(r.URL)}" target="_blank" '
                    f'rel="noopener">Open notice ↗</a></div>'
                )
            parts.append('</div>')
        body = "".join(parts)

    now_athens = _now_athens_str()
    return DAILY_HTML_TEMPLATE.format(
        DATE_PRETTY=pretty,
        GENERATED_AT=now_athens,
        REGIONS_SCANNED=regions_scanned,
        TOTAL=len(recalls),
        T1=t1,
        OUTBREAK=outbreak,
        BODY=body,
    )


def _h(s: str) -> str:
    """Lightweight HTML escape for the tiny report."""
    if not s:
        return ""
    return (str(s).replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))


def _now_athens_str() -> str:
    """ISO-ish Athens time for the header."""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Europe/Athens")).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


# ---------------------------------------------------------------------------
# daily-index.json — dashboard reads this to show the latest-day card
# ---------------------------------------------------------------------------
def update_daily_index(target_date: date, recalls: List[Recall]) -> None:
    """
    Maintain docs/daily-index.json as a small rolling window, not a
    one-day snapshot.

    Why not just "keep one day" like before:
      An empty-brief day (model returned 0, search API timeout, budget
      cap hit mid-run) would wipe the entire dashboard — exactly what
      happened on Apr 22 2026. Now we keep up to KEEP_DAYS entries so
      a single bad day doesn't erase yesterday's legit brief.

    Behavior:
      1. Load existing entries from daily-index.json.
      2. Replace (or insert) today's target_date entry.
      3. Drop entries older than KEEP_DAYS from today.
      4. Delete matching daily/*.html files that fell out of the window.

    KEEP_DAYS is intentionally small (7) — this is still a "recent daily
    briefs" feed, not an archive. Weekly/monthly reports handle history.
    """
    KEEP_DAYS = 7
    DAILY_DIR.mkdir(parents=True, exist_ok=True)
    iso = target_date.isoformat()

    # Region summary for the card on the dashboard
    region_counts: Dict[str, int] = {}
    for r in recalls:
        region_counts[r.Region or "Other"] = region_counts.get(r.Region or "Other", 0) + 1

    entry = {
        "date": iso,
        "url": f"daily/{iso}.html",
        "total": len(recalls),
        "tier1": sum(1 for r in recalls if r.Tier == 1),
        "outbreak": sum(1 for r in recalls if r.Outbreak == 1),
        "by_region": region_counts,
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    # Load existing, upsert today's entry, trim window
    existing: List[Dict[str, Any]] = []
    if DAILY_INDEX.exists():
        try:
            existing = json.loads(DAILY_INDEX.read_text()).get("entries", [])
        except Exception as e:
            log.warning("Could not parse existing daily-index.json: %s", e)
    existing = [e for e in existing if e.get("date") != iso]
    existing.append(entry)
    existing.sort(key=lambda e: e.get("date", ""), reverse=True)

    # Keep only entries within KEEP_DAYS of today's target_date
    cutoff = (target_date - timedelta(days=KEEP_DAYS - 1)).isoformat()
    existing = [e for e in existing if e.get("date", "") >= cutoff]

    DAILY_INDEX.write_text(json.dumps({"entries": existing}, indent=2))
    log.info("Wrote %s (%d entry/entries, keeping last %d days)",
             DAILY_INDEX, len(existing), KEEP_DAYS)

    # Delete HTML files for dates no longer in the index. This is the
    # safety net for disk hygiene — index.json is the source of truth,
    # any HTML whose date isn't in the index is garbage-collected.
    keep_dates = {e["date"] for e in existing}
    if DAILY_DIR.exists():
        deleted = 0
        for f in DAILY_DIR.glob("*.html"):
            stem = f.stem
            if len(stem) == 10 and stem[4] == "-" and stem[7] == "-" and stem not in keep_dates:
                try:
                    f.unlink()
                    deleted += 1
                    log.info("  Removed out-of-window brief: %s", f.name)
                except Exception as e:
                    log.warning("  Could not remove %s: %s", f, e)
        if deleted:
            log.info("Removed %d out-of-window daily brief(s).", deleted)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="Target YYYY-MM-DD (default: yesterday Athens)")
    ap.add_argument("--regions", help="Comma-separated subset of regions",
                    default="")
    ap.add_argument("--dry-run", action="store_true",
                    help="Run API calls but don't write xlsx/html/commit")
    args = ap.parse_args()

    if not TAVILY_API_KEY:
        log.error("TAVILY_API_KEY not set — cannot run.")
        # No target known yet; use today's UTC date as best-effort marker so
        # the Exa fallback still sees a recent status file and can run.
        write_status_file(
            ok=False, recalls_count=0, regions_done=0,
            target_date=datetime.now(timezone.utc).date(),
        )
        return 1

    # Determine target date (Athens yesterday)
    if args.date:
        target = date.fromisoformat(args.date)
    else:
        try:
            from zoneinfo import ZoneInfo
            now_athens = datetime.now(ZoneInfo("Europe/Athens"))
        except Exception:
            now_athens = datetime.now(timezone.utc) + timedelta(hours=3)
        target = (now_athens - timedelta(days=1)).date()

    log.info("=" * 64)
    log.info("Daily Recall Search — target date: %s", target.isoformat())
    log.info("=" * 64)

    # Budget check
    ledger = load_ledger()
    week_spent = current_week_spend_eur(ledger)
    log.info("Budget: weekly spend so far €%.3f / cap €%.2f",
             week_spent, HARD_CAP_EUR_PER_WEEK)
    if week_spent >= HARD_CAP_EUR_PER_WEEK:
        log.error("WEEKLY BUDGET EXCEEDED (€%.3f ≥ €%.2f). Aborting run.",
                  week_spent, HARD_CAP_EUR_PER_WEEK)
        return 2

    # Region filter
    target_regions = [r["region"] for r in REGION_SPECS]
    if args.regions:
        want = {r.strip() for r in args.regions.split(",") if r.strip()}
        target_regions = [r for r in target_regions if r in want]
    log.info("Regions: %s", target_regions)

    # Load existing Recalls sheet for dedup
    approved = load_existing(XLSX_PATH) if XLSX_PATH.exists() else []
    pending = load_pending(XLSX_PATH) if XLSX_PATH.exists() else []

    existing_urls = {normalize_key(row.get("URL", "") or row.get("url", ""))
                     for row in approved + pending}
    existing_sigs = set()
    for row in approved + pending:
        existing_sigs.add((
            str(row.get("Date") or row.get("date") or "")[:10],
            normalize_key(str(row.get("Company") or row.get("company") or ""))[:60],
            normalize_key(str(row.get("Product") or row.get("product") or ""))[:60],
        ))

    # Run region sweeps
    new_recalls: List[Recall] = []
    run_cost_eur = 0.0
    regions_done = 0

    for spec in REGION_SPECS:
        if spec["region"] not in target_regions:
            continue

        # Per-run cap check
        ledger_before = sum(float(e["eur"]) for e in ledger.get("entries", [])
                            if e["ts"].startswith(datetime.now(timezone.utc)
                                                  .strftime("%Y-%m-%d")))
        if ledger_before >= HARD_CAP_EUR_PER_RUN:
            log.warning("Per-run cap €%.2f reached, skipping remaining regions",
                        HARD_CAP_EUR_PER_RUN)
            break

        log.info("→ Region %s", spec["region"])
        result = call_tavily_search(target, spec["region"], spec["agencies"],
                                    ledger)
        regions_done += 1
        if not result:
            continue

        raw_rows = result.get("recalls") or []
        log.info("   raw=%d", len(raw_rows))

        # If the model returned zero, capture the raw response for debug.
        # Empty-brief episodes like Apr 22 2026 (the day this logging was
        # added) are almost always the model being too strict about date
        # matching or timing out mid-search. Dumping the raw text gives us
        # something concrete to tune against.
        if len(raw_rows) == 0:
            log.warning("   [%s] empty — raw response dump follows:", spec["region"])
            log.warning("   %s", json.dumps(result)[:2000])

        # Compute the acceptable 3-day publish-date window. Must match the
        # window described in build_user_prompt().
        accept_dates = {
            (target - timedelta(days=1)).isoformat(),
            target.isoformat(),
            (target + timedelta(days=1)).isoformat(),
        }

        for row in raw_rows:
            if not is_in_scope(row):
                continue
            # Date guard — model must return a date inside the 3-day
            # window (yesterday ±1), not some unrelated historical date.
            row_date = (row.get("date") or "")[:10]
            if row_date not in accept_dates:
                log.info("   skipping out-of-window row (%s not in %s)",
                         row_date, sorted(accept_dates))
                continue
            if is_duplicate(row, existing_urls, existing_sigs):
                continue
            rec = to_recall(row)
            if rec is None:
                continue
            new_recalls.append(rec)
            existing_urls.add(normalize_key(rec.URL))
            existing_sigs.add((
                rec.Date,
                normalize_key(rec.Company)[:60],
                normalize_key(rec.Product)[:60],
            ))

        log.info("   kept=%d after filter+dedup", len(new_recalls))

    # Persist ledger
    save_ledger(ledger)
    new_week_spent = current_week_spend_eur(ledger)
    log.info("Week spend now: €%.3f / cap €%.2f",
             new_week_spent, HARD_CAP_EUR_PER_WEEK)

    log.info("Total new candidate recalls after filter + dedup: %d",
             len(new_recalls))

    # ========================================================================
    # STEP 1: Write new candidates to PENDING (not Recalls).
    # ========================================================================
    # The URL guardian workflow is the sole gate that promotes Pending→Recalls
    # after validating each URL actually resolves. Going straight to Recalls
    # pollutes the sheet with unverified rows and is what broke the Apr 22
    # daily brief (Nestlé Colombia cereulide, VFA Vietnam rat-poison etc.
    # appeared in the brief without ever existing in the Recalls sheet).
    scraped_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if new_recalls and not args.dry_run:
        updated_pending = append_to_pending(
            existing_pending=pending,
            approved=approved,
            new_recalls=new_recalls,
            scraped_at=scraped_at,
        )
        pending_delta = len(updated_pending) - len(pending)
        # ── Gap-finder gating (audit 2026-04-29): Tavily search-based
        # discovery, same trust level as gap_finder_tavily.py.
        from pipeline.merge_master import STATUS_PENDING_GAP
        tagged = 0
        for r in updated_pending:
            if r.get("ScrapedAt") == scraped_at:
                r["Status"] = STATUS_PENDING_GAP
                tagged += 1
        save_xlsx_with_pending(
            xlsx_path=XLSX_PATH,
            approved_rows=sort_rows(approved),  # Recalls sheet untouched
            pending_rows=sort_rows(updated_pending),
        )
        log.info("Appended %d candidate rows to PENDING sheet "
                 "(%d tagged Status=pending_gap; pending total=%d). "
                 "URL guardian will validate + promote to Recalls.",
                 pending_delta, tagged, len(updated_pending))
    elif new_recalls:
        log.info("DRY RUN: would append %d candidate rows to PENDING",
                 len(new_recalls))

    # ========================================================================
    # STEP 2: Render daily HTML briefs FROM THE RECALLS SHEET.
    # ========================================================================
    # Regulators commonly publish recalls dated 2-3 days in the past (the
    # recall happens, a few days pass, then the agency posts it). We therefore
    # rebuild a sliding window of [today .. target - LOOKBACK_DAYS] every
    # daily run so any late-promoted row lands in the right brief.
    #
    # Also includes TODAY explicitly — recalls promoted same-day need a brief
    # rendered for today, not just yesterday.
    #
    # Env override: BRIEF_LOOKBACK_DAYS (default 4 → covers today, yesterday,
    # 2 days ago, 3 days ago, 4 days ago = 5 daily briefs total).
    BRIEF_LOOKBACK_DAYS = int(os.getenv("BRIEF_LOOKBACK_DAYS", "4"))
    DAILY_DIR.mkdir(parents=True, exist_ok=True)
    brief_paths: list[str] = []

    # Always include today (for same-day promotions) PLUS the
    # target..target-LOOKBACK window. Use a set + sorted descending list to
    # avoid double-rendering when target == today.
    try:
        from zoneinfo import ZoneInfo
        today_athens = datetime.now(ZoneInfo("Europe/Athens")).date()
    except Exception:
        today_athens = (datetime.now(timezone.utc) + timedelta(hours=3)).date()
    dates_to_render = {today_athens}
    for offset in range(BRIEF_LOOKBACK_DAYS + 1):
        dates_to_render.add(target - timedelta(days=offset))
    sorted_dates = sorted(dates_to_render, reverse=True)
    log.info("Will (re)render %d daily briefs: %s",
             len(sorted_dates),
             ", ".join(d.isoformat() for d in sorted_dates))

    for d in sorted_dates:
        day_recalls = load_recalls_for_date(XLSX_PATH, d)
        log.info("Rendering brief for %s from Recalls sheet: %d row(s) match",
                 d.isoformat(), len(day_recalls))
        day_html_path = DAILY_DIR / f"{d.isoformat()}.html"
        # regions_done only applies to the target day's search; use 0 for
        # back-fill days (the brief template tolerates 0 gracefully).
        html = render_daily_html(d, day_recalls,
                                 regions_done if d == target else 0)
        if not args.dry_run:
            day_html_path.write_text(html, encoding="utf-8")
            log.info("Wrote %s", day_html_path)
            update_daily_index(d, day_recalls)
            brief_paths.append(str(day_html_path))

    # Primary brief (today's target) — used in commit message stats
    brief_recalls = load_recalls_for_date(XLSX_PATH, target)

    # --- Commit + push ---
    if not args.dry_run and not SKIP_COMMIT:
        paths = [str(XLSX_PATH), str(DAILY_INDEX), str(SPEND_LEDGER)]
        paths.extend(brief_paths)
        msg = (f"Daily recall search {target.isoformat()}: "
               f"+{len(new_recalls)} → Pending, "
               f"{len(brief_recalls)} in brief from Recalls, "
               f"€{new_week_spent-week_spent:.3f} spent "
               f"(rebuilt {len(brief_paths)} daily briefs, "
               f"window: today + {BRIEF_LOOKBACK_DAYS} days back)")
        git_commit_and_push(ROOT, paths, msg)
        log.info("Committed and pushed.")

    # --- Status file for Exa fallback ----------------------------------------
    # Written unconditionally so the Exa workflow always has a fresh file to
    # read; should_fallback=True if Tavily hit rate-limit/auth or returned
    # zero recalls across all regions.
    write_status_file(
        ok=True,
        recalls_count=len(new_recalls),
        regions_done=regions_done,
        target_date=target,
    )

    return 0
