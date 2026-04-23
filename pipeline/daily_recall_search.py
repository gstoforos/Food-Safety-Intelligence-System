"""
Daily Recall Search — Claude-powered global recall finder (Pending-first).

Uses Claude Haiku 4.5 with web search for verified URLs.

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
  - Hard cap per run: €0.12 (config HARD_CAP_EUR_PER_RUN)
  - Hard cap per week: €0.50 (HARD_CAP_EUR_PER_WEEK) — persisted in
    docs/data/.spend_ledger.json so a bad run can't blow the budget.
  - Current per-run expected cost ≈ €0.01 (gpt-4o-mini-search-preview only,
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

# ---------------------------------------------------------------------------
# Budget
# ---------------------------------------------------------------------------
HARD_CAP_EUR_PER_RUN = float(os.getenv("DAILY_BUDGET_EUR_PER_RUN", "0.15"))
HARD_CAP_EUR_PER_WEEK = float(os.getenv("DAILY_BUDGET_EUR_PER_WEEK", "1.00"))
USD_TO_EUR = 0.92  # static — close enough; doesn't need to be exact

# Claude Haiku 4.5 pricing per 1M tokens
PRICE_INPUT_USD_PER_1M = 0.80
PRICE_OUTPUT_USD_PER_1M = 4.00

MODEL = os.getenv("CLAUDE_DAILY_MODEL", "claude-haiku-4-5-20251001")
API_KEY = os.getenv("ANTHROPIC_API_KEY", "").strip()
API_ENDPOINT = "https://api.anthropic.com/v1/messages"

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
    yday = target_date
    prev = target_date - timedelta(days=1)
    nxt  = target_date + timedelta(days=1)
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
        f"health alert, food safety notice, market withdrawal, or RASFF "
        f"notification officially published by a food regulator in {region} "
        f"within a 3-day window centered on yesterday. The target day is:\n\n"
        f"  TARGET DAY (yesterday Athens time): {fmt_variants}\n"
        f"  ACCEPTABLE PUBLISH DATES: {prev.isoformat()} | "
        f"{yday.isoformat()} | {nxt.isoformat()}\n\n"
        f"Include recalls whose publication/issue date falls on ANY of "
        f"those three dates. Regulator sites often show dates as DD/MM/YYYY "
        f"or localized text — normalize them and include if they match one "
        f"of the three acceptable ISO dates above.\n\n"
        f"IMPORTANT — RASFF (EU Rapid Alert System):\n"
        f"  RASFF notifications (Alerts, Information Notifications, Border "
        f"Rejections) are functionally equivalent to recalls. Search RASFF "
        f"directly: 'RASFF notification {yday.strftime('%B %Y')}' and also "
        f"'webgate.ec.europa.eu rasff {yday.strftime('%d %B %Y')}'. Include "
        f"every RASFF notification whose date falls in the 3-day window. "
        f"Set source='RASFF (EU)' and country=origin country.\n\n"
        f"REGULATORS TO COVER (search each by name + recent recalls): {agencies}\n\n"
        f"SEARCH STRATEGY:\n"
        f"  - For each regulator, search '<agency name> recall "
        f"{yday.strftime('%B %Y')}' and '<agency name> recent recalls'\n"
        f"  - Also try '<agency name> food safety {yday.strftime('%d %B %Y')}'\n"
        f"  - Then open the 2-3 most recent recall notices from each agency "
        f"and check their publication date against the 3-day window\n"
        f"  - If you see phrases like 'today', 'this week', 'yesterday', "
        f"'hier', 'aujourd'hui', 'heute', translate them using the fact "
        f"that the current date is {nxt.isoformat()}\n"
        f"  - For RASFF: search 'RASFF window latest notifications' and "
        f"check the most recent entries\n\n"
        f"IN SCOPE — include recalls where the hazard is:\n"
        f"  • Pathogens: Listeria, Salmonella, E. coli / STEC / O157, "
        f"Clostridium botulinum, Norovirus, Hepatitis A, Campylobacter, "
        f"Cronobacter, Bacillus cereus, Cyclospora, Shigella, Vibrio, "
        f"Yersinia, Brucella\n"
        f"  • Mould / spoilage: visible mould contamination, yeast "
        f"overgrowth, spoilage microorganisms\n"
        f"  • Biotoxins: histamine/scombrotoxin, marine biotoxins "
        f"(DSP/PSP/ASP, domoic acid, saxitoxin, ciguatera), cereulide\n"
        f"  • Mycotoxins: aflatoxin, ochratoxin, patulin, Alternaria, "
        f"fumonisin, zearalenone, deoxynivalenol\n"
        f"  • Foreign material: glass, metal, plastic, wood, stone\n"
        f"  • Rodent / insect / pest contamination (physical hazard)\n"
        f"  • Chemical: heavy metals (lead, cadmium, mercury, arsenic) "
        f"over legal limit, pesticide residues over MRL, ethylene oxide "
        f"(EtO), chlorate, dioxins/PCBs, mineral oil (MOAH/MOSH), Sudan "
        f"dyes, melamine, unauthorized substances (rodenticide, DMAE, "
        f"novel food ingredients, unauthorized additives/colours)\n\n"
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
        f'"BVL (DE)", "RASFF (EU)", "FSANZ (AU)"\n'
        f'  "company": firm/producer name\n'
        f'  "brand": brand name or "—"\n'
        f'  "product": full product description with size/pack\n'
        f'  "hazard_type": one of PATHOGEN, BIOTOXIN, MYCOTOXIN, MOULD, '
        f"FOREIGN_MATERIAL, PEST_CONTAMINATION, CHEMICAL\n"
        f'  "pathogen": specific agent (e.g. "Listeria monocytogenes", '
        f'"Salmonella Enteritidis", "mould", "glass fragments", "rodent"), '
        f'or "—"\n'
        f'  "reason": short cause description in English\n'
        f'  "class": recall class if stated (Class I/II/III, Volontaire, '
        f'Alert, Border Rejection, Recall) — or "Recall"\n'
        f'  "outbreak": 1 if ANY illness/hospitalisation/death mentioned, '
        f"else 0\n"
        f'  "url": DEEP-LINK URL to the specific recall detail page on the '
        f"regulator's official domain. Must be a URL you actually retrieved "
        f"via web_search. NEVER a category or homepage. If you cannot find "
        f"a specific deep-link page, use the category page URL as fallback — "
        f"do NOT omit the recall.\n"
        f'  "notes": distribution area, lot codes, illness count, RASFF '
        f"reference number if applicable, or any extra context.\n\n"
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
# Claude API call (with web search tool)
# ---------------------------------------------------------------------------
def call_claude_search(target_date: date, region: str, agencies: str,
                       ledger: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Single call to Claude Haiku 4.5 with web_search tool.
    Returns parsed JSON or None. Updates the budget ledger.

    Claude's web search returns real URLs from actual search results —
    unlike OpenAI's search-preview which often hallucinates URLs.
    The URLs still go through URL guardian validation before promotion.
    """
    if not API_KEY:
        log.error("ANTHROPIC_API_KEY not set")
        return None

    user_prompt = build_user_prompt(target_date, region, agencies)

    body = {
        "model": MODEL,
        "max_tokens": 4096,
        "system": SYSTEM_PROMPT,
        "tools": [{"type": "web_search_20250305", "name": "web_search"}],
        "messages": [
            {"role": "user", "content": user_prompt},
        ],
    }

    try:
        r = requests.post(
            API_ENDPOINT,
            headers={
                "x-api-key": API_KEY,
                "anthropic-version": "2025-03-19",
                "content-type": "application/json",
            },
            json=body,
            timeout=180,  # web search can take longer
        )
    except Exception as e:
        log.warning("Claude request failed for %s: %s", region, e)
        return None

    if r.status_code != 200:
        log.warning("Claude %d for %s: %s", r.status_code, region, r.text[:300])
        return None

    resp = r.json()

    # Extract text from response — Claude returns content blocks
    content_blocks = resp.get("content", [])
    texts = [blk.get("text", "") for blk in content_blocks
             if blk.get("type") == "text" and blk.get("text")]
    if not texts:
        log.warning("Claude returned no text blocks for %s", region)
        return None

    # The last text block typically contains the JSON answer
    msg = texts[-1].strip()

    # Token usage
    usage = resp.get("usage", {}) or {}
    in_tok = int(usage.get("input_tokens", 0))
    out_tok = int(usage.get("output_tokens", 0))
    cost_usd = (in_tok * PRICE_INPUT_USD_PER_1M / 1_000_000 +
                out_tok * PRICE_OUTPUT_USD_PER_1M / 1_000_000)
    cost_eur = cost_usd * USD_TO_EUR
    record_spend(ledger, cost_eur, region, in_tok, out_tok)
    log.info("  [%s] in=%d out=%d cost≈€%.4f", region, in_tok, out_tok, cost_eur)

    # Strip markdown fences if Claude added them
    text = msg
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z]*\s*\n?", "", text)
        text = re.sub(r"\n```\s*$", "", text).strip()

    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        log.warning("[%s] JSON parse failed: %s | %s", region, e, text[:250])
        return None

    return data


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
    r"bacillus\s*cereus|cereulide|shigella|yersinia|brucell|"
    r"histamine|scombro|biotoxin|dsp|psp|asp|domoic|saxitox|ciguatera|"
    r"aflatox|ochratox|patulin|alternaria|fumonisin|zearalenone|"
    r"deoxynivalenol|mycotox|mould|mold|"
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
    r"chlorpyrifos|glyphosate|dmae|novel\s+food|"
    r"ethylene\s*oxide|eto\b|chlorate|chlorpropham|"
    r"dioxin|pcb|mineral\s+oil|moah|mosh|"
    r"sudan\s+(?:dye|red|iv)|melamine|acrylamide|"
    r"pah|polycyclic|unauthorized\s+(?:gmo|colour|color|additive))",
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

    valid_hazards = {"PATHOGEN", "BIOTOXIN", "MYCOTOXIN", "MOULD",
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
            Source=row.get("source", "") or "OpenAI-daily",
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
                   "  [via OpenAI daily 10:00 Athens search]")[:500],
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
<a class="back" href="https://www.advfood.tech/food-safety-intelligence">← Dashboard</a>
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
Pathogens + biotoxins + mycotoxins + mould + foreign material + pest + chemical hazards only.<br>
Allergen-only, labeling, quality issues excluded per AFTS scope.<br>
Source: recalls.xlsx (verified URLs only).<br>
<a href="https://www.advfood.tech/food-safety-intelligence">Back to dashboard</a> · <a href="../daily-index.json">JSON archive</a>
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

    if not API_KEY:
        log.error("ANTHROPIC_API_KEY not set — cannot run.")
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

        # Per-run cap check — only count THIS run's spend (entries added
        # since we started), not the whole day's cumulative spend.
        this_run_spend = sum(
            float(e["eur"]) for e in ledger.get("entries", [])
            if e.get("ts", "") >= datetime.now(timezone.utc).strftime("%Y-%m-%dT")
        )
        if this_run_spend >= HARD_CAP_EUR_PER_RUN:
            log.warning("Per-run cap €%.2f reached, skipping remaining regions",
                        HARD_CAP_EUR_PER_RUN)
            break

        log.info("→ Region %s", spec["region"])
        result = call_claude_search(target, spec["region"], spec["agencies"],
                                    ledger)
        regions_done += 1
        if not result:
            continue

        raw_rows = result.get("recalls") or []
        log.info("   raw=%d", len(raw_rows))

        # Retry once if the model returned zero — often a transient search
        # timeout or overly strict date matching on first attempt.
        if len(raw_rows) == 0:
            log.warning("   [%s] empty on first try — retrying once", spec["region"])
            result2 = call_claude_search(target, spec["region"], spec["agencies"],
                                         ledger)
            if result2:
                raw_rows = result2.get("recalls") or []
                log.info("   retry raw=%d", len(raw_rows))
            if len(raw_rows) == 0:
                log.warning("   [%s] still empty after retry — raw dump:", spec["region"])
                log.warning("   %s", json.dumps(result)[:2000])

        # Compute the acceptable 3-day publish-date window. Must match the
        # window described in build_user_prompt().
        accept_dates = {
            (target - timedelta(days=1)).isoformat(),
            target.isoformat(),
            (target + timedelta(days=1)).isoformat(),
        }

        region_kept = 0
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
            region_kept += 1
            existing_urls.add(normalize_key(rec.URL))
            existing_sigs.add((
                rec.Date,
                normalize_key(rec.Company)[:60],
                normalize_key(rec.Product)[:60],
            ))

        log.info("   [%s] kept=%d new (total so far=%d)",
                 spec["region"], region_kept, len(new_recalls))

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
        save_xlsx_with_pending(
            xlsx_path=XLSX_PATH,
            approved_rows=sort_rows(approved),  # Recalls sheet untouched
            pending_rows=sort_rows(updated_pending),
        )
        log.info("Appended %d candidate rows to PENDING sheet (pending "
                 "total=%d). URL guardian will validate + promote to Recalls.",
                 pending_delta, len(updated_pending))
    elif new_recalls:
        log.info("DRY RUN: would append %d candidate rows to PENDING",
                 len(new_recalls))

    # ========================================================================
    # STEP 2: Render the daily HTML brief FROM THE RECALLS SHEET ONLY.
    # ========================================================================
    # The brief renders ONLY from the verified Recalls sheet. OpenAI results
    # sit in Pending until the URL guardian validates + promotes them. This
    # means the brief shows only recalls with verified URLs — no hallucinated
    # OpenAI links. A "0 recalls" brief is generated (and committed) so the
    # dashboard card always appears, even on quiet days.
    brief_recalls = load_recalls_for_date(XLSX_PATH, target)
    log.info("Rendering brief for %s from Recalls sheet: %d verified row(s)",
             target.isoformat(), len(brief_recalls))

    DAILY_DIR.mkdir(parents=True, exist_ok=True)
    daily_html_path = DAILY_DIR / f"{target.isoformat()}.html"
    html = render_daily_html(target, brief_recalls, regions_done)
    if not args.dry_run:
        daily_html_path.write_text(html, encoding="utf-8")
        log.info("Wrote %s", daily_html_path)
        update_daily_index(target, brief_recalls)

    # --- Commit + push ---
    if not args.dry_run and not SKIP_COMMIT:
        paths = [str(XLSX_PATH), str(daily_html_path), str(DAILY_INDEX),
                 str(SPEND_LEDGER)]
        msg = (f"Daily recall search {target.isoformat()}: "
               f"+{len(new_recalls)} → Pending, "
               f"{len(brief_recalls)} in brief from Recalls, "
               f"€{new_week_spent-week_spent:.3f} spent")
        git_commit_and_push(ROOT, paths, msg)
        log.info("Committed and pushed.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
