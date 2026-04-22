"""
Daily Recall Search — OpenAI-powered "yesterday across every region" sweep.

REPLACES pipeline/gap_finder_openai.py.

Every morning 10:00 Athens this job asks gpt-4o-mini-search-preview (OpenAI's
specialized web-search model) to find EVERY official food recall, safety
alert, or withdrawal posted YESTERDAY by a national or regional regulator,
grouped by region. It:

  1. Runs 5 region sweeps (Europe, N.America, LATAM, Asia-Pacific, MEA)
  2. Extracts structured rows, one per recall — in scope: pathogens,
     biotoxins, mycotoxins, glass/metal/plastic foreign objects, rodent/insect
     contamination, chemical contaminants (heavy metals, pesticides over
     limits). OUT of scope: allergens-only, labeling errors, quality issues.
  3. Dedups against the existing Recalls + Pending sheets by URL and by
     (company, product, date) fuzzy match.
  4. Appends survivors to the Recalls sheet directly (bypasses Pending
     since each row carries a live regulator URL verified by the model).
  5. Writes docs/daily/YYYY-MM-DD.html — mobile-first styled report.
  6. Updates docs/daily-index.json so the dashboard can render the latest
     daily-summary card between News and Weekly tabs.

Budget controls:
  - Hard cap per run: €0.12 (config HARD_CAP_EUR_PER_RUN)
  - Hard cap per week: €0.50 (HARD_CAP_EUR_PER_WEEK) — persisted in
    docs/data/.spend_ledger.json so a bad run can't blow the budget.
  - Each run records spend against the ledger after completing.
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
    save_xlsx_with_pending, merge_new,
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
HARD_CAP_EUR_PER_RUN = float(os.getenv("DAILY_BUDGET_EUR_PER_RUN", "0.12"))
HARD_CAP_EUR_PER_WEEK = float(os.getenv("DAILY_BUDGET_EUR_PER_WEEK", "0.50"))
USD_TO_EUR = 0.92  # static — close enough; doesn't need to be exact

# gpt-4o-mini-search-preview pricing per 1M tokens
PRICE_INPUT_USD_PER_1M = 0.15
PRICE_OUTPUT_USD_PER_1M = 0.60

MODEL = os.getenv("OPENAI_DAILY_MODEL", "gpt-4o-mini-search-preview")
API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
API_ENDPOINT = "https://api.openai.com/v1/chat/completions"

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
    return (
        f"TASK: Using your web_search tool, find every food recall, public "
        f"health alert, food safety notice, or product withdrawal that was "
        f"officially published in {region} on {target_date.isoformat()} "
        f"(yesterday, not today, not earlier dates — strictly the day "
        f"{target_date.strftime('%A %B %d %Y')}).\n\n"
        f"REGULATORS TO COVER (search each): {agencies}.\n\n"
        f"IN SCOPE — include recalls where the hazard is:\n"
        f"  • Pathogens: Listeria, Salmonella, E. coli / STEC / O157, "
        f"Clostridium botulinum, Norovirus, Hepatitis A, Campylobacter, "
        f"Cronobacter, Bacillus cereus, Cyclospora, Shigella, Vibrio, "
        f"Yersinia\n"
        f"  • Biotoxins: histamine/scombrotoxin, marine biotoxins "
        f"(DSP/PSP/ASP, domoic acid, saxitoxin, ciguatera), cereulide\n"
        f"  • Mycotoxins: aflatoxin, ochratoxin, patulin, Alternaria, "
        f"fumonisin, zearalenone, deoxynivalenol\n"
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
        f'{{"date": "{target_date.isoformat()}", "region": "{region}", '
        f'"recalls": [ROW, ROW, ...]}}\n\n'
        f"Each ROW object has these fields (always all of them):\n"
        f'  "date": "YYYY-MM-DD" — must equal {target_date.isoformat()} '
        f"(publish date or recall-notice issue date)\n"
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
        f"the specific recall page, OMIT that recall entirely.\n"
        f'  "notes": distribution area, lot codes, illness count, or any '
        f"extra context worth capturing.\n\n"
        f"CRITICAL RULES:\n"
        f"1. If a regulator published nothing yesterday in {region}, "
        f'return {{"date": "...", "region": "...", "recalls": []}}.\n'
        f"2. NEVER invent URLs. Every URL must appear verbatim in a real "
        f"search result you ran.\n"
        f"3. NEVER include allergen-only recalls.\n"
        f"4. The recall-notice publish date must be exactly "
        f"{target_date.isoformat()} — do not return older recalls that "
        f"you happen to find.\n"
        f"5. Return ONLY the JSON object, nothing else."
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
# OpenAI call
# ---------------------------------------------------------------------------
def call_openai_search(target_date: date, region: str, agencies: str,
                       ledger: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Single call to gpt-4o-mini-search-preview. Returns parsed JSON or None.
    Updates the budget ledger.
    """
    if not API_KEY:
        log.error("OPENAI_API_KEY not set")
        return None

    body = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": build_user_prompt(
                target_date, region, agencies)},
        ],
        # gpt-4o-mini-search-preview has web search built in — no temperature
        # gpt-4o-mini-search-preview does NOT accept temperature != 1
        "max_tokens": 4096,
        "web_search_options": {},  # trigger web search
    }

    try:
        r = requests.post(
            API_ENDPOINT,
            headers={
                "Authorization": f"Bearer {API_KEY}",
                "Content-Type": "application/json",
            },
            json=body,
            timeout=120,
        )
    except Exception as e:
        log.warning("OpenAI request failed for %s: %s", region, e)
        return None

    if r.status_code != 200:
        log.warning("OpenAI %d for %s: %s", r.status_code, region, r.text[:300])
        return None

    resp = r.json()
    try:
        msg = resp["choices"][0]["message"]["content"]
    except (KeyError, IndexError):
        log.warning("Unexpected response shape for %s: %s", region, str(resp)[:300])
        return None

    usage = resp.get("usage", {}) or {}
    in_tok = int(usage.get("prompt_tokens", 0))
    out_tok = int(usage.get("completion_tokens", 0))
    cost_usd = (in_tok * PRICE_INPUT_USD_PER_1M / 1_000_000 +
                out_tok * PRICE_OUTPUT_USD_PER_1M / 1_000_000)
    cost_eur = cost_usd * USD_TO_EUR
    record_spend(ledger, cost_eur, region, in_tok, out_tok)
    log.info("  [%s] in=%d out=%d cost≈€%.4f", region, in_tok, out_tok, cost_eur)

    # Strip markdown fences if the model added them
    text = msg.strip()
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
ALLERGEN_ONLY_MARKERS = re.compile(
    r"undeclared\s+(?:milk|egg|peanut|nut|soy|wheat|gluten|sulphite|sulfite|"
    r"fish|shellfish|sesame|celery|mustard|lupin|crustacean)",
    re.I,
)


def is_in_scope(row: Dict[str, Any]) -> bool:
    """Allergen-only rejector + hazard-type sanity check."""
    reason = (row.get("reason") or "") + " " + (row.get("notes") or "")
    pathogen = (row.get("pathogen") or "").lower()
    hazard_type = (row.get("hazard_type") or "").upper().strip()

    valid_hazards = {"PATHOGEN", "BIOTOXIN", "MYCOTOXIN",
                     "FOREIGN_MATERIAL", "PEST_CONTAMINATION", "CHEMICAL"}
    if hazard_type not in valid_hazards:
        return False

    # Reject rows that are purely allergen-labelled even if hazard_type says otherwise
    if hazard_type in ("PATHOGEN", "BIOTOXIN", "MYCOTOXIN"):
        # Must have a specific hazard word
        if "allergen" in pathogen or pathogen in ("", "—"):
            return False

    if ALLERGEN_ONLY_MARKERS.search(reason) and "listeria" not in reason.lower() \
            and "salmonella" not in reason.lower() \
            and hazard_type == "PATHOGEN":
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
<a class="back" href="./index.html">← Dashboard</a>
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
<a href="./index.html">Back to dashboard</a> · <a href="./daily-index.json">JSON archive</a>
</div>
</div></body></html>
"""


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
    """Append to docs/daily-index.json so the dashboard can render a card."""
    DAILY_DIR.mkdir(parents=True, exist_ok=True)
    entries = []
    if DAILY_INDEX.exists():
        try:
            data = json.loads(DAILY_INDEX.read_text())
            entries = data.get("entries", [])
        except Exception:
            entries = []

    iso = target_date.isoformat()
    # Remove any existing entry for this date (idempotent)
    entries = [e for e in entries if e.get("date") != iso]

    # Compute region summary
    region_counts: Dict[str, int] = {}
    for r in recalls:
        region_counts[r.Region or "Other"] = region_counts.get(r.Region or "Other", 0) + 1

    entries.append({
        "date": iso,
        "url": f"daily/{iso}.html",
        "total": len(recalls),
        "tier1": sum(1 for r in recalls if r.Tier == 1),
        "outbreak": sum(1 for r in recalls if r.Outbreak == 1),
        "by_region": region_counts,
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    })
    entries.sort(key=lambda e: e["date"], reverse=True)
    # Keep last 90 days
    entries = entries[:90]

    DAILY_INDEX.write_text(json.dumps({"entries": entries}, indent=2))
    log.info("Updated %s (%d entries)", DAILY_INDEX, len(entries))


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
        log.error("OPENAI_API_KEY not set — cannot run.")
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
        result = call_openai_search(target, spec["region"], spec["agencies"],
                                    ledger)
        regions_done += 1
        if not result:
            continue

        raw_rows = result.get("recalls") or []
        log.info("   raw=%d", len(raw_rows))

        for row in raw_rows:
            if not is_in_scope(row):
                continue
            # Date guard — model must return yesterday, reject drift
            if (row.get("date") or "")[:10] != target.isoformat():
                log.info("   skipping wrong-date row (%s != %s)",
                         (row.get("date") or "")[:10], target.isoformat())
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

    log.info("Total new recalls after filter + dedup: %d", len(new_recalls))

    # --- Write xlsx ---
    if new_recalls and not args.dry_run:
        merged = merge_new(approved, new_recalls)
        save_xlsx_with_pending(
            xlsx_path=XLSX_PATH,
            approved_rows=sort_rows(merged),
            pending_rows=sort_rows(pending),
        )
        log.info("Appended %d rows to Recalls sheet (total=%d)",
                 len(new_recalls), len(merged))
    elif new_recalls:
        log.info("DRY RUN: would append %d rows to Recalls", len(new_recalls))

    # --- Write daily HTML report ---
    DAILY_DIR.mkdir(parents=True, exist_ok=True)
    daily_html_path = DAILY_DIR / f"{target.isoformat()}.html"
    html = render_daily_html(target, new_recalls, regions_done)
    if not args.dry_run:
        daily_html_path.write_text(html, encoding="utf-8")
        log.info("Wrote %s", daily_html_path)
        update_daily_index(target, new_recalls)

    # --- Commit + push ---
    if not args.dry_run and not SKIP_COMMIT:
        paths = [str(XLSX_PATH), str(daily_html_path), str(DAILY_INDEX),
                 str(SPEND_LEDGER)]
        msg = (f"Daily recall search {target.isoformat()}: +{len(new_recalls)} "
               f"rows, {regions_done} regions, €{new_week_spent-week_spent:.3f} "
               f"spent")
        git_commit_and_push(ROOT, paths, msg)
        log.info("Committed and pushed.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
