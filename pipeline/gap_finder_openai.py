"""
OpenAI gap-finder — a scheduled job that asks OpenAI's knowledge base for all
food pathogen recalls worldwide in the last N days and appends any that aren't
already in Recalls or Pending.

This is our safety net against scraper gaps. Example: the Liquid Blenz Corp
Botulism recall published April 14 2026 at
https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/liquid-blenz-corp-recalls-product-due-possible-health-risk
was missed by the FDA scraper — a job like this would catch it.

Runs daily at 06:00 UTC (between the 17:00 scrape and the 07:30 Claude URL
check). Whatever it appends goes through the 07:30 URL check and is promoted
to Recalls only if the URL is live.

Single global query per run (~1 API call, ~$0.01 on gpt-4o-mini). Intentionally
aggressive dedup so the same URL never gets re-appended.
"""
from __future__ import annotations
import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scrapers._models import Recall, normalize_pathogen, normalize_country, infer_region, assign_tier  # noqa: E402
from pipeline.merge_master import (  # noqa: E402
    load_existing, load_pending,
    append_to_pending, sort_rows, save_xlsx_with_pending,
)
from pipeline.commit_github import git_commit_and_push  # noqa: E402

import requests as _requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("openai-gap-finder")

DATA_DIR = ROOT / "docs" / "data"
XLSX_PATH = DATA_DIR / "recalls.xlsx"
JSON_PATH = DATA_DIR / "recalls.json"

SINCE_DAYS = int(os.getenv("GAP_SINCE_DAYS", "5"))
SKIP_COMMIT = os.getenv("SKIP_COMMIT", "").lower() in ("1", "true", "yes")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_GAP_MODEL", "gpt-4o-mini-search-preview")
OPENAI_ENABLED = bool(OPENAI_API_KEY)

# 5-region specs covering all 66+ agencies
REGION_SPECS = [
    {"region": "Europe", "agencies": (
        "RASFF (EU, webgate.ec.europa.eu/rasff-window), RappelConso (France), "
        "BVL/lebensmittelwarnung.de (Germany), AGES (Austria), FSAI (Ireland), "
        "FSA UK, FSS Scotland, Livsmedelsverket (Sweden), Fødevarestyrelsen (Denmark), "
        "Mattilsynet (Norway), Ruokavirasto (Finland), ŠVPS (Slovakia), SZPI (Czech), "
        "AESAN (Spain), Min. Salute (Italy), NVWA (Netherlands), AFSCA (Belgium), "
        "ASAE (Portugal), BLV (Switzerland), EFET (Greece), GIS (Poland), "
        "PVD (Latvia), VTA (Estonia), VMVT (Lithuania), MAST (Iceland)")},
    {"region": "NorthAmerica", "agencies": (
        "FDA (fda.gov), USDA FSIS (fsis.usda.gov), CDC, "
        "CFIA Canada (recalls-rappels.canada.ca), MAPAQ Quebec")},
    {"region": "LATAM", "agencies": (
        "ANVISA Brazil, COFEPRIS Mexico, ANMAT Argentina, ISP Chile, "
        "INVIMA Colombia, DIGESA Peru, ARCSA Ecuador, MSP Uruguay")},
    {"region": "AsiaPacific", "agencies": (
        "FSANZ Australia, MPI New Zealand, MFDS Korea, MHLW Japan, "
        "CFS Hong Kong, SFA Singapore, FSSAI India, FDA Philippines, "
        "BPOM Indonesia, MoH Malaysia, TFDA Taiwan, Thai FDA, SAMR China")},
    {"region": "MiddleEastAfrica", "agencies": (
        "SFDA Saudi Arabia, MoCCAE UAE, MoH Israel, MoPH Qatar, "
        "TGTHB Turkey, NAFDAC Nigeria, NCC South Africa, NFSA Egypt, "
        "ONSSA Morocco, KEBS Kenya, FDA Ghana")},
]


# ─────────────────────────────────────────────────────────────────────────────
# Primary-region weighting — see pipeline/gap_finder_claude.py for the full
# rationale. OpenAI's strength is Bing-backed search over Spanish / Portuguese
# regulator pages and emerging-market sites — so its primaries are LATAM and
# MiddleEastAfrica. Other regions still swept (lighter pass).
#
# Multiple primary regions allowed for OpenAI specifically because LATAM +
# Middle East / Africa together are roughly the same "weight" as Europe alone.
# ─────────────────────────────────────────────────────────────────────────────
PRIMARY_REGIONS = [r.strip() for r in os.getenv(
    "GAP_PRIMARY_REGIONS", "LATAM,MiddleEastAfrica"
).split(",") if r.strip()]


def _primary_banner(primary_list: List[str]) -> str:
    primary_str = " + ".join(primary_list)
    return (
        f"⚑ PRIMARY-REGION DEEP SWEEP — '{primary_str}' is your strongest "
        f"region. Spend EXTRA effort here: open every regulator listing page, "
        f"scroll the 'most recent' / 'press releases' / 'alertas' / "
        f"'noticias' section, and follow links into individual recall pages "
        f"so you capture details (Date, Company, Brand, Product, Pathogen, "
        f"full URL). Other regions still in scope but use lighter sweeps. ⚑\n\n"
    )


def _ordered_specs(primary_list: List[str]) -> List[Dict[str, Any]]:
    primary_specs = [s for s in REGION_SPECS if s["region"] in primary_list]
    others = [s for s in REGION_SPECS if s["region"] not in primary_list]
    return primary_specs + others


GAP_FINDER_SYSTEM = (
    "You are a senior food safety analyst. You search worldwide regulators for "
    "food recalls, public health alerts, RASFF notifications, and market withdrawals. "
    "You NEVER make up URLs or facts. Return ONLY strict JSON — no markdown, no prose."
)


GAP_FINDER_PROMPT = """Using your web search, find EVERY food recall, public health alert, RASFF notification, or market withdrawal issued in {region} in the last {since_days} days.

Today's date: {today}

REGULATORS TO SEARCH: {agencies}

In scope: Listeria, Salmonella, E. coli / STEC / O157:H7, Clostridium
botulinum, Norovirus, Hepatitis A, Campylobacter, Cyclospora, Vibrio, Cronobacter
sakazakii, Bacillus cereus / cereulide, Shigella, Yersinia, Brucella,
Aflatoxins, Ochratoxin A, Patulin, marine biotoxins (DSP/PSP/ASP),
Histamine (scombrotoxin), other mycotoxins, mould/mold contamination,
foreign material (glass / metal / plastic / wood / stone fragments),
rodent / insect / pest contamination,
chemical hazards: heavy metals (lead, cadmium, mercury, arsenic), ethylene
oxide, dioxins/PCBs, mineral oil (MOAH/MOSH), pesticide residues over MRL,
Sudan dyes, melamine, chlorate, unauthorized substances.
Also include EU RASFF notifications (alerts, border rejections).

OUT of scope: undeclared allergens (unless combined with in-scope hazard),
labeling errors, quality complaints, non-food products.

For each recall return ALL fields below:
- Date       : YYYY-MM-DD, the recall / alert publication date
- Source     : agency short name, e.g. "FDA", "USDA FSIS", "RASFF", "FSA", "CFIA"
- Company    : firm / producer name
- Brand      : commercial brand name ("—" if not stated)
- Product    : full product description including size / pack where available
- Pathogen   : specific pathogen detected, e.g. "Listeria monocytogenes"
- Reason     : short cause description
- Class      : recall class ("Recall" / "Alert" / "Class I/II/III" / "Public Health Alert" etc.)
- Country    : English country name, e.g. "United States", "France"
- Outbreak   : 1 if illnesses / cases mentioned, else 0
- URL        : FULL deep-link URL to the specific recall page — NOT a homepage
               or category page. You MUST include a verifiable URL. If you cannot
               produce a specific recall-page URL, OMIT that recall entirely.
- Notes      : distribution area, lot / batch info, illness count, extra context

CRITICAL RULES:
1. Only include recalls you are confident actually happened and whose URL you are
   confident points to the real recall page. If uncertain, omit.
2. Never invent or hallucinate URLs. Every URL must be a real page that exists.
3. The URL must be specific (e.g. .../liquid-blenz-corp-recalls-product-due-..
   or .../fiche-rappel/12345), NEVER a homepage or category listing.
4. Coverage goal: worldwide — US, EU member states, UK, Canada, Australia, NZ,
   Japan, Korea, China/HK, India, Brazil, Mexico, Argentina, South Africa,
   Middle East, etc.

Return strict JSON:
{{"recalls": [{{"Date":"...","Source":"...","Company":"...","Brand":"...","Product":"...","Pathogen":"...","Reason":"...","Class":"...","Country":"...","Outbreak":0,"URL":"...","Notes":"..."}}]}}

If no pathogen recalls happened in the last {since_days} days, return: {{"recalls": []}}
"""


def _call_openai_search(prompt: str, system: str, max_tokens: int = 4096) -> str:
    """Call gpt-4o-mini-search-preview with web search enabled."""
    try:
        r = _requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": OPENAI_MODEL,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": prompt},
                ],
                "max_tokens": max_tokens,
                "web_search_options": {},
            },
            timeout=120,
        )
        if r.status_code != 200:
            log.warning("OpenAI %d: %s", r.status_code, r.text[:300])
            return ""
        return r.json()["choices"][0]["message"]["content"] or ""
    except Exception as e:
        log.warning("OpenAI call failed: %s", e)
        return ""


def query_openai_for_gaps(since_days: int) -> List[Dict[str, Any]]:
    """5-region intensive sweep with web search. Returns raw recall dicts."""
    if not OPENAI_ENABLED:
        log.warning("OPENAI_API_KEY not set — gap-finder cannot run")
        return []

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    all_recalls: List[Dict[str, Any]] = []

    primaries = PRIMARY_REGIONS
    specs = _ordered_specs(primaries)

    for spec in specs:
        region = spec["region"]
        agencies = spec["agencies"]
        is_primary = region in primaries
        log.info("→ Region %s%s", region, "  [PRIMARY]" if is_primary else "")

        prompt = GAP_FINDER_PROMPT.format(
            since_days=since_days, today=today,
            region=region, agencies=agencies,
        )
        if is_primary:
            prompt = _primary_banner(primaries) + prompt

        txt = _call_openai_search(prompt, system=GAP_FINDER_SYSTEM)
        if not txt:
            log.warning("  [%s] empty response", region)
            continue

        # Strip markdown fences
        txt = txt.strip()
        if txt.startswith("```"):
            import re
            txt = re.sub(r"^```[a-zA-Z]*\s*\n?", "", txt)
            txt = re.sub(r"\n```\s*$", "", txt).strip()

        try:
            data = json.loads(txt)
        except json.JSONDecodeError as e:
            log.warning("  [%s] JSON parse failed: %s | %s", region, e, txt[:250])
            continue

        rows = data.get("recalls", []) or []
        log.info("  [%s] raw=%d", region, len(rows))
        all_recalls.extend(rows)

    log.info("OpenAI total proposed: %d recalls across %d regions",
             len(all_recalls), len(REGION_SPECS))
    return all_recalls


def to_recall_objects(raw: List[Dict[str, Any]]) -> List[Recall]:
    """Convert raw OpenAI dicts to normalized Recall objects."""
    out: List[Recall] = []
    for row in raw:
        try:
            pathogen = normalize_pathogen(row.get("Pathogen", "") or "")
            country = normalize_country(row.get("Country", "") or "")
            outbreak = int(row.get("Outbreak", 0) or 0)
            rec = Recall(
                Date=(row.get("Date") or "")[:10],
                Source=row.get("Source", "") or "OpenAI-gap",
                Company=row.get("Company", "") or "",
                Brand=row.get("Brand", "") or "—",
                Product=row.get("Product", "") or "",
                Pathogen=pathogen,
                Reason=row.get("Reason", "") or "",
                Class=row.get("Class", "") or "",
                Country=country,
                Region=infer_region(country) if country else "",
                Tier=assign_tier(pathogen, outbreak),
                Outbreak=outbreak,
                URL=(row.get("URL") or "").strip(),
                Notes=row.get("Notes", "") or "",
            )
            rec = rec.normalize()
            # Hard filter: we require both Pathogen and URL. Rows without either
            # can never be promoted and would only pollute Pending.
            if not rec.Pathogen or rec.Pathogen in ("—", ""):
                continue
            if not rec.URL or not rec.URL.lower().startswith(("http://", "https://")):
                continue
            out.append(rec)
        except Exception as e:
            log.warning("Skipping malformed gap-finder row: %s (%s)", e, row)
            continue
    log.info("Gap-finder: %d raw -> %d valid Recall objects", len(raw), len(out))
    return out


def main() -> int:
    t0 = datetime.now(timezone.utc)
    scraped_at = t0.strftime("%Y-%m-%dT%H:%M:%SZ")
    log.info("=" * 60)
    log.info("OpenAI gap-finder run: %s", scraped_at)
    log.info("Data dir: %s", DATA_DIR)

    if not OPENAI_ENABLED:
        log.error("OPENAI_API_KEY missing — aborting")
        return 2

    # ---- Load current state ---------------------------------------------
    approved = load_existing(XLSX_PATH) if XLSX_PATH.exists() else []
    existing_pending = load_pending(XLSX_PATH) if XLSX_PATH.exists() else []
    log.info("State: %d approved + %d existing pending", len(approved), len(existing_pending))

    # ---- Ask OpenAI -----------------------------------------------------
    raw = query_openai_for_gaps(SINCE_DAYS)
    if not raw:
        log.info("Nothing proposed — exiting cleanly")
        return 0

    # ---- Normalize to Recall objects ------------------------------------
    recalls = to_recall_objects(raw)
    if not recalls:
        log.info("No gap-finder rows survived validation — exiting cleanly")
        return 0

    # ---- Append to Pending ----------------------------------------------
    before = len(existing_pending)
    pending = append_to_pending(
        existing_pending=existing_pending,
        approved=approved,
        new_recalls=recalls,
        scraped_at=scraped_at,
    )
    net_new = len(pending) - before
    log.info("Gap-finder added %d net-new rows to Pending", net_new)

    # ---- Save ------------------------------------------------------------
    save_xlsx_with_pending(approved, sort_rows(pending), XLSX_PATH)

    # ---- Commit ----------------------------------------------------------
    if not SKIP_COMMIT and net_new > 0:
        ok = git_commit_and_push(
            repo_dir=ROOT,
            files=["docs/data/recalls.xlsx"],
            message=f"FSIS gap-finder {t0.strftime('%Y-%m-%d')} (+{net_new} pending)",
        )
        if not ok:
            log.error("Git push failed")
            return 1

    elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
    log.info("DONE in %.1fs | +%d pending rows", elapsed, net_new)
    return 0


if __name__ == "__main__":
    sys.exit(main())
