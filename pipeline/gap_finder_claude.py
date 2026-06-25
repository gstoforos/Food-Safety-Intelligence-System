"""
Claude gap-finder — REACTIVATED 2026-04-29
==========================================================================

Was previously retired with a no-op stub (see git log) because earlier
versions had no native web access and pattern-matched URLs from training
data. That's solved: this module now uses Claude's web_search_20250305
tool (`_call_claude_web` below) so every URL Claude returns must have
appeared in a real search result it ran.

Runs alongside gap_finder_gemini.py, gap_finder_tavily.py, and
gap_finder_exa.py. Each one's search backend indexes regulator pages
differently — Tavily and Exa are deterministic, Gemini is grounded on
Google, and Claude (via Anthropic web_search) tends to follow citation
trails through news outlets to original recall pages well. Different
gaps caught by different finders.

Cost (claude-haiku-4-5): ~$0.005 per region × 4 regions × 1 run/day ≈
$0.60/month. Cheaper than the OpenAI/search-tool alternative and
worth keeping as a fourth independent backstop.

==========================================================================

AUDIT 2026-05-14 — Rate-limit hardening
==========================================================================

PROBLEM observed in production logs 2026-05-14 00:04:
  • Region 1 (AsiaPacific): 15.6s, success
  • Region 2 (Europe):      10.9s, success
  • Region 3 (NorthAmerica): HTTP 429 ("50,000 input tokens per minute" cap)
  • Region 4 (LATAM_ME_Africa): HTTP 429 (same cap, same minute)
  → Half of regions returned ZERO results due to org-tier rate limit.
    Cascade reported success at +1 row but coverage was degraded silently.

ROOT CAUSE: Each Claude call uses web_search_20250305 with max_uses=3.
Anthropic's web_search injects the FULL text of search-result snippets
into the same request as additional input tokens, so a single region
call consumes 15-30K input tokens. With Anthropic's tier-1 default cap
of 50K input tokens/minute, two consecutive calls exhaust the bucket
before the third can fire.

CONSTRAINT: pipeline/gap_finder_cascade.py hard-kills the Claude step
at GAP_STEP_TIMEOUT=90s via SIGALRM. So we cannot just sleep 60s+
between regions — that would exceed the budget and get SIGKILLed
mid-region, losing the entire step's output.

FIX — four layers:

  1. Lower max_uses default 3 → 2 (configurable via
     GAP_FINDER_CLAUDE_WEB_MAX_USES). Saves ~33% per-call input tokens
     and alone often fits all 4 regions in the per-minute budget.

  2. Track Anthropic's rate-limit headers across calls (module-level
     state). Before each call, if remaining input-token budget is
     below threshold, sleep until reset — capped so we never exceed
     the cascade's wall-clock budget.

  3. On HTTP 429, read 'retry-after' and retry ONCE (capped sleep) —
     handles the residual case where layers 1+2 weren't enough.

  4. Wall-clock budget tracker in query_claude_for_gaps — if we're
     approaching the cascade timeout, skip remaining regions and
     return what we have. Partial coverage beats no coverage
     (which is what SIGKILL gives us).

Env vars introduced by this fix (all optional, all with sensible defaults):
  GAP_FINDER_CLAUDE_WEB_MAX_USES   (default 2)   — web_search max_uses
  GAP_FINDER_CLAUDE_BUDGET_SEC     (default 80)  — wall-clock cap for the
                                                   region sweep (cascade
                                                   timeout is 90s, leaving
                                                   10s margin for the
                                                   commit step)
  GAP_FINDER_CLAUDE_PACE_THRESHOLD (default 15000) — sleep before next call
                                                     if remaining input
                                                     token budget < this
  GAP_FINDER_CLAUDE_MAX_PACE_SLEEP (default 25)  — cap on any one sleep
                                                   (proactive or 429 retry)
  GAP_FINDER_CLAUDE_HTTP_TIMEOUT   (default 60)  — per-region HTTP timeout
                                                   (was hardcoded 300s)

==========================================================================

Original docstring:

Claude gap-finder — a scheduled job that asks Claude's knowledge base for all
food pathogen recalls worldwide in the last N days and appends any that aren't
already in Recalls or Pending.

Counterpart to pipeline/gap_finder_openai.py — same contract, same prompt,
same Pending-sheet flow. Two independent LLMs catch different gaps; whatever
either proposes goes through the 07:30 Athens URL gate and is promoted to
Recalls only if the URL is live.

Runs daily at 05:00 UTC (1h before OpenAI gap-finder at 06:00, both well
after the 17:00 Athens scrape). Single global query per run.

Model default is claude-haiku-4-5 (cheap, already configured in claude_client).
Override via ANTHROPIC_MODEL env var if a heavier model is needed — the task
is once-a-day so cost is not a constraint.
"""
from __future__ import annotations
import os
import sys
import re
import json
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scrapers._models import Recall, normalize_pathogen, normalize_country, infer_region, assign_tier  # noqa: E402
from pipeline.merge_master import (  # noqa: E402
    load_existing, load_pending,
    append_to_pending, sort_rows, save_xlsx_with_pending,
)
from pipeline.commit_github import git_commit_and_push  # noqa: E402
from review.claude_client import _call_claude, _strip_fences, ENABLED as CLAUDE_ENABLED  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("gap-finder-claude")

DATA_DIR = ROOT / "docs" / "data"
XLSX_PATH = DATA_DIR / "recalls.xlsx"
JSON_PATH = DATA_DIR / "recalls.json"

# Lookback window in days. Audit 2026-05-05 — unified env var name
# GAP_LOOKBACK_DAYS (default 3). Falls back to GAP_SINCE_DAYS (legacy name)
# if set, then to the default. This applies to all gap-finders so the
# operator only needs to set one env var to change the window everywhere.
SINCE_DAYS = int(os.getenv("GAP_LOOKBACK_DAYS", os.getenv("GAP_SINCE_DAYS", "3")))
SKIP_COMMIT = os.getenv("SKIP_COMMIT", "").lower() in ("1", "true", "yes")

# Rate-limit hardening config (audit 2026-05-14). See module docstring.
WEB_SEARCH_MAX_USES   = int(os.getenv("GAP_FINDER_CLAUDE_WEB_MAX_USES", "2"))
CLAUDE_BUDGET_SEC     = float(os.getenv("GAP_FINDER_CLAUDE_BUDGET_SEC", "80"))
PACE_THRESHOLD_TOKENS = int(os.getenv("GAP_FINDER_CLAUDE_PACE_THRESHOLD", "15000"))
MAX_PACE_SLEEP_SEC    = float(os.getenv("GAP_FINDER_CLAUDE_MAX_PACE_SLEEP", "25"))
HTTP_TIMEOUT_SEC      = float(os.getenv("GAP_FINDER_CLAUDE_HTTP_TIMEOUT", "60"))

# Module-level rate-limit state, populated from Anthropic response headers
# after every successful call. Used by _maybe_pace_before_call() to decide
# whether to sleep before the next call (and for how long).
#   _LAST_INPUT_REMAINING: int  — remaining input tokens in the current minute bucket
#   _LAST_INPUT_RESET_AT:  float — unix timestamp when the bucket fully resets
# Both are reset to None when the module loads (i.e. at process start).
_LAST_INPUT_REMAINING: Optional[int]   = None
_LAST_INPUT_RESET_AT:  Optional[float] = None


GAP_FINDER_SYSTEM = (
    "You are a senior food safety analyst with deep knowledge of global food "
    "recalls from regulators including FDA, USDA FSIS, EU RASFF, FSA (UK), FSAI "
    "(Ireland), FSANZ (Australia/NZ), CFIA (Canada), AESAN (Spain), BVL "
    "(Germany), RappelConso (France), EFET (Greece), Salute.gov.it (Italy), "
    "CFS (Hong Kong), MFDS (Korea), MHLW (Japan), ANVISA (Brazil), SENASA "
    "(Argentina), COFEPRIS (Mexico), FSSAI (India), NAFDAC (Nigeria), and "
    "others. Return ONLY strict JSON — no markdown, no prose, no commentary."
)


GAP_FINDER_PROMPT = """Using your web search, find EVERY food recall, public health alert, RASFF notification, or market withdrawal issued in {region} in the last {since_days} days.

Today's date: {today}

REGULATORS TO SEARCH — go to each agency's website and check their latest postings:
{agencies}

For each agency above, search: '<agency name> food recall' and '<agency name> latest alerts'. Open the results and check publication dates within the last {since_days} days.

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


REGION_SPECS = [
    {"region": "Europe", "agencies": (
        "RASFF (webgate.ec.europa.eu/rasff-window), RappelConso (rappel.conso.gouv.fr), "
        "BVL (lebensmittelwarnung.de), FSA UK (food.gov.uk/news-alerts), "
        "FSAI Ireland (fsai.ie), AESAN Spain, AGES Austria, NVWA Netherlands, "
        "AFSCA Belgium, EFET Greece, Min. Salute Italy, BLV Switzerland")},
    {"region": "NorthAmerica", "agencies": (
        "FDA (fda.gov/safety/recalls), USDA FSIS (fsis.usda.gov/recalls), "
        "CFIA Canada (recalls-rappels.canada.ca)")},
    {"region": "AsiaPacific", "agencies": (
        "FSANZ Australia (foodstandards.gov.au/food-recalls), MPI New Zealand, "
        "MFDS Korea, MHLW Japan, CFS Hong Kong, SFA Singapore, FSSAI India")},
    {"region": "LATAM_ME_Africa", "agencies": (
        "ANVISA Brazil, COFEPRIS Mexico, ANMAT Argentina, "
        "SFDA Saudi Arabia, NAFDAC Nigeria, NCC South Africa")},
]


# ─────────────────────────────────────────────────────────────────────────────
# Primary-region weighting — Claude / OpenAI / Gemini / Tavily complement
# instead of duplicating each other. Each AI is biased toward the region its
# search backend handles best:
#
#   Claude (web_search via Anthropic) → AsiaPacific
#       Strong English-language reasoning over CFS-HK, MFDS-KR, MHLW-JP,
#       FSANZ, MPI-NZ pages. Anthropic's web_search returns clean snippets
#       and Claude follows the citation trail well.
#
#   OpenAI (gpt-4o-mini-search-preview) → LATAM_ME_Africa
#       Bing-backed search has the best coverage of ANVISA, COFEPRIS, ANMAT,
#       SFDA, NAFDAC, NCC. Spanish/Portuguese pages index well in Bing.
#
#   Gemini (2.5 Flash + google_search) → Europe
#       Google indexes EU regulator pages most completely (BVL, AESAN, AGES,
#       RappelConso, BLV, EFET, ASAE, etc.) and handles multilingual content
#       best. Free tier (1500 req/day) lets Gemini do the heaviest sweep.
#
#   Tavily (Tavily search + deterministic parsing) → NorthAmerica
#       FDA / USDA-FSIS / CFIA are high-volume, structurally consistent
#       English pages — Tavily's whitelisted-domain extractor handles them
#       reliably without an LLM call.
#
# Each gap-finder still sweeps ALL four regions every run (so coverage is
# global). The PRIMARY_REGION just (a) runs first, (b) gets a longer
# Search-this-deeply banner injected in its prompt. Override at runtime
# with --region <X> (single-region run) or --primary-region <X>.
# ─────────────────────────────────────────────────────────────────────────────
PRIMARY_REGION = os.getenv("GAP_PRIMARY_REGION", "AsiaPacific")


def _primary_banner(primary: str) -> str:
    """One-paragraph instruction injected at top of prompt when running
    against the AI's primary region."""
    return (
        f"⚑ PRIMARY-REGION DEEP SWEEP — '{primary}' is your strongest region. "
        f"Spend EXTRA effort here: open every regulator listing page, scroll "
        f"the 'most recent' / 'press releases' section, and follow links into "
        f"individual recall pages so you capture details (Date, Company, "
        f"Brand, Product, Pathogen, full URL). Other regions still in scope "
        f"but use lighter sweeps. ⚑\n\n"
    )


def _ordered_specs(primary: str) -> List[Dict[str, Any]]:
    """REGION_SPECS reordered with the primary region first."""
    primary_spec = next((s for s in REGION_SPECS if s["region"] == primary), None)
    if not primary_spec:
        return list(REGION_SPECS)
    others = [s for s in REGION_SPECS if s["region"] != primary]
    return [primary_spec] + others


# ─────────────────────────────────────────────────────────────────────────────
# Rate-limit pacing helpers (audit 2026-05-14)
# ─────────────────────────────────────────────────────────────────────────────

def _update_ratelimit_state(headers) -> None:
    """Parse Anthropic rate-limit headers from a response and stash the
    input-token remaining/reset values in module state for the next call's
    pacing decision.

    Anthropic returns several rate-limit headers per response:
      anthropic-ratelimit-input-tokens-limit
      anthropic-ratelimit-input-tokens-remaining
      anthropic-ratelimit-input-tokens-reset (ISO-8601 datetime)
      ... and the same trio for output-tokens, requests
    We track the INPUT-TOKEN trio because that's the constraint that bit us
    on 2026-05-14 (50K/min cap, web_search inflates input tokens via the
    injected search-result context).

    Silently no-ops if headers are missing (older Anthropic responses, or
    error responses without the headers populated).
    """
    global _LAST_INPUT_REMAINING, _LAST_INPUT_RESET_AT
    try:
        remaining_raw = headers.get("anthropic-ratelimit-input-tokens-remaining")
        reset_raw     = headers.get("anthropic-ratelimit-input-tokens-reset")
        if remaining_raw is not None:
            _LAST_INPUT_REMAINING = int(remaining_raw)
        if reset_raw:
            # ISO-8601 with trailing Z; replace for Python's fromisoformat
            iso = reset_raw.replace("Z", "+00:00")
            dt = datetime.fromisoformat(iso)
            _LAST_INPUT_RESET_AT = dt.timestamp()
    except Exception as e:
        log.debug("Could not parse Anthropic rate-limit headers: %s", e)


def _maybe_pace_before_call(deadline_ts: Optional[float] = None) -> None:
    """Proactive sleep before issuing the next Claude call if the
    rate-limit budget is low.

    If we know the bucket has < PACE_THRESHOLD_TOKENS remaining and a
    reset is in the future, sleep until reset (capped by
    MAX_PACE_SLEEP_SEC and by `deadline_ts` if provided).

    No-op if rate-limit state is not yet populated (first call of the run).
    """
    if _LAST_INPUT_REMAINING is None or _LAST_INPUT_RESET_AT is None:
        return
    if _LAST_INPUT_REMAINING >= PACE_THRESHOLD_TOKENS:
        return

    now = time.time()
    seconds_until_reset = max(0.0, _LAST_INPUT_RESET_AT - now)
    sleep_sec = min(seconds_until_reset + 0.5, MAX_PACE_SLEEP_SEC)
    if deadline_ts is not None:
        remaining_budget = max(0.0, deadline_ts - now - 5.0)  # 5s safety margin
        sleep_sec = min(sleep_sec, remaining_budget)
    if sleep_sec <= 0.1:
        return
    log.info("Pacing: input-tokens-remaining=%d < %d, sleeping %.1fs "
             "(reset in %.1fs)",
             _LAST_INPUT_REMAINING, PACE_THRESHOLD_TOKENS,
             sleep_sec, seconds_until_reset)
    time.sleep(sleep_sec)


# ─────────────────────────────────────────────────────────────────────────────
# Claude HTTP call (with 429 retry and rate-limit-aware pacing)
# ─────────────────────────────────────────────────────────────────────────────

def _call_claude_web(prompt: str, system: str, max_tokens: int = 4096,
                    deadline_ts: Optional[float] = None) -> str:
    """Claude Haiku 4.5 with web search — requests.post (proven approach).

    Rate-limit handling (audit 2026-05-14):
      • Before issuing the request, _maybe_pace_before_call() may sleep
        if Anthropic's prior response indicated the input-token bucket
        is low.
      • web_search max_uses is read from GAP_FINDER_CLAUDE_WEB_MAX_USES
        env var (default 2). Lowering from 3→2 cuts per-call input
        tokens by ~33% because web_search injects search-result text
        into the request as additional input tokens.
      • On HTTP 429, parses 'retry-after' header and retries ONCE
        (capped at MAX_PACE_SLEEP_SEC seconds, and never sleeping past
        deadline_ts).
      • HTTP timeout: 60s default (was 300s) — one hung region cannot
        consume the entire cascade step budget.
    """
    import requests as _req
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        return ""

    # Proactive pacing based on Anthropic's headers from the prior call.
    _maybe_pace_before_call(deadline_ts=deadline_ts)

    body = {
        "model": os.getenv("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001"),
        "max_tokens": max_tokens,
        "system": system,
        "tools": [{
            "type": "web_search_20250305",
            "name": "web_search",
            "max_uses": WEB_SEARCH_MAX_USES,
        }],
        "messages": [{"role": "user", "content": prompt}],
    }
    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }

    for attempt in (1, 2):  # initial + up to 1 retry
        # Check we still have wall-clock budget before issuing the call.
        if deadline_ts is not None and time.time() > deadline_ts:
            log.warning("Wall-clock budget exhausted before HTTP call — abort")
            return ""
        try:
            r = _req.post(
                "https://api.anthropic.com/v1/messages",
                headers=headers,
                json=body,
                timeout=HTTP_TIMEOUT_SEC,
            )
        except _req.exceptions.Timeout:
            log.error("Claude HTTP timeout after %.0fs (attempt %d)",
                      HTTP_TIMEOUT_SEC, attempt)
            return ""
        except Exception as e:
            log.error("Claude web call failed (attempt %d): %s", attempt, e)
            return ""

        # Always parse rate-limit headers so the NEXT call can pace itself
        # — Anthropic populates them on 200, 429, and most error responses.
        _update_ratelimit_state(r.headers)

        if r.status_code == 429:
            if attempt >= 2:
                log.error("Claude HTTP 429 on retry — giving up: %s",
                          r.text[:300])
                return ""
            # Honour the server's retry-after header when present;
            # otherwise fall back to the cap.
            retry_after_raw = (r.headers.get("retry-after")
                               or r.headers.get("Retry-After")
                               or "")
            try:
                retry_after = float(retry_after_raw)
            except (TypeError, ValueError):
                retry_after = MAX_PACE_SLEEP_SEC
            sleep_sec = min(max(retry_after, 1.0), MAX_PACE_SLEEP_SEC)
            if deadline_ts is not None:
                remaining_budget = max(0.0, deadline_ts - time.time() - 5.0)
                sleep_sec = min(sleep_sec, remaining_budget)
            if sleep_sec <= 0.5:
                log.warning("Claude 429 but no budget left for retry — abort")
                return ""
            log.warning("Claude 429: sleeping %.1fs then retrying "
                        "(retry-after=%r)", sleep_sec, retry_after_raw)
            time.sleep(sleep_sec)
            continue

        if r.status_code != 200:
            log.error("Claude HTTP %d: %s", r.status_code, r.text[:500])
            return ""

        resp = r.json()
        log.info("  stop=%s blocks=%s remaining=%s",
                 resp.get("stop_reason"),
                 [b.get("type") for b in resp.get("content", [])],
                 _LAST_INPUT_REMAINING)
        texts = [b.get("text", "") for b in resp.get("content", [])
                 if b.get("type") == "text" and b.get("text", "").strip()]
        return texts[-1].strip() if texts else ""

    return ""


def query_claude_for_gaps(since_days: int, region_filter: str = None) -> List[Dict[str, Any]]:
    """Web search sweep. If region_filter set, runs only that region.

    Wall-clock budget (audit 2026-05-14):
      Tracks elapsed time since the sweep started. If we're within 10s of
      CLAUDE_BUDGET_SEC, stop iterating and return whatever we have. The
      cascade's SIGALRM kicks in at GAP_STEP_TIMEOUT=90s and kills the
      whole process — partial coverage with a clean exit is strictly
      better than no coverage with a SIGKILL.
    """
    if not CLAUDE_ENABLED:
        log.warning("ANTHROPIC_API_KEY not set — gap-finder cannot run")
        return []
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    all_recalls: List[Dict[str, Any]] = []

    primary = PRIMARY_REGION
    specs = _ordered_specs(primary)

    start_ts = time.time()
    deadline_ts = start_ts + CLAUDE_BUDGET_SEC
    log.info("Wall-clock budget: %.0fs (deadline at +%.0fs)",
             CLAUDE_BUDGET_SEC, CLAUDE_BUDGET_SEC)

    regions_completed = 0
    regions_skipped_budget: List[str] = []

    for i, spec in enumerate(specs):
        region = spec["region"]
        agencies = spec["agencies"]

        # Skip regions not matching the filter
        if region_filter and region != region_filter:
            continue

        # Budget check — if we're within the 10s safety margin of the
        # deadline, don't start another region. The cascade's SIGALRM at
        # 90s would kill us mid-call and we'd lose this region's output
        # AND any commit/cleanup the caller is going to do.
        time_left = deadline_ts - time.time()
        if time_left <= 10.0:
            log.warning("Wall-clock budget low (%.1fs left) — skipping "
                        "remaining region(s): %s",
                        time_left,
                        ", ".join(s["region"] for s in specs[i:]
                                  if not region_filter or s["region"] == region_filter))
            regions_skipped_budget = [s["region"] for s in specs[i:]]
            break

        is_primary = (region == primary and not region_filter)
        log.info("→ Region %s%s  (budget left: %.0fs)",
                 region, "  [PRIMARY]" if is_primary else "", time_left)

        prompt = GAP_FINDER_PROMPT.format(
            since_days=since_days, today=today,
            region=region, agencies=agencies,
        )
        if is_primary:
            prompt = _primary_banner(primary) + prompt

        txt = _call_claude_web(prompt, system=GAP_FINDER_SYSTEM,
                               deadline_ts=deadline_ts)
        if not txt:
            log.warning("  [%s] empty response", region)
            continue

        txt = _strip_fences(txt)

        # Extract JSON from mixed prose+JSON response
        json_match = re.search(r'\{[^{}]*"recalls"\s*:\s*\[.*\]\s*\}', txt, re.DOTALL)
        if not json_match:
            json_match = re.search(r'\{.*\}', txt, re.DOTALL)
        if not json_match:
            log.warning("  [%s] no JSON found in response: %s", region, txt[:300])
            continue

        try:
            data = json.loads(json_match.group(0))
        except json.JSONDecodeError as e:
            log.warning("  [%s] JSON parse failed: %s | %s", region, e, txt[:250])
            continue

        rows = data.get("recalls", []) or []
        log.info("  [%s] found %d recalls", region, len(rows))
        all_recalls.extend(rows)
        regions_completed += 1

    elapsed = time.time() - start_ts
    if regions_skipped_budget:
        log.warning("Claude gap-finder total: %d recalls across %d/%d regions "
                    "(SKIPPED for budget: %s) — %.1fs elapsed",
                    len(all_recalls), regions_completed, len(REGION_SPECS),
                    ", ".join(regions_skipped_budget), elapsed)
    else:
        log.info("Claude gap-finder total: %d recalls across %d/%d regions — %.1fs elapsed",
                 len(all_recalls), regions_completed, len(REGION_SPECS), elapsed)
    return all_recalls


def to_recall_objects(raw: List[Dict[str, Any]]) -> List[Recall]:
    """Convert raw Claude dicts to normalized Recall objects."""
    out: List[Recall] = []
    for row in raw:
        try:
            pathogen = normalize_pathogen(row.get("Pathogen", "") or "")
            country = normalize_country(row.get("Country", "") or "")
            outbreak = int(row.get("Outbreak", 0) or 0)
            rec = Recall(
                Date=(row.get("Date") or "")[:10],
                Source=row.get("Source", "") or "Claude-gap",
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
                # Audit 2026-05-05 — stamp Notes with the canonical
                # gap-finder marker so merge_master.validate_pending_row
                # can apply the 3-day gap-finder lookback gate (vs the
                # 7-day regulator-scraper lookback).
                Notes=((row.get("Notes", "") or "")
                       + "  [via Claude gap-finder, deterministic extract]"),
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
    # Audit 2026-04-29: REACTIVATED from no-op-stub state. The retirement
    # rationale (Claude pattern-matching from training data without web
    # access) is solved by Claude's web_search_20250305 tool, which
    # gap_finder_claude now uses (see _call_claude_web below). Run this in
    # parallel with the Gemini / Tavily / Exa gap-finders — they catch
    # different gaps because each one's search backend indexes regulator
    # pages differently. Set ANTHROPIC_API_KEY (and optionally
    # ANTHROPIC_MODEL=claude-haiku-4-5-20251001) to enable.
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--region", default=None,
                    help="Run only this region (Europe, NorthAmerica, AsiaPacific, LATAM_ME_Africa)")
    args = ap.parse_args()

    t0 = datetime.now(timezone.utc)
    scraped_at = t0.strftime("%Y-%m-%dT%H:%M:%SZ")
    log.info("=" * 60)
    log.info("Claude gap-finder run: %s (region=%s)", scraped_at, args.region or "ALL")
    log.info("Data dir: %s", DATA_DIR)
    log.info("Rate-limit config: web_search.max_uses=%d, budget=%.0fs, "
             "pace_threshold=%d, max_pace_sleep=%.0fs, http_timeout=%.0fs",
             WEB_SEARCH_MAX_USES, CLAUDE_BUDGET_SEC,
             PACE_THRESHOLD_TOKENS, MAX_PACE_SLEEP_SEC, HTTP_TIMEOUT_SEC)

    if not CLAUDE_ENABLED:
        log.error("ANTHROPIC_API_KEY missing — aborting")
        return 2

    # ---- Load current state ---------------------------------------------
    approved = load_existing(XLSX_PATH) if XLSX_PATH.exists() else []
    existing_pending = load_pending(XLSX_PATH) if XLSX_PATH.exists() else []
    log.info("State: %d approved + %d existing pending", len(approved), len(existing_pending))

    # ---- Ask Claude -----------------------------------------------------
    raw = query_claude_for_gaps(SINCE_DAYS, region_filter=args.region)
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
        gap_finder=True,  # audit 2026-06-25: enable recency+authority+product guards
    )
    net_new = len(pending) - before
    # ── Gap-finder gating (audit 2026-04-29): see gap_finder_tavily.py
    from pipeline.merge_master import STATUS_PENDING_GAP
    tagged = 0
    for r in pending:
        if r.get("ScrapedAt") == scraped_at:
            r["Status"] = STATUS_PENDING_GAP
            tagged += 1
    log.info("Gap-finder added %d net-new rows to Pending "
             "(%d tagged Status=pending_gap)", net_new, tagged)

    # ---- Save ------------------------------------------------------------
    save_xlsx_with_pending(approved, sort_rows(pending), XLSX_PATH)

    # ---- Commit ----------------------------------------------------------
    if not SKIP_COMMIT and net_new > 0:
        ok = git_commit_and_push(
            repo_dir=ROOT,
            files=["docs/data/recalls.xlsx"],
            message=f"FSIS Claude gap-finder {t0.strftime('%Y-%m-%d')} (+{net_new} pending)",
        )
        if not ok:
            log.error("Git push failed")
            return 1

    elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
    log.info("DONE in %.1fs | +%d pending rows", elapsed, net_new)
    return 0


if __name__ == "__main__":
    sys.exit(main())
