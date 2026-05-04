"""
Perplexity Sonar gap-finder — midnight cascade fallback (replaces OpenAI).

Same JSON contract as gap_finder_openai.py / gap_finder_tavily.py:
  • Returns list of recall dicts in the Tavily-shaped format
  • Dedup-appends to Pending via append_to_pending()
  • Same Status=pending_gap tagging on inserted rows
  • Same git_commit_and_push hook

Architecture:
  • Routes via OpenRouter (openrouter.ai/api/v1/chat/completions) so the
    same API key + Python client also serves the dissent reviewer
    (item #2 in George's plan). One OpenRouter $10 deposit covers both.
  • Model: perplexity/sonar — basic search-enabled Perplexity model,
    flat-rate ~$1/M tokens. Web search is built into the model; no
    separate per-call search fee like OpenAI's web_search.
  • Cost: 30 runs/month × ~7K tokens ≈ €0.20/month
    (vs ~€2.76/month on gpt-4o-mini-search-preview).

Replaces the role of gap_finder_openai.py for the STANDALONE
openai-gap-finder.yml workflow (renamed → perplexity-gap-finder.yml).
The CASCADE's internal 4th-fallback still uses gap_finder_openai if
OPENAI_API_KEY is set; otherwise it skips that step.

Required env vars:
  OPENROUTER_API_KEY            — required
  GAP_SINCE_DAYS=7              — optional (default 7)
  SKIP_COMMIT=1                 — optional (don't auto git push)

Returns: 0 on success, non-zero on failure.
"""
from __future__ import annotations
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pipeline.merge_master import (  # noqa: E402
    load_existing, load_pending,
    append_to_pending, sort_rows, save_xlsx_with_pending,
    STATUS_PENDING_GAP,
)
from pipeline.commit_github import git_commit_and_push  # noqa: E402

# Reuse Tavily's deterministic post-filter + recall-builder. Same pattern
# gap_finder_openai.py uses — keeps the URL whitelist and source mapping
# logic in ONE place.
from pipeline.gap_finder_tavily import (  # noqa: E402
    results_to_recalls,
    _is_generic_url,
    _lookup_source,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("gap-finder-perplexity")

DATA_DIR = ROOT / "docs" / "data"
XLSX_PATH = DATA_DIR / "recalls.xlsx"

SINCE_DAYS = int(os.getenv("GAP_SINCE_DAYS", "7"))
SKIP_COMMIT = os.getenv("SKIP_COMMIT", "").lower() in ("1", "true", "yes")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "").strip()

# Default to perplexity/sonar (basic). Override via env to switch to
# perplexity/sonar-pro or anthropic/claude-haiku-4-5:online or
# google/gemini-2.5-flash:online without code changes.
PERPLEXITY_MODEL = os.getenv("PERPLEXITY_MODEL", "perplexity/sonar")

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"


# ─────────────────────────────────────────────────────────────────────────
# Prompt — matches gap_finder_openai.py SYSTEM_PROMPT verbatim so the
# output JSON shape stays identical and downstream code (results_to_recalls)
# doesn't need to change.
# ─────────────────────────────────────────────────────────────────────────
SYSTEM_PROMPT = (
    "You are a food-safety analyst. Search the web for FOOD RECALLS published "
    "by OFFICIAL REGULATORY AGENCIES in the last {since_days} days whose cause "
    "is a PATHOGEN, BIOLOGICAL TOXIN, MYCOTOXIN, RODENTICIDE, HEAVY METAL, or "
    "PHYSICAL HAZARD. Return ONLY recalls from the agencies' official domains "
    "(fda.gov, fsis.usda.gov, recalls-rappels.canada.ca, rappel.conso.gouv.fr, "
    "food.gov.uk, fsai.ie, foodstandards.gov.au, lebensmittelwarnung.de, "
    "blv.admin.ch, ages.at, efet.gr, etc.). Skip news outlets."
)

USER_PROMPT = """\
For each recall you find, return ONE strict-JSON object. Required fields:

- Date          : YYYY-MM-DD (regulator publication date)
- Source        : agency short name (e.g. "FDA", "RappelConso (FR)", "CFIA")
- Company       : recalling firm
- Brand         : product brand (or "—" if same as company / unbranded)
- Product       : product name and pack size
- Pathogen      : specific hazard ("Listeria monocytogenes", "Salmonella",
                  "Alternaria toxins", "Bromadiolone", etc.)
- Reason        : one-sentence cause
- Class         : recall class ("Recall", "Class I/II/III", "Public Health Alert")
- Country       : English country name
- Outbreak      : STRICT — set 1 ONLY if a specific number of confirmed/probable
                  illnesses or hospitalisations is mentioned, or "outbreak" /
                  "épidémie" / "Ausbruch" / "brote" describes THIS hazard, or
                  there are deaths attributed to it. Default 0.
- URL           : FULL deep-link to the SPECIFIC recall page on the agency's
                  domain. NEVER a Google redirect, NEVER a search-result URL,
                  NEVER an aggregator. For rappel.conso.gouv.fr, the path must
                  use the INTEGER fiche number (5 digits, currently in the
                  22000s) — NOT the year and NOT a reference slug like
                  "2026-04-0305".
- Notes         : distribution area, lot info, illness count

Return ONLY a JSON array. No prose, no markdown fences. If you find nothing,
return []. Window: last {since_days} days only.
"""


def _call_perplexity_search() -> Optional[List[Dict[str, Any]]]:
    """One Perplexity Sonar request via OpenRouter. Returns parsed list of
    recall dicts on success, None on failure. Same return contract as
    gap_finder_openai._call_openai_search() so callers can swap freely."""
    if not OPENROUTER_API_KEY:
        log.error("OPENROUTER_API_KEY not set")
        return None

    body = {
        "model": PERPLEXITY_MODEL,
        "messages": [
            {"role": "system",
             "content": SYSTEM_PROMPT.format(since_days=SINCE_DAYS)},
            {"role": "user",
             "content": USER_PROMPT.format(since_days=SINCE_DAYS)},
        ],
        "max_tokens": 4000,
        # Perplexity sonar respects this hint and returns cleaner JSON.
        # OpenRouter passes it through to the upstream provider.
        "temperature": 0.1,
    }

    try:
        r = requests.post(
            OPENROUTER_URL,
            headers={
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "Content-Type":  "application/json",
                # OpenRouter attribution headers (required by ToS — they
                # show up in your OpenRouter dashboard so you can trace
                # spend per app).
                "HTTP-Referer":  "https://advfood.tech",
                "X-Title":       "FSIS Gap-Finder",
            },
            json=body,
            timeout=120,
        )
    except requests.RequestException as e:
        log.error("OpenRouter request failed: %s", e)
        return None

    if r.status_code != 200:
        log.error("OpenRouter HTTP %d: %s", r.status_code, r.text[:300])
        return None

    try:
        choice = r.json()["choices"][0]["message"]["content"]
    except (KeyError, IndexError, ValueError) as e:
        log.error("OpenRouter response shape unexpected: %s", e)
        return None

    # Strip markdown fences if present. Perplexity Sonar sometimes wraps
    # JSON output in ```json ... ``` despite explicit instructions. Handle
    # defensively — same logic as the OpenAI path.
    txt = choice.strip()
    if txt.startswith("```"):
        txt = txt.split("```", 2)[1]
        if txt.startswith("json"):
            txt = txt[4:]
        txt = txt.rsplit("```", 1)[0]
        txt = txt.strip()

    # Sonar occasionally embeds the JSON inside an English explanation. If
    # the raw text isn't a JSON array, try to locate the first '[' and
    # last ']' and parse what's between them.
    if not txt.startswith("["):
        lbr = txt.find("[")
        rbr = txt.rfind("]")
        if lbr >= 0 and rbr > lbr:
            txt = txt[lbr:rbr + 1]

    try:
        items = json.loads(txt)
    except json.JSONDecodeError as e:
        log.error("Perplexity returned non-JSON: %s | snippet=%s",
                  e, txt[:200])
        return None

    if not isinstance(items, list):
        log.error("Perplexity returned non-list: %s",
                  type(items).__name__)
        return None

    log.info("Perplexity returned %d candidate recalls", len(items))
    return items


def main() -> int:
    t0 = datetime.now(timezone.utc)
    scraped_at = t0.strftime("%Y-%m-%dT%H:%M:%SZ")
    log.info("=" * 60)
    log.info("Perplexity gap-finder run: %s | model=%s",
             scraped_at, PERPLEXITY_MODEL)

    if not OPENROUTER_API_KEY:
        log.error("OPENROUTER_API_KEY not set — exiting")
        return 1
    if not XLSX_PATH.exists():
        log.error("recalls.xlsx not found at %s", XLSX_PATH)
        return 1

    items = _call_perplexity_search()
    if items is None:
        log.error("Perplexity call failed — cannot proceed")
        return 1

    # Convert items into Tavily-shaped dicts so results_to_recalls can
    # handle them. Same wrapping logic as gap_finder_openai.py — keeps
    # the URL whitelist and dedup behaviour consistent across providers.
    wrapped: List[Dict[str, Any]] = []
    for it in items:
        url = (it.get("URL") or "").strip()
        if not url:
            continue
        if _lookup_source(url) is None:
            continue
        if _is_generic_url(url):
            continue
        wrapped.append({
            "title": (it.get("Product") or it.get("Company") or "")[:200],
            "url":   url,
            "content": (
                f"{it.get('Reason','')} | {it.get('Notes','')} | "
                f"{it.get('Pathogen','')} | brand={it.get('Brand','')}"
            ),
            "published_date": (it.get("Date") or "")[:10],
            # Pass through the structured fields for results_to_recalls
            # to use. Reuses _openai_struct key — results_to_recalls
            # doesn't care about the source name, just the shape.
            "_openai_struct": it,
        })

    if not wrapped:
        log.info("No regulator-whitelisted candidates after filtering")
        return 0

    recalls = results_to_recalls(wrapped)
    if not recalls:
        log.info("No actionable recalls after extraction")
        return 0

    approved = load_existing(XLSX_PATH)
    pending  = load_pending(XLSX_PATH)
    new_pending = append_to_pending(
        existing_pending=pending,
        approved=approved,
        new_recalls=recalls,
        scraped_at=scraped_at,
    )
    added = len(new_pending) - len(pending)
    # Same gap-finder gating audit as Tavily/OpenAI — tag new rows so the
    # URL guardian + claude-check know to scrutinise them harder.
    tagged = 0
    for r in new_pending:
        if r.get("ScrapedAt") == scraped_at:
            r["Status"] = STATUS_PENDING_GAP
            tagged += 1
    log.info("Perplexity gap-finder: added %d new rows to Pending "
             "(%d tagged Status=pending_gap)", added, tagged)

    if added == 0:
        log.info("No new rows — already covered upstream. Exiting.")
        return 0

    save_xlsx_with_pending(
        approved_rows=sort_rows(approved),
        pending_rows=sort_rows(new_pending),
        xlsx_path=XLSX_PATH,
    )

    if not SKIP_COMMIT:
        msg = (f"Perplexity gap-finder (cascade fallback): "
               f"+{added} rows to Pending ({scraped_at})")
        git_commit_and_push(ROOT, [str(XLSX_PATH)], msg)
        log.info("Committed and pushed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
