"""
OpenAI gap-finder — final cascade fallback.

Used by pipeline/gap_finder_cascade.py as the last step after Gemini FREE,
Gemini PAID, and Claude have all failed. Same JSON contract as the other
gap-finders (returns list of recall dicts, dedup-appends to Pending),
different search backend.

Cost: ~$0.10 / call on gpt-4o-mini-search-preview. Only fires when 3
upstream providers have already failed in the same cron slot.

Required env vars:
  OPENAI_API_KEY                — required
  GAP_SINCE_DAYS=7              — optional (default 7)
  SKIP_COMMIT=1                 — optional (don't auto git push)

Returns: 0 on success, non-zero on failure (cascade then logs and exits).
"""
from __future__ import annotations
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pipeline.merge_master import (  # noqa: E402
    load_existing, load_pending,
    append_to_pending, sort_rows, save_xlsx_with_pending,
)
from pipeline.commit_github import git_commit_and_push  # noqa: E402

# Reuse Tavily's deterministic post-filter + recall-builder. They live
# next to this file in the pipeline/ package and depend only on local
# helpers — no Tavily-specific HTTP calls.
from pipeline.gap_finder_tavily import (  # noqa: E402
    results_to_recalls,
    _is_generic_url,
    _lookup_source,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("gap-finder-openai")

DATA_DIR = ROOT / "docs" / "data"
XLSX_PATH = DATA_DIR / "recalls.xlsx"

SINCE_DAYS = int(os.getenv("GAP_SINCE_DAYS", "7"))
SKIP_COMMIT = os.getenv("SKIP_COMMIT", "").lower() in ("1", "true", "yes")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_GAP_MODEL", "gpt-4o-mini-search-preview")

OPENAI_URL = "https://api.openai.com/v1/chat/completions"


# ─────────────────────────────────────────────────────────────────────────
# Prompt — minimal, mirrors the Gemini contract
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


def _call_openai_search() -> Optional[List[Dict[str, Any]]]:
    """One OpenAI request with web-search-enabled model. Returns parsed list
    of recall dicts on success, None on failure."""
    if not OPENAI_API_KEY:
        log.error("OPENAI_API_KEY not set")
        return None

    body = {
        "model": OPENAI_MODEL,
        "messages": [
            {"role": "system",
             "content": SYSTEM_PROMPT.format(since_days=SINCE_DAYS)},
            {"role": "user",
             "content": USER_PROMPT.format(since_days=SINCE_DAYS)},
        ],
        "max_tokens": 4000,
    }

    try:
        r = requests.post(
            OPENAI_URL,
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json=body,
            timeout=120,
        )
    except requests.RequestException as e:
        log.error("OpenAI request failed: %s", e)
        return None

    if r.status_code != 200:
        log.error("OpenAI HTTP %d: %s", r.status_code, r.text[:300])
        return None

    try:
        choice = r.json()["choices"][0]["message"]["content"]
    except (KeyError, IndexError, ValueError) as e:
        log.error("OpenAI response shape unexpected: %s", e)
        return None

    # Strip markdown fences if present (defensive — newer models usually skip them)
    txt = choice.strip()
    if txt.startswith("```"):
        txt = txt.split("```", 2)[1]
        if txt.startswith("json"):
            txt = txt[4:]
        txt = txt.rsplit("```", 1)[0]
        txt = txt.strip()

    try:
        items = json.loads(txt)
    except json.JSONDecodeError as e:
        log.error("OpenAI returned non-JSON: %s | snippet=%s",
                  e, txt[:200])
        return None

    if not isinstance(items, list):
        log.error("OpenAI returned non-list: %s", type(items).__name__)
        return None

    log.info("OpenAI returned %d candidate recalls", len(items))
    return items


def main() -> int:
    t0 = datetime.now(timezone.utc)
    scraped_at = t0.strftime("%Y-%m-%dT%H:%M:%SZ")
    log.info("=" * 60)
    log.info("OpenAI gap-finder run: %s | model=%s",
             scraped_at, OPENAI_MODEL)

    if not OPENAI_API_KEY:
        log.error("OPENAI_API_KEY not set — exiting")
        return 1
    if not XLSX_PATH.exists():
        log.error("recalls.xlsx not found at %s", XLSX_PATH)
        return 1

    items = _call_openai_search()
    if items is None:
        log.error("OpenAI call failed — cannot proceed")
        return 1

    # Convert OpenAI items into Tavily-shaped dicts so results_to_recalls
    # can handle them. OpenAI items already have most of the fields we need;
    # we just assemble a synthetic "title/url/content" wrapper.
    wrapped: List[Dict[str, Any]] = []
    for it in items:
        url = (it.get("URL") or "").strip()
        if not url:
            continue
        if _lookup_source(url) is None:
            continue
        if _is_generic_url(url):
            continue
        # Synthesise a Tavily-shaped record
        wrapped.append({
            "title": (it.get("Product") or it.get("Company") or "")[:200],
            "url":   url,
            "content": (
                f"{it.get('Reason','')} | {it.get('Notes','')} | "
                f"{it.get('Pathogen','')} | brand={it.get('Brand','')}"
            ),
            "published_date": (it.get("Date") or "")[:10],
            # Pass through the structured fields for results_to_recalls to use
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
        gap_finder=True,  # audit 2026-06-25: enable recency+authority+product guards
    )
    added = len(new_pending) - len(pending)
    # ── Gap-finder gating (audit 2026-04-29): see gap_finder_tavily.py
    from pipeline.merge_master import STATUS_PENDING_GAP
    tagged = 0
    for r in new_pending:
        if r.get("ScrapedAt") == scraped_at:
            r["Status"] = STATUS_PENDING_GAP
            tagged += 1
    log.info("OpenAI gap-finder: added %d new rows to Pending "
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
        msg = (f"OpenAI gap-finder (cascade fallback): "
               f"+{added} rows to Pending ({scraped_at})")
        git_commit_and_push(ROOT, [str(XLSX_PATH)], msg)
        log.info("Committed and pushed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
