"""
Claude Haiku 4.5 reviewer — runs ONLY on Tier-1 critical recalls to stretch $5 free credit.
Optional: enable by setting ANTHROPIC_API_KEY. If absent, this client no-ops.

Cost estimate: Haiku 4.5 ~$1/M input, $5/M output.
~50 Tier-1 rows/day × ~600 tokens = 30K tokens/day = ~$0.10/day.
$5 credit lasts ~50 days at full review.
"""
from __future__ import annotations
import os
import json
import logging
from typing import List, Dict, Any, Optional
import requests

log = logging.getLogger(__name__)

API_KEY = os.getenv("ANTHROPIC_API_KEY", "").strip()
MODEL = os.getenv("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")
TIMEOUT = 60
ENABLED = bool(API_KEY)

if not ENABLED:
    log.info("ANTHROPIC_API_KEY not set. Claude review will be skipped.")


REVIEW_PROMPT = """Review these Tier-1 critical food recalls (Listeria/STEC/Botulinum/cereulide/biotoxins).

For each row, verify:
1. Pathogen classification is correct (Tier-1 vs Tier-2)
2. Outbreak flag accuracy (illness/cases reported = 1, single detection = 0)
3. URL points to a specific recall page, not a homepage/category
4. Critical info present: Company, Product, lot/batch identifier in Notes

Return strict JSON:
{"flags": [{"row_index": <int>, "severity": "high|medium|low", "issue": "...", "suggested_fix": {...}}]}
Only flag rows with real issues. Empty array if all clean.

Rows to review:
"""


def _call_claude(prompt: str, max_tokens: int = 4000) -> Optional[str]:
    if not ENABLED:
        return None
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": MODEL,
                "max_tokens": max_tokens,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.1,
            },
            timeout=TIMEOUT,
        )
        if r.status_code != 200:
            log.warning("Claude %d: %s", r.status_code, r.text[:200])
            return None
        content = r.json().get("content", [])
        if content and content[0].get("type") == "text":
            return content[0]["text"]
        return None
    except Exception as e:
        log.warning("Claude call failed: %s", e)
        return None


def review_tier1(rows: List[Dict[str, Any]], batch_size: int = 15) -> List[Dict[str, Any]]:
    """Review only Tier-1 (critical) rows to stretch $5 credit."""
    if not ENABLED:
        return []
    tier1_rows = [(i, r) for i, r in enumerate(rows) if int(r.get("Tier", 2)) == 1]
    if not tier1_rows:
        log.info("Claude review: no Tier-1 rows to review")
        return []
    log.info("Claude review: %d Tier-1 rows", len(tier1_rows))

    all_flags: List[Dict[str, Any]] = []
    for batch_start in range(0, len(tier1_rows), batch_size):
        chunk = tier1_rows[batch_start:batch_start + batch_size]
        chunk_data = [{"row_index": idx, **row} for idx, row in chunk]
        prompt = REVIEW_PROMPT + json.dumps(chunk_data, ensure_ascii=False, indent=2)
        txt = _call_claude(prompt)
        if not txt:
            continue
        # Strip markdown fences if present
        txt = txt.strip()
        if txt.startswith("```"):
            txt = txt.split("```")[1]
            if txt.startswith("json"):
                txt = txt[4:].strip()
        try:
            data = json.loads(txt)
            all_flags.extend(data.get("flags", []))
        except json.JSONDecodeError as e:
            log.warning("Claude JSON parse failed: %s | text=%s", e, txt[:200])
    log.info("Claude review: %d flags raised", len(all_flags))
    return all_flags
