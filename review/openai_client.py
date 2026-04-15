"""
OpenAI client for review pass — paid (gpt-4o-mini, ~$0.005/recall).
Reviews scraped + enriched rows for:
  - URL validity (matches description, not generic landing)
  - Missing required fields
  - Pathogen taxonomy consistency
  - Suggested corrections
"""
from __future__ import annotations
import os
import json
import logging
from typing import List, Dict, Any, Optional
import requests

log = logging.getLogger(__name__)

API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
TIMEOUT = 60
ENABLED = bool(API_KEY)

if not ENABLED:
    log.warning("OPENAI_API_KEY not set. OpenAI review will be skipped.")


REVIEW_SYSTEM = """You are a senior food safety analyst reviewing recall records for accuracy.
For each record, flag issues:
  - URL_INVALID: URL is a homepage, category page, or generic landing (not a specific recall)
  - URL_MISMATCH: URL likely doesn't match the described recall
  - MISSING_FIELD: required field empty (Date, Company, Product, Pathogen, URL)
  - PATHOGEN_INCONSISTENT: pathogen name doesn't match Reason text
  - DATE_FORMAT: date not in YYYY-MM-DD or seems wrong
  - COUNTRY_INCONSISTENT: Country doesn't match Source agency
  - DUPLICATE_RISK: looks like a duplicate of another recent recall

Return strictly: {"reviews": [{"row_index": <int>, "issues": ["CODE",...], "suggested_fixes": {"FieldName": "value"}, "confidence": 0.0-1.0}]}
Only include rows with issues. Empty array if all clean."""


def _call_openai(prompt: str, system: str = REVIEW_SYSTEM, max_tokens: int = 4000) -> Optional[str]:
    if not ENABLED:
        return None
    try:
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": MODEL,
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.1,
                "max_tokens": max_tokens,
                "response_format": {"type": "json_object"},
            },
            timeout=TIMEOUT,
        )
        if r.status_code != 200:
            log.warning("OpenAI %d: %s", r.status_code, r.text[:200])
            return None
        return r.json()["choices"][0]["message"]["content"]
    except Exception as e:
        log.warning("OpenAI call failed: %s", e)
        return None


def review_batch(rows: List[Dict[str, Any]], batch_size: int = 20) -> List[Dict[str, Any]]:
    """Review rows in batches. Returns list of review issues per row."""
    if not ENABLED:
        return []
    all_reviews: List[Dict[str, Any]] = []
    for i in range(0, len(rows), batch_size):
        chunk = rows[i:i + batch_size]
        prompt = (
            f"Review these {len(chunk)} food recall records (row_index relative to this batch starting at 0):\n\n"
            + json.dumps(chunk, ensure_ascii=False, indent=2)
        )
        txt = _call_openai(prompt)
        if not txt:
            continue
        try:
            data = json.loads(txt)
            for rev in data.get("reviews", []):
                # Adjust row_index back to global index
                rev["row_index"] = rev.get("row_index", 0) + i
                all_reviews.append(rev)
        except json.JSONDecodeError as e:
            log.warning("OpenAI JSON parse failed: %s", e)
    log.info("OpenAI review: %d issues across %d rows", len(all_reviews), len(rows))
    return all_reviews


def find_missing_recalls(country: str, agency: str, since_days: int = 7) -> List[Dict[str, Any]]:
    """
    Ask OpenAI: 'What pathogen recalls happened in {country} in the last {N} days
    that we may have missed?' — returns suggestions for gap_finder.
    """
    if not ENABLED:
        return []
    prompt = f"""List all known food pathogen recalls (Listeria, Salmonella, E. coli/STEC, Botulinum, cereulide,
Hepatitis A, Norovirus, Campylobacter, Cyclospora, Vibrio, Cronobacter, biotoxins, mycotoxins) issued by {agency}
in {country} in the last {since_days} days.

For each, provide: date, company, product, pathogen, source URL (specific recall page, not homepage).
Return strict JSON: {{"recalls": [{{"Date":"YYYY-MM-DD","Company":"...","Product":"...","Pathogen":"...","URL":"..."}}]}}
If you cannot verify any, return {{"recalls": []}}."""
    txt = _call_openai(prompt, system="You are a food safety analyst with knowledge of recent recalls.")
    if not txt:
        return []
    try:
        return json.loads(txt).get("recalls", [])
    except json.JSONDecodeError:
        return []
