"""
FSIS Agent – Gemini AI Enricher
Classifies pathogen, confidence, risk summary, affected population.
Uses Gemini 2.0 Flash (free tier: 1,500 req/day).
"""
import os
import time
import json
import logging

import requests

from config import GEMINI_API_URL, GEMINI_BATCH_DELAY, GEMINI_MAX_RETRIES, PATHOGENS
from database import get_unenriched, update_enrichment

log = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a food safety expert AI. Given an FDA food recall record,
extract the following as JSON only (no markdown, no preamble):

{
  "pathogen": "<primary pathogen name, or 'Unknown' if unclear>",
  "pathogen_confidence": "<HIGH|MEDIUM|LOW>",
  "risk_summary": "<one sentence, max 20 words, plain English>",
  "affected_population": "<comma-separated vulnerable groups, or 'General public'>"
}

Pathogen must be one of: """ + ", ".join(PATHOGENS) + """, or 'Unknown'.
Respond ONLY with the JSON object."""


def _call_gemini(recall: dict) -> dict | None:
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        raise EnvironmentError("GEMINI_API_KEY not set")

    prompt = f"""Product: {recall['product']}
Reason for recall: {recall['reason']}
Company: {recall['firm']}
Classification: {recall['classification']}"""

    payload = {
        "contents": [
            {
                "parts": [
                    {"text": SYSTEM_PROMPT + "\n\n" + prompt}
                ]
            }
        ],
        "generationConfig": {
            "temperature": 0.1,
            "maxOutputTokens": 200,
        }
    }

    url = f"{GEMINI_API_URL}?key={api_key}"

    for attempt in range(1, GEMINI_MAX_RETRIES + 1):
        try:
            resp = requests.post(url, json=payload, timeout=30)
        except requests.RequestException as e:
            log.warning(f"Gemini request error (attempt {attempt}): {e}")
            time.sleep(5 * attempt)
            continue

        if resp.status_code == 429:
            wait = 60 * attempt
            log.warning(f"Gemini rate limit – sleeping {wait}s")
            time.sleep(wait)
            continue

        if resp.status_code != 200:
            log.error(f"Gemini HTTP {resp.status_code}: {resp.text[:200]}")
            return None

        try:
            text = resp.json()["candidates"][0]["content"]["parts"][0]["text"]
            text = text.strip().strip("```json").strip("```").strip()
            return json.loads(text)
        except (KeyError, json.JSONDecodeError) as e:
            log.warning(f"Gemini parse error: {e} | raw: {resp.text[:300]}")
            return None

    return None


def enrich_batch(limit: int = 100):
    """Enrich up to `limit` un-enriched recalls."""
    records = get_unenriched(limit)
    if not records:
        log.info("No unenriched records.")
        return

    log.info(f"Enriching {len(records)} records via Gemini...")
    ok = 0
    fail = 0

    for i, rec in enumerate(records, 1):
        result = _call_gemini(rec)

        if result:
            update_enrichment(
                recall_id          = rec["id"],
                pathogen           = result.get("pathogen", "Unknown"),
                confidence         = result.get("pathogen_confidence", "LOW"),
                risk_summary       = result.get("risk_summary", ""),
                affected_population= result.get("affected_population", "General public"),
            )
            ok += 1
            log.debug(f"  [{i}/{len(records)}] {rec['id']} → {result.get('pathogen')}")
        else:
            # Fallback: keyword scan from reason text
            detected = _keyword_fallback(rec["reason"])
            update_enrichment(
                recall_id          = rec["id"],
                pathogen           = detected,
                confidence         = "LOW",
                risk_summary       = "Pathogen detected via keyword scan.",
                affected_population= "General public",
            )
            fail += 1
            log.warning(f"  [{i}/{len(records)}] {rec['id']} – fallback to keyword ({detected})")

        if i < len(records):
            time.sleep(GEMINI_BATCH_DELAY)

    log.info(f"Enrichment complete: {ok} AI-enriched, {fail} keyword-fallback.")


def _keyword_fallback(reason: str) -> str:
    r = reason.lower()
    for p in PATHOGENS:
        if p.lower() in r:
            return p
    return "Unknown"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    enrich_batch(100)
