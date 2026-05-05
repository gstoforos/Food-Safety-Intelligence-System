"""
OpenRouter client — provides a Claude-compatible interface for FSIS reviewers.

Mirrors review/claude_client.py so existing code can use OpenRouter as a
drop-in dissent / fallback reviewer. Routes through OpenRouter's unified
API which gives access to DeepSeek, Llama, Qwen, and other open models.

PRIMARY USE CASE — second-pass evening review:
  After the morning gemini-url-gate (06:00) + claude-check (07:00) chain,
  an evening 19:00 gemini-url-gate + 20:00 openrouter-check chain provides
  independent verification on rows added by the 18:00 daily-scrape.

Two reviewers > one reviewer for catching subtle data-quality bugs.

MODEL ROUTING:
  Default: deepseek/deepseek-r1:free       (FREE — strongest reasoning available)
  Fallback if :free unavailable: deepseek/deepseek-chat-v3.1 (~$0.14/$0.28/M)

  R1 chosen as default because it beats DeepSeek Chat on multilingual
  extraction (your French RappelConso pages) and hybrid Tier framework
  reasoning. Slower than Chat (~30-60s/row vs ~10-15s), but with the
  $10 OpenRouter deposit, free-tier access is stable at 1000 req/day —
  way above your 30 row/day usage.

  Override via OPENROUTER_MODEL env var. Common alternatives:
    - meta-llama/llama-3.3-70b-instruct:free
    - qwen/qwen-2.5-72b-instruct:free
    - deepseek/deepseek-r1:free  (slow but strong reasoning)

Cost expectation:
  - Free path: $0/run
  - Paid worst case (DeepSeek Chat): ~$0.0006/row × 30 rows/day × 30 days = $0.50/mo
  - On both paths, we explicitly request the :free model first; if OpenRouter
    returns 402 (insufficient quota) or 429 (rate limit), the caller can
    retry with a paid alternative.

Enable by setting OPENROUTER_API_KEY. If absent, calls no-op (return None).
"""
from __future__ import annotations
import os
import json
import logging
from typing import Optional, Dict, Any
import requests

log = logging.getLogger(__name__)

API_KEY = os.getenv("OPENROUTER_API_KEY", "").strip()
MODEL = os.getenv("OPENROUTER_MODEL", "deepseek/deepseek-r1:free")
FALLBACK_MODEL = os.getenv("OPENROUTER_FALLBACK_MODEL", "deepseek/deepseek-chat-v3.1")
TIMEOUT = 180  # R1 reasoning model can take 30-60s per row
ENABLED = bool(API_KEY)

API_URL = "https://openrouter.ai/api/v1/chat/completions"

# Required headers per https://openrouter.ai/docs#requests
HEADERS_BASE = {
    "Content-Type": "application/json",
    # OpenRouter uses these for ranking / abuse prevention; they're optional
    # but recommended.
    "HTTP-Referer": "https://advfood.tech/food-safety-intelligence",
    "X-Title": "AFTS FSIS",
}

if not ENABLED:
    log.info("OPENROUTER_API_KEY not set. OpenRouter review will be skipped.")


# ===========================================================================
# Low-level call — Claude-compatible signature
# ===========================================================================
def _call_openrouter(prompt: str, max_tokens: int = 4000,
                      system: Optional[str] = None,
                      model: Optional[str] = None) -> Optional[str]:
    """
    Single OpenRouter chat completion. Returns text content, or None on
    failure.

    Mirrors _call_claude() in review/claude_client.py for drop-in use.

    The :free models can become temporarily unavailable (rate limits,
    rotated out). On 402/429, retries with FALLBACK_MODEL automatically.
    """
    if not ENABLED:
        return None

    use_model = model or MODEL
    body = {
        "model": use_model,
        "max_tokens": max_tokens,
        "temperature": 0.1,
        "messages": [],
    }
    if system:
        body["messages"].append({"role": "system", "content": system})
    body["messages"].append({"role": "user", "content": prompt})

    headers = dict(HEADERS_BASE)
    headers["Authorization"] = f"Bearer {API_KEY}"

    try:
        r = requests.post(API_URL, headers=headers, json=body, timeout=TIMEOUT)
    except requests.RequestException as e:
        log.warning("OpenRouter network error: %s", e)
        return None

    # Handle :free quota / rate-limit errors — auto-retry with paid fallback
    if r.status_code in (402, 429) and use_model.endswith(":free"):
        log.info("OpenRouter %s returned %d — retrying with %s",
                 use_model, r.status_code, FALLBACK_MODEL)
        return _call_openrouter(prompt, max_tokens=max_tokens,
                                system=system, model=FALLBACK_MODEL)

    if r.status_code >= 400:
        log.warning("OpenRouter HTTP %d: %s", r.status_code, r.text[:300])
        return None

    try:
        data = r.json()
    except ValueError:
        log.warning("OpenRouter returned non-JSON: %s", r.text[:200])
        return None

    # OpenAI-compatible response shape:
    #   { "choices": [ { "message": { "content": "..." } } ] }
    try:
        content = data["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError):
        log.warning("OpenRouter response missing content: %s", str(data)[:300])
        return None

    if not isinstance(content, str) or not content.strip():
        log.warning("OpenRouter returned empty content")
        return None

    return content


# ===========================================================================
# Helpers — same as Claude client
# ===========================================================================
def _strip_fences(text: str) -> str:
    """Strip ```json ... ``` fences from a model response."""
    if not text:
        return text
    s = text.strip()
    if s.startswith("```"):
        # Drop the leading fence line
        lines = s.split("\n")
        if lines:
            lines = lines[1:]
        # Drop the trailing fence line
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        s = "\n".join(lines).strip()
    return s
