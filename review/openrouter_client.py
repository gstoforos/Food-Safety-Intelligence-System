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
from typing import Optional, Dict, Any, List
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
def _strip_fences(txt: str) -> str:
    """Strip ```json ... ``` fences from a model response."""
    if not txt:
        return txt
    s = txt.strip()
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


# ===========================================================================
# Reasoning-model artifact handling (DeepSeek R1, QwQ, etc.)
# ===========================================================================
# Free reasoning models return their chain-of-thought wrapped in <think>...
# </think> tags before the actual JSON answer. Some models also expose
# reasoning via a separate `reasoning` field in the response, in which
# case `content` is already clean. The Claude API has no equivalent so
# claude_client doesn't strip these — but for OpenRouter we must, or
# json.loads chokes on the <think> prefix.
import re

_THINK_RE = re.compile(r"<think>.*?</think>", re.DOTALL | re.IGNORECASE)


def _strip_thinking(text: str) -> str:
    """Remove <think>...</think> blocks emitted by reasoning models.

    DeepSeek R1, QwQ, and other reasoning models prepend chain-of-thought
    inside <think> tags. The actual JSON answer follows. Strip the
    thinking before any json.loads attempt.

    Idempotent and safe on text without thinking blocks.
    """
    if not text:
        return text
    cleaned = _THINK_RE.sub("", text).strip()
    # Some R1 variants emit "<think>" without a closing tag if max_tokens
    # truncates mid-reasoning — drop everything up to the last newline
    # before any '{' or '[' as a defensive recovery.
    if "<think>" in cleaned.lower():
        for opener in ("{", "["):
            idx = cleaned.find(opener)
            if idx > 0:
                cleaned = cleaned[idx:].strip()
                break
    return cleaned


def _clean_response(text: str) -> str:
    """Combined cleaner: strip JSON fences, then thinking blocks.

    Use this on every OpenRouter response before json.loads. Mirrors
    _strip_fences()'s role in claude_client.py but adds reasoning-model
    handling that Anthropic's API doesn't need.

    Order matters: fences are the OUTER wrapper (markdown emitted by the
    model), <think> blocks live INSIDE the wrapped content. Strip outer
    first so the inner-block recovery logic sees clean JSON boundaries.
    """
    return _strip_thinking(_strip_fences(text))


# ===========================================================================
# Identical-prompt parity with claude_client
# ===========================================================================
# Importing the prompt constants from claude_client guarantees the
# OpenRouter reviewer asks exactly the same questions as the Claude
# reviewer. Single source of truth — if you tune REVIEW_BATCH_SYSTEM
# in claude_client, the OpenRouter dissent reviewer picks up the change
# automatically. Safe to import even when ANTHROPIC_API_KEY is unset
# (claude_client only no-ops API calls; module-level constants load).
try:
    from review.claude_client import (
        REVIEW_PROMPT,
        REVIEW_BATCH_SYSTEM,
        EXTRACTION_SYSTEM,
        EXTRACTION_PROMPT,
    )
except ImportError:
    # Defensive fallback: claude_client.py somehow not importable.
    # Leave the constants undefined; review_tier1 / review_batch /
    # extract_recalls_from_html will raise NameError loudly so the
    # operator notices the mis-deploy rather than silently using
    # divergent prompts.
    log.error("review.claude_client not importable — OpenRouter reviewer "
              "cannot run with prompt parity. Fix the import path.")
    raise


# ===========================================================================
# Mode 1 — Tier-1 reviewer (mirrors claude_client.review_tier1)
# ===========================================================================
def review_tier1(rows: List[Dict[str, Any]],
                 batch_size: int = 15) -> List[Dict[str, Any]]:
    """Tier-1-only reviewer using OpenRouter as backend.

    Behaviorally identical to claude_client.review_tier1 — same prompt,
    same input filter (Tier==1 only), same output schema. Different
    backend so this can serve as an independent dissent reviewer in the
    morning Claude / evening OpenRouter dual-pass setup.
    """
    if not ENABLED:
        return []
    tier1_rows = [(i, r) for i, r in enumerate(rows)
                  if int(r.get("Tier", 2) or 2) == 1]
    if not tier1_rows:
        log.info("OpenRouter review: no Tier-1 rows to review")
        return []
    log.info("OpenRouter review: %d Tier-1 rows (model=%s)",
             len(tier1_rows), MODEL)

    all_flags: List[Dict[str, Any]] = []
    for batch_start in range(0, len(tier1_rows), batch_size):
        chunk = tier1_rows[batch_start:batch_start + batch_size]
        chunk_data = [{"row_index": idx, **row} for idx, row in chunk]
        prompt = REVIEW_PROMPT + json.dumps(chunk_data, ensure_ascii=False, indent=2)
        txt = _call_openrouter(prompt)
        if not txt:
            continue
        try:
            data = json.loads(_clean_response(txt))
            all_flags.extend(data.get("flags", []))
        except json.JSONDecodeError as e:
            log.warning("OpenRouter review JSON parse failed: %s | text=%s",
                        e, txt[:200])
    log.info("OpenRouter review: %d flags raised", len(all_flags))
    return all_flags


# ===========================================================================
# Mode 1b — Full-batch reviewer (mirrors claude_client.review_batch)
# ===========================================================================
def review_batch(rows: List[Dict[str, Any]],
                 batch_size: int = 20) -> List[Dict[str, Any]]:
    """Full review pass (all rows, not just Tier-1).

    Returns the same {row_index, issues, suggested_fixes, confidence}
    schema as claude_client.review_batch, so compute_rejections() in
    pipeline/run_all.py can swap reviewers without code changes.

    Use this as the evening dissent reviewer running after the morning
    Claude review — independent backend, identical prompt, catches the
    same class of issues if the Claude reviewer hallucinates a verdict.
    """
    if not ENABLED:
        return []
    all_reviews: List[Dict[str, Any]] = []
    for i in range(0, len(rows), batch_size):
        chunk = rows[i:i + batch_size]
        prompt = (
            f"Review these {len(chunk)} food recall records (row_index "
            f"relative to this batch starting at 0):\n\n"
            + json.dumps(chunk, ensure_ascii=False, indent=2)
        )
        txt = _call_openrouter(prompt, system=REVIEW_BATCH_SYSTEM)
        if not txt:
            continue
        try:
            data = json.loads(_clean_response(txt))
            for rev in data.get("reviews", []):
                # Adjust row_index back to global index — same convention
                # as claude_client.review_batch and the retired openai_review.
                rev["row_index"] = rev.get("row_index", 0) + i
                all_reviews.append(rev)
        except json.JSONDecodeError as e:
            log.warning("OpenRouter review_batch JSON parse failed: %s | text=%s",
                        e, txt[:200])
    log.info("OpenRouter review_batch: %d issues across %d rows",
             len(all_reviews), len(rows))
    return all_reviews


# ===========================================================================
# Mode 2 — HTML extraction fallback (mirrors claude_client.extract_recalls_from_html)
# ===========================================================================
def extract_recalls_from_html(
    html: str,
    source_url: str,
    agency: str,
    country: str,
    language: str = "en",
    extra_hints: str = "",
    since_days: int = 30,
    max_tokens: int = 4000,
) -> List[Dict[str, Any]]:
    """Extract structured recalls from regulator HTML using OpenRouter.

    Behavioral mirror of claude_client.extract_recalls_from_html — same
    prompt, same return shape. Use as the second-tier fallback when both
    Gemini AND Claude fail on a difficult page (e.g. Anthropic API
    outage), or as a free-tier-first cost-saver for routine pages.
    """
    if not ENABLED:
        return []
    prompt = EXTRACTION_PROMPT.format(
        html=html[:120_000],       # match Gemini/Claude cap
        source_url=source_url,
        agency=agency,
        country=country,
        language=language,
        extra_hints=extra_hints or "",
        since_days=since_days,
    )
    txt = _call_openrouter(prompt, max_tokens=max_tokens, system=EXTRACTION_SYSTEM)
    if not txt:
        return []
    try:
        data = json.loads(_clean_response(txt))
        recalls = data.get("recalls", []) or []
        if recalls:
            log.info("OpenRouter fallback extracted %d recalls from %s",
                     len(recalls), source_url)
        return recalls
    except json.JSONDecodeError as e:
        log.warning("OpenRouter extract JSON parse failed: %s | text=%s",
                    e, txt[:200])
        return []
