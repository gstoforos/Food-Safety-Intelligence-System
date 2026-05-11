"""
Gemini reviewer — direct Google API, drop-in substitute for OpenRouter.

Mirrors review/openrouter_client.py exactly so pipeline/gemini_check.py
can swap reviewers without changing any review logic. Imports the same
prompts from review.claude_client to guarantee that the Gemini dissent
reviewer asks exactly the same questions as the Claude reviewer —
single source of truth for prompts, single point of tuning.

WHY THIS MODULE EXISTS — MAY 2026 OPENROUTER FAILURE
=====================================================
For ~2 weeks (late April through May 10) the OpenRouter dissent reviewer
returned HTTP 404 on every call because the configured DeepSeek free-
tier model (deepseek/deepseek-chat-v3.1:free) was retired in the V4
transition. Every Pending row SKIPped at the reviewer stage → 0
promotions to Recalls regardless of how many real recalls were found.

Replacing OpenRouter with direct Gemini API gives:
  • Stable model strings. Google doesn't rotate gemini-2.5-flash out
    of the free tier on short notice.
  • Native JSON enforcement via responseMimeType="application/json".
    The model is forced to emit valid JSON at the generation step —
    no more json.JSONDecodeError from chatty preambles.
  • Reuses the existing 5-key rotation pool (GEMINI_API_KEY_1..5)
    already configured in GitHub Actions secrets. No new keys, no
    new dependencies, no new free-tier accounts.
  • 1500 req/day per key × 5 keys = 7500/day capacity. The reviewer
    uses ~50 calls/day so we're at ~0.7% of capacity. The same key
    pool is shared with scrapers (url_gate_gemini, gap_finder_gemini,
    sunday_gemini_qa, generic Gemini scrapers) but total daily usage
    is still well under the pool limit.

PROMPT PARITY (critical)
========================
All prompts (REVIEW_PROMPT, REVIEW_BATCH_SYSTEM, EXTRACTION_PROMPT,
EXTRACTION_SYSTEM) and the row-level CLAUDE_CHECK_PROMPT /
CLAUDE_CHECK_SYSTEM are imported from claude_client / claude_check.
This module never defines its own prompts. Tune claude_check.py and
this reviewer picks up the change on the next run.

MODEL ROUTING
=============
  Primary:  gemini-2.5-flash        (strong reasoning, ~250 RPD per project
                                     on free tier as of May 2026)
  Fallback: gemini-2.5-flash-lite   (highest free-tier throughput,
                                     ~1000 RPD per project, lower latency)

  IMPORTANT — gemini-2.0-flash was RETIRED by Google on 2026-03-03 and
  must NOT be used as a fallback. The original draft of this module
  defaulted to gemini-2.0-flash and would have failed on first
  fallback engagement (same failure mode that killed the OpenRouter
  DeepSeek setup). Fixed before production deploy.

  Override via env:
    GEMINI_REVIEWER_MODEL          — primary
    GEMINI_REVIEWER_FALLBACK_MODEL — fallback

  Both models support responseMimeType="application/json" and system
  instructions, which the review prompts rely on.

KEY ROTATION
============
Reuses the existing FREE-tier key pool exclusively:
  GEMINI_API_KEY_FREE    — primary free-tier key
  GEMINI_API_KEY_FREE_2  — secondary free-tier key
  GEMINI_REVIEWER_KEY    — optional override (single dedicated key for isolation)

DELIBERATELY EXCLUDED:
  GEMINI_API_KEY         — this is your paid key, not used by the reviewer.
                           Adding it here would silently route reviewer load
                           onto the paid quota when free keys throttle — the
                           opposite of the cost-defensive posture this module
                           is built for.

On HTTP 429 (rate limit) the key is marked blocked for this process and
the next call rotates to another. When all FREE keys are blocked for the
primary model, we drop to the fallback model (which has its own per-key
quota, separate from the primary's quota — Google's free tier is
model-specific, not account-wide). If both models exhaust their free key
pools simultaneously, calls return None and rows SKIP for retry next
run — same contract as the OpenRouter client had.

REASONING-MODEL ARTIFACT HANDLING
=================================
Gemini 2.5 Flash and 2.0 Flash do NOT emit <think>...</think> tags
by default. The _strip_thinking helper is kept for defensive parity
with openrouter_client (idempotent and harmless on non-reasoning
output). Future Gemini reasoning models or third-party overrides via
GEMINI_REVIEWER_MODEL env var will continue to parse cleanly.

ENABLE
======
Set at least one of GEMINI_API_KEY_FREE or GEMINI_API_KEY_FREE_2 (or
the optional override GEMINI_REVIEWER_KEY). If no free key is
configured, ENABLED=False and all calls return None — pipeline
treats this as fetch-failure and SKIPs the row (same behavior as the
OpenRouter client did).

NOTE: the paid GEMINI_API_KEY is deliberately ignored — see KEY
ROTATION section above for the rationale.
"""
from __future__ import annotations
import os
import re
import json
import logging
from typing import Optional, Dict, Any, List
from itertools import cycle
from threading import Lock
import requests

log = logging.getLogger(__name__)


# ============================================================================
# Model configuration
# ============================================================================
MODEL = os.getenv("GEMINI_REVIEWER_MODEL", "gemini-2.5-flash")
# Fallback model selection note (2026-05-11):
#   gemini-2.0-flash was retired by Google on 2026-03-03 and would
#   return errors if used as a fallback (exactly the failure mode that
#   killed the OpenRouter DeepSeek setup). The current free-tier
#   workhorse is gemini-2.5-flash-lite — higher RPD quota (~1000/day
#   per project vs full Flash's ~250/day) and lower latency than full
#   Flash, at the cost of slightly weaker reasoning. For binary
#   reviewer verdicts on a single recall row, Flash-Lite is more than
#   adequate.
FALLBACK_MODEL = os.getenv("GEMINI_REVIEWER_FALLBACK_MODEL", "gemini-2.5-flash-lite")
TIMEOUT = 60  # Gemini Flash typically responds in 2-8s; 60s is generous


# ============================================================================
# Key rotation pool (mirrors pipeline/gemini_client.py pattern)
# ============================================================================
def _collect_keys() -> List[str]:
    """Build the free-tier key rotation pool.

    Priority order:
      1. GEMINI_REVIEWER_KEY — explicit override, isolates the reviewer
         on a single dedicated key (useful for debugging quota issues).
      2. GEMINI_API_KEY_FREE + GEMINI_API_KEY_FREE_2 — the standard
         two-key free-tier rotation pool (matches the secrets configured
         in the FSIS GitHub Actions repo as of 2026-05-11).

    DELIBERATELY does NOT read GEMINI_API_KEY (paid). If both free keys
    are exhausted, calls return None — the reviewer SKIPs rows for retry
    next run rather than silently spilling load onto the paid key. This
    keeps the reviewer's $0/month cost guarantee intact regardless of
    how heavily other pipeline stages are using the paid key.

    Add a third free key (e.g. GEMINI_API_KEY_FREE_3) to the OS env and
    extend the loop below to pick it up — no other changes needed.
    """
    # Explicit override path: single dedicated key for full isolation
    override = os.getenv("GEMINI_REVIEWER_KEY")
    if override:
        return [override.strip()]

    keys: List[str] = []
    for name in ("GEMINI_API_KEY_FREE", "GEMINI_API_KEY_FREE_2"):
        v = os.getenv(name)
        if v:
            keys.append(v.strip())
    return keys


_keys = _collect_keys()
ENABLED = bool(_keys)

if ENABLED:
    _key_cycle = cycle(_keys)
    log.info("Gemini reviewer: loaded %d key(s) for rotation", len(_keys))
else:
    _key_cycle = None
    log.info("GEMINI_API_KEY* not set. Gemini reviewer will be skipped.")

_lock = Lock()
_blocked_keys_per_model: Dict[str, set] = {}  # model -> set of blocked keys


def _next_key(model: str) -> Optional[str]:
    """Get next available key for `model` (round-robin, skipping blocked).

    Block tracking is per-model because Google's free tier quotas are
    per-model — a key exhausted on gemini-2.5-flash may still have
    capacity on gemini-2.0-flash.
    """
    if not _key_cycle:
        return None
    blocked = _blocked_keys_per_model.setdefault(model, set())
    with _lock:
        for _ in range(len(_keys)):
            k = next(_key_cycle)
            if k not in blocked:
                return k
    return None


# ============================================================================
# Low-level Gemini API call — drop-in compatible with _call_openrouter / _call_claude
# ============================================================================
API_URL_FMT = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"


def _call_gemini(prompt: str, max_tokens: int = 4000,
                 system: Optional[str] = None,
                 model: Optional[str] = None,
                 _already_fellback: bool = False) -> Optional[str]:
    """
    Single Gemini chat completion. Returns text content, or None on failure.

    Signature mirrors openrouter_client._call_openrouter exactly so the
    monkey-patch in pipeline/gemini_check.py is a 1:1 substitution.

    Behavior:
      • Rotates through configured keys on 429 (rate-limit)
      • Falls back to FALLBACK_MODEL when primary's full key pool is
        exhausted (all keys 429 for this model in this process)
      • Returns None on any other terminal failure (caller treats as
        fetch failure → SKIP row, retry next run — same as OpenRouter
        client's contract)
    """
    if not ENABLED:
        return None

    use_model = model or MODEL
    body: Dict[str, Any] = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.1,
            "maxOutputTokens": max_tokens,
            # Force valid JSON output at the API level. Claude reviewer
            # prompts already instruct "respond with JSON only", but
            # forcing the response MIME makes parsing reliable even
            # when the model would otherwise add prose preamble.
            "responseMimeType": "application/json",
        },
    }
    if system:
        body["systemInstruction"] = {"parts": [{"text": system}]}

    # Try all configured keys on this model before falling back to the
    # secondary model. Each 429 marks the key as blocked for this
    # (model, process) and the loop rotates to the next key.
    for _ in range(len(_keys)):
        key = _next_key(use_model)
        if not key:
            break  # All keys blocked for this model — fall through to fallback
        url = API_URL_FMT.format(model=use_model, key=key)
        try:
            r = requests.post(url, json=body, timeout=TIMEOUT)
        except requests.RequestException as exc:
            log.warning("Gemini network error (model=%s, key=...%s): %s",
                        use_model, key[-6:], exc)
            continue

        if r.status_code == 200:
            return _extract_text(r.json())

        if r.status_code == 429:
            log.info("Gemini 429 on model=%s key=...%s — marking blocked, rotating",
                     use_model, key[-6:])
            with _lock:
                _blocked_keys_per_model[use_model].add(key)
            continue

        if r.status_code in (400, 404):
            # Genuine client error or model not found — fallback won't
            # help if the request is malformed. But if it's a "model
            # deprecated" 404, fallback might. Try once.
            log.warning("Gemini HTTP %d on model=%s: %s",
                        r.status_code, use_model, r.text[:200])
            break

        # 5xx — server side issue, log and retry with another key in case
        # of regional outage
        log.warning("Gemini HTTP %d on model=%s key=...%s: %s",
                    r.status_code, use_model, key[-6:], r.text[:200])

    # Primary key pool exhausted (or model returned 4xx). Try fallback model.
    if (
        not _already_fellback
        and FALLBACK_MODEL
        and FALLBACK_MODEL != use_model
    ):
        log.info("Gemini primary model=%s exhausted/failed — falling back to %s",
                 use_model, FALLBACK_MODEL)
        return _call_gemini(
            prompt, max_tokens=max_tokens, system=system,
            model=FALLBACK_MODEL, _already_fellback=True,
        )

    return None


def _extract_text(data: Dict[str, Any]) -> Optional[str]:
    """Extract response text from Gemini's candidate structure.

    Gemini wraps text in candidates[0].content.parts[N].text. With
    responseMimeType=application/json there's typically one part with
    the full JSON string. Defensive against malformed responses.

    Audit 2026-05-11: detect MAX_TOKENS truncation explicitly. If the
    model hits the output cap mid-JSON, returning the partial text
    causes JSONDecodeError ("Unterminated string ...") downstream and
    the caller SKIPs. The skip is correct, but logging it as a generic
    parse failure obscured the real cause for 3 rows in the 2026-05-11
    20:00 Athens run on gemini-2.5-flash-lite (which has a tighter
    output budget than flash). Distinguish the two so the fix
    (raise maxOutputTokens) is visible.
    """
    try:
        candidates = data.get("candidates") or []
        if not candidates:
            log.warning("Gemini response missing candidates: %s", str(data)[:200])
            return None
        cand0 = candidates[0]
        finish_reason = (cand0.get("finishReason") or "").upper()
        parts = (cand0.get("content") or {}).get("parts") or []
        if not parts:
            log.warning("Gemini response missing parts (finishReason=%s): %s",
                        finish_reason or "unspecified", str(data)[:200])
            return None
        # Join all text parts (usually just one, but defensive)
        text = "".join(p.get("text", "") for p in parts).strip()
        if finish_reason == "MAX_TOKENS":
            # Truncated mid-output. The JSON is unparseable; signal SKIP
            # to the caller so the row is retried next run with hopefully
            # a different model / key / capacity.
            log.warning("Gemini response truncated by MAX_TOKENS (got %d chars). "
                        "Bump maxOutputTokens or use gemini-2.5-flash (not -lite). "
                        "Tail: %s",
                        len(text), text[-120:])
            return None
        if finish_reason and finish_reason not in ("STOP", "MODEL_LENGTH"):
            # SAFETY, RECITATION, OTHER, etc. — model refused / filtered.
            log.warning("Gemini response stopped with finishReason=%s "
                        "(text len=%d). Treating as failure.",
                        finish_reason, len(text))
            return None
        return text if text else None
    except (KeyError, IndexError, TypeError, AttributeError) as exc:
        log.warning("Gemini response parse error: %s | %s", exc, str(data)[:200])
        return None


# ============================================================================
# Helpers — mirror openrouter_client.py exactly so the rest of the pipeline
# (which calls _clean_response on every Gemini reply) sees identical behavior.
# ============================================================================
def _strip_fences(txt: str) -> str:
    """Strip ```json ... ``` fences from a model response.

    Gemini's responseMimeType=application/json shouldn't emit fences,
    but defensive — some Gemini variants and any future model swap
    via env var may still wrap output. Idempotent on clean JSON.
    """
    if not txt:
        return txt
    s = txt.strip()
    if s.startswith("```"):
        lines = s.split("\n")
        if lines:
            lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        s = "\n".join(lines).strip()
    return s


_THINK_RE = re.compile(r"<think>.*?</think>", re.DOTALL | re.IGNORECASE)


def _strip_thinking(text: str) -> str:
    """Remove <think>...</think> blocks (defensive — Gemini Flash doesn't emit these).

    Kept for parity with openrouter_client and to future-proof against
    GEMINI_REVIEWER_MODEL being overridden to a reasoning variant.
    Idempotent. Safe on text without thinking blocks.
    """
    if not text:
        return text
    cleaned = _THINK_RE.sub("", text).strip()
    if "<think>" in cleaned.lower():
        for opener in ("{", "["):
            idx = cleaned.find(opener)
            if idx > 0:
                cleaned = cleaned[idx:].strip()
                break
    return cleaned


def _clean_response(text: str) -> str:
    """Combined cleaner: strip fences, then thinking blocks.

    Public name matches openrouter_client._clean_response so the
    monkey-patch in pipeline/gemini_check.py can import it under the
    same symbol with no call-site changes.
    """
    return _strip_thinking(_strip_fences(text))


# ============================================================================
# Identical-prompt parity with claude_client (single source of truth)
# ============================================================================
try:
    from review.claude_client import (
        REVIEW_PROMPT,
        REVIEW_BATCH_SYSTEM,
        EXTRACTION_SYSTEM,
        EXTRACTION_PROMPT,
    )
except ImportError:
    log.error(
        "review.claude_client not importable — Gemini reviewer cannot run "
        "with prompt parity. Fix the import path."
    )
    raise


# ============================================================================
# Mode 1 — Tier-1 reviewer (mirrors claude_client.review_tier1)
# ============================================================================
def review_tier1(rows: List[Dict[str, Any]],
                 batch_size: int = 15) -> List[Dict[str, Any]]:
    """Tier-1-only reviewer using Gemini as backend.

    Behaviorally identical to claude_client.review_tier1 — same prompt,
    same input filter (Tier==1 only), same output schema. Different
    backend so this serves as an independent dissent reviewer in the
    morning Claude / evening Gemini dual-pass setup.
    """
    if not ENABLED:
        return []
    tier1_rows = [(i, r) for i, r in enumerate(rows)
                  if int(r.get("Tier", 2) or 2) == 1]
    if not tier1_rows:
        log.info("Gemini review: no Tier-1 rows to review")
        return []
    log.info("Gemini review: %d Tier-1 rows (model=%s)",
             len(tier1_rows), MODEL)

    all_flags: List[Dict[str, Any]] = []
    for batch_start in range(0, len(tier1_rows), batch_size):
        chunk = tier1_rows[batch_start:batch_start + batch_size]
        chunk_data = [{"row_index": idx, **row} for idx, row in chunk]
        prompt = REVIEW_PROMPT + json.dumps(chunk_data, ensure_ascii=False, indent=2)
        txt = _call_gemini(prompt)
        if not txt:
            continue
        try:
            data = json.loads(_clean_response(txt))
            all_flags.extend(data.get("flags", []))
        except json.JSONDecodeError as e:
            log.warning("Gemini review JSON parse failed: %s | text=%s",
                        e, txt[:200])
    log.info("Gemini review: %d flags raised", len(all_flags))
    return all_flags


# ============================================================================
# Mode 1b — Full-batch reviewer (mirrors claude_client.review_batch)
# ============================================================================
def review_batch(rows: List[Dict[str, Any]],
                 batch_size: int = 20) -> List[Dict[str, Any]]:
    """Full review pass (all rows, not just Tier-1).

    Returns the same {row_index, issues, suggested_fixes, confidence}
    schema as claude_client.review_batch, so compute_rejections() in
    pipeline/run_all.py can swap reviewers without code changes.
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
        txt = _call_gemini(prompt, system=REVIEW_BATCH_SYSTEM)
        if not txt:
            continue
        try:
            data = json.loads(_clean_response(txt))
            for rev in data.get("reviews", []):
                rev["row_index"] = rev.get("row_index", 0) + i
                all_reviews.append(rev)
        except json.JSONDecodeError as e:
            log.warning("Gemini review_batch JSON parse failed: %s | text=%s",
                        e, txt[:200])
    log.info("Gemini review_batch: %d issues across %d rows",
             len(all_reviews), len(rows))
    return all_reviews


# ============================================================================
# Mode 2 — HTML extraction fallback (mirrors claude_client.extract_recalls_from_html)
# ============================================================================
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
    """Extract structured recalls from regulator HTML using Gemini.

    Behavioral mirror of claude_client.extract_recalls_from_html — same
    prompt, same return shape.
    """
    if not ENABLED:
        return []
    prompt = EXTRACTION_PROMPT.format(
        html=html[:120_000],
        source_url=source_url,
        agency=agency,
        country=country,
        language=language,
        extra_hints=extra_hints or "",
        since_days=since_days,
    )
    txt = _call_gemini(prompt, max_tokens=max_tokens, system=EXTRACTION_SYSTEM)
    if not txt:
        return []
    try:
        data = json.loads(_clean_response(txt))
        recalls = data.get("recalls", []) or []
        if recalls:
            log.info("Gemini fallback extracted %d recalls from %s",
                     len(recalls), source_url)
        return recalls
    except json.JSONDecodeError as e:
        log.warning("Gemini extract JSON parse failed: %s | text=%s",
                    e, txt[:200])
        return []
