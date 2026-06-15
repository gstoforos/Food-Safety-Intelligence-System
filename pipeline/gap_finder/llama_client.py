"""
AFTS Food Safety Intelligence — Greek Gap Finder
Module 4b: Llama Client

OpenAI-compatible HTTP client for the AFTS llama-server VPS
(Qwen 2.5 7B Instruct Q4_K_M, hosted on Hetzner CCX13 via Tailscale).

Used by ALL AFTS AI features that share the same €10/month VPS:
  • Greek gap finder field extraction (Module 5: extractor.py)
  • Future EU market gap finders (Italy/Spain/France)
  • AdvThermaLogic AI assistant
  • FSIS classifier experiments
  • Anything else we build before the Mac migration

Migration-safe: when you move from Hetzner to Mac, change LLAMA_BASE_URL
in env vars. Zero code changes anywhere else.

Features:
  • OpenAI-compatible /v1/chat/completions
  • Structured JSON output mode (response_format json_schema)
  • Retry with exponential backoff
  • Bounded request timeout
  • Self-test against a known canonical Qwen response

CLI:
    python -m pipeline.gap_finder_gr.llama_client --selftest
    python -m pipeline.gap_finder_gr.llama_client --health
    python -m pipeline.gap_finder_gr.llama_client --prompt "ποια είναι η πρωτεύουσα της Ελλάδας;"

Env vars:
    LLAMA_BASE_URL   — e.g. http://100.x.y.z:8080/v1  (Tailscale IP of VPS)
                       or http://localhost:8080/v1  (after Mac migration)
                       default: http://localhost:8080/v1
    LLAMA_API_KEY    — optional bearer token (llama-server can be configured to require)
    LLAMA_MODEL      — model name reported by server (default: qwen2.5-7b-instruct)
    LLAMA_TIMEOUT    — request timeout seconds (default: 120)
"""

from __future__ import annotations
import argparse
import json
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import requests


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION (env-driven, sane defaults)
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_BASE_URL = "http://localhost:8080/v1"
DEFAULT_MODEL = "qwen2.5-7b-instruct"
DEFAULT_TIMEOUT = 45   # was 120 — a healthy Qwen-7B call finishes <30s; 120 just prolongs hangs on a dead box


def _env(key: str, default: str) -> str:
    v = os.environ.get(key)
    return v if v else default


# ─────────────────────────────────────────────────────────────────────────────
# CLIENT
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class LlamaError(Exception):
    """All errors from this client are raised as this type."""
    message: str
    status_code: Optional[int] = None
    body: Optional[str] = None

    def __str__(self) -> str:
        if self.status_code:
            return f"LlamaError {self.status_code}: {self.message}"
        return f"LlamaError: {self.message}"


@dataclass
class LlamaClient:
    base_url: str = field(default_factory=lambda: _env("LLAMA_BASE_URL", DEFAULT_BASE_URL))
    api_key: Optional[str] = field(default_factory=lambda: os.environ.get("LLAMA_API_KEY"))
    model: str = field(default_factory=lambda: _env("LLAMA_MODEL", DEFAULT_MODEL))
    timeout: int = field(default_factory=lambda: int(_env("LLAMA_TIMEOUT", str(DEFAULT_TIMEOUT))))
    max_retries: int = 2   # was 3
    backoff_base: float = 2.0

    # ── Circuit breaker ─────────────────────────────────────────────────────
    # Class-level (shared across all instances in a run). After
    # _BREAKER_THRESHOLD consecutive total failures, the breaker OPENS and all
    # subsequent calls fail instantly (no socket wait), so a dead Llama box
    # costs ~1 min instead of timeout×retries×records (was up to 48 min for a
    # Greek run). Classification still works without the LLM, so records fall
    # back to title-only immediately. The breaker resets on any success.

    # ── Request plumbing ────────────────────────────────────────────────────

    # Shared breaker state (class attributes, not per-instance).
    _breaker_failures: int = 0
    _breaker_open: bool = False
    _BREAKER_THRESHOLD: int = 2

    @classmethod
    def _breaker_record_failure(cls) -> None:
        cls._breaker_failures += 1
        if cls._breaker_failures >= cls._BREAKER_THRESHOLD:
            if not cls._breaker_open:
                print(f"  [llama] CIRCUIT OPEN — {cls._breaker_failures} consecutive "
                      f"failures; remaining records skip the LLM and use "
                      f"title-only fallback", flush=True)
            cls._breaker_open = True

    @classmethod
    def _breaker_record_success(cls) -> None:
        if cls._breaker_failures or cls._breaker_open:
            cls._breaker_failures = 0
            cls._breaker_open = False

    @classmethod
    def reset_breaker(cls) -> None:
        """Reset breaker between independent runs (call at pipeline start)."""
        cls._breaker_failures = 0
        cls._breaker_open = False

    def _headers(self) -> dict[str, str]:
        h = {"Content-Type": "application/json", "Accept": "application/json"}
        if self.api_key:
            h["Authorization"] = f"Bearer {self.api_key}"
        return h

    def _post(self, path: str, payload: dict) -> dict:
        # Circuit breaker: if already tripped this run, fail instantly so we
        # don't burn timeout×retries on a known-dead box.
        if type(self)._breaker_open:
            raise LlamaError("circuit open — Llama box unresponsive this run")

        url = f"{self.base_url.rstrip('/')}{path}"
        last_exc: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = requests.post(
                    url,
                    headers=self._headers(),
                    data=json.dumps(payload),
                    timeout=self.timeout,
                )
                if resp.status_code >= 500:
                    raise LlamaError(
                        f"server error on {path}",
                        status_code=resp.status_code,
                        body=resp.text[:500],
                    )
                if resp.status_code >= 400:
                    # Don't retry 4xx — it's a client bug
                    raise LlamaError(
                        f"client error on {path}",
                        status_code=resp.status_code,
                        body=resp.text[:500],
                    )
                type(self)._breaker_record_success()
                return resp.json()
            except (requests.Timeout, requests.ConnectionError, LlamaError) as e:
                last_exc = e
                if isinstance(e, LlamaError) and e.status_code and 400 <= e.status_code < 500:
                    raise
                if attempt < self.max_retries:
                    sleep_s = self.backoff_base ** (attempt - 1)
                    time.sleep(sleep_s)
                    continue
                type(self)._breaker_record_failure()
                raise LlamaError(f"all {self.max_retries} attempts failed: {e}") from e
        # Unreachable but appeases type-checker
        raise LlamaError(f"unexpected loop exit: {last_exc}")

    def _get(self, path: str) -> dict:
        url = f"{self.base_url.rstrip('/')}{path}"
        try:
            resp = requests.get(url, headers=self._headers(), timeout=self.timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            raise LlamaError(f"GET {path} failed: {e}") from e

    # ── Public API ──────────────────────────────────────────────────────────

    def health(self) -> bool:
        """Returns True if the server responds to /models. Doesn't raise."""
        try:
            self._get("/models")
            return True
        except LlamaError:
            return False

    def chat(
        self,
        messages: list[dict[str, str]],
        temperature: float = 0.0,
        max_tokens: int = 1024,
        json_schema: Optional[dict] = None,
        stop: Optional[list[str]] = None,
    ) -> str:
        """
        Standard chat completion. Returns the assistant message content as a string.

        For deterministic extraction tasks (gap finder, classifier), keep
        temperature=0.0. For creative tasks (summaries), raise to 0.3–0.7.

        If json_schema is given, the response is constrained to match the schema
        (llama.cpp supports this via the 'response_format' field).
        """
        payload: dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "stream": False,
        }
        if stop:
            payload["stop"] = stop
        if json_schema:
            payload["response_format"] = {
                "type": "json_schema",
                "json_schema": {"name": "response", "schema": json_schema},
            }

        data = self._post("/chat/completions", payload)
        try:
            return data["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError) as e:
            raise LlamaError(
                f"malformed response: {e}", body=json.dumps(data)[:500]
            ) from e

    def chat_json(
        self,
        messages: list[dict[str, str]],
        json_schema: dict,
        temperature: float = 0.0,
        max_tokens: int = 1024,
    ) -> dict:
        """
        Like chat() but parses the response as JSON validated against json_schema.
        Raises LlamaError if the response isn't valid JSON.
        """
        raw = self.chat(
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            json_schema=json_schema,
        )
        try:
            return json.loads(raw)
        except json.JSONDecodeError as e:
            # Some models wrap JSON in ```json ... ``` — strip that defensively
            cleaned = raw.strip()
            for fence in ("```json", "```"):
                if cleaned.startswith(fence):
                    cleaned = cleaned[len(fence):].lstrip()
                if cleaned.endswith("```"):
                    cleaned = cleaned[:-3].rstrip()
            try:
                return json.loads(cleaned)
            except json.JSONDecodeError:
                raise LlamaError(
                    f"response not valid JSON: {e}", body=raw[:500]
                ) from e


# ─────────────────────────────────────────────────────────────────────────────
# SELF-TEST
# ─────────────────────────────────────────────────────────────────────────────

def selftest(client: LlamaClient) -> int:
    print("=" * 78)
    print("AFTS Llama Client — Self-Test")
    print("=" * 78)
    print(f"  base_url: {client.base_url}")
    print(f"  model:    {client.model}")
    print(f"  timeout:  {client.timeout}s")
    print()

    passed = failed = 0

    # 1. Health check
    print("[1] Health check (GET /models)...", end=" ", flush=True)
    if client.health():
        print("✓ OK")
        passed += 1
    else:
        print("✗ FAIL — server unreachable. Check LLAMA_BASE_URL and Tailscale.")
        failed += 1
        return 1  # No point continuing if we can't reach the server

    # 2. Basic Greek prompt
    print("[2] Greek question ('ποια είναι η πρωτεύουσα της Ελλάδας;')...",
          end=" ", flush=True)
    try:
        ans = client.chat(
            messages=[
                {"role": "system", "content": "Απαντάς σύντομα και ακριβώς, στα ελληνικά."},
                {"role": "user", "content": "Ποια είναι η πρωτεύουσα της Ελλάδας;"},
            ],
            temperature=0.0,
            max_tokens=50,
        )
        if "αθήν" in ans.lower() or "athens" in ans.lower():
            print(f"✓ OK ('{ans.strip()[:60]}')")
            passed += 1
        else:
            print(f"✗ FAIL (got: {ans.strip()[:80]})")
            failed += 1
    except LlamaError as e:
        print(f"✗ FAIL ({e})")
        failed += 1

    # 3. JSON-mode extraction (the actual use case for gap finder)
    print("[3] JSON-mode extraction (food recall fields)...", end=" ", flush=True)
    try:
        schema = {
            "type": "object",
            "properties": {
                "company": {"type": "string"},
                "brand": {"type": "string"},
                "pathogen": {"type": "string"},
            },
            "required": ["company", "brand", "pathogen"],
        }
        data = client.chat_json(
            messages=[
                {
                    "role": "system",
                    "content": "Extract the company, brand, and pathogen from "
                               "the recall text. Reply with JSON only.",
                },
                {
                    "role": "user",
                    "content": "Ο ΕΦΕΤ ανακαλεί φέτα ΒΥΤΙΝΑΣ ΠΟΠ της εταιρείας "
                               "Τυροκομικά Βυτίνας Α.Ε. λόγω Listeria monocytogenes.",
                },
            ],
            json_schema=schema,
            max_tokens=200,
        )
        if (isinstance(data, dict)
            and "listeria" in data.get("pathogen", "").lower()
            and "βυτίν" in data.get("brand", "").lower()):
            print(f"✓ OK ({data})")
            passed += 1
        else:
            print(f"✗ FAIL (got: {data})")
            failed += 1
    except LlamaError as e:
        print(f"✗ FAIL ({e})")
        failed += 1

    # 4. Retry resilience — invalid endpoint should fail fast (4xx, no retry)
    print("[4] 4xx error fails fast (no retry storm)...", end=" ", flush=True)
    start = time.time()
    try:
        client._post("/this/does/not/exist", {"foo": "bar"})
        print("✗ FAIL — should have raised")
        failed += 1
    except LlamaError as e:
        elapsed = time.time() - start
        if e.status_code and 400 <= e.status_code < 500 and elapsed < 5:
            print(f"✓ OK (failed in {elapsed:.2f}s with {e.status_code})")
            passed += 1
        else:
            print(f"✗ FAIL (status={e.status_code}, elapsed={elapsed:.2f}s)")
            failed += 1

    print()
    print("=" * 78)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 78)
    return 0 if failed == 0 else 1


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(description="AFTS Llama Client")
    parser.add_argument("--base-url", help=f"Override LLAMA_BASE_URL (default env or {DEFAULT_BASE_URL})")
    parser.add_argument("--model", help=f"Override model name (default env or {DEFAULT_MODEL})")
    parser.add_argument("--timeout", type=int, help="Override request timeout seconds")
    parser.add_argument("--health", action="store_true", help="Just check if server is up")
    parser.add_argument("--selftest", action="store_true", help="Run full self-test suite")
    parser.add_argument("--prompt", help="One-shot prompt (sends single user message)")
    parser.add_argument("--system", default="You are a helpful assistant.",
                        help="System prompt for --prompt mode")
    parser.add_argument("--max-tokens", type=int, default=256)
    parser.add_argument("--temperature", type=float, default=0.0)
    args = parser.parse_args()

    kwargs: dict[str, Any] = {}
    if args.base_url:
        kwargs["base_url"] = args.base_url
    if args.model:
        kwargs["model"] = args.model
    if args.timeout:
        kwargs["timeout"] = args.timeout
    client = LlamaClient(**kwargs)

    if args.health:
        ok = client.health()
        print(f"Server at {client.base_url}: {'✓ UP' if ok else '✗ DOWN'}")
        return 0 if ok else 1

    if args.selftest:
        return selftest(client)

    if args.prompt:
        try:
            ans = client.chat(
                messages=[
                    {"role": "system", "content": args.system},
                    {"role": "user", "content": args.prompt},
                ],
                temperature=args.temperature,
                max_tokens=args.max_tokens,
            )
            print(ans)
            return 0
        except LlamaError as e:
            print(f"ERROR: {e}", file=sys.stderr)
            return 1

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
