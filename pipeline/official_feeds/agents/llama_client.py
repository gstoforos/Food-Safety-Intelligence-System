"""
Llama client — talks to afts-llama-vps via OpenAI-compatible /v1/chat/completions.

Reads LLAMA_BASE_URL from env (e.g. http://afts-llama-vps:8080/v1 over Tailscale).
No API key needed — the box is reachable only on the tailnet.

Implements the OpenAI tool-calling loop: send messages, if the model returns
tool_calls, execute them locally and feed results back, repeat until the
model returns a final text response.
"""

from __future__ import annotations

import json
import os
import time
from typing import Callable, Optional

import requests


LLAMA_BASE_URL    = os.environ.get("LLAMA_BASE_URL", "").rstrip("/")
LLAMA_MODEL       = os.environ.get("LLAMA_MODEL", "qwen2.5-7b-instruct")
LLAMA_TIMEOUT     = int(os.environ.get("LLAMA_TIMEOUT_SEC", "90"))
LLAMA_MAX_LOOPS   = 6    # cap tool-calling loop depth (raised from 4 so the
                         # agent has room for staged discovery: site-scoped
                         # search → broad search to find the recalling firm →
                         # site search — before the caller's best-Searx-hit
                         # fallback takes over (audit 2026-07-02)

# Circuit breaker — after 3 failures, stop calling for this run.
_STATE = {"failures": 0, "open": False}

# ── WHY the last chat() gave up (audit 2026-09-16) ────────────────────────
#
# chat() returned a bare None for four different reasons, and the callers
# (recall_url_agent, recall_review_agent) turned every one of them into the
# same string: "INFRA: no llama response (retry)".
#
# When every row retries, both agents exit 3 on purpose — "make it loud",
# which is right. But the email then says only "All jobs have failed", and
# the log says only "no llama response", so an operator reasonably concludes
# the VPS is down and goes to restart a box that is already running.
#
# On 2026-09-16 both reviewers were red from 05:46 while the Italian gap
# finder had just verified 6 rows through the SAME llama at 05:41. The model
# was demonstrably up. Nothing in either agent's output could say that.
#
# So record the reason. It costs nothing and it is the difference between
# "the box is down" and "the box is fine, the tool loop never converged".
_LAST_FAILURE = {"reason": "", "detail": ""}


def _fail(reason: str, detail: str = "") -> None:
    _LAST_FAILURE["reason"] = reason
    _LAST_FAILURE["detail"] = str(detail)[:300]


_ANNOTATED = {"done": False}


def _annotate_once(title: str, body: str, had_tools: bool) -> None:
    """Put the FIRST llama failure where a human will actually see it.

    Writes a GitHub Actions ``::error`` annotation and appends to
    GITHUB_STEP_SUMMARY, so the cause reaches the run page and the failure
    email instead of dying in step output. Once per run — three identical
    annotations before the circuit breaker trips is noise, not signal.

    A no-op outside Actions.
    """
    if _ANNOTATED["done"]:
        return
    _ANNOTATED["done"] = True
    one = " ".join(str(body).split())[:400]
    print(f"::error title={title}::{one}")
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    try:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(f"\n### {title}\n\n")
            if had_tools:
                fh.write(
                    "This request carried **`tools[]`** (function calling). A "
                    "llama-server started without a tool-capable chat template "
                    "answers plain completions normally — which is why the gap "
                    "finders keep working — and rejects every reviewer request.\n\n")
            fh.write("The server said:\n\n```\n" + one + "\n```\n")
    except OSError:
        pass


def last_failure() -> dict:
    """Why the most recent chat() returned None. Empty reason = no failure."""
    return dict(_LAST_FAILURE)


def failure_summary() -> str:
    """One line an agent can print, or "" when nothing has failed."""
    r = _LAST_FAILURE.get("reason")
    if not r:
        return ""
    d = _LAST_FAILURE.get("detail")
    return f"{r}: {d}" if d else r


def is_configured() -> bool:
    return bool(LLAMA_BASE_URL)


def is_open() -> bool:
    return _STATE["open"]


def chat(messages: list[dict],
          tools: Optional[list[dict]] = None,
          tool_executor: Optional[Callable[[str, dict], str]] = None,
          temperature: float = 0.0,
          max_tokens: int = 256) -> Optional[str]:
    """
    Run a tool-calling conversation with the Llama. Returns the model's
    final text response, or None on failure.

    Args:
        messages:       OpenAI-style message list to seed the conversation.
        tools:          OpenAI tool schema list (each {"type":"function", ...}).
        tool_executor:  callback(name, args_dict) -> str result.  Called for
                          every tool_call the model emits.
        temperature:    sampling temperature.
        max_tokens:     per-turn max output tokens.
    """
    if not LLAMA_BASE_URL:
        _fail("LLAMA_BASE_URL is not set",
              "the workflow did not pass the secret into this step")
        return None
    if _STATE["open"]:
        _fail("circuit breaker open",
              "3 consecutive failures earlier in this run; see the first one")
        return None

    url = f"{LLAMA_BASE_URL}/chat/completions"
    headers = {"Content-Type": "application/json"}

    # Iterate: model -> tool calls -> tool results -> model -> ... -> text
    history = list(messages)
    for loop in range(LLAMA_MAX_LOOPS):
        payload = {
            "model":       LLAMA_MODEL,
            "messages":    history,
            "temperature": temperature,
            "max_tokens":  max_tokens,
        }
        if tools:
            payload["tools"]       = tools
            payload["tool_choice"] = "auto"

        try:
            resp = requests.post(url, json=payload, headers=headers,
                                  timeout=LLAMA_TIMEOUT)
        except Exception as e:   # noqa: BLE001
            _STATE["failures"] += 1
            print(f"  [llama] network: {e}")
            _fail("network error talking to llama", str(e))
            _annotate_once("llama network error", str(e), bool(tools))
            if _STATE["failures"] >= 3:
                _STATE["open"] = True
                print(f"  [llama] CIRCUIT OPEN: 3 consecutive failures")
            return None

        if resp.status_code != 200:
            _STATE["failures"] += 1
            body = resp.text[:300].replace(chr(10), " ")
            print(f"  [llama] HTTP {resp.status_code}: {body}")
            # The server's own words are the whole diagnosis, and until now
            # they only ever reached stdout — where nobody reads them,
            # because the email says "All jobs have failed" and stops there.
            # A tools[] rejection in particular is invisible from outside:
            # GET /models still passes and plain completions still work, so
            # every other signal says the box is healthy.
            _fail(f"llama returned HTTP {resp.status_code}",
                  (body + ("  | this request carried tools[] — a server "
                           "without a tool-capable chat template rejects "
                           "exactly these and nothing else" if tools else "")))
            _annotate_once(f"llama HTTP {resp.status_code}", body, bool(tools))
            if _STATE["failures"] >= 3:
                _STATE["open"] = True
            return None

        _STATE["failures"] = 0
        data = resp.json()
        try:
            msg = data["choices"][0]["message"]
        except (KeyError, IndexError):
            print(f"  [llama] malformed response: {data!s:.200}")
            return None

        tool_calls = msg.get("tool_calls") or []
        if not tool_calls:
            # Final answer
            return (msg.get("content") or "").strip()

        # Append the assistant's tool-call message to history
        history.append({
            "role":       "assistant",
            "content":    msg.get("content") or "",
            "tool_calls": tool_calls,
        })

        # Execute each tool call locally, append the results
        if tool_executor is None:
            print("  [llama] model called a tool but no executor provided")
            return None

        for tc in tool_calls:
            name = tc.get("function", {}).get("name", "")
            args_raw = tc.get("function", {}).get("arguments", "{}")
            try:
                args = json.loads(args_raw) if isinstance(args_raw, str) else args_raw
            except json.JSONDecodeError:
                args = {}
            print(f"  [llama → tool] {name}({args!s:.150})")
            try:
                result = tool_executor(name, args)
            except Exception as e:   # noqa: BLE001
                result = f"ERROR: {e}"
            # Truncate huge results so we don't blow context
            if len(result) > 4000:
                result = result[:4000] + "\n…[truncated]"
            history.append({
                "role":         "tool",
                "tool_call_id": tc.get("id", ""),
                "content":      result,
            })
            time.sleep(0.1)

    # Loop exhaustion is NOT an outage. The model answered every turn; it
    # just kept asking for another tool call instead of committing to a
    # final answer. The usual cause is the tool returning nothing useful —
    # an empty Searx result set will do it, and Searx fails soft (returns
    # []), so a dead search box looks exactly like a dead model from here.
    print(f"  [llama] hit max tool loop depth ({LLAMA_MAX_LOOPS}) — the model "
          f"kept requesting tools and never returned a final answer. This is "
          f"NOT an unreachable model: it replied {LLAMA_MAX_LOOPS} times. "
          f"Check SEARX_URL before restarting the VPS.")
    _fail("tool loop never converged",
          f"model replied {LLAMA_MAX_LOOPS} times but only ever asked for "
          f"more tool calls — usually an empty/failing Searx")
    return None
