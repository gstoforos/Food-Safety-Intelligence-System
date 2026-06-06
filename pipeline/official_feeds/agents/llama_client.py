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
LLAMA_TIMEOUT     = int(os.environ.get("LLAMA_TIMEOUT_SEC", "45"))
LLAMA_MAX_LOOPS   = 2    # cap tool-calling loop depth

# Circuit breaker — after 3 failures, stop calling for this run.
_STATE = {"failures": 0, "open": False}


def is_configured() -> bool:
    return bool(LLAMA_BASE_URL)


def is_open() -> bool:
    return _STATE["open"]


def chat(messages: list[dict],
          tools: Optional[list[dict]] = None,
          tool_executor: Optional[Callable[[str, dict], str]] = None,
          temperature: float = 0.0,
          max_tokens: int = 1024) -> Optional[str]:
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
        return None
    if _STATE["open"]:
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
            if _STATE["failures"] >= 3:
                _STATE["open"] = True
                print(f"  [llama] CIRCUIT OPEN: 3 consecutive failures")
            return None

        if resp.status_code != 200:
            _STATE["failures"] += 1
            print(f"  [llama] HTTP {resp.status_code}: "
                  f"{resp.text[:200].replace(chr(10), ' ')}")
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

    print(f"  [llama] hit max tool loop depth ({LLAMA_MAX_LOOPS})")
    return None
