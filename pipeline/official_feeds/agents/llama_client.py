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
# 90 s was too short and cost whole runs. The box is 2 CPU vCPUs with no
# GPU: a first token after a 5 000-character page has to prompt-eval the
# entire conversation, which takes minutes, not seconds. Every one of those
# turns was recorded as "network error talking to llama" and the row went
# back to Pending as an infra retry — on a server that was working, just
# thinking. The job's own time budget (--time-budget-min) is the real stop,
# and it is 20-40 minutes; this only needs to be longer than one slow turn.
LLAMA_TIMEOUT     = int(os.environ.get("LLAMA_TIMEOUT_SEC", "300"))
# ── CONTEXT BUDGET (2026-09-22) ─────────────────────────────────────────
# llama-server runs with --ctx-size 8192. The conversation is the system
# prompt + the row + the tool schema + every page the model has fetched,
# and a single regulator page at MAX_PAGE_CHARS=5000 is already a quarter
# of that. Two fetches and the server answers:
#
#     HTTP 500 {"error":{"message":"Context size has been exceeded."}}
#
# which the client counted as a failure, tripped the circuit breaker on,
# and reported as an outage. It is not an outage: it is us handing the
# model more text than it can hold. Four RappelConso rows died this way on
# 2026-09-22 in a single run.
#
# So the client now keeps its own budget and trims the conversation to fit
# BEFORE sending, oldest tool results first — those are pages the model has
# already read and summarised into its reasoning; the newest one is the one
# it is working on. Chars, not tokens, because we cannot tokenise here:
# ~3.5 chars/token for French and Polish, so 14 000 chars ≈ 4 000 tokens,
# leaving room for the tool schema and the reply inside 8192.
LLAMA_CTX_CHARS   = int(os.environ.get("LLAMA_CTX_CHAR_BUDGET", "14000"))
TOOL_RESULT_CHARS = int(os.environ.get("LLAMA_TOOL_RESULT_CHARS", "2500"))
_OLD_TOOL_KEEP    = 400      # what an already-read page is trimmed down to
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


def _conv_chars(history: list[dict]) -> int:
    """Chars this history will cost on the wire.

    tool_calls are counted too: an assistant turn that requests a tool
    carries the function name and its JSON arguments, and those are sent
    to the model exactly like content. Counting only `content` made the
    budget read low on precisely the conversations that overflow — the
    ones with several tool rounds.
    """
    n = 0
    for m in history:
        n += len(str(m.get("content") or ""))
        for tc in (m.get("tool_calls") or []):
            fn = tc.get("function") or {}
            n += len(str(fn.get("name") or "")) + len(str(fn.get("arguments") or ""))
    return n


def _fit_context(history: list[dict], hard: bool = False) -> tuple[list[dict], int]:
    """Trim the conversation to the char budget. Returns (history, n_trimmed).

    Oldest tool results go first, and the most recent one is trimmed only
    when `hard` — the model is reasoning about that page right now, so
    cutting it is the last resort rather than the first.
    """
    budget = LLAMA_CTX_CHARS // (2 if hard else 1)
    if _conv_chars(history) <= budget:
        return history, 0
    tool_idx = [i for i, m in enumerate(history) if m.get("role") == "tool"]
    order = tool_idx[:-1] if (tool_idx and not hard) else tool_idx
    n = 0
    for i in order:
        if _conv_chars(history) <= budget:
            break
        body = str(history[i].get("content") or "")
        keep = _OLD_TOOL_KEEP if not hard else 200
        if len(body) > keep:
            history[i] = dict(history[i])
            history[i]["content"] = (
                body[:keep] + "\n…[earlier tool result trimmed so the "
                              "conversation fits the model's context]")
            n += 1
    return history, n


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
        history, _t = _fit_context(history)
        if _t:
            print(f"  [llama] trimmed {_t} earlier tool result(s) to stay "
                  f"inside the {LLAMA_CTX_CHARS}-char context budget")
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
            # CONTEXT OVERFLOW IS OURS TO FIX, NOT AN OUTAGE (2026-09-22).
            # Retry once on a halved budget before calling anything failed.
            if (resp.status_code == 500
                    and "context size" in resp.text.lower()):
                history, _h = _fit_context(history, hard=True)
                if _h:
                    print(f"  [llama] server says the context is full — "
                          f"trimmed {_h} tool result(s) hard and retrying "
                          f"this turn once")
                    payload["messages"] = history
                    resp = requests.post(url, json=payload, headers=headers,
                                          timeout=LLAMA_TIMEOUT)
                else:
                    # Nothing left to trim: the seed alone does not fit.
                    # Re-sending the identical payload buys an identical
                    # 500 and, at LLAMA_TIMEOUT=300, can cost five minutes
                    # per row. Fall through and report it instead.
                    print("  [llama] context is full and there is nothing "
                          "left to trim — the system prompt + row + tool "
                          "schema alone exceed --ctx-size. Not retrying.")
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
            # NAME THE RIGHT CAUSE (2026-09-22).
            #
            # This hint used to be appended to EVERY non-200 that carried
            # tools[]. So the 500 immediately above — the one that says
            # "Context size has been exceeded" in the server's own words —
            # was reported as a chat-template problem. That theory was
            # probed on the VPS on 2026-09-17 and came back ALREADY OK.
            # Following it a second time means restarting a box whose only
            # complaint is that we sent it too much text.
            #
            # A template rejection is a 4xx that says "template". An
            # overflow is a 500 that says "context". Different sentences,
            # different hints, and the context one must say plainly that
            # the VPS is not the problem.
            _ctx = "context size" in body.lower() or "n_ctx" in body.lower()
            if _ctx:
                extra = ("  | THE BOX IS FINE. The conversation did not fit "
                         "in --ctx-size. The client already trimmed and "
                         "retried once; if you see this, trimming was not "
                         "enough. Lower LLAMA_CTX_CHAR_BUDGET / "
                         "REVIEW_MAX_PAGE_CHARS, or raise --ctx-size on "
                         "llama-server. Do NOT restart the VPS.")
            elif tools and resp.status_code < 500 and any(
                    w in body.lower()
                    for w in ("template", "tool_call", "function call",
                              "does not support")):
                extra = ("  | this request carried tools[] and the server's "
                         "own words match a chat template that cannot "
                         "accept them")
            else:
                extra = ""
            _fail(f"llama returned HTTP {resp.status_code}", body + extra)
            _annotate_once(f"llama HTTP {resp.status_code}", body + extra,
                           bool(tools) and not _ctx)
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
            if len(result) > TOOL_RESULT_CHARS:
                result = (result[:TOOL_RESULT_CHARS]
                          + "\n…[truncated to fit the model's context]")
            history.append({
                "role":         "tool",
                "tool_call_id": tc.get("id", ""),
                "content":      result,
            })
            time.sleep(0.1)

    # ── LOOP EXHAUSTION: ASK FOR THE VERDICT, DON'T THROW THE RUN AWAY ──
    #
    # Loop exhaustion is NOT an outage. The model answered every turn; it
    # just kept asking for another tool call instead of committing to a
    # final answer. The usual cause is the tool returning nothing useful —
    # an empty Searx result set will do it, and Searx fails soft (returns
    # []), so a dead search box looks exactly like a dead model from here.
    #
    # Returning None here (the behaviour until 2026-09-17) made every such
    # row come back "INFRA: no llama response (retry)", the whole run exit 3,
    # and the work of all six turns be discarded. Observed on 2026-09-17:
    # reviewer 1 spent six searches on one Prime Line Distributors row and
    # published nothing, on a box where llama was demonstrably healthy.
    #
    # A model that has searched six times has either found what it needs or
    # established that it cannot. Both are answers. So spend ONE more call
    # with tool_choice="none" — the model physically cannot ask for another
    # tool — and take whatever verdict it gives. If it says it could not
    # confirm, that is a real reject/retry decided on evidence, not an
    # infrastructure guess.
    print(f"  [llama] hit max tool loop depth ({LLAMA_MAX_LOOPS}) — the model "
          f"kept requesting tools and never returned a final answer. This is "
          f"NOT an unreachable model: it replied {LLAMA_MAX_LOOPS} times. "
          f"Forcing a final answer with tools disabled.")

    history.append({
        "role": "user",
        "content": (
            "You have used every available tool call. Do not request another "
            "tool — you will not be given one. Answer NOW, in the exact "
            "output format the first message specified, using only what you "
            "already have. If the searches did not let you confirm this "
            "recall, say so plainly in that format rather than asking to "
            "search again."
        ),
    })
    history, _ = _fit_context(history, hard=True)
    try:
        resp = requests.post(
            url,
            json={"model": LLAMA_MODEL, "messages": history,
                  "temperature": temperature, "max_tokens": max_tokens,
                  "tool_choice": "none"},
            headers=headers, timeout=LLAMA_TIMEOUT)
        if resp.status_code == 200:
            final = ((resp.json()["choices"][0]["message"].get("content")
                      or "").strip())
            if final:
                print("  [llama] forced final answer obtained "
                      f"({len(final)} chars) — the run is NOT an infra failure")
                return final
            print("  [llama] forced final call returned empty content")
        else:
            print(f"  [llama] forced final call HTTP {resp.status_code}: "
                  f"{resp.text[:200].replace(chr(10), ' ')}")
    except Exception as e:   # noqa: BLE001
        print(f"  [llama] forced final call failed: {type(e).__name__}: {e}")

    _fail("tool loop never converged and the forced final answer failed",
          f"model replied {LLAMA_MAX_LOOPS} times but only ever asked for "
          f"more tool calls — usually an empty/failing Searx")
    return None
