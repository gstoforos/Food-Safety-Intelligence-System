"""
Searx client — the web-search tool the Llama agent uses.

Self-hosted on afts-llama-vps (same box as the Llama). Reachable from the
GitHub runner via Tailscale. No API token. No external service.

Env:
    SEARX_URL   — http://afts-llama-vps:8888/search (Tailscale hostname)
"""

from __future__ import annotations

import os
import requests
from typing import Optional


SEARX_URL      = os.environ.get("SEARX_URL", "").strip()
SEARX_TIMEOUT  = 30

_STATE = {"failures": 0, "open": False}

# ── Searx is the reviewers' SILENT dependency (audit 2026-09-16) ──────────
#
# Every failure path below returns [] — deliberately, so one bad query does
# not kill a review run. But the model on the other end cannot tell "no
# results" from "search is dead": it just asks for another search, and after
# LLAMA_MAX_LOOPS turns llama_client gives up and returns None, which both
# reviewers report as "INFRA: no llama response (retry)".
#
# So a dead Searx is indistinguishable, in the logs and in the failure
# email, from a dead VPS. On 2026-09-16 both reviewers were red while the
# same llama was verifying gap-finder rows normally five minutes earlier —
# the gap finders do not use Searx, and that is the whole difference.
#
# The workflows now health-check SEARX_URL alongside the model. This counter
# is the other half: it says whether searching actually worked this run.
_STATS = {"queries": 0, "empty": 0, "errors": 0}


def stats() -> dict:
    """Search outcomes for this run, for the agent's end-of-run summary."""
    return dict(_STATS)


def health_line() -> str:
    """One line naming Searx's state, or "" when it was never used."""
    if not SEARX_URL:
        return "Searx: SEARX_URL not set — the reviewers ran blind"
    if not _STATS["queries"]:
        return ""
    if _STATS["errors"] >= _STATS["queries"]:
        return (f"Searx: FAILED every one of {_STATS['queries']} queries "
                f"— treat this, not the model, as the outage")
    if _STATS["empty"] + _STATS["errors"] >= _STATS["queries"]:
        return (f"Searx: reachable but returned nothing for all "
                f"{_STATS['queries']} queries")
    return (f"Searx: {_STATS['queries']} queries, {_STATS['empty']} empty, "
            f"{_STATS['errors']} errors")


def is_configured() -> bool:
    return bool(SEARX_URL)


def search(query: str,
           max_results: int = 8,
           include_domains: Optional[list[str]] = None) -> list[dict]:
    """Run a Searx query. Returns [{url, title, content}, ...]."""
    if not SEARX_URL or _STATE["open"]:
        _STATS["errors"] += 1
        return []
    _STATS["queries"] += 1

    # If we want to restrict to specific domains, append site: filters
    q = query
    if include_domains:
        sites = " OR ".join(f"site:{d}" for d in include_domains)
        q = f"{query} ({sites})"

    params = {
        "q":          q,
        "format":     "json",
        "categories": "general",
        "safesearch": "0",
    }
    try:
        resp = requests.get(SEARX_URL, params=params, timeout=SEARX_TIMEOUT)
        if resp.status_code != 200:
            _STATE["failures"] += 1
            print(f"  [searx] HTTP {resp.status_code}: "
                  f"{resp.text[:160].replace(chr(10), ' ')}")
            _STATS["errors"] += 1
            if _STATE["failures"] >= 3:
                _STATE["open"] = True
                print("  [searx] CIRCUIT OPEN — every later query in this run "
                      "returns [] without asking. The model will keep "
                      "searching and never converge; that surfaces as "
                      "'no llama response'. It is Searx.")
            return []
        data = resp.json()
    except Exception as e:   # noqa: BLE001
        _STATE["failures"] += 1
        print(f"  [searx] network: {e}")
        _STATS["errors"] += 1
        if _STATE["failures"] >= 3:
            _STATE["open"] = True
            print("  [searx] CIRCUIT OPEN after 3 network failures — see the "
                  "note at the top of this file before blaming the VPS.")
        return []

    _STATE["failures"] = 0
    raw = data.get("results", []) or []
    out = []
    for r in raw[:max_results]:
        out.append({
            "url":     r.get("url", ""),
            "title":   r.get("title", ""),
            "content": (r.get("content") or "")[:600],
        })
    return out
