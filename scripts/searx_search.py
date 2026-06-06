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


def is_configured() -> bool:
    return bool(SEARX_URL)


def search(query: str,
           max_results: int = 8,
           include_domains: Optional[list[str]] = None) -> list[dict]:
    """Run a Searx query. Returns [{url, title, content}, ...]."""
    if not SEARX_URL or _STATE["open"]:
        return []

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
            if _STATE["failures"] >= 3:
                _STATE["open"] = True
                print(f"  [searx] CIRCUIT OPEN")
            return []
        data = resp.json()
    except Exception as e:   # noqa: BLE001
        _STATE["failures"] += 1
        print(f"  [searx] network: {e}")
        if _STATE["failures"] >= 3:
            _STATE["open"] = True
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
