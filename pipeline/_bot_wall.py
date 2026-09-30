"""Bot-wall / challenge-page detection (2026-09-30).

WHY
---
salute.gov.it sits behind a Gcore browser-validation wall. To a datacentre
fetch it answers HTTP 200 with a challenge page whose <title> is "Gcore". The
article fetchers counted that as a successful fetch, extract_title() took the
challenge page's title, and the Italian gap finder stored it as the recall's
Product. Eleven rows in Weekly_Rejected and Rejected carry Product "Gcore",
the earliest from 2026-07-31. One of them (Ambrosini hamburger di scottona,
Salmonella) held the authority URL while its news twin held the hazard, and
each was rejected by the gate the other would have satisfied.

A challenge page is not the notice. It is a statement about us — the same
invariant as the lying 404: "I could not read it" is not data.

WHERE THIS RUNS
---------------
* every article fetcher's fetch_html()  — a wall becomes status "bot_wall"
  with empty HTML, so callers fall back exactly as for any failed fetch;
* extract_title()                        — never returns a wall title;
* _gap_finder_guards.product_is_garbage  — a wall title is garbage;
* merge_master._write_sheet              — the writer choke point blanks a
  wall title in Product / Company / Brand on every write, so a row that got
  one by any route cannot publish on it (an empty Product holds the row).

Deliberately conservative. A TITLE is matched only as the whole title, never
as a substring, so a product called "Gcore protein bar" survives. HTML is
flagged only when the visible text is short AND a challenge marker is present,
because many real pages load Cloudflare or Gcore scripts.
"""
from __future__ import annotations

import re

#: Whole-title matches (compared case-insensitively, whitespace-collapsed,
#: trailing punctuation/ellipsis stripped).
WALL_TITLES = frozenset({
    "gcore",
    "just a moment",
    "attention required! | cloudflare",
    "attention required",
    "access denied",
    "access to this page has been denied",
    "pardon our interruption",
    "request rejected",
    "request blocked",
    "the requested url was rejected. please consult with your administrator",
    "checking your browser",
    "checking your browser before accessing",
    "ddos-guard",
    "ddos protection by cloudflare",
    "403 forbidden",
    "forbidden",
    "401 unauthorized",
    "security check",
    "human verification",
    "robot check",
    "are you a robot",
    "are you a human",
    "please verify you are a human",
    "verify you are human",
    "one more step",
    "bot verification",
    "captcha",
    "incapsula incident",
    "error 1020",
    "please wait",
    "please enable javascript",
    "javascript is required",
    "enable javascript and cookies to continue",
})

#: Markers that only a challenge page carries. Checked only on short pages.
_WALL_MARKERS = re.compile(
    r"cf-chl-|/cdn-cgi/challenge-platform|cf_chl_opt|_incapsula_resource|"
    r"captcha-delivery\.com|datadome|px-captcha|perimeterx|"
    r"gcore[^<]{0,40}(?:challenge|protection|validation|verif)|"
    r"ddos-guard|__ddg|awswaf|aws-waf-token|"
    r"please enable (?:javascript|cookies)|checking your browser",
    re.I)

_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.I | re.S)
_TAG_RE = re.compile(r"<[^>]+>")
_SCRIPT_RE = re.compile(r"<(script|style|noscript)[^>]*>.*?</\1>", re.I | re.S)

#: Visible text shorter than this, together with a marker, is a wall.
SHORT_PAGE_CHARS = 1500


def _norm(t: str) -> str:
    t = re.sub(r"\s+", " ", str(t or "")).strip().lower()
    return t.rstrip(" .…!?:-|").strip()


def is_wall_title(text: str) -> bool:
    """True iff the WHOLE string is a known challenge-page title."""
    n = _norm(text)
    return bool(n) and n in WALL_TITLES


def is_bot_wall_html(html: str) -> bool:
    """True iff this HTML is a challenge page rather than content."""
    if not html:
        return False
    m = _TITLE_RE.search(html)
    if m and is_wall_title(_TAG_RE.sub("", m.group(1))):
        return True
    visible = _TAG_RE.sub(" ", _SCRIPT_RE.sub(" ", html))
    visible = re.sub(r"\s+", " ", visible).strip()
    if len(visible) >= SHORT_PAGE_CHARS:
        return False
    return bool(_WALL_MARKERS.search(html))


def screen(resolved_url: str, html: str, status: str):
    """Post-filter for fetch_html(): a wall becomes ('', 'bot_wall')."""
    if status == "ok" and is_bot_wall_html(html):
        return resolved_url, "", "bot_wall"
    return resolved_url, html, status


def scrub_fields(row: dict, fields=("Product", "Company", "Brand")) -> list:
    """Blank any field whose whole value is a wall title. Returns the names
    of the fields it blanked (so the caller can note it)."""
    hit = []
    for f in fields:
        if f in row and is_wall_title(row.get(f)):
            row[f] = ""
            hit.append(f)
    return hit
