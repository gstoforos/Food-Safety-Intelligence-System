#!/usr/bin/env python3
"""INTEL AGENT — reads the news feed, proposes work. Writes nothing.

WHAT IT IS FOR
--------------
On 2026-08-28 Food Safety News published "Hundreds sick and two dead in UK
Salmonella outbreaks": three S. Enteritidis clusters, 474 cases, two deaths,
imported eggs served in cafes and takeaways, and NO RECALL. The register
caught the article in the NEWS sheet and could not catch the outbreak,
because a recall-derived corpus has nothing to ingest when no notice is
issued.

But the register was already carrying the upstream notifications — a UK
egg-laying-facility S. Enteritidis positive, Dutch and Belgian egg
notifications, a Salmonella-Poland signal at p=0.00007. Nobody connected
them, because connecting them was a person reading two tabs side by side.

That is the job this agent does: read a news item, extract the epidemiology,
find the rows in the register that plausibly belong to the same picture, and
write a PROPOSAL. It never opens recalls.xlsx.

WHAT IT MAY NOT DO
------------------
Assert a link. Naming candidate rows is retrieval; declaring them the same
event is a finding, and a finding needs evidence the agent does not have.
Every link_event proposal it emits carries `confidence` and the curator
requires hard evidence before applying one — see _contract.py.

    python -m pipeline.agents.outbreak_intel --since 3
    python -m pipeline.agents.outbreak_intel --url https://... --dry-run
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from pipeline.agents._contract import (  # noqa: E402
    Evidence, Proposal, write_proposals,
)

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
AGENT = "outbreak-intel"

EXTRACT_PROMPT = """You are reading one food-safety news article. Return JSON only.

Extract ONLY what the article states. Every field you cannot support from the
text must be null. Do not infer, do not average, do not fill gaps from your
own knowledge of the event. A null is correct; a plausible number is not.

{
  "is_outbreak": true|false,
  "pathogen": "genus and species as written, or null",
  "serotypes": ["as written"],
  "cases": int|null,
  "deaths": int|null,
  "hospitalisations": int|null,
  "countries": ["..."],
  "implicated_food": "as written, or null",
  "companies": ["only if the article names them"],
  "agencies": ["UKHSA, ECDC, FSA, ..."],
  "period": "as written, or null",
  "recall_issued": true|false|null,
  "quotes": ["<=200 chars verbatim, one per numeric claim above"]
}

Rules:
- `recall_issued` is false ONLY if the article says so; otherwise null.
- Every number in the object must appear in `quotes` verbatim.
- If the article is not about an outbreak, set is_outbreak false and stop.

ARTICLE:
"""


def _load_news(since_days: int) -> List[Dict[str, Any]]:
    import pandas as pd
    if not XLSX.exists():
        return []
    n = pd.read_excel(XLSX, "NEWS").fillna("")
    col = "Published (UTC)" if "Published (UTC)" in n.columns else n.columns[0]
    cutoff = datetime.now(timezone.utc) - timedelta(days=since_days)
    out = []
    for _, r in n.iterrows():
        try:
            pub = datetime.fromisoformat(str(r[col])[:19].replace(" ", "T"))
            pub = pub.replace(tzinfo=timezone.utc)
        except Exception:                                   # noqa: BLE001
            continue
        if pub >= cutoff:
            out.append(r.to_dict())
    return out


def _fetch(url: str) -> str:
    import requests
    r = requests.get(url, timeout=25,
                     headers={"User-Agent": "AFTS-FSIS/1.0 (+advfood.tech)"})
    r.raise_for_status()
    txt = re.sub(r"<script.*?</script>|<style.*?</style>", " ", r.text,
                 flags=re.S | re.I)
    txt = re.sub(r"<[^>]+>", " ", txt)
    return re.sub(r"\s+", " ", txt)[:18000]


def extract(url: str, text: str) -> Optional[Dict[str, Any]]:
    from review.claude_client import _call_claude, _strip_fences
    raw = _call_claude(EXTRACT_PROMPT + text, max_tokens=1500)
    if not raw:
        return None
    try:
        return json.loads(_strip_fences(raw))
    except Exception:                                       # noqa: BLE001
        return None


def _quotes_support_numbers(f: Dict[str, Any]) -> List[str]:
    """Every number claimed must appear verbatim in a quote.

    This is the cheapest possible hallucination check and it catches the
    failure that matters: a model that read '55 illnesses' and wrote 155.
    """
    joined = " ".join(str(q) for q in (f.get("quotes") or []))
    bad = []
    for k in ("cases", "deaths", "hospitalisations"):
        v = f.get(k)
        if v in (None, "", 0):
            continue
        if not re.search(rf"\b{int(v)}\b", joined):
            bad.append(f"{k}={v} appears in no quote")
    return bad


def candidates(findings: Dict[str, Any], window_days: int = 120
               ) -> List[Dict[str, Any]]:
    """Register rows that plausibly belong to the same picture.

    Retrieval only. Deliberately generous on recall and deliberately silent
    about causation: it returns rows a human should look at, ranked, and
    says nothing about whether they are the same event.
    """
    import pandas as pd
    if not XLSX.exists():
        return []
    df = pd.read_excel(XLSX, "Recalls").fillna("")
    df["_d"] = pd.to_datetime(df["Date"], errors="coerce")

    genus = (findings.get("pathogen") or "").split()[0].lower() if findings.get("pathogen") else ""
    countries = {c.lower() for c in (findings.get("countries") or [])}
    food = (findings.get("implicated_food") or "").lower()
    food_terms = [w for w in re.findall(r"[a-z]{4,}", food)][:4]
    serotypes = [s.lower() for s in (findings.get("serotypes") or [])]

    hi = datetime.now(timezone.utc).date()
    lo = hi - timedelta(days=window_days)
    out = []
    for _, r in df.iterrows():
        d = r["_d"]
        if pd.isna(d) or not (lo <= d.date() <= hi):
            continue
        blob = " ".join(str(r.get(c, "")) for c in
                        ("Product", "Reason", "Company", "Country", "Pathogen")).lower()
        score, why = 0, []
        if genus and genus in str(r.get("Pathogen", "")).lower():
            score += 3; why.append(f"pathogen genus {genus}")
        if countries and str(r.get("Country", "")).lower() in countries:
            score += 2; why.append("country named in the article")
        for s in serotypes:
            if s and s in blob:
                score += 3; why.append(f"serotype {s}"); break
        hits = [t for t in food_terms if t in blob]
        if hits:
            score += 2; why.append("food terms " + ", ".join(hits))
        if score >= 5:
            out.append({"url": str(r.get("URL", "")), "date": str(d)[:10],
                        "source": str(r.get("Source", "")),
                        "product": str(r.get("Product", ""))[:90],
                        "pathogen": str(r.get("Pathogen", "")),
                        "country": str(r.get("Country", "")),
                        "score": score, "why": "; ".join(why)})
    out.sort(key=lambda x: (-x["score"], x["date"]), reverse=False)
    return out[:25]


def build_proposals(url: str, title: str, findings: Dict[str, Any],
                    cands: List[Dict[str, Any]]) -> List[Proposal]:
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    news_ev = Evidence(kind="news_article", url=url,
                       quote=" | ".join((findings.get("quotes") or [])[:3])[:400],
                       retrieved_utc=now)
    props: List[Proposal] = []

    if not findings.get("is_outbreak"):
        return props

    # An outbreak with no recall is the case the register structurally
    # cannot hold. Say so explicitly rather than leaving a silent gap.
    if findings.get("recall_issued") is False:
        props.append(Proposal(
            action="flag", target={"news_url": url},
            reason=("Outbreak with NO product recall — nothing for a "
                    "recall-derived register to ingest. Logged so the gap is "
                    "visible instead of silent. " + (title or "")[:120]),
            evidence=[news_ev], confidence=0.9, agent=AGENT,
            changes={"epidemiology": {k: findings.get(k) for k in
                                      ("pathogen", "serotypes", "cases",
                                       "deaths", "hospitalisations",
                                       "countries", "implicated_food",
                                       "period", "agencies")}}))

    for c in cands:
        props.append(Proposal(
            action="link_event", target={"url": c["url"]},
            changes={"candidate_event": url},
            reason=("Candidate relation to the reported outbreak, on "
                    f"retrieval only: {c['why']}. NOT an assertion that this "
                    "row belongs to that outbreak — the curator requires "
                    "regulator evidence before any link is written."),
            evidence=[news_ev,
                      Evidence(kind="corpus_row", url=c["url"],
                               quote=f"{c['date']} {c['source']} {c['product']}",
                               retrieved_utc=now)],
            confidence=min(0.2 + 0.1 * c["score"], 0.75), agent=AGENT))
    return props


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--url", help="one article instead of the NEWS feed")
    ap.add_argument("--since", type=int, default=2, help="NEWS lookback days")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)

    items = ([{"Link": a.url, "Title": ""}] if a.url
             else [n for n in _load_news(a.since) if str(n.get("Link", "")).startswith("http")])
    if not items:
        print("no news items in window")
        return 0

    all_props: List[Proposal] = []
    for it in items:
        url, title = str(it.get("Link", "")), str(it.get("Title", ""))
        try:
            text = _fetch(url)
        except Exception as e:                              # noqa: BLE001
            print(f"  fetch failed {url}: {e}")
            continue
        f = extract(url, text)
        if not f:
            print(f"  no extraction {url}")
            continue
        bad = _quotes_support_numbers(f)
        if bad:
            # Refuse the whole extraction. A model that invented one number
            # is not trusted for the others.
            print(f"  REFUSED {url}: {'; '.join(bad)}")
            continue
        if not f.get("is_outbreak"):
            print(f"  not an outbreak: {title[:60]}")
            continue
        cands = candidates(f)
        props = build_proposals(url, title, f, cands)
        all_props.extend(props)
        print(f"  {title[:60] or url[:60]}: {f.get('cases')} cases, "
              f"recall_issued={f.get('recall_issued')}, "
              f"{len(cands)} candidate row(s), {len(props)} proposal(s)")

    if not all_props:
        print("nothing to propose")
        return 0
    if a.dry_run:
        print(json.dumps([p.proposal_id for p in all_props], indent=1))
        return 0
    p = write_proposals(all_props, AGENT)
    print(f"wrote {len(all_props)} proposal(s) -> {p}")
    print("NOTHING has been written to recalls.xlsx. Run the curator:")
    print(f"  python -m pipeline.agents.curator --proposals {p}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
