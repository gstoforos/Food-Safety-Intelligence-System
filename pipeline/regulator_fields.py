#!/usr/bin/env python3
"""Recover the taxonomy the regulator already gave us.

WHY THIS EXISTS
---------------
The schema proposal of 2026-08-28 planned to have an AI reviewer infer
FoodCategory, NoticeType and a hazard-certainty grade from free text. For
a third of the register that work was already done — by RASFF, at source,
using its own controlled vocabulary — and the values were sitting unread
inside `Reason` and `Notes`:

    Reason : Listeria monocytogenes in halloumi from Cyprus; risk: serious;
             category: milk and milk products
    Notes  : [RASFF #2026.7533; classification: alert notification;
              category: milk and milk products; notifId=868574]

Measured on the corpus of 2026-08-28, over 510 RASFF rows:

    category         490/510   96%
    risk             489/510   96%
    classification   489/510   96%
    RASFF id         508/510  100%

Inferring any of that with a model would have been slower, more expensive
and less authoritative than a regex. Worse, it would have replaced the
regulator's judgement with a guess and left no way to tell them apart.

SCOPE — read before extending
-----------------------------
This module reads RASFF rows ONLY. CFIA and RappelConso publish category
fields on their web pages, but those fields are NOT in the text FSIS
stored: measured on the same corpus, CFIA carries them on 0 of 55 rows and
RappelConso on 5 of 650 (and those five look RASFF-derived). Recovering
them means re-fetching every notice, which is a different job with a
different cost. Do not add a CFIA branch here expecting it to find
anything.

    python -m pipeline.regulator_fields --report
    python -m pipeline.regulator_fields --write
"""
from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Optional

ROOT = Path(__file__).resolve().parent.parent
XLSX = ROOT / "docs" / "data" / "recalls.xlsx"

# Values stop at ';' (field separator), '[' (start of the next Notes stamp)
# and '~' (any join separator a caller may have introduced). An earlier
# draft of this parser stopped only at ';' and captured the trailing join
# marker into the value, which split "fruits and vegetables" into five
# spurious buckets. Anchor on the delimiters, not on length.
_STOP = r"[^;~\[\]\n]+"
_RE_CATEGORY = re.compile(r"category:\s*(" + _STOP + r")", re.I)
_RE_RISK = re.compile(r"\brisk:\s*(" + _STOP + r")", re.I)
_RE_CLASS = re.compile(r"classification:\s*(" + _STOP + r")", re.I)
_RE_ID = re.compile(r"RASFF\s*#\s*([0-9][0-9.]*)", re.I)
_RE_NOTIF = re.compile(r"notifId\s*=\s*(\d+)", re.I)

# ── RASFF product category → FSIS FoodCategory ───────────────────────────
# Every key below was observed in the corpus; there are 20 distinct values
# and all 20 are mapped. An unmapped category returns None rather than
# "other", so a new RASFF vocabulary term shows up as a gap instead of
# being silently absorbed.
CATEGORY_MAP: Dict[str, str] = {
    "poultry meat and poultry meat products": "meat-raw",
    "meat and meat products (other than poultry)": "meat-raw",
    "nuts, nut products and seeds": "nuts-seeds",
    "fruits and vegetables": "fresh-produce",
    "vegetables and vegetable products": "fresh-produce",
    "milk and milk products": "dairy-other",
    "herbs and spices": "herbs-spices",
    "cereals and bakery products": "bakery-cereal",
    "prepared dishes and snacks": "prepared-meals",
    "fish and fish products": "fish-seafood",
    "bivalve molluscs and products thereof": "fish-seafood",
    "crustaceans and products thereof": "fish-seafood",
    "cephalopods and products thereof": "fish-seafood",
    "eggs and egg products": "eggs-egg-products",
    "cocoa and cocoa preparations, coffee and tea": "beverages",
    "dietetic foods, food supplements and fortified foods": "supplements",
    "ices and desserts": "confectionery-snacks",
    "soups, broths, sauces and condiments": "sauces-condiments",
    "other food product / mixed": "other",
}
# NOTE on meat: RASFF's "poultry meat" says nothing about whether the
# product is raw or ready-to-eat, and mapping it to meat-rte would assert
# a processing state the notice does not carry. meat-raw is the honest
# default for a commodity category; ProcessType stays unknown.

CLASSIFICATION_MAP: Dict[str, str] = {
    "alert notification": "consumer-recall",
    "border rejection notification": "border-rejection",
    "information notification for attention": "information",
    "information notification for follow-up": "information",
}

# RASFF grades risk itself. Adopt its scale verbatim rather than inventing
# one: these are the notifying authority's words, not our inference.
RISK_VALUES = ("serious", "potentially serious", "not serious", "potential risk")


@dataclass
class RasffFields:
    rasff_id: Optional[str] = None
    notif_id: Optional[str] = None
    category_raw: Optional[str] = None
    food_category: Optional[str] = None
    risk: Optional[str] = None
    classification_raw: Optional[str] = None
    notice_type: Optional[str] = None

    def filled(self) -> int:
        return sum(1 for v in asdict(self).values() if v)


def _first(rx: re.Pattern, text: str) -> Optional[str]:
    m = rx.search(text or "")
    if not m:
        return None
    v = m.group(1).strip().strip(".").strip()
    return v or None


def parse_rasff(reason: str = "", notes: str = "") -> RasffFields:
    """Pull the RASFF taxonomy out of a row's stored text.

    Reason and Notes are searched separately, not concatenated: joining
    them lets a value run past the end of one field and into the next.
    """
    out = RasffFields()
    for text in (str(reason or ""), str(notes or "")):
        out.rasff_id = out.rasff_id or _first(_RE_ID, text)
        out.notif_id = out.notif_id or _first(_RE_NOTIF, text)
        out.category_raw = out.category_raw or _first(_RE_CATEGORY, text)
        out.classification_raw = out.classification_raw or _first(_RE_CLASS, text)
        risk = _first(_RE_RISK, text)
        if risk and risk.lower() in RISK_VALUES:
            out.risk = out.risk or risk.lower()

    if out.category_raw:
        # A few rows carry trailing prose after the category
        # ("poultry meat. RASFF notification 852690."). Take the part
        # before the first full stop and re-test.
        key = out.category_raw.lower().split(".")[0].strip()
        out.food_category = CATEGORY_MAP.get(key)
        if out.food_category is None and key.startswith("poultry meat"):
            out.food_category = CATEGORY_MAP["poultry meat and poultry meat products"]
    if out.classification_raw:
        out.notice_type = CLASSIFICATION_MAP.get(out.classification_raw.lower())
    return out


# =============================================================================
# CLI
# =============================================================================

def main(argv=None) -> int:
    import pandas as pd

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--xlsx", default=str(XLSX))
    ap.add_argument("--report", action="store_true", default=True)
    a = ap.parse_args(argv)

    df = pd.read_excel(a.xlsx, "Recalls")
    rows = df[df["Source"].astype(str).str.strip() == "RASFF (EU)"]
    parsed = [parse_rasff(r.get("Reason"), r.get("Notes"))
              for _, r in rows.iterrows()]
    n = len(parsed)
    if not n:
        print("no RASFF rows")
        return 0

    def pct(f):
        k = sum(1 for p in parsed if getattr(p, f))
        return f"{k:>4}/{n}  {k / n * 100:5.1f}%"

    print(f"RASFF rows: {n} of {len(df)} ({n / len(df) * 100:.0f}% of the register)")
    for f in ("rasff_id", "category_raw", "food_category", "risk",
              "classification_raw", "notice_type"):
        print(f"  {f:20} {pct(f)}")

    from collections import Counter
    for f in ("food_category", "risk", "notice_type"):
        c = Counter(getattr(p, f) for p in parsed if getattr(p, f))
        print(f"\n{f}:")
        for k, v in c.most_common():
            print(f"  {v:>4}  {k}")

    unmapped = Counter(p.category_raw for p in parsed
                       if p.category_raw and not p.food_category)
    if unmapped:
        print("\nUNMAPPED categories — extend CATEGORY_MAP:")
        for k, v in unmapped.most_common():
            print(f"  {v:>4}  {k}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
