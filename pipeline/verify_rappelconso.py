#!/usr/bin/env python3
"""Reconcile every RappelConso row against the official DGCCRF open data.

WHY THIS EXISTS (audit 2026-08-02)
==================================
RappelConso is the single largest source in this database — over 500 rows,
roughly 40% of everything published — and until now the only thing checking
those rows was a language model reading a web page. That produced two
failure modes no LLM reviewer can be trusted to catch:

  1. CROSS-ROW CONTAMINATION. The url-gate sends rows to the model in
     batches and maps decisions back by a self-reported row_index. A shifted
     index writes a NEIGHBOUR's content onto a row: genuine text, correct
     grammar, plausible in isolation, describing a completely different
     recall. Ten rows were found holding verbatim RASFF notification text
     ("; risk: serious; category: …") on French fiches that never emit it.

  2. SAME-CLASS CONTAMINATION, which is worse because nothing flags it.
     Fiche 22184 (Salaisons Jouvin, merguez) carried Reason 'Germina brand
     "Brocoli Calabrese" seeds recalled due to possible contamination with
     pathogenic E. coli' while the official record says "présence de
     salmonelle". The Pathogen/Reason hazard-class guard stays silent —
     Salmonella and E. coli are both biological, the classes overlap, and
     the rule is conservative by design. Only the source can catch it.

There is no need to guess at any of this. The DGCCRF publishes every fiche
as open data under Licence Ouverte, and it is queryable per record:

    https://tabular-api.data.gouv.fr/api/resources/{RESOURCE}/data/?id__exact=N

`id` in that dataset IS the fiche number in the rappel.conso.gouv.fr URL, so
the join is exact — no fuzzy matching, no model, no tokens. This turns the
largest source in the database from "a model said it was fine" into a
deterministic reconciliation that either agrees with the French government
or names the field that does not.

WHAT IT CHECKS
--------------
    Reason   vs motif_rappel        — the contamination vector. Compared on
                                      content words after translation-tolerant
                                      normalisation, because our Reason is an
                                      English rendering, not a copy.
    Pathogen vs risques_encourus    — mapped through PATHOGEN_MAP below.
    Brand    vs marque_produit
    Class    vs nature_juridique_rappel
    Date     vs date_publication

Reason is scored by OVERLAP, not equality: a translated sentence will never
match byte-for-byte, and a checker that cries wolf gets switched off. What it
is really looking for is a Reason that shares NOTHING with the source — which
is the exact signature of contamination.

NETWORK NOTE
------------
This script needs outbound HTTPS to tabular-api.data.gouv.fr. It is written
to run in CI (the daily workflow) where that is available. It makes one
request per fiche with a polite delay, and caches responses to
docs/data/rappelconso-cache.json so a re-run costs nothing.

Publication lag is normal: the export trails the website by about a week, so
the newest fiches simply will not be there yet. Those are reported as
'pending publication', never as errors.

Usage
-----
    python -m pipeline.verify_rappelconso --report          # audit only
    python -m pipeline.verify_rappelconso --apply           # repair Reason/
                                                            # Brand/Class
    python -m pipeline.verify_rappelconso --since 2026-06-01
    python -m pipeline.verify_rappelconso --fiche 22184

--apply repairs ONLY the fields the official record is authoritative for and
never invents: it will not write a Company or Product that the open data does
not carry, and it never overwrites Pathogen (which was correct on all ten
contaminated rows found in the 2026-08-02 audit — it is the surrounding
fields that get overwritten). Pathogen disagreements are REPORTED for a human.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
import unicodedata
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

RESOURCE = "5a4e7174-657c-4920-af1f-3440a996837c"
API = ("https://tabular-api.data.gouv.fr/api/resources/"
       f"{RESOURCE}/data/?id__exact={{fiche}}")
CACHE = ROOT / "docs" / "data" / "rappelconso-cache.json"
UA = "AFTS-FSIS/1.0 (+https://fsis.advfood.tech) reconciliation"

# risques_encourus -> the canonical Pathogen label used in this workbook.
# Ordered: the first substring that matches wins, so put the specific ones
# first ("escherichia coli shiga" before a bare "escherichia coli").
PATHOGEN_MAP = [
    ("listeria",                       "Listeria monocytogenes"),
    ("escherichia coli shiga",         "Shiga toxin-producing E. coli (STEC)"),
    ("stec",                           "Shiga toxin-producing E. coli (STEC)"),
    ("escherichia coli",               "Escherichia coli"),
    ("salmonella",                     "Salmonella"),
    ("clostridium botulinum",          "Clostridium botulinum"),
    ("botulisme",                      "Clostridium botulinum"),
    ("bacillus cereus",                "Bacillus cereus"),
    ("staphylocoque",                  "Staphylococcus aureus"),
    ("campylobacter",                  "Campylobacter"),
    ("norovirus",                      "Norovirus"),
    ("hépatite a",                     "Hepatitis A"),
    ("hepatite a",                     "Hepatitis A"),
    ("cronobacter",                    "Cronobacter"),
    ("vibrio",                         "Vibrio"),
    ("yersinia",                       "Yersinia"),
    ("histamine",                      "Histamine / scombrotoxin"),
    ("biotoxines marines",             "Marine biotoxin"),
    ("aflatoxine",                     "Aflatoxin"),
    ("ochratoxine",                    "Ochratoxin A"),
    ("patuline",                       "Patulin"),
    ("mycotoxine",                     "Mycotoxin"),
    ("inertes",                        "Foreign material"),
    ("corps étranger",                 "Foreign material"),
    ("hydrocarbures aromatiques",      "MOAH / MOSH"),
    ("éléments traces métalliques",    "Heavy metals"),
    ("produits phytosanitaires",       "Pesticide residues"),
    ("médicaments vétérinaires",       "Veterinary drug residues"),
    ("traitement vétérinaire",         "Veterinary drug residues"),
]

CLASS_MAP = {
    "volontaire (sans arrêté préfectoral)": "Voluntary",
    "imposé par arrêté préfectoral": "Mandatory",
}

# Words that carry no discriminating signal when comparing a French motif
# against its English rendering.
_STOP = frozenset("""
a an the of in on to and or de du des la le les un une dans sur au aux et
est pour par avec sans suite presence présence detection détection dans
produit produits due to recall recalled possible potential potentielle
possible risque risques risk level teneur taux lot lots analyse
""".split())


def _fold(s: str) -> str:
    s = unicodedata.normalize("NFD", str(s or "").lower())
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return re.sub(r"[^a-z0-9 ]+", " ", s)


def _content_words(s: str) -> set:
    return {w for w in _fold(s).split() if len(w) > 3 and w not in _STOP}


def fiche_of(url: str) -> Optional[str]:
    m = re.search(r"fiche-rappel/(\d+)", str(url or ""))
    return m.group(1) if m else None


def canonical_pathogen(risques: str) -> Optional[str]:
    r = _fold(risques)
    for needle, label in PATHOGEN_MAP:
        if _fold(needle) in r:
            return label
    return None


# ─── Fetch ──────────────────────────────────────────────────────────────────

def _load_cache() -> Dict[str, Any]:
    try:
        return json.loads(CACHE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_cache(cache: Dict[str, Any]) -> None:
    try:
        CACHE.parent.mkdir(parents=True, exist_ok=True)
        CACHE.write_text(json.dumps(cache, ensure_ascii=False, indent=1),
                         encoding="utf-8")
    except Exception as exc:                       # pragma: no cover
        print(f"  (cache write skipped: {exc})", file=sys.stderr)


def fetch_fiche(fiche: str, cache: Dict[str, Any],
                delay: float = 0.4) -> Optional[Dict[str, Any]]:
    """Return the official record for `fiche`, or None if not published yet.

    A miss is cached as the sentinel "__absent__" so a re-run does not re-ask
    for fiches the export genuinely does not carry (some ids are simply not
    in it — the 2026-08-02 audit found 22205 missing between 22204 and 22206).
    """
    if fiche in cache:
        hit = cache[fiche]
        return None if hit == "__absent__" else hit
    req = urllib.request.Request(API.format(fiche=fiche),
                                 headers={"User-Agent": UA,
                                          "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, ValueError, OSError) as exc:
        print(f"  fiche {fiche}: fetch failed ({type(exc).__name__}) — skipped",
              file=sys.stderr)
        return None
    finally:
        time.sleep(delay)
    rows = payload.get("data") or payload.get("records") or []
    rec = rows[0] if rows else None
    cache[fiche] = rec if rec else "__absent__"
    return rec


# ─── Compare ────────────────────────────────────────────────────────────────

def compare(row: Dict[str, Any], rec: Dict[str, Any]) -> List[Dict[str, str]]:
    """Return one finding per field that disagrees with the official record."""
    findings: List[Dict[str, str]] = []

    motif = str(rec.get("motif_rappel") or "")
    ours = str(row.get("Reason") or "")
    src_w, our_w = _content_words(motif), _content_words(ours)
    # Only a claim of CONTAMINATION, never of imperfect translation: both
    # sides must carry real content and share none of it.
    if src_w and our_w and not (src_w & our_w):
        # Second chance via the hazard vocabulary — "présence de salmonelle"
        # against "Presence of Salmonella" shares no stemmed token in some
        # spellings, but both name the same organism.
        p_src = canonical_pathogen(str(rec.get("risques_encourus") or ""))
        if not (p_src and _fold(p_src.split()[0]) in _fold(ours)):
            findings.append({
                "field": "Reason", "severity": "contamination",
                "ours": ours[:150], "source": motif[:150],
                "note": "shares no content word with the official motif_rappel "
                        "— the signature of a neighbour's text written onto "
                        "this row"})

    p_src = canonical_pathogen(str(rec.get("risques_encourus") or ""))
    p_our = str(row.get("Pathogen") or "").strip()
    if p_src and p_our and _fold(p_src) != _fold(p_our):
        findings.append({
            "field": "Pathogen", "severity": "review",
            "ours": p_our, "source": str(rec.get("risques_encourus"))[:120],
            "note": f"official risques_encourus maps to {p_src!r} — NOT "
                    f"auto-repaired, a human decides"})

    b_src = str(rec.get("marque_produit") or "").strip()
    b_our = str(row.get("Brand") or "").strip()
    if b_src and b_src not in ("sans marque", "pas de marque", "inconnu",
                               "neutre"):
        if _fold(b_src) != _fold(b_our):
            findings.append({"field": "Brand", "severity": "repairable",
                             "ours": b_our, "source": b_src, "note": ""})

    c_src = CLASS_MAP.get(str(rec.get("nature_juridique_rappel") or "").strip())
    c_our = str(row.get("Class") or "").strip()
    if c_src and c_src != c_our:
        findings.append({"field": "Class", "severity": "repairable",
                         "ours": c_our, "source": c_src, "note": ""})

    d_src = str(rec.get("date_publication") or "")[:10]
    d_our = str(row.get("Date") or "")[:10]
    if d_src and d_our and d_src != d_our:
        findings.append({"field": "Date", "severity": "review",
                         "ours": d_our, "source": d_src,
                         "note": "official date_publication — NOT auto-repaired"})
    return findings


def _titlecase(s: str) -> str:
    return " ".join(w if w.isupper() else w.capitalize() for w in s.split())


# ─── Main ───────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--xlsx", default=str(ROOT / "docs" / "data" / "recalls.xlsx"))
    ap.add_argument("--apply", action="store_true",
                    help="repair Reason/Brand/Class from the official record")
    ap.add_argument("--report", action="store_true", help="audit only (default)")
    ap.add_argument("--since", default=None, help="only rows dated >= YYYY-MM-DD")
    ap.add_argument("--fiche", default=None, help="check one fiche and stop")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--delay", type=float, default=0.4)
    args = ap.parse_args()

    from pipeline.merge_master import (
        load_existing, load_pending, sort_rows, save_xlsx_with_pending,
        mirror_json_from_xlsx,
    )
    xlsx = Path(args.xlsx)
    approved = load_existing(xlsx)

    targets = []
    for row in approved:
        f = fiche_of(row.get("URL"))
        if not f:
            continue
        if args.fiche and f != args.fiche:
            continue
        if args.since and str(row.get("Date") or "") < args.since:
            continue
        targets.append((f, row))
    if args.limit:
        targets = targets[:args.limit]

    print(f"Reconciling {len(targets)} RappelConso row(s) against the official "
          f"DGCCRF open data\n")

    cache = _load_cache()
    absent, clean, flagged, repaired = [], 0, [], 0

    for fiche, row in targets:
        rec = fetch_fiche(fiche, cache, delay=args.delay)
        if not rec:
            absent.append((fiche, str(row.get("Date"))))
            continue
        findings = compare(row, rec)
        if not findings:
            clean += 1
            continue
        flagged.append((fiche, row, findings))
        print(f"fiche {fiche}  {row.get('Date')}  "
              f"{str(row.get('Company'))[:40]}")
        for f in findings:
            print(f"    {f['severity']:14} {f['field']:9} "
                  f"ours   = {f['ours'][:88]!r}")
            print(f"    {'':14} {'':9} source = {f['source'][:88]!r}")
            if f["note"]:
                print(f"    {'':14} {'':9} {f['note']}")
        print()

        if args.apply:
            changed = False
            for f in findings:
                if f["field"] == "Reason" and f["severity"] == "contamination":
                    # The official motif is authoritative; keep it in French
                    # rather than machine-translating, and say so.
                    row["Reason"] = str(rec.get("motif_rappel") or "").strip()
                    changed = True
                elif f["field"] == "Brand":
                    row["Brand"] = _titlecase(f["source"])
                    changed = True
                elif f["field"] == "Class":
                    row["Class"] = f["source"]
                    changed = True
            if changed:
                row["Notes"] = (
                    str(row.get("Notes") or "").strip()
                    + f" [rappelconso-reconcile: repaired from the official "
                      f"DGCCRF open-data record for fiche {fiche}]"
                ).strip()[:1600]
                repaired += 1

    _save_cache(cache)

    print("─" * 70)
    print(f"agrees with the official record : {clean}")
    print(f"disagrees                       : {len(flagged)}")
    print(f"not in the export yet           : {len(absent)}"
          f"{'  (publication lag is normal)' if absent else ''}")
    if absent[:8]:
        print("    " + ", ".join(f"{f} ({d})" for f, d in absent[:8])
              + (" …" if len(absent) > 8 else ""))

    contamination = sum(1 for _, _, fs in flagged
                        for f in fs if f["severity"] == "contamination")
    if contamination:
        print(f"\n!! {contamination} row(s) carry a Reason that shares NO content "
              f"with the official motif_rappel.\n"
              f"   That is cross-row contamination, not a translation "
              f"difference.")

    if args.apply and repaired:
        save_xlsx_with_pending(sort_rows(approved), sort_rows(load_pending(xlsx)),
                               xlsx)
        mirror_json_from_xlsx(xlsx, ROOT / "docs" / "data" / "recalls.json")
        print(f"\n✓ repaired {repaired} row(s) + recalls.json mirrored")
    elif args.apply:
        print("\n(nothing to repair)")

    return 1 if contamination else 0


if __name__ == "__main__":
    raise SystemExit(main())
