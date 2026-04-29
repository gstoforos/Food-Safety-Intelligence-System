"""
RappelConso HTML freshness check — every-hour real-time backstop.

WHY THIS EXISTS (different reason from rappelconso_freshness.py)
----------------------------------------------------------------
The data.gouv.fr open-data API is updated via batch sync ONCE EVERY
24 HOURS (per Data.gouv.fr documentation). But the consumer-facing site
rappel.conso.gouv.fr publishes fiches in REAL TIME — minutes after
DGCCRF approval. So:

    rappelconso_freshness.py        — API path, lags up to 24h
    rappelconso_html_freshness.py   — HTML path, ~0 lag (THIS FILE)

That's the path that would have caught the 4 Listeria recalls on
28/04/2026 (fiches 22141, 22142, 22143, 22145) and the 3 Alternaria
recalls on 27/04 (fiches 22107, 22108, 22109) within the same hour
they were published — instead of George finding them by hand at 23:22.

HOW IT WORKS
------------
RappelConso publishes fiches into category pages indexed by Alimentation
sub-category (sub-IDs from the live site, audit 2026-04-29):

    /categorie/2    Additifs alimentaires
    /categorie/7    Alcool et vin
    /categorie/13   Aliments diététiques et nutrition
    /categorie/18   Aliments pour animaux domestiques
    /categorie/21   Aliments pour animaux d'élevage
    /categorie/25   Aliments pour bébés
    /categorie/31   Beurres / margarines / huiles
    /categorie/36   Boissons non alcoolisées
    /categorie/40   Cacao, café et thé
    /categorie/45   Céréales et produits de boulangerie
    /categorie/51   Eaux
    /categorie/55   Escargots et grenouilles
    /categorie/58   Fruits et légumes
    /categorie/61   Herbes et épices
    /categorie/64   Lait et produits laitiers
    /categorie/70   Miel et gelée royale
    /categorie/73   Noix et graines
    /categorie/76   Oeufs et produits à base d'oeufs
    /categorie/79   Plats préparés et snacks
    /categorie/80   Produits de la pêche et d'aquaculture
    /categorie/85   Produits sucrés
    /categorie/89   Soupes, sauces et condiments
    /categorie/94   Viandes
    /categorie/100  Autres

Each page lists the most recent ~10 fiches in that subcategory. Page 1 of
each subcategory always has TODAY'S new fiches if any. We parse the
listing HTML with BeautifulSoup, extract (fiche_id, title, brand, date,
risque, motif), and append anything not yet in Recalls/Pending.

Cost: 0 €. ~24 unauthenticated GETs per hour to rappel.conso.gouv.fr,
which is a public government site with no rate limits documented.

CLI
---
    python -m review.rappelconso_html_freshness            # all subcats
    python -m review.rappelconso_html_freshness --cats 79,73,94  # quick
    python -m review.rappelconso_html_freshness --dry-run

Dependencies: beautifulsoup4 (already in requirements.txt for other
HTML scrapers).
"""
from __future__ import annotations
import argparse
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Dict, Any, Optional

import requests

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scrapers._models import (  # noqa: E402
    Recall, normalize_pathogen, normalize_country, infer_region, assign_tier,
)
from scrapers._pathogen_vocab import for_languages  # noqa: E402
from pipeline.merge_master import (  # noqa: E402
    load_existing, load_pending,
    append_to_pending, sort_rows, save_xlsx_with_pending,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("rappelconso-html-freshness")

DATA_DIR = ROOT / "docs" / "data"
XLSX_PATH = DATA_DIR / "recalls.xlsx"

BASE = "https://rappel.conso.gouv.fr"

# Alimentation subcategory IDs harvested from the live site 2026-04-29.
# Order roughly matches recall volume so high-traffic categories run first.
ALIMENTATION_CATS = [
    79,   # Plats préparés et snacks (high-volume — Sebastien fiches landed here)
    64,   # Lait et produits laitiers (high-volume — Gorgonzola fiches here)
    94,   # Viandes (high-volume)
    80,   # Produits de la pêche et d'aquaculture
    73,   # Noix et graines (Alternaria sunflower-seed fiches)
    58,   # Fruits et légumes
    45,   # Céréales et produits de boulangerie
    61,   # Herbes et épices
    85,   # Produits sucrés
    100,  # Autres
    89,   # Soupes, sauces et condiments
    36,   # Boissons non alcoolisées
    25,   # Aliments pour bébés
    13,   # Aliments diététiques et nutrition
    40,   # Cacao, café et thé
    18,   # Aliments pour animaux domestiques
    21,   # Aliments pour animaux d'élevage
    7,    # Alcool et vin
    31,   # Beurres / margarines / huiles
    2,    # Additifs alimentaires
    51,   # Eaux
    55,   # Escargots et grenouilles
    70,   # Miel et gelée royale
    76,   # Oeufs et produits à base d'oeufs
]

PATHOGEN_KEYWORDS = for_languages("en", "fr")

# Fiche page link pattern in listing HTML
FICHE_LINK_RX = re.compile(
    r'/fiche-rappel/(?P<fid>\d+)/Interne',
    re.IGNORECASE,
)

# Date format on listing cards: "28/04/2026"
LISTING_DATE_RX = re.compile(r'\b(\d{2})/(\d{2})/(\d{4})\b')


# ─────────────────────────────────────────────────────────────────────────
# HTTP helpers
# ─────────────────────────────────────────────────────────────────────────

UA = "Mozilla/5.0 (FSIS-html-freshness/1.0; +https://advfood.tech)"

def _get(url: str, *, timeout: int = 20) -> Optional[str]:
    try:
        r = requests.get(url, headers={"User-Agent": UA, "Accept": "text/html"},
                         timeout=timeout)
        if r.status_code != 200:
            log.warning("GET %s -> HTTP %d", url, r.status_code)
            return None
        return r.text
    except requests.RequestException as e:
        log.warning("GET %s failed: %s", url, e)
        return None


# ─────────────────────────────────────────────────────────────────────────
# Listing-page parser
# ─────────────────────────────────────────────────────────────────────────

def parse_listing(html: str) -> List[Dict[str, Any]]:
    """Extract fiche cards from a /categorie/{ID} listing page.

    Cards have this shape (audit 2026-04-29):
       <li>
         <h3><a href="/fiche-rappel/22141/Interne">Bruschetta</a></h3>
         <p><strong>Risques :</strong> Listeria monocytogenes ...</p>
         <p><strong>Motif :</strong> Potentiel présence de Listeria</p>
         LES ATELIERS DE SEBASTIEN
         28/04/2026
         Plats préparés et snacks
       </li>

    BeautifulSoup is more reliable than regex for HTML, but we keep
    a regex fallback so the script still runs if bs4 fails.
    """
    rows: List[Dict[str, Any]] = []
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        log.warning("beautifulsoup4 not installed — using regex fallback")
        return _parse_listing_regex(html)

    soup = BeautifulSoup(html, "html.parser")

    # Each card starts with an <a> pointing at /fiche-rappel/<id>/Interne.
    # Walk those anchors and inspect the surrounding container for the
    # other fields.
    for a in soup.find_all("a", href=FICHE_LINK_RX):
        m = FICHE_LINK_RX.search(a.get("href", ""))
        if not m:
            continue
        fid = m.group("fid")
        title = a.get_text(strip=True)

        # Walk up to the smallest ancestor that contains both the link
        # AND the date / risque / motif text. Try ancestors li/article/div.
        card = a
        for _ in range(5):
            card = card.parent
            if card is None:
                break
            txt = card.get_text(" ", strip=True)
            if "Risques" in txt and "Motif" in txt and LISTING_DATE_RX.search(txt):
                break
        if card is None:
            continue
        card_text = card.get_text(" ", strip=True)

        # Risque = text after "Risques :" up to "Motif"
        risk = ""
        rm = re.search(r"Risques\s*:?\s*(.+?)(?:\s*Motif|\s*\d{2}/\d{2}/\d{4}|$)",
                       card_text, re.IGNORECASE)
        if rm:
            risk = rm.group(1).strip()
        # Motif = text after "Motif :"
        motif = ""
        mm = re.search(r"Motif\s*:?\s*(.+?)(?:\s*\d{2}/\d{2}/\d{4}|$)",
                       card_text, re.IGNORECASE)
        if mm:
            motif = mm.group(1).strip()
        # Date
        date_iso = ""
        dm = LISTING_DATE_RX.search(card_text)
        if dm:
            date_iso = f"{dm.group(3)}-{dm.group(2)}-{dm.group(1)}"

        # Brand = best-effort. The card has the brand on its own line
        # right before the date. Heuristic: take the substring between
        # the motif and the date.
        brand = ""
        if motif and date_iso:
            after_motif = card_text.split(motif, 1)[-1]
            before_date = after_motif.split(LISTING_DATE_RX.search(after_motif).group(0), 1)[0] \
                if LISTING_DATE_RX.search(after_motif) else after_motif
            brand = before_date.strip(" -•·")
            # Keep just the last brand-shaped token (≤80 chars)
            brand = brand[-120:].strip(" -•·")

        rows.append({
            "fid":        fid,
            "title":      title,
            "risque":     risk,
            "motif":      motif,
            "date":       date_iso,
            "brand":      brand,
            "url":        f"{BASE}/fiche-rappel/{fid}/Interne",
        })
    return rows


def _parse_listing_regex(html: str) -> List[Dict[str, Any]]:
    """Lightweight fallback if BeautifulSoup isn't available."""
    rows = []
    seen = set()
    for m in FICHE_LINK_RX.finditer(html):
        fid = m.group("fid")
        if fid in seen:
            continue
        seen.add(fid)
        # Window 600 chars around the link, search for date + risque + motif
        start = max(0, m.start() - 200)
        end = m.end() + 600
        window = html[start:end]
        risk = ""
        rm = re.search(r"Risques\s*:?\s*</strong>\s*(.+?)\s*</p>", window, re.IGNORECASE | re.DOTALL)
        if rm:
            risk = re.sub(r"<[^>]+>", "", rm.group(1)).strip()
        motif = ""
        mm = re.search(r"Motif\s*:?\s*</strong>\s*(.+?)\s*</p>", window, re.IGNORECASE | re.DOTALL)
        if mm:
            motif = re.sub(r"<[^>]+>", "", mm.group(1)).strip()
        date_iso = ""
        dm = LISTING_DATE_RX.search(window)
        if dm:
            date_iso = f"{dm.group(3)}-{dm.group(2)}-{dm.group(1)}"
        rows.append({
            "fid": fid, "title": "", "risque": risk, "motif": motif,
            "date": date_iso, "brand": "",
            "url": f"{BASE}/fiche-rappel/{fid}/Interne",
        })
    return rows


# ─────────────────────────────────────────────────────────────────────────
# Pathogen-scope filter (uses centralised vocab)
# ─────────────────────────────────────────────────────────────────────────

def in_pathogen_scope(row: Dict[str, Any]) -> bool:
    blob = (row.get("risque", "") + " " + row.get("motif", "")).lower()
    return any(kw in blob for kw in PATHOGEN_KEYWORDS)


def to_recall(row: Dict[str, Any]) -> Recall:
    pathogen = normalize_pathogen(row.get("risque", "")) or normalize_pathogen(row.get("motif", ""))
    country = normalize_country("France")
    return Recall(
        Date=row.get("date") or datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        Source="RappelConso (FR)",
        Company=(row.get("brand") or "")[:120],
        Brand=(row.get("brand") or "—")[:120],
        Product=(row.get("title") or "")[:300],
        Pathogen=pathogen,
        Reason=(row.get("motif") or "")[:300],
        Class="Volontaire",
        Country=country,
        Region=infer_region(country),
        Tier=assign_tier(pathogen, 0),
        Outbreak=0,
        URL=row.get("url", ""),
        Notes=(
            f"[freshness backstop HTML "
            f"{datetime.now(timezone.utc).strftime('%Y-%m-%d')}; "
            f"fiche {row.get('fid', '?')}; from listing scrape]"
        ),
    ).normalize()


# ─────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────

def main(argv: Iterable[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--cats", default="",
                    help="Comma-separated subcategory IDs to sweep (default: all 24)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print findings, do NOT write to Pending")
    ap.add_argument("--xlsx", default=str(XLSX_PATH))
    ap.add_argument("--sleep", type=float, default=0.4,
                    help="Pause between category requests (default 0.4s, "
                         "polite to the gov.fr server)")
    args = ap.parse_args(list(argv) if argv is not None else None)

    xlsx_path = Path(args.xlsx)
    if not xlsx_path.exists():
        log.error("recalls.xlsx not found at %s", xlsx_path)
        return 2

    cats = (
        [int(c.strip()) for c in args.cats.split(",") if c.strip().isdigit()]
        if args.cats else list(ALIMENTATION_CATS)
    )
    log.info("Sweeping %d Alimentation subcategories on %s", len(cats), BASE)

    all_rows: List[Dict[str, Any]] = []
    seen_fids = set()
    for cid in cats:
        url = f"{BASE}/categorie/{cid}"
        html = _get(url)
        if not html:
            continue
        rows = parse_listing(html)
        new = 0
        for r in rows:
            if r["fid"] in seen_fids:
                continue
            seen_fids.add(r["fid"])
            all_rows.append(r)
            new += 1
        log.info("  /categorie/%-3d -> %d cards (%d new across run)", cid, len(rows), new)
        time.sleep(args.sleep)

    log.info("Total unique fiches scraped: %d", len(all_rows))

    in_scope = [r for r in all_rows if in_pathogen_scope(r)]
    log.info("Pathogen-scope fiches: %d / %d", len(in_scope), len(all_rows))

    # Compare to existing data
    approved = load_existing(xlsx_path)
    pending = load_pending(xlsx_path)
    have = set()
    for r in approved + pending:
        u = (r.get("URL") or "").strip()
        if u:
            have.add(u)

    missing: List[Recall] = []
    for row in in_scope:
        if row["url"] in have:
            continue
        missing.append(to_recall(row))

    if not missing:
        log.info("No gaps found — RappelConso HTML coverage complete.")
        return 0

    log.warning("FOUND %d MISSING RAPPELCONSO RECALLS (HTML path):", len(missing))
    for r in missing:
        log.warning("  %s | %s | %s | %s",
                    r.Date, (r.Brand or "—")[:25], r.Product[:40], r.URL)

    if args.dry_run:
        log.info("--dry-run: not writing.")
        return 0

    scraped_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    new_pending = append_to_pending(
        existing_pending=pending,
        approved=approved,
        new_recalls=missing,
        scraped_at=scraped_at,
    )
    added = len(new_pending) - len(pending)
    log.info("Appended %d rows to Pending (total pending=%d)", added, len(new_pending))

    save_xlsx_with_pending(
        approved_rows=sort_rows(approved),
        pending_rows=sort_rows(new_pending),
        xlsx_path=xlsx_path,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
