"""BLV (CH) Switzerland — FSVO recalls and public warnings.

WHY THIS REPLACES THE TEN-LINE VERSION (audit 2026-10-10)
=========================================================
The previous file was the ten-line ``GenericLLMScraper`` wrapper that
``scrapers/_listing.py`` describes as the GIS failure shape. It ran every
day, fetched the listing with HTTP 200, and returned nothing:
``docs/data/scraper_health.json`` reported ``BLV (CH)`` with
``last_yield 0`` and ``last_nonzero_utc 2026-09-06``; scraper-health
called it ``OK_EMPTY``. The last Swiss row in Recalls was dated
2026-09-04, and the rows before it carried the note "added from the
regulator listing, which the BLV scraper had missed".

What it missed in those five weeks, all on the FSVO's own listing
(https://www.blv.admin.ch/blv/de/home/lebensmittel-und-ernaehrung/
rueckrufe-und-oeffentliche-warnungen.html, read 2026-10-10):

    11.09.2026  public warning    Salmonella, Coop Naturaplan organic shrimp
    23.09.2026  public warning    Bisphenol A, Royal Orient bamboo shoots
    02.10.2026  RecallSwiss 1146  Kägi Butterbiscuits, metal fragments
    07.10.2026  RecallSwiss 1147  Rapelli tartare, Escherichia coli
    07.10.2026  RecallSwiss 1148  Le Grand'Joie pork shank terrine,
                                  suspected botulism, TWO human cases
                                  (Canton of Valais communiqué, 07.10.2026)
    09.10.2026  RecallSwiss 1150  Cokoc gummies, mould

The botulism recall reached AFTS only as a Food Safety News article in
the NEWS sheet.

WHY IT RETURNED NOTHING
-----------------------
Two structural reasons, either of which is enough:

1. Since 2026-08 every FSVO *recall* links off-site to the RecallSwiss
   portal (``https://www.recallswiss.admin.ch/customer-access/#Recalls/<n>``),
   a single-page application. The shared deterministic floor
   (``_listing.extract_links``) is same-host only, so even with a
   ``DETAIL_URL_RE`` it would have dropped every recall link.
2. The listing renders a recall's date INSIDE its link text
   ("… zurück — 07.10.2026"), while ``_listing.nearest_date`` looks BEHIND
   the anchor first — which on this page is the previous entry's date.
   Each recall would have been stamped with its neighbour's date.

THIS VERSION
============
- Keeps the LLM path, and adds a BLV-specific deterministic parse as a
  FLOOR, merged by URL exactly as the shared floor does.
- Accepts exactly two detail shapes: RecallSwiss ``#Recalls/<n>`` (off-host,
  on purpose) and FSVO news items ``/<lang>/newnsb/<id>``.
- Date: the date inside the link text when there is one (recalls);
  otherwise the nearest date BEHIND the link, bounded by the previous
  matched link so a neighbour's date can never be taken (warnings render
  "23. September 2026" above their heading). German/French/Italian month
  names are parsed here, locally — the shared table is English-only on
  purpose.
- Company, Product and hazard are read from the FSVO's own sentence
  ("<Firma> ruft das Produkt «…» wegen <Gefahr> zurück"). Nothing is
  inferred beyond that sentence; an unrecognised hazard is left empty for
  review, and allergen-only, cold-chain and date-labelling notices are not
  emitted at all (outside AFTS scope).
- Reason is written in English (the published-Reason rule); the FSVO's
  original German sentence is kept in Notes.
- Loud: every run logs how many entries the floor found and how many it
  added, prefixed ``BLV:``.
"""
from __future__ import annotations

import logging
import re
from datetime import date
from typing import Dict, List, Optional

from scrapers._base import GenericLLMScraper

log = logging.getLogger(__name__)

LISTING_URL = ("https://www.blv.admin.ch/blv/de/home/lebensmittel-und-ernaehrung/"
               "rueckrufe-und-oeffentliche-warnungen.html")
_SITE = "https://www.blv.admin.ch"

#: The two detail shapes on the FSVO listing, matched against the href as
#: written in the markup.
_HREF = re.compile(r'href\s*=\s*["\']([^"\']+)["\']', re.I)
_DETAIL = re.compile(
    r"(?:recallswiss\.admin\.ch/customer-access/?#Recalls/\d+"
    r"|(?:^|/)(?:de|fr|it|en)/newnsb/[A-Za-z0-9_-]{8,})",
    re.I,
)

_TAGS = re.compile(r"<[^>]+>")
_WS = re.compile(r"\s+")

_NUMERIC_DATE = re.compile(r"\b(\d{1,2})\.(\d{1,2})\.(20\d{2})\b")
_WORD_DATE = re.compile(r"\b(\d{1,2})\.?\s+([A-Za-zÄÖÜäöüéèûô]+)\s+(20\d{2})\b")

#: DE / FR / IT month names — local to this agency, which publishes in all
#: three. The shared table in _listing.py is English-only by design.
_MONTHS = {
    "januar": 1, "jänner": 1, "janvier": 1, "gennaio": 1,
    "februar": 2, "février": 2, "fevrier": 2, "febbraio": 2,
    "märz": 3, "maerz": 3, "mars": 3, "marzo": 3,
    "april": 4, "avril": 4, "aprile": 4,
    "mai": 5, "maggio": 5,
    "juni": 6, "juin": 6, "giugno": 6,
    "juli": 7, "juillet": 7, "luglio": 7,
    "august": 8, "août": 8, "aout": 8, "agosto": 8,
    "september": 9, "septembre": 9, "settembre": 9,
    "oktober": 10, "octobre": 10, "ottobre": 10,
    "november": 11, "novembre": 11,
    "dezember": 12, "décembre": 12, "decembre": 12, "dicembre": 12,
}

#: Ordered: the first matching needle wins, so specific before general
#: (STEC before generic E. coli; metal before bare "Fremdkörper").
_HAZARDS = (
    (("botulis", "botulinum"), "Clostridium botulinum"),
    (("listeri",), "Listeria monocytogenes"),
    (("salmonell",), "Salmonella"),
    (("stec", "shiga", "ehec", "vtec"), "Shiga toxin-producing E. coli (STEC)"),
    (("escherichia coli", "e. coli", "e.coli"), "Escherichia coli (generic)"),
    (("norovirus",), "Norovirus"),
    (("hepatitis a",), "Hepatitis A virus"),
    (("schimmel", "moisissure", "muffa"), "Mold"),
    (("aflatoxin",), "Aflatoxin"),
    (("ochratoxin",), "Ochratoxin"),
    (("bisphenol",), "Bisphenol A (chemical contaminant)"),
    (("cadmium",), "Cadmium (heavy metal)"),
    (("quecksilber", "mercure"), "Mercury (heavy metal)"),
    (("ethylenoxid", "oxyde d'éthylène"), "Ethylene oxide"),
    (("pestizid", "pflanzenschutz"), "Pesticide residues"),
    (("metall",), "Foreign material (metal)"),
    (("glassplitter", "glasscherben", "glasstück", "éclats de verre"),
     "Foreign material (glass)"),
    (("kunststoff", "plastik", "plastique"), "Foreign material (plastic)"),
    (("fremdkörper", "corps étranger"), "Foreign material"),
)

#: Notices outside the AFTS scope whatever else they say. Applied only
#: when no in-scope hazard matched.
_OUT_OF_SCOPE = (
    "nicht deklariert", "non déclaré", "non dichiarat",       # allergen-only
    "kühlkette", "chaîne du froid", "catena del freddo",      # cold chain
    "verbrauchsdatum", "haltbarkeitsdatum", "falsches datum",  # date labelling
)

#: "<Firma> ruft das Produkt «…» [und «…»] wegen <Gefahr> zurück"
_RECALL_SENTENCE = re.compile(
    r"^(?P<company>.+?)\s+ruft\s+(?:das|die)\s+Produkte?\s+(?P<products>.+?)\s+"
    r"(?:wegen|aufgrund\s+von|aufgrund|infolge)\s+(?P<hazard>.+?)\s+zurück\b",
    re.I,
)
_GUILLEMETS = re.compile(r"«([^»]+)»")
_WARNING_PREFIX = re.compile(r"^(?:Mehr\s+über\s+)?«?\s*Öffentliche\s+Warnung:\s*", re.I)


def _clean(text: str) -> str:
    return _WS.sub(" ", _TAGS.sub(" ", text or "")).strip()


def _strip_trailing_date(title: str) -> str:
    return re.sub(r"\s*[—–-]\s*\d{1,2}\.\d{1,2}\.20\d{2}\s*$", "", title).strip()


def parse_date(text: str) -> Optional[str]:
    """First real date in `text` (DD.MM.YYYY, or "23. September 2026")."""
    for m in _NUMERIC_DATE.finditer(text or ""):
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1))).isoformat()
        except ValueError:
            continue
    for m in _WORD_DATE.finditer(text or ""):
        month = _MONTHS.get(m.group(2).lower())
        if not month:
            continue
        try:
            return date(int(m.group(3)), month, int(m.group(1))).isoformat()
        except ValueError:
            continue
    return None


def _last_date_in(text: str) -> Optional[str]:
    """The real date nearest the END of `text`."""
    found = []
    for pat in (_NUMERIC_DATE, _WORD_DATE):
        for m in pat.finditer(text or ""):
            iso = parse_date(m.group(0))
            if iso:
                found.append((m.start(), iso))
    return max(found)[1] if found else None


def hazard_of(text: str) -> str:
    low = (text or "").lower()
    for needles, label in _HAZARDS:
        if any(n in low for n in needles):
            return label
    return ""


def out_of_scope(text: str) -> bool:
    low = (text or "").lower()
    return not hazard_of(text) and any(n in low for n in _OUT_OF_SCOPE)


def read_title(title: str) -> Dict[str, str]:
    """Company / Product / hazard text from the FSVO's own sentence.

    Only what the sentence says. A warning title names no company, so
    Company stays empty for review rather than being guessed.
    """
    t = _strip_trailing_date(_clean(title))
    m = _RECALL_SENTENCE.search(t)
    if m:
        products = _GUILLEMETS.findall(m.group("products")) or [m.group("products")]
        return {
            "kind": "recall",
            "company": m.group("company").strip(),
            "product": "; ".join(p.strip() for p in products),
            "hazard_text": m.group("hazard").strip(),
            "title": t,
        }
    w = _WARNING_PREFIX.sub("", t).rstrip("»").strip()
    if w != t or t.lower().startswith("öffentliche warnung"):
        hz, sep, prod = w.partition(" in ")          # "<Gefahr> in <Produkt>"
        return {
            "kind": "warning",
            "company": "",
            "product": prod.strip() if sep else "",
            "hazard_text": hz.strip() if sep else w,
            "title": "Öffentliche Warnung: " + w,
        }
    return {"kind": "unknown", "company": "", "product": "", "hazard_text": t, "title": t}


def parse_listing(html: str) -> List[Dict[str, str]]:
    """Every recall / warning on the FSVO listing, in page order.

    Pure string handling — no network — so it is tested from a fixture.
    """
    html = html or ""
    matches = []
    for m in _HREF.finditer(html):
        href = m.group(1).strip()
        if not _DETAIL.search(href):
            continue
        if href.startswith("/"):
            href = _SITE + href
        matches.append((m.start(), m.end(), href))

    out: List[Dict[str, str]] = []
    seen = set()
    prev_end = 0
    for start, end, href in matches:
        open_end = html.find(">", end)
        close = html.find("</a>", open_end + 1) if open_end >= 0 else -1
        anchor_text = _clean(html[open_end + 1:close]) if 0 <= open_end < close else ""
        # Date inside the link text first (recalls); otherwise behind the
        # link, but never further back than the previous matched link.
        when = parse_date(anchor_text) or _last_date_in(_clean(html[prev_end:start])) or ""
        prev_end = close if close > 0 else end
        key = href.rstrip("/").lower()
        if key in seen:
            continue
        seen.add(key)
        out.append({"url": href, "title": anchor_text, "date": when})
    return out


class BLVScraper(GenericLLMScraper):
    AGENCY = "BLV (CH)"
    COUNTRY = "Switzerland"
    INDEX_URLS = [LISTING_URL]
    LANGUAGE = "de"

    def hazard_from_title(self, title: str) -> str:  # noqa: D401
        return hazard_of(title)

    def _merge_deterministic(self, html, url, llm_rows, cutoff):
        """FSVO floor: RecallSwiss recalls + public warnings. Never raises."""
        try:
            entries = parse_listing(html)
        except Exception as exc:  # noqa: BLE001
            log.exception("BLV: listing parse failed: %s", exc)
            return llm_rows
        if not entries:
            log.warning("BLV: %s carried no RecallSwiss or newnsb link — the "
                        "listing markup changed; check the page", url)
            return llm_rows

        have = {str(getattr(r, "URL", "") or "").rstrip("/").lower() for r in llm_rows}
        added = []
        skipped_scope = 0
        for e in entries:
            if e["url"].rstrip("/").lower() in have:
                continue
            if e["date"]:
                try:
                    if date.fromisoformat(e["date"]) < cutoff:
                        continue
                except (TypeError, ValueError):
                    pass
            info = read_title(e["title"])
            if out_of_scope(info["title"]):
                skipped_scope += 1
                continue
            hazard = hazard_of(info["hazard_text"]) or hazard_of(info["title"])
            kind = "recall" if info["kind"] == "recall" else "public warning"
            added.append(self._new_recall(
                Date=e["date"],
                Company=info["company"],
                Product=info["product"] or info["title"],
                Pathogen=hazard,
                Reason=f"{hazard} — FSVO {kind}" if hazard else "",
                URL=e["url"],
                Notes=f'listing (deterministic parse); FSVO title: "{info["title"]}"',
            ))
        log.warning("BLV: listing parse found %d entr(ies); %d added beside the "
                    "LLM path's %d; %d out of scope (allergen / cold chain / date)",
                    len(entries), len(added), len(llm_rows), skipped_scope)
        return list(llm_rows) + added
