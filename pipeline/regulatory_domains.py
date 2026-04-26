"""
pipeline/regulatory_domains.py — single source of truth: which URL hosts
are regulators-of-record (allowed in Recalls) vs news/aggregator sites
(NEWS sheet only, NEVER Recalls).

Why this module exists
======================
The 2026-04 audit found 27+ news-aggregator URLs (produktwarnung.eu,
foodsafetynews.com, en.sedaily.com, capitalfm.co.ke, briefly.co.za, …)
sitting in the Recalls sheet, tagged with regulator Source labels like
"BVL (DE)" or "NCC (ZA)". Root cause: gap-finders without real web
search hallucinated the agency URL — and the URL gate let them through
because the URL was *alive* (200 OK), just wrong domain.

Fix: a hard regulator-domain whitelist enforced at the URL gate. Any
Pending row whose URL is NOT on REGULATOR_DOMAINS does not get promoted
to Recalls.

Identity
========
A URL is a regulator URL if its host (with leading "www." stripped)
either equals one of the keys in REGULATOR_DOMAINS or ends with
"." + key. So "www.fda.gov" matches "fda.gov", and
"recalls-rappels.canada.ca" matches the explicit subdomain key.
"""
from __future__ import annotations
from typing import Dict, Optional, Tuple
from urllib.parse import urlparse


# ─────────────────────────────────────────────────────────────────────
# REGULATOR domains — the only hosts allowed in the Recalls sheet
# Mapping: host → (source_label, country)
# ─────────────────────────────────────────────────────────────────────
REGULATOR_DOMAINS: Dict[str, Tuple[str, str]] = {
    # ── North America ─────────────────────────────────────────────────
    "fda.gov":                      ("FDA",           "United States"),
    "accessdata.fda.gov":           ("FDA",           "United States"),
    "fsis.usda.gov":                ("USDA FSIS",     "United States"),
    "cdc.gov":                      ("CDC",           "United States"),
    "inspection.canada.ca":         ("CFIA",          "Canada"),
    "recalls-rappels.canada.ca":    ("CFIA",          "Canada"),
    "quebec.ca":                    ("MAPAQ",         "Canada"),
    # ── Europe (EU + non-EU) ──────────────────────────────────────────
    "food.gov.uk":                  ("FSA",           "United Kingdom"),
    "foodstandards.gov.scot":       ("FSS",           "United Kingdom"),
    "fsai.ie":                      ("FSAI",          "Ireland"),
    "rappelconso.gouv.fr":          ("RappelConso",   "France"),
    "rappel.conso.gouv.fr":         ("RappelConso",   "France"),
    "agriculture.gouv.fr":          ("DGAL",          "France"),
    "aesan.gob.es":                 ("AESAN",         "Spain"),
    "lebensmittelwarnung.de":       ("BVL",           "Germany"),
    "bvl.bund.de":                  ("BVL",           "Germany"),
    "bfr.bund.de":                  ("BfR",           "Germany"),
    "ages.at":                      ("AGES",          "Austria"),
    "salute.gov.it":                ("Min. Salute",   "Italy"),
    "nvwa.nl":                      ("NVWA",          "Netherlands"),
    "favv-afsca.be":                ("FAVV",          "Belgium"),
    "afsca.be":                     ("FAVV",          "Belgium"),
    "foedevarestyrelsen.dk":        ("Fødevarestyrelsen", "Denmark"),
    "livsmedelsverket.se":          ("Livsmedelsverket",  "Sweden"),
    "mattilsynet.no":               ("Mattilsynet",   "Norway"),
    "ruokavirasto.fi":              ("Ruokavirasto",  "Finland"),
    "gis.gov.pl":                   ("GIS",           "Poland"),
    "nebih.gov.hu":                 ("NEBIH",         "Hungary"),
    "ansvsa.ro":                    ("ANSVSA",        "Romania"),
    "bfsa.bg":                      ("BFSA",          "Bulgaria"),
    "szpi.gov.cz":                  ("SZPI",          "Czech Republic"),
    "svps.sk":                      ("ŠVPS",          "Slovakia"),
    "mast.is":                      ("MAST",          "Iceland"),
    "asae.gov.pt":                  ("ASAE",          "Portugal"),
    "efet.gr":                      ("EFET",          "Greece"),
    "webgate.ec.europa.eu":         ("RASFF",         "EU"),
    "food.ec.europa.eu":            ("DG SANTE",      "EU"),
    "efsa.europa.eu":               ("EFSA",          "EU"),
    "ecdc.europa.eu":               ("ECDC",          "EU"),
    "blv.admin.ch":                 ("BLV",           "Switzerland"),
    "bag.admin.ch":                 ("BAG",           "Switzerland"),
    "admin.ch":                     ("BLV",           "Switzerland"),
    # ── Asia-Pacific ──────────────────────────────────────────────────
    "foodstandards.gov.au":         ("FSANZ",         "Australia"),
    "mpi.govt.nz":                  ("MPI NZ",        "New Zealand"),
    "cfs.gov.hk":                   ("CFS",           "Hong Kong"),
    "mfds.go.kr":                   ("MFDS",          "South Korea"),
    "mhlw.go.jp":                   ("MHLW",          "Japan"),
    "samr.gov.cn":                  ("SAMR",          "China"),
    "sfa.gov.sg":                   ("SFA",           "Singapore"),
    "fda.gov.ph":                   ("FDA PH",        "Philippines"),
    "fssai.gov.in":                 ("FSSAI",         "India"),
    "tfda.gov.tw":                  ("TFDA",          "Taiwan"),
    "bpom.go.id":                   ("BPOM",          "Indonesia"),
    "moh.gov.my":                   ("KKM",           "Malaysia"),
    "fda.moph.go.th":               ("ThaiFDA",       "Thailand"),
    "vfa.gov.vn":                   ("VFA",           "Vietnam"),
    # ── Latin America ─────────────────────────────────────────────────
    "argentina.gob.ar":             ("ANMAT",         "Argentina"),
    "anmat.gob.ar":                 ("ANMAT",         "Argentina"),
    "anvisa.gov.br":                ("ANVISA",        "Brazil"),
    "gov.br":                       ("ANVISA",        "Brazil"),
    "cofepris.gob.mx":              ("COFEPRIS",      "Mexico"),
    "gob.mx":                       ("COFEPRIS",      "Mexico"),
    "arcsa.gob.ec":                 ("ARCSA",         "Ecuador"),
    "digesa.gob.pe":                ("DIGESA",        "Peru"),
    "invima.gov.co":                ("INVIMA",        "Colombia"),
    "ispch.cl":                     ("ISP",           "Chile"),
    "msp.gub.uy":                   ("MSP",           "Uruguay"),
    # ── Middle East & Africa ──────────────────────────────────────────
    "moccae.gov.ae":                ("MOCCAE",        "United Arab Emirates"),
    "health.gov.il":                ("MOH",           "Israel"),
    "moph.gov.qa":                  ("MOPH",          "Qatar"),
    "sfda.gov.sa":                  ("SFDA",          "Saudi Arabia"),
    "tarimorman.gov.tr":            ("TGTHB",         "Turkey"),
    "nafdac.gov.ng":                ("NAFDAC",        "Nigeria"),
    "kebs.org":                     ("KEBS",          "Kenya"),
    "nfsa.gov.eg":                  ("NFSA",          "Egypt"),
    "onssa.gov.ma":                 ("ONSSA",         "Morocco"),
    "ncc.org.za":                   ("NCC",           "South Africa"),
}


# ─────────────────────────────────────────────────────────────────────
# NEWS / aggregator domains — never allowed in Recalls
# ─────────────────────────────────────────────────────────────────────
NEWS_DOMAINS = {
    # Global / English
    "foodsafetynews.com",
    "food-safety.com",
    "foodsafetymagazine.com",
    "outbreaknewstoday.com",
    "foodnavigator.com",
    "foodnavigator-asia.com",
    "foodnavigator-usa.com",
    "foodnavigator-latam.com",
    "fooddive.com",
    "just-food.com",
    "thepacker.com",
    "globalfoodrecall.com",
    "foodrecall.com",
    "recall.report",
    "reuters.com",
    "bloomberg.com",
    "ft.com",
    # Germany / Austria
    "produktwarnung.eu",
    "produktwarnung.de",
    "lebensmittelpraxis.de",
    "agrarheute.com",
    "blick.ch",
    # Italy
    "ilfattoalimentare.it",
    "ilsalvagente.it",
    "altroconsumo.it",
    # France / Spain / Portugal
    "rappel-aliments.fr",
    "elespanol.com",
    "infosalus.com",
    # Vietnam / Asia
    "vietnam.vnanet.vn",
    "vnexpress.net",
    "tuoitrenews.vn",
    "en.sedaily.com",
    "sedaily.com",
    "koreaherald.com",
    "koreatimes.co.kr",
    "japantimes.co.jp",
    # Africa
    "iol.co.za",
    "businesstech.co.za",
    "htxt.co.za",
    "timelessnews.co.za",
    "briefly.co.za",
    "capitalfm.co.ke",
    "the-star.co.ke",
    "dailymaverick.co.za",
    # Latin America
    "unotv.com",
    "xataka.com.mx",
    "chequeado.com",
    "infobae.com",
    "clarin.com",
    "ole.com.ar",
    # Independent aggregators / trade-bloc bulletins
    "beaconbio.org",
    "comesa.org",
}


# ─────────────────────────────────────────────────────────────────────
# Public helpers
# ─────────────────────────────────────────────────────────────────────
def _host(url: str) -> str:
    if not url or not url.startswith("http"):
        return ""
    h = (urlparse(url).netloc or "").lower()
    if h.startswith("www."):
        h = h[4:]
    return h


def _matches(host: str, domain_set) -> bool:
    """Match host against a set/dict of domain strings.
    Match if host == key or host endswith '.' + key (subdomain match).
    """
    if not host:
        return False
    for d in domain_set:
        if host == d or host.endswith("." + d):
            return True
    return False


def is_regulator_url(url: str) -> bool:
    """True if the URL host is a regulator-of-record."""
    return _matches(_host(url), REGULATOR_DOMAINS)


def is_news_url(url: str) -> bool:
    """True if the URL host is a known news outlet / aggregator."""
    return _matches(_host(url), NEWS_DOMAINS)


def lookup_regulator(url: str) -> Optional[Tuple[str, str]]:
    """Return (source_label, country) for a regulator URL, or None."""
    host = _host(url)
    if not host:
        return None
    if host in REGULATOR_DOMAINS:
        return REGULATOR_DOMAINS[host]
    for d, info in REGULATOR_DOMAINS.items():
        if host.endswith("." + d):
            return info
    return None


def is_promotable_to_recalls(url: str) -> bool:
    """The single rule that gates promotion Pending → Recalls.

    Promotable iff: regulator URL AND not on the news blocklist.
    News URLs and unclassified URLs stay in Pending.
    """
    return is_regulator_url(url) and not is_news_url(url)
