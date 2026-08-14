"""Canonical source identity — one regulator, one label.

WHY THIS EXISTS (audit 2026-08-14)
==================================
The register carries 48 distinct Source labels for roughly 35 actual
authorities. Any coverage, baseline or seasonality figure computed on the
raw label is wrong before it starts, because the same agency is counted as
two or three independent observers.

Measured on the live corpus, 2026-08-14:

    FAVV (BE)              9  ┐ ONE agency. Federaal Agentschap voor de
    AFSCA                  2  ┘ Veiligheid van de Voedselketen (Dutch) /
                               Agence fédérale pour la Sécurité de la
                               Chaîne alimentaire (French). The split is
                               TEMPORAL, not editorial: every "FAVV (BE)"
                               row is dated 23 Jan - 24 Jun and every
                               "AFSCA" row 29-30 Jul, so a scraper change
                               in late July renamed the label mid-series.
                               Treated as two sources, Belgium looks like
                               it lost one feed and gained another.

    BVL (DE)              19  ┐ ONE authority. Bundesamt für
    BVL                    1  │ Verbraucherschutz und
    Lebensmittelwarnung.de 2  ┘ Lebensmittelsicherheit and its own public
                               portal.

    Salute (IT)            2  ┐ ONE ministry, three spellings of
    Min. Salute (IT)       2  │ Ministero della Salute.
    Ministero della Salute 1  ┘

AGGREGATORS ARE NOT SOURCES
---------------------------
Thirteen rows carry "CFS (HK) - aggregator (<origin>)": Hong Kong's Centre
for Food Safety re-publishing another regulator's notice —

    RappelConso FR 7, MPI NZ 2, UK FSA 1, SFA SG 1, MHLW Japan 1, Czech 1

These are RE-REPORTS. Counting them as independent observations
double-counts one event and inflates apparent jurisdictional reach. They
are excluded from coverage denominators, NOT deleted: the row is still a
real notice and the origin label records what it mirrors.

(An earlier analysis put this at 8 rows. It is 13 — the count only comes
out right once every "aggregator (...)" variant is enumerated rather than
matched by prefix.)

WHAT THIS MODULE IS NOT
-----------------------
It does not rewrite the Source column. The raw label is evidence of which
scraper produced a row and stays as written. This maps label -> canonical
identity at ANALYSIS time, which is where the distinction matters.
"""
from __future__ import annotations

import re
from typing import Dict, Optional, Tuple

# raw Source label -> canonical source id
CANONICAL: Dict[str, str] = {
    # Belgium — one agency, two languages, split temporally by a scraper
    # rename in late July 2026.
    "FAVV (BE)": "BE-FAVV",
    "AFSCA": "BE-FAVV",
    # Germany — authority plus its own public portal.
    "BVL (DE)": "DE-BVL",
    "BVL": "DE-BVL",
    "Lebensmittelwarnung.de": "DE-BVL",
    # Italy — three spellings of one ministry.
    "Salute (IT)": "IT-SALUTE",
    "Min. Salute (IT)": "IT-SALUTE",
    "Ministero della Salute": "IT-SALUTE",
    # Canada — CFIA is federal; MAPAQ is the Québec provincial authority
    # and is genuinely separate, so it is NOT merged.
    "CFIA": "CA-CFIA",
    "MAPAQ QC": "CA-MAPAQ",
    # United States — FDA and USDA FSIS are separate agencies with
    # separate jurisdictions. CDC is surveillance, not a recall issuer.
    "FDA": "US-FDA",
    "USDA FSIS": "US-FSIS",
    "CDC": "US-CDC",
    "FDA PH": "PH-FDA",          # Philippines FDA — NOT the US FDA
    # Everything else, one label one source.
    "RappelConso (FR)": "FR-RAPPELCONSO",
    "RASFF (EU)": "EU-RASFF",
    "FSAI (IE)": "IE-FSAI",
    "FSA (UK)": "UK-FSA",
    "AESAN (ES)": "ES-AESAN",
    "FSANZ (AU)": "AU-FSANZ",
    "BLV (CH)": "CH-BLV",
    "EFET (GR)": "GR-EFET",
    "CFS (HK)": "HK-CFS",
    "NCC (ZA)": "ZA-NCC",
    "NVWA (NL)": "NL-NVWA",
    "COFEPRIS (MX)": "MX-COFEPRIS",
    "AGES (AT)": "AT-AGES",
    "MPI (NZ)": "NZ-MPI",
    "SFA (SG)": "SG-SFA",
    "ANVISA (BR)": "BR-ANVISA",
    "MFDS (KR)": "KR-MFDS",
    "EFSA": "EU-EFSA",
    "ECDC": "EU-ECDC",
    "GIS (PL)": "PL-GIS",
    "VFA": "VN-VFA",
    "SZPI (CZ)": "CZ-SZPI",
    "BeaconBio (TW)": "TW-BEACONBIO",
    "MSP (UY)": "UY-MSP",
    "COMESA": "COMESA",
    "INVIMA (CO)": "CO-INVIMA",
    "ANMAT (AR)": "AR-ANMAT",
}

# A re-report of another regulator's notice. Not an independent observation.
_AGGREGATOR = re.compile(r"\s*-\s*aggregator\s*\(([^)]*)\)\s*$", re.I)

# Media, not a regulator.
_NEWS = re.compile(r"food\s*safety\s*news|via\s+google\s+news|citing\b", re.I)


def canonical_source(label: str) -> Tuple[str, str, Optional[str]]:
    """Map a raw Source label to (canonical_id, kind, mirrors).

    kind is one of:
        "regulator"   an independent observation — counts toward coverage
        "aggregator"  a re-report; `mirrors` names the origin it copies
        "news"        media reporting; never an independent observation
        "unknown"     label not in the table — counts as its own source but
                      is reported so the table can be extended rather than
                      silently guessed at

    Callers computing coverage MUST drop "aggregator" and "news" from the
    denominator. Counting Hong Kong re-publishing a RappelConso fiche as a
    second observation is how a register ends up claiming reach it does
    not have.
    """
    s = str(label or "").strip()
    if not s:
        return "", "unknown", None

    m = _AGGREGATOR.search(s)
    if m:
        base = _AGGREGATOR.sub("", s).strip()
        base_id = CANONICAL.get(base, base.upper().replace(" ", "-"))
        return base_id, "aggregator", m.group(1).strip()

    if _NEWS.search(s):
        return "NEWS", "news", None

    if s in CANONICAL:
        return CANONICAL[s], "regulator", None

    return s.upper().replace(" ", "-"), "unknown", None


def is_independent(label: str) -> bool:
    """True when this row is an independent regulator observation."""
    return canonical_source(label)[1] == "regulator"
