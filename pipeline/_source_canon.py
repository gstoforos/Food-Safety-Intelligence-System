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
    # 2026-10-04: the bare label the Italian gap finder writes, and the
    # label tools/monitored_sources.py publishes as the canonical one.
    # Both returned kind='unknown' from this module, so the docstring's
    # "ONE ministry, three spellings" was really five.
    "Salute": "IT-SALUTE",
    "Ministero della Salute (IT)": "IT-SALUTE",
    "Ministero Salute": "IT-SALUTE",
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
    # ── 2026-10-07. THE SIX LABELS THE WRITER NOW PRODUCES. ─────────────
    # merge_master.SOURCE_ALIASES gained six entries today so that the last
    # six fleet configs stop stamping a bare authority name onto published
    # rows (Hungary did it this morning). Every label the WRITER produces
    # has to be identifiable here —
    # tests/test_a_source_label_is_one_label::
    # test_every_label_the_writer_produces_is_a_known_regulator holds that
    # boundary — and four of the six were the Italy shape all over again:
    # the bare collector name was in this table while the registry's own
    # label, the one that actually reaches Recalls, was not.
    #
    #     "AFSCA" was here; "AFSCA (BE)" was not.
    #     "FDA PH" was here; "FDA (PH)" was not.
    #
    # ONE AGENCY PER ID, including the two that were renamed: NÉBIH was
    # merged into NKFH (hu.py records the 2026-09-30 move to nkfh.gov.hu),
    # and Estonia's VTA became PTA (pta.agri.ee), which is the host the
    # registry entry reads. Both names resolve to one identity so the
    # independence checks do not count a rename as two observations.
    "AFSCA (BE)": "BE-FAVV",
    "FAVV-AFSCA": "BE-FAVV",
    "NKFH (HU)": "HU-NKFH",
    "NÉBIH": "HU-NKFH",
    "NEBIH": "HU-NKFH",
    "VTA (EE)": "EE-VTA",
    "PTA": "EE-VTA",
    "HAH (HR)": "HR-HAH",
    "HAPIH": "HR-HAH",
    "FDA (GH)": "GH-FDA",
    "FDA Ghana": "GH-FDA",
    "FDA (PH)": "PH-FDA",
    # ── 2026-10-26. SIX MORE REGISTRY LABELS, THE ITALY SHAPE AGAIN. ─────
    # Measured on Recalls this morning: six labels that the registry
    # (tools/monitored_sources.SOURCES) names, that the writer publishes,
    # and that this table had never heard of — so canonical_source()
    # returned kind='unknown' for 30 published rows and the coverage and
    # independence checks counted four national authorities as "their own
    # unknown source":
    #
    #     'CAA (JP)'            15 rows      'Mattilsynet'        10 rows
    #     'MAST (IS)'            1 row       'NFSA (EG)'           1 row
    #     'Ruokavirasto (FI)'    1 row       'VFA (VN)'            1 row
    #
    # Vietnam is the exact defect the 2026-10-07 note above describes:
    # the bare collector name "VFA" was in this table, the registry's own
    # label "VFA (VN)" — the one that reaches Recalls — was not.
    #
    # These ids are also the keys verify_urls.HOST_FOR_CANONICAL_SOURCE
    # uses, so a label absent here is a host check that does not run.
    "CAA (JP)": "JP-CAA",
    "CAA": "JP-CAA",
    "Mattilsynet": "NO-MATTILSYNET",
    "Mattilsynet (NO)": "NO-MATTILSYNET",
    "MAST (IS)": "IS-MAST",
    "MAST": "IS-MAST",
    "NFSA (EG)": "EG-NFSA",
    "NFSA": "EG-NFSA",
    "Ruokavirasto (FI)": "FI-RUOKAVIRASTO",
    "Ruokavirasto": "FI-RUOKAVIRASTO",
    "VFA (VN)": "VN-VFA",
    # ── 2026-10-27. NEW ZEALAND, UNDER A LABEL NO REGISTRY USES. ─────────
    # "MPI NZ" is what pipeline/regulatory_domains.py and the two
    # gap_finder_tavily tables wrote (they now write "MPI (NZ)"). It
    # reached Recalls on 2026-10-24, and canonical_source() derived
    # "MPI-NZ" from it while "MPI (NZ)" maps to "NZ-MPI" — one regulator
    # with two canonical ids, so the near-duplicate and coverage checks saw
    # two independent observers, and
    # verify_urls.HOST_FOR_CANONICAL_SOURCE["NZ-MPI"] never ran on those
    # rows. Kept here permanently: the archive holds rows under the old
    # label and this module does not rewrite the Source column.
    "MPI NZ": "NZ-MPI",
    "MPI": "NZ-MPI",
    # ── 2026-10-27. THE SAME SWEEP, SEVEN MORE LABELS. ──────────────────
    # Found by tests/test_every_collector_table_label_is_a_registry_label,
    # which reads the three collector host->label tables instead of
    # waiting for a published row. Those tables wrote a bare authority name
    # that neither the registry nor this module knew — the Italy shape, for
    # the seventh and eighth time. The tables now write the registry label;
    # these are the identities those labels resolve to, plus the bare forms
    # so a row already queued under one is not orphaned.
    "FAVV": "BE-FAVV",
    "FSS (Scotland)": "UK-FSS",
    "FSS": "UK-FSS",
    "Thai FDA (TH)": "TH-FDA",
    "Thai FDA": "TH-FDA",
    "ThaiFDA": "TH-FDA",
    "SZPI": "CZ-SZPI",
    "Min. Salute": "IT-SALUTE",
    # Slovakia and the French ministry directorate were added to
    # tools/monitored_sources.SOURCES the same morning — both were
    # monitored (a scraper, a whitelisted host) and counted nowhere.
    "ŠVPS (SK)": "SK-SVPS",
    "ŠVPS": "SK-SVPS",
    "SVPS": "SK-SVPS",
    "DGAL (FR)": "FR-DGAL",
    "DGAL": "FR-DGAL",
    # ── 2026-10-27. THE REMAINING THIRTY REGISTRY LABELS. ───────────────
    # Measured this morning against tools/monitored_sources.SOURCES: 30 of
    # the 78 registry labels had NO canonical identity. Every one is a
    # regulator the fleet monitors, and canonical_source() returned
    # kind='unknown' for all of them — which means two things at once: the
    # coverage denominator counted each as "its own unknown source", and
    # verify_urls.HOST_FOR_CANONICAL_SOURCE could not be reached for it, so
    # no row of theirs was ever host-checked.
    #
    # Two of the thirty stopped being hypothetical the same morning: the
    # offline promoter published a Livsmedelsverket (SE) row and a
    # Fodevarestyrelsen (DK) row, and
    # test_the_host_check_cannot_be_switched_off_by_a_rename went red for
    # both — found, as every previous recurrence of this defect was, by a
    # row that had ALREADY PUBLISHED.
    #
    # So the whole remainder is filed at once rather than one regulator per
    # incident. Every id below is DERIVED from the registry row itself (the
    # label gives the country code and the short name) and
    # tests/test_every_registry_label_has_an_identity.py re-derives the
    # claim, so registry entry 79 cannot be added without one.
    "ASAE (PT)": "PT-ASAE",
    "SECLU (LU)": "LU-SECLU",
    "Fødevarestyrelsen (DK)": "DK-FODEVARESTYRELSEN",
    "Livsmedelsverket (SE)": "SE-LIVSMEDELSVERKET",
    "ANSVSA (RO)": "RO-ANSVSA",
    "BFSA (BG)": "BG-BFSA",
    "UVHVVR (SI)": "SI-UVHVVR",
    "PVD (LV)": "LV-PVD",
    "VMVT (LT)": "LT-VMVT",
    "FSA BiH (BA)": "BA-FSABIH",
    "FVA (MK)": "MK-FVA",
    "ANSA (MD)": "MD-ANSA",
    "TGTHB (TR)": "TR-TGTHB",
    "ACHIPIA (CL)": "CL-ACHIPIA",
    "ISP (CL)": "CL-ISP",
    "DIGESA (PE)": "PE-DIGESA",
    "ARCSA (EC)": "EC-ARCSA",
    "MHLW (JP)": "JP-MHLW",
    "SAMR (CN)": "CN-SAMR",
    "TFDA (TW)": "TW-TFDA",
    "BPOM (ID)": "ID-BPOM",
    "KKM (MY)": "MY-KKM",
    "FSSAI (IN)": "IN-FSSAI",
    "SFDA (SA)": "SA-SFDA",
    "MoCCAE (AE)": "AE-MOCCAE",
    "MoPH (QA)": "QA-MOPH",
    "MoH (IL)": "IL-MOH",
    "ONSSA (MA)": "MA-ONSSA",
    "NAFDAC (NG)": "NG-NAFDAC",
    "KEBS (KE)": "KE-KEBS",
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
