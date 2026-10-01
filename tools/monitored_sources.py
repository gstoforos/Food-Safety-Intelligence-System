"""Every official source the pipeline monitors — the one list the dashboard
counts.

    "also number of sources...40? we have add more and more" — operator, 2026-10-01

The dashboard's Sources tile counted distinct Source values among PUBLISHED
rows (40 on 2026-10-01): a regulator that is scraped every day but has not yet
issued an in-scope recall did not exist on it. The header said "66 sources",
typed by hand months ago. Both were wrong.

This list is the official regulators and EU-level systems the scrapers,
official-feed collectors and the gap-finder fleet read. News sites and
aggregators (Food Safety News, Outbreak News Today, …) are leads, not
sources, and are not counted.

When a collector for a new regulator is added, add it here and run:

    python -m tools.monitored_sources        # rewrites docs/data/sources.json

tests/test_monitored_sources.py fails if a gap-finder country or a published
Source label is missing from this list.
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "data" / "sources.json"

# (label as published in Source, country / scope, regulator domain(s),
#  space-separated when a regulator publishes on more than one host)
SOURCES = [
    # ── EU-level ──
    ("RASFF (EU)", "European Union", "webgate.ec.europa.eu"),
    ("EFSA", "European Union", "efsa.europa.eu"),
    ("ECDC", "European Union", "ecdc.europa.eu"),
    # ── Europe ──
    ("RappelConso (FR)", "France", "rappel.conso.gouv.fr"),
    ("BVL (DE)", "Germany", "lebensmittelwarnung.de"),
    ("Ministero della Salute (IT)", "Italy", "salute.gov.it"),
    ("AESAN (ES)", "Spain", "aesan.gob.es"),
    ("ASAE (PT)", "Portugal", "asae.gov.pt"),
    ("EFET (GR)", "Greece", "efet.gr"),
    ("AFSCA (BE)", "Belgium", "favv-afsca.be"),
    ("NVWA (NL)", "Netherlands", "nvwa.nl"),
    ("SECLU (LU)", "Luxembourg", "securite-alimentaire.public.lu"),
    ("AGES (AT)", "Austria", "ages.at"),
    ("BLV (CH)", "Switzerland", "blv.admin.ch"),
    ("FSA (UK)", "United Kingdom", "food.gov.uk"),
    ("FSS (Scotland)", "United Kingdom", "foodstandards.gov.scot"),
    ("FSAI (IE)", "Ireland", "fsai.ie"),
    ("Fødevarestyrelsen (DK)", "Denmark", "foedevarestyrelsen.dk"),
    ("Livsmedelsverket (SE)", "Sweden", "livsmedelsverket.se"),
    ("Mattilsynet", "Norway", "mattilsynet.no"),
    ("Ruokavirasto (FI)", "Finland", "ruokavirasto.fi"),
    ("MAST (IS)", "Iceland", "mast.is"),
    ("GIS (PL)", "Poland", "gov.pl"),
    ("SZPI (CZ)", "Czechia", "szpi.gov.cz"),
    ("NKFH (HU)", "Hungary", "nkfh.gov.hu nebih.gov.hu"),  # NÉBIH's successor; both hosts read
    ("ANSVSA (RO)", "Romania", "ansvsa.ro"),
    ("BFSA (BG)", "Bulgaria", "bfsa.bg"),
    ("HAH (HR)", "Croatia", "hapih.hr"),
    ("UVHVVR (SI)", "Slovenia", "gov.si"),
    ("VTA (EE)", "Estonia", "pta.agri.ee"),
    ("PVD (LV)", "Latvia", "pvd.gov.lv"),
    ("VMVT (LT)", "Lithuania", "vmvt.lt"),
    ("FSA BiH (BA)", "Bosnia and Herzegovina", "fsa.gov.ba"),
    ("FVA (MK)", "North Macedonia", "fva.gov.mk"),
    ("ANSA (MD)", "Moldova", "ansa.gov.md"),
    ("TGTHB (TR)", "Türkiye", "tarimorman.gov.tr"),
    # ── North America ──
    ("FDA", "United States", "fda.gov"),
    ("USDA FSIS", "United States", "fsis.usda.gov"),
    ("CDC", "United States", "cdc.gov"),
    ("CFIA", "Canada", "inspection.canada.ca"),
    ("MAPAQ QC", "Canada", "mapaq.gouv.qc.ca"),
    ("COFEPRIS (MX)", "Mexico", "gob.mx"),
    # ── Latin America ──
    ("ANVISA (BR)", "Brazil", "gov.br"),
    ("ANMAT (AR)", "Argentina", "argentina.gob.ar"),
    ("ACHIPIA (CL)", "Chile", "achipia.gob.cl"),
    ("ISP (CL)", "Chile", "ispch.cl"),
    ("INVIMA (CO)", "Colombia", "invima.gov.co"),
    ("DIGESA (PE)", "Peru", "digesa.minsa.gob.pe"),
    ("ARCSA (EC)", "Ecuador", "controlsanitario.gob.ec"),
    ("MSP (UY)", "Uruguay", "gub.uy"),
    # ── Asia ──
    ("CAA (JP)", "Japan", "caa.go.jp"),
    ("MHLW (JP)", "Japan", "mhlw.go.jp"),
    ("MFDS (KR)", "South Korea", "mfds.go.kr"),
    ("SAMR (CN)", "China", "samr.gov.cn"),
    ("CFS (HK)", "Hong Kong", "cfs.gov.hk"),
    ("TFDA (TW)", "Taiwan", "fda.gov.tw"),
    ("SFA (SG)", "Singapore", "sfa.gov.sg"),
    ("Thai FDA (TH)", "Thailand", "fda.moph.go.th"),
    ("VFA (VN)", "Vietnam", "vfa.gov.vn"),
    ("BPOM (ID)", "Indonesia", "pom.go.id"),
    ("FDA (PH)", "Philippines", "fda.gov.ph"),
    ("KKM (MY)", "Malaysia", "moh.gov.my"),
    ("FSSAI (IN)", "India", "fssai.gov.in"),
    # ── Middle East ──
    ("SFDA (SA)", "Saudi Arabia", "sfda.gov.sa"),
    ("MoCCAE (AE)", "United Arab Emirates", "moccae.gov.ae"),
    ("MoPH (QA)", "Qatar", "moph.gov.qa"),
    ("MoH (IL)", "Israel", "health.gov.il"),
    # ── Africa ──
    ("NFSA (EG)", "Egypt", "nfsa.gov.eg"),
    ("ONSSA (MA)", "Morocco", "onssa.gov.ma"),
    ("NAFDAC (NG)", "Nigeria", "nafdac.gov.ng"),
    ("FDA (GH)", "Ghana", "fdaghana.gov.gh"),
    ("KEBS (KE)", "Kenya", "kebs.org"),
    ("NCC (ZA)", "South Africa", "thencc.org.za"),
    ("COMESA", "Eastern and Southern Africa", "comesa.int"),
    # ── Oceania ──
    ("FSANZ (AU)", "Australia", "foodstandards.gov.au"),
    ("MPI (NZ)", "New Zealand", "mpi.govt.nz"),
]

#: Published Source labels that are not a monitored regulator of their own:
#: a news report citing a regulator, or a one-off provider label.
NOT_A_SOURCE_PREFIXES = ("Food Safety News", "BeaconBio")


def payload() -> dict:
    countries = {c for _, c, _ in SOURCES
                 if c not in ("European Union", "Eastern and Southern Africa")}
    return {"count": len(SOURCES), "countries": len(countries),
            "sources": [{"label": l, "country": c, "domain": d}
                        for l, c, d in SOURCES]}


def main() -> int:
    OUT.write_text(json.dumps(payload(), ensure_ascii=False, indent=1) + "\n",
                   encoding="utf-8")
    p = payload()
    print(f"{p['count']} sources, {p['countries']} countries -> {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
