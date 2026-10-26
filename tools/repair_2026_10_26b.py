# -*- coding: utf-8 -*-
"""Operator review 2026-10-26: undo the stale upload of 2026-10-25 23:59.

WHAT HAPPENED
At 23:59 Athens on 2026-10-25 docs/data/recalls.xlsx was overwritten by the
DATA zip of that MORNING (base 97d8e02, commit 270e7b70) instead of the
evening one (base 196fd6c). The register went back ~13 hours:

  * one PUBLISHED row vanished from Recalls — BLV (CH) 2026-10-24, Lidl
    Schweiz AG smoked trout fillets, Listeria monocytogenes (published
    2026-10-25, in no sheet afterwards). Its absence also left it out of the
    W43 weekly build of 2026-10-26 02:12;
  * six Pending rows and eight archive rows (Rejected / Weekly_Rejected)
    vanished from every sheet. Archives are append-only;
  * the evening fix (tools/repair_2026_10_25c.py, Imperio Company, NAFDAC
    archive, SUPERSEDED banners) never reached main — it ships again with
    this commit.

WHAT THIS SCRIPT DOES
Re-inserts every lost row VERBATIM, field for field, from the register as it
stood at 270e7b70^ (git history), into the sheet it was in. Nothing is edited
or invented. A row is skipped if its (URL, Product) is already back in ANY
sheet, so the script is idempotent against a main that moves all day.
One exception to "same sheet": the Pending NAFDAC row whose own URL carries
a 2021 alert number goes to Rejected with tools/repair_2026_10_25c.py's
verdict, because that script runs before this one and would otherwise have
archived it.
NOTHING IS REMOVED FROM Recalls.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "recalls.json"
TODAY = "2026-10-26"

OLD_ALERT_KEYS = ("public-alert-no-0047-2021",)
OLD_ALERT_REASON = ("REJECTED — not food and not current: a NAFDAC alert whose "
                    "own URL carries an alert number from an earlier year "
                    "(medicine or cosmetic recall). Operator review 2026-10-25; "
                    "restored after the stale upload 2026-10-26.")

#: Copied verbatim from docs/data/recalls.xlsx at commit 270e7b70^.
LOST = json.loads(r"""[
 {
  "sheet": "Recalls",
  "row": {
   "Date": "2026-10-24",
   "Source": "BLV (CH)",
   "Company": "Lidl Schweiz AG",
   "Brand": "Nautica",
   "Product": "smoked trout fillets - natural",
   "Pathogen": "Listeria monocytogenes",
   "Reason": "public warning due to Listeria monocytogenes contamination",
   "Class": "Recall",
   "Country": "Switzerland",
   "Region": "Europe",
   "Tier": 1,
   "Outbreak": 0,
   "URL": "https://www.blv.admin.ch/de/newnsb/V67FO2BgSwqP",
   "Notes": "listing (deterministic parse); FSVO title: \"Öffentliche Warnung: Listerien in geräucherten Forellenfilets Natur der Marke Nautica, verkauft bei Lidl Schweiz\" [needs cleanup: Product not in English] [safety-hold: reviewed but not approved this run; not promoted] [review-agent 2026-10-25: pending_gap_v2 → pending_gap_v3 (Qwen verified; awaiting reviewer 3 confirmation)] [promote 2026-10-25: status 'pending_gap_v3'->'pending' — passes every publish-gate rule]",
   "DateAdded": "2026-10-25",
   "LastUpdated": "2026-10-25",
   "report_week": "W43",
   "FoodCategory": "fish-seafood",
   "ProcessType": "cured-smoked",
   "ConsumptionState": "ready-to-eat",
   "StorageCondition": "chilled",
   "PackagingType": "unknown",
   "PackagingForm": "unknown",
   "PreservationSystem": "cured-smoked",
   "HazardGroup": "pathogen-bacterial",
   "HazardCertainty": "unknown",
   "NoticeType": "consumer-recall",
   "SeverityClass": "not-classified",
   "EnrichedBy": "enrich-schema/1",
   "EnrichedAt": "2026-10-25",
   "EnrichmentTier": "tier2-structured"
  }
 },
 {
  "sheet": "Pending",
  "row": {
   "Date": "2026-10-24",
   "Source": "MPI NZ",
   "Company": "Arla protein pudding",
   "Brand": "Arla protein pudding",
   "Product": "due to possible glass fragments",
   "Pathogen": "Glass fragments",
   "Reason": "Glass fragments",
   "Class": "Recall",
   "Country": "New Zealand",
   "Region": "Oceania",
   "Tier": 3,
   "Outbreak": 0,
   "URL": "https://www.mpi.govt.nz/food-safety-home/food-recalls-and-complaints/recalled-food-products/arla-protein-pudding-vanilla-cookie",
   "Notes": "Arla protein pudding recalled due to possible glass fragments | MPI\n\n# Arla protein pudding recalled due to possible glass fragments\n\n Date: 24 October 2026 \n\n### Media contact\n\nMPI media team\n\n### Media email\n\nmedia@mpi.govt.nz\n\nNew Zealand Food Safety is supporting Hutchinsons Ltd in its recall of  [via Exa daily search, deterministic extract]  [via Exa daily search, deterministic extract] [url-agent 2026-10-25: pending_gap→pending_gap_v1 URL confirmed] [url-agent 2026-10-25: pending_gap_v1→pending_gap_v2 URL confirmed]",
   "ScrapedAt": "2026-10-25T08:12:33Z",
   "Status": "pending_gap_v2"
  }
 },
 {
  "sheet": "Pending",
  "row": {
   "Date": "2026-10-24",
   "Source": "NAFDAC (NG)",
   "Product": "Artemether/Lumefantrine 80mg/480mg",
   "Reason": "Suspected substandard and falsified",
   "Class": "Alert",
   "Country": "Nigeria",
   "Region": "Africa",
   "Tier": 3,
   "Outbreak": 0,
   "URL": "https://nafdac.gov.ng/public-alert-no-042-2026-alert-on-the-seizure-of-suspected-substandard-and-falsified-bppl-artemether-lumefantrine-80mg-480mg/",
   "Notes": "Discovered via news: punchng.com [locality: Nigeria] [safety-hold: reviewed but not approved this run; not promoted]",
   "ScrapedAt": "2026-10-25T18:45:59.768086+00:00",
   "Status": "pending_gap_v2"
  }
 },
 {
  "sheet": "Pending",
  "row": {
   "Date": "2026-10-22",
   "Source": "NAFDAC (NG)",
   "Product": "Proguanil (Projeanil and Re-granil) Tablet B.P 100mg",
   "Reason": "Counterfeit with fake NAFDAC registration number (04-6433)",
   "Class": "Alert",
   "Country": "Nigeria",
   "Region": "Africa",
   "Tier": 3,
   "Outbreak": 0,
   "URL": "https://nafdac.gov.ng/public-alert-no-023-2026-alert-on-counterfeit-brands-of-proguanil-projeanil-and-re-granil-tablet-b-p-100mg-with-fake-nafdac-registration-number-04-6433-found-in-nigeria/",
   "Notes": "Discovered via news: vanguardngr.com [locality: Nigeria] [safety-hold: reviewed but not approved this run; not promoted]",
   "ScrapedAt": "2026-10-25T18:47:00.021212+00:00",
   "Status": "pending_gap_v2"
  }
 },
 {
  "sheet": "Pending",
  "row": {
   "Date": "2026-10-16",
   "Source": "NAFDAC (NG)",
   "Product": "Medicines",
   "Reason": "Adulterated and substandard medicines (Recall ID 038/2026)",
   "Class": "Alert",
   "Country": "Nigeria",
   "Region": "Africa",
   "Tier": 3,
   "Outbreak": 0,
   "URL": "https://nafdac.gov.ng/public-alert-no-038-2026-alert-on-adulterated-and-substandard-medicines-nitras-kadin-2-acefyl-and-loratadine-24-recalled-by-the-drug-regulatory-authority-of-pakistan-drap/",
   "Notes": "Discovered via news: vanguardngr.com [locality: Nigeria] [safety-hold: reviewed but not approved this run; not promoted]",
   "ScrapedAt": "2026-10-25T18:49:54.837817+00:00",
   "Status": "pending_gap_v2"
  }
 },
 {
  "sheet": "Pending",
  "row": {
   "Date": "2026-10-12",
   "Source": "NAFDAC (NG)",
   "Company": "Leama Chemi Pharma (PVT) Ltd",
   "Product": "Pediatric Suspension",
   "Reason": "Substandard product",
   "Class": "Alert",
   "Country": "Nigeria",
   "Region": "Africa",
   "Tier": 3,
   "Outbreak": 0,
   "URL": "https://nafdac.gov.ng/public-alert-no-0047-2021-recall-of-substandard-lefin-pediatric-suspension-manufactured-by-m-s-leama-chemi-pharma-pvt-ltd/",
   "Notes": "Discovered via news: thisdaylive.com [locality: Nigeria] [reason-cleanup: the extractor's own empty-identifier template text was stripped from Reason — it names no identifier and would have been published verbatim. Same class of defect as the 'Recall ID 842632' prompt example named in publish-gate rule 2] [safety-hold: reviewed but not approved this run; not promoted]",
   "ScrapedAt": "2026-10-25T18:52:23.379028+00:00",
   "Status": "pending_gap_v2"
  }
 },
 {
  "sheet": "Weekly_Rejected",
  "row": {
   "Date": "2026-10-20",
   "Source": "NAFDAC (NG)",
   "Product": "NAFDAC Discloses Cause of Death of NYSC Member in Enugu Orientation Camp - Legit News",
   "Reason": "No official NAFDAC press-release URL found in the source article — discovery via news only; not promoted to Recalls.",
   "Country": "Nigeria",
   "Notes": "Discovered via news: legit.ng",
   "DateAdded": "2026-10-25T18:49:06.683621+00:00",
   "Week_Added": "2026-11-01",
   "RejectedBy": "gap_finder/ng/rules.py",
   "RejectionReason": "no-authority-url: no official NAFDAC press-release URL in the source article (news-only discovery)",
   "Reviewed": "N"
  }
 },
 {
  "sheet": "Rejected",
  "row": {
   "Source": "ASAE (PT)",
   "Product": "PSP cerca manifestantes no Rossio e ordena que tendas sejam retiradas",
   "Reason": "No official ASAE press-release URL found in the source article — discovery via news only; not promoted to Recalls.",
   "Country": "Portugal",
   "Notes": "Discovered via news: noticiasaominuto.com",
   "RejectedBy": "gap_finder/pt/rules.py"
  }
 },
 {
  "sheet": "Rejected",
  "row": {
   "Date": "2026-10-14",
   "Source": "ASAE (PT)",
   "Product": "Nódulo retirado do pulmão do papa é cancerígeno - RTP",
   "Reason": "No official ASAE press-release URL found in the source article — discovery via news only; not promoted to Recalls.",
   "Country": "Portugal",
   "Notes": "Discovered via news: rtp.pt",
   "RejectedBy": "gap_finder/pt/rules.py"
  }
 },
 {
  "sheet": "Rejected",
  "row": {
   "Date": "2026-10-16",
   "Source": "KEBS (KE)",
   "Product": "Kebs warns against misuse of quality mark on miraa juices - The Star, Kenya",
   "Reason": "No matching hazard category — defer to manual review.",
   "Country": "Kenya",
   "Notes": "Discovered via news: the-star.co.ke",
   "RejectedBy": "gap_finder/ke/rules.py"
  }
 },
 {
  "sheet": "Rejected",
  "row": {
   "Date": "2026-10-19",
   "Source": "KEBS (KE)",
   "Product": "Police seize 114 crates of suspected illicit alcohol in Kirinyaga - Citizen Digital",
   "Reason": "No matching hazard category — defer to manual review.",
   "Country": "Kenya",
   "Notes": "Discovered via news: citizen.digital",
   "RejectedBy": "gap_finder/ke/rules.py"
  }
 },
 {
  "sheet": "Rejected",
  "row": {
   "Date": "2026-10-21",
   "Source": "KEBS (KE)",
   "Product": "Government moves to seize assets of illicit alcohol cartels - KBC Digital",
   "Reason": "No matching hazard category — defer to manual review.",
   "Country": "Kenya",
   "URL": "https://news.google.com/rss/articles/CBMiigFBVV95cUxOTnQ4WVhRdWV4SnFuWVl3M0ZaUUYzQU45QmRvQnpMUi1LR1A2em9LVGRpckVmc1hxckpock5qWDRrUG9IaEEzUkx6R2RoOFVlMUtKRWpsM1JuSWhlb3hXTF9hOXhoNXRtOHlXWjd1cUJJU25nZmhGLVA5XzhxMHlZV2xWRnZCeFFyWHc?oc=5",
   "Notes": "Discovered via news: kbc.co.ke",
   "RejectedBy": "gap_finder/ke/rules.py"
  }
 },
 {
  "sheet": "Rejected",
  "row": {
   "Date": "2026-10-16",
   "Source": "KEBS (KE)",
   "Product": "KEBS warns against misuse of Standardisation Mark on jaba juice - KBC Digital",
   "Reason": "No matching hazard category — defer to manual review.",
   "Country": "Kenya",
   "URL": "https://news.google.com/rss/articles/CBMijwFBVV95cUxOWmx2QXgwZURqZDdCSzc0R3JrUWc1UnBxdmVFZUZaOVVheWI3bWIzR0hna3hCOVRaQmZWTXpZQWRkanlUNGJtT3p6Yk8yYTZGSHd2UFpMQmh2RTczeTNseUh6NDVwZDZzT00zWS1Fc0Y3WGowUEdDVnYzcjlrSjNyN0lQaC1XUkQxSzJ4YlZENA?oc=5",
   "Notes": "Discovered via news: kbc.co.ke",
   "RejectedBy": "gap_finder/ke/rules.py"
  }
 },
 {
  "sheet": "Rejected",
  "row": {
   "Date": "2026-10-15",
   "Source": "SFA (SG)",
   "Product": "Hyundai to recall over 94,600 US vehicles over seat motor fire risk, NHTSA says - CNA",
   "Reason": "No official SFA press-release URL found in the source article — discovery via news only; not promoted to Recalls.",
   "Country": "Singapore",
   "Notes": "Discovered via news: channelnewsasia.com",
   "RejectedBy": "gap_finder/sg/rules.py"
  }
 }
]""")


def _key(url, product):
    return (str(url or "").strip(), str(product or "").strip()[:60])


def main() -> int:
    import openpyxl
    wb = openpyxl.load_workbook(XLSX)
    present = set()
    for ws in wb.worksheets:
        head = [c.value for c in ws[1]]
        if "URL" not in head or "Product" not in head:
            continue
        ui, pi = head.index("URL"), head.index("Product")
        for row in ws.iter_rows(min_row=2, values_only=True):
            present.add(_key(row[ui], row[pi]))

    restored = {}
    for item in LOST:
        row, sheet = dict(item["row"]), item["sheet"]
        k = _key(row.get("URL"), row.get("Product"))
        if k in present:
            continue
        if sheet == "Pending" and any(x in str(row.get("URL", "")).lower()
                                      for x in OLD_ALERT_KEYS):
            sheet = "Rejected"
            row.update(RejectReason=OLD_ALERT_REASON,
                       RejectedBy="operator review " + TODAY,
                       RejectedAt=TODAY, Status="rejected")
        ws = wb[sheet]
        head = [c.value for c in ws[1]]
        ws.append([row.get(h) for h in head])
        present.add(k)
        restored[sheet] = restored.get(sheet, 0) + 1
        print(f"  restored -> {sheet}: {row.get('Source')} {row.get('Date')} "
              f"{str(row.get('Product'))[:50]!r}")
    wb.save(XLSX)
    wb.close()
    print("restored:", restored or "nothing (already present)")
    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    print("ROWS_REMOVED=0")
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
