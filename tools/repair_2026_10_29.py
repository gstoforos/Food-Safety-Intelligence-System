# -*- coding: utf-8 -*-
"""Morning fix pass 2026-10-29.

Everything here was read on a regulator page this morning or is a faithful
translation of text already on the row. NOTHING LEAVES Recalls.

A. FSANZ (AU) 2026-10-28, "Bomberry Various Products – 150g" — published
   with Company AND Brand both set to the recall TITLE. The register named
   a company that does not exist. The authority page names the supplier:
   "Atomic Changes Pty Ltd t/a Bomberry". Its Pathogen was the one-off
   string "Foreign Matter", which no subscriber alert term could reach
   (tests/test_alert_vocab.py). Company, Brand, Product, Pathogen and
   Reason are set from the page; the scraper defect that produced it is
   fixed in scrapers/oceania/fsanz.py in the same upload.

B. Six Products / one Reason published in a foreign language. Translated
   faithfully, originals kept in Notes, "needs cleanup" stamps dropped
   where the row is now clean. Two of the six carried no stamp at all —
   the detector misses short names, which is why the operator reads every
   Product published in the last 26 h by hand.

C. Two archive rows sharing a URL with a published recall and saying
   nothing about which copy wins (tests/test_a_recall_is_not_both_
   published_and_rejected.py). Annotated, never deleted.

D. Three Pending rows unblocked on a field whose value is on the row's own
   page, read today:
     GIS (PL) ostrzezenie ... substancji psychoaktywnej — the product is
       BRAIN WASH cannabis gummies and the substance is THC. Reviewer 1
       had REJECTED it as "not a microbial pathogen", but THC over a
       reference dose is explicitly in AFTS scope and the register already
       publishes six such rows out of RappelConso.
     SFDA warning 19788 — Arla Protein Pudding, glass fragments. The page
       names no importer, so Company takes the register's own
       "(not specified in ...)" form rather than a guess.
     NVWA ossenworst — Company "Albert Heijn" is in the row's own text and
       the Date is in the row's own URL path (/2026/10/22/).

E. Three Pending rows archived, each for a reason read on its own page:
     CFIA "Nazari Co brand Pistachio Inshell AA 26/28" — the notice is
       dated 2025-10-23 (published 2025-10-27).
     CFIA "Various pistachios recalled due to Salmonella" — published
       2025-09-24.
     Both carry Date 2026-10-28, the discovery date. The register's window
     begins 2026-01-02, so these are out-of-window 2025 notices, and the
     Date is a scrape date. A third copy ("Certain pistachios ...") is
     already status 'rejected' and is left alone — its page was not read.
     NAFDAC public-alert-no-003-2020, hand sanitizers — not a food (R1),
     and the 2020 alert number against a 2026-10-28 Date is what
     tests/test_alert_number_year_matches_date.py fires on.

F. Five CAA (JP) recalls the register does not hold, each read on its own
   recall.caa.go.jp detail page today. The detail pages carry no 掲載日, so
   Date is the page's own 対応開始日, as for every CAA row in the register.
   Queued through tools/add_manual_row.py; normal review path.
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "recalls.json"
TODAY = "2026-10-29"
BY = "morning fix pass " + TODAY

# --------------------------------------------------------------------------
# A + B — field fixes on published rows, keyed on the row's own URL
# --------------------------------------------------------------------------

BOMBERRY_URL = ("https://www.foodstandards.gov.au/food-recalls/recall-alert/"
                "bomberry-various-products-150g")

RECALLS_FIXES = [
    {
        "url": BOMBERRY_URL,
        "set": {
            "Company": "Atomic Changes Pty Ltd t/a Bomberry",
            "Brand": "Bomberry",
            "Product": (
                "Bomberry chocolate-coated berries, 150 g: Blueberries + "
                "Beligan Chocolate – Dark Chocolate; Blueberries + Beligan "
                "Chocolate – White and Milk Chocolate; Raspberries + Beligan "
                "Chocolate – White and Dark Chocolate; Raspberries + Beligan "
                "Chocolate – White and Milk Chocolate (all best-before dates "
                "up to and including 20 January 2028)"),
            "Pathogen": "Foreign material (metal)",
            "Reason": (
                "The presence of foreign matter (metal). Food products "
                "containing metal may cause illness/injury if consumed."),
        },
        "note": (
            "[morning-fix 2026-10-29: Company and Brand were BOTH the recall "
            "title, \"Bomberry Various Products – 150g\" — the register named "
            "a company that does not exist. Read on the FSANZ recall-alert "
            "page today: supplier \"Atomic Changes Pty Ltd t/a Bomberry\", "
            "brand \"Bomberry\", the four 150 g product variants and their "
            "date marking, Problem \"The presence of foreign matter "
            "(metal).\", Food safety hazard \"Food products containing metal "
            "may cause illness/injury if consumed.\", recall date 28 October "
            "2026, sold nationally and online. FSANZ's own spelling "
            "\"Beligan\" is kept as published. Pathogen was the one-off "
            "string \"Foreign Matter\", which no alert term in "
            "tools/alert_vocab.py could reach; set to the register's "
            "canonical \"Foreign material (metal)\" (10 rows), the "
            "parenthetical taken from the page's own Problem line. The cause "
            "— the title has no \" - \" so the company part parsed empty and "
            "scrapers/oceania/fsanz.py fell back to Company=product — is "
            "fixed in this upload.]"),
    },
    {
        "url": "https://rappel.conso.gouv.fr/fiche-rappel/23944/interne",
        "set": {
            "Product": "Smoked bacon, 12 slices",
            "Reason": "Analysis found the presence of Salmonella.",
        },
        "drop_stamps": ["[needs cleanup: Reason not in English]"],
        "note": (
            "[morning-fix 2026-10-29: Reason and Product translated from the "
            "French already on the row; no fact added and the fiche itself "
            "was not re-read. "
            "[original Reason (fr): \"Une analyse a mis en évidence la "
            "présence de Salmonella.\"] "
            "[original product: bacon fumé 12 tranches] "
            "Product carried no needs-cleanup stamp — the detector misses "
            "short names, the same shape as \"foie de poulet\".]"),
    },
    {
        "url": "https://www.recallswiss.admin.ch/customer-access/#Recalls/1176",
        "set": {"Product": "Bonaparte cream cheese, 200 g"},
        "note": (
            "[morning-fix 2026-10-29: Product translated from the German "
            "already on the row and in the FSVO title quoted above. "
            "[original product: Bonaparte Rahmkäse 200g] "
            "No needs-cleanup stamp was set on this row.]"),
    },
    {
        "url": "https://www.recallswiss.admin.ch/customer-access/#Recalls/1172",
        "set": {"Product": "Minibrie with garlic, porcini and chilli"},
        "note": (
            "[morning-fix 2026-10-29: Product translated from the German "
            "already on the row and in the FSVO title quoted above "
            "(Knoblauch = garlic, Steinpilz = porcini, Chili = chilli); the "
            "product name Minibrie is kept as published. "
            "[original product: Minibrie-Knoblauch, Steinpilz und Chili] "
            "No needs-cleanup stamp was set on this row.]"),
    },
    {
        "url": "https://www.blv.admin.ch/de/newnsb/CLZJKepd2Wwl",
        "set": {"Product": "Minibrie with chestnut, garlic, porcini and chilli"},
        "drop_stamps": ["[needs cleanup: Product not in English]"],
        "note": (
            "[morning-fix 2026-10-29: Product translated from the German "
            "already on the row and in the FSVO title quoted above (Marroni "
            "= chestnut, Knoblauch = garlic, Steinpilz = porcini, Chili = "
            "chilli); the product name Minibrie is kept as published. "
            "[original product: Minibrie Marroni, Knoblauch, Steinpilz und "
            "Chili]]"),
    },
    {
        "url": ("https://webgate.ec.europa.eu/rasff-window/screen/"
                "notification/882030"),
        "set": {
            "Product": ("Salmonella spp - poultry neck trimmings, product on "
                        "the market"),
        },
        "note": (
            "[morning-fix 2026-10-29: Product was Polish and truncated. "
            "Translated from the full original text already on this row "
            "(\"wycinki z szyjek drobiowych produkt na rynku\" = trimmings "
            "from poultry necks, product on the market), cross-read against "
            "RASFF's own English rendering on the same row, \"Salmonella "
            "spp. – poultry products, country of origin: Poland\". The RASFF "
            "field format — hazard prefix, and Company/Brand "
            "\"Notification 2026.9510\" — is left exactly as the scraper "
            "writes it. "
            "[original product: Salmonella spp - wycinki z szyjek ...]]"),
    },
    {
        "url": ("https://webgate.ec.europa.eu/rasff-window/screen/"
                "notification/880927"),
        "set": {
            "Product": ("Escherichia coli above legal limits in clams "
                        "(vongole or lupini)"),
        },
        "note": (
            "[morning-fix 2026-10-29: Product was the notification's "
            "Italian/English double text. Translated from the original "
            "already on this row; \"lupino\" is the Italian name for the "
            "lupine clam. Reason keeps RASFF's own \"; risk: ...; category: "
            "...\" format untouched. "
            "[original product: E.coli  oltre i limiti in vongole o "
            "lupino/E.coli above legal limits in clams form Italy]]"),
    },
]

# --------------------------------------------------------------------------
# C — archive rows contradicting the register
# --------------------------------------------------------------------------

DUP_NOTE = (
    " [SUPERSEDED " + TODAY + " — the recall at this URL is PUBLISHED in "
    "Recalls. This archive row is the earlier refusal of the same recall and "
    "is kept for the trail only; the published row wins. Annotated by the "
    "morning fix pass, which found it through "
    "tests/test_a_recall_is_not_both_published_and_rejected.py.]")

# --------------------------------------------------------------------------
# D — Pending rows unblocked on their own page
# --------------------------------------------------------------------------

PENDING_FIXES = [
    {
        "url": ("https://www.gov.pl/web/gis/ostrzezenie-publiczne-dotyczace-"
                "zywnosci-stwierdzenie-substancji-psychoaktywnej-w-"
                "suplemencie-diety"),
        "set": {
            "Date": "2026-10-28",
            "Company": "DR JOINT sp. z o.o.",
            "Brand": "BRAIN WASH",
            "Product": ("BRAIN WASH food supplement — cannabis gummies, "
                        "multifruit flavour with added vitamin C, 100 g "
                        "(lot 0708BW06O)"),
            "Pathogen": "Delta-9-tetrahydrocannabinol (THC)",
            "Reason": (
                "Tetrahydrocannabinol (THC), a psychoactive substance, found "
                "in a food supplement. The National Institute of Public "
                "Health NIZP-PZH assessed that consuming the product, given "
                "the detected THC content, may pose a health risk to "
                "consumers."),
            "Status": "pending",
            "RejectedBy": "",
        },
        "note": (
            "[morning-fix 2026-10-29: reviewer 1 REJECTED this row as "
            "\"Hazard is not a microbial pathogen, but an undeclared "
            "psychoactive substance\". THC over a reference dose is "
            "explicitly in AFTS scope and the register already publishes six "
            "such rows out of RappelConso, so the refusal was a scope error, "
            "not a verdict on the recall. Read on the GIS notice today: "
            "product \"BRAIN WASH suplement diety, żelki konopne o smaku "
            "wieloowocowym z dodatkiem witaminy C, 100 g\", lot 0708BW06O, "
            "producer \"DR JOINT sp. z o.o., ul. Prymasa Stefana "
            "Wyszyńskiego 1B, 62-800 Kalisz\", published 28.10.2026, hazard "
            "[original Reason (pl): \"spożycie tego produktu, biorąc pod "
            "uwagę stwierdzoną zawartość THC, może stwarzać zagrożenie dla "
            "zdrowia konsumentów.\"]. Pathogen takes the register's "
            "canonical \"Delta-9-tetrahydrocannabinol (THC)\", which "
            "tools/alert_vocab.py reaches. "
            "[original product: stwierdzenie substancji psychoaktywnej w "
            "suplemencie diety]]"),
    },
    {
        "url": "https://www.sfda.gov.sa/en/warnings/19788",
        "set": {
            "Company": "(not specified in SFDA warning 19788)",
            "Brand": "Arla Protein",
            "Product": (
                "Arla Protein Pudding, 6 × 200 g — chocolate, hazelnut "
                "latte, salted caramel and crème brûlée flavours, produced "
                "in Denmark (lots ES26257001, ES26282001, ES26284001, "
                "ES26284002, ES26284003, ES26247001, ES26274001, "
                "ES26276001, ES26276002, ES26267001, ES26273001)"),
            "Reason": (
                "Potential presence of small glass fragments. SFDA warned "
                "consumers not to eat the products and to dispose of them, "
                "following a voluntary notification from the company; SFDA "
                "reports no related incidents in Saudi Arabia."),
        },
        "note": (
            "[morning-fix 2026-10-29: the row's Product and Notes were the "
            "warning page's own heading chrome and Company was empty, so the "
            "publish gate could never pass it. Read on SFDA warning 19788 "
            "today: title \"SFDA Issues Warning Against Certain Arla Protein "
            "Pudding Products from Denmark\", warning date 27/10/2026, the "
            "four flavours, the eleven batch numbers and the glass-fragment "
            "hazard. The page names NO importer, so Company takes the "
            "register's existing \"(not specified in ...)\" form rather than "
            "a guess — the same form as the AESAN rows.]"),
    },
    {
        "url": ("https://www.nvwa.nl/actueel/nieuws/2026/10/22/"
                "veiligheidswaarschuwing-ah-ossenworst-van-albert-heijn"),
        "set": {
            "Date": "2026-10-22",
            "Company": "Albert Heijn",
            "Brand": "Albert Heijn",
            "Product": "Sliced ossenworst (cured raw beef sausage)",
            "Reason": (
                "Shiga toxin-producing E. coli (STEC) may be present in the "
                "product. NVWA advises consumers not to eat the ossenworst."),
        },
        "drop_stamps": ["[date-unknown: claude-check please verify Date]"],
        "note": (
            "[morning-fix 2026-10-29: blocked on an empty Company and an "
            "empty Date, both of which are on the row itself. Company "
            "\"Albert Heijn\" is named in the row's own warning text; Date "
            "2026-10-22 is the date segment of the row's own NVWA URL "
            "(/actueel/nieuws/2026/10/22/). Product and Reason translated "
            "from the Dutch already on the row and the \"2 days ago · \" "
            "search-snippet prefix dropped. "
            "[original product: 2 days ago · Albert Heijn waarschuwt voor "
            "hun plakjes ossenworst] "
            "[original Reason (nl): \"De shigatoxine-producerende E. "
            "coli-bacterie (STEC) kan aanwezig zijn in het product. Eet de "
            "ossenworst niet\"]]"),
    },
]

# --------------------------------------------------------------------------
# E — Pending rows archived
# --------------------------------------------------------------------------

PENDING_ARCHIVE = [
    {
        "url": ("https://recalls-rappels.canada.ca/en/alert-recall/"
                "nazari-co-brand-pistachio-inshell-aa-2628-recalled-due-"
                "salmonella"),
        "reason": (
            "Out of window — read on the CFIA notice today (2026-10-29): "
            "recall date 2025-10-23, published 2025-10-27, last updated "
            "2025-10-27, Class 2, recalling firm 2181322 Ontario Inc., brand "
            "Nazari Co, Pistachio Inshell AA 26/28, 50 kg. The row carries "
            "Date 2026-10-28, which is the day the official-feed collector "
            "found it, not the day CFIA issued it — a scrape-date-in-Date "
            "defect. The register's window begins 2026-01-02, so a 2025 "
            "notice is not published. Not a verdict on the recall itself."),
    },
    {
        "url": ("https://recalls-rappels.canada.ca/en/alert-recall/"
                "various-pistachios-recalled-due-salmonella-0"),
        "reason": (
            "Out of window — read on the CFIA notice today (2026-10-29): "
            "published 2025-09-24, last updated 2025-09-24, recalling firms "
            "Mega Sanjha Punjab Grocery Store, Briwood Farm Market, Omonia "
            "Foods Import, Great West Italian Importers Ltd. and O.K. "
            "General Food Store. The row carries Date 2026-10-28, the "
            "discovery date — the same scrape-date-in-Date defect as the "
            "Nazari Co row. The register's window begins 2026-01-02, so a "
            "2025 notice is not published. Not a verdict on the recall "
            "itself."),
    },
    {
        "url": ("https://nafdac.gov.ng/public-alert-no-003-2020-alert-on-"
                "illegal-distribution-and-sale-of-unregistered-hand-"
                "sanitizers/"),
        "reason": (
            "Out of scope — hand sanitiser is not a food (scope rule R1), so "
            "no field fix could ever make this row publishable. It also "
            "carries NAFDAC public alert 003/2020 against Date 2026-10-28, "
            "five years apart, which is the shape "
            "tests/test_alert_number_year_matches_date.py fires on; that "
            "test reads Pending, so the row kept the suite red while it sat "
            "there. Discovered via punchng.com."),
    },
]

# --------------------------------------------------------------------------
# F — CAA (JP) recalls the register does not hold
# --------------------------------------------------------------------------

CAA = "https://www.recall.caa.go.jp/result/detail.php?rcl=%s&screenkbn=01"

NEW_ROWS = [
    {
        "Date": "2026-10-28",
        "Source": "CAA (JP)",
        "Company": "Noguchi",
        "Brand": "Noguchi",
        "Product": ("Shiro-shimeji mushrooms, sold as shiro-shimeji and "
                    "found to be haiiro-shimeji (Clitocybe nebularis)"),
        "Pathogen": "Mushroom toxins (Clitocybe nebularis)",
        "Reason": ("The mushrooms sold as shiro-shimeji were found to be "
                   "haiiro-shimeji, a species classified as a poisonous "
                   "mushroom. Refund / recall."),
        "Class": "Recall",
        "Country": "Japan",
        "Outbreak": 0,
        "URL": CAA % "00000036065",
        "Notes": (
            "[operator review 2026-10-29: daily-global MISSING item. Read on "
            "the CAA detail page: 件名 「のぐち「シロシメジ」 - 返金／回収」, "
            "回収理由 「毒きのこに分類されている「ハイイロシメジ」であること"
            "が判明」, 対応開始日 2026年10月28日. CAA listed it on 2026/10/29. "
            "The page has no 事業者名 field and the 連絡先 and 対応方法 fields "
            "are blank, so Company is the business named first in the 件名, "
            "romanised. No lot, date mark or region is printed. The page "
            "shows no 掲載日, so Date is the 対応開始日, as for every CAA row "
            "in the register.] "
            "[original company: のぐち] "
            "[original product: シロシメジ]"),
    },
    {
        "Date": "2026-10-26",
        "Source": "CAA (JP)",
        "Company": "Maruni Shokuhin",
        "Brand": "Maruni Shokuhin",
        "Product": ("Ryuji-supervised supreme soy-sauce ramen, 2 servings "
                    "(sold at Lopia stores in the Kanto and Chubu regions)"),
        "Pathogen": "Mold",
        "Reason": "Contamination by mold. Exchange / recall.",
        "Class": "Recall",
        "Country": "Japan",
        "Outbreak": 0,
        "URL": CAA % "00000036062",
        "Notes": (
            "[operator review 2026-10-29: daily-global MISSING item. Read on "
            "the CAA detail page: 件名 「マルニ食品「料理研究家リュウジ監修2食"
            "至高の醤油ラーメン」（関東、中部地区のロピア各店舗で販売） - "
            "交換／回収」, 回収理由 「カビによる汚染」, 対応開始日 "
            "2026年10月26日. CAA listed it on 2026/10/29. The page has no "
            "事業者名 field, so Company is the business named first in the "
            "件名, romanised. No lot or date mark is printed. The page shows "
            "no 掲載日, so Date is the 対応開始日.] "
            "[original company: マルニ食品] "
            "[original product: 料理研究家リュウジ監修2食至高の醤油ラーメン]"),
    },
    {
        "Date": "2026-10-26",
        "Source": "CAA (JP)",
        "Company": "Mimatsu Shokuhin",
        "Brand": "Seven Premium",
        "Product": ("Seven Premium shrimp shumai (sold at York-Benimaru "
                    "stores from northern Kanto through Tohoku)"),
        "Pathogen": "Mold",
        "Reason": "Contamination by mold. Refund / recall.",
        "Class": "Recall",
        "Country": "Japan",
        "Outbreak": 0,
        "URL": CAA % "00000036057",
        "Notes": (
            "[operator review 2026-10-29: daily-global MISSING item. Read on "
            "the CAA detail page: 件名 「みまつ食品「セブンプレミアム海老焼売」"
            "（北関東～東北のヨークベニマルで販売） - 返金／回収」, 回収理由 "
            "「カビによる汚染」, 対応開始日 2026年10月26日. CAA listed it on "
            "2026/10/28. The page has no 事業者名 field, so Company is the "
            "business named first in the 件名, romanised; Seven Premium is "
            "the retail brand printed on the pack. No lot or date mark is "
            "printed. The page shows no 掲載日, so Date is the 対応開始日.] "
            "[original company: みまつ食品] "
            "[original product: セブンプレミアム海老焼売]"),
    },
    {
        "Date": "2026-10-25",
        "Source": "CAA (JP)",
        "Company": "PAUSE bake shop",
        "Brand": "PAUSE bake shop",
        "Product": "Baked goods: pound cake three-variety set",
        "Pathogen": "Mold",
        "Reason": "Mold growth. Refund / recall.",
        "Class": "Recall",
        "Country": "Japan",
        "Outbreak": 0,
        "URL": CAA % "00000036064",
        "Notes": (
            "[operator review 2026-10-29: daily-global MISSING item. Read on "
            "the CAA detail page: 件名 「PAUSE bake shop「焼き菓子：パウンド3種"
            "セット」 - 返金／回収」, 回収理由 「カビの発生」, 対応開始日 "
            "2026年10月25日. CAA listed it on 2026/10/29. The page has no "
            "事業者名 field, so Company is the business named first in the "
            "件名; it is already in Latin script and is kept as published. No "
            "lot or date mark is printed. The page shows no 掲載日, so Date "
            "is the 対応開始日.] "
            "[original product: 焼き菓子：パウンド3種セット]"),
    },
    {
        "Date": "2026-10-18",
        "Source": "CAA (JP)",
        "Company": "Tokyo Fruits",
        "Brand": "Tokyo Fruits",
        "Product": "Canned cut tomatoes",
        "Pathogen": "Foreign material (can interior coating)",
        "Reason": ("Possible foreign-matter contamination caused by peeling "
                   "of the can's interior coating. Refund / recall."),
        "Class": "Recall",
        "Country": "Japan",
        "Outbreak": 0,
        "URL": CAA % "00000036063",
        "Notes": (
            "[operator review 2026-10-29: daily-global MISSING item. Read on "
            "the CAA detail page: 件名 「東京フルーツ「カットトマト缶」 - "
            "返金／回収」, 回収理由 「内面塗装の剥がれによる異物混入の可能性」, "
            "対応開始日 2026年10月18日. CAA listed it on 2026/10/29, eleven "
            "days after the firm's own response start date, which is why it "
            "was not in the register. The page has no 事業者名 field, so "
            "Company is the business named first in the 件名, romanised. The "
            "対象の特定情報 field is empty, so no lot, quantity or date mark "
            "is printed. The page shows no 掲載日, so Date is the 対応開始日, "
            "as for every CAA row in the register — which puts this row in "
            "the closed ISO week 2026-W42.] "
            "[original company: 東京フルーツ] "
            "[original product: カットトマト缶]"),
    },
]


# --------------------------------------------------------------------------

def _head(ws):
    return [c.value for c in ws[1]]


def _rowdict(ws, head, r):
    return {h: ws.cell(r, i + 1).value for i, h in enumerate(head)}


def _set(ws, head, r, field, value):
    ws.cell(r, head.index(field) + 1).value = value


def _norm(u):
    return str(u or "").strip().rstrip("/").lower()


def _apply_notes(ws, head, r, row, note, drop_stamps):
    notes = str(row.get("Notes") or "").strip()
    for stamp in drop_stamps or ():
        notes = notes.replace(stamp, "").strip()
    notes = re.sub(r"\s{2,}", " ", (notes + " " + note).strip())
    _set(ws, head, r, "Notes", notes)


def _fix_sheet(wb, sheet, specs) -> int:
    ws = wb[sheet]
    head = _head(ws)
    done = 0
    for spec in specs:
        target = _norm(spec["url"])
        hit = False
        for r in range(2, ws.max_row + 1):
            row = _rowdict(ws, head, r)
            if _norm(row.get("URL")) != target:
                continue
            for field, value in spec["set"].items():
                if field not in head:
                    continue
                _set(ws, head, r, field, value)
            _apply_notes(ws, head, r, row, spec["note"],
                         spec.get("drop_stamps"))
            done += 1
            hit = True
            print(f"  {sheet}: fixed {row.get('Source')!r} "
                  f"{spec['url'][-52:]}")
        if not hit:
            raise SystemExit(
                f"{sheet}: no row at {spec['url']} — main has moved under "
                f"this repair script; re-read the sheet before re-running")
    return done


def _archive(arch, src_row, reason, rejected_by):
    out = []
    for h in _head(arch):
        if h in ("RejectReason", "RejectionReason"):
            out.append(reason)
        elif h == "RejectedBy":
            out.append(rejected_by)
        elif h == "RejectedAt":
            out.append(TODAY)
        elif h == "Status":
            out.append("rejected")
        else:
            out.append(src_row.get(h, ""))
    arch.append(out)


def _archive_pending(wb) -> int:
    ws = wb["Pending"]
    head = _head(ws)
    arch = wb["Rejected"]
    done = 0
    for spec in PENDING_ARCHIVE:
        target = _norm(spec["url"])
        hit = False
        for r in range(ws.max_row, 1, -1):
            row = _rowdict(ws, head, r)
            if _norm(row.get("URL")) != target:
                continue
            _archive(arch, row, spec["reason"], BY)
            ws.delete_rows(r, 1)
            done += 1
            hit = True
            print(f"  Pending -> Rejected: {row.get('Source')!r} "
                  f"{str(row.get('Product'))[:46]!r}")
        if not hit:
            raise SystemExit(
                f"Pending: no row at {spec['url']} — main has moved under "
                f"this repair script")
    return done


def _annotate_overlaps(wb) -> int:
    """Archive rows sharing a URL with a published row, with no verdict.

    The verdict goes in the sheet's own REASON column, not in Notes:
    tests/test_a_recall_is_not_both_published_and_rejected.py reads
    RejectionReason / RejectReason and nothing else, and URLs are compared
    with the writer's own dedup key, not a hand-rolled strip — a trailing
    slash or an http/https difference hid one of the two rows from the
    repair pass of 2026-10-26, which wrote to Notes.
    """
    from pipeline.merge_master import _normalize_url_for_dedup as _key

    rec = wb["Recalls"]
    rhead = _head(rec)
    ui = rhead.index("URL")
    published = {_key(str(rec.cell(r, ui + 1).value or ""))
                 for r in range(2, rec.max_row + 1)}
    published -= {"", "nan", "none"}

    # The markers the test accepts as self-explaining.
    verdict = re.compile(r"superseded|duplicate|unresolved contradiction|"
                         r"broken provenance|already_approved", re.I)
    annotated = 0
    for sheet in ("Rejected", "Weekly_Rejected"):
        if sheet not in wb.sheetnames:
            continue
        ws = wb[sheet]
        head = _head(ws)
        why = ("RejectReason" if "RejectReason" in head
               else "RejectionReason" if "RejectionReason" in head else None)
        if "URL" not in head or why is None:
            continue
        for r in range(2, ws.max_row + 1):
            row = _rowdict(ws, head, r)
            if _key(str(row.get("URL") or "")) not in published:
                continue
            existing = str(row.get(why) or "").strip()
            if verdict.search(existing):
                continue
            if existing.lower() in ("", "nan", "none"):
                existing = ""
            _set(ws, head, r, why, (existing + DUP_NOTE).strip())
            if "Notes" in head:
                _set(ws, head, r, "Notes",
                     (str(row.get("Notes") or "").strip()
                      + " [see " + why + ": SUPERSEDED " + TODAY + "]").strip())
            annotated += 1
            print(f"  {sheet}: SUPERSEDED annotated in {why} — "
                  f"{row.get('Source')!r} "
                  f"{str(row.get('Product'))[:42]!r}")
    return annotated


GIS_URL = ("https://www.gov.pl/web/gis/ostrzezenie-publiczne-dotyczace-"
           "zywnosci-stwierdzenie-substancji-psychoaktywnej-w-suplemencie-"
           "diety")

GIS_SUPERSEDE = (
    " [SUPERSEDED " + TODAY + " — this refusal was a SCOPE ERROR, not a "
    "verdict on the recall. Reviewer 1 refused the row because the hazard "
    "\"is not a microbial pathogen, but an undeclared psychoactive "
    "substance\". AFTS scope has included THC over a reference dose and "
    "undeclared drugs since 2026-10-01, and the register already publishes "
    "six Delta-9-THC rows out of RappelConso (2026-09-30, 2026-10-06 x2, "
    "2026-10-21 x2, 2026-10-26) plus three \"Undeclared pharmacological "
    "ingredient\" rows out of the FDA. The substance here is THC, confirmed "
    "on the GIS notice read 2026-10-29: BRAIN WASH cannabis gummies, lot "
    "0708BW06O, DR JOINT sp. z o.o., published 28.10.2026, with NIZP-PZH "
    "assessing a health risk at the detected THC content. The Pending row "
    "is repaired and passes every publish-gate rule, so merge_master's "
    "SUPERSEDED re-promotion exception is what should read this banner. The "
    "root cause — reviewer 1 treating \"not a microbial pathogen\" as "
    "out-of-scope — is unfixed and is named in today's run notes.]")


def _supersede_gis_scope_refusal(wb) -> int:
    """Mark reviewer 1's scope refusal of the GIS THC row SUPERSEDED.

    The repaired Pending row passes the full publish gate but is blocked by
    the re-promotion guard, which reads the archived rejection reason. The
    guard has a designed exception for an explicitly SUPERSEDED archive row
    (pipeline/merge_master.py), and that is the honest record here: the
    refusal's grounds are gone, the recall is real and in scope.
    """
    from pipeline.merge_master import _normalize_url_for_dedup as _key

    target = _key(GIS_URL)
    done = 0
    for sheet in ("Weekly_Rejected", "Rejected"):
        if sheet not in wb.sheetnames:
            continue
        ws = wb[sheet]
        head = _head(ws)
        why = ("RejectReason" if "RejectReason" in head
               else "RejectionReason" if "RejectionReason" in head else None)
        if "URL" not in head or why is None:
            continue
        for r in range(2, ws.max_row + 1):
            row = _rowdict(ws, head, r)
            if _key(str(row.get("URL") or "")) != target:
                continue
            existing = str(row.get(why) or "").strip()
            if "superseded" in existing.lower():
                continue
            if existing.lower() in ("nan", "none"):
                existing = ""
            _set(ws, head, r, why, (existing + GIS_SUPERSEDE).strip())
            done += 1
            print(f"  {sheet}: SUPERSEDED (scope error) annotated in {why} — "
                  f"GIS (PL) psychoactive substance / THC")
    if not done:
        raise SystemExit(
            "no archive row found at the GIS URL — the re-promotion guard "
            "named Weekly_Rejected; re-read the sheet before re-running")
    return done


def main() -> int:
    if not XLSX.exists():
        raise SystemExit(f"workbook not found: {XLSX}")

    wb = openpyxl.load_workbook(XLSX)
    recalls_fixed = _fix_sheet(wb, "Recalls", RECALLS_FIXES)
    pending_fixed = _fix_sheet(wb, "Pending", PENDING_FIXES)
    archived = _archive_pending(wb)
    annotated = _annotate_overlaps(wb) + _supersede_gis_scope_refusal(wb)
    wb.save(XLSX)
    wb.close()

    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False,
                                     encoding="utf-8") as fh:
        json.dump(NEW_ROWS, fh, ensure_ascii=False, indent=2)
        rows_path = fh.name
    try:
        proc = subprocess.run(
            [sys.executable, str(ROOT / "tools" / "add_manual_row.py"),
             rows_path], cwd=str(ROOT), text=True, capture_output=True)
        print(proc.stdout.rstrip())
        if proc.stderr.strip():
            print(proc.stderr.rstrip(), file=sys.stderr)
    finally:
        Path(rows_path).unlink(missing_ok=True)
    if proc.returncode != 0:
        raise SystemExit(f"add_manual_row refused a row ({proc.returncode})")

    from pipeline.merge_master import mirror_json_from_xlsx
    print(f"recalls.json re-mirrored: {mirror_json_from_xlsx(XLSX, JSON)} rows")
    print(f"RECALLS_FIELDS_FIXED={recalls_fixed}")
    print(f"PENDING_FIELDS_FIXED={pending_fixed}")
    print(f"PENDING_ARCHIVED={archived}")
    print(f"ARCHIVE_ROWS_ANNOTATED={annotated}")
    print(f"ROWS_QUEUED={len(NEW_ROWS)}")
    print("ROWS_REMOVED=0")
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
