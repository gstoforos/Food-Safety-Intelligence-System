# -*- coding: utf-8 -*-
"""Morning fix pass 2026-11-10.

Base: main at 9f8b6631 ("Italian gap finder: 2026-11-10 auto-update").

Every value set below was read this morning on the regulator's own page, or is
a faithful translation of text already on the row it is written to. NO ROW
LEAVES Recalls, so the commit message carries no `remove-rows`.

TWO TESTS WERE RED ON MAIN AT THE BASE SHA, both the same assertion:

    test_a_recall_is_not_both_published_and_rejected
        ::test_every_shared_url_says_which_copy_wins[Rejected-RejectReason]        A
        ::test_every_shared_url_says_which_copy_wins[Weekly_Rejected-RejectionReason]  B

Three archive rows share a URL with a PUBLISHED recall and say nothing about
which copy wins. Both failures are fixed here, and both have a cause in code
that is fixed in the same commit.


A. THE CONFIRMER CANNOT READ A PDF, AND REJECTED A REAL STEC RECALL FOR IT
==========================================================================
Rejected holds, with an EMPTY RejectReason:

    Date      2026-11-06
    Source    BVL (DE)
    Product   Rinder-Peperonisalami, Rindfleischsalami, Rindfleischsalami
              rein Rind, Pizza Rindfleischsalami
    Notes     "REJECTED: Confirmer: row was at pending (not reviewed by
               reviewer 2) and page does not mention any of ['norderstedter',
               'fleisch', 'wurstwaren', 'peter', 'faden', 'rinder'] — the
               cited URL describes s…"
    URL       …/261106_05_SH_Rindfleischsalamiprodukte_Presse.pdf
                  ?__blob=publicationFile&v=2

The same URL is PUBLISHED in Recalls. The confirmer's keyword check is right
to exist and its verdict here is false, and the reason is the URL: the row
cites the press-release **PDF blob**, not the notice. A keyword check run over
a PDF's bytes finds no German words, so the confirmer concluded the page does
not name the firm — "I could not read it" reported as "it is gone", the same
invariant pipeline/_bot_wall.py was written for.

READ THIS MORNING on the regulator's own HTML notice,
https://www.lebensmittelwarnung.de/___lebensmittelwarnung.de/Meldungen/2026/
11_November/261106_05_SH_Rindfleischsalamiprodukte/
261106_05_SH_Rindfleischsalamiprodukte.html — which the page's own og:url
declares as its canonical address:

    heading               "Diverse Rindfleischsalamiprodukte"
    Hersteller            Norderstedter Fleisch- und Wurstwaren Peter Faden
                          GmbH & Co KG, Kösliner Weg 14, 22850 Norderstedt
    Grund der Meldung     "Krankheitserreger"
    Weitere Infos         "… in einer Einzeluntersuchung Shigatoxinbildende
                           Escherichia coli (STEC) nachgewiesen wurden."
                          "Der Rückruf erfolgt vorsorglich."
    Erstveröffentlichung  06.11.2026, created by Schleswig-Holstein
    Letzte Aktualisierung 09.11.2026
    Aktualisierungshinweise
                          "09.11.2026: Die Produktbezeichnungen sowie die
                           betroffenen Chargen wurden korrigiert. Anstelle der
                           Chargen 260926-12.11.2026 und 260927-13.11.2026
                           sind die Chargen 260924-12.11.2026 und
                           260925-13.11.2026 betroffen."
    Betroffene Bundesländer  15 of 16 (Saarland is not listed)

So the recall is real, the published row is right, and the archive row is
wrong. Two things follow, and this pass does both:

  1. The archive rows are marked SUPERSEDED, naming the cause.
  2. THE PUBLISHED ROW'S URL BECOMES THE HTML NOTICE. This is the root fix,
     not cosmetics: yesterday's pass (tools/repair_2026_11_07.py, section A)
     already repaired a fabricated Pathogen on this same row and already
     quoted this same HTML page as "the HTML notice beside the PDF the row
     cites" — and left the PDF address in place. The confirmer then rejected
     the row again overnight, which is why the same test is red again this
     morning. While the row cites a PDF, every reviewer pass that reads the
     page will keep failing on it, every day. The HTML address is the same
     regulator, the same notice, the same per-recall page, and it is the one
     the notice itself declares canonical. It is reachable: this pass opened
     it and it answered 200.

     The 09.11.2026 update is recorded in Notes and NO NEW ROW is added for
     it. It is an amendment to an already-published recall — corrected
     product names and two lot codes swapped — not a new recall, and R4's
     24-hour window is about new notices.


B. A CONSENT BANNER AS THE PRODUCT, ON RECALLS THAT ARE PUBLISHED
=================================================================
Weekly_Rejected holds two rows whose Product is **"Ihre
Datenschutzeinstellungen"** — the AGES (AT) cookie-consent heading — under
URLs whose recalls are PUBLISHED in Recalls:

    2026-11-05  …/produkt/alnatura-pinienkerne-60g-und-alnatura-salatkerne-
                mix-100g        published as "Pine nuts 60g and Salad kernels
                                mix 100g", Salmonella
    2026-10-28  …/produkt/produktwarnung-salmonellen-in-tomaten-aus-zwei-
                regionen-in-sizilien
                                published as "Cocktail tomatoes", 2026-10-16

Their RejectionReason is "llm-extraction-failed: Llama timeout or
circuit-open; title-only fallback could not produce clean fields", which
explains the extraction but says nothing about which copy of the recall wins.
Both are duplicates of a recall the register went on to capture correctly, so
both are marked SUPERSEDED.

The AGES listing was read this morning
(https://www.ages.at/mensch/produktwarnungen-produktrueckrufe) and confirms
the Alnatura entry: "Alnatura Pinienkerne 60g und Alnatura Salatkerne-Mix
100g", 05.11.2026, Firma "Alnatura Produktions- und Handels GmbH",
Rückrufgrund "Salmonellen", Ablaufdatum 13.07.2027. The tomato page could not
be opened this morning (the fetch failed with a client error), so THE
PUBLISHED ROW'S DATE OF 2026-10-16 IS LEFT EXACTLY AS IT IS. The archive
copies of that URL carry four different Dates — 2026-10-14, 2026-10-16,
2026-10-26 and 2026-10-28 — which are the days the extractor retried, not
publication dates; that is Fault 2's shape in an archive sheet and this pass
does not guess which is right. It is named in the hand-off instead.

THE CODE HALF, in this same commit: "Ihre Datenschutzeinstellungen" is not a
new accident. Sixteen rows across the workbook carry page furniture as their
Product — that heading 13 times (newest 2026-11-06), BVL's "Navigation und
Service" 3 times, an FDA outbreak table's HEADER ROW twice, an SFA share
widget twice. pipeline/_bot_wall.py gains FURNITURE_TITLES and
is_not_the_notice(), wired into the same three gates that already refuse a
challenge-page title — the gap-finder guard, the title extractor and
merge_master._write_sheet — and held by
tests/test_page_furniture_is_not_a_product.py.


C. TRANSLATION
==============
Two published rows carried a live "[needs cleanup: …]" stamp this morning and
a third carried a STALE one. Every Product published in the last 26 h was also
read by hand (there was one, the FDA "Sausage, Egg & Cheese Biscuit Sandwich",
already English).

  * fiche 24045, "pops de poulet pané" → "Breaded chicken pops". Its REASON
    was also mixed-language — "Detection of Salmonella - attente sérotypage",
    half English and half French — and becomes "Detection of Salmonella —
    serotyping pending". The French is kept in Notes. The detector stamped the
    Product and did not see the Reason, because the Reason's non-English part
    is two words.
  * Swiss recall 1187, "Seedorfer Biräweggä mit/ohne Nuss ganz/halb,
    Biräweggä klein mit Nuss und Dörrobstbiräweggä". Translated from the FSVO
    title already quoted verbatim in this row's own Notes: "Baggenstos
    Spezialitäten AG ruft die Produkte «Seedorfer Biräweggä mit/ohne Nuss
    ganz/halb, Biräweggä klein mit Nuss und Dörrobstbiräweggä» wegen
    Fremdkörper (Plastik) zurück". Biräweggä is Swiss German for Birnenweggen,
    a pear-filled pastry loaf; Seedorf is a place; Dörrobst is dried fruit.
    The dialect product name is KEPT alongside the English gloss, as this
    register keeps protected and regional names.
  * AESAN alert 2026_80, "Cooked ham (jamón cocido), 175 g, …" — ALREADY
    English. The stamp is stale: tools/repair_2026_11_08.py repaired this
    Product (it had been a search-engine snippet) and did not drop the stamp.
    Only the stamp is removed; not one field is touched. There is precedent:
    the 2026-10-27 pass dropped a stale stamp from a BLV row the same way.
    Its separate "[date-unknown: claude-check please verify Date]" flag is
    LEFT ALONE — the AESAN alert page was not opened this morning, so the Date
    is not confirmed and the flag is still true.


E. A HAZARD LABEL IN ITALIAN, WHICH NO SUBSCRIBER ALERT CAN REACH
================================================================
Pending holds, at Status 'pending' and blocked by publish-gate rule 10c:

    Date      2026-11-03
    Source    Ministero della Salute (IT)
    Company   IN's Mercato        Brand  Selezione Più
    Product   Tuna fillets
    Pathogen  "Istamina exceeding limit"
    Reason    "Possible presence of istamina above legal limits"
    URL       …/Cartello%20di%20Richiamo%20Filetti%20di%20Tonno%20Surgelato
              %20lotto%20n.%2076504_01_001…pdf

The gate's own words: "Pathogen 'Istamina exceeding limit' matches no word
any subscriber alert offers". "Istamina" is Italian for histamine. The
register's canonical value — on 23 PUBLISHED rows, several of them Italian
tuna — is "Histamine / scombrotoxin", and the alert term of that name has
carried the tokens ["histamine", "scombrotox"] all along. So this is NOT a
vocabulary gap: the vocabulary was always right and the label was in the
wrong language. The value is in the row's OWN Reason, which is the fix
category this is filed under.

A SECOND COPY is already archived in Rejected on the same label (2026-10-20,
"Tuna fillets"), so the shape recurs rather than being one bad row.

THE CODE HALF, in this same commit: scrapers/_models.PATHOGEN_RULES matched
only the English "histamine", so "istamina", "histamina" (es/pt) and
"histamin" (de/sv/no) all normalised to nothing. The pattern now accepts
them, held by tests/test_istamina_is_histamine.py, which also pins the
near-misses that must NOT match — "melamine", "dimethylamylamine", "amine"
and "histaminergic" are all still empty.

THE PDF COULD NOT BE RE-READ THIS MORNING. salute.gov.it answered the notice
URL with a Gcore "Browser Validation Page" — 831 characters of HTML titled
"Gcore", reference 0a87c69feb…, timestamped 11/10/2026 07:15 UTC — which is
the exact wall pipeline/_bot_wall.py was written for. So the label is
canonicalised from the row's own Reason text and from nothing else, and NO
other field is touched: not Date, not Company, not Brand, not Product, not
the lot number in the URL. Italy is not stale by this register's rule — the
Italian gap finder committed at 07:45 this morning — and salute.gov.it has a
scraper (scrapers/salute_it.py), so the wall is a block on this pass, not a
gap in the pipeline.

Unblocking the label lets the register's own gate decide the row, which is
the design. It is expected to publish.


F. A 2024 RECALL WEARING A 2026 DATE
====================================
Pending holds, at Status 'rejected':

    Date      2026-11-03
    Source    NCC (ZA)
    Company   BM Foods (a member of Sea Harvest Group)
    Brand     Shoprite Checkers
    Product   Deli Hummus range
    Pathogen  Listeria Monocytogenes      Tier 1
    URL       https://thencc.org.za/media-statement-deli-hummus-range-
              product-safety-recall/
    Notes     "Discovered via news: citizen.co.za … [review-agent 2026-11-09:
               REJECTED — Date, Company, Brand, Product, Pathogen, Reason,
               Country, and Region cannot be confirmed from the page.]"

THE AUTHORITY PAGE WAS READ THIS MORNING and it is TWO YEARS OLD. It prints
"Date Posted: October 3, 2024"; Shoprite Checkers notified the NCC on
16 September 2024; the sell-by dates are "10 September 2024 to 08 October
2024". Its HTML metadata carries a published time of 2024-10-03 and a
modified time of 2026-08-15.

So the row is not a November 2026 recall at all, and this is the more
dangerous failure of the two the reviewer could have made: the review agent
rejected it for the right outcome on the wrong ground — "cannot be confirmed
from the page" — when the page confirms the company, brand, products, pack
sizes, hazard and reason exactly, and refutes only the Date.

Everything else on the row is correct against the page: manufactured by "BM
Foods (a member of Sea Harvest Group)", the "Deli Hummus range" (Traditional
125 g and 300 g, Reduced Fat 125 g, Zataar 125 g and 300 g, Red Pepper 125 g
and 300 g, Caramelised Onion 125 g), "the detection of Listeria Monocytogenes
which is a species of foodborne bacteria", no illnesses reported.

WHY NOTHING CAUGHT IT. The gap-finder recency guard
(pipeline/_gap_finder_guards, MAX_AGE_DAYS = 30) rejects a row whose own Date
is older than 30 days. It reads the ROW's Date, and the row's Date came from
the news article that discovered it, not from the authority page — so a
two-year-old recall arrived looking three days old and the guard had nothing
to fire on. That is a real hole and it is NOT patched here: closing it needs
the authority page's own date at ingestion time, which is a fetch and a
design decision, not a morning edit. It is written up for George instead.

The row is annotated so that no later pass resurrects it, and the annotation
carries the 2024 date. It is NOT promoted, NOT given a corrected Date, and
NOT removed: the re-promotion guard already blocks it ("already in Rejected")
and an archive is append-only.


D. SEVEN MISSED RECALLS FROM JAPAN
==================================
tools/manual_rows_2026_11_10.json, queued through add_manual_row. The CAA
published THIRTEEN food notices on 2026/11/09 and the register has none of
them; seven are in scope. There is no scraper module for recall.caa.go.jp
anywhere under scrapers/. Each row's Notes carry the full reading, the
romanisation rule and the scope ledger for all thirteen.
"""
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:                 # run as a script from anywhere
    sys.path.insert(0, str(ROOT))
XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
ROWS_JSON = ROOT / "tools" / "manual_rows_2026_11_10.json"

STAMP = "morning-fix 2026-11-10"

# ── A. the German salami recall ────────────────────────────────────────────
DE_PDF = ("https://www.lebensmittelwarnung.de/___lebensmittelwarnung.de/"
          "Meldungen/2026/11_November/261106_05_SH_Rindfleischsalamiprodukte/"
          "261106_05_SH_Rindfleischsalamiprodukte_Presse.pdf"
          "?__blob=publicationFile&v=2")
DE_HTML = ("https://www.lebensmittelwarnung.de/___lebensmittelwarnung.de/"
           "Meldungen/2026/11_November/261106_05_SH_Rindfleischsalamiprodukte/"
           "261106_05_SH_Rindfleischsalamiprodukte.html")

# ── B. the two AGES URLs ───────────────────────────────────────────────────
AT_PINE = ("https://www.ages.at/mensch/produktwarnungen-produktrueckrufe/"
           "produkt/alnatura-pinienkerne-60g-und-alnatura-salatkerne-mix-100g")
AT_TOM = ("https://www.ages.at/mensch/produktwarnungen-produktrueckrufe/"
          "produkt/produktwarnung-salmonellen-in-tomaten-aus-zwei-regionen-"
          "in-sizilien")

# ── C. the translation targets ─────────────────────────────────────────────
CH_BIRA = "https://www.recallswiss.admin.ch/customer-access/#Recalls/1187"
FR_POPS = "https://rappel.conso.gouv.fr/fiche-rappel/24045/interne"
ES_HAM = "https://www.aesan.gob.es/alertas/2026_80"

# ── E. the Italian histamine label ─────────────────────────────────────────
IT_TUNA = ("https://www.salute.gov.it/new/sites/default/files/external_data/"
           "avvisi_sicurezza_alimentare/Cartello%20di%20Richiamo%20Filetti%20"
           "di%20Tonno%20Surgelato%20lotto%20n.%2076504_01_001%20_aggiornato"
           "%20_1__1793979220.pdf")

# ── F. the 2024 South African recall ───────────────────────────────────────
ZA_HUMMUS = ("https://thencc.org.za/media-statement-deli-hummus-range-"
             "product-safety-recall/")

PENDING_FIXES = [
    # E — the label becomes the register's own canonical English value.
    {
        "url": IT_TUNA,
        "set": {"Pathogen": "Histamine / scombrotoxin",
                "Reason": "Possible presence of histamine above legal limits"},
        "note": (
            f"[{STAMP}: Pathogen canonicalised from the Italian "
            f"\"Istamina exceeding limit\" to this register's own value "
            f"\"Histamine / scombrotoxin\", which 23 published rows already "
            f"carry. The publish gate held this row on rule 10c — "
            f"\"Pathogen 'Istamina exceeding limit' matches no word any "
            f"subscriber alert offers\" — and the alert term "
            f"\"Histamine / scombrotoxin\" has carried the tokens "
            f"[histamine, scombrotox] all along, so the vocabulary was never "
            f"the gap: the label was in Italian. The value comes from THIS "
            f"ROW'S OWN Reason, \"Possible presence of istamina above legal "
            f"limits\", and from nothing else. THE NOTICE COULD NOT BE "
            f"RE-READ: salute.gov.it answered this PDF with a Gcore "
            f"\"Browser Validation Page\" (831 characters of HTML titled "
            f"\"Gcore\", reference 0a87c69feb…, 11/10/2026 07:15 UTC) — the "
            f"wall pipeline/_bot_wall.py exists for. So no other field is "
            f"touched: Date, Company, Brand, Product and the lot number in "
            f"the URL are left exactly as they stand. "
            f"[original Pathogen (it): \"Istamina exceeding limit\"] "
            f"[original Reason (it, as stored): \"Possible presence of "
            f"istamina above legal limits\"] "
            f"[the code half is scrapers/_models.PATHOGEN_RULES, which "
            f"matched only the English \"histamine\"; a second copy of this "
            f"defect is already archived in Rejected (2026-10-20, Tuna "
            f"fillets) on the same Italian label.]]"),
    },
    # F — the two-year-old South African recall. No field is set.
    {
        "url": ZA_HUMMUS,
        "set": {},
        "note": (
            f"[{STAMP}: THIS RECALL IS FROM OCTOBER 2024, NOT NOVEMBER 2026. "
            f"The NCC's own page was read this morning at {ZA_HUMMUS} and "
            f"prints \"Date Posted: October 3, 2024\"; Shoprite Checkers "
            f"notified the NCC on 16 September 2024 and the sell-by dates are "
            f"\"10 September 2024 to 08 October 2024\". The page's metadata "
            f"gives a published time of 2024-10-03 and a modified time of "
            f"2026-08-15. The row's Date of 2026-11-03 came from the news "
            f"article that discovered it (citizen.co.za), not from the "
            f"authority page. "
            f"The review agent's 2026-11-09 verdict reached the right outcome "
            f"on the WRONG ground: it said Date, Company, Brand, Product, "
            f"Pathogen, Reason, Country and Region \"cannot be confirmed from "
            f"the page\", and in fact the page confirms every one of them "
            f"except the Date — manufactured by \"BM Foods (a member of Sea "
            f"Harvest Group)\", the Deli Hummus range (Traditional 125 g and "
            f"300 g, Reduced Fat 125 g, Zataar 125 g and 300 g, Red Pepper "
            f"125 g and 300 g, Caramelised Onion 125 g), \"the detection of "
            f"Listeria Monocytogenes which is a species of foodborne "
            f"bacteria\", no illnesses reported. "
            f"NOT promoted, NOT re-dated and NOT removed: the re-promotion "
            f"guard already blocks this URL as already-in-Rejected, and an "
            f"archive is append-only. Annotated so that no later pass "
            f"resurrects a 2024 recall as a 2026 one. "
            f"[root cause, NOT patched here: the gap-finder recency guard "
            f"(pipeline/_gap_finder_guards, MAX_AGE_DAYS=30) reads the ROW's "
            f"Date, and a row whose Date came from a news article arrives "
            f"looking days old however old the notice is. Closing that needs "
            f"the authority page's own date at ingestion — a fetch and a "
            f"design decision, not a morning edit.]]"),
    },
]


RECALLS_FIXES = [
    # A — the URL becomes the canonical HTML notice.
    {
        "url": DE_PDF,
        "set": {"URL": DE_HTML},
        "note": (
            f"[{STAMP}: URL repaired from the press-release PDF blob "
            f"(…_Presse.pdf?__blob=publicationFile&v=2) to the regulator's "
            f"own HTML notice at the same address, "
            f"{DE_HTML} — which that page's og:url declares canonical. The "
            f"PDF is why the confirm agent rejected this row overnight: its "
            f"keyword check found none of ['norderstedter','fleisch'," 
            f"'wurstwaren','peter','faden','rinder'] in a PDF's bytes and "
            f"reported the page as not naming the firm. The HTML notice was "
            f"opened this morning and names the firm as \"Norderstedter "
            f"Fleisch- und Wurstwaren Peter Faden GmbH & Co KG\", gives "
            f"\"Grund der Meldung: Krankheitserreger\" and \"… in einer "
            f"Einzeluntersuchung Shigatoxinbildende Escherichia coli (STEC) "
            f"nachgewiesen wurden\", and lists 14 Produktbezeichnungen and 43 "
            f"lot codes across 15 federal states. "
            f"REGULATOR UPDATE OF 09.11.2026, recorded here and NOT given a "
            f"row of its own because it amends this already-published recall "
            f"rather than announcing a new one — Aktualisierungshinweise, "
            f"verbatim: \"09.11.2026: Die Produktbezeichnungen sowie die "
            f"betroffenen Chargen wurden korrigiert. Anstelle der Chargen "
            f"260926-12.11.2026 und 260927-13.11.2026 sind die Chargen "
            f"260924-12.11.2026 und 260925-13.11.2026 betroffen.\" "
            f"Erstveröffentlichung stays 06.11.2026, so Date is unchanged.]"),
    },
    # C — Swiss dialect Product.
    {
        "url": CH_BIRA,
        "set": {"Product": (
            "Seedorf pear-filled pastry loaves (Seedorfer Biräweggä) with and "
            "without nuts, whole and half; small Biräweggä with nuts; and "
            "dried-fruit Biräweggä (Dörrobstbiräweggä)")},
        "note": (
            f"[{STAMP}: Product translated from the FSVO title already quoted "
            f"verbatim in this row's own Notes. Biräweggä is Swiss German for "
            f"Birnenweggen, a pear-filled pastry loaf; Seedorfer is the place "
            f"name Seedorf; Dörrobst is dried fruit. The dialect name is kept "
            f"beside the English gloss, as this register keeps regional and "
            f"protected names. No pack size, lot or date is added — the row "
            f"carries none. [original product: Seedorfer Biräweggä mit/ohne "
            f"Nuss ganz/halb, Biräweggä klein mit Nuss und "
            f"Dörrobstbiräweggä]]"),
        "drop_stamps": ["[needs cleanup: Product not in English]"],
    },
    # C — French Product AND the mixed-language Reason.
    {
        "url": FR_POPS,
        "set": {"Product": "Breaded chicken pops",
                "Reason": "Detection of Salmonella — serotyping pending"},
        "note": (
            f"[{STAMP}: Product and Reason translated from this row's own "
            f"text. The Reason was MIXED-LANGUAGE — half English, half French "
            f"— and carried no stamp: the detector marked the Product and "
            f"missed the Reason because its non-English part is two words. "
            f"\"attente sérotypage\" is RappelConso's note that the Salmonella "
            f"serotype is not yet known; the register does not name a serotype "
            f"it has not been given. [original product: pops de poulet pané] "
            f"[original Reason (fr): \"Detection of Salmonella - attente "
            f"sérotypage\"]]"),
        "drop_stamps": ["[needs cleanup: Product not in English]"],
    },
    # C — the STALE Spanish stamp. No field is set.
    {
        "url": ES_HAM,
        "set": {},
        "note": (
            f"[{STAMP}: dropped a STALE \"needs cleanup\" stamp. This Product "
            f"is already English — tools/repair_2026_11_08.py replaced the "
            f"search-engine snippet that used to be here and did not remove "
            f"the stamp, so reviewer 3 and the offline promoter have been "
            f"re-reading a repaired row every morning since. Not one field is "
            f"changed by this edit. The row's separate \"[date-unknown]\" flag "
            f"is left in place: the AESAN alert page was not opened this "
            f"morning, so the Date is still unconfirmed.]"),
        "drop_stamps": ["[needs cleanup: Product not in English]"],
    },
]

ARCHIVE_NOTES = [
    {
        "sheet": "Rejected", "column": "RejectReason", "url": DE_PDF,
        "reviewed": f"superseded by {STAMP}",
        "prefix": (
            f"[SUPERSEDED {STAMP} — the rejection ground is contradicted by "
            f"the recall itself. The confirm agent refused this row because "
            f"'page does not mention any of [norderstedter, fleisch, "
            f"wurstwaren, peter, faden, rinder]'. The cited address is the "
            f"press-release PDF blob, and a keyword check over a PDF's bytes "
            f"finds no German words — 'I could not read it' is not 'it is "
            f"gone'. The regulator's own HTML notice, read {STAMP} at "
            f"{DE_HTML}, names the firm as 'Norderstedter Fleisch- und "
            f"Wurstwaren Peter Faden GmbH & Co KG' and the hazard as "
            f"'Shigatoxinbildende Escherichia coli (STEC)'. The PUBLISHED row "
            f"in Recalls wins and now cites that HTML notice. This archive row "
            f"is kept as the audit trail of the false verdict.] "),
    },
    {
        "sheet": "Weekly_Rejected", "column": "RejectionReason", "url": DE_PDF,
        "reviewed": f"superseded by {STAMP}",
        "prefix": (
            f"[SUPERSEDED {STAMP} — DUPLICATE of the published row. This is "
            f"the Germany gap finder's news-discovered copy (tagesschau.de) of "
            f"the same STEC recall, at the same regulator address. The "
            f"PUBLISHED row in Recalls wins; it now cites the regulator's HTML "
            f"notice rather than the press-release PDF.] "),
    },
    {
        "sheet": "Weekly_Rejected", "column": "RejectionReason", "url": AT_PINE,
        "reviewed": f"superseded by {STAMP}",
        "prefix": (
            f"[SUPERSEDED {STAMP} — DUPLICATE of the published row. Product "
            f"here is 'Ihre Datenschutzeinstellungen', the AGES cookie-consent "
            f"heading: the LLM extraction timed out and the title-only "
            f"fallback read the page's chrome instead of the notice. The "
            f"recall itself was captured correctly and is PUBLISHED in Recalls "
            f"as 'Pine nuts 60g and Salad kernels mix 100g', Salmonella, "
            f"2026-11-05 — confirmed {STAMP} on the AGES listing, which prints "
            f"'Alnatura Pinienkerne 60g und Alnatura Salatkerne-Mix 100g', "
            f"05.11.2026, Firma 'Alnatura Produktions- und Handels GmbH', "
            f"Rückrufgrund 'Salmonellen'. The published copy wins. The code "
            f"half is pipeline/_bot_wall.FURNITURE_TITLES, in this same "
            f"commit.] "),
    },
    {
        "sheet": "Weekly_Rejected", "column": "RejectionReason", "url": AT_TOM,
        "reviewed": f"superseded by {STAMP}",
        "prefix": (
            f"[SUPERSEDED {STAMP} — DUPLICATE of the published row. Same "
            f"defect as the Alnatura row: Product is the AGES consent heading "
            f"'Ihre Datenschutzeinstellungen'. The recall is PUBLISHED in "
            f"Recalls as 'Cocktail tomatoes'. The AGES page could NOT be "
            f"opened {STAMP} (the fetch failed with a client error), so the "
            f"published row's Date of 2026-10-16 is left exactly as it stands "
            f"and this annotation claims nothing about it. NOTE FOR A LATER "
            f"PASS: the archive copies of this one URL carry four different "
            f"Dates — 2026-10-14, 2026-10-16, 2026-10-26, 2026-10-28 — which "
            f"are the days the extractor retried, not publication dates.] "),
    },
]


def _head(ws):
    return [c.value for c in next(ws.iter_rows(min_row=1, max_row=1))]


def _rowdict(ws, head, r):
    return {h: ws.cell(r, i + 1).value for i, h in enumerate(head)}


def _set(ws, head, r, field, value):
    ws.cell(r, head.index(field) + 1).value = value


def _key(u):
    from pipeline.merge_master import _normalize_url_for_dedup as k
    return k(str(u or ""))


def _find_rows(ws, url):
    head = _head(ws)
    col = head.index("URL") + 1
    target = _key(url)
    return [r for r in range(2, ws.max_row + 1)
            if _key(ws.cell(r, col).value) == target]


def _apply_notes(ws, head, r, row, note, drop_stamps):
    notes = str(row.get("Notes") or "").strip()
    if notes.lower() in ("none", "nan"):
        notes = ""
    for stamp in drop_stamps or ():
        notes = notes.replace(stamp, " ")
    notes = re.sub(r"[ \t]{2,}", " ", (notes + " " + note).strip())
    _set(ws, head, r, "Notes", notes)


def _fix_sheet(wb, sheet, specs, required=True) -> int:
    if sheet not in wb.sheetnames:
        return 0
    ws = wb[sheet]
    head = _head(ws)
    done = 0
    for spec in specs:
        hit = False
        for r in _find_rows(ws, spec["url"]):
            row = _rowdict(ws, head, r)
            if STAMP in str(row.get("Notes") or ""):
                print(f"  {sheet}:{r}: already carries {STAMP}, left alone")
                hit = True
                continue
            for field, value in spec["set"].items():
                if field not in head:
                    continue
                _set(ws, head, r, field, value)
            _apply_notes(ws, head, r, row, spec["note"], spec.get("drop_stamps"))
            done += 1
            hit = True
            print(f"  {sheet}:{r}: {str(row.get('Source'))[:18]!r} "
                  f"{spec['url'][-44:]}")
        if not hit and required:
            raise SystemExit(
                f"{sheet}: no row at {spec['url']} — main has moved under this "
                f"repair script; re-read the sheet before re-running")
    return done


def annotate_archives(wb, specs) -> int:
    done = 0
    for spec in specs:
        sheet = spec["sheet"]
        if sheet not in wb.sheetnames:                      # pragma: no cover
            continue
        ws = wb[sheet]
        head = _head(ws)
        rows = _find_rows(ws, spec["url"])
        if not rows:
            raise SystemExit(
                f"{sheet}: no archive row at {spec['url']} — main has moved "
                f"under this repair script")
        col = spec["column"]
        if col not in head:                                 # pragma: no cover
            raise SystemExit(f"{sheet} has no {col} column")
        for r in rows:
            row = _rowdict(ws, head, r)
            cur = str(row.get(col) or "").strip()
            if cur.lower() in ("none", "nan"):
                cur = ""
            if STAMP in cur:
                print(f"  {sheet}:{r}: already annotated, left alone")
                continue
            _set(ws, head, r, col, (spec["prefix"] + cur).strip())
            if "Reviewed" in head:
                _set(ws, head, r, "Reviewed", spec["reviewed"])
            done += 1
            print(f"  {sheet}:{r}: {spec['reviewed']} "
                  f"{str(row.get('Date'))[:10]} {str(row.get('Source'))[:16]}")
    return done


def check_the_two_red_tests_are_fixed(wb) -> None:
    """Re-run the failing assertion in-process, so a silent miss cannot ship."""
    RESOLVED = ("superseded", "duplicate", "unresolved contradiction",
                "broken provenance", "already_approved")
    ws = wb["Recalls"]
    head = _head(ws)
    ucol = head.index("URL") + 1
    published = {_key(ws.cell(r, ucol).value)
                 for r in range(2, ws.max_row + 1)} - {"", "nan"}
    silent = []
    for sheet, col in (("Weekly_Rejected", "RejectionReason"),
                       ("Rejected", "RejectReason")):
        if sheet not in wb.sheetnames:                      # pragma: no cover
            continue
        a = wb[sheet]
        ah = _head(a)
        if col not in ah:                                   # pragma: no cover
            continue
        au = ah.index("URL") + 1
        ac = ah.index(col) + 1
        for r in range(2, a.max_row + 1):
            if _key(a.cell(r, au).value) not in published:
                continue
            reason = str(a.cell(r, ac).value or "").lower()
            if not any(m in reason for m in RESOLVED):
                silent.append((sheet, r, str(a.cell(r, 1).value)[:10]))
    if silent:
        raise SystemExit(
            f"still {len(silent)} silent archive row(s) sharing a URL with a "
            f"published recall: {silent[:6]}. The two red tests would stay "
            f"red — do not ship this.")
    print("  OK: no archive row shares a URL with a published recall in "
          "silence (both red tests now pass on the data)")


def queue_manual_rows() -> int:
    """D. Through the real code path, never straight into Recalls."""
    if not ROWS_JSON.exists():                              # pragma: no cover
        raise SystemExit(f"missing {ROWS_JSON}")
    proc = subprocess.run(
        [sys.executable, str(ROOT / "tools" / "add_manual_row.py"),
         str(ROWS_JSON)],
        cwd=str(ROOT), capture_output=True, text=True)
    sys.stdout.write(proc.stdout)
    sys.stderr.write(proc.stderr)
    if proc.returncode != 0:
        # rcl=…36103 is the same firm, hazard and day as …36102 under its own
        # CAA recall id. If the near-duplicate rule refuses it, that refusal
        # is correct on its own terms and this pass accepts it.
        print(f"  NOTE: add_manual_row exited {proc.returncode} — read the "
              f"per-row verdict lines above. A near-duplicate refusal of "
              f"rcl=00000036103 is an accepted outcome.")
    return 0


def main() -> int:
    if not XLSX.exists():
        raise SystemExit(f"workbook not found: {XLSX}")

    wb = openpyxl.load_workbook(XLSX)

    print("A+C. Recalls: the PDF->HTML URL repair and three translations")
    recalls_fixed = _fix_sheet(wb, "Recalls", RECALLS_FIXES)
    print("A+C. Weekly_Review mirror (same specs; not every row is mirrored)")
    mirror = _fix_sheet(wb, "Weekly_Review", RECALLS_FIXES, required=False)
    print("E+F. Pending: the Italian histamine label and the 2024 ZA recall")
    pending_fixed = _fix_sheet(wb, "Pending", PENDING_FIXES)
    print("A+B. Archive annotations: three rows published AND rejected")
    annotated = annotate_archives(wb, ARCHIVE_NOTES)
    print("Verify the two red tests are fixed on the data")
    check_the_two_red_tests_are_fixed(wb)

    wb.save(XLSX)
    wb.close()

    print("D. Queue the seven verified Japanese misses")
    queue_manual_rows()

    print(f"\nrepair_2026_11_10: Recalls {recalls_fixed} fixed, "
          f"Weekly_Review {mirror} mirrored, Pending {pending_fixed} fixed, "
          f"{annotated} archive row(s) annotated, 0 rows removed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
