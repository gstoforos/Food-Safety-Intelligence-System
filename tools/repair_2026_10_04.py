# -*- coding: utf-8 -*-
"""Morning fix pass, 2026-10-04. Five faults, all found on the live register.

Run on a FRESH clone of main. Every fact written below was read on an
authority page or on the row's own URL, and the Notes say which.

Removing a row from Recalls trips test_register_never_shrinks unless the
COMMIT MESSAGE carries a deletion marker, so the commit for this run must
contain "remove-rows".

FAULT A — A DRUG RECALL IN A FOOD REGISTER
------------------------------------------
    FDA · 2026-10-01 · Greenwich Rx · "Glutathione Injection" · endotoxin
    .../greenwich-rx-issues-voluntary-nationwide-recall-compounded-
    glutathione-due-elevated-endotoxin-levels

Greenwich Rx is a 503A compounding pharmacy in Tomball, Texas; the product
is a compounded sterile injectable. It is not food. Verified 2026-10-04 by
web search: American Pharmaceutical Review and Newsroom America both report
"Greenwich Rx, a 503A compounding pharmacy in Tomball, Texas, is
voluntarily recalling specific lots of compounded glutathione to the
consumer level over potential elevated endotoxin levels", and four other
Texas pharmacies issued the same recall.

It passed all three reviewers and the publish gate on 2026-10-04. Three
tests broke on it the same morning, each naming a different symptom of the
one cause:

    test_alert_vocab::test_every_row_matches_at_least_one_pathogen_term
        -> "rows no pathogen alert can reach: ['endotoxin']"
    test_curator_scope_2026_09_09::test_no_published_row_is_refused...
        -> "the curator refuses the hazard of published row(s):
            ['endotoxin'] — either the class map is missing vocabulary or
            these rows do not belong in Recalls"
    test_a_recall_is_not_both_published_and_rejected::
        test_every_shared_url_says_which_copy_wins[Weekly_Rejected-...]
        -> the same URL sits in Weekly_Rejected, silently contradicting it

The curator's test asked the right question. The answer is the second
branch: the row does not belong in Recalls. "endotoxin" is NOT added to
tools/alert_vocab.py — doing that would teach the register that a
parenteral drug hazard is a food hazard and let the next four through.

The gate now refuses the shape (pipeline/_publish_gate.py rule 10,
_non_food_drug_blockers: a non-food dosage form in Product, or a
compounded-drug marker in Company/Brand/URL). Checked against all 1,910
published rows: this is the only one it refuses.

FAULT B — THE SAME RECALL PUBLISHED TWICE, THE SECOND COPY MISDATED
-------------------------------------------------------------------
    r54   FDA                     · 2026-09-29 · STEC             · W39
    r351  FDA (via Google News)   · 2026-09-01 · E. coli O26:H11  · W36

One event, two rows. r54 cites Sierra Nevada Cheese Company's own FDA
recall notice. r351 was added 2026-10-04 and cites FDA's outbreak
INVESTIGATION page, which is not the firm's per-recall notice (R3), and
carries Date 2026-09-01 — a date that appears nowhere on that page. It was
read off the "september-2026" in the URL slug, so the recall filed into
September week 36 instead of week 39, one month and four weeks early.

Both copies claim Outbreak=1 for the same 13 illnesses, so every
"top outbreaks" list and every outbreak count double-counted it.

r351 is removed; r54 stays and is annotated. The gate now refuses a Source
that names its discovery channel (rule 11), which is what "FDA (via Google
News)" is and what broke
test_monitored_sources::test_every_published_source_is_counted.

FAULT C — TWO ITALIAN RECALLS MISTRANSLATED, BOTH UNPUBLISHED
-------------------------------------------------------------
Both sat in Pending with a Product that names the wrong food:

    "Sweet prosciutto"           <- salamella dolce.  A fresh pork SAUSAGE,
                                    not a dry-cured ham. The row's own URL
                                    is the ministry's PDF named
                                    "SALAMELLA DOLCE INTERA".
    "Smoked salami with truffle" <- salame STAGIONATO al tartufo. Stagionato
                                    is aged/cured; smoked is affumicato.

Verified 2026-10-04 on ilfattoalimentare.it, the discovery source the rows
already name, each article linking the Ministero della Salute PDF that is
the row's URL. Only the product NAME is corrected — no lot, weight or
use-by date is imported from a news page.

Also stripped: the extractor's own empty-identifier template text,
"(Recall ID N/A)" and "(Recall ID not provided)", which would have been
published verbatim to subscribers. This is the same class of defect as the
"Recall ID 842632" prompt example already named in publish gate rule 2.

FAULT D — TWO FDA ROWS BLOCKED ON A FIELD THEIR OWN NOTICE STATES
-----------------------------------------------------------------
Both were "FDA HTML fallback — claude-check needs to enrich Date+Pathogen"
with Company, Product and Reason all set to the page headline. Both notices
were read in full on 2026-10-04 and the fields filled from them.

FAULT E — AN EXTENSION IS NOT A NEW RECALL
------------------------------------------
    r195 (published) .../gias-foods-...-because-possible        2026-09-15
    Pending          .../gias-foods-...-because-possible-0      2026-10-01

Read both: the first recalls lots L6079C and L6080C, the second recalls ALL
lot codes of the same 22 oz / UPC 194346442706 product for the same
Listeria monocytogenes. That is the already-published recall widened, not a
second recall. The published row records the extension; no duplicate is
created.
"""
from __future__ import annotations

import datetime as dt
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "recalls.json"

TODAY = dt.date.today().isoformat()

# ── Rows identified by their authority URL — the only stable key. Named
#    rather than matched by rule, so this script can never touch a row its
#    author did not read.
GREENWICH_URL = (
    "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/"
    "greenwich-rx-issues-voluntary-nationwide-recall-compounded-glutathione-"
    "due-elevated-endotoxin-levels")

SIERRA_NEWS_URL = (
    "https://www.fda.gov/food/outbreaks-foodborne-illness/"
    "outbreak-investigation-e-coli-o26h11-raw-milk-cheese-september-2026")

SIERRA_OFFICIAL_URL = (
    "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/"
    "sierra-nevada-cheese-company-recalls-graziers-raw-milk-cheese-because-"
    "possible-health-risk")

GIAS_PUBLISHED_URL = (
    "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/"
    "gias-foods-inc-recalls-bettergoods-authentic-italian-lemon-alfredo-"
    "fettuccine-because-possible")
GIAS_EXTENSION_URL = GIAS_PUBLISHED_URL + "-0"

WINCO_URL = (
    "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/"
    "winco-foods-boise-id-recalling-following-salsa-bean-dip-trays-due-"
    "inclusion-recalled-peperoncini-gl")

STEWARTS_URL = (
    "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/"
    "stewarts-shop-corp-recalls-heavenly-medley-and-vanilla-chocolate-"
    "strawberry-due-possible-foreign")

SAINT_FRANCIS_URL = (
    "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/"
    "saint-francis-apizza-llc-issues-voluntary-recall-its-frozen-pizzas-"
    "missing-sub-ingredients-label-soy")

SALAMELLA_URL = (
    "https://www.salute.gov.it/new/sites/default/files/external_data/"
    "avvisi_sicurezza_alimentare/cartello%20di%20richiamo%20modello%20"
    "ministero%20SALAMELLA%20DOLCE%20INTERA_1790933640.pdf")

SALAME_TARTUFO_URL = (
    "https://www.salute.gov.it/new/sites/default/files/external_data/"
    "avvisi_sicurezza_alimentare/Modulo%20richiamo%20conforme_1790069655.pdf")

GREENWICH_REASON = (
    "operator review " + TODAY + ": out_of_scope_not_food — Greenwich Rx is "
    "a 503A compounding pharmacy and the recalled product is a compounded "
    "sterile injectable (glutathione for injection). This is a FOOD recall "
    "register (R1, food only); a drug, device or cosmetic recall belongs in "
    "neither Recalls nor the subscriber reports. Verified " + TODAY + " by "
    "web search — American Pharmaceutical Review and Newsroom America both "
    "describe Greenwich Rx as a 503A compounding pharmacy in Tomball, Texas "
    "recalling compounded glutathione to the consumer level. "
    "fda.gov/safety/recalls-market-withdrawals-safety-alerts is a single "
    "bucket for food, drug, device and cosmetic notices, so neither the host "
    "nor the path establishes that a notice is about food, and nothing in "
    "the pipeline asked. The hazard 'endotoxin' resolves to no hazard class "
    "at all, which is why publish-gate rule 8 (a subset test) was vacuous "
    "and rule 1 (Pathogen must not be EMPTY) was satisfied. "
    "pipeline/_publish_gate.py rule 10 now refuses the shape. 'endotoxin' is "
    "deliberately NOT added to tools/alert_vocab.py: it is a parenteral drug "
    "hazard, not a food hazard, and teaching the alert vocabulary otherwise "
    "would admit the next one. Removed from Recalls " + TODAY + "."
)

SIERRA_REMOVAL_REASON = (
    "operator review " + TODAY + ": duplicate_of_published_recall — the same "
    "Sierra Nevada Cheese Company / Graziers raw milk cheese event is "
    "already published from the firm's own FDA recall notice at "
    + SIERRA_OFFICIAL_URL + " with Date 2026-09-29, report_week W39, and the "
    "same 13 illnesses. This copy cites FDA's outbreak INVESTIGATION page, "
    "which is not the regulator's per-recall notice (R3), and its Date "
    "2026-09-01 appears nowhere on that page — it is the 'september-2026' in "
    "the URL slug read as a date, which filed the recall into September and "
    "report_week W36, four weeks and one month early. Both copies carried "
    "Outbreak=1 for one outbreak, so every outbreak count and every top-"
    "outbreak list double-counted it. Source was 'FDA (via Google News)': a "
    "discovery channel, not an authority, which is what broke "
    "test_monitored_sources::test_every_published_source_is_counted; "
    "publish-gate rule 11 now refuses a Source that names its mirror. "
    "Removed from Recalls " + TODAY + "; the 2026-09-29 copy is what the "
    "register says."
)

SAINT_FRANCIS_REASON = (
    "operator review " + TODAY + ": out_of_scope_allergen_only — FDA's own "
    "recalls listing gives the reason as 'Undeclared soy allergen' (read "
    + TODAY + " on fda.gov/safety/recalls-market-withdrawals-safety-alerts, "
    "entry dated 10/03/2026). Allergen-only and labelling recalls are "
    "excluded from this register by the printed AFTS scope. The row had been "
    "sitting at status 'pending_enrichment' since 2026-10-04 with Company, "
    "Product and Reason all set to the page headline, waiting for an "
    "enrichment that would never make it publishable. Archived with a "
    "terminal reason so nothing re-ingests it."
)

LIVE_SHEETS = ("Recalls", "Pending", "Weekly_Review", "Weekly_Rejected")

_RECALL_ID_NOISE = re.compile(
    r"\s*\(\s*Recall\s*ID\s*(?:N/?A|not\s+provided|not\s+specified|unknown|"
    r"none)\s*\)\s*", re.IGNORECASE)


def _head(ws):
    return [str(c.value) for c in ws[1]]


def _col(head, name):
    return head.index(name) + 1 if name in head else None


def _find_rows(ws, url):
    head = _head(ws)
    ucol = _col(head, "URL")
    if not ucol:
        return []
    return [r for r in range(2, ws.max_row + 1)
            if str(ws.cell(r, ucol).value or "").strip() == url]


def _stamp(ws, r, text):
    head = _head(ws)
    ncol = _col(head, "Notes")
    if not ncol:
        return
    prior = str(ws.cell(r, ncol).value or "").strip()
    ws.cell(r, ncol).value = (prior + " " + text).strip()


def _archive(wb, sheet, row, head_src, reason, by):
    """Append a row to an archive sheet with a terminal reason."""
    if sheet not in wb.sheetnames:
        print(f"  WARNING: no {sheet} sheet — row removed with no archive "
              f"entry, which loses the audit trail")
        return False
    arch = wb[sheet]
    ahead = _head(arch)
    out = []
    for h in ahead:
        if h in ("RejectReason", "RejectionReason"):
            out.append(reason)
        elif h == "RejectedBy":
            out.append(by)
        elif h == "RejectedAt":
            out.append(TODAY)
        elif h == "Status":
            out.append("rejected")
        elif h == "Reviewed":
            out.append("Y")
        elif h == "Week_Added":
            out.append(TODAY)
        else:
            out.append(row.get(h, ""))
    arch.append(out)
    print(f"  {sheet}: archived with a terminal reason ({len(ahead)} columns)")
    return True


# ── FAULT A ────────────────────────────────────────────────────────────────
def remove_greenwich(wb) -> int:
    removed = 0
    ws = wb["Recalls"]
    head = _head(ws)
    targets = _find_rows(ws, GREENWICH_URL)
    for r in reversed(targets):
        row = {h: ws.cell(r, i + 1).value for i, h in enumerate(head)}
        print(f"  Recalls row {r}: {row.get('Date')} {row.get('Company')} "
              f"{row.get('Product')!r} — removing (not food)")
        _archive(wb, "Rejected", row, head, GREENWICH_REASON,
                 "operator review " + TODAY)
        ws.delete_rows(r, 1)
        removed += 1

    # The Weekly_Rejected copy must stop silently contradicting the register.
    if "Weekly_Rejected" in wb.sheetnames:
        wr = wb["Weekly_Rejected"]
        whead = _head(wr)
        rrcol = _col(whead, "RejectionReason")
        rvcol = _col(whead, "Reviewed")
        for r in _find_rows(wr, GREENWICH_URL):
            prior = str(wr.cell(r, rrcol).value or "").strip() if rrcol else ""
            if rrcol:
                wr.cell(r, rrcol).value = (
                    f"[UPHELD {TODAY} — this archive row is the register's "
                    f"answer. The Recalls copy was removed {TODAY} as out of "
                    f"scope: not food. The refusal recorded below named the "
                    f"wrong defect (a missing Date and a skipped reviewer), "
                    f"which is why repairing those defects let the row be "
                    f"re-promoted; the real reason it cannot be published is "
                    f"that a compounded glutathione injection from a 503A "
                    f"compounding pharmacy is a drug, not a food.] " + prior)
            if rvcol:
                wr.cell(r, rvcol).value = "UPHELD"
            print(f"  Weekly_Rejected row {r}: marked UPHELD")

    # Weekly_Review is the live rolling review slice, not an archive.
    if "Weekly_Review" in wb.sheetnames:
        wv = wb["Weekly_Review"]
        for r in reversed(_find_rows(wv, GREENWICH_URL)):
            wv.delete_rows(r, 1)
            print(f"  Weekly_Review row {r}: removed")
    return removed


# ── FAULT B ────────────────────────────────────────────────────────────────
def remove_sierra_duplicate(wb) -> int:
    removed = 0
    ws = wb["Recalls"]
    head = _head(ws)
    for r in reversed(_find_rows(ws, SIERRA_NEWS_URL)):
        row = {h: ws.cell(r, i + 1).value for i, h in enumerate(head)}
        print(f"  Recalls row {r}: {row.get('Date')} {row.get('Source')} "
              f"— removing (duplicate, misdated)")
        _archive(wb, "Rejected", row, head, SIERRA_REMOVAL_REASON,
                 "operator review " + TODAY)
        ws.delete_rows(r, 1)
        removed += 1

    if removed:
        # Say so on the copy that survives.
        for name in LIVE_SHEETS:
            if name not in wb.sheetnames:
                continue
            for r in _find_rows(wb[name], SIERRA_OFFICIAL_URL):
                _stamp(wb[name], r,
                       f"[dedupe {TODAY}: a second copy of this event was "
                       f"published {TODAY} from FDA's outbreak-investigation "
                       f"page ({SIERRA_NEWS_URL}) with Date 2026-09-01 — the "
                       f"'september-2026' in that slug read as a date — "
                       f"Source 'FDA (via Google News)' and report_week W36, "
                       f"and with Outbreak=1 for the same 13 illnesses as "
                       f"this row. That copy was removed; this one, from the "
                       f"firm's own FDA recall notice, is what the register "
                       f"says about this event]")
                print(f"  {name} row {r}: annotated as the surviving copy")

    if "Weekly_Review" in wb.sheetnames:
        wv = wb["Weekly_Review"]
        for r in reversed(_find_rows(wv, SIERRA_NEWS_URL)):
            wv.delete_rows(r, 1)
            print(f"  Weekly_Review row {r}: removed")
    return removed


# ── FAULT C ────────────────────────────────────────────────────────────────
#: Product corrections, keyed by the row's own authority URL. The English
#: value, the original Italian to keep in Notes, and where it was read.
ITALIAN_PRODUCT_FIXES = {
    SALAMELLA_URL: dict(
        was="Sweet prosciutto",
        product="Sweet salamella (fresh pork sausage), whole, vacuum-packed",
        original="salamella dolce (intera, sottovuoto)",
        note=(
            "[product-repair {today}: Product read 'Sweet prosciutto'. The "
            "food is salamella dolce — a fresh pork SAUSAGE, not a dry-cured "
            "ham. Read {today} on the row's own authority URL, whose filename "
            "is 'cartello di richiamo modello ministero SALAMELLA DOLCE "
            "INTERA', and on ilfattoalimentare.it/richiamo-salamella-dolce.html"
            ", the discovery source this row already names, which links that "
            "same Ministero della Salute PDF and gives the producer as "
            "Franchi Salumi Srl, Follonica (GR) and the notice date as "
            "2 October 2026 — the Date this row already carries. No lot, "
            "weight or use-by date is imported from the news page.] "
            "[original product: salamella dolce (intera, sottovuoto)]"),
    ),
    SALAME_TARTUFO_URL: dict(
        was="Smoked salami with truffle",
        product="Aged salami with truffle",
        original="salame stagionato al tartufo",
        note=(
            "[product-repair {today}: Product read 'Smoked salami with "
            "truffle'. The Italian is 'salame STAGIONATO al tartufo' — "
            "stagionato is aged/cured; smoked is affumicato. Read {today} on "
            "ilfattoalimentare.it/richiamo-salame-stagionato-tartufo.html, "
            "the discovery source this row already names, which links the "
            "Ministero della Salute PDF that is this row's URL and gives the "
            "producer as Salumificio F.lli Costantini Srl and the notice date "
            "as 22 September 2026 — the Date this row already carries.] "
            "[original product: salame stagionato al tartufo]"),
    ),
}


def repair_italian_products(wb) -> int:
    fixed = 0
    for url, spec in ITALIAN_PRODUCT_FIXES.items():
        for name in wb.sheetnames:
            ws = wb[name]
            head = _head(ws)
            pcol = _col(head, "Product")
            if not pcol or not _col(head, "URL"):
                continue
            for r in _find_rows(ws, url):
                cur = str(ws.cell(r, pcol).value or "").strip()
                if cur != spec["was"]:
                    continue
                ws.cell(r, pcol).value = spec["product"]
                _stamp(ws, r, spec["note"].format(today=TODAY))
                print(f"  {name} row {r}: Product {cur!r} -> "
                      f"{spec['product']!r}")
                fixed += 1
    return fixed


def strip_recall_id_noise(wb) -> int:
    """Remove the extractor's own empty-identifier template text."""
    fixed = 0
    for name in wb.sheetnames:
        ws = wb[name]
        head = _head(ws)
        rcol = _col(head, "Reason")
        if not rcol:
            continue
        for r in range(2, ws.max_row + 1):
            cur = str(ws.cell(r, rcol).value or "")
            if not cur or not _RECALL_ID_NOISE.search(cur):
                continue
            new = _RECALL_ID_NOISE.sub(" ", cur).strip().rstrip(",;").strip()
            if not new or new == cur:
                continue
            ws.cell(r, rcol).value = new
            _stamp(ws, r,
                   f"[reason-cleanup {TODAY}: the extractor's own "
                   f"empty-identifier template text was stripped from Reason "
                   f"({cur[len(new):].strip()!r}) — it names no identifier "
                   f"and would have been published verbatim. Same class of "
                   f"defect as the 'Recall ID 842632' prompt example named in "
                   f"publish-gate rule 2]")
            print(f"  {name} row {r}: Reason {cur!r} -> {new!r}")
            fixed += 1
    return fixed


# ── FAULT D ────────────────────────────────────────────────────────────────
#: Fields read in full on the FDA notice at each row's own URL on 2026-10-04.
FDA_ENRICHMENTS = {
    WINCO_URL: dict(
        Date="2026-10-01",
        Company="WinCo Foods LLC",
        Brand="WinCo Foods",
        Product=("Salsa Bean Dip Tray Express, 54 oz (UPC 2-67173) and Salsa "
                 "Bean Dip Tray, 16-inch catering tray, 104 oz "
                 "(UPC 2-67240); Best If Used By 14 Aug 2026 through "
                 "25 Sep 2026"),
        Pathogen="Foreign material (pest)",
        Reason=("Recalled because a single lot of the Golden Greek "
                "Peperoncini used as an ingredient was recalled by G. L. "
                "Mezzetta Inc. due to potential pest inclusion"),
        Tier=3,
        note=(
            "[enrich {today}: this row was 'FDA HTML fallback — claude-check "
            "needs to enrich Date+Pathogen' with Company, Product and Reason "
            "all set to the page headline, and promote_gate_passing refused "
            "it on 'Pathogen is empty'. The hazard was in the row's own URL "
            "all along: the slug ends 'due-inclusion-recalled-peperoncini-gl'"
            ", truncated from 'from G.L. Mezzetta'. The FDA notice at this "
            "row's own URL was read in full {today}: announcement date "
            "October 1 2026 (FDA's recalls listing dates the posting "
            "10/02/2026); recalling firm WinCo Foods LLC; two Salsa Bean Dip "
            "Tray items, 54 oz and 104 oz; and verbatim, 'a single lot of "
            "their Golden Greek Peperoncini has been recalled due to "
            "potential pest inclusion'. Pest is in the printed AFTS scope. "
            "Pathogen is set to 'Foreign material (pest)', the same value the "
            "upstream Mezzetta recall already carries in Recalls (2026-09-26)"
            ", so one alert reaches both]"),
    ),
    STEWARTS_URL: dict(
        Date="2026-10-02",
        Company="Stewart's Shops Corp.",
        Brand="Stewart's Shops",
        Product=("Heavenly Medley half-gallon ice cream (UPC 0 82086 44371 "
                 "1, Best By 10 Feb 2027) and Vanilla-Chocolate-Strawberry "
                 "half-gallon ice cream (UPC 0 82086 44377 3, Best By 11 Feb "
                 "2027), 1.89 L paperboard cartons"),
        Pathogen="Foreign material (metal)",
        Reason=("Recalled after a metal object was discovered by a consumer "
                "in a half-gallon carton of ice cream"),
        Tier=3,
        note=(
            "[enrich {today}: this row was 'FDA HTML fallback — claude-check "
            "needs to enrich Date+Pathogen' with Product and Reason set to "
            "the page headline, and promote_gate_passing refused it on "
            "'Pathogen is empty'. The hazard was in the row's own URL: the "
            "slug ends 'due-possible-foreign'. The FDA notice at this row's "
            "own URL was read in full {today}: announcement date October 2 "
            "2026; Stewart's Shops Corp. of Saratoga Springs, NY; two "
            "half-gallon flavours with their UPCs and Best By dates; and "
            "verbatim, 'The recall was initiated after a metal object was "
            "discovered by a consumer in a half-gallon carton of ice cream'. "
            "Foreign material is in the printed AFTS scope]"),
    ),
}


def enrich_fda_rows(wb) -> int:
    fixed = 0
    for url, spec in FDA_ENRICHMENTS.items():
        note = spec.pop("note")
        for name in LIVE_SHEETS:
            if name not in wb.sheetnames:
                continue
            ws = wb[name]
            head = _head(ws)
            for r in _find_rows(ws, url):
                changed = []
                for field, value in spec.items():
                    c = _col(head, field)
                    if not c:
                        continue
                    was = ws.cell(r, c).value
                    if str(was or "").strip() == str(value):
                        continue
                    ws.cell(r, c).value = value
                    changed.append(field)
                scol = _col(head, "Status")
                if scol and str(ws.cell(r, scol).value or "") != "pending":
                    ws.cell(r, scol).value = "pending"
                    changed.append("Status")
                if changed:
                    _stamp(ws, r, note.format(today=TODAY))
                    print(f"  {name} row {r}: filled {changed}")
                    fixed += 1
        spec["note"] = note
    return fixed


# ── FAULT E ────────────────────────────────────────────────────────────────
def record_gias_extension(wb) -> int:
    """The -0 notice widens the published recall; it is not a second one."""
    fixed = 0
    for name in LIVE_SHEETS:
        if name not in wb.sheetnames:
            continue
        for r in _find_rows(wb[name], GIAS_PUBLISHED_URL):
            _stamp(wb[name], r,
                   f"[extension {TODAY}: FDA published a second notice for "
                   f"this product on 2026-10-01 at {GIAS_EXTENSION_URL} "
                   f"(the '-0' permalink), widening the recall from lot "
                   f"codes L6079C and L6080C to ALL lot codes of the same "
                   f"22 oz pack, UPC 194346442706, for the same Listeria "
                   f"monocytogenes. Both notices were read {TODAY}. That is "
                   f"this recall extended, not a second recall, so no "
                   f"duplicate row was created]")
            print(f"  {name} row {r}: extension recorded")
            fixed += 1

    # And say so on the Pending copy, which otherwise waits forever.
    if "Pending" in wb.sheetnames:
        ws = wb["Pending"]
        head = _head(ws)
        scol = _col(head, "Status")
        for r in _find_rows(ws, GIAS_EXTENSION_URL):
            row = {h: ws.cell(r, i + 1).value for i, h in enumerate(head)}
            _archive(wb, "Rejected", row, head,
                     "operator review " + TODAY + ": extension_of_published_"
                     "recall — FDA's '-0' permalink of 2026-10-01 widens the "
                     "already-published Gias Foods recall (Recalls, "
                     "2026-09-15, " + GIAS_PUBLISHED_URL + ") from lot codes "
                     "L6079C and L6080C to all lot codes of the same 22 oz "
                     "pack, UPC 194346442706, for the same Listeria "
                     "monocytogenes. Both notices read " + TODAY + ". The "
                     "extension is recorded in the published row's Notes "
                     "rather than published as a second recall.",
                     "operator review " + TODAY)
            ws.delete_rows(r, 1)
            print(f"  Pending row {r}: Gias extension archived, not published")
            fixed += 1
    return fixed


# ── FAULT F — an allergen-only row parked in Pending forever ──────────────
def archive_saint_francis(wb) -> int:
    if "Pending" not in wb.sheetnames:
        return 0
    ws = wb["Pending"]
    head = _head(ws)
    n = 0
    for r in reversed(_find_rows(ws, SAINT_FRANCIS_URL)):
        row = {h: ws.cell(r, i + 1).value for i, h in enumerate(head)}
        _archive(wb, "Rejected", row, head, SAINT_FRANCIS_REASON,
                 "operator review " + TODAY)
        ws.delete_rows(r, 1)
        print(f"  Pending row {r}: Saint Francis Apizza archived "
              f"(allergen-only)")
        n += 1
    return n


# ── FAULT G — the Italian gap finder's refusal is superseded ──────────────
def supersede_italian_refusal(wb) -> int:
    """gap_finder/it refused the salamella row on a vocabulary gap now fixed."""
    if "Weekly_Rejected" not in wb.sheetnames:
        return 0
    ws = wb["Weekly_Rejected"]
    head = _head(ws)
    rrcol = _col(head, "RejectionReason")
    rvcol = _col(head, "Reviewed")
    n = 0
    for r in _find_rows(ws, SALAMELLA_URL):
        prior = str(ws.cell(r, rrcol).value or "").strip() if rrcol else ""
        if rrcol:
            ws.cell(r, rrcol).value = (
                f"[SUPERSEDED {TODAY} — the defect named below was a "
                f"vocabulary gap in pipeline/gap_finder/rules.py, now "
                f"repaired, and this recall is PUBLISHED in Recalls. "
                f"FOREIGN_MATTER could express 'foreign body' in English, "
                f"French and Greek only; Italian 'corpo estraneo' existed "
                f"solely in the _FM_CONTEXT_N licence set, which cannot match "
                f"on its own, so 'possibile presenza di un corpo estraneo' "
                f"reached no hazard branch. 14 of 18 fleet languages had the "
                f"same hole. This row is kept as the audit trail of the "
                f"original refusal; the Recalls copy is what the register "
                f"says.] " + prior)
        if rvcol:
            ws.cell(r, rvcol).value = "SUPERSEDED"
        print(f"  Weekly_Rejected row {r}: marked SUPERSEDED")
        n += 1
    return n


def main() -> int:
    import openpyxl
    if not XLSX.exists():
        print("no workbook at", XLSX)
        return 1
    wb = openpyxl.load_workbook(XLSX)
    before = wb["Recalls"].max_row - 1

    print("FAULT A — a drug recall in a food register")
    n_a = remove_greenwich(wb)
    print(f"  {n_a} removed\n")

    print("FAULT B — the same recall published twice, second copy misdated")
    n_b = remove_sierra_duplicate(wb)
    print(f"  {n_b} removed\n")

    print("FAULT C — two Italian recalls mistranslated")
    n_c = repair_italian_products(wb)
    n_c2 = strip_recall_id_noise(wb)
    print(f"  {n_c} products corrected, {n_c2} Reasons cleaned\n")

    print("FAULT D — two FDA rows blocked on a field their own notice states")
    n_d = enrich_fda_rows(wb)
    print(f"  {n_d} rows filled\n")

    print("FAULT E — an extension is not a new recall")
    n_e = record_gias_extension(wb)
    print(f"  {n_e} rows touched\n")

    print("FAULT F — an allergen-only row parked in Pending")
    n_f = archive_saint_francis(wb)
    print(f"  {n_f} archived\n")

    print("FAULT G — a refusal superseded by the vocabulary fix")
    n_g = supersede_italian_refusal(wb)
    print(f"  {n_g} marked\n")

    total = n_a + n_b + n_c + n_c2 + n_d + n_e + n_f + n_g
    if not total:
        print("nothing to do")
        print("ROWS_REMOVED=0")
        return 0

    wb.save(XLSX)
    after = openpyxl.load_workbook(XLSX)["Recalls"].max_row - 1
    print(f"Recalls {before} -> {after}")

    from pipeline.merge_master import mirror_json_from_xlsx
    n = mirror_json_from_xlsx(XLSX, JSON)
    print(f"recalls.json re-mirrored: {n} rows")

    print(f"ROWS_REMOVED={n_a + n_b}")
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
