# -*- coding: utf-8 -*-
"""Morning fix 2026-10-28 — AESAN language copies, and the row that vanished.

THE HEADLINE
------------
On 2026-10-27 the morning-fix pass read AESAN alert 2026_75 on the agency's
own page, archived the six language copies of it that were clogging Pending,
and queued one clean row:

    AESAN (ES) 2026_75 / ES2026/648 — Arkan "Pure Sesame Tahini", 400 g,
    lot 0257502493, best before 12/2027, Salmonella spp., origin Egypt.
    Tier 1.

The row never landed. Nothing in any sheet of the workbook of 2026-10-28
carries it; the word "Arkan" appears in no row. The other two rows that
pass queued (BVL Abido Sumak, CFIA RA-82850) are both published.

WHY. The archive the script had just written became the thing that blocked
its own replacement. ``_is_human_rejection`` reads any ``operator review``
stamp as a permanent verdict, so the six copies — one of them at the bare
address ``aesan.gob.es/alertas/2026_75``, the same normalised key as the
clean row — turned into a terminal rejection on that URL. The
terminal-rejection guard in ``append_to_pending`` dropped the clean row and
logged it at INFO. ``tools/add_manual_row.py`` reported only the NET growth
of Pending, saw +2, printed "Queued 2 row(s)" and exited 0. The script then
printed ``ROWS_QUEUED=3`` from ``len(NEW_ROWS)``. Three separate places
said the row was fine.

Both halves are fixed in code in the same pass as this script:
  * ``append_to_pending`` now returns a per-row verdict (``outcomes=``) and
    takes ``override_terminal=`` for a row an operator has just verified.
  * ``add_manual_row.py`` prints one line per requested row and exits
    NON-ZERO when any of them did not land.
  * ``_normalize_url_for_dedup`` now strips AESAN's language segment, so one
    alert is one key — the admin.ch fix, on the agency that needed it next.
  * tests/test_a_language_variant_is_never_a_second_row.py and
    tests/test_add_manual_row_reports_every_row.py hold both.
  * scrapers/europe_eu/aesan.py pointed at an index that lists NO alerts;
    repointed at the live one. (1 AESAN row in 30 days vs 5 October alerts.)

WHAT THIS SCRIPT DOES TO THE DATA
---------------------------------
A. Translates three published RappelConso rows whose Product and/or Reason
   is still French, and clears a stale needs-cleanup stamp on a fourth.
   Direct translation only — every original is kept in Notes. No fetch, no
   new facts.
B. Marks Weekly_Rejected's copy of FSA alert 6946 (SuperValu/Centra salmon
   and prawn cocktail, Listeria, outbreak) SUPERSEDED. The same URL is
   PUBLISHED in Recalls; the archive row's verdict is "Unable to fetch
   official regulator URL", a reviewer failing to reach a page, not a
   judgment on the recall. Held by
   tests/test_a_recall_is_not_both_published_and_rejected.py, which fails
   on main today.
C. Annotates every archived language copy of AESAN 2026_72 (Listeria in
   cooked mussels) as a duplicate of the published row, and archives the
   two copies still sitting in Pending. 2026_72 is PUBLISHED at
   aesan.gob.es/alertas/2026_72; the copies can never promote.
D. Fills the Pending row for AESAN 2026_74 (Salmonella spp. in pasteurised
   egg white) from the alert page, read today, and archives its duplicate
   language copy. The row was blocked on Product, which held a search-
   engine snippet.
E. Queues two rows through tools/add_manual_row.py: AESAN 2026_75 (the row
   that vanished, re-verified on the page today, with --override-terminal
   over yesterday's bookkeeping archive) and AESAN 2026_76 / ES2026/647
   (Arla protein pudding, glass fragments, published 27/10/2026, in no
   sheet at all).

NOTHING IS REMOVED FROM Recalls.
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
XLSX = ROOT / "docs" / "data" / "recalls.xlsx"
JSON = ROOT / "docs" / "data" / "recalls.json"
TODAY = "2026-10-28"
STAMP = "morning fix " + TODAY

# ---------------------------------------------------------------------------
# A. translations of published rows (direct translation; originals kept)
# ---------------------------------------------------------------------------
#: url -> {field: new value}, plus the Notes additions and the stamps to drop.
TRANSLATIONS = {
    "https://rappel.conso.gouv.fr/fiche-rappel/23929/interne": {
        "set": {"Product": "Stuffed chicken leg"},
        "note": "[original product: cuisse de poulet farcie] "
                "[" + STAMP + ": Product translated from French. Direct "
                "translation of the row's own text; no fetch, nothing added.]",
        "drop": ["[needs cleanup: Product not in English]"],
    },
    "https://rappel.conso.gouv.fr/fiche-rappel/23938/interne": {
        "set": {"Product": "Salmon pizza",
                "Reason": "Presence of a foreign body"},
        "note": "[original product: pizza saumon] "
                "[original Reason (fr): \"Presence of un corps étranger\"] "
                "[" + STAMP + ": Product and Reason translated from French. "
                "Direct translation of the row's own text; no fetch, nothing "
                "added.]",
        "drop": ["[needs cleanup: Product not in English]",
                 "[needs cleanup: Reason not in English]"],
    },
    "https://rappel.conso.gouv.fr/fiche-rappel/23931/interne": {
        "set": {"Product": "Basil chicken",
                "Reason": "Presence of Listeria bacteria at <10 CFU/g"},
        "note": "[original product: poulet basilic] "
                "[original Reason (fr): \"Présence du germe Listeria "
                "<10ufc/g\"] "
                "[" + STAMP + ": Product and Reason translated from French. "
                "Direct translation of the row's own text; no fetch, nothing "
                "added.]",
        "drop": ["[needs cleanup: Product not in English]",
                 "[needs cleanup: Reason not in English]"],
    },
    # Translated on 2026-10-27 as the bilingual "Olive pâté (pâté aux
    # olives)", with [original product: Pâté aux olives] already in Notes.
    # The stamp kept coming back because it is EARNED: the French half is
    # still in the Product, so pipeline._language.product_needs_english
    # returns True and the offline promoter re-stamps the row on every pass.
    # Measured today: clearing the stamp alone put it straight back. The
    # policy is Product in English with the original in Notes, which the
    # Notes already carry, so the parenthetical comes out.
    "https://rappel.conso.gouv.fr/fiche-rappel/23933/interne": {
        "set": {"Product": "Olive pâté"},
        "note": "[" + STAMP + ": the French half of the bilingual Product "
                "was removed — the original is already recorded above as "
                "[original product: Pâté aux olives]. Clearing the "
                "needs-cleanup stamp without this only had the promoter "
                "re-stamp the row on the next pass.]",
        "drop": ["[needs cleanup: Product not in English]"],
    },
}

# ---------------------------------------------------------------------------
# B. the published-and-rejected contradiction
# ---------------------------------------------------------------------------
FSA_6946 = ("https://alerts.food.gov.uk/article/6946/SuperValu-and-Centra-"
            "recall-salmon-and-prawn-cocktail-products-because-of-"
            "contamination-with-Listeria-monocytogenes")
FSA_6946_NOTE = (
    "[" + STAMP + ": SUPERSEDED — the published Recalls row on this exact "
    "URL wins. The verdict recorded here, \"URL agent: Unable to fetch "
    "official regulator URL\", is a reviewer that could not reach the page, "
    "not a finding about the recall; the row was re-promoted on 2026-10-28 "
    "and the FSA notice is live. Kept for the trail, not a contradiction.]")

# ---------------------------------------------------------------------------
# C / D. AESAN language copies
# ---------------------------------------------------------------------------
AESAN_72 = "/alertas/2026_72"
AESAN_72_PUBLISHED = "https://aesan.gob.es/alertas/2026_72"
# WHY EVERY ONE OF THESE NOTES SAYS "SUPERSEDED".
#
# Collapsing AESAN's language segment means an archived /en/ copy and the
# bare Spanish address are now the SAME dedup key. An archive row stamped
# by a person reads as a terminal verdict (_is_human_rejection), so a
# "duplicate" note on a language copy would put a permanent block on the
# canonical URL of the alert — and that is precisely how yesterday's pass
# lost 2026_75. Measured again here today: with the first draft of this
# script, which archived the duplicates as plain duplicates, the offline
# promoter refused the 2026_74 row this same script had just filled
# ("re-promotion BLOCKED ... already in Rejected").
#
# "superseded" is the marker _is_terminal_rejection already honours
# (merge_master line ~1568, added 2026-10-11) and the one
# test_a_recall_is_not_both_published_and_rejected accepts as saying which
# copy wins. So every duplicate note below is written as a SUPERSEDED
# verdict: the trail is kept, append-only, and the alert's own URL stays
# reachable. Re-ingestion is still blocked by keys_in_approved once the
# clean row is published, which is the right gate for it.
AESAN_72_NOTE = (
    "[" + STAMP + ": SUPERSEDED — duplicate_language_copy. One language copy "
    "of AESAN alert 2026_72 (ES2026/637, Listeria monocytogenes in cooked "
    "mussels). The alert is PUBLISHED in Recalls at " + AESAN_72_PUBLISHED
    + ", which is the copy that wins. AESAN serves one notice at up to seven "
    "addresses; _normalize_url_for_dedup now collapses them, so no further "
    "copy of this alert can be reviewed as a separate recall.]")
AESAN_72_ARCHIVE_REASON = (
    "SUPERSEDED — duplicate_language_copy of AESAN alert 2026_72, which is "
    "already published in Recalls at " + AESAN_72_PUBLISHED
    + " (" + STAMP + ")")

# The six archives yesterday's pass wrote for 2026_75 are the block that
# lost the row. They are bookkeeping about page copies, not a finding about
# the recall, so they are retired the same way rather than left as a
# permanent verdict on the alert's own address.
#: The three alerts whose language copies are retired below. The archive
#: loop matches on the alert id, so one list covers both sheets.
AESAN_ALERTS = ("2026_72", "2026_74", "2026_75")
AESAN_75_NOTE = (
    "[" + STAMP + ": SUPERSEDED — duplicate_language_copy. One of the "
    "language copies of AESAN alert 2026_75 (ES2026/648, Salmonella spp. in "
    "Arkan Pure Sesame Tahini) archived on 2026-10-27. That archive was "
    "bookkeeping about duplicate page copies, but it read as a permanent "
    "human verdict on the alert's own URL and blocked the clean row the same "
    "pass queued, which never landed. Retired here; the clean row read on "
    "the alert page is the copy that wins.]")

AESAN_74 = "/alertas/2026_74"
AESAN_74_KEEP = "https://www.aesan.gob.es/alertas/2026_74"
AESAN_74_FIELDS = {
    "Date": "2026-10-23",
    "Company": "(not specified in AESAN alert 2026_74)",
    "Brand": "Casa de Castrelo",
    "Product": "Pasteurised egg white, 500 ml plastic bottle, chilled, "
               "lot 6265, use by 27/10/26",
    "Pathogen": "Salmonella",
    "Reason": "Presence of Salmonella spp. in pasteurised egg white (AESAN "
              "ref. ES2026/642). Initial distribution to Andalucía; "
              "redistribution to other autonomous communities not ruled out.",
    "Class": "Alert",
}
AESAN_74_NOTE = (
    "[" + STAMP + ": fields read on aesan.gob.es/alertas/2026_74 — published "
    "\"23/10/2026\", ref ES2026/642, brand \"Casa de Castrelo\", "
    "\"Clara de huevo pasteurizada\", 500 ml, \"Botella de plástico\", "
    "refrigerated, lot 6265, \"Fecha de caducidad 27/10/26\", initial "
    "distribution Andalucía. AESAN names no operator beyond the brand, and "
    "states no country of origin. Product was a search-engine snippet of the "
    "agency's own front matter, which is why the row could not pass the "
    "publish gate.] [original product: Clara de huevo pasteurizada] "
    "[original Reason (es): \"Alerta por presencia de Salmonella spp. en "
    "clara de huevo pasteurizada. (Ref. ES2026/642)\"]")
AESAN_74_NOTE_ARCHIVE = (
    "[" + STAMP + ": SUPERSEDED — duplicate_language_copy. One language copy "
    "of AESAN alert 2026_74 (ES2026/642, Salmonella spp. in pasteurised egg "
    "white). The Spanish-address row was filled from the alert page, read "
    "today, and is the copy that wins.]")
AESAN_74_ARCHIVE_REASON = (
    "SUPERSEDED — duplicate_language_copy of AESAN alert 2026_74; the "
    "Spanish-address row was filled from the alert page and kept ("
    + STAMP + ")")

# ---------------------------------------------------------------------------
# F. SFA (SG) — a reviewer that could not fetch the page, read as a verdict
# ---------------------------------------------------------------------------
# The row was archived "URL agent: The fetched page is not the specific
# recall page, but rather the SFA homepage". The URL on the row IS the
# specific notice: it was read today and transcribed below. Company was
# empty and Product held the notice's opening sentence, so the row could
# not pass the publish gate either way.
SFA_URL = ("https://www.sfa.gov.sg/news-publications/newsroom/recall-of-arla-"
           "protein-salted-caramel-flavour-pudding--200g--due-to-potential-"
           "presence-of-glass-fragments")
SFA_FIELDS = {
    "Date": "2026-10-26",
    "Source": "SFA (SG)",
    "Company": "Trade-Pro Food Distribution Pte Ltd",
    "Brand": "Arla Protein",
    "Product": "Arla Protein Salted Caramel Flavour Pudding, 200 g, "
               "best before 28.04.2027",
    "Pathogen": "Glass fragments",
    "Reason": "Potential presence of glass fragments. SFA directed a "
              "precautionary recall of the imported product, which "
              "originates in Denmark, following a notification from Food "
              "Standards Australia New Zealand. The recall is ongoing.",
    "Class": "Recall",
    "Country": "Singapore",
}
SFA_NOTE = (
    "[" + STAMP + ": SUPERSEDED — the archived verdict was a reviewer that "
    "could not reach the page, not a finding. Read today on the SFA notice "
    "at the URL on this row: headline \"Recall of Arla Protein Salted "
    "Caramel Flavour Pudding (200g) due to potential presence of glass "
    "fragments\", published 26 Oct 2026 (page last updated 27 Oct 2026), "
    "200 g, best before 28.04.2027, product of Denmark, importer "
    "\"Trade-Pro Food Distribution Pte Ltd\", precautionary and ongoing, "
    "following an FSANZ notification. SFA states no batch or lot number. "
    "Source relabelled from the bare gap-finder \"SFA\" to the registry "
    "label \"SFA (SG)\". Spain, the UK, Australia, New Zealand, Austria "
    "and Norway have all published their own action in this incident.]")

SFA_ARCHIVE_NOTE = (
    "[" + STAMP + ": SUPERSEDED — \"not the specific recall page, but rather "
    "the SFA homepage\" is a reviewer that could not reach the page. The "
    "notice was read today at this exact URL: \"Recall of Arla Protein "
    "Salted Caramel Flavour Pudding (200g) due to potential presence of "
    "glass fragments\", published 26 Oct 2026. The Pending row was filled "
    "from the notice and is the copy that wins.]")

# ---------------------------------------------------------------------------
# E. rows queued through the real path
# ---------------------------------------------------------------------------
NEW_ROWS = [
    {
        "Date": "2026-10-26",
        "Source": "AESAN (ES)",
        "Company": "(not specified in AESAN alert 2026_75)",
        "Brand": "Arkan",
        "Product": "Pure Sesame Tahini, 400 g jar, lot 0257502493, "
                   "best before 12/2027",
        "Pathogen": "Salmonella",
        "Reason": "Presence of Salmonella spp. in sesame tahini from Egypt "
                  "(AESAN ref. ES2026/648). Distributed in Cataluña, Madrid "
                  "and País Vasco; redistribution to other regions not "
                  "ruled out.",
        "Class": "Alert",
        "Country": "Spain",
        "Outbreak": 0,
        "URL": "https://www.aesan.gob.es/alertas/2026_75",
        "Notes": (
            "[" + STAMP + ": RE-QUEUED. This row was verified and queued on "
            "2026-10-27 and never landed — the terminal-rejection guard "
            "dropped it against the archive the same script had just "
            "written, and add_manual_row reported only a net count. Read "
            "again today on aesan.gob.es/alertas/2026_75: ref ES2026/648, "
            "published \"26/10/2026\", product \"Pure Sesame Tahini\", brand "
            "Arkan, \"Bote\" 400 g, lot 0257502493, best before 12/2027, "
            "\"presencia de Salmonella spp.\", origin Egypt, regions "
            "Cataluña, Madrid, País Vasco. The alert names no operator. "
            "Queued with --override-terminal over yesterday's archive "
            "verdict, which was bookkeeping about the language copies and "
            "not a finding about this recall. The underlying RASFF "
            "notification is already in Recalls; this is Spain's own "
            "national action.] "
            "[original product: Tahini de sésamo (Pure Sesame Tahini)] "
            "[original Reason (es): \"Alerta por presencia de Salmonella en "
            "tahini de sésamo procedente de Egipto. (Ref. ES2026/648)\"]"),
    },
    {
        "Date": "2026-10-27",
        "Source": "AESAN (ES)",
        "Company": "(not specified in AESAN alert 2026_76)",
        "Brand": "Arla Protein",
        "Product": "Vanilla cookie flavour pudding, 200 g jar, chilled, "
                   "lot ES26271001, best before 25/04/2027",
        "Pathogen": "Glass fragments",
        "Reason": "Possible presence of glass fragments in pudding from "
                  "Denmark (AESAN ref. ES2026/647). Initial distribution "
                  "covered almost the whole of Spain; passed to the regional "
                  "authorities to verify withdrawal from sale. Consumers "
                  "holding the product are advised not to eat it.",
        "Class": "Alert",
        "Country": "Spain",
        "Outbreak": 0,
        "URL": "https://www.aesan.gob.es/alertas/2026_76",
        "Notes": (
            "[" + STAMP + ": missed by every collector — in no sheet of the "
            "workbook. Read on aesan.gob.es/alertas/2026_76: ref "
            "ES2026/647, published \"27/10/2026\", \"Vanilla cookie flavour "
            "pudding\", brand \"Arla protein\", \"Bote\" 200 g, refrigerated, "
            "lot ES26271001, \"Fecha de consumo preferente 25/04/2027\", "
            "origin Denmark, notified by the Danish authorities through "
            "RASFF, initial distribution \"casi todo el territorio "
            "nacional\". The alert names no operator and no Spanish region. "
            "Spain's own action in the Arla protein pudding glass incident "
            "already published for FSA (UK), FSANZ (AU), MPI (NZ) and "
            "AGES (AT).] "
            "[original Reason (es): \"Alerta por posible presencia de "
            "fragmentos de cristal en pudding procedente de Dinamarca. "
            "(Ref. ES2026/647)\"]"),
    },
]


# ---------------------------------------------------------------------------
def _head(ws):
    return [c.value for c in ws[1]]


#: Stamps a previous pass left on a row whose fields we are replacing with
#: verified English ones. Leaving them makes a repaired row look unrepaired.
_CLEANUP_STAMP_RE = re.compile(r"\s*\[needs cleanup:[^\]]*\]")


def _drop_cleanup_stamps(value) -> str:
    return " ".join(_CLEANUP_STAMP_RE.sub(" ", str(value or "")).split())


def _append_note(cell_value, note):
    cur = str(cell_value or "").strip()
    if note in cur:
        return cur
    return (cur + " " + note).strip()


def _edit_workbook() -> dict:
    import openpyxl
    wb = openpyxl.load_workbook(XLSX)
    counts = {"translated": 0, "superseded": 0, "annotated": 0,
              "archived": 0, "filled": 0}

    # ---- A. translations in Recalls -------------------------------------
    rec = wb["Recalls"]
    rh = _head(rec)
    r_url, r_notes = rh.index("URL") + 1, rh.index("Notes") + 1
    for row in range(2, rec.max_row + 1):
        url = str(rec.cell(row, r_url).value or "").strip()
        spec = TRANSLATIONS.get(url)
        if not spec:
            continue
        for field, value in spec["set"].items():
            rec.cell(row, rh.index(field) + 1).value = value
        notes = str(rec.cell(row, r_notes).value or "")
        for stamp in spec["drop"]:
            notes = notes.replace(stamp, "")
        notes = " ".join(notes.split())
        rec.cell(row, r_notes).value = _append_note(notes, spec["note"])
        counts["translated"] += 1
        print("  translated: %s" % url)

    # ---- C. annotate the published AESAN 2026_72 row --------------------
    for row in range(2, rec.max_row + 1):
        url = str(rec.cell(row, r_url).value or "").strip().lower()
        if "aesan.gob.es" in url and url.rstrip("/").endswith(AESAN_72):
            rec.cell(row, r_notes).value = _append_note(
                rec.cell(row, r_notes).value,
                "[" + STAMP + ": this is the copy that wins for AESAN alert "
                "2026_72; its language copies in Pending and the archives "
                "are annotated as duplicates.]")
            counts["annotated"] += 1

    # ---- B / C. archive-sheet annotations --------------------------------
    for sheet in ("Rejected", "Weekly_Rejected"):
        if sheet not in wb.sheetnames:
            continue
        ws = wb[sheet]
        h = _head(ws)
        if "URL" not in h or "Notes" not in h:
            continue
        ui, ni = h.index("URL") + 1, h.index("Notes") + 1
        # Weekly_Rejected calls it RejectionReason, Rejected calls it
        # RejectReason, and tests/test_a_recall_is_not_both_published_and_
        # rejected.py reads ONLY that column — a marker written into Notes
        # alone leaves the contradiction "silent" as far as it is concerned.
        ri = None
        for cand in ("RejectionReason", "RejectReason"):
            if cand in h:
                ri = h.index(cand) + 1
                break
        for row in range(2, ws.max_row + 1):
            url = str(ws.cell(row, ui).value or "").strip()
            low = url.lower()
            if url == FSA_6946:
                ws.cell(row, ni).value = _append_note(
                    ws.cell(row, ni).value, FSA_6946_NOTE)
                if ri:
                    ws.cell(row, ri).value = _append_note(
                        ws.cell(row, ri).value,
                        "| SUPERSEDED by the published row on the same URL ("
                        + STAMP + ")")
                counts["superseded"] += 1
                print("  superseded: %s/%s %s" % (sheet, row, url[:70]))
            elif ("aesan.gob.es" in low
                    and low.rstrip("/").rsplit("/", 1)[-1] in AESAN_ALERTS):
                alert = low.rstrip("/").rsplit("/", 1)[-1]
                ws.cell(row, ni).value = _append_note(
                    ws.cell(row, ni).value,
                    {"2026_72": AESAN_72_NOTE,
                     "2026_74": AESAN_74_NOTE_ARCHIVE,
                     "2026_75": AESAN_75_NOTE}[alert])
                if ri:
                    ws.cell(row, ri).value = _append_note(
                        ws.cell(row, ri).value,
                        "| SUPERSEDED — duplicate_language_copy of AESAN "
                        "alert " + alert + "; the published row at "
                        "aesan.gob.es/alertas/" + alert + " is the copy that "
                        "wins (" + STAMP + ")")
                counts["annotated"] += 1
                print("  superseded-dup: %s/%s %s" % (sheet, row, url[:70]))
            elif url == SFA_URL:
                ws.cell(row, ni).value = _append_note(
                    ws.cell(row, ni).value, SFA_ARCHIVE_NOTE)
                if ri:
                    ws.cell(row, ri).value = _append_note(
                        ws.cell(row, ri).value,
                        "| SUPERSEDED — the notice was read on sfa.gov.sg at "
                        "this URL (" + STAMP + ")")
                counts["superseded"] += 1
                print("  superseded: %s/%s %s" % (sheet, row, url[:70]))

    # ---- D. fill the AESAN 2026_74 Pending row --------------------------
    pend = wb["Pending"]
    ph = _head(pend)
    p_url, p_notes = ph.index("URL") + 1, ph.index("Notes") + 1
    for row in range(2, pend.max_row + 1):
        url = str(pend.cell(row, p_url).value or "").strip()
        if url != AESAN_74_KEEP:
            continue
        for field, value in AESAN_74_FIELDS.items():
            if field in ph:
                pend.cell(row, ph.index(field) + 1).value = value
        pend.cell(row, p_notes).value = _append_note(
            _drop_cleanup_stamps(pend.cell(row, p_notes).value),
            AESAN_74_NOTE)
        if "Status" in ph:
            pend.cell(row, ph.index("Status") + 1).value = "pending"
        counts["filled"] += 1
        print("  filled from the alert page: %s" % url)

    # ---- F. restore the SFA (SG) row from the notice ---------------------
    for row in range(2, pend.max_row + 1):
        if str(pend.cell(row, p_url).value or "").strip() != SFA_URL:
            continue
        for field, value in SFA_FIELDS.items():
            if field in ph:
                pend.cell(row, ph.index(field) + 1).value = value
        pend.cell(row, p_notes).value = _append_note(
            _drop_cleanup_stamps(pend.cell(row, p_notes).value), SFA_NOTE)
        if "Status" in ph:
            pend.cell(row, ph.index("Status") + 1).value = "pending"
        if "RejectedBy" in ph:
            pend.cell(row, ph.index("RejectedBy") + 1).value = ""
        counts["filled"] += 1
        print("  filled from the SFA notice: %s" % SFA_URL[:80])

    # ---- C / D. archive the duplicate Pending language copies -----------
    rej = wb["Rejected"]
    jh = _head(rej)
    for row in range(pend.max_row, 1, -1):
        url = str(pend.cell(row, p_url).value or "").strip()
        low = url.lower()
        if "aesan.gob.es" not in low:
            continue
        if low.rstrip("/").endswith(AESAN_72):
            reason = AESAN_72_ARCHIVE_REASON
        elif low.rstrip("/").endswith(AESAN_74) and url != AESAN_74_KEEP:
            reason = AESAN_74_ARCHIVE_REASON
        else:
            continue
        src = {h: pend.cell(row, i + 1).value for i, h in enumerate(ph)}
        out = []
        for h in jh:
            if h in ("RejectReason", "RejectionReason"):
                out.append(reason)
            elif h == "RejectedBy":
                out.append(STAMP)
            elif h == "RejectedAt":
                out.append(TODAY)
            elif h == "Status":
                out.append("rejected")
            elif h == "Notes":
                out.append(_append_note(src.get("Notes"), "[" + reason + "]"))
            else:
                out.append(src.get(h, ""))
        rej.append(out)
        pend.delete_rows(row, 1)
        counts["archived"] += 1
        print("  Pending -> Rejected (duplicate): %s" % url[:80])

    wb.save(XLSX)
    wb.close()
    return counts


def _queue_new_rows() -> int:
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False,
                                     encoding="utf-8") as fh:
        json.dump(NEW_ROWS, fh, ensure_ascii=False, indent=2)
        rows_path = fh.name
    try:
        proc = subprocess.run(
            [sys.executable, str(ROOT / "tools" / "add_manual_row.py"),
             rows_path, "--override-terminal"],
            cwd=str(ROOT), text=True, capture_output=True)
        print(proc.stdout.rstrip())
        if proc.stderr.strip():
            print(proc.stderr.rstrip(), file=sys.stderr)
    finally:
        Path(rows_path).unlink(missing_ok=True)
    if proc.returncode != 0:
        # This is the whole point of today's code fix: a row that does not
        # land stops the pass instead of disappearing into a net count.
        raise SystemExit(
            "add_manual_row refused a row (exit %d) — see the per-row result "
            "above. Do not ship a pass whose rows did not queue."
            % proc.returncode)
    return len(NEW_ROWS)


def main() -> int:
    counts = _edit_workbook()
    queued = _queue_new_rows()
    from pipeline.merge_master import mirror_json_from_xlsx
    print("recalls.json re-mirrored: %d rows" % mirror_json_from_xlsx(XLSX, JSON))
    for k, v in counts.items():
        print("%s=%d" % (k.upper(), v))
    print("ROWS_QUEUED=%d" % queued)
    print("ROWS_REMOVED=0")
    return 0


if __name__ == "__main__":                                  # pragma: no cover
    raise SystemExit(main())
