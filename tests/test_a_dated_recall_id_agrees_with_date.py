# -*- coding: utf-8 -*-
"""A recall identifier that contains its own date must agree with Date.

WHAT THIS CAUGHT (morning-fix 2026-10-13)
=========================================
A GIS (PL) public warning reached Recalls on 2026-10-13 with

    Date    2026-10-12
    Reason  "Presence of Listeria monocytogenes (Recall ID GIS-2026-08-19-001)"
    URL     https://www.gov.pl/web/gis/ostrzezenie-publiczne-dotyczace-
            zywnosci-wykrycie-obecnosci-bakterii-listeria-monocytogenes-w-
            jednej-partii-sera-podpuszczkowego
    Notes   "Discovered via news: portalspozywczy.pl"

The notice on gov.pl is dated **19.08.2026**. The row was found by a Polish
trade-news article in October, and the discovery date was written into Date,
so a two-month-old August warning published as a 12 October recall: it
joined W42's weekly page, today's daily brief, and October's running total,
and it was missing from August's — the month whose report and subscriber
email had already gone out.

Nothing on the row was hidden. Its own Reason carried the regulator's
identifier, and that identifier carries the date: GIS-**2026-08-19**-001.
The register held one row of this shape and it was wrong by 54 days.

WHY THIS TEST SHAPE
-------------------
Not "news-discovered rows must be checked" — that is a process, and the
register cannot assert one. Not "Date must differ from DateAdded" either:
a recall found on its publication day legitimately has them equal, and
three published rows do.

This asserts the narrow, mechanical thing: when a row's OWN text spells a
regulator identifier that embeds a full ISO-shaped date, that date is a
fact about the notice, and Date must be it. No network, no judgement, and
it cannot fire on a row that does not carry such an identifier.

DELIBERATELY NOT MATCHED: RappelConso's reference numbers. Those read
"ref. 2026-04-0326" — year, month, and a four-digit sequence within the
month, not a day. 191 published rows carry one, and reading the sequence
as a day is what makes a date check on this field look impossible. The
pattern below requires FOUR dash-separated groups after the agency prefix
(agency-YYYY-MM-DD-seq), which the RappelConso form cannot satisfy.
"""

from __future__ import annotations

import json
import re
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RECALLS_JSON = ROOT / "docs" / "data" / "recalls.json"

#: agency prefix, then YYYY-MM-DD, then a sequence number. The trailing
#: group is what keeps RappelConso's "ref. 2026-04-0326" out: it has no
#: fourth component, so "0326" can never be read as a day plus a sequence.
DATED_ID = re.compile(
    r"\b([A-Z][A-Z0-9]{1,7})-(20\d{2})-(\d{2})-(\d{2})-(\d{1,4})\b")


def _rows():
    return json.loads(RECALLS_JSON.read_text("utf-8"))


def test_the_pattern_does_not_fire_on_a_rappelconso_reference():
    """The guard above, asserted rather than assumed."""
    assert not DATED_ID.search("RappelConso fiche 23707 (ref. 2026-10-0036)")
    assert not DATED_ID.search("ref 2026-09-0242")
    m = DATED_ID.search("Recall ID GIS-2026-08-19-001")
    assert m and m.group(2, 3, 4) == ("2026", "08", "19")


def test_every_dated_recall_id_agrees_with_the_rows_date():
    offenders = []
    for r in _rows():
        own = " ".join(str(r.get(f) or "") for f in ("Reason", "Notes"))
        for m in DATED_ID.finditer(own):
            iso = f"{m.group(2)}-{m.group(3)}-{m.group(4)}"
            try:
                date.fromisoformat(iso)
            except ValueError:
                continue            # not a real calendar date; not our claim
            if iso != str(r.get("Date") or "")[:10]:
                offenders.append((str(r.get("Date"))[:10], iso,
                                  str(r.get("Source")), m.group(0)))

    assert not offenders, (
        "%d published row(s) carry a regulator identifier whose own date "
        "disagrees with Date:\n    %s\n  \n  The identifier is the "
        "regulator's, so it is the notice's date. A row found by a news "
        "article months after the notice must carry the NOTICE's date — "
        "writing the discovery date into Date moves the recall into the "
        "wrong week, the wrong month, and out of the report that should "
        "have carried it." % (
            len(offenders),
            "\n    ".join("Date %s vs %s  (%s, %r)" % o for o in offenders)))
