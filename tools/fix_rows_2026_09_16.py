#!/usr/bin/env python3
"""Apply the verified row corrections from the 2026-09-16 accuracy brief.

SCOPE IS DELIBERATELY NARROW. Only the rows named in that brief are
touched, each matched by its own URL, and only the specific cells the
brief flagged. An earlier draft of this script ran the FR->EN translator
across the whole sheet and produced 262 unrequested edits (including
"Salmonella spp.." double-periods) — a bulk migration is a separate
decision from fixing the day's flagged rows, so it is not done here.

A cell is corrected ONLY when the correct value was read off the
regulator's own notice. Anything unverifiable is listed under SKIPPED and
left exactly as it is, so the sheet never gains a value no source
supports.

Each edited row gets a provenance tag appended to Notes, matching the
"[<agent> <date>: …]" convention the confirm agent and URL guardian use.

Usage:
    python3 tools/fix_rows_2026_09_16.py --dry-run
    python3 tools/fix_rows_2026_09_16.py --apply
"""
from __future__ import annotations

import argparse
from pathlib import Path

import openpyxl

REPO = Path(__file__).resolve().parents[1]
XLSX = REPO / "docs" / "data" / "recalls.xlsx"
TAG = "[brief-fix 2026-09-16"

DANONE_URL = (
    "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts/"
    "so-delicious-dairy-freer-salted-caramel-cluster-frozen-dessert-pints-"
    "voluntarily-recalled-danone-us"
)

# One entry per row the brief flagged.
#   match  — substring of the row's URL that identifies it (or None to
#            match on Company+Brand when the URL cell is blank)
#   sheet  — the sheet the row must be on
#   set    — {field: (new_value, why)}
FIXES = [
    # ── Recalls (published) ─────────────────────────────────────────────
    {"sheet": "Recalls", "match": "fiche-rappel/23519",
     "set": {"Reason": ("Microbiological non-conformity",
                        "language policy: Reason must be English")}},
    {"sheet": "Recalls", "match": "fiche-rappel/23524",
     "set": {"Reason": ("Microbiological non-conformity",
                        "language policy: Reason must be English")}},
    # Pre-existing half-translated row (fiche 23473), outside the brief's
    # 24h window but the sole remaining offender in
    # tests/test_language_policy.py. The replacement is exactly what
    # _translate_reason_fr_to_en() now returns for the stored value, so
    # this is the root-cause fix applied to the row it already broke.
    {"sheet": "Recalls", "match": "fiche-rappel/23473",
     "set": {"Reason": ("Presence of Salmonella found during own-check "
                        "microbiological testing",
                        "French tail left by head-only translation")}},
    {"sheet": "Recalls", "match": "notification/872923",
     "set": {"Product": ("E coli STEC in French Brie cheese",
                         "double space + lowercase nationality"),
             "Reason": ("E coli STEC in French Brie cheese; risk: serious; "
                        "category: milk and milk products",
                        "double space + lowercase nationality")}},

    # ── Pending ─────────────────────────────────────────────────────────
    # Firm named on the notice; the confirm agent held these for
    # "no recalling firm on the row".
    {"sheet": "Pending", "match": "fiche-rappel/23517",
     "set": {"Company": ("LE FROMAGER DES HALLES",
                         "named on the notice"),
             "Reason": ("Detection of E. coli STEC",
                        "language policy: Reason must be English")}},
    {"sheet": "Pending", "match": "fiche-rappel/23518",
     "set": {"Company": ("Société Fromagère de Meaux",
                         "named on the notice"),
             "Reason": ("Precautionary recall following detection of "
                        "E. coli STEC on a product cut in store",
                        "language policy: Reason must be English")}},
    {"sheet": "Pending", "match": "fiche-rappel/23498",
     "set": {"Company": ("ALDI CENTRALE D'ACHAT ET COMPAGNIE ALDI",
                         "named on the notice")}},
    {"sheet": "Pending", "match": "fiche-rappel/23512",
     "set": {"Pathogen": ("Salmonella",
                          "named in Reason but cell was empty"),
             "Reason": ("Detection of Salmonella",
                        "language policy: Reason must be English")}},
    # Headline pasted into Company, Brand and Product.
    {"sheet": "Pending", "match": "sjomathuset-as-tilbakekaller-leroy-laks-loin",
     "set": {"Company": ("Sjømathuset AS", "headline was in Company"),
             "Brand": ("Lerøy", "headline was in Brand"),
             "Product": ("Lerøy laks loin 600 g og 250 g "
                         "(lot 315219, 315215)",
                         "headline + markdown was in Product")}},
    # GIS rows: "> " markdown artefact and a raw Polish headline.
    {"sheet": "Pending",
     "match": "listeria-monocytogenes-w-jednej-partii-sera-podpuszczkowego",
     "set": {"Company": ("MLECZ SERY GÓRSKIE SP. z o.o.",
                         "named on the notice"),
             "Product": ("Gałka wędzona (ser podpuszczkowy), "
                         "partia 223.2026/M",
                         "markdown artefact + notice-type boilerplate"),
             "Reason": ("Detection of Listeria monocytogenes in one batch "
                        "of rennet cheese",
                        "markdown artefact; Reason must be English")}},
    {"sheet": "Pending",
     "match": "alkaloidy-pirolizydynowe-w-okreslonej-partii-herbatki-ziolowej",
     "set": {"Company": ("Herbapol - Lublin S.A.", "named on the notice"),
             "Product": ("Pokrzywa – herbatka ziołowa, Zielnik Polski, "
                         "20 torebek (partia L 0036)",
                         "markdown artefact + notice-type boilerplate"),
             "Reason": ("Pyrrolizidine alkaloids above the maximum "
                        "permitted level in one batch of nettle herbal tea",
                        "markdown artefact; Reason must be English")}},
    # URL blanked by the guardian; the FDA advisory exists.
    {"sheet": "Pending", "match": None,
     "identify": {"Company": "Danone USA", "Brand": "So Delicious Dairy Free"},
     "set": {"URL": (DANONE_URL, "FDA advisory located")}},
]

FRENCH_REASON_NOTE = """\
--french-reasons scope, and why it is NOT the default
-----------------------------------------------------
Running the repaired _translate_reason_fr_to_en() over the whole Recalls
sheet would change 127 rows. Only EIGHT of those are the bug this commit
fixes — Reason still in French:

    "Non conformite microbiologique"   (x4)
    "Non conformité microbiologique"
    "Non-conformité microbiologique"
    "non conformite microbiologique"
    "Non conformité chimique"

The other ~119 are the PATHOGEN-FIXUP pass, not a language fix:
capitalisation, "spp" -> "spp.", and "Escherichia coli" -> "E. coli".
Those are cosmetic, they were never what the language policy asked for,
and applying them in bulk would actively damage rows:

  * "Escherichia coli" is the correct scientific binomial. Several rows
    spell it out deliberately; shortening it is a style opinion.
  * The fixups are language-blind, so they rewrite pathogen names inside
    Polish, Romanian and Italian Reason prose — e.g.
      "Stwierdzenie obecności DNA specyficznego werotoksycznych
       Escherichia coli ..."  ->  "... werotoksycznych E. coli ..."
    which mangles a non-English sentence the policy does not govern.

So this flag translates ONLY the French non-conformité rows. A broader
normalisation pass is a separate decision and belongs in its own commit
with its own review.
"""

SKIPPED = [
    "Recalls / fiche 23519 — Company and Brand both read 'Talleyrand'. "
    "The fiche detail page could not be read from this environment (the "
    "portal is a JS app that serves the listing to a fetcher, and the "
    "open-data API is blocked by the egress proxy), so there is no "
    "verified firm to write in its place. Left untouched: an unverified "
    "replacement would be no better than the value it replaced.",
    "Recalls / fiche 23524 — Company 'Cooperative U' is produced on "
    "purpose by _distributor_as_company() for single-distributor "
    "unbranded fiches, a convention documented in that function. "
    "Overriding it is a policy decision for George, not a correction.",
    "Recalls / RASFF 2026.8125 — 'grountnuts' and 'Nigaragua' are almost "
    "certainly verbatim from the RASFF subject line (RASFF subjects do "
    "carry typos). webgate could not be read to confirm, and 'correcting' "
    "source wording would make the row diverge from the notification.",
    "Pending / fiche 23498 — Reason 'Detection of Campylobacter spp' is "
    "ENGLISH and therefore correct: tests/test_language_policy.py requires "
    "Reason to be English while Company/Brand/Product keep the "
    "regulator's language. The 2026-09-16 brief flagged this the wrong "
    "way round.",
]


def main() -> int:
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true")
    g.add_argument("--apply", action="store_true")
    ap.add_argument(
        "--french-reasons", action="store_true",
        help="ALSO translate the 8 historical rows whose Reason is still "
             "French non-conformité boilerplate (outside the brief's 24h "
             "window, same root cause). Opt-in: see FRENCH_REASON_NOTE.")
    args = ap.parse_args()

    wb = openpyxl.load_workbook(XLSX)
    changes, unmatched = [], []

    for fix in FIXES:
        ws = wb[fix["sheet"]]
        idx = {c.value: i + 1 for i, c in enumerate(ws[1]) if c.value}
        target = None
        for row in range(2, ws.max_row + 1):
            def cell(name):
                return str(ws.cell(row=row, column=idx[name]).value or "") \
                    if name in idx else ""
            if fix["match"]:
                if fix["match"] in cell("URL"):
                    target = row
                    break
            else:
                ident = fix.get("identify", {})
                if all(v in cell(k) for k, v in ident.items()):
                    target = row
                    break
        if target is None:
            unmatched.append(f"{fix['sheet']}: {fix['match'] or fix.get('identify')}")
            continue

        notes = str(ws.cell(row=target, column=idx["Notes"]).value or "")
        if TAG in notes:
            continue
        applied = []
        for field, (new, why) in fix["set"].items():
            if field not in idx:
                continue
            old = str(ws.cell(row=target, column=idx[field]).value or "")
            if old == new:
                continue
            ws.cell(row=target, column=idx[field]).value = new
            changes.append((fix["sheet"], target, field, old, new, why))
            applied.append(field)
        if applied:
            ws.cell(row=target, column=idx["Notes"]).value = (
                f"{notes} {TAG}: corrected "
                f"{', '.join(sorted(applied))} against the source notice]"
            ).strip()

    print(f"{'DRY RUN' if args.dry_run else 'APPLYING'}: "
          f"{len(changes)} cell change(s) across "
          f"{len({(s, r) for s, r, *_ in changes})} row(s)\n")
    cur = None
    for sheet, row, field, old, new, why in changes:
        if (sheet, row) != cur:
            cur = (sheet, row)
            print(f"  {sheet} row {row}")
        shown_old = old[:66] + ("…" if len(old) > 66 else "")
        print(f"     {field:<8} - {shown_old!r}")
        print(f"     {'':<8} + {new[:66]!r}")
        print(f"     {'':<8}   ({why})")

    if unmatched:
        print("\nNOT FOUND (row may have moved sheets since the brief):")
        for u in unmatched:
            print(f"  - {u}")

    print("\nSKIPPED — unverifiable or deliberate:")
    for s in SKIPPED:
        print(f"  - {s}\n")

    # ── Optional: the 8 historical French-Reason rows ──────────────────
    fr_changes = []
    if args.french_reasons:
        import re as _re
        FR_RX = _re.compile(
            r"^\s*non[\s-]*conformit[ée]\s+(microbiologique|chimique)\s*\.?\s*$",
            _re.IGNORECASE)
        FR_MAP = {"microbiologique": "Microbiological non-conformity",
                  "chimique": "Chemical non-conformity"}
        ws = wb["Recalls"]
        idx = {c.value: i + 1 for i, c in enumerate(ws[1]) if c.value}
        for row in range(2, ws.max_row + 1):
            cell = ws.cell(row=row, column=idx["Reason"])
            m = FR_RX.match(str(cell.value or ""))
            if not m:
                continue
            new = FR_MAP[m.group(1).lower()]
            if new == cell.value:
                continue
            fr_changes.append(("Recalls", row, "Reason",
                               str(cell.value), new,
                               "French Reason (historical)"))
            cell.value = new
        print(f"\n--french-reasons: {len(fr_changes)} historical row(s)")
        for _s, r, _f, old, new, _w in fr_changes:
            print(f"  Recalls row {r}: {old!r} -> {new!r}")
    else:
        print("\n(historical French-Reason rows NOT touched; "
              "re-run with --french-reasons to include them)")

    if not args.apply:
        print("\n(dry run — nothing written; derived artifacts not rebuilt)")
        return 0

    wb.save(XLSX)
    print(f"Wrote {XLSX.relative_to(REPO)}")

    # ── Rebuild the derived artifacts ──────────────────────────────────
    #
    # recalls.xlsx is NOT the only published copy of this data:
    # docs/data/recalls.json is the JSON mirror the dashboard reads, and
    # editing the xlsx alone leaves the site serving the old values. The
    # mirror is regenerated through merge_master.mirror_json_from_xlsx(),
    # which merge_master documents as "the ONLY sanctioned way to produce
    # recalls.json" — it re-reads the xlsx from disk and strips the
    # internal-only columns, so json can never drift from xlsx.
    import importlib
    import sys
    if str(REPO) not in sys.path:
        sys.path.insert(0, str(REPO))
    mm = importlib.import_module("pipeline.merge_master")
    json_path = REPO / "docs" / "data" / "recalls.json"
    before = json_path.read_bytes() if json_path.exists() else b""
    n = mm.mirror_json_from_xlsx(XLSX, json_path)
    after = json_path.read_bytes()
    print(f"Mirrored {n} rows -> {json_path.relative_to(REPO)} "
          f"({'changed' if before != after else 'unchanged'})")

    print("\nSTILL STALE — regenerate with the repo's own builders:")
    print("  docs/data/afts-recalls-public.xlsx  (public xlsx; the")
    print("      'Rebuild public xlsx' job writes it, daily 23:30 Athens)")
    print("  docs/daily/2026-09-15.html          (rendered daily brief;")
    print("      merge_master.rebuild_daily_briefs_for_promoted())")
    print("  Weekly/monthly summaries that already embedded these rows.")
    print("  These have their own scheduled jobs, so the next run picks")
    print("  the corrections up. Force them only if you need the site")
    print("  correct before then.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
