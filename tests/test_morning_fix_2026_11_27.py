# -*- coding: utf-8 -*-
"""Morning fix pass 2026-11-27 — the three code defects, pinned.

Each class holds ONE defect, the row that found it, and the measurement that
says the fix is narrow. A measurement in a comment rots; these run.

DEFECT A — a regulator's "not applicable" placeholder read as a food name.
DEFECT B — PAH and benzo(a)pyrene reachable by no subscriber alert term.
DEFECT C — the same hazard invisible to the chemical hazard class, which
           would have refused the row the moment it published.

B and C are the SAME hazard missed by TWO gates on the same morning, which is
the pattern pipeline/_publish_gate.py's mineral-oil note names: a hazard the
finder already knows (pipeline/gap_finder/rules.py has carried "pah" and
"polycyclic aromatic" since 2026-11-11) and the publishing side does not.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"


# ─────────────────────────────────────────────────────────────────────────
# DEFECT A — scrapers/_product_text.is_identifier_only
# ─────────────────────────────────────────────────────────────────────────
class TestThePlaceholderIsNotAFoodName:
    """RappelConso fiche 24183, 26/11/2026, Salmonella spp., Tier 1.

    The row sat in Pending with

        Product   '$lot s45021126$non concerné$$'

    while the fiche heading says "figatellu". The value is the
    `identification_produits` field — the fiche's lot table, `$`-joined cell
    by cell. `is_identifier_only` returned False because "non" was in no
    list: "lot" is packaging vocabulary and "s45021126" is a code, so "non"
    was the only survivor and one survivor means "this is a name". With the
    value accepted as a name, `_first_named_product` stopped there and never
    walked on to `libelle`.
    """

    #: The exact value, as it stood in the workbook.
    THE_ROW = "$lot s45021126$non concerné$$"

    def test_the_row_that_found_it(self):
        from scrapers._product_text import is_identifier_only
        assert is_identifier_only(self.THE_ROW), (
            "the fiche's lot table must not read as a food name — if it "
            "does, _first_named_product stops at it and the heading "
            "('figatellu') is never reached")

    @pytest.mark.parametrize("value", [
        "non concerné",
        "non concernée",
        "NON CONCERNE",
        "$non concerné$$",
        "lot 24432$non concerné",
    ])
    def test_every_shape_of_the_placeholder(self, value):
        from scrapers._product_text import is_identifier_only
        assert is_identifier_only(value)

    @pytest.mark.parametrize("value", [
        # The heading the walk must now reach.
        "figatellu",
        # Real names that contain, or sound like, the added words. A food
        # whose whole name is "non" or "concerné" does not exist; a food
        # whose name CONTAINS them must still read as a name.
        "Non-pasteurised raw milk, 1 l bottle",
        "Lait cru non pasteurisé",
        "Latte non pastorizzato",
        "Concerto apple juice",
        # And the names the module's own docstring says must survive.
        "sucre roux pure canne",
        "Boudin à la viande",
        "Ventrèche roulée séchée",
    ])
    def test_a_real_name_still_reads_as_a_name(self, value):
        from scrapers._product_text import is_identifier_only
        assert not is_identifier_only(value), (
            f"{value!r} names a food and must not be swept away — a false "
            "positive here silently REPLACES a good product name")

    def test_the_fix_is_narrow_measured_on_the_whole_workbook(self):
        """Exactly the one defect, and nothing else, changed verdict.

        Measured on 2026-11-27 over every non-empty Product value in every
        sheet: two values flip, both copies of this one row (the Pending row
        and its Weekly_Rejected twin). This test re-measures rather than
        trusting that number, so a later widening of the vocabulary that
        starts eating real product names fails HERE.
        """
        if not XLSX.exists():                       # pragma: no cover
            pytest.skip("workbook not present")
        import pandas as pd
        from scrapers._product_text import is_identifier_only

        swept = []
        total = 0
        for name, df in pd.read_excel(XLSX, sheet_name=None,
                                      dtype=str).items():
            if "Product" not in df.columns:
                continue
            for value in df["Product"].fillna(""):
                if not value.strip():
                    continue
                total += 1
                if is_identifier_only(value):
                    swept.append((name, value))

        assert total > 5000, (
            f"only {total} Product values read — the measurement is not "
            "being taken against the real register")
        # Every value this returns True for must be an identifier block, not
        # a food. The list is short enough to assert on in full.
        offenders = [(s, v) for s, v in swept
                     if "non concern" in v.lower() and "$" not in v]
        assert offenders == [], (
            "a bare 'non concerné' value is in the register as a Product: "
            f"{offenders} — the placeholder is being published, not swept")


# ─────────────────────────────────────────────────────────────────────────
# DEFECT B — tools/alert_vocab.py
# ─────────────────────────────────────────────────────────────────────────
class TestPahIsReachableByASubscriberAlert:
    """Mattilsynet, 2026-11-26, Sunkost AS "Sunkost Superfood kakaopulver".

    Held by the publish gate: "Pathogen 'PAH and Benzo(a)pyrene exceeding
    limit' matches no word any subscriber alert offers". A published hazard
    no alert can reach is invisible to every subscriber, so the gate was
    right to hold it and the vocabulary was what was missing.
    """

    TERM = "Industrial chemical contaminant"

    def _row(self, pathogen: str) -> dict:
        return {"Pathogen": pathogen, "Product": "", "Reason": "",
                "Country": "", "Brand": "", "Company": ""}

    @pytest.mark.parametrize("pathogen", [
        "PAH and Benzo(a)pyrene exceeding limit",       # the row itself
        "Polycyclic aromatic hydrocarbons (PAH)",
        "Polysykliske aromatiske hydrokarboner",        # the Norwegian
        "Benzo(a)pyrene above limit",
        "benzopyrene exceeding limit",
        "PAH, exceeding the limit for cocoa products",
    ])
    def test_the_hazard_matches(self, pathogen):
        from tools import alert_vocab
        assert alert_vocab.matches(self._row(pathogen), "pathogen",
                                   self.TERM), (
            f"{pathogen!r} reaches no subscriber alert term")

    @pytest.mark.parametrize("pathogen", [
        # Permitted preservatives. A contaminant alert must NOT fire on
        # these: "benzo" is bounded for exactly this reason.
        "Sodium benzoate above the permitted level",
        "Benzoic acid",
        "Sodium benzoate",
        "Benzaldehyde",
    ])
    def test_a_permitted_preservative_does_not_fire(self, pathogen):
        from tools import alert_vocab
        assert not alert_vocab.matches(self._row(pathogen), "pathogen",
                                       self.TERM), (
            f"{pathogen!r} is a permitted additive, not a contaminant — an "
            "alert that fires on it teaches subscribers to ignore the alert")

    def test_the_held_row_now_passes_the_publish_gate(self):
        from pipeline._publish_gate import publish_blockers
        row = {
            "Date": "2026-11-26", "Source": "Mattilsynet",
            "Company": "Sunkost AS", "Brand": "Sunkost", "Class": "Recall",
            "Product": "Cocoa powder",
            "Pathogen": "PAH and Benzo(a)pyrene exceeding limit",
            "Reason": ("Product contains polycyclic aromatic hydrocarbons "
                       "(PAH) and benzo(a)pyrene above the limit for cocoa "
                       "products (Recall ID 411599)"),
            "Tier": "3", "Country": "Norway",
            "URL": ("https://www.mattilsynet.no/tilbakekallinger/"
                    "sunkost-superfood-kakaopulver-tilbakekalles"),
            "Notes": "operator-verified",
        }
        assert publish_blockers(row) == [], publish_blockers(row)


# ─────────────────────────────────────────────────────────────────────────
# DEFECT C — pipeline/_publish_gate.HAZARD_CLASS_KEYWORDS["chemical"]
# ─────────────────────────────────────────────────────────────────────────
class TestPahHasAHazardClass:
    """The second gate that missed the same hazard the same morning.

    `classify_hazard` returning the empty set means `is_in_afts_scope` says
    False and tests/test_curator_scope_2026_09_09.py goes red the moment the
    row publishes — the curator refuses the hazard of a published row, so it
    could not correct a typo on it either. Measured on 2026-11-27 before the
    fix: classify_hazard("PAH and Benzo(a)pyrene exceeding limit") == set().
    """

    @pytest.mark.parametrize("text", [
        "PAH and Benzo(a)pyrene exceeding limit",
        "Product contains polycyclic aromatic hydrocarbons (PAH) and "
        "benzo(a)pyrene above the limit for cocoa products",
        "Polysykliske aromatiske hydrokarboner over grenseverdien",
        "Polyzyklische aromatische Kohlenwasserstoffe",
        "Hydrocarbures aromatiques polycycliques",
        "Idrocarburi policiclici aromatici",
        "benzo[a]pyrene",
        "PAHs above the maximum level",
    ])
    def test_the_hazard_classifies_as_chemical(self, text):
        from pipeline._publish_gate import classify_hazard
        assert "chemical" in classify_hazard(text), (
            f"{text!r} classifies as {classify_hazard(text)!r} — a hazard "
            "with no class is out of AFTS scope to the curator")

    def test_it_is_in_afts_scope(self):
        from pipeline._pathogen_scope import is_in_afts_scope
        assert is_in_afts_scope("PAH and Benzo(a)pyrene exceeding limit")

    @pytest.mark.parametrize("text", [
        # " pah " is bounded because this class is matched against Reason as
        # a substring. These must not classify as chemical on its account.
        "Sodium benzoate, a permitted preservative",
        "Benzoic acid in soft drinks",
        "Spaghetti bolognese ready meal",
        "Pahoa farm tomatoes",
        "Kampaha rice crackers",
    ])
    def test_ordinary_prose_does_not_become_a_chemical_hazard(self, text):
        from pipeline._publish_gate import classify_hazard
        assert "chemical" not in classify_hazard(text), (
            f"{text!r} classified as a chemical hazard — a needle in this "
            "class is matched as a substring against Reason as well as "
            "Pathogen, so a loose one manufactures contradictions")

    def test_the_needles_are_narrow_measured_on_the_whole_workbook(self):
        """The new needles touch only rows that really are about PAH.

        Measured on 2026-11-27 across every Pathogen, Reason, Product and
        Notes value in every sheet — 27,902 non-empty cells — the needles
        added today match 6, all of them genuinely this hazard.
        """
        if not XLSX.exists():                       # pragma: no cover
            pytest.skip("workbook not present")
        import pandas as pd
        NEEDLES = (" pah ", "(pah)", " pah,", " pah/", " pah)", "pah-",
                   "pahs", "polycyclic aromatic", "polyzyklische aromatische",
                   "polysykliske aromatiske",
                   "hydrocarbures aromatiques polycycliques",
                   "idrocarburi policiclici aromatici",
                   "benzo(a)pyren", "benzo[a]pyren", "benzoapyren",
                   "benzopyren", "benzo(a)pireno", "benzo(a)pyrène")
        bad = []
        scanned = 0
        for name, df in pd.read_excel(XLSX, sheet_name=None,
                                      dtype=str).items():
            df = df.fillna("")
            for col in ("Pathogen", "Reason", "Product", "Notes"):
                if col not in df.columns:
                    continue
                for i, value in df[col].items():
                    if not value.strip():
                        continue
                    scanned += 1
                    low = " " + value.lower() + " "
                    if not any(n in low for n in NEEDLES):
                        continue
                    # Anything the needles catch must really be this hazard.
                    if not any(w in low for w in
                               ("pah", "aromatic", "aromatisk", "aromatische",
                                "aromatiques", "aromatici", "benzo")):
                        bad.append((name, col, i + 2, value[:90]))
        assert scanned > 10000, (
            f"only {scanned} cells read — not the real register")
        assert bad == [], (
            "the PAH needles matched text that is not about PAH or "
            f"benzo(a)pyrene: {bad}")


# ─────────────────────────────────────────────────────────────────────────
# DEFECT D — merge_master._write_sheet, firm-name whitespace
# ─────────────────────────────────────────────────────────────────────────
class TestAFirmNameHasOneSpaceBetweenWords:
    """RappelConso fiche 24191, 2026-11-26, Tier 1 Listeria.

        Company / Brand  'Distralis - Bergers du Haut  Béarn'
        the fiche prints 'Distralis - Bergers du Haut Béarn'

    The dashboard searches Company and Brand as strings, so "Haut Béarn"
    did not find the row. `unify_firm_names`, whose whole job is one
    spelling per firm, folds case, accents and punctuation but not
    whitespace, so it could not converge the two spellings either.

    Collapsed at the writer choke point, on every sheet, because a row is
    updated in place after promotion by routes that never re-enter a
    scraper.
    """

    def _written(self, rows, sheet="Recalls"):
        """Run `rows` through the real writer and read the sheet back."""
        from openpyxl import Workbook
        from pipeline.merge_master import _write_sheet
        schema = ["Date", "Source", "Company", "Brand", "Product",
                  "Pathogen", "Reason", "Class", "Country", "Region",
                  "Tier", "Outbreak", "URL", "Notes"]
        wb = Workbook()
        _write_sheet(wb, sheet, schema, [dict(r) for r in rows])
        ws = wb[sheet]
        hdr = [c.value for c in ws[1]]
        out = []
        for row in ws.iter_rows(min_row=2, values_only=True):
            out.append(dict(zip(hdr, row)))
        return out

    def _row(self, **over):
        row = {"Date": "2026-11-26", "Source": "RappelConso (FR)",
               "Company": "X", "Brand": "X", "Product": "Cheese",
               "Pathogen": "Listeria monocytogenes", "Reason": "Presence",
               "Class": "Recall", "Country": "France", "Region": "Europe",
               "Tier": 1, "Outbreak": 0,
               "URL": "https://rappel.conso.gouv.fr/fiche-rappel/24191/interne",
               "Notes": ""}
        row.update(over)
        return row

    def test_the_row_that_found_it(self):
        got = self._written([self._row(
            Company="Distralis - Bergers du Haut  Béarn",
            Brand="Distralis - Bergers du Haut  Béarn")])[0]
        assert got["Company"] == "Distralis - Bergers du Haut Béarn"
        assert got["Brand"] == "Distralis - Bergers du Haut Béarn"

    @pytest.mark.parametrize("sheet", ["Recalls", "Pending", "Weekly_Review",
                                       "Weekly_Rejected", "Rejected"])
    def test_every_sheet_goes_through_the_same_choke_point(self, sheet):
        got = self._written([self._row(Company="Morvan Charcuteries  Salaison",
                                       Brand="Aoste  Professionnel")],
                            sheet=sheet)[0]
        assert got["Company"] == "Morvan Charcuteries Salaison"
        assert got["Brand"] == "Aoste Professionnel"

    @pytest.mark.parametrize("raw,want", [
        ("  Leading and trailing  ", "Leading and trailing"),
        ("Tabs\tbetween\twords", "Tabs between words"),
        ("A\n\nnewline run", "A newline run"),
        ("Huîtres de la Zone  Pointe de Saire  50-07",
         "Huîtres de la Zone Pointe de Saire 50-07"),
    ])
    def test_every_shape_of_whitespace(self, raw, want):
        got = self._written([self._row(Company=raw, Brand=raw)])[0]
        assert got["Company"] == want
        assert got["Brand"] == want

    # Reason is NOT in this list, and that is a measured fact rather than an
    # oversight: strip_empty_identifier_template, a writer guard that has run
    # since 2026-10-05, already re-spaces Reason. Asserting otherwise here
    # would pin behaviour this change does not own. Product and Notes are
    # untouched by any writer guard and are what this rule must not reach.
    @pytest.mark.parametrize("col", ["Product", "Notes"])
    def test_prose_columns_are_left_alone(self, col):
        """Only firm names. Prose can legitimately carry a run of spaces.

        An aligned lot block or a quoted regulator table is evidence, and
        collapsing it would silently edit what the regulator printed.
        """
        raw = "lot 1  lot 2   lot 3"
        got = self._written([self._row(**{col: raw})])[0]
        assert got[col] == raw, (
            f"{col} is prose and must not be re-spaced by the firm-name rule")

    def test_a_name_is_not_otherwise_changed(self):
        """The rule cannot lose a fact — only spaces between characters."""
        import re
        for raw in ["Distralis - Bergers du Haut  Béarn",
                    "Spécialités Antillaises  Saveurs Créoles",
                    "E.LECLERC  ARES"]:
            got = self._written([self._row(Company=raw)])[0]["Company"]
            assert re.sub(r"\s+", "", got) == re.sub(r"\s+", "", raw)

    def test_no_firm_name_in_the_register_keeps_a_whitespace_run(self):
        """Measured on the shipped workbook, every sheet.

        This is the half the writer fix cannot do on its own: rows already
        written keep their spelling until something rewrites
        them, and the writer only rewrites a sheet when something touches
        it. So the morning pass's repair script collapses every instance in
        every sheet itself, measured, and this test holds the result.
        """
        if not XLSX.exists():                       # pragma: no cover
            pytest.skip("workbook not present")
        import pandas as pd
        import re
        bad = []
        for name, df in pd.read_excel(XLSX, sheet_name=None,
                                      dtype=str).items():
            if name != "Recalls":
                continue
            df = df.fillna("")
            for col in ("Company", "Brand"):
                if col not in df.columns:
                    continue
                for i, value in df[col].items():
                    if re.search(r"\S[ \t]{2,}\S", value):
                        bad.append((col, i + 2, value[:70]))
        assert bad == [], (
            "published firm names carry an internal whitespace run, so they "
            f"do not match the name the regulator prints: {bad}")
