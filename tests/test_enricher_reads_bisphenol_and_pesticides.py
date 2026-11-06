# -*- coding: utf-8 -*-
"""Two hazards the scope rule names by name, that the enricher could not read.

WHY THIS FILE EXISTS (morning fix 2026-11-06)
=============================================
``pipeline.enrich_pending_offline`` is the deterministic, zero-token,
zero-network step that fills Pathogen from the row's own hazard text. The
publish gate blocks on an empty Pathogen, so a hazard this module cannot
read is a recall that cannot be published, however real it is.

Two such hazards were found in Pending on 2026-11-06, both named in the
AFTS scope rule by name, both already published in the register under a
canonical spelling, and both reported by the enricher as "no hazard term
in the row's own text":

  RappelConso 24011, 04/11/2026, CARREFOUR
      Motif "mise en évidence de bisphénol A dans certains lots de ficelle
      alimentaire de notre fournisseur Meyer-Sansboeuf" — bisphenol A
      migrating from butcher's twine into tied meat sold at the counter
      across France between 06/07 and 19/10/2026.
      Four other places in this repo already knew the word: the publish
      gate's chemical list, pipeline/gap_finder/rules.py,
      tools/alert_vocab.py and scrapers/europe_non_eu/blv_ch.py, which
      maps it to the spelling one published row already carries.

  RappelConso 24026, 06/11/2026, Itafrut
      Motif "Dépassement des limites autorisées de pesticides" — Tahiti
      limes from Brazil. Fifteen published rows already carry a pesticide
      Pathogen, including RappelConso's own row of 2026-10-13 whose
      spelling is the canonical one.

The generic rule returns the generic label. It must NOT guess a substance:
"Acetamiprid (pesticide residue)" is available only to a reader that knows
which substance, and the whole point of this module is that it never reads
anything outside the row.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.enrich_pending_offline import infer_pathogen        # noqa: E402

XLSX = ROOT / "docs" / "data" / "recalls.xlsx"


class TestBisphenol(unittest.TestCase):
    def test_the_fiche_24011_reason_is_read(self):
        got, why = infer_pathogen({"Reason": (
            "Mise en évidence de bisphénol a dans certains lots de ficelle "
            "alimentaire de notre fournisseur Meyer-Sansboeuf")})
        self.assertEqual("Bisphenol A (chemical contaminant)", got, why)

    def test_the_english_and_spanish_italian_spellings_are_read(self):
        for text in ("Bisphenol A migration above the specific migration "
                     "limit",
                     "Presencia de bisfenol A por encima del límite",
                     "BPA sopra il limite di migrazione specifica"):
            got, why = infer_pathogen({"Reason": text})
            self.assertEqual("Bisphenol A (chemical contaminant)", got,
                             f"{text!r} -> {why}")

    def test_the_canonical_spelling_matches_the_register(self):
        """A new spelling is a new label nothing else in the repo knows."""
        if not XLSX.exists():                           # pragma: no cover
            self.skipTest("no workbook")
        import pandas as pd
        published = set(
            str(v) for v in
            pd.read_excel(XLSX, "Recalls")["Pathogen"].astype(str))
        self.assertIn("Bisphenol A (chemical contaminant)", published)


class TestPesticides(unittest.TestCase):
    def test_the_fiche_24026_reason_is_read(self):
        got, why = infer_pathogen(
            {"Reason": "Dépassement des limites autorisées de pesticides"})
        self.assertEqual("Pesticide residues", got, why)

    def test_the_mrl_wording_is_read(self):
        for text in ("Pesticide residues found above the maximum residue "
                     "level.",
                     "Residue above the MRL for this commodity",
                     "Plant protection product residues above the limit"):
            got, why = infer_pathogen({"Reason": text})
            self.assertEqual("Pesticide residues", got, f"{text!r} -> {why}")

    def test_it_does_not_guess_a_substance(self):
        got, _ = infer_pathogen(
            {"Reason": "Dépassement des limites autorisées de pesticides"})
        self.assertEqual("Pesticide residues", got)
        self.assertNotIn("(", got.replace("Pesticide residues", ""),
                         "the generic rule must not name a substance it "
                         "cannot know")

    def test_the_canonical_spelling_matches_the_register(self):
        if not XLSX.exists():                           # pragma: no cover
            self.skipTest("no workbook")
        import pandas as pd
        published = set(
            str(v) for v in
            pd.read_excel(XLSX, "Recalls")["Pathogen"].astype(str))
        self.assertIn("Pesticide residues", published)


class TestNoRegression(unittest.TestCase):
    """The two new rules must not relabel a row the register has published.

    Measured over the 2026-11-06 register when they were added: zero rows
    whose published Pathogen names neither a pesticide nor bisphenol are
    claimed by either rule.
    """

    def test_no_published_row_is_relabelled(self):
        if not XLSX.exists():                           # pragma: no cover
            self.skipTest("no workbook")
        import pandas as pd
        conflicts = []
        for row in pd.read_excel(XLSX, "Recalls").to_dict("records"):
            got, _ = infer_pathogen(row)
            pub = str(row.get("Pathogen") or "")
            if not pub or not got:
                continue
            if got == "Pesticide residues" and "esticid" not in pub.lower():
                conflicts.append((row.get("Date"), pub))
            if got.startswith("Bisphenol") and "isphenol" not in pub.lower():
                conflicts.append((row.get("Date"), pub))
        self.assertEqual([], conflicts)


if __name__ == "__main__":                              # pragma: no cover
    unittest.main()
