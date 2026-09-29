"""The Region column is a continent. `region_en` is a province.

AUDIT 2026-09-29 (morning fix pass).

pipeline/gap_finder/extractor.py mapped the LLM's `region_en` field — which
its own EXTRACTION_SCHEMA documents as "region/city/prefecture ... e.g.
'Lesvos', 'Attica'" — straight into the Region COLUMN, which
pipeline/_publish_gate.py VALID_REGIONS restricts to eight continental
values. Any other value is a hard publish-gate blocker.

So the gap finders split into two classes of output, silently:

    article says nothing about a locality  ->  Region ""       -> publishable
    article names a province or region     ->  Region "Toscana" -> refused,
                                                                   for ever

MEASURED at commit 82620c48, on docs/data/recalls.xlsx:

    Pending row 5        Salute (IT), 2026-09-15, Listeria monocytogenes,
                         Caseificio Valdarno brie, Tier 1, Region "Toscana".
                         The only accepted row of this morning's Italian run
                         (94 candidates, 22 verified, 1 accepted), and
                         `python -m pipeline.promote_gate_passing` refuses it
                         on Region and nothing else.

    Weekly_Rejected 429  GIS (PL), 2026-08-21, Bacillus cereus, Lidl Pilos
                         High Protein Pudding 200 g, Tier 1, official gov.pl
                         URL. Region held the model's prose non-answer, "Not
                         specified in the article, but the product was
                         distributed in Poland as per the distributor details
                         provided in the article." The review agent approved
                         it on 2026-09-28 and the confirm agent passed it;
                         merge_master's re-promotion exception then could not
                         re-admit it, because that exception requires the row
                         to pass the FULL publish gate first.

Two Tier-1 recalls held out of the register by one field-mapping line. These
cases are asserted below by shape, not by those two rows, so they keep
holding after the data is repaired.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline._publish_gate import VALID_REGIONS, publish_blockers   # noqa: E402
from pipeline.gap_finder.countries import all_codes, get             # noqa: E402
from pipeline.gap_finder.extractor import (                          # noqa: E402
    build_pending_row,
    gate_region_for,
)
from pipeline.gap_finder.rules import Classification                 # noqa: E402


def _classification():
    """A minimal accepted classification, whatever Classification's shape."""
    try:
        return Classification(verdict="accept", category="pathogen",
                              rule="listeria", matched_term="listeria",
                              tier=1, outbreak_qualifies=False)
    except TypeError:                                    # pragma: no cover
        c = Classification.__new__(Classification)
        for k, v in (("verdict", "accept"), ("category", "pathogen"),
                     ("rule", "listeria"), ("matched_term", "listeria"),
                     ("tier", 1), ("outbreak_qualifies", False)):
            object.__setattr__(c, k, v)
        return c


_VERIFIED = {
    "efet_url": "https://www.salute.gov.it/new/sites/default/files/"
                "external_data/avvisi_sicurezza_alimentare/x.pdf",
    "efet_title": "Richiamo",
    "efet_date_iso": "2026-09-15",
    "news_source_domain": "ilfattoalimentare.it",
}

_EXTRACTED_BASE = {
    "company": "Società Anonima Caseificio Valdarno",
    "brand": "Caseificio Valdarno",
    "product_en": "Brie cheese",
    "pathogen_en": "Listeria monocytogenes",
    "reason_en": "Listeria monocytogenes contamination",
    "date_iso": "2026-09-15",
}


class TestEveryCountryMapsToTheGateVocabulary(unittest.TestCase):

    def test_every_configured_country_yields_a_valid_region(self):
        """A country config that produces an unpublishable Region is a
        country whose entire output is dead on arrival."""
        bad = {}
        for code in sorted(all_codes()):
            region = gate_region_for(get(code))
            if region not in VALID_REGIONS:
                bad[code] = region
        self.assertEqual({}, bad,
                         f"gate_region_for returns a value outside "
                         f"VALID_REGIONS for: {bad}")

    def test_no_country_falls_through_to_unknown(self):
        """'Unknown' is the safety net, not the answer. A country config
        added without a line in the map should show up here, not ship rows
        stamped Unknown into the register."""
        unknown = sorted(c for c in all_codes()
                         if gate_region_for(get(c)) == "Unknown")
        self.assertEqual([], unknown,
                         f"country config(s) with no continental Region: "
                         f"{unknown} — add them to "
                         f"_GATE_REGION_BY_COUNTRY_CODE in "
                         f"pipeline/gap_finder/extractor.py")


class TestALocalityNeverReachesTheRegionColumn(unittest.TestCase):

    def _row(self, region_en: str, code: str = "it") -> dict:
        extracted = dict(_EXTRACTED_BASE, region_en=region_en)
        return build_pending_row(_VERIFIED, _classification(), extracted,
                                 get(code))

    def test_a_province_does_not_become_the_region(self):
        """The exact live case: Pending row 5, Region 'Toscana'."""
        row = self._row("Toscana")
        self.assertEqual("Europe", row["Region"])

    def test_the_province_is_not_thrown_away(self):
        """It is real information off the notice. It moves, it does not
        vanish."""
        row = self._row("Toscana")
        self.assertIn("Toscana", row["Notes"])

    def test_a_prose_non_answer_is_not_filed_as_a_locality(self):
        """Weekly_Rejected 429's value. Region must be the continent and
        Notes must not inherit a sentence of model prose."""
        prose = ("Not specified in the article, but the product was "
                 "distributed in Poland as per the distributor details "
                 "provided in the article.")
        row = self._row(prose, code="pl")
        self.assertEqual("Europe", row["Region"])
        self.assertNotIn("Not specified", row["Notes"])

    def test_an_empty_locality_still_yields_the_continent(self):
        """The previously-passing case must not regress into an empty
        Region: a nationwide recall is still in Europe."""
        row = self._row("")
        self.assertEqual("Europe", row["Region"])
        self.assertNotIn("[locality:", row["Notes"])

    def test_the_gate_does_not_block_the_row_on_region(self):
        """The whole point. A locality-bearing row must reach the register."""
        row = self._row("Toscana")
        blockers = [b for b in publish_blockers(row) if "Region" in str(b)]
        self.assertEqual([], blockers, publish_blockers(row))

    def test_every_country_produces_a_row_the_gate_accepts_on_region(self):
        for code in sorted(all_codes()):
            with self.subTest(code=code):
                row = self._row("Somewhere Province", code=code)
                blockers = [b for b in publish_blockers(row)
                            if "Region" in str(b)]
                self.assertEqual([], blockers)


if __name__ == "__main__":
    unittest.main()
