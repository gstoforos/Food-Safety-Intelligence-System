"""Visible mould is in scope from 2026-09-07 (operator decision).

Until then a recall whose only stated hazard was mould was archived as
quality/spoilage: FSANZ Mt Ossa spring water (14 Jul 2026), FSANZ Summer
Snow apple juice (6 Jul), RappelConso Racines ginger drink (27 Aug) and
EFET's recall of Oikogeneia Christodoulou juices (7 Sep, black mould in the
bottle neck). All four are now published. These tests pin the rule and its
boundary: fermentation, off-odour and bare "possible spoilage" stay out.
"""
import json, sys, unittest
sys.path.insert(0, ".")


class TestScopeVocabulary(unittest.TestCase):
    def test_mould_is_in_scope_in_every_language_we_collect(self):
        from pipeline._pathogen_scope import is_in_scope
        for p in ("Mould", "Mold", "Moisissures", "Muffa", "Moho", "Schimmel",
                  "Ευρωτίαση", "Fungal growth"):
            self.assertTrue(is_in_scope(p), p)

    def test_quality_terms_are_still_out_of_scope(self):
        from pipeline._pathogen_scope import is_in_scope
        for p in ("Spoilage", "Fermentation", "Off-odour", "Cold chain rupture",
                  "Quality defect", "Undeclared sugar"):
            self.assertFalse(is_in_scope(p), p)

    def test_mould_is_tier_2_not_tier_1(self):
        from pipeline._pathogen_scope import ALWAYS_TIER1_KEYWORDS
        from scrapers._models import assign_tier, normalize_pathogen
        self.assertNotIn("mould", [k.lower() for k in ALWAYS_TIER1_KEYWORDS])
        self.assertEqual("Mould", normalize_pathogen("black mould inside the bottle neck"))
        self.assertEqual(2, assign_tier("Mould", 0, "Recall", "apple juice 1 L"))

    def test_a_named_toxin_still_wins_over_the_mould_label(self):
        from scrapers._models import normalize_pathogen
        self.assertEqual("Aflatoxin", normalize_pathogen("Aflatoxin B1 from mould growth"))

    def test_the_word_moulding_is_not_a_hazard(self):
        from scrapers._models import normalize_pathogen
        self.assertEqual("", normalize_pathogen("moulding machine maintenance"))


class TestPublishGate(unittest.TestCase):
    ROW = {"Date": "2026-09-07", "Source": "EFET (GR)", "Company": "VITOM ABEE",
           "Brand": "Oikogeneia Christodoulou", "Product": "Orange juice 250 ml",
           "Pathogen": "Mould", "Class": "Recall", "Country": "Greece",
           "Region": "Europe", "Tier": 2, "Outbreak": 0,
           "Reason": "Development of black mould inside the neck of the bottle",
           "URL": ("https://www.efet.gr/index.php/el/enimerosi/deltia-typou/"
                   "anakleiseis-cat/item/5413-deltio-typou-anaklisi-mi-asfalon-trofimon")}

    def test_mould_has_its_own_hazard_class(self):
        from pipeline._publish_gate import classify_hazard
        self.assertEqual({"mould"}, classify_hazard("Mould"))
        self.assertEqual({"fermentation"}, classify_hazard("possible spoilage"))

    def test_a_mould_row_publishes(self):
        from pipeline._publish_gate import publish_blockers
        self.assertEqual([], publish_blockers(dict(self.ROW)))

    def test_a_spoilage_only_row_still_does_not(self):
        from pipeline._publish_gate import publish_blockers
        row = dict(self.ROW, Pathogen="Spoilage", Reason="Possible spoilage")
        self.assertTrue(publish_blockers(row))


class TestRejectionReversal(unittest.TestCase):
    def test_a_mould_rejection_is_retryable_again(self):
        from pipeline.merge_master import _is_terminal_rejection
        self.assertFalse(_is_terminal_rejection(
            "REJECTED: out_of_scope_quality_spoilage — visible mould in bottles"))

    def test_other_scope_rejections_stay_permanent(self):
        from pipeline.merge_master import _is_terminal_rejection
        for d in ("REJECTED: out_of_scope_quality_spoilage — possible spoilage, no pathogen named",
                  "REJECTED: out_of_scope_labelling — undeclared sugar",
                  "REJECTED: not_food — lamps"):
            self.assertTrue(_is_terminal_rejection(d), d)


class TestRegister(unittest.TestCase):
    def test_the_four_mould_recalls_are_published_with_their_own_urls(self):
        rows = json.load(open("docs/data/recalls.json", encoding="utf-8"))
        by_url = {r["URL"]: r for r in rows}
        expected = {
            "https://www.foodstandards.gov.au/food-recalls/recall-alert/tasmanian-mountain-waters-exp-mt-ossa-australian-spring-water-10l": "2026-07-14",
            "https://www.foodstandards.gov.au/food-recalls/recall-alert/updated-07072026-bellevue-orchard-pty-ltd-summer-snow-pink-lady-apple": "2026-07-06",
            "https://rappel.conso.gouv.fr/fiche-rappel/23336/Interne": "2026-08-27",
            "https://www.efet.gr/index.php/el/enimerosi/deltia-typou/anakleiseis-cat/item/5413-deltio-typou-anaklisi-mi-asfalon-trofimon": "2026-09-07",
        }
        for url, date in expected.items():
            self.assertIn(url, by_url, url)
            r = by_url[url]
            self.assertEqual(date, str(r["Date"])[:10])
            self.assertEqual("Mould", r["Pathogen"])
            self.assertEqual(2, int(r["Tier"]))

    def test_the_efet_row_cites_efet_not_a_news_mirror(self):
        rows = json.load(open("docs/data/recalls.json", encoding="utf-8"))
        efet = [r for r in rows if "5413-deltio-typou" in r["URL"]]
        self.assertEqual(1, len(efet))
        self.assertTrue(efet[0]["URL"].startswith("https://www.efet.gr/"))
        self.assertIn("tovima.gr", efet[0]["Notes"])


if __name__ == "__main__":
    unittest.main()
