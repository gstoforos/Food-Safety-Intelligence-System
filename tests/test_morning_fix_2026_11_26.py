# -*- coding: utf-8 -*-
"""Morning fix pass 2026-11-26 — the three defects of this run, pinned.

Each class below holds ONE defect, and each carries the measurement that
justified the rule, so a later pass can tell a deliberate narrow rule from
an oversight.

1. TestChouxAndRecette        pipeline/_language.py  — two French product
                              words no list held, so a wholly French
                              Product published unstamped.
2. TestAccentFreeFrenchHazard pipeline/_language.py  — 'Biotoxines psp'
                              read as English: too short for the two-word
                              detector, no accent for the accent rules.
3. TestVariousIsAPlaceholder  pipeline/_publish_gate.py — the notifier's
                              own 'Diverses' standing where the register
                              needs a firm and a food, and defeating
                              near-duplicate detection while it stood.
4. TestInScopeHazardIsNotA-   pipeline/_url_guard.py — reviewer 1 rejecting
   GroundForRejection         rows because the hazard is a foreign body,
                              a hazard class this register publishes.
"""
from __future__ import annotations

import unittest

from pipeline._language import product_needs_english, reason_needs_english
from pipeline._publish_gate import _blank, _NAME_SENTINELS
from pipeline._url_guard import reject_refusal


class TestChouxAndRecette(unittest.TestCase):
    """RappelConso fiche 24157, published 2026-11-24, Tier 1, Listeria."""

    #: The row as it published. Every word in it defeated a different
    #: rule: "chantilly" and "fabrication" are English words too, "locale"
    #: is an English word in its own right, and "choux" and "recette" were
    #: in no list at all.
    THE_ROW = "choux chantilly x2 (recette locale) fabrication"

    def test_the_row_that_published_is_caught(self):
        self.assertTrue(product_needs_english(self.THE_ROW))

    def test_either_word_alone_is_enough(self):
        self.assertTrue(product_needs_english("choux à la crème"))
        self.assertTrue(product_needs_english("recette du chef"))

    #: THE FALSE POSITIVES THE RULE MUST NOT HAVE. All five are published
    #: English Products that keep "choux" as the culinary term it also is
    #: in English. They are held by the rule's "and NO English word
    #: anywhere" guard, and that guard is the reason only two words could
    #: be added. If one of these starts failing, the guard has been
    #: loosened, not the vocabulary widened.
    ENGLISH_ROWS_THAT_KEEP_CHOUX = (
        'Milk-and-egg custard & whipped-cream choux buns; '
        '"Shiawase no Shu Kurimu" custard choux',
        "Piémontaise salad with ham; choux trio",
        "Paris-Brest (choux pastry ring)",
        "Paris-Brest (choux pastry ring) (sold at the service counter)",
        "Paris-Brest (choux pastry ring) x2",
    )

    def test_english_products_that_keep_choux_still_pass(self):
        for product in self.ENGLISH_ROWS_THAT_KEEP_CHOUX:
            with self.subTest(product=product[:40]):
                self.assertFalse(product_needs_english(product))

    #: MEASURED AND DELIBERATELY LEFT OUT. The first three are English
    #: words; the last three are kept verbatim inside already translated
    #: English Products in this register, so adding them would fire on
    #: correct rows. A later pass that wants one of these must re-measure.
    def test_the_words_deliberately_left_out_stay_out(self):
        for product in (
            # 'bûche' etc. kept as French cheese-shape names, Recalls row 440
            'Picodons, apérichèvre, cœur, brique, bûche, pyramide, tommes',
            # 'paupiettes' kept in an English description, Recalls row 235
            "Meat and meat preparations tied with white butcher's twine and "
            "sold at the butcher's counter (paupiettes, roasting joints, etc.)",
        ):
            with self.subTest(product=product[:40]):
                self.assertFalse(product_needs_english(product))


class TestAccentFreeFrenchHazard(unittest.TestCase):
    """RappelConso fiche 24179, published 2026-11-25, Tier 1."""

    def test_the_reason_that_published_is_caught(self):
        # Two words, so nothing for the two-function-word detector to
        # count; no accent, so neither accent rule applies.
        self.assertTrue(reason_needs_english("Biotoxines psp"))
        self.assertTrue(reason_needs_english("Biotoxines marines PSP"))

    #: THE DISTINCTION THAT IS THE WHOLE RULE: the French plural carries an
    #: "e", the English one does not. Both of these are published English
    #: Reasons that a stem or substring match would flag — the first counts
    #: ZERO English words and the second counts one, so neither is saved by
    #: the two-English-word escape.
    ENGLISH_REASONS_WITH_BIOTOXINS = (
        "Fishing-zone closure — DSP (diarrhetic shellfish poisoning) "
        "marine biotoxins",
        "Phytoplankton + biotoxins exceeding regulatory threshold",
        "Histamine above 200 mg detected (endogenous biotoxin)",
    )

    def test_the_english_plural_still_passes(self):
        for reason in self.ENGLISH_REASONS_WITH_BIOTOXINS:
            with self.subTest(reason=reason[:40]):
                self.assertFalse(reason_needs_english(reason))

    def test_an_english_reason_quoting_the_french_term_passes(self):
        # Two English words beside it, so the escape above fires first.
        self.assertFalse(reason_needs_english(
            "PSP marine biotoxins detected in shellfish "
            '(fiche motif: "Biotoxines PSP")'))


class TestVariousIsAPlaceholder(unittest.TestCase):
    """RappelConso fiche 24179 published with Company = Brand = Product
    = the notifier's own word for "various"."""

    def test_the_three_fields_that_published_are_blank_to_the_gate(self):
        for value in ("Diverses", "diverses", "DIVERSES"):
            with self.subTest(value=value):
                self.assertTrue(_blank(value))

    def test_the_other_languages_too(self):
        for value in ("various", "Various", "divers", "verschiedene",
                      "diversi", "diversos", "olika", "forskellige"):
            with self.subTest(value=value):
                self.assertTrue(_blank(value))

    def test_a_brand_of_various_stays_legal(self):
        # The CFIA row of 2026-03-02 carries Brand 'Various', which is a
        # real answer and must not be read as page debris by the name
        # checks. Company and Product are still held, by _blank() in the
        # required-identity-fields loop.
        self.assertIn("various", _NAME_SENTINELS)
        self.assertIn("diverses", _NAME_SENTINELS)

    def test_a_real_firm_name_is_not_blank(self):
        for value in ("Apex Foodsource Ltd.", "BSA Wiberg Inc.", "Auchan",
                      "Comité Régional de la Conchyliculture de Méditerranée",
                      "Diversey Ltd", "Diverse Foods Limited"):
            with self.subTest(value=value):
                self.assertFalse(_blank(value))


class TestInScopeHazardIsNotAGroundForRejection(unittest.TestCase):
    """Reviewer 1 discarded five rows because the hazard was a foreign
    body. Foreign material is in scope and the register publishes it."""

    #: The two CFIA recalls this reason suppressed outright. Neither ever
    #: reached a subscriber.
    YESHI = {"URL": "https://recalls-rappels.canada.ca/en/alert-recall/"
                    "yeshi-brand-sesame-ginger-nutritional-yeast-dressing-"
                    "dip-recalled-due-pieces-glass"}
    WIBERG = {"URL": "https://recalls-rappels.canada.ca/en/alert-recall/"
                     "bsa-wiberg-brand-dry-mixes-recalled-due-pieces-metal"}
    #: fiche 24087, rejected on this reason AND published at Recalls[123].
    FICHE = {"URL": "https://rappel.conso.gouv.fr/fiche-rappel/24087/interne"}

    def test_the_five_measured_rejections_are_refused(self):
        for row, why in (
            (self.YESHI, "URL agent: Hazard is a foreign body (glass)"),
            (self.WIBERG, "URL agent: Hazard is a foreign body (metal)"),
            (self.FICHE, "Hazard is a foreign body (glass)"),
        ):
            with self.subTest(why=why):
                self.assertTrue(reject_refusal(row, why))

    def test_every_in_scope_hazard_class(self):
        for why in (
            "Hazard is a foreign body",
            "Hazard is foreign material",
            "hazard is a physical hazard",
            "Hazard is glass",
            "Hazard is metal",
            "Hazard is mould",
            "Hazard is a pest",
            "Hazard is a heavy metal",
            "Hazard is a chemical contaminant",
            "Hazard is a pesticide residue",
            "Hazard is a marine biotoxin",
            "Hazard is an aflatoxin",
            "Hazard is a glycoalkaloid",
        ):
            with self.subTest(why=why):
                self.assertTrue(reject_refusal(self.YESHI, why))

    #: STILL REVIEWER 1'S TO DECIDE. Every one of these is a content
    #: verdict this guard has no business touching, and the rule would be
    #: far too wide if it caught any of them.
    def test_the_out_of_scope_verdicts_pass_through(self):
        for why in (
            "Out of scope: undeclared allergen only",
            "allergen-only out of scope",
            "labelling error, no hazard named",
            "Packaging defect only",
            "Quality/spoilage is outside AFTS scope; no pathogen, toxin, "
            "foreign body or chemical contaminant is named.",
            "pet_food_out_of_scope — AFTS-FSIS is a HUMAN-food register",
            "non-food consumer product",
            "not a recall — this is an outbreak investigation page",
            "pre-2026, outside the register's window",
        ):
            with self.subTest(why=why):
                self.assertEqual("", reject_refusal(self.YESHI, why))

    def test_a_non_authority_host_proves_nothing(self):
        # Same brake as the reachability rule: with no regulator URL on the
        # row there is nothing to contradict the model with.
        self.assertEqual("", reject_refusal(
            {"URL": "https://efoodalert.com/2026/11/24/something"},
            "Hazard is a foreign body (glass)"))
        self.assertEqual("", reject_refusal(
            {"URL": ""}, "Hazard is a foreign body (glass)"))

    def test_the_reachability_rule_still_works(self):
        self.assertTrue(reject_refusal(
            self.YESHI, "No official regulator URL found"))

    def test_the_refusal_says_which_classes_remain_the_reviewers(self):
        out = reject_refusal(self.YESHI, "Hazard is a foreign body (glass)")
        self.assertIn("in scope", out.lower())
        self.assertIn("allergen", out.lower())


if __name__ == "__main__":                              # pragma: no cover
    unittest.main()
