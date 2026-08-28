import sys, unicodedata, unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.product_axes import (process_type, consumption_state, storage_condition,
                          packaging_type, packaging_form, split_cfia_category,
                          enrich)

def R(p="", r="", n=""): return {"Product": p, "Reason": r, "Notes": n}

class TestProcess(unittest.TestCase):
    def test_polish_raw_beats_english_sausage(self):
        # the failure that motivated the module
        v, c, e = process_type(R("Peklimar Metka kielbasa surowa metka"))
        self.assertEqual("raw", v); self.assertEqual("surowa", e)
    def test_french_cru(self):
        self.assertEqual("raw", process_type(R("lait cru de vache"))[0])
    def test_cooked(self):
        self.assertEqual("heat-treated", process_type(R("jambon cuit superieur"))[0])
    def test_smoked_beats_fermented(self):
        self.assertEqual("cured-smoked", process_type(R("saumon fume tranche"))[0])
    def test_cheese_is_fermented(self):
        self.assertEqual("fermented", process_type(R("Gorgonzola AOP doux"))[0])
    def test_unknown_stays_unknown(self):
        self.assertEqual("unknown", process_type(R("reference 601849"))[0])

class TestBoundaries(unittest.TestCase):
    def test_cru_does_not_fire_inside_crustaces(self):
        self.assertNotEqual("raw", process_type(R("crustaces cuits"))[0])
    def test_raw_does_not_fire_inside_strawberry(self):
        self.assertEqual("unknown", process_type(R("strawberry jam"))[0])
    def test_plural_matches(self):
        self.assertEqual("fish-not-relevant", "fish-not-relevant")
        self.assertEqual("loose", packaging_type(R("crevettes vendues en vrac"))[0])

class TestConsumption(unittest.TestCase):
    def test_metka_is_rte(self):
        self.assertEqual("ready-to-eat", consumption_state(R("kielbasa metka"))[0])
    def test_cook_instruction_beats_salad_noun(self):
        v,_,_ = consumption_state(R("raw chicken salad kit", "must be cooked"))
        self.assertEqual("cook-before-eating", v)
    def test_corn_dog_needs_cooking(self):
        self.assertEqual("cook-before-eating", consumption_state(R("Corn Dogs 1.35 kg"))[0])

class TestStorage(unittest.TestCase):
    def test_use_by_proves_chilled(self):
        v,c,_ = storage_condition(R("Gorgonzola", "DLC 7 septembre 2026"))
        self.assertEqual(("chilled","high"), (v,c))
    def test_frozen_wins(self):
        v,c,_ = storage_condition(R("surgele -18C", "DLC 01/09"))
        self.assertEqual(("frozen","high"), (v,c))
    def test_best_before_is_LOW_confidence(self):
        # the Poland correction: raw chilled sausage carrying a best-before
        v,c,_ = storage_condition(R("kielbasa surowa", "Best before 01/08/2026"))
        self.assertEqual("ambient", v)
        self.assertEqual("low", c, "best-before must never be asserted as fact")
    def test_unknown_when_silent(self):
        self.assertEqual("unknown", storage_condition(R("manicamp"))[0])

class TestPackaging(unittest.TestCase):
    def test_vacuum_beats_tray(self):
        self.assertEqual("vacuum", packaging_type(R("barquette sous vide"))[0])
    def test_counter_is_loose(self):
        self.assertEqual("loose", packaging_type(R("vendu au rayon traditionnel"))[0])
    def test_no_guessing(self):
        self.assertEqual("unknown", packaging_type(R("Gorgonzola AOP doux 150 g"))[0])

class TestAxisSeparation(unittest.TestCase):
    def test_cfia_process_suffix_leaves_food_category(self):
        commodity, proc = split_cfia_category("Food - Meat and poultry - Processed")
        self.assertEqual("heat-treated", proc)
        self.assertNotIn("processed", commodity.lower(),
                         "process token must not stay in the commodity string")
    def test_raw_suffix(self):
        self.assertEqual("raw", split_cfia_category("Food - Meat and poultry - Raw")[1])
    def test_no_suffix_unchanged(self):
        c, p = split_cfia_category("Food - Dairy")
        self.assertIsNone(p); self.assertEqual("Food - Dairy", c)
    def test_module_never_emits_food_category(self):
        self.assertNotIn("FoodCategory", enrich(R("brie")).keys())

if __name__ == "__main__":
    unittest.main(verbosity=1)


# ---------------------------------------------------------------------------
# Vocabulary integrity — the class of bug, not one instance of it
# ---------------------------------------------------------------------------

import pipeline.product_axes as PA

_STOPWORDS = set("""
the a an and or of in on at to for is are was were be been it its this that
la le les un une des du de et ou en au aux sur pour est sont ce cet cette
el los las y o en con por para es son este esta
der die das und oder ein eine den dem des im am zu ist sind
il lo gli i e o di da in con per un una che
de het een van voor met is zijn
""".split())

_TABLES = ("PROCESS_TERMS", "CONSUMPTION_TERMS", "PACKAGING_TERMS",
           "FROZEN_TERMS", "CHILLED_TERMS", "AMBIENT_TERMS",
           "AMBIENT_BY_NATURE", "CHILLED_BY_NATURE")


def _norm(s):
    s = unicodedata.normalize("NFKD", str(s or ""))
    return "".join(c for c in s if not unicodedata.combining(c)).lower()


def _all_terms():
    for name in _TABLES:
        tab = getattr(PA, name, None)
        if tab is None:
            continue
        items = tab.items() if isinstance(tab, dict) else [(name, tab)]
        for label, terms in items:
            if isinstance(terms, str):
                terms = [terms]
            for t in terms:
                yield name, label, t


class TestVocabularyIntegrity(unittest.TestCase):
    """The French "the" (tea) was written with its accent already stripped.

    _n() strips accents from the TEXT as well, so the term became identical
    to the English article and classified every row whose Reason contained
    it: "Ochratoxin A above the regulatory limit" came back as a dried
    product. 118 rows, 29% of the dried bucket, on a definite article.

    Accent-stripping cannot be undone at match time, so the only defence is
    refusing to hold a term that collides. This test is that defence.
    """

    def test_no_term_collides_with_a_stopword(self):
        bad = [(n, l, t) for n, l, t in _all_terms() if _norm(t) in _STOPWORDS]
        self.assertEqual([], bad, f"terms that normalise to a stopword: {bad}")

    def test_no_term_is_too_short_to_be_distinctive(self):
        bad = [(n, l, t) for n, l, t in _all_terms() if len(_norm(t)) <= 2]
        self.assertEqual([], bad, f"terms of 2 characters or fewer: {bad}")

    def test_every_term_is_already_accent_free(self):
        """_n() strips accents from the text, so an accented term never fires."""
        bad = [(n, l, t) for n, l, t in _all_terms() if _norm(t) != t.lower()]
        self.assertEqual([], bad, f"terms carrying accents (they can never "
                                  f"match once _n() has run): {bad}")

    def test_the_article_no_longer_classifies_anything(self):
        v, _c, _e = process_type(R(r="Ochratoxin A above the regulatory limit"))
        self.assertEqual("unknown", v)

    def test_french_tea_still_matches_on_an_unambiguous_form(self):
        self.assertEqual("dried", process_type(R("sachet de the vert bio"))[0])


class TestNotesAreNeverRead(unittest.TestCase):
    """Notes is the audit trail. It now carries long English prose from the
    curator and the promotion stamps; matching on it would classify rows by
    their own history."""

    def test_notes_cannot_influence_any_axis(self):
        clean = R("olive oil", "chemical migration")
        noisy = R("olive oil", "chemical migration",
                  "[promoted 2026-08-28: frozen raw milk cheese vacuum pack "
                  "dried powder ready to eat]")
        for fn in (process_type, consumption_state, storage_condition,
                   packaging_type):
            self.assertEqual(fn(clean)[0], fn(noisy)[0], fn.__name__)


if __name__ == "__main__":
    unittest.main()


class TestPackagingForm(unittest.TestCase):
    """The coarser axis, and the honest ceiling behind it.

    Only 86 of the 1,383 rows packaging_type cannot place contain any
    container word in any of the eight languages the vocabulary covers, so
    15.3% is the hard ceiling for a seven-value packaging axis from
    Product + Reason. packaging_form asks a question the text can answer.
    """

    def test_counter_sold_is_unpackaged(self):
        v, c, _ = packaging_form(R("roti de boeuf - vendu au rayon traditionnel"))
        self.assertEqual("unpackaged", v)
        self.assertEqual("high", c)

    def test_named_container_is_packaged_at_high_confidence(self):
        v, c, _ = packaging_form(R("gorgonzola en barquette 150 g"))
        self.assertEqual(("packaged", "high"), (v, c))

    def test_a_declared_net_weight_is_only_low_confidence(self):
        """An inference must be labelled as one so a stratum can exclude it."""
        v, c, e = packaging_form(R("mousse de foie superieure 200g"))
        self.assertEqual(("packaged", "low"), (v, c))
        self.assertEqual("declared net weight", e)

    def test_silence_stays_unknown(self):
        self.assertEqual("unknown", packaging_form(R("alfalfa sprouts"))[0])

    def test_loose_beats_a_weight_in_the_same_string(self):
        """"vendu a la coupe ... 250g" is counter-sold, not a pack."""
        self.assertEqual("unpackaged",
                         packaging_form(R("fromage vendu a la coupe 250g"))[0])


class TestAmbiguousEnglishTerms(unittest.TestCase):
    """Every one of these was a live false positive on 2026-08-28.

    They were introduced while raising coverage and found by hand-reading
    the 30 most recent rows. Coverage bought with a word that means two
    things is not coverage.
    """

    def test_illness_cases_are_not_a_carton(self):
        v, _c, _e = packaging_type(
            R("Alfalfa Sprouts", "Of the 55 reported cases, 46 involved STEC"))
        self.assertEqual("unknown", v)

    def test_the_modal_verb_can_is_not_a_tin(self):
        v, _c, _e = packaging_type(
            R("flax seeds", "hydrocyanic acid can release cyanide"))
        self.assertEqual("unknown", v)
        v, _c, _e = packaging_type(
            R("beef", "symptoms can include severe and bloody diarrhea"))
        self.assertEqual("unknown", v)

    def test_glass_in_the_food_is_not_glass_packaging(self):
        for reason in ("possible foreign material contamination with glass",
                       "potential presence of small glass fragments in the jam",
                       "a shard of brown glass cannot be ruled out"):
            self.assertEqual("unknown", packaging_type(R("product", reason))[0],
                             reason)

    def test_a_brand_called_box_is_not_a_carton(self):
        v, _c, _e = packaging_type(
            R("Green Box Limited enoki mushroom", "Listeria monocytogenes"))
        self.assertEqual("unknown", v)

    def test_a_fajita_wrap_is_a_food_not_a_package(self):
        v, _c, _e = packaging_type(
            R("Snacksters Chicken Fajita Wrap", "STEC"))
        self.assertEqual("unknown", v)

    def test_the_explicit_forms_still_match(self):
        self.assertEqual("canned", packaging_type(R("tuna, 700 g can"))[0])
        self.assertEqual("glass", packaging_type(R("jam in glass jar 30 g"))[0])
        self.assertEqual("carton", packaging_type(
            R("Cardboard boxes containing 100 pieces"))[0])
        self.assertEqual("flexible", packaging_type(
            R("chicken caesar — clear plastic wrapped package"))[0])
        self.assertEqual("canned", packaging_type(R("infant formula 31.7oz tin"))[0])


# ── Regression: the regulator's category text is not product wording ─────
# Audit 2026-08-28. _text() fed the whole Reason to the keyword matchers,
# so the RASFF commodity family "meat and meat products (other than
# poultry)" put the bare token "meat" in front of every matcher, and
# "meat" sat in CONSUMPTION_TERMS["cook-before-eating"]. Every RASFF meat
# row was cook-before-eating on the strength of a category label — 475
# rows carried that label, 261 decided by a bare commodity word, 113 of
# them naming a ready-to-eat food, 42 of those Listeria monocytogenes.

def test_regulator_metadata_is_stripped_before_keywords_run():
    reason = ("Listeria monocytogenes in halloumi from Cyprus; "
              "risk: serious; category: milk and milk products")
    kept = PA._strip_regulator_metadata(reason)
    assert "category:" not in kept and "risk:" not in kept
    assert "halloumi" in kept, "free text must survive untouched"


def test_a_reason_without_metadata_is_returned_unchanged():
    reason = "Salmonella in chicken; sold nationwide; recall notice"
    assert PA._strip_regulator_metadata(reason) == reason


def test_cooked_ham_under_the_rasff_meat_family_is_ready_to_eat():
    row = {"Product": "chilled sliced cooked ham, vacuum packed 200 g",
           "Reason": ("Listeria monocytogenes; risk: serious; "
                      "category: meat and meat products (other than poultry)")}
    state, _c, ev = PA.consumption_state(row)
    assert state == "ready-to-eat", (state, ev)


def test_a_bare_species_name_decides_nothing():
    for word in ("meat", "pork", "chicken", "beef", "duck", "veal",
                 "viande", "porc", "boeuf", "poulet", "dinde", "turkey",
                 "egg", "rice", "pasta", "mushroom", "huitre"):
        assert word not in PA.CONSUMPTION_TERMS["cook-before-eating"], word


def test_packaging_and_sales_channel_decide_nothing():
    for word in ("tray", "sachet", "pack", "counter", "deli",
                 "deli counter", "rayon traditionnel", "sliced",
                 "tranche", "date", "milk", "salmon", "herb"):
        assert word not in PA.CONSUMPTION_TERMS["ready-to-eat"], word


def test_a_use_by_date_does_not_make_a_food_ready_to_eat():
    row = {"Product": "ailes de poulet jaune 800g", "Reason": "use by date 2026-08-01"}
    state, _c, _e = PA.consumption_state(row)
    assert state != "ready-to-eat", state


def test_raw_poultry_at_the_deli_counter_is_cook_before_eating():
    row = {"Product": "filet de dinde cru vendu au rayon traditionnel",
           "Reason": "Salmonella"}
    assert PA.consumption_state(row)[0] == "cook-before-eating"


def test_a_flour_made_from_a_ready_to_eat_commodity_is_an_ingredient():
    row = {"Product": "", "Reason": "Aflatoxin B1 in Groundnut flours from China"}
    assert PA.consumption_state(row)[0] == "ingredient"


def test_cured_raw_ham_is_still_ready_to_eat():
    """Bare "cru" is deliberately not a cook-before-eating term: jambon cru
    is prosciutto and is eaten exactly as sold."""
    row = {"Product": "Sliced unsmoked raw ham (GTIN 220725903)", "Reason": "Listeria"}
    assert PA.consumption_state(row)[0] == "ready-to-eat"
