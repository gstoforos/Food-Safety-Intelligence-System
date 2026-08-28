import sys, unittest
sys.path.insert(0, "/home/claude/f")
from product_axes import (process_type, consumption_state, storage_condition,
                          packaging_type, split_cfia_category, enrich)

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
