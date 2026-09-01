"""The page must describe THIS row.

Each test is a real incident. Every one of these rows passed the URL-shape
checks the three reviewers already ran — well-formed, regulator domain,
HTTP 200 — and was wrong anyway.
"""
import sys, unittest
sys.path.insert(0, ".")
from pipeline import _provenance as P


class TestFiche22230(unittest.TestCase):
    """A Brie/Listeria row citing a SHEIN plush-toy fiche."""
    ROW = {"Company": "Fromagerie Milleret", "Brand": "Milleret",
           "Product": "Brie de Meaux 350g",
           "Pathogen": "Listeria monocytogenes",
           "URL": "https://rappel.conso.gouv.fr/fiche-rappel/22230/Interne"}

    def test_toy_page_is_rejected(self):
        page = "shein peluche jouet enfant rappel risque etouffement"
        self.assertTrue(P.check(self.ROW, page_text=page),
                        "the plush-toy page must not corroborate a Brie row")

    def test_correct_page_passes(self):
        page = ("fromagerie milleret rappelle brie de meaux 350g "
                "listeria monocytogenes")
        self.assertEqual([], P.check(self.ROW, page_text=page))


class TestWrongSourceIncidents(unittest.TestCase):
    def test_obrien_ham_on_a_supplement_blog(self):
        row = {"Company": "O'Brien Fine Foods", "Brand": "O'Brien",
               "Product": "ham products", "Pathogen": "Listeria monocytogenes",
               "URL": "https://supplement.ge/food-alerts/2026/"}
        page = "supplements vitamins georgia shop protein powder"
        self.assertTrue(P.check(row, page_text=page))


class TestGenericTokens(unittest.TestCase):
    """A pathogen name proves nothing about WHICH recall a page describes."""

    def test_pathogen_alone_is_not_an_anchor(self):
        row = {"Company": "", "Brand": "", "Product": "",
               "Pathogen": "Listeria monocytogenes", "URL": "https://x"}
        self.assertEqual([], P.anchors_for(row),
                         "listeria/monocytogenes are generic and must not "
                         "be used to confirm provenance")

    def test_no_anchors_means_silence_not_rejection(self):
        row = {"Company": "", "Brand": "", "Product": "",
               "Pathogen": "Listeria monocytogenes", "URL": "https://x"}
        self.assertEqual([], P.check(row, page_text="unrelated listeria page"),
                         "with nothing distinctive to test, the honest answer "
                         "is no finding — not a rejection")


class TestInfrastructureIsNotADefect(unittest.TestCase):
    """Several regulators 403 datacentre traffic. Rejecting on that would
    discard real recalls for an infrastructure reason."""

    ROW = {"Company": "Fromagerie Milleret", "Product": "Brie",
           "Pathogen": "Listeria monocytogenes", "URL": "https://x"}

    def test_unreachable_is_lenient_by_default(self):
        self.assertEqual([], P.check(self.ROW, page_text=""))

    def test_strict_mode_reports_it(self):
        self.assertTrue(P.check(self.ROW, page_text="",
                                treat_unreachable_as_problem=True))


class TestWiredIntoTheAgents(unittest.TestCase):
    def test_reviewer_2_calls_it(self):
        src = open("pipeline/recall_review_agent.py").read()
        self.assertIn("_provenance_flags", src)

    def test_reviewer_3_calls_it(self):
        src = open("pipeline/recall_confirm_agent.py").read()
        self.assertIn("_provenance", src)

    def test_reviewer_1_calls_it(self):
        src = open("pipeline/recall_url_agent.py").read()
        self.assertIn("_provenance_ok", src)


if __name__ == "__main__":
    unittest.main()
