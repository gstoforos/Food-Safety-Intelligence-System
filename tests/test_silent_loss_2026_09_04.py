"""Rows that were evicted from Pending with no record, 2026-09-02..04.

Replaying the workbook across the six agent commits of those three days
found 50 evictions that landed in neither Recalls, Weekly_Rejected nor
Rejected: 18 distinct rows, 12 of them RASFF notifications, plus two
RappelConso fiches and one CFIA notice that the deterministic publish gate
rejected for "Company is empty" although the notices were sound. Each test
below pins one link of that chain.
"""
import io, sys, unittest
from contextlib import redirect_stdout
from unittest import mock
sys.path.insert(0, ".")


class TestProvenanceShellAndNoFetch(unittest.TestCase):
    """A JS application shell is 'could not read', never 'does not mention'."""

    def test_rasff_is_never_fetched_and_never_blocks(self):
        from pipeline import _provenance as P
        row = {"Company": "Origin: Sweden | Notifying: Denmark",
               "Product": "Detection of Listeria monocytogenes in ready to eat meals",
               "Pathogen": "Listeria monocytogenes",
               "URL": "https://webgate.ec.europa.eu/rasff-window/screen/notification/870143"}
        with mock.patch.object(P, "fetch_text", side_effect=AssertionError("fetched")):
            self.assertEqual([], P.check(row))

    def test_short_page_is_a_shell_not_a_mismatch(self):
        from pipeline import _provenance as P
        row = {"Company": "Laiterie de Coaticook Limitée", "Brand": "Coaticook",
               "Product": "Aged Cheddar Cheese", "Pathogen": "Listeria monocytogenes",
               "URL": "https://example.org/notice/1"}
        with mock.patch.object(P, "fetch_text", return_value=("", "js_shell")):
            self.assertEqual([], P.check(row))
        self.assertFalse(P.is_dead_status("js_shell"))


class TestConfirmAgentHold(unittest.TestCase):
    """Company empty + Brand set is a row for reviewer 2, not a rejection."""

    def test_source_holds_instead_of_evicting(self):
        src = open("pipeline/recall_confirm_agent.py", encoding="utf-8").read()
        self.assertIn("held = [r for r in lane", src)
        self.assertIn('full_pending[idx]["Status"] = "pending_gap_v2"', src)
        self.assertIn("record_rejections(archived", src)

    def test_hold_status_is_neither_promoted_nor_archived(self):
        from pipeline.merge_master import promote_approved
        row = {"Date": "2026-09-01", "Source": "CFIA", "Company": "",
               "Brand": "Coaticook", "Product": "Aged Cheddar Cheese",
               "Pathogen": "Listeria monocytogenes", "Reason": "Listeria monocytogenes",
               "Class": "Recall", "Country": "Canada", "Region": "North America",
               "Tier": 1, "Outbreak": 0, "Status": "pending_gap_v2",
               "URL": "https://recalls-rappels.canada.ca/en/alert-recall/coaticook-brand-aged-cheddar-cheese-recalled-due-listeria-monocytogenes",
               "Notes": "CFIA open-data NID=82581"}
        new, kept, archived = promote_approved([row], [], {}, archive_immediately=True,
                                               previously_rejected={})
        self.assertEqual(([], 1, []), (new, len(kept), archived))

    def test_review_agent_lane_includes_the_hold_status(self):
        from pipeline.recall_review_agent import AGENT2_STATUSES
        self.assertIn("pending_gap_v2", AGENT2_STATUSES)


class TestRappelConsoDistributorAsFirm(unittest.TestCase):
    def test_single_distributor_becomes_company(self):
        from scrapers.rappelconso import _distributor_as_company as f
        self.assertEqual("Carrefour Hyper Dax", f("carrefour hyper dax uniquement"))
        self.assertEqual("Carrefour Hyper Rambouillet", f("CARREFOUR HYPER RAMBOUILLET UNIQUEMENT"))
        self.assertEqual("Super U Mirepoix", f("super u mirepoix"))

    def test_lists_and_phrases_are_not_firms(self):
        from scrapers.rappelconso import _distributor_as_company as f
        for s in ("agidra; grand frais", "liste ci jointe", "gms", "voir pièce jointe",
                  "vente directe", "grandes et moyennes surfaces",
                  "leclerc - systeme u - intermarche - gemma", "", None):
            self.assertEqual("", f(s), s)


if __name__ == "__main__":
    unittest.main()
