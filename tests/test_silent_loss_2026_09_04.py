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
        self.assertIn("_is_firm_problem(p) for p in probs", src)
        self.assertIn('full_pending[idx]["Status"] = "pending_gap_v2"', src)
        self.assertIn("record_rejections(archived", src)

    def test_missing_firm_is_the_only_problem_means_hold(self):
        """Brand-only (CFIA RA-82581) and firm-less (FSA-PRIN-43-2026) rows
        are holds; a bad URL is still a block even with the firm missing."""
        from pipeline import _provenance
        from pipeline.recall_confirm_agent import confirm, _is_firm_problem
        base = {"Date": "2026-09-04", "Pathogen": "Listeria monocytogenes",
                "Reason": "Contamination with Listeria monocytogenes", "Class": "Recall"}
        cfia = dict(base, Source="CFIA", Company="", Brand="Coaticook",
                    Product="Aged Cheddar Cheese", Notes="CFIA open-data NID=82581",
                    URL="https://recalls-rappels.canada.ca/en/alert-recall/coaticook-brand-aged-cheddar-cheese-recalled-due-listeria-monocytogenes")
        fsa = dict(base, Source="FSA (UK)", Company="", Brand="",
                   Product="Inspired to Cook by Sainsbury's Pitted Black Olives",
                   Notes="source_id=FSA-PRIN-43-2026 [via official-feed collector]",
                   URL="https://alerts.food.gov.uk/news-alerts/alert/fsa-prin-43-2026")
        meta = dict(fsa, URL="http://data.food.gov.uk/food-alerts/id/FSA-PRIN-43-2026")
        with mock.patch.object(_provenance, "check", return_value=[]):
            for row in (cfia, fsa):
                probs = confirm(row, [])
                self.assertTrue(probs and all(_is_firm_problem(p) for p in probs), probs)
            probs = confirm(meta, [])
            self.assertFalse(all(_is_firm_problem(p) for p in probs), probs)

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
        from scrapers.europe_eu.rappelconso import _distributor_as_company as f
        self.assertEqual("Carrefour Hyper Dax", f("carrefour hyper dax uniquement"))
        self.assertEqual("Carrefour Hyper Rambouillet", f("CARREFOUR HYPER RAMBOUILLET UNIQUEMENT"))
        self.assertEqual("Super U Mirepoix", f("super u mirepoix"))

    def test_lists_and_phrases_are_not_firms(self):
        from scrapers.europe_eu.rappelconso import _distributor_as_company as f
        for s in ("agidra; grand frais", "liste ci jointe", "gms", "voir pièce jointe",
                  "vente directe", "grandes et moyennes surfaces",
                  "leclerc - systeme u - intermarche - gemma", "", None):
            self.assertEqual("", f(s), s)


class TestRappelConsoScopeGuard(unittest.TestCase):
    """Fiche 23001 (FertilTech copper sulphate, Maison-Habitat) reached Pending
    from the LIVE scraper (scrapers/europe_eu/rappelconso.py) on 2026-09-03 and
    again on 2026-09-04. The record's own top-level category decides; the
    unbranded food fiche beside it must still come through, with the single
    distributor as its Company."""

    def test_non_food_category_is_skipped_food_is_kept(self):
        import scrapers.europe_eu.rappelconso as R

        class _Resp:
            status_code = 200
            def json(self):
                return {"results": [
                    {"categorie_de_produit": "Maison-Habitat",
                     "sous_categorie_de_produit": "Produits chimiques",
                     "motif_rappel": "Présence de sulfate de nickel — listeria (word planted to force the keyword match)",
                     "risques_encourus_par_le_consommateur": "Cancérogène",
                     "identifiant_unique_de_l_alerte": "23001",
                     "date_de_publication": "2026-09-03",
                     "nom_de_la_societe_responsable_de_la_commercialisation": "FertilTech",
                     "marque_produit": "FertilTech",
                     "modeles_ou_references": "sulfate de cuivre 750 g",
                     "distributeurs": "amazon"},
                    {"categorie_de_produit": "Alimentation",
                     "sous_categorie_de_produit": "Viandes",
                     "motif_rappel": "Présence de Listeria monocytogenes",
                     "risques_encourus_par_le_consommateur": "Listeria monocytogenes (agent responsable de la listériose)",
                     "identifiant_unique_de_l_alerte": "23420",
                     "date_de_publication": "2026-09-02",
                     "nom_de_la_societe_responsable_de_la_commercialisation": "",
                     "marque_produit": "Sans marque",
                     "modeles_ou_references": "Rillettes d'oie vendues au rayon traditionnel",
                     "distributeurs": "carrefour hyper dax uniquement"},
                ]}

        with mock.patch.object(R, "fetch", return_value=_Resp()), \
             mock.patch.object(R, "datetime", wraps=R.datetime) as dtm:
            dtm.utcnow.return_value = R.datetime(2026, 9, 5)
            with redirect_stdout(io.StringIO()):
                out = R.RappelConsoScraper().scrape(since_days=30)
        self.assertEqual(1, len(out), [r.URL for r in out])
        self.assertIn("23420", out[0].URL)
        self.assertEqual("Carrefour Hyper Dax", out[0].Company)
        self.assertEqual("Unbranded", out[0].Brand)


class TestFSAFirmAndUrl(unittest.TestCase):
    """FSA-PRIN-43-2026 was lost twice on 2026-09-04: the scraper cited the
    linked-data @id and stored the headline as Company; the official-feed
    collector found no firm because the record carries no alertAuthor."""

    ITEM = {"@id": "http://data.food.gov.uk/food-alerts/id/FSA-PRIN-43-2026",
            "alertURL": "https://alerts.food.gov.uk/news-alerts/alert/fsa-prin-43-2026",
            "notation": "FSA-PRIN-43-2026", "created": "2026-09-04T00:00:00Z",
            "title": "Sainsbury's recalls Inspired to Cook by Sainsbury's Pitted Black Olives because of contamination with Listeria monocytogenes",
            "description": "Sainsbury's is recalling Inspired to Cook by Sainsbury's Pitted Black Olives because Listeria monocytogenes has been found in the product.",
            "productDetails": [{"productName": "Inspired to Cook by Sainsbury's Pitted Black Olives",
                                "packSizeDescription": "120g"}]}

    def test_scraper_uses_alert_url_and_names_the_firm(self):
        import scrapers.europe_non_eu.fsa_uk as F

        class _Resp:
            status_code = 200
            text = ""
            def json(self):
                return {"items": [TestFSAFirmAndUrl.ITEM]}

        with mock.patch.object(F, "fetch", return_value=_Resp()), \
             mock.patch.object(F, "datetime", wraps=F.datetime) as dtm:
            dtm.utcnow.return_value = F.datetime(2026, 9, 5)
            out = F.FSAUKScraper().scrape(since_days=30)
        self.assertEqual(1, len(out))
        self.assertEqual("https://alerts.food.gov.uk/news-alerts/alert/fsa-prin-43-2026", out[0].URL)
        self.assertEqual("Sainsbury's", out[0].Company)
        self.assertTrue(out[0].Product.startswith("Inspired to Cook by Sainsbury's Pitted Black Olives"))

    def test_collector_reads_the_firm_from_the_title(self):
        from pipeline.official_feeds.sources.uk import _firm
        self.assertEqual("Sainsbury's", _firm(self.ITEM))
        self.assertEqual("Greencore", _firm({"reportingBusiness": {"commonName": "Greencore"},
                                             "title": "Asda recalls ..."}))
        self.assertEqual("", _firm({"title": "Possible Presence of Gluten in Amaizin Organic Corn products"}))


if __name__ == "__main__":
    unittest.main()
