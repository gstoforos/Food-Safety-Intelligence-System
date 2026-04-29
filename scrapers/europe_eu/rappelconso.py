"""RappelConso France — official open data API (data.economie.gouv.fr).

IMPORTANT — KNOWN API LATENCY:
  This open-data endpoint is updated via a BATCH SYNC ONCE EVERY 24 HOURS,
  not in real time. The consumer-facing site rappel.conso.gouv.fr publishes
  fiches as soon as DGCCRF approves them; the API mirror lags by up to 24h.

  Consequence: this scraper alone cannot guarantee same-day capture of a
  fiche published in the morning. The companion review/rappelconso_html_freshness.py
  hits the live HTML site directly and runs hourly to backstop that gap.

ARCHITECTURE (audit 2026-04-29 — fixed after V1 deprecation):

  RappelConso went through a V1 → V2 migration that completed end of 2025.
  The legacy `rappelconso0` dataset is now HTTP 404. The V2 dataset has
  the same content but renamed several columns. To survive future drift,
  this scraper now uses a THREE-LAYER fallback:

    Layer 1 - V2 API with filter:    fast, server-side date+category filter
    Layer 2 - V2 API no filter:      get all recent rows, filter client-side
                                     (survives field-name renames)
    Layer 3 - data.gouv.fr bulk JSON: 40MB blob, refreshed hourly, ALWAYS
                                     works as long as the dataset exists

  Layer 3 is the safety net. If V2 is renamed to V3, the bulk-JSON URL
  (https://www.data.gouv.fr/api/1/datasets/r/<resource-id>) might also
  change, but the change is a single resource ID swap rather than a
  structural break. Easier to fix.
"""
from __future__ import annotations
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
import json
import logging
from scrapers._base import BaseScraper, fetch
from scrapers._models import Recall
from scrapers._pathogen_vocab import for_languages

log = logging.getLogger(__name__)


class RappelConsoScraper(BaseScraper):
    AGENCY = "RappelConso (FR)"
    COUNTRY = "France"

    API_BASE = "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets"

    # V2 only. V1 (rappelconso0) is now HTTP 404 (deprecated end of 2025).
    # Two V2 variants exist; both are identical in content. Try
    # `gtin-espaces` first because that's the primary published variant.
    DATASETS = (
        "rappelconso-v2-gtin-espaces",
        "rappelconso-v2-gtin-trie",
    )

    # data.gouv.fr ultimate fallback — 40MB JSON refreshed hourly. Resource
    # ID points to the V2 JSON file; if RappelConso ships V3 this ID will
    # change (pointer-only fix, not structural).
    BULK_JSON_URL = ("https://www.data.gouv.fr/api/1/datasets/r/"
                     "7b212733-7f5b-4ff3-b5b2-c7fea20f9cb1")

    PATHOGEN_KEYWORDS = for_languages("en", "fr")

    # ── Field-name maps (V1 vs V2 vs anything-else) ───────────────────
    # We probe each candidate name in order; first non-empty wins. This
    # makes the parser tolerant to field-name drift.
    DATE_FIELDS    = ("date_de_publication", "date_publication")
    CATEGORY_FIELDS = ("categorie_de_produit", "categorie_produit",
                       "categorie")
    SUBCATEGORY_FIELDS = ("sous_categorie_de_produit", "sous_categorie_produit")
    BRAND_FIELDS   = ("nom_de_la_marque_du_produit", "marque_de_produit",
                      "marque")
    COMPANY_FIELDS = (
        "nom_de_la_societe_responsable_de_la_commercialisation",
        "nom_de_la_societe_responsable_de_la_premiere_mise_sur_le_marche",
        "societe",
    )
    PRODUCT_FIELDS = ("noms_des_modeles_ou_references",
                      "noms_des_modeles_ou_des_references",
                      "denomination_du_produit",
                      "produit")
    REASON_FIELDS  = ("motif_du_rappel", "motif")
    RISKS_FIELDS   = ("risques_encourus_par_le_consommateur",
                      "risques_encourus", "risque")
    CLASS_FIELDS   = ("nature_juridique_du_rappel", "nature_du_rappel",
                      "nature_juridique")
    DISTRIB_FIELDS = ("distributeurs", "lien_vers_la_liste_des_distributeurs")
    URL_FIELDS     = ("lien_vers_la_fiche_rappel", "lien_vers_la_fiche")
    FID_FIELDS     = ("identifiant_unique_de_l_alerte",
                      "identifiant_unique_de_la_fiche", "id")
    REF_FIELDS     = ("reference_fiche", "numero_de_la_fiche",
                      "reference_de_la_fiche")

    def _first(self, rec: Dict[str, Any], candidates: tuple) -> str:
        """Return first non-empty field value among candidates."""
        for c in candidates:
            v = rec.get(c)
            if v:
                return str(v)
        return ""

    # ─── Layer 1: V2 API with server-side filter ──────────────────────
    def _try_dataset_filtered(self, dataset: str, since_days: int):
        url = f"{self.API_BASE}/{dataset}/records"
        cutoff = (datetime.utcnow() - timedelta(days=since_days)).strftime("%Y-%m-%d")
        # Probe each known date-field name. Use the first one that the
        # API accepts without error.
        for date_field in self.DATE_FIELDS:
            for cat_field in self.CATEGORY_FIELDS:
                params = {
                    "where": (f'{date_field} >= "{cutoff}" AND '
                              f'{cat_field} = "Alimentation"'),
                    "limit": 500,
                    "order_by": f"{date_field} DESC",
                }
                r = fetch(self.session, url, params=params)
                if not r or r.status_code != 200:
                    continue
                try:
                    data = r.json()
                except Exception:
                    continue
                results = data.get("results", [])
                if results:
                    log.info("RappelConso L1 dataset=%s field=%s/%s -> %d records",
                             dataset, date_field, cat_field, len(results))
                    return results
        return None

    # ─── Layer 2: V2 API with no filter, client-side filtering ────────
    def _try_dataset_unfiltered(self, dataset: str):
        """Pull last 500 rows ordered by date desc. We then filter
        client-side by date and category. Slower than L1 but immune to
        field-name drift on the WHERE clause."""
        url = f"{self.API_BASE}/{dataset}/records"
        # Try ordering by each known date field
        for date_field in self.DATE_FIELDS:
            params = {
                "limit": 500,
                "order_by": f"{date_field} DESC",
            }
            r = fetch(self.session, url, params=params)
            if not r or r.status_code != 200:
                continue
            try:
                data = r.json()
            except Exception:
                continue
            results = data.get("results", [])
            if results:
                log.info("RappelConso L2 dataset=%s order_by=%s -> %d records",
                         dataset, date_field, len(results))
                return results
        return None

    # ─── Layer 3: data.gouv.fr bulk JSON download ─────────────────────
    def _try_bulk_json(self):
        """40MB JSON file, refreshed hourly on data.gouv.fr. Always works
        as long as the dataset exists. Slower (~40MB transfer) but the
        ultimate safety net."""
        try:
            r = fetch(self.session, self.BULK_JSON_URL)
            if not r or r.status_code != 200:
                return None
            data = r.json()
            # Bulk download is a list of records (not wrapped in {"results":})
            if isinstance(data, list):
                results = data
            elif isinstance(data, dict) and "data" in data:
                results = data["data"]
            else:
                return None
            log.info("RappelConso L3 bulk JSON -> %d total records", len(results))
            return results
        except Exception as e:
            log.warning("RappelConso L3 bulk JSON failed: %s", e)
            return None

    def scrape(self, since_days: int = 30) -> List[Recall]:
        cutoff_dt = datetime.utcnow() - timedelta(days=since_days)
        cutoff = cutoff_dt.strftime("%Y-%m-%d")

        results = None
        used_layer = None

        # Layer 1
        for ds in self.DATASETS:
            results = self._try_dataset_filtered(ds, since_days)
            if results is not None:
                used_layer = f"L1/{ds}"
                break

        # Layer 2
        if results is None:
            for ds in self.DATASETS:
                results = self._try_dataset_unfiltered(ds)
                if results is not None:
                    used_layer = f"L2/{ds}"
                    break

        # Layer 3
        if results is None:
            results = self._try_bulk_json()
            if results is not None:
                used_layer = "L3/bulk-json"

        if not results:
            log.warning("RappelConso: ALL three layers failed or empty "
                        "(L1 server-filtered, L2 unfiltered, L3 bulk JSON). "
                        "Either the dataset is offline or all field names "
                        "have drifted. Manual investigation required.")
            return []

        log.info("RappelConso: using layer %s with %d candidate records",
                 used_layer, len(results))

        out: List[Recall] = []
        skipped_by_date = 0
        skipped_by_category = 0
        skipped_by_pathogen = 0

        for rec in results:
            try:
                # Client-side date filter (essential for L2/L3 layers
                # that didn't filter on server side)
                pub_date = self._first(rec, self.DATE_FIELDS)[:10]
                if pub_date and pub_date < cutoff:
                    skipped_by_date += 1
                    continue

                # Client-side category filter
                cat = self._first(rec, self.CATEGORY_FIELDS)
                if cat and "alimentation" not in cat.lower():
                    skipped_by_category += 1
                    continue

                # Pathogen keyword check across motif + risques (some 04/27
                # Alternaria fiches had the keyword only on the motif side)
                reason_text = (self._first(rec, self.REASON_FIELDS).lower() +
                               " " + self._first(rec, self.RISKS_FIELDS).lower())
                if not any(p in reason_text for p in self.PATHOGEN_KEYWORDS):
                    skipped_by_pathogen += 1
                    continue

                fid = self._first(rec, self.FID_FIELDS) or self._first(rec, self.REF_FIELDS)
                url = (self._first(rec, self.URL_FIELDS) or
                       (f"https://rappel.conso.gouv.fr/fiche-rappel/{fid}/Interne"
                        if fid else ""))
                out.append(self._new_recall(
                    Date=pub_date,
                    Company=self._first(rec, self.COMPANY_FIELDS),
                    Brand=self._first(rec, self.BRAND_FIELDS) or "—",
                    Product=(self._first(rec, self.PRODUCT_FIELDS) or
                             self._first(rec, self.SUBCATEGORY_FIELDS))[:300],
                    Pathogen=self._first(rec, self.RISKS_FIELDS)[:200],
                    Reason=self._first(rec, self.REASON_FIELDS)[:300],
                    Class=self._first(rec, self.CLASS_FIELDS) or "Volontaire",
                    URL=url,
                    Outbreak=0,
                    Notes=self._first(rec, self.DISTRIB_FIELDS)[:200],
                ))
            except Exception as e:
                log.warning("RappelConso row parse failed: %s", e)

        log.info("RappelConso: %d pathogen recalls (skipped: %d by date, "
                 "%d non-food, %d non-pathogen)",
                 len(out), skipped_by_date, skipped_by_category,
                 skipped_by_pathogen)
        return out
