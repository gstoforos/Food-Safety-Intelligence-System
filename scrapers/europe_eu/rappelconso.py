"""RappelConso France — official open data API (data.economie.gouv.fr).

IMPORTANT — KNOWN API LATENCY (per Data.gouv.fr docs, audit 2026-04-29):
  This open-data endpoint is updated via a BATCH SYNC ONCE EVERY 24 HOURS,
  not in real time. The consumer-facing site rappel.conso.gouv.fr publishes
  fiches as soon as DGCCRF approves them; the API mirror lags by up to 24h.

  Consequence: this scraper alone cannot guarantee same-day capture of a
  fiche published in the morning. The companion review/rappelconso_html_freshness.py
  hits the live HTML site directly and runs hourly to backstop that gap.
"""
from __future__ import annotations
from datetime import datetime, timedelta
from typing import List
import logging
from scrapers._base import BaseScraper, fetch
from scrapers._models import Recall
from scrapers._pathogen_vocab import for_languages

log = logging.getLogger(__name__)


class RappelConsoScraper(BaseScraper):
    AGENCY = "RappelConso (FR)"
    COUNTRY = "France"

    # Dataset fallback chain:
    # 1) rappelconso-v2-gtin-espaces — the 2024+ canonical dataset
    # 2) rappelconso0                 — the legacy dataset (still populated in parallel
    #                                   by DGCCRF but risks deprecation)
    # Try each in order — first one that returns 200 with results wins.
    API_BASE = "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets"
    DATASETS = (
        "rappelconso-v2-gtin-espaces",
        "rappelconso0",
    )

    # Multilingual pathogen keywords pulled from scrapers/_pathogen_vocab.py.
    # for_languages("en", "fr") = CORE (158 universal scientific names) +
    # BY_LANGUAGE["fr"] (45 French terms) — covers every variant of
    # listeria/salmonella/alternaria/etc. that has ever appeared on a
    # French-language recall fiche.
    PATHOGEN_KEYWORDS = for_languages("en", "fr")

    def _try_dataset(self, dataset: str, since_days: int):
        url = f"{self.API_BASE}/{dataset}/records"
        cutoff = (datetime.utcnow() - timedelta(days=since_days)).strftime("%Y-%m-%d")
        params = {
            "where": f'date_publication >= "{cutoff}" AND categorie_de_produit = "Alimentation"',
            # 30-day window can produce >100 food recalls (typical RappelConso
            # volume is 5-10/day, so 30d ≈ 150-300). limit=100 silently
            # truncated the OLDEST rows in the window, losing rare mycotoxin
            # recalls when a busy day pushed them off the page.
            #   audit 2026-04-29 — bumped 100 → 500
            "limit": 500,
            "order_by": "date_publication DESC",
        }
        r = fetch(self.session, url, params=params)
        if not r or r.status_code != 200:
            return None
        try:
            data = r.json()
        except Exception as e:
            log.warning("%s JSON parse failed: %s", dataset, e)
            return None
        results = data.get("results", [])
        log.info("RappelConso dataset=%s -> %d records", dataset, len(results))
        return results

    def scrape(self, since_days: int = 30) -> List[Recall]:
        results = None
        for ds in self.DATASETS:
            results = self._try_dataset(ds, since_days)
            if results is not None:
                log.info("Using dataset: %s", ds)
                break
        if not results:
            log.warning("All RappelConso datasets failed or empty")
            return []

        out: List[Recall] = []
        for rec in results:
            try:
                # Combine motif + risques for keyword check. The Alternaria
                # 27/04/2026 fiches (22107/08/09) had risques="Autres
                # contaminants chimiques" and motif="...toxines d'Alternaria"
                # — only the motif side carried the actionable keyword.
                reason = ((rec.get("motif_du_rappel") or "").lower() + " " +
                          (rec.get("risques_encourus_par_le_consommateur") or "").lower())
                if not any(p in reason for p in self.PATHOGEN_KEYWORDS):
                    continue
                ref = rec.get("reference_fiche") or rec.get("numero_de_la_fiche") or ""
                fid = rec.get("identifiant_unique_de_l_alerte") or ref
                url = (rec.get("lien_vers_la_fiche_rappel") or
                       (f"https://rappel.conso.gouv.fr/fiche-rappel/{fid}/Interne" if fid else ""))
                out.append(self._new_recall(
                    Date=rec.get("date_publication", "")[:10],
                    Company=rec.get("nom_de_la_societe_responsable_de_la_commercialisation", ""),
                    Brand=rec.get("nom_de_la_marque_du_produit", "—"),
                    Product=(rec.get("noms_des_modeles_ou_references", "") or
                             rec.get("sous_categorie_de_produit", ""))[:300],
                    Pathogen=rec.get("risques_encourus_par_le_consommateur", "")[:200],
                    Reason=rec.get("motif_du_rappel", "")[:300],
                    Class=rec.get("nature_juridique_du_rappel", "Volontaire"),
                    URL=url,
                    Outbreak=0,
                    Notes=(rec.get("distributeurs", "") or "")[:200],
                ))
            except Exception as e:
                log.warning("RappelConso row parse failed: %s", e)
        log.info("RappelConso: %d pathogen recalls", len(out))
        return out
