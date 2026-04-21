"""RappelConso France — official open data API (data.economie.gouv.fr)."""
from __future__ import annotations
from datetime import datetime, timedelta
from typing import List
import logging
from scrapers._base import BaseScraper, fetch
from scrapers._models import Recall

log = logging.getLogger(__name__)


class RappelConsoScraper(BaseScraper):
    AGENCY = "RappelConso (FR)"
    COUNTRY = "France"
    BASE_URL = (
        "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/"
        "rappelconso0/records"
    )

    PATHOGEN_KEYWORDS = (
        "listeria", "salmonella", "salmonelle", "e. coli", "stec",
        "escherichia", "shigatox", "botulin", "norovirus",
        "campylobacter", "cyclospora", "vibrio", "cronobacter",
        "bacillus cereus", "cereulide", "histamine", "biotoxin",
        "biotoxine", "aflatoxin", "ochratoxin", "patulin", "mycotox",
        "hépatit", "hepatit",
    )

    def scrape(self, since_days: int = 30) -> List[Recall]:
        cutoff = (datetime.utcnow() - timedelta(days=since_days)).strftime("%Y-%m-%d")
        params = {
            "where": f'date_publication >= "{cutoff}" AND categorie_de_produit = "Alimentation"',
            "limit": 100,
            "order_by": "date_publication DESC",
        }
        r = fetch(self.session, self.BASE_URL, params=params)
        if not r:
            return []
        out: List[Recall] = []
        for rec in r.json().get("results", []):
            try:
                reason = (rec.get("motif_du_rappel") or "").lower() + " " + \
                         (rec.get("risques_encourus_par_le_consommateur") or "").lower()
                if not any(p in reason for p in self.PATHOGEN_KEYWORDS):
                    continue
                ref = rec.get("reference_fiche") or rec.get("numero_de_la_fiche") or ""
                # Build deep link: prefer fiche-rappel/<id>/Interne
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
