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

    # Pathogen / biological-toxin / mycotoxin keywords matched against the
    # combined motif_du_rappel + risques_encourus_par_le_consommateur fields.
    #
    # IMPORTANT (audit 2026-04-29):
    #   The original list missed three real pathogen recalls on 27-04-2026
    #   (RappelConso fiches 22107 / 22108 / 22109 — Alternaria toxins on
    #   sunflower seeds, brands SUN and Petit Prix). The risques field for
    #   those rows reads "Autres contaminants chimiques" and the motif
    #   reads "Dépassement possible des niveaux indicatifs en toxines
    #   d'Alternaria" — neither contained "mycotox", so the filter
    #   silently dropped all three. Added "alternaria", plus explicit
    #   FR/EN spellings of the other mycotoxins George tracks
    #   (fumonisin, deoxynivalenol/DON, zearalenone, T-2/HT-2, ergot)
    #   so a future French-language fiche cannot be silently dropped.
    PATHOGEN_KEYWORDS = (
        # Bacterial pathogens
        "listeria", "salmonella", "salmonelle", "e. coli", "stec",
        "escherichia", "shigatox", "botulin", "botulism", "norovirus",
        "campylobacter", "cyclospora", "vibrio", "cronobacter",
        "bacillus cereus", "cereulide", "shigella", "yersinia",
        # Viral & parasitic
        "hépatit", "hepatit", "cryptosporid",
        # Marine/biological toxins
        "histamine", "biotoxin", "biotoxine", "ciguatera", "tétrodotox",
        "tetrodotoxin", "saxitoxin", "domoic", "okadaic",
        # Mycotoxins — generic + every species name we have ever seen on
        # an EU agency notice. NEW additions are flagged "(audit 2026-04-29)".
        "mycotox",                    # catches mycotoxin/mycotoxine
        "aflatoxin", "aflatoxine",
        "ochratoxin", "ochratoxine",
        "patulin", "patuline",
        "alternaria",                 # (audit 2026-04-29) fiches 22107-09
        "fumonisin", "fumonisine",
        "deoxynivalenol", "déoxynivalénol", " don ",   # DON, with spaces to avoid "don" in prose
        "zearalenone", "zéaralénone",
        "t-2 toxin", "ht-2 toxin", "trichothecene", "trichothécène",
        "ergot",
    )

    def scrape(self, since_days: int = 30) -> List[Recall]:
        cutoff = (datetime.utcnow() - timedelta(days=since_days)).strftime("%Y-%m-%d")
        params = {
            "where": f'date_publication >= "{cutoff}" AND categorie_de_produit = "Alimentation"',
            # 30-day window can produce >100 food recalls (typical
            # RappelConso volume is 5-10/day in the food category, so 30d
            # ≈ 150-300). limit=100 silently truncated the OLDEST rows in
            # the window, which is fine on its own — but combined with
            # rare mycotoxin recalls landing on a busy day, you can lose
            # them mid-window. 500 covers the worst-case month with
            # comfortable headroom and the API supports it.
            #   audit 2026-04-29 — bumped 100 → 500
            "limit": 500,
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
                # Normalise (Company, Brand) — see scrapers/_company_normalise.py
                from scrapers._company_normalise import normalise_company_brand
                _co, _br = normalise_company_brand(
                    rec.get("nom_de_la_societe_responsable_de_la_commercialisation", ""),
                    rec.get("nom_de_la_marque_du_produit", "—"),
                )
                out.append(self._new_recall(
                    Date=rec.get("date_publication", "")[:10],
                    Company=_co,
                    Brand=_br,
                    Product=(rec.get("noms_des_modeles_ou_references", "") or
                             rec.get("sous_categorie_de_produit", ""))[:300],
                    Pathogen=rec.get("risques_encourus_par_le_consommateur", "")[:200],
                    Reason=rec.get("motif_du_rappel", "")[:300],
                    Class=rec.get("nature_juridique_du_rappel", "Voluntary"),
                    URL=url,
                    Outbreak=0,
                    Notes=(rec.get("distributeurs", "") or "")[:200],
                ))
            except Exception as e:
                log.warning("RappelConso row parse failed: %s", e)
        log.info("RappelConso: %d pathogen recalls", len(out))
        return out
