"""RappelConso France — official open data API (data.economie.gouv.fr)."""
from __future__ import annotations
from datetime import datetime, timedelta
from typing import List
import logging
from scrapers._base import BaseScraper, fetch
from scrapers._models import Recall

log = logging.getLogger(__name__)


def _distributor_as_company(distributeurs: str) -> str:
    """The firm for an unbranded fiche that names no responsible company.

    Returns the single distributor named in the API's `distributeurs` field,
    title-cased the way the register already writes it, with the trailing
    "uniquement" ("only") removed. Returns "" — never a guess — when the
    field is empty or lists several distributors.
    """
    import re
    s = re.sub(r"\s+", " ", str(distributeurs or "")).strip(" .;,")
    if not s or re.search(r"[;,/]| - |\bet\b", s):
        return ""
    # Phrases the field carries instead of a name (seen in the register:
    # "liste ci jointe", "cf liste jointe", "gms", "voir pièce jointe",
    # "vente directe", "établissements de restauration", "enseignes
    # magasins biologiques", "magasins u"). None of these is a firm.
    generic = ("liste", "jointe", "pièce", "piece", "voir ", "cf ", "gms",
               "grandes", "surfaces", "vente directe", "restauration",
               "commerce", "enseigne", "magasins", "national", "france",
               "internet", "en ligne", "distributeurs", "divers", "toute",
               "tous ", "toutes ")
    if any(g in s.lower() for g in generic):
        return ""
    s = re.sub(r"^uniquement\s+|\s+uniquement$", "", s, flags=re.I).strip()
    if not s or len(s) > 80:
        return ""
    from scrapers._company_normalise import normalise_company_brand
    co, _ = normalise_company_brand(s, "—")
    return co


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
        skipped_scope = 0
        for rec in r.json().get("results", []):
            try:
                # SCOPE GUARD ON THE RECORD ITSELF (audit 2026-09-04). Fiche
                # 23001 — FertilTech copper sulphate, a garden chemical filed
                # under "Maison-Habitat > Produits chimiques" — reached
                # Pending from this scraper on 2026-09-03 despite the
                # `categorie_de_produit = "Alimentation"` clause above. The
                # API's own record says what category a fiche is in, so
                # check it here rather than trust the query alone.
                cat = (rec.get("categorie_de_produit") or "").strip()
                if cat and cat.lower() != "alimentation":
                    skipped_scope += 1
                    continue
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
                # DISTRIBUTOR AS FIRM (audit 2026-09-04). A "sans marque"
                # fiche sold at one shop's own counter names no responsible
                # company in the API — only `distributeurs`. Fiches 23420 and
                # 23421 (rillettes d'oie / steak haché at Carrefour Hyper Dax
                # and Rambouillet, Listeria and Salmonella) arrived with
                # Company empty and were evicted by the publish gate on every
                # run. The register already carries this case as Company =
                # the distributor ("Carrefour Market Gometz-la-Ville", "Super
                # U Mirepoix", "Intermarché Hyper Annemasse"), so fill it the
                # same way from the fiche's own field. Only a SINGLE named
                # distributor qualifies — a list ("agidra; grand frais") is
                # not a firm — and the trailing "uniquement" (= "only") is
                # part of the sentence, not the name.
                if not _co.strip():
                    _co = _distributor_as_company(rec.get("distributeurs", "") or "")
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
        log.info("RappelConso: %d pathogen recalls (%d non-food fiches skipped)",
                 len(out), skipped_scope)
        return out
