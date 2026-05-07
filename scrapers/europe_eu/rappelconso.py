"""RappelConso France — official open data API (data.economie.gouv.fr).

KNOWN API LATENCY:
    The open-data endpoint syncs from the consumer-facing site once every
    24 hours. Same-day capture is best-effort; the companion
    review/rappelconso_html_freshness.py hits the live HTML hourly to
    backstop the gap.

ARCHITECTURE (audit 2026-05-06 — major rewrite)
================================================
Production data showed eight of the ten most-recent RappelConso rows
landing in Recalls with Company="Unbranded" and Brand="—" — but the
underlying fiches (e.g. fiche 22184, 22186, 22188) clearly show brand
information mid-page on rappel.conso.gouv.fr. The previous scraper
returned almost no descriptive fields because:

  1. **Field-name probing was too narrow.** RappelConso V2 has at least
     six different fields that can carry brand/manufacturer info, and
     the active code only probed three. When `nom_de_la_marque_du_produit`
     was empty, the row was emitted with Brand="—" even though
     `marques_du_produit` (plural) or `nom_du_fabricant` had data.

  2. **No diagnostics.** When a row came out with empty fields, no log
     line told operators which fields were probed, which were present,
     and which were empty. The pipeline silently degraded to "Unbranded".

  3. **No cross-field fallback.** When the dedicated brand/company
     fields were empty, the scraper didn't fall back to other fields
     that often carry the producer name (e.g. `nom_du_fabricant`,
     `noms_des_modeles_ou_references` often contains "BRAND - product
     name" patterns).

This rewrite:
  • Probes every known field-name variant (V1 + V2 + V3-pre).
  • Tries cross-field synthesis — if no direct brand field but the
    product/model field starts with a brand-shaped token, use that.
  • Logs explicitly per-row which field provided which value.
  • Sets descriptive Notes telling claude-check exactly which fields
    were empty so the reviewer can decide whether to fail or pass.
  • Three-layer fallback (V2 filtered → V2 unfiltered → bulk JSON)
    preserved unchanged.
"""
from __future__ import annotations
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, Tuple
import logging
import re

from scrapers._base import BaseScraper, fetch
from scrapers._models import Recall
from scrapers._pathogen_vocab import for_languages
from scrapers._company_normalise import normalise_company_brand

log = logging.getLogger(__name__)


# Tokens that mean "no specific brand named" across French regulator
# vocabulary. Matched case-insensitively. If a brand field contains ONLY
# one of these, we treat it as empty for fallback purposes.
_NO_BRAND_TOKENS = (
    "sans marque", "sans", "no brand", "marque inconnue", "non commercialisé",
    "n/a", "na", "—", "-", "—", "vide", "non communiqué",
)


def _is_no_brand(s: str) -> bool:
    if not s or not s.strip():
        return True
    return s.strip().lower() in _NO_BRAND_TOKENS


class RappelConsoScraper(BaseScraper):
    AGENCY = "RappelConso (FR)"
    COUNTRY = "France"

    API_BASE = "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets"

    # V2 datasets. Both contain identical content.
    DATASETS = (
        "rappelconso-v2-gtin-espaces",
        "rappelconso-v2-gtin-trie",
    )

    # data.gouv.fr ultimate fallback — 40MB JSON refreshed hourly.
    BULK_JSON_URL = ("https://www.data.gouv.fr/api/1/datasets/r/"
                     "7b212733-7f5b-4ff3-b5b2-c7fea20f9cb1")

    PATHOGEN_KEYWORDS = for_languages("en", "fr")

    # ─────────────────────────────────────────────────────────────────
    # FIELD-NAME MAPS — exhaustive. Order matters for _first(): the
    # first non-empty wins. List from most-specific to least-specific.
    # When you add a new candidate, put it where it belongs by
    # specificity (more specific names go first).
    # ─────────────────────────────────────────────────────────────────
    DATE_FIELDS = (
        "date_de_publication",
        "date_publication",
        "date_de_publication_du_rappel",
    )
    CATEGORY_FIELDS = (
        "categorie_de_produit",
        "categorie_produit",
        "categorie",
    )
    SUBCATEGORY_FIELDS = (
        "sous_categorie_de_produit",
        "sous_categorie_produit",
        "sous_categorie",
    )
    # BRAND — multiple historical names. Audit 2026-05-07 added the V2
    # canonical name (`marque_produit`) at the top — confirmed from the
    # orchestrator's first-record key dump on the L3 bulk JSON. With this
    # populated, the existing brand-as-Company fallback at scrape() will
    # auto-fill Company too.
    BRAND_FIELDS = (
        "marque_produit",                # ✅ V2 canonical (data.gouv.fr 2026)
        "nom_de_la_marque_du_produit",   # V1 canonical
        "marques_du_produit",            # V2 plural variant
        "nom_des_marques_du_produit",
        "marque_de_produit",
        "marque_du_produit",
        "marque",
    )
    # COMPANY (= recalling firm). V2 appears to have dropped most of the
    # explicit COMPANY fields the V1 schema carried — none of the probes
    # below match the V2 first-record dump (2026-05-07). Brand-as-Company
    # fallback in scrape() now does the heavy lifting; this list remains
    # in case data.gouv.fr re-introduces them or for older V1 records
    # served via a different layer.
    COMPANY_FIELDS = (
        "nom_de_la_societe_responsable_de_la_commercialisation",
        "nom_de_la_societe_responsable_de_la_premiere_mise_sur_le_marche",
        "nom_du_fabricant",
        "conditionneur",
        "societe",
        "responsable_de_la_commercialisation",
        "fabricant",
    )
    # PRODUCT — V2 dropped the plural prefix `noms_des_`. Confirmed
    # 2026-05-07: actual fields are `modeles_ou_references` and
    # `identification_produits`. Also adding `libelle` (the recall title
    # / descriptive label) as a final fallback — a populated title is
    # always better than empty Product, and it lets the brand-from-product
    # synthesizer have something to chew on.
    PRODUCT_FIELDS = (
        "modeles_ou_references",         # ✅ V2 canonical
        "identification_produits",       # ✅ V2 canonical (alt)
        "libelle",                       # ✅ V2 — title fallback
        "noms_des_modeles_ou_references",   # V1
        "noms_des_modeles_ou_des_references",
        "noms_des_modeles_ou_references_du_produit",
        "denomination_du_produit",
        "denomination",
        "produit",
        "modele",
        "reference_produit",
    )
    REASON_FIELDS = (
        "motif_rappel",                  # ✅ V2 canonical (2026-05-07 audit)
        "motif_du_rappel",               # V1
        "motif",
        "raison_du_rappel",
        "description",
    )
    RISKS_FIELDS = (
        # `risques_encourus_par_le_consommateur` is still in the V2 export
        # (kept under its long V1 name even though hidden from the
        # visualisation per the 2023-10-26 changelog). Today's run captures
        # confirm it's the field carrying pathogen text. Keep it first.
        "risques_encourus_par_le_consommateur",
        "description_complementaire_risque",  # ✅ V2 backup (visible in dump)
        "risques_consommateur",          # V2 hypothesised short form
        "risques_encourus",
        "risques",
        "risque",
        "danger",
    )
    CLASS_FIELDS = (
        "nature_juridique_rappel",       # ✅ V2 canonical (2026-05-07 audit)
        "nature_juridique_du_rappel",    # V1
        "nature_du_rappel",
        "nature_juridique",
        "type_de_rappel",
    )
    DISTRIB_FIELDS = (
        "distributeurs",
        "lien_vers_la_liste_des_distributeurs",
        "lieux_de_vente",
    )
    URL_FIELDS = (
        "lien_vers_la_fiche_rappel",
        "lien_vers_la_fiche",
        "lien",
    )
    FID_FIELDS = (
        "identifiant_unique_de_l_alerte",
        "identifiant_unique_de_la_fiche",
        "id",
    )
    REF_FIELDS = (
        "reference_fiche",
        "numero_de_la_fiche",
        "reference_de_la_fiche",
        "numero",
    )

    # ─────────────────────────────────────────────────────────────────
    # Helper utilities
    # ─────────────────────────────────────────────────────────────────
    def _first(self, rec: Dict[str, Any], candidates: tuple,
               which: str = "") -> Tuple[str, str]:
        """Return (value, field_name) — the first candidate that yielded
        a non-empty value. Returns ('', '') if everything was empty.
        ``which`` is a label used for diagnostics only.
        """
        for c in candidates:
            v = rec.get(c)
            if v not in (None, "", "—", "-"):
                return str(v).strip(), c
        return "", ""

    def _diagnostic(self, rec: Dict[str, Any], candidates: tuple) -> str:
        """Return a compact 'field=value | field=' summary for logs."""
        parts = []
        for c in candidates:
            v = rec.get(c)
            if v in (None, ""):
                parts.append(f"{c}=")
            else:
                parts.append(f"{c}={str(v)[:30]!r}")
        return " | ".join(parts)

    # ─────────────────────────────────────────────────────────────────
    # Layer 1 — V2 API with server-side filter
    # ─────────────────────────────────────────────────────────────────
    def _try_dataset_filtered(self, dataset: str, since_days: int):
        """L1: server-side date filter. The V2 datasets
        (rappelconso-v2-gtin-espaces, rappelconso-v2-gtin-trie) are
        food-only by definition — no need to filter by category.

        Audit 2026-05-06: previously also AND'd categorie_de_produit =
        "Alimentation" — that was the V1 value, V2 categories are
        "Viandes" / "Plats préparés et snacks" / etc. The legacy filter
        returned 0 rows in production for every V2 query, forcing
        fallthrough to L2 (unfiltered) every run.
        """
        url = f"{self.API_BASE}/{dataset}/records"
        cutoff = (datetime.utcnow() - timedelta(days=since_days)).strftime("%Y-%m-%d")
        for date_field in self.DATE_FIELDS:
            params = {
                "where": f'{date_field} >= "{cutoff}"',
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
                log.info("RappelConso L1 dataset=%s field=%s -> %d records",
                         dataset, date_field, len(results))
                return results
        return None

    # ─────────────────────────────────────────────────────────────────
    # Layer 2 — V2 API unfiltered, client-side filter
    # ─────────────────────────────────────────────────────────────────
    def _try_dataset_unfiltered(self, dataset: str):
        url = f"{self.API_BASE}/{dataset}/records"
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

    # ─────────────────────────────────────────────────────────────────
    # Layer 3 — data.gouv.fr bulk JSON
    # ─────────────────────────────────────────────────────────────────
    def _try_bulk_json(self):
        try:
            r = fetch(self.session, self.BULK_JSON_URL)
            if not r or r.status_code != 200:
                return None
            data = r.json()
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

    # ─────────────────────────────────────────────────────────────────
    # Cross-field synthesis — when dedicated brand/company fields are
    # empty, try to extract from the product description, motif, etc.
    # ─────────────────────────────────────────────────────────────────
    def _synthesize_brand_from_product(self, product: str) -> str:
        """RappelConso operators sometimes write product strings like
        'BRAND - Description' or 'BRAND model - 500 g'. If the dedicated
        brand fields were empty but the product field starts with a
        brand-shaped token (1-3 words, leading capital, before a separator),
        return that token.

        Pattern: text up to the first " - ", " – ", " — ", or " : "
        separator. Constraints: first character must be a letter (no
        leading numbers); total length 2-40; not a generic category word.
        """
        if not product:
            return ""
        # Allow any case after the first letter — operators write "BRAND model"
        # and "Saint-Albray" alike. Match anything up to the first separator.
        m = re.match(
            r"^([A-ZÀ-ÝŒ][\w\-'’À-ÝÀ-ÿœ\s]{1,38}?)\s*[-–—:]\s+",
            product,
        )
        if not m:
            return ""
        candidate = m.group(1).strip()
        # Reject if the candidate is just a category word
        category_words = {
            "viandes", "plats", "produits", "boissons", "fruits", "légumes",
            "produits laitiers", "snacks", "aliments",
        }
        if candidate.lower() in category_words:
            return ""
        if 2 <= len(candidate) <= 40:
            return candidate
        return ""

    # ─────────────────────────────────────────────────────────────────
    # Main scrape
    # ─────────────────────────────────────────────────────────────────
    def scrape(self, since_days: int = 30) -> List[Recall]:
        cutoff_dt = datetime.utcnow() - timedelta(days=since_days)
        cutoff = cutoff_dt.strftime("%Y-%m-%d")

        results = None
        used_layer = None

        for ds in self.DATASETS:
            results = self._try_dataset_filtered(ds, since_days)
            if results is not None:
                used_layer = f"L1/{ds}"
                break

        if results is None:
            for ds in self.DATASETS:
                results = self._try_dataset_unfiltered(ds)
                if results is not None:
                    used_layer = f"L2/{ds}"
                    break

        if results is None:
            results = self._try_bulk_json()
            if results is not None:
                used_layer = "L3/bulk-json"

        if not results:
            log.warning("RappelConso: ALL three layers failed or empty. "
                        "Manual investigation required.")
            return []

        log.info("RappelConso: using layer %s with %d candidate records",
                 used_layer, len(results))

        # On the FIRST record, dump the full key list so operators can
        # see which V2 column names actually came back. Helpful when
        # the upstream rename a field — we'll see it in next workflow log.
        if results:
            sample_keys = sorted(results[0].keys())
            log.info("RappelConso: first record has %d fields: %s",
                     len(sample_keys), ", ".join(sample_keys[:30])
                     + ("..." if len(sample_keys) > 30 else ""))

        out: List[Recall] = []
        skipped_by_date = 0
        skipped_by_pathogen = 0
        rows_with_empty_company = 0
        rows_with_synthesized_brand = 0

        for rec in results:
            try:
                pub_date, _ = self._first(rec, self.DATE_FIELDS)
                pub_date = pub_date[:10]
                if pub_date and pub_date < cutoff:
                    skipped_by_date += 1
                    continue

                # Audit 2026-05-06: removed the legacy V1 filter
                #   if cat and "alimentation" not in cat.lower(): continue
                # which dropped every V2 row. V2 categories are "Viandes",
                # "Plats préparés et snacks", "Aliments diététiques et
                # nutrition", etc. — never "Alimentation". The V2 datasets
                # are food-only by definition, so no filter needed here.
                cat, _ = self._first(rec, self.CATEGORY_FIELDS)
                # cat is now collected for downstream use only (Notes
                # enrichment, fallback Product) — not as a filter.

                reason, reason_field = self._first(rec, self.REASON_FIELDS)
                risks, risks_field = self._first(rec, self.RISKS_FIELDS)
                reason_text = (reason + " " + risks).lower()
                if not any(p in reason_text for p in self.PATHOGEN_KEYWORDS):
                    skipped_by_pathogen += 1
                    continue

                fid, _ = self._first(rec, self.FID_FIELDS)
                if not fid:
                    fid, _ = self._first(rec, self.REF_FIELDS)

                url, _ = self._first(rec, self.URL_FIELDS)
                if not url and fid:
                    url = f"https://rappel.conso.gouv.fr/fiche-rappel/{fid}/Interne"

                # ─── Brand / Company / Product extraction ───────────
                brand_raw, brand_field = self._first(rec, self.BRAND_FIELDS)
                company_raw, company_field = self._first(rec, self.COMPANY_FIELDS)
                product_raw, product_field = self._first(rec, self.PRODUCT_FIELDS)

                # If brand field had only "Sans marque", treat as empty
                # so we can fall back to other sources
                if _is_no_brand(brand_raw):
                    brand_raw = ""
                    brand_field = ""

                # Cross-field synthesis: if no brand but product looks
                # like "BRAND - description", lift the brand from there
                synthesized = ""
                if not brand_raw and product_raw:
                    synthesized = self._synthesize_brand_from_product(product_raw)
                    if synthesized:
                        brand_raw = synthesized
                        brand_field = "(synthesized from product)"
                        rows_with_synthesized_brand += 1

                # If we still have no Company, fall back to Brand
                # (single retailer's own-brand recalls). If we have no
                # Brand either, leave both empty so downstream can flag.
                if not company_raw:
                    company_raw = brand_raw

                # Per-row diagnostic when Company ends up empty —
                # operators can grep for this pattern to see exactly
                # which fields were probed.
                if not company_raw:
                    rows_with_empty_company += 1
                    log.warning(
                        "RappelConso fid=%s URL=%s — empty Company after "
                        "all probes. Brand probes: %s | Company probes: %s | "
                        "Product probes: %s",
                        fid, url[:60],
                        self._diagnostic(rec, self.BRAND_FIELDS)[:200],
                        self._diagnostic(rec, self.COMPANY_FIELDS)[:200],
                        self._diagnostic(rec, self.PRODUCT_FIELDS)[:120],
                    )

                co, br = normalise_company_brand(company_raw, brand_raw or "—")

                # Subcategory as Product-fallback ONLY when product field is
                # genuinely empty. The subcategory is often a useless
                # category label like "viandes" — shouldn't masquerade
                # as the product description. When we do fall back,
                # stamp Notes so claude-check knows to enrich.
                product_for_emit = product_raw
                product_is_subcat_fallback = False
                if not product_for_emit:
                    subcat, _ = self._first(rec, self.SUBCATEGORY_FIELDS)
                    product_for_emit = subcat
                    product_is_subcat_fallback = True

                distrib, _ = self._first(rec, self.DISTRIB_FIELDS)
                klass, _ = self._first(rec, self.CLASS_FIELDS)

                # Notes — encode field provenance for claude-check
                notes_parts = [distrib[:120]] if distrib else []
                if not company_raw and not brand_raw:
                    notes_parts.append(
                        "[scraper-flag: no Company/Brand from API — "
                        "claude-check please extract from page]"
                    )
                if product_is_subcat_fallback:
                    notes_parts.append(
                        "[scraper-flag: Product is subcategory fallback]"
                    )
                if synthesized:
                    notes_parts.append(
                        f"[scraper: brand synthesized from product field]"
                    )

                out.append(self._new_recall(
                    Date=pub_date,
                    Company=co,
                    Brand=br,
                    Product=product_for_emit[:300],
                    Pathogen=risks[:200],
                    Reason=reason[:300],
                    Class=klass or "Voluntary",
                    URL=url,
                    Outbreak=0,
                    Notes=" ".join(notes_parts)[:300],
                ))
            except Exception as e:
                log.warning("RappelConso row parse failed: %s", e)

        log.info(
            "RappelConso: %d pathogen recalls (skipped: %d by date, "
            "%d non-pathogen). Quality flags: %d empty Company, "
            "%d brand synthesized.",
            len(out), skipped_by_date,
            skipped_by_pathogen, rows_with_empty_company,
            rows_with_synthesized_brand,
        )
        return out
