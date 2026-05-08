"""TGTHB (TR) food safety scraper — uses Gemini for HTML extraction.

URL retargeting (Batch 3, 2026-05-08):
The previous URL `tarimorman.gov.tr/Duyuru` was the generic Ministry of
Agriculture announcements page (covering everything from agricultural
subsidies to administrative notices), which consistently returned 0 rows
from Gemini for food recall purposes.

The dedicated `guvenilirgida` (Reliable Food) subdomain hosts the
'Gıda Kamuoyu Duyurusu' (GKD — Food Public Disclosure) section, which
publishes the official lists required under Article 31(6) of Turkey's
Veterinary Services, Plant Health, Food and Feed Law (5996). These lists
cover:
  - 'Taklit veya Tağşiş Yapılan Gıdalar' (Imitated/Adulterated Foods)
  - 'Sağlığı Tehlikeye Düşürecek Gıdalar' (Foods Endangering Health)

The base /gkd page surfaces both categories. Gemini handles these
structured listings well — each entry has firm name, product name,
brand, batch/serial number, and the type of fraud/contamination.

Note: Many TGTHB entries are food-fraud (mislabeling, adulteration with
cheaper ingredients) rather than pathogen contamination. The downstream
pathogen filter in pipeline/merge_master will keep only pathogen
matches; this is fine — the scraper's job is to surface candidates.
"""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class TGTHBScraper(GenericGeminiScraper):
    AGENCY = "TGTHB (TR)"
    COUNTRY = "Turkey"
    INDEX_URLS = [
        'https://guvenilirgida.tarimorman.gov.tr/gkd',
    ]
    LANGUAGE = "tr"
    EXTRACTION_HINTS = (
        "All entries are food safety disclosures. Two main categories: "
        "(a) 'Taklit veya Tağşiş' (Imitation/Adulteration) — usually "
        "mislabeling or substitution, NOT pathogen-related. "
        "(b) 'Sağlığı Tehlikeye Düşürecek' (Endangering Health) — these "
        "may include pathogen contamination, illegal additives, or "
        "drug residues. Extract entries with firm name, product name, "
        "brand, and batch/serial number when available. Pathogens may "
        "appear in Turkish as 'Listeria', 'Salmonella', 'patojen', "
        "'mikrobiyolojik kontaminasyon', or specific bacterium names."
    )
