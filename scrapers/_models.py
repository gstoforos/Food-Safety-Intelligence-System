"""
Recall data model — shared by all 57 scrapers + AI clients.
Schema matches existing recalls.xlsx columns exactly.
"""
from __future__ import annotations
from dataclasses import dataclass, field, asdict
from datetime import datetime, date
from typing import Optional, List, Dict, Any
import re
import unicodedata


# ===== Pathogen taxonomy (regex -> canonical name) =====
PATHOGEN_RULES = [
    ("Listeria monocytogenes", r"(listeria\s*monocytogenes|l\.\s*monocytogenes|\blm\b|listeri[ae])"),
    ("Salmonella Enteritidis", r"salmonella\s*enteritidis"),
    ("Salmonella Typhimurium", r"salmonella\s*typhimurium"),
    ("Salmonella spp.", r"salmonella"),
    ("E. coli O157:H7", r"o\s*157[:\-\s]?h7|escherichia\s*coli\s*o157"),
    ("STEC (Shiga toxin-producing E. coli)", r"stec|shiga\s*toxin|o(26|45|103|104|111|121|145):h\d"),
    ("E. coli", r"e\.?\s*coli|escherichia\s*coli"),
    ("Clostridium botulinum", r"botulin|botulism|c\.\s*botulinum|clostridium"),
    ("Norovirus", r"norovirus|norwalk"),
    ("Hepatitis A", r"hepatit[ie]s\s*a|\bhav\b"),
    ("Campylobacter", r"campylobacter"),
    ("Cyclospora", r"cyclospora"),
    ("Vibrio", r"vibrio"),
    ("Cronobacter sakazakii", r"cronobacter"),
    ("Bacillus cereus / cereulide", r"bacillus\s*cereus|cereulid[ae]"),
    ("Aflatoxins", r"aflatoxin"),
    ("Ochratoxin A", r"ochratoxin"),
    ("Patulin", r"patulin"),
    ("Lipophilic biotoxins (DSP)", r"lipophilic|diarr?h?etic\s*shellfish|dsp\b"),
    ("Paralytic shellfish toxins (PSP)", r"paralytic\s*shellfish|saxitoxin|psp\b"),
    ("Amnesic shellfish toxins (ASP)", r"amnesic\s*shellfish|domoic|asp\b"),
    ("Histamine (scombrotoxin)", r"histamine|scombro"),
    ("Shigella", r"shigella"),
    ("Yersinia", r"yersinia"),
    ("Mycotoxin", r"mycotoxin"),
]


def normalize_pathogen(text: str) -> str:
    """Map any free-text pathogen mention to canonical name."""
    if not text:
        return ""
    t = text.lower()
    for canon, pattern in PATHOGEN_RULES:
        if re.search(pattern, t):
            return canon
    return text.strip()


def normalize_country(text: str) -> str:
    """Map ISO-2/ISO-3/various names to canonical English country name."""
    if not text:
        return ""
    t = text.strip().lower()
    mapping = {
        "us": "USA", "usa": "USA", "united states": "USA", "u.s.": "USA", "u.s.a.": "USA",
        "uk": "United Kingdom", "gb": "United Kingdom", "great britain": "United Kingdom",
        "fr": "France", "de": "Germany", "deutschland": "Germany",
        "it": "Italy", "italia": "Italy", "es": "Spain", "españa": "Spain",
        "gr": "Greece", "ελλάδα": "Greece", "ellada": "Greece",
        "be": "Belgium", "belgique": "Belgium", "belgië": "Belgium",
        "nl": "Netherlands", "nederland": "Netherlands",
        "pt": "Portugal", "ie": "Ireland", "at": "Austria", "österreich": "Austria",
        "se": "Sweden", "sverige": "Sweden", "dk": "Denmark", "danmark": "Denmark",
        "fi": "Finland", "suomi": "Finland", "pl": "Poland", "polska": "Poland",
        "cz": "Czech Republic", "česko": "Czech Republic",
        "hu": "Hungary", "magyarország": "Hungary",
        "ro": "Romania", "sk": "Slovakia", "si": "Slovenia", "hr": "Croatia",
        "bg": "Bulgaria", "lt": "Lithuania", "lv": "Latvia", "ee": "Estonia",
        "cy": "Cyprus", "mt": "Malta", "lu": "Luxembourg",
        "ch": "Switzerland", "schweiz": "Switzerland", "suisse": "Switzerland",
        "no": "Norway", "norge": "Norway", "is": "Iceland", "ísland": "Iceland",
        "ca": "Canada", "jp": "Japan", "日本": "Japan",
        "kr": "South Korea", "한국": "South Korea",
        "cn": "China", "中国": "China", "hk": "Hong Kong", "香港": "Hong Kong",
        "sg": "Singapore", "tw": "Taiwan", "台灣": "Taiwan",
        "in": "India", "th": "Thailand", "vn": "Vietnam", "việt nam": "Vietnam",
        "my": "Malaysia", "id": "Indonesia", "ph": "Philippines",
        "au": "Australia", "nz": "New Zealand",
        "za": "South Africa", "ke": "Kenya", "ng": "Nigeria", "eg": "Egypt", "ma": "Morocco", "gh": "Ghana",
        "br": "Brazil", "brasil": "Brazil", "ar": "Argentina", "mx": "Mexico", "méxico": "Mexico",
        "cl": "Chile", "co": "Colombia", "pe": "Peru", "ec": "Ecuador", "uy": "Uruguay",
        "il": "Israel", "ae": "UAE", "sa": "Saudi Arabia", "qa": "Qatar", "tr": "Turkey", "türkiye": "Turkey",
    }
    return mapping.get(t, text.strip().title())


def assign_tier(pathogen: str, outbreak: int = 0) -> int:
    """Tier 1 = critical (Listeria/STEC/Botulinum/biotoxins/HepA/cereulide or any outbreak).
       Tier 2 = single detection without outbreak link."""
    if outbreak == 1:
        return 1
    p = (pathogen or "").lower()
    tier1_keywords = ["listeria", "stec", "botulin", "cereulide", "biotoxin", "saxitoxin",
                      "domoic", "hepatit", "o157", "cronobacter", "shiga"]
    if any(k in p for k in tier1_keywords):
        return 1
    return 2


def parse_date(text: str) -> str:
    """Parse any date format -> YYYY-MM-DD string. Returns '' on failure."""
    if not text:
        return ""
    if isinstance(text, (datetime, date)):
        return text.strftime("%Y-%m-%d")
    text = str(text).strip()
    formats = [
        "%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%d-%m-%Y", "%m/%d/%Y", "%d.%m.%Y",
        "%d %B %Y", "%B %d, %Y", "%d %b %Y", "%b %d, %Y",
        "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y%m%d",
        "%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S GMT",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(text[:len(fmt)+5], fmt).strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            continue
    # Try ISO with timezone
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        pass
    return ""


@dataclass
class Recall:
    """Canonical recall row — matches recalls.xlsx schema exactly."""
    Date: str = ""               # YYYY-MM-DD
    Source: str = ""             # e.g. "FDA", "RappelConso (FR)"
    Company: str = ""
    Brand: str = ""
    Product: str = ""
    Pathogen: str = ""
    Reason: str = ""
    Class: str = ""              # Recall / Alert / PHA / etc.
    Country: str = ""
    Region: str = ""             # North America / Europe / Asia / Oceania / Africa / South America / Middle East
    Tier: int = 2
    Outbreak: int = 0
    URL: str = ""
    Notes: str = ""

    def normalize(self) -> "Recall":
        """Apply all normalizations. Call once before merging."""
        self.Date = parse_date(self.Date)
        self.Pathogen = normalize_pathogen(self.Pathogen)
        self.Country = normalize_country(self.Country) if self.Country else self.Country
        if not self.Region and self.Country:
            self.Region = COUNTRY_REGION.get(self.Country, "")
        self.Tier = assign_tier(self.Pathogen, self.Outbreak)
        # Strip control chars and normalize unicode
        for f in ["Source", "Company", "Brand", "Product", "Reason", "Class", "Notes"]:
            v = getattr(self, f)
            if v:
                v = unicodedata.normalize("NFKC", str(v))
                v = re.sub(r"\s+", " ", v).strip()
                setattr(self, f, v)
        return self

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def dedup_key(self) -> str:
        """For deduplication: prefer URL, fallback to date+company+pathogen."""
        if self.URL:
            return self.URL.lower().strip()
        co = unicodedata.normalize("NFD", self.Company or "").encode("ascii", "ignore").decode().lower()
        co = re.sub(r"[^a-z0-9]", "", co)[:30]
        return f"{self.Date}|{co}|{normalize_pathogen(self.Pathogen)[:30]}"


# Region inference from country
COUNTRY_REGION = {
    "USA": "North America", "Canada": "North America", "Mexico": "North America",
    "France": "Europe", "Germany": "Europe", "Italy": "Europe", "Spain": "Europe",
    "Greece": "Europe", "Belgium": "Europe", "Netherlands": "Europe", "Portugal": "Europe",
    "Ireland": "Europe", "Austria": "Europe", "Sweden": "Europe", "Denmark": "Europe",
    "Finland": "Europe", "Poland": "Europe", "Czech Republic": "Europe", "Hungary": "Europe",
    "Romania": "Europe", "Slovakia": "Europe", "Slovenia": "Europe", "Croatia": "Europe",
    "Bulgaria": "Europe", "Lithuania": "Europe", "Latvia": "Europe", "Estonia": "Europe",
    "Cyprus": "Europe", "Malta": "Europe", "Luxembourg": "Europe", "United Kingdom": "Europe",
    "Switzerland": "Europe", "Norway": "Europe", "Iceland": "Europe",
    "Japan": "Asia", "South Korea": "Asia", "China": "Asia", "Hong Kong": "Asia",
    "Singapore": "Asia", "Taiwan": "Asia", "India": "Asia", "Thailand": "Asia",
    "Vietnam": "Asia", "Malaysia": "Asia", "Indonesia": "Asia", "Philippines": "Asia",
    "Australia": "Oceania", "New Zealand": "Oceania",
    "South Africa": "Africa", "Kenya": "Africa", "Nigeria": "Africa",
    "Egypt": "Africa", "Morocco": "Africa", "Ghana": "Africa",
    "Brazil": "South America", "Argentina": "South America", "Chile": "South America",
    "Colombia": "South America", "Peru": "South America", "Ecuador": "South America",
    "Uruguay": "South America",
    "Israel": "Middle East", "UAE": "Middle East", "Saudi Arabia": "Middle East",
    "Qatar": "Middle East", "Turkey": "Middle East",
}


def infer_region(country: str) -> str:
    return COUNTRY_REGION.get(country, "")
