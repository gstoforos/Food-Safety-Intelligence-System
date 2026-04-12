"""
FSIS Agent – Configuration
FDA module (Phase 1)
"""

# ── openFDA ──────────────────────────────────────────────────────────────────
FDA_API_BASE   = "https://api.fda.gov/food/enforcement.json"
FDA_API_KEY    = ""          # optional – set env var OPENFDA_API_KEY for higher rate limits
FDA_PAGE_LIMIT = 100         # max per request (openFDA cap)
FDA_LOOKBACK_DAYS = 90       # how far back to pull on first run

# ── Gemini ────────────────────────────────────────────────────────────────────
GEMINI_API_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "gemini-2.0-flash:generateContent"
)
# Set env var GEMINI_API_KEY – never hard-code
GEMINI_BATCH_DELAY = 1.2     # seconds between calls (free tier ~1 req/s safe)
GEMINI_MAX_RETRIES = 3

# ── Database ──────────────────────────────────────────────────────────────────
DB_PATH = "fsis_fda.db"

# ── Pathogen whitelist (recall reason must contain at least one) ──────────────
PATHOGENS = [
    "Salmonella",
    "Listeria",
    "E. coli",
    "Escherichia coli",
    "Clostridium botulinum",
    "Botulinum",
    "Norovirus",
    "Hepatitis A",
    "Campylobacter",
    "Vibrio",
    "Cyclospora",
    "Yersinia",
    "Bacillus cereus",
    "Brucella",
    "Mycotoxin",
    "Aflatoxin",
    "Staphylococcus",
    "Shigella",
    "Cronobacter",
]

# ── FDA Classification severity map ──────────────────────────────────────────
CLASS_SEVERITY = {
    "Class I":   "CRITICAL",   # life-threatening
    "Class II":  "MODERATE",   # adverse health consequences
    "Class III": "LOW",        # unlikely to cause harm
}

# ── Report ────────────────────────────────────────────────────────────────────
REPORT_OUTPUT = "docs/index.html"   # GitHub Pages serves from /docs
REPORT_TITLE  = "FDA Food Safety Intelligence – Pathogen Recall Monitor"
BRAND_NAME    = "AFTS · Food Safety Intelligence"
BRAND_URL     = "https://advfood.tech"

# AFTS palette
COLOR_BG      = "#111111"
COLOR_SURFACE = "#1a1a1a"
COLOR_ORANGE  = "#E8601A"
COLOR_WHITE   = "#F5F5F5"
COLOR_MUTED   = "#888888"
COLOR_CRITICAL= "#E8601A"
COLOR_MODERATE= "#E8A01A"
COLOR_LOW     = "#4CAF50"
