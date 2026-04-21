"""MPI (NZ) food safety scraper — uses Gemini for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericGeminiScraper


class MPINZScraper(GenericGeminiScraper):
    AGENCY = "MPI (NZ)"
    COUNTRY = "New Zealand"
    INDEX_URLS = ['https://www.mpi.govt.nz/food-safety-home/food-recalls/recalled-food-products/']
    LANGUAGE = "en"
