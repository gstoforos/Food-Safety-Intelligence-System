"""MPI (NZ) food safety scraper — uses our own model (Qwen, VPS) for HTML extraction."""
from __future__ import annotations
from scrapers._base import GenericLLMScraper


class MPINZScraper(GenericLLMScraper):
    AGENCY = "MPI (NZ)"
    COUNTRY = "New Zealand"
    INDEX_URLS = ['https://www.mpi.govt.nz/food-safety-home/food-recalls/recalled-food-products/']
    LANGUAGE = "en"
