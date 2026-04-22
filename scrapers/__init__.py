"""
scrapers/rss_regulators/__init__.py
====================================
Auto-discovered subpackage of RSS-based regulator scrapers.

Each agency module defines a subclass of BaseRegulatorRSS. run_all.py's
discover_scrapers() walks every subpackage under scrapers/, so dropping
new files here is all that's needed to register a new RSS scraper — no
registration, no imports, no workflow changes.
"""
