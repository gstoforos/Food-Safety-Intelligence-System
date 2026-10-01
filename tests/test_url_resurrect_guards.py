"""url_resurrect was a Gemini-grounded URL repair tool — RETIRED 2026-09-30
(operator ruling: no Gemini anywhere). URL resolution is reviewer 1's job
(pipeline/recall_url_agent.py, our own model + Searx).

This file is kept as an empty skip only so the suite stays green whether or
not pipeline/url_resurrect.py has been deleted yet. Delete both.
"""
import pytest

pytest.skip("url_resurrect retired 2026-09-30 (no Gemini)", allow_module_level=True)
