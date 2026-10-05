"""A news item is tagged Outbreak only with human cases (2026-10-05).

The daily accuracy brief flagged Food Safety News "Czech testing finds
Salmonella problem in poultry meat" — a retail-testing story, title naming no
illness — tagged Event=Outbreak because the word appeared in its summary.
"""
import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location("news_mod", ROOT / "scrapers" / "news.py")
news = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(news)

T = "Czech testing finds Salmonella problem in poultry meat"


def test_a_testing_story_is_not_an_outbreak():
    text = T + " Past Salmonella outbreaks were linked to poultry; 30% of samples were positive."
    assert news._classify_event(text, T) == "News"


def test_a_summary_with_human_cases_is_an_outbreak():
    assert news._classify_event(T + " The outbreak has sickened 40 people.", T) == "Outbreak"


def test_an_outbreak_title_still_decides():
    t = "Salmonella outbreak linked to eggs"
    assert news._classify_event(t + " Investigation continues.", t) == "Outbreak"


def test_a_recall_is_still_a_recall():
    t = "Company recalls chicken over Listeria"
    assert news._classify_event(t + " No illnesses reported.", t) == "Recall"


def test_the_writer_passes_the_title():
    src = (ROOT / "scrapers" / "news.py").read_text(encoding="utf-8")
    assert "event = _classify_event(text, title)" in src
