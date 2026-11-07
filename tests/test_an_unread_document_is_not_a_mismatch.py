# -*- coding: utf-8 -*-
"""A document we could not read never becomes a verdict about the row.

THE INCIDENT (2026-11-07)
=========================
``pipeline/_provenance.fetch_text`` strips HTML tags out of ``r.text`` and
calls anything longer than ``_MIN_PAGE_CHARS`` (300) readable. A PDF has no
tags to strip. ``requests`` decodes its bytes — latin-1, when the server
declares no charset — into a few thousand characters of binary noise, which
clears the 300-character floor and is returned with status ``"ok"``. No
anchor token can occur in noise, so ``check()`` fell through to its last
branch and reported

    page does not mention any of ['norderstedter', 'fleisch', 'wurstwaren',
    'peter', 'faden', 'rinder'] — the cited URL describes something other
    than this row

about a document it had never read a word of.

The row that refusal landed on was real: the Norderstedter Fleisch- und
Wurstwaren Peter Faden GmbH & Co KG recall of fourteen beef-salami products,
published by BVL (DE) on 2026-11-06 after Shiga toxin-producing *E. coli*
(STEC) was found in a sample — fifteen German federal states, forty lot
codes. The confirm agent put it in Weekly_Rejected while the offline
promoter published the same row from Pending, so the register held it both
published and thrown away.

MEASURED on the 2026-11-07 register: 31 rows carry that "does not mention
any of" refusal and 6 of them cite a PDF — BVL (DE) twice, Hong Kong CFS
three times, MHLW (JP) once. Three regulators that publish recalls as PDF.

THE RULE
========
``fetch_text`` already separates "could not read" from "does not exist" for
an application shell (``js_shell``) and for a bot wall (``bot_wall``). A
binary document is the same fact one content type later, so it returns a
status of its own and ``check()`` treats it as infrastructure: not dead, not
a content mismatch, nothing said about the row.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline import _provenance as P  # noqa: E402

#: The row the incident was about, with the anchors it offers.
BVL_ROW = {
    "Date": "2026-11-06",
    "Source": "BVL (DE)",
    "Company": "Norderstedter Fleisch- und Wurstwaren Peter Faden GmbH & Co KG",
    "Brand": "",
    "Product": "Beef pepperoni salami, beef salami, pure-beef salami",
    "Pathogen": "Shiga toxin-producing E. coli (STEC)",
    "Reason": "Shiga toxin-producing Escherichia coli (STEC) detected",
    "URL": ("https://www.lebensmittelwarnung.de/___lebensmittelwarnung.de/"
            "Meldungen/2026/11_November/261106_05_SH_Rindfleischsalamiprodukte/"
            "261106_05_SH_Rindfleischsalamiprodukte_Presse.pdf"
            "?__blob=publicationFile&v=2"),
}


def _pdf_body(n: int = 4000) -> str:
    """A PDF decoded the way requests decodes an undeclared charset."""
    return ("%PDF-1.4\n%\xe2\xe3\xcf\xd3\n"
            "1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
            "4 0 obj<</Length 2000/Filter/FlateDecode>>stream\n"
            + "".join(chr((i * 7919) % 256) for i in range(n))
            + "\nendstream endobj\ntrailer<</Root 1 0 R>>\n%%EOF")


class _Resp:
    def __init__(self, body, ctype="", status=200):
        self.text = body
        self.status_code = status
        self.headers = {"Content-Type": ctype} if ctype else {}


# ---------------------------------------------------------------------------
# the hole itself
# ---------------------------------------------------------------------------

def test_pdf_noise_would_have_cleared_the_length_floor():
    """Why the guard has to come BEFORE tag-stripping.

    If this ever stops being true the regression is gone for another
    reason, and that is worth knowing rather than silently relying on.
    """
    body = _pdf_body()
    text = re.sub(r"<[^>]+>", " ", body)
    assert len(P._norm(text).strip()) >= P._MIN_PAGE_CHARS, (
        "a decoded PDF no longer clears _MIN_PAGE_CHARS — re-read the "
        "docstring before deleting anything")


@pytest.mark.parametrize("ctype", ["application/pdf",
                                   "application/pdf;charset=UTF-8",
                                   "APPLICATION/PDF",
                                   "application/octet-stream"])
def test_a_declared_binary_type_is_unread(ctype):
    status = P._unreadable_document_status(_Resp("whatever", ctype))
    assert status.startswith("binary_document:"), status
    assert not P.is_dead_status(status), (
        "an unread document must never read as a dead page — a dead status "
        "blocks the row")


def test_the_magic_bytes_are_enough_when_the_server_declares_nothing():
    """lebensmittelwarnung.de serves PDFs through ?__blob=publicationFile."""
    status = P._unreadable_document_status(_Resp(_pdf_body(), ""))
    assert status == "binary_document:pdf", status


def test_html_is_still_readable():
    body = "<html><body><p>" + ("Norderstedter Fleisch " * 60) + "</p></body></html>"
    assert P._unreadable_document_status(_Resp(body, "text/html")) == ""


# ---------------------------------------------------------------------------
# what check() does with it
# ---------------------------------------------------------------------------

def test_check_says_nothing_about_a_row_whose_notice_is_a_pdf(monkeypatch):
    monkeypatch.setattr(P, "fetch_text",
                        lambda url, timeout=25: ("", "binary_document:pdf"))
    assert P.check(BVL_ROW) == [], (
        "an unread PDF produced a verdict about the row — this is the "
        "2026-11-06 BVL salami rejection all over again")


def test_an_unread_pdf_is_reportable_when_the_caller_asks_for_proof(monkeypatch):
    """A model-sourced row may still be held — but on 'unread', not 'wrong'."""
    monkeypatch.setattr(P, "fetch_text",
                        lambda url, timeout=25: ("", "binary_document:pdf"))
    out = P.check(BVL_ROW, treat_unreachable_as_problem=True)
    assert len(out) == 1
    assert "could not read" in out[0]
    assert "describes something other than this row" not in out[0]


def test_a_real_content_mismatch_still_blocks(monkeypatch):
    monkeypatch.setattr(
        P, "fetch_text",
        lambda url, timeout=25: ("this page is about something else entirely "
                                 * 20, "ok"))
    out = P.check(BVL_ROW)
    assert out and "describes something other than this row" in out[0], (
        "the guard must not have swallowed the genuine mismatch case")


def test_a_dead_page_still_blocks(monkeypatch):
    monkeypatch.setattr(P, "fetch_text",
                        lambda url, timeout=25: ("", "http_404"))
    out = P.check(BVL_ROW)
    assert out and "does not exist" in out[0]


if __name__ == "__main__":                              # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
