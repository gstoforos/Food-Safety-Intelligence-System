"""
AUDIT 2026-10-23 — A PDF WAS BEING CLASSIFIED AS IF IT WERE ARTICLE TEXT
========================================================================

The Italian gap finder's 05:41 run on 2026-10-23 wrote this to
docs/data/gap_finder_it/rejected_records.jsonl:

    Product : "Richiamati filetti di tonno IN's Mercato per istamina e
               integratore per possibile contaminazione"
    Pathogen: "ei"
    Reason  : "Undeclared allergen — out of scope (Rule B reject)."
    URL     : salute.gov.it/.../Cartello%20Richiamo%20Filetti%20di%20Tonno
              %20Surgelato_1792575197.pdf

"ei" is German and Dutch for egg. There is no egg in that recall and no
allergen in it either: the hazard is histamine, and the register had
PUBLISHED the row off that very URL the day before (Recalls 2026-10-20,
IN's Mercato, "Histamine / scombrotoxin").

The authority page is a PDF. article_fetcher had no content-type branch,
so it read resp.text — the PDF's compressed streams lossily decoded to a
str — handed it to BeautifulSoup, got back a few hundred characters of
mojibake, saw len >= 200 and recorded it as "the authority press release".
rules.classify() then matched the two-letter allergen needle "ei" inside
that noise and returned a verdict about a document nothing had read.

Measured on main at 44c81b0, across docs/data/gap_finder_*/verified.jsonl:
11 rows whose efet_body begins "%PDF-", in five countries — it (7 of the
17 verified that day), de, md, se, us. Italy's ministry publishes almost
every recall only as a PDF cartello, so for that country the classifier's
verdict was noise most of the time.

The direction of the error is luck. Here it buried a real recall. The
other way it would have invented a hazard, which is the one unforgivable
failure (rule 5, no fabrication).

These tests hold the repair at both ends:

  * the WRITER — _body_from_response, where the bytes still exist, must
    convert a PDF to its real text and must return "" (never the bytes)
    when it cannot;
  * the READER — extract_body must refuse raw PDF input even if some other
    caller hands it some;
  * the CLASSIFIER — the exact noise that produced the 'ei' verdict must
    not produce an allergen reject any more.
"""

from __future__ import annotations

import io
import sys
import zlib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.gap_finder import article_fetcher, pdf_text    # noqa: E402
from pipeline.gap_finder import rules                        # noqa: E402


# ──────────────────────────────────────────────────────────────────────
# A real PDF, built here so the test needs no network and no fixture file.
# ──────────────────────────────────────────────────────────────────────

CARTELLO_TEXT = (
    "Ministero della Salute - Avviso di richiamo"
    "\nProdotto: Filetti di tonno surgelato"
    "\nMotivo del richiamo: possibile presenza di istamina"
)


def _make_pdf(text: str) -> bytes:
    """A minimal single-page PDF whose text pypdf can extract.

    Written by hand rather than with reportlab so the test does not depend
    on a report-builder dependency to prove a fetch-layer fix.
    """
    def esc(s: str) -> str:
        return s.replace("\\", r"\\").replace("(", r"\(").replace(")", r"\)")

    lines = text.split("\n")
    content = "BT /F1 11 Tf 40 760 Td 14 TL\n"
    content += "\n".join(f"({esc(l)}) Tj T*" for l in lines)
    content += "\nET"
    stream = content.encode("latin-1", "replace")

    objs = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] "
        b"/Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>",
        b"<< /Length " + str(len(stream)).encode() + b" >>\nstream\n"
        + stream + b"\nendstream",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    out = bytearray(b"%PDF-1.7\n")
    offsets = []
    for i, body in enumerate(objs, 1):
        offsets.append(len(out))
        out += str(i).encode() + b" 0 obj\n" + body + b"\nendobj\n"
    xref_at = len(out)
    out += b"xref\n0 " + str(len(objs) + 1).encode() + b"\n"
    out += b"0000000000 65535 f \n"
    for off in offsets:
        out += ("%010d 00000 n \n" % off).encode()
    out += (b"trailer\n<< /Size " + str(len(objs) + 1).encode()
            + b" /Root 1 0 R >>\nstartxref\n"
            + str(xref_at).encode() + b"\n%%EOF\n")
    return bytes(out)


def _binary_pdf_noise() -> bytes:
    """A PDF whose text CANNOT be extracted — the shape that caused the bug.

    A compressed stream with no text operators: pypdf reads the structure,
    finds nothing to extract, and returns empty pages. This is what a
    scanned cartello looks like, and lossily decoding it to str is where
    the 'ei' needle came from.
    """
    blob = zlib.compress(bytes(range(256)) * 24)
    return (b"%PDF-1.7\n%\xb5\xb6\n1 0 obj\n<< /Length "
            + str(len(blob)).encode() + b" /Filter /FlateDecode >>\nstream\n"
            + blob + b"\nendstream\nendobj\n%%EOF\n")


class _Resp:
    """The 2 fields _body_from_response reads off a 200 response."""

    def __init__(self, raw: bytes, ctype: str = ""):
        self.content = raw
        self.headers = {"content-type": ctype} if ctype else {}
        # What requests / curl_cffi hand back: a lossy decode of the bytes.
        self.text = raw.decode("utf-8", "replace")


# ──────────────────────────────────────────────────────────────────────
# looks_like_pdf — decided on the bytes, never on the URL
# ──────────────────────────────────────────────────────────────────────

def test_a_pdf_is_recognised_from_its_magic_bytes():
    pdf = _make_pdf(CARTELLO_TEXT)
    assert pdf_text.looks_like_pdf(pdf)
    assert pdf_text.looks_like_pdf(pdf.decode("utf-8", "replace")), (
        "the lossy str decode keeps the ASCII %PDF- header — that is the "
        "form article_fetcher used to pass on, so it must be recognised")


def test_a_pdf_is_recognised_from_its_content_type():
    assert pdf_text.looks_like_pdf(b"", "application/pdf")
    assert pdf_text.looks_like_pdf(b"", "Application/PDF; charset=binary")


def test_an_html_page_that_merely_mentions_pdf_is_not_a_pdf():
    html = ("<html><body><p>Il cartello di richiamo e disponibile in "
            "formato PDF: scarica il %PDF documento.</p>" + "x" * 4000
            + "</body></html>")
    assert not pdf_text.looks_like_pdf(html), (
        "the magic scan is windowed to the first KiB on purpose: a page "
        "that talks about PDFs is not one")


# ──────────────────────────────────────────────────────────────────────
# The writer: _body_from_response
# ──────────────────────────────────────────────────────────────────────

def test_the_fetch_layer_returns_the_pdfs_real_text():
    body, status = article_fetcher._body_from_response(
        _Resp(_make_pdf(CARTELLO_TEXT), "application/pdf"))
    assert status == "ok_pdf"
    assert "istamina" in body.lower(), (
        "the hazard word the classifier needs is in the cartello; the "
        "fetch layer must deliver it instead of the compressed bytes")
    assert "%PDF-" not in body


def test_an_unreadable_pdf_yields_an_empty_body_not_the_bytes():
    body, status = article_fetcher._body_from_response(
        _Resp(_binary_pdf_noise(), "application/pdf"))
    assert body == "", (
        "a PDF whose text cannot be extracted must produce NO body. "
        "Returning the bytes is what let rules.classify() match 'ei' in "
        "the noise and reject a published histamine recall as an allergen.")
    assert status == "pdf_unreadable"


def test_an_ordinary_html_response_is_untouched():
    html = "<html><body><article>" + "Listeria " * 60 + "</article></body></html>"
    body, status = article_fetcher._body_from_response(
        _Resp(html.encode(), "text/html; charset=utf-8"))
    assert status == "ok"
    assert body == html, "the PDF screen must not touch an HTML fetch"


# ──────────────────────────────────────────────────────────────────────
# The reader: extract_body
# ──────────────────────────────────────────────────────────────────────

def test_extract_body_reads_a_pdf_rather_than_its_markup_noise():
    pdf_as_str = _make_pdf(CARTELLO_TEXT).decode("utf-8", "replace")
    body = article_fetcher.extract_body(pdf_as_str)
    assert "%PDF" not in body
    assert "istamina" in body.lower()


def test_extract_body_refuses_to_return_noise_for_an_unreadable_pdf():
    noise = _binary_pdf_noise().decode("utf-8", "replace")
    body = article_fetcher.extract_body(noise)
    assert body == "", (
        "extract_body is public and called on bodies from cached "
        "verified.jsonl too — the one outcome that invents hazards is "
        "refused at the reader as well as the writer")


def test_the_pdf_body_passes_through_the_authority_length_floor():
    """enrich_candidate accepts an authority body only at >= 200 chars.

    The wrapper must not shrink a real cartello below that floor, or the
    fix would trade a wrong verdict for a silently dropped recall.
    """
    long_cartello = CARTELLO_TEXT + "\n" + ("Lotto 1792575197 - " * 20)
    body, _ = article_fetcher._body_from_response(
        _Resp(_make_pdf(long_cartello), "application/pdf"))
    assert len(article_fetcher.extract_body(body)) >= 200


# ──────────────────────────────────────────────────────────────────────
# The classifier: the exact verdict that started this
# ──────────────────────────────────────────────────────────────────────

def test_pdf_noise_no_longer_produces_an_allergen_reject():
    """The regression, end to end.

    Before the fix the body below classified reject/allergen with
    matched_term 'ei'. After it the body is empty, which classifies
    'unknown' — defer to manual review. Unknown is the honest answer when
    the notice could not be read; allergen was a fabrication.
    """
    body, _ = article_fetcher._body_from_response(
        _Resp(_binary_pdf_noise(), "application/pdf"))
    verdict = rules.classify(pathogen="", reason=body)
    assert verdict.category != "allergen", (
        f"a PDF we cannot read classified as {verdict.category!r} with "
        f"matched_term {verdict.matched_term!r} — the noise is still "
        f"reaching the lexicons")


def test_a_readable_cartello_classifies_on_its_real_hazard():
    body, _ = article_fetcher._body_from_response(
        _Resp(_make_pdf(CARTELLO_TEXT), "application/pdf"))
    text = article_fetcher.extract_body(body)
    verdict = rules.classify(pathogen="", reason=text)
    assert verdict.category != "allergen", (
        "the tuna cartello names histamine and no allergen at all")


# ──────────────────────────────────────────────────────────────────────
# The dependency this rests on
# ──────────────────────────────────────────────────────────────────────

def test_pypdf_is_a_declared_dependency():
    """pdf_text's first strategy must be installed in the Actions runner.

    If it is not, every PDF authority page silently becomes an empty body:
    safe, but every Italian recall would go to manual review forever.
    """
    req = (ROOT / "requirements.txt").read_text("utf-8")
    assert "pypdf" in req
    from pypdf import PdfReader                                  # noqa: F401


def test_pdf_text_does_not_import_claude_check():
    """Operator ruling 2026-09-30: nothing may run or import claude_check.

    pipeline/claude_check.py has the same extraction chain. Reusing it by
    import would have been shorter and would have broken that rule, so the
    chain is deliberately reimplemented in pdf_text.
    """
    src = (ROOT / "pipeline" / "gap_finder" / "pdf_text.py").read_text("utf-8")
    for line in src.splitlines():
        stripped = line.strip()
        if stripped.startswith(("import ", "from ")):
            assert "claude_check" not in stripped, (
                f"pdf_text imports claude_check: {stripped!r}")
