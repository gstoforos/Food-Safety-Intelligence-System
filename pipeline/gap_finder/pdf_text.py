"""
AFTS Food Safety Intelligence — Gap Finder
PDF detection and text extraction for fetched authority pages.

╔════════════════════════════════════════════════════════════════════════════╗
║  2026-10-23 — A PDF WAS BEING CLASSIFIED AS IF IT WERE ARTICLE TEXT       ║
╠════════════════════════════════════════════════════════════════════════════╣
║ Found by the morning fix pass. The Italian gap finder rejected            ║
║                                                                            ║
║   "Richiamati filetti di tonno IN's Mercato per istamina e integratore     ║
║    per possibile contaminazione"                                           ║
║                                                                            ║
║ as  allergen: Undeclared allergen — out of scope (Rule B reject)           ║
║ with Pathogen "ei"  — German/Dutch for egg.                                ║
║                                                                            ║
║ There is no egg in that recall. The hazard is histamine, and the register  ║
║ had already PUBLISHED the row the day before, off the same authority URL.  ║
║                                                                            ║
║ The authority URL is a PDF:                                                ║
║   salute.gov.it/.../Cartello%20Richiamo%20Filetti%20di%20Tonno...pdf       ║
║                                                                            ║
║ article_fetcher had no content-type branch. It read resp.text — the PDF's  ║
║ compressed bytes lossily decoded to a str — handed that to BeautifulSoup,  ║
║ got several hundred characters of mojibake back, saw len >= 200 and        ║
║ accepted it as "the authority press release". rules.classify() then        ║
║ matched the two-letter allergen needle "ei" somewhere inside the binary    ║
║ noise and returned a confident verdict about a document it had never read. ║
║                                                                            ║
║ Measured across docs/data/gap_finder_*/verified.jsonl on main at           ║
║ 44c81b0: 11 rows whose efet_body begins "%PDF-", in 5 countries —          ║
║ it (7 of 17 verified rows that day), de, md, se, us. Italy's Ministero     ║
║ della Salute publishes most of its recalls ONLY as a PDF cartello, so for  ║
║ that country the classifier's verdict was noise most of the time. It can   ║
║ fall either way: a false REJECT buries a real recall (what happened        ║
║ here), and a false ACCEPT would invent a hazard — which rule 5, no         ║
║ fabrication, exists to prevent.                                            ║
║                                                                            ║
║ This module is the repair. Two jobs, both deterministic and offline:       ║
║                                                                            ║
║   looks_like_pdf()  — is this response a PDF? (magic bytes or             ║
║                       content-type; never a guess from the URL suffix,     ║
║                       which lies in both directions)                       ║
║   extract_text()    — the real text, via pypdf.                            ║
║                                                                            ║
║ pypdf is already in requirements.txt (added 2026-05-13) and the fallback   ║
║ chain here is the same one pipeline/claude_check.py uses. The chain is     ║
║ REIMPLEMENTED rather than imported: claude_check stays in the repo but     ║
║ nothing may run or import it (operator ruling 2026-09-30, held by          ║
║ tests/test_nothing_runs_claude_check.py).                                  ║
║                                                                            ║
║ WHEN EXTRACTION FAILS the answer is "" — never the raw bytes. An empty     ║
║ body makes the candidate classify 'unknown: No matching hazard category    ║
║ — defer to manual review', which is the honest outcome: we could not read  ║
║ the notice, so we do not pretend to know what it says.                     ║
╚════════════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import io
import logging

log = logging.getLogger(__name__)

# The PDF magic number. A conforming file begins with it; the spec also
# tolerates a few junk bytes before the header, which is why the scan below
# looks in a window rather than only at offset 0.
PDF_MAGIC_BYTES = b"%PDF-"
PDF_MAGIC_TEXT = "%PDF-"

# How far into the response to look for the header. 1 KiB is far more slack
# than the spec allows and still cannot be reached by an HTML page that
# merely mentions a PDF further down.
_MAGIC_WINDOW = 1024

# Upper bound on extracted text. Mirrors article_fetcher.MAX_BODY_CHARS'
# purpose (keep the extractor prompt small); applied here so a 400-page
# annex cannot blow up the record.
MAX_PDF_CHARS = 20000


def looks_like_pdf(body: "str | bytes | None", content_type: str = "") -> bool:
    """True when this response is a PDF document.

    Decided on evidence from the response itself, in this order:

      1. the Content-Type header, when the server sent one; and
      2. the %PDF- header in the first kilobyte of the body.

    NOT decided on the URL. A `.pdf` suffix is not proof (salute.gov.it
    serves an HTML error page at some .pdf paths when the file has moved),
    and the absence of one is not proof either — ages.at and
    lebensmittelwarnung.de both serve PDFs off extensionless routes. The
    bytes are the only thing that cannot be wrong.

    `body` may be bytes (what the HTTP layer received) or str (what
    requests/curl_cffi hands back after its own lossy decode). The decode
    mangles the compressed streams but leaves the ASCII header intact, so
    the magic string survives in both forms and both are checked.
    """
    if content_type and "application/pdf" in content_type.lower():
        return True
    if not body:
        return False
    if isinstance(body, bytes):
        return PDF_MAGIC_BYTES in body[:_MAGIC_WINDOW]
    return PDF_MAGIC_TEXT in body[:_MAGIC_WINDOW]


def extract_text(pdf_bytes: bytes) -> str:
    """Extract the text of a PDF. Returns "" when it cannot be read.

    Strategy chain, lightest dependency first — the same order
    pipeline/claude_check.py settled on, kept because pypdf alone covers
    very nearly every real regulator cartello:

      1. pypdf      — in requirements.txt, pure-Python
      2. PyPDF2     — the legacy fork, sometimes present
      3. pdfplumber — heaviest, best at the columnar tables Latin American
                      regulators use

    Every failure is swallowed and logged. A gap finder must not crash
    because one notice is a scanned image or an encrypted form — it must
    report that it could not read it, which is what "" means to the caller.
    """
    if not pdf_bytes:
        return ""

    # Strategy 1 — pypdf.
    try:
        from pypdf import PdfReader                    # type: ignore
        reader = PdfReader(io.BytesIO(pdf_bytes))
        pages = [(p.extract_text() or "") for p in reader.pages]
        text = "\n".join(pages).strip()
        if text:
            return text[:MAX_PDF_CHARS]
    except Exception as exc:                           # noqa: BLE001
        log.warning("pypdf failed: %s: %s", type(exc).__name__, str(exc)[:120])

    # Strategy 2 — PyPDF2 (legacy fork).
    try:
        from PyPDF2 import PdfReader as PdfReader2     # type: ignore
        reader2 = PdfReader2(io.BytesIO(pdf_bytes))
        pages = [(p.extract_text() or "") for p in reader2.pages]
        text = "\n".join(pages).strip()
        if text:
            return text[:MAX_PDF_CHARS]
    except Exception as exc:                           # noqa: BLE001
        log.warning("PyPDF2 failed: %s: %s", type(exc).__name__, str(exc)[:120])

    # Strategy 3 — pdfplumber.
    try:
        import pdfplumber                              # type: ignore
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            pages = [(pg.extract_text() or "") for pg in pdf.pages]
        text = "\n".join(pages).strip()
        if text:
            return text[:MAX_PDF_CHARS]
    except Exception as exc:                           # noqa: BLE001
        log.warning("pdfplumber failed: %s: %s",
                    type(exc).__name__, str(exc)[:120])

    return ""


def as_html_document(text: str) -> str:
    """Wrap extracted PDF text so the HTML-shaped callers keep working.

    Every caller in article_fetcher does `extract_body(html)`. Returning
    bare text there would leave extract_body's BeautifulSoup path to guess,
    and _find_main_content's density heuristic has nothing to work with on
    a bare string. Wrapping is the smallest change that keeps one contract:
    whatever fetch_html returns is HTML, and extract_body reads it.

    The text is escaped, so a cartello that happens to contain "<" cannot
    turn into markup, and each line becomes its own <p> so extract_body's
    newline join reproduces the PDF's line breaks.

    No <title> is emitted on purpose. extract_title() then returns "" and
    enrich_candidate falls back to the news headline, which is a real title
    someone wrote — better than the first line of a cartello, which is
    usually a ministry letterhead.
    """
    from html import escape
    if not text:
        return ""
    paras = "\n".join(f"<p>{escape(line)}</p>"
                      for line in text.split("\n") if line.strip())
    return ("<html><body><div class=\"pdf-text\">"
            + paras + "</div></body></html>")
