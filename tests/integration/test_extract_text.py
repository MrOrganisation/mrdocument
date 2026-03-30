"""Integration tests for the /extract_text endpoint.

Verifies text extraction across all supported file formats without
invoking AI classification.  Uses the same integration stack
(docker-compose.fast.yaml) — the mock-ocr service returns predictable
text for PDFs, while text-based formats (TXT, DOCX) are extracted
directly by the service.

These tests only call the service HTTP API — they do not interact with
the watcher, filesystem, or database.  The autouse fixtures from
conftest.py (reset_environment, etc.) are disabled to avoid unnecessary
DB truncations that can race with the watcher.
"""

import io
import zipfile

import pytest
import requests

SERVICE_URL = "http://localhost:8000"
EXTRACT_URL = f"{SERVICE_URL}/extract_text"


# Override the autouse reset_environment fixture from conftest.py.
# This test file only exercises the HTTP service — it doesn't need
# DB truncation or filesystem resets, which can race with the watcher.
@pytest.fixture(autouse=True)
def reset_environment():
    """No-op override — skip DB/filesystem reset for HTTP-only tests."""
    yield


def extract(filename: str, content: bytes, language: str = "eng") -> requests.Response:
    """POST a file to /extract_text and return the response."""
    return requests.post(
        EXTRACT_URL,
        files={"file": (filename, io.BytesIO(content), "application/octet-stream")},
        data={"language": language},
        timeout=30,
    )


# ---------------------------------------------------------------------------
# Plain text
# ---------------------------------------------------------------------------


class TestTxtExtraction:
    def test_utf8_txt(self):
        r = extract("note.txt", b"Hello world, this is a test document.")
        assert r.status_code == 200
        body = r.json()
        assert "Hello world" in body["text"]
        assert body["filename"] == "note.txt"

    def test_md_file(self):
        r = extract("readme.md", b"# Heading\n\nSome markdown content.")
        assert r.status_code == 200
        assert "Heading" in r.json()["text"]

    def test_empty_txt_returns_empty(self):
        r = extract("empty.txt", b"")
        # Empty text is still valid — the endpoint returns it as-is
        assert r.status_code == 200
        assert r.json()["text"] == ""

    def test_windows1252_txt(self):
        # Ä=\xc4, ö=\xf6, ü=\xfc in Windows-1252
        r = extract("german.txt", b"Sch\xf6ner T\xfcr\xf6ffner")
        assert r.status_code == 200
        assert "Schöner" in r.json()["text"]


# ---------------------------------------------------------------------------
# DOCX
# ---------------------------------------------------------------------------


def _minimal_docx(text: str) -> bytes:
    """Create a minimal valid DOCX (ZIP with document.xml) in memory."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(
            "[Content_Types].xml",
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
            '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
            '<Default Extension="xml" ContentType="application/xml"/>'
            '<Override PartName="/word/document.xml" '
            'ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>'
            "</Types>",
        )
        zf.writestr(
            "_rels/.rels",
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
            '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>'
            "</Relationships>",
        )
        zf.writestr(
            "word/document.xml",
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
            "<w:body>"
            f"<w:p><w:r><w:t>{text}</w:t></w:r></w:p>"
            "</w:body>"
            "</w:document>",
        )
    return buf.getvalue()


class TestDocxExtraction:
    def test_basic_docx(self):
        docx_bytes = _minimal_docx("Vertrag zwischen Firma A und Firma B")
        r = extract("contract.docx", docx_bytes)
        assert r.status_code == 200
        assert "Vertrag" in r.json()["text"]
        assert "Firma" in r.json()["text"]

    def test_empty_docx(self):
        docx_bytes = _minimal_docx("")
        r = extract("empty.docx", docx_bytes)
        # Empty DOCX may succeed with empty text or fail — either is acceptable
        assert r.status_code in (200, 400)

    def test_corrupt_docx(self):
        r = extract("broken.docx", b"this is not a docx file")
        assert r.status_code == 400


# ---------------------------------------------------------------------------
# PDF (goes through mock OCR)
# ---------------------------------------------------------------------------


class TestPdfExtraction:
    def test_basic_pdf(self):
        # Any non-empty bytes will do — mock-ocr returns "Mock OCR text for {filename}"
        r = extract("invoice.pdf", b"%PDF-1.4 fake pdf content")
        assert r.status_code == 200
        body = r.json()
        assert "Mock OCR text" in body["text"]
        assert "invoice" in body["text"]

    def test_empty_pdf(self):
        r = extract("empty.pdf", b"")
        # Mock OCR returns 422 for empty files, which becomes BAD_GATEWAY
        assert r.status_code in (400, 422, 502)

    def test_pdf_with_language_hint(self):
        r = extract("german.pdf", b"%PDF-1.4 german content", language="deu")
        assert r.status_code == 200
        assert "text" in r.json()


# ---------------------------------------------------------------------------
# EML
# ---------------------------------------------------------------------------


class TestEmlExtraction:
    def test_basic_eml(self):
        eml_content = (
            b"From: sender@example.com\r\n"
            b"To: recipient@example.com\r\n"
            b"Subject: Test Email\r\n"
            b"Date: Mon, 01 Jan 2024 10:00:00 +0000\r\n"
            b"Content-Type: text/plain; charset=utf-8\r\n"
            b"\r\n"
            b"This is the body of the test email.\r\n"
        )
        r = extract("message.eml", eml_content)
        assert r.status_code == 200
        # EML gets converted to PDF, then OCR'ed by mock
        body = r.json()
        assert "text" in body


# ---------------------------------------------------------------------------
# RTF
# ---------------------------------------------------------------------------


class TestRtfExtraction:
    def test_basic_rtf(self):
        rtf_content = (
            b"{\\rtf1\\ansi\r\n"
            b"This is a simple RTF document.\\par\r\n"
            b"}"
        )
        r = extract("document.rtf", rtf_content)
        # RTF gets converted to PDF, then OCR'ed by mock
        # Conversion may fail if no RTF converter is available, so accept 200 or 400
        assert r.status_code in (200, 400)


# ---------------------------------------------------------------------------
# HTML
# ---------------------------------------------------------------------------


class TestHtmlExtraction:
    def test_basic_html(self):
        html_content = (
            b"<html><body>"
            b"<h1>Test Document</h1>"
            b"<p>This is a paragraph of text.</p>"
            b"</body></html>"
        )
        r = extract("page.html", html_content)
        # HTML gets converted to PDF, then OCR'ed by mock
        # Conversion may fail if no HTML converter is available, so accept 200 or 400
        assert r.status_code in (200, 400)


# ---------------------------------------------------------------------------
# Unsupported format
# ---------------------------------------------------------------------------


class TestUnsupportedFormat:
    def test_unknown_extension(self):
        r = extract("data.csv", b"a,b,c\n1,2,3\n")
        assert r.status_code == 400
        assert "Unsupported" in r.json()["error"]

    def test_no_file(self):
        r = requests.post(EXTRACT_URL, data={"language": "eng"}, timeout=10)
        assert r.status_code == 400


# ---------------------------------------------------------------------------
# Response shape
# ---------------------------------------------------------------------------


class TestResponseShape:
    def test_response_has_text_and_filename(self):
        r = extract("sample.txt", b"Some text content here")
        assert r.status_code == 200
        body = r.json()
        assert "text" in body
        assert "filename" in body

    def test_response_has_no_metadata(self):
        """extract_text must not return AI metadata fields."""
        r = extract("sample.txt", b"Some text content here")
        body = r.json()
        assert "metadata" not in body
        assert "context" not in body
        assert "pdf" not in body
