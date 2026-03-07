"""Integration tests for MrDocument service with mock OCR and AI services."""

import base64
import io
import json
from unittest.mock import MagicMock, patch

import pikepdf
import pytest
from aiohttp import web

from mrdocument.server import MrDocumentServer


# Sample PDF bytes (minimal valid PDF)
SAMPLE_PDF = b"""%PDF-1.4
1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj
2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj
3 0 obj<</Type/Page/MediaBox[0 0 612 792]/Parent 2 0 R>>endobj
xref
0 4
0000000000 65535 f 
0000000009 00000 n 
0000000052 00000 n 
0000000101 00000 n 
trailer<</Size 4/Root 1 0 R>>
startxref
172
%%EOF"""

SAMPLE_TEXT = """ACME Corporation
123 Main Street
Anytown, USA 12345

Date: January 15, 2024

Invoice #12345

Dear Customer,

This is your invoice for services rendered.

Total: $500.00

Thank you for your business.
"""

# Sample EML file (minimal valid email)
SAMPLE_EML = b"""From: sender@example.com
To: recipient@example.com
Subject: Invoice #12345 from ACME Corporation
Date: Mon, 15 Jan 2024 10:30:00 +0000
MIME-Version: 1.0
Content-Type: text/plain; charset="utf-8"

ACME Corporation
123 Main Street
Anytown, USA 12345

Date: January 15, 2024

Invoice #12345

Dear Customer,

This is your invoice for services rendered.

Total: $500.00

Thank you for your business.
"""


class MockOcrServer:
    """Mock OCR server for testing."""

    def __init__(self):
        self.app = web.Application()
        self.app.router.add_get("/health", self.health)
        self.app.router.add_post("/ocr", self.ocr)

    async def health(self, request):
        return web.json_response({"status": "healthy", "service": "ocrmypdf"})

    async def ocr(self, request):
        reader = await request.multipart()
        pdf_bytes = None
        filename = "document.pdf"

        async for field in reader:
            if field.name == "file":
                filename = field.filename or "document.pdf"
                pdf_bytes = await field.read()

        if pdf_bytes is None:
            return web.json_response({"error": "No file provided"}, status=400)

        # Return mock OCR result
        return web.json_response(
            {
                "pdf": base64.b64encode(pdf_bytes).decode("utf-8"),
                "text": SAMPLE_TEXT,
                "filename": f"ocr_{filename}",
            }
        )


@pytest.fixture
async def mock_ocr_server(aiohttp_server):
    """Create a mock OCR server."""
    mock = MockOcrServer()
    return await aiohttp_server(mock.app)


@pytest.fixture
def mock_anthropic():
    """Mock the Anthropic client."""
    with patch("mrdocument.ai.anthropic.Anthropic") as mock_class:
        mock_client = MagicMock()
        mock_class.return_value = mock_client

        # Mock the messages.create response
        mock_response = MagicMock()
        mock_response.content = [
            MagicMock(
                text=json.dumps(
                    {
                        "type": "Invoice",
                        "date": "2024-01-15",
                        "sender": "ACME Corporation",
                        "topic": "Office Supplies 2024",
                        "subject": "Order 12345",
                        "keywords": ["$500", "services", "payment"],
                    }
                )
            )
        ]
        mock_client.messages.create.return_value = mock_response

        yield mock_client


@pytest.fixture
async def mrdocument_client(aiohttp_client, mock_ocr_server, mock_anthropic):
    """Create a MrDocument test client."""
    ocr_url = f"http://{mock_ocr_server.host}:{mock_ocr_server.port}"

    server = MrDocumentServer(
        ocr_url=ocr_url,
        anthropic_api_key="test-api-key",
    )
    app = server.create_app()
    return await aiohttp_client(app)


class TestHealthEndpoint:
    """Tests for the /health endpoint."""

    async def test_health_returns_healthy(self, mrdocument_client):
        """Health endpoint returns healthy status when OCR is available."""
        response = await mrdocument_client.get("/health")

        assert response.status == 200
        data = await response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "mrdocument"
        assert data["ocr_service"] == "healthy"


class TestProcessEndpoint:
    """Tests for the /process endpoint."""

    def _make_mock_context_response(self, context_name: str):
        """Create mock tool_use response for context determination."""
        block = MagicMock()
        block.type = "tool_use"
        block.name = "classify_context"
        block.input = {"context": context_name}
        return MagicMock(content=[block])

    def _make_mock_metadata_response(self, metadata: dict):
        """Create mock tool_use response for metadata extraction."""
        block = MagicMock()
        block.type = "tool_use"
        block.name = "extract_metadata"
        block.input = metadata
        return MagicMock(content=[block])

    def _default_contexts(self):
        """Return default contexts for tests."""
        return [
            {
                "name": "work",
                "description": "Work-related documents",
                "filename": "{type}-{date}-{sender}",
                "fields": {
                    "type": {"instructions": "Identify the document type."},
                    "sender": {"instructions": "Identify the sender."},
                    "topic": {"instructions": "Identify the topic."},
                    "subject": {"instructions": "Provide a brief subject."},
                    "keywords": {"instructions": "Extract keywords."},
                },
            },
            {
                "name": "private",
                "description": "Personal documents",
                "filename": "{type}-{date}",
                "fields": {
                    "type": {"instructions": "Identify the document type."},
                    "sender": {"instructions": "Identify the sender."},
                    "topic": {"instructions": "Identify the topic."},
                    "subject": {"instructions": "Provide a brief subject."},
                    "keywords": {"instructions": "Extract keywords."},
                },
            },
        ]

    async def test_process_pdf_success(self, mrdocument_client, mock_anthropic):
        """Successfully process a PDF document with contexts."""
        from aiohttp import FormData

        mock_anthropic.messages.create.side_effect = [
            self._make_mock_context_response("work"),
            self._make_mock_metadata_response({
                "type": "Invoice",
                "date": "2024-01-15",
                "sender": "ACME Corporation",
                "topic": "Office Supplies 2024",
                "subject": "Order 12345",
                "keywords": ["$500", "services", "payment"],
            }),
        ]

        form = FormData()
        form.add_field("file", SAMPLE_PDF, filename="test.pdf", content_type="application/pdf")
        form.add_field("contexts", json.dumps(self._default_contexts()))

        response = await mrdocument_client.post("/process", data=form)

        assert response.status == 200
        data = await response.json()

        assert "filename" in data
        assert "pdf" in data
        assert "metadata" in data

        # Check metadata
        assert data["metadata"]["type"] == "Invoice"
        assert data["metadata"]["date"] == "2024-01-15"
        assert data["metadata"]["sender"] == "ACME Corporation"
        assert data["metadata"]["topic"] == "Office Supplies 2024"
        assert data["metadata"]["context"] == "work"

        # Verify PDF is valid and has embedded metadata
        pdf_bytes = base64.b64decode(data["pdf"])
        with pikepdf.open(io.BytesIO(pdf_bytes)) as pdf:
            assert pdf.docinfo.get("/Creator") == "MrDocument"
            assert pdf.docinfo.get("/Author") == "ACME Corporation"

    async def test_process_missing_file(self, mrdocument_client):
        """Return error when no file is provided."""
        response = await mrdocument_client.post("/process", data=b"")

        assert response.status == 400
        data = await response.json()
        assert "error" in data
        assert "multipart" in data["error"].lower()

    async def test_process_missing_contexts(self, mrdocument_client):
        """Return error when contexts is not provided."""
        from aiohttp import FormData

        form = FormData()
        form.add_field("file", SAMPLE_PDF, filename="test.pdf", content_type="application/pdf")

        response = await mrdocument_client.post("/process", data=form)

        assert response.status == 400
        data = await response.json()
        assert "error" in data
        assert "contexts" in data["error"].lower()

    async def test_process_non_pdf(self, mrdocument_client):
        """Return error when file is not a supported type."""
        from aiohttp import FormData

        form = FormData()
        form.add_field("file", b"not a pdf", filename="test.txt", content_type="text/plain")
        form.add_field("contexts", json.dumps(self._default_contexts()))

        response = await mrdocument_client.post("/process", data=form)

        assert response.status == 400
        data = await response.json()
        assert "error" in data

    async def test_process_with_language(self, mrdocument_client, mock_anthropic):
        """Process with custom language parameter."""
        from aiohttp import FormData

        mock_anthropic.messages.create.side_effect = [
            self._make_mock_context_response("work"),
            self._make_mock_metadata_response({
                "type": "Invoice",
                "date": "2024-01-15",
                "sender": "Test",
                "topic": "Test",
                "subject": "Test",
                "keywords": [],
            }),
        ]

        form = FormData()
        form.add_field("file", SAMPLE_PDF, filename="test.pdf", content_type="application/pdf")
        form.add_field("language", "deu")
        form.add_field("contexts", json.dumps(self._default_contexts()))

        response = await mrdocument_client.post("/process", data=form)

        assert response.status == 200

    async def test_process_with_field_config(self, mrdocument_client, mock_anthropic):
        """Process with field configuration in context."""
        from aiohttp import FormData

        mock_anthropic.messages.create.side_effect = [
            self._make_mock_context_response("work"),
            self._make_mock_metadata_response({
                "type": "Invoice",
                "date": "2024-01-15",
                "sender": "ACME Corporation",
                "topic": "Project Alpha",
                "subject": "Order",
                "keywords": ["payment"],
            }),
        ]

        contexts = [
            {
                "name": "work",
                "description": "Work documents",
                "filename": "{type}-{date}-{sender}",
                "fields": {
                    "type": {
                        "instructions": "Identify the document type.",
                        "candidates": ["Invoice", "Receipt", "Contract"],
                        "allow_new_candidates": False,
                    },
                    "sender": {
                        "instructions": "Identify the sender.",
                        "candidates": ["ACME Corporation", "Globex Inc"],
                    },
                    "topic": {"instructions": "Identify the topic."},
                    "subject": {"instructions": "Provide a brief subject."},
                    "keywords": {"instructions": "Extract keywords."},
                },
            },
            {
                "name": "private",
                "description": "Personal documents",
                "filename": "{type}-{date}",
                "fields": {
                    "type": {"instructions": "Identify the document type."},
                    "sender": {"instructions": "Identify the sender."},
                    "topic": {"instructions": "Identify the topic."},
                    "subject": {"instructions": "Provide a brief subject."},
                    "keywords": {"instructions": "Extract keywords."},
                },
            },
        ]

        form = FormData()
        form.add_field("file", SAMPLE_PDF, filename="test.pdf", content_type="application/pdf")
        form.add_field("contexts", json.dumps(contexts))

        response = await mrdocument_client.post("/process", data=form)

        assert response.status == 200
        data = await response.json()
        assert "filename" in data
        assert "metadata" in data
        assert data["metadata"]["context"] == "work"

    async def test_process_with_invalid_contexts_json(self, mrdocument_client):
        """Return error when contexts field contains invalid JSON."""
        from aiohttp import FormData

        form = FormData()
        form.add_field("file", SAMPLE_PDF, filename="test.pdf", content_type="application/pdf")
        form.add_field("contexts", "not valid json")

        response = await mrdocument_client.post("/process", data=form)

        assert response.status == 400
        data = await response.json()
        assert "error" in data
        assert "contexts" in data["error"].lower() or "json" in data["error"].lower()

    async def test_process_with_empty_contexts(self, mrdocument_client):
        """Return error when contexts is empty."""
        from aiohttp import FormData

        form = FormData()
        form.add_field("file", SAMPLE_PDF, filename="test.pdf", content_type="application/pdf")
        form.add_field("contexts", json.dumps([]))

        response = await mrdocument_client.post("/process", data=form)

        assert response.status == 400
        data = await response.json()
        assert "error" in data
        assert "contexts" in data["error"].lower()

    async def test_process_with_name_short_mapping(self, mrdocument_client, mock_anthropic):
        """Process with name/short mapping returns short values."""
        from aiohttp import FormData

        mock_anthropic.messages.create.side_effect = [
            self._make_mock_context_response("work"),
            self._make_mock_metadata_response({
                "type": "Invoice (sent to client)",
                "date": "2024-01-15",
                "sender": "Test Corp",
                "topic": "Project",
                "subject": "Order",
                "keywords": [],
            }),
        ]

        contexts = [
            {
                "name": "work",
                "description": "Work documents",
                "filename": "{type}-{date}",
                "fields": {
                    "type": {
                        "instructions": "Identify the document type.",
                        "candidates": [
                            {"name": "Invoice (sent to client)", "short": "Invoice"},
                            {"name": "Receipt (payment)", "short": "Receipt"},
                        ],
                        "allow_new_candidates": False,
                    },
                    "sender": {"instructions": "Identify the sender."},
                    "topic": {"instructions": "Identify the topic."},
                    "subject": {"instructions": "Provide a brief subject."},
                    "keywords": {"instructions": "Extract keywords."},
                },
            },
        ]

        form = FormData()
        form.add_field("file", SAMPLE_PDF, filename="test.pdf", content_type="application/pdf")
        form.add_field("contexts", json.dumps(contexts))

        response = await mrdocument_client.post("/process", data=form)

        assert response.status == 200
        data = await response.json()
        assert data["metadata"]["type"] == "Invoice"

    async def test_process_with_custom_instructions(self, mrdocument_client, mock_anthropic):
        """Process with custom field instructions."""
        from aiohttp import FormData

        mock_anthropic.messages.create.side_effect = [
            self._make_mock_context_response("work"),
            self._make_mock_metadata_response({
                "type": "Invoice",
                "date": "2024-01-15",
                "sender": "Test",
                "topic": "PROJ-2024-001",
                "subject": "Order",
                "keywords": [],
            }),
        ]

        contexts = [
            {
                "name": "work",
                "description": "Work documents",
                "filename": "{type}-{date}-{topic}",
                "fields": {
                    "type": {"instructions": "Identify the document type."},
                    "sender": {"instructions": "Identify the sender."},
                    "topic": {"instructions": "Use the project code format PROJ-YYYY-NNN"},
                    "subject": {"instructions": "Provide a brief subject."},
                    "keywords": {"instructions": "Extract keywords."},
                },
            },
        ]

        form = FormData()
        form.add_field("file", SAMPLE_PDF, filename="test.pdf", content_type="application/pdf")
        form.add_field("contexts", json.dumps(contexts))

        response = await mrdocument_client.post("/process", data=form)

        assert response.status == 200
        data = await response.json()
        assert "topic" in data["metadata"]

    async def test_process_eml_success(self, mrdocument_client, mock_anthropic):
        """Successfully process an EML file (converts to PDF first)."""
        from aiohttp import FormData

        mock_anthropic.messages.create.side_effect = [
            self._make_mock_context_response("work"),
            self._make_mock_metadata_response({
                "type": "Email",
                "date": "2024-01-15",
                "sender": "sender@example.com",
                "topic": "Test",
                "subject": "Test Subject",
                "keywords": [],
            }),
        ]

        form = FormData()
        form.add_field("file", SAMPLE_EML, filename="test.eml", content_type="message/rfc822")
        form.add_field("contexts", json.dumps(self._default_contexts()))

        response = await mrdocument_client.post("/process", data=form)

        assert response.status == 200
        data = await response.json()

        assert "filename" in data
        assert "pdf" in data
        assert "metadata" in data

        # Verify returned file is a valid PDF
        pdf_bytes = base64.b64decode(data["pdf"])
        with pikepdf.open(io.BytesIO(pdf_bytes)) as pdf:
            assert pdf.docinfo.get("/Creator") == "MrDocument"

    async def test_process_eml_with_language(self, mrdocument_client, mock_anthropic):
        """Process EML with custom language parameter."""
        from aiohttp import FormData

        mock_anthropic.messages.create.side_effect = [
            self._make_mock_context_response("work"),
            self._make_mock_metadata_response({
                "type": "Email",
                "date": "2024-01-15",
                "sender": "sender@example.com",
                "topic": "Test",
                "subject": "Test",
                "keywords": [],
            }),
        ]

        form = FormData()
        form.add_field("file", SAMPLE_EML, filename="test.eml", content_type="message/rfc822")
        form.add_field("language", "deu")
        form.add_field("contexts", json.dumps(self._default_contexts()))

        response = await mrdocument_client.post("/process", data=form)

        assert response.status == 200


class TestMetadataFilename:
    """Tests for filename generation from metadata."""

    def test_full_metadata(self):
        """Generate filename with all metadata fields."""
        from datetime import date

        from mrdocument.ai import DocumentMetadata

        metadata = DocumentMetadata(
            fields={
                "type": "Invoice",
                "sender": "ACME Corp",
                "topic": "Office Supplies",
                "subject": "Order",
                "keywords": ["payment", "net30"],
            },
            date=date(2024, 1, 15),
        )
        filename = metadata.to_filename("{type}-{date}-{sender}-{topic}-{subject}")
        assert filename == "invoice-2024-01-15-acme_corp-office_supplies-order.pdf"

    def test_partial_metadata(self):
        """Generate filename with partial metadata."""
        from datetime import date

        from mrdocument.ai import DocumentMetadata

        metadata = DocumentMetadata(
            fields={"subject": "Contract"},
            date=date(2024, 1, 15),
        )
        filename = metadata.to_filename("{date}-{subject}")
        assert filename == "2024-01-15-contract.pdf"

    def test_no_metadata(self):
        """Generate fallback filename when no metadata."""
        from mrdocument.ai import DocumentMetadata

        metadata = DocumentMetadata(fields={}, date=None)
        filename = metadata.to_filename("{type}-{date}")
        assert filename == "document.pdf"

    def test_sanitize_special_chars(self):
        """Special characters in metadata are sanitized."""
        from datetime import date

        from mrdocument.ai import DocumentMetadata

        metadata = DocumentMetadata(
            fields={
                "type": "Invoice",
                "sender": "Company/Inc.",
                "subject": "Invoice: #123",
            },
            date=date(2024, 1, 15),
        )
        filename = metadata.to_filename("{type}-{date}-{sender}-{subject}")
        assert "/" not in filename
        assert ":" not in filename

    def test_sanitize_umlauts(self):
        """Umlauts are transliterated."""
        from datetime import date

        from mrdocument.ai import DocumentMetadata

        metadata = DocumentMetadata(
            fields={
                "type": "Rechnung",
                "sender": "Müller GmbH",
                "subject": "Büroausstattung",
            },
            date=date(2024, 1, 15),
        )
        filename = metadata.to_filename("{type}-{date}-{sender}-{subject}")
        assert "ü" not in filename
        assert "mueller" in filename or "muller" in filename
        assert "bueroausstattung" in filename or "buroausstattung" in filename

    def test_sanitize_hyphens_in_fields(self):
        """Hyphens in fields (except date) are replaced with underscores."""
        from datetime import date

        from mrdocument.ai import DocumentMetadata

        metadata = DocumentMetadata(
            fields={
                "type": "E-Mail",
                "sender": "ABC-Corp",
                "subject": "Re: Follow-up",
            },
            date=date(2024, 1, 15),
        )
        filename = metadata.to_filename("{type}-{date}-{sender}-{subject}")
        # Date should keep hyphens, others should not
        assert "2024-01-15" in filename
        assert "e_mail" in filename
        assert "abc_corp" in filename

    def test_sanitize_whitespace(self):
        """Whitespace is replaced with underscores."""
        from datetime import date

        from mrdocument.ai import DocumentMetadata

        metadata = DocumentMetadata(
            fields={
                "type": "Bank Statement",
                "sender": "Big Bank Inc",
                "subject": "Monthly Statement",
            },
            date=date(2024, 1, 15),
        )
        filename = metadata.to_filename("{type}-{date}-{sender}-{subject}")
        assert " " not in filename
        assert "bank_statement" in filename

    def test_with_topic(self):
        """Generate filename with topic field."""
        from datetime import date

        from mrdocument.ai import DocumentMetadata

        metadata = DocumentMetadata(
            fields={
                "type": "Letter",
                "sender": "Bank",
                "topic": "House Purchase Elm Street",
                "subject": "Mortgage Approval",
                "keywords": ["mortgage", "approval"],
            },
            date=date(2024, 1, 15),
        )
        filename = metadata.to_filename("{type}-{date}-{sender}-{topic}-{subject}")
        assert filename == "letter-2024-01-15-bank-house_purchase_elm_street-mortgage_approval.pdf"


class TestOcrClient:
    """Tests for the OCR client."""

    async def test_ocr_client_success(self, mock_ocr_server):
        """OCR client successfully processes a PDF."""
        from mrdocument.ocr import OcrClient

        client = OcrClient(f"http://{mock_ocr_server.host}:{mock_ocr_server.port}")

        result = await client.process_pdf(SAMPLE_PDF, "test.pdf")

        assert result.pdf_bytes == SAMPLE_PDF
        assert result.text == SAMPLE_TEXT
        assert "test.pdf" in result.filename

    async def test_ocr_health_check(self, mock_ocr_server):
        """OCR client health check works."""
        from mrdocument.ocr import OcrClient

        client = OcrClient(f"http://{mock_ocr_server.host}:{mock_ocr_server.port}")

        assert await client.health_check() is True

    async def test_ocr_health_check_failure(self):
        """OCR client health check returns False when service unavailable."""
        from mrdocument.ocr import OcrClient

        client = OcrClient("http://localhost:59999")  # Non-existent service

        assert await client.health_check() is False


class TestAiClient:
    """Tests for the AI client."""

    # =========================================================================
    # Helper method tests
    # =========================================================================

    def test_get_candidate_names_with_strings(self, mock_anthropic):
        """_get_candidate_names extracts names from string candidates."""
        from mrdocument.ai import AiClient

        client = AiClient("test-key")
        candidates = ["Invoice", "Receipt", "Contract"]

        names = client._get_candidate_names(candidates)

        assert names == ["Invoice", "Receipt", "Contract"]

    def test_get_candidate_names_with_name_short_objects(self, mock_anthropic):
        """_get_candidate_names extracts 'name' from {name, short} objects."""
        from mrdocument.ai import AiClient

        client = AiClient("test-key")
        candidates = [
            {"name": "Invoice (sent)", "short": "Invoice"},
            {"name": "Receipt (received)", "short": "Receipt"},
        ]

        names = client._get_candidate_names(candidates)

        assert names == ["Invoice (sent)", "Receipt (received)"]

    def test_get_candidate_names_with_mixed_candidates(self, mock_anthropic):
        """_get_candidate_names handles mixed string and object candidates."""
        from mrdocument.ai import AiClient

        client = AiClient("test-key")
        candidates = [
            "SimpleString",
            {"name": "Object Name", "short": "Short"},
            "AnotherString",
        ]

        names = client._get_candidate_names(candidates)

        assert names == ["SimpleString", "Object Name", "AnotherString"]

    def test_get_candidate_names_handles_name_only_dicts(self, mock_anthropic):
        """_get_candidate_names handles dicts with only 'name' (no 'short')."""
        from mrdocument.ai import AiClient

        client = AiClient("test-key")
        candidates = [
            {"name": "Has Name"},  # Missing "short" - should still work
            {"no_name_key": "Invalid"},  # Missing "name" - should be skipped
        ]

        names = client._get_candidate_names(candidates)

        assert names == ["Has Name"]

    def test_build_name_to_short_mapping_with_objects(self, mock_anthropic):
        """_build_name_to_short_mapping creates mapping from {name, short} objects."""
        from mrdocument.ai import AiClient

        client = AiClient("test-key")
        candidates = [
            {"name": "Invoice (sent)", "short": "Invoice"},
            {"name": "Receipt (received)", "short": "Receipt"},
        ]

        mapping = client._build_name_to_short_mapping(candidates)

        assert mapping == {
            "Invoice (sent)": "Invoice",
            "Receipt (received)": "Receipt",
        }

    def test_build_name_to_short_mapping_with_strings(self, mock_anthropic):
        """_build_name_to_short_mapping returns empty for string-only candidates."""
        from mrdocument.ai import AiClient

        client = AiClient("test-key")
        candidates = ["Invoice", "Receipt", "Contract"]

        mapping = client._build_name_to_short_mapping(candidates)

        assert mapping == {}  # Strings don't need mapping

    def test_build_name_to_short_mapping_handles_name_only_dicts(self, mock_anthropic):
        """_build_name_to_short_mapping handles dicts with only 'name' (no 'short')."""
        from mrdocument.ai import AiClient

        client = AiClient("test-key")
        candidates = [
            "SimpleString",
            {"name": "Full Name", "short": "Short"},
            {"name": "Name Only"},  # Missing "short" - should not cause error
        ]

        mapping = client._build_name_to_short_mapping(candidates)

        assert mapping == {"Full Name": "Short"}
        assert "Name Only" not in mapping

    def test_apply_name_to_short_mapping(self, mock_anthropic):
        """_apply_name_to_short_mapping applies mapping to value."""
        from mrdocument.ai import AiClient

        client = AiClient("test-key")
        field_config = {
            "candidates": [
                {"name": "Invoice (sent)", "short": "Invoice"},
                {"name": "Receipt (received)", "short": "Receipt"},
            ]
        }

        result = client._apply_name_to_short_mapping("Invoice (sent)", field_config)

        assert result == "Invoice"

    def test_apply_name_to_short_mapping_no_match(self, mock_anthropic):
        """_apply_name_to_short_mapping returns original when no mapping match."""
        from mrdocument.ai import AiClient

        client = AiClient("test-key")
        field_config = {
            "candidates": [
                {"name": "Invoice (sent)", "short": "Invoice"},
            ]
        }

        result = client._apply_name_to_short_mapping("Other Value", field_config)

        assert result == "Other Value"

    def test_apply_name_to_short_mapping_none_value(self, mock_anthropic):
        """_apply_name_to_short_mapping returns None for None input."""
        from mrdocument.ai import AiClient

        client = AiClient("test-key")
        field_config = {"candidates": ["Invoice"]}

        result = client._apply_name_to_short_mapping(None, field_config)

        assert result is None

    # =========================================================================
    # Tool building tests
    # =========================================================================

    def test_build_field_schema_with_enum(self, mock_anthropic):
        """_build_field_schema creates enum when allow_new_candidates=False."""
        from mrdocument.ai import AiClient

        client = AiClient("test-key")
        field_config = {
            "instructions": "Identify the document type.",
            "candidates": ["Invoice", "Receipt", "Contract"],
            "allow_new_candidates": False,
        }

        schema = client._build_field_schema("type", field_config)

        assert schema["type"] == "string"
        assert "enum" in schema
        assert set(schema["enum"]) == {"Invoice", "Receipt", "Contract"}

    def test_build_field_schema_without_enum(self, mock_anthropic):
        """_build_field_schema creates string type when allow_new_candidates=True."""
        from mrdocument.ai import AiClient

        client = AiClient("test-key")
        field_config = {
            "instructions": "Identify the document type.",
            "candidates": ["Invoice", "Receipt"],
            "allow_new_candidates": True,
        }

        schema = client._build_field_schema("type", field_config)

        assert schema["type"] == "string"
        assert "enum" not in schema

    def test_build_field_schema_default_allows_new(self, mock_anthropic):
        """_build_field_schema defaults to allow_new_candidates=True."""
        from mrdocument.ai import AiClient

        client = AiClient("test-key")
        field_config = {
            "instructions": "Identify the document type.",
            "candidates": ["Invoice", "Receipt"],
        }

        schema = client._build_field_schema("type", field_config)

        assert schema["type"] == "string"
        assert "enum" not in schema  # Default allows new candidates

    def test_build_field_schema_with_name_short_extracts_names(self, mock_anthropic):
        """_build_field_schema uses display names in enum."""
        from mrdocument.ai import AiClient

        client = AiClient("test-key")
        field_config = {
            "instructions": "Identify the document type.",
            "candidates": [
                {"name": "Invoice (sent)", "short": "Invoice"},
                {"name": "Bill (received)", "short": "Bill"},
            ],
            "allow_new_candidates": False,
        }

        schema = client._build_field_schema("type", field_config)

        assert "enum" in schema
        assert "Invoice (sent)" in schema["enum"]
        assert "Bill (received)" in schema["enum"]
        # Short values should NOT be in enum
        assert "Invoice" not in schema["enum"]
        assert "Bill" not in schema["enum"]

    def test_build_context_tool_creates_enum(self, mock_anthropic):
        """_build_context_tool creates tool with context enum."""
        from mrdocument.ai import AiClient

        client = AiClient("test-key")
        contexts = [
            {"name": "work", "description": "Work documents"},
            {"name": "private", "description": "Personal documents"},
        ]

        tool = client._build_context_tool(contexts)

        assert tool["name"] == "classify_context"
        schema = tool["input_schema"]["properties"]["context"]
        assert "enum" in schema
        assert set(schema["enum"]) == {"work", "private"}

    def test_build_extraction_tool_structure(self, mock_anthropic):
        """_build_extraction_tool creates correct tool structure."""
        from mrdocument.ai import AiClient

        client = AiClient("test-key")
        field_configs = {
            "type": {
                "instructions": "Identify the document type.",
                "candidates": ["Invoice", "Receipt"],
                "allow_new_candidates": False,
            },
            "sender": {
                "instructions": "Identify the sender.",
                "candidates": ["ACME Corp"],
            },
        }

        tool = client._build_extraction_tool(field_configs)

        assert tool["name"] == "extract_metadata"
        props = tool["input_schema"]["properties"]
        assert "type" in props
        assert "sender" in props
        assert "date" in props  # Always included automatically

    # =========================================================================
    # Parse tool result tests
    # =========================================================================

    def test_parse_tool_result_basic(self, mock_anthropic):
        """_parse_tool_result parses basic result correctly."""
        from mrdocument.ai import AiClient
        from datetime import date

        client = AiClient("test-key")
        result = {
            "type": "Invoice",
            "date": "2024-01-15",
            "sender": "Test Corp",
            "topic": "Project X",
            "subject": "Order 123",
            "keywords": ["urgent", "payment"],
        }
        field_configs = {
            "type": {"instructions": "Identify the document type."},
            "sender": {"instructions": "Identify the sender."},
            "topic": {"instructions": "Identify the topic."},
            "subject": {"instructions": "Provide a brief subject."},
            "keywords": {"instructions": "Extract keywords."},
        }

        metadata = client._parse_tool_result(result, field_configs, None, "work")

        assert metadata.fields.get("type") == "Invoice"
        assert metadata.date == date(2024, 1, 15)
        assert metadata.fields.get("sender") == "Test Corp"
        assert metadata.fields.get("topic") == "Project X"
        assert metadata.fields.get("subject") == "Order 123"
        assert metadata.fields.get("keywords") == ["urgent", "payment"]
        assert metadata.context == "work"

    def test_parse_tool_result_with_null_fields(self, mock_anthropic):
        """_parse_tool_result handles null fields correctly."""
        from mrdocument.ai import AiClient

        client = AiClient("test-key")
        result = {
            "type": None,
            "date": None,
            "sender": None,
            "topic": None,
            "subject": "Some Subject",
            "keywords": [],
        }
        field_configs = {
            "type": {"instructions": "Identify the document type."},
            "sender": {"instructions": "Identify the sender."},
            "topic": {"instructions": "Identify the topic."},
            "subject": {"instructions": "Provide a brief subject."},
            "keywords": {"instructions": "Extract keywords."},
        }

        metadata = client._parse_tool_result(result, field_configs, None, "work")

        assert metadata.fields.get("type") is None
        assert metadata.date is None
        assert metadata.fields.get("sender") is None
        assert metadata.fields.get("topic") is None
        assert metadata.fields.get("subject") == "Some Subject"
        assert metadata.fields.get("keywords") == []

    def test_parse_tool_result_applies_name_short_mapping(self, mock_anthropic):
        """_parse_tool_result applies name→short mapping."""
        from mrdocument.ai import AiClient

        client = AiClient("test-key")
        result = {
            "type": "Invoice (sent to client)",
            "date": "2024-01-15",
            "sender": "ACME Corp",
            "topic": "Project",
            "subject": "Order",
            "keywords": [],
        }
        field_configs = {
            "type": {
                "instructions": "Identify the document type.",
                "candidates": [
                    {"name": "Invoice (sent to client)", "short": "Invoice"},
                    {"name": "Receipt (payment)", "short": "Receipt"},
                ],
            },
            "sender": {"instructions": "Identify the sender."},
            "topic": {"instructions": "Identify the topic."},
            "subject": {"instructions": "Provide a brief subject."},
            "keywords": {"instructions": "Extract keywords."},
        }

        metadata = client._parse_tool_result(result, field_configs, None, "work")

        assert metadata.fields.get("type") == "Invoice"  # Mapped to short form

    def test_parse_tool_result_invalid_date(self, mock_anthropic):
        """_parse_tool_result handles invalid date format."""
        from mrdocument.ai import AiClient

        client = AiClient("test-key")
        result = {
            "type": "Invoice",
            "date": "not-a-date",
            "sender": "Test",
            "topic": None,
            "subject": None,
            "keywords": [],
        }
        field_configs = {
            "type": {"instructions": "Identify the document type."},
            "sender": {"instructions": "Identify the sender."},
            "topic": {"instructions": "Identify the topic."},
            "subject": {"instructions": "Provide a brief subject."},
            "keywords": {"instructions": "Extract keywords."},
        }

        metadata = client._parse_tool_result(result, field_configs, None, "work")

        assert metadata.date is None  # Invalid date becomes None

    # =========================================================================
    # Context determination tests
    # =========================================================================

    async def test_determine_context_returns_context(self, mock_anthropic):
        """determine_context returns the determined context name."""
        from mrdocument.ai import AiClient

        # Mock tool_use response
        mock_block = MagicMock()
        mock_block.type = "tool_use"
        mock_block.name = "classify_context"
        mock_block.input = {"context": "work"}
        mock_anthropic.messages.create.return_value.content = [mock_block]

        client = AiClient("test-key")
        contexts = [
            {"name": "work", "description": "Work documents"},
            {"name": "private", "description": "Personal documents"},
        ]

        result = await client.determine_context("Sample text", contexts)

        assert result == "work"

    async def test_determine_context_requires_contexts(self, mock_anthropic):
        """determine_context raises error when no contexts provided."""
        from mrdocument.ai import AiClient, ConfigurationError

        client = AiClient("test-key")

        with pytest.raises(ConfigurationError, match="No contexts provided"):
            await client.determine_context("Sample text", [])

    async def test_determine_context_requires_text(self, mock_anthropic):
        """determine_context raises error for empty text."""
        from mrdocument.ai import AiClient, AiError

        client = AiClient("test-key")
        contexts = [{"name": "work", "description": "Work"}]

        with pytest.raises(AiError, match="empty text"):
            await client.determine_context("", contexts)

    # =========================================================================
    # Full extraction tests
    # =========================================================================

    async def test_extract_metadata_two_pass(self, mock_anthropic):
        """extract_metadata performs two-pass extraction."""
        from mrdocument.ai import AiClient

        # First call: context determination (tool_use)
        context_block = MagicMock()
        context_block.type = "tool_use"
        context_block.name = "classify_context"
        context_block.input = {"context": "work"}

        # Second call: metadata extraction (tool_use)
        metadata_block = MagicMock()
        metadata_block.type = "tool_use"
        metadata_block.name = "extract_metadata"
        metadata_block.input = {
            "type": "Invoice",
            "date": "2024-01-15",
            "sender": "ACME Corp",
            "topic": "Project Alpha",
            "subject": "Order 123",
            "keywords": ["payment"],
        }

        mock_anthropic.messages.create.side_effect = [
            MagicMock(content=[context_block]),
            MagicMock(content=[metadata_block]),
        ]

        client = AiClient("test-key")
        contexts = [
            {
                "name": "work",
                "description": "Work documents",
                "filename": "{type}-{date}-{sender}",
                "fields": {
                    "type": {"instructions": "Identify the document type."},
                    "sender": {"instructions": "Identify the sender."},
                    "topic": {"instructions": "Identify the topic."},
                    "subject": {"instructions": "Provide a brief subject."},
                    "keywords": {"instructions": "Extract keywords."},
                },
            },
            {
                "name": "private",
                "description": "Personal documents",
                "filename": "{type}-{date}",
                "fields": {
                    "type": {"instructions": "Identify the document type."},
                    "sender": {"instructions": "Identify the sender."},
                },
            },
        ]

        metadata, filename_pattern = await client.extract_metadata("Sample text", contexts)

        assert metadata.context == "work"
        assert metadata.fields.get("type") == "Invoice"
        assert metadata.fields.get("sender") == "ACME Corp"
        assert mock_anthropic.messages.create.call_count == 2

    async def test_extract_metadata_with_field_config(self, mock_anthropic):
        """extract_metadata uses field configuration from context."""
        from mrdocument.ai import AiClient

        # Mock responses
        context_block = MagicMock()
        context_block.type = "tool_use"
        context_block.name = "classify_context"
        context_block.input = {"context": "work"}

        metadata_block = MagicMock()
        metadata_block.type = "tool_use"
        metadata_block.name = "extract_metadata"
        metadata_block.input = {
            "type": "Invoice (sent)",
            "date": "2024-01-15",
            "sender": "Test",
            "topic": "Topic",
            "subject": "Subject",
            "keywords": [],
        }

        mock_anthropic.messages.create.side_effect = [
            MagicMock(content=[context_block]),
            MagicMock(content=[metadata_block]),
        ]

        client = AiClient("test-key")
        contexts = [
            {
                "name": "work",
                "description": "Work documents",
                "filename": "{type}-{date}",
                "fields": {
                    "type": {
                        "instructions": "Identify the document type.",
                        "candidates": [
                            {"name": "Invoice (sent)", "short": "Invoice"},
                            {"name": "Bill (received)", "short": "Bill"},
                        ],
                        "allow_new_candidates": False,
                    },
                    "sender": {"instructions": "Identify the sender."},
                    "topic": {"instructions": "Identify the topic."},
                    "subject": {"instructions": "Provide a brief subject."},
                    "keywords": {"instructions": "Extract keywords."},
                },
            },
        ]

        metadata, filename_pattern = await client.extract_metadata("Sample text", contexts)

        assert metadata.fields.get("type") == "Invoice"  # Should be mapped to short form

    async def test_extract_metadata_empty_text(self, mock_anthropic):
        """extract_metadata returns empty metadata for empty text."""
        from mrdocument.ai import AiClient

        client = AiClient("test-key")
        contexts = [
            {
                "name": "work",
                "description": "Work",
                "filename": "{date}",
                "fields": {
                    "type": {"instructions": "Identify the document type."},
                },
            }
        ]

        metadata, filename_pattern = await client.extract_metadata("", contexts)

        assert metadata.fields == {}
        assert metadata.date is None

    # =========================================================================
    # Field description tests
    # =========================================================================

    def test_build_field_description_with_instructions(self, mock_anthropic):
        """_build_field_description includes custom instructions."""
        from mrdocument.ai import AiClient

        client = AiClient("test-key")
        field_config = {
            "instructions": "Use the project code format",
            "candidates": ["Project A", "Project B"],
        }

        description = client._build_field_description("topic", field_config)

        assert "project code format" in description

    def test_build_field_description_with_blacklist(self, mock_anthropic):
        """_build_field_description includes blacklist warning."""
        from mrdocument.ai import AiClient

        client = AiClient("test-key")
        field_config = {
            "instructions": "Identify the document type.",
            "blacklist": ["Unknown", "Other"],
        }

        description = client._build_field_description("type", field_config)

        assert "Unknown" in description
        assert "Other" in description

    def test_build_field_description_strict_mode(self, mock_anthropic):
        """_build_field_description indicates strict mode."""
        from mrdocument.ai import AiClient

        client = AiClient("test-key")
        field_config = {
            "instructions": "Identify the document type.",
            "candidates": ["Invoice", "Receipt"],
            "allow_new_candidates": False,
        }

        description = client._build_field_description("type", field_config)

        # Should indicate strict selection required
        assert "must" in description.lower() or "only" in description.lower() or "choose" in description.lower()


class TestUtilityFunctions:
    """Tests for utility parsing functions."""

    def test_parse_contexts_valid(self):
        """Parse valid contexts JSON."""
        from mrdocument.server import parse_contexts

        data = [
            {
                "name": "work",
                "description": "Work-related documents",
                "filename": "{type}-{date}",
                "fields": {
                    "type": {"instructions": "Identify the document type."},
                    "topic": {"instructions": "Use project code"},
                },
            },
            {
                "name": "private",
                "description": "Personal documents",
                "filename": "{date}",
                "fields": {
                    "type": {"instructions": "Identify the document type."},
                },
            }
        ]

        contexts = parse_contexts(data)

        assert contexts is not None
        assert len(contexts) == 2
        assert contexts[0]["name"] == "work"
        assert contexts[0]["description"] == "Work-related documents"
        assert contexts[1]["name"] == "private"

    def test_parse_contexts_with_field_config(self):
        """Parse contexts with field configurations."""
        from mrdocument.server import parse_contexts

        data = [
            {
                "name": "work",
                "description": "Work documents",
                "filename": "{type}-{date}-{sender}",
                "fields": {
                    "type": {
                        "instructions": "Identify the document type.",
                        "candidates": ["Invoice", "Receipt"],
                        "allow_new_candidates": False,
                    },
                    "sender": {
                        "instructions": "Identify the sender.",
                        "candidates": ["ACME Corp"],
                        "blacklist": ["Unknown"],
                    },
                },
            }
        ]

        contexts = parse_contexts(data)

        assert contexts is not None
        assert len(contexts) == 1
        assert contexts[0]["name"] == "work"
        # Field configs should be passed through
        assert "fields" in contexts[0]
        assert "type" in contexts[0]["fields"]
        assert "sender" in contexts[0]["fields"]

    def test_parse_contexts_missing_required_fields(self):
        """Parse contexts with missing required fields."""
        from mrdocument.server import parse_contexts

        data = [
            {"name": "work", "description": "Work"},  # Missing filename and fields
            {"name": "private", "filename": "{date}"},  # Missing description and fields
        ]

        contexts = parse_contexts(data)

        # Should return None since no valid contexts
        assert contexts is None

    def test_parse_contexts_none(self):
        """Parse None returns None."""
        from mrdocument.server import parse_contexts

        assert parse_contexts(None) is None

    def test_parse_contexts_invalid_type(self):
        """Parse invalid type returns None."""
        from mrdocument.server import parse_contexts

        assert parse_contexts("not a list") is None
        assert parse_contexts({"not": "a list"}) is None

    def test_parse_contexts_empty_list(self):
        """Parse empty list returns None."""
        from mrdocument.server import parse_contexts

        assert parse_contexts([]) is None


class TestPdfMetadataEmbedding:
    """Tests for PDF metadata embedding."""

    def test_embed_full_metadata(self):
        """Embed all metadata fields into PDF."""
        from datetime import date

        from mrdocument.pdf import embed_metadata

        result = embed_metadata(
            SAMPLE_PDF,
            doc_type="Invoice",
            doc_date=date(2024, 1, 15),
            sender="ACME Corporation",
            topic="Office Supplies 2024",
            subject="Order 12345",
            keywords=["payment", "net30", "services"],
        )

        # Verify the result is valid PDF
        with pikepdf.open(io.BytesIO(result)) as pdf:
            # Check Info dictionary
            assert pdf.docinfo.get("/Title") == "Order 12345"
            assert pdf.docinfo.get("/Author") == "ACME Corporation"
            assert pdf.docinfo.get("/Subject") == "Invoice"
            assert pdf.docinfo.get("/Topic") == "Office Supplies 2024"
            # Keywords should contain type, topic, sender, and keywords
            pdf_keywords = str(pdf.docinfo.get("/Keywords"))
            assert "Invoice" in pdf_keywords
            assert "Office Supplies 2024" in pdf_keywords
            assert "ACME Corporation" in pdf_keywords
            assert "payment" in pdf_keywords
            assert "net30" in pdf_keywords
            assert "services" in pdf_keywords
            assert "20240115" in str(pdf.docinfo.get("/CreationDate"))
            assert pdf.docinfo.get("/Creator") == "MrDocument"

            # Check XMP metadata
            with pdf.open_metadata() as meta:
                assert meta.get("dc:title") == "Order 12345"
                assert "ACME Corporation" in str(meta.get("dc:creator"))
                assert "Invoice" in str(meta.get("dc:subject"))
                assert meta.get("dc:description") == "Office Supplies 2024"

    def test_embed_partial_metadata(self):
        """Embed partial metadata into PDF."""
        from datetime import date

        from mrdocument.pdf import embed_metadata

        result = embed_metadata(
            SAMPLE_PDF,
            doc_type=None,
            doc_date=date(2024, 1, 15),
            sender="Test Sender",
            topic=None,
            subject=None,
            keywords=[],
        )

        with pikepdf.open(io.BytesIO(result)) as pdf:
            assert pdf.docinfo.get("/Author") == "Test Sender"
            assert pdf.docinfo.get("/Title") is None
            assert pdf.docinfo.get("/Subject") is None
            assert pdf.docinfo.get("/Topic") is None

    def test_embed_no_metadata(self):
        """Embed with no metadata still produces valid PDF."""
        from mrdocument.pdf import embed_metadata

        result = embed_metadata(
            SAMPLE_PDF,
            doc_type=None,
            doc_date=None,
            sender=None,
            topic=None,
            subject=None,
            keywords=[],
        )

        # Should still be a valid PDF
        with pikepdf.open(io.BytesIO(result)) as pdf:
            assert pdf.docinfo.get("/Creator") == "MrDocument"

    def test_embed_topic_only(self):
        """Embed topic metadata into PDF."""
        from mrdocument.pdf import embed_metadata

        result = embed_metadata(
            SAMPLE_PDF,
            doc_type=None,
            doc_date=None,
            sender=None,
            topic="House Purchase Elm Street",
            subject=None,
            keywords=[],
        )

        with pikepdf.open(io.BytesIO(result)) as pdf:
            assert pdf.docinfo.get("/Topic") == "House Purchase Elm Street"
            assert "House Purchase Elm Street" in str(pdf.docinfo.get("/Keywords"))

            with pdf.open_metadata() as meta:
                assert meta.get("dc:description") == "House Purchase Elm Street"

    def test_embed_keywords_only(self):
        """Embed keywords only into PDF."""
        from mrdocument.pdf import embed_metadata

        result = embed_metadata(
            SAMPLE_PDF,
            doc_type=None,
            doc_date=None,
            sender=None,
            topic=None,
            subject=None,
            keywords=["urgent", "review", "Q1"],
        )

        with pikepdf.open(io.BytesIO(result)) as pdf:
            pdf_keywords = str(pdf.docinfo.get("/Keywords"))
            assert "urgent" in pdf_keywords
            assert "review" in pdf_keywords
            assert "Q1" in pdf_keywords

    async def test_process_pdf_embeds_metadata(self, mrdocument_client, mock_anthropic):
        """Process endpoint embeds metadata into returned PDF."""
        from aiohttp import FormData

        # Mock tool_use responses
        context_block = MagicMock()
        context_block.type = "tool_use"
        context_block.name = "classify_context"
        context_block.input = {"context": "work"}

        metadata_block = MagicMock()
        metadata_block.type = "tool_use"
        metadata_block.name = "extract_metadata"
        metadata_block.input = {
            "type": "Invoice",
            "date": "2024-01-15",
            "sender": "ACME Corporation",
            "topic": "Office Supplies 2024",
            "subject": "Order 12345",
            "keywords": ["$500", "services", "payment"],
        }

        mock_anthropic.messages.create.side_effect = [
            MagicMock(content=[context_block]),
            MagicMock(content=[metadata_block]),
        ]

        contexts = [
            {
                "name": "work",
                "description": "Work documents",
                "filename": "{type}-{date}-{sender}",
                "fields": {
                    "type": {"instructions": "Identify the document type."},
                    "sender": {"instructions": "Identify the sender."},
                    "topic": {"instructions": "Identify the topic."},
                    "subject": {"instructions": "Provide a brief subject."},
                    "keywords": {"instructions": "Extract keywords."},
                },
            },
            {
                "name": "private",
                "description": "Personal documents",
                "filename": "{type}-{date}",
                "fields": {
                    "type": {"instructions": "Identify the document type."},
                    "sender": {"instructions": "Identify the sender."},
                },
            },
        ]

        form = FormData()
        form.add_field("file", SAMPLE_PDF, filename="test.pdf", content_type="application/pdf")
        form.add_field("contexts", json.dumps(contexts))

        response = await mrdocument_client.post("/process", data=form)

        assert response.status == 200
        data = await response.json()

        # Check keywords in response
        assert data["metadata"]["keywords"] == ["$500", "services", "payment"]

        # Decode the returned PDF and check metadata
        pdf_bytes = base64.b64decode(data["pdf"])
        with pikepdf.open(io.BytesIO(pdf_bytes)) as pdf:
            assert pdf.docinfo.get("/Title") == "Order 12345"
            assert pdf.docinfo.get("/Author") == "ACME Corporation"
            assert pdf.docinfo.get("/Subject") == "Invoice"
            assert pdf.docinfo.get("/Topic") == "Office Supplies 2024"
            assert pdf.docinfo.get("/Creator") == "MrDocument"
            # Keywords should contain type, topic, sender, and keywords
            pdf_keywords = str(pdf.docinfo.get("/Keywords"))
            assert "Invoice" in pdf_keywords
            assert "$500" in pdf_keywords
            assert "services" in pdf_keywords


class TestEmlConversion:
    """Tests for EML to PDF conversion."""

    def test_parse_simple_eml(self):
        """Parse a simple EML file."""
        from mrdocument.eml import parse_eml

        parsed = parse_eml(SAMPLE_EML)

        assert parsed.subject == "Invoice #12345 from ACME Corporation"
        assert parsed.sender == "sender@example.com"
        assert parsed.to == "recipient@example.com"
        assert "2024" in parsed.date
        assert "Invoice #12345" in parsed.body_text
        assert parsed.attachments == []

    def test_parse_html_email(self):
        """Parse an email with HTML body."""
        from mrdocument.eml import parse_eml

        html_eml = b"""From: sender@example.com
To: recipient@example.com
Subject: HTML Test
Date: Mon, 15 Jan 2024 10:30:00 +0000
MIME-Version: 1.0
Content-Type: text/html; charset="utf-8"

<html><body><h1>Hello World</h1><p>This is HTML content.</p></body></html>
"""
        parsed = parse_eml(html_eml)

        assert parsed.subject == "HTML Test"
        assert parsed.body_html is not None
        assert "<h1>Hello World</h1>" in parsed.body_html
        assert parsed.body_text is None

    def test_parse_multipart_email(self):
        """Parse a multipart email with both text and HTML."""
        from mrdocument.eml import parse_eml

        multipart_eml = b"""From: sender@example.com
To: recipient@example.com
Subject: Multipart Test
Date: Mon, 15 Jan 2024 10:30:00 +0000
MIME-Version: 1.0
Content-Type: multipart/alternative; boundary="boundary123"

--boundary123
Content-Type: text/plain; charset="utf-8"

Plain text version.
--boundary123
Content-Type: text/html; charset="utf-8"

<html><body><p>HTML version.</p></body></html>
--boundary123--
"""
        parsed = parse_eml(multipart_eml)

        assert parsed.subject == "Multipart Test"
        assert parsed.body_text == "Plain text version."
        assert "<p>HTML version.</p>" in parsed.body_html

    def test_parse_email_with_attachment(self):
        """Parse an email with attachments."""
        from mrdocument.eml import parse_eml

        attachment_eml = b"""From: sender@example.com
To: recipient@example.com
Subject: With Attachment
Date: Mon, 15 Jan 2024 10:30:00 +0000
MIME-Version: 1.0
Content-Type: multipart/mixed; boundary="boundary456"

--boundary456
Content-Type: text/plain; charset="utf-8"

See attached file.
--boundary456
Content-Type: application/pdf; name="document.pdf"
Content-Disposition: attachment; filename="document.pdf"
Content-Transfer-Encoding: base64

JVBERi0xLjQKMSAwIG9iajw8L1R5cGUvQ2F0YWxvZy9QYWdlcyAyIDAgUj4+ZW5kb2JqCg==
--boundary456--
"""
        parsed = parse_eml(attachment_eml)

        assert parsed.subject == "With Attachment"
        assert len(parsed.attachments) == 1
        assert parsed.attachments[0].filename == "document.pdf"
        assert parsed.attachments[0].content_type == "application/pdf"

    def test_eml_to_pdf_produces_valid_pdf(self):
        """Convert EML to PDF produces a valid PDF file."""
        from mrdocument.eml import eml_to_pdf

        pdf_bytes = eml_to_pdf(SAMPLE_EML)

        # Should be a valid PDF
        assert pdf_bytes.startswith(b"%PDF")
        with pikepdf.open(io.BytesIO(pdf_bytes)) as pdf:
            assert len(pdf.pages) >= 1

    def test_eml_to_pdf_contains_headers(self):
        """Converted PDF contains email headers."""
        from mrdocument.eml import eml_to_pdf

        pdf_bytes = eml_to_pdf(SAMPLE_EML)

        # The PDF should contain the email headers and body
        # We verify by checking the PDF is non-trivial size
        assert len(pdf_bytes) > 1000  # A reasonable PDF with content

    def test_eml_to_pdf_html_email(self):
        """Convert HTML email to PDF."""
        from mrdocument.eml import eml_to_pdf

        html_eml = b"""From: sender@example.com
To: recipient@example.com
Subject: HTML Email
Date: Mon, 15 Jan 2024 10:30:00 +0000
MIME-Version: 1.0
Content-Type: text/html; charset="utf-8"

<html><body><h1>Important Notice</h1><p>This is important content.</p></body></html>
"""
        pdf_bytes = eml_to_pdf(html_eml)

        assert pdf_bytes.startswith(b"%PDF")
        with pikepdf.open(io.BytesIO(pdf_bytes)) as pdf:
            assert len(pdf.pages) >= 1
