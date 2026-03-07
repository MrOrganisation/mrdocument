"""Tests for step3.py — Processor service calls with mock aiohttp server."""

import base64
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
import pytest_asyncio
from aiohttp import web

from models import State, PathEntry, Record
from step3 import Processor, SttConfig, _is_audio, _get_content_type


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts():
    return datetime(2025, 1, 1, tzinfo=timezone.utc)


def _make_record(**kwargs) -> Record:
    defaults = {
        "original_filename": "test.pdf",
        "source_hash": "abc123",
    }
    defaults.update(kwargs)
    return Record(**defaults)


# ---------------------------------------------------------------------------
# Mock mrdocument service
# ---------------------------------------------------------------------------

class MockService:
    """Fake mrdocument HTTP service for testing."""

    def __init__(self):
        self.calls: list[dict] = []
        self.response_metadata: dict = {
            "context": "work",
            "date": "2025-01-15",
            "assigned_filename": "2025-01-Invoice.pdf",
        }
        self.response_pdf_bytes: bytes = b"fake pdf output content"
        self.fail_status: int | None = None
        # Audio endpoint controls
        self.classify_audio_fail: bool = False
        self.classify_audio_keyterms: list[str] = ["keyword1", "keyword2"]
        self.classify_transcript_fail: bool = False
        self.classify_transcript_context: str = "work"
        self.classify_transcript_metadata: dict = {"context": "work", "date": "2025-03-15"}
        self.classify_transcript_keyterms: list[str] = ["richer_keyword"]
        self.classify_transcript_speakers: int | None = 2
        self.process_transcript_fail: bool = False

    async def handle_process(self, request: web.Request) -> web.Response:
        reader = await request.multipart()
        fields: dict = {}
        file_data: bytes | None = None

        while True:
            part = await reader.next()
            if part is None:
                break
            if part.name == "file":
                file_data = await part.read()
                fields["filename"] = part.filename
                fields["content_type"] = part.headers.get("Content-Type", "")
            else:
                fields[part.name] = await part.text()

        self.calls.append({"fields": fields, "file_data": file_data})

        if self.fail_status is not None:
            return web.Response(status=self.fail_status, text="Error")

        pdf_b64 = base64.b64encode(self.response_pdf_bytes).decode()
        return web.json_response({
            "metadata": self.response_metadata,
            "filename": self.response_metadata.get("assigned_filename"),
            "pdf": pdf_b64,
        })

    async def handle_classify_audio(self, request: web.Request) -> web.Response:
        data = await request.json()
        self.calls.append({"endpoint": "classify_audio", "data": data})
        if self.classify_audio_fail:
            return web.Response(status=500, text="Error")
        return web.json_response({
            "context": "work",
            "metadata": {"context": "work"},
            "transcription_keyterms": self.classify_audio_keyterms,
        })

    async def handle_classify_transcript(self, request: web.Request) -> web.Response:
        data = await request.json()
        self.calls.append({"endpoint": "classify_transcript", "data": data})
        if self.classify_transcript_fail:
            return web.Response(status=500, text="Error")
        return web.json_response({
            "context": self.classify_transcript_context,
            "metadata": self.classify_transcript_metadata,
            "transcription_keyterms": self.classify_transcript_keyterms,
            "number_of_speakers": self.classify_transcript_speakers,
        })

    async def handle_process_transcript(self, request: web.Request) -> web.Response:
        data = await request.json()
        self.calls.append({"endpoint": "process_transcript", "data": data})
        if self.process_transcript_fail:
            return web.Response(status=500, text="Error")
        pdf_b64 = base64.b64encode(self.response_pdf_bytes).decode()
        return web.json_response({
            "metadata": self.response_metadata,
            "filename": self.response_metadata.get("assigned_filename"),
            "pdf": pdf_b64,
        })

    def make_app(self) -> web.Application:
        app = web.Application()
        app.router.add_post("/process", self.handle_process)
        app.router.add_post("/classify_audio", self.handle_classify_audio)
        app.router.add_post("/classify_transcript", self.handle_classify_transcript)
        app.router.add_post("/process_transcript", self.handle_process_transcript)
        return app


# ---------------------------------------------------------------------------
# Mock STT service
# ---------------------------------------------------------------------------

class MockSTTService:
    """Fake STT HTTP service for testing."""

    def __init__(self):
        self.calls: list[dict] = []
        self.transcript: dict = {
            "segments": [
                {"text": "mock transcript text", "start": 0.0, "end": 5.0, "speaker": "SPEAKER_00"},
            ],
        }
        self.fail_status: int | None = None

    async def handle_transcribe(self, request: web.Request) -> web.Response:
        reader = await request.multipart()
        fields: dict = {}
        file_data: bytes | None = None

        while True:
            part = await reader.next()
            if part is None:
                break
            if part.name == "file":
                file_data = await part.read()
                fields["filename"] = part.filename
            else:
                fields[part.name] = await part.text()

        self.calls.append({"fields": fields, "file_data": file_data})

        if self.fail_status is not None:
            return web.Response(status=self.fail_status, text="STT Error")

        return web.json_response({"transcript": self.transcript})

    def make_app(self) -> web.Application:
        app = web.Application()
        app.router.add_post("/transcribe", self.handle_transcribe)
        return app


@pytest_asyncio.fixture
async def mock_service():
    """Start a mock mrdocument service, yield it, then cleanup."""
    service = MockService()
    runner = web.AppRunner(service.make_app())
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    service.url = f"http://127.0.0.1:{port}"
    yield service
    await runner.cleanup()


@pytest_asyncio.fixture
async def mock_stt():
    """Start a mock STT service, yield it, then cleanup."""
    service = MockSTTService()
    runner = web.AppRunner(service.make_app())
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    service.url = f"http://127.0.0.1:{port}"
    yield service
    await runner.cleanup()


def _setup_source(root: Path, rel_path: str, content: bytes = b"source pdf") -> None:
    """Create a source file at root/rel_path."""
    full = root / rel_path
    full.parent.mkdir(parents=True, exist_ok=True)
    full.write_bytes(content)


# ---------------------------------------------------------------------------
# Success path
# ---------------------------------------------------------------------------

class TestProcessorSuccess:
    @pytest.mark.asyncio
    async def test_success_writes_output_and_sidecar(self, tmp_path, mock_service):
        """Service returns metadata + base64 PDF → file written, sidecar written."""
        _setup_source(tmp_path, "archive/doc.pdf")
        record = _make_record(
            source_paths=[PathEntry("archive/doc.pdf", _ts())],
            output_filename="uuid-out",
            state=State.NEEDS_PROCESSING,
        )

        processor = Processor(tmp_path, mock_service.url)
        await processor.process_one(record)

        output = tmp_path / ".output" / "uuid-out"
        assert output.exists()
        assert output.read_bytes() == mock_service.response_pdf_bytes

        sidecar = tmp_path / ".output" / "uuid-out.meta.json"
        assert sidecar.exists()
        data = json.loads(sidecar.read_text())
        assert data["context"] == "work"
        assert data["metadata"]["date"] == "2025-01-15"
        assert data["assigned_filename"] == "2025-01-Invoice.pdf"

    @pytest.mark.asyncio
    async def test_source_read_correct_file(self, tmp_path, mock_service):
        """Processor reads correct source file from source_paths."""
        source_content = b"specific source content here"
        _setup_source(tmp_path, "archive/invoice.pdf", source_content)
        record = _make_record(
            source_paths=[PathEntry("archive/invoice.pdf", _ts())],
            output_filename="uuid-src",
            state=State.NEEDS_PROCESSING,
        )

        processor = Processor(tmp_path, mock_service.url)
        await processor.process_one(record)

        assert len(mock_service.calls) == 1
        assert mock_service.calls[0]["file_data"] == source_content
        assert mock_service.calls[0]["fields"]["filename"] == "invoice.pdf"

    @pytest.mark.asyncio
    async def test_sidecar_has_expected_keys(self, tmp_path, mock_service):
        """Sidecar JSON contains context, metadata, assigned_filename."""
        _setup_source(tmp_path, "archive/doc.pdf")
        record = _make_record(
            source_paths=[PathEntry("archive/doc.pdf", _ts())],
            output_filename="uuid-keys",
            state=State.NEEDS_PROCESSING,
        )

        processor = Processor(tmp_path, mock_service.url)
        await processor.process_one(record)

        sidecar = tmp_path / ".output" / "uuid-keys.meta.json"
        data = json.loads(sidecar.read_text())
        assert "context" in data
        assert "metadata" in data
        assert "assigned_filename" in data

    @pytest.mark.asyncio
    async def test_atomic_write_no_tmp_left(self, tmp_path, mock_service):
        """Output is written atomically via rename; no .tmp file remains."""
        _setup_source(tmp_path, "archive/doc.pdf")
        record = _make_record(
            source_paths=[PathEntry("archive/doc.pdf", _ts())],
            output_filename="uuid-atomic",
            state=State.NEEDS_PROCESSING,
        )

        processor = Processor(tmp_path, mock_service.url)
        await processor.process_one(record)

        output = tmp_path / ".output" / "uuid-atomic"
        assert output.exists()
        assert output.stat().st_size > 0

        tmp = tmp_path / ".output" / "uuid-atomic.tmp"
        assert not tmp.exists()


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------

class TestProcessorErrors:
    @pytest.mark.asyncio
    async def test_error_500_writes_zero_byte(self, tmp_path, mock_service):
        """Service returns 500 → 0-byte file written."""
        _setup_source(tmp_path, "archive/doc.pdf")
        mock_service.fail_status = 500
        record = _make_record(
            source_paths=[PathEntry("archive/doc.pdf", _ts())],
            output_filename="uuid-err",
            state=State.NEEDS_PROCESSING,
        )

        processor = Processor(
            tmp_path, mock_service.url, max_retries=0,
        )
        await processor.process_one(record)

        output = tmp_path / ".output" / "uuid-err"
        assert output.exists()
        assert output.stat().st_size == 0

    @pytest.mark.asyncio
    async def test_unreachable_writes_zero_byte(self, tmp_path):
        """Service unreachable → 0-byte file written."""
        _setup_source(tmp_path, "archive/doc.pdf")
        record = _make_record(
            source_paths=[PathEntry("archive/doc.pdf", _ts())],
            output_filename="uuid-unreach",
            state=State.NEEDS_PROCESSING,
        )

        # Use an address where nothing listens
        processor = Processor(
            tmp_path, "http://127.0.0.1:19999", max_retries=0,
        )
        await processor.process_one(record)

        output = tmp_path / ".output" / "uuid-unreach"
        assert output.exists()
        assert output.stat().st_size == 0


# ---------------------------------------------------------------------------
# Audio vs document type detection
# ---------------------------------------------------------------------------

class TestTypeDetection:
    @pytest.mark.asyncio
    async def test_document_sends_type_document(self, tmp_path, mock_service):
        """PDF file sends type=document to service."""
        _setup_source(tmp_path, "archive/report.pdf")
        record = _make_record(
            source_paths=[PathEntry("archive/report.pdf", _ts())],
            output_filename="uuid-doc",
            state=State.NEEDS_PROCESSING,
        )

        processor = Processor(tmp_path, mock_service.url)
        await processor.process_one(record)

        assert len(mock_service.calls) == 1
        assert mock_service.calls[0]["fields"]["type"] == "document"
        assert "application/pdf" in mock_service.calls[0]["fields"]["content_type"]

    @pytest.mark.asyncio
    async def test_audio_without_stt_url_creates_zero_byte(self, tmp_path, mock_service):
        """Audio file without stt_url → 0-byte output."""
        _setup_source(tmp_path, "archive/recording.mp3")
        record = _make_record(
            source_paths=[PathEntry("archive/recording.mp3", _ts())],
            output_filename="uuid-aud",
            state=State.NEEDS_PROCESSING,
        )

        processor = Processor(tmp_path, mock_service.url)
        await processor.process_one(record)

        output = tmp_path / ".output" / "uuid-aud"
        assert output.exists()
        assert output.stat().st_size == 0

    @pytest.mark.asyncio
    async def test_unsupported_extension_writes_zero_byte(self, tmp_path, mock_service):
        """Unsupported file type → 0-byte output, no service call."""
        _setup_source(tmp_path, "archive/font.ttf")
        record = _make_record(
            source_paths=[PathEntry("archive/font.ttf", _ts())],
            output_filename="uuid-unsup",
            state=State.NEEDS_PROCESSING,
        )

        processor = Processor(tmp_path, mock_service.url)
        await processor.process_one(record)

        output = tmp_path / ".output" / "uuid-unsup"
        assert output.exists()
        assert output.stat().st_size == 0
        assert len(mock_service.calls) == 0  # no service call made

    def test_is_audio_extensions(self):
        """Audio extension detection covers expected formats."""
        assert _is_audio(".mp3")
        assert _is_audio(".flac")
        assert _is_audio(".wav")
        assert _is_audio(".m4a")
        assert not _is_audio(".pdf")
        assert not _is_audio(".docx")

    def test_content_type_mapping(self):
        """Content type lookup returns correct MIME types."""
        assert _get_content_type(".pdf") == "application/pdf"
        assert _get_content_type(".mp3") == "audio/mpeg"
        assert _get_content_type(".jpg") == "image/jpeg"
        assert _get_content_type(".unknown") == "application/octet-stream"


# ---------------------------------------------------------------------------
# SttConfig
# ---------------------------------------------------------------------------

class TestSttConfig:
    def test_load_returns_none_when_missing(self, tmp_path):
        """No stt.yaml → returns None."""
        assert SttConfig.load(tmp_path) is None

    def test_load_returns_defaults(self, tmp_path):
        """Empty stt.yaml → default config."""
        (tmp_path / "stt.yaml").write_text("# empty\n")
        config = SttConfig.load(tmp_path)
        assert config is not None
        assert config.language == "de-DE"
        assert config.elevenlabs_model == "scribe_v2"

    def test_load_custom_values(self, tmp_path):
        """Custom stt.yaml values are loaded."""
        (tmp_path / "stt.yaml").write_text(
            "language: en-US\nelevenlabs_model: scribe_v1\n"
            "enable_diarization: false\ndiarization_speaker_count: 3\n"
        )
        config = SttConfig.load(tmp_path)
        assert config.language == "en-US"
        assert config.elevenlabs_model == "scribe_v1"
        assert config.enable_diarization is False
        assert config.diarization_speaker_count == 3


# ---------------------------------------------------------------------------
# Audio processing helpers
# ---------------------------------------------------------------------------

def _setup_audio(root: Path, filename: str = "recording.mp3") -> None:
    """Create an audio file in incoming/."""
    _setup_source(root, f"archive/{filename}", b"fake audio bytes")


def _write_stt_yaml(root: Path, **kwargs) -> None:
    """Write stt.yaml in root."""
    config = {
        "language": "de-DE",
        "elevenlabs_model": "scribe_v2",
        "enable_diarization": True,
        "diarization_speaker_count": 2,
    }
    config.update(kwargs)
    import yaml
    (root / "stt.yaml").write_text(yaml.dump(config))


# ---------------------------------------------------------------------------
# Audio processing — success path
# ---------------------------------------------------------------------------

class TestAudioSuccess:
    @pytest.mark.asyncio
    async def test_audio_full_flow(self, tmp_path, mock_service, mock_stt):
        """Audio file → classify → STT → process_transcript → output written."""
        _setup_audio(tmp_path)
        _write_stt_yaml(tmp_path)
        record = _make_record(
            source_paths=[PathEntry("archive/recording.mp3", _ts())],
            output_filename="uuid-audio-ok",
            state=State.NEEDS_PROCESSING,
        )

        processor = Processor(
            tmp_path, mock_service.url, stt_url=mock_stt.url, max_retries=0,
        )
        await processor.process_one(record)

        # Output file written
        output = tmp_path / ".output" / "uuid-audio-ok"
        assert output.exists()
        assert output.read_bytes() == mock_service.response_pdf_bytes

        # Sidecar written
        sidecar = tmp_path / ".output" / "uuid-audio-ok.meta.json"
        assert sidecar.exists()
        data = json.loads(sidecar.read_text())
        assert data["context"] == "work"

        # STT was called with audio file
        assert len(mock_stt.calls) == 1
        assert mock_stt.calls[0]["fields"]["filename"] == "recording.mp3"
        assert mock_stt.calls[0]["fields"]["language"] == "de-DE"

        # Service endpoints called: classify_audio + process_transcript
        endpoint_calls = [c["endpoint"] for c in mock_service.calls if "endpoint" in c]
        assert "classify_audio" in endpoint_calls
        assert "process_transcript" in endpoint_calls

    @pytest.mark.asyncio
    async def test_audio_stt_receives_keyterms(self, tmp_path, mock_service, mock_stt):
        """STT request includes keyterms from classify_audio."""
        _setup_audio(tmp_path)
        _write_stt_yaml(tmp_path)
        mock_service.classify_audio_keyterms = ["Schulze", "IT-Infrastruktur"]
        record = _make_record(
            source_paths=[PathEntry("archive/recording.mp3", _ts())],
            output_filename="uuid-keyterms",
            state=State.NEEDS_PROCESSING,
        )

        processor = Processor(
            tmp_path, mock_service.url, stt_url=mock_stt.url, max_retries=0,
        )
        await processor.process_one(record)

        assert len(mock_stt.calls) == 1
        keyterms_json = mock_stt.calls[0]["fields"].get("keyterms")
        assert keyterms_json is not None
        keyterms = json.loads(keyterms_json)
        assert "Schulze" in keyterms


# ---------------------------------------------------------------------------
# Audio processing — error paths
# ---------------------------------------------------------------------------

class TestAudioErrors:
    @pytest.mark.asyncio
    async def test_no_stt_yaml_creates_zero_byte(self, tmp_path, mock_service, mock_stt):
        """No stt.yaml → 0-byte output."""
        _setup_audio(tmp_path)
        # No _write_stt_yaml
        record = _make_record(
            source_paths=[PathEntry("archive/recording.mp3", _ts())],
            output_filename="uuid-no-stt-yaml",
            state=State.NEEDS_PROCESSING,
        )

        processor = Processor(
            tmp_path, mock_service.url, stt_url=mock_stt.url, max_retries=0,
        )
        await processor.process_one(record)

        output = tmp_path / ".output" / "uuid-no-stt-yaml"
        assert output.exists()
        assert output.stat().st_size == 0

    @pytest.mark.asyncio
    async def test_no_stt_url_creates_zero_byte(self, tmp_path, mock_service):
        """No stt_url → 0-byte output."""
        _setup_audio(tmp_path)
        _write_stt_yaml(tmp_path)
        record = _make_record(
            source_paths=[PathEntry("archive/recording.mp3", _ts())],
            output_filename="uuid-no-stt-url",
            state=State.NEEDS_PROCESSING,
        )

        processor = Processor(tmp_path, mock_service.url, max_retries=0)
        await processor.process_one(record)

        output = tmp_path / ".output" / "uuid-no-stt-url"
        assert output.exists()
        assert output.stat().st_size == 0

    @pytest.mark.asyncio
    async def test_stt_failure_creates_zero_byte(self, tmp_path, mock_service, mock_stt):
        """STT returns 500 → 0-byte output."""
        _setup_audio(tmp_path)
        _write_stt_yaml(tmp_path)
        mock_stt.fail_status = 500
        record = _make_record(
            source_paths=[PathEntry("archive/recording.mp3", _ts())],
            output_filename="uuid-stt-fail",
            state=State.NEEDS_PROCESSING,
        )

        processor = Processor(
            tmp_path, mock_service.url, stt_url=mock_stt.url, max_retries=0,
        )
        await processor.process_one(record)

        output = tmp_path / ".output" / "uuid-stt-fail"
        assert output.exists()
        assert output.stat().st_size == 0

    @pytest.mark.asyncio
    async def test_stt_empty_transcript_creates_zero_byte(self, tmp_path, mock_service, mock_stt):
        """STT returns empty segments → 0-byte output."""
        _setup_audio(tmp_path)
        _write_stt_yaml(tmp_path)
        mock_stt.transcript = {"segments": []}
        record = _make_record(
            source_paths=[PathEntry("archive/recording.mp3", _ts())],
            output_filename="uuid-empty-transcript",
            state=State.NEEDS_PROCESSING,
        )

        processor = Processor(
            tmp_path, mock_service.url, stt_url=mock_stt.url, max_retries=0,
        )
        await processor.process_one(record)

        output = tmp_path / ".output" / "uuid-empty-transcript"
        assert output.exists()
        assert output.stat().st_size == 0

    @pytest.mark.asyncio
    async def test_classify_audio_failure_continues(self, tmp_path, mock_service, mock_stt):
        """classify_audio fails → STT still called without keyterms, processing continues."""
        _setup_audio(tmp_path)
        _write_stt_yaml(tmp_path)
        mock_service.classify_audio_fail = True
        record = _make_record(
            source_paths=[PathEntry("archive/recording.mp3", _ts())],
            output_filename="uuid-classify-fail",
            state=State.NEEDS_PROCESSING,
        )

        processor = Processor(
            tmp_path, mock_service.url, stt_url=mock_stt.url, max_retries=0,
        )
        await processor.process_one(record)

        # STT was still called
        assert len(mock_stt.calls) == 1
        # No keyterms sent
        assert "keyterms" not in mock_stt.calls[0]["fields"]

        # Output still written
        output = tmp_path / ".output" / "uuid-classify-fail"
        assert output.exists()
        assert output.stat().st_size > 0

    @pytest.mark.asyncio
    async def test_process_transcript_failure_creates_zero_byte(
        self, tmp_path, mock_service, mock_stt,
    ):
        """STT succeeds but process_transcript fails → 0-byte output."""
        _setup_audio(tmp_path)
        _write_stt_yaml(tmp_path)
        mock_service.process_transcript_fail = True
        record = _make_record(
            source_paths=[PathEntry("archive/recording.mp3", _ts())],
            output_filename="uuid-proc-fail",
            state=State.NEEDS_PROCESSING,
        )

        processor = Processor(
            tmp_path, mock_service.url, stt_url=mock_stt.url, max_retries=0,
        )
        await processor.process_one(record)

        output = tmp_path / ".output" / "uuid-proc-fail"
        assert output.exists()
        assert output.stat().st_size == 0


# ---------------------------------------------------------------------------
# Audio processing — intro two-pass
# ---------------------------------------------------------------------------

class TestAudioIntroTwoPass:
    @pytest.mark.asyncio
    async def test_intro_runs_two_stt_passes(self, tmp_path, mock_service, mock_stt):
        """Intro file triggers classify_transcript + second STT pass."""
        _setup_audio(tmp_path, "besprechung-intro.mp3")
        _write_stt_yaml(tmp_path)
        record = _make_record(
            original_filename="besprechung-intro.mp3",
            source_paths=[PathEntry("archive/besprechung-intro.mp3", _ts())],
            output_filename="uuid-intro",
            state=State.NEEDS_PROCESSING,
        )

        processor = Processor(
            tmp_path, mock_service.url, stt_url=mock_stt.url, max_retries=0,
        )
        await processor.process_one(record)

        # Two STT calls (first pass + second pass)
        assert len(mock_stt.calls) == 2

        # Second pass has richer keyterms
        second_call = mock_stt.calls[1]
        keyterms_json = second_call["fields"].get("keyterms")
        assert keyterms_json is not None
        keyterms = json.loads(keyterms_json)
        assert "richer_keyword" in keyterms

        # classify_transcript was called
        endpoint_calls = [c["endpoint"] for c in mock_service.calls if "endpoint" in c]
        assert "classify_transcript" in endpoint_calls

        # Output still written successfully
        output = tmp_path / ".output" / "uuid-intro"
        assert output.exists()
        assert output.stat().st_size > 0

    @pytest.mark.asyncio
    async def test_intro_fallback_on_second_pass_failure(
        self, tmp_path, mock_service, mock_stt,
    ):
        """Second STT pass fails → first pass transcript used for process_transcript."""
        _setup_audio(tmp_path, "meeting-intro.mp3")
        _write_stt_yaml(tmp_path)

        # Make the second STT call fail by toggling after first call
        original_transcript = mock_stt.transcript.copy()
        call_count = [0]
        original_handler = mock_stt.handle_transcribe

        async def failing_second_pass(request):
            call_count[0] += 1
            if call_count[0] >= 2:
                mock_stt.fail_status = 500
            return await original_handler(request)

        # Replace the handler
        mock_stt.app = mock_stt.make_app()
        mock_stt.app.router._resources = []
        mock_stt.app.router.add_post("/transcribe", failing_second_pass)

        record = _make_record(
            original_filename="meeting-intro.mp3",
            source_paths=[PathEntry("archive/meeting-intro.mp3", _ts())],
            output_filename="uuid-intro-fallback",
            state=State.NEEDS_PROCESSING,
        )

        processor = Processor(
            tmp_path, mock_service.url, stt_url=mock_stt.url, max_retries=0,
        )
        await processor.process_one(record)

        # Output still written (using first pass transcript)
        output = tmp_path / ".output" / "uuid-intro-fallback"
        assert output.exists()
        assert output.stat().st_size > 0

    @pytest.mark.asyncio
    async def test_intro_classify_transcript_failure_uses_first_pass(
        self, tmp_path, mock_service, mock_stt,
    ):
        """classify_transcript fails → only one STT pass, processing continues."""
        _setup_audio(tmp_path, "call-intro.mp3")
        _write_stt_yaml(tmp_path)
        mock_service.classify_transcript_fail = True
        record = _make_record(
            original_filename="call-intro.mp3",
            source_paths=[PathEntry("archive/call-intro.mp3", _ts())],
            output_filename="uuid-intro-ct-fail",
            state=State.NEEDS_PROCESSING,
        )

        processor = Processor(
            tmp_path, mock_service.url, stt_url=mock_stt.url, max_retries=0,
        )
        await processor.process_one(record)

        # Only one STT call (no second pass)
        assert len(mock_stt.calls) == 1

        # Output still written
        output = tmp_path / ".output" / "uuid-intro-ct-fail"
        assert output.exists()
        assert output.stat().st_size > 0
