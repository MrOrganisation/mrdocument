"""
Processing service calls for document watcher v2.

Sends documents to the mrdocument service for classification and OCR,
and orchestrates audio processing via STT + mrdocument service.
Writes results and sidecar metadata to .output/.
"""

import asyncio
import base64
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import aiohttp
import yaml

from models import Record

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Extension-based type detection
# ---------------------------------------------------------------------------

AUDIO_EXTENSIONS = frozenset({
    ".flac", ".wav", ".mp3", ".ogg", ".webm",
    ".mp4", ".m4a", ".mkv", ".avi", ".mov",
})

DOCUMENT_EXTENSIONS = frozenset({
    ".pdf", ".eml", ".html", ".htm", ".docx", ".txt", ".md", ".rtf",
    ".jpg", ".jpeg", ".png", ".gif", ".tiff", ".tif",
    ".bmp", ".webp", ".ppm", ".pgm", ".pbm", ".pnm",
})

CONTENT_TYPE_MAP = {
    ".pdf": "application/pdf",
    ".eml": "message/rfc822",
    ".html": "text/html",
    ".htm": "text/html",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".txt": "text/plain",
    ".md": "text/markdown",
    ".rtf": "application/rtf",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".gif": "image/gif",
    ".tiff": "image/tiff",
    ".tif": "image/tiff",
    ".bmp": "image/bmp",
    ".webp": "image/webp",
    ".ppm": "image/x-portable-pixmap",
    ".pgm": "image/x-portable-graymap",
    ".pbm": "image/x-portable-bitmap",
    ".pnm": "image/x-portable-anymap",
    ".flac": "audio/flac",
    ".wav": "audio/wav",
    ".mp3": "audio/mpeg",
    ".ogg": "audio/ogg",
    ".webm": "video/webm",
    ".mp4": "video/mp4",
    ".m4a": "audio/mp4",
    ".mkv": "video/x-matroska",
    ".avi": "video/x-msvideo",
    ".mov": "video/quicktime",
}


def _get_content_type(ext: str) -> str:
    """Get MIME content type for a file extension."""
    return CONTENT_TYPE_MAP.get(ext, "application/octet-stream")


def _is_audio(ext: str) -> bool:
    """Check if extension indicates an audio/video file."""
    return ext in AUDIO_EXTENSIONS


# ---------------------------------------------------------------------------
# STT Configuration
# ---------------------------------------------------------------------------

@dataclass
class SttConfig:
    """STT (Speech-to-Text) configuration loaded from stt.yaml."""

    language: str = "de-DE"
    elevenlabs_model: str = "scribe_v2"
    enable_diarization: bool = True
    diarization_speaker_count: int = 2

    @classmethod
    def load(cls, user_root: Path) -> Optional["SttConfig"]:
        """Load STT config from user folder. Returns None if not configured."""
        stt_path = user_root / "stt.yaml"
        if not stt_path.exists():
            return None
        try:
            with open(stt_path) as f:
                data = yaml.safe_load(f)
            if not data or not isinstance(data, dict):
                return cls()
            return cls(
                language=data.get("language", cls.language),
                elevenlabs_model=data.get("elevenlabs_model", cls.elevenlabs_model),
                enable_diarization=data.get("enable_diarization", cls.enable_diarization),
                diarization_speaker_count=data.get("diarization_speaker_count", cls.diarization_speaker_count),
            )
        except Exception as e:
            logger.warning("Failed to load stt.yaml from %s: %s", user_root, e)
            return None


# ---------------------------------------------------------------------------
# Processor
# ---------------------------------------------------------------------------

class Processor:
    """Sends documents/audio to the mrdocument service for processing."""

    def __init__(
        self,
        root: Path,
        service_url: str,
        stt_url: Optional[str] = None,
        timeout: float = 900.0,
        max_retries: int = 3,
        retry_delay: float = 2.0,
        contexts: Optional[list] = None,
        context_manager=None,
    ):
        self.root = root
        self.service_url = service_url
        self.stt_url = stt_url
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.contexts = contexts or []
        self.context_manager = context_manager

    async def process_one(self, record: Record) -> None:
        """Process a single record by calling the appropriate service.

        Documents go to mrdocument /process.
        Audio files go through the STT orchestration flow.
        On success: writes output file and sidecar JSON to .output/.
        On error: creates a 0-byte file at .output/{output_filename}.
        """
        output_dir = self.root / ".output"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / record.output_filename
        sidecar_path = output_dir / f"{record.output_filename}.meta.json"

        try:
            source_path = self.root / record.source_file.path
            ext = source_path.suffix.lower()
            if ext not in DOCUMENT_EXTENSIONS and ext not in AUDIO_EXTENSIONS:
                logger.warning(
                    "Skipping unsupported file type %s: %s",
                    ext or "(no extension)", record.source_file.path,
                )
                output_path.touch()
                return
            file_bytes = source_path.read_bytes()
            content_type = _get_content_type(ext)

            # If context is pre-set (e.g., from sorted/), filter to that context
            contexts = self.contexts
            if record.context:
                filtered = [c for c in self.contexts if c.get("name") == record.context]
                if filtered:
                    contexts = filtered

            if _is_audio(ext):
                result = await self._process_audio(
                    file_bytes, source_path.name, content_type,
                    contexts=contexts,
                )
            else:
                result = await self._call_service(
                    file_bytes, source_path.name, content_type, "document",
                    contexts=contexts,
                )

            if result is None:
                output_path.touch()
                return

            # Extract response
            metadata = result.get("metadata", {})
            content_b64 = result.get("pdf")
            text_content = result.get("text")

            if content_b64:
                content_bytes = base64.b64decode(content_b64)
            elif text_content:
                # Transcript text from /process_transcript
                content_bytes = text_content.encode("utf-8")
            else:
                content_bytes = file_bytes

            # For audio/transcript results, ensure filename has .txt extension
            suggested_filename = result.get("filename")
            if suggested_filename and text_content:
                # Strip any existing extension (e.g. .pdf) before adding .txt
                suggested_filename = Path(suggested_filename).stem + ".txt"

            # Atomic write: tmp file → rename
            tmp_path = output_path.with_suffix(".tmp")
            tmp_path.write_bytes(content_bytes)
            tmp_path.rename(output_path)

            # Write sidecar
            sidecar = {
                "context": metadata.get("context"),
                "metadata": metadata,
                "assigned_filename": suggested_filename or result.get("filename"),
            }
            sidecar_path.write_text(json.dumps(sidecar))

            # Process new_clues from service response
            new_clues = result.get("new_clues", {})
            if new_clues and self.context_manager:
                context_name = metadata.get("context")
                if context_name:
                    for field_name, clue_data in new_clues.items():
                        value = clue_data.get("value")
                        clue = clue_data.get("clue")
                        if value and self.context_manager.is_new_item(context_name, field_name, value):
                            self.context_manager.record_new_item(context_name, field_name, value)
                        if value and clue:
                            self.context_manager.record_new_clue(context_name, field_name, value, clue)

            logger.info(
                "Processed %s → %s",
                record.source_file.path, record.output_filename,
            )

        except Exception as e:
            logger.error(
                "Failed to process %s: %s", record.output_filename, e,
            )
            output_path.touch()

    # ------------------------------------------------------------------
    # Document processing (unchanged logic, uses _call_with_retry)
    # ------------------------------------------------------------------

    async def _call_service(
        self,
        file_bytes: bytes,
        filename: str,
        content_type: str,
        file_type: str,
        contexts: Optional[list] = None,
    ) -> Optional[dict]:
        """Call the mrdocument /process endpoint with retry logic."""
        ctx_list = contexts if contexts is not None else self.contexts
        async with aiohttp.ClientSession() as session:

            def make_request(sess):
                data = aiohttp.FormData()
                data.add_field(
                    "file", file_bytes,
                    filename=filename,
                    content_type=content_type,
                )
                data.add_field("type", file_type)
                data.add_field("contexts", json.dumps(ctx_list))
                data.add_field("user_dir", str(self.root))
                return sess.post(
                    f"{self.service_url}/process",
                    data=data,
                    timeout=aiohttp.ClientTimeout(total=self.timeout),
                )

            return await self._call_with_retry(
                session, make_request, label="process", source=filename,
            )

    # ------------------------------------------------------------------
    # Audio processing — STT orchestration
    # ------------------------------------------------------------------

    async def _process_audio(
        self,
        file_bytes: bytes,
        filename: str,
        content_type: str,
        contexts: Optional[list] = None,
    ) -> Optional[dict]:
        """Orchestrate audio processing: classify → STT → process transcript.

        Returns the response dict on success, or None on failure.
        """
        ctx_list = contexts if contexts is not None else self.contexts

        # Check prerequisites
        if not self.stt_url:
            logger.warning("Audio file %s skipped: no STT URL configured", filename)
            return None

        stt_config = SttConfig.load(self.root)
        if stt_config is None:
            logger.warning("Audio file %s skipped: no stt.yaml found", filename)
            return None

        async with aiohttp.ClientSession() as session:
            # Step 1: Classify audio by filename (optional)
            keyterms = await self._classify_audio(session, filename, contexts=ctx_list)

            # Step 2: First STT pass (required)
            transcript = await self._stt_transcribe(
                session, file_bytes, filename, content_type,
                stt_config, keyterms,
            )
            if transcript is None:
                return None

            # Validate transcript
            if not transcript.get("segments"):
                logger.error("Empty transcript for %s", filename)
                return None

            logger.info(
                "Got transcript for %s: %d segments",
                filename, len(transcript.get("segments", [])),
            )

            # Step 3: Intro two-pass (optional)
            pre_classified = None
            if "intro" in filename.lower():
                transcript, pre_classified = await self._intro_two_pass(
                    session, file_bytes, filename, content_type,
                    stt_config, transcript, contexts=ctx_list,
                )

            # Step 4: Process transcript (required)
            return await self._process_transcript(
                session, transcript, filename, pre_classified,
                contexts=ctx_list,
            )

    async def _classify_audio(
        self,
        session: aiohttp.ClientSession,
        filename: str,
        contexts: Optional[list] = None,
    ) -> Optional[list[str]]:
        """Classify audio by filename to get transcription keyterms.

        Optional — failure returns None (processing continues without keyterms).
        """
        ctx_list = contexts if contexts is not None else self.contexts
        try:
            request_body = {
                "filename": filename,
                "contexts": ctx_list,
            }

            def make_request(sess):
                return sess.post(
                    f"{self.service_url}/classify_audio",
                    json=request_body,
                    timeout=aiohttp.ClientTimeout(total=120),
                )

            result = await self._call_with_retry(
                session, make_request,
                max_retries=2, label="classify_audio", source=filename,
            )

            if result is None:
                return None

            context = result.get("context")
            keyterms = result.get("transcription_keyterms", [])
            if keyterms:
                logger.info(
                    "Audio classification for %s: context=%s, %d keyterms",
                    filename, context, len(keyterms),
                )
            return keyterms or None

        except Exception as e:
            logger.warning("Audio classification failed: %s", e)
            return None

    async def _stt_transcribe(
        self,
        session: aiohttp.ClientSession,
        file_bytes: bytes,
        filename: str,
        content_type: str,
        stt_config: SttConfig,
        keyterms: Optional[list[str]] = None,
        speaker_count: Optional[int] = None,
    ) -> Optional[dict]:
        """Send audio to STT service for transcription.

        Returns the transcript dict on success, or None on failure.
        """
        def make_request(sess):
            data = aiohttp.FormData()
            data.add_field("file", file_bytes, filename=filename, content_type=content_type)
            data.add_field("language", stt_config.language)
            data.add_field("elevenlabs_model", stt_config.elevenlabs_model)
            data.add_field("enable_diarization", str(stt_config.enable_diarization).lower())
            n_speakers = speaker_count if speaker_count else stt_config.diarization_speaker_count
            data.add_field("diarization_speaker_count", str(n_speakers))
            if keyterms:
                data.add_field("keyterms", json.dumps(keyterms))
            return sess.post(
                f"{self.stt_url}/transcribe",
                data=data,
                timeout=aiohttp.ClientTimeout(total=1800),
            )

        result = await self._call_with_retry(
            session, make_request, label="stt_transcribe", source=filename,
        )
        if result is None:
            return None

        return result.get("transcript")

    async def _intro_two_pass(
        self,
        session: aiohttp.ClientSession,
        file_bytes: bytes,
        filename: str,
        content_type: str,
        stt_config: SttConfig,
        transcript: dict,
        contexts: Optional[list] = None,
    ) -> tuple[dict, Optional[dict]]:
        """Handle intro file two-pass flow.

        Returns (final_transcript, pre_classified).
        Falls back to original transcript on any failure.
        """
        ctx_list = contexts if contexts is not None else self.contexts
        logger.info("Intro file detected, starting two-pass flow for %s", filename)
        pre_classified = None

        # Classify transcript to get richer keyterms + speaker count
        try:
            request_body = {
                "transcript": transcript,
                "filename": filename,
                "contexts": ctx_list,
            }

            def make_classify(sess):
                return sess.post(
                    f"{self.service_url}/classify_transcript",
                    json=request_body,
                    timeout=aiohttp.ClientTimeout(total=300),
                )

            ct_result = await self._call_with_retry(
                session, make_classify,
                max_retries=2, label="classify_transcript", source=filename,
            )

            if ct_result is None:
                logger.warning("Transcript classification failed, using first pass only")
                return transcript, None

            keyterms_2 = ct_result.get("transcription_keyterms", [])
            n_speakers = ct_result.get("number_of_speakers")
            ct_context = ct_result.get("context")
            ct_metadata = ct_result.get("metadata", {})

            logger.info(
                "Transcript classification: context=%s, keyterms=%d, speakers=%s",
                ct_context, len(keyterms_2) if keyterms_2 else 0, n_speakers,
            )

            # Build pre_classified if classification succeeded
            if ct_context and ct_metadata:
                pc_fields = {k: v for k, v in ct_metadata.items() if k not in ("context", "date")}
                pre_classified = {
                    "context": ct_context,
                    "date": ct_metadata.get("date"),
                    "fields": pc_fields,
                }

            # Second STT pass with improved keyterms
            if keyterms_2:
                logger.info("Running second STT pass with %d keyterms", len(keyterms_2))
                speaker_count = n_speakers if n_speakers and n_speakers > 1 else None
                transcript_2 = await self._stt_transcribe(
                    session, file_bytes, filename, content_type,
                    stt_config, keyterms=keyterms_2, speaker_count=speaker_count,
                )
                if transcript_2 and transcript_2.get("segments"):
                    logger.info(
                        "Second STT pass successful: %d segments",
                        len(transcript_2["segments"]),
                    )
                    transcript = transcript_2
                else:
                    logger.warning("Second STT pass failed, using first pass")

        except Exception as e:
            logger.warning("Intro two-pass error: %s, using first pass", e)

        return transcript, pre_classified

    async def _process_transcript(
        self,
        session: aiohttp.ClientSession,
        transcript: dict,
        filename: str,
        pre_classified: Optional[dict] = None,
        contexts: Optional[list] = None,
    ) -> Optional[dict]:
        """Send transcript to mrdocument for processing."""
        ctx_list = contexts if contexts is not None else self.contexts
        request_body: dict = {
            "transcript": transcript,
            "filename": Path(filename).stem,
            "contexts": ctx_list,
            "user_dir": str(self.root),
        }
        if pre_classified:
            request_body["pre_classified"] = pre_classified

        def make_request(sess):
            return sess.post(
                f"{self.service_url}/process_transcript",
                json=request_body,
                timeout=aiohttp.ClientTimeout(total=1800),
            )

        return await self._call_with_retry(
            session, make_request, label="process_transcript", source=filename,
        )

    # ------------------------------------------------------------------
    # Shared retry logic
    # ------------------------------------------------------------------

    async def _call_with_retry(
        self,
        session: aiohttp.ClientSession,
        request_fn,
        max_retries: Optional[int] = None,
        label: str = "service",
        source: Optional[str] = None,
    ) -> Optional[dict]:
        """Execute an HTTP request with retry logic.

        Args:
            session: The aiohttp session to use.
            request_fn: Async callable(session) -> aiohttp.ClientResponse.
            max_retries: Override self.max_retries for this call.
            label: Label for log messages.
            source: Source filename for log messages.

        Returns parsed JSON on success, None on failure.
        """
        retries = max_retries if max_retries is not None else self.max_retries
        delay = self.retry_delay
        max_delay = 30.0
        tag = f"{label} [{source}]" if source else label

        for attempt in range(retries + 1):
            try:
                async with request_fn(session) as response:
                    if response.status == 200:
                        return await response.json()

                    if response.status < 500 and response.status != 429:
                        logger.error(
                            "%s client error: HTTP %d", tag, response.status,
                        )
                        return None

                    logger.warning(
                        "%s error (attempt %d/%d): HTTP %d",
                        tag, attempt + 1, retries + 1, response.status,
                    )

            except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                logger.warning(
                    "%s connection error (attempt %d/%d): %s",
                    tag, attempt + 1, retries + 1, e,
                )

            if attempt < retries:
                await asyncio.sleep(delay)
                delay = min(delay * 2, max_delay)

        return None
