"""ElevenLabs backend for speech-to-text transcription."""

import logging
import os
import uuid
from pathlib import Path
from typing import Optional

import requests

from ..transcript import TranscriptResult, TranscriptSegment, TranscriptWord
from . import Backend, TranscriptionJob

logger = logging.getLogger(__name__)

# ElevenLabs API base URL (override with ELEVENLABS_BASE_URL env var)
API_BASE = os.environ.get("ELEVENLABS_BASE_URL", "https://api.elevenlabs.io/v1")

# Model options
# See: https://elevenlabs.io/docs/api-reference/speech-to-text/convert
ELEVENLABS_MODELS = {
    "scribe_v1": "Scribe v1 - Standard transcription model",
    "scribe_v1_experimental": "Scribe v1 Experimental - Latest features",
    "scribe_v2": "Scribe v2 - Latest transcription model",
}


class ElevenLabsBackend(Backend):
    """ElevenLabs Speech-to-Text backend."""

    name = "elevenlabs"

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "scribe_v2",
        **kwargs,
    ):
        """
        Initialize ElevenLabs backend.

        Args:
            api_key: ElevenLabs API key. If not provided, uses ELEVENLABS_API_KEY env var.
            model: Model to use (scribe_v1, scribe_v1_experimental, or scribe_v2).
        """
        self.api_key = api_key or os.environ.get("ELEVENLABS_API_KEY")
        self.model = model
        self._results: dict[str, TranscriptResult] = {}

    def _get_headers(self) -> dict:
        """Get API request headers."""
        if not self.api_key:
            raise ValueError(
                "ElevenLabs API key not set. Set ELEVENLABS_API_KEY environment variable "
                "or pass api_key parameter."
            )
        return {
            "xi-api-key": self.api_key,
        }

    def supports_async(self) -> bool:
        """ElevenLabs supports async operations."""
        return True

    def requires_upload(self) -> bool:
        """ElevenLabs handles file upload directly."""
        return False

    def transcribe(
        self,
        audio_path: Path,
        language: str = "de-DE",
        enable_diarization: bool = False,
        speaker_count: int = 2,
        enable_word_timestamps: bool = False,
        keyterms: Optional[list[str]] = None,
        original_filename: Optional[str] = None,
        **kwargs,
    ) -> TranscriptionJob:
        """
        Start transcription of an audio file.

        Args:
            audio_path: Path to the audio file.
            language: Language code (e.g., 'de', 'en', 'de-DE').
            enable_diarization: Enable speaker diarization.
            speaker_count: Expected number of speakers (used as hint).
            enable_word_timestamps: Include word-level timestamps.
            keyterms: Optional list of key terms to help transcription accuracy.

        Returns:
            TranscriptionJob with job_id and initial status.
        """
        url = f"{API_BASE}/speech-to-text"

        # Convert language code to ISO 639-1 (2-letter) if needed
        # ElevenLabs uses 2-letter codes like 'de', 'en', etc.
        lang_code = language.split("-")[0].lower() if "-" in language else language.lower()

        # Prepare multipart form data
        upload_name = original_filename or audio_path.name
        with open(audio_path, "rb") as f:
            files = {
                "file": (upload_name, f, "audio/flac"),
            }
            data = {
                "model_id": self.model,
                "language_code": lang_code,
                "diarize": str(enable_diarization).lower(),
                "tag_audio_events": "true",  # Enable audio event detection (laughs, etc.)
            }

            if enable_diarization and speaker_count:
                data["num_speakers"] = str(speaker_count)

            if enable_word_timestamps:
                data["timestamps_granularity"] = "word"
            else:
                data["timestamps_granularity"] = "none"

            if keyterms:
                data["keyterms"] = keyterms

            logger.info(f"[ElevenLabs] Starting transcription: model={self.model}, lang={lang_code}, diarize={enable_diarization}, keyterms={len(keyterms) if keyterms else 0}")

            try:
                response = requests.post(
                    url,
                    headers=self._get_headers(),
                    files=files,
                    data=data,
                    timeout=300,  # 5 minute timeout for upload
                )
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                error_detail = ""
                try:
                    error_detail = response.json().get("detail", {})
                except Exception:
                    error_detail = response.text[:200]
                status_code = response.status_code
                logger.error(f"[ElevenLabs] API error (HTTP {status_code}): {e}. Detail: {error_detail}")
                # 4xx (except 429) = input problem, not retryable
                if 400 <= status_code < 500 and status_code != 429:
                    raise ValueError(f"ElevenLabs rejected the input (HTTP {status_code}): {error_detail}")
                raise RuntimeError(f"ElevenLabs API error: {e}. Detail: {error_detail}")
            except requests.exceptions.RequestException as e:
                logger.error(f"[ElevenLabs] Request failed: {e}")
                raise RuntimeError(f"Request failed: {e}")

        result_data = response.json()
        logger.info(f"[ElevenLabs] Transcription complete")
        logger.debug(f"[ElevenLabs] Response: {result_data}")

        # ElevenLabs returns the result synchronously for the convert endpoint
        # Parse the response into our transcript format
        transcript = self._parse_response(result_data, language)

        # Generate a job ID for tracking
        job_id = str(uuid.uuid4())
        self._results[job_id] = transcript

        return TranscriptionJob(
            job_id=job_id,
            backend=self.name,
            is_complete=True,
            result=transcript,
            progress=100,
        )

    def _parse_response(self, data: dict, language: str) -> TranscriptResult:
        """
        Parse ElevenLabs API response into TranscriptResult.

        Args:
            data: API response data.
            language: Original language code.

        Returns:
            TranscriptResult with parsed segments.
        """
        segments = []

        # ElevenLabs returns different structures based on settings
        # Main text is in 'text' field
        # Words/segments with timestamps are in 'words' or 'segments'

        if "words" in data and data["words"]:
            # Word-level timestamps available
            words = []
            for word_data in data["words"]:
                word = TranscriptWord(
                    word=word_data.get("text", ""),
                    start_time=word_data.get("start", 0.0),
                    end_time=word_data.get("end", 0.0),
                    speaker_tag=word_data.get("speaker_id"),
                )
                words.append(word)

            # Group words into segments by speaker or by sentence
            if words:
                current_segment_words = []
                current_speaker = words[0].speaker_tag

                for word in words:
                    # Start new segment on speaker change
                    if word.speaker_tag != current_speaker and current_segment_words:
                        seg = self._create_segment_from_words(current_segment_words, current_speaker)
                        segments.append(seg)
                        current_segment_words = []
                        current_speaker = word.speaker_tag

                    current_segment_words.append(word)

                # Don't forget the last segment
                if current_segment_words:
                    seg = self._create_segment_from_words(current_segment_words, current_speaker)
                    segments.append(seg)

        elif "segments" in data and data["segments"]:
            # Segment-level data
            for seg_data in data["segments"]:
                words = []
                if "words" in seg_data:
                    for word_data in seg_data["words"]:
                        words.append(TranscriptWord(
                            word=word_data.get("text", ""),
                            start_time=word_data.get("start", 0.0),
                            end_time=word_data.get("end", 0.0),
                            speaker_tag=seg_data.get("speaker_id"),
                        ))

                segment = TranscriptSegment(
                    text=seg_data.get("text", ""),
                    confidence=seg_data.get("confidence", 1.0),
                    words=words,
                    speaker_tag=seg_data.get("speaker_id"),
                    start_time=seg_data.get("start", 0.0),
                    end_time=seg_data.get("end", 0.0),
                )
                segments.append(segment)

        else:
            # Only full text available, create single segment
            full_text = data.get("text", "")
            if full_text:
                segments.append(TranscriptSegment(
                    text=full_text,
                    confidence=1.0,
                    words=[],
                    speaker_tag=None,
                    start_time=0.0,
                    end_time=0.0,
                ))

        # Get detected language
        detected_language = data.get("language_code", language)

        return TranscriptResult(
            segments=segments,
            language_code=detected_language,
        )

    def _create_segment_from_words(
        self,
        words: list[TranscriptWord],
        speaker_tag: Optional[int],
    ) -> TranscriptSegment:
        """Create a segment from a list of words."""
        text = " ".join(w.word for w in words)
        start_time = words[0].start_time if words else 0.0
        end_time = words[-1].end_time if words else 0.0

        return TranscriptSegment(
            text=text,
            confidence=1.0,
            words=words,
            speaker_tag=speaker_tag,
            start_time=start_time,
            end_time=end_time,
        )

    def get_status(self, job_id: str, **kwargs) -> TranscriptionJob:
        """
        Get the status of a transcription job.

        ElevenLabs convert endpoint is synchronous, so jobs are always complete.
        """
        result = self._results.get(job_id)
        return TranscriptionJob(
            job_id=job_id,
            backend=self.name,
            is_complete=True,
            result=result,
            progress=100,
        )

    def get_result(self, job_id: str, **kwargs) -> Optional[TranscriptResult]:
        """
        Get the transcript result for a completed job.

        Args:
            job_id: The job identifier.

        Returns:
            TranscriptResult if available.
        """
        return self._results.get(job_id)

    def delete_job(self, job_id: str) -> bool:
        """
        Delete a transcription job from local cache.

        Args:
            job_id: The job identifier.

        Returns:
            True if deleted, False if not found.
        """
        if job_id in self._results:
            del self._results[job_id]
            return True
        return False
