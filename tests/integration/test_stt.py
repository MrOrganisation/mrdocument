"""Direct integration tests for the STT service.

Tests exercise the real STT service (with a mocked ElevenLabs backend)
to verify transcription, format conversion, diarization, and error handling.

Requires:
    - STT service running and exposed on port 8002
    - mock-elevenlabs service providing canned transcripts
    - Generated test audio in ``generated/`` (run ``generate_audio.py``)
"""

import json
from pathlib import Path

import pytest
import requests

from conftest import TestConfig

STT_URL = "http://localhost:8002"


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


class TestSttHealth:
    """Verify STT service health and ElevenLabs connectivity."""

    def test_health_endpoint(self):
        resp = requests.get(f"{STT_URL}/health", timeout=5)
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "healthy"
        assert data["elevenlabs_key_set"] is True


# ---------------------------------------------------------------------------
# Transcription: native formats (no conversion)
# ---------------------------------------------------------------------------


class TestSttTranscribeNative:
    """Test transcription of formats that need no conversion (.mp3)."""

    def test_mp3_returns_segments(self, generated_dir: Path):
        """MP3 file produces a transcript with segments."""
        audio = generated_dir / "telefonat.mp3"
        assert audio.exists(), f"Missing test audio: {audio}"

        with open(audio, "rb") as f:
            resp = requests.post(
                f"{STT_URL}/transcribe",
                files={"file": ("telefonat.mp3", f, "audio/mpeg")},
                data={"language": "de-DE"},
                timeout=60,
            )
        assert resp.status_code == 200
        transcript = resp.json()["transcript"]
        assert "segments" in transcript
        assert len(transcript["segments"]) > 0

    def test_mp3_transcript_content(self, generated_dir: Path):
        """Transcript text matches the canned ElevenLabs response."""
        audio = generated_dir / "telefonat.mp3"
        with open(audio, "rb") as f:
            resp = requests.post(
                f"{STT_URL}/transcribe",
                files={"file": ("telefonat.mp3", f, "audio/mpeg")},
                data={"language": "de-DE"},
                timeout=60,
            )
        segments = resp.json()["transcript"]["segments"]
        full_text = " ".join(s["text"] for s in segments)
        assert "Keller" in full_text
        assert "Bueroausstattung" in full_text

    def test_segment_timestamps(self, generated_dir: Path):
        """Each segment has start/end timestamps."""
        audio = generated_dir / "telefonat.mp3"
        with open(audio, "rb") as f:
            resp = requests.post(
                f"{STT_URL}/transcribe",
                files={"file": ("telefonat.mp3", f, "audio/mpeg")},
                data={"language": "de-DE"},
                timeout=60,
            )
        segments = resp.json()["transcript"]["segments"]
        for seg in segments:
            assert "start" in seg, "Segment missing 'start' timestamp"
            assert "end" in seg, "Segment missing 'end' timestamp"
            assert seg["end"] >= seg["start"], "end < start"


# ---------------------------------------------------------------------------
# Transcription: formats requiring FFmpeg conversion
# ---------------------------------------------------------------------------


class TestSttConversion:
    """Test transcription of formats that require FFmpeg conversion."""

    def test_mov_conversion(self, generated_dir: Path):
        """MOV video is converted and transcribed."""
        audio = generated_dir / "videocall.mov"
        assert audio.exists(), f"Missing test file: {audio}"

        with open(audio, "rb") as f:
            resp = requests.post(
                f"{STT_URL}/transcribe",
                files={"file": ("videocall.mov", f, "video/quicktime")},
                data={"language": "de-DE"},
                timeout=120,
            )
        assert resp.status_code == 200
        segments = resp.json()["transcript"]["segments"]
        assert len(segments) > 0
        full_text = " ".join(s["text"] for s in segments)
        assert "Fischer" in full_text or "Projektbesprechung" in full_text

    def test_mp4_conversion(self, generated_dir: Path):
        """MP4 video is converted and transcribed."""
        audio = generated_dir / "sprachnachricht.mp4"
        assert audio.exists(), f"Missing test file: {audio}"

        with open(audio, "rb") as f:
            resp = requests.post(
                f"{STT_URL}/transcribe",
                files={"file": ("sprachnachricht.mp4", f, "video/mp4")},
                data={"language": "de-DE"},
                timeout=120,
            )
        assert resp.status_code == 200
        segments = resp.json()["transcript"]["segments"]
        assert len(segments) > 0
        full_text = " ".join(s["text"] for s in segments)
        assert "Mueller" in full_text or "Hautuntersuchung" in full_text

    def test_m4a_conversion(self, generated_dir: Path):
        """M4A audio is converted and transcribed."""
        audio = generated_dir / "sorted-wrongctx-audio.m4a"
        assert audio.exists(), f"Missing test file: {audio}"

        with open(audio, "rb") as f:
            resp = requests.post(
                f"{STT_URL}/transcribe",
                files={"file": ("sorted-wrongctx-audio.m4a", f, "audio/mp4")},
                data={"language": "de-DE"},
                timeout=120,
            )
        assert resp.status_code == 200
        segments = resp.json()["transcript"]["segments"]
        assert len(segments) > 0


# ---------------------------------------------------------------------------
# Diarization
# ---------------------------------------------------------------------------


class TestSttDiarization:
    """Test speaker diarization features."""

    def test_diarization_produces_speaker_tags(self, generated_dir: Path):
        """With diarization enabled, segments have speaker labels."""
        audio = generated_dir / "telefonat.mp3"
        with open(audio, "rb") as f:
            resp = requests.post(
                f"{STT_URL}/transcribe",
                files={"file": ("telefonat.mp3", f, "audio/mpeg")},
                data={
                    "language": "de-DE",
                    "enable_diarization": "true",
                    "diarization_speaker_count": "2",
                },
                timeout=60,
            )
        assert resp.status_code == 200
        segments = resp.json()["transcript"]["segments"]
        assert len(segments) > 0
        has_speaker = any(s.get("speaker") is not None for s in segments)
        assert has_speaker, "No speaker tags in diarized transcript"

    def test_no_diarization(self, generated_dir: Path):
        """Without diarization, transcript still works (speakers may be None)."""
        audio = generated_dir / "telefonat.mp3"
        with open(audio, "rb") as f:
            resp = requests.post(
                f"{STT_URL}/transcribe",
                files={"file": ("telefonat.mp3", f, "audio/mpeg")},
                data={
                    "language": "de-DE",
                    "enable_diarization": "false",
                },
                timeout=60,
            )
        assert resp.status_code == 200
        segments = resp.json()["transcript"]["segments"]
        assert len(segments) > 0


# ---------------------------------------------------------------------------
# Word-level timestamps
# ---------------------------------------------------------------------------


class TestSttWordTimestamps:
    """Test word-level timestamp features."""

    def test_word_timestamps_returned(self, generated_dir: Path):
        """With word timestamps enabled, segments contain words array."""
        audio = generated_dir / "telefonat.mp3"
        with open(audio, "rb") as f:
            resp = requests.post(
                f"{STT_URL}/transcribe",
                files={"file": ("telefonat.mp3", f, "audio/mpeg")},
                data={
                    "language": "de-DE",
                    "enable_word_timestamps": "true",
                },
                timeout=60,
            )
        assert resp.status_code == 200
        segments = resp.json()["transcript"]["segments"]
        has_words = any("words" in s and len(s["words"]) > 0 for s in segments)
        assert has_words, "No word-level data with enable_word_timestamps=true"

    def test_word_timestamps_structure(self, generated_dir: Path):
        """Word objects have expected fields."""
        audio = generated_dir / "telefonat.mp3"
        with open(audio, "rb") as f:
            resp = requests.post(
                f"{STT_URL}/transcribe",
                files={"file": ("telefonat.mp3", f, "audio/mpeg")},
                data={
                    "language": "de-DE",
                    "enable_word_timestamps": "true",
                },
                timeout=60,
            )
        segments = resp.json()["transcript"]["segments"]
        words_found = False
        for seg in segments:
            for w in seg.get("words", []):
                words_found = True
                assert "word" in w, "Word missing 'word' field"
                assert "start" in w, "Word missing 'start' field"
                assert "end" in w, "Word missing 'end' field"
        assert words_found, "No words found in any segment"


# ---------------------------------------------------------------------------
# Keyterms
# ---------------------------------------------------------------------------


class TestSttKeyterms:
    """Test keyterms parameter handling."""

    def test_keyterms_accepted(self, generated_dir: Path):
        """Keyterms parameter is forwarded without error."""
        audio = generated_dir / "telefonat.mp3"
        with open(audio, "rb") as f:
            resp = requests.post(
                f"{STT_URL}/transcribe",
                files={"file": ("telefonat.mp3", f, "audio/mpeg")},
                data={
                    "language": "de-DE",
                    "keyterms": json.dumps(["Keller", "Partner", "Bueroausstattung"]),
                },
                timeout=60,
            )
        assert resp.status_code == 200
        assert len(resp.json()["transcript"]["segments"]) > 0

    def test_invalid_keyterms_rejected(self, generated_dir: Path):
        """Invalid keyterms JSON returns 400."""
        audio = generated_dir / "telefonat.mp3"
        with open(audio, "rb") as f:
            resp = requests.post(
                f"{STT_URL}/transcribe",
                files={"file": ("telefonat.mp3", f, "audio/mpeg")},
                data={
                    "language": "de-DE",
                    "keyterms": "not valid json",
                },
                timeout=60,
            )
        assert resp.status_code == 400 or resp.status_code == 422


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestSttErrors:
    """Test error handling and input validation."""

    def test_unsupported_format(self):
        """Unsupported file format returns 400."""
        resp = requests.post(
            f"{STT_URL}/transcribe",
            files={"file": ("test.xyz", b"not audio data", "application/octet-stream")},
            data={"language": "de-DE"},
            timeout=10,
        )
        assert resp.status_code == 400

    def test_missing_file(self):
        """Request without file returns 422."""
        resp = requests.post(
            f"{STT_URL}/transcribe",
            data={"language": "de-DE"},
            timeout=10,
        )
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# ElevenLabs model selection
# ---------------------------------------------------------------------------


class TestSttModel:
    """Test model parameter forwarding."""

    def test_custom_model(self, generated_dir: Path):
        """Custom ElevenLabs model is accepted."""
        audio = generated_dir / "telefonat.mp3"
        with open(audio, "rb") as f:
            resp = requests.post(
                f"{STT_URL}/transcribe",
                files={"file": ("telefonat.mp3", f, "audio/mpeg")},
                data={
                    "language": "de-DE",
                    "elevenlabs_model": "scribe_v1",
                },
                timeout=60,
            )
        assert resp.status_code == 200
        assert len(resp.json()["transcript"]["segments"]) > 0
