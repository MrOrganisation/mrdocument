"""Integration test for chunked transcript correction.

Verifies that large transcripts (exceeding the model's output token limit)
are automatically split into chunks, corrected individually, and reassembled.

Requires:
    - mrdocument-service running on localhost:8000
    - mock-anthropic running (echoes segments back unchanged)
"""

import json

import pytest
import requests

SERVICE_URL = "http://localhost:8000"

# Minimal context definition matching the test config
TEST_CONTEXT = {
    "name": "arbeit",
    "description": "Work documents",
    "filename": "{context}-{type}-{date}-{sender}",
    "audio_filename": "{context}-{date}-{sender}-{type}",
    "fields": {
        "type": {
            "instructions": "Identify the document type.",
            "candidates": ["Telefonat", "Besprechung", "Rechnung"],
            "allow_new_candidates": False,
        },
        "sender": {
            "instructions": "Identify the sender.",
            "candidates": ["Schulze GmbH", "Keller und Partner"],
            "allow_new_candidates": True,
        },
    },
}


def _make_segments(n, chars_per_segment=200):
    """Generate n transcript segments with deterministic text."""
    segments = []
    for i in range(n):
        text = f"Segment {i}: " + f"Dies ist ein Testsegment Nummer {i}. " * (chars_per_segment // 40)
        segments.append({
            "text": text,
            "start": round(i * 3.0, 2),
            "end": round(i * 3.0 + 2.8, 2),
            "speaker": f"SPEAKER_{i % 2}",
        })
    return segments


class TestTranscriptChunking:
    """Test that large transcripts are chunked and reassembled correctly."""

    @pytest.mark.timeout(120)
    def test_small_transcript_single_chunk(self):
        """A small transcript (<48k tokens) goes through as one chunk."""
        segments = _make_segments(10)
        transcript = {"language": "de", "segments": segments}

        resp = requests.post(
            f"{SERVICE_URL}/process_transcript",
            json={
                "transcript": transcript,
                "filename": "small-test.m4a",
                "contexts": [TEST_CONTEXT],
            },
            timeout=120,
        )
        assert resp.status_code == 200, f"Failed: {resp.text}"
        data = resp.json()

        corrected = data["corrected_json"]
        assert corrected["language"] == "de"
        assert len(corrected["segments"]) == 10

        # Mock echoes text back — verify all segments present
        for i, seg in enumerate(corrected["segments"]):
            assert f"Segment {i}" in seg["text"]
            assert "start" in seg
            assert "end" in seg

    @pytest.mark.timeout(300)
    def test_large_transcript_chunked(self):
        """A large transcript (>48k output tokens) is split into chunks.

        With ~200 chars per segment and 4 chars/token, we need
        ~960 segments to exceed 48k tokens (960 * 200 / 4 = 48000).
        Use 1200 segments to clearly exceed the limit.
        """
        n_segments = 1200
        segments = _make_segments(n_segments)
        transcript = {"language": "de", "segments": segments}

        total_chars = sum(len(s["text"]) for s in segments)
        assert total_chars > 192_000, f"Not enough chars to trigger chunking: {total_chars}"

        resp = requests.post(
            f"{SERVICE_URL}/process_transcript",
            json={
                "transcript": transcript,
                "filename": "large-test.m4a",
                "contexts": [TEST_CONTEXT],
            },
            timeout=300,
        )
        assert resp.status_code == 200, f"Failed: {resp.text}"
        data = resp.json()

        corrected = data["corrected_json"]
        assert corrected["language"] == "de"
        assert len(corrected["segments"]) == n_segments, (
            f"Expected {n_segments} segments, got {len(corrected['segments'])}"
        )

        # Verify all segments are present and in order
        for i, seg in enumerate(corrected["segments"]):
            assert f"Segment {i}" in seg["text"], (
                f"Segment {i} text mismatch: {seg['text'][:80]}"
            )
            assert seg["start"] == round(i * 3.0, 2)
            assert seg["end"] == round(i * 3.0 + 2.8, 2)

    @pytest.mark.timeout(120)
    def test_segment_metadata_preserved(self):
        """Timestamps and speaker tags survive the chunking round-trip."""
        segments = _make_segments(20)
        transcript = {"language": "de", "segments": segments}

        resp = requests.post(
            f"{SERVICE_URL}/process_transcript",
            json={
                "transcript": transcript,
                "filename": "metadata-test.m4a",
                "contexts": [TEST_CONTEXT],
            },
            timeout=120,
        )
        assert resp.status_code == 200
        corrected = resp.json()["corrected_json"]

        for i, seg in enumerate(corrected["segments"]):
            assert seg["start"] == segments[i]["start"]
            assert seg["end"] == segments[i]["end"]
