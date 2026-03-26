"""Tests for large payloads that require a model with >200k context.

These use claude-opus-4-6[1M] (1M context window) since the payloads
exceed what haiku/sonnet can handle.
"""

import json
import os
import random
import string

import pytest

from conftest import TEST_MODEL

LARGE_MODEL = os.environ.get("LARGE_MODEL", "claude-opus-4-6[1M]")


def _make_transcript_segments(n_segments, chars_per_segment=200):
    """Generate a realistic transcript JSON payload."""
    segments = []
    for i in range(n_segments):
        # Mix of German-ish filler to approximate real transcripts
        words = " ".join(
            "".join(random.choices(string.ascii_lowercase, k=random.randint(3, 12)))
            for _ in range(chars_per_segment // 7)
        )
        segments.append({
            "start": round(i * 3.0, 2),
            "end": round(i * 3.0 + 2.8, 2),
            "text": f"Segment {i}: {words}",
            "speaker": f"SPEAKER_{i % 3}",
        })
    return segments


def _build_correction_payload(segments):
    """Build the same payload that transcript.rs sends."""
    text_array = [seg["text"] for seg in segments]
    text_json = json.dumps(text_array, indent=2, ensure_ascii=False)

    prompt = (
        "You are a transcript correction assistant.\n"
        "Correct the following transcript segments. Fix typos, grammar, "
        "punctuation, and capitalization. Return a JSON array of corrected "
        "strings in the same order. Output ONLY the JSON array.\n\n"
        f"Text segments to correct:\n```json\n{text_json}\n```"
    )
    return prompt, len(text_array)


@pytest.mark.timeout(3600)
def test_large_transcript_correction():
    """A ~250k char transcript (similar to the production failure case)
    is processed via the file-based approach without context overflow."""
    # ~1200 segments * ~200 chars ≈ 240k chars, matching the real failure
    segments = _make_transcript_segments(1200)
    payload, n_segments = _build_correction_payload(segments)

    assert len(payload) > 200_000, f"Payload too small: {len(payload)} chars"

    import requests
    from conftest import ADAPTER_URL

    r = requests.post(
        f"{ADAPTER_URL}/v1/messages",
        json={
            "model": LARGE_MODEL,
            "max_tokens": 128000,
            "stream": True,
            "messages": [{"role": "user", "content": payload}],
        },
        headers={
            "x-api-key": "dummy",
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        timeout=600,
    )
    assert r.status_code == 200
    body = r.json()

    assert body["type"] == "message"
    assert body["content"][0]["type"] == "text"
    result = body["content"][0]["text"]
    assert len(result) > 0, "Empty result from large transcript correction"


@pytest.mark.timeout(3600)
def test_large_document_extraction():
    """A >200k char document payload with tool_use extraction still works."""
    # Build a large "document" by repeating realistic invoice text
    invoice_block = (
        "Rechnung Nr. 2025-4711\n"
        "Von: Müller & Söhne GmbH, Industriestraße 42, 80331 München\n"
        "An: Max Mustermann, Beispielweg 7, 10115 Berlin\n"
        "Datum: 15.03.2025\n"
        "1x Beratungsdienstleistung IT-Infrastruktur  2.500,00 EUR\n"
        "1x Netzwerk-Audit                           1.200,00 EUR\n"
        "Gesamt (brutto): 4.403,00 EUR\n"
        "---\n"
    )
    # Repeat to exceed 200k chars
    repetitions = (200_001 // len(invoice_block)) + 1
    large_doc = invoice_block * repetitions
    assert len(large_doc) > 200_000

    extract_tool = {
        "name": "extract_metadata",
        "description": "Extract metadata from the document.",
        "input_schema": {
            "type": "object",
            "properties": {
                "type": {
                    "type": "string",
                    "enum": ["Rechnung", "Vertrag", "Brief"],
                    "description": "The document type.",
                },
                "sender": {
                    "type": "string",
                    "description": "The sender or author.",
                },
                "date": {
                    "type": "string",
                    "description": "Document date in YYYY-MM-DD format.",
                },
            },
            "required": ["type", "sender"],
        },
    }

    import requests
    from conftest import ADAPTER_URL

    r = requests.post(
        f"{ADAPTER_URL}/v1/messages",
        json={
            "model": LARGE_MODEL,
            "max_tokens": 512,
            "system": "You are a document metadata extraction assistant.",
            "messages": [{"role": "user", "content": f"Extract metadata from this document:\n\n{large_doc}"}],
            "tools": [extract_tool],
            "tool_choice": {"type": "tool", "name": "extract_metadata"},
        },
        headers={
            "x-api-key": "dummy",
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        timeout=600,
    )
    assert r.status_code == 200
    body = r.json()
    assert body["stop_reason"] == "tool_use"

    inp = body["content"][0]["input"]
    assert inp["type"] == "Rechnung"
    assert "Müller" in inp.get("sender", "") or "Muller" in inp.get("sender", "")
