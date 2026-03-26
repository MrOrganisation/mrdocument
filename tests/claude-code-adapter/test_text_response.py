"""Tests for plain text (non-tool) responses via the adapter."""

import pytest

from conftest import TEST_MODEL


@pytest.mark.timeout(120)
def test_simple_text_response(post_messages):
    """Adapter returns a well-formed Messages API text response."""
    r = post_messages({
        "model": TEST_MODEL,
        "max_tokens": 64,
        "messages": [{"role": "user", "content": "Reply with exactly: PONG"}],
    })
    assert r.status_code == 200
    body = r.json()

    assert body["type"] == "message"
    assert body["role"] == "assistant"
    assert body["stop_reason"] == "end_turn"
    assert body["model"] == TEST_MODEL

    # Content structure
    assert isinstance(body["content"], list)
    assert len(body["content"]) >= 1
    assert body["content"][0]["type"] == "text"
    assert "PONG" in body["content"][0]["text"]

    # Usage present
    assert "usage" in body
    assert body["usage"]["input_tokens"] > 0
    assert body["usage"]["output_tokens"] > 0

    # ID format
    assert body["id"].startswith("msg_")


@pytest.mark.timeout(120)
def test_system_prompt_is_respected(post_messages):
    """System prompt is forwarded to the CLI and influences the response."""
    r = post_messages({
        "model": TEST_MODEL,
        "max_tokens": 64,
        "system": "You are a bot that only ever replies with the word BANANA. Nothing else.",
        "messages": [{"role": "user", "content": "Hello"}],
    })
    assert r.status_code == 200
    body = r.json()
    text = body["content"][0]["text"]
    assert "BANANA" in text.upper()


@pytest.mark.timeout(120)
def test_multiline_response(post_messages):
    """Adapter handles multi-line model output."""
    r = post_messages({
        "model": TEST_MODEL,
        "max_tokens": 256,
        "messages": [{"role": "user", "content": "List exactly 3 fruits, one per line."}],
    })
    assert r.status_code == 200
    text = r.json()["content"][0]["text"]
    lines = [l.strip() for l in text.strip().splitlines() if l.strip()]
    assert len(lines) >= 3
