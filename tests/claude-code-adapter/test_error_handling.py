"""Tests for error handling and edge cases."""

import pytest

from conftest import ADAPTER_URL, TEST_MODEL

import requests


@pytest.mark.timeout(30)
def test_empty_body_returns_error():
    """Sending an empty body returns a 500 with structured error."""
    r = requests.post(
        f"{ADAPTER_URL}/v1/messages",
        data="",
        headers={"content-type": "application/json"},
        timeout=10,
    )
    assert r.status_code == 500
    body = r.json()
    assert body["type"] == "error"
    assert body["error"]["type"] == "server_error"


@pytest.mark.timeout(30)
def test_malformed_json_returns_error():
    """Sending invalid JSON returns a 500 with structured error."""
    r = requests.post(
        f"{ADAPTER_URL}/v1/messages",
        data="{not json}",
        headers={"content-type": "application/json"},
        timeout=10,
    )
    assert r.status_code == 500
    body = r.json()
    assert body["type"] == "error"


@pytest.mark.timeout(120)
def test_invalid_model_returns_error(post_messages):
    """Requesting a nonexistent model returns a structured error."""
    r = post_messages({
        "model": "claude-nonexistent-model-99",
        "max_tokens": 64,
        "messages": [{"role": "user", "content": "Hello"}],
    })
    assert r.status_code == 500
    body = r.json()
    assert body["type"] == "error"
    assert "message" in body["error"]


@pytest.mark.timeout(120)
def test_empty_messages_still_handled(post_messages):
    """Request with empty messages array returns a structured error (not a crash)."""
    r = post_messages({
        "model": TEST_MODEL,
        "max_tokens": 64,
        "messages": [],
    })
    # Might succeed with empty prompt or error — either way, must not crash
    assert r.status_code in (200, 500)
    body = r.json()
    if r.status_code == 500:
        assert body["type"] == "error"


@pytest.mark.timeout(120)
def test_error_response_matches_anthropic_format(post_messages):
    """Error responses use the Anthropic error envelope format."""
    r = post_messages({
        "model": "this-model-does-not-exist",
        "max_tokens": 64,
        "messages": [{"role": "user", "content": "Hello"}],
    })
    assert r.status_code == 500
    body = r.json()
    # Anthropic error format: {"type": "error", "error": {"type": "...", "message": "..."}}
    assert body["type"] == "error"
    assert "type" in body["error"]
    assert "message" in body["error"]
