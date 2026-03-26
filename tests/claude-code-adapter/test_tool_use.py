"""Tests for tool-use responses (the primary use case in this project)."""

import json

import pytest

from conftest import TEST_MODEL

CLASSIFY_TOOL = {
    "name": "classify_context",
    "description": "Classify the document into one of the available contexts.",
    "input_schema": {
        "type": "object",
        "properties": {
            "context": {
                "type": "string",
                "enum": ["work", "personal", "medical"],
                "description": "The context this document belongs to.",
            },
        },
        "required": ["context"],
    },
}

EXTRACT_TOOL = {
    "name": "extract_metadata",
    "description": "Extract metadata from the document.",
    "input_schema": {
        "type": "object",
        "properties": {
            "type": {
                "type": "string",
                "enum": ["Invoice", "Contract", "Letter"],
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


@pytest.mark.timeout(120)
def test_forced_tool_returns_tool_use_block(post_messages):
    """When tool_choice forces a tool, response has a tool_use content block."""
    r = post_messages({
        "model": TEST_MODEL,
        "max_tokens": 256,
        "messages": [{"role": "user", "content": "This is a work invoice from ACME Corp dated 2025-01-15."}],
        "tools": [CLASSIFY_TOOL],
        "tool_choice": {"type": "tool", "name": "classify_context"},
    })
    assert r.status_code == 200
    body = r.json()

    assert body["type"] == "message"
    assert body["stop_reason"] == "tool_use"

    content = body["content"]
    assert len(content) >= 1

    tool_block = content[0]
    assert tool_block["type"] == "tool_use"
    assert tool_block["name"] == "classify_context"
    assert tool_block["id"].startswith("toolu_")

    inp = tool_block["input"]
    assert "context" in inp
    assert inp["context"] in ("work", "personal", "medical")


@pytest.mark.timeout(120)
def test_classify_picks_correct_context(post_messages):
    """Model picks a reasonable context given a clear document."""
    r = post_messages({
        "model": TEST_MODEL,
        "max_tokens": 256,
        "system": "You are a document classification assistant.",
        "messages": [{"role": "user", "content": (
            "Classify this document:\n\n"
            "Patient: John Smith\nDiagnosis: Seasonal allergies\n"
            "Prescribed: Cetirizine 10mg daily\nDoctor: Dr. Brown"
        )}],
        "tools": [CLASSIFY_TOOL],
        "tool_choice": {"type": "tool", "name": "classify_context"},
    })
    assert r.status_code == 200
    inp = r.json()["content"][0]["input"]
    assert inp["context"] == "medical"


@pytest.mark.timeout(120)
def test_extract_metadata_returns_required_fields(post_messages):
    """Extraction tool returns all required fields with plausible values."""
    r = post_messages({
        "model": TEST_MODEL,
        "max_tokens": 512,
        "system": "You are a document metadata extraction assistant.",
        "messages": [{"role": "user", "content": (
            "Extract metadata from this document:\n\n"
            "INVOICE #12345\nFrom: ACME Corporation\nDate: March 15, 2025\n"
            "To: John Smith\nAmount: $1,234.56\nDue: April 15, 2025"
        )}],
        "tools": [EXTRACT_TOOL],
        "tool_choice": {"type": "tool", "name": "extract_metadata"},
    })
    assert r.status_code == 200
    body = r.json()
    assert body["stop_reason"] == "tool_use"

    inp = body["content"][0]["input"]
    assert inp.get("type") == "Invoice"
    assert "ACME" in inp.get("sender", "")
    assert "2025" in inp.get("date", "")


@pytest.mark.timeout(120)
def test_auto_tool_choice_with_single_tool(post_messages):
    """tool_choice 'auto' still results in the tool being called."""
    r = post_messages({
        "model": TEST_MODEL,
        "max_tokens": 256,
        "messages": [{"role": "user", "content": "This is a personal letter from my mother."}],
        "tools": [CLASSIFY_TOOL],
        "tool_choice": {"type": "auto"},
    })
    assert r.status_code == 200
    body = r.json()
    assert body["stop_reason"] == "tool_use"
    assert body["content"][0]["type"] == "tool_use"
    assert body["content"][0]["name"] == "classify_context"


@pytest.mark.timeout(120)
def test_enum_constraint_respected(post_messages):
    """Model respects the enum constraint in tool schema."""
    r = post_messages({
        "model": TEST_MODEL,
        "max_tokens": 256,
        "messages": [{"role": "user", "content": "This is a work email about project deadlines."}],
        "tools": [CLASSIFY_TOOL],
        "tool_choice": {"type": "tool", "name": "classify_context"},
    })
    assert r.status_code == 200
    context = r.json()["content"][0]["input"]["context"]
    assert context in ("work", "personal", "medical"), f"Got unexpected context: {context}"
