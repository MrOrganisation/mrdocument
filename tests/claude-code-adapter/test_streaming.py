"""Tests for SSE streaming responses.

The Rust service uses streaming when extended_thinking is enabled.
It parses the SSE events and reconstructs the full message, so the
adapter's SSE output must be compatible with parse_sse_response().
"""

import json

import pytest

from conftest import TEST_MODEL


def _parse_sse_events(response):
    """Parse SSE events from a streaming response, mirroring the Rust parser."""
    events = []
    for line in response.iter_lines(decode_unicode=True):
        if line and line.startswith("data: "):
            data = json.loads(line[6:])
            events.append(data)
    return events


def _reconstruct_message(events):
    """Reconstruct a Messages API response from SSE events (like parse_sse_response)."""
    message_id = ""
    model = ""
    content_blocks = []
    current_block_type = ""
    current_tool_name = ""
    current_tool_id = ""
    text_accum = ""
    json_accum = ""
    input_tokens = 0
    output_tokens = 0
    stop_reason = ""

    for event in events:
        etype = event.get("type", "")

        if etype == "message_start":
            msg = event.get("message", {})
            message_id = msg.get("id", "")
            model = msg.get("model", "")
            usage = msg.get("usage", {})
            input_tokens = usage.get("input_tokens", 0)

        elif etype == "content_block_start":
            cb = event.get("content_block", {})
            current_block_type = cb.get("type", "")
            if current_block_type == "tool_use":
                current_tool_name = cb.get("name", "")
                current_tool_id = cb.get("id", "")
                json_accum = ""
            else:
                text_accum = ""

        elif etype == "content_block_delta":
            delta = event.get("delta", {})
            dt = delta.get("type", "")
            if dt == "text_delta":
                text_accum += delta.get("text", "")
            elif dt == "input_json_delta":
                json_accum += delta.get("partial_json", "")

        elif etype == "content_block_stop":
            if current_block_type == "tool_use":
                inp = json.loads(json_accum) if json_accum else {}
                content_blocks.append({
                    "type": "tool_use",
                    "id": current_tool_id,
                    "name": current_tool_name,
                    "input": inp,
                })
            elif current_block_type == "text":
                content_blocks.append({
                    "type": "text",
                    "text": text_accum,
                })
            current_block_type = ""

        elif etype == "message_delta":
            delta = event.get("delta", {})
            stop_reason = delta.get("stop_reason", "")
            usage = event.get("usage", {})
            output_tokens = usage.get("output_tokens", 0)

    return {
        "id": message_id,
        "type": "message",
        "role": "assistant",
        "content": content_blocks,
        "model": model,
        "stop_reason": stop_reason,
        "usage": {"input_tokens": input_tokens, "output_tokens": output_tokens},
    }


@pytest.mark.timeout(120)
def test_streaming_text_response(post_messages):
    """SSE stream for a text response reconstructs to valid Messages API format."""
    r = post_messages(
        {
            "model": TEST_MODEL,
            "max_tokens": 64,
            "stream": True,
            "messages": [{"role": "user", "content": "Reply with exactly: PONG"}],
        },
        stream=True,
    )
    assert r.status_code == 200
    assert "text/event-stream" in r.headers.get("Content-Type", "")

    events = _parse_sse_events(r)
    assert len(events) >= 4  # message_start, block_start, block_delta, block_stop, message_delta, message_stop

    msg = _reconstruct_message(events)
    assert msg["id"].startswith("msg_")
    assert msg["model"] == TEST_MODEL
    assert msg["stop_reason"] == "end_turn"
    assert len(msg["content"]) >= 1
    assert msg["content"][0]["type"] == "text"
    assert "PONG" in msg["content"][0]["text"]


@pytest.mark.timeout(120)
def test_streaming_tool_use_response(post_messages):
    """SSE stream for a tool_use response can be reconstructed by parse_sse_response."""
    classify_tool = {
        "name": "classify_context",
        "description": "Classify the document.",
        "input_schema": {
            "type": "object",
            "properties": {
                "context": {
                    "type": "string",
                    "enum": ["work", "personal"],
                    "description": "The context.",
                },
            },
            "required": ["context"],
        },
    }

    r = post_messages(
        {
            "model": TEST_MODEL,
            "max_tokens": 256,
            "stream": True,
            "messages": [{"role": "user", "content": "This is a work memo about Q3 targets."}],
            "tools": [classify_tool],
            "tool_choice": {"type": "tool", "name": "classify_context"},
        },
        stream=True,
    )
    assert r.status_code == 200

    events = _parse_sse_events(r)
    msg = _reconstruct_message(events)

    assert msg["stop_reason"] == "tool_use"
    assert len(msg["content"]) >= 1

    tool_block = msg["content"][0]
    assert tool_block["type"] == "tool_use"
    assert tool_block["name"] == "classify_context"
    assert tool_block["id"].startswith("toolu_")
    assert tool_block["input"]["context"] in ("work", "personal")


@pytest.mark.timeout(120)
def test_sse_event_ordering(post_messages):
    """SSE events follow the correct Anthropic protocol ordering."""
    r = post_messages(
        {
            "model": TEST_MODEL,
            "max_tokens": 64,
            "stream": True,
            "messages": [{"role": "user", "content": "Say hi."}],
        },
        stream=True,
    )
    assert r.status_code == 200

    events = _parse_sse_events(r)
    types = [e["type"] for e in events]

    assert types[0] == "message_start"
    assert types[-1] == "message_stop"
    assert "message_delta" in types
    assert "content_block_start" in types
    assert "content_block_stop" in types
