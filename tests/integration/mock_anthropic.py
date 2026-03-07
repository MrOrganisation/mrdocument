"""Mock Anthropic API backend for integration tests.

Provides a mock Anthropic Messages API that returns canned responses
based on document metadata extracted from the request.

Endpoints:
    GET  /health       - Health check
    POST /v1/messages  - Mock Anthropic Messages API

Usage:
    gunicorn --bind 0.0.0.0:8080 --workers 2 --timeout 30 mock_anthropic:app
"""

import json
import re
import uuid

from flask import Flask, Response, jsonify, request

app = Flask("mock_anthropic")

# ---------------------------------------------------------------------------
# Document metadata
# ---------------------------------------------------------------------------
DOCUMENT_METADATA = {
    "arbeit_rechnung_schulze": {
        "context": "arbeit",
        "type": "Rechnung",
        "sender": "Schulze GmbH",
        "date": "2025-03-15",
    },
    "arbeit_vertrag_fischer": {
        "context": "arbeit",
        "type": "Vertrag",
        "sender": "Fischer AG",
        "date": "2025-06-01",
    },
    "arbeit_rechnung_keller": {
        "context": "arbeit",
        "type": "Rechnung",
        "sender": "Keller und Partner",
        "date": "2025-05-12",
    },
    "arbeit_angebot_fischer": {
        "context": "arbeit",
        "type": "Angebot",
        "sender": "Fischer AG",
        "date": "2025-08-05",
    },
    "arbeit_angebot_keller": {
        "context": "arbeit",
        "type": "Angebot",
        "sender": "Keller und Partner",
        "date": "2025-09-20",
    },
    "privat_arztbrief_braun": {
        "context": "privat",
        "type": "Arztbrief",
        "sender": "Dr. Braun",
        "date": "2025-04-10",
    },
    "privat_versicherung_allianz": {
        "context": "privat",
        "type": "Versicherung",
        "sender": "Allianz",
        "date": "2025-07-22",
    },
    "privat_kontoauszug_sparkasse": {
        "context": "privat",
        "type": "Kontoauszug",
        "sender": "Sparkasse",
        "date": "2025-11-30",
    },
    "privat_arztbrief_mueller": {
        "context": "privat",
        "type": "Arztbrief",
        "sender": "Dr. Mueller",
        "date": "2025-10-15",
    },
    "privat_kontoauszug_allianz": {
        "context": "privat",
        "type": "Kontoauszug",
        "sender": "Allianz",
        "date": "2025-02-18",
    },
    # Audio files (mp3)
    "besprechung-intro": {
        "context": "arbeit",
        "type": "Besprechung",
        "sender": "Schulze GmbH",
        "date": "2025-03-15",
    },
    "arztgespraech-intro": {
        "context": "privat",
        "type": "Arztgespraech",
        "sender": "Dr. Braun",
        "date": "2025-04-10",
    },
    "telefonat": {
        "context": "arbeit",
        "type": "Telefonat",
        "sender": "Keller und Partner",
        "date": "2025-09-20",
    },
    "telefonat-pattern": {
        "context": "arbeit",
        "type": "Telefonat",
        "sender": "Schulze GmbH",
        "date": "2025-09-20",
    },
    # Lifecycle integration tests (inline txt files)
    "dup_incoming_doc": {
        "context": "arbeit",
        "type": "Rechnung",
        "sender": "Schulze GmbH",
        "date": "2025-12-01",
    },
    "dup_sorted_doc": {
        "context": "privat",
        "type": "Arztbrief",
        "sender": "Dr. Mueller",
        "date": "2025-12-05",
    },
    "missing_test_doc": {
        "context": "privat",
        "type": "Versicherung",
        "sender": "Allianz",
        "date": "2025-12-15",
    },
    "trash_test_doc": {
        "context": "arbeit",
        "type": "Angebot",
        "sender": "Keller und Partner",
        "date": "2025-12-20",
    },
    "rename_test_doc": {
        "context": "arbeit",
        "type": "Rechnung",
        "sender": "Schulze GmbH",
        "date": "2025-12-25",
    },
    "move_context_doc": {
        "context": "arbeit",
        "type": "Vertrag",
        "sender": "Fischer AG",
        "date": "2025-12-28",
    },
    "stray_archive_doc": {
        "context": "arbeit",
        "type": "Rechnung",
        "sender": "Schulze GmbH",
        "date": "2025-12-30",
    },
    # Quick succession duplicate tests
    "quick_dup_doc": {
        "context": "arbeit",
        "type": "Angebot",
        "sender": "Schulze GmbH",
        "date": "2025-12-02",
    },
    "quick_dup_copy": {
        "context": "arbeit",
        "type": "Angebot",
        "sender": "Schulze GmbH",
        "date": "2025-12-02",
    },
    # Error handling / recovery tests
    "error_test_doc": {
        "context": "arbeit",
        "type": "Rechnung",
        "sender": "Fischer AG",
        "date": "2025-12-08",
    },
    # Smart folder lifecycle tests
    "sf_move_doc": {
        "context": "arbeit",
        "type": "Rechnung",
        "sender": "Schulze GmbH",
        "date": "2025-12-10",
    },
    "sf_trash_doc": {
        "context": "arbeit",
        "type": "Rechnung",
        "sender": "Keller und Partner",
        "date": "2025-12-12",
    },
    # Video files (mov, mp4)
    "videocall": {
        "context": "arbeit",
        "type": "Besprechung",
        "sender": "Fischer AG",
        "date": "2025-05-12",
    },
    "sprachnachricht": {
        "context": "privat",
        "type": "Sprachnachricht",
        "sender": "Dr. Mueller",
        "date": "2025-06-18",
    },
}


def _lookup_document(user_message: str) -> dict | None:
    """Extract filename from user message and look up metadata."""
    match = re.search(r"Original filename:\s*(\S+)", user_message)
    if not match:
        return None
    filename = match.group(1)
    # Strip extension and normalise to lookup key
    stem = re.sub(r"\.[^.]+$", "", filename)
    return DOCUMENT_METADATA.get(stem)


def _anthropic_response(tool_name: str, tool_input: dict, tool_id: str | None = None) -> dict:
    """Build a response matching the Anthropic Messages API spec."""
    return {
        "id": f"msg_{uuid.uuid4().hex[:24]}",
        "type": "message",
        "role": "assistant",
        "content": [
            {
                "type": "tool_use",
                "id": tool_id or f"toolu_{uuid.uuid4().hex[:24]}",
                "name": tool_name,
                "input": tool_input,
            }
        ],
        "model": "mock-model",
        "stop_reason": "tool_use",
        "usage": {"input_tokens": 100, "output_tokens": 50},
    }


def _extract_user_message(body: dict) -> str:
    """Extract user message text from Anthropic request body."""
    for msg in body.get("messages", []):
        if msg.get("role") == "user":
            content = msg.get("content", "")
            if isinstance(content, str):
                return content
            if isinstance(content, list):
                parts = []
                for block in content:
                    if isinstance(block, dict) and block.get("type") == "text":
                        parts.append(block.get("text", ""))
                return "".join(parts)
    return ""


def _text_response(text: str) -> dict:
    """Build a non-tool text response matching the Anthropic Messages API."""
    return {
        "id": f"msg_{uuid.uuid4().hex[:24]}",
        "type": "message",
        "role": "assistant",
        "content": [{"type": "text", "text": text}],
        "model": "mock-model",
        "stop_reason": "end_turn",
        "usage": {"input_tokens": 100, "output_tokens": 50},
    }


def _sse_text_response(text: str) -> Response:
    """Return a streaming SSE response with a text content block."""
    msg_id = f"msg_{uuid.uuid4().hex[:24]}"

    def generate():
        yield f"event: message_start\ndata: {json.dumps({'type': 'message_start', 'message': {'id': msg_id, 'type': 'message', 'role': 'assistant', 'content': [], 'model': 'mock-model', 'usage': {'input_tokens': 100, 'output_tokens': 0}}})}\n\n"
        yield f"event: content_block_start\ndata: {json.dumps({'type': 'content_block_start', 'index': 0, 'content_block': {'type': 'text', 'text': ''}})}\n\n"
        yield f"event: content_block_delta\ndata: {json.dumps({'type': 'content_block_delta', 'index': 0, 'delta': {'type': 'text_delta', 'text': text}})}\n\n"
        yield f"event: content_block_stop\ndata: {json.dumps({'type': 'content_block_stop', 'index': 0})}\n\n"
        yield f"event: message_delta\ndata: {json.dumps({'type': 'message_delta', 'delta': {'stop_reason': 'end_turn'}, 'usage': {'output_tokens': 50}})}\n\n"
        yield f"event: message_stop\ndata: {json.dumps({'type': 'message_stop'})}\n\n"

    return Response(generate(), mimetype="text/event-stream")


def _sse_tool_response(tool_name: str, tool_input: dict) -> Response:
    """Return a streaming SSE response with a tool_use content block."""
    msg_id = f"msg_{uuid.uuid4().hex[:24]}"
    tool_id = f"toolu_{uuid.uuid4().hex[:24]}"
    input_json = json.dumps(tool_input)

    def generate():
        yield f"event: message_start\ndata: {json.dumps({'type': 'message_start', 'message': {'id': msg_id, 'type': 'message', 'role': 'assistant', 'content': [], 'model': 'mock-model', 'usage': {'input_tokens': 100, 'output_tokens': 0}}})}\n\n"
        yield f"event: content_block_start\ndata: {json.dumps({'type': 'content_block_start', 'index': 0, 'content_block': {'type': 'tool_use', 'id': tool_id, 'name': tool_name, 'input': {}}})}\n\n"
        yield f"event: content_block_delta\ndata: {json.dumps({'type': 'content_block_delta', 'index': 0, 'delta': {'type': 'input_json_delta', 'partial_json': input_json}})}\n\n"
        yield f"event: content_block_stop\ndata: {json.dumps({'type': 'content_block_stop', 'index': 0})}\n\n"
        yield f"event: message_delta\ndata: {json.dumps({'type': 'message_delta', 'delta': {'stop_reason': 'tool_use'}, 'usage': {'output_tokens': 50}})}\n\n"
        yield f"event: message_stop\ndata: {json.dumps({'type': 'message_stop'})}\n\n"

    return Response(generate(), mimetype="text/event-stream")


def _build_correction_text(user_message: str) -> str:
    """Build a mock transcript correction response.

    The real service sends a JSON array of segment texts for correction.
    We echo them back unchanged (mock "correction").
    """
    match = re.search(r"```json\s*(\[[\s\S]*?\])\s*```", user_message)
    if match:
        return match.group(1)
    return '["Mock corrected transcript."]'


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "service": "mock-anthropic"})


@app.route("/v1/messages", methods=["POST"])
def mock_anthropic_messages():
    """Mock Anthropic Messages API — returns tool_use or text based on request."""
    body = request.get_json(silent=True)
    if not body:
        return jsonify({"error": "Invalid JSON"}), 400

    is_streaming = body.get("stream", False)

    # Determine which tool is being requested
    tool_choice = body.get("tool_choice", {})
    tool_name = tool_choice.get("name", "")

    # For tool_choice: auto, infer tool from available tools
    if not tool_name and tool_choice.get("type") == "auto":
        tools = body.get("tools", [])
        if tools:
            tool_name = tools[0].get("name", "")

    user_message = _extract_user_message(body)
    meta = _lookup_document(user_message)

    # No tool requested — text generation (e.g. transcript correction)
    if not tool_name:
        text = _build_correction_text(user_message)
        if is_streaming:
            return _sse_text_response(text)
        return jsonify(_text_response(text))

    # Resolve tool response payload
    if tool_name == "classify_context":
        context = meta["context"] if meta else "privat"
        # Respect the tool schema's enum — pick from available contexts
        tools = body.get("tools", [])
        for t in tools:
            if t.get("name") == "classify_context":
                schema = t.get("input_schema", {})
                ctx_prop = schema.get("properties", {}).get("context", {})
                allowed = ctx_prop.get("enum", [])
                if allowed and context not in allowed:
                    context = allowed[0]
                break
        tool_input = {"context": context}
    elif tool_name == "extract_metadata":
        if meta:
            tool_input = {
                "type": meta["type"],
                "date": meta["date"],
                "sender": meta["sender"],
            }
        else:
            tool_input = {
                "type": "Sonstiges",
                "date": "2025-01-01",
                "sender": "Unbekannt",
            }
    else:
        # Fallback for unknown tools
        if meta:
            tool_input = {
                "context": meta["context"],
                "type": meta.get("type", "Sonstiges"),
                "date": meta.get("date", "2025-01-01"),
                "sender": meta.get("sender", "Unbekannt"),
            }
        else:
            tool_input = {
                "context": "privat",
                "type": "Sonstiges",
                "date": "2025-01-01",
                "sender": "Unbekannt",
            }
        tool_name = tool_name or "extract_metadata"

    if is_streaming:
        return _sse_tool_response(tool_name, tool_input)
    return jsonify(_anthropic_response(tool_name, tool_input))
