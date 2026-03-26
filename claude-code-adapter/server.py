#!/usr/bin/env python3
"""
Claude Code Adapter — translates Anthropic Messages API requests into
claude CLI invocations.

Listens on :8080 (configurable via PORT env var) and exposes:
  GET  /health        — health check
  POST /v1/messages   — Anthropic Messages API (subset used by this project)

Internally runs `claude --print --output-format json` with the mounted
local credentials (~/.claude/).
"""

import json
import logging
import os
import re
import subprocess
import uuid
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("claude-code-adapter")

CLAUDE_BINARY = os.environ.get("CLAUDE_BINARY", "claude")
CLI_TIMEOUT = int(os.environ.get("CLI_TIMEOUT", "3600"))


# ---------------------------------------------------------------------------
# HTTP handler
# ---------------------------------------------------------------------------

class ClaudeCodeHandler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        log.debug(fmt, *args)

    # --- routing ---

    def do_GET(self):
        if self.path == "/health":
            self._json_response(200, {"status": "ok"})
        else:
            self._json_response(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/v1/messages":
            self._json_response(404, {"error": "not found"})
            return
        try:
            body = self._read_body()
            request = json.loads(body)
            log.info(
                "POST /v1/messages  model=%s  stream=%s",
                request.get("model"),
                request.get("stream", False),
            )
            response = self._handle_messages(request)
            if request.get("stream"):
                self._sse_response(response)
            else:
                self._json_response(200, response)
        except Exception as e:
            log.exception("Error handling request")
            self._json_response(500, {
                "type": "error",
                "error": {"type": "server_error", "message": str(e)},
            })

    # --- core logic ---

    def _handle_messages(self, request):
        model = request.get("model", "sonnet")
        system_prompt = request.get("system", "")
        messages = request.get("messages", [])
        tools = request.get("tools", [])
        tool_choice = request.get("tool_choice", {})

        user_message = _extract_user_message(messages)
        prompt = _build_prompt(user_message, tools, tool_choice)
        result_text, cost_usd = _invoke_claude(prompt, system_prompt, model)
        return _build_response(result_text, model, tools, cost_usd, len(prompt))

    # --- response helpers ---

    def _read_body(self):
        length = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(length).decode("utf-8")

    def _json_response(self, status, data):
        body = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _sse_response(self, message):
        """Emit a fully-formed message as Anthropic-compatible SSE events."""
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()

        msg_id = message["id"]
        model = message["model"]
        usage = message.get("usage", {})
        content = message.get("content", [])
        stop_reason = message.get("stop_reason", "end_turn")

        self._sse_event("message_start", {
            "type": "message_start",
            "message": {
                "id": msg_id,
                "type": "message",
                "role": "assistant",
                "content": [],
                "model": model,
                "stop_reason": None,
                "stop_sequence": None,
                "usage": {
                    "input_tokens": usage.get("input_tokens", 0),
                    "output_tokens": 0,
                },
            },
        })

        for idx, block in enumerate(content):
            if block["type"] == "tool_use":
                self._sse_event("content_block_start", {
                    "type": "content_block_start",
                    "index": idx,
                    "content_block": {
                        "type": "tool_use",
                        "id": block["id"],
                        "name": block["name"],
                        "input": {},
                    },
                })
                self._sse_event("content_block_delta", {
                    "type": "content_block_delta",
                    "index": idx,
                    "delta": {
                        "type": "input_json_delta",
                        "partial_json": json.dumps(block["input"]),
                    },
                })
            elif block["type"] == "text":
                self._sse_event("content_block_start", {
                    "type": "content_block_start",
                    "index": idx,
                    "content_block": {"type": "text", "text": ""},
                })
                self._sse_event("content_block_delta", {
                    "type": "content_block_delta",
                    "index": idx,
                    "delta": {"type": "text_delta", "text": block["text"]},
                })

            self._sse_event("content_block_stop", {
                "type": "content_block_stop",
                "index": idx,
            })

        self._sse_event("message_delta", {
            "type": "message_delta",
            "delta": {"stop_reason": stop_reason, "stop_sequence": None},
            "usage": {"output_tokens": usage.get("output_tokens", 0)},
        })

        self._sse_event("message_stop", {"type": "message_stop"})

    def _sse_event(self, event, data):
        line = f"event: {event}\ndata: {json.dumps(data)}\n\n"
        self.wfile.write(line.encode("utf-8"))
        self.wfile.flush()


# ---------------------------------------------------------------------------
# Prompt construction
# ---------------------------------------------------------------------------

def _extract_user_message(messages):
    """Concatenate all user message text from the Messages API messages list."""
    parts = []
    for msg in messages:
        if msg.get("role") != "user":
            continue
        content = msg.get("content", "")
        if isinstance(content, str):
            parts.append(content)
        elif isinstance(content, list):
            for block in content:
                if isinstance(block, dict) and block.get("type") == "text":
                    parts.append(block["text"])
    return "\n\n".join(parts)


def _build_prompt(user_message, tools, tool_choice):
    """
    When tools are present, embed the tool schema in the prompt so that the
    model responds with a JSON object matching the schema.
    """
    if not tools:
        return user_message

    tool = tools[0]
    tool_name = tool["name"]
    tool_desc = tool.get("description", "")
    schema = tool.get("input_schema", {})
    properties = schema.get("properties", {})
    required = schema.get("required", [])

    instruction = (
        f"\n\n---\n"
        f"INSTRUCTION: You must call the '{tool_name}' tool.\n"
        f"Tool description: {tool_desc}\n\n"
        f"Respond with ONLY a valid JSON object with these properties:\n"
        f"{json.dumps(properties, indent=2)}\n\n"
        f"Required fields: {json.dumps(required)}\n\n"
        f"IMPORTANT: Output ONLY the raw JSON object. "
        f"No markdown code fences, no explanation, no text before or after the JSON."
    )

    return user_message + instruction


# ---------------------------------------------------------------------------
# Claude CLI invocation
# ---------------------------------------------------------------------------

def _invoke_claude(prompt, system_prompt, model):
    """Run the claude CLI and return (result_text, cost_usd)."""
    cmd = [
        CLAUDE_BINARY, "--print",
        "--output-format", "json",
        "--model", model,
        "--max-turns", "1",
    ]
    if system_prompt:
        cmd.extend(["--system-prompt", system_prompt])

    log.info(
        "Invoking claude CLI  model=%s  prompt_len=%d  system_len=%d  timeout=%ds",
        model, len(prompt), len(system_prompt), CLI_TIMEOUT,
    )

    try:
        proc = subprocess.run(
            cmd,
            input=prompt,
            capture_output=True,
            text=True,
            timeout=CLI_TIMEOUT,
        )
    except subprocess.TimeoutExpired as exc:
        stderr = (exc.stderr or "").strip() if isinstance(exc.stderr, str) else ""
        stdout = (exc.stdout or "").strip() if isinstance(exc.stdout, str) else ""
        log.error(
            "claude CLI timed out after %ds  stderr=%s  stdout=%.500s",
            CLI_TIMEOUT, stderr, stdout,
        )
        raise RuntimeError(
            f"claude CLI timed out after {CLI_TIMEOUT}s. stderr: {stderr}"
        ) from exc

    if proc.returncode != 0:
        stderr = proc.stderr.strip()
        stdout = proc.stdout.strip()
        detail = stderr or stdout or "(no output)"
        log.error("claude CLI failed (rc=%d): %s", proc.returncode, detail)
        raise RuntimeError(f"claude CLI exited with code {proc.returncode}: {detail}")

    try:
        output = json.loads(proc.stdout)
    except json.JSONDecodeError:
        log.error(
            "claude CLI returned non-JSON  stdout=%.500s  stderr=%.500s",
            proc.stdout.strip(), proc.stderr.strip(),
        )
        raise RuntimeError(
            f"claude CLI returned non-JSON output: {proc.stdout[:500]}"
        )

    result_text = output.get("result", "")
    cost_usd = output.get("cost_usd", 0.0)
    is_error = output.get("is_error", False)

    log.info(
        "claude CLI responded  result_len=%d  cost=$%.6f  is_error=%s  "
        "duration_ms=%s  num_turns=%s  session=%s",
        len(result_text), cost_usd, is_error,
        output.get("duration_ms"), output.get("num_turns"),
        output.get("session_id", "")[:12],
    )

    if is_error:
        log.error("claude CLI reported error: %s", result_text)
        raise RuntimeError(f"claude CLI error: {result_text or '(empty error)'}")

    if not result_text:
        log.error(
            "claude CLI returned empty result  full_output=%s",
            json.dumps(output, indent=2),
        )
        raise RuntimeError(
            f"claude CLI returned empty result. "
            f"Session: {output.get('session_id', 'unknown')}, "
            f"turns: {output.get('num_turns', '?')}, "
            f"subtype: {output.get('subtype', '?')}"
        )

    return result_text, cost_usd


# ---------------------------------------------------------------------------
# Response construction
# ---------------------------------------------------------------------------

def _extract_json(text):
    """Best-effort extraction of a JSON object from model text output."""
    text = text.strip()

    # 1. Try direct parse
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # 2. Strip markdown code fences
    md = re.search(r"```(?:json)?\s*\n?(.*?)\n?\s*```", text, re.DOTALL)
    if md:
        try:
            return json.loads(md.group(1).strip())
        except json.JSONDecodeError:
            pass

    # 3. Find outermost { ... }
    depth = 0
    start = None
    for i, ch in enumerate(text):
        if ch == "{":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start is not None:
                try:
                    return json.loads(text[start : i + 1])
                except json.JSONDecodeError:
                    start = None

    log.warning("Failed to extract JSON from response: %.200s", text)
    return {}


def _build_response(result_text, model, tools, cost_usd, prompt_len):
    """Build an Anthropic Messages API compatible response."""
    msg_id = f"msg_{uuid.uuid4().hex[:24]}"

    # Rough token estimates (≈4 chars/token)
    input_tokens = max(1, prompt_len // 4)
    output_tokens = max(1, len(result_text) // 4)

    if tools:
        tool = tools[0]
        tool_input = _extract_json(result_text)
        tool_id = f"toolu_{uuid.uuid4().hex[:24]}"

        return {
            "id": msg_id,
            "type": "message",
            "role": "assistant",
            "content": [
                {
                    "type": "tool_use",
                    "id": tool_id,
                    "name": tool["name"],
                    "input": tool_input,
                }
            ],
            "model": model,
            "stop_reason": "tool_use",
            "usage": {
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
            },
        }

    return {
        "id": msg_id,
        "type": "message",
        "role": "assistant",
        "content": [{"type": "text", "text": result_text}],
        "model": model,
        "stop_reason": "end_turn",
        "usage": {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
        },
    }


# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


def main():
    port = int(os.environ.get("PORT", "8080"))
    server = ThreadedHTTPServer(("0.0.0.0", port), ClaudeCodeHandler)
    log.info("Claude Code adapter listening on :%d", port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Shutting down")
        server.shutdown()


if __name__ == "__main__":
    main()
