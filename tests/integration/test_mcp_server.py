"""Integration tests for the MCP server.

Tests the full flow: health check, OAuth token exchange, SSE connection,
and tool calls through the MCP protocol.

Requires the integration stack to be running (docker-compose.fast.yaml)
with the mcp-server container.
"""

import json
import queue
import threading
import time

import pytest
import requests

MCP_URL = "http://localhost:8091"
HEALTH_URL = f"{MCP_URL}/health"
TOKEN_URL = f"{MCP_URL}/oauth/token"
SSE_URL = f"{MCP_URL}/sse"

# Test credentials — the watcher creates these on startup
USERNAME = "testuser"


# Override autouse fixture — no DB/filesystem reset needed
@pytest.fixture(autouse=True)
def reset_environment():
    yield


@pytest.fixture(scope="module")
def mcp_password():
    """Read the .mcp-password file created by the watcher."""
    import subprocess
    result = subprocess.run(
        [
            "docker", "exec", "integration-mrdocument-watcher-1",
            "cat", "/sync/testuser/.mcp-password",
        ],
        capture_output=True, text=True, timeout=10,
    )
    if result.returncode != 0:
        pytest.skip("No .mcp-password file found — watcher may not have created it")
    pw = result.stdout.strip()
    if not pw:
        pytest.skip(".mcp-password file is empty")
    return pw


@pytest.fixture(scope="module")
def access_token(mcp_password):
    """Get an OAuth access token."""
    r = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": USERNAME,
            "client_secret": mcp_password,
        },
        timeout=10,
    )
    assert r.status_code == 200, f"OAuth failed: {r.status_code} {r.text}"
    return r.json()["access_token"]


class TestHealthCheck:
    def test_health(self):
        r = requests.get(HEALTH_URL, timeout=5)
        assert r.status_code == 200
        assert r.json()["status"] == "healthy"


class TestOAuthMetadata:
    def test_well_known(self):
        r = requests.get(f"{MCP_URL}/.well-known/oauth-authorization-server", timeout=5)
        assert r.status_code == 200
        body = r.json()
        assert "token_endpoint" in body
        assert "client_credentials" in body["grant_types_supported"]


class TestOAuthToken:
    def test_valid_credentials(self, mcp_password):
        r = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": USERNAME,
                "client_secret": mcp_password,
            },
            timeout=10,
        )
        assert r.status_code == 200
        body = r.json()
        assert "access_token" in body
        assert body["token_type"] == "Bearer"
        assert body["expires_in"] > 0

    def test_wrong_password(self):
        r = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": USERNAME,
                "client_secret": "wrong-password",
            },
            timeout=10,
        )
        assert r.status_code == 401

    def test_wrong_grant_type(self, mcp_password):
        r = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "authorization_code",
                "client_id": USERNAME,
                "client_secret": mcp_password,
            },
            timeout=10,
        )
        assert r.status_code == 400

    def test_unknown_user(self):
        r = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": "nonexistent",
                "client_secret": "whatever",
            },
            timeout=10,
        )
        assert r.status_code == 401


class TestSSEConnection:
    def test_sse_without_auth_returns_401(self):
        r = requests.get(SSE_URL, timeout=5, stream=True)
        # Without auth, the handler should reject before SSE starts
        assert r.status_code == 401
        r.close()

    def test_sse_with_invalid_token_returns_401(self):
        r = requests.get(
            SSE_URL,
            headers={"Authorization": "Bearer invalid-token"},
            timeout=5,
            stream=True,
        )
        assert r.status_code == 401
        r.close()

    def test_sse_connects_with_valid_token(self, access_token):
        """SSE connection should succeed and send an endpoint event."""
        r = requests.get(
            SSE_URL,
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=10,
            stream=True,
        )
        assert r.status_code == 200

        # Read the first SSE event — should be the messages endpoint
        messages_url = None
        for line in r.iter_lines(decode_unicode=True):
            if line and line.startswith("data: ") and "/messages/" in line:
                messages_url = line[6:].strip()
                break
        r.close()

        assert messages_url is not None, "SSE did not send messages endpoint"
        assert "/messages/" in messages_url


class SSEReader:
    """Background thread that reads SSE events into a queue."""

    def __init__(self, url, headers, timeout=30):
        self._events = queue.Queue()
        self._messages_url = None
        self._ready = threading.Event()
        self._resp = requests.get(url, headers=headers, timeout=timeout, stream=True)
        assert self._resp.status_code == 200
        self._thread = threading.Thread(target=self._read_loop, daemon=True)
        self._thread.start()

    def _read_loop(self):
        try:
            for line in self._resp.iter_lines(decode_unicode=True):
                if not line:
                    continue
                if line.startswith("data: "):
                    data = line[6:].strip()
                    if self._messages_url is None and "/messages/" in data:
                        self._messages_url = data
                        self._ready.set()
                    else:
                        try:
                            self._events.put(json.loads(data))
                        except json.JSONDecodeError:
                            pass
        except Exception:
            pass
        finally:
            self._ready.set()

    @property
    def messages_url(self):
        self._ready.wait(timeout=10)
        url = self._messages_url
        if url and url.startswith("/"):
            return f"{MCP_URL}{url}"
        return url

    def wait_for(self, request_id, timeout=15):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                msg = self._events.get(timeout=1)
                if msg.get("id") == request_id:
                    return msg
            except queue.Empty:
                continue
        return None

    def close(self):
        self._resp.close()


class TestMCPProtocol:
    """Test full MCP protocol flow: connect SSE, initialize, list tools, call tool."""

    def _mcp_session(self, access_token):
        """Connect SSE, initialize, return (sse_reader, messages_url, headers)."""
        headers = {"Authorization": f"Bearer {access_token}"}
        sse = SSEReader(SSE_URL, headers)

        messages_url = sse.messages_url
        assert messages_url, "No messages endpoint from SSE"

        # Initialize
        r = requests.post(
            messages_url,
            headers=headers,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "test", "version": "0.1.0"},
                },
            },
            timeout=10,
        )
        assert r.status_code == 202

        msg = sse.wait_for(1)
        assert msg is not None, "No initialize response"

        # Send initialized notification
        requests.post(
            messages_url,
            headers=headers,
            json={"jsonrpc": "2.0", "method": "notifications/initialized"},
            timeout=5,
        )

        return sse, messages_url, headers

    def test_list_tools(self, access_token):
        sse, messages_url, headers = self._mcp_session(access_token)
        try:
            requests.post(
                messages_url,
                headers=headers,
                json={
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/list",
                    "params": {},
                },
                timeout=10,
            )

            msg = sse.wait_for(2)
            assert msg is not None, "No tools/list response"
            tools = msg.get("result", {}).get("tools", [])
            tool_names = {t["name"] for t in tools}
            assert "find_documents" in tool_names
            assert "get_document_content" in tool_names
            assert "get_document_summary" in tool_names
            assert "list_contexts" in tool_names
            assert "list_fields" in tool_names
            assert "list_candidates" in tool_names
        finally:
            sse.close()

    def test_find_documents(self, access_token):
        sse, messages_url, headers = self._mcp_session(access_token)
        try:
            requests.post(
                messages_url,
                headers=headers,
                json={
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/call",
                    "params": {
                        "name": "find_documents",
                        "arguments": {"query": {}},
                    },
                },
                timeout=10,
            )

            msg = sse.wait_for(2, timeout=30)
            assert msg is not None, "No find_documents response"
            content = msg.get("result", {}).get("content", [])
            assert len(content) > 0
            assert content[0]["type"] == "text"
        finally:
            sse.close()
