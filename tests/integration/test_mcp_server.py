"""Integration tests for the MCP server (Streamable HTTP transport).

Tests the full flow: health check, OAuth token exchange, MCP protocol
over POST /mcp (initialize, list tools, call tool).

Requires the integration stack to be running (docker-compose.fast.yaml)
with the mcp-server container.
"""

import json
import time

import pytest
import requests

MCP_URL = "http://localhost:8091"
HEALTH_URL = f"{MCP_URL}/health"
TOKEN_URL = f"{MCP_URL}/oauth/token"
ENDPOINT_URL = f"{MCP_URL}/mcp"

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
        pytest.skip("No .mcp-password file found")
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


def _post_mcp(access_token, payload, session_id=None, timeout=30):
    """POST a JSON-RPC message to /mcp, return (response, session_id)."""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }
    if session_id:
        headers["mcp-session-id"] = session_id

    r = requests.post(ENDPOINT_URL, headers=headers, json=payload, timeout=timeout)
    new_session_id = r.headers.get("mcp-session-id", session_id)

    # Parse response — may be JSON or SSE
    content_type = r.headers.get("content-type", "")
    if "text/event-stream" in content_type:
        # Parse SSE: find the data line matching our request ID
        for line in r.text.split("\n"):
            line = line.strip()
            if line.startswith("data: "):
                try:
                    msg = json.loads(line[6:])
                    if msg.get("id") == payload.get("id"):
                        return msg, new_session_id
                except json.JSONDecodeError:
                    continue
        return {}, new_session_id
    else:
        return r.json() if r.text else {}, new_session_id


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

    def test_wrong_password(self):
        r = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": USERNAME,
                "client_secret": "wrong",
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


class TestMCPAuth:
    def test_mcp_without_auth_returns_401(self):
        r = requests.post(
            ENDPOINT_URL,
            json={"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}},
            timeout=5,
        )
        assert r.status_code == 401

    def test_mcp_with_invalid_token_returns_401(self):
        r = requests.post(
            ENDPOINT_URL,
            headers={"Authorization": "Bearer invalid-token"},
            json={"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}},
            timeout=5,
        )
        assert r.status_code == 401


class TestMCPProtocol:
    """Test full MCP protocol: initialize, list tools, call tool via Streamable HTTP."""

    def _init_session(self, access_token):
        """Initialize an MCP session, return session_id."""
        result, session_id = _post_mcp(access_token, {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "test", "version": "0.1.0"},
            },
        })
        assert "result" in result, f"Initialize failed: {result}"
        assert session_id, "No session ID returned"

        # Send initialized notification
        _post_mcp(access_token, {
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
        }, session_id=session_id)

        return session_id

    def test_list_tools(self, access_token):
        session_id = self._init_session(access_token)

        result, _ = _post_mcp(access_token, {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/list",
            "params": {},
        }, session_id=session_id)

        tools = result.get("result", {}).get("tools", [])
        tool_names = {t["name"] for t in tools}
        assert "find_documents" in tool_names
        assert "get_document_content" in tool_names
        assert "get_document_summary" in tool_names
        assert "list_contexts" in tool_names
        assert "list_fields" in tool_names
        assert "list_candidates" in tool_names

    def test_find_documents(self, access_token):
        session_id = self._init_session(access_token)

        result, _ = _post_mcp(access_token, {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "find_documents",
                "arguments": {"query": {}},
            },
        }, session_id=session_id, timeout=30)

        assert "result" in result, f"find_documents failed: {result}"
        content = result.get("result", {}).get("content", [])
        assert len(content) > 0
        assert content[0]["type"] == "text"
