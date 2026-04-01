"""Tests for the SSE session lifecycle.

The full MCP protocol test (SSE + POST messages) requires a running server
because the SSE stream and message POSTs must happen concurrently —
Starlette's synchronous TestClient cannot handle this.

These tests verify the parts that can be tested in-process:
health, OAuth, and SSE auth rejection.

The full protocol flow (connect, initialize, list_tools, call_tool) is
tested in tests/integration/test_mcp_server.py against real Docker
containers.
"""

import json

import pytest
from starlette.testclient import TestClient

from mcp_server.auth import TokenStore, UserCredentialStore
from mcp_server.contexts import ContextReader
from mcp_server.db import DatabaseManager
from mcp_server import server as srv


class _MockDBManager:
    async def get_pool(self, username, password):
        return f"mock-pool-{username}"

    async def start(self):
        pass

    async def close(self):
        pass


@pytest.fixture
def _setup_server(tmp_sync_root, monkeypatch):
    monkeypatch.setattr(srv, "SYNC_ROOT", str(tmp_sync_root))
    monkeypatch.setattr(srv, "MCP_SUBDIR", "")
    monkeypatch.setattr(srv, "DATABASE_HOST", "localhost")
    monkeypatch.setattr(srv, "DATABASE_PORT", 5432)
    monkeypatch.setattr(srv, "DATABASE_NAME", "mrdocument")
    monkeypatch.setattr(srv, "DatabaseManager", lambda *a, **kw: _MockDBManager())
    yield


@pytest.fixture
def client(_setup_server):
    with TestClient(srv.starlette_app) as c:
        yield c


@pytest.fixture
def token(client):
    r = client.post(
        "/oauth/token",
        data={
            "grant_type": "client_credentials",
            "client_id": "testuser",
            "client_secret": "test-mcp-password-456",
        },
    )
    assert r.status_code == 200, f"OAuth failed: {r.text}"
    return r.json()["access_token"]


class TestSSESession:
    def test_health(self, client):
        r = client.get("/health")
        assert r.status_code == 200
        assert r.json()["status"] == "healthy"

    def test_oauth_metadata(self, client):
        r = client.get("/.well-known/oauth-authorization-server")
        assert r.status_code == 200
        body = r.json()
        assert "token_endpoint" in body
        assert "client_credentials" in body["grant_types_supported"]

    def test_oauth_valid(self, client):
        r = client.post(
            "/oauth/token",
            data={
                "grant_type": "client_credentials",
                "client_id": "testuser",
                "client_secret": "test-mcp-password-456",
            },
        )
        assert r.status_code == 200
        body = r.json()
        assert "access_token" in body
        assert body["token_type"] == "Bearer"

    def test_oauth_invalid_password(self, client):
        r = client.post(
            "/oauth/token",
            data={
                "grant_type": "client_credentials",
                "client_id": "testuser",
                "client_secret": "wrong",
            },
        )
        assert r.status_code == 401

    def test_oauth_invalid_grant_type(self, client):
        r = client.post(
            "/oauth/token",
            data={
                "grant_type": "authorization_code",
                "client_id": "testuser",
                "client_secret": "test-mcp-password-456",
            },
        )
        assert r.status_code == 400

    def test_sse_rejects_no_auth(self, client):
        r = client.get("/sse")
        assert r.status_code == 401

    def test_sse_rejects_bad_token(self, client):
        r = client.get("/sse", headers={"Authorization": "Bearer invalid"})
        assert r.status_code == 401

    # NOTE: SSE connection + MCP protocol flow cannot be tested with
    # TestClient — the synchronous stream() blocks the event loop.
    # See tests/integration/test_mcp_server.py for the full flow test.
