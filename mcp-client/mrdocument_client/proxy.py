"""Local MCP proxy: stdio <-> remote Streamable HTTP.

Runs locally on the client machine (which has VPN access).
Claude Desktop/Code connects via stdio. The proxy forwards all
MCP requests to the remote MCP server over Streamable HTTP with
OAuth authentication.
"""

import argparse
import asyncio
import json
import logging
import os
import platform
import subprocess
import sys
from pathlib import Path

import httpx
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

logger = logging.getLogger("mrdocument-proxy")


class RemoteMCPClient:
    """Connects to a remote MCP server via Streamable HTTP."""

    def __init__(self, base_url: str, username: str, password: str, ca_cert: str | None = None) -> None:
        self._base_url = base_url.rstrip("/")
        self._mcp_url = f"{self._base_url}/mcp"
        self._token_url = f"{self._base_url}/oauth/token"
        self._username = username
        self._password = password
        self._access_token: str | None = None
        self._session_id: str | None = None
        self._verify = ca_cert if ca_cert else True
        self._client = httpx.AsyncClient(timeout=120, verify=self._verify)

    async def _ensure_token(self) -> str:
        if self._access_token is not None:
            return self._access_token

        resp = await self._client.post(
            self._token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": self._username,
                "client_secret": self._password,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        self._access_token = data["access_token"]
        return self._access_token

    async def _headers(self) -> dict[str, str]:
        token = await self._ensure_token()
        h = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        }
        if self._session_id:
            h["mcp-session-id"] = self._session_id
        return h

    async def _post_jsonrpc(self, payload: dict) -> dict:
        """POST a JSON-RPC message to /mcp, return the parsed response."""
        headers = await self._headers()
        resp = await self._client.post(self._mcp_url, headers=headers, json=payload)
        resp.raise_for_status()

        # Capture session ID from response
        sid = resp.headers.get("mcp-session-id")
        if sid:
            self._session_id = sid

        content_type = resp.headers.get("content-type", "")
        if "text/event-stream" in content_type:
            # Parse SSE response for the result
            return self._parse_sse_response(resp.text, payload.get("id"))
        else:
            return resp.json()

    async def _post_jsonrpc_streaming(self, payload: dict) -> dict:
        """POST with streaming response for potentially long operations."""
        headers = await self._headers()
        async with self._client.stream("POST", self._mcp_url, headers=headers, json=payload) as resp:
            resp.raise_for_status()
            sid = resp.headers.get("mcp-session-id")
            if sid:
                self._session_id = sid

            content_type = resp.headers.get("content-type", "")
            if "text/event-stream" in content_type:
                text = ""
                async for chunk in resp.aiter_text():
                    text += chunk
                return self._parse_sse_response(text, payload.get("id"))
            else:
                body = b""
                async for chunk in resp.aiter_bytes():
                    body += chunk
                return json.loads(body)

    def _parse_sse_response(self, text: str, request_id: int | None) -> dict:
        """Extract JSON-RPC response from SSE event stream."""
        for line in text.split("\n"):
            line = line.strip()
            if line.startswith("data: "):
                try:
                    msg = json.loads(line[6:])
                    if request_id is None or msg.get("id") == request_id:
                        return msg
                except json.JSONDecodeError:
                    continue
        return {}

    async def initialize(self) -> None:
        """Initialize the MCP session."""
        result = await self._post_jsonrpc({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "mrdocument-proxy", "version": "0.1.0"},
            },
        })
        logger.debug("Initialize response: %s", result)

        # Send initialized notification (no response expected)
        headers = await self._headers()
        await self._client.post(
            self._mcp_url,
            headers=headers,
            json={"jsonrpc": "2.0", "method": "notifications/initialized"},
        )

    async def list_tools(self) -> list[Tool]:
        if not self._session_id:
            await self.initialize()

        result = await self._post_jsonrpc({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/list",
            "params": {},
        })
        return [
            Tool(
                name=t["name"],
                description=t.get("description", ""),
                inputSchema=t.get("inputSchema", {}),
            )
            for t in result.get("result", {}).get("tools", [])
        ]

    async def call_tool(self, name: str, arguments: dict) -> str:
        if not self._session_id:
            await self.initialize()

        result = await self._post_jsonrpc_streaming({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        })
        content = result.get("result", {}).get("content", [])
        texts = [c.get("text", "") for c in content if c.get("type") == "text"]
        return "\n".join(texts) if texts else json.dumps(result)

    async def close(self) -> None:
        await self._client.aclose()


def create_proxy_server(remote: RemoteMCPClient) -> Server:
    server = Server("mrdocument-proxy")
    _cached_tools: list[Tool] = []

    @server.list_tools()
    async def list_tools() -> list[Tool]:
        nonlocal _cached_tools
        try:
            _cached_tools = await remote.list_tools()
        except Exception as e:
            logger.error("Failed to list remote tools: %s", e)
            if not _cached_tools:
                raise
        return _cached_tools

    @server.call_tool()
    async def call_tool(name: str, arguments: dict) -> list[TextContent]:
        try:
            result = await remote.call_tool(name, arguments)
        except Exception as e:
            result = json.dumps({"error": str(e)})
        return [TextContent(type="text", text=result)]

    return server


def _resolve_ca_cert(ca_cert: str | None) -> str | None:
    """Resolve the CA certificate path.

    If ca_cert is given, use it directly.
    On macOS, exports all certificates from the Keychain (system roots +
    System + user login) into a temp PEM bundle so that httpx trusts
    both public CAs and any self-signed CA the user has marked as trusted.
    """
    if ca_cert:
        return ca_cert

    if platform.system() != "Darwin":
        return None

    import tempfile

    keychains = [
        "/System/Library/Keychains/SystemRootCertificates.keychain",
        "/Library/Keychains/System.keychain",
        os.path.expanduser("~/Library/Keychains/login.keychain-db"),
    ]

    all_certs = ""
    for kc in keychains:
        try:
            result = subprocess.run(
                ["security", "find-certificate", "-a", "-p", kc],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0:
                all_certs += result.stdout
        except Exception:
            pass

    if not all_certs.strip():
        return None

    ca_file = os.path.join(tempfile.gettempdir(), "mrdocument-ca-bundle.pem")
    with open(ca_file, "w") as f:
        f.write(all_certs)
    logger.debug("Exported macOS Keychain certs to %s", ca_file)
    return ca_file


async def run(url: str, username: str, password: str, ca_cert: str | None = None) -> None:
    resolved_ca = _resolve_ca_cert(ca_cert)
    remote = RemoteMCPClient(url, username, password, ca_cert=resolved_ca)
    server = create_proxy_server(remote)
    try:
        async with stdio_server() as (read_stream, write_stream):
            await server.run(
                read_stream,
                write_stream,
                server.create_initialization_options(),
            )
    finally:
        await remote.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="MrDocument MCP proxy: connects Claude Desktop/Code to the remote MCP server"
    )
    parser.add_argument("--url", required=True, help="Remote MCP server base URL")
    parser.add_argument("--user", required=True, help="Username")
    parser.add_argument("--password", help="MCP password")
    parser.add_argument("--password-file", help="Path to .mcp-password file")
    parser.add_argument("--ca-cert", help="Path to CA certificate PEM file (auto-detected on macOS)")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )

    if args.password:
        password = args.password
    elif args.password_file:
        password = Path(args.password_file).read_text().strip()
    else:
        parser.error("Either --password or --password-file is required")

    asyncio.run(run(args.url, args.user, password, ca_cert=args.ca_cert))


if __name__ == "__main__":
    main()
