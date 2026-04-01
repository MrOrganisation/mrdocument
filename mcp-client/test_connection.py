"""Test connectivity to the remote MrDocument MCP server."""

import asyncio
import os
import sys

import httpx

from mrdocument_client.proxy import _resolve_ca_cert


async def test():
    url = os.environ.get("MRDOCUMENT_URL", "https://mcp.mrdocument.parmenides.net")
    user = os.environ.get("MRDOCUMENT_USER")
    pw_file = os.environ.get("MRDOCUMENT_PASSWORD_FILE")

    if not user or not pw_file:
        print("Error: MRDOCUMENT_USER and MRDOCUMENT_PASSWORD_FILE must be set")
        sys.exit(1)

    pw = open(pw_file).read().strip()
    print(f"Password file: {pw_file} ({len(pw)} chars, starts with {pw[:6]}...)")

    verify = _resolve_ca_cert(os.environ.get("MRDOCUMENT_CA_CERT"))
    if verify:
        print(f"Using CA bundle: {verify}")
    else:
        verify = True

    print(f"Connecting to {url} as {user}...")

    async with httpx.AsyncClient(verify=verify) as c:
        try:
            r = await c.get(f"{url}/health")
            print(f"Health: {r.status_code} {r.json()}")
        except Exception as e:
            print(f"Health: FAILED — {e}")
            return

        try:
            print(f"OAuth:  POST {url}/oauth/token client_id={user}")
            r = await c.post(
                f"{url}/oauth/token",
                data={
                    "grant_type": "client_credentials",
                    "client_id": user,
                    "client_secret": pw,
                },
            )
            print(f"OAuth:  {r.status_code} {r.json()}")
        except Exception as e:
            print(f"OAuth:  FAILED — {e}")


asyncio.run(test())
