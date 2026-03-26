"""Fixtures for claude-code-adapter tests.

These tests run against a live claude-code-adapter (and real Claude CLI),
so they require valid credentials and will incur API costs.
"""

import json
import os
import time

import pytest
import requests

ADAPTER_URL = os.environ.get("ADAPTER_URL", "http://localhost:18080")
# Model for cheap/fast tests — haiku is cheapest
TEST_MODEL = os.environ.get("TEST_MODEL", "claude-haiku-4-5")


def _wait_for_health(url, timeout=60):
    """Block until the adapter health endpoint responds."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"{url}/health", timeout=5)
            if r.status_code == 200:
                return
        except requests.ConnectionError:
            pass
        time.sleep(1)
    pytest.fail(f"Adapter at {url} did not become healthy within {timeout}s")


@pytest.fixture(scope="session", autouse=True)
def adapter_healthy():
    """Ensure the adapter is reachable before running any tests."""
    _wait_for_health(ADAPTER_URL)


@pytest.fixture()
def post_messages():
    """Helper that sends a POST /v1/messages and returns the response."""

    def _post(body, *, stream=False, timeout=300):
        r = requests.post(
            f"{ADAPTER_URL}/v1/messages",
            json=body,
            headers={
                "x-api-key": "dummy",
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            timeout=timeout,
            stream=stream,
        )
        return r

    return _post
