"""Integration tests for Directus integration.

Verifies that the watcher creates Directus users and configures
context dropdown choices via the mock Directus API.
"""

import subprocess
import time

import requests

from conftest import (
    TestConfig,
    restart_watcher,
)

MOCK_DIRECTUS_URL = "http://localhost:18055"


def directus_get_users(external_identifier=None):
    """Query the mock Directus for users, optionally filtered."""
    params = {}
    if external_identifier:
        params["filter[external_identifier][_eq]"] = external_identifier
    resp = requests.get(f"{MOCK_DIRECTUS_URL}/users", params=params, timeout=5)
    assert resp.status_code == 200
    return resp.json()["data"]


class TestDirectusUserProvisioning:
    """Directus user creation triggered by user directory discovery."""

    def test_user_created_on_discovery(self, test_config: TestConfig):
        """Watcher creates a Directus user for the discovered test user."""
        # The watcher discovers "testuser" at startup (before tests run).
        users = directus_get_users(external_identifier="testuser")
        assert len(users) == 1, f"Expected 1 Directus user, got {len(users)}"

        user = users[0]
        assert user["external_identifier"] == "testuser"
        assert user["email"] == "testuser@mrdocument.local"
        assert user["role"], "User should have a role assigned"

    def test_password_file_written(self, test_config: TestConfig):
        """Watcher writes .directus-password to the user root directory."""
        result = subprocess.run(
            [
                "docker", "exec", "integration-mrdocument-watcher-1",
                "cat", "/sync/testuser/mrdocument/.directus-password",
            ],
            capture_output=True, text=True, timeout=10,
        )
        assert result.returncode == 0, (
            f".directus-password not found in watcher container: {result.stderr}"
        )
        password = result.stdout.strip()
        assert len(password) >= 32, (
            f"Password too short ({len(password)} chars), expected >= 32"
        )

    def test_user_creation_idempotent(self, test_config: TestConfig):
        """Restarting the watcher does not create a duplicate user."""
        # Verify user exists before restart
        users_before = directus_get_users(external_identifier="testuser")
        assert len(users_before) == 1

        restart_watcher(timeout=15)

        # Give the watcher a moment to run setup_user again
        time.sleep(3)

        users_after = directus_get_users(external_identifier="testuser")
        assert len(users_after) == 1, (
            f"Expected 1 user after restart, got {len(users_after)}"
        )


class TestDirectusContextDropdown:
    """Context field dropdown is populated with discovered contexts."""

    def test_context_choices_populated(self, test_config: TestConfig):
        """Watcher updates the context field with valid choices."""
        resp = requests.get(
            f"{MOCK_DIRECTUS_URL}/fields/documents_v2/context",
            timeout=5,
        )
        assert resp.status_code == 200
        meta = resp.json()["data"]["meta"]
        choices = meta.get("options", {}).get("choices", [])
        values = [c["value"] for c in choices]
        assert "arbeit" in values, f"'arbeit' not in context choices: {values}"
        assert "privat" in values, f"'privat' not in context choices: {values}"
