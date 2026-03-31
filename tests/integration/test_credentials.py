"""Integration tests for per-user credential file creation.

Verifies that the watcher creates the credential files that downstream
services (MCP server, Directus) rely on for authentication.
"""

import subprocess
from pathlib import Path

import pytest


# Override autouse fixture — these tests only check files, no DB/filesystem reset needed.
@pytest.fixture(autouse=True)
def reset_environment():
    yield


class TestDatabaseCredentials:
    """The watcher must create a .db-password file for each user.

    This file contains the PostgreSQL role password used internally by
    the MCP server for per-user RLS-isolated database connections.
    """

    def test_db_password_file_exists(self, test_config):
        pw_file = test_config.sync_folder / ".db-password"
        assert pw_file.is_file(), (
            f".db-password not found at {pw_file} — "
            "watcher's ensure_user_role should create it on startup"
        )

    def test_db_password_file_not_empty(self, test_config):
        pw_file = test_config.sync_folder / ".db-password"
        content = pw_file.read_text().strip()
        assert len(content) > 0, ".db-password file is empty"

    def test_db_password_authenticates(self, test_config):
        """The password in .db-password must allow a PostgreSQL connection."""
        pw_file = test_config.sync_folder / ".db-password"
        password = pw_file.read_text().strip()

        result = subprocess.run(
            [
                "docker", "exec", "integration-mrdocument-db-1",
                "psql",
                f"postgresql://testuser:{password}@localhost:5432/mrdocument",
                "-t", "-A", "-c", "SELECT current_user",
            ],
            capture_output=True, text=True, timeout=10,
        )
        assert result.returncode == 0, f"psql failed: {result.stderr}"
        assert result.stdout.strip() == "testuser"


class TestMcpCredentials:
    """The watcher must create a .mcp-password file for each user.

    This is the password users present in Bearer tokens to authenticate
    against the MCP server. It is separate from the database password.
    """

    def test_mcp_password_file_exists(self, test_config):
        pw_file = test_config.sync_folder / ".mcp-password"
        assert pw_file.is_file(), (
            f".mcp-password not found at {pw_file} — "
            "watcher should create it on startup"
        )

    def test_mcp_password_file_not_empty(self, test_config):
        pw_file = test_config.sync_folder / ".mcp-password"
        content = pw_file.read_text().strip()
        assert len(content) > 0, ".mcp-password file is empty"

    def test_mcp_password_differs_from_db_password(self, test_config):
        """MCP and DB passwords must be independent."""
        mcp_pw = (test_config.sync_folder / ".mcp-password").read_text().strip()
        db_pw = (test_config.sync_folder / ".db-password").read_text().strip()
        assert mcp_pw != db_pw, "MCP password must not be the same as DB password"
