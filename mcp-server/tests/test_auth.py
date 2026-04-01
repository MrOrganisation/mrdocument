"""Tests for the authentication module."""

import base64

import pytest

from mcp_server.auth import AuthError, UserCredentialStore, decode_bearer_token


class TestDecodeBearer:
    def test_valid_token(self):
        token = base64.b64encode(b"testuser:password123").decode()
        username, password = decode_bearer_token(f"Bearer {token}")
        assert username == "testuser"
        assert password == "password123"

    def test_username_lowercased(self):
        token = base64.b64encode(b"TestUser:password123").decode()
        username, _ = decode_bearer_token(f"Bearer {token}")
        assert username == "testuser"

    def test_password_with_colon(self):
        token = base64.b64encode(b"user:pass:with:colons").decode()
        username, password = decode_bearer_token(f"Bearer {token}")
        assert username == "user"
        assert password == "pass:with:colons"

    def test_missing_bearer_prefix(self):
        with pytest.raises(AuthError, match="Bearer scheme"):
            decode_bearer_token("Basic abc123")

    def test_invalid_base64(self):
        with pytest.raises(AuthError, match="Invalid Bearer token"):
            decode_bearer_token("Bearer not-valid-base64!!!")

    def test_no_colon_separator(self):
        token = base64.b64encode(b"nocolon").decode()
        with pytest.raises(AuthError, match="username:password"):
            decode_bearer_token(f"Bearer {token}")

    def test_empty_username(self):
        token = base64.b64encode(b":password").decode()
        with pytest.raises(AuthError, match="must not be empty"):
            decode_bearer_token(f"Bearer {token}")

    def test_empty_password(self):
        token = base64.b64encode(b"user:").decode()
        with pytest.raises(AuthError, match="must not be empty"):
            decode_bearer_token(f"Bearer {token}")


class TestUserCredentialStore:
    def test_discovers_users_with_mcp_password(self, tmp_sync_root):
        store = UserCredentialStore(str(tmp_sync_root), subdir="")
        assert "testuser" in store.known_users
        # otheruser has no .mcp-password file
        assert "otheruser" not in store.known_users

    def test_validate_correct_mcp_password(self, tmp_sync_root):
        store = UserCredentialStore(str(tmp_sync_root), subdir="")
        assert store.validate("testuser", "test-mcp-password-456") is True

    def test_validate_db_password_rejected(self, tmp_sync_root):
        """DB password must not work for MCP auth."""
        store = UserCredentialStore(str(tmp_sync_root), subdir="")
        assert store.validate("testuser", "test-password-123") is False

    def test_validate_wrong_password(self, tmp_sync_root):
        store = UserCredentialStore(str(tmp_sync_root), subdir="")
        assert store.validate("testuser", "wrong-password") is False

    def test_validate_unknown_user(self, tmp_sync_root):
        store = UserCredentialStore(str(tmp_sync_root), subdir="")
        assert store.validate("unknown", "password") is False

    def test_get_db_password(self, tmp_sync_root):
        store = UserCredentialStore(str(tmp_sync_root), subdir="")
        assert store.get_db_password("testuser") == "test-password-123"

    def test_get_db_password_unknown_user(self, tmp_sync_root):
        store = UserCredentialStore(str(tmp_sync_root), subdir="")
        assert store.get_db_password("unknown") is None

    def test_refresh_picks_up_new_user(self, tmp_sync_root):
        store = UserCredentialStore(str(tmp_sync_root), subdir="")
        assert "newuser" not in store.known_users

        # Add a new user with both password files
        new_dir = tmp_sync_root / "newuser"
        new_dir.mkdir()
        (new_dir / ".mcp-password").write_text("new-mcp-pw")
        (new_dir / ".db-password").write_text("new-db-pw")

        store.refresh()
        assert "newuser" in store.known_users
        assert store.validate("newuser", "new-mcp-pw") is True
        assert store.get_db_password("newuser") == "new-db-pw"

    def test_validate_triggers_refresh_on_miss(self, tmp_sync_root):
        store = UserCredentialStore(str(tmp_sync_root), subdir="")

        # Add user after initial load
        new_dir = tmp_sync_root / "lateuser"
        new_dir.mkdir()
        (new_dir / ".mcp-password").write_text("late-mcp-pw")
        (new_dir / ".db-password").write_text("late-db-pw")

        # validate should trigger refresh and find the new user
        assert store.validate("lateuser", "late-mcp-pw") is True

    def test_nonexistent_sync_root(self, tmp_path):
        store = UserCredentialStore(str(tmp_path / "nonexistent"))
        assert store.known_users == set()

    def test_get_username_dir(self, tmp_sync_root):
        store = UserCredentialStore(str(tmp_sync_root), subdir="")
        assert store.get_username_dir("testuser") == tmp_sync_root / "testuser"
