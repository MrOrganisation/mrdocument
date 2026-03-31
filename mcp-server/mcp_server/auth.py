"""Authentication: per-user credential discovery from sync directory password files.

Two password files per user:
  - .mcp-password: used to authenticate incoming MCP Bearer tokens
  - .db-password:  PostgreSQL role password for RLS-isolated DB connections

Both are created by the watcher's setup_user on startup.
"""

import base64
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

MCP_PASSWORD_FILENAME = ".mcp-password"
DB_PASSWORD_FILENAME = ".db-password"


class AuthError(Exception):
    """Raised when authentication fails."""


class UserCredentialStore:
    """Discovers and validates per-user credentials.

    The watcher creates two password files in each user's sync directory:
      - .mcp-password for authenticating MCP Bearer tokens
      - .db-password  for opening per-user PostgreSQL connections

    The MCP server validates incoming tokens against .mcp-password and
    uses the .db-password internally to connect to PostgreSQL with RLS.
    """

    def __init__(self, sync_root: str) -> None:
        self._sync_root = Path(sync_root)
        self._mcp_passwords: dict[str, str] = {}  # username -> mcp password
        self._db_passwords: dict[str, str] = {}   # username -> db password
        self.refresh()

    def refresh(self) -> None:
        """Rescan sync_root for password files."""
        new_mcp: dict[str, str] = {}
        new_db: dict[str, str] = {}
        if not self._sync_root.is_dir():
            logger.warning("Sync root does not exist: %s", self._sync_root)
            return

        for entry in self._sync_root.iterdir():
            if not entry.is_dir():
                continue
            username = entry.name.lower()
            for filename, target in [
                (MCP_PASSWORD_FILENAME, new_mcp),
                (DB_PASSWORD_FILENAME, new_db),
            ]:
                pw_file = entry / filename
                if pw_file.is_file():
                    try:
                        password = pw_file.read_text().strip()
                        if password:
                            target[username] = password
                    except OSError:
                        logger.warning(
                            "Could not read password file: %s", pw_file, exc_info=True
                        )

        added = set(new_mcp) - set(self._mcp_passwords)
        removed = set(self._mcp_passwords) - set(new_mcp)
        if added:
            logger.info("Discovered credentials for users: %s", ", ".join(sorted(added)))
        if removed:
            logger.info("Removed credentials for users: %s", ", ".join(sorted(removed)))

        self._mcp_passwords = new_mcp
        self._db_passwords = new_db

    def validate(self, username: str, password: str) -> bool:
        """Check if the given MCP credentials match."""
        stored = self._mcp_passwords.get(username.lower())
        if stored is None:
            self.refresh()
            stored = self._mcp_passwords.get(username.lower())
        return stored is not None and stored == password

    def get_db_password(self, username: str) -> str | None:
        """Return the PostgreSQL password for a user, or None."""
        pw = self._db_passwords.get(username.lower())
        if pw is None:
            self.refresh()
            pw = self._db_passwords.get(username.lower())
        return pw

    def get_username_dir(self, username: str) -> Path:
        """Return the sync directory path for a user."""
        return self._sync_root / username

    @property
    def known_users(self) -> set[str]:
        """Return the set of known usernames."""
        return set(self._mcp_passwords.keys())


def decode_bearer_token(authorization: str) -> tuple[str, str]:
    """Decode a Bearer token containing base64(username:password).

    Returns (username, password).
    Raises AuthError on invalid format.
    """
    if not authorization.startswith("Bearer "):
        raise AuthError("Authorization header must use Bearer scheme")

    token = authorization[len("Bearer "):]
    try:
        decoded = base64.b64decode(token).decode("utf-8")
    except Exception as e:
        raise AuthError(f"Invalid Bearer token encoding: {e}") from e

    if ":" not in decoded:
        raise AuthError("Bearer token must contain username:password")

    username, password = decoded.split(":", 1)
    if not username or not password:
        raise AuthError("Username and password must not be empty")

    return username.lower(), password
