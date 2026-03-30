"""Authentication: discover per-user PostgreSQL credentials from password files."""

import base64
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

# Password filename written by the watcher's ensure_user_role
PASSWORD_FILENAME = ".db-password"


class AuthError(Exception):
    """Raised when authentication fails."""


class UserCredentialStore:
    """Discovers and validates per-user PostgreSQL credentials.

    The watcher creates a .db-password file in each user's sync directory
    containing the PostgreSQL password for their per-user role.
    This store reads those files and validates incoming credentials.
    """

    def __init__(self, sync_root: str) -> None:
        self._sync_root = Path(sync_root)
        self._credentials: dict[str, str] = {}  # username -> password
        self.refresh()

    def refresh(self) -> None:
        """Rescan sync_root for .db-password files."""
        new_creds: dict[str, str] = {}
        if not self._sync_root.is_dir():
            logger.warning("Sync root does not exist: %s", self._sync_root)
            return

        for entry in self._sync_root.iterdir():
            if not entry.is_dir():
                continue
            pw_file = entry / PASSWORD_FILENAME
            if pw_file.is_file():
                try:
                    password = pw_file.read_text().strip()
                    if password:
                        username = entry.name.lower()
                        new_creds[username] = password
                except OSError:
                    logger.warning(
                        "Could not read password file: %s", pw_file, exc_info=True
                    )

        added = set(new_creds) - set(self._credentials)
        removed = set(self._credentials) - set(new_creds)
        if added:
            logger.info("Discovered credentials for users: %s", ", ".join(sorted(added)))
        if removed:
            logger.info("Removed credentials for users: %s", ", ".join(sorted(removed)))

        self._credentials = new_creds

    def validate(self, username: str, password: str) -> bool:
        """Check if the given credentials match the stored password."""
        stored = self._credentials.get(username.lower())
        if stored is None:
            # Try refreshing in case a new user was added
            self.refresh()
            stored = self._credentials.get(username.lower())
        return stored is not None and stored == password

    def get_username_dir(self, username: str) -> Path:
        """Return the sync directory path for a user."""
        return self._sync_root / username

    @property
    def known_users(self) -> set[str]:
        """Return the set of known usernames."""
        return set(self._credentials.keys())


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
