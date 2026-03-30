"""Database connection pool management with per-user isolation."""

import asyncio
import json
import logging
import time
import uuid
from datetime import date, datetime
from typing import Any

import asyncpg

logger = logging.getLogger(__name__)

# Idle pool timeout in seconds (30 minutes)
IDLE_POOL_TIMEOUT = 30 * 60


class DatabaseManager:
    """Manages per-user asyncpg connection pools.

    Each authenticated user gets their own connection pool connecting
    as their PostgreSQL role, so Row-Level Security enforces data isolation.
    """

    def __init__(self, host: str, port: int, database: str) -> None:
        self._host = host
        self._port = port
        self._database = database
        self._pools: dict[str, asyncpg.Pool] = {}
        self._pool_passwords: dict[str, str] = {}
        self._pool_last_used: dict[str, float] = {}
        self._cleanup_task: asyncio.Task | None = None

    async def start(self) -> None:
        """Start the background pool cleanup task."""
        self._cleanup_task = asyncio.create_task(self._cleanup_idle_pools())

    async def close(self) -> None:
        """Close all connection pools and stop cleanup."""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        for username, pool in self._pools.items():
            try:
                await pool.close()
            except Exception:
                logger.warning("Error closing pool for user %s", username, exc_info=True)
        self._pools.clear()
        self._pool_passwords.clear()
        self._pool_last_used.clear()

    async def get_pool(self, username: str, password: str) -> asyncpg.Pool:
        """Get or create a connection pool for the given user.

        If the password has changed since the pool was created,
        the old pool is closed and a new one is created.
        """
        existing_password = self._pool_passwords.get(username)
        if username in self._pools and existing_password == password:
            self._pool_last_used[username] = time.monotonic()
            return self._pools[username]

        # Password changed or new user — (re)create pool
        if username in self._pools:
            logger.info("Password changed for user %s, recreating pool", username)
            try:
                await self._pools[username].close()
            except Exception:
                pass

        pool = await asyncpg.create_pool(
            host=self._host,
            port=self._port,
            database=self._database,
            user=username,
            password=password,
            min_size=1,
            max_size=3,
            command_timeout=30,
        )
        self._pools[username] = pool
        self._pool_passwords[username] = password
        self._pool_last_used[username] = time.monotonic()
        logger.info("Created connection pool for user %s", username)
        return pool

    async def execute_query(
        self, pool: asyncpg.Pool, sql: str, params: list[Any]
    ) -> list[dict]:
        """Execute a parameterized query and return results as JSON-safe dicts."""
        async with pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
            return [_row_to_dict(row) for row in rows]

    async def _cleanup_idle_pools(self) -> None:
        """Periodically close connection pools that have been idle too long."""
        while True:
            await asyncio.sleep(60)
            now = time.monotonic()
            stale = [
                username
                for username, last_used in self._pool_last_used.items()
                if now - last_used > IDLE_POOL_TIMEOUT
            ]
            for username in stale:
                logger.info("Closing idle pool for user %s", username)
                pool = self._pools.pop(username, None)
                self._pool_passwords.pop(username, None)
                self._pool_last_used.pop(username, None)
                if pool:
                    try:
                        await pool.close()
                    except Exception:
                        logger.warning(
                            "Error closing idle pool for %s", username, exc_info=True
                        )


def _row_to_dict(row: asyncpg.Record) -> dict:
    """Convert an asyncpg Record to a JSON-serializable dict."""
    result = {}
    for key, value in row.items():
        result[key] = _serialize_value(value)
    return result


def _serialize_value(value: Any) -> Any:
    """Convert database types to JSON-serializable values."""
    if value is None:
        return None
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (dict, list)):
        # JSONB columns are already decoded by asyncpg
        return value
    return value
