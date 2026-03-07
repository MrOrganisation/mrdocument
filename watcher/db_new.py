"""
Database layer for document watcher v2.

Uses asyncpg to manage the mrdocument.documents_v2 table, providing CRUD
and query operations for Record lifecycle tracking.
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import UUID

import asyncpg

from models import State, PathEntry, Record
logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS mrdocument;

CREATE TABLE IF NOT EXISTS mrdocument.documents_v2 (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    original_filename       TEXT NOT NULL,
    source_hash             TEXT NOT NULL,

    -- Path lists (JSONB arrays of {"path": "...", "timestamp": "..."})
    source_paths            JSONB NOT NULL DEFAULT '[]',
    current_paths           JSONB NOT NULL DEFAULT '[]',
    missing_source_paths    JSONB NOT NULL DEFAULT '[]',
    missing_current_paths   JSONB NOT NULL DEFAULT '[]',

    -- Content
    context                 TEXT,
    metadata                JSONB,
    assigned_filename       TEXT,
    hash                    TEXT,

    -- Processing
    output_filename         TEXT,
    state                   TEXT NOT NULL DEFAULT 'is_new'
                            CHECK (state IN (
                                'is_new', 'needs_processing', 'is_missing',
                                'has_error', 'needs_deletion',
                                'is_deleted', 'is_complete'
                            )),

    -- Temp fields
    target_path             TEXT,
    source_reference        TEXT,
    current_reference       TEXT,
    duplicate_sources       JSONB NOT NULL DEFAULT '[]',
    deleted_paths           JSONB NOT NULL DEFAULT '[]',

    -- Owner
    username                TEXT,

    -- Timestamps
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Drop legacy V1 tables if they still exist
DROP TABLE IF EXISTS mrdocument.file_locations;
DROP TABLE IF EXISTS mrdocument.documents;

CREATE INDEX IF NOT EXISTS idx_docs_v2_source_hash
    ON mrdocument.documents_v2(source_hash);

CREATE INDEX IF NOT EXISTS idx_docs_v2_hash
    ON mrdocument.documents_v2(hash)
    WHERE hash IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_docs_v2_output_filename
    ON mrdocument.documents_v2(output_filename)
    WHERE output_filename IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_docs_v2_state
    ON mrdocument.documents_v2(state);

CREATE INDEX IF NOT EXISTS idx_docs_v2_metadata
    ON mrdocument.documents_v2 USING gin(metadata);

CREATE INDEX IF NOT EXISTS idx_docs_v2_username
    ON mrdocument.documents_v2(username)
    WHERE username IS NOT NULL;

-- Auto-update trigger on updated_at
CREATE OR REPLACE FUNCTION mrdocument.update_documents_v2_updated_at()
RETURNS TRIGGER AS $$
BEGIN NEW.updated_at = now(); RETURN NEW; END;
$$ LANGUAGE plpgsql;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'documents_v2_updated_at'
    ) THEN
        CREATE TRIGGER documents_v2_updated_at
            BEFORE UPDATE ON mrdocument.documents_v2
            FOR EACH ROW EXECUTE FUNCTION mrdocument.update_documents_v2_updated_at();
    END IF;
END
$$;
"""


class DocumentDBv2:
    """PostgreSQL database interface for document watcher v2."""

    def __init__(self, database_url: Optional[str] = None):
        self.database_url = database_url or os.environ.get(
            "DATABASE_URL", "postgresql://mrdocument:mrdocument@postgres:5432/mrdocument"
        )
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        """Connect to the database and ensure schema exists."""
        self._pool = await asyncpg.create_pool(self.database_url, min_size=2, max_size=10)
        async with self._pool.acquire() as conn:
            await conn.execute(SCHEMA_SQL)
        logger.info("DocumentDBv2 connected and schema ready")

    async def disconnect(self) -> None:
        """Close the database connection pool."""
        if self._pool:
            await self._pool.close()
            self._pool = None
            logger.info("DocumentDBv2 disconnected")

    @property
    def pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError("DocumentDBv2 not connected. Call connect() first.")
        return self._pool

    # =========================================================================
    # Conversion helpers
    # =========================================================================

    @staticmethod
    def _path_entries_to_json(entries: list[PathEntry]) -> str:
        """Serialize a list of PathEntry to JSON string."""
        return json.dumps([
            {"path": e.path, "timestamp": e.timestamp.isoformat()}
            for e in entries
        ])

    @staticmethod
    def _json_to_path_entries(data: Any) -> list[PathEntry]:
        """Deserialize JSON (list of dicts or already-parsed) to list of PathEntry."""
        if data is None:
            return []
        if isinstance(data, str):
            data = json.loads(data)
        return [
            PathEntry(
                path=item["path"],
                timestamp=datetime.fromisoformat(item["timestamp"]),
            )
            for item in data
        ]

    @staticmethod
    def _parse_jsonb(data: Any) -> Any:
        """Parse a JSONB value from asyncpg (returns raw JSON strings)."""
        if data is None:
            return None
        if isinstance(data, str):
            return json.loads(data)
        return data

    @staticmethod
    def _row_to_record(row: asyncpg.Record) -> Record:
        """Convert a database row to a Record."""
        dup = DocumentDBv2._parse_jsonb(row["duplicate_sources"])
        dlp = DocumentDBv2._parse_jsonb(row["deleted_paths"])
        return Record(
            id=row["id"],
            original_filename=row["original_filename"],
            source_hash=row["source_hash"],
            source_paths=DocumentDBv2._json_to_path_entries(row["source_paths"]),
            current_paths=DocumentDBv2._json_to_path_entries(row["current_paths"]),
            missing_source_paths=DocumentDBv2._json_to_path_entries(row["missing_source_paths"]),
            missing_current_paths=DocumentDBv2._json_to_path_entries(row["missing_current_paths"]),
            context=row["context"],
            metadata=DocumentDBv2._parse_jsonb(row["metadata"]),
            assigned_filename=row["assigned_filename"],
            hash=row["hash"],
            output_filename=row["output_filename"],
            state=State(row["state"]),
            target_path=row["target_path"],
            source_reference=row["source_reference"],
            current_reference=row["current_reference"],
            duplicate_sources=dup if dup else [],
            deleted_paths=dlp if dlp else [],
            username=row["username"],
        )

    @staticmethod
    def _record_to_params(record: Record) -> dict:
        """Convert a Record to a dict of database parameters."""
        return {
            "id": record.id,
            "original_filename": record.original_filename,
            "source_hash": record.source_hash,
            "source_paths": DocumentDBv2._path_entries_to_json(record.source_paths),
            "current_paths": DocumentDBv2._path_entries_to_json(record.current_paths),
            "missing_source_paths": DocumentDBv2._path_entries_to_json(record.missing_source_paths),
            "missing_current_paths": DocumentDBv2._path_entries_to_json(record.missing_current_paths),
            "context": record.context,
            "metadata": json.dumps(record.metadata) if record.metadata is not None else None,
            "assigned_filename": record.assigned_filename,
            "hash": record.hash,
            "output_filename": record.output_filename,
            "state": record.state.value,
            "target_path": record.target_path,
            "source_reference": record.source_reference,
            "current_reference": record.current_reference,
            "duplicate_sources": json.dumps(record.duplicate_sources),
            "deleted_paths": json.dumps(record.deleted_paths),
            "username": record.username,
        }

    # =========================================================================
    # CRUD operations
    # =========================================================================

    async def create_record(self, record: Record) -> UUID:
        """Insert a new record. Returns the UUID."""
        p = self._record_to_params(record)
        await self.pool.execute(
            """
            INSERT INTO mrdocument.documents_v2 (
                id, original_filename, source_hash,
                source_paths, current_paths,
                missing_source_paths, missing_current_paths,
                context, metadata, assigned_filename, hash,
                output_filename, state,
                target_path, source_reference, current_reference,
                duplicate_sources, deleted_paths,
                username
            ) VALUES (
                $1, $2, $3,
                $4::jsonb, $5::jsonb,
                $6::jsonb, $7::jsonb,
                $8, $9::jsonb, $10, $11,
                $12, $13,
                $14, $15, $16,
                $17::jsonb, $18::jsonb,
                $19
            )
            """,
            p["id"], p["original_filename"], p["source_hash"],
            p["source_paths"], p["current_paths"],
            p["missing_source_paths"], p["missing_current_paths"],
            p["context"], p["metadata"], p["assigned_filename"], p["hash"],
            p["output_filename"], p["state"],
            p["target_path"], p["source_reference"], p["current_reference"],
            p["duplicate_sources"], p["deleted_paths"],
            p["username"],
        )
        logger.debug("Created record %s: %s", record.id, record.original_filename)
        return record.id

    async def get_record(self, record_id: UUID) -> Optional[Record]:
        """Get a record by ID, or None if not found."""
        row = await self.pool.fetchrow(
            "SELECT * FROM mrdocument.documents_v2 WHERE id = $1",
            record_id,
        )
        if row is None:
            return None
        return self._row_to_record(row)

    async def save_record(self, record: Record) -> None:
        """Full update of an existing record."""
        p = self._record_to_params(record)
        await self.pool.execute(
            """
            UPDATE mrdocument.documents_v2 SET
                original_filename = $2,
                source_hash = $3,
                source_paths = $4::jsonb,
                current_paths = $5::jsonb,
                missing_source_paths = $6::jsonb,
                missing_current_paths = $7::jsonb,
                context = $8,
                metadata = $9::jsonb,
                assigned_filename = $10,
                hash = $11,
                output_filename = $12,
                state = $13,
                target_path = $14,
                source_reference = $15,
                current_reference = $16,
                duplicate_sources = $17::jsonb,
                deleted_paths = $18::jsonb
            WHERE id = $1
            """,
            p["id"], p["original_filename"], p["source_hash"],
            p["source_paths"], p["current_paths"],
            p["missing_source_paths"], p["missing_current_paths"],
            p["context"], p["metadata"], p["assigned_filename"], p["hash"],
            p["output_filename"], p["state"],
            p["target_path"], p["source_reference"], p["current_reference"],
            p["duplicate_sources"], p["deleted_paths"],
        )
        logger.debug("Saved record %s", record.id)

    async def delete_record(self, record_id: UUID) -> bool:
        """Delete a record by ID. Returns True if deleted, False if not found."""
        result = await self.pool.execute(
            "DELETE FROM mrdocument.documents_v2 WHERE id = $1",
            record_id,
        )
        count = int(result.split()[-1])
        if count:
            logger.debug("Deleted record %s", record_id)
        return count > 0

    # =========================================================================
    # Query operations
    # =========================================================================

    async def get_records_by_state(self, state: State, username: Optional[str] = None) -> list[Record]:
        """Get all records with the given state."""
        if username:
            rows = await self.pool.fetch(
                "SELECT * FROM mrdocument.documents_v2 WHERE state = $1 AND username = $2",
                state.value, username,
            )
        else:
            rows = await self.pool.fetch(
                "SELECT * FROM mrdocument.documents_v2 WHERE state = $1",
                state.value,
            )
        return [self._row_to_record(row) for row in rows]

    async def get_record_by_source_hash(self, source_hash: str) -> Optional[Record]:
        """Get a record by source_hash (most recent first)."""
        row = await self.pool.fetchrow(
            """
            SELECT * FROM mrdocument.documents_v2
            WHERE source_hash = $1
            ORDER BY created_at DESC
            LIMIT 1
            """,
            source_hash,
        )
        if row is None:
            return None
        return self._row_to_record(row)

    async def get_record_by_hash(self, hash_value: str) -> Optional[Record]:
        """Get a record by hash (most recent first)."""
        row = await self.pool.fetchrow(
            """
            SELECT * FROM mrdocument.documents_v2
            WHERE hash = $1
            ORDER BY created_at DESC
            LIMIT 1
            """,
            hash_value,
        )
        if row is None:
            return None
        return self._row_to_record(row)

    async def get_record_by_output_filename(self, filename: str) -> Optional[Record]:
        """Get a record by output_filename."""
        row = await self.pool.fetchrow(
            """
            SELECT * FROM mrdocument.documents_v2
            WHERE output_filename = $1
            ORDER BY created_at DESC
            LIMIT 1
            """,
            filename,
        )
        if row is None:
            return None
        return self._row_to_record(row)

    async def get_snapshot(self, username: Optional[str] = None) -> list[Record]:
        """Get all records, optionally filtered by username."""
        if username:
            rows = await self.pool.fetch(
                "SELECT * FROM mrdocument.documents_v2 WHERE username = $1 ORDER BY created_at",
                username,
            )
        else:
            rows = await self.pool.fetch(
                "SELECT * FROM mrdocument.documents_v2 ORDER BY created_at"
            )
        return [self._row_to_record(row) for row in rows]

    async def get_records_with_temp_fields(self, username: Optional[str] = None) -> list[Record]:
        """Get records where any temp field is non-null or state=needs_deletion."""
        if username:
            rows = await self.pool.fetch(
                """
                SELECT * FROM mrdocument.documents_v2
                WHERE username = $1
                  AND (target_path IS NOT NULL
                    OR source_reference IS NOT NULL
                    OR current_reference IS NOT NULL
                    OR duplicate_sources != '[]'::jsonb
                    OR deleted_paths != '[]'::jsonb
                    OR state = 'needs_deletion')
                """,
                username,
            )
        else:
            rows = await self.pool.fetch(
                """
                SELECT * FROM mrdocument.documents_v2
                WHERE target_path IS NOT NULL
                   OR source_reference IS NOT NULL
                   OR current_reference IS NOT NULL
                   OR duplicate_sources != '[]'::jsonb
                   OR deleted_paths != '[]'::jsonb
                   OR state = 'needs_deletion'
                """
            )
        return [self._row_to_record(row) for row in rows]

    async def get_records_with_output_filename(self, username: Optional[str] = None) -> list[Record]:
        """Get records where output_filename is set."""
        if username:
            rows = await self.pool.fetch(
                """
                SELECT * FROM mrdocument.documents_v2
                WHERE output_filename IS NOT NULL AND username = $1
                """,
                username,
            )
        else:
            rows = await self.pool.fetch(
                """
                SELECT * FROM mrdocument.documents_v2
                WHERE output_filename IS NOT NULL
                """
            )
        return [self._row_to_record(row) for row in rows]
