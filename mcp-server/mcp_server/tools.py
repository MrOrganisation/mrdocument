"""MCP tool implementations for document search and context enumeration."""

import uuid
from typing import Any

import asyncpg

from .contexts import ContextReadError, ContextReader
from .db import DatabaseManager
from .query_dsl import QueryBuilder, QueryBuildError

# Columns returned by find_documents (excludes content and summary)
FIND_SELECT_COLUMNS = [
    "id",
    "original_filename",
    "context",
    "metadata",
    "tags",
    "description",
    "assigned_filename",
    "language",
    "state",
    "source_hash",
    "date_added",
    "created_at",
    "updated_at",
    "source_paths",
    "current_paths",
]

# Columns allowed in ORDER BY
ALLOWED_ORDER_COLUMNS = {
    "created_at",
    "updated_at",
    "original_filename",
    "date_added",
    "assigned_filename",
}


class DocumentTools:
    """Implements all MCP tools for document search and context enumeration."""

    def __init__(
        self,
        db_manager: DatabaseManager,
        context_reader: ContextReader,
    ) -> None:
        self._db = db_manager
        self._ctx = context_reader

    # --- Database tools ---

    async def find_documents(
        self,
        pool: asyncpg.Pool,
        query: dict,
        limit: int = 50,
        offset: int = 0,
        order_by: str = "created_at",
        order_dir: str = "desc",
    ) -> list[dict]:
        """Search documents using the query DSL.

        Returns records without content and summary fields.
        """
        # Build WHERE clause from DSL
        builder = QueryBuilder()
        where_clause, params = builder.build(query)

        # Sanitize ORDER BY
        if order_by not in ALLOWED_ORDER_COLUMNS:
            order_by = "created_at"
        if order_dir.lower() not in ("asc", "desc"):
            order_dir = "desc"

        # Add LIMIT and OFFSET as parameters
        params.append(limit)
        limit_placeholder = f"${len(params)}"
        params.append(offset)
        offset_placeholder = f"${len(params)}"

        select = ", ".join(FIND_SELECT_COLUMNS)
        where = f"WHERE {where_clause}" if where_clause else ""

        sql = (
            f"SELECT {select} FROM mrdocument.documents_v2 "
            f"{where} "
            f"ORDER BY {order_by} {order_dir} "
            f"LIMIT {limit_placeholder} OFFSET {offset_placeholder}"
        )

        return await self._db.execute_query(pool, sql, params)

    async def get_document_content(
        self, pool: asyncpg.Pool, document_id: str
    ) -> dict | None:
        """Fetch the full text content for a single document.

        Returns None if the document is not found (or belongs to another user).
        """
        doc_id = uuid.UUID(document_id)
        sql = (
            "SELECT id, original_filename, content "
            "FROM mrdocument.documents_v2 WHERE id = $1"
        )
        rows = await self._db.execute_query(pool, sql, [doc_id])
        return rows[0] if rows else None

    async def get_document_summary(
        self, pool: asyncpg.Pool, document_id: str
    ) -> dict | None:
        """Fetch the summary for a single document.

        Returns None if the document is not found (or belongs to another user).
        """
        doc_id = uuid.UUID(document_id)
        sql = (
            "SELECT id, original_filename, summary "
            "FROM mrdocument.documents_v2 WHERE id = $1"
        )
        rows = await self._db.execute_query(pool, sql, [doc_id])
        return rows[0] if rows else None

    # --- Filesystem tools ---

    def list_contexts(self, username: str) -> list[dict]:
        """Enumerate all contexts available to the user."""
        return self._ctx.list_contexts(username)

    def list_fields(self, username: str, context: str) -> list[dict]:
        """Enumerate all fields for a given context."""
        return self._ctx.list_fields(username, context)

    def list_candidates(self, username: str, context: str, field: str) -> dict:
        """Return the merged candidate list for a context field."""
        return self._ctx.list_candidates(username, context, field)
