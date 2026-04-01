"""Tests for the tool implementations."""

import pytest
from unittest.mock import AsyncMock, MagicMock

from mcp_server.contexts import ContextReader
from mcp_server.db import DatabaseManager
from mcp_server.tools import DocumentTools


@pytest.fixture
def mock_db():
    db = MagicMock(spec=DatabaseManager)
    db.execute_query = AsyncMock(return_value=[])
    return db


@pytest.fixture
def tools(mock_db, tmp_sync_root):
    reader = ContextReader(str(tmp_sync_root), subdir="")
    return DocumentTools(mock_db, reader)


@pytest.fixture
def mock_pool():
    return MagicMock()


class TestFindDocuments:
    @pytest.mark.asyncio
    async def test_empty_query(self, tools, mock_db, mock_pool):
        await tools.find_documents(mock_pool, query={})
        mock_db.execute_query.assert_called_once()
        sql = mock_db.execute_query.call_args[0][1]
        assert "WHERE" not in sql
        assert "LIMIT" in sql
        assert "OFFSET" in sql

    @pytest.mark.asyncio
    async def test_query_with_filter(self, tools, mock_db, mock_pool):
        await tools.find_documents(
            mock_pool, query={"context": {"$eq": "arbeit"}}
        )
        sql = mock_db.execute_query.call_args[0][1]
        assert "WHERE" in sql
        assert "context = $1" in sql

    @pytest.mark.asyncio
    async def test_excludes_content_and_summary(self, tools, mock_db, mock_pool):
        await tools.find_documents(mock_pool, query={})
        sql = mock_db.execute_query.call_args[0][1]
        # Should NOT select content or summary
        select_part = sql.split("FROM")[0]
        assert "summary" not in select_part
        # content is tricky because "content" could match "source_content_hash"
        # but we don't select that either. Let's check the columns list.
        assert ", content," not in select_part
        assert "SELECT content" not in select_part

    @pytest.mark.asyncio
    async def test_order_by_sanitization(self, tools, mock_db, mock_pool):
        await tools.find_documents(
            mock_pool, query={}, order_by="DROP TABLE", order_dir="asc"
        )
        sql = mock_db.execute_query.call_args[0][1]
        # Invalid order_by should fall back to created_at
        assert "created_at asc" in sql

    @pytest.mark.asyncio
    async def test_order_dir_sanitization(self, tools, mock_db, mock_pool):
        await tools.find_documents(
            mock_pool, query={}, order_dir="DROP"
        )
        sql = mock_db.execute_query.call_args[0][1]
        assert "desc" in sql

    @pytest.mark.asyncio
    async def test_limit_and_offset(self, tools, mock_db, mock_pool):
        await tools.find_documents(
            mock_pool, query={}, limit=10, offset=20
        )
        params = mock_db.execute_query.call_args[0][2]
        assert 10 in params
        assert 20 in params


class TestGetDocumentContent:
    @pytest.mark.asyncio
    async def test_found(self, tools, mock_db, mock_pool):
        mock_db.execute_query.return_value = [
            {"id": "abc-123", "original_filename": "test.pdf", "content": "hello"}
        ]
        result = await tools.get_document_content(
            mock_pool, "00000000-0000-0000-0000-000000000001"
        )
        assert result["content"] == "hello"

    @pytest.mark.asyncio
    async def test_not_found(self, tools, mock_db, mock_pool):
        mock_db.execute_query.return_value = []
        result = await tools.get_document_content(
            mock_pool, "00000000-0000-0000-0000-000000000001"
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_invalid_uuid_raises(self, tools, mock_db, mock_pool):
        with pytest.raises(ValueError):
            await tools.get_document_content(mock_pool, "not-a-uuid")


class TestGetDocumentSummary:
    @pytest.mark.asyncio
    async def test_found(self, tools, mock_db, mock_pool):
        mock_db.execute_query.return_value = [
            {"id": "abc-123", "original_filename": "test.pdf", "summary": "A summary"}
        ]
        result = await tools.get_document_summary(
            mock_pool, "00000000-0000-0000-0000-000000000001"
        )
        assert result["summary"] == "A summary"


class TestListContexts:
    def test_returns_contexts(self, tools):
        contexts = tools.list_contexts("testuser")
        names = {c["name"] for c in contexts}
        assert "privat" in names
        assert "arbeit" in names


class TestListFields:
    def test_returns_fields(self, tools):
        fields = tools.list_fields("testuser", "arbeit")
        names = {f["name"] for f in fields}
        assert "type" in names
        assert "sender" in names


class TestListCandidates:
    def test_returns_candidates(self, tools):
        result = tools.list_candidates("testuser", "privat", "type")
        assert len(result["candidates"]) == 3
