"""Integration tests against the PostgreSQL database.

These tests run against the same postgres:17-alpine instance used by the
integration test stack (docker-compose.fast.yaml). The DB is expected at
localhost:5433 with credentials mrdocument:mrdocument.

Override with TEST_DATABASE_URL if needed.

The tests:
1. Connect as admin to apply the schema and create per-user roles.
2. Insert test data under two users.
3. Verify RLS isolation, query DSL, and content/summary retrieval.
4. Clean up test roles and data afterwards.
"""

import os
import uuid

import asyncpg
import pytest
import pytest_asyncio

from mcp_server.db import DatabaseManager
from mcp_server.tools import DocumentTools
from mcp_server.contexts import ContextReader

# Default: integration test DB exposed on localhost:5433
DB_HOST = os.environ.get("TEST_DB_HOST", "localhost")
DB_PORT = int(os.environ.get("TEST_DB_PORT", "5433"))
DB_NAME = os.environ.get("TEST_DB_NAME", "mrdocument")
DB_USER = os.environ.get("TEST_DB_USER", "mrdocument")
DB_PASSWORD = os.environ.get("TEST_DB_PASSWORD", "mrdocument")

pytestmark = pytest.mark.asyncio

# Test user credentials
USER_A = "mcp_test_user_a"
USER_A_PW = "test_pw_a_12345"
USER_B = "mcp_test_user_b"
USER_B_PW = "test_pw_b_67890"

# Test document IDs (fixed for predictable lookups)
DOC_A1_ID = uuid.UUID("00000000-0000-0000-0000-00000000aa01")
DOC_A2_ID = uuid.UUID("00000000-0000-0000-0000-00000000aa02")
DOC_B1_ID = uuid.UUID("00000000-0000-0000-0000-00000000bb01")


async def _try_connect() -> asyncpg.Connection | None:
    """Try to connect to the integration test database."""
    try:
        return await asyncpg.connect(
            host=DB_HOST, port=DB_PORT, database=DB_NAME,
            user=DB_USER, password=DB_PASSWORD, timeout=5,
        )
    except (OSError, asyncpg.PostgresError):
        return None


@pytest_asyncio.fixture(scope="module")
async def admin_conn():
    """Admin connection for setup/teardown. Skips if DB is unreachable."""
    conn = await _try_connect()
    if conn is None:
        pytest.skip(
            f"Integration DB not reachable at {DB_HOST}:{DB_PORT} "
            "(start the integration stack with make test-integration)"
        )

    # The watcher normally applies the schema, but it may not have run yet
    # in a test-only scenario. Apply the minimal schema needed for our tests.
    # The full schema is applied by the watcher container; we just ensure
    # RLS and the columns we query exist.
    await conn.execute("CREATE SCHEMA IF NOT EXISTS mrdocument")
    await conn.execute("""
        ALTER TABLE mrdocument.documents_v2 ENABLE ROW LEVEL SECURITY
    """)
    await conn.execute("""
        DROP POLICY IF EXISTS user_isolation ON mrdocument.documents_v2
    """)
    await conn.execute("""
        CREATE POLICY user_isolation ON mrdocument.documents_v2
            FOR ALL
            USING (username = current_user)
            WITH CHECK (username = current_user)
    """)

    # Create test roles
    for username, password in [(USER_A, USER_A_PW), (USER_B, USER_B_PW)]:
        exists = await conn.fetchval(
            "SELECT 1 FROM pg_roles WHERE rolname = $1", username
        )
        if exists:
            await conn.execute(f"REASSIGN OWNED BY {username} TO {DB_USER}")
            await conn.execute(f"DROP OWNED BY {username}")
            await conn.execute(f"DROP ROLE {username}")

        await conn.execute(
            f"CREATE ROLE {username} WITH LOGIN PASSWORD '{password}'"
        )
        await conn.execute(
            f"GRANT USAGE ON SCHEMA mrdocument TO {username}"
        )
        await conn.execute(
            f"GRANT SELECT, INSERT, UPDATE, DELETE "
            f"ON mrdocument.documents_v2 TO {username}"
        )

    # Insert test data as admin (bypasses RLS since table owner)
    await conn.execute(
        "DELETE FROM mrdocument.documents_v2 WHERE username IN ($1, $2)",
        USER_A, USER_B,
    )

    # User A documents
    await conn.execute(
        """
        INSERT INTO mrdocument.documents_v2
            (id, original_filename, source_hash, context, metadata, tags,
             description, summary, content, language, state, username, date_added)
        VALUES
            ($1, 'invoice_schulze.pdf', 'hash_a1', 'arbeit',
             '{"type": "Rechnung", "sender": "Schulze GmbH", "date": "2024-06-15"}'::jsonb,
             '["invoice", "urgent"]'::jsonb,
             'Invoice from Schulze GmbH',
             'Monthly invoice for consulting services from Schulze GmbH dated June 2024.',
             'Rechnung Nr. 2024-0615 von Schulze GmbH an MrDocument AG. Betrag: 5000 EUR.',
             'de', 'is_complete', $2, '2024-06-15'),
            ($3, 'contract_fischer.pdf', 'hash_a2', 'arbeit',
             '{"type": "Vertrag", "sender": "Fischer AG", "date": "2024-03-01"}'::jsonb,
             '["contract"]'::jsonb,
             'Service contract with Fischer AG',
             'Annual service contract for IT support with Fischer AG.',
             'Vertrag zwischen MrDocument AG und Fischer AG. Laufzeit: 12 Monate.',
             'de', 'is_complete', $2, '2024-03-01')
        """,
        DOC_A1_ID, USER_A, DOC_A2_ID,
    )

    # User B document
    await conn.execute(
        """
        INSERT INTO mrdocument.documents_v2
            (id, original_filename, source_hash, context, metadata, tags,
             description, summary, content, language, state, username, date_added)
        VALUES
            ($1, 'insurance_allianz.pdf', 'hash_b1', 'privat',
             '{"type": "Versicherung", "sender": "Allianz"}'::jsonb,
             '["insurance"]'::jsonb,
             'Insurance policy from Allianz',
             'Annual household insurance policy from Allianz SE.',
             'Versicherungspolice Nr. 12345 der Allianz SE. Hausratversicherung.',
             'de', 'is_complete', $2, '2024-01-10')
        """,
        DOC_B1_ID, USER_B,
    )

    yield conn

    # Teardown: remove test data and roles
    await conn.execute(
        "DELETE FROM mrdocument.documents_v2 WHERE username IN ($1, $2)",
        USER_A, USER_B,
    )
    for username in [USER_A, USER_B]:
        await conn.execute(f"REASSIGN OWNED BY {username} TO {DB_USER}")
        await conn.execute(f"DROP OWNED BY {username}")
        await conn.execute(f"DROP ROLE {username}")

    await conn.close()


@pytest_asyncio.fixture
async def db_manager(admin_conn):
    """DatabaseManager instance connected to the test database."""
    mgr = DatabaseManager(DB_HOST, DB_PORT, DB_NAME)
    await mgr.start()
    yield mgr
    await mgr.close()


@pytest_asyncio.fixture
async def pool_a(db_manager):
    """Connection pool for User A."""
    return await db_manager.get_pool(USER_A, USER_A_PW)


@pytest_asyncio.fixture
async def pool_b(db_manager):
    """Connection pool for User B."""
    return await db_manager.get_pool(USER_B, USER_B_PW)


@pytest_asyncio.fixture
def tools(db_manager, tmp_path):
    """DocumentTools with a real DB manager and dummy context reader."""
    reader = ContextReader(str(tmp_path))
    return DocumentTools(db_manager, reader)


# -----------------------------------------------------------------------
# RLS isolation tests
# -----------------------------------------------------------------------

class TestRLSIsolation:
    async def test_user_a_sees_only_own_documents(self, db_manager, pool_a):
        rows = await db_manager.execute_query(
            pool_a,
            "SELECT id, username FROM mrdocument.documents_v2",
            [],
        )
        usernames = {r["username"] for r in rows}
        assert usernames == {USER_A}
        assert len(rows) == 2

    async def test_user_b_sees_only_own_documents(self, db_manager, pool_b):
        rows = await db_manager.execute_query(
            pool_b,
            "SELECT id, username FROM mrdocument.documents_v2",
            [],
        )
        usernames = {r["username"] for r in rows}
        assert usernames == {USER_B}
        assert len(rows) == 1

    async def test_user_a_cannot_read_user_b_document(self, db_manager, pool_a):
        rows = await db_manager.execute_query(
            pool_a,
            "SELECT id FROM mrdocument.documents_v2 WHERE id = $1",
            [DOC_B1_ID],
        )
        assert rows == []

    async def test_user_b_cannot_read_user_a_document(self, db_manager, pool_b):
        rows = await db_manager.execute_query(
            pool_b,
            "SELECT id FROM mrdocument.documents_v2 WHERE id = $1",
            [DOC_A1_ID],
        )
        assert rows == []


# -----------------------------------------------------------------------
# Query DSL integration tests
# -----------------------------------------------------------------------

class TestQueryDSLIntegration:
    async def test_eq_filter(self, tools, pool_a):
        results = await tools.find_documents(
            pool_a, query={"context": {"$eq": "arbeit"}}
        )
        assert len(results) == 2
        for r in results:
            assert r["context"] == "arbeit"

    async def test_metadata_dot_notation(self, tools, pool_a):
        results = await tools.find_documents(
            pool_a, query={"metadata.sender": {"$eq": "Schulze GmbH"}}
        )
        assert len(results) == 1
        assert results[0]["original_filename"] == "invoice_schulze.pdf"

    async def test_metadata_ilike(self, tools, pool_a):
        results = await tools.find_documents(
            pool_a, query={"metadata.sender": {"$ilike": "%fischer%"}}
        )
        assert len(results) == 1
        assert results[0]["original_filename"] == "contract_fischer.pdf"

    async def test_tags_contains(self, tools, pool_a):
        results = await tools.find_documents(
            pool_a, query={"tags": {"$contains": "urgent"}}
        )
        assert len(results) == 1
        assert str(results[0]["id"]) == str(DOC_A1_ID)

    async def test_fulltext_search(self, tools, pool_a):
        results = await tools.find_documents(
            pool_a, query={"content": {"$search": "Schulze"}}
        )
        assert len(results) == 1
        assert str(results[0]["id"]) == str(DOC_A1_ID)

    async def test_date_range(self, tools, pool_a):
        results = await tools.find_documents(
            pool_a,
            query={"date_added": {"$gte": "2024-04-01", "$lte": "2024-12-31"}},
        )
        assert len(results) == 1
        assert str(results[0]["id"]) == str(DOC_A1_ID)

    async def test_or_combinator(self, tools, pool_a):
        results = await tools.find_documents(
            pool_a,
            query={
                "$or": [
                    {"metadata.type": {"$eq": "Rechnung"}},
                    {"metadata.type": {"$eq": "Vertrag"}},
                ]
            },
        )
        assert len(results) == 2

    async def test_and_combinator(self, tools, pool_a):
        results = await tools.find_documents(
            pool_a,
            query={
                "$and": [
                    {"metadata.type": {"$eq": "Rechnung"}},
                    {"metadata.sender": {"$eq": "Schulze GmbH"}},
                ]
            },
        )
        assert len(results) == 1

    async def test_empty_query_returns_all_user_docs(self, tools, pool_a):
        results = await tools.find_documents(pool_a, query={})
        assert len(results) == 2

    async def test_find_does_not_return_content_or_summary(self, tools, pool_a):
        results = await tools.find_documents(pool_a, query={})
        for r in results:
            assert "content" not in r
            assert "summary" not in r

    async def test_pagination(self, tools, pool_a):
        page1 = await tools.find_documents(pool_a, query={}, limit=1, offset=0)
        page2 = await tools.find_documents(pool_a, query={}, limit=1, offset=1)
        assert len(page1) == 1
        assert len(page2) == 1
        assert page1[0]["id"] != page2[0]["id"]

    async def test_metadata_exists(self, tools, pool_a):
        results = await tools.find_documents(
            pool_a, query={"metadata.sender": {"$exists": True}}
        )
        assert len(results) == 2

    async def test_ne_filter(self, tools, pool_a):
        results = await tools.find_documents(
            pool_a, query={"metadata.type": {"$ne": "Rechnung"}}
        )
        assert len(results) == 1
        assert results[0]["original_filename"] == "contract_fischer.pdf"


# -----------------------------------------------------------------------
# get_document_content / get_document_summary integration
# -----------------------------------------------------------------------

class TestDocumentContentSummary:
    async def test_get_content(self, tools, pool_a):
        result = await tools.get_document_content(pool_a, str(DOC_A1_ID))
        assert result is not None
        assert "Rechnung" in result["content"]
        assert "Schulze" in result["content"]

    async def test_get_summary(self, tools, pool_a):
        result = await tools.get_document_summary(pool_a, str(DOC_A1_ID))
        assert result is not None
        assert "Schulze" in result["summary"]

    async def test_get_content_other_user_returns_none(self, tools, pool_a):
        result = await tools.get_document_content(pool_a, str(DOC_B1_ID))
        assert result is None

    async def test_get_summary_other_user_returns_none(self, tools, pool_a):
        result = await tools.get_document_summary(pool_a, str(DOC_B1_ID))
        assert result is None

    async def test_get_content_nonexistent_returns_none(self, tools, pool_a):
        fake_id = "00000000-0000-0000-0000-ffffffffffff"
        result = await tools.get_document_content(pool_a, fake_id)
        assert result is None
