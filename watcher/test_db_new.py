"""Tests for db_new.py — DocumentDBv2 CRUD, queries, and JSONB round-trips."""

import os
from datetime import datetime, timezone, timedelta
from uuid import uuid4

import pytest
import pytest_asyncio

from models import State, PathEntry, Record
from db_new import DocumentDBv2


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def db():
    """Connect to test database, yield, cleanup, disconnect."""
    database_url = os.environ.get(
        "DATABASE_URL",
        "postgresql://mrdocument:mrdocument@localhost:5432/mrdocument",
    )
    db = DocumentDBv2(database_url=database_url)
    await db.connect()
    yield db
    # Cleanup: delete all rows from documents_v2
    await db.pool.execute("DELETE FROM mrdocument.documents_v2")
    await db.disconnect()


def _make_record(**kwargs) -> Record:
    """Helper to create a Record with sensible defaults."""
    defaults = {
        "original_filename": "test.pdf",
        "source_hash": "abc123",
    }
    defaults.update(kwargs)
    return Record(**defaults)


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------

class TestCRUD:
    @pytest.mark.asyncio
    async def test_create_and_get(self, db):
        r = _make_record()
        rid = await db.create_record(r)
        assert rid == r.id

        fetched = await db.get_record(rid)
        assert fetched is not None
        assert fetched.id == r.id
        assert fetched.original_filename == "test.pdf"
        assert fetched.source_hash == "abc123"
        assert fetched.state == State.IS_NEW

    @pytest.mark.asyncio
    async def test_get_nonexistent(self, db):
        result = await db.get_record(uuid4())
        assert result is None

    @pytest.mark.asyncio
    async def test_save_updates(self, db):
        r = _make_record()
        await db.create_record(r)

        r.state = State.NEEDS_PROCESSING
        r.context = "work"
        r.output_filename = "output-uuid"
        await db.save_record(r)

        fetched = await db.get_record(r.id)
        assert fetched.state == State.NEEDS_PROCESSING
        assert fetched.context == "work"
        assert fetched.output_filename == "output-uuid"

    @pytest.mark.asyncio
    async def test_delete(self, db):
        r = _make_record()
        await db.create_record(r)

        deleted = await db.delete_record(r.id)
        assert deleted is True

        fetched = await db.get_record(r.id)
        assert fetched is None

    @pytest.mark.asyncio
    async def test_delete_nonexistent(self, db):
        deleted = await db.delete_record(uuid4())
        assert deleted is False


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

class TestQueries:
    @pytest.mark.asyncio
    async def test_get_records_by_state(self, db):
        r1 = _make_record(source_hash="h1", state=State.IS_NEW)
        r2 = _make_record(source_hash="h2", state=State.NEEDS_PROCESSING)
        r3 = _make_record(source_hash="h3", state=State.IS_NEW)
        await db.create_record(r1)
        await db.create_record(r2)
        await db.create_record(r3)

        new_records = await db.get_records_by_state(State.IS_NEW)
        assert len(new_records) == 2
        ids = {r.id for r in new_records}
        assert r1.id in ids
        assert r3.id in ids

    @pytest.mark.asyncio
    async def test_get_record_by_source_hash(self, db):
        r = _make_record(source_hash="unique_source")
        await db.create_record(r)

        fetched = await db.get_record_by_source_hash("unique_source")
        assert fetched is not None
        assert fetched.id == r.id

    @pytest.mark.asyncio
    async def test_get_record_by_source_hash_not_found(self, db):
        fetched = await db.get_record_by_source_hash("nonexistent")
        assert fetched is None

    @pytest.mark.asyncio
    async def test_get_record_by_hash(self, db):
        r = _make_record(hash="content_hash_abc")
        await db.create_record(r)

        fetched = await db.get_record_by_hash("content_hash_abc")
        assert fetched is not None
        assert fetched.id == r.id

    @pytest.mark.asyncio
    async def test_get_record_by_hash_not_found(self, db):
        fetched = await db.get_record_by_hash("nonexistent")
        assert fetched is None

    @pytest.mark.asyncio
    async def test_get_record_by_output_filename(self, db):
        r = _make_record(output_filename="out-uuid-123")
        await db.create_record(r)

        fetched = await db.get_record_by_output_filename("out-uuid-123")
        assert fetched is not None
        assert fetched.id == r.id

    @pytest.mark.asyncio
    async def test_get_record_by_output_filename_not_found(self, db):
        fetched = await db.get_record_by_output_filename("nonexistent")
        assert fetched is None

    @pytest.mark.asyncio
    async def test_get_snapshot(self, db):
        r1 = _make_record(source_hash="s1")
        r2 = _make_record(source_hash="s2")
        r3 = _make_record(source_hash="s3")
        await db.create_record(r1)
        await db.create_record(r2)
        await db.create_record(r3)

        snapshot = await db.get_snapshot()
        assert len(snapshot) == 3

    @pytest.mark.asyncio
    async def test_get_records_with_temp_fields(self, db):
        r1 = _make_record(source_hash="t1", target_path="sorted/work/file.pdf")
        r2 = _make_record(source_hash="t2")  # no temp fields
        r3 = _make_record(source_hash="t3", source_reference="incoming/file.pdf")
        r4 = _make_record(source_hash="t4", state=State.NEEDS_DELETION)
        await db.create_record(r1)
        await db.create_record(r2)
        await db.create_record(r3)
        await db.create_record(r4)

        results = await db.get_records_with_temp_fields()
        ids = {r.id for r in results}
        assert r1.id in ids
        assert r2.id not in ids
        assert r3.id in ids
        assert r4.id in ids

    @pytest.mark.asyncio
    async def test_get_records_with_output_filename(self, db):
        r1 = _make_record(source_hash="o1", output_filename="out1")
        r2 = _make_record(source_hash="o2")  # no output_filename
        r3 = _make_record(source_hash="o3", output_filename="out3")
        await db.create_record(r1)
        await db.create_record(r2)
        await db.create_record(r3)

        results = await db.get_records_with_output_filename()
        ids = {r.id for r in results}
        assert r1.id in ids
        assert r2.id not in ids
        assert r3.id in ids


# ---------------------------------------------------------------------------
# Username filtering
# ---------------------------------------------------------------------------

class TestUsernameFiltering:
    @pytest.mark.asyncio
    async def test_get_snapshot_filters_by_username(self, db):
        r1 = _make_record(source_hash="u1", username="heike")
        r2 = _make_record(source_hash="u2", username="ole")
        r3 = _make_record(source_hash="u3", username="heike")
        await db.create_record(r1)
        await db.create_record(r2)
        await db.create_record(r3)

        heike_records = await db.get_snapshot("heike")
        assert len(heike_records) == 2
        ids = {r.id for r in heike_records}
        assert r1.id in ids
        assert r3.id in ids

        ole_records = await db.get_snapshot("ole")
        assert len(ole_records) == 1
        assert ole_records[0].id == r2.id

    @pytest.mark.asyncio
    async def test_get_snapshot_without_username_returns_all(self, db):
        r1 = _make_record(source_hash="a1", username="heike")
        r2 = _make_record(source_hash="a2", username="ole")
        await db.create_record(r1)
        await db.create_record(r2)

        all_records = await db.get_snapshot()
        assert len(all_records) == 2

    @pytest.mark.asyncio
    async def test_get_records_with_temp_fields_filters_by_username(self, db):
        r1 = _make_record(source_hash="tf1", username="heike",
                          target_path="sorted/work/file.pdf")
        r2 = _make_record(source_hash="tf2", username="ole",
                          source_reference="incoming/file.pdf")
        await db.create_record(r1)
        await db.create_record(r2)

        heike = await db.get_records_with_temp_fields("heike")
        assert len(heike) == 1
        assert heike[0].id == r1.id

        ole = await db.get_records_with_temp_fields("ole")
        assert len(ole) == 1
        assert ole[0].id == r2.id

    @pytest.mark.asyncio
    async def test_get_records_with_output_filename_filters_by_username(self, db):
        r1 = _make_record(source_hash="of1", username="heike",
                          output_filename="out1")
        r2 = _make_record(source_hash="of2", username="ole",
                          output_filename="out2")
        await db.create_record(r1)
        await db.create_record(r2)

        heike = await db.get_records_with_output_filename("heike")
        assert len(heike) == 1
        assert heike[0].id == r1.id

    @pytest.mark.asyncio
    async def test_username_round_trip(self, db):
        r = _make_record(username="heike")
        await db.create_record(r)

        fetched = await db.get_record(r.id)
        assert fetched.username == "heike"

    @pytest.mark.asyncio
    async def test_get_records_by_state_filters_by_username(self, db):
        r1 = _make_record(source_hash="s1", username="heike", state=State.IS_NEW)
        r2 = _make_record(source_hash="s2", username="ole", state=State.IS_NEW)
        await db.create_record(r1)
        await db.create_record(r2)

        heike = await db.get_records_by_state(State.IS_NEW, "heike")
        assert len(heike) == 1
        assert heike[0].id == r1.id


# ---------------------------------------------------------------------------
# JSONB round-trips
# ---------------------------------------------------------------------------

class TestJSONBRoundTrips:
    @pytest.mark.asyncio
    async def test_path_entry_list_round_trip(self, db):
        ts1 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        ts2 = datetime(2025, 6, 15, 8, 30, 0, tzinfo=timezone.utc)
        r = _make_record(
            source_paths=[
                PathEntry("incoming/test.pdf", ts1),
                PathEntry("archive/test.pdf", ts2),
            ],
            current_paths=[
                PathEntry("sorted/work/test.pdf", ts2),
            ],
        )
        await db.create_record(r)

        fetched = await db.get_record(r.id)
        assert len(fetched.source_paths) == 2
        assert fetched.source_paths[0].path == "incoming/test.pdf"
        assert fetched.source_paths[0].timestamp == ts1
        assert fetched.source_paths[1].path == "archive/test.pdf"
        assert fetched.source_paths[1].timestamp == ts2
        assert len(fetched.current_paths) == 1
        assert fetched.current_paths[0].path == "sorted/work/test.pdf"

    @pytest.mark.asyncio
    async def test_metadata_dict_round_trip(self, db):
        meta = {
            "title": "Invoice 2025-001",
            "date": "2025-01-15",
            "amount": 99.50,
            "tags": ["finance", "invoice"],
        }
        r = _make_record(metadata=meta)
        await db.create_record(r)

        fetched = await db.get_record(r.id)
        assert fetched.metadata == meta

    @pytest.mark.asyncio
    async def test_empty_lists_round_trip(self, db):
        r = _make_record()
        await db.create_record(r)

        fetched = await db.get_record(r.id)
        assert fetched.source_paths == []
        assert fetched.current_paths == []
        assert fetched.missing_source_paths == []
        assert fetched.missing_current_paths == []
        assert fetched.duplicate_sources == []
        assert fetched.deleted_paths == []

    @pytest.mark.asyncio
    async def test_duplicate_sources_round_trip(self, db):
        r = _make_record(
            duplicate_sources=["incoming/copy1.pdf", "incoming/copy2.pdf"],
            deleted_paths=["trash/old.pdf"],
        )
        await db.create_record(r)

        fetched = await db.get_record(r.id)
        assert fetched.duplicate_sources == ["incoming/copy1.pdf", "incoming/copy2.pdf"]
        assert fetched.deleted_paths == ["trash/old.pdf"]

    @pytest.mark.asyncio
    async def test_null_metadata_round_trip(self, db):
        r = _make_record(metadata=None)
        await db.create_record(r)

        fetched = await db.get_record(r.id)
        assert fetched.metadata is None

    @pytest.mark.asyncio
    async def test_missing_paths_round_trip(self, db):
        ts = datetime(2025, 3, 1, tzinfo=timezone.utc)
        r = _make_record(
            missing_source_paths=[PathEntry("incoming/gone.pdf", ts)],
            missing_current_paths=[PathEntry("sorted/work/gone.pdf", ts)],
        )
        await db.create_record(r)

        fetched = await db.get_record(r.id)
        assert len(fetched.missing_source_paths) == 1
        assert fetched.missing_source_paths[0].path == "incoming/gone.pdf"
        assert len(fetched.missing_current_paths) == 1
        assert fetched.missing_current_paths[0].path == "sorted/work/gone.pdf"

    @pytest.mark.asyncio
    async def test_full_record_round_trip(self, db):
        """All fields populated, round-trip through DB."""
        ts1 = datetime(2025, 1, 1, tzinfo=timezone.utc)
        ts2 = datetime(2025, 6, 1, tzinfo=timezone.utc)
        r = Record(
            original_filename="invoice.pdf",
            source_hash="src_hash_abc",
            source_paths=[PathEntry("incoming/invoice.pdf", ts1)],
            current_paths=[PathEntry("sorted/work/invoice.pdf", ts2)],
            missing_source_paths=[PathEntry("incoming/old.pdf", ts1)],
            missing_current_paths=[],
            context="work",
            metadata={"title": "Invoice", "amount": 100},
            assigned_filename="2025-01-Invoice.pdf",
            hash="content_hash_xyz",
            output_filename="out-uuid",
            state=State.IS_COMPLETE,
            target_path="sorted/work/2025-01-Invoice.pdf",
            source_reference="incoming/invoice.pdf",
            current_reference=".output/out-uuid",
            duplicate_sources=["incoming/copy.pdf"],
            deleted_paths=["trash/invoice.pdf"],
            username="heike",
        )
        await db.create_record(r)

        fetched = await db.get_record(r.id)
        assert fetched.original_filename == r.original_filename
        assert fetched.source_hash == r.source_hash
        assert fetched.context == r.context
        assert fetched.metadata == r.metadata
        assert fetched.assigned_filename == r.assigned_filename
        assert fetched.hash == r.hash
        assert fetched.output_filename == r.output_filename
        assert fetched.state == r.state
        assert fetched.target_path == r.target_path
        assert fetched.source_reference == r.source_reference
        assert fetched.current_reference == r.current_reference
        assert fetched.duplicate_sources == r.duplicate_sources
        assert fetched.deleted_paths == r.deleted_paths
        assert fetched.username == r.username
        assert len(fetched.source_paths) == 1
        assert len(fetched.current_paths) == 1
        assert len(fetched.missing_source_paths) == 1
