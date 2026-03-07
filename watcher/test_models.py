"""Tests for models.py — types, computed properties, and methods."""

from datetime import datetime, timezone, timedelta
from uuid import UUID, uuid4

import pytest

from models import State, EventType, PathEntry, ChangeItem, Record


# ---------------------------------------------------------------------------
# TestState
# ---------------------------------------------------------------------------

class TestState:
    def test_all_values_exist(self):
        expected = {
            "is_new", "needs_processing", "is_missing",
            "has_error", "needs_deletion", "is_deleted", "is_complete",
        }
        assert {s.value for s in State} == expected

    def test_string_equality(self):
        assert State.IS_NEW == "is_new"
        assert State.NEEDS_PROCESSING == "needs_processing"
        assert State.IS_MISSING == "is_missing"
        assert State.HAS_ERROR == "has_error"
        assert State.NEEDS_DELETION == "needs_deletion"
        assert State.IS_DELETED == "is_deleted"
        assert State.IS_COMPLETE == "is_complete"

    def test_invalid_raises_value_error(self):
        with pytest.raises(ValueError):
            State("nonexistent")


# ---------------------------------------------------------------------------
# TestPathEntry
# ---------------------------------------------------------------------------

class TestPathEntry:
    def test_creation(self):
        ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
        pe = PathEntry("archive/file.pdf", ts)
        assert pe.path == "archive/file.pdf"
        assert pe.timestamp == ts

    def test_sorting_by_timestamp(self):
        ts1 = datetime(2025, 1, 1, tzinfo=timezone.utc)
        ts2 = datetime(2025, 6, 1, tzinfo=timezone.utc)
        ts3 = datetime(2025, 3, 1, tzinfo=timezone.utc)
        entries = [
            PathEntry("c.pdf", ts2),
            PathEntry("a.pdf", ts1),
            PathEntry("b.pdf", ts3),
        ]
        sorted_entries = sorted(entries)
        assert sorted_entries[0].path == "a.pdf"
        assert sorted_entries[1].path == "b.pdf"
        assert sorted_entries[2].path == "c.pdf"

    def test_equality(self):
        ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
        a = PathEntry("archive/file.pdf", ts)
        b = PathEntry("archive/file.pdf", ts)
        assert a == b

    def test_hashable(self):
        ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
        a = PathEntry("archive/file.pdf", ts)
        b = PathEntry("archive/file.pdf", ts)
        c = PathEntry("archive/other.pdf", ts)
        s = {a, b, c}
        assert len(s) == 2


# ---------------------------------------------------------------------------
# TestRecordDefaults
# ---------------------------------------------------------------------------

class TestRecordDefaults:
    def test_minimal_construction(self):
        r = Record(original_filename="test.pdf", source_hash="abc123")
        assert r.original_filename == "test.pdf"
        assert r.source_hash == "abc123"
        assert isinstance(r.id, UUID)
        assert r.source_paths == []
        assert r.current_paths == []
        assert r.missing_source_paths == []
        assert r.missing_current_paths == []
        assert r.context is None
        assert r.metadata is None
        assert r.assigned_filename is None
        assert r.hash is None
        assert r.output_filename is None
        assert r.state == State.IS_NEW
        assert r.target_path is None
        assert r.source_reference is None
        assert r.current_reference is None
        assert r.duplicate_sources == []
        assert r.deleted_paths == []

    def test_partial_construction(self):
        ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
        pe = PathEntry("incoming/test.pdf", ts)
        r = Record(
            original_filename="test.pdf",
            source_hash="abc123",
            source_paths=[pe],
            state=State.NEEDS_PROCESSING,
            context="work",
        )
        assert r.source_paths == [pe]
        assert r.state == State.NEEDS_PROCESSING
        assert r.context == "work"

    def test_mutable_defaults_independent(self):
        r1 = Record(original_filename="a.pdf", source_hash="aaa")
        r2 = Record(original_filename="b.pdf", source_hash="bbb")
        ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
        r1.source_paths.append(PathEntry("incoming/a.pdf", ts))
        assert r2.source_paths == []
        r1.duplicate_sources.append("some/path")
        assert r2.duplicate_sources == []


# ---------------------------------------------------------------------------
# TestRecordComputedProperties
# ---------------------------------------------------------------------------

class TestRecordComputedProperties:
    def test_source_file_most_recent(self):
        ts1 = datetime(2025, 1, 1, tzinfo=timezone.utc)
        ts2 = datetime(2025, 6, 1, tzinfo=timezone.utc)
        r = Record(
            original_filename="test.pdf",
            source_hash="abc",
            source_paths=[
                PathEntry("incoming/test.pdf", ts1),
                PathEntry("archive/test.pdf", ts2),
            ],
        )
        assert r.source_file.path == "archive/test.pdf"
        assert r.source_file.timestamp == ts2

    def test_source_file_none_when_empty(self):
        r = Record(original_filename="test.pdf", source_hash="abc")
        assert r.source_file is None
        assert r.source_location is None
        assert r.source_location_path is None
        assert r.source_filename is None

    def test_current_file_none_when_empty(self):
        r = Record(original_filename="test.pdf", source_hash="abc")
        assert r.current_file is None
        assert r.current_location is None
        assert r.current_location_path is None
        assert r.current_filename is None

    def test_two_segment_path(self):
        ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
        r = Record(
            original_filename="file.pdf",
            source_hash="abc",
            source_paths=[PathEntry("archive/file.pdf", ts)],
        )
        assert r.source_location == "archive"
        assert r.source_location_path == ""
        assert r.source_filename == "file.pdf"

    def test_three_segment_path(self):
        ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
        r = Record(
            original_filename="file.pdf",
            source_hash="abc",
            source_paths=[PathEntry("archive/sub/file.pdf", ts)],
        )
        assert r.source_location == "archive"
        assert r.source_location_path == "sub"
        assert r.source_filename == "file.pdf"

    def test_deep_sorted_path(self):
        ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
        r = Record(
            original_filename="file.pdf",
            source_hash="abc",
            current_paths=[PathEntry("sorted/work/invoices/file.pdf", ts)],
        )
        assert r.current_location == "sorted"
        assert r.current_location_path == "work/invoices"
        assert r.current_filename == "file.pdf"

    def test_output_path(self):
        ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
        r = Record(
            original_filename="file.pdf",
            source_hash="abc",
            current_paths=[PathEntry(".output/some-uuid", ts)],
        )
        assert r.current_location == ".output"
        assert r.current_location_path == ""
        assert r.current_filename == "some-uuid"


# ---------------------------------------------------------------------------
# TestRecordClearTemporaryFields
# ---------------------------------------------------------------------------

class TestRecordClearTemporaryFields:
    def test_all_fields_cleared(self):
        r = Record(
            original_filename="test.pdf",
            source_hash="abc",
            target_path="sorted/work/test.pdf",
            source_reference="incoming/test.pdf",
            current_reference=".output/uuid",
            duplicate_sources=["incoming/copy.pdf"],
            deleted_paths=["trash/old.pdf"],
        )
        r.clear_temporary_fields()
        assert r.target_path is None
        assert r.source_reference is None
        assert r.current_reference is None
        assert r.duplicate_sources == []
        assert r.deleted_paths == []


# ---------------------------------------------------------------------------
# TestChangeItem
# ---------------------------------------------------------------------------

class TestChangeItem:
    def test_addition_with_hash_and_size(self):
        ci = ChangeItem(
            event_type=EventType.ADDITION,
            path="incoming/new.pdf",
            hash="sha256abc",
            size=1024,
        )
        assert ci.event_type == EventType.ADDITION
        assert ci.path == "incoming/new.pdf"
        assert ci.hash == "sha256abc"
        assert ci.size == 1024

    def test_removal_without_hash_and_size(self):
        ci = ChangeItem(
            event_type=EventType.REMOVAL,
            path="archive/old.pdf",
        )
        assert ci.event_type == EventType.REMOVAL
        assert ci.path == "archive/old.pdf"
        assert ci.hash is None
        assert ci.size is None
