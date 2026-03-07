"""Tests for step2.py — preprocess and reconcile pure functions."""

from datetime import datetime, timezone, timedelta
from uuid import uuid4

import pytest

from models import State, EventType, PathEntry, ChangeItem, Record
from step2 import preprocess, reconcile, compute_target_path, _is_collision_variant


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts(offset_hours=0):
    """Create a UTC timestamp with optional hour offset."""
    return datetime(2025, 1, 1, tzinfo=timezone.utc) + timedelta(hours=offset_hours)


def _make_record(**kwargs) -> Record:
    """Create a Record with sensible defaults."""
    defaults = {
        "original_filename": "test.pdf",
        "source_hash": "abc123",
    }
    defaults.update(kwargs)
    return Record(**defaults)


def _make_change(event_type, path, hash=None, size=None):
    """Create a ChangeItem."""
    return ChangeItem(event_type=event_type, path=path, hash=hash, size=size)


def _noop_sidecar(path):
    """Default sidecar reader returning empty dict."""
    return {}


# ---------------------------------------------------------------------------
# Preprocessing: .output additions
# ---------------------------------------------------------------------------

class TestPreprocessOutput:
    def test_output_matches_sidecar_ingested(self):
        """Addition in .output, matches output_filename → sidecar ingested."""
        record = _make_record(
            output_filename="uuid-123",
            state=State.NEEDS_PROCESSING,
        )
        change = _make_change(
            EventType.ADDITION, ".output/uuid-123",
            hash="content_hash", size=1024,
        )
        sidecar_data = {
            "context": "work",
            "metadata": {"date": "2025-01-01", "title": "Invoice"},
            "assigned_filename": "2025-01-Invoice.pdf",
        }

        modified, new = preprocess([change], [record], lambda p: sidecar_data)

        assert len(modified) == 1
        assert modified[0] is record
        assert record.context == "work"
        assert record.metadata == {"date": "2025-01-01", "title": "Invoice"}
        assert record.assigned_filename == "2025-01-Invoice.pdf"
        assert record.hash == "content_hash"
        assert record.output_filename is None
        assert len(record.current_paths) == 1
        assert record.current_paths[0].path == ".output/uuid-123"
        assert new == []

    def test_output_zero_byte_sets_error(self):
        """Addition in .output, 0-byte → has_error set."""
        record = _make_record(
            output_filename="uuid-456",
            state=State.NEEDS_PROCESSING,
        )
        change = _make_change(
            EventType.ADDITION, ".output/uuid-456",
            hash="empty_hash", size=0,
        )

        modified, new = preprocess([change], [record], _noop_sidecar)

        assert len(modified) == 1
        assert record.state == State.HAS_ERROR
        assert record.output_filename is None
        assert new == []

    def test_output_no_match_ignored(self):
        """Addition in .output, no match → ignored."""
        record = _make_record(output_filename="other-uuid")
        change = _make_change(
            EventType.ADDITION, ".output/unknown-uuid",
            hash="some_hash", size=100,
        )

        modified, new = preprocess([change], [record], _noop_sidecar)

        assert modified == []
        assert new == []


# ---------------------------------------------------------------------------
# Preprocessing: incoming additions
# ---------------------------------------------------------------------------

class TestPreprocessIncoming:
    def test_incoming_matches_source_hash(self):
        """Addition in incoming, matches source_hash → added to source_paths."""
        record = _make_record(source_hash="file_hash_abc")
        change = _make_change(
            EventType.ADDITION, "incoming/doc.pdf",
            hash="file_hash_abc", size=500,
        )

        modified, new = preprocess([change], [record], _noop_sidecar)

        assert len(modified) == 1
        assert modified[0] is record
        assert any(pe.path == "incoming/doc.pdf" for pe in record.source_paths)
        assert new == []

    def test_incoming_unknown_creates_new(self):
        """Addition in incoming, unknown → new record created."""
        record = _make_record(source_hash="existing_hash")
        change = _make_change(
            EventType.ADDITION, "incoming/new.pdf",
            hash="new_hash", size=1024,
        )

        modified, new = preprocess([change], [record], _noop_sidecar)

        assert modified == []
        assert len(new) == 1
        assert new[0].original_filename == "new.pdf"
        assert new[0].source_hash == "new_hash"
        assert new[0].source_paths[0].path == "incoming/new.pdf"

    def test_two_identical_sources_same_cycle(self):
        """Two files with the same source_hash in one cycle should produce
        one record with both paths, not two separate records."""
        change_a = _make_change(
            EventType.ADDITION, "incoming/a.pdf",
            hash="same_hash", size=500,
        )
        change_b = _make_change(
            EventType.ADDITION, "incoming/b.pdf",
            hash="same_hash", size=500,
        )

        modified, new = preprocess([change_a, change_b], [], _noop_sidecar)

        assert len(new) == 1, (
            f"Expected 1 record for identical sources, got {len(new)}"
        )
        paths = [pe.path for pe in new[0].source_paths]
        assert "incoming/a.pdf" in paths
        assert "incoming/b.pdf" in paths


# ---------------------------------------------------------------------------
# Preprocessing: sorted additions
# ---------------------------------------------------------------------------

class TestPreprocessSorted:
    def test_sorted_matches_hash(self):
        """Addition in sorted, matches hash → added to current_paths."""
        record = _make_record(hash="sorted_hash")
        change = _make_change(
            EventType.ADDITION, "sorted/work/doc.pdf",
            hash="sorted_hash", size=800,
        )

        modified, new = preprocess([change], [record], _noop_sidecar)

        assert len(modified) == 1
        assert any(pe.path == "sorted/work/doc.pdf" for pe in record.current_paths)

    def test_sorted_matches_source_hash_not_hash(self):
        """Addition in sorted, matches source_hash but not hash → source_paths."""
        record = _make_record(source_hash="src_hash", hash="different_hash")
        change = _make_change(
            EventType.ADDITION, "sorted/work/original.pdf",
            hash="src_hash", size=600,
        )

        modified, new = preprocess([change], [record], _noop_sidecar)

        assert len(modified) == 1
        assert any(
            pe.path == "sorted/work/original.pdf"
            for pe in record.source_paths
        )
        assert not any(
            pe.path == "sorted/work/original.pdf"
            for pe in record.current_paths
        )

    def test_sorted_unknown_creates_new(self):
        """Addition in sorted, unknown → new record with context from dir."""
        change = _make_change(
            EventType.ADDITION, "sorted/personal/unknown.pdf",
            hash="unknown_hash", size=300,
        )

        modified, new = preprocess([change], [], _noop_sidecar)

        assert modified == []
        assert len(new) == 1
        assert new[0].original_filename == "unknown.pdf"
        assert new[0].context == "personal"


# ---------------------------------------------------------------------------
# Preprocessing: other locations
# ---------------------------------------------------------------------------

class TestPreprocessOtherLocations:
    def test_archive_matches_source_hash(self):
        """Addition in archive, matches source_hash → added to source_paths."""
        record = _make_record(source_hash="arch_hash")
        change = _make_change(
            EventType.ADDITION, "archive/doc.pdf",
            hash="arch_hash", size=500,
        )

        modified, new = preprocess([change], [record], _noop_sidecar)

        assert len(modified) == 1
        assert any(pe.path == "archive/doc.pdf" for pe in record.source_paths)

    def test_missing_matches_source_hash(self):
        """Addition in missing, matches source_hash → added to source_paths."""
        record = _make_record(source_hash="miss_hash")
        change = _make_change(
            EventType.ADDITION, "missing/doc.pdf",
            hash="miss_hash", size=500,
        )

        modified, new = preprocess([change], [record], _noop_sidecar)

        assert len(modified) == 1
        assert any(pe.path == "missing/doc.pdf" for pe in record.source_paths)

    def test_missing_both_match_uses_source_hash(self):
        """Missing file matches both hash and source_hash → only source_hash used."""
        record = _make_record(
            source_hash="both_hash",
            hash="both_hash",
        )
        change = _make_change(
            EventType.ADDITION, "missing/doc.pdf",
            hash="both_hash", size=500,
        )

        modified, new = preprocess([change], [record], _noop_sidecar)

        assert len(modified) == 1
        assert any(pe.path == "missing/doc.pdf" for pe in record.source_paths)
        assert not any(
            pe.path == "missing/doc.pdf" for pe in record.current_paths
        )

    def test_missing_unknown_not_created(self):
        """Addition in missing, unknown → not created (no new records)."""
        change = _make_change(
            EventType.ADDITION, "missing/stray.pdf",
            hash="stray_hash", size=500,
        )

        modified, new = preprocess([change], [], _noop_sidecar)

        assert modified == []
        assert new == []

    def test_archive_unknown_not_created(self):
        """Addition in archive, unknown → not created (stray handled by Step 1)."""
        change = _make_change(
            EventType.ADDITION, "archive/stray.pdf",
            hash="stray_hash", size=500,
        )

        modified, new = preprocess([change], [], _noop_sidecar)

        assert modified == []
        assert new == []

    def test_processed_matches_hash(self):
        """Addition in processed, matches hash → added to current_paths."""
        record = _make_record(hash="proc_hash")
        change = _make_change(
            EventType.ADDITION, "processed/doc.pdf",
            hash="proc_hash", size=700,
        )

        modified, new = preprocess([change], [record], _noop_sidecar)

        assert len(modified) == 1
        assert any(pe.path == "processed/doc.pdf" for pe in record.current_paths)

    def test_trash_matches_source_hash(self):
        """Addition in trash, matches source_hash → added to source_paths."""
        record = _make_record(source_hash="trash_hash")
        change = _make_change(
            EventType.ADDITION, "trash/doc.pdf",
            hash="trash_hash", size=500,
        )

        modified, new = preprocess([change], [record], _noop_sidecar)

        assert len(modified) == 1
        assert any(pe.path == "trash/doc.pdf" for pe in record.source_paths)


# ---------------------------------------------------------------------------
# Preprocessing: removals
# ---------------------------------------------------------------------------

class TestPreprocessRemovals:
    def test_removal_from_source_paths(self):
        """Removal from source_paths → moved to missing_source_paths."""
        record = _make_record(
            source_paths=[PathEntry("incoming/doc.pdf", _ts())],
        )
        change = _make_change(EventType.REMOVAL, "incoming/doc.pdf")

        modified, new = preprocess([change], [record], _noop_sidecar)

        assert len(modified) == 1
        assert not any(
            pe.path == "incoming/doc.pdf" for pe in record.source_paths
        )
        assert any(
            pe.path == "incoming/doc.pdf" for pe in record.missing_source_paths
        )

    def test_removal_from_current_paths(self):
        """Removal from current_paths → moved to missing_current_paths."""
        record = _make_record(
            current_paths=[PathEntry("sorted/work/doc.pdf", _ts())],
        )
        change = _make_change(EventType.REMOVAL, "sorted/work/doc.pdf")

        modified, new = preprocess([change], [record], _noop_sidecar)

        assert len(modified) == 1
        assert not any(
            pe.path == "sorted/work/doc.pdf" for pe in record.current_paths
        )
        assert any(
            pe.path == "sorted/work/doc.pdf"
            for pe in record.missing_current_paths
        )


# ---------------------------------------------------------------------------
# Preprocessing: location-aware priority
# ---------------------------------------------------------------------------

class TestPreprocessLocationAware:
    def test_archive_both_match_uses_source_hash(self):
        """Archive file matches both hash and source_hash → only source_hash used."""
        record = _make_record(
            source_hash="both_hash",
            hash="both_hash",
        )
        change = _make_change(
            EventType.ADDITION, "archive/doc.pdf",
            hash="both_hash", size=500,
        )

        modified, new = preprocess([change], [record], _noop_sidecar)

        assert len(modified) == 1
        assert any(pe.path == "archive/doc.pdf" for pe in record.source_paths)
        assert not any(
            pe.path == "archive/doc.pdf" for pe in record.current_paths
        )

    def test_sorted_both_match_prefers_hash(self):
        """Sorted file matches both hash and source_hash → hash preferred."""
        record = _make_record(
            source_hash="both_hash",
            hash="both_hash",
        )
        change = _make_change(
            EventType.ADDITION, "sorted/work/doc.pdf",
            hash="both_hash", size=500,
        )

        modified, new = preprocess([change], [record], _noop_sidecar)

        assert len(modified) == 1
        assert any(
            pe.path == "sorted/work/doc.pdf" for pe in record.current_paths
        )
        assert not any(
            pe.path == "sorted/work/doc.pdf" for pe in record.source_paths
        )


# ---------------------------------------------------------------------------
# Preprocessing: idempotency (restart safety)
# ---------------------------------------------------------------------------

class TestPreprocessIdempotency:
    def test_duplicate_source_path_not_appended(self):
        """ADDITION for a path already in source_paths should not add a duplicate."""
        record = _make_record(
            source_hash="abc123",
            state=State.IS_COMPLETE,
            source_paths=[PathEntry("archive/doc.pdf", _ts())],
        )
        change = _make_change(
            EventType.ADDITION, "archive/doc.pdf",
            hash="abc123", size=100,
        )

        modified, new = preprocess([change], [record], _noop_sidecar)

        assert len(modified) == 0, "Record should not be marked modified"
        assert len(record.source_paths) == 1, "No duplicate path should be added"

    def test_duplicate_current_path_not_appended(self):
        """ADDITION for a path already in current_paths should not add a duplicate."""
        record = _make_record(
            hash="def456",
            state=State.IS_COMPLETE,
            current_paths=[PathEntry("sorted/work/doc.pdf", _ts())],
        )
        change = _make_change(
            EventType.ADDITION, "sorted/work/doc.pdf",
            hash="def456", size=100,
        )

        modified, new = preprocess([change], [record], _noop_sidecar)

        assert len(modified) == 0, "Record should not be marked modified"
        assert len(record.current_paths) == 1, "No duplicate path should be added"

    def test_new_path_still_appended(self):
        """ADDITION for a genuinely new path should still be added."""
        record = _make_record(
            source_hash="abc123",
            state=State.IS_COMPLETE,
            source_paths=[PathEntry("archive/old.pdf", _ts())],
        )
        change = _make_change(
            EventType.ADDITION, "incoming/new-copy.pdf",
            hash="abc123", size=100,
        )

        modified, new = preprocess([change], [record], _noop_sidecar)

        assert len(modified) == 1
        assert any(pe.path == "incoming/new-copy.pdf" for pe in record.source_paths)


# ---------------------------------------------------------------------------
# Reconciliation: source_paths section
# ---------------------------------------------------------------------------

class TestReconcileSourcePaths:
    def test_source_in_trash(self):
        """Source in trash → needs_deletion."""
        record = _make_record(
            source_paths=[PathEntry("trash/doc.pdf", _ts())],
        )

        result = reconcile(record)

        assert result is record
        assert result.state == State.NEEDS_DELETION

    def test_is_new_sets_processing(self):
        """IS_NEW → output_filename, needs_processing, source_reference, archive."""
        record = _make_record(
            state=State.IS_NEW,
            source_paths=[PathEntry("incoming/invoice.pdf", _ts())],
        )

        result = reconcile(record)

        assert result is record
        assert result.state == State.NEEDS_PROCESSING
        assert result.output_filename is not None
        assert result.source_reference == "incoming/invoice.pdf"
        archive_entries = [
            pe for pe in result.source_paths
            if pe.path.startswith("archive/")
        ]
        assert len(archive_entries) == 1
        assert archive_entries[0].path == "archive/invoice.pdf"

    def test_source_not_archive_not_lost(self):
        """Source not in archive → source_reference, archive, duplicates."""
        record = _make_record(
            state=State.NEEDS_PROCESSING,
            source_paths=[
                PathEntry("incoming/doc.pdf", _ts(0)),
                PathEntry("incoming/copy.pdf", _ts(1)),  # most recent
            ],
        )

        result = reconcile(record)

        assert result.source_reference == "incoming/copy.pdf"
        assert "incoming/doc.pdf" in result.duplicate_sources
        archive_entries = [
            pe for pe in result.source_paths
            if pe.path.startswith("archive/")
        ]
        assert len(archive_entries) >= 1

    def test_is_complete_new_source_is_duplicate(self):
        """IS_COMPLETE + new source in incoming/ → duplicate, no reprocessing."""
        record = _make_record(
            state=State.IS_COMPLETE,
            source_paths=[
                PathEntry("archive/doc.pdf", _ts(0)),
                PathEntry("incoming/doc.pdf", _ts(1)),
            ],
            current_paths=[PathEntry("sorted/work/doc.pdf", _ts(0))],
            context="work",
            assigned_filename="doc.pdf",
            hash="abc",
        )

        result = reconcile(record)

        assert result.state == State.IS_COMPLETE
        assert "incoming/doc.pdf" in result.duplicate_sources
        # Archive source preserved, incoming removed from source_paths
        assert any(pe.path == "archive/doc.pdf" for pe in result.source_paths)
        assert not any(pe.path.startswith("incoming/") for pe in result.source_paths)
        # Current paths untouched — no reprocessing
        assert any(pe.path == "sorted/work/doc.pdf" for pe in result.current_paths)
        assert result.output_filename is None

    def test_is_complete_new_source_in_sorted_is_duplicate(self):
        """IS_COMPLETE + new source in sorted/ → duplicate, no reprocessing."""
        record = _make_record(
            state=State.IS_COMPLETE,
            source_paths=[
                PathEntry("archive/doc.pdf", _ts(0)),
                PathEntry("sorted/work/doc.pdf", _ts(1)),
            ],
            current_paths=[PathEntry("processed/doc.pdf", _ts(0))],
            context="work",
            assigned_filename="doc.pdf",
            hash="abc",
        )

        result = reconcile(record)

        assert result.state == State.IS_COMPLETE
        assert "sorted/work/doc.pdf" in result.duplicate_sources
        assert not any(
            Record._decompose_path(pe.path)[0] == "sorted"
            for pe in result.source_paths
        )

    def test_is_complete_source_in_archive_no_duplicate(self):
        """IS_COMPLETE + source already in archive → no duplicate, no change."""
        record = _make_record(
            state=State.IS_COMPLETE,
            source_paths=[PathEntry("archive/doc.pdf", _ts())],
            current_paths=[PathEntry("sorted/work/doc.pdf", _ts())],
            context="work",
            assigned_filename="doc.pdf",
            hash="abc",
        )

        result = reconcile(record)

        assert result.state == State.IS_COMPLETE
        assert result.duplicate_sources == []

    def test_is_missing_recovers_with_new_source(self):
        """IS_MISSING + new source in incoming/ → reprocess (recovery)."""
        record = _make_record(
            state=State.IS_MISSING,
            source_paths=[
                PathEntry("missing/doc.pdf", _ts(0)),
                PathEntry("incoming/doc.pdf", _ts(1)),
            ],
            missing_current_paths=[PathEntry("sorted/work/doc.pdf", _ts())],
            context="work",
            assigned_filename="doc.pdf",
            hash="abc",
        )

        result = reconcile(record)

        assert result.state == State.NEEDS_PROCESSING
        assert result.output_filename is not None

    def test_is_missing_sets_source_reference(self):
        """IS_COMPLETE with missing_current_paths and source in archive → IS_MISSING, source_reference set."""
        record = _make_record(
            state=State.IS_COMPLETE,
            source_paths=[PathEntry("archive/doc.pdf", _ts())],
            missing_current_paths=[PathEntry("sorted/work/doc.pdf", _ts())],
            context="work",
            assigned_filename="doc.pdf",
            hash="abc",
        )

        result = reconcile(record)

        assert result.state == State.IS_MISSING
        assert result.source_reference == "archive/doc.pdf"

    def test_is_missing_no_source_reference_without_archive(self):
        """Source in missing/ already → no source_reference set (don't re-move)."""
        record = _make_record(
            state=State.IS_COMPLETE,
            source_paths=[PathEntry("missing/doc.pdf", _ts())],
            missing_current_paths=[PathEntry("sorted/work/doc.pdf", _ts())],
            context="work",
            assigned_filename="doc.pdf",
            hash="abc",
        )

        result = reconcile(record)

        assert result.state == State.IS_MISSING
        assert result.source_reference is None


# ---------------------------------------------------------------------------
# Reconciliation: has_error section
# ---------------------------------------------------------------------------

class TestReconcileHasError:
    def test_error_source_in_error_no_current_deletes(self):
        """HAS_ERROR + source in error + no current → returns None."""
        record = _make_record(
            state=State.HAS_ERROR,
            source_paths=[PathEntry("error/doc.pdf", _ts())],
            current_paths=[],
        )

        result = reconcile(record)

        assert result is None

    def test_error_source_in_archive(self):
        """HAS_ERROR + source in archive → source_reference, duplicates cleared."""
        record = _make_record(
            state=State.HAS_ERROR,
            source_paths=[PathEntry("archive/doc.pdf", _ts())],
            duplicate_sources=["incoming/extra.pdf"],
        )

        result = reconcile(record)

        assert result is not None
        assert result.source_reference == "archive/doc.pdf"
        assert result.duplicate_sources == []

    def test_error_with_current_paths(self):
        """HAS_ERROR + current_paths → moved to deleted_paths."""
        record = _make_record(
            state=State.HAS_ERROR,
            source_paths=[PathEntry("error/doc.pdf", _ts())],
            current_paths=[PathEntry(".output/uuid", _ts())],
        )

        result = reconcile(record)

        assert result is not None
        assert result.current_paths == []
        assert ".output/uuid" in result.deleted_paths


# ---------------------------------------------------------------------------
# Reconciliation: current_paths section
# ---------------------------------------------------------------------------

class TestReconcileCurrentPaths:
    def test_current_in_trash_needs_deletion(self):
        """Processed file in trash/ (matched via hash) → needs_deletion."""
        record = _make_record(
            state=State.IS_COMPLETE,
            source_paths=[PathEntry("archive/doc.pdf", _ts())],
            current_paths=[PathEntry("trash/doc.pdf", _ts(1))],
            hash="some_hash",
            context="work",
            assigned_filename="doc.pdf",
        )

        result = reconcile(record)

        assert result is record
        assert result.state == State.NEEDS_DELETION

    def test_is_new_needs_processing(self):
        """IS_NEW in current section → needs_processing, output_filename set."""
        # No source_paths → source section skipped → reach current section
        record = _make_record(state=State.IS_NEW)

        result = reconcile(record)

        assert result.state == State.NEEDS_PROCESSING
        assert result.output_filename is not None

    def test_is_deleted_removed(self):
        """IS_DELETED → record deleted from DB (returns None).

        After needs_deletion → is_deleted, the next cycle detects removals
        (files moved to void/) which populate missing_*_paths. Without this
        guard, the record would incorrectly transition to is_missing.
        """
        record = _make_record(
            state=State.IS_DELETED,
            missing_current_paths=[PathEntry("sorted/work/doc.pdf", _ts())],
            missing_source_paths=[PathEntry("archive/doc.pdf", _ts())],
        )

        result = reconcile(record)

        assert result is None

    def test_needs_processing_no_current(self):
        """NEEDS_PROCESSING, no current → unchanged."""
        record = _make_record(
            state=State.NEEDS_PROCESSING,
            output_filename="some-uuid",
        )

        result = reconcile(record)

        assert result.state == State.NEEDS_PROCESSING

    def test_no_current_missing_current_is_missing(self):
        """No current + missing_current → IS_MISSING."""
        record = _make_record(
            state=State.IS_COMPLETE,
            missing_current_paths=[PathEntry("sorted/work/doc.pdf", _ts())],
        )

        result = reconcile(record)

        assert result.state == State.IS_MISSING

    def test_is_missing_current_reappeared(self):
        """IS_MISSING + current reappeared → IS_COMPLETE."""
        record = _make_record(
            state=State.IS_MISSING,
            current_paths=[PathEntry("sorted/work/doc.pdf", _ts())],
            context="work",
            assigned_filename="doc.pdf",
        )

        result = reconcile(record)

        assert result.state == State.IS_COMPLETE

    def test_is_missing_still_empty(self):
        """IS_MISSING + still empty → unchanged."""
        record = _make_record(
            state=State.IS_MISSING,
            missing_current_paths=[PathEntry("sorted/work/doc.pdf", _ts())],
        )

        result = reconcile(record)

        assert result.state == State.IS_MISSING

    def test_invalid_location_deleted(self):
        """Invalid location → moved to deleted_paths."""
        record = _make_record(
            state=State.IS_COMPLETE,
            current_paths=[PathEntry("incoming/doc.pdf", _ts())],
        )

        result = reconcile(record)

        assert result.current_paths == []
        assert "incoming/doc.pdf" in result.deleted_paths

    def test_multiple_keep_most_recent(self):
        """Multiple current_paths → keep most recent, others to deleted."""
        record = _make_record(
            state=State.IS_COMPLETE,
            current_paths=[
                PathEntry("sorted/work/old.pdf", _ts(0)),
                PathEntry("sorted/work/new.pdf", _ts(2)),
                PathEntry("sorted/work/mid.pdf", _ts(1)),
            ],
            context="work",
            assigned_filename="new.pdf",
        )

        result = reconcile(record)

        assert len(result.current_paths) == 1
        assert result.current_paths[0].path == "sorted/work/new.pdf"
        assert "sorted/work/old.pdf" in result.deleted_paths
        assert "sorted/work/mid.pdf" in result.deleted_paths

    def test_single_output_target_processed(self):
        """Single in .output from incoming → target_path=processed/."""
        record = _make_record(
            state=State.NEEDS_PROCESSING,
            source_paths=[PathEntry("archive/test.pdf", _ts())],
            current_paths=[PathEntry(".output/uuid-out", _ts())],
            context="work",
            assigned_filename="2025-01-Invoice.pdf",
        )

        result = reconcile(record)

        assert result.target_path == "processed/2025-01-Invoice.pdf"
        assert result.current_reference == ".output/uuid-out"

    def test_single_output_from_sorted_target_sorted(self):
        """Single in .output from sorted/ → target_path=sorted/."""
        record = _make_record(
            state=State.NEEDS_PROCESSING,
            source_paths=[PathEntry("archive/test.pdf", _ts())],
            missing_source_paths=[PathEntry("sorted/work/test.pdf", _ts(-1))],
            current_paths=[PathEntry(".output/uuid-out", _ts())],
            context="work",
            assigned_filename="2025-01-Invoice.pdf",
        )

        result = reconcile(record)

        assert result.target_path == "sorted/work/2025-01-Invoice.pdf"
        assert result.current_reference == ".output/uuid-out"

    def test_single_processed_is_complete(self):
        """Single in processed → IS_COMPLETE."""
        record = _make_record(
            state=State.NEEDS_PROCESSING,
            current_paths=[PathEntry("processed/doc.pdf", _ts())],
        )

        result = reconcile(record)

        assert result.state == State.IS_COMPLETE

    def test_single_reviewed_target_sorted(self):
        """Single in reviewed → target_path to sorted, current_reference set."""
        record = _make_record(
            state=State.IS_COMPLETE,
            current_paths=[PathEntry("reviewed/doc.pdf", _ts())],
            context="work",
            assigned_filename="2025-01-Doc.pdf",
        )

        result = reconcile(record)

        assert result.target_path == "sorted/work/2025-01-Doc.pdf"
        assert result.current_reference == "reviewed/doc.pdf"

    def test_single_sorted_matches_complete(self):
        """Single in sorted, path matches → IS_COMPLETE."""
        record = _make_record(
            state=State.NEEDS_PROCESSING,
            current_paths=[
                PathEntry("sorted/work/2025-01-Invoice.pdf", _ts()),
            ],
            context="work",
            assigned_filename="2025-01-Invoice.pdf",
        )

        result = reconcile(record)

        assert result.state == State.IS_COMPLETE

    def test_single_sorted_doesnt_match(self):
        """Single in sorted, path doesn't match → adopts user's filename."""
        record = _make_record(
            state=State.IS_COMPLETE,
            current_paths=[PathEntry("sorted/work/wrong_name.pdf", _ts())],
            context="work",
            assigned_filename="2025-01-Invoice.pdf",
        )

        result = reconcile(record)

        assert result.assigned_filename == "wrong_name.pdf"
        assert result.state == State.IS_COMPLETE
        assert result.target_path is None

    def test_single_sorted_context_changed_missing_fields(self):
        """Single in sorted, context changed → missing fields added."""
        record = _make_record(
            state=State.IS_COMPLETE,
            current_paths=[PathEntry("sorted/work/doc.pdf", _ts())],
            context="work",
            metadata={"context": "work", "date": "2025-01-01"},
            assigned_filename="doc.pdf",
        )
        cfn = {"work": ["context", "date", "category"]}

        result = reconcile(record, context_field_names=cfn)

        assert "category" in result.metadata
        assert result.metadata["category"] is None
        assert result.metadata["context"] == "work"
        assert result.metadata["date"] == "2025-01-01"


# ---------------------------------------------------------------------------
# compute_target_path
# ---------------------------------------------------------------------------

class TestComputeTargetPath:
    def test_with_context_and_filename(self):
        record = _make_record(
            context="work", assigned_filename="2025-01-Invoice.pdf",
        )
        assert compute_target_path(record) == "sorted/work/2025-01-Invoice.pdf"

    def test_no_context_returns_none(self):
        record = _make_record(context=None, assigned_filename="doc.pdf")
        assert compute_target_path(record) is None

    def test_no_filename_returns_none(self):
        record = _make_record(context="work", assigned_filename=None)
        assert compute_target_path(record) is None

    def test_with_context_folders(self):
        """context_folders builds subdirectory hierarchy from metadata."""
        record = _make_record(
            context="arbeit",
            assigned_filename="arbeit-rechnung-2025.pdf",
            metadata={"context": "arbeit", "sender": "Schulze GmbH", "type": "Rechnung"},
        )
        folders = {"arbeit": ["context", "sender"]}
        assert compute_target_path(record, folders) == (
            "sorted/arbeit/Schulze GmbH/arbeit-rechnung-2025.pdf"
        )

    def test_with_context_folders_missing_field(self):
        """Falls back to context-only path if folder field is missing from metadata."""
        record = _make_record(
            context="arbeit",
            assigned_filename="arbeit-rechnung-2025.pdf",
            metadata={"context": "arbeit"},
        )
        folders = {"arbeit": ["context", "sender"]}
        # "sender" is missing, so only "context" folder part is used
        assert compute_target_path(record, folders) == (
            "sorted/arbeit/arbeit-rechnung-2025.pdf"
        )


# ---------------------------------------------------------------------------
# _is_collision_variant
# ---------------------------------------------------------------------------

class TestIsCollisionVariant:
    def test_exact_match(self):
        assert _is_collision_variant(
            "sorted/work/invoice.pdf",
            "sorted/work/invoice.pdf",
        )

    def test_collision_suffix_matches(self):
        assert _is_collision_variant(
            "sorted/work/invoice_e3ca2c9b.pdf",
            "sorted/work/invoice.pdf",
        )

    def test_different_suffix_still_matches(self):
        assert _is_collision_variant(
            "sorted/work/invoice_abcd1234.pdf",
            "sorted/work/invoice.pdf",
        )

    def test_different_filename_no_match(self):
        assert not _is_collision_variant(
            "sorted/work/receipt_e3ca2c9b.pdf",
            "sorted/work/invoice.pdf",
        )

    def test_different_directory_no_match(self):
        assert not _is_collision_variant(
            "sorted/personal/invoice_e3ca2c9b.pdf",
            "sorted/work/invoice.pdf",
        )

    def test_non_hex_suffix_no_match(self):
        """Only 8 hex chars count as collision suffix."""
        assert not _is_collision_variant(
            "sorted/work/invoice_zzzzzzzz.pdf",
            "sorted/work/invoice.pdf",
        )

    def test_wrong_length_suffix_no_match(self):
        """Suffix must be exactly 8 hex chars."""
        assert not _is_collision_variant(
            "sorted/work/invoice_abcd12.pdf",
            "sorted/work/invoice.pdf",
        )

    def test_deep_path_with_collision(self):
        assert _is_collision_variant(
            "sorted/belege/nain_trading/79901/belege-2025-naturescene_231x173-972_e3ca2c9b.pdf",
            "sorted/belege/nain_trading/79901/belege-2025-naturescene_231x173-972.pdf",
        )

    def test_txt_extension(self):
        assert _is_collision_variant(
            "sorted/work/transcript_aabbccdd.txt",
            "sorted/work/transcript.txt",
        )


# ---------------------------------------------------------------------------
# Reconcile: sorted collision variants
# ---------------------------------------------------------------------------

class TestReconcileSortedCollisionVariant:
    def test_collision_variant_is_complete(self):
        """File in sorted/ with collision suffix → IS_COMPLETE, no target_path."""
        record = _make_record(
            state=State.NEEDS_PROCESSING,
            current_paths=[
                PathEntry("sorted/work/2025-01-Invoice_e3ca2c9b.pdf", _ts()),
            ],
            context="work",
            assigned_filename="2025-01-Invoice.pdf",
        )

        result = reconcile(record)

        assert result.state == State.IS_COMPLETE
        assert result.target_path is None
        assert result.current_reference is None

    def test_collision_variant_with_context_folders(self):
        """Collision variant with deep folder path → IS_COMPLETE."""
        record = _make_record(
            state=State.NEEDS_PROCESSING,
            current_paths=[
                PathEntry("sorted/arbeit/Schulze GmbH/rechnung_abcd1234.pdf", _ts()),
            ],
            context="arbeit",
            assigned_filename="rechnung.pdf",
            metadata={"context": "arbeit", "sender": "Schulze GmbH"},
        )
        folders = {"arbeit": ["context", "sender"]}

        result = reconcile(record, context_folders=folders)

        assert result.state == State.IS_COMPLETE
        assert result.target_path is None

    def test_user_rename_adopts_new_filename(self):
        """File in sorted/ with different name → adopts user's name, no move."""
        record = _make_record(
            state=State.IS_COMPLETE,
            current_paths=[PathEntry("sorted/work/my-custom-name.pdf", _ts())],
            context="work",
            assigned_filename="2025-01-Invoice.pdf",
        )

        result = reconcile(record)

        assert result.assigned_filename == "my-custom-name.pdf"
        assert result.state == State.IS_COMPLETE
        assert result.target_path is None

    def test_user_rename_context_change(self):
        """File moved to different context dir → adopts context + keeps filename."""
        record = _make_record(
            state=State.IS_COMPLETE,
            current_paths=[PathEntry("sorted/personal/my-custom-name.pdf", _ts())],
            context="work",
            assigned_filename="my-custom-name.pdf",
        )

        result = reconcile(record)

        assert result.context == "personal"
        assert result.assigned_filename == "my-custom-name.pdf"
        assert result.state == State.IS_COMPLETE
        assert result.target_path is None

    def test_user_rename_and_context_change(self):
        """File renamed AND moved to different context → adopts both changes."""
        record = _make_record(
            state=State.IS_COMPLETE,
            current_paths=[PathEntry("sorted/personal/renamed.pdf", _ts())],
            context="work",
            assigned_filename="2025-01-Invoice.pdf",
        )

        result = reconcile(record)

        assert result.assigned_filename == "renamed.pdf"
        assert result.context == "personal"
        assert result.state == State.IS_COMPLETE
        assert result.target_path is None

    def test_exact_match_still_works(self):
        """Exact path match (no collision suffix) → IS_COMPLETE as before."""
        record = _make_record(
            state=State.NEEDS_PROCESSING,
            current_paths=[
                PathEntry("sorted/work/2025-01-Invoice.pdf", _ts()),
            ],
            context="work",
            assigned_filename="2025-01-Invoice.pdf",
        )

        result = reconcile(record)

        assert result.state == State.IS_COMPLETE
        assert result.target_path is None


# ---------------------------------------------------------------------------
# Preprocessing: .output duplicate hash detection
# ---------------------------------------------------------------------------

class TestPreprocessOutputDuplicateHash:
    def test_output_hash_matches_other_source_hash(self):
        """Output hash matches another record's source_hash → duplicate."""
        r1 = _make_record(source_hash="XHASH", state=State.IS_COMPLETE)
        r2 = _make_record(
            source_hash="other",
            output_filename="uuid-r2",
            state=State.NEEDS_PROCESSING,
            source_paths=[PathEntry("archive/r2.pdf", _ts())],
        )
        change = _make_change(
            EventType.ADDITION, ".output/uuid-r2",
            hash="XHASH", size=1024,
        )

        modified, new = preprocess([change], [r1, r2], lambda p: {
            "context": "work", "metadata": {}, "assigned_filename": "out.pdf",
        })

        assert len(modified) == 1
        assert modified[0] is r2
        assert r2.state == State.HAS_ERROR
        assert r2.output_filename is None
        assert ".output/uuid-r2" in r2.deleted_paths
        assert "archive/r2.pdf" in r2.duplicate_sources
        assert r2.source_paths == []
        assert r2.current_paths == []
        # r1 unchanged
        assert r1.state == State.IS_COMPLETE

    def test_output_hash_matches_other_hash(self):
        """Output hash matches another record's hash → duplicate."""
        r1 = _make_record(source_hash="src1", hash="XHASH", state=State.IS_COMPLETE)
        r2 = _make_record(
            source_hash="src2",
            output_filename="uuid-r2",
            state=State.NEEDS_PROCESSING,
            source_paths=[PathEntry("archive/r2.pdf", _ts())],
        )
        change = _make_change(
            EventType.ADDITION, ".output/uuid-r2",
            hash="XHASH", size=1024,
        )

        modified, new = preprocess([change], [r1, r2], lambda p: {
            "context": "work", "metadata": {}, "assigned_filename": "out.pdf",
        })

        assert r2.state == State.HAS_ERROR
        assert ".output/uuid-r2" in r2.deleted_paths
        assert "archive/r2.pdf" in r2.duplicate_sources

    def test_output_hash_matches_own_source_hash_only(self):
        """Output hash matches own source_hash (no-op processing) → normal ingestion."""
        r = _make_record(
            source_hash="XHASH",
            output_filename="uuid-r",
            state=State.NEEDS_PROCESSING,
        )
        change = _make_change(
            EventType.ADDITION, ".output/uuid-r",
            hash="XHASH", size=1024,
        )

        modified, new = preprocess([change], [r], lambda p: {
            "context": "work", "metadata": {"date": "2025"}, "assigned_filename": "out.pdf",
        })

        assert r.hash == "XHASH"
        assert r.context == "work"
        assert len(r.current_paths) == 1
        assert r.deleted_paths == []

    def test_output_hash_no_match(self):
        """No record has the output hash → normal ingestion."""
        r = _make_record(
            source_hash="src",
            output_filename="uuid-r",
            state=State.NEEDS_PROCESSING,
        )
        change = _make_change(
            EventType.ADDITION, ".output/uuid-r",
            hash="NEWHASH", size=1024,
        )

        modified, new = preprocess([change], [r], lambda p: {
            "context": "work", "metadata": {}, "assigned_filename": "out.pdf",
        })

        assert r.hash == "NEWHASH"
        assert r.context == "work"
        assert len(r.current_paths) == 1
        assert r.deleted_paths == []

    def test_output_duplicate_keeps_non_archive_sources(self):
        """Duplicate detected → only archive paths go to duplicate_sources."""
        r1 = _make_record(source_hash="XHASH", state=State.IS_COMPLETE)
        r2 = _make_record(
            source_hash="src2",
            output_filename="uuid-r2",
            state=State.NEEDS_PROCESSING,
            source_paths=[
                PathEntry("archive/r2.pdf", _ts()),
                PathEntry("missing/r2.pdf", _ts(1)),
            ],
        )
        change = _make_change(
            EventType.ADDITION, ".output/uuid-r2",
            hash="XHASH", size=1024,
        )

        modified, new = preprocess([change], [r1, r2], lambda p: {
            "context": "work", "metadata": {}, "assigned_filename": "out.pdf",
        })

        assert r2.state == State.HAS_ERROR
        assert "archive/r2.pdf" in r2.duplicate_sources
        # missing/r2.pdf stays in source_paths
        assert len(r2.source_paths) == 1
        assert r2.source_paths[0].path == "missing/r2.pdf"

    def test_two_identical_outputs_same_cycle(self):
        """Two different sources produce identical output in the same cycle.

        The first output is ingested normally; the second is detected as
        duplicate because the first record's hash was set in-place during
        the same preprocess() call.
        """
        r1 = _make_record(
            source_hash="src1",
            output_filename="uuid-r1",
            state=State.NEEDS_PROCESSING,
            source_paths=[PathEntry("archive/r1.pdf", _ts())],
        )
        r2 = _make_record(
            source_hash="src2",
            output_filename="uuid-r2",
            state=State.NEEDS_PROCESSING,
            source_paths=[PathEntry("archive/r2.pdf", _ts())],
        )
        # Both outputs have the same hash
        c1 = _make_change(
            EventType.ADDITION, ".output/uuid-r1",
            hash="IDENTICAL", size=1024,
        )
        c2 = _make_change(
            EventType.ADDITION, ".output/uuid-r2",
            hash="IDENTICAL", size=1024,
        )

        modified, new = preprocess([c1, c2], [r1, r2], lambda p: {
            "context": "work", "metadata": {}, "assigned_filename": "out.pdf",
        })

        # r1: ingested normally (first in the batch)
        assert r1.hash == "IDENTICAL"
        assert r1.context == "work"
        assert len(r1.current_paths) == 1
        assert r1.deleted_paths == []

        # r2: detected as duplicate (r1.hash was already set)
        assert r2.state == State.HAS_ERROR
        assert r2.output_filename is None
        assert ".output/uuid-r2" in r2.deleted_paths
        assert "archive/r2.pdf" in r2.duplicate_sources
        assert r2.source_paths == []


# ---------------------------------------------------------------------------
# Preprocessing: reset/ detection
# ---------------------------------------------------------------------------

class TestPreprocessIncomingMatchesHash:
    def test_incoming_matches_record_hash(self):
        """File in incoming/ whose hash matches record.hash → added to current_paths."""
        record = _make_record(
            source_hash="SOURCE",
            hash="PROCESSED",
            state=State.IS_COMPLETE,
            current_paths=[PathEntry("sorted/work/doc.pdf", _ts())],
        )
        change = _make_change(
            EventType.ADDITION, "incoming/copy.pdf",
            hash="PROCESSED", size=500,
        )

        modified, new = preprocess([change], [record], _noop_sidecar)

        assert len(modified) == 1
        assert modified[0] is record
        assert any(pe.path == "incoming/copy.pdf" for pe in record.current_paths)
        assert len(new) == 0

    def test_incoming_matches_source_hash_preferred(self):
        """source_hash match takes priority over hash match for incoming."""
        record = _make_record(
            source_hash="HASH1",
            hash="HASH1",  # same as source_hash (txt file scenario)
            state=State.IS_COMPLETE,
        )
        change = _make_change(
            EventType.ADDITION, "incoming/doc.txt",
            hash="HASH1", size=500,
        )

        modified, new = preprocess([change], [record], _noop_sidecar)

        assert len(modified) == 1
        # Matched by source_hash (first in list) → source_paths
        assert any(pe.path == "incoming/doc.txt" for pe in record.source_paths)

    def test_incoming_hash_match_cleaned_by_reconcile(self):
        """incoming/ path matched by hash → reconcile cleans it as invalid location."""
        record = _make_record(
            source_hash="SOURCE",
            hash="PROCESSED",
            state=State.IS_COMPLETE,
            current_paths=[
                PathEntry("sorted/work/doc.pdf", _ts(0)),
                PathEntry("incoming/copy.pdf", _ts(1)),
            ],
            context="work",
            assigned_filename="doc.pdf",
        )

        result = reconcile(record)

        # incoming/ is not a valid current location → moved to deleted_paths
        assert "incoming/copy.pdf" in result.deleted_paths
        assert not any(pe.path == "incoming/copy.pdf" for pe in result.current_paths)
        assert result.state == State.IS_COMPLETE


class TestPreprocessReset:
    def test_reset_matches_hash(self):
        """Addition in reset/, matches hash → added to current_paths."""
        record = _make_record(hash="reset_hash", state=State.IS_COMPLETE)
        change = _make_change(
            EventType.ADDITION, "reset/doc.pdf",
            hash="reset_hash", size=700,
        )

        modified, new = preprocess([change], [record], _noop_sidecar)

        assert len(modified) == 1
        assert any(pe.path == "reset/doc.pdf" for pe in record.current_paths)

    def test_reset_unknown_not_created(self):
        """Addition in reset/, unknown hash → not created (no new records)."""
        change = _make_change(
            EventType.ADDITION, "reset/stray.pdf",
            hash="unknown_hash", size=500,
        )

        modified, new = preprocess([change], [], _noop_sidecar)

        assert modified == []
        assert new == []

    def test_reset_does_not_match_source_hash(self):
        """reset/ matches by hash, not source_hash."""
        record = _make_record(source_hash="src_hash", hash=None)
        change = _make_change(
            EventType.ADDITION, "reset/doc.pdf",
            hash="src_hash", size=500,
        )

        modified, new = preprocess([change], [record], _noop_sidecar)

        # source_hash match is not configured for reset/
        assert modified == []


# ---------------------------------------------------------------------------
# Reconcile: reset/ → sorted/
# ---------------------------------------------------------------------------

class TestReconcileReset:
    def test_reset_moves_to_sorted(self):
        """File in reset/ → recompute filename, route to sorted/."""
        record = _make_record(
            state=State.IS_COMPLETE,
            current_paths=[PathEntry("reset/doc.pdf", _ts())],
            context="work",
            metadata={"context": "work", "date": "2025-01-15", "type": "Invoice"},
            assigned_filename="old-name.pdf",
            original_filename="original.pdf",
        )

        def recompute(r):
            return "invoice-2025-01-15.pdf"

        result = reconcile(record, recompute_filename=recompute)

        assert result.assigned_filename == "invoice-2025-01-15.pdf"
        assert result.target_path == "sorted/work/invoice-2025-01-15.pdf"
        assert result.current_reference == "reset/doc.pdf"
        assert result.state == State.IS_COMPLETE

    def test_reset_uses_context_folders(self):
        """reset/ respects context_folders for subdirectory hierarchy."""
        record = _make_record(
            state=State.IS_COMPLETE,
            current_paths=[PathEntry("reset/doc.pdf", _ts())],
            context="work",
            metadata={"context": "work", "date": "2025", "sender": "Acme"},
            assigned_filename="old.pdf",
        )

        def recompute(r):
            return "invoice-2025.pdf"

        result = reconcile(
            record,
            context_folders={"work": ["context", "sender"]},
            recompute_filename=recompute,
        )

        assert result.assigned_filename == "invoice-2025.pdf"
        assert result.target_path == "sorted/work/Acme/invoice-2025.pdf"

    def test_reset_without_recompute_callback(self):
        """reset/ without recompute_filename → keeps existing assigned_filename."""
        record = _make_record(
            state=State.IS_COMPLETE,
            current_paths=[PathEntry("reset/doc.pdf", _ts())],
            context="work",
            assigned_filename="existing-name.pdf",
        )

        result = reconcile(record)

        assert result.assigned_filename == "existing-name.pdf"
        assert result.target_path == "sorted/work/existing-name.pdf"
        assert result.current_reference == "reset/doc.pdf"

    def test_reset_recompute_returns_none(self):
        """If recompute returns None, assigned_filename is unchanged."""
        record = _make_record(
            state=State.IS_COMPLETE,
            current_paths=[PathEntry("reset/doc.pdf", _ts())],
            context="work",
            assigned_filename="keep-this.pdf",
        )

        result = reconcile(record, recompute_filename=lambda r: None)

        assert result.assigned_filename == "keep-this.pdf"
        assert result.target_path == "sorted/work/keep-this.pdf"

    def test_reset_no_context(self):
        """reset/ with no context → no target_path (can't determine sorted/ location)."""
        record = _make_record(
            state=State.IS_COMPLETE,
            current_paths=[PathEntry("reset/doc.pdf", _ts())],
            context=None,
            assigned_filename="doc.pdf",
        )

        result = reconcile(record, recompute_filename=lambda r: "new.pdf")

        assert result.assigned_filename == "new.pdf"
        assert result.target_path is None
        assert result.state == State.IS_COMPLETE

    def test_reset_no_assigned_filename(self):
        """reset/ with no assigned_filename and recompute fails → no target."""
        record = _make_record(
            state=State.IS_COMPLETE,
            current_paths=[PathEntry("reset/doc.pdf", _ts())],
            context="work",
            assigned_filename=None,
        )

        result = reconcile(record, recompute_filename=lambda r: None)

        assert result.target_path is None

    def test_reset_deduplicates_current_paths(self):
        """If file is in both reset/ and processed/, dedup keeps most recent."""
        record = _make_record(
            state=State.IS_COMPLETE,
            current_paths=[
                PathEntry("processed/doc.pdf", _ts(0)),
                PathEntry("reset/doc.pdf", _ts(1)),
            ],
            context="work",
            assigned_filename="doc.pdf",
        )

        result = reconcile(record)

        # Dedup keeps most recent (reset/), deletes older (processed/)
        assert len(result.current_paths) == 1
        assert result.current_paths[0].path == "reset/doc.pdf"
        assert "processed/doc.pdf" in result.deleted_paths

    def test_reset_not_cleaned_as_invalid_location(self):
        """reset/ is in VALID_CURRENT_LOCATIONS, not deleted as invalid."""
        record = _make_record(
            state=State.IS_COMPLETE,
            current_paths=[PathEntry("reset/doc.pdf", _ts())],
            context="work",
            assigned_filename="doc.pdf",
        )

        result = reconcile(record)

        # Should NOT be in deleted_paths
        assert "reset/doc.pdf" not in result.deleted_paths
        assert result.current_reference == "reset/doc.pdf"
