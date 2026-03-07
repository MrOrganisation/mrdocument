"""Tests for step4.py — FilesystemReconciler moves based on temp fields."""

from datetime import datetime, timezone
from pathlib import Path

import pytest

from models import State, PathEntry, Record
from step4 import FilesystemReconciler, _move_file


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts():
    return datetime(2025, 1, 1, tzinfo=timezone.utc)


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _make_record(**kwargs) -> Record:
    defaults = {
        "original_filename": "test.pdf",
        "source_hash": "abc123",
    }
    defaults.update(kwargs)
    return Record(**defaults)


def _write_file(root: Path, rel_path: str, content: bytes = b"test content") -> None:
    full = root / rel_path
    full.parent.mkdir(parents=True, exist_ok=True)
    full.write_bytes(content)


# ---------------------------------------------------------------------------
# source_reference → archive
# ---------------------------------------------------------------------------

class TestSourceReferenceArchive:
    def test_source_moved_to_archive(self, tmp_path):
        """source_reference file moved to archive/."""
        _write_file(tmp_path, "incoming/doc.pdf")
        record = _make_record(
            source_paths=[PathEntry("incoming/doc.pdf", _ts())],
            source_reference="incoming/doc.pdf",
            state=State.NEEDS_PROCESSING,
        )

        reconciler = FilesystemReconciler(tmp_path)
        reconciler.reconcile([record])

        assert (tmp_path / "archive" / "doc.pdf").exists()
        assert not (tmp_path / "incoming" / "doc.pdf").exists()

    def test_archive_dir_created(self, tmp_path):
        """archive/ directory created if it doesn't exist."""
        _write_file(tmp_path, "incoming/doc.pdf")
        record = _make_record(
            source_paths=[PathEntry("incoming/doc.pdf", _ts())],
            source_reference="incoming/doc.pdf",
            state=State.NEEDS_PROCESSING,
        )

        reconciler = FilesystemReconciler(tmp_path)
        reconciler.reconcile([record])

        assert (tmp_path / "archive").is_dir()
        assert (tmp_path / "archive" / "doc.pdf").exists()


# ---------------------------------------------------------------------------
# source_reference → error (has_error)
# ---------------------------------------------------------------------------

class TestSourceReferenceError:
    def test_has_error_moves_to_error(self, tmp_path):
        """has_error state → source moved to error/."""
        _write_file(tmp_path, "incoming/doc.pdf")
        record = _make_record(
            source_paths=[PathEntry("incoming/doc.pdf", _ts())],
            source_reference="incoming/doc.pdf",
            state=State.HAS_ERROR,
        )

        reconciler = FilesystemReconciler(tmp_path)
        reconciler.reconcile([record])

        assert (tmp_path / "error" / "doc.pdf").exists()
        assert not (tmp_path / "incoming" / "doc.pdf").exists()


# ---------------------------------------------------------------------------
# source_reference → missing (is_missing)
# ---------------------------------------------------------------------------

class TestSourceReferenceMissing:
    def test_is_missing_moves_to_missing(self, tmp_path):
        """is_missing state → source moved to missing/."""
        _write_file(tmp_path, "archive/doc.pdf")
        record = _make_record(
            source_paths=[PathEntry("archive/doc.pdf", _ts())],
            source_reference="archive/doc.pdf",
            state=State.IS_MISSING,
        )

        reconciler = FilesystemReconciler(tmp_path)
        reconciler.reconcile([record])

        assert (tmp_path / "missing" / "doc.pdf").exists()
        assert not (tmp_path / "archive" / "doc.pdf").exists()


# ---------------------------------------------------------------------------
# duplicate_sources → duplicates
# ---------------------------------------------------------------------------

class TestDuplicateSources:
    def test_duplicates_moved_to_duplicates(self, tmp_path):
        """duplicate_sources moved to duplicates/{location}/{filename}."""
        _write_file(tmp_path, "incoming/dup.pdf")
        record = _make_record(
            duplicate_sources=["incoming/dup.pdf"],
        )

        reconciler = FilesystemReconciler(tmp_path)
        reconciler.reconcile([record])

        assert (tmp_path / "duplicates" / "incoming" / "dup.pdf").exists()
        assert not (tmp_path / "incoming" / "dup.pdf").exists()

    def test_duplicate_with_subdir(self, tmp_path):
        """duplicate in sorted/work/dup.pdf → duplicates/sorted/work/dup.pdf."""
        _write_file(tmp_path, "sorted/work/dup.pdf")
        record = _make_record(
            duplicate_sources=["sorted/work/dup.pdf"],
        )

        reconciler = FilesystemReconciler(tmp_path)
        reconciler.reconcile([record])

        assert (tmp_path / "duplicates" / "sorted" / "work" / "dup.pdf").exists()
        assert not (tmp_path / "sorted" / "work" / "dup.pdf").exists()

    def test_multiple_duplicates(self, tmp_path):
        """Multiple duplicate_sources all moved to duplicates/."""
        _write_file(tmp_path, "incoming/dup1.pdf")
        _write_file(tmp_path, "incoming/dup2.pdf")
        record = _make_record(
            duplicate_sources=["incoming/dup1.pdf", "incoming/dup2.pdf"],
        )

        reconciler = FilesystemReconciler(tmp_path)
        reconciler.reconcile([record])

        assert (tmp_path / "duplicates" / "incoming" / "dup1.pdf").exists()
        assert (tmp_path / "duplicates" / "incoming" / "dup2.pdf").exists()
        assert not (tmp_path / "incoming" / "dup1.pdf").exists()
        assert not (tmp_path / "incoming" / "dup2.pdf").exists()

    def test_duplicate_collision_appends_uuid(self, tmp_path):
        """Duplicate with same name already in duplicates/ → UUID appended."""
        _write_file(tmp_path, "incoming/dup.pdf", b"second copy")
        # Pre-existing file in duplicates/
        _write_file(tmp_path, "duplicates/incoming/dup.pdf", b"first copy")

        record = _make_record(
            duplicate_sources=["incoming/dup.pdf"],
        )

        reconciler = FilesystemReconciler(tmp_path)
        reconciler.reconcile([record])

        assert not (tmp_path / "incoming" / "dup.pdf").exists()
        # Original still there
        assert (tmp_path / "duplicates" / "incoming" / "dup.pdf").read_bytes() == b"first copy"
        # Collision file created
        dup_files = list((tmp_path / "duplicates" / "incoming").iterdir())
        assert len(dup_files) == 2
        collision = [f for f in dup_files if f.name != "dup.pdf"][0]
        assert collision.read_bytes() == b"second copy"

    def test_duplicate_missing_source_skipped(self, tmp_path):
        """Duplicate source already gone → no error, just skip."""
        record = _make_record(
            duplicate_sources=["incoming/gone.pdf"],
        )

        reconciler = FilesystemReconciler(tmp_path)
        reconciler.reconcile([record])

        # No duplicates/ dir created since nothing was moved
        assert not (tmp_path / "duplicates").exists()

    def test_duplicate_dirs_created(self, tmp_path):
        """duplicates/ and subdirectories created as needed."""
        _write_file(tmp_path, "sorted/work/invoices/dup.pdf")
        record = _make_record(
            duplicate_sources=["sorted/work/invoices/dup.pdf"],
        )

        reconciler = FilesystemReconciler(tmp_path)
        reconciler.reconcile([record])

        assert (tmp_path / "duplicates" / "sorted" / "work/invoices" / "dup.pdf").exists()

    def test_duplicates_not_in_void(self, tmp_path):
        """Duplicates go to duplicates/, NOT void/."""
        _write_file(tmp_path, "incoming/dup.pdf")
        record = _make_record(
            duplicate_sources=["incoming/dup.pdf"],
        )

        reconciler = FilesystemReconciler(tmp_path)
        reconciler.reconcile([record])

        assert (tmp_path / "duplicates" / "incoming" / "dup.pdf").exists()
        assert not (tmp_path / "void").exists()


# ---------------------------------------------------------------------------
# current_reference + target_path
# ---------------------------------------------------------------------------

class TestCurrentReferenceTargetPath:
    def test_current_moved_to_target(self, tmp_path):
        """current_reference file moved to target_path."""
        _write_file(tmp_path, ".output/uuid-123")
        record = _make_record(
            current_reference=".output/uuid-123",
            target_path="sorted/work/2025-Invoice.pdf",
        )

        reconciler = FilesystemReconciler(tmp_path)
        reconciler.reconcile([record])

        assert (tmp_path / "sorted" / "work" / "2025-Invoice.pdf").exists()
        assert not (tmp_path / ".output" / "uuid-123").exists()

    def test_target_parent_dirs_created(self, tmp_path):
        """Parent directories for target_path created as needed."""
        _write_file(tmp_path, "reviewed/doc.pdf")
        record = _make_record(
            current_reference="reviewed/doc.pdf",
            target_path="sorted/personal/taxes/2025-Tax.pdf",
        )

        reconciler = FilesystemReconciler(tmp_path)
        reconciler.reconcile([record])

        assert (tmp_path / "sorted" / "personal" / "taxes" / "2025-Tax.pdf").exists()


# ---------------------------------------------------------------------------
# deleted_paths → void
# ---------------------------------------------------------------------------

class TestDeletedPaths:
    def test_deleted_paths_moved_to_void(self, tmp_path):
        """deleted_paths moved to void/{location}/{filename}."""
        _write_file(tmp_path, ".output/old-uuid")
        record = _make_record(
            deleted_paths=[".output/old-uuid"],
        )

        reconciler = FilesystemReconciler(tmp_path)
        reconciler.reconcile([record])

        assert (tmp_path / "void" / _today() / ".output" / "old-uuid").exists()
        assert not (tmp_path / ".output" / "old-uuid").exists()

    def test_multiple_deleted_paths(self, tmp_path):
        """Multiple deleted paths all moved to void."""
        _write_file(tmp_path, ".output/uuid-a")
        _write_file(tmp_path, "processed/doc.pdf")
        record = _make_record(
            deleted_paths=[".output/uuid-a", "processed/doc.pdf"],
        )

        reconciler = FilesystemReconciler(tmp_path)
        reconciler.reconcile([record])

        assert (tmp_path / "void" / _today() / ".output" / "uuid-a").exists()
        assert (tmp_path / "void" / _today() / "processed" / "doc.pdf").exists()


# ---------------------------------------------------------------------------
# needs_deletion: all files to void
# ---------------------------------------------------------------------------

class TestNeedsDeletion:
    def test_all_paths_moved_to_void(self, tmp_path):
        """NEEDS_DELETION: all source + current paths to void."""
        _write_file(tmp_path, "archive/doc.pdf")
        _write_file(tmp_path, "sorted/work/doc.pdf")
        record = _make_record(
            source_paths=[PathEntry("archive/doc.pdf", _ts())],
            current_paths=[PathEntry("sorted/work/doc.pdf", _ts())],
            state=State.NEEDS_DELETION,
        )

        reconciler = FilesystemReconciler(tmp_path)
        reconciler.reconcile([record])

        assert (tmp_path / "void" / _today() / "archive" / "doc.pdf").exists()
        assert (tmp_path / "void" / _today() / "sorted" / "work" / "doc.pdf").exists()
        assert not (tmp_path / "archive" / "doc.pdf").exists()
        assert not (tmp_path / "sorted" / "work" / "doc.pdf").exists()


# ---------------------------------------------------------------------------
# Collision handling
# ---------------------------------------------------------------------------

class TestCollision:
    def test_collision_appends_uuid(self, tmp_path):
        """Existing file at dest → UUID appended, both files preserved."""
        _write_file(tmp_path, "incoming/doc.pdf", b"source content")
        _write_file(tmp_path, "archive/doc.pdf", b"existing content")
        record = _make_record(
            source_paths=[PathEntry("incoming/doc.pdf", _ts())],
            source_reference="incoming/doc.pdf",
            state=State.NEEDS_PROCESSING,
        )

        reconciler = FilesystemReconciler(tmp_path)
        reconciler.reconcile([record])

        # Original file still at dest
        assert (tmp_path / "archive" / "doc.pdf").read_bytes() == b"existing content"
        # Source moved to collision path
        assert not (tmp_path / "incoming" / "doc.pdf").exists()

        # Find the collision file
        archive_files = list((tmp_path / "archive").iterdir())
        assert len(archive_files) == 2
        collision_file = [f for f in archive_files if f.name != "doc.pdf"][0]
        assert collision_file.name.startswith("doc_")
        assert collision_file.name.endswith(".pdf")
        assert collision_file.read_bytes() == b"source content"


# ---------------------------------------------------------------------------
# No-op: all temp fields null
# ---------------------------------------------------------------------------

class TestNoOp:
    def test_noop_no_changes(self, tmp_path):
        """All temp fields null → nothing happens."""
        _write_file(tmp_path, "archive/doc.pdf")
        record = _make_record(
            source_paths=[PathEntry("archive/doc.pdf", _ts())],
            state=State.IS_COMPLETE,
        )

        reconciler = FilesystemReconciler(tmp_path)
        reconciler.reconcile([record])

        assert (tmp_path / "archive" / "doc.pdf").exists()
        assert not (tmp_path / "void").exists()


# ---------------------------------------------------------------------------
# Parent dirs created as needed
# ---------------------------------------------------------------------------

class TestParentDirs:
    def test_void_subdirs_created(self, tmp_path):
        """void/ and subdirectories created as needed."""
        _write_file(tmp_path, "sorted/work/taxes/report.pdf")
        record = _make_record(
            deleted_paths=["sorted/work/taxes/report.pdf"],
        )

        reconciler = FilesystemReconciler(tmp_path)
        reconciler.reconcile([record])

        assert (tmp_path / "void" / _today() / "sorted" / "work/taxes" / "report.pdf").exists()


# ---------------------------------------------------------------------------
# Multiple records processed
# ---------------------------------------------------------------------------

class TestMultipleRecords:
    def test_multiple_records(self, tmp_path):
        """Multiple records each have their moves applied."""
        _write_file(tmp_path, "incoming/a.pdf")
        _write_file(tmp_path, "incoming/b.pdf")
        record_a = _make_record(
            source_paths=[PathEntry("incoming/a.pdf", _ts())],
            source_reference="incoming/a.pdf",
            state=State.NEEDS_PROCESSING,
        )
        record_b = _make_record(
            source_paths=[PathEntry("incoming/b.pdf", _ts())],
            source_reference="incoming/b.pdf",
            state=State.NEEDS_PROCESSING,
        )

        reconciler = FilesystemReconciler(tmp_path)
        reconciler.reconcile([record_a, record_b])

        assert (tmp_path / "archive" / "a.pdf").exists()
        assert (tmp_path / "archive" / "b.pdf").exists()
        assert not (tmp_path / "incoming" / "a.pdf").exists()
        assert not (tmp_path / "incoming" / "b.pdf").exists()


# ---------------------------------------------------------------------------
# Missing source file (already gone)
# ---------------------------------------------------------------------------

class TestMissingSource:
    def test_missing_source_skipped(self, tmp_path):
        """Source file already gone → no error, just skip."""
        record = _make_record(
            source_reference="incoming/gone.pdf",
            state=State.NEEDS_PROCESSING,
        )

        reconciler = FilesystemReconciler(tmp_path)
        # Should not raise
        reconciler.reconcile([record])


# ---------------------------------------------------------------------------
# Current paths updated after successful move
# ---------------------------------------------------------------------------

class TestCurrentPathsUpdated:
    def test_current_paths_updated_after_move(self, tmp_path):
        """After moving .output/UUID → processed/file.pdf, current_paths
        should reflect the new location (preventing stale deleted_paths
        warnings on the next cycle)."""
        _write_file(tmp_path, ".output/uuid-123", b"processed content")
        record = _make_record(
            current_paths=[PathEntry(".output/uuid-123", _ts())],
            current_reference=".output/uuid-123",
            target_path="processed/result.pdf",
            state=State.NEEDS_PROCESSING,
        )

        reconciler = FilesystemReconciler(tmp_path)
        reconciler.reconcile([record])

        assert (tmp_path / "processed" / "result.pdf").exists()
        assert not (tmp_path / ".output" / "uuid-123").exists()
        # current_paths should now point to the new location
        assert len(record.current_paths) == 1
        assert record.current_paths[0].path == "processed/result.pdf"

    def test_deleted_output_sidecar_cleaned_up(self, tmp_path):
        """File in .output via deleted_paths → sidecar also removed."""
        _write_file(tmp_path, ".output/uuid-del", b"output content")
        _write_file(tmp_path, ".output/uuid-del.meta.json", b'{"context":"work"}')
        record = _make_record(
            deleted_paths=[".output/uuid-del"],
        )

        reconciler = FilesystemReconciler(tmp_path)
        reconciler.reconcile([record])

        assert (tmp_path / "void" / _today() / ".output" / "uuid-del").exists()
        assert not (tmp_path / ".output" / "uuid-del").exists()
        assert not (tmp_path / ".output" / "uuid-del.meta.json").exists()

    def test_current_paths_unchanged_on_failure(self, tmp_path):
        """If .output/UUID is already gone, current_paths entry moves to
        missing_current_paths (existing behavior)."""
        # Don't create the source file — it's missing
        record = _make_record(
            current_paths=[PathEntry(".output/uuid-gone", _ts())],
            current_reference=".output/uuid-gone",
            target_path="processed/result.pdf",
            state=State.NEEDS_PROCESSING,
        )

        reconciler = FilesystemReconciler(tmp_path)
        reconciler.reconcile([record])

        assert len(record.current_paths) == 0
        assert len(record.missing_current_paths) == 1
        assert record.missing_current_paths[0].path == ".output/uuid-gone"

    def test_current_paths_reflect_collision_name(self, tmp_path):
        """When target already exists, current_paths should record the
        actual collision-variant filename, not the intended target_path.
        Otherwise the next cycle's dedup logic would delete the wrong file."""
        _write_file(tmp_path, ".output/uuid-a", b"new output")
        _write_file(tmp_path, "processed/report.pdf", b"existing file")

        record = _make_record(
            current_paths=[PathEntry(".output/uuid-a", _ts())],
            current_reference=".output/uuid-a",
            target_path="processed/report.pdf",
            state=State.NEEDS_PROCESSING,
        )

        reconciler = FilesystemReconciler(tmp_path)
        reconciler.reconcile([record])

        # Existing file untouched
        assert (tmp_path / "processed" / "report.pdf").read_bytes() == b"existing file"
        # current_paths should record the collision-variant path
        assert len(record.current_paths) == 1
        actual = record.current_paths[0].path
        assert actual != "processed/report.pdf", (
            "current_paths should have collision-variant name, not original target"
        )
        assert actual.startswith("processed/report_")
        assert actual.endswith(".pdf")
        # The collision file should exist at the recorded path
        assert (tmp_path / actual).read_bytes() == b"new output"


# ---------------------------------------------------------------------------
# Source paths updated after successful move
# ---------------------------------------------------------------------------

class TestSourcePathsUpdated:
    def test_source_paths_updated_after_move(self, tmp_path):
        """After moving incoming/doc.pdf → archive/doc.pdf, source_paths
        should reflect archive/ so reconcile doesn't retry next cycle.
        The original path is preserved in missing_source_paths."""
        _write_file(tmp_path, "incoming/doc.pdf")
        record = _make_record(
            source_paths=[PathEntry("incoming/doc.pdf", _ts())],
            source_reference="incoming/doc.pdf",
            state=State.NEEDS_PROCESSING,
        )

        reconciler = FilesystemReconciler(tmp_path)
        reconciler.reconcile([record])

        assert len(record.source_paths) == 1
        assert record.source_paths[0].path == "archive/doc.pdf"
        # Original path preserved so came_from_sorted etc. can detect origin
        assert len(record.missing_source_paths) == 1
        assert record.missing_source_paths[0].path == "incoming/doc.pdf"

    def test_sorted_source_origin_preserved(self, tmp_path):
        """After moving sorted/arbeit/doc.pdf → archive/doc.pdf,
        missing_source_paths should still contain the sorted/ path.
        This is critical for came_from_sorted to detect the original
        location and route the output back to sorted/."""
        _write_file(tmp_path, "sorted/arbeit/doc.pdf")
        record = _make_record(
            source_paths=[PathEntry("sorted/arbeit/doc.pdf", _ts())],
            source_reference="sorted/arbeit/doc.pdf",
            state=State.NEEDS_PROCESSING,
        )

        reconciler = FilesystemReconciler(tmp_path)
        reconciler.reconcile([record])

        assert (tmp_path / "archive" / "doc.pdf").exists()
        assert len(record.source_paths) == 1
        assert record.source_paths[0].path == "archive/doc.pdf"
        # The original sorted/ path must be preserved
        assert any(
            Record._decompose_path(pe.path)[0] == "sorted"
            for pe in record.missing_source_paths
        )

    def test_source_paths_updated_to_missing(self, tmp_path):
        """After moving archive/ → missing/, source_paths should say
        missing/ so step4 won't retry and warn 'source not found'.
        Original archive/ path preserved in missing_source_paths."""
        _write_file(tmp_path, "archive/doc.pdf")
        record = _make_record(
            source_paths=[PathEntry("archive/doc.pdf", _ts())],
            source_reference="archive/doc.pdf",
            state=State.IS_MISSING,
        )

        reconciler = FilesystemReconciler(tmp_path)
        reconciler.reconcile([record])

        assert (tmp_path / "missing" / "doc.pdf").exists()
        assert len(record.source_paths) == 1
        assert record.source_paths[0].path == "missing/doc.pdf"
        assert len(record.missing_source_paths) == 1
        assert record.missing_source_paths[0].path == "archive/doc.pdf"

    def test_source_paths_updated_with_collision(self, tmp_path):
        """When archive/ already has a file with the same name,
        source_paths records the collision-variant path.
        Original path preserved in missing_source_paths."""
        _write_file(tmp_path, "incoming/doc.pdf", b"new")
        _write_file(tmp_path, "archive/doc.pdf", b"existing")
        record = _make_record(
            source_paths=[PathEntry("incoming/doc.pdf", _ts())],
            source_reference="incoming/doc.pdf",
            state=State.NEEDS_PROCESSING,
        )

        reconciler = FilesystemReconciler(tmp_path)
        reconciler.reconcile([record])

        assert len(record.source_paths) == 1
        actual = record.source_paths[0].path
        assert actual.startswith("archive/doc_")
        assert actual.endswith(".pdf")
        assert (tmp_path / actual).read_bytes() == b"new"
        assert len(record.missing_source_paths) == 1
        assert record.missing_source_paths[0].path == "incoming/doc.pdf"

    def test_missing_source_updates_missing_source_paths(self, tmp_path):
        """Source file already gone → source_paths entry moves to
        missing_source_paths."""
        record = _make_record(
            source_paths=[PathEntry("incoming/gone.pdf", _ts())],
            source_reference="incoming/gone.pdf",
            state=State.NEEDS_PROCESSING,
        )

        reconciler = FilesystemReconciler(tmp_path)
        reconciler.reconcile([record])

        assert len(record.source_paths) == 0
        assert len(record.missing_source_paths) == 1
        assert record.missing_source_paths[0].path == "incoming/gone.pdf"


# ---------------------------------------------------------------------------
# Reset flow: reset/ → sorted/ (integration with reconcile)
# ---------------------------------------------------------------------------

class TestResetFlow:
    def test_reset_file_moved_to_sorted(self, tmp_path):
        """File placed in reset/ is moved to sorted/{context}/{assigned_filename}."""
        _write_file(tmp_path, "reset/doc.pdf", b"content")
        record = _make_record(
            current_paths=[PathEntry("reset/doc.pdf", _ts())],
            current_reference="reset/doc.pdf",
            target_path="sorted/work/invoice-2025-01-15.pdf",
            state=State.IS_COMPLETE,
        )

        reconciler = FilesystemReconciler(tmp_path)
        reconciler.reconcile([record])

        assert (tmp_path / "sorted" / "work" / "invoice-2025-01-15.pdf").exists()
        assert not (tmp_path / "reset" / "doc.pdf").exists()
        assert record.current_paths[0].path == "sorted/work/invoice-2025-01-15.pdf"

    def test_reset_creates_sorted_subdirs(self, tmp_path):
        """sorted/ subdirectories are created as needed during reset."""
        _write_file(tmp_path, "reset/doc.pdf", b"content")
        record = _make_record(
            current_paths=[PathEntry("reset/doc.pdf", _ts())],
            current_reference="reset/doc.pdf",
            target_path="sorted/work/Acme/invoice.pdf",
            state=State.IS_COMPLETE,
        )

        reconciler = FilesystemReconciler(tmp_path)
        reconciler.reconcile([record])

        assert (tmp_path / "sorted" / "work" / "Acme" / "invoice.pdf").exists()

    def test_reset_collision_gets_uuid(self, tmp_path):
        """If target already exists in sorted/, collision UUID is appended."""
        _write_file(tmp_path, "reset/doc.pdf", b"reset content")
        _write_file(tmp_path, "sorted/work/invoice.pdf", b"existing")
        record = _make_record(
            current_paths=[PathEntry("reset/doc.pdf", _ts())],
            current_reference="reset/doc.pdf",
            target_path="sorted/work/invoice.pdf",
            state=State.IS_COMPLETE,
        )

        reconciler = FilesystemReconciler(tmp_path)
        reconciler.reconcile([record])

        # Original untouched
        assert (tmp_path / "sorted" / "work" / "invoice.pdf").read_bytes() == b"existing"
        # Collision file created
        actual = record.current_paths[0].path
        assert actual.startswith("sorted/work/invoice_")
        assert actual.endswith(".pdf")
        assert (tmp_path / actual).read_bytes() == b"reset content"
