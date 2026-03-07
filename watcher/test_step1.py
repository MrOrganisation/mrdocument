"""Tests for step1.py — FilesystemDetector scan, diff, stray handling."""

import asyncio
import hashlib
from datetime import datetime, timezone
from pathlib import Path

import pytest

from models import EventType, PathEntry, Record, State
from step1 import FilesystemDetector, _is_ignored, _is_config_file


def _write_file(path: Path, content: bytes = b"test content") -> None:
    """Helper to create a file with given content."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def _hash_bytes(data: bytes) -> str:
    """Compute SHA-256 of bytes."""
    return hashlib.sha256(data).hexdigest()


def _make_record(**kwargs) -> Record:
    """Helper to create a Record with sensible defaults."""
    defaults = {
        "original_filename": "test.pdf",
        "source_hash": "abc123",
    }
    defaults.update(kwargs)
    return Record(**defaults)


def _setup_dirs(root: Path) -> None:
    """Create the standard directory structure."""
    for d in ("archive", "incoming", "reviewed", "processed", "reset",
              "trash", ".output", "sorted", "error"):
        (root / d).mkdir(parents=True, exist_ok=True)


@pytest.fixture(autouse=True)
def _cleanup_detectors():
    """Stop all FilesystemDetector observers after each test to avoid
    exhausting the inotify instance limit."""
    created: list[FilesystemDetector] = []
    original_init = FilesystemDetector.__init__

    def tracking_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        created.append(self)

    FilesystemDetector.__init__ = tracking_init
    yield
    FilesystemDetector.__init__ = original_init
    for detector in created:
        detector.stop()


# ---------------------------------------------------------------------------
# Initial scan
# ---------------------------------------------------------------------------

class TestInitialScan:
    @pytest.mark.asyncio
    async def test_initial_scan_detects_all_files(self, tmp_path):
        _setup_dirs(tmp_path)
        c1 = b"file one"
        c2 = b"file two"
        c3 = b"file three"
        _write_file(tmp_path / "incoming" / "doc1.pdf", c1)
        _write_file(tmp_path / "archive" / "doc2.pdf", c2)
        _write_file(tmp_path / "sorted" / "work" / "doc3.pdf", c3)

        detector = FilesystemDetector(root=tmp_path)
        # archive file needs to be known to not be flagged as stray
        snapshot = [_make_record(source_hash=_hash_bytes(c2))]
        changes = await detector.detect(snapshot)

        paths = {c.path for c in changes}
        assert "incoming/doc1.pdf" in paths
        assert "archive/doc2.pdf" in paths
        assert "sorted/work/doc3.pdf" in paths
        assert all(c.event_type == EventType.ADDITION for c in changes)

    @pytest.mark.asyncio
    async def test_initial_scan_detects_reset_files(self, tmp_path):
        """Files in reset/ are detected during initial scan when known."""
        _setup_dirs(tmp_path)
        content = b"reset file"
        _write_file(tmp_path / "reset" / "doc.pdf", content)
        file_hash = _hash_bytes(content)

        # File must be known (hash matches a record) or it's treated as stray
        record = _make_record(hash=file_hash)

        detector = FilesystemDetector(root=tmp_path)
        changes = await detector.detect([record])

        paths = {c.path for c in changes}
        assert "reset/doc.pdf" in paths

    @pytest.mark.asyncio
    async def test_unknown_reset_file_moved_to_error(self, tmp_path):
        """Unknown file in reset/ is treated as stray (moved to error/)."""
        _setup_dirs(tmp_path)
        _write_file(tmp_path / "reset" / "unknown.pdf", b"unknown")

        detector = FilesystemDetector(root=tmp_path)
        changes = await detector.detect([])

        # File moved to error, not in changes
        paths = {c.path for c in changes}
        assert "reset/unknown.pdf" not in paths
        assert (tmp_path / "error" / "unknown.pdf").exists()


# ---------------------------------------------------------------------------
# Restart idempotency
# ---------------------------------------------------------------------------

class TestRestartIdempotency:
    """Full scan after restart should not re-emit ADDITIONs for tracked files."""

    @pytest.mark.asyncio
    async def test_full_scan_skips_files_already_in_db(self, tmp_path):
        """Files whose path+hash match a DB record should not produce ADDITIONs."""
        _setup_dirs(tmp_path)
        c_archive = b"archived source"
        c_sorted = b"sorted output"
        _write_file(tmp_path / "archive" / "doc.pdf", c_archive)
        _write_file(tmp_path / "sorted" / "work" / "doc.pdf", c_sorted)

        now = datetime.now(timezone.utc)
        record = _make_record(
            source_hash=_hash_bytes(c_archive),
            hash=_hash_bytes(c_sorted),
            state=State.IS_COMPLETE,
            source_paths=[PathEntry("archive/doc.pdf", now)],
            current_paths=[PathEntry("sorted/work/doc.pdf", now)],
        )

        detector = FilesystemDetector(root=tmp_path)
        changes = await detector.detect([record])

        # Neither file should produce an ADDITION — both are already tracked
        paths = {c.path for c in changes}
        assert "archive/doc.pdf" not in paths, (
            "archive/doc.pdf should not be re-emitted on restart"
        )
        assert "sorted/work/doc.pdf" not in paths, (
            "sorted/work/doc.pdf should not be re-emitted on restart"
        )

    @pytest.mark.asyncio
    async def test_full_scan_detects_new_file_alongside_tracked(self, tmp_path):
        """Genuinely new files should still be detected alongside tracked ones."""
        _setup_dirs(tmp_path)
        c_existing = b"existing content"
        c_new = b"brand new file"
        _write_file(tmp_path / "archive" / "old.pdf", c_existing)
        _write_file(tmp_path / "incoming" / "new.pdf", c_new)

        now = datetime.now(timezone.utc)
        record = _make_record(
            source_hash=_hash_bytes(c_existing),
            state=State.IS_COMPLETE,
            source_paths=[PathEntry("archive/old.pdf", now)],
        )

        detector = FilesystemDetector(root=tmp_path)
        changes = await detector.detect([record])

        paths = {c.path for c in changes}
        assert "archive/old.pdf" not in paths, (
            "Tracked file should not produce ADDITION"
        )
        assert "incoming/new.pdf" in paths, (
            "New file should still be detected"
        )

    @pytest.mark.asyncio
    async def test_full_scan_detects_changed_hash(self, tmp_path):
        """File at tracked path but with different hash should be detected."""
        _setup_dirs(tmp_path)
        c_original = b"original content"
        c_modified = b"modified while watcher was down"
        # File on disk has different content than what DB recorded
        _write_file(tmp_path / "incoming" / "doc.pdf", c_modified)

        now = datetime.now(timezone.utc)
        record = _make_record(
            source_hash=_hash_bytes(c_original),
            state=State.IS_COMPLETE,
            source_paths=[PathEntry("incoming/doc.pdf", now)],
        )

        detector = FilesystemDetector(root=tmp_path)
        changes = await detector.detect([record])

        paths = {c.path for c in changes}
        assert "incoming/doc.pdf" in paths, (
            "File with changed hash should be detected as ADDITION"
        )

    @pytest.mark.asyncio
    async def test_full_scan_emits_removal_for_missing_db_paths(self, tmp_path):
        """Paths in DB but not on disk should still produce REMOVAL events."""
        _setup_dirs(tmp_path)

        now = datetime.now(timezone.utc)
        record = _make_record(
            source_hash="deadbeef",
            state=State.IS_COMPLETE,
            source_paths=[PathEntry("archive/gone.pdf", now)],
            current_paths=[PathEntry("sorted/work/gone.pdf", now)],
        )

        detector = FilesystemDetector(root=tmp_path)
        changes = await detector.detect([record])

        removal_paths = {c.path for c in changes if c.event_type == EventType.REMOVAL}
        assert "archive/gone.pdf" in removal_paths
        assert "sorted/work/gone.pdf" in removal_paths


# ---------------------------------------------------------------------------
# Additions
# ---------------------------------------------------------------------------

class TestAdditions:
    @pytest.mark.asyncio
    async def test_new_file_detected_as_addition(self, tmp_path):
        _setup_dirs(tmp_path)
        detector = FilesystemDetector(root=tmp_path)

        # First scan: empty
        changes = await detector.detect([])
        assert changes == []

        # Add a file and wait for inotify delivery
        content = b"new document"
        _write_file(tmp_path / "incoming" / "new.pdf", content)
        await asyncio.sleep(0.1)

        changes = await detector.detect([])
        assert len(changes) == 1
        assert changes[0].event_type == EventType.ADDITION
        assert changes[0].path == "incoming/new.pdf"
        assert changes[0].hash == _hash_bytes(content)
        assert changes[0].size == len(content)


# ---------------------------------------------------------------------------
# Removals
# ---------------------------------------------------------------------------

class TestRemovals:
    @pytest.mark.asyncio
    async def test_removed_file_detected_as_removal(self, tmp_path):
        _setup_dirs(tmp_path)
        content = b"existing file"
        _write_file(tmp_path / "incoming" / "exist.pdf", content)

        detector = FilesystemDetector(root=tmp_path)
        await detector.detect([])

        # Remove the file and wait for inotify delivery
        (tmp_path / "incoming" / "exist.pdf").unlink()
        await asyncio.sleep(0.1)

        changes = await detector.detect([])
        assert len(changes) == 1
        assert changes[0].event_type == EventType.REMOVAL
        assert changes[0].path == "incoming/exist.pdf"
        assert changes[0].hash is None
        assert changes[0].size is None


# ---------------------------------------------------------------------------
# Stray detection
# ---------------------------------------------------------------------------

class TestStrayDetection:
    @pytest.mark.asyncio
    async def test_unknown_in_archive_moved_to_error(self, tmp_path):
        _setup_dirs(tmp_path)
        _write_file(tmp_path / "archive" / "stray.pdf", b"unknown")

        detector = FilesystemDetector(root=tmp_path)
        changes = await detector.detect([])

        # File should NOT be in change list
        assert all(c.path != "archive/stray.pdf" for c in changes)
        # File should have been moved to error/
        assert (tmp_path / "error" / "stray.pdf").exists()
        assert not (tmp_path / "archive" / "stray.pdf").exists()

    @pytest.mark.asyncio
    async def test_unknown_in_incoming_not_moved(self, tmp_path):
        _setup_dirs(tmp_path)
        _write_file(tmp_path / "incoming" / "new.pdf", b"new incoming")

        detector = FilesystemDetector(root=tmp_path)
        changes = await detector.detect([])

        paths = {c.path for c in changes}
        assert "incoming/new.pdf" in paths
        assert (tmp_path / "incoming" / "new.pdf").exists()

    @pytest.mark.asyncio
    async def test_unknown_in_sorted_subdir_not_moved(self, tmp_path):
        _setup_dirs(tmp_path)
        _write_file(tmp_path / "sorted" / "work" / "new.pdf", b"sorted file")

        detector = FilesystemDetector(root=tmp_path)
        changes = await detector.detect([])

        paths = {c.path for c in changes}
        assert "sorted/work/new.pdf" in paths
        assert (tmp_path / "sorted" / "work" / "new.pdf").exists()

    @pytest.mark.asyncio
    async def test_known_file_in_archive_not_moved(self, tmp_path):
        _setup_dirs(tmp_path)
        content = b"known content"
        _write_file(tmp_path / "archive" / "known.pdf", content)

        snapshot = [_make_record(source_hash=_hash_bytes(content))]

        detector = FilesystemDetector(root=tmp_path)
        changes = await detector.detect(snapshot)

        paths = {c.path for c in changes}
        assert "archive/known.pdf" in paths
        assert (tmp_path / "archive" / "known.pdf").exists()
        assert not (tmp_path / "error" / "known.pdf").exists()

    @pytest.mark.asyncio
    async def test_output_file_matches_output_filename(self, tmp_path):
        _setup_dirs(tmp_path)
        _write_file(tmp_path / ".output" / "uuid-123", b"output content")

        snapshot = [_make_record(output_filename="uuid-123")]

        detector = FilesystemDetector(root=tmp_path)
        changes = await detector.detect(snapshot)

        paths = {c.path for c in changes}
        assert ".output/uuid-123" in paths
        assert (tmp_path / ".output" / "uuid-123").exists()

    @pytest.mark.asyncio
    async def test_output_file_stray_when_output_filename_cleared(self, tmp_path):
        """If output_filename is None (cleared after ingestion), .output file
        is treated as stray — demonstrates the re-launch race condition result.

        When step3 is re-launched and writes .output/UUID a second time,
        the record's output_filename has already been cleared by the first
        ingestion. The second file has no matching record → stray.
        """
        _setup_dirs(tmp_path)
        _write_file(tmp_path / ".output" / "uuid-456", b"orphaned output")

        # Record exists but output_filename was cleared (ingested)
        snapshot = [_make_record(output_filename=None, state=State.IS_COMPLETE)]

        detector = FilesystemDetector(root=tmp_path)
        changes = await detector.detect(snapshot)

        # File should NOT appear as a change (deleted as stray)
        paths = {c.path for c in changes}
        assert ".output/uuid-456" not in paths
        assert not (tmp_path / ".output" / "uuid-456").exists()

    @pytest.mark.asyncio
    async def test_output_file_stray_incremental_after_relaunch(self, tmp_path):
        """Incremental detection: .output file written by re-launched step3
        is classified as stray because output_filename was already cleared.

        Simulates the sequence:
        1. First full scan establishes baseline
        2. Step3 (re-launched) writes .output/UUID
        3. Incremental detection finds new file
        4. Record has output_filename=None → stray
        """
        _setup_dirs(tmp_path)

        # Initial scan: no .output files
        detector = FilesystemDetector(root=tmp_path)
        await detector.detect([])

        # Record: output_filename cleared (first processing already ingested)
        snapshot = [_make_record(output_filename=None, state=State.IS_COMPLETE)]

        # Simulate re-launched step3 writing the file AFTER initial scan
        _write_file(tmp_path / ".output" / "relaunched-uuid", b"second output")
        await asyncio.sleep(0.1)  # Let inotify fire

        changes = await detector.detect(snapshot)

        # File should be stray (deleted)
        paths = {c.path for c in changes}
        assert ".output/relaunched-uuid" not in paths
        assert not (tmp_path / ".output" / "relaunched-uuid").exists()


# ---------------------------------------------------------------------------
# Ignored files
# ---------------------------------------------------------------------------

class TestIgnoredFiles:
    @pytest.mark.asyncio
    async def test_hidden_files_ignored(self, tmp_path):
        _setup_dirs(tmp_path)
        _write_file(tmp_path / "incoming" / ".hidden", b"hidden")
        _write_file(tmp_path / "incoming" / "~tempfile", b"temp")
        _write_file(tmp_path / "incoming" / "normal.pdf", b"normal")

        detector = FilesystemDetector(root=tmp_path)
        changes = await detector.detect([])

        paths = {c.path for c in changes}
        assert "incoming/normal.pdf" in paths
        assert "incoming/.hidden" not in paths
        assert "incoming/~tempfile" not in paths

    @pytest.mark.asyncio
    async def test_syncthing_temp_files_ignored(self, tmp_path):
        _setup_dirs(tmp_path)
        # Syncthing pattern: contains ".syncthing."
        _write_file(tmp_path / "incoming" / "foo.syncthing.bar.pdf", b"sync")
        # Temp extension
        _write_file(tmp_path / "incoming" / "document.tmp", b"tmp")
        _write_file(tmp_path / "incoming" / "real.pdf", b"real")

        detector = FilesystemDetector(root=tmp_path)
        changes = await detector.detect([])

        paths = {c.path for c in changes}
        assert "incoming/real.pdf" in paths
        assert "incoming/foo.syncthing.bar.pdf" not in paths
        assert "incoming/document.tmp" not in paths


# ---------------------------------------------------------------------------
# File size
# ---------------------------------------------------------------------------

class TestFileSize:
    @pytest.mark.asyncio
    async def test_file_size_included_in_additions(self, tmp_path):
        _setup_dirs(tmp_path)
        content = b"x" * 42
        _write_file(tmp_path / "incoming" / "sized.pdf", content)

        detector = FilesystemDetector(root=tmp_path)
        changes = await detector.detect([])

        assert len(changes) == 1
        assert changes[0].size == 42


# ---------------------------------------------------------------------------
# Parent dirs created
# ---------------------------------------------------------------------------

class TestParentDirs:
    @pytest.mark.asyncio
    async def test_error_dir_created_for_stray(self, tmp_path):
        """error/ directory is created if it doesn't exist."""
        # Only create archive, not error
        (tmp_path / "archive").mkdir()
        _write_file(tmp_path / "archive" / "stray.pdf", b"stray")

        detector = FilesystemDetector(root=tmp_path)
        changes = await detector.detect([])

        assert (tmp_path / "error").exists()
        assert (tmp_path / "error" / "stray.pdf").exists()


# ---------------------------------------------------------------------------
# Multiple records
# ---------------------------------------------------------------------------

class TestMultipleRecords:
    @pytest.mark.asyncio
    async def test_multiple_detect_cycles(self, tmp_path):
        _setup_dirs(tmp_path)
        detector = FilesystemDetector(root=tmp_path)

        # Cycle 1: add file
        _write_file(tmp_path / "incoming" / "a.pdf", b"aaa")
        changes = await detector.detect([])
        assert len(changes) == 1
        assert changes[0].path == "incoming/a.pdf"

        # Cycle 2: add another, first still present
        _write_file(tmp_path / "incoming" / "b.pdf", b"bbb")
        await asyncio.sleep(0.1)
        changes = await detector.detect([])
        assert len(changes) == 1
        assert changes[0].path == "incoming/b.pdf"

        # Cycle 3: remove first
        (tmp_path / "incoming" / "a.pdf").unlink()
        await asyncio.sleep(0.1)
        changes = await detector.detect([])
        assert len(changes) == 1
        assert changes[0].event_type == EventType.REMOVAL
        assert changes[0].path == "incoming/a.pdf"


# ---------------------------------------------------------------------------
# Config file detection
# ---------------------------------------------------------------------------

class TestIsConfigFile:
    def test_context_yaml(self):
        assert _is_config_file("sorted/arbeit/context.yaml") is True

    def test_smartfolders_yaml(self):
        assert _is_config_file("sorted/privat/smartfolders.yaml") is True

    def test_regular_file_in_sorted(self):
        assert _is_config_file("sorted/arbeit/doc.pdf") is False

    def test_config_in_wrong_location(self):
        assert _is_config_file("incoming/context.yaml") is False

    def test_deeply_nested(self):
        assert _is_config_file("sorted/arbeit/sub/context.yaml") is False

    def test_top_level(self):
        assert _is_config_file("context.yaml") is False


class TestConfigChangedFlag:
    @pytest.mark.asyncio
    async def test_config_file_not_in_full_scan_changes(self, tmp_path):
        """Config files in sorted/ should not appear in full scan changes."""
        _setup_dirs(tmp_path)
        _write_file(tmp_path / "sorted" / "work" / "context.yaml", b"name: work")
        _write_file(tmp_path / "sorted" / "work" / "doc.pdf", b"real doc")

        detector = FilesystemDetector(root=tmp_path)
        changes = await detector.detect([])

        paths = {c.path for c in changes}
        assert "sorted/work/context.yaml" not in paths
        assert "sorted/work/doc.pdf" in paths

    @pytest.mark.asyncio
    async def test_config_file_sets_flag_incremental(self, tmp_path):
        """Config file change in incremental mode sets config_changed flag."""
        _setup_dirs(tmp_path)

        detector = FilesystemDetector(root=tmp_path)
        # First scan to start observer
        await detector.detect([])
        assert detector.config_changed is False

        # Create config file
        _write_file(tmp_path / "sorted" / "work" / "context.yaml", b"name: work")
        await asyncio.sleep(0.1)

        changes = await detector.detect([])
        assert detector.config_changed is True
        # Config file should NOT appear in changes
        paths = {c.path for c in changes}
        assert "sorted/work/context.yaml" not in paths

    @pytest.mark.asyncio
    async def test_smartfolders_yaml_sets_flag(self, tmp_path):
        """smartfolders.yaml change sets config_changed flag."""
        _setup_dirs(tmp_path)

        detector = FilesystemDetector(root=tmp_path)
        await detector.detect([])

        _write_file(tmp_path / "sorted" / "work" / "smartfolders.yaml", b"smart_folders: {}")
        await asyncio.sleep(0.1)

        changes = await detector.detect([])
        assert detector.config_changed is True
        paths = {c.path for c in changes}
        assert "sorted/work/smartfolders.yaml" not in paths

    @pytest.mark.asyncio
    async def test_generated_yaml_not_in_full_scan(self, tmp_path):
        """generated.yaml in sorted/ should not appear in full scan changes."""
        _setup_dirs(tmp_path)
        _write_file(tmp_path / "sorted" / "work" / "generated.yaml", b"fields: {}")
        _write_file(tmp_path / "sorted" / "work" / "doc.pdf", b"real doc")

        detector = FilesystemDetector(root=tmp_path)
        changes = await detector.detect([])

        paths = {c.path for c in changes}
        assert "sorted/work/generated.yaml" not in paths
        assert "sorted/work/doc.pdf" in paths

    @pytest.mark.asyncio
    async def test_generated_yaml_sets_flag_incremental(self, tmp_path):
        """generated.yaml change in incremental mode sets config_changed flag."""
        _setup_dirs(tmp_path)

        detector = FilesystemDetector(root=tmp_path)
        await detector.detect([])
        assert detector.config_changed is False

        _write_file(tmp_path / "sorted" / "work" / "generated.yaml", b"fields: {}")
        await asyncio.sleep(0.1)

        changes = await detector.detect([])
        assert detector.config_changed is True
        paths = {c.path for c in changes}
        assert "sorted/work/generated.yaml" not in paths
