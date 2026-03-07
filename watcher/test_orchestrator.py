"""Tests for orchestrator.py — integration tests with real DB + mock service + real filesystem."""

import asyncio
import base64
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio
import yaml
from aiohttp import web

from db_new import DocumentDBv2
from models import State, PathEntry, Record
from orchestrator import (
    DocumentWatcherV2, context_field_names_from_sorter,
    contexts_for_api_from_sorter, context_folders_from_sorter,
)
from sorter import SorterContextManager
from step1 import compute_sha256


# ---------------------------------------------------------------------------
# Mock mrdocument service
# ---------------------------------------------------------------------------

class MockService:
    """Fake mrdocument HTTP service for testing."""

    def __init__(self):
        self.calls: list[dict] = []
        self.response_metadata: dict = {
            "context": "work",
            "date": "2025-01-15",
            "assigned_filename": "work-Invoice-2025-Acme-Payment.pdf",
        }
        self.response_pdf_bytes: bytes = b"fake pdf output content"
        self.response_new_clues: dict | None = None
        self.fail_status: int | None = None

    async def handle_process(self, request: web.Request) -> web.Response:
        reader = await request.multipart()
        fields: dict = {}
        file_data: bytes | None = None

        while True:
            part = await reader.next()
            if part is None:
                break
            if part.name == "file":
                file_data = await part.read()
                fields["filename"] = part.filename
                fields["content_type"] = part.headers.get("Content-Type", "")
            else:
                fields[part.name] = await part.text()

        self.calls.append({"fields": fields, "file_data": file_data})

        if self.fail_status is not None:
            return web.Response(status=self.fail_status, text="Error")

        pdf_b64 = base64.b64encode(self.response_pdf_bytes).decode()
        body = {
            "metadata": self.response_metadata,
            "pdf": pdf_b64,
        }
        if self.response_new_clues is not None:
            body["new_clues"] = self.response_new_clues
        return web.json_response(body)

    def make_app(self) -> web.Application:
        app = web.Application()
        app.router.add_post("/process", self.handle_process)
        return app


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
    await db.pool.execute("DELETE FROM mrdocument.documents_v2")
    await db.disconnect()


@pytest_asyncio.fixture
async def mock_service():
    """Start a mock mrdocument service, yield it, then cleanup."""
    service = MockService()
    runner = web.AppRunner(service.make_app())
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    service.url = f"http://127.0.0.1:{port}"
    yield service
    await runner.cleanup()


@pytest.fixture
def user_root(tmp_path):
    """Create standard directory structure."""
    for d in ("archive", "incoming", "reviewed", "processed",
              "trash", ".output", "sorted", "error", "void", "missing"):
        (tmp_path / d).mkdir()
    return tmp_path


def _write_file(root: Path, rel_path: str, content: bytes = b"test pdf content") -> None:
    full = root / rel_path
    full.parent.mkdir(parents=True, exist_ok=True)
    full.write_bytes(content)


# ---------------------------------------------------------------------------
# End-to-end: new file → processed → sorted
# ---------------------------------------------------------------------------

class TestEndToEnd:
    @pytest.mark.asyncio
    async def test_new_file_to_sorted(self, user_root, db, mock_service):
        """Full lifecycle: incoming → archive + .output → sorted."""
        content = b"test document content for e2e"
        _write_file(user_root, "incoming/invoice.pdf", content)

        watcher = DocumentWatcherV2(
            root=user_root,
            db=db,
            service_url=mock_service.url,
            context_field_names={"work": ["context", "date", "type", "sender", "topic"]},
        )

        # Cycle 1: detect file, create record, reconcile (sets output_filename),
        #          launch processing (background), reconcile again, move to sorted
        await watcher.run_cycle()
        await watcher.shutdown()

        # After cycle 1: source should be in archive
        assert (user_root / "archive" / "invoice.pdf").exists()
        assert not (user_root / "incoming" / "invoice.pdf").exists()

        # Service was called
        assert len(mock_service.calls) == 1

        # Cycle 2: detect .output file, read sidecar, reconcile → target_path, move to sorted
        await watcher.run_cycle()

        # File should now be in sorted/work/
        expected_sorted = user_root / "sorted" / "work" / mock_service.response_metadata["assigned_filename"]
        assert expected_sorted.exists()
        assert expected_sorted.read_bytes() == mock_service.response_pdf_bytes

        # DB record should be IS_COMPLETE
        records = await db.get_snapshot()
        assert len(records) == 1
        assert records[0].state == State.IS_COMPLETE
        assert records[0].context == "work"

    @pytest.mark.asyncio
    async def test_multiple_files_in_one_cycle(self, user_root, db, mock_service):
        """Multiple files in incoming/ all processed in one pass."""
        for i in range(3):
            _write_file(user_root, f"incoming/doc{i}.pdf", f"content {i}".encode())

        watcher = DocumentWatcherV2(
            root=user_root,
            db=db,
            service_url=mock_service.url,
        )

        # Cycle 1: detect + launch processing + reconcile
        await watcher.run_cycle()
        await watcher.shutdown()

        # All 3 should be in archive
        for i in range(3):
            assert (user_root / "archive" / f"doc{i}.pdf").exists()

        # Service called 3 times
        assert len(mock_service.calls) == 3

        # 3 records in DB
        records = await db.get_snapshot()
        assert len(records) == 3


# ---------------------------------------------------------------------------
# Error recovery
# ---------------------------------------------------------------------------

class TestErrorRecovery:
    @pytest.mark.asyncio
    async def test_service_error_then_recovery(self, user_root, db, mock_service):
        """Service returns 500 → HAS_ERROR; fix service → can detect error state."""
        content = b"error test doc"
        _write_file(user_root, "incoming/doc.pdf", content)

        mock_service.fail_status = 500
        watcher = DocumentWatcherV2(
            root=user_root,
            db=db,
            service_url=mock_service.url,
            processor_timeout=5.0,
        )
        # Override processor to not retry
        watcher.processor.max_retries = 0

        # Cycle 1: detect + launch processing (fails in background) + reconcile
        await watcher.run_cycle()
        await watcher.shutdown()

        # Cycle 2: .output 0-byte detected → HAS_ERROR
        await watcher.run_cycle()

        records = await db.get_snapshot()
        # Record should exist with HAS_ERROR state
        error_records = [r for r in records if r.state == State.HAS_ERROR]
        assert len(error_records) >= 1


# ---------------------------------------------------------------------------
# Stray detection
# ---------------------------------------------------------------------------

class TestStrayDetection:
    @pytest.mark.asyncio
    async def test_unknown_file_in_archive_moved_to_error(self, user_root, db, mock_service):
        """Unknown file in archive/ (not matching any record) → moved to error/."""
        _write_file(user_root, "archive/stray.pdf", b"unknown stray file")

        watcher = DocumentWatcherV2(
            root=user_root,
            db=db,
            service_url=mock_service.url,
        )

        await watcher.run_cycle()

        # Stray should be moved to error/
        assert (user_root / "error" / "stray.pdf").exists()
        assert not (user_root / "archive" / "stray.pdf").exists()

        # No DB records created for strays
        records = await db.get_snapshot()
        assert len(records) == 0


# ---------------------------------------------------------------------------
# File removal tracking
# ---------------------------------------------------------------------------

class TestFileRemovalTracking:
    @pytest.mark.asyncio
    async def test_source_removal_tracked(self, user_root, db, mock_service):
        """Removing a source file after detection tracks it in missing_source_paths."""
        content = b"removal test doc"
        _write_file(user_root, "incoming/doc.pdf", content)

        watcher = DocumentWatcherV2(
            root=user_root,
            db=db,
            service_url=mock_service.url,
        )

        # Cycle 1: detect and launch processing
        await watcher.run_cycle()
        await watcher.shutdown()

        # Now remove the archive file (source was moved to archive in cycle 1)
        archive_file = user_root / "archive" / "doc.pdf"
        if archive_file.exists():
            archive_file.unlink()

        # Cycle 2: detect removal
        await watcher.run_cycle()

        records = await db.get_snapshot()
        assert len(records) >= 1
        # At least one record should have missing_source_paths
        has_missing = any(len(r.missing_source_paths) > 0 for r in records)
        assert has_missing


# ---------------------------------------------------------------------------
# Duplicate detection
# ---------------------------------------------------------------------------

class TestDuplicateDetection:
    @pytest.mark.asyncio
    async def test_same_hash_second_copy(self, user_root, db, mock_service):
        """Same file (same content hash) placed twice → second matched to same record."""
        content = b"duplicate content bytes"
        _write_file(user_root, "incoming/first.pdf", content)

        watcher = DocumentWatcherV2(
            root=user_root,
            db=db,
            service_url=mock_service.url,
        )

        # Cycle 1: process first file
        await watcher.run_cycle()
        await watcher.shutdown()

        # Place duplicate in incoming
        _write_file(user_root, "incoming/second.pdf", content)

        # Cycle 2: detect duplicate
        await watcher.run_cycle()

        # Should still be 1 record (second matched by source_hash)
        records = await db.get_snapshot()
        assert len(records) == 1


# ---------------------------------------------------------------------------
# Duplicate output hash detection
# ---------------------------------------------------------------------------

class TestDuplicateOutputHash:
    @pytest.mark.asyncio
    async def test_output_hash_matches_other_source_hash(self, user_root, db, mock_service):
        """Two files process; second output hash = first's source_hash → duplicate discarded."""
        content_a = b"source content for file A - source hash test"
        content_b = b"different source content for file B - source hash test"

        # Process A first with default mock output (different from content_a)
        _write_file(user_root, "incoming/a.pdf", content_a)
        watcher = DocumentWatcherV2(
            root=user_root, db=db, service_url=mock_service.url,
            context_field_names={"work": ["context", "date", "type", "sender", "topic"]},
        )

        # Cycle 1: detect A, create record, process in background
        await watcher.run_cycle()
        await watcher.shutdown()

        # Cycle 2: ingest A's .output → IS_COMPLETE, move to sorted/
        await watcher.run_cycle()

        records = await db.get_snapshot()
        assert len(records) == 1
        assert records[0].state == State.IS_COMPLETE
        a_hash = records[0].hash
        a_source_hash = records[0].source_hash
        assert a_hash != a_source_hash  # mock output differs from source

        # Now change mock to return content_a bytes → output hash = A's source_hash
        mock_service.response_pdf_bytes = content_a
        mock_service.response_metadata["assigned_filename"] = "work-B-Output.pdf"
        mock_service.calls.clear()

        _write_file(user_root, "incoming/b.pdf", content_b)

        # Cycle 3: detect B, create record, process in background
        await watcher.run_cycle()
        await watcher.shutdown()

        assert len(mock_service.calls) == 1  # B was sent to service

        # Cycle 4: detect B's .output → hash matches A's source_hash → duplicate
        await watcher.run_cycle()

        # B's record should be gone (deleted by reconcile after HAS_ERROR + no sources)
        records = await db.get_snapshot()
        assert len(records) == 1
        assert records[0].state == State.IS_COMPLETE
        assert records[0].source_hash == a_source_hash

        # A's sorted file still intact
        sorted_files = list((user_root / "sorted" / "work").glob("*.pdf"))
        assert len(sorted_files) == 1

        # B's output was NOT placed in sorted/
        assert not (user_root / "sorted" / "work" / "work-B-Output.pdf").exists()

    @pytest.mark.asyncio
    async def test_output_hash_matches_other_hash(self, user_root, db, mock_service):
        """Two files process with same mock output → second output hash = first's hash → duplicate."""
        content_a = b"source content for file A - hash collision test"
        content_b = b"different source content for file B - hash collision test"

        # Mock returns same output bytes for both
        shared_output = b"shared processed output bytes"
        mock_service.response_pdf_bytes = shared_output

        # Process A
        _write_file(user_root, "incoming/a.pdf", content_a)
        watcher = DocumentWatcherV2(
            root=user_root, db=db, service_url=mock_service.url,
            context_field_names={"work": ["context", "date", "type", "sender", "topic"]},
        )

        # Cycle 1: detect A, process
        await watcher.run_cycle()
        await watcher.shutdown()

        # Cycle 2: ingest A's .output → IS_COMPLETE
        await watcher.run_cycle()

        records = await db.get_snapshot()
        assert len(records) == 1
        assert records[0].state == State.IS_COMPLETE
        a_hash = records[0].hash
        assert a_hash == compute_sha256(user_root / "sorted" / "work" / mock_service.response_metadata["assigned_filename"])

        # Process B with same mock output
        mock_service.response_metadata["assigned_filename"] = "work-B-Collision.pdf"
        mock_service.calls.clear()

        _write_file(user_root, "incoming/b.pdf", content_b)

        # Cycle 3: detect B, process
        await watcher.run_cycle()
        await watcher.shutdown()

        assert len(mock_service.calls) == 1

        # Cycle 4: detect B's .output → hash matches A's hash → duplicate
        await watcher.run_cycle()

        # Only A's record remains
        records = await db.get_snapshot()
        assert len(records) == 1
        assert records[0].state == State.IS_COMPLETE
        assert records[0].hash == a_hash

        # A's file still in sorted/
        assert (user_root / "sorted" / "work" / "work-Invoice-2025-Acme-Payment.pdf").exists()

        # B's output never made it to sorted/
        assert not (user_root / "sorted" / "work" / "work-B-Collision.pdf").exists()

        # Run one more cycle to let stray detection clean up orphaned files
        await watcher.run_cycle()

        # B's orphaned archive source cleaned up by stray detection
        archive_files = list((user_root / "archive").iterdir())
        assert len(archive_files) == 1  # only A's archive copy remains


# ---------------------------------------------------------------------------
# Needs deletion (trash)
# ---------------------------------------------------------------------------

class TestNeedsDeletion:
    @pytest.mark.asyncio
    async def test_file_moved_to_trash_triggers_deletion(self, user_root, db, mock_service):
        """File moved to trash/ → record state becomes IS_DELETED, files moved to void."""
        content = b"trash test doc"
        _write_file(user_root, "incoming/doc.pdf", content)

        watcher = DocumentWatcherV2(
            root=user_root,
            db=db,
            service_url=mock_service.url,
        )

        # Cycle 1: detect and launch processing
        await watcher.run_cycle()
        await watcher.shutdown()

        # Simulate user moving file to trash: copy with same hash to trash
        _write_file(user_root, "trash/doc.pdf", content)

        # Cycle 2-3: detect trash, trigger deletion
        await watcher.run_cycle()
        await watcher.run_cycle()

        records = await db.get_snapshot()
        deleted = [r for r in records if r.state == State.IS_DELETED]
        assert len(deleted) >= 1

    @pytest.mark.asyncio
    async def test_processed_file_in_trash_triggers_deletion(self, user_root, db, mock_service):
        """Processed file (matched by hash) placed in trash/ → record deleted."""
        content = b"trash processed file test"
        _write_file(user_root, "incoming/doc.pdf", content)

        watcher = DocumentWatcherV2(
            root=user_root,
            db=db,
            service_url=mock_service.url,
            context_field_names={"work": ["context", "date", "type", "sender", "topic"]},
        )

        # Cycle 1: detect, process
        await watcher.run_cycle()
        await watcher.shutdown()

        # Cycle 2: ingest .output → IS_COMPLETE, move to sorted/
        await watcher.run_cycle()

        records = await db.get_snapshot()
        assert len(records) == 1
        assert records[0].state == State.IS_COMPLETE

        # Get the processed file content (in sorted/) to place in trash
        assigned = mock_service.response_metadata["assigned_filename"]
        sorted_file = user_root / "sorted" / "work" / assigned
        assert sorted_file.exists()
        processed_content = sorted_file.read_bytes()

        # User places the processed file in trash/
        _write_file(user_root, "trash/doc.pdf", processed_content)

        # Cycle 3-4: detect trash entry (matched via hash), trigger deletion
        await watcher.run_cycle()
        await watcher.run_cycle()

        records = await db.get_snapshot()
        deleted = [r for r in records if r.state == State.IS_DELETED]
        assert len(deleted) >= 1


# ---------------------------------------------------------------------------
# Sidecar read
# ---------------------------------------------------------------------------

class TestSidecarRead:
    @pytest.mark.asyncio
    async def test_sidecar_populates_metadata(self, user_root, db, mock_service):
        """Sidecar JSON read properly populates record context/metadata/assigned_filename."""
        content = b"sidecar test doc"
        _write_file(user_root, "incoming/doc.pdf", content)

        watcher = DocumentWatcherV2(
            root=user_root,
            db=db,
            service_url=mock_service.url,
        )

        # Cycle 1: detect, launch processing (creates .output + sidecar)
        await watcher.run_cycle()
        await watcher.shutdown()

        # Cycle 2: detect .output, read sidecar, reconcile
        await watcher.run_cycle()

        records = await db.get_snapshot()
        assert len(records) == 1
        r = records[0]
        assert r.context == "work"
        assert r.metadata is not None
        assert r.assigned_filename == mock_service.response_metadata["assigned_filename"]


# ---------------------------------------------------------------------------
# No changes cycle
# ---------------------------------------------------------------------------

class TestNoChanges:
    @pytest.mark.asyncio
    async def test_empty_cycle_no_errors(self, user_root, db, mock_service):
        """Cycle with no files → no errors, no DB changes."""
        watcher = DocumentWatcherV2(
            root=user_root,
            db=db,
            service_url=mock_service.url,
        )

        await watcher.run_cycle()

        records = await db.get_snapshot()
        assert len(records) == 0

    @pytest.mark.asyncio
    async def test_idempotent_cycles(self, user_root, db, mock_service):
        """Running extra cycles after processing doesn't create duplicates."""
        content = b"idempotent test"
        _write_file(user_root, "incoming/doc.pdf", content)

        watcher = DocumentWatcherV2(
            root=user_root,
            db=db,
            service_url=mock_service.url,
        )

        # Run 3 cycles to fully process
        await watcher.run_cycle()
        await watcher.shutdown()
        await watcher.run_cycle()
        await watcher.run_cycle()

        # Run 2 more idle cycles
        await watcher.run_cycle()
        await watcher.run_cycle()

        records = await db.get_snapshot()
        assert len(records) == 1
        # Service called exactly once
        assert len(mock_service.calls) == 1


# ---------------------------------------------------------------------------
# context_field_names_from_sorter
# ---------------------------------------------------------------------------

class TestContextFieldNamesBridge:
    def test_extracts_field_names(self):
        """Bridge function extracts field names from context manager."""

        class FakeContext:
            def __init__(self, field_names):
                self.field_names = field_names

        class FakeManager:
            def __init__(self):
                self.contexts = {
                    "work": FakeContext(["context", "date", "type", "sender", "topic"]),
                    "personal": FakeContext(["context", "date", "category"]),
                }

        result = context_field_names_from_sorter(FakeManager())
        assert result == {
            "work": ["context", "date", "type", "sender", "topic"],
            "personal": ["context", "date", "category"],
        }

    def test_empty_contexts(self):
        """Empty context manager returns empty dict."""

        class FakeManager:
            def __init__(self):
                self.contexts = {}

        result = context_field_names_from_sorter(FakeManager())
        assert result == {}


# ---------------------------------------------------------------------------
# _read_sidecar
# ---------------------------------------------------------------------------

class TestReadSidecar:
    def test_reads_valid_sidecar(self, user_root):
        """Valid sidecar JSON read correctly."""
        sidecar_data = {"context": "work", "metadata": {"date": "2025-01-15"}}
        sidecar_path = user_root / ".output" / "uuid-123.meta.json"
        sidecar_path.write_text(json.dumps(sidecar_data))

        watcher = DocumentWatcherV2(
            root=user_root,
            db=None,  # Not used for sidecar reading
            service_url="http://unused",
        )

        result = watcher._read_sidecar(".output/uuid-123")
        assert result == sidecar_data

    def test_missing_sidecar_returns_empty(self, user_root):
        """Missing sidecar file returns empty dict."""
        watcher = DocumentWatcherV2(
            root=user_root,
            db=None,
            service_url="http://unused",
        )

        result = watcher._read_sidecar(".output/nonexistent")
        assert result == {}

    def test_invalid_json_returns_empty(self, user_root):
        """Invalid JSON in sidecar returns empty dict."""
        sidecar_path = user_root / ".output" / "bad.meta.json"
        sidecar_path.write_text("not valid json {{{")

        watcher = DocumentWatcherV2(
            root=user_root,
            db=None,
            service_url="http://unused",
        )

        result = watcher._read_sidecar(".output/bad")
        assert result == {}


# ---------------------------------------------------------------------------
# Collision variant: file in sorted/ with UUID suffix stays stable
# ---------------------------------------------------------------------------

class TestCollisionVariant:
    @pytest.mark.asyncio
    async def test_collision_suffix_file_stays_stable(self, user_root, db, mock_service):
        """File with collision UUID suffix in sorted/ should not be moved again.

        Reproduces the bug where sorted/work/file_<uuid>.pdf was detected each
        cycle as not matching sorted/work/file.pdf, causing infinite move loops.
        """
        content = b"collision test doc"
        _write_file(user_root, "incoming/doc.pdf", content)

        watcher = DocumentWatcherV2(
            root=user_root,
            db=db,
            service_url=mock_service.url,
            context_field_names={"work": ["context", "date", "type", "sender", "topic"]},
        )

        # Cycle 1: detect → process → reconcile → move to archive
        await watcher.run_cycle()
        await watcher.shutdown()

        # Cycle 2: detect .output → reconcile → move to sorted/
        await watcher.run_cycle()

        # Now simulate a collision: rename the sorted file to have a UUID suffix
        # (as _move_file does when dest exists)
        assigned = mock_service.response_metadata["assigned_filename"]
        sorted_file = user_root / "sorted" / "work" / assigned
        if sorted_file.exists():
            collision_name = sorted_file.stem + "_e3ca2c9b" + sorted_file.suffix
            collision_file = sorted_file.parent / collision_name
            sorted_file.rename(collision_file)

            # Cycle 3: detect the rename (removal + addition)
            await watcher.run_cycle()

            # Cycle 4: should be stable — no more moves
            await watcher.run_cycle()

            # File should still be at the collision path, not moved again
            assert collision_file.exists(), (
                "Collision-suffixed file should stay in place"
            )

            # DB record should be IS_COMPLETE
            records = await db.get_snapshot()
            complete = [r for r in records if r.state == State.IS_COMPLETE]
            assert len(complete) == 1

            # No target_path set (nothing to move)
            assert complete[0].target_path is None

    @pytest.mark.asyncio
    async def test_multiple_cycles_no_oscillation(self, user_root, db, mock_service):
        """Multiple cycles after collision should not produce new filesystem changes."""
        content = b"oscillation test doc"
        _write_file(user_root, "incoming/doc.pdf", content)

        watcher = DocumentWatcherV2(
            root=user_root,
            db=db,
            service_url=mock_service.url,
            context_field_names={"work": ["context", "date", "type", "sender", "topic"]},
        )

        # Process fully
        await watcher.run_cycle()
        await watcher.shutdown()
        await watcher.run_cycle()

        # Simulate collision
        assigned = mock_service.response_metadata["assigned_filename"]
        sorted_file = user_root / "sorted" / "work" / assigned
        if sorted_file.exists():
            collision_name = sorted_file.stem + "_abcd1234" + sorted_file.suffix
            collision_file = sorted_file.parent / collision_name
            sorted_file.rename(collision_file)

        # Run 3 more cycles — should stabilize after first one detects rename
        await watcher.run_cycle()
        await watcher.run_cycle()
        await watcher.run_cycle()

        # Service should only have been called once (original processing)
        assert len(mock_service.calls) == 1

        # Only 1 record, IS_COMPLETE
        records = await db.get_snapshot()
        assert len(records) == 1
        assert records[0].state == State.IS_COMPLETE


# ---------------------------------------------------------------------------
# User rename in sorted/
# ---------------------------------------------------------------------------

class TestUserRename:
    @pytest.mark.asyncio
    async def test_user_rename_preserved(self, user_root, db, mock_service):
        """User renames a file in sorted/ — filename is NOT reset to assigned_filename."""
        content = b"rename test document"
        _write_file(user_root, "incoming/report.pdf", content)

        watcher = DocumentWatcherV2(
            root=user_root,
            db=db,
            service_url=mock_service.url,
            context_field_names={"work": ["context", "date", "type", "sender", "topic"]},
        )

        # Cycle 1: incoming → archive + .output
        await watcher.run_cycle()
        await watcher.shutdown()

        # Cycle 2: .output → sorted/
        await watcher.run_cycle()

        assigned = mock_service.response_metadata["assigned_filename"]
        sorted_file = user_root / "sorted" / "work" / assigned
        assert sorted_file.exists()

        # User renames the file
        user_name = "my-custom-name.pdf"
        renamed_file = sorted_file.parent / user_name
        sorted_file.rename(renamed_file)
        assert renamed_file.exists()

        # Cycle 3: detect rename (removal + addition)
        await watcher.run_cycle()

        # Cycle 4: should be stable
        await watcher.run_cycle()

        # File stays at user-chosen name
        assert renamed_file.exists(), "User-renamed file should stay in place"
        assert not sorted_file.exists(), "Original assigned filename should not reappear"

        # DB record reflects the new name
        records = await db.get_snapshot()
        complete = [r for r in records if r.state == State.IS_COMPLETE]
        assert len(complete) == 1
        assert complete[0].assigned_filename == user_name
        assert complete[0].target_path is None

    @pytest.mark.asyncio
    async def test_user_move_after_rename_preserves_name(self, user_root, db, mock_service):
        """User renames a file, then moves it to a different context — name stays."""
        content = b"move after rename test"
        _write_file(user_root, "incoming/invoice.pdf", content)

        # Create a second context directory
        (user_root / "sorted" / "personal").mkdir(parents=True, exist_ok=True)

        watcher = DocumentWatcherV2(
            root=user_root,
            db=db,
            service_url=mock_service.url,
            context_field_names={
                "work": ["context", "date", "type", "sender", "topic"],
                "personal": ["context", "date", "type", "sender", "topic"],
            },
        )

        # Cycle 1+2: incoming → sorted/work/assigned-name.pdf
        await watcher.run_cycle()
        await watcher.shutdown()
        await watcher.run_cycle()

        assigned = mock_service.response_metadata["assigned_filename"]
        sorted_file = user_root / "sorted" / "work" / assigned
        assert sorted_file.exists()

        # Step A: User renames
        user_name = "my-invoice.pdf"
        renamed_file = sorted_file.parent / user_name
        sorted_file.rename(renamed_file)

        # Cycle 3: detect rename
        await watcher.run_cycle()
        assert renamed_file.exists()

        # Step B: User moves to different context (to change metadata)
        moved_file = user_root / "sorted" / "personal" / user_name
        renamed_file.rename(moved_file)

        # Cycle 4: detect move
        await watcher.run_cycle()

        # Cycle 5: should be stable
        await watcher.run_cycle()

        # File stays at user-chosen name in new context
        assert moved_file.exists(), "Moved file should stay in place"
        assert not renamed_file.exists(), "Old location should be empty"

        # DB record reflects new name and new context
        records = await db.get_snapshot()
        complete = [r for r in records if r.state == State.IS_COMPLETE]
        assert len(complete) == 1
        assert complete[0].assigned_filename == user_name
        assert complete[0].context == "personal"
        assert complete[0].target_path is None


# ---------------------------------------------------------------------------
# Background processing behavior
# ---------------------------------------------------------------------------

class TestBackgroundProcessing:
    @pytest.mark.asyncio
    async def test_processing_is_non_blocking(self, user_root, db, mock_service):
        """run_cycle returns quickly while processing runs in background."""
        content = b"non-blocking test doc"
        _write_file(user_root, "incoming/doc.pdf", content)

        watcher = DocumentWatcherV2(
            root=user_root,
            db=db,
            service_url=mock_service.url,
        )

        # Cycle 1: detect + launch background processing
        await watcher.run_cycle()

        # Processing task is in-flight (not yet completed)
        assert len(watcher._in_flight) == 1

        # Wait for background tasks to finish
        await watcher.shutdown()

        # Now in-flight is empty and service was called
        assert len(watcher._in_flight) == 0
        assert len(mock_service.calls) == 1

    @pytest.mark.asyncio
    async def test_in_flight_not_relaunched(self, user_root, db, mock_service):
        """Second cycle skips records already being processed."""
        content = b"in-flight test doc"
        _write_file(user_root, "incoming/doc.pdf", content)

        # Use a slow mock: block processing until we release it
        gate = asyncio.Event()
        original_process = mock_service.handle_process

        async def slow_process(request):
            await gate.wait()
            return await original_process(request)

        # Patch the handler
        mock_service.handle_process = slow_process
        app = mock_service.make_app()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        slow_url = f"http://127.0.0.1:{port}"

        try:
            watcher = DocumentWatcherV2(
                root=user_root,
                db=db,
                service_url=slow_url,
            )

            # Cycle 1: launch processing (blocked by gate)
            await watcher.run_cycle()
            assert len(watcher._in_flight) == 1

            # Cycle 2: should NOT re-launch (already in-flight)
            await watcher.run_cycle()
            assert len(watcher._in_flight) == 1

            # Release the gate and wait
            gate.set()
            await watcher.shutdown()
            assert len(watcher._in_flight) == 0
        finally:
            await runner.cleanup()

    @pytest.mark.asyncio
    async def test_in_flight_cleared_on_error(self, user_root, db, mock_service):
        """Record ID removed from _in_flight even when processing fails."""
        content = b"error clear test doc"
        _write_file(user_root, "incoming/doc.pdf", content)

        mock_service.fail_status = 500
        watcher = DocumentWatcherV2(
            root=user_root,
            db=db,
            service_url=mock_service.url,
            processor_timeout=5.0,
        )
        watcher.processor.max_retries = 0

        await watcher.run_cycle()
        await watcher.shutdown()

        # In-flight should be cleared despite error
        assert len(watcher._in_flight) == 0

    @pytest.mark.asyncio
    async def test_semaphore_limits_concurrency(self, user_root, db, mock_service):
        """max_concurrent=2 limits parallel processing tasks."""
        for i in range(4):
            _write_file(user_root, f"incoming/doc{i}.pdf", f"content {i}".encode())

        concurrent_count = 0
        max_seen = 0
        gate = asyncio.Event()

        original_process = mock_service.handle_process

        async def counting_process(request):
            nonlocal concurrent_count, max_seen
            concurrent_count += 1
            max_seen = max(max_seen, concurrent_count)
            await gate.wait()
            result = await original_process(request)
            concurrent_count -= 1
            return result

        mock_service.handle_process = counting_process
        app = mock_service.make_app()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        counting_url = f"http://127.0.0.1:{port}"

        try:
            watcher = DocumentWatcherV2(
                root=user_root,
                db=db,
                service_url=counting_url,
                max_concurrent=2,
            )

            # Launch all 4 processing tasks
            await watcher.run_cycle()

            # Give tasks time to hit the semaphore
            await asyncio.sleep(0.1)

            # Release gate and let them all complete
            gate.set()
            await watcher.shutdown()

            # At most 2 were running concurrently
            assert max_seen <= 2
            assert len(mock_service.calls) == 4
        finally:
            await runner.cleanup()


# ---------------------------------------------------------------------------
# Config reload integration
# ---------------------------------------------------------------------------

class TestConfigReload:
    @pytest.mark.asyncio
    async def test_config_changed_triggers_reload(self, user_root, db, mock_service):
        """Config file change detected → reload_config called, _pending_full_scan set."""
        import yaml
        # Set up initial context via sorted/
        ctx_dir = user_root / "sorted" / "work"
        ctx_dir.mkdir(parents=True, exist_ok=True)
        (ctx_dir / "context.yaml").write_text(yaml.dump({
            "name": "work",
            "filename": "{context}-{type}-{date}",
            "fields": {"type": {"instructions": "Type"}},
        }))

        context_manager = SorterContextManager(user_root, "testuser")
        context_manager.load()

        watcher = DocumentWatcherV2(
            root=user_root,
            db=db,
            service_url=mock_service.url,
            context_field_names={"work": ["context", "date", "type"]},
            context_manager=context_manager,
        )

        # First cycle: full scan, starts observer
        await watcher.run_cycle()

        # Modify config file (triggers inotify)
        (ctx_dir / "context.yaml").write_text(yaml.dump({
            "name": "work",
            "filename": "{context}-{type}-{date}-{sender}",
            "fields": {
                "type": {"instructions": "Type"},
                "sender": {"instructions": "Sender"},
            },
        }))
        await asyncio.sleep(0.1)

        # Incremental cycle detects config change
        await watcher.run_cycle(full_scan=False)

        # Config should be reloaded
        assert "sender" in watcher.context_field_names.get("work", [])
        # Full scan should be pending
        assert watcher._pending_full_scan is True

    @pytest.mark.asyncio
    async def test_config_files_not_in_changes(self, user_root, db, mock_service):
        """Config files should not appear as document changes."""
        import yaml
        ctx_dir = user_root / "sorted" / "work"
        ctx_dir.mkdir(parents=True, exist_ok=True)
        (ctx_dir / "context.yaml").write_text(yaml.dump({
            "name": "work",
            "filename": "{context}-{date}",
            "fields": {},
        }))
        # Also add a real document
        _write_file(user_root, "incoming/doc.pdf", b"test content")

        watcher = DocumentWatcherV2(
            root=user_root,
            db=db,
            service_url=mock_service.url,
        )

        # Full scan
        await watcher.run_cycle()

        # Check DB — no record should exist for context.yaml
        records = await db.get_snapshot()
        for r in records:
            assert "context.yaml" not in r.original_filename

    @pytest.mark.asyncio
    async def test_smartfolders_yaml_reload(self, user_root, db, mock_service):
        """Smart folders reloaded from sorted/ after config change."""
        import yaml
        ctx_dir = user_root / "sorted" / "work"
        ctx_dir.mkdir(parents=True, exist_ok=True)
        (ctx_dir / "context.yaml").write_text(yaml.dump({
            "name": "work",
            "filename": "{context}-{type}-{date}",
            "fields": {"type": {"instructions": "Type"}},
        }))

        context_manager = SorterContextManager(user_root, "testuser")
        context_manager.load()

        watcher = DocumentWatcherV2(
            root=user_root,
            db=db,
            service_url=mock_service.url,
            context_field_names={"work": ["context", "date", "type"]},
            context_manager=context_manager,
        )

        # Initially no smart folders
        assert watcher.smart_folder_reconciler is None

        # First cycle starts observer
        await watcher.run_cycle()

        # Add smartfolders.yaml
        (ctx_dir / "smartfolders.yaml").write_text(yaml.dump({
            "smart_folders": {
                "invoices": {
                    "context": "work",
                    "condition": {"field": "type", "value": "Invoice"},
                },
            },
        }))
        await asyncio.sleep(0.1)

        # Incremental cycle triggers reload
        await watcher.run_cycle(full_scan=False)

        # Smart folder reconciler should now be set
        assert watcher.smart_folder_reconciler is not None


# ---------------------------------------------------------------------------
# Generated candidates & clues — sorted/ config path
# ---------------------------------------------------------------------------

CONTEXT_YAML_WITH_CANDIDATES = {
    "name": "work",
    "filename": "{context}-{type}-{date}-{sender}",
    "fields": {
        "type": {"instructions": "Document type"},
        "sender": {
            "instructions": "Sender",
            "candidates": [
                "Acme Corp",
                {"name": "Beta Inc", "clues": ["Uses BETA format"], "allow_new_clues": True},
            ],
            "allow_new_candidates": True,
        },
    },
}


def _setup_sorted_context(root, context_yaml=None):
    """Create sorted/{name}/context.yaml and return the context manager."""
    ctx_data = context_yaml or CONTEXT_YAML_WITH_CANDIDATES
    ctx_dir = root / "sorted" / ctx_data["name"]
    ctx_dir.mkdir(parents=True, exist_ok=True)
    (ctx_dir / "context.yaml").write_text(yaml.dump(ctx_data))
    mgr = SorterContextManager(root, "testuser")
    mgr.load()
    return mgr



class TestGeneratedDataSorted:
    """Generated data tests using sorted/{context}/ config layout."""

    def test_record_new_item_creates_generated_file(self, user_root):
        mgr = _setup_sorted_context(user_root)

        assert mgr.record_new_item("work", "sender", "New Corp") is True

        gen_path = user_root / "sorted" / "work" / "generated.yaml"
        assert gen_path.exists()
        data = yaml.safe_load(gen_path.read_text())
        assert "New Corp" in data["fields"]["sender"]["candidates"]

    def test_record_new_item_duplicate_rejected(self, user_root):
        mgr = _setup_sorted_context(user_root)
        mgr.record_new_item("work", "sender", "New Corp")

        assert mgr.record_new_item("work", "sender", "New Corp") is False

    def test_record_new_item_existing_base_rejected(self, user_root):
        mgr = _setup_sorted_context(user_root)

        assert mgr.record_new_item("work", "sender", "Acme Corp") is False

    def test_record_new_clue_adds_to_generated(self, user_root):
        mgr = _setup_sorted_context(user_root)

        assert mgr.record_new_clue("work", "sender", "Beta Inc", "New clue") is True

        gen_path = user_root / "sorted" / "work" / "generated.yaml"
        data = yaml.safe_load(gen_path.read_text())
        entry = next(
            c for c in data["fields"]["sender"]["candidates"]
            if isinstance(c, dict) and c.get("name") == "Beta Inc"
        )
        assert "New clue" in entry["clues"]

    def test_record_new_clue_duplicate_rejected(self, user_root):
        mgr = _setup_sorted_context(user_root)
        mgr.record_new_clue("work", "sender", "Beta Inc", "New clue")

        assert mgr.record_new_clue("work", "sender", "Beta Inc", "New clue") is False

    def test_record_new_clue_existing_base_clue_rejected(self, user_root):
        mgr = _setup_sorted_context(user_root)

        assert mgr.record_new_clue("work", "sender", "Beta Inc", "Uses BETA format") is False

    def test_record_new_clue_simple_string_candidate_rejected(self, user_root):
        mgr = _setup_sorted_context(user_root)

        # "Acme Corp" is a simple string candidate — no clues allowed
        assert mgr.record_new_clue("work", "sender", "Acme Corp", "Some clue") is False

    def test_record_new_clue_no_allow_new_clues_rejected(self, user_root):
        ctx = {
            "name": "work",
            "filename": "{context}-{date}-{sender}",
            "fields": {
                "sender": {
                    "instructions": "Sender",
                    "candidates": [
                        {"name": "Beta Inc", "clues": ["Existing"], "allow_new_clues": False},
                    ],
                },
            },
        }
        mgr = _setup_sorted_context(user_root, ctx)

        assert mgr.record_new_clue("work", "sender", "Beta Inc", "New") is False

    def test_is_new_item_true_for_unknown(self, user_root):
        mgr = _setup_sorted_context(user_root)

        assert mgr.is_new_item("work", "sender", "Unknown Corp") is True

    def test_is_new_item_false_for_base(self, user_root):
        mgr = _setup_sorted_context(user_root)

        assert mgr.is_new_item("work", "sender", "Acme Corp") is False

    def test_is_new_item_false_for_generated(self, user_root):
        mgr = _setup_sorted_context(user_root)
        mgr.record_new_item("work", "sender", "New Corp")

        assert mgr.is_new_item("work", "sender", "New Corp") is False

    def test_is_new_item_false_for_no_candidates_field(self, user_root):
        mgr = _setup_sorted_context(user_root)

        # "type" has no candidates
        assert mgr.is_new_item("work", "type", "Invoice") is False

    def test_is_new_item_false_for_allow_new_candidates_false(self, user_root):
        ctx = {
            "name": "work",
            "filename": "{context}-{date}-{sender}",
            "fields": {
                "sender": {
                    "instructions": "Sender",
                    "candidates": ["Acme Corp"],
                    "allow_new_candidates": False,
                },
            },
        }
        mgr = _setup_sorted_context(user_root, ctx)

        assert mgr.is_new_item("work", "sender", "New Corp") is False

    def test_get_context_for_api_merges_generated(self, user_root):
        mgr = _setup_sorted_context(user_root)
        mgr.record_new_item("work", "sender", "New Corp")
        mgr.record_new_clue("work", "sender", "Beta Inc", "New clue")

        ctx = mgr.get_context_for_api("work")
        candidates = ctx["fields"]["sender"]["candidates"]
        names = [c if isinstance(c, str) else c.get("name") for c in candidates]
        assert "Acme Corp" in names
        assert "Beta Inc" in names
        assert "New Corp" in names

        beta = next(c for c in candidates if isinstance(c, dict) and c.get("name") == "Beta Inc")
        assert "Uses BETA format" in beta["clues"]
        assert "New clue" in beta["clues"]

    def test_generated_survives_reload(self, user_root):
        mgr = _setup_sorted_context(user_root)
        mgr.record_new_item("work", "sender", "New Corp")

        # Fresh manager loads generated from disk
        mgr2 = SorterContextManager(user_root, "testuser")
        mgr2.load()

        assert mgr2.is_new_item("work", "sender", "New Corp") is False
        ctx = mgr2.get_context_for_api("work")
        names = [c if isinstance(c, str) else c.get("name") for c in ctx["fields"]["sender"]["candidates"]]
        assert "New Corp" in names

    def test_contexts_for_api_from_sorter_merges_generated(self, user_root):
        mgr = _setup_sorted_context(user_root)
        mgr.record_new_item("work", "sender", "New Corp")

        api_ctxs = contexts_for_api_from_sorter(mgr)
        assert len(api_ctxs) == 1
        candidates = api_ctxs[0]["fields"]["sender"]["candidates"]
        names = [c if isinstance(c, str) else c.get("name") for c in candidates]
        assert "New Corp" in names

    def test_generated_file_empty_after_no_data(self, user_root):
        """If generated data is empty, file is removed."""
        mgr = _setup_sorted_context(user_root)
        gen_path = user_root / "sorted" / "work" / "generated.yaml"

        # Force save with no data — file should not exist
        mgr._save_generated_file("work")
        assert not gen_path.exists()



# ---------------------------------------------------------------------------
# End-to-end: new_clues from service → generated file (requires DB)
# ---------------------------------------------------------------------------

class TestNewCluesEndToEnd:
    @pytest.mark.asyncio
    async def test_new_clues_recorded_sorted(self, user_root, db, mock_service):
        """Service returns new_clues → record_new_item/record_new_clue called, generated.yaml written."""
        ctx_dir = user_root / "sorted" / "work"
        ctx_dir.mkdir(parents=True, exist_ok=True)
        (ctx_dir / "context.yaml").write_text(yaml.dump(CONTEXT_YAML_WITH_CANDIDATES))

        context_manager = SorterContextManager(user_root, "testuser")
        context_manager.load()

        mock_service.response_new_clues = {
            "sender": {"value": "New Corp", "clue": None},
        }

        content = b"new clues test doc"
        _write_file(user_root, "incoming/doc.pdf", content)

        watcher = DocumentWatcherV2(
            root=user_root,
            db=db,
            service_url=mock_service.url,
            context_field_names=context_field_names_from_sorter(context_manager),
            context_folders=context_folders_from_sorter(context_manager),
            contexts_for_api=contexts_for_api_from_sorter(context_manager),
            context_manager=context_manager,
        )

        await watcher.run_cycle()
        await watcher.shutdown()

        # Generated file should exist with the new candidate
        gen_path = ctx_dir / "generated.yaml"
        assert gen_path.exists()
        data = yaml.safe_load(gen_path.read_text())
        assert "New Corp" in data["fields"]["sender"]["candidates"]

    @pytest.mark.asyncio
    async def test_new_clues_with_clue_recorded_sorted(self, user_root, db, mock_service):
        """Service returns new_clues with a clue for existing candidate → clue stored."""
        ctx_dir = user_root / "sorted" / "work"
        ctx_dir.mkdir(parents=True, exist_ok=True)
        (ctx_dir / "context.yaml").write_text(yaml.dump(CONTEXT_YAML_WITH_CANDIDATES))

        context_manager = SorterContextManager(user_root, "testuser")
        context_manager.load()

        mock_service.response_new_clues = {
            "sender": {"value": "Beta Inc", "clue": "Sends quarterly reports"},
        }

        content = b"clue test doc"
        _write_file(user_root, "incoming/doc.pdf", content)

        watcher = DocumentWatcherV2(
            root=user_root,
            db=db,
            service_url=mock_service.url,
            context_field_names=context_field_names_from_sorter(context_manager),
            context_folders=context_folders_from_sorter(context_manager),
            contexts_for_api=contexts_for_api_from_sorter(context_manager),
            context_manager=context_manager,
        )

        await watcher.run_cycle()
        await watcher.shutdown()

        gen_path = ctx_dir / "generated.yaml"
        assert gen_path.exists()
        data = yaml.safe_load(gen_path.read_text())
        entry = next(
            c for c in data["fields"]["sender"]["candidates"]
            if isinstance(c, dict) and c.get("name") == "Beta Inc"
        )
        assert "Sends quarterly reports" in entry["clues"]

    @pytest.mark.asyncio
    async def test_new_clues_survives_config_reload(self, user_root, db, mock_service):
        """Generated data persists across config reload."""
        ctx_dir = user_root / "sorted" / "work"
        ctx_dir.mkdir(parents=True, exist_ok=True)
        (ctx_dir / "context.yaml").write_text(yaml.dump(CONTEXT_YAML_WITH_CANDIDATES))

        context_manager = SorterContextManager(user_root, "testuser")
        context_manager.load()

        mock_service.response_new_clues = {
            "sender": {"value": "New Corp", "clue": None},
        }

        content = b"reload test doc"
        _write_file(user_root, "incoming/doc.pdf", content)

        watcher = DocumentWatcherV2(
            root=user_root,
            db=db,
            service_url=mock_service.url,
            context_field_names=context_field_names_from_sorter(context_manager),
            context_folders=context_folders_from_sorter(context_manager),
            contexts_for_api=contexts_for_api_from_sorter(context_manager),
            context_manager=context_manager,
        )

        await watcher.run_cycle()
        await watcher.shutdown()

        # Trigger config reload
        watcher.reload_config()

        # Generated data should still be present after reload
        assert context_manager.is_new_item("work", "sender", "New Corp") is False
        ctx = context_manager.get_context_for_api("work")
        names = [c if isinstance(c, str) else c.get("name") for c in ctx["fields"]["sender"]["candidates"]]
        assert "New Corp" in names

    @pytest.mark.asyncio
    async def test_no_new_clues_no_generated_file(self, user_root, db, mock_service):
        """When service returns no new_clues, no generated file is created."""
        ctx_dir = user_root / "sorted" / "work"
        ctx_dir.mkdir(parents=True, exist_ok=True)
        (ctx_dir / "context.yaml").write_text(yaml.dump(CONTEXT_YAML_WITH_CANDIDATES))

        context_manager = SorterContextManager(user_root, "testuser")
        context_manager.load()

        # No new_clues in response (default)
        content = b"no clues doc"
        _write_file(user_root, "incoming/doc.pdf", content)

        watcher = DocumentWatcherV2(
            root=user_root,
            db=db,
            service_url=mock_service.url,
            context_field_names=context_field_names_from_sorter(context_manager),
            context_folders=context_folders_from_sorter(context_manager),
            contexts_for_api=contexts_for_api_from_sorter(context_manager),
            context_manager=context_manager,
        )

        await watcher.run_cycle()
        await watcher.shutdown()

        gen_path = ctx_dir / "generated.yaml"
        assert not gen_path.exists()

    @pytest.mark.asyncio
    async def test_processor_has_context_manager(self, user_root, db, mock_service):
        """Processor receives context_manager from orchestrator."""
        ctx_dir = user_root / "sorted" / "work"
        ctx_dir.mkdir(parents=True, exist_ok=True)
        (ctx_dir / "context.yaml").write_text(yaml.dump(CONTEXT_YAML_WITH_CANDIDATES))

        context_manager = SorterContextManager(user_root, "testuser")
        context_manager.load()

        watcher = DocumentWatcherV2(
            root=user_root,
            db=db,
            service_url=mock_service.url,
            context_manager=context_manager,
        )

        assert watcher.processor.context_manager is context_manager

    @pytest.mark.asyncio
    async def test_processor_context_manager_updated_on_reload(self, user_root, db, mock_service):
        """reload_config updates processor.context_manager."""
        ctx_dir = user_root / "sorted" / "work"
        ctx_dir.mkdir(parents=True, exist_ok=True)
        (ctx_dir / "context.yaml").write_text(yaml.dump(CONTEXT_YAML_WITH_CANDIDATES))

        context_manager = SorterContextManager(user_root, "testuser")
        context_manager.load()

        watcher = DocumentWatcherV2(
            root=user_root,
            db=db,
            service_url=mock_service.url,
            context_field_names=context_field_names_from_sorter(context_manager),
            context_folders=context_folders_from_sorter(context_manager),
            contexts_for_api=contexts_for_api_from_sorter(context_manager),
            context_manager=context_manager,
        )

        watcher.reload_config()

        assert watcher.processor.context_manager is context_manager


# ---------------------------------------------------------------------------
# Multi-user isolation
# ---------------------------------------------------------------------------

def _make_user_root(base: Path, name: str) -> Path:
    """Create a standard directory structure for a named user."""
    root = base / name
    root.mkdir()
    for d in ("archive", "incoming", "reviewed", "processed",
              "trash", ".output", "sorted", "error", "void", "missing"):
        (root / d).mkdir()
    return root


class TestMultiUser:
    """Verify that two users sharing one DB don't interfere with each other."""

    @pytest.mark.asyncio
    async def test_two_users_pdf_isolation(self, tmp_path, db, mock_service):
        """Each user processes a PDF; each sees only their own record."""
        alice_root = _make_user_root(tmp_path, "alice")
        bob_root = _make_user_root(tmp_path, "bob")

        _write_file(alice_root, "incoming/alice-invoice.pdf", b"alice content")
        _write_file(bob_root, "incoming/bob-receipt.pdf", b"bob content")

        alice_watcher = DocumentWatcherV2(
            root=alice_root, db=db, service_url=mock_service.url,
            name="alice",
        )
        bob_watcher = DocumentWatcherV2(
            root=bob_root, db=db, service_url=mock_service.url,
            name="bob",
        )

        # Cycle 1: detect + process for both
        await alice_watcher.run_cycle()
        await alice_watcher.shutdown()
        await bob_watcher.run_cycle()
        await bob_watcher.shutdown()

        # Cycle 2: pick up .output results
        await alice_watcher.run_cycle()
        await bob_watcher.run_cycle()

        # Each user sees exactly 1 record via filtered snapshot
        alice_records = await db.get_snapshot("alice")
        bob_records = await db.get_snapshot("bob")
        assert len(alice_records) == 1
        assert len(bob_records) == 1

        assert alice_records[0].original_filename == "alice-invoice.pdf"
        assert alice_records[0].username == "alice"
        assert bob_records[0].original_filename == "bob-receipt.pdf"
        assert bob_records[0].username == "bob"

        # Global snapshot has both
        all_records = await db.get_snapshot()
        assert len(all_records) == 2

    @pytest.mark.asyncio
    async def test_two_users_pdf_and_audio(self, tmp_path, db, mock_service):
        """Alice processes a PDF, Bob drops an audio file (errors without STT).

        Verifies isolation across different processing paths.
        """
        alice_root = _make_user_root(tmp_path, "alice")
        bob_root = _make_user_root(tmp_path, "bob")

        _write_file(alice_root, "incoming/alice-doc.pdf", b"alice pdf")
        _write_file(bob_root, "incoming/bob-meeting.m4a", b"fake audio")

        alice_watcher = DocumentWatcherV2(
            root=alice_root, db=db, service_url=mock_service.url,
            name="alice",
        )
        bob_watcher = DocumentWatcherV2(
            root=bob_root, db=db, service_url=mock_service.url,
            name="bob",
            # No stt_url → audio will error
        )

        # Cycle 1: detect + process
        await alice_watcher.run_cycle()
        await alice_watcher.shutdown()
        await bob_watcher.run_cycle()
        await bob_watcher.shutdown()

        # Cycle 2: pick up results
        await alice_watcher.run_cycle()
        await bob_watcher.run_cycle()

        alice_records = await db.get_snapshot("alice")
        bob_records = await db.get_snapshot("bob")
        assert len(alice_records) == 1
        assert len(bob_records) == 1

        # Alice's PDF should complete successfully
        assert alice_records[0].original_filename == "alice-doc.pdf"
        # Bob's audio should error (no STT configured)
        assert bob_records[0].original_filename == "bob-meeting.m4a"
        assert bob_records[0].state == State.HAS_ERROR

        # Neither user's records leak into the other's snapshot
        assert all(r.username == "alice" for r in alice_records)
        assert all(r.username == "bob" for r in bob_records)

    @pytest.mark.asyncio
    async def test_restart_no_cross_contamination(self, tmp_path, db, mock_service):
        """After processing, a simulated restart produces 0 changes per user.

        This is the core regression test for the cross-user contamination bug:
        without username filtering, user A's step1 would generate REMOVAL events
        for user B's archive paths (they don't exist on A's filesystem), causing
        an infinite loop of modifications on every restart.
        """
        alice_root = _make_user_root(tmp_path, "alice")
        bob_root = _make_user_root(tmp_path, "bob")

        _write_file(alice_root, "incoming/alice-doc.pdf", b"alice content")
        _write_file(bob_root, "incoming/bob-doc.pdf", b"bob content")

        alice_w1 = DocumentWatcherV2(
            root=alice_root, db=db, service_url=mock_service.url,
            name="alice",
        )
        bob_w1 = DocumentWatcherV2(
            root=bob_root, db=db, service_url=mock_service.url,
            name="bob",
        )

        # Process to completion
        for _ in range(3):
            await alice_w1.run_cycle()
            await alice_w1.shutdown()
            await bob_w1.run_cycle()
            await bob_w1.shutdown()

        # Verify both completed
        alice_records = await db.get_snapshot("alice")
        bob_records = await db.get_snapshot("bob")
        assert len(alice_records) == 1
        assert len(bob_records) == 1

        # Capture state before restart
        alice_state_before = (alice_records[0].state, len(alice_records[0].source_paths))
        bob_state_before = (bob_records[0].state, len(bob_records[0].source_paths))

        # Simulate restart: new watcher instances (fresh FilesystemDetector,
        # no _previous_state), same DB
        alice_w2 = DocumentWatcherV2(
            root=alice_root, db=db, service_url=mock_service.url,
            name="alice",
        )
        bob_w2 = DocumentWatcherV2(
            root=bob_root, db=db, service_url=mock_service.url,
            name="bob",
        )

        # Run restart cycles — should stabilize quickly
        for _ in range(3):
            await alice_w2.run_cycle()
            await bob_w2.run_cycle()

        # Verify records are unchanged after restart
        alice_after = await db.get_snapshot("alice")
        bob_after = await db.get_snapshot("bob")
        assert len(alice_after) == 1
        assert len(bob_after) == 1

        alice_state_after = (alice_after[0].state, len(alice_after[0].source_paths))
        bob_state_after = (bob_after[0].state, len(bob_after[0].source_paths))
        assert alice_state_after == alice_state_before
        assert bob_state_after == bob_state_before

        # No cross-contamination: alice's paths should not appear on bob's records
        for r in bob_after:
            for pe in r.source_paths:
                assert "alice" not in pe.path
        for r in alice_after:
            for pe in r.source_paths:
                assert "bob" not in pe.path

    @pytest.mark.asyncio
    async def test_stray_detection_isolated(self, tmp_path, db, mock_service):
        """Unknown files in one user's archive don't affect the other user."""
        alice_root = _make_user_root(tmp_path, "alice")
        bob_root = _make_user_root(tmp_path, "bob")

        # Alice has a normal file
        _write_file(alice_root, "incoming/alice-doc.pdf", b"alice content")

        # Bob has an unknown file in archive (stray)
        _write_file(bob_root, "archive/mystery.pdf", b"unknown content")

        alice_watcher = DocumentWatcherV2(
            root=alice_root, db=db, service_url=mock_service.url,
            name="alice",
        )
        bob_watcher = DocumentWatcherV2(
            root=bob_root, db=db, service_url=mock_service.url,
            name="bob",
        )

        await alice_watcher.run_cycle()
        await bob_watcher.run_cycle()

        # Bob's stray file should be moved to error/
        assert not (bob_root / "archive" / "mystery.pdf").exists()
        assert any((bob_root / "error").iterdir())

        # Alice's record should be unaffected
        alice_records = await db.get_snapshot("alice")
        assert len(alice_records) == 1
        assert alice_records[0].original_filename == "alice-doc.pdf"

        # Bob has no records (stray was moved, not tracked)
        bob_records = await db.get_snapshot("bob")
        assert len(bob_records) == 0


# ---------------------------------------------------------------------------
# Step3 completion + re-launch race condition
# ---------------------------------------------------------------------------

class TestProcessingRelaunchRace:
    """Tests for the race where step3 completes and _in_flight is cleared
    before the .output file is detected, causing step 7 to re-launch the
    same record.

    The sequence:
    1. Record enters NEEDS_PROCESSING, output_filename=UUID. Step3 launches.
    2. Step3 completes: writes .output/UUID, _in_flight.discard(id).
    3. SAME cycle, step 7: record still has output_filename=UUID (not yet
       ingested), id NOT in _in_flight → re-launched!
    4. Next cycle: first .output/UUID detected, ingested, output_filename=None.
    5. Second step3 finishes, writes .output/UUID AGAIN.
    6. Next cycle: output_filename=None → _is_known returns False → stray!
    """

    @pytest.mark.asyncio
    async def test_relaunch_gap_exists(self, user_root, db, mock_service):
        """After step3 completes, the record is still launchable (output_filename
        set, not in _in_flight).  This gap is the root cause of the re-launch bug.

        The test asserts the gap should NOT exist.  It FAILS on the current code,
        proving the bug.
        """
        _write_file(user_root, "incoming/doc.pdf", b"gap test content")

        watcher = DocumentWatcherV2(
            root=user_root,
            db=db,
            service_url=mock_service.url,
        )

        # Cycle 1: detect incoming → create record (IS_NEW) → reconcile sets
        # output_filename, state=NEEDS_PROCESSING, moves file to archive.
        # Step 7 does NOT launch yet (output_filename was set by reconcile
        # which runs AFTER step 7).
        await watcher.run_cycle()

        # Cycle 2: step 7 finds record with output_filename → launches step3
        await watcher.run_cycle()

        # Wait for step3 to complete (background task finishes, _in_flight cleared)
        await asyncio.sleep(0.5)
        assert len(watcher._in_flight) == 0, "step3 should have completed"
        assert len(mock_service.calls) == 1, "service should have been called once"

        # .output file should exist on disk
        output_files = [
            f for f in (user_root / ".output").iterdir()
            if not f.name.endswith(".meta.json")
        ]
        assert len(output_files) == 1, ".output file should exist"

        # THE BUG: record still has output_filename set in DB, AND is not
        # in _in_flight.  This means step 7 would re-launch it.
        launchable = await db.get_records_with_output_filename(watcher.name)
        not_in_flight = [r for r in launchable if r.id not in watcher._in_flight]

        # This assertion should PASS in correct code (no launchable records
        # after step3 completes).  It FAILS now because output_filename is
        # still set and _in_flight is empty.
        assert len(not_in_flight) == 0, (
            f"After step3 completes, no record should be launchable, but found "
            f"{len(not_in_flight)} record(s) with output_filename set and not "
            f"in _in_flight.  Step 7 would re-launch these."
        )

        await watcher.shutdown()

    @pytest.mark.asyncio
    async def test_relaunch_produces_duplicate_service_call(self, user_root, db, mock_service):
        """Force the race: step3 completes between detection and step 7.

        We simulate this by patching the detector to return no .output changes
        in the cycle after step3 completes (as if detection ran before step3
        wrote the file), while step3 has already completed (output_filename
        set, not in _in_flight).

        The test asserts the service is called only once.  It FAILS on the
        current code because step 7 re-launches, producing a second call.
        """
        _write_file(user_root, "incoming/doc.pdf", b"relaunch test content")

        watcher = DocumentWatcherV2(
            root=user_root,
            db=db,
            service_url=mock_service.url,
        )

        # Cycle 1: detect incoming → create record → reconcile sets output_filename
        await watcher.run_cycle()

        # Cycle 2: step 7 launches step3
        await watcher.run_cycle()

        # Wait for step3 to finish
        await asyncio.sleep(0.5)
        assert len(watcher._in_flight) == 0
        assert len(mock_service.calls) == 1

        # Patch detector.detect to return [] — simulates the case where
        # detection ran before step3 wrote the .output file.  Step3 has
        # already completed, so step 7 will see: output_filename set,
        # not in _in_flight → re-launch.
        original_detect = watcher.detector.detect

        async def detect_no_output(snapshot):
            return []

        watcher.detector.detect = detect_no_output

        # Cycle 3: detection sees nothing → preprocess does nothing →
        # step 7 queries DB and finds record with output_filename → re-launch!
        await watcher.run_cycle()

        # Wait for the second step3 to complete
        await asyncio.sleep(0.5)
        await watcher.shutdown()

        # Restore detector
        watcher.detector.detect = original_detect

        # THE BUG: service was called TWICE for the same document.
        assert len(mock_service.calls) == 1, (
            f"Service was called {len(mock_service.calls)} times — step3 was "
            f"re-launched.  Expected exactly 1 call for a single document."
        )

    @pytest.mark.asyncio
    async def test_relaunch_causes_stray_output(self, user_root, db, mock_service):
        """Full end-to-end: re-launch → second .output → stray → deleted.

        Sequence that triggers the stray:
        1. Step3 #1 completes, writes .output/UUID
        2. Force re-launch (step3 #2), but gate it so it doesn't complete yet
        3. A normal cycle ingests .output/UUID → output_filename = None
        4. Release gate → step3 #2 completes, writes .output/UUID again
        5. Next cycle: output_filename is None → .output/UUID is stray → deleted

        This test asserts no files end up in error/.  Stray .output files are
        deleted (not moved to error/) since we created them ourselves.
        """
        _write_file(user_root, "incoming/doc.pdf", b"stray output test content")

        # Gated mock service: second call blocks until gate is released
        call_count = 0
        gate = asyncio.Event()
        original_process = mock_service.handle_process

        async def gated_process(request):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                await gate.wait()
            return await original_process(request)

        mock_service.handle_process = gated_process
        app = mock_service.make_app()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        gated_url = f"http://127.0.0.1:{port}"

        try:
            watcher = DocumentWatcherV2(
                root=user_root,
                db=db,
                service_url=gated_url,
            )

            # Cycle 1: detect incoming → create record → reconcile sets output_filename
            await watcher.run_cycle()

            # Cycle 2: step 7 launches step3 #1
            await watcher.run_cycle()
            await asyncio.sleep(0.5)
            assert call_count == 1, "step3 #1 should have been called"
            assert len(watcher._in_flight) == 0, "step3 #1 should have completed"

            # Patch detect to return [] → force re-launch (step3 #2)
            original_detect = watcher.detector.detect

            async def detect_no_output(snapshot):
                return []

            watcher.detector.detect = detect_no_output

            # Cycle 3: detect sees nothing → step 7 re-launches → step3 #2
            # starts but blocks on gate
            await watcher.run_cycle()
            await asyncio.sleep(0.1)
            assert call_count == 2, "step3 #2 should have been called (blocked on gate)"

            # Restore detect so .output/UUID gets ingested
            watcher.detector.detect = original_detect

            # Cycle 4: detects .output/UUID from step3 #1, ingests it →
            # output_filename = None.  Step3 #2 is still blocked.
            await watcher.run_cycle()

            # Verify output_filename is now None
            records = await db.get_snapshot(watcher.name)
            assert len(records) == 1
            assert records[0].output_filename is None, (
                "output_filename should be None after ingesting .output"
            )

            # Release gate → step3 #2 completes, writes .output/UUID again
            gate.set()
            await asyncio.sleep(0.5)

            # Run cycles to trigger stray detection on the second .output
            for _ in range(3):
                await watcher.run_cycle()
                await asyncio.sleep(0.1)

            await watcher.shutdown()

            # THE BUG: the second .output file becomes a stray → deleted
            error_files = list((user_root / "error").iterdir())
            assert len(error_files) == 0, (
                f"Files were moved to error/ due to stray detection: "
                f"{[f.name for f in error_files]}.  This is caused by the "
                f"re-launch race condition producing an orphaned .output file."
            )

        finally:
            await runner.cleanup()
