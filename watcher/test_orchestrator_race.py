"""Unit tests for the step3 re-launch race condition in orchestrator.py.

These tests demonstrate the bug where step3 completes (clearing _in_flight)
but the record still has output_filename set in the DB, causing step 7 to
re-launch processing for the same document.

All tests use an in-memory mock DB — no PostgreSQL required.
"""

import asyncio
import base64
import copy
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from aiohttp import web

from models import State, PathEntry, Record
from orchestrator import DocumentWatcherV2


# ---------------------------------------------------------------------------
# In-memory mock DB
# ---------------------------------------------------------------------------

class MockDB:
    """In-memory database that implements the DocumentDBv2 interface."""

    def __init__(self):
        self._records: dict[UUID, Record] = {}

    async def get_snapshot(self, username=None):
        records = list(self._records.values())
        if username:
            records = [r for r in records if r.username == username]
        return [copy.deepcopy(r) for r in records]

    async def create_record(self, record):
        self._records[record.id] = copy.deepcopy(record)

    async def save_record(self, record):
        self._records[record.id] = copy.deepcopy(record)

    async def delete_record(self, record_id):
        self._records.pop(record_id, None)

    async def get_records_with_output_filename(self, username=None):
        records = list(self._records.values())
        if username:
            records = [r for r in records if r.username == username]
        return [copy.deepcopy(r) for r in records if r.output_filename is not None]

    async def get_records_with_temp_fields(self, username=None):
        result = []
        for r in self._records.values():
            if username and r.username != username:
                continue
            if (r.target_path or r.source_reference or r.current_reference
                    or r.duplicate_sources or r.deleted_paths
                    or r.state == State.NEEDS_DELETION):
                result.append(copy.deepcopy(r))
        return result


# ---------------------------------------------------------------------------
# Mock mrdocument service
# ---------------------------------------------------------------------------

class MockService:
    def __init__(self):
        self.calls: list[dict] = []
        self.response_metadata = {
            "context": "work",
            "date": "2025-01-15",
            "assigned_filename": "work-Invoice-2025.pdf",
        }
        self.response_pdf_bytes = b"fake pdf output content"

    async def handle_process(self, request):
        reader = await request.multipart()
        fields = {}
        file_data = None
        while True:
            part = await reader.next()
            if part is None:
                break
            if part.name == "file":
                file_data = await part.read()
                fields["filename"] = part.filename
            else:
                fields[part.name] = await part.text()
        self.calls.append({"fields": fields, "file_data": file_data})
        pdf_b64 = base64.b64encode(self.response_pdf_bytes).decode()
        return web.json_response({
            "metadata": self.response_metadata,
            "filename": self.response_metadata.get("assigned_filename"),
            "pdf": pdf_b64,
        })

    def make_app(self):
        app = web.Application()
        app.router.add_post("/process", self.handle_process)
        return app


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def user_root(tmp_path):
    for d in ("archive", "incoming", "reviewed", "processed",
              "trash", ".output", "sorted", "error", "void", "missing"):
        (tmp_path / d).mkdir()
    return tmp_path


@pytest.fixture
def db():
    return MockDB()


@pytest_asyncio.fixture
async def mock_service():
    service = MockService()
    runner = web.AppRunner(service.make_app())
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    service.url = f"http://127.0.0.1:{port}"
    yield service
    await runner.cleanup()


def _write_file(root, rel_path, content=b"test pdf content"):
    full = root / rel_path
    full.parent.mkdir(parents=True, exist_ok=True)
    full.write_bytes(content)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestProcessingRelaunchRace:
    """Tests for the race where step3 completes and _in_flight is cleared
    before the .output file is detected, causing step 7 to re-launch the
    same record.

    The sequence:
    1. Record enters NEEDS_PROCESSING, output_filename=UUID. Step3 launches.
    2. Step3 completes: writes .output/UUID, _in_flight.discard(id).
    3. Next cycle, step 7: record still has output_filename=UUID (not yet
       ingested), id NOT in _in_flight → re-launched!
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
            root=user_root, db=db, service_url=mock_service.url,
        )

        # Cycle 1: detect incoming → create record → reconcile sets
        # output_filename + state=NEEDS_PROCESSING, moves file to archive.
        # Step 7 does NOT launch (output_filename set by reconcile AFTER step 7).
        await watcher.run_cycle()

        # Cycle 2: step 7 finds record with output_filename → launches step3.
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
    async def test_relaunch_produces_duplicate_service_call(
        self, user_root, db, mock_service,
    ):
        """Force the race: step3 completes between detection and step 7.

        We simulate this by patching the detector to return no .output changes
        in the cycle after step3 completes (as if detection ran before step3
        wrote the file).

        The test asserts the service is called only once.  It FAILS on the
        current code because step 7 re-launches, producing a second call.
        """
        _write_file(user_root, "incoming/doc.pdf", b"relaunch test content")

        watcher = DocumentWatcherV2(
            root=user_root, db=db, service_url=mock_service.url,
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
        # detection ran before step3 wrote the .output file.
        async def detect_no_output(snapshot):
            return []

        watcher.detector.detect = detect_no_output

        # Cycle 3: detection sees nothing → preprocess does nothing →
        # step 7 queries DB and finds record with output_filename → re-launch!
        await watcher.run_cycle()

        # Wait for the second step3 to complete
        await asyncio.sleep(0.5)
        await watcher.shutdown()

        # THE BUG: service was called TWICE for the same document.
        assert len(mock_service.calls) == 1, (
            f"Service was called {len(mock_service.calls)} times — step3 was "
            f"re-launched.  Expected exactly 1 call for a single document."
        )

    @pytest.mark.asyncio
    async def test_output_ingested_immediately(self, user_root, db, mock_service):
        """After step3 completes, the .output file should already be in
        current_paths (ingested by the background task).

        Without the fix, current_paths is empty until the next cycle's
        preprocess detects the .output file — leaving a window where step 7
        can re-launch processing.

        This test FAILS on buggy code (current_paths empty after step3)
        and PASSES with the fix.
        """
        _write_file(user_root, "incoming/doc.pdf", b"stray test content")

        watcher = DocumentWatcherV2(
            root=user_root, db=db, service_url=mock_service.url,
        )

        # Cycle 1: detect incoming → create record → reconcile sets
        # output_filename + state=NEEDS_PROCESSING, moves file to archive.
        await watcher.run_cycle()

        # Cycle 2: step 7 launches step3
        await watcher.run_cycle()

        # Wait for step3 to complete
        await asyncio.sleep(0.5)
        assert len(mock_service.calls) == 1

        # THE BUG: without the fix, current_paths is empty after step3
        # completes — the .output hasn't been ingested yet.  With the fix,
        # the background task ingests the .output immediately.
        records = await db.get_snapshot(watcher.name)
        assert len(records) == 1

        output_in_current = any(
            pe.path.startswith(".output/") for pe in records[0].current_paths
        )
        assert output_in_current, (
            f"After step3 completes, .output should already be in current_paths "
            f"(ingested by background task).  Got current_paths="
            f"{[pe.path for pe in records[0].current_paths]}, "
            f"output_filename={records[0].output_filename}"
        )

        await watcher.shutdown()

    @pytest.mark.asyncio
    async def test_duplicate_source_single_record(self, user_root, db, mock_service):
        """Two identical source files added in the same cycle should produce
        only one record (preprocess deduplicates via new_records search)."""
        _write_file(user_root, "incoming/a.pdf", b"identical content")
        _write_file(user_root, "incoming/b.pdf", b"identical content")

        watcher = DocumentWatcherV2(
            root=user_root, db=db, service_url=mock_service.url,
        )

        # Cycle 1: detect both → one record (same source_hash)
        await watcher.run_cycle()
        records = await db.get_snapshot(watcher.name)
        assert len(records) == 1, (
            f"Expected 1 record for identical files, got {len(records)}"
        )

        await watcher.shutdown()
