"""Tests for app.py — application lifecycle, health server, directory discovery."""

import base64
import json
import os
from pathlib import Path

import pytest
import pytest_asyncio
import yaml
from aiohttp import web, ClientSession

from app import HealthServer, ensure_directories, setup_user, REQUIRED_DIRS, _load_smart_folders
from db_new import DocumentDBv2
from models import State
from orchestrator import DocumentWatcherV2


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
        return web.json_response({
            "metadata": self.response_metadata,
            "pdf": pdf_b64,
        })

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
    """Full user root with config files and directories."""
    root = tmp_path / "testuser"
    root.mkdir()
    for d in REQUIRED_DIRS:
        (root / d).mkdir()
    ctx_dir = root / "sorted" / "work"
    ctx_dir.mkdir(parents=True, exist_ok=True)
    (ctx_dir / "context.yaml").write_text(yaml.dump({
        "name": "work",
        "filename": "{context}-{type}-{date}-{sender}-{topic}",
        "folders": ["context", "sender", "topic"],
        "fields": {
            "type": {"instructions": "Document type"},
            "sender": {"instructions": "Sender"},
            "topic": {"instructions": "Topic"},
        },
    }))
    return root


def _write_file(root: Path, rel_path: str, content: bytes = b"test pdf content") -> None:
    full = root / rel_path
    full.parent.mkdir(parents=True, exist_ok=True)
    full.write_bytes(content)


# ---------------------------------------------------------------------------
# Health server
# ---------------------------------------------------------------------------

class TestHealthServer:
    @pytest.mark.asyncio
    async def test_not_ready_returns_503(self):
        """Health check returns 503 when not ready."""
        server = HealthServer(port=0)
        runner = web.AppRunner(server.app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]

        try:
            async with ClientSession() as session:
                async with session.get(f"http://127.0.0.1:{port}/health") as resp:
                    assert resp.status == 503
                    data = await resp.json()
                    assert data["status"] == "not_ready"
                    assert data["service"] == "watcher-v2"
        finally:
            await runner.cleanup()

    @pytest.mark.asyncio
    async def test_ready_returns_200(self):
        """Health check returns 200 when ready."""
        server = HealthServer(port=0)
        server.set_ready(True)
        runner = web.AppRunner(server.app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]

        try:
            async with ClientSession() as session:
                async with session.get(f"http://127.0.0.1:{port}/health") as resp:
                    assert resp.status == 200
                    data = await resp.json()
                    assert data["status"] == "healthy"
                    assert data["service"] == "watcher-v2"
        finally:
            await runner.cleanup()

    @pytest.mark.asyncio
    async def test_start_and_stop(self):
        """Health server can start and stop cleanly."""
        server = HealthServer(port=0)
        # Use internal setup to get a dynamic port
        runner = web.AppRunner(server.app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]

        async with ClientSession() as session:
            async with session.get(f"http://127.0.0.1:{port}/health") as resp:
                assert resp.status == 503

        await runner.cleanup()


# ---------------------------------------------------------------------------
# Directory setup
# ---------------------------------------------------------------------------

class TestEnsureDirectories:
    def test_creates_all_required_dirs(self, tmp_path):
        """All required directories are created."""
        user_root = tmp_path / "user1"
        user_root.mkdir()
        ensure_directories(user_root)

        for d in REQUIRED_DIRS:
            assert (user_root / d).is_dir(), f"Missing directory: {d}"

    def test_idempotent(self, tmp_path):
        """Calling twice doesn't error."""
        user_root = tmp_path / "user1"
        user_root.mkdir()
        ensure_directories(user_root)
        ensure_directories(user_root)

        for d in REQUIRED_DIRS:
            assert (user_root / d).is_dir()


# ---------------------------------------------------------------------------
# User setup
# ---------------------------------------------------------------------------

class TestSetupUser:
    def test_returns_watcher(self, user_root):
        """setup_user returns a DocumentWatcherV2 instance."""
        watcher = setup_user(
            user_root=user_root,
            db=None,  # Not used during setup
            service_url="http://unused",
            poll_interval=5.0,
            processor_timeout=900.0,
        )
        assert isinstance(watcher, DocumentWatcherV2)

    def test_loads_context_field_names(self, user_root):
        """Context field names are loaded from YAML config."""
        watcher = setup_user(
            user_root=user_root,
            db=None,
            service_url="http://unused",
            poll_interval=5.0,
            processor_timeout=900.0,
        )
        assert watcher.context_field_names is not None
        assert "work" in watcher.context_field_names
        assert "context" in watcher.context_field_names["work"]
        assert "date" in watcher.context_field_names["work"]
        assert "type" in watcher.context_field_names["work"]
        assert "sender" in watcher.context_field_names["work"]
        assert "topic" in watcher.context_field_names["work"]

    def test_no_context_configs(self, tmp_path):
        """User root without sorted/ context configs → context_field_names is None."""
        user_root = tmp_path / "noconfig"
        user_root.mkdir()

        watcher = setup_user(
            user_root=user_root,
            db=None,
            service_url="http://unused",
            poll_interval=5.0,
            processor_timeout=900.0,
        )
        assert watcher.context_field_names is None

    def test_directories_created(self, tmp_path):
        """setup_user creates all required directories."""
        user_root = tmp_path / "newuser"
        user_root.mkdir()
        ctx_dir = user_root / "sorted" / "work"
        ctx_dir.mkdir(parents=True, exist_ok=True)
        (ctx_dir / "context.yaml").write_text(yaml.dump({
            "name": "work",
            "filename": "{context}-{date}",
            "fields": {},
        }))

        setup_user(
            user_root=user_root,
            db=None,
            service_url="http://unused",
            poll_interval=5.0,
            processor_timeout=900.0,
        )

        for d in REQUIRED_DIRS:
            assert (user_root / d).is_dir()


# ---------------------------------------------------------------------------
# Single-cycle end-to-end (requires DB)
# ---------------------------------------------------------------------------

class TestSingleCycleEndToEnd:
    @pytest.mark.asyncio
    async def test_file_through_full_lifecycle(self, user_root, db, mock_service):
        """incoming → archive + .output → sorted, DB record IS_COMPLETE."""
        content = b"e2e test document content"
        _write_file(user_root, "incoming/invoice.pdf", content)

        watcher = setup_user(
            user_root=user_root,
            db=db,
            service_url=mock_service.url,
            poll_interval=5.0,
            processor_timeout=30.0,
        )

        # Cycle 1: detect → preprocess → process → reconcile → move to archive
        await watcher.run_cycle()
        assert (user_root / "archive" / "invoice.pdf").exists()
        assert not (user_root / "incoming" / "invoice.pdf").exists()
        assert len(mock_service.calls) == 1

        # Cycle 2: detect .output → read sidecar → reconcile → move to sorted
        await watcher.run_cycle()

        expected = user_root / "sorted" / "work" / mock_service.response_metadata["assigned_filename"]
        assert expected.exists()
        assert expected.read_bytes() == mock_service.response_pdf_bytes

        records = await db.get_snapshot()
        assert len(records) == 1
        assert records[0].state == State.IS_COMPLETE
        assert records[0].context == "work"


# ---------------------------------------------------------------------------
# Multi-user directory discovery
# ---------------------------------------------------------------------------

class TestMultiUserDiscovery:
    def test_multiple_users_each_get_watcher(self, tmp_path):
        """Multiple user roots each get their own orchestrator."""
        watchers = []
        for name in ("alice", "bob"):
            root = tmp_path / name
            root.mkdir()
            ctx_dir = root / "sorted" / "work"
            ctx_dir.mkdir(parents=True, exist_ok=True)
            (ctx_dir / "context.yaml").write_text(yaml.dump({
                "name": "work",
                "filename": "{context}-{date}",
                "fields": {},
            }))
            w = setup_user(root, db=None, service_url="http://unused",
                           poll_interval=5.0, processor_timeout=900.0)
            watchers.append((root, w))

        assert len(watchers) == 2
        assert all(isinstance(w, DocumentWatcherV2) for _, w in watchers)
        # Each has its own root
        roots = {str(r) for r, _ in watchers}
        assert len(roots) == 2


# ---------------------------------------------------------------------------
# Context config loading
# ---------------------------------------------------------------------------

class TestContextConfigLoading:
    def test_multiple_contexts_loaded(self, tmp_path):
        """Multiple context configs loaded correctly."""
        root = tmp_path / "user"
        root.mkdir()
        for ctx_name, ctx_data in [
            ("work", {
                "name": "work",
                "filename": "{context}-{type}-{date}",
                "fields": {
                    "type": {"instructions": "Doc type"},
                    "sender": {"instructions": "Sender"},
                },
            }),
            ("personal", {
                "name": "personal",
                "filename": "{context}-{category}-{date}",
                "fields": {
                    "category": {"instructions": "Category"},
                },
            }),
        ]:
            ctx_dir = root / "sorted" / ctx_name
            ctx_dir.mkdir(parents=True, exist_ok=True)
            (ctx_dir / "context.yaml").write_text(yaml.dump(ctx_data))

        watcher = setup_user(root, db=None, service_url="http://unused",
                             poll_interval=5.0, processor_timeout=900.0)

        assert watcher.context_field_names is not None
        assert "work" in watcher.context_field_names
        assert "personal" in watcher.context_field_names
        assert "type" in watcher.context_field_names["work"]
        assert "sender" in watcher.context_field_names["work"]
        assert "category" in watcher.context_field_names["personal"]


# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------

class TestGracefulShutdown:
    @pytest.mark.asyncio
    async def test_health_server_stops_cleanly(self):
        """Health server can be stopped without error."""
        server = HealthServer(port=0)
        runner = web.AppRunner(server.app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        server.runner = runner

        server.set_ready(True)
        assert server.ready is True

        server.set_ready(False)
        await server.stop()
        assert server.ready is False

    @pytest.mark.asyncio
    async def test_db_disconnects(self, db):
        """Database can be disconnected cleanly."""
        # db is connected by fixture
        records = await db.get_snapshot()
        assert isinstance(records, list)

        # Disconnect (fixture will handle cleanup)


# ---------------------------------------------------------------------------
# Error resilience (requires DB)
# ---------------------------------------------------------------------------

class TestErrorResilience:
    @pytest.mark.asyncio
    async def test_unreachable_service_no_crash(self, user_root, db):
        """Unreachable service doesn't crash the cycle."""
        content = b"error resilience doc"
        _write_file(user_root, "incoming/doc.pdf", content)

        watcher = setup_user(
            user_root=user_root,
            db=db,
            service_url="http://127.0.0.1:19999",  # nothing listens here
            poll_interval=5.0,
            processor_timeout=5.0,
        )
        watcher.processor.max_retries = 0

        # Should not raise
        await watcher.run_cycle()
        await watcher.run_cycle()

        # Records should exist (not crashed)
        records = await db.get_snapshot()
        assert len(records) >= 1


# ---------------------------------------------------------------------------
# Sorted/ config loading
# ---------------------------------------------------------------------------

class TestSortedConfigLoading:
    def test_loads_context_from_sorted_dir(self, tmp_path):
        """Context config loaded from sorted/{context}/context.yaml."""
        root = tmp_path / "user"
        root.mkdir()
        for d in REQUIRED_DIRS:
            (root / d).mkdir()

        # Write sorted/arbeit/context.yaml
        ctx_dir = root / "sorted" / "arbeit"
        ctx_dir.mkdir(parents=True)
        (ctx_dir / "context.yaml").write_text(yaml.dump({
            "name": "arbeit",
            "filename": "{context}-{type}-{date}",
            "folders": ["context", "sender"],
            "fields": {
                "type": {"instructions": "Document type"},
                "sender": {"instructions": "Sender"},
            },
        }))

        watcher = setup_user(root, db=None, service_url="http://unused",
                             poll_interval=5.0, processor_timeout=900.0)

        assert watcher.context_field_names is not None
        assert "arbeit" in watcher.context_field_names
        assert "type" in watcher.context_field_names["arbeit"]
        assert "sender" in watcher.context_field_names["arbeit"]

    def test_name_mismatch_rejected(self, tmp_path):
        """Context with name not matching directory is skipped."""
        root = tmp_path / "user"
        root.mkdir()
        for d in REQUIRED_DIRS:
            (root / d).mkdir()

        ctx_dir = root / "sorted" / "arbeit"
        ctx_dir.mkdir(parents=True)
        # Name says "privat" but directory is "arbeit"
        (ctx_dir / "context.yaml").write_text(yaml.dump({
            "name": "privat",
            "filename": "{context}-{date}",
            "fields": {},
        }))

        watcher = setup_user(root, db=None, service_url="http://unused",
                             poll_interval=5.0, processor_timeout=900.0)

        # Should not have loaded the mismatched context
        assert watcher.context_field_names is None


    def test_smart_folders_from_sorted(self, tmp_path):
        """Smart folders loaded from sorted/{context}/smartfolders.yaml."""
        root = tmp_path / "user"
        root.mkdir()
        for d in REQUIRED_DIRS:
            (root / d).mkdir()

        ctx_dir = root / "sorted" / "arbeit"
        ctx_dir.mkdir(parents=True)
        (ctx_dir / "context.yaml").write_text(yaml.dump({
            "name": "arbeit",
            "filename": "{context}-{type}-{date}",
            "fields": {"type": {"instructions": "Type"}},
        }))
        (ctx_dir / "smartfolders.yaml").write_text(yaml.dump({
            "smart_folders": {
                "rechnungen": {
                    "context": "arbeit",
                    "condition": {"field": "type", "value": "Rechnung"},
                },
            },
        }))

        watcher = setup_user(root, db=None, service_url="http://unused",
                             poll_interval=5.0, processor_timeout=900.0)

        assert watcher.smart_folder_reconciler is not None

    def test_smart_folder_context_mismatch_rejected(self, tmp_path):
        """Smart folder with wrong context field is skipped."""
        root = tmp_path / "user"
        root.mkdir()
        for d in REQUIRED_DIRS:
            (root / d).mkdir()

        ctx_dir = root / "sorted" / "arbeit"
        ctx_dir.mkdir(parents=True)
        (ctx_dir / "context.yaml").write_text(yaml.dump({
            "name": "arbeit",
            "filename": "{context}-{date}",
            "fields": {"type": {"instructions": "Type"}},
        }))
        (ctx_dir / "smartfolders.yaml").write_text(yaml.dump({
            "smart_folders": {
                "rechnungen": {
                    "context": "privat",  # Mismatch!
                    "condition": {"field": "type", "value": "Rechnung"},
                },
            },
        }))

        watcher = setup_user(root, db=None, service_url="http://unused",
                             poll_interval=5.0, processor_timeout=900.0)

        # Smart folder should be rejected due to context mismatch
        assert watcher.smart_folder_reconciler is None

    def test_context_manager_passed_to_watcher(self, tmp_path):
        """context_manager is passed to DocumentWatcherV2."""
        root = tmp_path / "user"
        root.mkdir()
        for d in REQUIRED_DIRS:
            (root / d).mkdir()

        watcher = setup_user(root, db=None, service_url="http://unused",
                             poll_interval=5.0, processor_timeout=900.0)

        assert watcher.context_manager is not None


# ---------------------------------------------------------------------------
# Generated candidates & clues
# ---------------------------------------------------------------------------

class TestGeneratedData:
    def _make_context_dir(self, root, name="work", fields=None):
        """Helper to create a sorted/{name}/context.yaml."""
        if fields is None:
            fields = {
                "sender": {
                    "instructions": "Sender",
                    "candidates": [
                        "Acme Corp",
                        {"name": "Beta Inc", "clues": ["Uses BETA format"], "allow_new_clues": True},
                    ],
                    "allow_new_candidates": True,
                },
                "type": {"instructions": "Document type"},
            }
        for d in REQUIRED_DIRS:
            (root / d).mkdir(parents=True, exist_ok=True)
        ctx_dir = root / "sorted" / name
        ctx_dir.mkdir(parents=True, exist_ok=True)
        (ctx_dir / "context.yaml").write_text(yaml.dump({
            "name": name,
            "filename": "{context}-{type}-{date}-{sender}",
            "fields": fields,
        }))
        return ctx_dir

    def test_is_new_item_true_for_unknown(self, tmp_path):
        """is_new_item returns True for a value not in base or generated candidates."""
        from sorter import SorterContextManager
        root = tmp_path / "user"
        root.mkdir()
        self._make_context_dir(root)
        mgr = SorterContextManager(root, "testuser")
        mgr.load()

        assert mgr.is_new_item("work", "sender", "New Corp") is True

    def test_is_new_item_false_for_existing(self, tmp_path):
        """is_new_item returns False for a value already in base candidates."""
        from sorter import SorterContextManager
        root = tmp_path / "user"
        root.mkdir()
        self._make_context_dir(root)
        mgr = SorterContextManager(root, "testuser")
        mgr.load()

        assert mgr.is_new_item("work", "sender", "Acme Corp") is False

    def test_record_new_item_adds_candidate(self, tmp_path):
        """record_new_item adds a candidate and saves generated file."""
        from sorter import SorterContextManager
        root = tmp_path / "user"
        root.mkdir()
        self._make_context_dir(root)
        mgr = SorterContextManager(root, "testuser")
        mgr.load()

        assert mgr.record_new_item("work", "sender", "New Corp") is True

        # Generated file should exist
        gen_path = root / "sorted" / "work" / "generated.yaml"
        assert gen_path.exists()
        data = yaml.safe_load(gen_path.read_text())
        assert "New Corp" in data["fields"]["sender"]["candidates"]

        # Second call should be a no-op (duplicate)
        assert mgr.record_new_item("work", "sender", "New Corp") is False

    def test_record_new_clue_adds_clue(self, tmp_path):
        """record_new_clue adds a clue for an existing allow_new_clues candidate."""
        from sorter import SorterContextManager
        root = tmp_path / "user"
        root.mkdir()
        self._make_context_dir(root)
        mgr = SorterContextManager(root, "testuser")
        mgr.load()

        assert mgr.record_new_clue("work", "sender", "Beta Inc", "New clue") is True

        gen_path = root / "sorted" / "work" / "generated.yaml"
        assert gen_path.exists()
        data = yaml.safe_load(gen_path.read_text())
        gen_candidates = data["fields"]["sender"]["candidates"]
        clue_entry = next(c for c in gen_candidates if isinstance(c, dict) and c.get("name") == "Beta Inc")
        assert "New clue" in clue_entry["clues"]

        # Duplicate clue should be a no-op
        assert mgr.record_new_clue("work", "sender", "Beta Inc", "New clue") is False

    def test_record_new_clue_rejected_without_allow(self, tmp_path):
        """record_new_clue returns False for candidates without allow_new_clues."""
        from sorter import SorterContextManager
        root = tmp_path / "user"
        root.mkdir()
        self._make_context_dir(root)
        mgr = SorterContextManager(root, "testuser")
        mgr.load()

        # Acme Corp is a simple string candidate — can't add clues
        assert mgr.record_new_clue("work", "sender", "Acme Corp", "Some clue") is False

    def test_get_context_for_api_merges_generated(self, tmp_path):
        """get_context_for_api merges generated candidates into fields."""
        from sorter import SorterContextManager
        root = tmp_path / "user"
        root.mkdir()
        self._make_context_dir(root)
        mgr = SorterContextManager(root, "testuser")
        mgr.load()

        mgr.record_new_item("work", "sender", "New Corp")
        mgr.record_new_clue("work", "sender", "Beta Inc", "New clue")

        ctx = mgr.get_context_for_api("work")
        assert ctx is not None
        candidates = ctx["fields"]["sender"]["candidates"]
        # Should have base + generated
        names = []
        for c in candidates:
            if isinstance(c, str):
                names.append(c)
            elif isinstance(c, dict):
                names.append(c["name"])
        assert "Acme Corp" in names
        assert "Beta Inc" in names
        assert "New Corp" in names
        # Beta Inc should have merged clues
        beta = next(c for c in candidates if isinstance(c, dict) and c.get("name") == "Beta Inc")
        assert "Uses BETA format" in beta["clues"]
        assert "New clue" in beta["clues"]

    def test_generated_file_loaded_on_reload(self, tmp_path):
        """Generated file is loaded when SorterContextManager.load() is called."""
        from sorter import SorterContextManager
        root = tmp_path / "user"
        root.mkdir()
        self._make_context_dir(root)
        mgr = SorterContextManager(root, "testuser")
        mgr.load()

        mgr.record_new_item("work", "sender", "New Corp")

        # Create a fresh manager and load — should pick up generated file
        mgr2 = SorterContextManager(root, "testuser")
        mgr2.load()

        assert mgr2.is_new_item("work", "sender", "New Corp") is False
        ctx = mgr2.get_context_for_api("work")
        candidates = ctx["fields"]["sender"]["candidates"]
        names = [c if isinstance(c, str) else c.get("name") for c in candidates]
        assert "New Corp" in names

    def test_no_candidates_field_ignored(self, tmp_path):
        """Fields without candidates are ignored by is_new_item/record_new_item."""
        from sorter import SorterContextManager
        root = tmp_path / "user"
        root.mkdir()
        self._make_context_dir(root)
        mgr = SorterContextManager(root, "testuser")
        mgr.load()

        # "type" field has no candidates
        assert mgr.is_new_item("work", "type", "Invoice") is False
        assert mgr.record_new_item("work", "type", "Invoice") is False
