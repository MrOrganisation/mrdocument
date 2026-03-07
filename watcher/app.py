"""
Document watcher v2 — application entry point.

Single-process replacement for the v1 watcher.py + sorter.py pair.
Uses a polling loop with DocumentWatcherV2 orchestrators per user root.
"""

import asyncio
import logging
import os
import sys
import time
from pathlib import Path
from typing import Optional

from aiohttp import web

from db_new import DocumentDBv2
from orchestrator import DocumentWatcherV2, context_field_names_from_sorter, context_folders_from_sorter, contexts_for_api_from_sorter
from sorter import SorterContextManager, WatcherConfig, get_username_from_root
from step5 import SmartFolderEntry, RootSmartFolderEntry

logger = logging.getLogger(__name__)


def _load_smart_folders(context_manager) -> list | None:
    """Load smart folders: try sorted/ YAML files first, fallback to embedded.

    Returns a list of SmartFolderEntry or None.
    """
    # Contexts with sorted/ smartfolders.yaml files
    sorted_sf = context_manager.load_smart_folders_from_sorted()
    # Contexts that have embedded smart_folders in context YAML
    embedded_contexts = set()
    for ctx_name, ctx in context_manager.contexts.items():
        if ctx.smart_folders:
            embedded_contexts.add(ctx_name)

    smart_folders = []
    # For each context: prefer sorted/ file, fallback to embedded
    all_contexts = set(sorted_sf.keys()) | embedded_contexts
    for ctx_name in all_contexts:
        if ctx_name in sorted_sf:
            for sf_name, sf_config in sorted_sf[ctx_name]:
                smart_folders.append(SmartFolderEntry(
                    context=ctx_name,
                    config=sf_config,
                ))
        elif ctx_name in embedded_contexts:
            ctx = context_manager.contexts[ctx_name]
            for sf_name, sf_config in ctx.smart_folders.items():
                smart_folders.append(SmartFolderEntry(
                    context=ctx_name,
                    config=sf_config,
                ))

    return smart_folders if smart_folders else None


def _load_root_smart_folders(root: Path) -> list[RootSmartFolderEntry] | None:
    """Load root-level smart folders from {root}/smartfolders.yaml.

    Returns a list of RootSmartFolderEntry or None if file missing/empty.
    """
    config_path = root / "smartfolders.yaml"
    if not config_path.is_file():
        return None

    try:
        import yaml
        data = yaml.safe_load(config_path.read_text())
    except Exception as e:
        logger.warning("Failed to parse %s: %s", config_path, e)
        return None

    if not isinstance(data, dict):
        return None

    sf_dict = data.get("smart_folders")
    if not isinstance(sf_dict, dict):
        return None

    entries = []
    for sf_name, sf_data in sf_dict.items():
        if not isinstance(sf_data, dict):
            logger.warning("Root smart folder '%s': expected dict, skipping", sf_name)
            continue

        context = sf_data.get("context")
        path_str = sf_data.get("path")
        if not context or not path_str:
            logger.warning(
                "Root smart folder '%s': missing context or path, skipping",
                sf_name,
            )
            continue

        # Resolve path: absolute stays absolute, relative resolves against root
        path = Path(path_str)
        if not path.is_absolute():
            path = root / path

        # Parse condition/filename_regex via SmartFolderConfig
        from sorter import SmartFolderConfig
        config = SmartFolderConfig.from_dict(sf_name, sf_data, context_name=context)
        if config is None:
            continue

        entries.append(RootSmartFolderEntry(
            name=sf_name,
            context=context,
            path=path,
            config=config,
        ))

    return entries if entries else None


REQUIRED_DIRS = [
    "archive", "incoming", "reviewed", "processed",
    "trash", ".output", "sorted", "error", "void", "missing",
    "duplicates",
]


# ---------------------------------------------------------------------------
# Health server
# ---------------------------------------------------------------------------

class HealthServer:
    """HTTP server for health checks."""

    def __init__(self, port: int = 8080):
        self.port = port
        self.ready = False
        self.app = web.Application()
        self.app.router.add_get("/health", self.health_handler)
        self.runner = None

    async def health_handler(self, request: web.Request) -> web.Response:
        if not self.ready:
            return web.json_response(
                {"status": "not_ready", "service": "watcher-v2"},
                status=503,
            )
        return web.json_response(
            {"status": "healthy", "service": "watcher-v2"}
        )

    async def start(self):
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "0.0.0.0", self.port)
        await site.start()
        logger.info("Health server listening on port %d", self.port)

    async def stop(self):
        if self.runner:
            await self.runner.cleanup()

    def set_ready(self, ready: bool = True):
        self.ready = ready


# ---------------------------------------------------------------------------
# Directory setup
# ---------------------------------------------------------------------------

def ensure_directories(user_root: Path) -> None:
    """Create all required directories for a user root."""
    for d in REQUIRED_DIRS:
        (user_root / d).mkdir(parents=True, exist_ok=True)


def setup_user(
    user_root: Path,
    db: DocumentDBv2,
    service_url: str,
    poll_interval: float,
    processor_timeout: float,
    stt_url: Optional[str] = None,
    max_concurrent: int = 5,
) -> DocumentWatcherV2:
    """Set up a DocumentWatcherV2 for a single user root."""
    username = get_username_from_root(user_root)
    ensure_directories(user_root)

    context_field_names = None
    ctx_folders = None
    contexts_for_api = None
    smart_folders = None
    context_manager = SorterContextManager(user_root, username)
    if context_manager.load():
        context_field_names = context_field_names_from_sorter(context_manager)
        ctx_folders = context_folders_from_sorter(context_manager)
        contexts_for_api = contexts_for_api_from_sorter(context_manager)
        logger.info("[%s] Loaded %d context(s)", username, len(context_field_names))

        smart_folders = _load_smart_folders(context_manager)
        if smart_folders:
            logger.info("[%s] Loaded %d smart folder(s)", username, len(smart_folders))

    root_smart_folders = _load_root_smart_folders(user_root)
    if root_smart_folders:
        logger.info("[%s] Loaded %d root smart folder(s)", username, len(root_smart_folders))

    return DocumentWatcherV2(
        root=user_root,
        db=db,
        service_url=service_url,
        context_field_names=context_field_names,
        context_folders=ctx_folders,
        poll_interval=poll_interval,
        processor_timeout=processor_timeout,
        stt_url=stt_url,
        contexts_for_api=contexts_for_api,
        smart_folders=smart_folders,
        root_smart_folders=root_smart_folders,
        audio_links=True,
        max_concurrent=max_concurrent,
        name=username,
        context_manager=context_manager,
    )


# ---------------------------------------------------------------------------
# Per-watcher event loop
# ---------------------------------------------------------------------------

async def run_watcher(
    watcher: DocumentWatcherV2,
    full_scan_seconds: float,
    debounce_seconds: float,
) -> None:
    """Event-driven loop for a single watcher.

    - Startup: always runs a full scan.
    - Then waits for inotify events (incremental) or full_scan timer.
    - Debounces events by waiting for quiet before running a cycle.
    """
    try:
        had_activity = await watcher.run_cycle(full_scan=True)
        last_full = time.monotonic()

        while True:
            # Re-run immediately if the previous cycle had state transitions
            if had_activity:
                had_activity = await watcher.run_cycle(full_scan=False)
                continue

            time_to_full = full_scan_seconds - (time.monotonic() - last_full)

            if time_to_full <= 0:
                # Full scan due — wait for quiet first
                await watcher.wait_for_quiet(debounce_seconds)
                had_activity = await watcher.run_cycle(full_scan=True)
                last_full = time.monotonic()
            else:
                # Wait for inotify event or full scan timer
                got_event = await watcher.detector.wait_for_event(time_to_full)
                if got_event:
                    await watcher.wait_for_quiet(debounce_seconds)
                    had_activity = await watcher.run_cycle(full_scan=False)
                    # Config change detected → run full scan
                    if watcher._pending_full_scan:
                        watcher._pending_full_scan = False
                        had_activity = await watcher.run_cycle(full_scan=True)
                        last_full = time.monotonic()
                else:
                    # Timer expired, loop back to full scan branch
                    had_activity = False
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.error("[%s] Watcher task error: %s", watcher.name, e)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    # 1. Configuration
    mrdocument_url = os.environ.get("MRDOCUMENT_URL", "http://mrdocument-service:8000")
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("ERROR: DATABASE_URL environment variable is required", file=sys.stderr)
        sys.exit(1)
    stt_url = os.environ.get("STT_URL")  # Optional, None disables audio
    health_port = int(os.environ.get("HEALTH_PORT", "8080"))
    watcher_config_path = Path(os.environ.get("WATCHER_CONFIG", "/app/watcher.yaml"))
    poll_interval = float(os.environ.get("POLL_INTERVAL", "5"))
    processor_timeout = float(os.environ.get("PROCESSOR_TIMEOUT", "900"))
    max_concurrent = int(os.environ.get("MAX_CONCURRENT_PROCESSING", "5"))

    # 2. Logging
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    log_dir = os.environ.get("LOG_DIR")
    if log_dir:
        fh = logging.FileHandler(f"{log_dir}/watcher-v2.log")
        fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        logging.getLogger().addHandler(fh)

    logger.info("Watcher v2 starting")

    # 3. Database connection
    db = DocumentDBv2(database_url)
    try:
        await db.connect()
    except Exception as e:
        logger.error("Failed to connect to database: %s", e)
        sys.exit(1)

    # 4. Health server
    health_server = HealthServer(port=health_port)
    await health_server.start()

    try:
        # 5. Discover user roots
        watcher_config = WatcherConfig.load(watcher_config_path)
        watch_dirs = watcher_config.get_watch_directories()

        while not watch_dirs:
            logger.info("No watch folders found, waiting...")
            await asyncio.sleep(60)
            watch_dirs = watcher_config.get_watch_directories()

        logger.info("Discovered %d watch directories: %s",
                     len(watch_dirs), [str(d) for d in watch_dirs])

        # 6. Per-user orchestrator setup
        watchers: list[tuple[Path, DocumentWatcherV2]] = []
        known_dirs: set[Path] = set()
        watcher_tasks: list[asyncio.Task] = []

        debounce_seconds = watcher_config.debounce_seconds
        full_scan_seconds = watcher_config.full_scan_seconds

        for user_root in watch_dirs:
            watcher = setup_user(user_root, db, mrdocument_url,
                                 poll_interval, processor_timeout, stt_url,
                                 max_concurrent)
            watchers.append((user_root, watcher))
            known_dirs.add(user_root)
            task = asyncio.create_task(
                run_watcher(watcher, full_scan_seconds, debounce_seconds),
                name=f"watcher-{user_root.name}",
            )
            watcher_tasks.append(task)

        # 8. Directory discovery task
        async def discover_directories():
            while True:
                await asyncio.sleep(full_scan_seconds)
                current_dirs = watcher_config.get_watch_directories()
                for new_dir in current_dirs:
                    if new_dir not in known_dirs:
                        logger.info("New user directory discovered: %s", new_dir)
                        w = setup_user(new_dir, db, mrdocument_url,
                                       poll_interval, processor_timeout,
                                       stt_url, max_concurrent)
                        watchers.append((new_dir, w))
                        known_dirs.add(new_dir)
                        t = asyncio.create_task(
                            run_watcher(w, full_scan_seconds, debounce_seconds),
                            name=f"watcher-{new_dir.name}",
                        )
                        watcher_tasks.append(t)

        discovery_task = asyncio.create_task(
            discover_directories(), name="directory-discovery",
        )

        health_server.set_ready(True)
        logger.info(
            "Watcher v2 ready, debounce=%.1fs full_scan=%.1fs",
            debounce_seconds, full_scan_seconds,
        )

        # Wait for all watcher tasks (they run forever)
        await asyncio.gather(discovery_task, *watcher_tasks)

    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        health_server.set_ready(False)
        for _, watcher in watchers:
            await watcher.shutdown()
        await health_server.stop()
        await db.disconnect()
        logger.info("Watcher v2 stopped")


if __name__ == "__main__":
    asyncio.run(main())
