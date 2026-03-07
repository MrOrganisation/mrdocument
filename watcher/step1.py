"""
Filesystem detection for document watcher v2.

First cycle: full scan of watched directories to establish baseline state.
Subsequent cycles: inotify-driven incremental detection (only hash files
that actually changed on disk).
"""

import asyncio
import hashlib
import logging
import shutil
import threading
from pathlib import Path
from uuid import uuid4

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from models import EventType, ChangeItem, Record
from prefilter import SUPPORTED_EXTENSIONS

logger = logging.getLogger(__name__)

# Syncthing temp file patterns (from output_watcher.py)
SYNCTHING_PATTERNS = (".syncthing.", "~syncthing~")
TEMP_EXTENSIONS = (".tmp",)

# Config files that live inside sorted/ context directories
CONFIG_FILENAMES = {"context.yaml", "smartfolders.yaml", "generated.yaml"}


def _is_config_file(rel_path: str) -> bool:
    """Check if a relative path is a config file in sorted/{context}/."""
    parts = rel_path.split("/")
    return (len(parts) == 3
            and parts[0] == "sorted"
            and parts[2].lower() in CONFIG_FILENAMES)


# Directories scanned directly (non-recursive)
DIRECT_DIRS = ("archive", "incoming", "reviewed", "processed", "reset", "trash", ".output")

# Directories scanned recursively
RECURSIVE_DIRS = ("sorted",)

# Locations where unknown files are allowed (not stray)
ELIGIBLE_LOCATIONS = {"incoming", "sorted"}


def compute_sha256(file_path: Path) -> str:
    """Compute SHA-256 hash of a file."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _is_ignored(filename: str) -> bool:
    """Check if a filename should be ignored (hidden/temp files)."""
    if filename.startswith(".") or filename.startswith("~"):
        return True
    for pattern in SYNCTHING_PATTERNS:
        if pattern in filename:
            return True
    for ext in TEMP_EXTENSIONS:
        if filename.endswith(ext):
            return True
    return False


# ---------------------------------------------------------------------------
# Watchdog event collector
# ---------------------------------------------------------------------------

class _ChangeCollector(FileSystemEventHandler):
    """Collect changed relative paths from inotify events."""

    def __init__(self, root: Path, loop: asyncio.AbstractEventLoop):
        self.root = root
        self._changed: set[str] = set()
        self._lock = threading.Lock()
        self._event = asyncio.Event()
        self._loop = loop

    def _record(self, abs_path: str) -> None:
        try:
            rel = str(Path(abs_path).relative_to(self.root))
        except ValueError:
            return
        with self._lock:
            self._changed.add(rel)
        self._loop.call_soon_threadsafe(self._event.set)

    def on_created(self, event):
        if not event.is_directory:
            self._record(event.src_path)

    def on_deleted(self, event):
        if not event.is_directory:
            self._record(event.src_path)

    def on_modified(self, event):
        if not event.is_directory:
            self._record(event.src_path)

    def on_moved(self, event):
        if not event.is_directory:
            self._record(event.src_path)
        if hasattr(event, "dest_path") and not event.is_directory:
            self._record(event.dest_path)

    def drain(self) -> set[str]:
        """Return and clear all collected paths since last drain."""
        with self._lock:
            paths = self._changed
            self._changed = set()
            return paths


# ---------------------------------------------------------------------------
# Filesystem detector
# ---------------------------------------------------------------------------

class FilesystemDetector:
    """Scans watched directories and detects filesystem changes.

    First call to detect() does a full scan and starts an inotify observer.
    Subsequent calls use inotify events for O(changes) instead of O(all-files).
    """

    def __init__(self, root: Path, debounce_seconds: float = 2.0):
        self.root = root
        self.debounce_seconds = debounce_seconds
        self._previous_state: dict[str, str] = {}  # relative_path -> hash
        self._observer: Observer | None = None
        self._collector: _ChangeCollector | None = None
        self.config_changed = False
        self._root_sf_hash: str | None = None  # hash of root smartfolders.yaml

    def _start_observer(self) -> None:
        """Start inotify watches on all watched directories."""
        loop = asyncio.get_event_loop()
        self._collector = _ChangeCollector(self.root, loop)
        self._observer = Observer()
        for dirname in DIRECT_DIRS:
            dirpath = self.root / dirname
            if dirpath.is_dir():
                self._observer.schedule(
                    self._collector, str(dirpath), recursive=False,
                )
        for dirname in RECURSIVE_DIRS:
            dirpath = self.root / dirname
            if dirpath.is_dir():
                self._observer.schedule(
                    self._collector, str(dirpath), recursive=True,
                )
        self._observer.start()
        logger.info("Started filesystem observer on %s", self.root)

    def stop(self) -> None:
        """Stop the inotify observer."""
        if self._observer:
            self._observer.stop()
            self._observer.join()
            self._observer = None

    async def wait_for_event(self, timeout: float) -> bool:
        """Wait for a filesystem event, up to timeout seconds.

        Returns True if an event arrived, False if the timeout expired.
        """
        if self._collector is None:
            # Observer not started yet — cannot wait
            return False
        self._collector._event.clear()
        try:
            await asyncio.wait_for(self._collector._event.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return False

    def _scan(self) -> dict[str, tuple[str, int]]:
        """Scan all watched directories.

        Returns:
            Dict mapping relative_path to (hash, size).
        """
        result: dict[str, tuple[str, int]] = {}

        for dirname in DIRECT_DIRS:
            dirpath = self.root / dirname
            if not dirpath.is_dir():
                continue
            for f in dirpath.iterdir():
                if not f.is_file() or f.is_symlink():
                    continue
                if _is_ignored(f.name):
                    continue
                ext = f.suffix.lower()
                if ext and ext not in SUPPORTED_EXTENSIONS:
                    continue
                rel = str(f.relative_to(self.root))
                try:
                    file_hash = compute_sha256(f)
                    file_size = f.stat().st_size
                    result[rel] = (file_hash, file_size)
                except OSError as e:
                    logger.warning("Failed to read %s: %s", rel, e)

        for dirname in RECURSIVE_DIRS:
            dirpath = self.root / dirname
            if not dirpath.is_dir():
                continue
            for f in dirpath.rglob("*"):
                if not f.is_file() or f.is_symlink():
                    continue
                if _is_ignored(f.name):
                    continue
                ext = f.suffix.lower()
                if ext and ext not in SUPPORTED_EXTENSIONS:
                    continue
                rel = str(f.relative_to(self.root))
                if _is_config_file(rel):
                    continue
                try:
                    file_hash = compute_sha256(f)
                    file_size = f.stat().st_size
                    result[rel] = (file_hash, file_size)
                except OSError as e:
                    logger.warning("Failed to read %s: %s", rel, e)

        return result

    @staticmethod
    def _get_location(rel_path: str) -> str:
        """Extract the top-level location from a relative path."""
        return rel_path.split("/")[0]

    @staticmethod
    def _is_known(file_hash: str, rel_path: str, snapshot: list[Record]) -> bool:
        """Check if a file is known in the snapshot."""
        location = FilesystemDetector._get_location(rel_path)

        # .output files match by filename (or sidecar suffix)
        if location == ".output":
            filename = Path(rel_path).name
            # Sidecar files (.meta.json) are associated with their output file
            if filename.endswith(".meta.json"):
                base = filename[: -len(".meta.json")]
                if any(r.output_filename == base for r in snapshot):
                    return True
                # After output_filename is cleared, check current_paths
                base_path = f".output/{base}"
                return any(
                    pe.path == base_path
                    for r in snapshot for pe in r.current_paths
                )
            if any(r.output_filename == filename for r in snapshot):
                return True
            # After output_filename is cleared, check current_paths
            return any(
                pe.path == rel_path
                for r in snapshot for pe in r.current_paths
            )

        # Other locations: match by source_hash or hash
        for r in snapshot:
            if r.source_hash == file_hash:
                return True
            if r.hash is not None and r.hash == file_hash:
                return True

        return False

    def _delete_stray(self, rel_path: str) -> None:
        """Delete a stray file that we know we created (e.g. .output/)."""
        src = self.root / rel_path
        try:
            src.unlink(missing_ok=True)
            logger.info("Deleted stray output file: %s", rel_path)
        except Exception as e:
            logger.error("Failed to delete stray file %s: %s", rel_path, e)

    def _move_to_error(self, rel_path: str) -> None:
        """Move a stray file to the error/ directory."""
        src = self.root / rel_path
        error_dir = self.root / "error"
        error_dir.mkdir(parents=True, exist_ok=True)
        dest = error_dir / src.name
        if dest.exists():
            stem = dest.stem
            suffix = dest.suffix
            dest = error_dir / f"{stem}_{uuid4().hex[:8]}{suffix}"
        try:
            src.rename(dest)
            logger.info("Moved stray file to error: %s -> %s", rel_path, dest.name)
        except OSError:
            try:
                shutil.move(str(src), str(dest))
                logger.info("Moved stray file to error: %s -> %s", rel_path, dest.name)
            except Exception as e:
                logger.error("Failed to move stray file %s to error: %s", rel_path, e)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def _check_root_smartfolders_yaml(self) -> None:
        """Check if root smartfolders.yaml changed and set config_changed."""
        sf_path = self.root / "smartfolders.yaml"
        try:
            if sf_path.is_file():
                current_hash = compute_sha256(sf_path)
            else:
                current_hash = None
        except OSError:
            current_hash = None

        if current_hash != self._root_sf_hash:
            if self._root_sf_hash is not None:
                # Only flag change after first run (not on initial load)
                self.config_changed = True
            self._root_sf_hash = current_hash

    async def detect(self, db_snapshot: list[Record]) -> list[ChangeItem]:
        """Detect filesystem changes.

        First call: full scan + start inotify observer.
        Subsequent calls: process only inotify events.
        """
        self._check_root_smartfolders_yaml()
        if self._observer is None:
            return self._detect_full(db_snapshot)
        return self._detect_incremental(db_snapshot)

    def _detect_full(self, db_snapshot: list[Record]) -> list[ChangeItem]:
        """Full filesystem scan (first run only)."""
        # Start observer before scan so events during scan are captured.
        # They'll be processed as (harmless) re-hashes on the next cycle.
        self._start_observer()

        current_state = self._scan()
        changes: list[ChangeItem] = []

        # Build map of paths already tracked in DB → their record's hashes.
        # This makes restart idempotent: files already in DB with unchanged
        # hashes don't produce ADDITION events, preventing duplicate
        # PathEntry appends and spurious is_missing→is_complete transitions.
        known_paths: dict[str, str | None] = {}
        for record in db_snapshot:
            for pe in record.source_paths:
                known_paths[pe.path] = record.source_hash
            for pe in record.current_paths:
                known_paths[pe.path] = record.hash

        for rel_path, (file_hash, file_size) in current_state.items():
            location = self._get_location(rel_path)

            # Stray detection: unknown file in non-eligible location
            if not self._is_known(file_hash, rel_path, db_snapshot):
                if location not in ELIGIBLE_LOCATIONS:
                    if location == ".output":
                        self._delete_stray(rel_path)
                    else:
                        self._move_to_error(rel_path)
                    continue

            # Skip files already tracked in DB with unchanged hash
            if rel_path in known_paths and known_paths[rel_path] == file_hash:
                continue

            changes.append(ChangeItem(
                event_type=EventType.ADDITION,
                path=rel_path,
                hash=file_hash,
                size=file_size,
            ))

        # DB paths that don't exist on disk (stale/deleted while watcher was down)
        seen: set[str] = set()
        for record in db_snapshot:
            for pe in record.source_paths:
                if pe.path not in current_state and pe.path not in seen:
                    changes.append(ChangeItem(
                        event_type=EventType.REMOVAL,
                        path=pe.path,
                    ))
                    seen.add(pe.path)
            for pe in record.current_paths:
                if pe.path not in current_state and pe.path not in seen:
                    changes.append(ChangeItem(
                        event_type=EventType.REMOVAL,
                        path=pe.path,
                    ))
                    seen.add(pe.path)

        self._previous_state = {
            path: hash_val for path, (hash_val, _) in current_state.items()
        }

        return changes

    def _detect_incremental(self, db_snapshot: list[Record]) -> list[ChangeItem]:
        """Incremental detection from inotify events."""
        changed_paths = self._collector.drain()
        if not changed_paths:
            return []

        changes: list[ChangeItem] = []

        for rel_path in changed_paths:
            filename = Path(rel_path).name
            if _is_ignored(filename):
                continue
            # Config files in sorted/ trigger a reload, not a document change
            if _is_config_file(rel_path):
                self.config_changed = True
                continue
            # Skip unsupported file extensions (prefilter handles this in full scans)
            ext = Path(filename).suffix.lower()
            if ext and ext not in SUPPORTED_EXTENSIONS:
                continue

            abs_path = self.root / rel_path
            was_known = rel_path in self._previous_state

            if abs_path.is_file() and not abs_path.is_symlink():
                # File exists — hash it
                try:
                    file_hash = compute_sha256(abs_path)
                    file_size = abs_path.stat().st_size
                except OSError:
                    continue

                if not was_known:
                    # New file — stray detection
                    location = self._get_location(rel_path)
                    if not self._is_known(file_hash, rel_path, db_snapshot):
                        if location not in ELIGIBLE_LOCATIONS:
                            if location == ".output":
                                self._delete_stray(rel_path)
                            else:
                                self._move_to_error(rel_path)
                            continue

                    changes.append(ChangeItem(
                        event_type=EventType.ADDITION,
                        path=rel_path,
                        hash=file_hash,
                        size=file_size,
                    ))

                # Update state (new or modified)
                self._previous_state[rel_path] = file_hash
            else:
                # File gone
                if was_known:
                    changes.append(ChangeItem(
                        event_type=EventType.REMOVAL,
                        path=rel_path,
                    ))
                    del self._previous_state[rel_path]

        return changes
