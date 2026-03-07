"""
Filesystem reconciliation for document watcher v2.

Performs actual file moves based on temporary fields set by reconcile().
"""

import logging
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from models import PathEntry, Record, State

logger = logging.getLogger(__name__)


def _move_file(src: Path, dest: Path) -> Path | None:
    """Move a file from src to dest, handling collisions and parent dirs.

    Creates parent directories as needed.
    If dest already exists, appends a UUID fragment to the filename.
    Uses Path.rename() for atomic move (same filesystem).

    Returns the actual destination path on success, None if source not found.
    """
    if not src.exists():
        logger.warning("Source file not found, skipping: %s", src)
        return None

    dest.parent.mkdir(parents=True, exist_ok=True)

    if dest.exists():
        stem = dest.stem
        suffix = dest.suffix
        unique_id = uuid4().hex[:8]
        dest = dest.with_name(f"{stem}_{unique_id}{suffix}")

    src.rename(dest)
    return dest


def _void_dest(root: Path, path: str) -> Path:
    """Build a void/ destination with today's date as subdirectory."""
    location, location_path, filename = Record._decompose_path(path)
    date_dir = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if location_path:
        return root / "void" / date_dir / location / location_path / filename
    return root / "void" / date_dir / location / filename


class FilesystemReconciler:
    """Performs filesystem moves based on Record temporary fields."""

    def __init__(self, root: Path):
        self.root = root

    def reconcile(self, records: list[Record]) -> None:
        """Process all records, performing filesystem operations for each."""
        for record in records:
            self._reconcile_one(record)

    def _reconcile_one(self, record: Record) -> None:
        """Process a single record's temporary fields."""

        # 1. source_reference: move source to archive/error/missing
        if record.source_reference:
            src = self.root / record.source_reference
            _, _, filename = Record._decompose_path(record.source_reference)
            if record.state == State.HAS_ERROR:
                dest = self.root / "error" / filename
            elif record.state == State.IS_MISSING:
                dest = self.root / "missing" / filename
            else:
                dest = self.root / "archive" / filename
            actual = _move_file(src, dest)
            ref = record.source_reference
            if actual is not None:
                # Update source_paths to reflect the new location so
                # reconcile doesn't retry the move next cycle.
                # Keep the old entry in missing_source_paths so that
                # came_from_sorted can still detect the original location.
                dest_path = str(actual.relative_to(self.root))
                now = datetime.now(timezone.utc)
                for i, pe in enumerate(record.source_paths):
                    if pe.path == ref:
                        record.missing_source_paths.append(pe)
                        record.source_paths[i] = PathEntry(dest_path, now)
                        break
            else:
                # Source file is gone — update record to reflect this
                # so reconcile doesn't keep retrying.
                for i, pe in enumerate(record.source_paths):
                    if pe.path == ref:
                        record.missing_source_paths.append(pe)
                        record.source_paths.pop(i)
                        break

        # 2. duplicate_sources: each to duplicates/{location}/{location_path}/{filename}
        for dup_path in record.duplicate_sources:
            src = self.root / dup_path
            location, location_path, filename = Record._decompose_path(dup_path)
            if location_path:
                dest = self.root / "duplicates" / location / location_path / filename
            else:
                dest = self.root / "duplicates" / location / filename
            _move_file(src, dest)

        # 3. current_reference + target_path: move current to target
        if record.current_reference and record.target_path:
            src = self.root / record.current_reference
            dest = self.root / record.target_path
            actual = _move_file(src, dest)
            ref = record.current_reference
            if actual is not None:
                # Update current_paths to reflect the actual location
                # (may differ from target_path due to collision renaming).
                actual_path = str(actual.relative_to(self.root))
                now = datetime.now(timezone.utc)
                for i, pe in enumerate(record.current_paths):
                    if pe.path == ref:
                        record.current_paths[i] = PathEntry(
                            actual_path, now,
                        )
                        break
                # Clean up .meta.json sidecar if moving from .output
                loc = Record._decompose_path(record.current_reference)[0]
                if loc == ".output":
                    sidecar = src.parent / f"{src.name}.meta.json"
                    if sidecar.exists():
                        try:
                            sidecar.unlink()
                        except OSError:
                            pass
            else:
                for i, pe in enumerate(record.current_paths):
                    if pe.path == ref:
                        record.missing_current_paths.append(pe)
                        record.current_paths.pop(i)
                        break

        # 4. deleted_paths: each to void/{date}/{location}/{filename}
        for del_path in record.deleted_paths:
            src = self.root / del_path
            location = Record._decompose_path(del_path)[0]
            dest = _void_dest(self.root, del_path)
            if _move_file(src, dest):
                # Clean up .meta.json sidecar if deleting from .output
                if location == ".output":
                    sidecar = src.parent / f"{src.name}.meta.json"
                    if sidecar.exists():
                        try:
                            sidecar.unlink()
                        except OSError:
                            pass

        # 5. needs_deletion: all source + current paths to void
        if record.state == State.NEEDS_DELETION:
            for pe in record.source_paths:
                src = self.root / pe.path
                _move_file(src, _void_dest(self.root, pe.path))

            for pe in record.current_paths:
                src = self.root / pe.path
                _move_file(src, _void_dest(self.root, pe.path))
