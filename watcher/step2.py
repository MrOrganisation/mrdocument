"""
Preprocessing and reconciliation for document watcher v2.

Pure functions: no I/O, no DB, no filesystem operations.
"""

import logging
import re
from datetime import datetime, timezone
from typing import Callable, Optional
from uuid import uuid4

from models import State, EventType, PathEntry, ChangeItem, Record

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Location-aware matching table
# ---------------------------------------------------------------------------
# Each entry: (match_fields_in_priority_order, allows_new_records)
# "source_hash" → compare change.hash vs record.source_hash → source_paths
# "hash"        → compare change.hash vs record.hash        → current_paths

LOCATION_CONFIG: dict[str, tuple[list[str], bool]] = {
    "incoming":   (["source_hash", "hash"], True),
    "sorted":     (["hash", "source_hash"], True),
    "archive":    (["source_hash"], False),
    "missing":    (["source_hash"], False),
    "processed":  (["hash"], False),
    "reviewed":   (["hash"], False),
    "reset":      (["hash"], False),
    "trash":      (["source_hash", "hash"], False),
}

# Valid locations for current_paths entries
VALID_CURRENT_LOCATIONS = {".output", "processed", "reset", "reviewed", "sorted"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _find_by_source_hash(records: list[Record], hash_value: str) -> Optional[Record]:
    """Find first record whose source_hash matches."""
    for r in records:
        if r.source_hash == hash_value:
            return r
    return None


def _find_by_hash(records: list[Record], hash_value: str) -> Optional[Record]:
    """Find first record whose hash matches."""
    for r in records:
        if r.hash is not None and r.hash == hash_value:
            return r
    return None


def _find_by_output_filename(records: list[Record], filename: str) -> Optional[Record]:
    """Find first record whose output_filename matches."""
    for r in records:
        if r.output_filename == filename:
            return r
    return None


def _is_duplicate_hash(records: list[Record], hash_value: str, exclude: Record) -> bool:
    """Check if hash_value matches any other record's source_hash or hash."""
    if not hash_value:
        return False
    for r in records:
        if r is exclude:
            continue
        if r.source_hash == hash_value:
            return True
        if r.hash is not None and r.hash == hash_value:
            return True
    return False


def compute_target_path(
    record: Record,
    context_folders: Optional[dict[str, list[str]]] = None,
) -> Optional[str]:
    """Compute expected target path in sorted/ for a record.

    Uses context_folders config to build subdirectory hierarchy from metadata.
    Falls back to sorted/{context}/{filename} if folders not configured.
    """
    if not record.context or not record.assigned_filename:
        return None

    folders = (context_folders or {}).get(record.context)
    if folders and record.metadata:
        parts = []
        for field in folders:
            value = record.metadata.get(field)
            if value:
                parts.append(str(value))
            else:
                break
        if parts:
            return "sorted/" + "/".join(parts) + f"/{record.assigned_filename}"

    return f"sorted/{record.context}/{record.assigned_filename}"


# Regex matching collision suffixes: _<8 hex chars> before the file extension
# e.g. "file_e3ca2c9b.pdf" matches, "file.pdf" does not
_COLLISION_SUFFIX_RE = re.compile(r"_[0-9a-f]{8}(\.[^.]+)$")


def _is_collision_variant(actual_path: str, expected_path: str) -> bool:
    """Check if actual_path is expected_path or a collision-suffixed variant.

    step4._move_file appends _<8 hex chars> on collision.  A file at
    sorted/.../name_e3ca2c9b.pdf is effectively "at" sorted/.../name.pdf.
    """
    if actual_path == expected_path:
        return True
    # Strip collision suffix from actual and compare
    stripped = _COLLISION_SUFFIX_RE.sub(r"\1", actual_path)
    return stripped == expected_path


# ---------------------------------------------------------------------------
# preprocess
# ---------------------------------------------------------------------------

def _mark_modified(record: Record, modified: list[Record], modified_ids: set) -> None:
    """Add record to modified list if not already there."""
    if record.id not in modified_ids:
        modified.append(record)
        modified_ids.add(record.id)


def preprocess(
    changes: list[ChangeItem],
    records: list[Record],
    read_sidecar: Callable[[str], dict],
) -> tuple[list[Record], list[Record]]:
    """Process filesystem changes against existing records.

    Args:
        changes: Detected filesystem changes from step1.
        records: Current database records.
        read_sidecar: Callable that reads sidecar JSON for an .output path.

    Returns:
        (modified_records, new_records)
    """
    modified: list[Record] = []
    new_records: list[Record] = []
    modified_ids: set = set()
    now = datetime.now(timezone.utc)

    for change in changes:
        if change.event_type == EventType.ADDITION:
            _handle_addition(
                change, records, read_sidecar,
                modified, modified_ids, new_records, now,
            )
        elif change.event_type == EventType.REMOVAL:
            _handle_removal(change, records, modified, modified_ids)

    return modified, new_records


def _handle_addition(
    change: ChangeItem,
    records: list[Record],
    read_sidecar: Callable[[str], dict],
    modified: list[Record],
    modified_ids: set,
    new_records: list[Record],
    now: datetime,
) -> None:
    location, _, filename = Record._decompose_path(change.path)

    # .output handling
    if location == ".output":
        record = _find_by_output_filename(records, filename)
        if record is None:
            return
        if change.size == 0:
            record.state = State.HAS_ERROR
            record.output_filename = None
        else:
            sidecar = read_sidecar(change.path)
            if _is_duplicate_hash(records, change.hash, record):
                logger.warning(
                    "Duplicate hash %s for output %s, discarding result",
                    change.hash, change.path,
                )
                # Discard the .output file
                record.deleted_paths.append(change.path)
                # Move archive source to duplicates, keep non-archive sources
                remaining = []
                for pe in record.source_paths:
                    if Record._decompose_path(pe.path)[0] == "archive":
                        record.duplicate_sources.append(pe.path)
                    else:
                        remaining.append(pe)
                record.source_paths = remaining
                record.state = State.HAS_ERROR
                record.output_filename = None
                _mark_modified(record, modified, modified_ids)
                return
            # Preserve pre-set context (e.g., forced from sorted/ directory)
            if not record.context:
                record.context = sidecar.get("context")
            record.metadata = sidecar.get("metadata")
            record.assigned_filename = sidecar.get("assigned_filename")
            record.hash = change.hash
            record.current_paths.append(PathEntry(change.path, now))
            record.output_filename = None
        _mark_modified(record, modified, modified_ids)
        return

    # Non-.output locations
    config = LOCATION_CONFIG.get(location)
    if config is None:
        return

    match_fields, allows_new = config
    matched_record = None
    matched_field = None

    for field in match_fields:
        if field == "source_hash" and change.hash:
            matched_record = (
                _find_by_source_hash(records, change.hash)
                or _find_by_source_hash(new_records, change.hash)
            )
        elif field == "hash" and change.hash:
            matched_record = (
                _find_by_hash(records, change.hash)
                or _find_by_hash(new_records, change.hash)
            )
        if matched_record:
            matched_field = field
            break

    if matched_record:
        # Check if this path is already tracked — avoid duplicate entries
        # on restart when _detect_full re-emits ADDITIONs for known files.
        if matched_field == "hash":
            path_list = matched_record.current_paths
        else:
            path_list = matched_record.source_paths
        already_tracked = any(pe.path == change.path for pe in path_list)
        if not already_tracked:
            path_list.append(PathEntry(change.path, now))
            _mark_modified(matched_record, modified, modified_ids)
    elif allows_new:
        new_record = Record(
            original_filename=filename,
            source_hash=change.hash,
            source_paths=[PathEntry(change.path, now)],
        )
        # Pre-set context from directory for sorted/ files
        if location == "sorted":
            _, loc_path, _ = Record._decompose_path(change.path)
            if loc_path:
                new_record.context = loc_path.split("/")[0]
        new_records.append(new_record)


def _handle_removal(
    change: ChangeItem,
    records: list[Record],
    modified: list[Record],
    modified_ids: set,
) -> None:
    for record in records:
        # Check source_paths
        for pe in record.source_paths:
            if pe.path == change.path:
                record.source_paths.remove(pe)
                record.missing_source_paths.append(pe)
                _mark_modified(record, modified, modified_ids)
                return

        # Check current_paths
        for pe in record.current_paths:
            if pe.path == change.path:
                record.current_paths.remove(pe)
                record.missing_current_paths.append(pe)
                _mark_modified(record, modified, modified_ids)
                return


# ---------------------------------------------------------------------------
# reconcile
# ---------------------------------------------------------------------------

def reconcile(
    record: Record,
    context_field_names: Optional[dict[str, list[str]]] = None,
    context_folders: Optional[dict[str, list[str]]] = None,
    recompute_filename: Optional[Callable[[Record], Optional[str]]] = None,
) -> Optional[Record]:
    """Reconcile a record's state based on its paths and fields.

    Args:
        record: The record to reconcile (modified in place).
        context_field_names: Maps context name to its field_names for
            metadata completeness checks.
        context_folders: Maps context name to folder field list.
        recompute_filename: Callback that recomputes assigned_filename for a
            record from its metadata and context config.  Used by reset/.

    Returns:
        Modified record, or None if the record should be deleted.
    """
    record.clear_temporary_fields()

    # ------------------------------------------------------------------
    # Phase 1: source_paths
    # ------------------------------------------------------------------
    if record.source_paths:
        sf = record.source_file
        s_loc = Record._decompose_path(sf.path)[0]

        if s_loc == "trash":
            record.state = State.NEEDS_DELETION
            return record

        if record.state == State.IS_NEW:
            record.output_filename = str(uuid4())
            record.state = State.NEEDS_PROCESSING
            record.source_reference = sf.path
            now = datetime.now(timezone.utc)
            record.source_paths.append(
                PathEntry(f"archive/{record.source_filename}", now)
            )
            return record

        # IS_COMPLETE: new source is a duplicate — never reprocess.
        if (record.state == State.IS_COMPLETE
                and s_loc not in ("archive", "missing")):
            for pe in record.source_paths:
                pe_loc = Record._decompose_path(pe.path)[0]
                if pe_loc not in ("archive", "missing"):
                    record.duplicate_sources.append(pe.path)
            record.source_paths = [
                pe for pe in record.source_paths
                if Record._decompose_path(pe.path)[0] in ("archive", "missing")
            ]
            return record

        if (s_loc not in ("archive", "missing")
                and record.state != State.HAS_ERROR):
            record.source_reference = sf.path
            now = datetime.now(timezone.utc)
            new_paths = []
            for pe in record.source_paths:
                pe_loc = Record._decompose_path(pe.path)[0]
                if pe_loc in ("archive", "missing"):
                    new_paths.append(pe)
                elif pe.path != sf.path:
                    record.duplicate_sources.append(pe.path)
            new_paths.append(
                PathEntry(f"archive/{record.source_filename}", now)
            )
            record.source_paths = new_paths

        # Recovery: record with new source in processable location → reprocess.
        # Includes NEEDS_PROCESSING with output_filename cleared (previous
        # processing completed but state not yet IS_COMPLETE due to same-cycle
        # arrival of new source + old output reaching processed/).
        recoverable = record.state == State.IS_MISSING or (
            record.state == State.NEEDS_PROCESSING
            and record.output_filename is None
        )
        if (recoverable
                and s_loc not in ("archive", "missing", "error")):
            record.output_filename = str(uuid4())
            record.state = State.NEEDS_PROCESSING
            # Clear stale processing results
            record.context = None
            record.metadata = None
            record.assigned_filename = None
            record.hash = None
            record.missing_current_paths = []
            if record.current_paths:
                for pe in record.current_paths:
                    record.deleted_paths.append(pe.path)
                record.current_paths = []
            return record

    # ------------------------------------------------------------------
    # Phase 2: has_error
    # ------------------------------------------------------------------
    if record.state == State.HAS_ERROR:
        s_loc = record.source_location

        # Orphan record: source already in error/ and no current paths
        if s_loc == "error" and not record.current_paths:
            return None

        # No source paths at all (file was deleted externally) — clean up
        if s_loc is None and not record.current_paths:
            return None

        # Recovery: new source in a processable location → retry
        if s_loc in ("incoming", "sorted"):
            record.state = State.IS_NEW
            record.output_filename = str(uuid4())
            record.state = State.NEEDS_PROCESSING
            sf = record.source_file
            record.source_reference = sf.path
            now = datetime.now(timezone.utc)
            # Clean up stale source_paths (old archive entries etc.)
            new_paths = [pe for pe in record.source_paths if pe.path == sf.path]
            new_paths.append(
                PathEntry(f"archive/{record.source_filename}", now)
            )
            record.source_paths = new_paths
            # Clear stale processing state
            record.context = None
            record.metadata = None
            record.assigned_filename = None
            record.hash = None
            if record.current_paths:
                for pe in record.current_paths:
                    record.deleted_paths.append(pe.path)
                record.current_paths = []
            return record

        if s_loc == "archive":
            record.source_reference = record.source_file.path
            record.duplicate_sources = []

        if record.current_paths:
            for pe in record.current_paths:
                record.deleted_paths.append(pe.path)
            record.current_paths = []

        return record

    # ------------------------------------------------------------------
    # Phase 3: current_paths
    # ------------------------------------------------------------------

    # Trash detection via current_paths (processed file in trash/)
    if any(Record._decompose_path(pe.path)[0] == "trash" for pe in record.current_paths):
        record.state = State.NEEDS_DELETION
        return record

    # Early returns for special states
    if record.state == State.IS_NEW:
        record.output_filename = str(uuid4())
        record.state = State.NEEDS_PROCESSING
        return record

    if record.state == State.IS_DELETED:
        return None

    if record.state == State.NEEDS_PROCESSING and not record.current_paths:
        return record

    # Missing detection
    if not record.current_paths:
        if record.missing_current_paths:
            record.state = State.IS_MISSING
            # Move source from archive to missing/
            if record.source_paths:
                sf = record.source_file
                if Record._decompose_path(sf.path)[0] == "archive":
                    record.source_reference = sf.path
        return record

    # Reappearance
    if record.state == State.IS_MISSING:
        record.state = State.IS_COMPLETE

    # Invalid location cleanup
    valid = []
    for pe in record.current_paths:
        loc = Record._decompose_path(pe.path)[0]
        if loc in VALID_CURRENT_LOCATIONS:
            valid.append(pe)
        else:
            record.deleted_paths.append(pe.path)
    record.current_paths = valid

    if not record.current_paths:
        return record

    # Deduplicate: keep most recent
    if len(record.current_paths) > 1:
        sorted_paths = sorted(record.current_paths, key=lambda pe: pe.timestamp)
        for pe in sorted_paths[:-1]:
            record.deleted_paths.append(pe.path)
        record.current_paths = [sorted_paths[-1]]

    # Single current path handling
    current = record.current_paths[0]
    c_loc = Record._decompose_path(current.path)[0]

    if c_loc == ".output":
        if record.assigned_filename:
            # If source was in sorted/, return to sorted/ instead of processed/
            came_from_sorted = any(
                Record._decompose_path(pe.path)[0] == "sorted"
                for pe in list(record.source_paths) + list(record.missing_source_paths)
            )
            if came_from_sorted:
                target = compute_target_path(record, context_folders)
                record.target_path = target or f"processed/{record.assigned_filename}"
            else:
                record.target_path = f"processed/{record.assigned_filename}"
            record.current_reference = current.path
        return record

    if c_loc == "processed":
        record.state = State.IS_COMPLETE
        return record

    if c_loc == "reviewed":
        target = compute_target_path(record, context_folders)
        if target:
            record.target_path = target
            record.current_reference = current.path
        return record

    if c_loc == "reset":
        if recompute_filename:
            new_name = recompute_filename(record)
            if new_name:
                record.assigned_filename = new_name
        target = compute_target_path(record, context_folders)
        if target:
            record.target_path = target
            record.current_reference = current.path
        record.state = State.IS_COMPLETE
        return record

    if c_loc == "sorted":
        # Adopt user changes: if user renamed or moved the file, update record
        current_filename = current.path.rsplit("/", 1)[-1]
        if record.assigned_filename and current_filename != record.assigned_filename:
            if not _is_collision_variant(current.path,
                    compute_target_path(record, context_folders) or ""):
                record.assigned_filename = current_filename

        _, loc_path, _ = Record._decompose_path(current.path)
        if loc_path:
            dir_context = loc_path.split("/")[0]
            if dir_context != record.context:
                record.context = dir_context

        # Metadata completeness check
        if context_field_names and record.context:
            field_names = context_field_names.get(record.context, [])
            if not isinstance(record.metadata, dict):
                record.metadata = {}
            for fn in field_names:
                if fn not in record.metadata:
                    record.metadata[fn] = None

        record.state = State.IS_COMPLETE
        return record

    return record
