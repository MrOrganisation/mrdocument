"""
Orchestrator for document watcher v2.

Ties together steps 1-4 into a single polling cycle that handles
filesystem detection, preprocessing, service calls, reconciliation,
and filesystem moves.
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from db_new import DocumentDBv2
from models import PathEntry, Record, State
from prefilter import prefilter
from step1 import FilesystemDetector, compute_sha256
from step2 import preprocess, reconcile
from step3 import Processor
from step4 import FilesystemReconciler
from step5 import SmartFolderReconciler, RootSmartFolderReconciler
from step6 import AudioLinkReconciler

logger = logging.getLogger(__name__)


def context_field_names_from_sorter(context_manager) -> dict[str, list[str]]:
    """Extract context field names from a SorterContextManager.

    Bridge between sorter.py's ContextConfig and the v2 pipeline.
    Standalone function so the orchestrator doesn't depend on sorter internals.
    """
    return {name: ctx.field_names for name, ctx in context_manager.contexts.items()}


def context_folders_from_sorter(context_manager) -> dict[str, list[str]]:
    """Extract context folder hierarchy from a SorterContextManager.

    Maps context name to its folder field list (e.g., {"arbeit": ["context", "sender"]}).
    Used by reconcile to build sorted/ directory paths.
    """
    return {name: ctx.folders for name, ctx in context_manager.contexts.items()}


def _build_recompute_filename(context_manager) -> callable:
    """Build a callback that recomputes assigned_filename from record metadata.

    Uses the context's filename pattern (including conditional rules) and
    the record's original_filename for pattern matching / {source_filename}.
    """
    from sorter import _format_filename

    def recompute(record: Record) -> Optional[str]:
        if not record.context or not record.metadata:
            return None
        ctx = context_manager.contexts.get(record.context)
        if not ctx:
            return None
        pattern = ctx.resolve_filename_pattern(record.original_filename)
        metadata = dict(record.metadata)
        metadata.setdefault("context", record.context)
        new_name = _format_filename(metadata, pattern, source_filename=record.original_filename)
        # _format_filename always appends .pdf — preserve the original extension
        # when the record's assigned_filename uses a different one (e.g. .txt).
        if record.assigned_filename:
            orig_ext = Path(record.assigned_filename).suffix.lower()
            if orig_ext and orig_ext != ".pdf":
                new_name = Path(new_name).stem + orig_ext
        return new_name

    return recompute


def contexts_for_api_from_sorter(context_manager) -> list[dict]:
    """Get all contexts as raw dicts for mrdocument API calls.

    Loads the full YAML dict for each context (with fields, description, etc.)
    which is needed by /classify_audio and /classify_transcript endpoints.
    """
    result = []
    for name in context_manager.contexts:
        ctx = context_manager.get_context_for_api(name)
        if ctx:
            result.append(ctx)
    return result


class DocumentWatcherV2:
    """Main orchestrator: polling cycle that ties steps 1-4 together."""

    def __init__(
        self,
        root: Path,
        db: DocumentDBv2,
        service_url: str,
        context_field_names: Optional[dict[str, list[str]]] = None,
        context_folders: Optional[dict[str, list[str]]] = None,
        poll_interval: float = 5.0,
        processor_timeout: float = 900.0,
        stt_url: Optional[str] = None,
        contexts_for_api: Optional[list] = None,
        smart_folders: Optional[list] = None,
        root_smart_folders: Optional[list] = None,
        audio_links: bool = False,
        max_concurrent: int = 5,
        name: Optional[str] = None,
        context_manager=None,
    ):
        self.root = root
        self.name = name or root.name
        self.db = db
        self.service_url = service_url
        self.context_field_names = context_field_names
        self.context_folders = context_folders
        self.poll_interval = poll_interval
        self._in_flight: set = set()
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self.context_manager = context_manager
        self._pending_full_scan = False
        self._audio_links = audio_links
        self._recompute_filename = None

        self.detector = FilesystemDetector(root)
        self.processor = Processor(
            root, service_url,
            stt_url=stt_url,
            timeout=processor_timeout,
            contexts=contexts_for_api,
            context_manager=context_manager,
        )
        self.reconciler = FilesystemReconciler(root)
        self.smart_folder_reconciler = (
            SmartFolderReconciler(root, smart_folders)
            if smart_folders else None
        )
        self.root_smart_folder_reconciler = (
            RootSmartFolderReconciler(root, root_smart_folders)
            if root_smart_folders else None
        )
        self.audio_link_reconciler = (
            AudioLinkReconciler(root) if audio_links else None
        )

    def _read_sidecar(self, output_path: str) -> dict:
        """Read sidecar JSON for an .output file.

        Args:
            output_path: Relative path like ".output/uuid-123"

        Returns:
            Parsed sidecar dict, or {} if not found/parse error.
        """
        sidecar_path = self.root / f"{output_path}.meta.json"
        try:
            return json.loads(sidecar_path.read_text())
        except (FileNotFoundError, json.JSONDecodeError, OSError) as e:
            logger.warning("[%s] Failed to read sidecar %s: %s", self.name, sidecar_path, e)
            return {}

    async def _process_background(self, record: Record) -> None:
        """Process a single record in the background, respecting the semaphore.

        After process_one writes to .output/, immediately ingests the result
        and clears output_filename before releasing _in_flight.  This prevents
        the race where step 7 re-launches a completed record.
        """
        output_filename = record.output_filename
        record_id = record.id
        try:
            async with self._semaphore:
                await self.processor.process_one(record)

            # Ingest result immediately to close the re-launch gap
            await self._ingest_output(record_id, output_filename)
        except Exception as e:
            logger.error("[%s] Processing failed for %s: %s", self.name, output_filename, e)
            await self._clear_output_filename(record_id, output_filename)
        finally:
            self._in_flight.discard(record_id)

    async def _ingest_output(self, record_id, output_filename: str) -> None:
        """Ingest a completed .output file into the record.

        Reads sidecar metadata, computes hash, updates current_paths,
        and clears output_filename so step 7 won't re-launch.
        """
        output_path = self.root / ".output" / output_filename
        if not output_path.exists():
            return

        if output_path.stat().st_size == 0:
            await self._clear_output_filename(record_id, output_filename,
                                               error=True)
            return

        sidecar = self._read_sidecar(f".output/{output_filename}")
        file_hash = compute_sha256(output_path)
        now = datetime.now(timezone.utc)

        # Re-fetch record from DB (reconcile may have modified it)
        snapshot = await self.db.get_snapshot(self.name)
        fresh = next((r for r in snapshot if r.id == record_id), None)
        if fresh is None or fresh.output_filename != output_filename:
            return

        # Duplicate hash check: if another record already has this hash,
        # discard the output and mark as error (same logic as step2).
        is_dup = any(
            r is not fresh and (r.source_hash == file_hash or (r.hash is not None and r.hash == file_hash))
            for r in snapshot
        )
        if is_dup:
            logger.warning(
                "[%s] Duplicate hash %s for output %s, discarding",
                self.name, file_hash, output_filename,
            )
            fresh.deleted_paths.append(f".output/{output_filename}")
            remaining = []
            for pe in fresh.source_paths:
                if Record._decompose_path(pe.path)[0] == "archive":
                    fresh.duplicate_sources.append(pe.path)
                else:
                    remaining.append(pe)
            fresh.source_paths = remaining
            fresh.state = State.HAS_ERROR
            fresh.output_filename = None
            await self.db.save_record(fresh)
            return

        if not fresh.context:
            fresh.context = sidecar.get("context")
        fresh.metadata = sidecar.get("metadata")
        fresh.assigned_filename = sidecar.get("assigned_filename")
        fresh.hash = file_hash
        fresh.current_paths.append(
            PathEntry(f".output/{output_filename}", now)
        )
        fresh.output_filename = None
        await self.db.save_record(fresh)

    async def _clear_output_filename(self, record_id, output_filename: str,
                                      error: bool = False) -> None:
        """Clear output_filename on a record (e.g. after processing error)."""
        try:
            snapshot = await self.db.get_snapshot(self.name)
            fresh = next((r for r in snapshot if r.id == record_id), None)
            if fresh and fresh.output_filename == output_filename:
                if error:
                    fresh.state = State.HAS_ERROR
                fresh.output_filename = None
                await self.db.save_record(fresh)
        except Exception:
            pass

    async def shutdown(self) -> None:
        """Stop observer and wait for in-flight processing tasks."""
        self.detector.stop()
        tasks = [t for t in asyncio.all_tasks()
                 if t.get_name().startswith("process-")]
        if tasks:
            logger.info("[%s] Waiting for %d processing tasks...", self.name, len(tasks))
            await asyncio.gather(*tasks, return_exceptions=True)

    def reload_config(self) -> None:
        """Reload context and smart folder config from disk."""
        if self.context_manager is None:
            return

        self.context_manager.load()
        self.context_field_names = context_field_names_from_sorter(self.context_manager)
        self.context_folders = context_folders_from_sorter(self.context_manager)
        self._recompute_filename = _build_recompute_filename(self.context_manager)
        self.processor.contexts = contexts_for_api_from_sorter(self.context_manager)
        self.processor.context_manager = self.context_manager

        # Reload smart folders from sorted/ files, with fallback to embedded
        from app import _load_smart_folders, _load_root_smart_folders
        smart_folders = _load_smart_folders(self.context_manager)
        if smart_folders:
            self.smart_folder_reconciler = SmartFolderReconciler(self.root, smart_folders)
        else:
            self.smart_folder_reconciler = None

        # Reload root-level smart folders
        root_smart_folders = _load_root_smart_folders(self.root)
        if root_smart_folders:
            self.root_smart_folder_reconciler = RootSmartFolderReconciler(self.root, root_smart_folders)
        else:
            self.root_smart_folder_reconciler = None

        logger.info("[%s] Config reloaded: %d context(s)", self.name, len(self.context_manager.contexts))

    @staticmethod
    def _record_snapshot(record: Record) -> tuple:
        """Snapshot persistent fields for change detection."""
        return (
            record.state,
            record.output_filename,
            record.context,
            record.assigned_filename,
            record.hash,
            tuple(record.source_paths),
            tuple(record.current_paths),
            tuple(record.missing_source_paths),
            tuple(record.missing_current_paths),
            repr(record.metadata),
        )

    async def wait_for_quiet(self, debounce_seconds: float) -> None:
        """Wait until no events arrive for debounce_seconds."""
        while True:
            got_more = await self.detector.wait_for_event(debounce_seconds)
            if not got_more:
                return

    async def run_cycle(self, full_scan: bool = True) -> bool:
        """Execute one complete pipeline cycle.

        Args:
            full_scan: If True, run prefilter and full filesystem scan
                       in the detector (vs incremental inotify-only).
                       Symlink reconciliation runs on every cycle.

        Returns:
            True if any record state transitions occurred (caller should
            re-run the cycle to let downstream steps act on the new states).
        """
        t0 = time.monotonic()

        # 0. Move unsupported file types to error/ (full scan only)
        if full_scan:
            prefilter(self.root)
        t_prefilter = time.monotonic()

        # 1. Get current DB snapshot (filtered by username)
        snapshot = await self.db.get_snapshot(self.name)

        # 2. Detect filesystem changes
        changes = await self.detector.detect(snapshot)
        t_detect = time.monotonic()

        # Check if config files changed (triggers reload + full scan)
        if self.detector.config_changed:
            self.detector.config_changed = False
            self.reload_config()
            self._pending_full_scan = True

        if changes:
            logger.info(
                "[%s] Cycle: %d changes detected, %d records in DB",
                self.name, len(changes), len(snapshot),
            )
            for c in changes:
                logger.info("[%s]   change: %s %s", self.name, c.event_type.value, c.path)

        # 3-6. Preprocess if there are changes
        modified: list = []
        new_records: list = []
        if changes:
            modified, new_records = preprocess(changes, snapshot, self._read_sidecar)

            if modified or new_records:
                logger.info(
                    "[%s] Preprocess: %d modified, %d new",
                    self.name, len(modified), len(new_records),
                )

            for record in new_records:
                record.username = self.name
                await self.db.create_record(record)

            for record in modified:
                await self.db.save_record(record)

        # 7. Launch processing as background tasks (non-blocking)
        to_process = await self.db.get_records_with_output_filename(self.name)
        new_launches = [r for r in to_process if r.id not in self._in_flight]
        if new_launches:
            logger.info("[%s] Launching %d processing tasks (%d already in-flight)",
                        self.name, len(new_launches), len(self._in_flight))
        for record in new_launches:
            logger.info(
                "[%s]   process: %s state=%s src=%s",
                self.name, record.output_filename, record.state.value,
                record.source_file.path if record.source_file else "none",
            )
            self._in_flight.add(record.id)
            asyncio.create_task(self._process_background(record),
                                name=f"process-{record.output_filename}")

        # 8. Reconcile all records (only save when something changed)
        t_reconcile = time.monotonic()
        all_records = await self.db.get_snapshot(self.name)
        saves = 0
        state_transitions = 0
        for record in all_records:
            snap_before = self._record_snapshot(record)
            old_state = record.state
            result = reconcile(record, self.context_field_names, self.context_folders,
                              recompute_filename=self._recompute_filename)
            if result is None:
                logger.info("[%s] Reconcile: DELETE %s", self.name, record.id)
                await self.db.delete_record(record.id)
                saves += 1
            else:
                snap_after = self._record_snapshot(result)
                has_temp = (result.target_path or result.source_reference
                            or result.current_reference or result.duplicate_sources
                            or result.deleted_paths)
                changed = has_temp or snap_after != snap_before
                if result.state != old_state:
                    state_transitions += 1
                    logger.info(
                        "[%s] Reconcile: %s %s→%s ofn=%s src_ref=%s",
                        self.name, result.original_filename,
                        old_state.value, result.state.value,
                        result.output_filename,
                        result.source_reference,
                    )
                if changed:
                    await self.db.save_record(result)
                    saves += 1

        t_fs = time.monotonic()
        # 9. Filesystem reconcile (move files based on temp fields)
        actionable = await self.db.get_records_with_temp_fields(self.name)
        if actionable:
            logger.info("[%s] Filesystem reconcile: %d records", self.name, len(actionable))
        self.reconciler.reconcile(actionable)

        # 10. Clear temp fields and finalize
        for record in actionable:
            record.clear_temporary_fields()
            if record.state == State.NEEDS_DELETION:
                record.state = State.IS_DELETED
            await self.db.save_record(record)

        t_symlink = time.monotonic()
        # 11. Symlink reconciliation (smart folders + root smart folders + audio links)
        if self.smart_folder_reconciler or self.root_smart_folder_reconciler or self.audio_link_reconciler:
            complete_records = [
                r for r in await self.db.get_snapshot(self.name)
                if r.state == State.IS_COMPLETE
            ]
            sorted_records = [
                r for r in complete_records
                if r.current_location == "sorted"
            ]
            if self.smart_folder_reconciler:
                if sorted_records:
                    self.smart_folder_reconciler.reconcile(sorted_records)
                self.smart_folder_reconciler.cleanup_orphans()
            if self.root_smart_folder_reconciler:
                if sorted_records:
                    self.root_smart_folder_reconciler.reconcile(sorted_records)
                self.root_smart_folder_reconciler.cleanup_orphans()
            if self.audio_link_reconciler:
                audio_records = [
                    r for r in complete_records
                    if r.current_location in AudioLinkReconciler.LINK_LOCATIONS
                ]
                if audio_records:
                    self.audio_link_reconciler.reconcile(audio_records)
                self.audio_link_reconciler.cleanup_orphans()

        had_activity = bool(state_transitions or actionable
                            or modified or new_records)

        elapsed = time.monotonic() - t0
        logger.info(
            "[%s] Cycle done: %.1fs (prefilter=%.2f detect=%.2f reconcile=%.2f fs=%.2f symlink=%.2f)"
            ", %d records, %d changes, %d reconcile saves, %d state transitions",
            self.name, elapsed,
            t_prefilter - t0, t_detect - t_prefilter,
            t_fs - t_reconcile, t_symlink - t_fs,
            time.monotonic() - t_symlink,
            len(all_records), len(changes) if changes else 0, saves,
            state_transitions,
        )
        return had_activity

