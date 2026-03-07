# MrDocument Watcher -- Implementation Documentation

## Module Overview

```
mrdocument/watcher/
  app.py           -- Application entry point, multi-user discovery, health server
  main.py          -- Entry point wrapper (calls app.main())
  orchestrator.py  -- Pipeline orchestrator (DocumentWatcherV2)
  step1.py         -- Filesystem detection (FilesystemDetector)
  step2.py         -- Preprocessing and reconciliation (pure functions)
  step3.py         -- Processing service calls (Processor)
  step4.py         -- Filesystem moves (FilesystemReconciler)
  step5.py         -- Smart folder symlinks (SmartFolderReconciler, RootSmartFolderReconciler)
  step6.py         -- Audio link symlinks (AudioLinkReconciler)
  prefilter.py     -- Unsupported file type filtering
  models.py        -- Data models (Record, State, PathEntry, ChangeItem)
  db_new.py        -- PostgreSQL persistence (DocumentDBv2)
  sorter.py        -- Context and config management (SorterContextManager)
```


## app.py -- Application Bootstrap

### HealthServer

Lightweight HTTP server on configurable port (default 8080). Returns 503 during startup, 200 when all user watchers are initialized.

### main()

Async entry point:
1. Connects to PostgreSQL via `DATABASE_URL`.
2. Starts health server.
3. Discovers user directories via glob patterns from `WatcherConfig`.
4. For each user root: calls `setup_user()`, spawns `run_watcher()` task.
5. Periodically rescans for new user directories.
6. Graceful shutdown on KeyboardInterrupt.

### setup_user(root, db)

1. Creates all required directories (`REQUIRED_DIRS`).
2. Loads `SorterContextManager` for context parsing.
3. Extracts `context_field_names`, `context_folders`, `contexts_for_api`.
4. Loads smart folders from sorted/ configs and root `smartfolders.yaml`.
5. Runs V1 -> V2 migration (idempotent).
6. Returns configured `DocumentWatcherV2`.

### run_watcher(watcher, config)

Per-user event loop:
1. Initial full scan cycle.
2. Wait for inotify events with debounce.
3. Run incremental cycle on events.
4. Periodic full scan every `full_scan_seconds`.
5. If config changed: reload and force full scan.

### _load_smart_folders(context_manager)

Loads smart folder configs from `sorted/{context}/smartfolders.yaml` files. Falls back to `smart_folders` embedded in context YAML. Returns list of `SmartFolderEntry`.

### _load_root_smart_folders(root)

Parses `{root}/smartfolders.yaml`. Validates each entry has `context`, `path`, and at least a condition. Resolves relative paths against root. Returns list of `RootSmartFolderEntry` or None.


## orchestrator.py -- DocumentWatcherV2

### Constructor

Parameters: `root`, `db`, `service_url`, `context_field_names`, `context_folders`, `poll_interval`, `processor_timeout`, `stt_url`, `contexts_for_api`, `smart_folders`, `root_smart_folders`, `audio_links`, `max_concurrent`, `name`, `context_manager`.

Creates: `FilesystemDetector`, `Processor`, `FilesystemReconciler`, `SmartFolderReconciler`, `RootSmartFolderReconciler`, `AudioLinkReconciler`.

### run_cycle(full_scan=True)

The core pipeline method. Returns `True` if state transitions occurred.

**Step 0 -- Prefilter** (full scan only): Move unsupported files to `error/`.

**Step 1 -- Detect**: Get DB snapshot, detect filesystem changes. Check for config file changes.

**Steps 2-6 -- Preprocess**: Apply changes to records. Create new records, save modified records.

**Step 7 -- Launch processing**: Find records with `output_filename` set. Launch background tasks via `asyncio.create_task()` with semaphore-based concurrency control. Tasks are tracked by record ID in `_in_flight`.

**Step 8 -- Reconcile**: Fresh DB snapshot. For each record: snapshot before, call `reconcile()`, snapshot after. Save if changed. Delete if `reconcile()` returns None.

**Step 9 -- Filesystem reconcile**: Fetch records with temp fields. Call `FilesystemReconciler.reconcile()`.

**Step 10 -- Finalize**: Clear temp fields. Transition `needs_deletion` -> `is_deleted`. Save.

**Step 11 -- Symlinks**: For IS_COMPLETE records in sorted/: run smart folder, root smart folder, and audio link reconcilers + cleanup.

### reload_config()

Called when config files change. Reloads context manager, rebuilds field names/folders/API contexts, recreates smart folder reconcilers.

### Helper Functions

- `context_field_names_from_sorter()` -- Extracts `{context: [field_names]}` from context manager.
- `context_folders_from_sorter()` -- Extracts `{context: [folders]}` from context manager.
- `contexts_for_api_from_sorter()` -- Gets full context dicts for API calls.


## step1.py -- FilesystemDetector

### Watched Directories

- **Direct (non-recursive)**: `archive`, `incoming`, `reviewed`, `processed`, `trash`, `.output`
- **Recursive**: `sorted`

### detect(db_snapshot)

First call: full scan + start inotify observer.
Subsequent calls: process accumulated inotify events.

**Full scan:**
1. Walk all watched directories, compute SHA-256 for each file.
2. Compare against `_previous_state` dict.
3. Emit additions for new/changed files, removals for disappeared files.
4. Stray detection: unknown files in non-eligible locations moved to `error/`.

**Incremental:**
1. Process `_pending_events` from inotify.
2. Compute SHA-256 for added files.
3. Stray detection on additions.

### compute_sha256(path)

SHA-256 hash, reading in 64KB chunks.

### _ChangeCollector

Watchdog `FileSystemEventHandler` that collects events into an `asyncio.Queue`. Filters hidden/temp/Syncthing files. Coalesces move events into removal + addition.

### Config File Detection

- `_is_config_file(rel_path)`: Matches `sorted/{context}/context.yaml`, `sorted/{context}/smartfolders.yaml`, `sorted/{context}/generated.yaml`.
- `_check_root_smartfolders_yaml()`: Compares SHA-256 hash of `{root}/smartfolders.yaml` against stored hash.
- When detected: sets `config_changed = True` flag, does not emit as change.


## step2.py -- Preprocessing and Reconciliation

All functions are **pure** (no I/O, no side effects).

### preprocess(changes, records, read_sidecar)

Applies `ChangeItem` list to `Record` list. Returns `(modified, new_records)`.

**Location configuration:**
```python
LOCATION_CONFIG = {
    "incoming":   (["source_hash"], True),
    "sorted":     (["hash", "source_hash"], True),
    "archive":    (["source_hash"], False),
    "missing":    (["source_hash"], False),
    "processed":  (["hash"], False),
    "reviewed":   (["hash"], False),
    "trash":      (["source_hash", "hash"], False),
}
```

**Addition handling:**
1. `.output` files: match by `output_filename`. Size=0 -> `has_error`. Otherwise ingest sidecar. Duplicate hash check via `_is_duplicate_hash()` before setting record fields — if output hash matches another record's `source_hash` or `hash`, discard output and mark `has_error`.
2. Location-based matching by `hash` then `source_hash` per config.
3. New record creation for eligible locations.
4. Sorted/ new records inherit context from directory path.

### _is_duplicate_hash(records, hash_value, exclude)

Checks if `hash_value` matches any other record's `source_hash` or `hash`, excluding the given record. Returns `False` for empty hash values. Used during `.output` ingestion to detect byte-identical processing results.

**Removal handling:** Move path entries to `missing_*_paths`.

### reconcile(record, context_field_names, context_folders)

Evaluates a single record. Returns updated record or `None` (delete).

Three phases: source_paths, has_error, current_paths. See Specifications for details.

### compute_target_path(record, context_folders)

Builds `sorted/{context}/{folder1_value}/{folder2_value}/{assigned_filename}` from metadata and folder config. Returns `None` if context or filename missing.

### _is_collision_variant(actual_path, expected_path)

Checks if `actual_path` is `expected_path` with a `_[0-9a-f]{8}` UUID suffix before the extension. Used to prevent unnecessary re-sorting of collision-suffixed files.

### Metadata Handling

- For sorted/ files: fills missing context-defined fields with `None`.
- Context changes: adds all fields from new context as missing.
- Superfluous fields (null value, not in field_names): cleaned up.


## step3.py -- Processor

### SttConfig

Loaded from `{root}/stt.yaml`. Fields: `language`, `elevenlabs_model`, `enable_diarization`, `diarization_speaker_count`.

### Processor

#### process_one(record)

Routes to document or audio processing based on file extension.

**Document flow:**
1. Read source file bytes.
2. Call `_call_service()` with file, contexts, user_dir.
3. Write output + sidecar to `.output/`.

**Audio flow:**
1. Check prerequisites (STT URL, stt.yaml).
2. Optional: `_classify_audio()` for keyterms.
3. Required: `_stt_transcribe()` for transcript.
4. Intro detection (filename contains "intro"): `_intro_two_pass()`.
5. Required: `_process_transcript()` with transcript + optional pre-classification.

**Error handling:** Any failure creates a 0-byte file at `.output/{output_filename}`.

#### _call_service()

POST to `/process` with multipart FormData: `file`, `type`, `contexts`, `user_dir`.

#### _process_transcript()

POST to `/process_transcript` with JSON: `transcript`, `filename`, `contexts`, `user_dir`, optional `pre_classified`.

#### _call_with_retry(session, request_fn, max_retries, label, source)

Retry wrapper with exponential backoff. Retries on 5xx and 429 status codes and connection errors. Client errors (4xx except 429) are not retried.

#### New Clues Handling

Service responses may include `new_clues`. For each field/value pair, the context manager is consulted to record new candidates and clues for future use.


## step4.py -- FilesystemReconciler

### _move_file(src, dest)

1. Check source exists; log warning and return False if not.
2. Create parent directories.
3. If dest exists: append `_{uuid[:8]}` to stem.
4. `src.rename(dest)` for atomic same-filesystem move.

### reconcile(records)

Processes each record's temp fields in order:

1. **source_reference** -> `archive/`, `error/`, or `missing/` based on state. On failure: move path to `missing_source_paths`.
2. **duplicate_sources** -> `duplicates/{location}/{location_path}/{filename}`.
3. **current_reference + target_path** -> move to target. On success: **update `current_paths`** entry to new path. Clean up `.meta.json` sidecar if from `.output/`. On failure: move path to `missing_current_paths`.
4. **deleted_paths** -> `void/{location}/{location_path}/{filename}`. Clean up `.meta.json` sidecar if deleting from `.output/`.
5. **needs_deletion** state -> all source + current paths to `void/`.


## step5.py -- Smart Folder Symlinks

### SmartFolderEntry

Dataclass: `context`, `name`, `config` (SmartFolderConfig).

### SmartFolderReconciler

**reconcile(records):** For each IS_COMPLETE record in sorted/: find smart folders matching the record's context. For each matching folder: evaluate condition against metadata. If match and filename_regex passes: create symlink `sorted/{context}/{field_value}/{sf_name}/{filename}` -> `../{filename}`. If no match: remove symlink if it exists.

**cleanup_orphans():** Walk all smart folder directories. Remove broken symlinks and symlinks whose name doesn't match any real file in the parent directory. Never touch non-symlink files.

### RootSmartFolderEntry

Dataclass: `name`, `context`, `path` (resolved absolute Path), `config`.

### RootSmartFolderReconciler

**reconcile(records):** For each IS_COMPLETE record: find entries matching context. Evaluate condition. Create relative symlinks at configured paths. Remove when no longer matching.

**cleanup_orphans():** Only remove symlinks whose resolved target is within `sorted/` (safety: don't touch user-created symlinks).


## step6.py -- AudioLinkReconciler

**reconcile(records):** For each IS_COMPLETE record in sorted/ with an audio source:
- Source must be in archive/ with an audio extension.
- Create symlink: `{transcript_stem}{audio_ext}` -> relative path to archive source.
- Update symlink if transcript was renamed or relocated.

**cleanup_orphans():** Walk sorted/ recursively. Remove audio-extension symlinks pointing to archive/ that are broken or stale. Preserve smart folder symlinks (different parent pattern).


## prefilter.py

### prefilter(root)

Scans all top-level directories (except `error/`, `void/`, hidden). Recursively finds files with unsupported extensions. Skips config files in `sorted/{context}/`. Moves to `error/` with collision handling. Returns count of files moved.


## models.py

### State (str, Enum)

Values: `is_new`, `needs_processing`, `is_missing`, `has_error`, `needs_deletion`, `is_deleted`, `is_complete`.

### PathEntry (NamedTuple)

Fields: `path` (str), `timestamp` (datetime). Sortable by timestamp.

### ChangeItem (dataclass)

Fields: `event_type` (EventType), `path` (str), `hash` (Optional[str]), `size` (Optional[int]).

### Record (dataclass)

All identity, path, content, processing, and temp fields. Key methods:

- `source_file` / `current_file` -- Most recent PathEntry by timestamp.
- `source_location` / `current_location` -- Top-level directory from path decomposition.
- `_decompose_path(path)` -- Returns `(location, location_path, filename)`.
- `clear_temporary_fields()` -- Resets target_path, source_reference, current_reference, duplicate_sources, deleted_paths.


## db_new.py -- DocumentDBv2

### Schema

Table: `mrdocument.documents_v2` with indexes on `source_hash`, `hash` (partial), `output_filename` (partial), `state`, `metadata` (GIN), `username` (partial).

Auto-updated `updated_at` via trigger.

### Connection

- `connect()` -- Create asyncpg pool, ensure schema exists, run V1 backfill.
- `disconnect()` -- Close pool.

### CRUD

- `create_record(record)` -- INSERT with all fields.
- `get_record(id)` -- SELECT by UUID.
- `save_record(record)` -- UPDATE all fields by id.
- `delete_record(id)` -- DELETE by id.

### Queries

- `get_snapshot(username)` -- All records for user.
- `get_records_by_state(state, username)` -- Filter by state.
- `get_record_by_source_hash(hash, username)` -- Lookup by source hash.
- `get_record_by_hash(hash, username)` -- Lookup by current hash.
- `get_record_by_output_filename(name, username)` -- Lookup by output UUID.
- `get_records_with_temp_fields(username)` -- Records with actionable temp fields.
- `get_records_with_output_filename(username)` -- Records awaiting processing.

### Serialization

- `PathEntry` lists serialized to/from JSONB arrays of `{path, timestamp}`.
- Metadata as JSONB dict.
- Handles asyncpg's JSONB return types (string or dict).

### V1 Migration

- `migrate_from_v1(user_root, username)` -- Migrates processable V1 records.
- Status mapping: `processed`/`reviewed`/`sorted`/`duplicate` -> `is_complete`.
- Absolute paths -> relative paths.
- `ON CONFLICT (id) DO NOTHING` for idempotency.
- Attempts both `parent.name` and `user_root.name` as username for compatibility.


## sorter.py -- Context Management

### SorterContextManager

Central configuration loader for contexts, smart folders, and generated data.

**load():** Discovers contexts from `sorted/{context}/context.yaml` files. Loads `generated.yaml` for each context.

**get_context_for_api(name):** Returns full context dict merging base config with generated candidates/clues.

**load_smart_folders_from_sorted():** Parses `sorted/{context}/smartfolders.yaml` for each context.

**Generated data management:**
- `is_new_item(context, field, value)` -- Check if value is new to the field's candidates.
- `record_new_item(context, field, value)` -- Add to `generated.yaml` candidates.
- `record_new_clue(context, field, value, clue)` -- Add clue for a field value.

### SmartFolderCondition

Recursive condition evaluator:
- Statements: `{field, value}` -- case-insensitive regex full match.
- Operators: `{operator, operands}` -- `and` (all match), `or` (any match), `not` (negate single operand).
- `evaluate(fields)` -- Returns True/False against metadata dict.

### SmartFolderConfig

Name, condition, optional `filename_regex` (compiled, case-insensitive search).

### ContextConfig

Parses context YAML. Extracts `field_names` = `["context", "date"] + list(fields.keys())`. Supports conditional filename patterns.

### WatcherConfig

Loaded from YAML. Fields: `watch_patterns`, `debounce_seconds` (default 15), `full_scan_seconds` (default 300).

### Filename Utilities

- `_sanitize_filename_part(s)` -- Unicode normalization, character replacements, whitespace -> `_`.
- `_format_filename(pattern, metadata)` -- Template substitution with sanitized values.
- `get_username_from_root(user_root, sync_root)` -- Walks up path to find user directory under sync root.
