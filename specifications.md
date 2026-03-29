# MrDocument Watcher -- Specifications

## Overview

MrDocument is a multi-user document management system that automatically classifies, renames, and sorts documents using AI. It watches filesystem directories for new files, sends them to an AI-powered service for metadata extraction, and organizes them into a structured folder hierarchy.

The system consists of two main components:
- **mrdocument-service**: HTTP API for document classification, OCR, and metadata extraction.
- **mrdocument-watcher**: Filesystem watcher that orchestrates the pipeline.

This document specifies the watcher component.


## Architecture

### Multi-User Model

- The watcher discovers user roots by scanning a configurable set of glob patterns (default: `/sync/*`).
- Each user root is an independent mrdocument instance with its own configuration, contexts, and database records.
- New user directories are discovered automatically at runtime.
- Each user gets a dedicated orchestrator instance running in its own async task.
- All database queries are scoped by username for isolation.

### Directory Structure

Each user root contains the following directories:

| Directory | Purpose |
|-----------|---------|
| `incoming/` | Drop zone for new files. Flat (non-recursive). |
| `archive/` | Permanent storage of original source files. Flat. |
| `processed/` | Files awaiting human review. Flat. |
| `reviewed/` | Files approved by human, awaiting sorting. Flat. |
| `sorted/` | Final destination. Recursive, organized by context/fields. |
| `.output/` | Temporary processing output. Managed internally. |
| `error/` | Files that failed processing. Not watched. |
| `void/` | Deleted/orphaned files. Not watched. |
| `duplicates/` | Duplicate source files, maintaining path tree. Not watched. |
| `missing/` | Source files whose processed result is missing. Flat. |
| `reset/` | Place a processed file here to trigger filename recomputation. Watched. |
| `trash/` | User-initiated deletion. Watched for deletion triggers. |

### Configuration Files

| File | Location | Purpose |
|------|----------|---------|
| `context.yaml` | `sorted/{context}/` | Defines a context: name, fields, filename pattern, folders. |
| `smartfolders.yaml` | `sorted/{context}/` | Smart folder definitions for a context. |
| `smartfolders.yaml` | User root | Root-level smart folder definitions with arbitrary output paths. |
| `stt.yaml` | User root | Speech-to-text configuration (language, model, diarization). |
| `generated.yaml` | `sorted/{context}/` | Auto-generated candidates and clues (managed by the system). |
| `watcher.yaml` | Global | Watch patterns, debounce, scan intervals. |

### Health Endpoint

- HTTP server on configurable port (default 8080).
- `GET /health` returns 200 when ready, 503 during startup.


## Supported File Types

### Documents
`.pdf`, `.eml`, `.html`, `.htm`, `.docx`, `.txt`, `.md`, `.rtf`, `.jpg`, `.jpeg`, `.png`, `.gif`, `.tiff`, `.tif`, `.bmp`, `.webp`, `.ppm`, `.pgm`, `.pbm`, `.pnm`

### Audio/Video
`.flac`, `.wav`, `.mp3`, `.ogg`, `.webm`, `.mp4`, `.m4a`, `.mkv`, `.avi`, `.mov`

Files with unsupported extensions are moved to `error/` by the prefilter.


## Database Schema

### Record Fields

| Column | Type | Description |
|--------|------|-------------|
| `id` | UUID | Primary key. |
| `original_filename` | TEXT | Original filename when the file entered the system. Immutable. |
| `source_hash` | TEXT | SHA-256 hash of the original source file. Immutable. |
| `source_paths` | JSONB | List of `{path, timestamp}` entries where the source file is/was found. |
| `current_paths` | JSONB | List of `{path, timestamp}` entries where the processed file is/was found. |
| `missing_source_paths` | JSONB | Source paths that were observed but are now gone. |
| `missing_current_paths` | JSONB | Current paths that were observed but are now gone. |
| `context` | TEXT | Assigned context (e.g., "arbeit", "privat"). |
| `metadata` | JSONB | Extracted metadata fields (type, sender, date, etc.). |
| `assigned_filename` | TEXT | Filename computed from metadata and filename pattern. |
| `hash` | TEXT | SHA-256 hash of the processed file. |
| `output_filename` | TEXT | UUID filename for Step 3 output in `.output/`. Set when entering `needs_processing`, cleared after ingestion. |
| `state` | TEXT | Current state (see State Machine below). |
| `target_path` | TEXT | **Temporary.** Destination path for Step 4 file moves. |
| `source_reference` | TEXT | **Temporary.** Source file to be archived by Step 4. |
| `current_reference` | TEXT | **Temporary.** Current file to be moved by Step 4. |
| `duplicate_sources` | JSONB | **Temporary.** Source duplicates to be moved to `duplicates/` by Step 4. |
| `deleted_paths` | JSONB | **Temporary.** Files to be moved to `void/` by Step 4. |
| `username` | TEXT | Owner username for multi-user isolation. |

All paths in `source_paths` and `current_paths` are relative to the user's mrdocument root.

### Migration

- On startup, V1 records are migrated to V2 schema.
- Only records with processable V1 statuses (`processed`, `reviewed`, `sorted`, `duplicate`) are migrated as `is_complete`.
- Absolute paths are converted to relative paths.
- Migration is idempotent (`ON CONFLICT DO NOTHING`).
- The V1 table is never modified.


## State Machine

### States

| State | Description |
|-------|-------------|
| `is_new` | Initial state. Entry must never remain in this state after reconciliation. |
| `needs_processing` | Awaiting AI service call. `output_filename` is set. |
| `is_complete` | Successfully processed. All data available. |
| `is_missing` | Processed result disappeared from filesystem. Source moved to `missing/`. |
| `has_error` | Processing failed (0-byte output file). |
| `needs_deletion` | Marked for deletion (source was in `trash/`). |
| `is_deleted` | All files moved to `void/`. Entry must never remain in this state after reconciliation. |

### State Transitions

```
is_new ──────────────────────────► needs_processing
                                        │
                                        ├──► is_complete
                                        │        │
                                        │        ├──► is_missing ──► is_complete (reappearance)
                                        │        │
                                        │        └──► needs_processing (recovery from sorted/)
                                        │
                                        └──► has_error
                                                 │
                                                 ├──► needs_processing (recovery)
                                                 └──► [deleted from DB]

Any state ──► needs_deletion ──► is_deleted
```

### Invariants

After reconciliation, entries must satisfy the following invariants depending on their state:

#### `needs_processing`
- `source_paths` has exactly one entry.
- `source_location` is `archive`.
- `current_paths` is empty.
- `context`, `metadata`, `hash`, `assigned_filename` are all null.
- `output_filename` is not null.
- `target_path`, `source_reference`, `current_reference` are all null.

#### `is_complete`
- `source_paths` has at most one entry.
- `source_location` is `archive`.
- `current_paths` has exactly one entry.
- `current_location` is one of: `processed`, `reviewed`, `sorted`.
- `context`, `metadata`, `assigned_filename` are not null.
- `metadata` has no missing fields (no null values for context-defined field names).
- `hash` matches the file at the single `current_paths` entry.
- `target_path`, `source_reference`, `current_reference` are all null.

#### `is_missing`
- `source_paths` has at most one entry.
- `source_location` is `missing`.
- `current_paths` is empty.
- `missing_current_paths` has at least one entry.
- `context`, `metadata`, `hash`, `assigned_filename` are not null.
- `target_path`, `source_reference`, `current_reference` are all null.

#### `has_error`
- `output_filename` is null.
- `target_path` is null.

#### `needs_deletion`
- No specific field invariants.

#### Global Invariants (all states except `needs_deletion`)
- `original_filename` is not null and immutable.
- `source_hash` is not null and immutable.
- `source_paths` and `duplicate_sources` are non-intersecting.
- `source_paths` and `deleted_paths` are non-intersecting.
- `current_paths` and `deleted_paths` are non-intersecting.
- No two records may share a hash value across `source_hash` and `hash` columns. That is, for any hash value V, at most one record may have `source_hash = V` or `hash = V`.


## Metadata Model

Each context defines `field_names` -- the implicit fields (`context`, `date`) plus all keys from `fields:` in the context YAML.

| Term | Definition | Meaning |
|------|-----------|---------|
| **Missing** | Field is in `field_names`, record has the key with value `null` | Makes entry incomplete |
| **Not specified** | Field is in `field_names`, record does not have the key at all | Does not make entry incomplete |
| **Additional** | Field is NOT in `field_names`, record has the key with non-null value | Ignored |
| **Superfluous** | Field is NOT in `field_names`, record has the key with `null` value | Cleaned up |


## Processing Pipeline

Business logic is organized into steps. Steps 1, 2, and 4 run sequentially in a cycle. Step 3 runs independently.

### Execution Model

- Steps 1 -> 2 -> 4 form a sequential cycle.
- Step 3 runs independently, picking up entries where `output_filename` is not null.
- After Step 4 moves files, Step 1 of the next cycle detects the changes.
- Only Step 2 writes to the DB.
- Step 3 writes files only (result file + `.meta.json` sidecar to `.output/`). It does not write to the DB.
- Step 1 receives a read-only snapshot of the DB for stray file detection. It does not write to the DB.
- Step 4 receives a read-only snapshot of the DB and only performs filesystem operations. It does not write to the DB. It does update `current_paths` in-memory after successful moves for consistency.
- `void/` is not watched -- files moved there are effectively removed from the system.
- `error/` is not watched -- files moved there are for user review.
- `.output/` is cleaned up separately (not by any of the four steps).

### Step 1: Detect Filesystem Changes

This step detects filesystem changes and filters out stray files.

- Step 1 considers all files directly in: `archive`, `incoming`, `reviewed`, `processed`, `reset`, `trash`, `.output`.
- Step 1 considers all files recursively in `sorted`.
- It scans all files on startup and watches all directories for changes via inotify.

**Quiescence window:** Events are accumulated until no new changes have arrived for a configurable duration (debounce). Once quiescent, Step 1 processes the events.

**Stray file handling:**
- Only `incoming` and `sorted` (recursive) accept new files (files not known to the DB).
- Using the read-only DB snapshot, Step 1 checks each addition against known hashes (`source_hash`, `hash`) and `output_filename` values.
- If an added file is unknown and is in a directory that does not accept new files, it is moved to `error/` immediately. This addition is not included in the change list.

**Ignored files:**
- Hidden files (`.` prefix).
- Syncthing temporary files (`.syncthing.*`, `~syncthing~*`).
- `.tmp` files.

**Config file detection:**
- `context.yaml`, `smartfolders.yaml`, `generated.yaml` in `sorted/{context}/` are detected as config files.
- They are excluded from the change list but set a `config_changed` flag that triggers a reload.

**Output:** A list of `ChangeItem`s, each being:
- `addition`: A file was created or appeared. Includes the path, SHA-256 hash, and file size in bytes.
- `removal`: A file was deleted or disappeared. Includes the path.

### Step 2: Reconcile Database

This step receives the change list from Step 1, updates the DB, and prepares instructions for Step 4.

#### Preprocessing

Preprocessing applies the change list to the DB, updating `source_paths` and `current_paths`. Matching is location-aware:

| Location | Match against | New entries |
|---|---|---|
| `incoming` | `source_hash` | Yes |
| `sorted` | `hash` first, then `source_hash` | Yes |
| `archive`, `missing` | `source_hash` only | No |
| `processed`, `reviewed` | `hash` only | No |
| `reset` | `hash` only | No |
| `trash` | `source_hash` or `hash` | No |
| `.output` | `output_filename` only | No |

**For each addition:**

- If `.output`: Match filename against `output_filename` of entries in `needs_processing`. If no match: ignore. If file size is 0 bytes: set to `has_error`, clear `output_filename`. Otherwise: read sidecar. **Duplicate hash check**: if the output hash matches any other record's `source_hash` or `hash`, discard the output (add to `deleted_paths`), move archive sources to `duplicate_sources`, set `has_error`, clear `output_filename`. If not duplicate: set `context`, `metadata`, `assigned_filename`, `hash`. Add `.output` path to `current_paths`. Clear `output_filename`.

- If location matches against `hash`: Add path to `current_paths` if hash matches.

- If location matches against `source_hash`: Add path to `source_paths` if hash matches.

- If location accepts new entries and no match found: Create new entry with `original_filename`, `source_paths`, and `source_hash`.

**For each removal:**
- Remove from `source_paths` and add to `missing_source_paths`.
- Remove from `current_paths` and add to `missing_current_paths`.

#### Reconciliation

Reconciliation runs on every entry modified by preprocessing. It first **clears all temporary fields**, then re-evaluates the entry.

**Aliases:**
- `source_file`: Most recent item in `source_paths`.
- `current_file`: Most recent item in `current_paths`.
- `source_location`, `source_location_path`, `source_filename`: Decomposed from `source_file`.
- `current_location`, `current_location_path`, `current_filename`: Decomposed from `current_file`.

**Phase 1: source_paths**

- If `source_location` is `trash`: Set to `needs_deletion`. Return.
- If `is_new`: Set `output_filename` to new UUID. Set to `needs_processing`.
- If source not in `archive`: Set `source_reference` to `source_file`. Add `archive/{source_filename}` to `source_paths`. Move non-archive source_paths to `duplicate_sources`.
- Recovery: if `is_missing` or `is_complete` with source in processable location: Set new `output_filename`, set to `needs_processing`, clear stale processing results.

**Phase 2: has_error**

- If source in `error/` and no current_paths: Delete record from DB. Return.
- If no source_paths and no current_paths: Delete record from DB. Return.
- Recovery: if source in processable location (`incoming`, `sorted`): Set to `needs_processing`, clear stale state.
- If source in `archive`: Set `source_reference`.
- Move all current_paths to `deleted_paths`.

**Phase 3: current_paths**

- If any current_path is in `trash/`: Set to `needs_deletion`. Return.
- If `is_new`: Set to `needs_processing`.
- If `needs_processing` with no current_paths: Return unchanged.
- If no current_paths and has missing_current_paths: Set to `is_missing`. If source is in `archive`, set `source_reference` to trigger move to `missing/`.
- If `is_missing` and current_paths reappeared: Set to `is_complete`.
- Remove invalid locations from current_paths (→ `deleted_paths`).
- Deduplicate: keep most recent, move others to `deleted_paths`.
- Single `.output` path: Compute `target_path`, set `current_reference`.
- Single `processed` path: Set to `is_complete`.
- Single `reviewed` path: Compute `target_path`, set `current_reference`.
- Single `reset` path: Optionally recompute `assigned_filename`. Compute `target_path` to `sorted/`. Set `current_reference`. Deduplicate: if multiple entries in `current_paths`, keep most recent and move older entries to `deleted_paths`. Set to `is_complete`.
- Single `sorted` path: Adopt user changes (rename, context change), check metadata completeness. If path matches expected: `is_complete`. If not: set `current_reference` and `target_path`.

### Step 3: Process

This step runs independently on its own loop. It is the only step that communicates with the mrdocument service.

- Polls for entries where `output_filename` is not null.
- **Documents**: Sent to `/process` endpoint with file bytes, contexts, and `user_dir` (for cost tracking).
- **Audio files**: Orchestrated through a multi-step flow:
  1. Optional: Classify audio by filename via `/classify_audio` (get keyterms).
  2. Required: STT transcription via `/transcribe`.
  3. Intro files (filename contains "intro"): Two-pass flow -- classify transcript, second STT pass with improved keyterms and speaker count.
  4. Required: Process transcript via `/process_transcript`.
- On success: Write result file + `.meta.json` sidecar to `.output/{output_filename}`.
- On error: Create 0-byte file at `.output/{output_filename}`.
- Context filtering: If a record already has a context (e.g., from `sorted/`), the API call is filtered to that context only.

### Step 4: Reconcile Filesystem

This step executes file operations based on temporary fields.

**!!! Whenever moving a file, never overwrite an existing file at the destination. Append a UUID suffix if necessary. !!!**

For each entry with non-null temporary fields or in state `needs_deletion`:

1. **source_reference**: Move to `archive/{filename}`, `error/{filename}`, or `missing/{filename}` depending on state.
2. **duplicate_sources**: Move each to `duplicates/{location}/{location_path}/{filename}`.
3. **current_reference + target_path**: Move file to target_path. Update `current_paths` to reflect new location.
4. **deleted_paths**: Move each to `void/{location}/{location_path}/{filename}`.
5. **needs_deletion**: Move all source + current paths to `void/` maintaining path structure.

After all moves, temporary fields are cleared.

### Step 5: Smart Folder Symlinks

Smart folders create symlinks to documents based on metadata conditions.

**Context-level smart folders** (defined in `sorted/{context}/smartfolders.yaml`):
- Symlinks placed in `sorted/{context}/{field_value}/{smart_folder_name}/`.
- Relative symlinks pointing to `../{filename}`.

**Root-level smart folders** (defined in `{root}/smartfolders.yaml`):
- Symlinks placed at arbitrary paths specified in configuration.
- Relative symlinks computed via `os.path.relpath()`.
- Cleanup only removes symlinks whose resolved target is within `sorted/`.

**Condition evaluation:**
- Statements: `{field, value}` -- case-insensitive regex full match against metadata field.
- Operators: `{operator: and/or/not, operands: [...]}` -- recursive evaluation.
- Optional `filename_regex`: case-insensitive search (not full match) on filename.

**Symlink lifecycle:**
- Created when: file matches condition AND filename_regex.
- Removed when: file no longer matches condition or filename_regex.
- Non-symlink files in smart folder directories are never touched.
- Orphan cleanup removes broken and stale symlinks.

### Step 6: Audio Link Symlinks

When an audio file is transcribed, a symlink to the source audio file in `archive/` is placed alongside the transcript in `sorted/`.

- Link filename: `{transcript_stem}{original_audio_extension}`.
- Relative symlink to the archive file.
- Updated when transcript is renamed or relocated.
- Cleanup removes broken/stale audio links.
- Non-symlink files with the same name are never overwritten.


## Context Configuration

### Structure

```yaml
name: context_name
description: "Human-readable description"

filename: "{context}-{type}-{date}-{sender}"
audio_filename: "{context}-{date}-{sender}-{type}"  # Optional

fields:
  field_name:
    instructions: "AI instructions for extraction"
    candidates:
      - "Value 1"
      - "Value 2"
    allow_new_candidates: false  # or true

filename_keywords:
  - "keyword1"
  - "keyword2"

folders:
  - "context"
  - "field_name"
```

### Field Names

Field names are the implicit fields (`context`, `date`) plus all keys from `fields:`. They determine metadata completeness.

### Filename Pattern

- Template using `{field_name}` placeholders.
- Components joined by `-`.
- Dates (YYYY-MM-DD) treated as single components.
- Sanitization: accented characters replaced, whitespace → `_`, special characters removed.

### Folder Hierarchy

The `folders` list defines the directory structure under `sorted/{context}/`:
- `["context", "sender"]` → `sorted/arbeit/Schulze_GmbH/filename.pdf`
- Missing field values result in flat placement under context root.

### Smart Folder Conditions

```yaml
smart_folders:
  folder_name:
    condition:
      field: "type"
      value: "Rechnung"
    filename_regex: "pattern"  # Optional
```

Operators for complex conditions:
```yaml
condition:
  operator: "and"
  operands:
    - field: "type"
      value: "Rechnung"
    - field: "sender"
      value: "Schulze.*"
```


## Root-Level Smart Folders

Defined in `{root}/smartfolders.yaml`:

```yaml
smart_folders:
  - name: folder_name
    context: context_name
    path: /absolute/or/relative/path
    condition:
      field: "type"
      value: "Rechnung"
    filename_regex: "pattern"  # Optional

smartfolder_paths:
  - /path/to/watch
  - ./relative/path
```

- `path` is resolved relative to the user root if not absolute.
- Each entry must specify a `context` and a `path`.
- When two records would produce a symlink with the same filename, a numeric suffix (`_1`, `_2`, …) is added to resolve the collision.
- Removal of symlinks also covers suffixed names.
- Changes to this file trigger a config reload.

### `smartfolder_paths`

- A list of directory paths to recursively scan for per-directory `smartfolder.yaml` files (matched case-insensitively).
- Each directory containing a `smartfolder.yaml` becomes a root smart folder with that directory as the symlink output path and the directory name as the smart folder name.
- Per-directory `smartfolder.yaml` format:
  ```yaml
  context: arbeit
  condition:
    field: "type"
    value: "Rechnung"
  filename_regex: "2025"  # optional
  ```
- Paths are resolved relative to the user root if not absolute.
- Hidden directories (`.` prefix) are skipped during the recursive walk.
- Changes to any discovered `smartfolder.yaml` file trigger a config reload.


## STT Configuration

Defined in `{root}/stt.yaml`:

```yaml
language: "de-DE"
elevenlabs_model: "scribe_v2"
enable_diarization: true
diarization_speaker_count: 2
```

- Required for audio file processing. Without it, audio files are skipped.


## Cost Tracking

- The watcher sends `user_dir` with each `/process` and `/process_transcript` API call.
- The service records Anthropic API token usage per model, per user.
- Costs are flushed periodically to `/costs/{username}/mrdocument_costs.json`.
- Cost files contain per-day and total usage with input/output tokens, USD cost, and document counts.


## Prefilter

- Runs at the beginning of each full scan cycle.
- Scans all directories except `error/`, `void/`, and hidden directories.
- Moves files with unsupported extensions to `error/`.
- Skips config files (`context.yaml`, `smartfolders.yaml`, `generated.yaml`) in `sorted/{context}/`.
- Collision handling: append UUID suffix if destination exists.


## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | (required) | PostgreSQL connection string. |
| `MRDOCUMENT_URL` | `http://mrdocument-service:8000` | Document processing service URL. |
| `STT_URL` | (none) | Speech-to-text service URL. Audio disabled if not set. |
| `HEALTH_PORT` | `8080` | Health check HTTP port. |
| `WATCHER_CONFIG` | `/app/watcher.yaml` | Path to watcher configuration. |
| `POLL_INTERVAL` | `5` | Seconds between polling cycles. |
| `PROCESSOR_TIMEOUT` | `900` | Timeout in seconds for service calls. |
| `MAX_CONCURRENT_PROCESSING` | `5` | Maximum concurrent processing tasks. |
| `LOG_LEVEL` | `INFO` | Logging level. |
| `LOG_DIR` | (none) | Directory for log files. |
