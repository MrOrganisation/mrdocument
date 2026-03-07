# PLAN: models.py — Types, Computed Properties, Tests

## Implementation Order: 1 (no dependencies)

## Types

### `State(str, Enum)`
Values: `is_new`, `needs_processing`, `is_missing`, `is_lost`, `has_error`, `needs_deletion`, `is_deleted`, `is_complete`.
Using `str, Enum` so `State.IS_NEW == "is_new"` works.

### `EventType(str, Enum)`
Values: `addition`, `removal`.

### `PathEntry(NamedTuple)`
Fields: `path: str`, `timestamp: datetime`.
Immutable, hashable, sortable. Timestamps are always timezone-aware UTC.

### `ChangeItem` dataclass
Fields: `event_type: EventType`, `path: str`, `hash: Optional[str]`, `size: Optional[int]`.
Hash and size only for additions.

### `Record` dataclass (mutable, not frozen)

**Identity:**
- `id: UUID` (default_factory=uuid4)
- `original_filename: str`
- `source_hash: str`

**Paths:**
- `source_paths`, `current_paths`, `missing_source_paths`, `missing_current_paths` — all `list[PathEntry]` with default_factory=list

**Content:**
- `context`, `metadata`, `assigned_filename`, `hash` — all Optional, default None

**Processing:**
- `output_filename: Optional[str]`
- `state: State` (default IS_NEW)

**Temp fields:**
- `target_path`, `source_reference`, `current_reference` — Optional[str]
- `duplicate_sources`, `deleted_paths` — list[str]

## Computed Properties on Record

- `source_file` / `current_file` → most recent PathEntry by timestamp (or None)
- `source_location`, `source_location_path`, `source_filename` — decompose `source_file.path`
- `current_location`, `current_location_path`, `current_filename` — decompose `current_file.path`
- `_decompose_path(path) -> (location, location_path, filename)` — static method using `PurePosixPath`
  - `"archive/sub/file.pdf"` → `("archive", "sub", "file.pdf")`
  - `"archive/file.pdf"` → `("archive", "", "file.pdf")`
  - `".output/uuid"` → `(".output", "", "uuid")`

## Methods

- `clear_temporary_fields()` — sets all 6 temp fields to None/[]

## Tests (test_models.py)

- `TestState`: all values exist, string equality, invalid raises ValueError
- `TestPathEntry`: creation, sorting by timestamp, equality, hashable (usable in sets)
- `TestRecordDefaults`: minimal construction, partial construction, mutable defaults independent
- `TestRecordComputedProperties`: source_file most recent, None when empty, 2-segment path, 3-segment path, deep sorted path, .output path
- `TestRecordClearTemporaryFields`: all fields cleared
- `TestChangeItem`: addition with hash+size, removal without
