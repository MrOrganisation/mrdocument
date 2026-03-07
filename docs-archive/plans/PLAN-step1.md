# PLAN: step1.py — Filesystem Detection, Stray Handling, Tests

## Implementation Order: 4 (depends on models)

## Class: `FilesystemDetector`

```python
def __init__(self, root: Path, debounce_seconds: float = 2.0)
async def detect(self, db_snapshot: list[Record]) -> list[ChangeItem]
```

## Logic

1. **Scan** all watched directories, compute hashes for all files found
2. **Diff** against previous known state (or initial empty state on first run)
3. **Classify** each change as addition or removal
4. **Stray detection**: For each addition, check against snapshot's `source_hash`, `hash`, `output_filename`. If unknown AND location not in {`incoming`, `sorted`} → move to `error/`, exclude from change list.
5. **Return** change list with path, hash, size for additions; path for removals

## Watched directories

- Direct: `archive`, `incoming`, `reviewed`, `processed`, `trash`, `.output`
- Recursive: `sorted`
- Ignores: hidden files (`.`, `~`), Syncthing temp files (`.syncthing.*`, `*.tmp`)

## Debounce

Debounce is handled at the caller level (the main loop). The `detect()` method does a point-in-time scan + diff.

## Reuse from existing code

- `compute_sha256()` from `db.py:143`
- Syncthing temp file filter pattern from `output_watcher.py:34-47`

## Tests (test_step1.py) — uses tmp_path, no DB

- **Setup**: Create standard dir structure in tmp_path
- Test initial scan detects all files
- Test additions: place new file, detect() returns addition with hash and size
- Test removals: remove file between scans, detect() returns removal
- Test stray: unknown file in archive → moved to error, not in change list
- Test eligible: unknown file in incoming → in change list, not moved
- Test eligible: unknown file in sorted/sub/ → in change list, not moved
- Test known file in archive: hash matches snapshot source_hash → in change list, not moved
- Test .output file: matches output_filename → in change list
- Test hidden files ignored
- Test Syncthing temp files ignored
- Test file size included in additions
