# PLAN: step4.py — Filesystem Reconciliation, Tests

## Implementation Order: 6 (depends on models)

## Class: `FilesystemReconciler`

```python
def __init__(self, root: Path)
def reconcile(self, records: list[Record]) -> None
```

## Logic (per record, in order)

1. **source_reference**: If set, move `root/source_reference` to:
   - `has_error` → `root/error/{source_filename}`
   - `is_lost` → `root/lost/{source_filename}`
   - else → `root/archive/{source_filename}`

2. **duplicate_sources**: Each path → `root/void/{location_path}/{filename}`

3. **current_reference + target_path**: Move `root/current_reference` to `root/target_path`

4. **deleted_paths**: Each path → `root/void/{location_path}/{filename}`

5. **needs_deletion**: All source_paths + current_paths files → `root/void/{location_path}/{filename}`

## Collision handling

If dest exists: `{stem}_{uuid4().hex[:8]}{suffix}`. Pattern from `sorter.py`.

## Utility: `_move_file(src, dest)`

- Create parent dirs (`mkdir -p`)
- Append UUID if collision
- Use `Path.rename()` for atomic move (same filesystem)

## Tests (test_step4.py) — uses tmp_path, no DB

- source_reference to archive: file moved, dirs created
- source_reference to error (has_error): file moved to error/
- source_reference to lost (is_lost): file moved to lost/
- duplicate_sources to void: correct path structure
- current_reference + target_path: file moved
- deleted_paths to void: correct path structure
- needs_deletion: all files moved to void
- collision: existing file at dest → UUID appended, both files exist
- no-op: all temp fields null → nothing happens
- parent dirs created as needed
- multiple records processed
