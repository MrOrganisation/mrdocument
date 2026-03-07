# PLAN: step2.py — Preprocessing + Reconciliation, Tests

## Implementation Order: 3 (depends on models only — pure functions)

Two public functions, both pure (no I/O):

## `preprocess(changes, records, read_sidecar) -> (modified, new)`

### Parameters
- `changes: list[ChangeItem]`
- `records: list[Record]`
- `read_sidecar: Callable[[str], dict]` — injectable, reads `.meta.json` given the `.output` path. In production reads from disk, in tests returns a dict.

### Returns
`(modified_records: list[Record], new_records: list[Record])`

### Logic per addition
1. `.output` location → match filename against `output_filename` of records. No match → skip. Size=0 → set `has_error`, clear `output_filename`. Normal → call `read_sidecar`, set context/metadata/assigned_filename/hash, add to current_paths, clear output_filename.
2. Location matches `hash` (per table) → add to current_paths
3. Location matches `source_hash` (per table) → add to source_paths
4. Eligible location, no match → create new Record

### Logic per removal
Find record with path in source_paths or current_paths, move to missing_*_paths.

### Location-aware matching table (encoded as constants)

| Location | Match against | New entries |
|---|---|---|
| `incoming` | `source_hash` | Yes |
| `sorted` | `hash` first, then `source_hash` | Yes |
| `archive`, `lost` | `source_hash` only | No |
| `processed`, `reviewed` | `hash` only | No |
| `trash` | `source_hash` or `hash` | No |
| `.output` | `output_filename` only | No |

## `reconcile(record, context_field_names) -> Record | None`

### Parameters
- `record: Record`
- `context_field_names: Optional[dict[str, list[str]]]` — maps context name to its field_names, for metadata completeness checks. Injected so the function stays pure.

### Returns
Modified Record, or None if the record should be deleted (has_error with source in error).

### Logic (follows spec exactly)

1. `clear_temporary_fields()`
2. **source_paths**: trash→needs_deletion; is_new→set output_filename + needs_processing; archive/lost handling
3. **has_error**: source in error + no current_paths→return None (delete); else set source_reference to error, current_paths to deleted_paths
4. **current_paths**: is_new→needs_processing; is_lost→return; needs_processing no current→return; missing detection; reappearance; invalid location cleanup; multiple→deduplicate; single-file logic (.output, processed, reviewed, sorted)

## Helpers

- `infer_metadata_from_path(path)` — extracted for testability, reuses patterns from `sorter.py` for path→metadata inference
- `compute_target_path(record)` — extracted for testability, reuses filename formatting from `sorter.py`

## Tests (test_step2.py) — no DB, no filesystem

### Preprocessing tests
- addition in .output, matches output_filename → sidecar ingested, current_paths updated
- addition in .output, 0-byte → has_error set
- addition in .output, no match → ignored
- addition in incoming, matches source_hash → added to source_paths
- addition in incoming, unknown → new record created
- addition in sorted, matches hash → added to current_paths
- addition in sorted, matches source_hash (not hash) → added to source_paths
- addition in sorted, unknown → new record
- addition in archive, matches source_hash → added to source_paths
- addition in archive, unknown → not created (stray handled by Step 1)
- addition in processed, matches hash → added to current_paths
- addition in trash, matches source_hash → added to source_paths
- removal from source_paths → moved to missing_source_paths
- removal from current_paths → moved to missing_current_paths
- location-aware: archive file matches both hash and source_hash → only source_hash used
- location-aware: sorted file matches both → hash preferred

### Reconciliation tests (construct Record → call reconcile → assert)

**source_paths section:**
- source in trash → needs_deletion, return
- is_new → output_filename set (UUID), needs_processing, source_reference set, archive path added
- source not in archive, not is_lost → source_reference set, archive added, others to duplicate_sources
- is_lost, source not in lost → source_reference to lost

**has_error section:**
- has_error + source in error + no current_paths → returns None (delete)
- has_error + source in archive → source_reference to error, duplicates cleared
- has_error + has current_paths → current_paths moved to deleted_paths

**current_paths section:**
- is_new → needs_processing, output_filename set, fields cleared
- is_lost → unchanged
- needs_processing, no current_paths → unchanged
- no current_paths + missing_current_paths → is_missing
- is_missing + current_paths reappeared → is_complete
- is_missing + still empty → unchanged
- invalid location → moved to deleted_paths
- multiple current_paths → keep most recent, others to deleted_paths
- single in .output → target_path computed, current_reference set
- single in processed → is_complete
- single in reviewed → target_path to sorted, current_reference set
- single in sorted, path matches metadata → is_complete
- single in sorted, path doesn't match → current_reference + target_path
- single in sorted, context changed → missing fields added
