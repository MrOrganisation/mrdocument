# PLAN: Smart Folders — Symlink Management for Sorted Documents

## Implementation Order: 8 (depends on models, step2, step4, orchestrator)

## Overview

Smart folders are subdirectories inside `sorted/` leaf folders that contain symlinks to matching documents. When a document reaches its final location in `sorted/`, a new step evaluates it against all configured smart folder conditions and creates/removes symlinks accordingly.

### Key change from v1

In v1, smart folders were defined **inside each context YAML** (`smart_folders:` key in `arbeit.yaml`, `privat.yaml`). The feature request specifies:

- Smart folders are **no longer part of the context configuration**.
- A **separate config** defines all smart folders.
- Each smart folder specifies a **fixed context**.
- Condition syntax stays the same.

**However**, the integration test fixtures (`config/arbeit.yaml`, `config/privat.yaml`) still define `smart_folders:` inside the context YAML, and test expectations (`test_documents.py`) verify symlinks based on this. Since we cannot change test fixtures, we keep loading `smart_folders:` from context YAMLs as a supported source. The separate config is an additional source, not a replacement.

## Config Format

### Inside context YAML (backward-compatible, already parsed by `sorter.py`)

```yaml
# sorted/arbeit/context.yaml
smart_folders:
  rechnungen:
    condition:
      field: "type"
      value: "Rechnung"
```

### Separate config (new)

```yaml
# smart_folders.yaml (path configurable via SMART_FOLDERS_CONFIG env var)
smart_folders:
  rechnungen:
    context: arbeit
    condition:
      field: "type"
      value: "Rechnung"
  gesundheit:
    context: privat
    condition:
      field: "type"
      value: "Arztbrief"
    filename_regex: ".*brief.*"
```

Merged at load time: context-embedded definitions + standalone definitions. Standalone definitions specify `context:` explicitly. Duplicates (same name + same context) are logged as warning; standalone wins.

## Data Model

Reuse existing classes from `sorter.py` without modification:

- `SmartFolderCondition` — condition tree (statement / and / or / not)
- `SmartFolderConfig` — name, condition, filename_regex

New lightweight container:

```python
@dataclass
class SmartFolderEntry:
    """A smart folder bound to a specific context."""
    context: str
    config: SmartFolderConfig
```

## New Module: `step5.py` — Smart Folder Reconciliation

Smart folders are a **post-reconciliation** step. They run after step 4 has moved files to their final positions. This keeps the separation of concerns clean:

- Step 1: detect filesystem changes
- Step 2: reconcile DB state
- Step 3: process documents (service calls)
- Step 4: move files
- **Step 5: manage smart folder symlinks**

### Class: `SmartFolderReconciler`

```python
class SmartFolderReconciler:
    def __init__(self, root: Path, smart_folders: list[SmartFolderEntry])
    def reconcile(self, records: list[Record]) -> None
```

### Logic

`reconcile(records)` processes all records in state `IS_COMPLETE` whose `current_location` is `sorted`:

1. **For each record with `current_location == "sorted"`:**
   - Determine the leaf folder (parent directory of the file in sorted/).
   - Get the record's context and metadata.
   - For each smart folder entry matching this context:
     - Evaluate condition against metadata (cast values to str, use `SmartFolderCondition.evaluate()`).
     - Check filename_regex if configured.
     - **If matches**: ensure symlink exists at `{leaf_folder}/{sf_name}/{filename}` → `../{filename}`.
     - **If does not match**: remove symlink if it exists.

2. **Orphan cleanup** (runs once per cycle, not per record):
   - Walk all smart folder subdirectories in `sorted/`.
   - Remove broken symlinks (target doesn't exist).
   - Remove stale symlinks (name doesn't match any real file in leaf folder).
   - Ignore non-symlink files and directories (per spec: "completely ignored and never touched").

### Symlink structure

```
sorted/
└── arbeit/
    └── Schulze GmbH/
        ├── arbeit-rechnung-2025-03-15-schulze-gmbh.pdf
        └── rechnungen/                    # smart folder
            └── arbeit-rechnung-2025-03-15-schulze-gmbh.pdf -> ../arbeit-rechnung-2025-03-15-schulze-gmbh.pdf
```

- Symlinks are **relative**: `../{filename}` (one level up).
- Smart folder directories are created on demand.
- Non-symlink files in smart folder directories are never touched.

### When symlinks are updated

Symlinks are re-evaluated on every cycle for all `IS_COMPLETE` records in `sorted/`. This covers:

- **New file arrives in sorted/**: symlink created if condition matches.
- **File renamed/relocated within sorted/**: old symlink becomes stale (orphan cleanup removes it), new symlink created at new leaf folder.
- **Metadata changes**: condition re-evaluated, symlink added or removed accordingly.

This is simple and correct. Performance is acceptable because only `IS_COMPLETE` records in `sorted/` are checked, and symlink operations are cheap.

## Integration into Orchestrator

Add step 5 after step 4's temp field cleanup in `run_cycle()`:

```python
# After step 10 (clear temp fields)...

# 11. Smart folder symlink reconciliation
if self.smart_folder_reconciler:
    complete_in_sorted = [
        r for r in await self.db.get_snapshot()
        if r.state == State.IS_COMPLETE
        and r.current_location == "sorted"
    ]
    if complete_in_sorted:
        self.smart_folder_reconciler.reconcile(complete_in_sorted)
    self.smart_folder_reconciler.cleanup_orphans()
```

### Constructor changes

`DocumentWatcherV2.__init__` gets an optional `smart_folders: list[SmartFolderEntry]` parameter. If provided, creates `SmartFolderReconciler`.

### app.py changes

`setup_user()` loads smart folder config from:
1. Context YAMLs (via `SorterContextManager`) — existing `smart_folders:` keys.
2. Standalone config file (via `SMART_FOLDERS_CONFIG` env var) — new.

Merges both sources into `list[SmartFolderEntry]` and passes to orchestrator.

## Step 1 Interaction

Step 1 watches `sorted/` recursively. Smart folder symlinks are files under `sorted/`. Step 1 must **ignore symlinks** in its scan to avoid creating spurious change events.

**Confirmed**: `_scan()` in step1.py uses `f.is_file()` which returns `True` for symlinks. There is no `is_symlink()` check. Fix required: add `or f.is_symlink()` skip in both the flat (`iterdir`) and recursive (`rglob`) scan loops (lines 74-76 and 91-93).

## Tests (`test_step5.py`) — uses tmp_path, no DB

- **symlink_created_for_matching_record**: IS_COMPLETE record in sorted/ with matching metadata → symlink exists.
- **no_symlink_for_non_matching_record**: IS_COMPLETE record whose metadata doesn't match condition → no symlink.
- **symlink_removed_when_condition_no_longer_matches**: existing symlink, metadata changed to non-matching → symlink removed.
- **multiple_smart_folders_same_file**: file matches two smart folders → symlinks in both.
- **orphan_cleanup_broken_symlink**: symlink target deleted → symlink removed.
- **orphan_cleanup_stale_symlink**: symlink name doesn't match any real file → symlink removed.
- **non_symlink_files_ignored**: regular file in smart folder dir → not touched.
- **filename_regex_filtering**: filename_regex set, file doesn't match → no symlink.
- **smart_folder_dir_created_on_demand**: dir doesn't exist yet → created when first symlink is placed.
- **records_not_in_sorted_skipped**: IS_COMPLETE in processed/ → no symlink action.
- **operator_conditions**: and/or/not conditions evaluated correctly.
- **collision_avoidance**: if a non-symlink file with the same name exists in smart folder dir → do not overwrite, log warning.

## Integration test expectations

The 3 currently failing integration tests verify:
1. `arbeit/pdf` → `rechnungen` smart folder (type=Rechnung)
2. `arbeit/rtf` → `rechnungen` smart folder (type=Rechnung)
3. `privat/pdf` → `gesundheit` smart folder (type=Arztbrief)

These use `poll_for_smart_folder_symlink(leaf_dir, smart_folder, sorted_file.name)` with a 30s timeout. The v2 implementation must create symlinks within a single cycle after the file reaches `sorted/`.

## File changes summary

| File | Change |
|------|--------|
| `step5.py` | New. `SmartFolderReconciler` class. |
| `orchestrator.py` | Add step 5 call after step 4. Accept `smart_folders` param. |
| `app.py` | Load smart folder config, pass to orchestrator. |
| `step1.py` | Verify symlinks are skipped in filesystem scan (fix if needed). |
| `test_step5.py` | New. Unit tests for smart folder logic. |
