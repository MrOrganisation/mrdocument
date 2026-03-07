# Root-Level Smart Folders — Implementation Documentation

## Overview

Root-level smart folders extend the existing smart folder system. While context-level smart folders create symlink subdirectories within each leaf folder of `sorted/`, root-level smart folders place symlinks at arbitrary paths configured in a single YAML file at the mrdocument root.

## Files Modified

### `mrdocument/watcher/step5.py`

**New dataclass: `RootSmartFolderEntry`**

```python
@dataclass
class RootSmartFolderEntry:
    name: str            # Smart folder name (from YAML key)
    context: str         # Which context's documents to consider
    path: Path           # Resolved absolute path for symlink directory
    config: SmartFolderConfig  # Reuses existing condition/regex config
```

**New class: `RootSmartFolderReconciler`**

Constructor takes `root` (mrdocument root Path) and a list of `RootSmartFolderEntry`. Sets `self.sorted_dir = root / "sorted"` for cleanup safety checks.

**`reconcile(records)`**: For each `IS_COMPLETE` record in `sorted/`:
1. Skips records not matching the entry's context.
2. Builds `str_fields` dict from record metadata (same as existing reconciler).
3. For each entry matching the record's context:
   - Checks `filename_regex` — removes existing symlink if filename doesn't match.
   - Evaluates condition — creates or removes symlink accordingly.
   - Symlink path: `entry.path / filename`.
   - Symlink target: `os.path.relpath(file_path, entry.path)` — always relative.
   - Collision guard: `not symlink_path.exists() and not symlink_path.is_symlink()`.
   - Creates `entry.path` directory on demand (`mkdir(parents=True, exist_ok=True)`).

**`cleanup_orphans()`**: For each entry's directory:
1. Iterates all items; skips non-symlinks.
2. Resolves symlink target. Skips if target is not within `self.sorted_dir` (via `resolved.relative_to(self.sorted_dir)`).
3. If target doesn't exist, removes the symlink.

Key difference from the existing `SmartFolderReconciler`:
- Symlink target computed with `os.path.relpath()` instead of `Path("..") / filename`.
- Cleanup checks target is within `sorted/` instead of checking against leaf folder files.
- No walking/recursion — each entry has exactly one directory.

### `mrdocument/watcher/app.py`

**New function: `_load_root_smart_folders(root: Path) -> list[RootSmartFolderEntry] | None`**

1. Reads `{root}/smartfolders.yaml`. Returns `None` if file doesn't exist.
2. Parses YAML. Expects top-level `smart_folders` dict.
3. For each entry:
   - Validates `context` and `path` are present (skips with warning if not).
   - Resolves `path`: absolute stays absolute, relative resolved against `root`.
   - Calls `SmartFolderConfig.from_dict()` for condition/regex parsing (reuses existing logic).
   - Returns `None` if no valid entries.

**`setup_user()` changes**: Calls `_load_root_smart_folders(user_root)` and passes result to `DocumentWatcherV2` via new `root_smart_folders` parameter.

### `mrdocument/watcher/orchestrator.py`

**Constructor**: New `root_smart_folders` parameter. Creates `RootSmartFolderReconciler` if provided.

**`run_cycle()` step 11**: After existing smart folder reconciliation, calls:
```python
if self.root_smart_folder_reconciler:
    if sorted_records:
        self.root_smart_folder_reconciler.reconcile(sorted_records)
    self.root_smart_folder_reconciler.cleanup_orphans()
```
Reuses the same `sorted_records` list already fetched for the existing reconciler.

**`reload_config()`**: Calls `_load_root_smart_folders(self.root)` and recreates the reconciler (or sets to `None`).

### `mrdocument/watcher/step1.py`

**`FilesystemDetector.__init__`**: New `_root_sf_hash` field (initially `None`).

**New method: `_check_root_smartfolders_yaml()`**:
1. Computes SHA-256 of `{root}/smartfolders.yaml` (or `None` if file missing).
2. If hash differs from `_root_sf_hash` and this is not the first check, sets `config_changed = True`.
3. Updates `_root_sf_hash`.

**`detect()`**: Calls `_check_root_smartfolders_yaml()` before every detection cycle (both full and incremental). This ensures the config change is detected even when no document files changed.

## Data Flow

```
smartfolders.yaml → _load_root_smart_folders() → [RootSmartFolderEntry]
                                                        ↓
DocumentWatcherV2.__init__() → RootSmartFolderReconciler
                                        ↓
run_cycle() step 11 → reconcile(sorted_records) + cleanup_orphans()
                                        ↓
Symlinks created/removed at entry.path/{filename}
```

Config reload flow:
```
step1.detect() → _check_root_smartfolders_yaml() → config_changed = True
        ↓
orchestrator.run_cycle() → reload_config() → _load_root_smart_folders()
                                                      ↓
                           RootSmartFolderReconciler recreated with new entries
```
