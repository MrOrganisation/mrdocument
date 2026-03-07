# PLAN: Smart Folders — Changes to Existing Code

This plan covers all modifications to existing files needed to support smart folder symlinks. The new `step5.py` module is described in `PLAN-smart-folders.md`.

## 1. step1.py — Skip symlinks in filesystem scan

### Problem

`_scan()` uses `f.is_file()` which returns `True` for symlinks. Smart folder symlinks in `sorted/` would be scanned, hashed, and reported as additions/removals, causing false change events and potentially stray-file errors.

### Change

Add symlink skip in both scan loops.

```python
# Line 74-76 (flat scan loop)
for f in dirpath.iterdir():
    if not f.is_file() or f.is_symlink():  # added: or f.is_symlink()
        continue

# Line 91-93 (recursive scan loop)
for f in dirpath.rglob("*"):
    if not f.is_file() or f.is_symlink():  # added: or f.is_symlink()
        continue
```

Two lines changed. No other changes to step1.

## 2. orchestrator.py — Integrate step 5

### Changes

**Import:**
```python
from step5 import SmartFolderReconciler
```

**Constructor** — accept smart folder config:
```python
def __init__(
    self,
    ...
    smart_folders: Optional[list] = None,  # list[SmartFolderEntry]
):
    ...
    self.smart_folder_reconciler = (
        SmartFolderReconciler(root, smart_folders)
        if smart_folders else None
    )
```

**`run_cycle()`** — add step 5 after step 10 (temp field cleanup):

```python
# After the existing "for record in actionable" block (lines 176-180):

# 11. Smart folder symlink reconciliation
if self.smart_folder_reconciler:
    sorted_records = [
        r for r in await self.db.get_snapshot()
        if r.state == State.IS_COMPLETE
        and r.current_location == "sorted"
    ]
    self.smart_folder_reconciler.reconcile(sorted_records)
    self.smart_folder_reconciler.cleanup_orphans()
```

This queries a fresh snapshot after all DB writes are done, filters to IS_COMPLETE records in sorted/, and passes them to the reconciler. Orphan cleanup runs once per cycle.

## 3. app.py — Load and pass smart folder config

### Changes

**Import:**
```python
from step5 import SmartFolderEntry
from sorter import SmartFolderConfig, SmartFolderCondition
```

**`setup_user()`** — extract smart folders from context manager, pass to orchestrator:

```python
def setup_user(...) -> DocumentWatcherV2:
    ...
    # After loading context_manager:
    smart_folders = None
    if context_manager.contexts:
        smart_folders = []
        for ctx_name, ctx in context_manager.contexts.items():
            for sf_name, sf_config in ctx.smart_folders.items():
                smart_folders.append(SmartFolderEntry(
                    context=ctx_name,
                    config=sf_config,
                ))
        if smart_folders:
            logger.info("[%s] Loaded %d smart folder(s)", username, len(smart_folders))

    return DocumentWatcherV2(
        ...
        smart_folders=smart_folders,  # new parameter
    )
```

This extracts smart folder definitions from the existing `SorterContextManager` (which already parses `smart_folders:` from context YAMLs). No new config loading code needed — the existing `ContextConfig.from_dict()` already handles it.

## 4. Dockerfile.watcher — Add step5.py

```dockerfile
# After the existing step4.py COPY line:
COPY mrdocument/watcher/step5.py /app/step5.py
```

One line added.

## 5. db_new.py — No changes

The smart folder reconciler reads records but does not write to the DB. It only creates/removes symlinks on the filesystem. No new DB queries or schema changes needed.

## 6. step2.py — No changes

Smart folder symlinks are skipped by step 1 (change #1 above), so step 2 never sees them. No preprocessing or reconciliation changes needed.

## 7. step3.py — No changes

Step 3 processes documents via the mrdocument service. Smart folders are unrelated to document processing.

## 8. step4.py — No changes

Step 4 moves files based on temp fields. Smart folder symlinks are managed by step 5, not step 4. When step 4 moves a file out of `sorted/` (e.g., to `void/`), the symlink becomes broken and is cleaned up by step 5's orphan cleanup on the next cycle.

## 9. models.py — No changes

No new fields on `Record`. The smart folder reconciler reads existing fields (`state`, `current_location`, `context`, `metadata`, `assigned_filename`, `current_paths`).

## Summary of touched files

| File | Lines changed | Nature |
|------|--------------|--------|
| `step1.py` | 2 | Add `or f.is_symlink()` skip |
| `orchestrator.py` | ~15 | Import, constructor param, step 5 call in `run_cycle()` |
| `app.py` | ~15 | Extract smart folders from context manager, pass to orchestrator |
| `Dockerfile.watcher` | 1 | COPY step5.py |

Total: ~33 lines of changes across 4 existing files.
