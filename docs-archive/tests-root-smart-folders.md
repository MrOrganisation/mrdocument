# Root-Level Smart Folders — Test Documentation

## Test Location

All tests are in `mrdocument/watcher/test_step5.py`, alongside the existing smart folder tests. Run with:

```bash
make test-mrdocument-unit
```

## Test Classes and Cases

### `TestRootSmartFolder` — Symlink Creation and Removal

| Test | What It Verifies |
|------|-----------------|
| `test_symlink_created_at_absolute_path` | An `IS_COMPLETE` record with matching metadata creates a symlink at the configured absolute path. Verifies the symlink resolves to the actual file in `sorted/`. |
| `test_symlink_target_is_relative` | The raw symlink target (via `readlink()`) is a relative path, not absolute. Ensures portability. |
| `test_condition_filtering` | A record whose metadata doesn't match the condition produces no symlink. |
| `test_filename_regex_filtering` | A record whose filename doesn't match `filename_regex` produces no symlink, even if condition matches. |
| `test_symlink_removed_when_condition_no_longer_matches` | Pre-existing symlink is removed when the record's metadata changes to non-matching. |
| `test_wrong_context_skipped` | A record from context "privat" is skipped when the smart folder is configured for context "arbeit". |
| `test_collision_first_wins` | Two records with the same filename but different leaf folders: the first record's symlink is created, the second is silently skipped. |

### `TestRootSmartFolderCleanup` — Orphan Cleanup

| Test | What It Verifies |
|------|-----------------|
| `test_broken_symlink_into_sorted_removed` | A broken symlink whose target path is within `sorted/` is removed during cleanup. |
| `test_symlink_not_into_sorted_left_alone` | A symlink pointing to a file outside `sorted/` (e.g., `/external/doc.pdf`) is not touched during cleanup, even if the smart folder directory contains it. |
| `test_regular_files_untouched` | Regular (non-symlink) files in the smart folder directory are never removed. |

### `TestLoadRootSmartFolders` — Config Parsing

| Test | What It Verifies |
|------|-----------------|
| `test_valid_yaml_parsed` | A well-formed `smartfolders.yaml` is parsed into a list of `RootSmartFolderEntry` with correct name, context, and absolute path. |
| `test_relative_path_resolved` | A relative `path` value (e.g., `briefe_sammlung`) is resolved against the mrdocument root to produce an absolute path. |
| `test_missing_context_skipped` | An entry without `context` is skipped; if no valid entries remain, `None` is returned. |
| `test_missing_path_skipped` | An entry without `path` is skipped; if no valid entries remain, `None` is returned. |
| `test_file_not_found_returns_none` | When `smartfolders.yaml` doesn't exist, `_load_root_smart_folders()` returns `None`. |

## Test Helpers

- `_make_root_entry(name, context, path, field, value, filename_regex=None)` — Creates a `RootSmartFolderEntry` with a `SmartFolderConfig` built from field/value condition.
- Reuses existing helpers: `_make_record()`, `_write_file()`, `_make_condition()`, `_make_sf_config()`, `_ts()`.

## Integration Testing

Root-level smart folders are wired into the orchestrator's `run_cycle()` step 11. Integration tests covering the full pipeline (in `test_orchestrator.py` or live integration tests) will exercise root smart folders when a `smartfolders.yaml` is present in the test user root. The unit tests above cover the reconciler logic in isolation.

To verify config reload behavior (changing `smartfolders.yaml` triggers reload), test via the `FilesystemDetector._check_root_smartfolders_yaml()` method. The detector's `config_changed` flag is checked by the orchestrator on each cycle and triggers `reload_config()`.
