# Root-Level Smart Folders

## Smart Folders
### Specs

There are two types of smart folders:

#### Context-Level Smart Folders (existing)
* Smart folders are configured per context in `sorted/{context}/smartfolders.yaml`.
* Each smart folder has a name and defines conditions (field/value matching with regex) and/or a `filename_regex` filter.
* For each leaf folder in `sorted/` belonging to a context, a subdirectory named after the smart folder is created.
* Inside this subdirectory, symbolic links are created for every file in the parent leaf folder whose metadata matches the smart folder's conditions and whose filename matches `filename_regex` (if set).
* The symlink target is always relative (`../filename`).
* When a file's metadata changes so that it no longer matches, the symlink is removed.
* When a file is renamed or relocated within `sorted/`, its smart folder links are updated.

#### Root-Level Smart Folders (new)
* A single configuration file `smartfolders.yaml` in the mrdocument root defines root-level smart folders.
* Each entry specifies:
  * `context` (required): Which context's documents to consider.
  * `path` (required): Where to place symlinks. Can be absolute or relative to the mrdocument root.
  * `condition` and/or `filename_regex` (at least one required): Same format as context-level smart folders.
* For each `IS_COMPLETE` record in `sorted/` matching the specified context, condition, and filename regex, a symlink is placed at `{path}/{filename}`.
* The symlink target is a relative path from the smart folder directory to the actual file in `sorted/`.
* When a file no longer matches (metadata change, context mismatch, filename regex mismatch), the symlink is removed.
* Name collisions: if two files have the same filename, the first one encountered wins. The second is silently skipped.

### Config Format

```yaml
smart_folders:
  rechnungen_alle:
    context: arbeit
    path: /home/user/Desktop/Rechnungen
    condition:
      field: type
      value: Rechnung
  briefe:
    context: privat
    path: briefe_sammlung
    condition:
      field: type
      value: Brief
    filename_regex: "\.pdf$"
```

### Invariants

* Only `IS_COMPLETE` records in `sorted/` are considered for any smart folder.
* Only records matching the smart folder's `context` are considered.
* Smart folders never touch non-symlink files or directories within the smart folder path.
* Root-level smart folder cleanup only removes symlinks whose resolved target is within `sorted/`. Symlinks pointing elsewhere and regular files are never touched.
* The `smartfolders.yaml` file at the mrdocument root is watched for changes. When it changes, the configuration is reloaded and smart folder reconciliation runs with the updated config.

### Test Cases

* A matching record creates a symlink at the configured absolute path.
* A matching record creates a symlink at a relative path resolved against the mrdocument root.
* Symlink targets are relative (not absolute).
* Records whose metadata does not match the condition get no symlink.
* Records filtered out by `filename_regex` get no symlink.
* When a record's metadata changes so that it no longer matches, the existing symlink is removed.
* Records from a different context than the smart folder's context are skipped.
* When two files have the same filename, the first one wins — the second does not overwrite the first.
* Broken symlinks pointing into `sorted/` are removed during cleanup.
* Symlinks NOT pointing into `sorted/` are left untouched during cleanup.
* Regular files in the smart folder directory are never removed.
* Valid `smartfolders.yaml` is parsed correctly.
* Relative paths are resolved against the mrdocument root.
* Entries missing `context` or `path` are skipped with a warning.
* Missing `smartfolders.yaml` results in no root-level smart folders (no error).
