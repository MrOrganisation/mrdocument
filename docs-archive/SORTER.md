# Sorter

The sorter service watches two directories per user and manages the lifecycle of documents in `sorted/`:

- `reviewed/` — files arriving from the processing pipeline, ready to be placed into the folder tree
- `sorted/` — the nested folder structure where documents live permanently

## Folder Structure

Each context defines a `folders` list that determines the nesting:

```yaml
# work.yaml
name: work
filename: "{context}-{type}-{date}-{sender}-{topic}"
folders:
  - context    # depth 0
  - sender     # depth 1
  - topic      # depth 2 (leaf)
```

This produces a tree like:

```
sorted/
└── work/                  # context
    └── acme/              # sender
        ├── billing/       # topic (leaf folder)
        │   ├── doc.pdf
        │   └── @urgent/   # smart folder (symlinks)
        │       └── doc.pdf -> ../doc.pdf
        └── support/
            └── ticket.pdf
```

The **leaf folder** is the deepest level defined by `folders`. Files live at leaf depth. Smart folder subdirectories sit inside leaf folders.

## Metadata Field Terminology

Each context defines `field_names` — the implicit fields (`context`, `date`) plus all keys from `fields:` in the context YAML. A document's metadata record is compared against these field names to determine its state:

| Term | Definition | Action |
|------|-----------|--------|
| **Missing** | Field is in `field_names`, record has the key with value `null` | File is incomplete, eligible for reprocessing |
| **Not specified** | Field is in `field_names`, record does not have the key at all | No action (field was never part of the record) |
| **Additional** | Field is NOT in `field_names`, record has the key with non-null value | Left alone |
| **Superfluous** | Field is NOT in `field_names`, record has the key with `null` value | Cleaned up (removed from metadata) |

- A file is **incomplete** if it has any **missing** fields.
- A file is **complete** if it has no **missing** fields.
- **Migration backfill** converts "not specified" → "missing" (adds absent fields as `null`).
- **Reprocessing** fills "missing" (null) fields; existing non-null fields are locked.

Superfluous fields are automatically cleaned up at every metadata write point.

---

## Unsortable Folder

When a file cannot be placed into the folder hierarchy — because a folder field is either absent ("not specified") or null ("missing") — the file is moved to `unsortable/`:

```
/sync/username/unsortable/document.pdf
```

The DB record is updated with `status = 'unsortable'` and `current_file_path` pointing to the unsortable location.

Files in `unsortable/` are re-evaluated when:
- Folder configuration changes (`_resort_context`)
- Migration backfill adds the missing field values
- The file is manually moved back to `sorted/`

Filename conflicts in `unsortable/` are resolved by appending a UUID suffix.

---

## Entry Paths

Files reach `sorted/` through three routes:

### 1. Pipeline: incoming/ → processed/ → reviewed/ → sorted/

The normal flow. A file enters `incoming/`, gets processed by mrdocument (OCR, metadata extraction), lands in `processed/`, is reviewed by the user in `reviewed/`, then the sorter moves it to the correct leaf folder based on its DB metadata.

The file **keeps its current filename** — no rename happens because the pipeline already named it.

After sorting, the file's location is registered in the `file_locations` table and smart folder symlinks are created.

### 2. Direct placement into sorted/

A user (or sync tool) drops a file directly into `sorted/` at any depth. The sorter detects it and processes it. See [File Detection Flow](#file-detection-flow) below.

### 3. Startup scan

On startup, the sorter walks the entire `sorted/` tree. For each file:

- **Has DB record** — its location is backfilled into `file_locations` (idempotent). Additionally:
  - **Migration backfill** (migration=true only): for each field in `field_names` that is absent from the record ("not specified"), adds it as `null` ("missing"). This makes the file eligible for reprocessing.
  - **Reprocessing**: if the file has any missing fields (null values for context-defined fields), it is reprocessed through mrdocument with existing non-null fields as `locked_fields`. This fills in the null values while preserving existing metadata. Only runs if mrdocument URL is available.
- **No DB record** — treated as an unknown file and processed.

---

## File Detection Flow

When a new file appears in `sorted/` (via watchdog `on_created`), the sorter runs this sequence:

```
1. Is it a known leaf file? (DB record exists, at correct leaf depth, context has smart_folders)
   YES → update smart folder symlinks only
   NO  ↓

2. Compute SHA-256, search DB for hash match
   FOUND → this is a COPY of an existing document → _handle_copy
   NOT FOUND ↓

3. Is it at non-leaf depth? (fewer folder levels than context requires)
   YES → process as unknown file (is_leaf=false)
   NO  ↓

4. Is it at leaf depth but without a DB record?
   YES → process as unknown file (is_leaf=true)
```

---

## Unknown File Processing

Applies to files without a DB record, at any depth.

### Branch metadata inference

The file's folder path is mapped to metadata fields. For `folders: [context, sender, topic]`:

| Path | Inferred fields | Depth |
|------|----------------|-------|
| `sorted/work/invoice.pdf` | `{context: work}` | non-leaf |
| `sorted/work/acme/invoice.pdf` | `{context: work, sender: acme}` | non-leaf |
| `sorted/work/acme/billing/invoice.pdf` | `{context: work, sender: acme, topic: billing}` | leaf |

### Processing steps

1. **Create or find DB record.** If the file already has a record (e.g. status=reviewed), use it. Otherwise insert a new document.

2. **Merge inferred fields** from the folder path into metadata (without overwriting existing values).

3. **Check metadata completeness.** A file is complete when no context-defined field in the record has a `null` value. Fields absent from the record ("not specified") do not block completeness.
   - If complete and non-leaf: sort directly to the correct leaf, register in `file_locations`, and stop (steps 4–7 are skipped — no archiving, no mrdocument call).
   - Otherwise: continue to step 4.

4. **Call mrdocument** with the file and `locked_fields` set to the inferred folder values. The AI fills in the remaining metadata.

5. **Archive the original.** The pre-processing version of the file is moved to `archive/`. If a file with the same name already exists in `archive/`, a UUID suffix is appended for disambiguation. The archive filename is stored as `original_filename` in DB. If the document already has a DB record (i.e. it was previously archived — `original_file_hash` is always set on record creation), the file is simply deleted instead of archiving a duplicate.

6. **Determine output filename:**
   - File came through the pipeline (status was reviewed/processed/sorted) → **keep original name**
   - Context has `migration: true` → **keep original name**
   - Otherwise → **rename to `assigned_filename`** (computed from metadata + `filename` pattern)

7. **Write processed output.**
   - **Leaf depth:** write in place — file is already where it belongs.
   - **Non-leaf depth:** write in the same directory, then sort to the correct leaf.

8. **Register** the final path in `file_locations`. Store `assigned_filename` in DB.

---

## Assigned Filename

Every processed document gets an `assigned_filename` computed from its metadata and the context's `filename` pattern. This is stored in the `documents.assigned_filename` column.

```
filename: "{context}-{type}-{date}-{sender}-{topic}"
metadata: {context: work, type: invoice, date: 2026-02-06, sender: acme, topic: billing}
→ assigned_filename: "work-invoice-2026-02-06-acme-billing.pdf"
```

The assigned filename is the **canonical name** the system would give the file. Whether the file is actually renamed to this depends on the entry path and migration mode (see above).

The assigned filename is recomputed whenever metadata changes (moves, copies).

---

## File Locations

The `file_locations` table tracks every physical copy of a document in `sorted/`. Each row has:

- `document_id` — which document this is a copy of
- `file_path` — absolute path on disk (unique constraint)
- `file_hash` — SHA-256 of the file content

A document can have multiple locations (copies in different leaf folders). Locations are:

- **Added** when a file is sorted, processed in place, or detected as a copy.
- **Updated** when a file is moved (path changes).
- **Removed** when a file is deleted (via `on_deleted` handler) or when a copy becomes inconsistent after a move/copy.
- **Reconciled** on startup: entries pointing to non-existent files are deleted.
- **Backfilled** on startup: known files missing from the table are added.

---

## Copy Semantics

When a new file appears and its SHA-256 matches an existing document in DB:

1. Extract branch metadata from the new file's folder position.

2. **Consistent copy** — all inferred fields match the document's existing metadata.
   → Register the new location. All copies are kept.

3. **Inconsistent copy** — the new file is in a branch that contradicts the document's metadata (e.g. different topic).
   → Update metadata to match the **new** branch. Recompute assigned filename. Then scan all other locations: any whose branch is now inconsistent with the updated metadata are **physically deleted** and their location records removed.

The last copy wins: the most recent placement determines the document's metadata.

---

## Move Semantics

When a file is moved within `sorted/` (watchdog `on_moved` with both paths inside `sorted/`):

1. Update the `file_locations` path from old to new.
2. Extract branch metadata from the **destination** path.
3. Update the document's folder-derived metadata fields to match the new branch. Non-folder fields (date, keywords, etc.) are unchanged.
4. Recompute `assigned_filename` (informational — the file is **not renamed** on moves).
5. Update smart folder symlinks (remove old, create new).
6. Check all other locations for consistency. Delete any that now conflict.

Moves caused by the sorter itself (resorting) are excluded from this handling.

---

## Smart Folders

Smart folders are virtual views inside leaf folders, implemented as directories containing symlinks. They are defined per-context:

```yaml
smart_folders:
  "@urgent":
    condition:
      field: keywords
      value: ".*urgent.*"
  "@tax":
    condition:
      operator: and
      operands:
        - field: type
          value: "invoice|receipt"
        - field: topic
          value: "taxes"
    filename_regex: ".*\\.pdf$"
```

### Conditions

Conditions evaluate against the document's metadata fields (from DB). Types:

- **Statement:** `{field, value}` — case-insensitive regex full match against the field value.
- **Operators:** `{operator: and|or|not, operands: [...]}` — boolean logic over sub-conditions.

### filename_regex

Optional filter on the actual filename. If set, only files whose name matches (search, not full match) get symlinks. Files that don't match have their symlinks removed.

### Symlink structure

```
leaf_folder/
├── document.pdf          # actual file
└── @urgent/
    └── document.pdf -> ../document.pdf   # relative symlink
```

Smart folder directories are created automatically when a leaf folder is first populated. Orphaned symlinks (target deleted) are cleaned up on startup and reactively whenever a leaf folder is processed (file added, changed, or deleted).

---

## User Configuration

The user's `config.yaml` controls global sorter behavior:

```yaml
enabled: true        # default: true — set to false to skip this user entirely
migration: false     # default: false — set to true to keep original filenames
reviewed_folder: reviewed
sorted_folder: sorted
```

### enabled

When `false`, the sorter does not watch any of this user's folders or process any of their files. The user is completely skipped during discovery.

### migration

Per-user option for bulk-importing existing document collections. When `true`:

- Files placed in `sorted/` are processed by mrdocument (metadata extraction, OCR).
- The original is archived.
- The processed output **keeps the original filename** instead of being renamed to the assigned filename.
- `assigned_filename` is still computed and stored in DB for reference.
- **Field backfill**: when new fields are added to a context configuration, existing documents get those fields added as `null` (eligible for reprocessing). On startup, files with missing fields are reprocessed with existing metadata locked.

This is useful when importing a pre-organized folder of documents where the existing filenames are meaningful and should be preserved.

When `false` (default), processed output is renamed to the system-assigned filename. New fields added to context configs remain "not specified" in existing records (no backfill).

Files that came through the pipeline (incoming → processed → reviewed → sorted) always keep their name regardless of the migration setting.

---

## Database Schema

### documents table (relevant columns)

| Column | Purpose |
|--------|---------|
| `current_file_path` | Managed by OutputFolderWatcher (not the sorter) |
| `original_filename` | Filename as stored in archive/ (with UUID suffix if disambiguated) |
| `assigned_filename` | System-computed filename from metadata + pattern |
| `metadata` | JSONB — all extracted fields |
| `context_name` | Which context this document belongs to |
| `status` | Lifecycle state: incoming → processing → processed → reviewed → sorted/unsortable |

### file_locations table

| Column | Purpose |
|--------|---------|
| `document_id` | FK to documents |
| `file_path` | Absolute path on disk (unique) |
| `file_hash` | SHA-256 of file content |

Managed exclusively by the sorter (SmartFolderHandler). One document can have many locations. One path belongs to exactly one document.

---

## Config File Watching

The sorter watches `*.yaml` files in the user root directory. When a context file or `contexts.yaml` changes:

1. Old context configurations are snapshotted.
2. Context configurations are reloaded (debounced, 1-second window).
3. Config changes are detected and handled:
   - **New fields added** (migration=true only): all documents of the context get new fields backfilled as `null` ("not specified" → "missing"). If migration=false, new fields remain "not specified" in existing records.
   - **Folders changed**: all documents of the context are re-sorted based on new folder configuration. Files with all folder fields present are moved to the new leaf location. Files with missing/absent folder fields are moved to `unsortable/`. Old smart folder symlinks are cleaned up. Empty directories are removed.
4. Smart folders are re-evaluated with new conditions.
5. Symlinks are created/removed to match updated rules.

---

## Startup Sequence

1. Discover users with `contexts.yaml` in their root.
2. For each user: create sorter, load contexts, ensure `reviewed/` and `sorted/` exist.
3. Process existing files in `reviewed/` (sort them).
4. Process existing leaf folders for smart folder symlinks.
5. Walk `sorted/` tree: backfill `file_locations` for known files, process unknown files.
6. Start filesystem watchers for `reviewed/`, `sorted/`, and config files.
7. Periodically (60s) check for new user directories.

---

## Ignored Files

The following files are always ignored:

- Hidden files (`.` prefix)
- Temp files (`~` prefix, `.tmp` suffix)
- Syncthing intermediates (`.syncthing.` in name)
- Symlinks (in smart folders)

---

## Summary: What Happens to a File

| Scenario | Archive | Rename | Sort | Location tracked |
|----------|---------|--------|------|-----------------|
| Pipeline file (reviewed/ → sorted/) | already done | no | yes | yes |
| Unknown file at non-leaf, complete metadata | no | no | yes (move to leaf) | yes |
| Unknown file at non-leaf, incomplete metadata | yes | migration-dependent | yes (process + move) | yes |
| Unknown file at leaf | yes | migration-dependent | no (already at leaf) | yes |
| Copy to consistent branch | no | no | no | yes (new location added) |
| Copy to inconsistent branch | no | no | no | yes (old conflicting locations deleted) |
| Move within sorted/ | no | no | no | yes (path updated, metadata updated) |
| File with missing folder field | no | no | unsortable/ | yes (status=unsortable) |
| Reprocessed file (incoming, has record) | no | no | via pipeline | locked_fields from existing metadata |
| Config change: folders changed | no | no | re-sorted | paths updated |
