# Feature/Change Requests

For these specs and test cases, please check:

* Are the features already implemented accordingly? If not, add the new feature or adapt the existing one.
* Are the test cases present? If not, add the test case. If a feature changed and requires modifications to existing test cases, make a note of this but do not change the existing test case yet.

## Context Configurations
### Specs
* Context configuration files are now located in `sorted/{context}/context.yaml`.
* The watcher watches `sorted` for `context.yaml` files in direct subdirs and loads the contexts found. (Analogous to watching the mrdocument root right now.)

### Test Cases
* All contexts are loaded at startup.
* New contexts are loaded when added.
* Removed contextst are unloaded.
* `context.yaml`s are watched for changes and reloaded.
Note: You may change existing test cases for the Context Configurations without further confirmation.


## Database Schema Changes
### Specs
* The DB has the following columns:
* * `original_filename`: Original filename of the file that was added to the system.
* * `source_paths`: List of tuples of a path (including filename) where the source file is found within the mrdocument tree and a timestamp when it was first seen at this path.
* * `source_hash`: Hash of the source file.
* * `context`: The context the entry was assigned.
* * `metadata`: The metadata that was determined for this entry.
* * `assigned_filename`: Filename assigned using metadata and filename pattern.
* * `hash`: The hash of the processed file.
* * `current_paths`: List of tuples of a path (including filename) where the processed file is found within the mrdocument tree and a timestamp when it was first seen at this path.
* * `output_filename`: UUID filename for Step 3 output. Step 3 writes to `.output/{output_filename}`. Set when entering `needs_processing`, cleared after ingestion.
* * `state`: Enum of:
* * * `is_new`: Initial value.
* * * `needs_processing`: The file is marked for processing.
* * * `is_missing`: The source file had been processed in the past, but the result is gone missing.
* * * `is_lost`: The source file had been processed in the past, but the result is lost.
* * * `has_error`: Processing failed.
* * * `needs_deletion`: The entry's files are marked for deletion.
* * * `is_deleted`: All files have been deleted.
* * * `is_complete`: Was processed, all data available.
* * `target_path`: Temporary value. Destination path for Step 4 file moves.
* * `source_reference`: Temporary value. Source file to be archived by Step 4.
* * `current_reference`: Temporary value. Current file to be moved by Step 4.
* * `duplicate_sources`: Temporary value. Source duplicates to be voided by Step 4.
* * `deleted_paths`: Temporary value. Files to be voided by Step 4.
* * `missing_source_paths`: Bookkeeping for lost files.
* * `missing_current_paths`: Bookkeeping for lost files.

* All `source_paths` and `current_paths` are relative to the mrdocument root.

* There shal be migration code that checks the DB on startup and migrates the old DB to the new.
* This migration shall be completed before any processing starts.

* Each context defines `field_names` — the implicit fields (`context`, `date`) plus all keys from `fields:` in the context YAML. A document's metadata record is compared against these field names to determine its state:

| Term | Definition | Meaning |
|------|-----------|--------|
| **Missing** | Field is in `field_names`, record has the key with value `null` | Makes entry is incomplete |
| **Not specified** | Field is in `field_names`, record does not have the key at all | Does not make entry incomplete |
| **Additional** | Field is NOT in `field_names`, record has the key with non-null value | Can be ignored |
| **Superfluous** | Field is NOT in `field_names`, record has the key with `null` value | Can be cleaned up |


### Test Cases
* A database with the previous schema is succesfully and correctly migrated to the new schema.


## Logic
### Specs
Business logic is organized into four steps. Steps 1, 2, and 4 run sequentially in a cycle. Step 3 runs independently.

#### Execution Model
* Steps 1 → 2 → 4 form a sequential cycle.
* Step 3 runs independently on its own loop, picking up entries where `output_filename` is not null.
* After Step 4 moves files, Step 1 of the next cycle detects the changes.
* Only Step 2 writes to the DB.
* Step 3 writes files only (result file + `.meta.json` sidecar to `.output/`). It does not write to the DB.
* Step 1 receives a read-only snapshot of the DB for stray file detection. It does not write to the DB.
* Step 4 receives a read-only snapshot of the DB and only performs filesystem operations. It does not write to the DB.
* `void/` is not watched — files moved there are effectively removed from the system.
* `error/` is not watched — files moved there are for user review.
* `.output/` is cleaned up separately (not by any of the four steps).

#### Step 1: Detect Filesystem Changes
This step detects filesystem changes and filters out stray files. It receives a read-only snapshot of the DB but does not write to it.

* Step 1 considers all files directly in:
* * `archive`
* * `incoming`
* * `reviewed`
* * `processed`
* * `trash`
* * `.output`
* Step 1 considers all files recursively in `sorted`.
* It scans all files on startup and watches all directories for changes.

* Step 1 collects filesystem events using a quiescence window: events are accumulated until no new changes have arrived for a configurable duration (debounce). Once quiescent, Step 1 processes the events.

* Stray file handling:
* * Only `incoming` and `sorted` (recursive) accept new files (i.e. files not known to the DB).
* * Using the R/O DB snapshot, Step 1 checks each addition against known hashes (`source_hash`, `hash`) and `output_filename` values.
* * If an added file is unknown and is in a directory that does not accept new files, it is moved to `error/` immediately. This addition is not included in the change list.

* After stray file handling, Step 1 produces the change list and hands it to Step 2. The change list is a list of items, each being:
* * `addition`: A file was created or appeared at a path. Includes the path, the SHA-256 hash, and the file size in bytes.
* * `removal`: A file was deleted or disappeared from a path. Includes the path.

#### Step 2: Reconcile Database
This step receives the change list from Step 1, updates the DB, and prepares instructions for Step 4. It is the only step that writes to the DB (Step 3 does not).

##### Preprocessing
Preprocessing applies the change list to the DB, updating `source_paths` and `current_paths`. Matching is location-aware to handle the case where `source_hash` equals `hash`:

| Location | Match against | New entries |
|---|---|---|
| `incoming` | `source_hash` | Yes |
| `sorted` | `hash` first, then `source_hash` | Yes |
| `archive`, `lost` | `source_hash` only | No |
| `processed`, `reviewed` | `hash` only | No |
| `trash` | `source_hash` or `hash` | No |
| `.output` | `output_filename` only | No |

* For each `addition` in the change list:

* * If the file is under `.output`:
* * * Match the filename against `output_filename` of entries in `needs_processing` state.
* * * If no match: ignore (orphaned output; cleaned up separately).
* * * If file size is 0 bytes: set the matched entry to `has_error`. Clear `output_filename`. Done with this addition.
* * * Read the `.meta.json` sidecar file at `.output/{output_filename}.meta.json`.
* * * Set the entry's `context`, `metadata`, and `assigned_filename` from the sidecar.
* * * Set the entry's `hash` to the file's hash.
* * * Add the `.output` path to the entry's `current_paths` with the current timestamp.
* * * Clear `output_filename`.
* * * Done with this addition.

* * If the file is in a location that matches against `hash` (per table above):
* * * When the file hash equals the `hash` of an entry, the path is added to that entry's `current_paths` with the current timestamp, unless already present. Done with this addition.

* * If the file is in a location that matches against `source_hash` (per table above):
* * * When the file hash equals the `source_hash` of an entry, the path is added to that entry's `source_paths` with the current timestamp, unless already present. The `original_filename` is not modified even if the file has a different name. Done with this addition.

* * If the file is in a location that accepts new entries (per table above) and no match was found:
* * * A new entry is created with `original_filename`, `source_paths` and `source_hash` set accordingly.
* * * Done with this addition.

* For each `removal` in the change list:
* * If the removed path is in any entry's `source_paths`, it shall be removed and added to `missing_source_paths`.
* * If the removed path is in any entry's `current_paths`, it shall be removed and added to `missing_current_paths`.

* All entries modified by preprocessing are sent for reconciliation.

##### Reconciliation
Reconciliation runs on every entry modified by preprocessing. It first **clears all temporary fields** (`source_reference`, `current_reference`, `target_path`, `deleted_paths`, `duplicate_sources`, `output_filename`), then re-evaluates the entry and sets them as needed.

###### Aliases
* We define the following aliases:
* * `source_file`: Most recent item in `source_paths`.
* * `current_file`: Most recent item in `current_paths`.
* * `source_location`, `source_location_path` and `source_filename`: Defined as `source_file == "{mrdocument_root}/{source_location}/{source_location_path}/{source_filename}`.
* * `current_location`, `current_location_path` and `current_filename`: Defined as `current_file == "{mrdocument_root}/{current_location}/{current_location_path}/{current_filename}`.

###### Invariants
* After reconciliation, depending on the state of an entry the following invariants shall hold:
* * Entry `is_new`:
* * * The entry must never leave reconciliation in state `is_new`. I.e.: state `is_new` defaults to violated invariant.

* * Entry `needs_processing`:
* * * `source_paths` has exactly one entry.
* * * `source_location` is `archive`.
* * * `current_paths` is null.
* * * `context` is null.
* * * `metadata` is null.
* * * `hash` is null.
* * * `assigned_filename` is null.
* * * `output_filename` is not null.
* * * `target_path` is null.
* * * `source_reference`: is null.
* * * `current_reference`: is null.

* * Entry `is_missing`:
* * * `source_paths` has not more than one entry.
* * * `source_location` is `archive`.
* * * `current_paths` has no items.
* * * `missing_current_paths` has at least one item.
* * * `context` is not null.
* * * `metadata` is not null.
* * * `hash` is not null.
* * * `assigned_filename` is not null.
* * * `target_path` is null.
* * * `source_reference`: is null.
* * * `current_reference`: is null.

* * Entry `is_lost`:
* * * `source_paths` has not more than one entry.
* * * `source_location` is `lost`.
* * * `current_paths` has no items.
* * * `missing_current_paths` has at least one item.
* * * `context` is not null.
* * * `metadata` is not null.
* * * `hash` is not null.
* * * `assigned_filename` is not null.
* * * `target_path` is null.
* * * `source_reference`: is null.
* * * `current_reference`: is null.

* * Entry `has_error`:
* * * `output_filename` is null.
* * * `target_path` is null.

* * Entry `needs_deletion`:
* * * No specific invariants must be fulfilled.

* * Entry `is_deleted`:
* * * The entry must never leave reconciliation in state `is_deleted`. I.e.: state `is_deleted` defaults to violated invariant.

* * Entry `is_complete`:
* * * `source_paths` has not more than one entry.
* * * `source_location` is `archive`.
* * * `current_paths` has exactly one item.
* * * `current_location` is in [`processed`, `reviewed`, `sorted`].
* * * `context` is not null.
* * * `metadata` is not null and has no missing fields.
* * * `hash` is the correct hash for the single item in `current_paths`.
* * * `assigned_filename` is not null.
* * * `target_path` is null.
* * * `source_reference`: is null.
* * * `current_reference`: is null.

* * For all states except `needs_deletion` the following invariants shall hold:
* * * `original_filename` is not null and has not been changed.
* * * `source_hash` is not null and has not been changed.
* * * The set of paths in `source_paths` and the set of paths in `duplicate_sources` are non-intersecting.
* * * The set of paths in `source_paths` and the set of paths in `deleted_paths` are non-intersecting.
* * * The set of paths in `current_paths` and the set of paths in `deleted_paths` are non-intersecting.

* When an entry's invariants are violated, reconciliation proceeds through the following sections in order.

###### `source_paths`
* If `source_location` is `trash`:
* * Set entry to `needs_deletion`.
* * Reconciliation done. Return.

* If entry `is_new`:
* * Set `output_filename` to a new UUID.
* * Set to `needs_processing`.

* If not `is_lost`:
* * If none of the `source_paths` is under `archive`:
* * * Set `source_reference` to `source_file`.
* * * Add `archive/{source_filename}` with timestamp from `source_file` to `source_paths`.
* * Move all items from `source_paths` that do not have location `archive` to `duplicate_sources`.

* If `is_lost`:
* * If none of the `source_paths` is under `lost`:
* * * Set `source_reference` to `source_file`.
* * * Add `lost/{source_filename}` with timestamp from `source_file` to `source_paths`.
* * Move all items from `source_paths` that do not have location `lost` to `duplicate_sources`.

###### `has_error`
* If the entry `has_error`:
* * If `source_location` is `error` and `current_paths` is empty:
* * * Delete the record from the DB. Reconciliation done. Return.
* * If `source_location` is not `error`:
* * * Set `source_reference` to `source_file`.
* * * Add `error/{source_filename}` with timestamp from `source_file` to `source_paths`.
* * * Move all items from `source_paths` that do not have location `error` to `duplicate_sources`.
* * If `current_paths` is not empty:
* * * Move all items from `current_paths` to `deleted_paths`.
* * Reconciliation done. Return.

###### `current_paths`
* If the entry `is_new`:
* * Set entry to `needs_processing`.
* * Set `output_filename` to a new UUID.
* * Clear all fields that are supposed to be null according to invariants.
* * Reconciliation done. Return.

* If the entry `is_lost`:
* * Reconciliation done. Return.

* If the entry `needs_processing` and has no `current_paths`:
* * Reconciliation done. Return.

* If the entry has no `current_paths` and `missing_current_paths` is not empty:
* * Set entry to `is_missing`.

* If the entry `is_missing` or `is_lost`:
* * If the `current_paths` is not empty:
* * * Set entry to `is_complete`.
* * Else:
* * * Reconciliation done. Return.

* If the entry has at least one `current_paths`:
* * Set `current_reference` to `current_file`.
* * Move all items from `current_paths` that do not have location in [`.output`, `processed`, `reviewed`, `sorted`] to `deleted_paths`.

* If the entry has multiple `current_paths`:
* * Add all items from `current_paths` except the most recent to `deleted_paths`.
* * Only the most recent item remains in `current_paths`.

* If the entry has exactly one `current_paths`:
* * Clear `current_reference`.

* * If `current_location` is `.output`:
* * * Compute `target_path` as `sorted/{path_inferred_from_metadata}/{assigned_filename}`.
* * * Set `current_reference` to `current_file`.

* * If `current_location` is `processed`:
* * * Set entry to `is_complete`.

* * If `current_location` is `reviewed`:
* * * Compute `target_path` as `sorted/{path_inferred_from_metadata}/{current_filename}`.
* * * Set `current_reference` to `current_file`.

* * If `current_location` is `sorted`:
* * * If necessary: Update the metadata of the entry according to the metadata that can be inferred from the `current_location_path`.
* * * If this changed the context of the entry, add all metadata fields defined in the new context as missing to the entry.
* * * If `current_location_path` does not match `{path_inferred_from_metadata}`:
* * * * Set `current_reference` to `current_file`.
* * * * Set `target_path` to `sorted/{path_inferred_from_metadata}/{current_filename}`.
* * * Else:
* * * * Set entry to `is_complete`.

#### Step 3: Process
This step runs independently on its own loop, not sequentially with Steps 1, 2, and 4. It is the only step that communicates with the mrdocument service. It does not write to the DB.

* Step 3 polls for entries where `output_filename` is not null.
* For each such entry:
* * Read the source file from the most recent item in `source_paths`.
* * Determine the type of processing needed:
* * * If the file is a document (PDF, RTF, TXT, EML etc.), it is sent for the implemented two-pass classification.
* * * If the file is an audio file, it is sent for the implemented STT transcription, and the resulting transcript is then classified.
* * On success:
* * * Write the result file to a temporary location, then atomically rename to `.output/{output_filename}`.
* * * Write a `.meta.json` sidecar file at `.output/{output_filename}.meta.json` containing `context`, `metadata`, and `assigned_filename`.
* * * Step 1 of the next cycle will detect the new file. Step 2's preprocessing will match it by `output_filename` and ingest the sidecar.
* * On error:
* * * Create an empty (0-byte) file at `.output/{output_filename}`.
* * * Step 1 of the next cycle will detect the 0-byte file. Step 2's preprocessing will match it by `output_filename` and set the entry to `has_error`.

#### Step 4: Reconcile Filesystem
This step receives a read-only snapshot of the DB and executes file operations. It does not write to the DB. Step 1 of the next cycle detects all resulting changes.

!!! Whenever moving a file, never overwrite an existing file at the destination. Append a UUID if necessary. !!!

* Step 4 takes a snapshot of all entries that have non-null temporary fields or are in state `needs_deletion`.

* For each entry in the snapshot:

* * Process `source_reference`: If set, move the file at `source_reference` to the target location:
* * * For `has_error` entries: `error/{source_filename}`.
* * * For `is_lost` entries: `lost/{source_filename}`.
* * * For all other entries: `archive/{source_filename}`.

* * Process `duplicate_sources`: For each path in `duplicate_sources`, move the file to `void/{source_location_path}/{source_filename}`.

* * Process `current_reference`: If set and `target_path` is also set, move the file at `current_reference` to `target_path`.

* * Process `deleted_paths`: For each path in `deleted_paths`, move the file to `void/{location_path}/{filename}`.

* * Process `needs_deletion` entries: All files under `source_paths` and `current_paths` are moved to `void` maintaining their path structure, i.e. a file at `{mrdocument_root}/{location}/{location_path}/{filename}` is moved to `{mrdocument_root}/void/{location_path}/{filename}`.

### Test Cases
* Step 1 detects additions and removals correctly.
* Step 1's quiescence window batches rapid changes before handing off.
* Step 1 moves unknown files in non-eligible dirs (archive, processed, reviewed, .output) to `error`.
* Step 1 does not move unknown files in eligible dirs (incoming, sorted) — they become new entries.
* Step 2's preprocessing matches files by hash using location-aware rules (per matching table).
* Step 2's preprocessing matches `.output` files by `output_filename`, ingests `.meta.json` sidecar.
* Step 2's preprocessing sets `has_error` when a 0-byte file appears in `.output` matching an `output_filename`.
* Step 2's reconciliation correctly transitions entries through all states.
* Step 2's temporary fields are cleared at the start of reconciliation and re-set as needed.
* Step 3 processes entries in `needs_processing` and writes result + sidecar to `.output` without writing to DB.
* Step 3 writes a 0-byte file to `.output` on processing failure.
* Step 4 executes all file moves indicated by temporary fields.
* Step 4 does not overwrite existing files (appends UUID).
* Step 4 moves source to `error/` for `has_error` entries.
* A full cycle (1→2→4→1→2) correctly moves a new file from `incoming` through `archive` and `.output` to `sorted` as `is_complete`.
* A full cycle correctly moves a file from `reviewed` to `sorted`.
* A full cycle correctly handles a file dropped directly into `sorted` (new entry → processing → sorted).
* A full cycle correctly handles a file moved to `trash` (→ `needs_deletion` → files moved to `void`).
* A full cycle correctly handles duplicate `current_paths` (extras moved to `void`).
* A full cycle correctly handles `has_error`: source moved to `error`, record deleted after next cycle.
* An entry with missing current files transitions to `is_missing`.
* An `is_missing` entry with a reappeared file transitions to `is_complete`.
* When `source_hash == hash`, location-aware matching correctly distinguishes source vs current copies.


## Smart Folders
### Specs
* Smart folders shall not be part of the context configuration anymore.
* There shall be a separate config that defines all smart folders.
* It shall be necessary to specify a fixed context per smart folder.
* Specifying the conditions shall be the same way as it is right now.
* If a file changes location within `sorted`, all its links in smart folders are updated.
* If a file changes metadata, all its links in smart folders are updated. This may mean that they are removed from smart folders that do not match anymore and are added to smart folders that match now.
* All files and directories within smart folders that are not symbolic links shall be completely ignored and never touched.

### Test Cases
* Defined smart folders are created.
* For each smart folder, the matching documents and only those are linked as symbolic links into it.
* If conditions permit, the same file is linked into mulitple different smart folders.
* When a file is renamed/relocated within `sorted`, all of its links are updated.
* When a file changes metadata, its links are removed from non-matching smart folders and added to matching ones.


## Audio File Links
### Specs
* When a new audio file is transcribed, a link to the source audio file in `archive` shall be placed alongside the transcript TXT file.
* The link shall always have the same filename as the TXT.

### Test Cases
* The link is created and has the right filename.
* When the TXT file is renamed manually, the link is renamed as well.
* When the TXT file is moved inside sorted, the existing link is removed and a new link is created at the file's new location.

