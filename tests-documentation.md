# MrDocument Watcher -- Test Documentation

## Test Architecture

Tests are organized into two tiers:

- **Unit tests** (`mrdocument/watcher/test_*.py`): Fast, isolated tests using `tmp_path`, mock HTTP services, and (for DB tests) a real PostgreSQL instance. No Docker required for most.
- **Integration tests** (`mrdocument/tests/integration/`): End-to-end tests against a running Docker stack with real mrdocument-service, mock adapters (Anthropic, OCR, STT), and real filesystem operations. Optionally run via Syncthing for remote testing.

### Running Tests

```bash
# Unit tests (no Docker, except DB tests need PostgreSQL)
cd mrdocument/watcher
python3 -m pytest test_models.py test_step1.py test_step2.py test_step3.py \
    test_step4.py test_step5.py test_step6.py test_prefilter.py test_app.py -v

# DB tests (need PostgreSQL at localhost:5432)
python3 -m pytest test_db_new.py -v

# Orchestrator tests (need PostgreSQL + mock HTTP service)
python3 -m pytest test_orchestrator.py -v

# Integration tests (need Docker stack running)
cd mrdocument/tests/integration
python3 -m pytest test_documents.py test_audio.py test_migration.py test_costs.py -v
```


## Unit Tests

### test_models.py -- Data Model Tests

| Class | Tests | Description |
|-------|-------|-------------|
| `TestState` | 3 | State enum values, string equality, invalid raises ValueError. |
| `TestPathEntry` | 4 | Creation, timestamp sorting, equality, hashability. |
| `TestRecordDefaults` | 3 | Minimal construction, partial construction, mutable defaults independence. |
| `TestRecordComputedProperties` | 5 | `source_file` most recent, None when empty, path decomposition (2/3/deep segments, .output). |
| `TestRecordClearTemporaryFields` | 1 | All temp fields reset correctly. |
| `TestChangeItem` | 2 | Addition with hash/size, removal without hash/size. |

### test_step1.py -- Filesystem Detection

| Class | Tests | Description |
|-------|-------|-------------|
| `TestInitialScan` | 1 | Full scan detects all files across all watched directories. |
| `TestRestartIdempotency` | 4 | Files already in DB not re-emitted; new files detected alongside tracked; hash changes detected; missing DB paths emit REMOVAL. |
| `TestAdditions` | 1 | New file triggers ADDITION event. |
| `TestRemovals` | 1 | Removed file triggers REMOVAL event. |
| `TestStrayDetection` | 5 | Unknown files in archive/ moved to error/; incoming/ and sorted/ accept unknowns; known files not moved; .output matched by output_filename. |
| `TestIgnoredFiles` | 2 | Hidden files ignored; Syncthing temp patterns ignored. |
| `TestFileSize` | 1 | File size included in ADDITION events. |
| `TestParentDirs` | 1 | error/ directory auto-created for stray moves. |
| `TestMultipleRecords` | 1 | Multiple add/remove cycles produce correct change list. |
| `TestIsConfigFile` | 6 | context.yaml detected; smartfolders.yaml detected; regular files not flagged; wrong location not flagged; deeply nested not flagged; top-level not flagged. |
| `TestConfigChangedFlag` | 5 | Config files excluded from scan changes; config file changes set flag in incremental mode; smartfolders.yaml sets flag; generated.yaml excluded from scan; generated.yaml sets flag in incremental mode. |

### test_step2.py -- Preprocessing and Reconciliation

| Class | Tests | Description |
|-------|-------|-------------|
| `TestPreprocessOutput` | 3 | .output file with sidecar ingested; zero-byte sets HAS_ERROR; no match ignored. |
| `TestPreprocessOutputDuplicateHash` | 5 | Output hash matches other source_hash → duplicate; matches other hash → duplicate; matches own source_hash only → normal; no match → normal; keeps non-archive sources on duplicate. |
| `TestPreprocessIncoming` | 2 | Matches source_hash; unknown creates new record. |
| `TestPreprocessSorted` | 3 | Matches hash; matches source_hash; unknown creates new record with context. |
| `TestPreprocessOtherLocations` | 4 | archive/ matches source_hash; archive/ unknown not created; processed/ matches hash; trash/ matches source_hash. |
| `TestPreprocessRemovals` | 2 | Removal from source_paths -> missing_source_paths; removal from current_paths -> missing_current_paths. |
| `TestPreprocessLocationAware` | 2 | archive/ prefers source_hash when both match; sorted/ prefers hash when both match. |
| `TestPreprocessIdempotency` | 3 | Duplicate source path not appended; duplicate current path not appended; new path still appended. |
| `TestReconcileSourcePaths` | 4 | Source in trash -> NEEDS_DELETION; IS_NEW generates output_filename; source not in archive creates duplicates; IS_MISSING sets source_reference for archive source. |
| `TestReconcileHasError` | 3 | HAS_ERROR with no current deletes record; HAS_ERROR in archive updates source_reference; HAS_ERROR current paths moved to deleted. |
| `TestReconcileCurrentPaths` | 15 | Current in trash -> NEEDS_DELETION; IS_NEW sets output_filename; NEEDS_PROCESSING no current unchanged; missing current -> IS_MISSING; reappeared -> IS_COMPLETE; still missing stays; invalid location deleted; multiple keep most recent; .output from incoming -> processed/; .output from sorted -> sorted/; processed -> IS_COMPLETE; reviewed targets sorted/; sorted matching -> IS_COMPLETE; sorted non-matching adopts filename; context change adds missing fields. |
| `TestComputeTargetPath` | 4 | Basic target path; missing context returns None; missing filename returns None; folder hierarchy from metadata. |
| `TestIsCollisionVariant` | 9 | Exact match; collision suffix matched; different suffix matched; different filename no match; different directory no match; non-hex no match; wrong length no match; deep path; txt extension. |
| `TestReconcileSortedCollisionVariant` | 6 | Collision variant IS_COMPLETE; deep folder IS_COMPLETE; user rename adopted; context change adopted; both changes adopted; exact match still works. |

### test_step3.py -- Processing Service Calls

Uses `MockService` and `MockSTTService` for HTTP mocking.

| Class | Tests | Description |
|-------|-------|-------------|
| `TestProcessorSuccess` | 4 | Output and sidecar written; correct file read; sidecar has expected keys; atomic write (no .tmp left). |
| `TestProcessorErrors` | 2 | Service 500 writes zero-byte; unreachable writes zero-byte. |
| `TestTypeDetection` | 5 | PDF sends type=document; audio without STT URL zero-byte; unsupported extension zero-byte; audio extension detection; MIME type lookup. |
| `TestSttConfig` | 3 | Missing stt.yaml returns None; default values loaded; custom values loaded. |
| `TestAudioSuccess` | 2 | Complete audio pipeline; STT receives keyterms from classify. |
| `TestAudioErrors` | 6 | Missing stt.yaml; missing stt_url; STT failure; empty transcript; classify_audio failure continues; process_transcript failure. |
| `TestAudioIntroTwoPass` | 3 | Intro triggers two STT passes; second pass failure uses first; classify failure uses first pass. |

### test_step4.py -- Filesystem Moves

| Class | Tests | Description |
|-------|-------|-------------|
| `TestSourceReferenceArchive` | 2 | Source moved to archive/; archive/ directory created. |
| `TestSourceReferenceError` | 1 | HAS_ERROR moves source to error/. |
| `TestSourceReferenceMissing` | 1 | IS_MISSING moves source to missing/. |
| `TestDuplicateSources` | 7 | Duplicates moved to duplicates/; subdir structure preserved; multiple duplicates; collision UUID; missing source skipped; directories created; not in void/. |
| `TestCurrentReferenceTargetPath` | 2 | File moved to target_path; target parent dirs created. |
| `TestDeletedPaths` | 2 | deleted_paths moved to void/; multiple paths handled. |
| `TestNeedsDeletion` | 1 | All source + current paths moved to void/. |
| `TestCollision` | 1 | Existing file -> UUID suffix appended. |
| `TestNoOp` | 1 | No changes when all temp fields null. |
| `TestParentDirs` | 1 | void/ subdirectories created. |
| `TestMultipleRecords` | 1 | Multiple records each processed. |
| `TestMissingSource` | 1 | Missing source skipped without error. |
| `TestCurrentPathsUpdated` | 3 | current_paths updated after successful move; deleted .output sidecar cleaned up; moved to missing on failure. |

### test_step5.py -- Smart Folder Symlinks

| Class | Tests | Description |
|-------|-------|-------------|
| `TestSymlinkCreated` | 2 | Symlink for matching record; directory created on demand. |
| `TestNoSymlink` | 2 | No symlink for non-matching record; non-sorted records skipped. |
| `TestSymlinkRemoval` | 1 | Symlink removed when condition no longer matches. |
| `TestMultipleSmartFolders` | 1 | File in multiple smart folders simultaneously. |
| `TestOrphanCleanup` | 3 | Broken symlinks removed; stale symlinks removed; regular files not touched. |
| `TestFilenameRegex` | 2 | Non-matching files filtered out; matching files included. |
| `TestOperatorConditions` | 3 | AND, OR, NOT conditions work. |
| `TestCollisionAvoidance` | 1 | Non-symlink files not overwritten. |
| `TestRootSmartFolder` | 7 | Symlink at absolute path; relative target; condition filtering; filename regex; removal; wrong context skipped; collision first wins. |
| `TestRootSmartFolderCleanup` | 3 | Broken sorted/ links removed; external links preserved; regular files untouched. |
| `TestLoadRootSmartFolders` | 5 | YAML parsing; relative path resolution; missing context skipped; missing path skipped; missing file returns None. |

### test_step6.py -- Audio Link Symlinks

| Class | Tests | Description |
|-------|-------|-------------|
| `TestLinkCreated` | 3 | Symlink for audio record; nested path relative symlinks; link updated on target change. |
| `TestNoLink` | 3 | Non-audio skipped; non-complete skipped; non-archive source skipped. |
| `TestCollisionAvoidance` | 1 | Regular files not overwritten. |
| `TestOrphanCleanup` | 3 | Broken links removed; stale links removed; smart folder links preserved. |
| `TestRenameAndMove` | 2 | Rename updates link; move to different directory updates link. |
| `TestIdempotency` | 1 | Running twice is no-op. |

### test_prefilter.py -- Unsupported File Filtering

| Class | Tests | Description |
|-------|-------|-------------|
| `TestPrefilter` | 20 | Moves unsupported from incoming/, archive/, sorted/, processed/, reviewed/, trash/; skips error/ and void/; skips hidden dirs; keeps supported files; handles name collisions; handles multiple unsupported; skips hidden files; skips symlinks; handles empty root; creates error/; moves no-extension files; skips config files; skips generated.yaml; moves other YAML in sorted/. |

### test_db_new.py -- Database Layer

Requires PostgreSQL at `localhost:5432`.

| Class | Tests | Description |
|-------|-------|-------------|
| `TestCRUD` | 5 | Create and get; nonexistent returns None; save updates; delete removes; delete nonexistent returns False. |
| `TestQueries` | 10 | Records by state; by source_hash; source_hash not found; by hash; hash not found; by output_filename; output_filename not found; snapshot all; with temp fields; with output_filename. |
| `TestUsernameFiltering` | 6 | Snapshot filtered by username; no username returns all; temp fields filtered; output filename filtered; username round trip; state query filtered. |
| `TestJSONBRoundTrips` | 7 | PathEntry list; metadata dict; empty lists; duplicate_sources; null metadata; missing_paths; full record. |

### test_app.py -- Application Lifecycle

| Class | Tests | Description |
|-------|-------|-------------|
| `TestHealthServer` | 3 | 503 when not ready; 200 when ready; startup/shutdown. |
| `TestEnsureDirectories` | 2 | All required dirs created; idempotent. |
| `TestSetupUser` | 4 | Returns DocumentWatcherV2; context field names loaded; missing context configs handled; directories created. |
| `TestSingleCycleEndToEnd` | 1 | File through complete lifecycle (requires DB). |
| `TestMultiUserDiscovery` | 1 | Multiple users get separate watchers. |
| `TestContextConfigLoading` | 1 | Multiple contexts loaded. |
| `TestGracefulShutdown` | 2 | Health server stops; DB disconnects. |
| `TestErrorResilience` | 1 | Unreachable service no crash (requires DB). |
| `TestSortedConfigLoading` | 5 | Loads from sorted/; name mismatch rejected; smart folders loaded; smart folder context mismatch rejected; context_manager passed to watcher. |
| `TestGeneratedData` | 8 | is_new_item for unknown/existing; record_new_item; record_new_clue; clues rejected without allow_new; get_context_for_api merges generated; generated file loaded on reload; no candidates field ignored. |

### test_orchestrator.py -- Orchestrator Integration

Requires PostgreSQL. Uses `MockService` HTTP server.

| Class | Tests | Description |
|-------|-------|-------------|
| `TestEndToEnd` | 1 | New file through full pipeline to sorted/. |
| `TestProcessedToSorted` | 1 | File in processed/ moves to sorted/ via reviewed/. |
| `TestSortedInPlace` | 1 | File placed directly in sorted/ processed and renamed. |
| `TestDuplicateDetection` | 1 | Duplicate source detected and moved to duplicates/. |
| `TestDuplicateOutputHash` | 2 | Output hash matches other record's source_hash → duplicate discarded, record deleted; output hash matches other record's hash → duplicate discarded, record deleted, stray cleanup. |
| `TestNeedsDeletion` | 2 | Source file in trash triggers deletion; processed file (matched by hash) in trash triggers deletion. |
| `TestErrorRecovery` | 1 | HAS_ERROR from 0-byte output; recovery when new source appears. |
| `TestMissingFile` | 1 | Current file deletion -> IS_MISSING state. |
| `TestConfigReload` | 1 | Config file change triggers reload. |
| `TestSmartFolders` | 1 | Smart folder symlinks created for matching records. |
| `TestRootSmartFolders` | 1 | Root-level smart folder symlinks at arbitrary paths. |
| `TestAudioLinks` | 1 | Audio link symlinks placed next to transcripts. |
| `TestNewClues` | 1 | New clues from service recorded in generated.yaml. |
| `TestMultiUser` | 1 | Multi-user isolation (users don't see each other's records). |


## Integration Tests

Integration tests run against a real Docker stack. The mrdocument-service is **not mocked** -- only the adapters (Anthropic, OCR, STT) are mocked.

### Infrastructure

**Docker Compose configurations:**
- `docker-compose.fast.yaml` -- Local testing without Syncthing.
- `docker-compose.service-mock.yaml` -- Full setup with Syncthing sync.
- `docker-compose.test.yaml` -- All-in-one with real adapters.

**Services:**
- `mrdocument-service` -- Real service with mock Anthropic/OCR backends.
- `mrdocument-watcher` -- Real watcher watching `./testdata` mounted at `/sync/testuser`.
- `mrdocument-db` -- PostgreSQL 17.
- `mock-anthropic`, `mock-ocr`, `mock-elevenlabs` -- Mock adapter services.
- `stt` -- Real STT service using mock-elevenlabs as backend.

**Test configuration:**
- `test_config.yaml` / `test_config_fast.yaml` -- `sync_folder`, `poll_interval`, `max_timeout`.
- `config/sorted/{context}/context.yaml` -- Test context definitions (arbeit, privat).
- `config/sorted/{context}/smartfolders.yaml` -- Test smart folder configs.

### conftest.py -- Fixtures and Helpers

**Fixtures:**
- `test_config` (session) -- Parsed `TestConfig` with all directory paths.
- `generated_dir` (session) -- Path to pre-generated test documents.
- `clean_all_dirs` (session, autouse) -- Clears all directories at session start.
- `clean_working_dirs` (function) -- Clears transient dirs before a test.
- `clean_sorted` (function) -- Clears sorted/ explicitly.
- `reset_environment` (function, autouse) -- Truncates DB, clears all working directories, re-deploys configs before each test.
- `deploy_config` (session, autouse) -- Deploys test configs to sync folder.
- `ensure_service_ready` (session, autouse) -- Polls `/health` until 200.
- `ensure_syncthing_synced` (session, autouse) -- Waits for Syncthing sync (if enabled).

**YAML Fixture Collection:**
- `pytest_collect_file()` -- Collects `.yaml` files in `fixture_tests/` as pytest test items via `YamlFixtureFile`.

**Helpers:**
- `atomic_copy(src, dest)` -- Temp file + rename (mirrors Syncthing delivery).
- `poll_for_file(directory, pattern, interval, timeout)` -- Poll flat directory.
- `poll_for_file_recursive(directory, pattern, interval, timeout)` -- Poll recursive, filter symlinks.
- `verify_filename_components(filename, context, date, type)` -- Check filename parts.
- `verify_filename_keywords(filename, keywords)` / `verify_no_filename_keywords()` -- Keyword presence/absence.
- `verify_smart_folder_symlink()` / `poll_for_smart_folder_symlink()` -- Smart folder verification.
- `verify_audio_link_symlink()` / `poll_for_audio_link_symlink()` -- Audio link verification.
- `verify_intro_in_text(text, key_phrases)` -- Fuzzy intro text matching (>=60% word overlap).

### YAML Fixture Tests (`fixture_tests/`)

Most integration tests are declarative YAML fixtures. Each `.yaml` file defines a multi-step test with filesystem actions and expected tree assertions.

**Framework** (`fixtures/` package):
- `loader.py` -- Parses YAML fixtures. Supports `copy`, `move`, `delete`, `copy_match` input actions. Template expansion (e.g., `{CURRENT_DATE}`). Timeout parsing (`10s`, `2m`, or numeric).
- `scanner.py` -- Scans the sync folder tree (excludes hidden files, symlinks, YAML configs). Regex matching: each expected pattern must match >= 1 file, each file must match >= 1 pattern. Patterns prefixed with `~` are optional (absorb matching files but don't fail if absent).
- `runner.py` -- Executes fixture steps: performs input actions, then polls the filesystem tree until all expected patterns match or timeout expires. Produces detailed mismatch reports on failure.

**YAML fixture format:**
```yaml
contexts:
  - arbeit
files:
  - filename: example.pdf
    path_of_generated_file: generated/example.pdf  # or content: "inline text"
timeout: 300s  # default: 10s
steps:
  - input:
      - incoming/example.pdf                        # copy file to path
      - move: 'processed/arbeit-.*\.pdf'             # move matching file
        to: 'reviewed/'
      - delete: 'sorted/.*\.pdf'                     # delete matching file
      - copy_match: 'processed/arbeit-.*\.pdf'        # copy matching file
        to: 'reset/'
    expected:
      - 'archive/example\.pdf'                       # regex patterns
      - 'sorted/arbeit/.*/arbeit-.*\.pdf'
      - '~void/.*/processed/arbeit-.*\.pdf'          # ~ prefix = optional
```

**Fixture test files (37 total):**

| Category | Files | Description |
|----------|-------|-------------|
| Incoming pipeline | 6 (`incoming_{context}_{fmt}.yaml`) | Full pipeline: incoming/ -> processed/ -> reviewed/ -> sorted/. 2 contexts x 3 formats. |
| Sorted correct context | 6 (`sorted_correct_{context}_{fmt}.yaml`) | File placed in sorted/{correct_context}/. Verifies rename, archive, path structure. |
| Sorted wrong context | 6 (`sorted_wrong_{ctx}_to_{ctx}_{fmt}.yaml`) | File placed in sorted/{wrong_context}/. Verifies context preserved, filename adapted. |
| Sorted AI disagrees | 3 (`sorted_ai_disagrees_{fmt}.yaml`) | File in sorted/ where AI suggests different context. Verifies user placement is respected. |
| Subfolder field locking | 4 (`subfolder_locked_*.yaml`) | File in sorted/{context}/{subfolder}/. Verifies subfolder-derived fields are locked. |
| Reset pipeline | 3 (`reset_{context}_{fmt}.yaml`) | Copy processed file to reset/ -> filename recomputed and file re-sorted into sorted/. |
| Duplicate incoming | 1 | Same file submitted twice to incoming/ -> second copy to duplicates/. |
| Trash deletion | 1 | File processed -> sorted -> moved to trash/ -> all associated files to void/. |
| Missing file detection | 1 | File processed -> sorted -> deleted from sorted/ -> archive moved to missing/. |
| Error handling recovery | 1 | Empty PDF -> error/. Valid text file -> processed normally alongside error. |
| Stray files | 2 (`stray_archive.yaml`, `stray_incoming.yaml`) | Unknown files in archive/ -> error/. Unknown files in incoming/ -> processed normally. |
| Unsupported prefilter | 1 | `.ttf`, `.otf`, `.numbers`, `.tex` files moved to error/. |
| Conditional filename | 1 | Audio file with AI-disagreed context uses conditional filename pattern. |

### test_documents.py -- Document Pipeline (Python)

Tests remaining as Python because they require specific assertions not expressible in YAML.

| Class | Tests | Description |
|-------|-------|-------------|
| `TestFilenameKeywords` | 4 | Filename keywords from context config present/absent in output filenames. |
| `TestWatcherRestart` | 1 | In-flight document completes after watcher container restart. |

### test_lifecycle.py -- Document Lifecycle (Python)

Tests remaining as Python because they require DB queries, watcher restarts, symlink assertions, or content verification.

| Class | Tests | Description |
|-------|-------|-------------|
| `TestDuplicateSorted` | 1 | Original source file added to sorted/ after processing -> moved to duplicates/ (source_hash match). |
| `TestMissingFileRecovery` | 1 | Processed file deleted from sorted/, then re-added via incoming/ -> record recovers to complete state. |
| `TestTrashFromSorted` | 1 | File processed through sorted/, then source copy placed in trash/ -> archive and sorted output cleaned up. |
| `TestUserRenameInSorted` | 1 | User renames file in sorted/ -> record adopts new filename. |
| `TestUserMoveContext` | 1 | User moves file from sorted/arbeit/ to sorted/privat/ -> record updates context. |
| `TestSmartFolderRemovalOnMove` | 1 | Smart folder symlink removed when file re-sorted to different context. |
| `TestBrokenSmartFolderCleanup` | 1 | Smart folder symlink cleaned up after source file trashed. |
| `TestContentHashBackfill` | 1 | Content hashes NULLed in DB, watcher restarted -> hashes backfilled from files on disk. |
| `TestMigrationDedup` | 2 | Old DB without content hashes: sorted/ record beats processed/ records; most recent wins when all in sorted/. |

### test_audio.py -- Audio/STT Pipeline

| Class | Tests | Description |
|-------|-------|-------------|
| `TestAudioWithIntro` | 2 (beginning, end) | Audio with intro segment: two-pass STT flow. Verifies transcript content, audio link symlink. |
| `TestAudioWithoutIntro` | 1 | Single-pass STT. Verifies transcript, audio link. |
| `TestVideoFormats` | 2 (.mov, .mp4) | Video file transcription. |
| `TestAudioFilenamePattern` | 1 | Audio uses `audio_filename` pattern from context config. |

### test_migration.py -- V1 to V2 Migration

| Class | Tests | Description |
|-------|-------|-------------|
| `TestMigrationRecordCount` | 3 | Expected records migrated; correct count; unprocessed skipped. |
| `TestFieldMapping` | 5 (parametrized) | original_filename, source_hash, context, assigned_filename, metadata preserved. |
| `TestPathConversion` | 2 | Absolute paths -> relative paths. |
| `TestFileLocationsMerge` | 1 | file_locations merged into current_paths. |
| `TestStartupStalePaths` | 2 (parametrized) | Missing files -> IS_MISSING; paths moved to missing_*_paths. |
| `TestV1Preserved` | 2 | V1 records untouched; V1 statuses unchanged. |

### test_costs.py -- Cost Tracking

| Class | Tests | Description |
|-------|-------|-------------|
| `TestCostTracking` | 1 | Process a document, verify `costs/testuser/mrdocument_costs.json` appears with Anthropic usage data (input_tokens, output_tokens, cost, documents > 0). |


## Test Data

### Generated Documents

Pre-generated test documents in `tests/integration/generated/`:
- Each `(file_stem, format)` pair is unique across all tests.
- Formats: `.pdf`, `.txt`, `.rtf`.
- Content designed to trigger specific metadata extraction (context, type, sender, date).

### Generated Audio

Pre-generated audio files for STT testing:
- With and without intro segments.
- Various formats (`.m4a`, `.mp3`, `.mov`, `.mp4`).

### V1 Seed Data

`v1_seed.sql` provides V1 database records for migration testing.

### Test Contexts

Two test contexts with distinct field configurations:
- `arbeit` -- Business documents: Rechnung/Vertrag/Angebot types, filename_keywords, folders by context+sender.
- `privat` -- Personal documents: Arztbrief/Versicherung/Kontoauszug types, allow_new_candidates for sender, folders by context+type.


## Known Pre-existing Test Failures

- `test_parse_minimal_config` -- min_block=30 hardcode vs test expects 25 (scheduler, not watcher).
- `test_parse_with_scheduling_overrides` -- buffer=0 hardcode vs test expects 10 (scheduler, not watcher).
