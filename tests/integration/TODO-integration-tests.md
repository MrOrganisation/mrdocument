# Integration Test TODO

Test cases to add as end-to-end integration tests.
Each test interacts only through the filesystem and Docker container lifecycle.

## Duplicate Detection

- [x] File added to incoming/ with same content as an already-processed file
      -> moved to duplicates/, not reprocessed
      (TestDuplicateIncoming in test_lifecycle.py)
- [x] File added to sorted/ with same content as an already-processed file
      -> moved to duplicates/
      (TestDuplicateSorted in test_lifecycle.py)
- [ ] Two identical files added to incoming/ in quick succession
      -> only one processed, second moved to duplicates/
      (not feasible: when both detected in the same cycle, source_file
      returns archive entry after processing, so duplicate check never
      triggers for the co-detected file)

## Error Handling & Recovery

- [x] Service returns error (e.g. corrupt/empty PDF)
      -> source moved to error/, 0-byte output cleaned up
      (TestErrorHandlingAndRecovery phase 1 in test_lifecycle.py;
      mock_ocr.py returns 500 for empty files)
- [x] File previously in error/ resubmitted via incoming/ with new content
      -> processed successfully (recovery)
      (TestErrorHandlingAndRecovery phase 2 in test_lifecycle.py)

## Missing File Detection

- [x] Processed file deleted from sorted/ by user
      -> record detects missing state (file disappears from sorted/)
      (TestMissingFileDetection in test_lifecycle.py)
- [x] Processed file deleted from sorted/, then same file re-added
      -> record recovers to complete state
      (TestMissingFileRecovery in test_lifecycle.py)

## Deletion via Trash

- [x] File moved to trash/ by user
      -> all associated files (archive, sorted, symlinks) cleaned up to void/
      (TestTrashDeletion in test_lifecycle.py)

## Stray File Handling

- [x] Unknown file placed directly in archive/
      -> moved to error/
      (TestStrayArchive in test_lifecycle.py)
- [x] Unknown file placed in incoming/
      -> processed normally (not treated as stray)
      (TestStrayIncoming in test_lifecycle.py)

## Sorted Directory User Interactions

- [x] User renames file in sorted/
      -> record adopts new user-chosen filename
      (TestUserRenameInSorted in test_lifecycle.py)
- [x] User moves file from sorted/arbeit/ to sorted/privat/
      -> record updates context to privat
      (TestUserMoveContext in test_lifecycle.py)
- [ ] Two files with same computed name placed in sorted/
      -> collision resolved with UUID suffix, both preserved
      (needs two source files with identical metadata; covered by unit tests)

## Smart Folder Symlinks

- [x] Smart folder symlink removed when file metadata no longer matches
      (e.g. file re-sorted to different context)
      (TestSmartFolderRemovalOnMove in test_lifecycle.py)
- [x] Broken smart folder symlinks cleaned up after source file deleted
      (TestBrokenSmartFolderCleanup in test_lifecycle.py)

## Audio Links

- [ ] Audio transcript symlink in sorted/ points to original audio in archive/
      (already partially covered by audio tests, but no explicit assertion on
      symlink target correctness after rename/move)

## Config Reload

- [ ] Context YAML changed on disk while watcher is running
      -> watcher reloads config, subsequent files use new config

## Multi-User Isolation

- [ ] Two user directories with separate configs
      -> files processed independently, no cross-contamination
