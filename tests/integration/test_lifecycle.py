"""Integration tests for document lifecycle operations.

Tests cover duplicate detection, stray file handling, missing file detection,
trash deletion, user rename/move in sorted, smart folder lifecycle, error
handling/recovery, and related lifecycle behaviors.

Each test creates inline .txt files with unique content and uses mock metadata
entries keyed by filename stem. All (file_stem, format) pairs are unique
across the entire integration test suite.

NOTE: Many tests have been migrated to YAML fixtures in ``fixture_tests/``.
The following test classes remain as Python because they require DB queries,
watcher restarts, symlink assertions, content verification, or other logic
not expressible in the YAML fixture format.
"""

import hashlib
import shutil
import subprocess
import time
import uuid

import pytest

from conftest import (
    TestConfig,
    atomic_copy,
    db_exec,
    poll_for_file,
    poll_for_file_recursive,
    poll_for_file_recursive_in,
    poll_for_smart_folder_symlink,
    poll_until_gone,
    poll_until_symlink_gone,
    restart_watcher,
    verify_filename_components,
    write_test_file,
)


def _process_to_sorted(
    test_config: TestConfig,
    file_stem: str,
    context: str,
    date: str,
) -> tuple:
    """Helper: create txt file, process through incoming → processed → reviewed → sorted.

    Returns (sorted_file, archive_file) paths.
    """
    content = f"Test document {file_stem}. Unique: {uuid.uuid4().hex}"
    write_test_file(
        test_config.incoming_dir / f"{file_stem}.txt",
        content,
    )

    # Poll for processed output
    pattern = f"{context}-*{date}*.txt"
    processed_file = poll_for_file(
        test_config.processed_dir,
        pattern,
        test_config.poll_interval,
        test_config.max_timeout,
        exclude_names={f"{file_stem}.txt"},
    )
    assert processed_file is not None, (
        f"File not found in processed/ within {test_config.max_timeout}s "
        f"(pattern: {pattern})"
    )

    # Verify archive
    archive_file = poll_for_file(
        test_config.archive_dir,
        f"*{file_stem}*",
        test_config.poll_interval,
        30,
    )
    assert archive_file is not None, f"Not found in archive/ (*{file_stem}*)"

    # Move to reviewed → sorted
    existing_sorted = set(test_config.sorted_dir.rglob(f"*{date}*.txt"))
    reviewed_path = test_config.reviewed_dir / processed_file.name
    shutil.move(str(processed_file), reviewed_path)

    sorted_file = poll_for_file_recursive(
        test_config.sorted_dir,
        f"*{date}*.txt",
        test_config.poll_interval,
        test_config.max_timeout,
        exclude_paths=existing_sorted,
    )
    assert sorted_file is not None, (
        f"File not found in sorted/ within {test_config.max_timeout}s"
    )

    return sorted_file, archive_file


# ===================================================================
# Duplicate Detection
# ===================================================================


class TestDuplicateSorted:
    """Original source file added to sorted/ after already being processed
    should be moved to duplicates/.

    Uses an RTF file so that source_hash != hash (OCR changes content).
    The original source placed in sorted/ matches by source_hash on the
    IS_COMPLETE record, triggering duplicate detection.
    """

    def test_duplicate_sorted_moved_to_duplicates(
        self, test_config: TestConfig, generated_dir, clean_working_dirs,
    ):
        file_stem = "privat_kontoauszug_allianz"
        context = "privat"
        date = "2025-02-18"
        fmt = "rtf"

        src = generated_dir / f"{file_stem}.{fmt}"
        assert src.exists(), f"Source file missing: {src}"

        # Step 1: Process through incoming → processed → reviewed → sorted
        dest = test_config.incoming_dir / src.name
        atomic_copy(src, dest)

        out_ext = "pdf"  # rtf → pdf after OCR
        pattern = f"{context}-*{date}*.{out_ext}"
        processed = poll_for_file(
            test_config.processed_dir,
            pattern,
            test_config.poll_interval,
            test_config.max_timeout,
            exclude_names={src.name},
        )
        assert processed is not None, "File not processed"

        existing_sorted = set(test_config.sorted_dir.rglob(f"*{date}*.{out_ext}"))
        shutil.move(str(processed), test_config.reviewed_dir / processed.name)

        sorted_file = poll_for_file_recursive(
            test_config.sorted_dir,
            f"*{date}*.{out_ext}",
            test_config.poll_interval,
            test_config.max_timeout,
            exclude_paths=existing_sorted,
        )
        assert sorted_file is not None, "File not in sorted/"

        # Step 2: Place the ORIGINAL source RTF in sorted/{context}/
        # source_hash match on IS_COMPLETE record → duplicate
        ctx_dir = test_config.sorted_dir / context
        atomic_copy(src, ctx_dir / src.name)

        # Step 3: Poll for duplicate in duplicates/
        dup_file = poll_for_file_recursive_in(
            test_config.duplicates_dir,
            f"*{file_stem}*",
            test_config.poll_interval,
            test_config.max_timeout,
        )
        assert dup_file is not None, (
            f"Duplicate not found in duplicates/ within {test_config.max_timeout}s"
        )


# ===================================================================
# Missing File Recovery
# ===================================================================


class TestMissingFileRecovery:
    """Processed file deleted from sorted/, then same file re-added
    -> record recovers to complete state.
    """

    def test_missing_recovery_via_incoming(
        self, test_config: TestConfig, clean_working_dirs,
    ):
        # Use the available (arbeit_angebot_keller, rtf) pair
        file_stem = "arbeit_angebot_keller"
        context = "arbeit"
        date = "2025-09-20"
        fmt = "rtf"

        # Need the generated file for this test
        generated_dir = test_config.generated_dir
        src = generated_dir / f"{file_stem}.{fmt}"
        assert src.exists(), f"Source file missing: {src}"

        # Step 1: Process through incoming → processed → reviewed → sorted
        dest = test_config.incoming_dir / src.name
        atomic_copy(src, dest)

        out_ext = "pdf"  # rtf → pdf
        pattern = f"{context}-*{date}*.{out_ext}"
        processed = poll_for_file(
            test_config.processed_dir,
            pattern,
            test_config.poll_interval,
            test_config.max_timeout,
            exclude_names={src.name},
        )
        assert processed is not None, "File not processed"

        existing_sorted = set(test_config.sorted_dir.rglob(f"*{date}*.{out_ext}"))
        reviewed_path = test_config.reviewed_dir / processed.name
        shutil.move(str(processed), reviewed_path)

        sorted_file = poll_for_file_recursive(
            test_config.sorted_dir,
            f"*{date}*.{out_ext}",
            test_config.poll_interval,
            test_config.max_timeout,
            exclude_paths=existing_sorted,
        )
        assert sorted_file is not None, "File not in sorted/"

        # Step 2: Delete from sorted/ → goes missing
        sorted_file.unlink()

        # Wait for missing state (archive moves to missing/)
        missing_file = poll_for_file_recursive_in(
            test_config.missing_dir,
            f"*{file_stem}*",
            test_config.poll_interval,
            test_config.max_timeout,
        )
        assert missing_file is not None, "Archive not moved to missing/"

        # Step 3: Re-add via incoming/ with different content (new source_hash)
        new_dest = test_config.incoming_dir / f"{file_stem}.{fmt}"
        tmp = new_dest.with_suffix(".tmp")
        tmp.write_bytes(src.read_bytes() + b"\x01\x02")
        tmp.rename(new_dest)

        # Step 4: Should be reprocessed — poll for new output in processed/
        result = poll_for_file(
            test_config.processed_dir,
            pattern,
            test_config.poll_interval,
            test_config.max_timeout,
            exclude_names={src.name},
        )
        assert result is not None, (
            f"Recovery file not processed within {test_config.max_timeout}s"
        )

        verify_filename_components(
            result.name,
            expected_context=context,
            expected_date=date,
        )


# ===================================================================
# Deletion via Trash (sorted → trash with source_hash matching)
# ===================================================================


class TestTrashFromSorted:
    """File added directly to sorted/, then source copy placed in trash/.

    Verifies that the archive copy of the source is cleaned up when the
    user puts a copy of the original source file into trash/.
    """

    def test_sorted_then_trash_cleans_archive(
        self, test_config: TestConfig, clean_working_dirs,
    ):
        file_stem = "sorted_trash_doc"
        context = "arbeit"
        date = "2025-12-15"

        # --- Phase 1: Place file directly in sorted/{context}/ ---
        content = f"Test document {file_stem}. Unique: {uuid.uuid4().hex}"
        sorted_ctx_dir = test_config.sorted_dir / context
        sorted_ctx_dir.mkdir(parents=True, exist_ok=True)
        source_path = sorted_ctx_dir / f"{file_stem}.txt"
        write_test_file(source_path, content)

        # File should be moved to archive/ and processed output placed
        # back in sorted/
        archive_file = poll_for_file(
            test_config.archive_dir,
            f"*{file_stem}*",
            test_config.poll_interval,
            test_config.max_timeout,
        )
        assert archive_file is not None, (
            f"Source not found in archive/ within {test_config.max_timeout}s"
        )

        # Wait for the processed output to land in sorted/
        pattern = f"{context}-*{date}*.txt"
        sorted_output = poll_for_file_recursive(
            test_config.sorted_dir,
            pattern,
            test_config.poll_interval,
            test_config.max_timeout,
            exclude_paths={source_path},
        )
        assert sorted_output is not None, (
            f"Processed output not found in sorted/ within "
            f"{test_config.max_timeout}s (pattern: {pattern})"
        )

        # Let watcher settle
        time.sleep(2)

        # --- Phase 2: Put a copy of the source in trash/ ---
        trash_dest = test_config.trash_dir / f"{file_stem}.txt"
        test_config.trash_dir.mkdir(parents=True, exist_ok=True)
        # Write same content to trigger source_hash match
        write_test_file(trash_dest, content)

        # --- Phase 3: Verify cleanup ---
        # Archive copy should be cleaned up
        assert poll_until_gone(
            archive_file,
            test_config.poll_interval,
            test_config.max_timeout,
        ), (
            f"Archive file {archive_file.name} should be removed after "
            "source placed in trash/"
        )

        # Sorted output should be cleaned up
        assert poll_until_gone(
            sorted_output,
            test_config.poll_interval,
            test_config.max_timeout,
        ), "Sorted output should be removed after trash"

        # Trash file should be moved to void/
        assert poll_until_gone(
            trash_dest,
            test_config.poll_interval,
            test_config.max_timeout,
        ), "Trash file should be moved to void/"


# ===================================================================
# Sorted Directory User Interactions
# ===================================================================


class TestUserRenameInSorted:
    """User renames file in sorted/ -> record adopts new user-chosen filename."""

    def test_rename_adopted(
        self, test_config: TestConfig, clean_working_dirs,
    ):
        file_stem = "rename_test_doc"
        context = "arbeit"
        date = "2025-12-25"

        # Process through full pipeline to sorted
        sorted_file, _ = _process_to_sorted(
            test_config, file_stem, context, date,
        )

        # Rename the file in sorted/
        new_name = f"user-custom-name-{date}.txt"
        new_path = sorted_file.parent / new_name
        sorted_file.rename(new_path)

        # The file should stay at the new location (not be moved back)
        # Wait a few scan cycles to make sure the watcher doesn't revert
        time.sleep(3)
        assert new_path.exists(), (
            f"Renamed file should still exist at {new_path}"
        )

        # The old name should not reappear
        assert not sorted_file.exists(), (
            f"Old filename should not reappear at {sorted_file}"
        )


class TestUserMoveContext:
    """User moves file from sorted/arbeit/ to sorted/privat/
    -> record updates context.
    """

    def test_move_context_adopted(
        self, test_config: TestConfig, clean_working_dirs,
    ):
        file_stem = "move_context_doc"
        original_context = "arbeit"
        target_context = "privat"
        date = "2025-12-28"

        # Process through full pipeline to sorted (lands in arbeit/)
        sorted_file, _ = _process_to_sorted(
            test_config, file_stem, original_context, date,
        )

        # Verify it's under arbeit/
        rel = sorted_file.relative_to(test_config.sorted_dir)
        assert rel.parts[0] == original_context

        # Move to privat/ root
        privat_dir = test_config.sorted_dir / target_context
        privat_dir.mkdir(parents=True, exist_ok=True)
        new_path = privat_dir / sorted_file.name
        shutil.move(str(sorted_file), new_path)

        # Context change triggers reprocessing — poll for the reprocessed file
        # to appear under privat/ (may be renamed to match privat filename pattern).
        existing = set(privat_dir.rglob("*"))
        reprocessed = poll_for_file_recursive(
            privat_dir,
            f"{target_context}-*",
            test_config.poll_interval,
            test_config.max_timeout,
            exclude_paths=existing,
        )
        assert reprocessed is not None, (
            f"Reprocessed file not found under sorted/{target_context}/ "
            f"within {test_config.max_timeout}s after context change"
        )

        # The old location should be empty
        assert not sorted_file.exists(), (
            f"Old file should not exist at {sorted_file}"
        )


# ===================================================================
# Smart Folder Symlinks
# ===================================================================


class TestSmartFolderRemovalOnMove:
    """Smart folder symlink removed when file re-sorted to different context.

    Process a Rechnung (arbeit) → verify 'rechnungen' smart folder symlink.
    Move file to privat/ → verify symlink is cleaned up.
    """

    def test_smart_folder_removed_on_context_move(
        self, test_config: TestConfig, clean_working_dirs,
    ):
        file_stem = "sf_move_doc"
        context = "arbeit"
        date = "2025-12-10"

        # Process through full pipeline to sorted/
        sorted_file, _ = _process_to_sorted(
            test_config, file_stem, context, date,
        )

        # Verify smart folder 'rechnungen' symlink exists
        leaf_dir = sorted_file.parent
        assert poll_for_smart_folder_symlink(
            leaf_dir, "rechnungen", sorted_file.name,
            timeout=15,
        ), (
            f"Smart folder 'rechnungen' symlink not found at "
            f"{leaf_dir / 'rechnungen' / sorted_file.name}"
        )

        # Remember the old symlink path
        old_sf_link = leaf_dir / "rechnungen" / sorted_file.name

        # Move file to sorted/privat/ (Rechnung doesn't match privat's
        # 'gesundheit' smart folder which requires type=Arztbrief)
        privat_dir = test_config.sorted_dir / "privat"
        privat_dir.mkdir(parents=True, exist_ok=True)
        new_path = privat_dir / sorted_file.name
        shutil.move(str(sorted_file), new_path)

        # Old smart folder symlink should be cleaned up (broken → removed)
        assert poll_until_symlink_gone(
            old_sf_link,
            test_config.poll_interval,
            test_config.max_timeout,
        ), f"Smart folder symlink not removed: {old_sf_link}"


class TestSmartFolderTagCondition:
    """Smart folder symlink created when tags are added via SQL (Directus edit).

    Process a document to sorted/arbeit/, then add a tag via SQL UPDATE.
    The 'wichtig' smart folder matches ``tags: .*wichtig.*``, so a symlink
    should appear after the watcher picks up the DB change.
    """

    def test_tag_triggers_smart_folder(
        self, test_config: TestConfig, clean_working_dirs,
    ):
        file_stem = "sf_tags_doc"
        context = "arbeit"
        date = "2025-12-18"

        sorted_file, _ = _process_to_sorted(
            test_config, file_stem, context, date,
        )

        # No 'wichtig' symlink yet (no tags set)
        leaf_dir = sorted_file.parent
        sf_link = leaf_dir / "wichtig" / sorted_file.name
        assert not sf_link.exists(), f"Symlink should not exist yet: {sf_link}"

        # Add tag via SQL (simulates Directus edit)
        db_exec(
            "UPDATE mrdocument.documents_v2 "
            "SET tags = '[\"wichtig\"]'::jsonb "
            f"WHERE original_filename = '{file_stem}.txt'"
        )

        # Poll for the smart folder symlink to appear
        assert poll_for_smart_folder_symlink(
            leaf_dir, "wichtig", sorted_file.name,
            timeout=15,
        ), (
            f"Smart folder 'wichtig' symlink not found at {sf_link} "
            f"after adding tag via SQL"
        )


class TestBrokenSmartFolderCleanup:
    """Broken smart folder symlinks cleaned up after source file deleted
    (via trash/).

    Process a Rechnung (arbeit) → verify 'rechnungen' smart folder symlink.
    Trash the file → verify symlink is cleaned up.
    """

    def test_smart_folder_cleaned_after_trash(
        self, test_config: TestConfig, clean_working_dirs,
    ):
        file_stem = "sf_trash_doc"
        context = "arbeit"
        date = "2025-12-12"

        # Process through full pipeline to sorted/
        sorted_file, _ = _process_to_sorted(
            test_config, file_stem, context, date,
        )

        # Verify smart folder 'rechnungen' symlink exists
        leaf_dir = sorted_file.parent
        assert poll_for_smart_folder_symlink(
            leaf_dir, "rechnungen", sorted_file.name,
            timeout=15,
        ), (
            f"Smart folder 'rechnungen' symlink not found at "
            f"{leaf_dir / 'rechnungen' / sorted_file.name}"
        )

        # Remember the old symlink path
        old_sf_link = leaf_dir / "rechnungen" / sorted_file.name

        # Move file to trash/
        test_config.trash_dir.mkdir(parents=True, exist_ok=True)
        shutil.move(str(sorted_file), test_config.trash_dir / sorted_file.name)

        # Sorted file should be gone (moved to void by trash handling)
        assert poll_until_gone(
            sorted_file,
            test_config.poll_interval,
            test_config.max_timeout,
        ), "Sorted file should be removed after trash"

        # Smart folder symlink should be cleaned up
        assert poll_until_symlink_gone(
            old_sf_link,
            test_config.poll_interval,
            test_config.max_timeout,
        ), f"Smart folder symlink not removed after trash: {old_sf_link}"


class TestContentHashBackfill:
    """Test that content hashes are backfilled for existing records on restart.

    Processes a PDF through the pipeline, then NULLs out the content hashes
    in the DB and restarts the watcher.  The backfill logic should recompute
    them from the files on disk.
    """

    def test_content_hash_backfill_on_restart(
        self, test_config: TestConfig, generated_dir, clean_working_dirs,
    ):
        # 1. Process a dedicated PDF through incoming -> processed
        src = generated_dir / "backfill_test_rechnung.pdf"
        assert src.exists(), f"Generated PDF missing: {src}"
        dest = test_config.incoming_dir / "backfill_test_rechnung.pdf"
        atomic_copy(src, dest)

        # Poll processed/ for the output file
        processed = poll_for_file(
            test_config.processed_dir,
            "arbeit-*2025-03-15*.pdf",
            test_config.poll_interval,
            test_config.max_timeout,
            exclude_names={src.name},
        )
        assert processed is not None, "PDF not found in processed/ within timeout"

        # Move from processed/ to reviewed/ (mirrors what the test pipeline does)
        existing_sorted = set(test_config.sorted_dir.rglob("*2025-03-15*.pdf"))
        reviewed_path = test_config.reviewed_dir / processed.name
        shutil.move(str(processed), reviewed_path)

        # Poll sorted/ for the final file
        sorted_file = poll_for_file_recursive(
            test_config.sorted_dir,
            "*2025-03-15*.pdf",
            test_config.poll_interval,
            test_config.max_timeout,
            exclude_paths=existing_sorted,
        )
        assert sorted_file is not None, "PDF not found in sorted/ within timeout"

        # Wait for DB to be fully updated
        time.sleep(2)

        # 2. Verify source_content_hash is set (set during new record creation)
        row = db_exec(
            "SELECT source_content_hash, content_hash "
            "FROM mrdocument.documents_v2 "
            "WHERE original_filename = 'backfill_test_rechnung.pdf'"
        )
        assert row, f"Record not found in DB (sorted file: {sorted_file})"
        source_ch, content_ch = row.split("|")
        assert source_ch, "source_content_hash should be set after processing"
        # content_hash may or may not be set at this point — that's OK

        # 3. NULL out the content hashes
        db_exec(
            "UPDATE mrdocument.documents_v2 "
            "SET source_content_hash = NULL, content_hash = NULL "
            "WHERE original_filename = 'backfill_test_rechnung.pdf'"
        )

        # Verify they are NULL
        row = db_exec(
            "SELECT source_content_hash IS NULL, content_hash IS NULL "
            "FROM mrdocument.documents_v2 "
            "WHERE original_filename = 'backfill_test_rechnung.pdf'"
        )
        assert row == "t|t", f"Content hashes should be NULL, got: {row}"

        # 4. Restart watcher (triggers backfill)
        restart_watcher()

        # 5. Poll until source_content_hash is backfilled
        deadline = time.monotonic() + test_config.max_timeout
        backfilled = False
        while time.monotonic() < deadline:
            row = db_exec(
                "SELECT source_content_hash "
                "FROM mrdocument.documents_v2 "
                "WHERE original_filename = 'backfill_test_rechnung.pdf'"
            )
            if row and row != "":
                backfilled = True
                break
            time.sleep(test_config.poll_interval)

        assert backfilled, "source_content_hash was not backfilled after restart"

        # 6. Verify backfilled source_content_hash matches the original
        row = db_exec(
            "SELECT source_content_hash "
            "FROM mrdocument.documents_v2 "
            "WHERE original_filename = 'backfill_test_rechnung.pdf'"
        )
        assert row == source_ch, (
            f"source_content_hash mismatch: {row} != {source_ch}"
        )

        # 7. Verify content_hash was also backfilled (computed from sorted file)
        row = db_exec(
            "SELECT content_hash "
            "FROM mrdocument.documents_v2 "
            "WHERE original_filename = 'backfill_test_rechnung.pdf'"
        )
        assert row and row != "", (
            "content_hash should be backfilled from the sorted PDF file"
        )


class TestMigrationDedup:
    """Old DB without content hashes has three records with same PDF content
    but different metadata.  On watcher restart the backfill computes the
    content hashes and the post-backfill dedup removes the duplicates.

    Winner selection:
      1. Records with processed file under ``sorted/`` beat others.
      2. Among equals, the most recently updated record wins.
    """

    @staticmethod
    def _stop_watcher():
        subprocess.run(
            ["docker", "stop", "integration-mrdocument-watcher-1"],
            check=True, capture_output=True, timeout=15,
        )

    @staticmethod
    def _clear_duplicates(test_config):
        dup_dir = test_config.duplicates_dir
        if dup_dir.exists():
            shutil.rmtree(dup_dir)
        dup_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _insert_record(
        original_filename, source_hash, source_paths_json,
        current_paths_json, assigned_filename, updated_at=None,
    ):
        rec_id = str(uuid.uuid4())
        upd = f"'{updated_at}'" if updated_at else "now()"
        db_exec(
            f"INSERT INTO mrdocument.documents_v2 "
            f"(id, original_filename, source_hash, "
            f" source_content_hash, content_hash, "
            f" source_paths, current_paths, "
            f" context, assigned_filename, hash, "
            f" state, username, updated_at) "
            f"VALUES ("
            f"  '{rec_id}', '{original_filename}', '{source_hash}', "
            f"  NULL, NULL, "
            f"  '{source_paths_json}'::jsonb, '{current_paths_json}'::jsonb, "
            f"  'arbeit', '{assigned_filename}', '{source_hash}', "
            f"  'is_complete', 'testuser', {upd}"
            f")"
        )

    @staticmethod
    def _poll_record_count(pattern, expected, poll_interval, timeout):
        deadline = time.monotonic() + timeout
        count = None
        while time.monotonic() < deadline:
            count = db_exec(
                f"SELECT count(*) FROM mrdocument.documents_v2 "
                f"WHERE original_filename LIKE '{pattern}'"
            )
            if count == str(expected):
                return count
            time.sleep(poll_interval)
        return count

    # -----------------------------------------------------------------
    # Test 1: sorted/ record beats processed/ records
    # -----------------------------------------------------------------
    def test_migration_dedup_sorted_wins(
        self, test_config: TestConfig, generated_dir, clean_working_dirs,
    ):
        self._stop_watcher()
        self._clear_duplicates(test_config)

        variants = ["dedup_variant_a", "dedup_variant_b", "dedup_variant_c"]
        file_hashes = {}
        for v in variants:
            src = generated_dir / f"{v}.pdf"
            assert src.exists(), f"Generated PDF missing: {src}"
            shutil.copy2(str(src), str(test_config.archive_dir / f"{v}.pdf"))
            file_hashes[v] = hashlib.sha256(src.read_bytes()).hexdigest()

        # variant_b goes to sorted/, a and c go to processed/
        sorted_arbeit = test_config.sorted_dir / "arbeit"
        sorted_arbeit.mkdir(parents=True, exist_ok=True)

        processed_names = {
            "dedup_variant_a": "arbeit-rechnung-2025-03-15-schulze.pdf",
            "dedup_variant_b": "arbeit-rechnung-2025-03-16-schulze.pdf",
            "dedup_variant_c": "arbeit-rechnung-2025-03-17-schulze.pdf",
        }

        # a → processed/
        shutil.copy2(
            str(generated_dir / "dedup_variant_a.pdf"),
            str(test_config.processed_dir / processed_names["dedup_variant_a"]),
        )
        # b → sorted/arbeit/  (should be the winner)
        shutil.copy2(
            str(generated_dir / "dedup_variant_b.pdf"),
            str(sorted_arbeit / processed_names["dedup_variant_b"]),
        )
        # c → processed/
        shutil.copy2(
            str(generated_dir / "dedup_variant_c.pdf"),
            str(test_config.processed_dir / processed_names["dedup_variant_c"]),
        )

        # Broad cleanup: remove any records that could share hashes with
        # our variants (leftovers from prior test runs / watcher scans).
        db_exec(
            "DELETE FROM mrdocument.documents_v2 "
            "WHERE original_filename LIKE 'dedup_variant_%' "
            "   OR assigned_filename IN ("
            "       'arbeit-rechnung-2025-03-15-schulze.pdf', "
            "       'arbeit-rechnung-2025-03-16-schulze.pdf', "
            "       'arbeit-rechnung-2025-03-17-schulze.pdf')"
        )

        ts = "2025-01-01T00:00:00Z"
        for v in variants:
            if v == "dedup_variant_b":
                cp = f"sorted/arbeit/{processed_names[v]}"
            else:
                cp = f"processed/{processed_names[v]}"
            self._insert_record(
                original_filename=f"{v}.pdf",
                source_hash=file_hashes[v],
                source_paths_json=(
                    f'[{{"path": "archive/{v}.pdf", "timestamp": "{ts}"}}]'
                ),
                current_paths_json=(
                    f'[{{"path": "{cp}", "timestamp": "{ts}"}}]'
                ),
                assigned_filename=processed_names[v],
                updated_at="2025-06-01T00:00:00Z",
            )

        count = db_exec(
            "SELECT count(*) FROM mrdocument.documents_v2 "
            "WHERE original_filename LIKE 'dedup_variant_%'"
        )
        assert count == "3", f"Expected 3 records, got {count}"

        restart_watcher()

        final_count = self._poll_record_count(
            "dedup_variant_%", 1,
            test_config.poll_interval, test_config.max_timeout,
        )
        assert final_count == "1", (
            f"Expected 1 record after dedup, got {final_count}"
        )

        # Winner must be variant_b (the one in sorted/)
        surviving = db_exec(
            "SELECT original_filename FROM mrdocument.documents_v2 "
            "WHERE original_filename LIKE 'dedup_variant_%'"
        )
        assert surviving == "dedup_variant_b.pdf", (
            f"Expected sorted/ record (variant_b) to win, got {surviving}"
        )

        # Winner's files still in place
        assert (test_config.archive_dir / "dedup_variant_b.pdf").exists()
        assert (sorted_arbeit / processed_names["dedup_variant_b"]).exists()

        # Losers' files moved to duplicates/ (archive + processed files)
        dup_files = list(test_config.duplicates_dir.rglob("*.pdf"))
        assert len(dup_files) >= 4, (
            f"Expected >= 4 files in duplicates/ (2 losers x 2 files), "
            f"found {len(dup_files)}: {[str(f) for f in dup_files]}"
        )

    # -----------------------------------------------------------------
    # Test 2: most recent wins when all are in sorted/
    # -----------------------------------------------------------------
    def test_migration_dedup_most_recent_wins(
        self, test_config: TestConfig, generated_dir, clean_working_dirs,
    ):
        self._stop_watcher()
        self._clear_duplicates(test_config)

        variants = ["dedup_variant_a", "dedup_variant_b", "dedup_variant_c"]
        file_hashes = {}
        for v in variants:
            src = generated_dir / f"{v}.pdf"
            shutil.copy2(str(src), str(test_config.archive_dir / f"{v}.pdf"))
            file_hashes[v] = hashlib.sha256(src.read_bytes()).hexdigest()

        sorted_arbeit = test_config.sorted_dir / "arbeit"
        sorted_arbeit.mkdir(parents=True, exist_ok=True)

        processed_names = {
            "dedup_variant_a": "arbeit-rechnung-2025-03-15-schulze.pdf",
            "dedup_variant_b": "arbeit-rechnung-2025-03-16-schulze.pdf",
            "dedup_variant_c": "arbeit-rechnung-2025-03-17-schulze.pdf",
        }
        for v in variants:
            shutil.copy2(
                str(generated_dir / f"{v}.pdf"),
                str(sorted_arbeit / processed_names[v]),
            )

        # Broad cleanup: remove any records that could share hashes with
        # our variants (leftovers from prior test runs / watcher scans).
        db_exec(
            "DELETE FROM mrdocument.documents_v2 "
            "WHERE original_filename LIKE 'dedup_variant_%' "
            "   OR assigned_filename IN ("
            "       'arbeit-rechnung-2025-03-15-schulze.pdf', "
            "       'arbeit-rechnung-2025-03-16-schulze.pdf', "
            "       'arbeit-rechnung-2025-03-17-schulze.pdf')"
        )

        # All in sorted/, but variant_c has the newest updated_at
        timestamps = {
            "dedup_variant_a": "2025-01-01T00:00:00Z",
            "dedup_variant_b": "2025-06-01T00:00:00Z",
            "dedup_variant_c": "2025-12-01T00:00:00Z",
        }
        ts = "2025-01-01T00:00:00Z"
        for v in variants:
            self._insert_record(
                original_filename=f"{v}.pdf",
                source_hash=file_hashes[v],
                source_paths_json=(
                    f'[{{"path": "archive/{v}.pdf", "timestamp": "{ts}"}}]'
                ),
                current_paths_json=(
                    f'[{{"path": "sorted/arbeit/{processed_names[v]}", '
                    f'"timestamp": "{ts}"}}]'
                ),
                assigned_filename=processed_names[v],
                updated_at=timestamps[v],
            )

        count = db_exec(
            "SELECT count(*) FROM mrdocument.documents_v2 "
            "WHERE original_filename LIKE 'dedup_variant_%'"
        )
        assert count == "3", f"Expected 3 records, got {count}"

        restart_watcher()

        final_count = self._poll_record_count(
            "dedup_variant_%", 1,
            test_config.poll_interval, test_config.max_timeout,
        )
        assert final_count == "1", (
            f"Expected 1 record after dedup, got {final_count}"
        )

        # Winner must be variant_c (most recent updated_at)
        surviving = db_exec(
            "SELECT original_filename FROM mrdocument.documents_v2 "
            "WHERE original_filename LIKE 'dedup_variant_%'"
        )
        assert surviving == "dedup_variant_c.pdf", (
            f"Expected most recent record (variant_c) to win, got {surviving}"
        )

        # Winner's files still in place
        assert (test_config.archive_dir / "dedup_variant_c.pdf").exists()
        assert (sorted_arbeit / processed_names["dedup_variant_c"]).exists()

        # Losers' files moved to duplicates/
        dup_files = list(test_config.duplicates_dir.rglob("*.pdf"))
        assert len(dup_files) >= 4, (
            f"Expected >= 4 files in duplicates/ (2 losers x 2 files), "
            f"found {len(dup_files)}: {[str(f) for f in dup_files]}"
        )
