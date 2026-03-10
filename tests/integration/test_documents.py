"""Live integration tests for MrDocument document processing.

Tests exercise the full pipeline via Syncthing sync against a running
remote MrDocument instance.

Requires:
    - Generated test documents in ``generated/`` (run ``generate_documents.py``)
    - Syncthing syncing to the remote MrDocument instance
    - MrDocument config deployed (see ``config/``)

Each test uses a unique (document, format) pair to avoid DB deduplication
interference between tests (the service tracks documents by content hash).
"""

import shutil
import time

import pytest

from conftest import (
    CONFIG_DIR,
    TestConfig,
    atomic_copy,
    poll_for_file,
    poll_for_file_recursive,
    poll_for_smart_folder_symlink,
    restart_watcher,
    verify_filename_components,
    verify_filename_keywords,
    verify_no_filename_keywords,
    write_test_file,
)

# ---------------------------------------------------------------------------
# Document metadata for test parametrisation
#
# IMPORTANT: Each (file_stem, format) pair must be unique across ALL tests
# in a session, because the service's DB persists and deduplicates by
# content hash.
# ---------------------------------------------------------------------------

# Incoming pipeline: one unique document per (context, format)
INCOMING_DOCS = {
    ("arbeit", "pdf"): {
        "file_stem": "arbeit_rechnung_schulze",
        "context": "arbeit",
        "type": "Rechnung",
        "sender": "Schulze GmbH",
        "date": "2025-03-15",
        "smart_folder": "rechnungen",
        "expected_keywords": ["schulze"],
    },
    ("arbeit", "txt"): {
        "file_stem": "arbeit_vertrag_fischer",
        "context": "arbeit",
        "type": "Vertrag",
        "sender": "Fischer AG",
        "date": "2025-06-01",
        "smart_folder": None,
        "expected_keywords": [],
    },
    ("arbeit", "rtf"): {
        "file_stem": "arbeit_rechnung_keller",
        "context": "arbeit",
        "type": "Rechnung",
        "sender": "Keller und Partner",
        "date": "2025-05-12",
        "smart_folder": "rechnungen",
        "expected_keywords": ["keller"],
    },
    ("privat", "pdf"): {
        "file_stem": "privat_arztbrief_braun",
        "context": "privat",
        "type": "Arztbrief",
        "sender": "Dr. Braun",
        "date": "2025-04-10",
        "smart_folder": "gesundheit",
        "expected_keywords": [],
    },
    ("privat", "txt"): {
        "file_stem": "privat_versicherung_allianz",
        "context": "privat",
        "type": "Versicherung",
        "sender": "Allianz",
        "date": "2025-07-22",
        "smart_folder": None,
        "expected_keywords": [],
    },
    ("privat", "rtf"): {
        "file_stem": "privat_kontoauszug_sparkasse",
        "context": "privat",
        "type": "Kontoauszug",
        "sender": "Sparkasse",
        "date": "2025-11-30",
        "smart_folder": None,
        "expected_keywords": [],
    },
}

# Sorted correct-context: one unique document per (context, format)
SORTED_CORRECT = {
    ("arbeit", "pdf"): {
        "file_stem": "arbeit_angebot_fischer",
        "context": "arbeit",
        "type": "Angebot",
        "sender": "Fischer AG",
        "date": "2025-08-05",
        "expected_keywords": [],
    },
    ("arbeit", "txt"): {
        "file_stem": "arbeit_angebot_keller",
        "context": "arbeit",
        "type": "Angebot",
        "sender": "Keller und Partner",
        "date": "2025-09-20",
        "expected_keywords": ["keller"],
    },
    ("arbeit", "rtf"): {
        "file_stem": "arbeit_rechnung_schulze",
        "context": "arbeit",
        "type": "Rechnung",
        "sender": "Schulze GmbH",
        "date": "2025-03-15",
        "expected_keywords": ["schulze"],
    },
    ("privat", "pdf"): {
        "file_stem": "privat_kontoauszug_allianz",
        "context": "privat",
        "type": "Kontoauszug",
        "sender": "Allianz",
        "date": "2025-02-18",
        "expected_keywords": [],
    },
    ("privat", "txt"): {
        "file_stem": "privat_arztbrief_braun",
        "context": "privat",
        "type": "Arztbrief",
        "sender": "Dr. Braun",
        "date": "2025-04-10",
        "expected_keywords": [],
    },
    ("privat", "rtf"): {
        "file_stem": "privat_versicherung_allianz",
        "context": "privat",
        "type": "Versicherung",
        "sender": "Allianz",
        "date": "2025-07-22",
        "expected_keywords": [],
    },
}

# Sorted wrong-context: one unique document per (doc_key, format)
SORTED_WRONG = {
    ("arbeit_to_privat", "pdf"): {
        "file_stem": "arbeit_angebot_keller",
        "true_context": "arbeit",
        "forced_context": "privat",
        "type": "Angebot",
        "sender": "Keller und Partner",
        "date": "2025-09-20",
        "expected_keywords": [],
    },
    ("arbeit_to_privat", "txt"): {
        "file_stem": "arbeit_angebot_fischer",
        "true_context": "arbeit",
        "forced_context": "privat",
        "type": "Angebot",
        "sender": "Fischer AG",
        "date": "2025-08-05",
        "expected_keywords": [],
    },
    ("arbeit_to_privat", "rtf"): {
        "file_stem": "arbeit_vertrag_fischer",
        "true_context": "arbeit",
        "forced_context": "privat",
        "type": "Vertrag",
        "sender": "Fischer AG",
        "date": "2025-06-01",
        "expected_keywords": [],
    },
    ("privat_to_arbeit", "pdf"): {
        "file_stem": "privat_arztbrief_mueller",
        "true_context": "privat",
        "forced_context": "arbeit",
        "type": "Arztbrief",
        "sender": "Dr. Mueller",
        "date": "2025-10-15",
        "expected_keywords": [],
    },
    ("privat_to_arbeit", "txt"): {
        "file_stem": "privat_kontoauszug_allianz",
        "true_context": "privat",
        "forced_context": "arbeit",
        "type": "Kontoauszug",
        "sender": "Allianz",
        "date": "2025-02-18",
        "expected_keywords": [],
    },
    ("privat_to_arbeit", "rtf"): {
        "file_stem": "privat_arztbrief_braun",
        "true_context": "privat",
        "forced_context": "arbeit",
        "type": "Arztbrief",
        "sender": "Dr. Braun",
        "date": "2025-04-10",
        "expected_keywords": [],
    },
}

ALL_FORMATS = ["pdf", "txt", "rtf"]



# ===================================================================
# Class 1: Incoming → Processed → Reviewed → Sorted (full pipeline)
# ===================================================================


class TestIncomingPipeline:
    """Test the full incoming → processed → reviewed → sorted pipeline.

    Each parametrised case uses a unique document to avoid DB deduplication.
    """

    @pytest.mark.parametrize("fmt", ALL_FORMATS)
    @pytest.mark.parametrize("ctx", ["arbeit", "privat"])
    def test_incoming_pipeline(
        self, ctx: str, fmt: str, test_config: TestConfig, generated_dir,
        clean_working_dirs,
    ):
        doc = INCOMING_DOCS[(ctx, fmt)]
        src = generated_dir / f"{doc['file_stem']}.{fmt}"
        assert src.exists(), f"Source file missing: {src}"

        # --- Step 1: Copy to incoming/ ---
        dest = test_config.incoming_dir / src.name
        atomic_copy(src, dest)

        # --- Step 2: Poll processed/ for output ---
        # For PDF and RTF inputs the output is a PDF; for TXT it stays TXT
        if fmt in ("pdf", "rtf"):
            out_ext = "pdf"
        else:
            out_ext = fmt
        date = doc["date"]
        pattern = f"{ctx}-*{date}*.{out_ext}"
        result = poll_for_file(
            test_config.processed_dir,
            pattern,
            test_config.poll_interval,
            test_config.max_timeout,
            exclude_names={src.name},
        )
        assert result is not None, (
            f"File not found in processed/ within {test_config.max_timeout}s "
            f"(pattern: {pattern})"
        )

        # Verify filename components
        verify_filename_components(
            result.name,
            expected_context=doc["context"],
            expected_date=doc["date"],
            expected_type=doc["type"],
        )

        # Verify filename keywords
        if doc.get("expected_keywords"):
            verify_filename_keywords(result.name, doc["expected_keywords"])

        # --- Step 3: Verify archive contains original ---
        archived = poll_for_file(
            test_config.archive_dir,
            f"*{doc['file_stem']}*",
            test_config.poll_interval,
            30,
        )
        assert archived is not None, (
            f"Original not found in archive/ (pattern: *{doc['file_stem']}*)"
        )

        # --- Step 4: Move processed file to reviewed/ ---
        # Snapshot existing sorted files so we can exclude them in step 5
        existing_sorted = set(test_config.sorted_dir.rglob(f"*{date}*.{out_ext}"))
        reviewed_path = test_config.reviewed_dir / result.name
        shutil.move(str(result), reviewed_path)

        # --- Step 5: Poll sorted/ recursively for the NEW file ---
        sorted_file = poll_for_file_recursive(
            test_config.sorted_dir,
            f"*{date}*.{out_ext}",
            test_config.poll_interval,
            test_config.max_timeout,
            exclude_paths=existing_sorted,
        )
        assert sorted_file is not None, (
            f"File not found in sorted/ within {test_config.max_timeout}s"
        )

        # Verify folder structure
        rel = sorted_file.relative_to(test_config.sorted_dir)
        parts = rel.parts  # e.g. ("arbeit", "Schulze GmbH", "filename.pdf")

        assert parts[0] == ctx, f"Expected context folder '{ctx}', got '{parts[0]}'"
        assert len(parts) >= 3, (
            f"Expected at least 3 path components (context/field/file), got {parts}"
        )

        # --- Step 6: Verify smart folder symlink (poll — created async) ---
        smart_folder = doc.get("smart_folder")
        if smart_folder:
            leaf_dir = sorted_file.parent
            assert poll_for_smart_folder_symlink(
                leaf_dir, smart_folder, sorted_file.name
            ), f"Smart folder symlink missing: {leaf_dir}/{smart_folder}/{sorted_file.name}"


# ===================================================================
# Class 2: Sorted with correct context
# ===================================================================


class TestSortedCorrectContext:
    """Test placing documents directly into sorted/{correct_context}/."""

    @pytest.mark.parametrize("fmt", ALL_FORMATS)
    @pytest.mark.parametrize("ctx", ["arbeit", "privat"])
    def test_sorted_correct_context(
        self, ctx: str, fmt: str, test_config: TestConfig, generated_dir,
        clean_working_dirs,
    ):
        doc = SORTED_CORRECT[(ctx, fmt)]
        src = generated_dir / f"{doc['file_stem']}.{fmt}"
        assert src.exists(), f"Source file missing: {src}"

        # Copy to sorted/{context}/ (root level)
        ctx_dir = test_config.sorted_dir / ctx
        ctx_dir.mkdir(parents=True, exist_ok=True)
        date = doc["date"]
        # Snapshot existing files so we only find the NEW renamed file
        existing = set(ctx_dir.rglob(f"*{date}*"))
        dest = ctx_dir / src.name
        atomic_copy(src, dest)

        # Poll sorted/{context}/ recursively for renamed file
        result = poll_for_file_recursive(
            ctx_dir,
            f"*{date}*",
            test_config.poll_interval,
            test_config.max_timeout,
            exclude_paths=existing,
        )
        assert result is not None, (
            f"Renamed file not found in sorted/{ctx}/ "
            f"within {test_config.max_timeout}s (pattern: *{date}*)"
        )

        # Verify filename components
        verify_filename_components(
            result.name,
            expected_context=doc["context"],
            expected_date=doc["date"],
            expected_type=doc["type"],
        )

        # Verify filename keywords
        if doc.get("expected_keywords"):
            verify_filename_keywords(result.name, doc["expected_keywords"])

        # Verify archive contains original
        archived = poll_for_file(
            test_config.archive_dir,
            f"*{doc['file_stem']}*",
            test_config.poll_interval,
            30,
        )
        assert archived is not None, (
            f"Original not found in archive/ (pattern: *{doc['file_stem']}*)"
        )


# ===================================================================
# Class 3: Sorted with wrong context
# ===================================================================


class TestSortedWrongContext:
    """Test placing documents into sorted/ under the wrong context."""

    @pytest.mark.parametrize("fmt", ALL_FORMATS)
    @pytest.mark.parametrize(
        "doc_key", ["arbeit_to_privat", "privat_to_arbeit"]
    )
    def test_sorted_wrong_context(
        self, doc_key: str, fmt: str, test_config: TestConfig, generated_dir,
        clean_working_dirs,
    ):
        doc = SORTED_WRONG[(doc_key, fmt)]
        src = generated_dir / f"{doc['file_stem']}.{fmt}"
        assert src.exists(), f"Source file missing: {src}"

        forced_ctx = doc["forced_context"]

        # Copy to sorted/{forced_context}/ (root level)
        ctx_dir = test_config.sorted_dir / forced_ctx
        ctx_dir.mkdir(parents=True, exist_ok=True)
        date = doc["date"]
        # Snapshot existing files so we only find the NEW renamed file
        existing = set(ctx_dir.rglob(f"*{date}*"))
        dest = ctx_dir / src.name
        atomic_copy(src, dest)

        # Poll sorted/{forced_context}/ recursively for renamed file
        result = poll_for_file_recursive(
            ctx_dir,
            f"*{date}*",
            test_config.poll_interval,
            test_config.max_timeout,
            exclude_paths=existing,
        )
        assert result is not None, (
            f"Renamed file not found in sorted/{forced_ctx}/ "
            f"within {test_config.max_timeout}s (pattern: *{date}*)"
        )

        # Verify only context and date (type/sender unreliable for wrong context)
        verify_filename_components(
            result.name,
            expected_context=forced_ctx,
            expected_date=doc["date"],
        )

        # Verify filename keywords
        if doc.get("expected_keywords"):
            verify_filename_keywords(result.name, doc["expected_keywords"])

        # Verify archive contains original
        archived = poll_for_file(
            test_config.archive_dir,
            f"*{doc['file_stem']}*",
            test_config.poll_interval,
            30,
        )
        assert archived is not None, (
            f"Original not found in archive/ (pattern: *{doc['file_stem']}*)"
        )


# ===================================================================
# Class 4: Filename keywords
# ===================================================================

# Documents for keyword testing — each (stem, format) pair is unique
# across ALL test data (INCOMING_DOCS, SORTED_CORRECT, SORTED_WRONG).
KEYWORD_DOCS = [
    {
        "id": "keyword_match_schulze",
        "file_stem": "arbeit_rechnung_schulze",
        "fmt": "txt",
        "context": "arbeit",
        "date": "2025-03-15",
        "expected_keywords": ["schulze"],
        "absent_keywords": ["keller"],
    },
    {
        "id": "keyword_match_keller",
        "file_stem": "arbeit_rechnung_keller",
        "fmt": "pdf",
        "context": "arbeit",
        "date": "2025-05-12",
        "expected_keywords": ["keller"],
        "absent_keywords": ["schulze"],
    },
    {
        "id": "keyword_no_match_arbeit",
        "file_stem": "arbeit_angebot_fischer",
        "fmt": "rtf",
        "context": "arbeit",
        "date": "2025-08-05",
        "expected_keywords": [],
        "absent_keywords": ["schulze", "keller"],
    },
    {
        "id": "keyword_no_config_privat",
        "file_stem": "privat_kontoauszug_sparkasse",
        "fmt": "txt",
        "context": "privat",
        "date": "2025-11-30",
        "expected_keywords": [],
        "absent_keywords": ["schulze", "keller"],
    },
]


class TestFilenameKeywords:
    """Test that filename_keywords from context config are appended to output filenames.

    arbeit.yaml defines filename_keywords: ["schulze", "keller"].
    privat.yaml has no filename_keywords.
    """

    @pytest.mark.parametrize(
        "doc",
        KEYWORD_DOCS,
        ids=[d["id"] for d in KEYWORD_DOCS],
    )
    def test_filename_keywords(
        self, doc, test_config: TestConfig, generated_dir, clean_working_dirs,
    ):
        src = generated_dir / f"{doc['file_stem']}.{doc['fmt']}"
        assert src.exists(), f"Source file missing: {src}"

        # --- Copy to incoming/ ---
        dest = test_config.incoming_dir / src.name
        atomic_copy(src, dest)

        # --- Poll processed/ for output ---
        date = doc["date"]
        ctx = doc["context"]
        out_ext = "pdf" if doc["fmt"] in ("pdf", "rtf") else doc["fmt"]
        pattern = f"{ctx}-*{date}*.{out_ext}"
        result = poll_for_file(
            test_config.processed_dir,
            pattern,
            test_config.poll_interval,
            test_config.max_timeout,
            exclude_names={src.name},
        )
        assert result is not None, (
            f"File not found in processed/ within {test_config.max_timeout}s "
            f"(pattern: {pattern})"
        )

        # --- Verify expected keywords are present ---
        if doc["expected_keywords"]:
            verify_filename_keywords(result.name, doc["expected_keywords"])

        # --- Verify absent keywords are NOT present ---
        if doc["absent_keywords"]:
            verify_no_filename_keywords(result.name, doc["absent_keywords"])


# ===================================================================
# Class 5: Unsupported file types (prefilter)
# ===================================================================


class TestUnsupportedFilePrefilter:
    """Test that unsupported file types are moved to error/ by the prefilter.

    Files with extensions not in DOCUMENT_EXTENSIONS or AUDIO_EXTENSIONS
    should never enter the processing pipeline.  All files are dropped at
    once so only a single scan cycle is needed.
    """

    def test_unsupported_files_moved_to_error(
        self, test_config: TestConfig, clean_working_dirs,
    ):
        filenames = [
            "Montserrat-Bold.ttf",
            "BentonSans-Book.otf",
            "budget.numbers",
            "thesis.tex",
        ]
        # Drop all unsupported files at once
        for fn in filenames:
            dest = test_config.incoming_dir / fn
            tmp = dest.with_suffix(dest.suffix + ".tmp")
            tmp.write_bytes(b"unsupported file content")
            tmp.rename(dest)

        # Verify all land in error/
        for fn in filenames:
            result = poll_for_file(
                test_config.error_dir,
                f"*{fn}*",
                test_config.poll_interval,
                60,
            )
            assert result is not None, f"'{fn}' not in error/ within 60s"

        # Verify none entered the pipeline
        for fn in filenames:
            assert poll_for_file(
                test_config.archive_dir, f"*{fn}*", 1, 5,
            ) is None, f"'{fn}' should not be in archive/"



# ===================================================================
# Class 7: Reset pipeline (processed → reset/ → sorted/)
# ===================================================================

# Documents for reset testing — each (stem, format) pair must be unique
# across ALL test data in this file.
RESET_DOCS = {
    ("arbeit", "pdf"): {
        "file_stem": "arbeit_vertrag_fischer",
        "context": "arbeit",
        "type": "Vertrag",
        "sender": "Fischer AG",
        "date": "2025-06-01",
    },
    ("arbeit", "txt"): {
        "file_stem": "arbeit_rechnung_keller",
        "context": "arbeit",
        "type": "Rechnung",
        "sender": "Keller und Partner",
        "date": "2025-05-12",
    },
    ("privat", "pdf"): {
        "file_stem": "privat_kontoauszug_sparkasse",
        "context": "privat",
        "type": "Kontoauszug",
        "sender": "Sparkasse",
        "date": "2025-11-30",
    },
}

# Separate document for the standalone reset cleanup test — must not
# share (stem, format) with any other test data.
RESET_CLEANUP_DOC = {
    "file_stem": "privat_versicherung_allianz",
    "context": "privat",
    "type": "Versicherung",
    "sender": "Allianz",
    "date": "2025-07-22",
    "fmt": "pdf",
}


class TestResetPipeline:
    """Test the reset/ folder: placing a processed file triggers filename
    recomputation and move to sorted/.

    Flow: incoming → processed → copy to reset/ → poll sorted/ for
    file with recomputed assigned_filename.
    """

    @pytest.mark.parametrize(
        "ctx,fmt",
        list(RESET_DOCS.keys()),
        ids=[f"{ctx}-{fmt}" for ctx, fmt in RESET_DOCS],
    )
    def test_reset_recomputes_filename(
        self, ctx: str, fmt: str, test_config: TestConfig, generated_dir,
        clean_working_dirs,
    ):

        doc = RESET_DOCS[(ctx, fmt)]
        src = generated_dir / f"{doc['file_stem']}.{fmt}"
        assert src.exists(), f"Source file missing: {src}"

        # --- Step 1: Process through incoming → processed ---
        dest = test_config.incoming_dir / src.name
        atomic_copy(src, dest)

        out_ext = "pdf" if fmt in ("pdf", "rtf") else fmt
        date = doc["date"]
        pattern = f"{ctx}-*{date}*.{out_ext}"
        processed_file = poll_for_file(
            test_config.processed_dir,
            pattern,
            test_config.poll_interval,
            test_config.max_timeout,
            exclude_names={src.name},
        )
        assert processed_file is not None, (
            f"File not found in processed/ within {test_config.max_timeout}s "
            f"(pattern: {pattern})"
        )

        # --- Step 2: Copy processed file to reset/ ---
        # Snapshot sorted/ so we detect only the NEW file
        existing_sorted = set(test_config.sorted_dir.rglob(f"*{date}*.{out_ext}"))
        reset_dest = test_config.reset_dir / processed_file.name
        atomic_copy(processed_file, reset_dest)

        # --- Step 3: Poll sorted/ for the reset file ---
        sorted_file = poll_for_file_recursive(
            test_config.sorted_dir,
            f"*{date}*.{out_ext}",
            test_config.poll_interval,
            test_config.max_timeout,
            exclude_paths=existing_sorted,
        )
        assert sorted_file is not None, (
            f"Reset file not found in sorted/ within {test_config.max_timeout}s "
            f"(pattern: *{date}*.{out_ext})"
        )

        # --- Step 4: Verify filename has expected components ---
        verify_filename_components(
            sorted_file.name,
            expected_context=doc["context"],
            expected_date=doc["date"],
            expected_type=doc["type"],
        )

        # Verify it landed under the correct context folder
        rel = sorted_file.relative_to(test_config.sorted_dir)
        assert rel.parts[0] == ctx, (
            f"Expected context folder '{ctx}', got '{rel.parts[0]}'"
        )
        assert len(rel.parts) >= 3, (
            f"Expected at least 3 path components (context/field/file), got {rel.parts}"
        )

        # --- Step 5: Verify reset/ file was consumed (moved away) ---
        time.sleep(3)  # allow watcher to clean up
        assert not reset_dest.exists(), (
            f"Reset file should have been consumed: {reset_dest}"
        )

    def test_reset_file_removed_from_reset_dir(
        self, test_config: TestConfig, generated_dir, clean_working_dirs,
    ):
        """Verify that after reset processing, the file no longer exists in reset/."""
        doc = RESET_CLEANUP_DOC
        fmt = doc["fmt"]
        src = generated_dir / f"{doc['file_stem']}.{fmt}"
        assert src.exists(), f"Source file missing: {src}"

        # Process through incoming first
        dest = test_config.incoming_dir / src.name
        atomic_copy(src, dest)

        date = doc["date"]
        ctx = doc["context"]
        out_ext = "pdf" if fmt in ("pdf", "rtf") else fmt
        pattern = f"{ctx}-*{date}*.{out_ext}"
        processed_file = poll_for_file(
            test_config.processed_dir,
            pattern,
            test_config.poll_interval,
            test_config.max_timeout,
            exclude_names={src.name},
        )
        assert processed_file is not None, "File not found in processed/"

        # Copy to reset/
        reset_dest = test_config.reset_dir / processed_file.name
        atomic_copy(processed_file, reset_dest)

        # Wait for the file to be consumed from reset/
        deadline = time.monotonic() + test_config.max_timeout
        while time.monotonic() < deadline:
            if not reset_dest.exists():
                break
            time.sleep(test_config.poll_interval)

        assert not reset_dest.exists(), (
            f"File should be consumed from reset/ within {test_config.max_timeout}s"
        )

        # Verify nothing remains in reset/
        remaining = list(test_config.reset_dir.glob("*"))
        remaining_files = [f for f in remaining if f.is_file()]
        assert len(remaining_files) == 0, (
            f"reset/ should be empty after processing, found: {remaining_files}"
        )


# ===================================================================
# Class 8: Watcher restart recovery
# ===================================================================

RESTART_DOC = {
    "file_stem": "privat_arztbrief_mueller",
    "context": "privat",
    "type": "Arztbrief",
    "sender": "Dr. Mueller",
    "date": "2025-10-15",
    "fmt": "txt",
}


class TestWatcherRestart:
    """Test that an in-flight document completes after a watcher restart.

    Sequence:
      1. Drop file into incoming/
      2. Wait for it to appear in archive/ (step4 moved it, processing started)
      3. Restart the watcher container (simulates crash/kill)
      4. Verify the file eventually appears in processed/ (recovery worked)
    """

    def test_restart_recovers_in_flight_document(
        self, test_config: TestConfig, generated_dir, clean_working_dirs,
    ):
        doc = RESTART_DOC
        src = generated_dir / f"{doc['file_stem']}.{doc['fmt']}"
        assert src.exists(), f"Source file missing: {src}"

        dest = test_config.incoming_dir / src.name
        atomic_copy(src, dest)

        # Wait for the file to reach archive/ (processing has started)
        archive_pattern = f"*{doc['file_stem']}*"
        archived = poll_for_file(
            test_config.archive_dir,
            archive_pattern,
            test_config.poll_interval,
            test_config.max_timeout,
        )
        assert archived is not None, (
            f"File not found in archive/ within {test_config.max_timeout}s "
            f"(pattern: {archive_pattern})"
        )

        # Restart the watcher container
        restart_watcher()

        # After restart, the watcher should recover and complete processing
        ctx = doc["context"]
        date = doc["date"]
        pattern = f"{ctx}-*{date}*.{doc['fmt']}"
        result = poll_for_file(
            test_config.processed_dir,
            pattern,
            test_config.poll_interval,
            test_config.max_timeout,
            exclude_names={src.name},
        )
        assert result is not None, (
            f"File not found in processed/ after watcher restart "
            f"within {test_config.max_timeout}s (pattern: {pattern})"
        )

        verify_filename_components(
            result.name,
            expected_context=doc["context"],
            expected_date=doc["date"],
            expected_type=doc["type"],
        )


# ===================================================================
# Class 9: Sorted – AI reports different context, folder context wins
# ===================================================================

SORTED_AI_DISAGREES = [
    {
        "id": "pdf",
        "source_stem": "sorted_wrongctx_pdf",
        "source_fmt": "pdf",
        "test_filename": "sorted_wrongctx_pdf.pdf",
        "ai_context": "privat",
        "folder_context": "arbeit",
        "date": "2025-01-10",
        "out_ext": "pdf",
    },
    {
        "id": "txt",
        "source_stem": "sorted_wrongctx_txt",
        "source_fmt": "txt",
        "test_filename": "sorted_wrongctx_txt.txt",
        "ai_context": "arbeit",
        "folder_context": "privat",
        "date": "2025-01-15",
        "out_ext": "txt",
    },
    {
        "id": "m4a",
        "source_stem": None,
        "source_fmt": "m4a",
        "test_filename": "sorted-wrongctx-audio.m4a",
        "ai_context": "arbeit",
        "folder_context": "privat",
        "date": "2025-01-20",
        "out_ext": "txt",
    },
]


class TestSortedAiDisagrees:
    """Test that the folder context wins when the AI reports a different context.

    A document (PDF, TXT) or audio file (M4A) is placed directly into
    sorted/{context}/.  The mock AI classifies the content as belonging
    to a *different* context, but the system must keep the file in the
    original folder context.
    """

    @pytest.mark.parametrize(
        "spec",
        SORTED_AI_DISAGREES,
        ids=[s["id"] for s in SORTED_AI_DISAGREES],
    )
    def test_sorted_ai_disagrees_folder_wins(
        self, spec, test_config: TestConfig, generated_dir, clean_working_dirs,
    ):
        folder_ctx = spec["folder_context"]
        ctx_dir = test_config.sorted_dir / folder_ctx
        ctx_dir.mkdir(parents=True, exist_ok=True)
        date = spec["date"]
        out_ext = spec["out_ext"]

        # Snapshot existing files so we only detect the NEW renamed file
        existing = set(ctx_dir.rglob(f"*{date}*.{out_ext}"))

        # Prepare the test file
        dest = ctx_dir / spec["test_filename"]
        if spec["source_stem"] is not None:
            # Document: copy from generated (each has unique content)
            src = generated_dir / f"{spec['source_stem']}.{spec['source_fmt']}"
            assert src.exists(), f"Source file missing: {src}"
            atomic_copy(src, dest)
        elif spec["source_fmt"] == "m4a":
            # Audio: copy generated M4A file
            src = generated_dir / spec["test_filename"]
            assert src.exists(), f"Source audio missing: {src}"
            atomic_copy(src, dest)

        # Poll sorted/{folder_context}/ recursively for the renamed file
        result = poll_for_file_recursive(
            ctx_dir,
            f"*{date}*.{out_ext}",
            test_config.poll_interval,
            test_config.max_timeout,
            exclude_paths=existing,
        )
        assert result is not None, (
            f"Renamed file not found in sorted/{folder_ctx}/ "
            f"within {test_config.max_timeout}s (pattern: *{date}*.{out_ext})"
        )

        # Verify the output uses the FOLDER context, not the AI context
        verify_filename_components(
            result.name,
            expected_context=folder_ctx,
            expected_date=date,
        )
