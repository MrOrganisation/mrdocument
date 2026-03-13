"""Live integration tests for MrDocument document processing.

Tests exercise the full pipeline via Syncthing sync against a running
remote MrDocument instance.

Requires:
    - Generated test documents in ``generated/`` (run ``generate_documents.py``)
    - Syncthing syncing to the remote MrDocument instance
    - MrDocument config deployed (see ``config/``)

Each test uses a unique (document, format) pair to avoid DB deduplication
interference between tests (the service tracks documents by content hash).

NOTE: Many tests have been migrated to YAML fixtures in ``fixture_tests/``.
The following test classes remain as Python because they require specific
assertions (filename keywords, watcher restart) not expressible in YAML.
"""

import pytest

from conftest import (
    TestConfig,
    atomic_copy,
    poll_for_file,
    restart_watcher,
    verify_filename_components,
    verify_filename_keywords,
    verify_no_filename_keywords,
)


# ===================================================================
# Filename keywords
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
# Watcher restart recovery
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
