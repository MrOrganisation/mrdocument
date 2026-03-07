"""Live integration tests for MrDocument audio/STT processing.

Tests exercise the full pipeline via Syncthing sync against a running
remote MrDocument instance.

Requires:
    - Generated test audio in ``generated/`` (run ``generate_audio.py``)
    - Syncthing syncing to the remote MrDocument instance
    - MrDocument config deployed (see ``config/``)
    - ElevenLabs STT configured on the server
"""

import shutil
from pathlib import Path

import pytest

from conftest import (
    TestConfig,
    atomic_copy,
    poll_for_audio_link_symlink,
    poll_for_file,
    poll_for_file_recursive,
    verify_filename_components,
    verify_intro_in_text,
)

# ---------------------------------------------------------------------------
# Audio metadata
# ---------------------------------------------------------------------------

AUDIO_WITH_INTRO = {
    "beginning": {
        "filename": "besprechung-intro.mp3",
        "context": "arbeit",
        "date": "2025-03-15",
        "intro_key_phrases": [
            "fuenfzehnten Maerz zweitausendundfuenfundzwanzig",
            "Schulze",
            "IT-Infrastrukturprojekt",
        ],
    },
    "end": {
        "filename": "arztgespraech-intro.mp3",
        "context": "privat",
        "date": "2025-04-10",
        "intro_key_phrases": [
            "zehnten April zweitausendundfuenfundzwanzig",
            "Doktor Braun",
            "Allgemeinmedizin",
        ],
    },
}

AUDIO_WITHOUT_INTRO = {
    "filename": "telefonat.mp3",
    "context": "arbeit",
    "date": "2025-09-20",
}


# ===================================================================
# Class 1: Audio with intro
# ===================================================================


class TestAudioWithIntro:
    """Test audio files that contain an intro segment (two-pass STT)."""

    @pytest.mark.parametrize("intro_pos", ["beginning", "end"])
    def test_audio_with_intro(
        self, intro_pos: str, test_config: TestConfig, generated_dir,
        clean_working_dirs,
    ):
        spec = AUDIO_WITH_INTRO[intro_pos]
        src = generated_dir / spec["filename"]
        assert src.exists(), f"Source audio missing: {src}"

        # Copy to incoming/
        dest = test_config.incoming_dir / src.name
        atomic_copy(src, dest)

        # Poll processed/ for TXT output
        ctx = spec["context"]
        date = spec["date"]
        pattern = f"{ctx}-*{date}*.txt"
        result = poll_for_file(
            test_config.processed_dir,
            pattern,
            test_config.poll_interval,
            test_config.max_timeout,
        )
        assert result is not None, (
            f"TXT output not found in processed/ within {test_config.max_timeout}s "
            f"(pattern: {pattern})"
        )

        # Verify filename
        verify_filename_components(
            result.name,
            expected_context=ctx,
            expected_date=date,
        )

        # Verify archive contains original audio
        stem = src.stem  # e.g. "besprechung-intro"
        archived = poll_for_file(
            test_config.archive_dir,
            f"*{stem}*",
            test_config.poll_interval,
            30,
        )
        assert archived is not None, (
            f"Original audio not found in archive/ (pattern: *{stem}*)"
        )

        # Read transcript and check intro key phrases
        text = result.read_text(encoding="utf-8")
        assert len(text) > 0, "Transcript is empty"

        # Intro should always appear near the beginning of the transcript,
        # even when the intro segment is at the end of the audio file
        # (the STT pipeline must move it to the front).
        assert verify_intro_in_text(
            text, spec["intro_key_phrases"], check_first_n_chars=2000
        ), (
            f"Intro key phrases not found near beginning of transcript "
            f"(intro_pos={intro_pos!r}). "
            f"First 500 chars: {text[:500]!r}"
        )

        # --- Move to reviewed/ → sorted/ and verify audio link ---
        existing_sorted = set(test_config.sorted_dir.rglob(f"*{date}*.txt"))
        reviewed_path = test_config.reviewed_dir / result.name
        shutil.move(str(result), reviewed_path)

        sorted_file = poll_for_file_recursive(
            test_config.sorted_dir,
            f"*{date}*.txt",
            test_config.poll_interval,
            test_config.max_timeout,
            exclude_paths=existing_sorted,
        )
        assert sorted_file is not None, (
            f"Transcript not found in sorted/ within {test_config.max_timeout}s"
        )

        audio_ext = Path(spec["filename"]).suffix
        assert poll_for_audio_link_symlink(
            sorted_file, audio_ext, test_config.archive_dir,
        ), (
            f"Audio link symlink missing: "
            f"{sorted_file.parent / (sorted_file.stem + audio_ext)}"
        )


# ===================================================================
# Class 2: Audio without intro
# ===================================================================


class TestAudioWithoutIntro:
    """Test audio file without an intro segment (single-pass STT)."""

    def test_audio_without_intro(self, test_config: TestConfig, generated_dir,
                                    clean_working_dirs):
        spec = AUDIO_WITHOUT_INTRO
        src = generated_dir / spec["filename"]
        assert src.exists(), f"Source audio missing: {src}"

        # Copy to incoming/
        dest = test_config.incoming_dir / src.name
        atomic_copy(src, dest)

        # Poll processed/ for TXT output
        ctx = spec["context"]
        date = spec["date"]
        pattern = f"{ctx}-*{date}*.txt"
        result = poll_for_file(
            test_config.processed_dir,
            pattern,
            test_config.poll_interval,
            test_config.max_timeout,
        )
        assert result is not None, (
            f"TXT output not found in processed/ within {test_config.max_timeout}s "
            f"(pattern: {pattern})"
        )

        # Verify filename
        verify_filename_components(
            result.name,
            expected_context=ctx,
            expected_date=date,
        )

        # Verify archive contains original audio
        stem = src.stem  # "telefonat"
        archived = poll_for_file(
            test_config.archive_dir,
            f"*{stem}*",
            test_config.poll_interval,
            30,
        )
        assert archived is not None, (
            f"Original audio not found in archive/ (pattern: *{stem}*)"
        )

        # Basic sanity: transcript should not be empty
        text = result.read_text(encoding="utf-8")
        assert len(text) > 0, "Transcript is empty"

        # --- Move to reviewed/ → sorted/ and verify audio link ---
        existing_sorted = set(test_config.sorted_dir.rglob(f"*{date}*.txt"))
        reviewed_path = test_config.reviewed_dir / result.name
        shutil.move(str(result), reviewed_path)

        sorted_file = poll_for_file_recursive(
            test_config.sorted_dir,
            f"*{date}*.txt",
            test_config.poll_interval,
            test_config.max_timeout,
            exclude_paths=existing_sorted,
        )
        assert sorted_file is not None, (
            f"Transcript not found in sorted/ within {test_config.max_timeout}s"
        )

        audio_ext = Path(spec["filename"]).suffix
        assert poll_for_audio_link_symlink(
            sorted_file, audio_ext, test_config.archive_dir,
        ), (
            f"Audio link symlink missing: "
            f"{sorted_file.parent / (sorted_file.stem + audio_ext)}"
        )


# ===================================================================
# Class 3: Video formats (.mov, .mp4)
# ===================================================================

VIDEO_FILES = [
    {
        "id": "mov",
        "filename": "videocall.mov",
        "context": "arbeit",
        "date": "2025-05-12",
    },
    {
        "id": "mp4",
        "filename": "sprachnachricht.mp4",
        "context": "privat",
        "date": "2025-06-18",
    },
]


class TestVideoFormats:
    """Test that video formats (.mov, .mp4) are transcribed via audio extraction."""

    @pytest.mark.parametrize(
        "spec",
        VIDEO_FILES,
        ids=[v["id"] for v in VIDEO_FILES],
    )
    def test_video_transcription(
        self, spec, test_config: TestConfig, generated_dir, clean_working_dirs,
    ):
        src = generated_dir / spec["filename"]
        assert src.exists(), f"Source video missing: {src}"

        # Copy to incoming/
        dest = test_config.incoming_dir / src.name
        atomic_copy(src, dest)

        # Poll processed/ for TXT output
        ctx = spec["context"]
        date = spec["date"]
        pattern = f"{ctx}-*{date}*.txt"
        result = poll_for_file(
            test_config.processed_dir,
            pattern,
            test_config.poll_interval,
            test_config.max_timeout,
        )
        assert result is not None, (
            f"TXT output not found in processed/ within {test_config.max_timeout}s "
            f"(pattern: {pattern})"
        )

        # Verify filename
        verify_filename_components(
            result.name,
            expected_context=ctx,
            expected_date=date,
        )

        # Verify archive contains original video
        stem = Path(spec["filename"]).stem
        archived = poll_for_file(
            test_config.archive_dir,
            f"*{stem}*",
            test_config.poll_interval,
            30,
        )
        assert archived is not None, (
            f"Original video not found in archive/ (pattern: *{stem}*)"
        )

        # Transcript should not be empty
        text = result.read_text(encoding="utf-8")
        assert len(text) > 0, "Transcript is empty"

        # --- Move to reviewed/ → sorted/ and verify audio link ---
        existing_sorted = set(test_config.sorted_dir.rglob(f"*{date}*.txt"))
        reviewed_path = test_config.reviewed_dir / result.name
        shutil.move(str(result), reviewed_path)

        sorted_file = poll_for_file_recursive(
            test_config.sorted_dir,
            f"*{date}*.txt",
            test_config.poll_interval,
            test_config.max_timeout,
            exclude_paths=existing_sorted,
        )
        assert sorted_file is not None, (
            f"Transcript not found in sorted/ within {test_config.max_timeout}s"
        )

        audio_ext = Path(spec["filename"]).suffix
        assert poll_for_audio_link_symlink(
            sorted_file, audio_ext, test_config.archive_dir,
        ), (
            f"Audio link symlink missing: "
            f"{sorted_file.parent / (sorted_file.stem + audio_ext)}"
        )


# ===================================================================
# Class 4: Audio-specific filename pattern
# ===================================================================


class TestAudioFilenamePattern:
    """Test that audio transcripts use the audio_filename pattern when configured.

    arbeit.yaml defines:
        filename: "{context}-{type}-{date}-{sender}"
        audio_filename: "{context}-{date}-{sender}-{type}"

    So the telefonat.mp3 transcript (context=arbeit, date=2025-09-20) should
    produce a filename starting with "arbeit-2025-09-20" (audio pattern, date
    in second position) rather than "arbeit-Telefonat" (PDF pattern, type in
    second position).
    """

    def test_audio_uses_audio_filename_pattern(
        self, test_config: TestConfig, generated_dir, clean_working_dirs,
    ):
        spec = AUDIO_WITHOUT_INTRO
        src = generated_dir / spec["filename"]
        assert src.exists(), f"Source audio missing: {src}"

        # Use a different filename (for different mock metadata/output hash)
        # and append a marker byte (for different source hash) to avoid
        # collision with TestAudioWithoutIntro which uses the same source.
        dest = test_config.incoming_dir / "telefonat-pattern.mp3"
        tmp = dest.with_suffix(".tmp")
        tmp.write_bytes(src.read_bytes() + b"\x00")
        tmp.rename(dest)

        # Poll processed/ for TXT output — use date in second position (audio pattern)
        ctx = spec["context"]
        date = spec["date"]
        pattern = f"{ctx}-*{date}*.txt"
        result = poll_for_file(
            test_config.processed_dir,
            pattern,
            test_config.poll_interval,
            test_config.max_timeout,
        )
        assert result is not None, (
            f"TXT output not found in processed/ within {test_config.max_timeout}s "
            f"(pattern: {pattern})"
        )

        # Verify audio_filename pattern: stem should start with "arbeit-2025-09-20"
        # (context-date, audio pattern) rather than "arbeit-telefonat" (context-type, PDF pattern)
        stem = result.stem.lower()
        audio_prefix = f"{ctx}-{date}".lower()
        assert stem.startswith(audio_prefix), (
            f"Filename stem '{result.stem}' does not start with '{audio_prefix}'. "
            f"Expected audio_filename pattern (context-date-sender-type) but got "
            f"what looks like the PDF pattern (context-type-date-sender)."
        )
