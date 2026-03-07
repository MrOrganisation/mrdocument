"""Unit tests for step6.py — AudioLinkReconciler."""

import os
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest

from models import Record, State, PathEntry
from step6 import AudioLinkReconciler


def _ts():
    return datetime(2025, 1, 1, tzinfo=timezone.utc)


def _make_record(
    original_filename: str,
    source_path: str,
    current_path: str,
    state: State = State.IS_COMPLETE,
    source_hash: str = "abc123",
    current_hash: str = "def456",
):
    return Record(
        id=uuid4(),
        original_filename=original_filename,
        source_hash=source_hash,
        source_paths=[PathEntry(path=source_path, timestamp=_ts())],
        current_paths=[PathEntry(path=current_path, timestamp=_ts())],
        state=state,
        hash=current_hash,
        context="arbeit",
        metadata={"type": "Transcript"},
        assigned_filename=Path(current_path).name,
    )


# ── Creation ─────────────────────────────────────────────────────────


class TestLinkCreated:
    def test_symlink_created_for_audio_record(self, tmp_path):
        """An IS_COMPLETE audio-origin record gets a symlink next to the transcript."""
        root = tmp_path
        (root / "archive").mkdir()
        (root / "archive" / "meeting.mp3").write_bytes(b"audio")
        (root / "sorted" / "arbeit").mkdir(parents=True)
        (root / "sorted" / "arbeit" / "2025-01 Meeting Notes.txt").write_text("transcript")

        record = _make_record(
            original_filename="meeting.mp3",
            source_path="archive/meeting.mp3",
            current_path="sorted/arbeit/2025-01 Meeting Notes.txt",
        )

        reconciler = AudioLinkReconciler(root)
        reconciler.reconcile([record])

        link = root / "sorted" / "arbeit" / "2025-01 Meeting Notes.mp3"
        assert link.is_symlink()
        target = os.readlink(str(link))
        assert target == os.path.relpath(
            root / "archive" / "meeting.mp3",
            link.parent,
        )

    def test_symlink_created_for_processed_record(self, tmp_path):
        """An IS_COMPLETE audio-origin record in processed/ gets a symlink."""
        root = tmp_path
        (root / "archive").mkdir()
        (root / "archive" / "meeting.mp3").write_bytes(b"audio")
        (root / "processed").mkdir()
        (root / "processed" / "2025-01 Meeting Notes.txt").write_text("transcript")

        record = _make_record(
            original_filename="meeting.mp3",
            source_path="archive/meeting.mp3",
            current_path="processed/2025-01 Meeting Notes.txt",
        )

        reconciler = AudioLinkReconciler(root)
        reconciler.reconcile([record])

        link = root / "processed" / "2025-01 Meeting Notes.mp3"
        assert link.is_symlink()
        target = os.readlink(str(link))
        assert target == os.path.relpath(
            root / "archive" / "meeting.mp3",
            link.parent,
        )

    def test_symlink_for_nested_sorted_path(self, tmp_path):
        """Relative path is computed correctly for deeply nested sorted directories."""
        root = tmp_path
        (root / "archive").mkdir()
        (root / "archive" / "recording.wav").write_bytes(b"audio")
        (root / "sorted" / "privat" / "2025" / "januar").mkdir(parents=True)
        (root / "sorted" / "privat" / "2025" / "januar" / "Memo.txt").write_text("txt")

        record = _make_record(
            original_filename="recording.wav",
            source_path="archive/recording.wav",
            current_path="sorted/privat/2025/januar/Memo.txt",
        )

        reconciler = AudioLinkReconciler(root)
        reconciler.reconcile([record])

        link = root / "sorted" / "privat" / "2025" / "januar" / "Memo.wav"
        assert link.is_symlink()
        # Should resolve to the archive file
        resolved = link.resolve()
        assert resolved == (root / "archive" / "recording.wav").resolve()

    def test_link_updated_when_target_changes(self, tmp_path):
        """If the source file changes, the symlink is updated."""
        root = tmp_path
        (root / "archive").mkdir()
        (root / "archive" / "v2.mp3").write_bytes(b"audio2")
        (root / "sorted" / "arbeit").mkdir(parents=True)
        (root / "sorted" / "arbeit" / "Report.txt").write_text("transcript")

        link = root / "sorted" / "arbeit" / "Report.mp3"
        # Create a stale symlink pointing to old target
        link.symlink_to("../../archive/v1.mp3")

        record = _make_record(
            original_filename="v2.mp3",
            source_path="archive/v2.mp3",
            current_path="sorted/arbeit/Report.txt",
        )

        reconciler = AudioLinkReconciler(root)
        reconciler.reconcile([record])

        assert link.is_symlink()
        assert os.readlink(str(link)) == "../../archive/v2.mp3"


# ── No-op cases ──────────────────────────────────────────────────────


class TestNoLink:
    def test_no_link_for_non_audio_record(self, tmp_path):
        """Document-origin records don't get audio links."""
        root = tmp_path
        (root / "archive").mkdir()
        (root / "sorted" / "arbeit").mkdir(parents=True)
        (root / "sorted" / "arbeit" / "Invoice.txt").write_text("doc")

        record = _make_record(
            original_filename="invoice.pdf",
            source_path="archive/invoice.pdf",
            current_path="sorted/arbeit/Invoice.txt",
        )

        reconciler = AudioLinkReconciler(root)
        reconciler.reconcile([record])

        # No symlinks should exist
        links = [p for p in (root / "sorted" / "arbeit").iterdir() if p.is_symlink()]
        assert links == []

    def test_no_link_for_non_complete_record(self, tmp_path):
        """Records not in IS_COMPLETE state are skipped."""
        root = tmp_path
        (root / "archive").mkdir()
        (root / "archive" / "rec.mp3").write_bytes(b"audio")
        (root / "sorted" / "arbeit").mkdir(parents=True)

        record = _make_record(
            original_filename="rec.mp3",
            source_path="archive/rec.mp3",
            current_path="sorted/arbeit/Report.txt",
            state=State.HAS_ERROR,
        )

        reconciler = AudioLinkReconciler(root)
        reconciler.reconcile([record])

        links = [p for p in (root / "sorted" / "arbeit").iterdir() if p.is_symlink()]
        assert links == []

    def test_no_link_when_source_not_in_archive(self, tmp_path):
        """If the source file is not in archive/, no link is created."""
        root = tmp_path
        (root / "incoming").mkdir()
        (root / "sorted" / "arbeit").mkdir(parents=True)
        (root / "sorted" / "arbeit" / "Notes.txt").write_text("txt")

        record = _make_record(
            original_filename="call.mp3",
            source_path="incoming/call.mp3",
            current_path="sorted/arbeit/Notes.txt",
        )

        reconciler = AudioLinkReconciler(root)
        reconciler.reconcile([record])

        links = [p for p in (root / "sorted" / "arbeit").iterdir() if p.is_symlink()]
        assert links == []


# ── Collision avoidance ──────────────────────────────────────────────


class TestCollisionAvoidance:
    def test_non_symlink_not_overwritten(self, tmp_path):
        """A real file with the audio link name is not overwritten."""
        root = tmp_path
        (root / "archive").mkdir()
        (root / "archive" / "audio.mp3").write_bytes(b"audio")
        (root / "sorted" / "arbeit").mkdir(parents=True)
        (root / "sorted" / "arbeit" / "Report.txt").write_text("transcript")
        # Pre-existing real file at the link location
        (root / "sorted" / "arbeit" / "Report.mp3").write_bytes(b"real file")

        record = _make_record(
            original_filename="audio.mp3",
            source_path="archive/audio.mp3",
            current_path="sorted/arbeit/Report.txt",
        )

        reconciler = AudioLinkReconciler(root)
        reconciler.reconcile([record])

        # Should NOT be a symlink — real file preserved
        report_mp3 = root / "sorted" / "arbeit" / "Report.mp3"
        assert not report_mp3.is_symlink()
        assert report_mp3.read_bytes() == b"real file"


# ── Orphan cleanup ───────────────────────────────────────────────────


class TestOrphanCleanup:
    def test_broken_audio_link_removed(self, tmp_path):
        """A broken symlink pointing into archive/ is removed."""
        root = tmp_path
        (root / "archive").mkdir()
        (root / "sorted" / "arbeit").mkdir(parents=True)
        link = root / "sorted" / "arbeit" / "Gone.mp3"
        link.symlink_to("../../archive/deleted.mp3")
        assert link.is_symlink()

        reconciler = AudioLinkReconciler(root)
        reconciler.reconcile([])  # No records → no expected links
        reconciler.cleanup_orphans()

        assert not link.exists() and not link.is_symlink()

    def test_stale_audio_link_removed(self, tmp_path):
        """An audio link for a moved transcript is removed."""
        root = tmp_path
        (root / "archive").mkdir()
        (root / "archive" / "audio.mp3").write_bytes(b"audio")
        (root / "sorted" / "arbeit").mkdir(parents=True)
        # Stale link — transcript was moved away
        link = root / "sorted" / "arbeit" / "OldName.mp3"
        link.symlink_to("../../archive/audio.mp3")

        reconciler = AudioLinkReconciler(root)
        reconciler.reconcile([])  # No records match this link
        reconciler.cleanup_orphans()

        assert not link.is_symlink()

    def test_orphan_in_processed_removed(self, tmp_path):
        """An orphaned audio link in processed/ is cleaned up."""
        root = tmp_path
        (root / "archive").mkdir()
        (root / "archive" / "audio.mp3").write_bytes(b"audio")
        (root / "processed").mkdir()
        link = root / "processed" / "Stale.mp3"
        link.symlink_to("../archive/audio.mp3")

        reconciler = AudioLinkReconciler(root)
        reconciler.reconcile([])
        reconciler.cleanup_orphans()

        assert not link.is_symlink()

    def test_smart_folder_symlink_not_touched(self, tmp_path):
        """Symlinks pointing within sorted/ (smart folders) are left alone."""
        root = tmp_path
        (root / "archive").mkdir()
        (root / "sorted" / "arbeit" / "rechnungen").mkdir(parents=True)
        (root / "sorted" / "arbeit" / "Invoice.txt").write_text("doc")

        # Smart folder symlink: points within sorted, not to archive
        sf_link = root / "sorted" / "arbeit" / "rechnungen" / "Invoice.txt"
        sf_link.symlink_to("../Invoice.txt")

        reconciler = AudioLinkReconciler(root)
        reconciler.reconcile([])
        reconciler.cleanup_orphans()

        # Smart folder link should still exist
        assert sf_link.is_symlink()


# ── Rename / move scenarios ──────────────────────────────────────────


class TestRenameAndMove:
    def test_transcript_rename_updates_link(self, tmp_path):
        """When a transcript is renamed, old link is orphaned and new one is created."""
        root = tmp_path
        (root / "archive").mkdir()
        (root / "archive" / "call.flac").write_bytes(b"audio")
        (root / "sorted" / "arbeit").mkdir(parents=True)

        # Old link from before rename
        old_link = root / "sorted" / "arbeit" / "Old Name.flac"
        old_link.symlink_to("../../archive/call.flac")

        # Transcript has been renamed
        (root / "sorted" / "arbeit" / "New Name.txt").write_text("transcript")

        record = _make_record(
            original_filename="call.flac",
            source_path="archive/call.flac",
            current_path="sorted/arbeit/New Name.txt",
        )

        reconciler = AudioLinkReconciler(root)
        reconciler.reconcile([record])
        reconciler.cleanup_orphans()

        # Old link removed
        assert not old_link.is_symlink()
        # New link created
        new_link = root / "sorted" / "arbeit" / "New Name.flac"
        assert new_link.is_symlink()

    def test_transcript_moved_to_different_dir(self, tmp_path):
        """When a transcript moves directories, link follows."""
        root = tmp_path
        (root / "archive").mkdir()
        (root / "archive" / "rec.ogg").write_bytes(b"audio")
        (root / "sorted" / "arbeit" / "2025").mkdir(parents=True)
        (root / "sorted" / "privat").mkdir(parents=True)

        # Old link in old location
        old_link = root / "sorted" / "arbeit" / "2025" / "Notes.ogg"
        old_link.symlink_to("../../../archive/rec.ogg")

        # Transcript moved to privat
        (root / "sorted" / "privat" / "Notes.txt").write_text("transcript")

        record = _make_record(
            original_filename="rec.ogg",
            source_path="archive/rec.ogg",
            current_path="sorted/privat/Notes.txt",
        )

        reconciler = AudioLinkReconciler(root)
        reconciler.reconcile([record])
        reconciler.cleanup_orphans()

        # Old link removed
        assert not old_link.is_symlink()
        # New link at new location
        new_link = root / "sorted" / "privat" / "Notes.ogg"
        assert new_link.is_symlink()
        assert new_link.resolve() == (root / "archive" / "rec.ogg").resolve()


# ── Idempotency ──────────────────────────────────────────────────────


class TestIdempotency:
    def test_reconcile_twice_is_noop(self, tmp_path):
        """Running reconcile twice doesn't duplicate or break links."""
        root = tmp_path
        (root / "archive").mkdir()
        (root / "archive" / "audio.m4a").write_bytes(b"audio")
        (root / "sorted" / "arbeit").mkdir(parents=True)
        (root / "sorted" / "arbeit" / "Report.txt").write_text("transcript")

        record = _make_record(
            original_filename="audio.m4a",
            source_path="archive/audio.m4a",
            current_path="sorted/arbeit/Report.txt",
        )

        reconciler = AudioLinkReconciler(root)
        reconciler.reconcile([record])
        reconciler.reconcile([record])
        reconciler.cleanup_orphans()

        link = root / "sorted" / "arbeit" / "Report.m4a"
        assert link.is_symlink()
        # Only one symlink in the directory
        links = [p for p in (root / "sorted" / "arbeit").iterdir() if p.is_symlink()]
        assert len(links) == 1
