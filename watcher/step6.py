"""
Audio link reconciler — places symlinks to source audio files alongside transcripts.

When an audio file is transcribed, a symlink named after the transcript is
placed next to it in sorted/ or processed/, pointing back to the source audio
in archive/.
"""

import logging
import os
from pathlib import Path

from models import Record, State

logger = logging.getLogger(__name__)

AUDIO_EXTENSIONS = frozenset({
    ".flac", ".wav", ".mp3", ".ogg", ".webm",
    ".mp4", ".m4a", ".mkv", ".avi", ".mov",
})


class AudioLinkReconciler:
    """Maintains symlinks from transcript files to their source audio in archive/."""

    LINK_LOCATIONS = {"sorted", "processed"}

    def __init__(self, root: Path):
        self.root = root
        self.archive_dir = root / "archive"
        self._expected_links: set[Path] = set()

    def reconcile(self, records: list[Record]) -> None:
        """Ensure audio links exist for all completed audio-origin records.

        For each IS_COMPLETE record in sorted/ or processed/ whose
        original_filename has an audio extension, creates a symlink named
        after the transcript file (same stem + audio extension) pointing to
        the source audio in archive/.
        """
        self._expected_links.clear()

        for record in records:
            if record.state != State.IS_COMPLETE:
                continue
            if record.current_location not in self.LINK_LOCATIONS:
                continue

            # Check if this record originated from an audio file
            orig_ext = Path(record.original_filename).suffix.lower()
            if orig_ext not in AUDIO_EXTENSIONS:
                continue

            # Need source in archive
            source_file = record.source_file
            if source_file is None:
                continue
            source_loc = record._decompose_path(source_file.path)[0]
            if source_loc != "archive":
                continue

            # Current file (transcript) in sorted/ or processed/
            current_file = record.current_file
            if current_file is None:
                continue
            current_path = self.root / current_file.path
            if not current_path.exists():
                continue

            # Symlink: same stem as transcript + original audio extension
            link_name = current_path.stem + orig_ext
            link_path = current_path.parent / link_name
            self._expected_links.add(link_path)

            # Compute relative target from link location to archive source
            source_abs = self.root / source_file.path
            target = os.path.relpath(source_abs, link_path.parent)

            if link_path.is_symlink():
                if os.readlink(str(link_path)) == target:
                    continue
                # Target changed — recreate
                link_path.unlink()
            elif link_path.exists():
                # Non-symlink file with same name, don't overwrite
                logger.warning("Cannot create audio link, file exists: %s", link_path)
                continue

            link_path.symlink_to(target)
            logger.debug("Created audio link: %s -> %s", link_path, target)

    def cleanup_orphans(self) -> None:
        """Remove audio link symlinks that are no longer expected.

        Must be called after reconcile() so that _expected_links is populated.
        Walks sorted/ and processed/ for symlinks whose target resolves to
        archive/ and removes any not in the expected set.
        """
        archive_resolved = self.archive_dir.resolve()

        for loc in self.LINK_LOCATIONS:
            scan_dir = self.root / loc
            if not scan_dir.exists():
                continue

            for link_path in scan_dir.rglob("*"):
                if not link_path.is_symlink():
                    continue
                if link_path in self._expected_links:
                    continue

                # Check if this symlink points into archive/
                raw_target = os.readlink(str(link_path))
                resolved = (link_path.parent / raw_target).resolve()
                try:
                    resolved.relative_to(archive_resolved)
                except ValueError:
                    continue  # Not an audio link (e.g. smart folder symlink)

                # Audio link not in expected set — remove
                link_path.unlink()
                logger.debug("Removed orphaned audio link: %s", link_path)
