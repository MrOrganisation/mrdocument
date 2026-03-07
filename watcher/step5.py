"""
Smart folder symlink management for document watcher v2.

Creates and removes symlinks in smart folder subdirectories within sorted/
based on record metadata and smart folder conditions.

Also supports root-level smart folders configured via a single YAML file
at the mrdocument root, placing symlinks at arbitrary paths.
"""

import logging
import os
from dataclasses import dataclass
from pathlib import Path

from models import Record, State
from sorter import SmartFolderConfig

logger = logging.getLogger(__name__)


@dataclass
class SmartFolderEntry:
    """A smart folder bound to a specific context."""
    context: str
    config: SmartFolderConfig


class SmartFolderReconciler:
    """Manages smart folder symlinks based on record metadata."""

    def __init__(self, root: Path, smart_folders: list[SmartFolderEntry]):
        self.root = root
        self.sorted_dir = root / "sorted"
        self.smart_folders = smart_folders
        # Index by context for fast lookup
        self._by_context: dict[str, list[SmartFolderEntry]] = {}
        for entry in smart_folders:
            self._by_context.setdefault(entry.context, []).append(entry)

    def reconcile(self, records: list[Record]) -> None:
        """Evaluate smart folder conditions for records and manage symlinks.

        Args:
            records: Records to evaluate (should be IS_COMPLETE in sorted/).
        """
        for record in records:
            if record.state != State.IS_COMPLETE:
                continue
            if record.current_location != "sorted":
                continue
            if not record.context:
                continue

            entries = self._by_context.get(record.context)
            if not entries:
                continue

            current = record.current_file
            if not current:
                continue

            file_path = self.root / current.path
            if not file_path.exists() or file_path.is_symlink():
                continue

            leaf_folder = file_path.parent
            filename = file_path.name

            # Build string metadata for condition evaluation
            str_fields: dict[str, str] = {}
            if record.metadata:
                str_fields = {
                    k: str(v) for k, v in record.metadata.items()
                    if v is not None
                }

            for entry in entries:
                sf_config = entry.config
                sf_dir = leaf_folder / sf_config.name
                symlink_path = sf_dir / filename

                # Check filename regex filter
                if not sf_config.matches_filename(filename):
                    if symlink_path.is_symlink():
                        try:
                            symlink_path.unlink()
                        except OSError:
                            pass
                    continue

                # Evaluate condition
                condition_matches = (
                    sf_config.condition is None
                    or sf_config.condition.evaluate(str_fields)
                )

                if condition_matches:
                    # Create symlink if it doesn't exist
                    if not symlink_path.exists() and not symlink_path.is_symlink():
                        try:
                            sf_dir.mkdir(parents=True, exist_ok=True)
                            relative_target = Path("..") / filename
                            symlink_path.symlink_to(relative_target)
                            logger.info(
                                "Smart folder link: %s -> %s",
                                symlink_path.relative_to(self.root),
                                filename,
                            )
                        except Exception as e:
                            logger.error(
                                "Failed to create symlink %s: %s",
                                symlink_path, e,
                            )
                else:
                    # Condition doesn't match — remove symlink if it exists
                    if symlink_path.is_symlink():
                        try:
                            symlink_path.unlink()
                            logger.info(
                                "Removed smart folder link: %s",
                                symlink_path.relative_to(self.root),
                            )
                        except Exception as e:
                            logger.error(
                                "Failed to remove symlink %s: %s",
                                symlink_path, e,
                            )

    def cleanup_orphans(self) -> None:
        """Remove broken and stale symlinks from all smart folder directories.

        Walks sorted/ looking for known smart folder subdirectory names.
        For each, removes:
        - Broken symlinks (target doesn't exist)
        - Stale symlinks (name doesn't match any real file in parent leaf folder)

        Non-symlink files and directories in smart folders are never touched.
        """
        if not self.sorted_dir.is_dir():
            return

        sf_names = {entry.config.name for entry in self.smart_folders}
        if not sf_names:
            return

        self._walk_for_smart_folders(self.sorted_dir, sf_names)

    def _walk_for_smart_folders(self, directory: Path, sf_names: set[str]) -> None:
        """Recursively walk directory looking for smart folder subdirs."""
        try:
            children = list(directory.iterdir())
        except OSError:
            return

        for child in children:
            if not child.is_dir():
                continue
            if child.name.startswith("."):
                continue

            if child.name in sf_names:
                self._cleanup_smart_folder_dir(child)
            else:
                self._walk_for_smart_folders(child, sf_names)

    def _cleanup_smart_folder_dir(self, sf_dir: Path) -> None:
        """Clean up orphaned symlinks in a single smart folder directory."""
        leaf_folder = sf_dir.parent

        # Build set of real filenames in the leaf folder
        leaf_files: set[str] = set()
        try:
            for item in leaf_folder.iterdir():
                if item.is_file() and not item.is_symlink():
                    leaf_files.add(item.name)
        except OSError:
            return

        try:
            items = list(sf_dir.iterdir())
        except OSError:
            return

        for item in items:
            if item.name.startswith(".") or item.name.startswith("~"):
                continue

            if not item.is_symlink():
                # Non-symlink files in smart folders are never touched
                continue

            # Check if target exists
            try:
                target_exists = item.resolve().exists()
            except Exception:
                target_exists = False

            if not target_exists:
                try:
                    item.unlink()
                    logger.info(
                        "Cleaned up broken symlink: %s",
                        item.relative_to(self.root),
                    )
                except Exception:
                    pass
                continue

            # Check if symlink name matches a real file in the leaf folder
            if item.name not in leaf_files:
                try:
                    item.unlink()
                    logger.info(
                        "Cleaned up stale symlink: %s",
                        item.relative_to(self.root),
                    )
                except Exception:
                    pass


# ---------------------------------------------------------------------------
# Root-level smart folders
# ---------------------------------------------------------------------------

@dataclass
class RootSmartFolderEntry:
    """A root-level smart folder with an arbitrary output path."""
    name: str
    context: str
    path: Path  # resolved absolute path for symlink directory
    config: SmartFolderConfig


class RootSmartFolderReconciler:
    """Manages root-level smart folder symlinks at arbitrary paths."""

    def __init__(self, root: Path, entries: list[RootSmartFolderEntry]):
        self.root = root
        self.sorted_dir = root / "sorted"
        self.entries = entries

    def reconcile(self, records: list[Record]) -> None:
        """Create/remove symlinks for root-level smart folders."""
        for record in records:
            if record.state != State.IS_COMPLETE:
                continue
            if record.current_location != "sorted":
                continue
            if not record.context:
                continue

            current = record.current_file
            if not current:
                continue

            file_path = self.root / current.path
            if not file_path.exists() or file_path.is_symlink():
                continue

            filename = file_path.name

            str_fields: dict[str, str] = {}
            if record.metadata:
                str_fields = {
                    k: str(v) for k, v in record.metadata.items()
                    if v is not None
                }

            for entry in self.entries:
                if entry.context != record.context:
                    continue

                sf_config = entry.config
                symlink_path = entry.path / filename

                if not sf_config.matches_filename(filename):
                    if symlink_path.is_symlink():
                        try:
                            symlink_path.unlink()
                        except OSError:
                            pass
                    continue

                condition_matches = (
                    sf_config.condition is None
                    or sf_config.condition.evaluate(str_fields)
                )

                if condition_matches:
                    if not symlink_path.exists() and not symlink_path.is_symlink():
                        try:
                            entry.path.mkdir(parents=True, exist_ok=True)
                            relative_target = Path(
                                os.path.relpath(file_path, entry.path)
                            )
                            symlink_path.symlink_to(relative_target)
                            logger.info(
                                "Root smart folder link: %s -> %s",
                                symlink_path, filename,
                            )
                        except Exception as e:
                            logger.error(
                                "Failed to create root smart folder symlink %s: %s",
                                symlink_path, e,
                            )
                else:
                    if symlink_path.is_symlink():
                        try:
                            symlink_path.unlink()
                            logger.info(
                                "Removed root smart folder link: %s",
                                symlink_path,
                            )
                        except Exception as e:
                            logger.error(
                                "Failed to remove root smart folder symlink %s: %s",
                                symlink_path, e,
                            )

    def cleanup_orphans(self) -> None:
        """Remove orphaned symlinks from root smart folder directories.

        Only removes symlinks whose resolved target is within sorted/.
        Regular files and symlinks pointing elsewhere are left untouched.
        """
        for entry in self.entries:
            if not entry.path.is_dir():
                continue

            try:
                items = list(entry.path.iterdir())
            except OSError:
                continue

            for item in items:
                if not item.is_symlink():
                    continue

                try:
                    resolved = item.resolve()
                except Exception:
                    resolved = None

                if resolved is None:
                    continue

                # Only touch symlinks pointing into sorted/
                try:
                    resolved.relative_to(self.sorted_dir)
                except ValueError:
                    continue

                # Remove if target no longer exists
                if not resolved.exists():
                    try:
                        item.unlink()
                        logger.info(
                            "Cleaned up broken root smart folder symlink: %s",
                            item,
                        )
                    except Exception:
                        pass
