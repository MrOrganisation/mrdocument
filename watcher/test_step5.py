"""Tests for step5.py — SmartFolderReconciler symlink management."""

from datetime import datetime, timezone
from pathlib import Path

import pytest

from models import State, PathEntry, Record
from sorter import SmartFolderCondition, SmartFolderConfig
from step5 import SmartFolderEntry, SmartFolderReconciler, RootSmartFolderEntry, RootSmartFolderReconciler


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts():
    return datetime(2025, 1, 1, tzinfo=timezone.utc)


def _make_record(**kwargs) -> Record:
    defaults = {
        "original_filename": "test.pdf",
        "source_hash": "abc123",
    }
    defaults.update(kwargs)
    return Record(**defaults)


def _write_file(root: Path, rel_path: str, content: bytes = b"test content") -> None:
    full = root / rel_path
    full.parent.mkdir(parents=True, exist_ok=True)
    full.write_bytes(content)


def _make_condition(field: str, value: str) -> SmartFolderCondition:
    cond = SmartFolderCondition(field=field, value=value)
    cond.__post_init__()
    return cond


def _make_sf_config(name: str, field: str, value: str,
                    filename_regex: str = None) -> SmartFolderConfig:
    cond = _make_condition(field, value)
    config = SmartFolderConfig(name=name, condition=cond, filename_regex=filename_regex)
    config.__post_init__()
    return config


def _setup_reconciler(root: Path, entries: list[SmartFolderEntry]) -> SmartFolderReconciler:
    (root / "sorted").mkdir(parents=True, exist_ok=True)
    return SmartFolderReconciler(root, entries)


# ---------------------------------------------------------------------------
# Symlink creation
# ---------------------------------------------------------------------------

class TestSymlinkCreated:
    def test_symlink_created_for_matching_record(self, tmp_path):
        """IS_COMPLETE record in sorted/ with matching metadata -> symlink."""
        _write_file(tmp_path, "sorted/arbeit/Schulze/invoice.pdf")
        entry = SmartFolderEntry(
            context="arbeit",
            config=_make_sf_config("rechnungen", "type", "Rechnung"),
        )
        reconciler = _setup_reconciler(tmp_path, [entry])

        record = _make_record(
            state=State.IS_COMPLETE,
            context="arbeit",
            metadata={"type": "Rechnung"},
            current_paths=[PathEntry("sorted/arbeit/Schulze/invoice.pdf", _ts())],
        )
        reconciler.reconcile([record])

        symlink = tmp_path / "sorted/arbeit/Schulze/rechnungen/invoice.pdf"
        assert symlink.is_symlink()
        assert symlink.resolve() == (tmp_path / "sorted/arbeit/Schulze/invoice.pdf").resolve()

    def test_smart_folder_dir_created_on_demand(self, tmp_path):
        """Smart folder directory created when first symlink is placed."""
        _write_file(tmp_path, "sorted/arbeit/Schulze/invoice.pdf")
        entry = SmartFolderEntry(
            context="arbeit",
            config=_make_sf_config("rechnungen", "type", "Rechnung"),
        )
        reconciler = _setup_reconciler(tmp_path, [entry])

        sf_dir = tmp_path / "sorted/arbeit/Schulze/rechnungen"
        assert not sf_dir.exists()

        record = _make_record(
            state=State.IS_COMPLETE,
            context="arbeit",
            metadata={"type": "Rechnung"},
            current_paths=[PathEntry("sorted/arbeit/Schulze/invoice.pdf", _ts())],
        )
        reconciler.reconcile([record])
        assert sf_dir.is_dir()


# ---------------------------------------------------------------------------
# No symlink
# ---------------------------------------------------------------------------

class TestNoSymlink:
    def test_no_symlink_for_non_matching_record(self, tmp_path):
        """IS_COMPLETE record whose metadata doesn't match -> no symlink."""
        _write_file(tmp_path, "sorted/arbeit/Schulze/contract.pdf")
        entry = SmartFolderEntry(
            context="arbeit",
            config=_make_sf_config("rechnungen", "type", "Rechnung"),
        )
        reconciler = _setup_reconciler(tmp_path, [entry])

        record = _make_record(
            state=State.IS_COMPLETE,
            context="arbeit",
            metadata={"type": "Vertrag"},
            current_paths=[PathEntry("sorted/arbeit/Schulze/contract.pdf", _ts())],
        )
        reconciler.reconcile([record])

        symlink = tmp_path / "sorted/arbeit/Schulze/rechnungen/contract.pdf"
        assert not symlink.exists()

    def test_records_not_in_sorted_skipped(self, tmp_path):
        """IS_COMPLETE in processed/ -> no symlink action."""
        _write_file(tmp_path, "processed/invoice.pdf")
        entry = SmartFolderEntry(
            context="arbeit",
            config=_make_sf_config("rechnungen", "type", "Rechnung"),
        )
        reconciler = _setup_reconciler(tmp_path, [entry])

        record = _make_record(
            state=State.IS_COMPLETE,
            context="arbeit",
            metadata={"type": "Rechnung"},
            current_paths=[PathEntry("processed/invoice.pdf", _ts())],
        )
        reconciler.reconcile([record])

        # No smart folder dir should have been created anywhere
        assert not (tmp_path / "processed/rechnungen").exists()


# ---------------------------------------------------------------------------
# Symlink removal
# ---------------------------------------------------------------------------

class TestSymlinkRemoval:
    def test_symlink_removed_when_condition_no_longer_matches(self, tmp_path):
        """Existing symlink removed when metadata changes to non-matching."""
        _write_file(tmp_path, "sorted/arbeit/Schulze/invoice.pdf")
        sf_dir = tmp_path / "sorted/arbeit/Schulze/rechnungen"
        sf_dir.mkdir(parents=True)
        symlink = sf_dir / "invoice.pdf"
        symlink.symlink_to(Path("..") / "invoice.pdf")
        assert symlink.is_symlink()

        entry = SmartFolderEntry(
            context="arbeit",
            config=_make_sf_config("rechnungen", "type", "Rechnung"),
        )
        reconciler = _setup_reconciler(tmp_path, [entry])

        record = _make_record(
            state=State.IS_COMPLETE,
            context="arbeit",
            metadata={"type": "Vertrag"},  # no longer matches
            current_paths=[PathEntry("sorted/arbeit/Schulze/invoice.pdf", _ts())],
        )
        reconciler.reconcile([record])
        assert not symlink.exists()


# ---------------------------------------------------------------------------
# Multiple smart folders
# ---------------------------------------------------------------------------

class TestMultipleSmartFolders:
    def test_multiple_smart_folders_same_file(self, tmp_path):
        """File matches two smart folders -> symlinks in both."""
        _write_file(tmp_path, "sorted/arbeit/Schulze/invoice.pdf")
        entry1 = SmartFolderEntry(
            context="arbeit",
            config=_make_sf_config("rechnungen", "type", "Rechnung"),
        )
        entry2 = SmartFolderEntry(
            context="arbeit",
            config=_make_sf_config("schulze", "sender", "Schulze"),
        )
        reconciler = _setup_reconciler(tmp_path, [entry1, entry2])

        record = _make_record(
            state=State.IS_COMPLETE,
            context="arbeit",
            metadata={"type": "Rechnung", "sender": "Schulze"},
            current_paths=[PathEntry("sorted/arbeit/Schulze/invoice.pdf", _ts())],
        )
        reconciler.reconcile([record])

        assert (tmp_path / "sorted/arbeit/Schulze/rechnungen/invoice.pdf").is_symlink()
        assert (tmp_path / "sorted/arbeit/Schulze/schulze/invoice.pdf").is_symlink()


# ---------------------------------------------------------------------------
# Orphan cleanup
# ---------------------------------------------------------------------------

class TestOrphanCleanup:
    def test_orphan_cleanup_broken_symlink(self, tmp_path):
        """Symlink whose target was deleted -> removed."""
        leaf = tmp_path / "sorted/arbeit/Schulze"
        leaf.mkdir(parents=True)
        sf_dir = leaf / "rechnungen"
        sf_dir.mkdir()
        symlink = sf_dir / "deleted.pdf"
        symlink.symlink_to(Path("..") / "deleted.pdf")
        # Target does NOT exist -> broken
        assert symlink.is_symlink()

        entry = SmartFolderEntry(
            context="arbeit",
            config=_make_sf_config("rechnungen", "type", "Rechnung"),
        )
        reconciler = _setup_reconciler(tmp_path, [entry])
        reconciler.cleanup_orphans()

        assert not symlink.exists()

    def test_orphan_cleanup_stale_symlink(self, tmp_path):
        """Symlink name doesn't match any real file -> removed."""
        leaf = tmp_path / "sorted/arbeit/Schulze"
        leaf.mkdir(parents=True)
        # Real file has a different name
        (leaf / "current.pdf").write_bytes(b"content")
        sf_dir = leaf / "rechnungen"
        sf_dir.mkdir()
        # Symlink for an old name that no longer exists as a real file
        symlink = sf_dir / "old_name.pdf"
        symlink.symlink_to(Path("..") / "old_name.pdf")

        entry = SmartFolderEntry(
            context="arbeit",
            config=_make_sf_config("rechnungen", "type", "Rechnung"),
        )
        reconciler = _setup_reconciler(tmp_path, [entry])
        reconciler.cleanup_orphans()

        assert not symlink.exists()

    def test_non_symlink_files_ignored(self, tmp_path):
        """Regular file in smart folder dir -> not touched."""
        leaf = tmp_path / "sorted/arbeit/Schulze"
        leaf.mkdir(parents=True)
        sf_dir = leaf / "rechnungen"
        sf_dir.mkdir()
        regular_file = sf_dir / "notes.txt"
        regular_file.write_text("user notes")

        entry = SmartFolderEntry(
            context="arbeit",
            config=_make_sf_config("rechnungen", "type", "Rechnung"),
        )
        reconciler = _setup_reconciler(tmp_path, [entry])
        reconciler.cleanup_orphans()

        assert regular_file.exists()
        assert regular_file.read_text() == "user notes"


# ---------------------------------------------------------------------------
# Filename regex filtering
# ---------------------------------------------------------------------------

class TestFilenameRegex:
    def test_filename_regex_filtering(self, tmp_path):
        """filename_regex set, file doesn't match -> no symlink."""
        _write_file(tmp_path, "sorted/arbeit/Schulze/invoice.pdf")
        config = _make_sf_config("briefe", "type", "Rechnung",
                                 filename_regex=r"brief")
        entry = SmartFolderEntry(context="arbeit", config=config)
        reconciler = _setup_reconciler(tmp_path, [entry])

        record = _make_record(
            state=State.IS_COMPLETE,
            context="arbeit",
            metadata={"type": "Rechnung"},
            current_paths=[PathEntry("sorted/arbeit/Schulze/invoice.pdf", _ts())],
        )
        reconciler.reconcile([record])

        assert not (tmp_path / "sorted/arbeit/Schulze/briefe/invoice.pdf").exists()

    def test_filename_regex_matches(self, tmp_path):
        """filename_regex set, file matches -> symlink created."""
        _write_file(tmp_path, "sorted/arbeit/Schulze/arztbrief_2025.pdf")
        config = _make_sf_config("briefe", "type", "Arztbrief",
                                 filename_regex=r"brief")
        entry = SmartFolderEntry(context="arbeit", config=config)
        reconciler = _setup_reconciler(tmp_path, [entry])

        record = _make_record(
            state=State.IS_COMPLETE,
            context="arbeit",
            metadata={"type": "Arztbrief"},
            current_paths=[PathEntry("sorted/arbeit/Schulze/arztbrief_2025.pdf", _ts())],
        )
        reconciler.reconcile([record])

        assert (tmp_path / "sorted/arbeit/Schulze/briefe/arztbrief_2025.pdf").is_symlink()


# ---------------------------------------------------------------------------
# Operator conditions
# ---------------------------------------------------------------------------

class TestOperatorConditions:
    def test_and_condition(self, tmp_path):
        """AND condition: both operands must match."""
        _write_file(tmp_path, "sorted/arbeit/Schulze/invoice.pdf")
        cond = SmartFolderCondition(
            operator="and",
            operands=[
                _make_condition("type", "Rechnung"),
                _make_condition("sender", "Schulze"),
            ],
        )
        config = SmartFolderConfig(name="both", condition=cond)
        entry = SmartFolderEntry(context="arbeit", config=config)
        reconciler = _setup_reconciler(tmp_path, [entry])

        # Both match
        record = _make_record(
            state=State.IS_COMPLETE,
            context="arbeit",
            metadata={"type": "Rechnung", "sender": "Schulze"},
            current_paths=[PathEntry("sorted/arbeit/Schulze/invoice.pdf", _ts())],
        )
        reconciler.reconcile([record])
        assert (tmp_path / "sorted/arbeit/Schulze/both/invoice.pdf").is_symlink()

    def test_or_condition(self, tmp_path):
        """OR condition: at least one operand matches."""
        _write_file(tmp_path, "sorted/arbeit/Schulze/contract.pdf")
        cond = SmartFolderCondition(
            operator="or",
            operands=[
                _make_condition("type", "Rechnung"),
                _make_condition("type", "Vertrag"),
            ],
        )
        config = SmartFolderConfig(name="mixed", condition=cond)
        entry = SmartFolderEntry(context="arbeit", config=config)
        reconciler = _setup_reconciler(tmp_path, [entry])

        record = _make_record(
            state=State.IS_COMPLETE,
            context="arbeit",
            metadata={"type": "Vertrag"},
            current_paths=[PathEntry("sorted/arbeit/Schulze/contract.pdf", _ts())],
        )
        reconciler.reconcile([record])
        assert (tmp_path / "sorted/arbeit/Schulze/mixed/contract.pdf").is_symlink()

    def test_not_condition(self, tmp_path):
        """NOT condition: negated match."""
        _write_file(tmp_path, "sorted/arbeit/Schulze/contract.pdf")
        cond = SmartFolderCondition(
            operator="not",
            operands=[_make_condition("type", "Rechnung")],
        )
        config = SmartFolderConfig(name="non_invoices", condition=cond)
        entry = SmartFolderEntry(context="arbeit", config=config)
        reconciler = _setup_reconciler(tmp_path, [entry])

        record = _make_record(
            state=State.IS_COMPLETE,
            context="arbeit",
            metadata={"type": "Vertrag"},
            current_paths=[PathEntry("sorted/arbeit/Schulze/contract.pdf", _ts())],
        )
        reconciler.reconcile([record])
        assert (tmp_path / "sorted/arbeit/Schulze/non_invoices/contract.pdf").is_symlink()


# ---------------------------------------------------------------------------
# Collision avoidance
# ---------------------------------------------------------------------------

class TestCollisionAvoidance:
    def test_non_symlink_collision_not_overwritten(self, tmp_path):
        """Non-symlink file with same name in smart folder dir -> not overwritten."""
        _write_file(tmp_path, "sorted/arbeit/Schulze/invoice.pdf")
        sf_dir = tmp_path / "sorted/arbeit/Schulze/rechnungen"
        sf_dir.mkdir(parents=True)
        # Pre-existing regular file with same name
        collision = sf_dir / "invoice.pdf"
        collision.write_text("user placed this here")

        entry = SmartFolderEntry(
            context="arbeit",
            config=_make_sf_config("rechnungen", "type", "Rechnung"),
        )
        reconciler = _setup_reconciler(tmp_path, [entry])

        record = _make_record(
            state=State.IS_COMPLETE,
            context="arbeit",
            metadata={"type": "Rechnung"},
            current_paths=[PathEntry("sorted/arbeit/Schulze/invoice.pdf", _ts())],
        )
        reconciler.reconcile([record])

        # Regular file untouched, no symlink created (exists() is True for the file)
        assert not collision.is_symlink()
        assert collision.read_text() == "user placed this here"


# ===========================================================================
# Root-level smart folders
# ===========================================================================

def _make_root_entry(name, context, path, field, value,
                     filename_regex=None):
    config = _make_sf_config(name, field, value, filename_regex=filename_regex)
    return RootSmartFolderEntry(
        name=name, context=context, path=path, config=config,
    )


class TestRootSmartFolder:
    def test_symlink_created_at_absolute_path(self, tmp_path):
        """Root smart folder creates symlink at the configured absolute path."""
        _write_file(tmp_path, "sorted/arbeit/Schulze/invoice.pdf")
        target_dir = tmp_path / "Desktop" / "Rechnungen"

        entry = _make_root_entry(
            "rechnungen", "arbeit", target_dir, "type", "Rechnung",
        )
        reconciler = RootSmartFolderReconciler(tmp_path, [entry])

        record = _make_record(
            state=State.IS_COMPLETE,
            context="arbeit",
            metadata={"type": "Rechnung"},
            current_paths=[PathEntry("sorted/arbeit/Schulze/invoice.pdf", _ts())],
        )
        reconciler.reconcile([record])

        symlink = target_dir / "invoice.pdf"
        assert symlink.is_symlink()
        assert symlink.resolve() == (tmp_path / "sorted/arbeit/Schulze/invoice.pdf").resolve()

    def test_symlink_target_is_relative(self, tmp_path):
        """Symlink target uses relative path for portability."""
        _write_file(tmp_path, "sorted/arbeit/Schulze/invoice.pdf")
        target_dir = tmp_path / "links"

        entry = _make_root_entry(
            "rechnungen", "arbeit", target_dir, "type", "Rechnung",
        )
        reconciler = RootSmartFolderReconciler(tmp_path, [entry])

        record = _make_record(
            state=State.IS_COMPLETE,
            context="arbeit",
            metadata={"type": "Rechnung"},
            current_paths=[PathEntry("sorted/arbeit/Schulze/invoice.pdf", _ts())],
        )
        reconciler.reconcile([record])

        symlink = target_dir / "invoice.pdf"
        # The raw symlink target should be relative, not absolute
        raw_target = symlink.readlink()
        assert not raw_target.is_absolute()

    def test_condition_filtering(self, tmp_path):
        """Records that don't match condition get no symlink."""
        _write_file(tmp_path, "sorted/arbeit/Schulze/contract.pdf")
        target_dir = tmp_path / "Rechnungen"

        entry = _make_root_entry(
            "rechnungen", "arbeit", target_dir, "type", "Rechnung",
        )
        reconciler = RootSmartFolderReconciler(tmp_path, [entry])

        record = _make_record(
            state=State.IS_COMPLETE,
            context="arbeit",
            metadata={"type": "Vertrag"},
            current_paths=[PathEntry("sorted/arbeit/Schulze/contract.pdf", _ts())],
        )
        reconciler.reconcile([record])

        assert not (target_dir / "contract.pdf").exists()

    def test_filename_regex_filtering(self, tmp_path):
        """filename_regex filters out non-matching files."""
        _write_file(tmp_path, "sorted/arbeit/Schulze/invoice.pdf")
        target_dir = tmp_path / "briefe"

        entry = _make_root_entry(
            "briefe", "arbeit", target_dir, "type", "Rechnung",
            filename_regex=r"brief",
        )
        reconciler = RootSmartFolderReconciler(tmp_path, [entry])

        record = _make_record(
            state=State.IS_COMPLETE,
            context="arbeit",
            metadata={"type": "Rechnung"},
            current_paths=[PathEntry("sorted/arbeit/Schulze/invoice.pdf", _ts())],
        )
        reconciler.reconcile([record])

        assert not (target_dir / "invoice.pdf").exists()

    def test_symlink_removed_when_condition_no_longer_matches(self, tmp_path):
        """Existing symlink removed when metadata changes."""
        _write_file(tmp_path, "sorted/arbeit/Schulze/invoice.pdf")
        target_dir = tmp_path / "Rechnungen"
        target_dir.mkdir(parents=True)
        import os
        symlink = target_dir / "invoice.pdf"
        rel = os.path.relpath(tmp_path / "sorted/arbeit/Schulze/invoice.pdf", target_dir)
        symlink.symlink_to(rel)
        assert symlink.is_symlink()

        entry = _make_root_entry(
            "rechnungen", "arbeit", target_dir, "type", "Rechnung",
        )
        reconciler = RootSmartFolderReconciler(tmp_path, [entry])

        record = _make_record(
            state=State.IS_COMPLETE,
            context="arbeit",
            metadata={"type": "Vertrag"},
            current_paths=[PathEntry("sorted/arbeit/Schulze/invoice.pdf", _ts())],
        )
        reconciler.reconcile([record])
        assert not symlink.exists()

    def test_wrong_context_skipped(self, tmp_path):
        """Records from a different context are skipped."""
        _write_file(tmp_path, "sorted/privat/Müller/letter.pdf")
        target_dir = tmp_path / "Rechnungen"

        entry = _make_root_entry(
            "rechnungen", "arbeit", target_dir, "type", "Rechnung",
        )
        reconciler = RootSmartFolderReconciler(tmp_path, [entry])

        record = _make_record(
            state=State.IS_COMPLETE,
            context="privat",
            metadata={"type": "Rechnung"},
            current_paths=[PathEntry("sorted/privat/Müller/letter.pdf", _ts())],
        )
        reconciler.reconcile([record])

        assert not (target_dir / "letter.pdf").exists()

    def test_collision_first_wins(self, tmp_path):
        """If symlink already exists, don't overwrite."""
        _write_file(tmp_path, "sorted/arbeit/Schulze/invoice.pdf")
        _write_file(tmp_path, "sorted/arbeit/Müller/invoice.pdf")
        target_dir = tmp_path / "Rechnungen"
        target_dir.mkdir(parents=True)

        entry = _make_root_entry(
            "rechnungen", "arbeit", target_dir, "type", "Rechnung",
        )
        reconciler = RootSmartFolderReconciler(tmp_path, [entry])

        record1 = _make_record(
            original_filename="invoice1.pdf",
            source_hash="hash1",
            state=State.IS_COMPLETE,
            context="arbeit",
            metadata={"type": "Rechnung"},
            current_paths=[PathEntry("sorted/arbeit/Schulze/invoice.pdf", _ts())],
        )
        record2 = _make_record(
            original_filename="invoice2.pdf",
            source_hash="hash2",
            state=State.IS_COMPLETE,
            context="arbeit",
            metadata={"type": "Rechnung"},
            current_paths=[PathEntry("sorted/arbeit/Müller/invoice.pdf", _ts())],
        )
        reconciler.reconcile([record1, record2])

        symlink = target_dir / "invoice.pdf"
        assert symlink.is_symlink()
        # First one wins — points to Schulze
        assert "Schulze" in str(symlink.resolve())


class TestRootSmartFolderCleanup:
    def test_broken_symlink_into_sorted_removed(self, tmp_path):
        """Broken symlink pointing into sorted/ is removed."""
        (tmp_path / "sorted").mkdir(parents=True)
        target_dir = tmp_path / "Rechnungen"
        target_dir.mkdir(parents=True)

        symlink = target_dir / "gone.pdf"
        # Point into sorted/ but target doesn't exist
        import os
        rel = os.path.relpath(tmp_path / "sorted/arbeit/Schulze/gone.pdf", target_dir)
        symlink.symlink_to(rel)
        assert symlink.is_symlink()

        entry = _make_root_entry(
            "rechnungen", "arbeit", target_dir, "type", "Rechnung",
        )
        reconciler = RootSmartFolderReconciler(tmp_path, [entry])
        reconciler.cleanup_orphans()

        assert not symlink.exists()

    def test_symlink_not_into_sorted_left_alone(self, tmp_path):
        """Symlink NOT pointing into sorted/ is left untouched."""
        (tmp_path / "sorted").mkdir(parents=True)
        target_dir = tmp_path / "Rechnungen"
        target_dir.mkdir(parents=True)

        # Create a file outside sorted/ and symlink to it
        external = tmp_path / "external" / "doc.pdf"
        external.parent.mkdir(parents=True)
        external.write_bytes(b"external")

        import os
        symlink = target_dir / "doc.pdf"
        rel = os.path.relpath(external, target_dir)
        symlink.symlink_to(rel)
        assert symlink.is_symlink()

        entry = _make_root_entry(
            "rechnungen", "arbeit", target_dir, "type", "Rechnung",
        )
        reconciler = RootSmartFolderReconciler(tmp_path, [entry])
        reconciler.cleanup_orphans()

        # Symlink should still exist — not in sorted/
        assert symlink.is_symlink()

    def test_regular_files_untouched(self, tmp_path):
        """Regular files in the smart folder dir are never removed."""
        (tmp_path / "sorted").mkdir(parents=True)
        target_dir = tmp_path / "Rechnungen"
        target_dir.mkdir(parents=True)
        regular = target_dir / "notes.txt"
        regular.write_text("user notes")

        entry = _make_root_entry(
            "rechnungen", "arbeit", target_dir, "type", "Rechnung",
        )
        reconciler = RootSmartFolderReconciler(tmp_path, [entry])
        reconciler.cleanup_orphans()

        assert regular.exists()
        assert regular.read_text() == "user notes"


class TestLoadRootSmartFolders:
    def test_valid_yaml_parsed(self, tmp_path):
        """Valid smartfolders.yaml is parsed into RootSmartFolderEntry list."""
        import yaml
        from app import _load_root_smart_folders

        config = {
            "smart_folders": {
                "rechnungen": {
                    "context": "arbeit",
                    "path": "/home/user/Desktop/Rechnungen",
                    "condition": {"field": "type", "value": "Rechnung"},
                },
            }
        }
        (tmp_path / "smartfolders.yaml").write_text(yaml.dump(config))

        result = _load_root_smart_folders(tmp_path)
        assert result is not None
        assert len(result) == 1
        assert result[0].name == "rechnungen"
        assert result[0].context == "arbeit"
        assert result[0].path == Path("/home/user/Desktop/Rechnungen")

    def test_relative_path_resolved(self, tmp_path):
        """Relative path is resolved against the root."""
        import yaml
        from app import _load_root_smart_folders

        config = {
            "smart_folders": {
                "briefe": {
                    "context": "privat",
                    "path": "briefe_sammlung",
                    "condition": {"field": "type", "value": "Brief"},
                },
            }
        }
        (tmp_path / "smartfolders.yaml").write_text(yaml.dump(config))

        result = _load_root_smart_folders(tmp_path)
        assert result is not None
        assert result[0].path == tmp_path / "briefe_sammlung"

    def test_missing_context_skipped(self, tmp_path):
        """Entry without context is skipped."""
        import yaml
        from app import _load_root_smart_folders

        config = {
            "smart_folders": {
                "bad": {
                    "path": "/some/path",
                    "condition": {"field": "type", "value": "X"},
                },
            }
        }
        (tmp_path / "smartfolders.yaml").write_text(yaml.dump(config))

        result = _load_root_smart_folders(tmp_path)
        assert result is None

    def test_missing_path_skipped(self, tmp_path):
        """Entry without path is skipped."""
        import yaml
        from app import _load_root_smart_folders

        config = {
            "smart_folders": {
                "bad": {
                    "context": "arbeit",
                    "condition": {"field": "type", "value": "X"},
                },
            }
        }
        (tmp_path / "smartfolders.yaml").write_text(yaml.dump(config))

        result = _load_root_smart_folders(tmp_path)
        assert result is None

    def test_file_not_found_returns_none(self, tmp_path):
        """Missing smartfolders.yaml returns None."""
        from app import _load_root_smart_folders

        result = _load_root_smart_folders(tmp_path)
        assert result is None
