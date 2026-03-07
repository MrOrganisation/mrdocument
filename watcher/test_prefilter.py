"""Tests for prefilter.py — unsupported file type filtering."""

from pathlib import Path

import pytest

from prefilter import prefilter, SUPPORTED_EXTENSIONS, EXCLUDED_DIRS


def _write_file(path: Path, content: bytes = b"test content") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def _setup_dirs(root: Path) -> None:
    for d in ("archive", "incoming", "error", "sorted", "processed", "reviewed"):
        (root / d).mkdir(parents=True, exist_ok=True)


class TestPrefilter:
    def test_moves_unsupported_from_incoming(self, tmp_path):
        _setup_dirs(tmp_path)
        _write_file(tmp_path / "incoming" / "font.ttf")

        moved = prefilter(tmp_path)

        assert moved == 1
        assert not (tmp_path / "incoming" / "font.ttf").exists()
        assert (tmp_path / "error" / "font.ttf").exists()

    def test_moves_unsupported_from_archive(self, tmp_path):
        _setup_dirs(tmp_path)
        _write_file(tmp_path / "archive" / "spreadsheet.numbers")

        moved = prefilter(tmp_path)

        assert moved == 1
        assert not (tmp_path / "archive" / "spreadsheet.numbers").exists()
        assert (tmp_path / "error" / "spreadsheet.numbers").exists()

    def test_moves_unsupported_from_sorted_subdirs(self, tmp_path):
        _setup_dirs(tmp_path)
        _write_file(tmp_path / "sorted" / "work" / "font.ttf")
        _write_file(tmp_path / "sorted" / "privat" / "deep" / "nested.otf")

        moved = prefilter(tmp_path)

        assert moved == 2
        assert not (tmp_path / "sorted" / "work" / "font.ttf").exists()
        assert not (tmp_path / "sorted" / "privat" / "deep" / "nested.otf").exists()
        assert (tmp_path / "error" / "font.ttf").exists()
        assert (tmp_path / "error" / "nested.otf").exists()

    def test_moves_unsupported_from_processed(self, tmp_path):
        _setup_dirs(tmp_path)
        _write_file(tmp_path / "processed" / "thesis.tex")

        moved = prefilter(tmp_path)

        assert moved == 1
        assert (tmp_path / "error" / "thesis.tex").exists()

    def test_moves_unsupported_from_reviewed(self, tmp_path):
        _setup_dirs(tmp_path)
        _write_file(tmp_path / "reviewed" / "budget.numbers")

        moved = prefilter(tmp_path)

        assert moved == 1
        assert (tmp_path / "error" / "budget.numbers").exists()

    def test_moves_unsupported_from_trash(self, tmp_path):
        _setup_dirs(tmp_path)
        (tmp_path / "trash").mkdir(exist_ok=True)
        _write_file(tmp_path / "trash" / "junk.ttf")

        moved = prefilter(tmp_path)

        assert moved == 1
        assert (tmp_path / "error" / "junk.ttf").exists()

    def test_skips_error_dir(self, tmp_path):
        _setup_dirs(tmp_path)
        _write_file(tmp_path / "error" / "already_here.ttf")

        moved = prefilter(tmp_path)

        assert moved == 0
        assert (tmp_path / "error" / "already_here.ttf").exists()

    def test_skips_void_dir(self, tmp_path):
        _setup_dirs(tmp_path)
        (tmp_path / "void").mkdir(exist_ok=True)
        _write_file(tmp_path / "void" / "font.ttf")

        moved = prefilter(tmp_path)

        assert moved == 0
        assert (tmp_path / "void" / "font.ttf").exists()

    def test_skips_hidden_dirs(self, tmp_path):
        _setup_dirs(tmp_path)
        _write_file(tmp_path / ".output" / "some-uuid")

        moved = prefilter(tmp_path)

        assert moved == 0
        assert (tmp_path / ".output" / "some-uuid").exists()

    def test_keeps_supported_files(self, tmp_path):
        _setup_dirs(tmp_path)
        _write_file(tmp_path / "incoming" / "doc.pdf")
        _write_file(tmp_path / "incoming" / "photo.jpg")
        _write_file(tmp_path / "archive" / "recording.mp3")
        _write_file(tmp_path / "sorted" / "work" / "report.pdf")

        moved = prefilter(tmp_path)

        assert moved == 0
        assert (tmp_path / "incoming" / "doc.pdf").exists()
        assert (tmp_path / "incoming" / "photo.jpg").exists()
        assert (tmp_path / "archive" / "recording.mp3").exists()
        assert (tmp_path / "sorted" / "work" / "report.pdf").exists()

    def test_handles_name_collision(self, tmp_path):
        _setup_dirs(tmp_path)
        _write_file(tmp_path / "incoming" / "font.ttf", b"first")
        _write_file(tmp_path / "error" / "font.ttf", b"existing")

        moved = prefilter(tmp_path)

        assert moved == 1
        # Original in error/ untouched
        assert (tmp_path / "error" / "font.ttf").read_bytes() == b"existing"
        # New one got a unique suffix
        error_files = list((tmp_path / "error").iterdir())
        assert len(error_files) == 2

    def test_multiple_unsupported_across_dirs(self, tmp_path):
        _setup_dirs(tmp_path)
        _write_file(tmp_path / "incoming" / "a.ttf")
        _write_file(tmp_path / "incoming" / "b.otf")
        _write_file(tmp_path / "archive" / "c.numbers")
        _write_file(tmp_path / "sorted" / "work" / "d.tex")

        moved = prefilter(tmp_path)

        assert moved == 4
        assert len(list((tmp_path / "error").iterdir())) == 4

    def test_skips_hidden_files(self, tmp_path):
        _setup_dirs(tmp_path)
        _write_file(tmp_path / "incoming" / ".hidden_unsupported")

        moved = prefilter(tmp_path)

        assert moved == 0
        assert (tmp_path / "incoming" / ".hidden_unsupported").exists()

    def test_skips_symlinks(self, tmp_path):
        _setup_dirs(tmp_path)
        real = tmp_path / "incoming" / "real.pdf"
        _write_file(real)
        link = tmp_path / "incoming" / "link.ttf"
        link.symlink_to(real)

        moved = prefilter(tmp_path)

        assert moved == 0
        assert link.is_symlink()

    def test_empty_root(self, tmp_path):
        moved = prefilter(tmp_path)
        assert moved == 0

    def test_creates_error_dir(self, tmp_path):
        (tmp_path / "incoming").mkdir()
        _write_file(tmp_path / "incoming" / "font.ttf")
        assert not (tmp_path / "error").exists()

        prefilter(tmp_path)

        assert (tmp_path / "error").is_dir()

    def test_no_extension_moved(self, tmp_path):
        _setup_dirs(tmp_path)
        _write_file(tmp_path / "incoming" / "Makefile")

        moved = prefilter(tmp_path)

        assert moved == 1
        assert (tmp_path / "error" / "Makefile").exists()

    def test_config_files_not_moved(self, tmp_path):
        """context.yaml and smartfolders.yaml in sorted/ should not be moved to error/."""
        _setup_dirs(tmp_path)
        _write_file(tmp_path / "sorted" / "arbeit" / "context.yaml", b"name: arbeit")
        _write_file(tmp_path / "sorted" / "arbeit" / "smartfolders.yaml", b"smart_folders: {}")

        moved = prefilter(tmp_path)

        assert moved == 0
        assert (tmp_path / "sorted" / "arbeit" / "context.yaml").exists()
        assert (tmp_path / "sorted" / "arbeit" / "smartfolders.yaml").exists()

    def test_generated_yaml_not_moved(self, tmp_path):
        """generated.yaml in sorted/ should not be moved to error/."""
        _setup_dirs(tmp_path)
        _write_file(tmp_path / "sorted" / "arbeit" / "generated.yaml", b"fields: {}")

        moved = prefilter(tmp_path)

        assert moved == 0
        assert (tmp_path / "sorted" / "arbeit" / "generated.yaml").exists()

    def test_other_yaml_in_sorted_moved(self, tmp_path):
        """Non-config YAML files in sorted/ should still be moved to error/."""
        _setup_dirs(tmp_path)
        _write_file(tmp_path / "sorted" / "arbeit" / "random.yaml")

        moved = prefilter(tmp_path)

        assert moved == 1
        assert (tmp_path / "error" / "random.yaml").exists()

    def test_tmp_files_skipped(self, tmp_path):
        """Temp files (.tmp) should not be moved — used as atomic write intermediates."""
        _setup_dirs(tmp_path)
        _write_file(tmp_path / "incoming" / "upload.pdf.tmp")
        _write_file(tmp_path / "sorted" / "arbeit" / "doc.txt.tmp")

        moved = prefilter(tmp_path)

        assert moved == 0
        assert (tmp_path / "incoming" / "upload.pdf.tmp").exists()
        assert (tmp_path / "sorted" / "arbeit" / "doc.txt.tmp").exists()
