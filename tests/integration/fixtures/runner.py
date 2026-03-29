"""YAML fixture runner - orchestrates step execution."""

import os
import re
import shutil
import time
from pathlib import Path

import subprocess as _subprocess

from .loader import (
    CopyAction,
    CopyFromTreeAction,
    DeleteAction,
    FixtureSpec,
    MkdirAction,
    MoveAction,
    SleepAction,
    SqlAction,
    StartWatcherAction,
    StepSpec,
    StopWatcherAction,
)
from .scanner import TreeMatchResult, match_tree, scan_tree

DB_CONTAINER = os.environ.get(
    "DB_CONTAINER", "integration-mrdocument-db-1"
)

WATCHER_CONTAINER = os.environ.get(
    "WATCHER_CONTAINER", "integration-mrdocument-watcher-1"
)
WATCHER_HEALTH_URL = os.environ.get(
    "WATCHER_HEALTH_URL", "http://localhost:8080/health"
)


def _format_mismatch(
    fixture_name: str, step_index: int, step: StepSpec, result: TreeMatchResult,
    actual_files: set[str],
) -> str:
    """Format a detailed mismatch report for assertion errors."""
    lines = [
        f"YAML Fixture Failed: {fixture_name}, step {step_index} "
        f"(timeout: {step.timeout}s)",
        "",
    ]
    if result.unmatched_patterns:
        lines.append("Unmatched expected patterns:")
        for p in result.unmatched_patterns:
            lines.append(f"  - {p}")
        lines.append("")
    if result.extra_files:
        lines.append("Extra files (no pattern matched):")
        for f in result.extra_files:
            lines.append(f"  - {f}")
        lines.append("")
    lines.append("Full actual tree:")
    for f in sorted(actual_files):
        lines.append(f"  {f}")
    return "\n".join(lines)


class FixtureRunner:
    def __init__(self, sync_folder: Path, fixture: FixtureSpec, poll_interval: float = 5.0):
        self.sync_folder = sync_folder
        self.fixture = fixture
        self.poll_interval = poll_interval

    def run(self):
        for i, step in enumerate(self.fixture.steps):
            self._run_step(i, step)

    def _run_step(self, index: int, step: StepSpec):
        # 1. Execute input actions
        for action in step.inputs:
            if isinstance(action, CopyAction):
                self._copy_file(action)
            elif isinstance(action, MoveAction):
                self._move_file(action)
            elif isinstance(action, DeleteAction):
                self._delete_file(action)
            elif isinstance(action, CopyFromTreeAction):
                self._copy_from_tree(action)
            elif isinstance(action, MkdirAction):
                self._mkdir(action)
            elif isinstance(action, SleepAction):
                time.sleep(action.seconds)
            elif isinstance(action, SqlAction):
                self._run_sql(action)
            elif isinstance(action, StopWatcherAction):
                self._stop_watcher()
            elif isinstance(action, StartWatcherAction):
                self._start_watcher()

        # 2. Poll until tree matches or timeout
        deadline = time.monotonic() + step.timeout
        result = None
        actual = set()
        while time.monotonic() < deadline:
            actual = scan_tree(self.sync_folder)
            result = match_tree(actual, step.expected)
            if result.is_perfect_match():
                return
            time.sleep(self.poll_interval)

        # 3. Timeout - raise with detailed mismatch report
        if result is None:
            actual = scan_tree(self.sync_folder)
            result = match_tree(actual, step.expected)
        raise AssertionError(
            _format_mismatch(self.fixture.name, index, step, result, actual)
        )

    def _copy_file(self, action: CopyAction):
        """Copy a file from the files map to the destination path."""
        file_ref = self.fixture.files[action.filename]
        dest = self.sync_folder / action.dest_path
        dest.parent.mkdir(parents=True, exist_ok=True)

        if file_ref.content is not None:
            # Inline content: write atomically via tmp+rename
            tmp = dest.with_suffix(dest.suffix + ".tmp")
            tmp.write_text(file_ref.content, encoding="utf-8")
            os.rename(str(tmp), str(dest))
        elif file_ref.source_path is not None:
            # File on disk: atomic copy via tmp+rename
            tmp = dest.with_suffix(dest.suffix + ".tmp")
            shutil.copyfile(str(file_ref.source_path), str(tmp))
            os.rename(str(tmp), str(dest))
        else:
            raise ValueError(
                f"FileRef '{action.filename}' has neither content nor source_path"
            )

    def _move_file(self, action: MoveAction):
        """Find file matching regex in current tree and move it."""
        actual = scan_tree(self.sync_folder)
        matches = [f for f in actual if re.fullmatch(action.pattern, f)]
        if not matches:
            raise FileNotFoundError(
                f"No file matching pattern '{action.pattern}' in tree. "
                f"Files: {sorted(actual)}"
            )
        src_rel = matches[0]
        src = self.sync_folder / src_rel
        to_dir = self.sync_folder / action.to
        to_dir.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(to_dir / src.name))

    def _delete_file(self, action: DeleteAction):
        """Find file matching regex in current tree and delete it."""
        actual = scan_tree(self.sync_folder)
        matches = [f for f in actual if re.fullmatch(action.pattern, f)]
        if not matches:
            raise FileNotFoundError(
                f"No file matching pattern '{action.pattern}' in tree. "
                f"Files: {sorted(actual)}"
            )
        for m in matches:
            (self.sync_folder / m).unlink()

    def _copy_from_tree(self, action: CopyFromTreeAction):
        """Find file matching regex in current tree and copy it to destination."""
        actual = scan_tree(self.sync_folder)
        matches = [f for f in actual if re.fullmatch(action.pattern, f)]
        if not matches:
            raise FileNotFoundError(
                f"No file matching pattern '{action.pattern}' in tree. "
                f"Files: {sorted(actual)}"
            )
        src_rel = matches[0]
        src = self.sync_folder / src_rel
        to_dir = self.sync_folder / action.to
        to_dir.mkdir(parents=True, exist_ok=True)
        dest = to_dir / src.name
        tmp = dest.with_suffix(dest.suffix + ".tmp")
        shutil.copyfile(str(src), str(tmp))
        os.rename(str(tmp), str(dest))

    def _mkdir(self, action: MkdirAction):
        """Create a directory."""
        (self.sync_folder / action.path).mkdir(parents=True, exist_ok=True)

    def _run_sql(self, action: SqlAction):
        """Execute a SQL statement against the test database."""
        _subprocess.run(
            [
                "docker", "exec", DB_CONTAINER,
                "psql", "-U", "mrdocument", "-d", "mrdocument",
                "-c", action.sql,
            ],
            check=True, capture_output=True, text=True, timeout=10,
        )

    def _stop_watcher(self):
        """Stop the watcher container."""
        _subprocess.run(
            ["docker", "stop", WATCHER_CONTAINER],
            check=True, capture_output=True, timeout=30,
        )

    def _start_watcher(self):
        """Start the watcher container and wait for it to become healthy."""
        _subprocess.run(
            ["docker", "start", WATCHER_CONTAINER],
            check=True, capture_output=True, timeout=30,
        )
        from urllib.request import urlopen
        from urllib.error import URLError
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                with urlopen(WATCHER_HEALTH_URL, timeout=3) as resp:
                    if resp.status == 200:
                        return
            except (URLError, OSError):
                pass
            time.sleep(1)
        raise TimeoutError(
            f"Watcher not healthy at {WATCHER_HEALTH_URL} after 60s"
        )
