"""Fixtures and helpers for MrDocument live integration tests.

These tests run against a real MrDocument instance — either locally via
Docker (docker-compose.test.yaml) or remotely via Syncthing sync.
"""

import json
import os
import re
import shutil
import time
import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from urllib.request import Request, urlopen
from urllib.error import URLError

import pytest
import yaml

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

INTEGRATION_DIR = Path(__file__).parent
SYNCTHING_MODE = os.environ.get("SYNCTHING", "") == "1"

_fast_config = INTEGRATION_DIR / "test_config_fast.yaml"
CONFIG_FILE = (
    INTEGRATION_DIR / "test_config.yaml"
    if SYNCTHING_MODE or not _fast_config.exists()
    else _fast_config
)

# Character replacements mirroring ai.py CHAR_REPLACEMENTS
CHAR_REPLACEMENTS = {
    "ä": "ae", "ö": "oe", "ü": "ue",
    "Ä": "Ae", "Ö": "Oe", "Ü": "Ue",
    "ß": "ss", "æ": "ae", "œ": "oe",
    "ø": "o", "å": "a", "é": "e", "è": "e",
    "ê": "e", "ë": "e", "à": "a", "â": "a",
    "ù": "u", "û": "u", "ô": "o", "î": "i",
    "ï": "i", "ç": "c", "ñ": "n",
}


@dataclass
class TestConfig:
    """Resolved paths and polling settings for integration tests."""

    sync_folder: Path
    poll_interval: float = 5.0
    max_timeout: float = 15.0

    # Derived paths (set in __post_init__)
    incoming_dir: Path = field(init=False)
    processed_dir: Path = field(init=False)
    archive_dir: Path = field(init=False)
    reviewed_dir: Path = field(init=False)
    sorted_dir: Path = field(init=False)
    error_dir: Path = field(init=False)
    duplicates_dir: Path = field(init=False)
    reclassify_dir: Path = field(init=False)
    reset_dir: Path = field(init=False)
    trash_dir: Path = field(init=False)
    void_dir: Path = field(init=False)
    missing_dir: Path = field(init=False)
    history_dir: Path = field(init=False)
    costs_dir: Path = field(init=False)
    generated_dir: Path = field(init=False)

    def __post_init__(self):
        self.sync_folder = self.sync_folder.expanduser().resolve()
        self.incoming_dir = self.sync_folder / "incoming"
        self.processed_dir = self.sync_folder / "processed"
        self.archive_dir = self.sync_folder / "archive"
        self.reviewed_dir = self.sync_folder / "reviewed"
        self.sorted_dir = self.sync_folder / "sorted"
        self.error_dir = self.sync_folder / "error"
        self.duplicates_dir = self.sync_folder / "duplicates"
        self.reclassify_dir = self.sync_folder / "reclassify"
        self.reset_dir = self.sync_folder / "reset"
        self.trash_dir = self.sync_folder / "trash"
        self.void_dir = self.sync_folder / "void"
        self.missing_dir = self.sync_folder / "missing"
        self.history_dir = self.sync_folder / "history"
        self.costs_dir = INTEGRATION_DIR / "costs"
        self.generated_dir = INTEGRATION_DIR / "generated"


def _load_test_config() -> TestConfig:
    raw = yaml.safe_load(CONFIG_FILE.read_text())
    sync_path = Path(raw["sync_folder"])
    # Resolve relative paths against the integration test directory
    if not sync_path.is_absolute() and not str(sync_path).startswith("~"):
        sync_path = INTEGRATION_DIR / sync_path
    return TestConfig(
        sync_folder=sync_path,
        poll_interval=float(raw.get("poll_interval", 5)),
        max_timeout=float(raw.get("max_timeout", 15)),
    )


# ---------------------------------------------------------------------------
# Polling helpers
# ---------------------------------------------------------------------------


def poll_for_file(
    directory: Path,
    glob_pattern: str,
    interval: float,
    timeout: float,
    exclude_names: Optional[set[str]] = None,
) -> Optional[Path]:
    """Poll a flat directory for a file matching *glob_pattern*.

    Returns the first match or ``None`` if the timeout expires.
    Files whose name is in *exclude_names* are skipped.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        matches = [
            p for p in directory.glob(glob_pattern)
            if p.is_file() and (exclude_names is None or p.name not in exclude_names)
        ]
        if matches:
            return matches[0]
        time.sleep(interval)
    return None


def poll_for_file_recursive(
    directory: Path,
    glob_pattern: str,
    interval: float,
    timeout: float,
    exclude_paths: Optional[set[Path]] = None,
) -> Optional[Path]:
    """Poll recursively under *directory*, filtering out symlinks.

    Paths in *exclude_paths* are skipped (use to ignore pre-existing files).
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        matches = [
            p for p in directory.rglob(glob_pattern)
            if p.is_file() and not p.is_symlink()
            and (exclude_paths is None or p not in exclude_paths)
        ]
        if matches:
            return matches[0]
        time.sleep(interval)
    return None


def poll_until_gone(
    path: Path,
    interval: float,
    timeout: float,
) -> bool:
    """Poll until *path* no longer exists.  Returns True if gone, False on timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not path.exists():
            return True
        time.sleep(interval)
    return False


def poll_for_file_recursive_in(
    directory: Path,
    glob_pattern: str,
    interval: float,
    timeout: float,
) -> Optional[Path]:
    """Poll recursively under *directory* for any file matching pattern.

    Unlike ``poll_for_file_recursive``, does not exclude symlinks or pre-existing paths.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        matches = [p for p in directory.rglob(glob_pattern) if p.is_file()]
        if matches:
            return matches[0]
        time.sleep(interval)
    return None


def write_test_file(dest: Path, content: str) -> None:
    """Write a test file atomically (tmp + rename), triggering the watcher."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.rename(dest)


# ---------------------------------------------------------------------------
# Filename verification
# ---------------------------------------------------------------------------


def sanitize_like_ai(s: str) -> str:
    """Replicate the sanitisation from ai.py ``_sanitize``."""
    import unicodedata

    if not s:
        return ""
    for char, repl in CHAR_REPLACEMENTS.items():
        s = s.replace(char, repl)
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ASCII", "ignore").decode("ASCII")
    s = re.sub(r"\s+", "_", s)
    s = s.replace("-", "_")
    s = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", s)
    s = re.sub(r"_+", "_", s)
    s = s.strip("_")
    return s


def verify_filename_components(
    filename: str,
    expected_context: str,
    expected_date: str,
    expected_type: Optional[str] = None,
) -> None:
    """Assert that *filename* (stem) contains the expected components.

    Components are checked case-insensitively after sanitisation.
    """
    stem = Path(filename).stem.lower()
    ctx = sanitize_like_ai(expected_context).lower()
    date_str = expected_date  # dates are plain ASCII, no sanitisation needed

    assert ctx in stem, f"Context '{ctx}' not found in '{stem}'"
    assert date_str in stem, f"Date '{date_str}' not found in '{stem}'"

    if expected_type is not None:
        typ = sanitize_like_ai(expected_type).lower()
        assert typ in stem, f"Type '{typ}' not found in '{stem}'"


def verify_filename_keywords(filename: str, expected_keywords: list[str]) -> None:
    """Assert that filename contains all expected keywords (sanitized, case-insensitive)."""
    stem = Path(filename).stem.lower()
    for kw in expected_keywords:
        sanitized = sanitize_like_ai(kw).lower()
        assert sanitized in stem, f"Keyword '{sanitized}' not found in '{stem}'"


def verify_no_filename_keywords(filename: str, absent_keywords: list[str]) -> None:
    """Assert that filename does NOT contain any of the given keywords."""
    stem = Path(filename).stem.lower()
    for kw in absent_keywords:
        sanitized = sanitize_like_ai(kw).lower()
        assert sanitized not in stem, f"Unexpected keyword '{sanitized}' found in '{stem}'"


# ---------------------------------------------------------------------------
# Smart folder verification
# ---------------------------------------------------------------------------


def verify_smart_folder_symlink(
    leaf_dir: Path,
    smart_folder_name: str,
    filename: str,
) -> bool:
    """Return True if a symlink for *filename* exists inside *smart_folder_name*
    under *leaf_dir*."""
    sf_dir = leaf_dir / smart_folder_name
    if not sf_dir.is_dir():
        return False
    target = sf_dir / filename
    return target.is_symlink()


def poll_for_smart_folder_symlink(
    leaf_dir: Path,
    smart_folder_name: str,
    filename: str,
    interval: float = 2.0,
    timeout: float = 15.0,
) -> bool:
    """Poll for a smart folder symlink to appear (handles async creation delay)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if verify_smart_folder_symlink(leaf_dir, smart_folder_name, filename):
            return True
        time.sleep(interval)
    return False


# ---------------------------------------------------------------------------
# Audio link verification
# ---------------------------------------------------------------------------


def verify_audio_link_symlink(
    sorted_file: Path,
    audio_ext: str,
    archive_dir: Path,
) -> bool:
    """Return True if an audio link symlink exists next to *sorted_file*.

    The link should be named ``{sorted_file.stem}{audio_ext}`` and resolve
    to a file under *archive_dir*.
    """
    link_path = sorted_file.parent / (sorted_file.stem + audio_ext)
    if not link_path.is_symlink():
        return False
    resolved = link_path.resolve()
    try:
        resolved.relative_to(archive_dir.resolve())
        return resolved.exists()
    except ValueError:
        return False


def poll_until_symlink_gone(
    path: Path,
    interval: float,
    timeout: float,
) -> bool:
    """Poll until symlink at *path* no longer exists.  Returns True if gone."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not path.is_symlink():
            return True
        time.sleep(interval)
    return False


def poll_for_audio_link_symlink(
    sorted_file: Path,
    audio_ext: str,
    archive_dir: Path,
    interval: float = 2.0,
    timeout: float = 15.0,
) -> bool:
    """Poll for an audio link symlink to appear next to a transcript file."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if verify_audio_link_symlink(sorted_file, audio_ext, archive_dir):
            return True
        time.sleep(interval)
    return False


# ---------------------------------------------------------------------------
# Intro text verification (fuzzy)
# ---------------------------------------------------------------------------


def verify_intro_in_text(
    text_content: str,
    key_phrases: list[str],
    check_first_n_chars: int = 2000,
) -> bool:
    """Check that >= 60% of words in each *key_phrase* appear in the first
    *check_first_n_chars* of *text_content*.

    Returns True only if all key phrases pass the threshold.
    """
    region = text_content[:check_first_n_chars].lower()
    for phrase in key_phrases:
        words = phrase.lower().split()
        if not words:
            continue
        hits = sum(1 for w in words if w in region)
        if hits / len(words) < 0.60:
            return False
    return True


# ---------------------------------------------------------------------------
# Config deployment
# ---------------------------------------------------------------------------

CONFIG_DIR = INTEGRATION_DIR / "config"


def _deploy_configs(test_config: TestConfig) -> None:
    """Deploy sorted/{context}/ configs and root-level configs into the
    sync folder so the watcher discovers the test user."""
    if not CONFIG_DIR.is_dir():
        return
    test_config.sync_folder.mkdir(parents=True, exist_ok=True)
    # Deploy sorted/{context}/context.yaml and smartfolders.yaml
    sorted_config_dir = CONFIG_DIR / "sorted"
    if sorted_config_dir.is_dir():
        for ctx_dir in sorted_config_dir.iterdir():
            if ctx_dir.is_dir():
                dest_dir = test_config.sorted_dir / ctx_dir.name
                dest_dir.mkdir(parents=True, exist_ok=True)
                for src in ctx_dir.iterdir():
                    if src.suffix in (".yaml", ".yml"):
                        shutil.copy2(src, dest_dir / src.name)
    # Deploy root-level configs (config.yaml, stt.yaml)
    for src in CONFIG_DIR.iterdir():
        if src.is_file() and src.suffix in (".yaml", ".yml"):
            shutil.copy2(src, test_config.sync_folder / src.name)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def test_config() -> TestConfig:
    """Session-scoped test configuration."""
    return _load_test_config()


@pytest.fixture(scope="session")
def generated_dir(test_config: TestConfig) -> Path:
    """Path to the directory containing generated test documents/audio."""
    d = test_config.generated_dir
    assert d.is_dir(), (
        f"Generated data directory not found: {d}\n"
        "Run generate_documents.py and generate_audio.py first."
    )
    return d


def atomic_copy(src: Path, dest: Path) -> None:
    """Copy *src* to *dest* atomically (temp file + rename).

    This mirrors how Syncthing delivers files so that the watcher's
    ``on_moved`` handler fires with a complete file, avoiding the need
    for debounce delays.
    """
    tmp = dest.with_suffix(dest.suffix + ".tmp")
    shutil.copyfile(src, tmp)
    os.rename(str(tmp), str(dest))


def _clear_directory(d: Path) -> None:
    """Remove all files and subdirectories inside *d* (but keep *d* itself)."""
    if not d.exists():
        return
    for child in d.iterdir():
        if child.is_dir():
            shutil.rmtree(child, ignore_errors=True)
        else:
            try:
                child.unlink()
            except FileNotFoundError:
                pass


@pytest.fixture(scope="session", autouse=True)
def clean_all_dirs(test_config: TestConfig):
    """Clear all working directories once at session start."""
    for d in (
        test_config.incoming_dir,
        test_config.processed_dir,
        test_config.archive_dir,
        test_config.reviewed_dir,
        test_config.sorted_dir,
        test_config.error_dir,
        test_config.sync_folder / ".output",
        test_config.sync_folder / "reset",
        test_config.sync_folder / "void",
        test_config.sync_folder / "lost",
        test_config.sync_folder / "trash",
        test_config.sync_folder / "missing",
        test_config.costs_dir,
    ):
        _clear_directory(d)
        d.mkdir(parents=True, exist_ok=True)
    yield


@pytest.fixture()
def clean_working_dirs(test_config: TestConfig):
    """Clear transient directories before a test (on explicit request).

    Does NOT clear ``sorted/`` — that accumulates across tests.
    """
    for d in (
        test_config.incoming_dir,
        test_config.processed_dir,
        test_config.archive_dir,
        test_config.reviewed_dir,
        test_config.reset_dir,
    ):
        _clear_directory(d)
        d.mkdir(parents=True, exist_ok=True)
    yield


@pytest.fixture()
def clean_sorted(test_config: TestConfig):
    """Explicitly clear ``sorted/`` — use in class-level setup."""
    _clear_directory(test_config.sorted_dir)
    test_config.sorted_dir.mkdir(parents=True, exist_ok=True)


def _reset_db() -> None:
    """Truncate the documents table so every test starts with a clean DB."""
    import subprocess
    subprocess.run(
        [
            "docker", "exec", "integration-mrdocument-db-1",
            "psql", "-U", "mrdocument", "-d", "mrdocument",
            "-t", "-A", "-c", "TRUNCATE TABLE mrdocument.documents_v2",
        ],
        check=True, capture_output=True, text=True, timeout=10,
    )


@pytest.fixture(autouse=True)
def reset_environment(test_config: TestConfig):
    """Reset DB and filesystem before each test case.

    1. Truncate the documents table.
    2. Clear and recreate all working directories.
    3. Re-deploy config files into testdata.

    This is autouse so every test starts from a known-clean state without
    having to restart containers.
    """
    _reset_db()
    for d in (
        test_config.incoming_dir,
        test_config.processed_dir,
        test_config.archive_dir,
        test_config.reviewed_dir,
        test_config.sorted_dir,
        test_config.error_dir,
        test_config.duplicates_dir,
        test_config.reclassify_dir,
        test_config.reset_dir,
        test_config.trash_dir,
        test_config.void_dir,
        test_config.missing_dir,
        test_config.history_dir,
        test_config.sync_folder / ".output",
        test_config.sync_folder / "lost",
        test_config.costs_dir,
    ):
        _clear_directory(d)
        d.mkdir(parents=True, exist_ok=True)
    _deploy_configs(test_config)
    yield


# ---------------------------------------------------------------------------
# Service readiness
# ---------------------------------------------------------------------------

SERVICE_HEALTH_URL = "http://localhost:8000/health"
WATCHER_HEALTH_URL = "http://localhost:8080/health"
SERVICE_READY_TIMEOUT = 60  # seconds


@pytest.fixture(scope="session", autouse=True)
def deploy_config(test_config: TestConfig, clean_all_dirs) -> None:
    """Deploy configs once per session (initial setup).

    Depends on *clean_all_dirs* so that the sorted/ directory already exists.
    Per-test config deployment is handled by *reset_environment*.
    """
    _deploy_configs(test_config)


@pytest.fixture(scope="session", autouse=True)
def ensure_service_ready() -> None:
    """Wait for the MrDocument service to become healthy.

    Polls ``/health`` for up to *SERVICE_READY_TIMEOUT* seconds.
    If the service is unreachable the entire test session is skipped
    (so ``--collect-only`` works even without a running container).
    """
    deadline = time.monotonic() + SERVICE_READY_TIMEOUT
    last_err: Optional[Exception] = None
    while time.monotonic() < deadline:
        try:
            with urlopen(SERVICE_HEALTH_URL, timeout=3) as resp:
                if resp.status == 200:
                    return
        except (URLError, OSError) as exc:
            last_err = exc
        time.sleep(2)
    pytest.skip(
        f"MrDocument service not reachable at {SERVICE_HEALTH_URL} "
        f"after {SERVICE_READY_TIMEOUT}s (last error: {last_err})"
    )


def restart_watcher(timeout: float = 15) -> None:
    """Restart the watcher Docker container and wait for it to become healthy.

    Uses ``docker restart`` to stop and start the container, then polls
    the health endpoint until it responds 200 or the timeout expires.
    """
    import subprocess
    subprocess.run(
        ["docker", "restart", "integration-mrdocument-watcher-1"],
        check=True, capture_output=True, timeout=15,
    )
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urlopen(WATCHER_HEALTH_URL, timeout=3) as resp:
                if resp.status == 200:
                    return
        except (URLError, OSError):
            pass
        time.sleep(1)
    raise TimeoutError(
        f"Watcher not healthy at {WATCHER_HEALTH_URL} after {timeout}s"
    )


def watcher_logs(since: str = "5m") -> str:
    """Fetch recent watcher container logs.

    *since* is a Docker duration string (e.g. ``"5m"``, ``"30s"``).
    """
    import subprocess
    result = subprocess.run(
        [
            "docker", "logs", "--since", since,
            "integration-mrdocument-watcher-1",
        ],
        capture_output=True, text=True, timeout=10,
    )
    # Docker writes logs to stderr
    return result.stdout + result.stderr


def db_exec(sql: str) -> str:
    """Execute a SQL statement against the integration test database.

    Returns the stdout output from ``psql`` running inside the DB container.
    """
    import subprocess
    result = subprocess.run(
        [
            "docker", "exec", "integration-mrdocument-db-1",
            "psql", "-U", "mrdocument", "-d", "mrdocument",
            "-t", "-A", "-c", sql,
        ],
        check=True, capture_output=True, text=True, timeout=10,
    )
    return result.stdout.strip()


# ---------------------------------------------------------------------------
# Syncthing sync readiness
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# YAML Fixture Collection
# ---------------------------------------------------------------------------

FIXTURE_TESTS_DIR = INTEGRATION_DIR / "fixture_tests"


def pytest_collect_file(parent, file_path):
    """Collect YAML fixture files as pytest test items."""
    if (
        file_path.suffix == ".yaml"
        and file_path.parent == FIXTURE_TESTS_DIR
    ):
        return YamlFixtureFile.from_parent(parent, path=file_path)


class YamlFixtureFile(pytest.File):
    """Collector for a single YAML fixture file."""

    def collect(self):
        yaml_path = self.path

        def run_fixture(test_config):
            from fixtures.loader import load_fixture
            from fixtures.runner import FixtureRunner

            fixture = load_fixture(yaml_path, INTEGRATION_DIR)
            runner = FixtureRunner(
                test_config.sync_folder, fixture, test_config.poll_interval,
            )
            runner.run()

        run_fixture.__name__ = f"test_{self.path.stem}"
        run_fixture.__qualname__ = f"test_{self.path.stem}"
        run_fixture.__module__ = __name__
        yield pytest.Function.from_parent(
            self, name=self.path.stem, callobj=run_fixture,
        )


SYNCTHING_API_URL = "http://localhost:22384"
SYNCTHING_API_KEY = "test-api-key-syncthing"
SYNCTHING_FOLDER_ID = "mrdocument-testuser"
SYNCTHING_SYNC_TIMEOUT = 90  # seconds


@pytest.fixture(scope="session", autouse=True)
def ensure_syncthing_synced(deploy_config) -> None:
    """Wait for Syncthing to reach 100% sync completion.

    Polls the server's REST API for folder completion status.
    After sync completes, waits briefly for the watcher to discover
    the user folder via ``NewUserFolderHandler``.

    Skipped entirely when running in fast mode (no Syncthing).

    On timeout, issues a warning but does not fail — the Makefile
    sync-wait should have already ensured sync, and individual tests
    have their own poll timeouts.
    """
    if not SYNCTHING_MODE:
        return
    url = (
        f"{SYNCTHING_API_URL}/rest/db/completion"
        f"?folder={SYNCTHING_FOLDER_ID}"
    )
    deadline = time.monotonic() + SYNCTHING_SYNC_TIMEOUT
    while time.monotonic() < deadline:
        try:
            req = Request(url, headers={"X-API-Key": SYNCTHING_API_KEY})
            with urlopen(req, timeout=3) as resp:
                data = json.loads(resp.read())
                if int(data.get("completion", 0)) == 100:
                    # Give watcher time to discover user folder
                    time.sleep(3)
                    return
        except (URLError, OSError, json.JSONDecodeError, ValueError):
            pass
        time.sleep(2)
    warnings.warn(
        f"Syncthing sync not 100% after {SYNCTHING_SYNC_TIMEOUT}s — "
        "tests may need to wait for file propagation"
    )
