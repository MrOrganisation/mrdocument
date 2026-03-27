"""YAML fixture parsing and validation."""

import re
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Union

import yaml


@dataclass
class FileRef:
    filename: str
    source_path: Path | None = None
    content: str | None = None


@dataclass
class CopyAction:
    dest_path: str
    filename: str


@dataclass
class MoveAction:
    pattern: str
    to: str


@dataclass
class DeleteAction:
    pattern: str


@dataclass
class CopyFromTreeAction:
    pattern: str
    to: str


@dataclass
class MkdirAction:
    path: str


@dataclass
class SleepAction:
    seconds: float


@dataclass
class StopWatcherAction:
    pass


@dataclass
class StartWatcherAction:
    pass


InputAction = Union[CopyAction, MoveAction, DeleteAction, CopyFromTreeAction, MkdirAction, SleepAction, StopWatcherAction, StartWatcherAction]


@dataclass
class StepSpec:
    inputs: list[InputAction]
    expected: list[str]
    timeout: float


@dataclass
class FixtureSpec:
    name: str
    contexts: list[str]
    files: dict[str, FileRef]
    default_timeout: float
    steps: list[StepSpec]


def _parse_timeout(value: str | int | float) -> float:
    """Parse a timeout string like '10s', '2m' or a numeric value to seconds."""
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip().lower()
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*(s|m)", s)
    if m:
        num = float(m.group(1))
        unit = m.group(2)
        return num * 60 if unit == "m" else num
    return float(s)


def _expand_templates(s: str) -> str:
    """Expand template variables in a string."""
    today = date.today().isoformat()
    return s.replace("{CURRENT_DATE}", today)


def _parse_input(raw, files: dict[str, FileRef]) -> InputAction:
    """Parse a single input action from YAML."""
    if isinstance(raw, str):
        filename = Path(raw).name
        if filename not in files:
            raise ValueError(
                f"Copy action references unknown file '{filename}'. "
                f"Known files: {list(files.keys())}"
            )
        return CopyAction(dest_path=raw, filename=filename)
    if isinstance(raw, dict):
        if "move" in raw:
            return MoveAction(pattern=raw["move"], to=raw["to"])
        if "delete" in raw:
            return DeleteAction(pattern=raw["delete"])
        if "copy_match" in raw:
            return CopyFromTreeAction(pattern=raw["copy_match"], to=raw["to"])
        if "mkdir" in raw:
            return MkdirAction(path=raw["mkdir"])
        if "sleep" in raw:
            return SleepAction(seconds=float(raw["sleep"]))
    if raw == "stop_watcher":
        return StopWatcherAction()
    if raw == "start_watcher":
        return StartWatcherAction()
    raise ValueError(f"Unknown input action format: {raw}")


def load_fixture(path: Path, integration_dir: Path) -> FixtureSpec:
    """Load and validate a YAML fixture file."""
    raw = yaml.safe_load(path.read_text())

    # Parse files section
    files: dict[str, FileRef] = {}
    for f in raw.get("files", []):
        fn = f["filename"]
        source_path = None
        content = None
        if "path_of_generated_file" in f:
            p = Path(f["path_of_generated_file"])
            if not p.is_absolute():
                p = integration_dir / p
            source_path = p
        if "content" in f:
            content = f["content"]
        files[fn] = FileRef(filename=fn, source_path=source_path, content=content)

    default_timeout = _parse_timeout(raw.get("timeout", "10s"))

    # Parse steps
    steps: list[StepSpec] = []
    for step_raw in raw.get("steps", []):
        inputs = [_parse_input(i, files) for i in step_raw.get("input", [])]
        expected = [_expand_templates(e) for e in step_raw.get("expected", [])]
        timeout = _parse_timeout(step_raw.get("timeout", default_timeout))
        steps.append(StepSpec(inputs=inputs, expected=expected, timeout=timeout))

    return FixtureSpec(
        name=path.stem,
        contexts=raw.get("contexts", []),
        files=files,
        default_timeout=default_timeout,
        steps=steps,
    )
