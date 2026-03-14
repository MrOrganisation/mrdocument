"""Filesystem tree scanning and regex matching."""

import re
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class TreeMatchResult:
    matched: dict[str, list[str]] = field(default_factory=dict)
    unmatched_patterns: list[str] = field(default_factory=list)
    extra_files: list[str] = field(default_factory=list)

    def is_perfect_match(self) -> bool:
        return not self.unmatched_patterns and not self.extra_files


def scan_tree(sync_folder: Path) -> set[str]:
    """List all regular files as relative paths, excluding configs and hidden files."""
    result = set()
    for p in sync_folder.rglob("*"):
        if not p.is_file() or p.is_symlink():
            continue
        rel = p.relative_to(sync_folder)
        parts = rel.parts
        # Skip hidden files/dirs
        if any(part.startswith(".") for part in parts):
            continue
        # Skip YAML config files
        if p.suffix in (".yaml", ".yml"):
            continue
        result.add(str(rel))
    return result


def match_tree(actual_files: set[str], expected_patterns: list[str]) -> TreeMatchResult:
    """Regex matching of expected patterns against actual files.

    Each required pattern must match >= 1 file. Each file must match >= 1 pattern.
    Patterns prefixed with ``~`` are optional: they absorb matching files but
    do not fail when nothing matches.
    """
    matched: dict[str, list[str]] = {}
    matched_files: set[str] = set()
    optional: set[str] = set()

    for raw in expected_patterns:
        if raw.startswith("~"):
            pattern = raw[1:]
            optional.add(raw)
        else:
            pattern = raw
        hits = []
        for f in actual_files:
            if re.fullmatch(pattern, f):
                hits.append(f)
                matched_files.add(f)
        if hits:
            matched[raw] = hits

    unmatched_patterns = [
        p for p in expected_patterns
        if p not in matched and p not in optional
    ]
    extra_files = sorted(f for f in actual_files if f not in matched_files)

    return TreeMatchResult(
        matched=matched,
        unmatched_patterns=unmatched_patterns,
        extra_files=extra_files,
    )
