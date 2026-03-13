"""YAML fixture framework for MrDocument integration tests."""

from .loader import load_fixture, FixtureSpec, StepSpec, FileRef, CopyFromTreeAction
from .runner import FixtureRunner
from .scanner import scan_tree, match_tree, TreeMatchResult

__all__ = [
    "load_fixture",
    "FixtureSpec",
    "StepSpec",
    "FileRef",
    "CopyFromTreeAction",
    "FixtureRunner",
    "scan_tree",
    "match_tree",
    "TreeMatchResult",
]
