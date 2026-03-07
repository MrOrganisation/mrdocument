"""
Data models for the document watcher v2.

Defines State machine, change tracking types, and the Record dataclass
that represents a document throughout its lifecycle.
"""

from datetime import datetime
from enum import Enum
from pathlib import PurePosixPath
from typing import NamedTuple, Optional
from uuid import UUID, uuid4
from dataclasses import dataclass, field


class State(str, Enum):
    IS_NEW = "is_new"
    NEEDS_PROCESSING = "needs_processing"
    IS_MISSING = "is_missing"
    HAS_ERROR = "has_error"
    NEEDS_DELETION = "needs_deletion"
    IS_DELETED = "is_deleted"
    IS_COMPLETE = "is_complete"


class EventType(str, Enum):
    ADDITION = "addition"
    REMOVAL = "removal"


class PathEntry(NamedTuple):
    path: str
    timestamp: datetime


@dataclass
class ChangeItem:
    event_type: EventType
    path: str
    hash: Optional[str] = None
    size: Optional[int] = None


@dataclass
class Record:
    # Identity
    original_filename: str
    source_hash: str
    id: UUID = field(default_factory=uuid4)

    # Paths
    source_paths: list[PathEntry] = field(default_factory=list)
    current_paths: list[PathEntry] = field(default_factory=list)
    missing_source_paths: list[PathEntry] = field(default_factory=list)
    missing_current_paths: list[PathEntry] = field(default_factory=list)

    # Content
    context: Optional[str] = None
    metadata: Optional[dict] = None
    assigned_filename: Optional[str] = None
    hash: Optional[str] = None

    # Processing
    output_filename: Optional[str] = None
    state: State = State.IS_NEW

    # Temp fields
    target_path: Optional[str] = None
    source_reference: Optional[str] = None
    current_reference: Optional[str] = None
    duplicate_sources: list[str] = field(default_factory=list)
    deleted_paths: list[str] = field(default_factory=list)

    # Owner
    username: Optional[str] = None

    @property
    def source_file(self) -> Optional[PathEntry]:
        """Most recent source PathEntry by timestamp, or None."""
        if not self.source_paths:
            return None
        return max(self.source_paths, key=lambda e: e.timestamp)

    @property
    def current_file(self) -> Optional[PathEntry]:
        """Most recent current PathEntry by timestamp, or None."""
        if not self.current_paths:
            return None
        return max(self.current_paths, key=lambda e: e.timestamp)

    @staticmethod
    def _decompose_path(path: str) -> tuple[str, str, str]:
        """Decompose a path into (location, location_path, filename).

        Examples:
            "archive/sub/file.pdf" -> ("archive", "sub", "file.pdf")
            "archive/file.pdf" -> ("archive", "", "file.pdf")
            ".output/uuid" -> (".output", "", "uuid")
        """
        p = PurePosixPath(path)
        parts = p.parts
        location = parts[0]
        filename = parts[-1]
        if len(parts) > 2:
            location_path = str(PurePosixPath(*parts[1:-1]))
        else:
            location_path = ""
        return location, location_path, filename

    @property
    def source_location(self) -> Optional[str]:
        sf = self.source_file
        if sf is None:
            return None
        return self._decompose_path(sf.path)[0]

    @property
    def source_location_path(self) -> Optional[str]:
        sf = self.source_file
        if sf is None:
            return None
        return self._decompose_path(sf.path)[1]

    @property
    def source_filename(self) -> Optional[str]:
        sf = self.source_file
        if sf is None:
            return None
        return self._decompose_path(sf.path)[2]

    @property
    def current_location(self) -> Optional[str]:
        cf = self.current_file
        if cf is None:
            return None
        return self._decompose_path(cf.path)[0]

    @property
    def current_location_path(self) -> Optional[str]:
        cf = self.current_file
        if cf is None:
            return None
        return self._decompose_path(cf.path)[1]

    @property
    def current_filename(self) -> Optional[str]:
        cf = self.current_file
        if cf is None:
            return None
        return self._decompose_path(cf.path)[2]

    def clear_temporary_fields(self) -> None:
        """Reset all temporary fields to their defaults."""
        self.target_path = None
        self.source_reference = None
        self.current_reference = None
        self.duplicate_sources = []
        self.deleted_paths = []
