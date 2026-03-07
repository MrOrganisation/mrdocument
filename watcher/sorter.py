#!/usr/bin/env python3
"""
File sorter that watches reviewed folders and sorts files into nested folder structures.

For each user:
  /sync/{username}/reviewed/  - Files to be sorted (after human review)
  /sync/{username}/sorted/    - Destination with nested folder structure

Filename format: {context}-{field1}-{field2}-...{fieldN}.ext
  - First component (before first `-`) is the context name
  - Remaining components are field values according to the context's filename pattern
  - Dates (YYYY-MM-DD) are treated as single components despite containing `-`

Each context can define a `folders` field specifying which fields determine the folder structure.
"""

import asyncio
import base64
import json
import logging
import os
import re
import sys
import time
import unicodedata
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional, TYPE_CHECKING

import aiohttp
import yaml
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

if TYPE_CHECKING:
    from db import DocumentDB

# Configure logging
log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# =============================================================================
# Path Utilities
# =============================================================================


def get_username_from_root(user_root: Path, sync_root: str = "/sync") -> str:
    """
    Get the username (directory under sync root) from a user root path.
    
    For path /sync/heike/mrdocument, returns "heike".
    For path /sync/alice, returns "alice".
    
    Args:
        user_root: The user's config root directory
        sync_root: The root sync directory (default: /sync)
    
    Returns:
        The username (directory name directly under sync_root)
    """
    sync_root_path = Path(sync_root).resolve()
    user_root_resolved = user_root.resolve()
    
    # Walk up the path to find the directory directly under sync_root
    for parent in [user_root_resolved] + list(user_root_resolved.parents):
        if parent.parent == sync_root_path:
            return parent.name
    
    # Fallback: just use the user_root name
    return user_root.name


def _find_ci(directory: Path, filename: str) -> Optional[Path]:
    """Find *filename* inside *directory* using case-insensitive matching.

    Returns the first match or ``None``.  When multiple case variants
    exist, the choice is arbitrary.
    """
    target = filename.lower()
    try:
        for child in directory.iterdir():
            if child.name.lower() == target:
                return child
    except OSError:
        pass
    return None


# =============================================================================
# Configuration
# =============================================================================


@dataclass
class UserConfig:
    """User-specific configuration."""

    enabled: bool = True
    reviewed_folder: str = "reviewed"
    sorted_folder: str = "sorted"
    processed_folder: str = "processed"
    migration: bool = False
    ignore_files: list[str] = None

    def __post_init__(self):
        if self.ignore_files is None:
            self.ignore_files = []
        self._compiled_ignore: list[re.Pattern] = []
        for pattern in self.ignore_files:
            try:
                self._compiled_ignore.append(re.compile(pattern))
            except re.error as e:
                logger.warning("Invalid ignore_files pattern '%s': %s", pattern, e)

    def should_ignore_file(self, filename: str) -> bool:
        """Check if a filename matches any ignore_files pattern."""
        return any(pat.search(filename) for pat in self._compiled_ignore)

    @classmethod
    def from_dict(cls, data: Optional[dict[str, Any]]) -> "UserConfig":
        if data is None:
            return cls()
        return cls(
            enabled=bool(data.get("enabled", True)),
            reviewed_folder=data.get("reviewed_folder", cls.reviewed_folder),
            sorted_folder=data.get("sorted_folder", cls.sorted_folder),
            processed_folder=data.get("processed_folder", cls.processed_folder),
            migration=bool(data.get("migration", False)),
            ignore_files=data.get("ignore_files", []) or [],
        )


# =============================================================================
# Smart Folder Conditions
# =============================================================================


@dataclass
class SmartFolderCondition:
    """
    A condition for smart folder matching.
    
    Can be either:
    - A statement: field + value (case-insensitive regex match)
    - An operator: and/or/not with operands
    """
    
    # For statements
    field: Optional[str] = None
    value: Optional[str] = None
    _compiled_regex: Optional[re.Pattern] = None
    
    # For operators
    operator: Optional[str] = None  # "and", "or", "not"
    operands: Optional[list["SmartFolderCondition"]] = None
    
    def __post_init__(self):
        """Compile regex pattern."""
        if self.value is not None and self._compiled_regex is None:
            try:
                # Case-insensitive regex matching
                self._compiled_regex = re.compile(self.value, re.IGNORECASE)
            except re.error as e:
                logger.warning("Invalid regex pattern '%s': %s", self.value, e)
                self._compiled_regex = None
    
    def is_statement(self) -> bool:
        return self.field is not None and self.value is not None
    
    def is_operator(self) -> bool:
        return self.operator is not None
    
    def evaluate(self, fields: dict[str, str]) -> bool:
        """
        Evaluate the condition against parsed filename fields.
        
        Args:
            fields: Dict mapping field names to values from parsed filename
            
        Returns:
            True if condition matches, False otherwise
        """
        if self.is_statement():
            field_value = fields.get(self.field, "")
            
            # Regex matching (case-insensitive, full match)
            if self._compiled_regex is None:
                # Regex failed to compile
                return False
            match = self._compiled_regex.fullmatch(field_value)
            return match is not None
        
        if self.is_operator():
            if self.operator == "not":
                if not self.operands or len(self.operands) != 1:
                    logger.warning("'not' operator requires exactly one operand")
                    return False
                return not self.operands[0].evaluate(fields)
            
            elif self.operator == "and":
                if not self.operands:
                    return True  # Empty AND is true
                return all(op.evaluate(fields) for op in self.operands)
            
            elif self.operator == "or":
                if not self.operands:
                    return False  # Empty OR is false
                return any(op.evaluate(fields) for op in self.operands)
            
            else:
                logger.warning("Unknown operator: %s", self.operator)
                return False
        
        logger.warning("Invalid condition: neither statement nor operator")
        return False
    
    @classmethod
    def from_dict(cls, data: dict[str, Any], context_name: str = "", sf_name: str = "") -> Optional["SmartFolderCondition"]:
        """Parse a condition from YAML data."""
        if not isinstance(data, dict):
            logger.warning(
                "Condition must be a dict, got: %s (context '%s', smart folder '%s')",
                type(data), context_name, sf_name,
            )
            return None

        # Check if it's a statement (has field and value)
        if "field" in data and "value" in data:
            condition = cls(
                field=str(data["field"]),
                value=str(data["value"]),
            )
            # Trigger __post_init__ to compile regex
            condition.__post_init__()
            return condition

        # Check if it's an operator
        if "operator" in data:
            operator = str(data["operator"]).lower()
            if operator not in ("and", "or", "not"):
                logger.warning(
                    "Unknown operator: %s (context '%s', smart folder '%s')",
                    operator, context_name, sf_name,
                )
                return None

            operands_data = data.get("operands", [])

            # Handle single operand (for "not" or convenience)
            if isinstance(operands_data, dict):
                operands_data = [operands_data]

            if not isinstance(operands_data, list):
                logger.warning(
                    "Operands must be a list or dict, got: %s (context '%s', smart folder '%s')",
                    type(operands_data), context_name, sf_name,
                )
                return None

            operands = []
            for op_data in operands_data:
                op = cls.from_dict(op_data, context_name=context_name, sf_name=sf_name)
                if op:
                    operands.append(op)

            return cls(
                operator=operator,
                operands=operands,
            )

        logger.warning(
            "Condition must have either (field, value) or (operator, operands) "
            "(context '%s', smart folder '%s'): %s",
            context_name, sf_name, data,
        )
        return None


@dataclass
class SmartFolderConfig:
    """Configuration for a smart folder."""
    
    name: str
    condition: Optional[SmartFolderCondition] = None
    filename_regex: Optional[str] = None
    _compiled_filename_regex: Optional[re.Pattern] = None
    
    def __post_init__(self):
        """Compile filename regex pattern."""
        if self.filename_regex is not None and self._compiled_filename_regex is None:
            try:
                # Case-insensitive regex matching for filenames
                self._compiled_filename_regex = re.compile(self.filename_regex, re.IGNORECASE)
            except re.error as e:
                logger.warning(
                    "Invalid filename_regex pattern '%s' for smart folder '%s': %s",
                    self.filename_regex, self.name, e
                )
                self._compiled_filename_regex = None
    
    def matches_filename(self, filename: str) -> bool:
        """
        Check if filename matches the filename_regex filter.
        
        Returns True if:
        - No filename_regex is configured (all files pass)
        - The filename matches the regex pattern (search, not full match)
        
        Returns False if:
        - filename_regex is configured but failed to compile
        - The filename doesn't match the regex pattern
        """
        if self.filename_regex is None:
            return True  # No filter configured, all files pass
        
        if self._compiled_filename_regex is None:
            return False  # Regex failed to compile
        
        # Use search() to find pattern anywhere in filename
        return self._compiled_filename_regex.search(filename) is not None
    
    @classmethod
    def from_dict(cls, name: str, data: dict[str, Any], context_name: str = "") -> Optional["SmartFolderConfig"]:
        """Parse smart folder config from YAML data."""
        if not isinstance(data, dict):
            logger.warning("Smart folder config must be a dict")
            return None

        # Parse optional filename_regex
        filename_regex = data.get("filename_regex")
        if filename_regex is not None:
            filename_regex = str(filename_regex)

        # Parse optional condition
        condition = None
        condition_data = data.get("condition")
        if condition_data:
            condition = SmartFolderCondition.from_dict(condition_data, context_name=context_name, sf_name=name)
            if not condition:
                logger.warning("Failed to parse condition for smart folder '%s'", name)
                return None

        # At least one of condition or filename_regex must be present
        if condition is None and filename_regex is None:
            logger.warning("Smart folder '%s' must have 'condition' and/or 'filename_regex'", name)
            return None

        config = cls(name=name, condition=condition, filename_regex=filename_regex)
        # Trigger __post_init__ to compile regex
        config.__post_init__()
        return config


@dataclass
class FilenameRule:
    """A conditional filename pattern rule."""
    pattern: str
    match: Optional[str] = None  # regex; None = default/fallback


@dataclass
class ContextConfig:
    """Context configuration for sorting."""

    name: str
    filename_pattern: str
    folders: list[str]
    smart_folders: dict[str, SmartFolderConfig]
    field_names: list[str]
    filename_rules: list[FilenameRule] = field(default_factory=list)

    def resolve_filename_pattern(self, source_filename: Optional[str] = None) -> str:
        """Pick the first matching conditional rule, or fall back to default."""
        if not self.filename_rules or not source_filename:
            return self.filename_pattern
        for rule in self.filename_rules:
            if rule.match and re.search(rule.match, source_filename):
                return rule.pattern
        return self.filename_pattern

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Optional["ContextConfig"]:
        """Parse context from YAML data."""
        name = data.get("name")
        filename = data.get("filename")

        if not name or not filename:
            return None

        # Parse conditional filename rules
        filename_rules: list[FilenameRule] = []
        if isinstance(filename, list):
            default_pattern = None
            for entry in filename:
                if isinstance(entry, dict) and "pattern" in entry:
                    rule = FilenameRule(
                        pattern=entry["pattern"],
                        match=entry.get("match"),
                    )
                    filename_rules.append(rule)
                    if rule.match is None:
                        default_pattern = rule.pattern
            if not default_pattern:
                logger.error(
                    "Context '%s': filename is a conditional list with no default pattern "
                    "(add an entry without 'match')", name,
                )
                return None
            filename = default_pattern

        folders = data.get("folders", [])
        if not isinstance(folders, list):
            folders = []

        # Parse smart folders
        smart_folders: dict[str, SmartFolderConfig] = {}
        smart_folders_data = data.get("smart_folders", {})
        if isinstance(smart_folders_data, dict):
            for sf_name, sf_data in smart_folders_data.items():
                sf_config = SmartFolderConfig.from_dict(sf_name, sf_data, context_name=name)
                if sf_config:
                    smart_folders[sf_name] = sf_config

        # Build field_names: implicit fields + explicit fields from config
        field_names = ["context", "date"]
        fields_data = data.get("fields", {})
        if isinstance(fields_data, dict):
            for field_name in fields_data:
                if field_name not in field_names:
                    field_names.append(str(field_name))

        return cls(
            name=name,
            filename_pattern=filename,
            folders=[str(f) for f in folders if f],
            smart_folders=smart_folders,
            field_names=field_names,
            filename_rules=filename_rules,
        )


@dataclass 
class WatcherConfig:
    """Global watcher configuration."""

    watch_patterns: list[str]
    debounce_seconds: float = 15.0
    full_scan_seconds: float = 300.0

    @classmethod
    def load(cls, config_path: Path) -> "WatcherConfig":
        """Load watcher configuration from YAML file."""
        if not config_path.exists():
            logger.info("No watcher config at %s, using defaults", config_path)
            return cls(watch_patterns=["/sync/*"])

        try:
            with open(config_path) as f:
                data = yaml.safe_load(f)
            if not data:
                return cls(watch_patterns=["/sync/*"])
            patterns = data.get("watch_patterns", ["/sync/*"])
            debounce = float(data.get("debounce_seconds", 15.0))
            full_scan = float(data.get("full_scan_seconds", 300.0))
            return cls(
                watch_patterns=patterns,
                debounce_seconds=debounce,
                full_scan_seconds=full_scan,
            )
        except Exception as e:
            logger.warning("Failed to load watcher config from %s: %s", config_path, e)
            return cls(watch_patterns=["/sync/*"])

    def get_watch_directories(self) -> list[Path]:
        """Expand patterns to get directories."""
        import glob as glob_module
        
        directories: set[Path] = set()
        for pattern in self.watch_patterns:
            if "*" in pattern:
                matches = glob_module.glob(pattern)
                for match in matches:
                    path = Path(match)
                    if path.is_dir() and not path.name.startswith("."):
                        directories.add(path)
            else:
                path = Path(pattern)
                if path.exists() and path.is_dir():
                    directories.add(path)
        
        return sorted(directories)


# =============================================================================
# DB-based Metadata Lookup
# =============================================================================


async def _lookup_metadata_from_db(
    db: "DocumentDB",
    file_path: Path,
    username: str,
) -> Optional[dict[str, Any]]:
    """
    Look up document metadata from the database.

    Tries current_file_path first, then falls back to hash-based lookup.

    Returns the full DB record dict, or None if not found.
    """
    from db import compute_sha256

    record = await db.get_by_path(str(file_path), username)
    if record:
        return record

    # Fallback: hash-based lookup
    try:
        file_hash = compute_sha256(file_path)
        record = await db.get_by_hash(file_hash, username)
        if record:
            # Reconnect the path while we're at it
            await db.update_file_path(record["id"], str(file_path), new_hash=file_hash)
            return record
    except Exception as e:
        logger.warning("[%s] Hash-based lookup failed for %s: %s", username, file_path.name, e)

    return None


async def _check_previous_file_exists(
    db: "DocumentDB",
    record: dict[str, Any],
) -> bool:
    """
    Check if a previously-processed file is still physically available.

    For documents: checks that at least one file_location still exists on disk.
    For audio transcripts: the DB entry tracks the TXT output, so we check the
    same file_locations (which point to the TXT file).

    Returns True if at least one location is still physically present.
    """
    doc_id = record["id"]
    locations = await db.get_locations_for_document(doc_id)

    for loc in locations:
        loc_path = Path(loc["file_path"])
        if loc_path.exists():
            return True

    # Also check current_file_path as a fallback
    current_path = record.get("current_file_path")
    if current_path and Path(current_path).exists():
        return True

    return False


def _move_file_to_duplicates(
    file_path: Path,
    user_root: Path,
    duplicates_folder: str,
    username: str,
) -> Optional[Path]:
    """
    Move a file to the duplicates folder.

    Returns the destination path, or None on failure.
    """
    duplicates_dir = user_root / duplicates_folder
    duplicates_dir.mkdir(parents=True, exist_ok=True)

    dup_path = duplicates_dir / file_path.name
    if dup_path.exists():
        unique_id = uuid.uuid4().hex[:8]
        stem = file_path.stem
        suffix = file_path.suffix
        dup_path = duplicates_dir / f"{stem}_{unique_id}{suffix}"

    try:
        file_path.rename(dup_path)
        logger.info("[%s] Moved to duplicates: %s -> %s", username, file_path.name, dup_path.name)
        return dup_path
    except Exception as e:
        logger.error("[%s] Failed to move %s to duplicates: %s", username, file_path.name, e)
        return None


def _move_file_to_unprocessed(
    file_path: Path, user_root: Path, username: str,
) -> Optional[Path]:
    """
    Move a file to the unprocessed folder.

    Returns the destination path, or None on failure.
    """
    unprocessed_dir = user_root / "unprocessed"
    unprocessed_dir.mkdir(parents=True, exist_ok=True)

    target = unprocessed_dir / file_path.name
    if target.exists():
        unique_id = uuid.uuid4().hex[:8]
        target = unprocessed_dir / f"{file_path.stem}_{unique_id}{file_path.suffix}"

    try:
        file_path.rename(target)
        logger.info("[%s] Moved to unprocessed: %s -> %s", username, file_path.name, target.name)
        return target
    except Exception as e:
        logger.error("[%s] Failed to move %s to unprocessed: %s", username, file_path.name, e)
        return None


def _move_to_void(file_path: Path, user_root: Path, username: str) -> Optional[Path]:
    """
    Move a file to the void folder under a date subfolder (YYYY-MM-DD).
    Never overwrites — appends UUID on collision.

    Returns the destination path, or None on failure.
    """
    from datetime import date

    void_dir = user_root / "void" / date.today().isoformat()
    void_dir.mkdir(parents=True, exist_ok=True)

    dest = void_dir / file_path.name
    if dest.exists():
        unique_id = uuid.uuid4().hex[:8]
        dest = void_dir / f"{file_path.stem}-{unique_id}{file_path.suffix}"

    try:
        file_path.rename(dest)
        logger.info("[%s] Moved to void: %s -> %s", username, file_path.name, dest.name)
        return dest
    except OSError:
        try:
            import shutil
            shutil.move(str(file_path), str(dest))
            logger.info("[%s] Moved to void: %s -> %s", username, file_path.name, dest.name)
            return dest
        except Exception as e:
            logger.error("[%s] Failed to move %s to void: %s", username, file_path.name, e)
            return None


# =============================================================================
# Filename Helpers
# =============================================================================


AUDIO_EXTENSIONS = {
    ".flac", ".wav", ".mp3", ".ogg", ".webm",
    ".mp4", ".m4a", ".mkv", ".avi", ".mov",
}

PROCESSABLE_EXTENSIONS = {
    ".pdf", ".jpg", ".jpeg", ".png", ".tif", ".tiff",
    ".docx", ".rtf", ".txt", ".md", ".eml", ".html", ".htm",
}


def _is_audio_file(file_path: Path) -> bool:
    """Check if a file is an audio/video file that requires STT processing."""
    return file_path.suffix.lower() in AUDIO_EXTENSIONS


def _is_processable_file(file_path: Path) -> bool:
    """Check if a file type is supported by the /process endpoint."""
    return file_path.suffix.lower() in PROCESSABLE_EXTENSIONS


CHAR_REPLACEMENTS = {
    "ä": "ae", "ö": "oe", "ü": "ue",
    "Ä": "Ae", "Ö": "Oe", "Ü": "Ue",
    "ß": "ss", "æ": "ae", "œ": "oe", "ø": "o", "å": "a",
    "é": "e", "è": "e", "ê": "e", "ë": "e",
    "à": "a", "â": "a", "ù": "u", "û": "u",
    "ô": "o", "î": "i", "ï": "i", "ç": "c", "ñ": "n",
}


def _sanitize_filename_part(s: str) -> str:
    """Sanitize a string for use in a filename field."""
    for char, replacement in CHAR_REPLACEMENTS.items():
        s = s.replace(char, replacement)
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ASCII", "ignore").decode("ASCII")
    s = re.sub(r"\s+", "_", s)
    s = s.replace("-", "_")
    s = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", s)
    s = re.sub(r"_+", "_", s)
    s = s.strip("_")
    if len(s) > 50:
        s = s[:50].rstrip("_")
    return s


def _format_filename(
    metadata: dict[str, Any],
    filename_pattern: str,
    source_filename: Optional[str] = None,
) -> str:
    """
    Format filename from metadata and pattern.

    Replicates UserMetadataManager.format_filename() from watcher.py.
    The {source_filename} placeholder resolves to the original filename stem.
    """
    result = filename_pattern
    placeholders = re.findall(r"\{(\w+)\}", filename_pattern)

    for field_name in placeholders:
        if field_name == "source_filename":
            value = Path(source_filename).stem if source_filename else None
        else:
            value = metadata.get(field_name)
        placeholder = "{" + field_name + "}"
        if value:
            if field_name == "date":
                sanitized = str(value)
            elif isinstance(value, list):
                sanitized = "_".join(_sanitize_filename_part(str(v)) for v in value)
            else:
                sanitized = _sanitize_filename_part(str(value))
            result = result.replace(placeholder, sanitized)
        else:
            result = result.replace(placeholder, "")

    result = re.sub(r"[-_]{2,}", "-", result)
    result = result.strip("-_")
    if not result:
        return "document.pdf"
    return (result + ".pdf").lower()


def _ensure_dict(value: Any) -> dict[str, Any]:
    """Ensure a value is a dict, parsing JSON strings if needed."""
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed
        except (json.JSONDecodeError, TypeError):
            pass
    return {}


def _cleanup_metadata(metadata: dict[str, Any], context: ContextConfig) -> dict[str, Any]:
    """
    Remove superfluous fields from metadata.

    Superfluous = key not in context.field_names AND value is None.
    Additional fields (not in context but non-null) are left alone.
    """
    return {
        k: v
        for k, v in metadata.items()
        if k in context.field_names or v is not None
    }


def _has_missing_fields(metadata: dict[str, Any], context: ContextConfig) -> bool:
    """
    Check if metadata has any missing fields.

    Missing = key IS in metadata with value None, AND key is in context.field_names.
    Fields absent from metadata ("not specified") do NOT count as missing.
    """
    return any(
        k in metadata and metadata[k] is None
        for k in context.field_names
    )


# =============================================================================
# mrdocument API
# =============================================================================


async def _call_mrdocument(
    file_path: Path,
    mrdocument_url: str,
    language: str,
    contexts: list[dict[str, Any]],
    locked_fields: Optional[dict[str, Any]] = None,
    username: str = "",
) -> Optional[tuple[dict[str, Any], bytes, str]]:
    """
    Call mrdocument /process endpoint.

    Returns (metadata, output_bytes, output_ext) or None on failure.
    output_ext is ".pdf" or ".docx".
    """
    try:
        file_bytes = file_path.read_bytes()
    except Exception as e:
        logger.error("[%s] Failed to read file %s: %s", username, file_path.name, e)
        return None

    # Determine content type
    ext = file_path.suffix.lower()
    content_types = {
        ".pdf": "application/pdf",
        ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
        ".png": "image/png",
        ".tif": "image/tiff", ".tiff": "image/tiff",
        ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ".rtf": "application/rtf",
        ".txt": "text/plain", ".md": "text/plain",
    }
    content_type = content_types.get(ext, "application/octet-stream")

    try:
        async with aiohttp.ClientSession() as session:
            data = aiohttp.FormData()
            data.add_field("file", file_bytes, filename=file_path.name, content_type=content_type)
            data.add_field("language", language)
            data.add_field("contexts", json.dumps(contexts))

            if locked_fields:
                data.add_field("locked_fields", json.dumps(locked_fields))

            timeout = aiohttp.ClientTimeout(total=300)
            async with session.post(
                f"{mrdocument_url}/process", data=data, timeout=timeout
            ) as response:
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(
                        "[%s] mrdocument error (%d) for %s: %s",
                        username, response.status, file_path.name, error_text,
                    )
                    return None

                result = await response.json()
    except Exception as e:
        logger.error("[%s] mrdocument request failed for %s: %s", username, file_path.name, e)
        return None

    metadata = _ensure_dict(result.get("metadata", {}))
    docx_base64 = result.get("docx")
    pdf_base64 = result.get("pdf")

    is_docx_output = docx_base64 is not None
    output_base64 = docx_base64 if is_docx_output else pdf_base64
    output_ext = ".docx" if is_docx_output else ".pdf"

    if not output_base64:
        # Text files don't return transformed content
        is_text_file = ext in (".txt", ".md")
        if is_text_file:
            return (metadata, file_bytes, ext)
        logger.error("[%s] No document data in mrdocument response for %s", username, file_path.name)
        return None

    try:
        output_bytes = base64.b64decode(output_base64)
    except Exception as e:
        logger.error("[%s] Failed to decode mrdocument output for %s: %s", username, file_path.name, e)
        return None

    return (metadata, output_bytes, output_ext)


def _collect_keyterms_from_context(
    context_yaml: dict[str, Any],
    inferred_fields: dict[str, str],
) -> list[str]:
    """
    Collect transcription keyterms from a context config dict and inferred field values.

    Mirrors the keyterm collection logic in server.py classify_audio but operates
    on raw dicts (context YAML + inferred fields from folder structure).
    """
    keyterms: set[str] = set()

    # Context-level keyterms
    context_keyterms = context_yaml.get("transcription_keyterms", [])
    if context_keyterms:
        keyterms.update(context_keyterms)

    # Keyterms from matched candidates
    fields_config = context_yaml.get("fields", {})
    for field_name, field_config in fields_config.items():
        value = inferred_fields.get(field_name)
        if not value:
            continue

        candidates = field_config.get("candidates", [])
        if not candidates:
            continue

        for candidate in candidates:
            if isinstance(candidate, dict):
                candidate_name = candidate.get("name")
                candidate_short = candidate.get("short")
                if value == candidate_name or value == candidate_short:
                    candidate_keyterms = candidate.get("transcription_keyterms", [])
                    if candidate_keyterms:
                        keyterms.update(candidate_keyterms)
                    break

    return sorted(keyterms) if keyterms else []


async def _call_stt_and_process_transcript(
    file_path: Path,
    stt_url: str,
    mrdocument_url: str,
    stt_config: Any,
    contexts: list[dict[str, Any]],
    transcription_keyterms: Optional[list[str]] = None,
    locked_fields: Optional[dict[str, Any]] = None,
    user_dir: Optional[str] = None,
    primary_language: Optional[str] = None,
    username: str = "",
    is_intro: bool = False,
) -> Optional[tuple[dict[str, Any], str, dict]]:
    """
    Call STT /transcribe then mrdocument /process_transcript.

    When is_intro=True, runs a two-pass flow:
    1. First STT pass (with filename keyterms)
    2. Classify transcript via /classify_transcript (extract keyterms + speaker count)
    3. Second STT pass (with transcript keyterms + speaker count)
    4. Pass pre_classified metadata to /process_transcript

    Returns (metadata, text_content, corrected_json) or None on failure.
    """
    # Read file
    try:
        file_bytes = file_path.read_bytes()
    except Exception as e:
        logger.error("[%s] Failed to read audio file %s: %s", username, file_path.name, e)
        return None

    # Determine audio content type
    ext = file_path.suffix.lower()
    audio_content_types = {
        ".flac": "audio/flac",
        ".wav": "audio/wav",
        ".mp3": "audio/mpeg",
        ".ogg": "audio/ogg",
        ".webm": "audio/webm",
        ".mp4": "video/mp4",
        ".m4a": "audio/mp4",
        ".mkv": "video/x-matroska",
        ".avi": "video/x-msvideo",
        ".mov": "video/quicktime",
    }
    content_type = audio_content_types.get(ext, "application/octet-stream")

    # Step 1: POST to STT /transcribe
    logger.info("[%s] Sending audio to STT service: %s", username, file_path.name)
    try:
        async with aiohttp.ClientSession() as session:
            data = aiohttp.FormData()
            data.add_field("file", file_bytes, filename=file_path.name, content_type=content_type)
            data.add_field("language", stt_config.language)
            data.add_field("elevenlabs_model", stt_config.elevenlabs_model)
            data.add_field("enable_diarization", str(stt_config.enable_diarization).lower())
            data.add_field("diarization_speaker_count", str(stt_config.diarization_speaker_count))

            if transcription_keyterms:
                data.add_field("keyterms", json.dumps(transcription_keyterms))

            timeout = aiohttp.ClientTimeout(total=1800)  # 30 min for long audio
            async with session.post(
                f"{stt_url}/transcribe", data=data, timeout=timeout
            ) as response:
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(
                        "[%s] STT error (%d) for %s: %s",
                        username, response.status, file_path.name, error_text,
                    )
                    return None

                stt_result = await response.json()
    except Exception as e:
        logger.error("[%s] STT request failed for %s: %s", username, file_path.name, e)
        return None

    transcript_json = stt_result.get("transcript", {})
    if not transcript_json or not transcript_json.get("segments"):
        logger.error("[%s] No transcript data in STT response for %s", username, file_path.name)
        return None

    logger.info("[%s] Got transcript: %d segments", username, len(transcript_json.get("segments", [])))

    # Intro two-pass: classify transcript and do second STT pass
    pre_classified: Optional[dict[str, Any]] = None

    if is_intro:
        logger.info("[%s] Intro file detected (sorter), starting two-pass flow...", username)
        transcript_keyterms_2: Optional[list[str]] = None
        transcript_n_speakers: Optional[int] = None
        transcript_context: Optional[str] = None
        transcript_metadata: Optional[dict] = None

        # Classify transcript via /classify_transcript
        try:
            async with aiohttp.ClientSession() as session:
                classify_request = {
                    "transcript": transcript_json,
                    "filename": file_path.name,
                    "contexts": contexts,
                }
                ct_timeout = aiohttp.ClientTimeout(total=300)
                async with session.post(
                    f"{mrdocument_url}/classify_transcript",
                    json=classify_request, timeout=ct_timeout,
                ) as ct_response:
                    if ct_response.status == 200:
                        ct_result = await ct_response.json()
                        transcript_context = ct_result.get("context")
                        transcript_metadata = ct_result.get("metadata", {})
                        transcript_keyterms_2 = ct_result.get("transcription_keyterms", [])
                        transcript_n_speakers = ct_result.get("number_of_speakers")
                        logger.info(
                            "[%s] Transcript classification: context=%s, keyterms=%d, speakers=%s",
                            username, transcript_context,
                            len(transcript_keyterms_2) if transcript_keyterms_2 else 0,
                            transcript_n_speakers,
                        )
                    else:
                        error_text = await ct_response.text()
                        logger.warning(
                            "[%s] Transcript classification failed (%d): %s",
                            username, ct_response.status, error_text,
                        )
        except Exception as e:
            logger.warning("[%s] Transcript classification error: %s", username, e)

        # Second STT pass if we got keyterms
        if transcript_keyterms_2:
            logger.info("[%s] Running second STT pass with %d keyterms...", username, len(transcript_keyterms_2))
            try:
                async with aiohttp.ClientSession() as session:
                    data2 = aiohttp.FormData()
                    data2.add_field("file", file_bytes, filename=file_path.name, content_type=content_type)
                    data2.add_field("language", stt_config.language)
                    data2.add_field("elevenlabs_model", stt_config.elevenlabs_model)
                    data2.add_field("keyterms", json.dumps(transcript_keyterms_2))

                    if transcript_n_speakers and transcript_n_speakers > 1:
                        data2.add_field("enable_diarization", "true")
                        data2.add_field("diarization_speaker_count", str(transcript_n_speakers))
                    else:
                        data2.add_field("enable_diarization", str(stt_config.enable_diarization).lower())
                        data2.add_field("diarization_speaker_count", str(stt_config.diarization_speaker_count))

                    stt2_timeout = aiohttp.ClientTimeout(total=1800)
                    async with session.post(
                        f"{stt_url}/transcribe", data=data2, timeout=stt2_timeout,
                    ) as stt2_response:
                        if stt2_response.status == 200:
                            stt2_result = await stt2_response.json()
                            transcript_json_2 = stt2_result.get("transcript", {})
                            if transcript_json_2 and transcript_json_2.get("segments"):
                                transcript_json = transcript_json_2
                                logger.info("[%s] Second STT pass successful: %d segments", username, len(transcript_json.get("segments", [])))
                            else:
                                logger.warning("[%s] Second STT pass returned empty transcript, using first pass", username)
                        else:
                            error_text2 = await stt2_response.text()
                            logger.warning("[%s] Second STT pass failed (%d): %s", username, stt2_response.status, error_text2)
            except Exception as e:
                logger.warning("[%s] Second STT pass error: %s", username, e)

        # Build pre_classified if transcript classification succeeded
        if transcript_context and transcript_metadata:
            pc_fields = {k: v for k, v in transcript_metadata.items() if k not in ("context", "date")}
            pre_classified = {
                "context": transcript_context,
                "date": transcript_metadata.get("date"),
                "fields": pc_fields,
            }

    # Step 2: POST transcript to mrdocument /process_transcript
    logger.info("[%s] Sending transcript to mrdocument: %s", username, file_path.name)
    request_body: dict[str, Any] = {
        "transcript": transcript_json,
        "filename": file_path.stem,
        "contexts": contexts,
    }
    if user_dir:
        request_body["user_dir"] = user_dir
    if primary_language:
        request_body["primary_language"] = primary_language
    if locked_fields:
        request_body["locked_fields"] = locked_fields
    if pre_classified:
        request_body["pre_classified"] = pre_classified

    try:
        async with aiohttp.ClientSession() as session:
            timeout = aiohttp.ClientTimeout(total=1800)  # 30 min for correction
            async with session.post(
                f"{mrdocument_url}/process_transcript",
                json=request_body, timeout=timeout,
            ) as response:
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(
                        "[%s] mrdocument error (%d) for %s: %s",
                        username, response.status, file_path.name, error_text,
                    )
                    return None

                mrdoc_result = await response.json()
    except Exception as e:
        logger.error("[%s] mrdocument request failed for %s: %s", username, file_path.name, e)
        return None

    # Extract results
    corrected_json = mrdoc_result.get("corrected_json", {})
    text_content = mrdoc_result.get("text", "")
    metadata = _ensure_dict(mrdoc_result.get("metadata", {}))
    suggested_filename = mrdoc_result.get("filename", "transcript.pdf")

    # Store suggested_filename in metadata for callers
    metadata["_suggested_filename"] = suggested_filename

    return (metadata, text_content, corrected_json)


# =============================================================================
# Context Manager
# =============================================================================


class SorterContextManager:
    """Manages context configurations for sorting."""

    def __init__(self, user_root: Path, username: str):
        self.user_root = user_root
        self.username = username
        self.contexts: dict[str, ContextConfig] = {}
        self.generated_data: dict[str, dict[str, Any]] = {}
        self.generated_files: dict[str, Path] = {}

    def load(self) -> bool:
        """Load contexts from sorted/{context}/context.yaml files."""
        return self._load_from_sorted()

    def _load_from_sorted(self) -> bool:
        """Load contexts from sorted/{context}/context.yaml files."""
        sorted_dir = self.user_root / "sorted"
        if not sorted_dir.is_dir():
            return False

        contexts: dict[str, ContextConfig] = {}
        for ctx_dir in sorted_dir.iterdir():
            if not ctx_dir.is_dir() or ctx_dir.name.startswith("."):
                continue
            ctx_yaml = _find_ci(ctx_dir, "context.yaml")
            if ctx_yaml is None:
                continue
            try:
                with open(ctx_yaml) as f:
                    data = yaml.safe_load(f)
                if not data or not isinstance(data, dict):
                    continue
                context = ContextConfig.from_dict(data)
                if not context:
                    continue
                if context.name.lower() != ctx_dir.name.lower():
                    logger.warning(
                        "[%s] Context name '%s' does not match directory '%s', skipping",
                        self.username, context.name, ctx_dir.name,
                    )
                    continue
                contexts[context.name.lower()] = context
            except Exception as e:
                logger.warning("Failed to load context from %s: %s", ctx_yaml, e)

        if not contexts:
            return False

        self.contexts = contexts
        self.generated_data = {}
        self.generated_files = {}
        for ctx_name in contexts:
            ctx_dir = self.user_root / "sorted" / ctx_name
            gen_path = _find_ci(ctx_dir, "generated.yaml") or ctx_dir / "generated.yaml"
            self.generated_files[ctx_name] = gen_path
            gen_fields = self._load_generated_file(ctx_name, gen_path)
            if gen_fields:
                self.generated_data[ctx_name] = gen_fields
        logger.info(
            "[%s] Sorter loaded %d context(s) from sorted/: %s",
            self.username, len(self.contexts), list(self.contexts.keys()),
        )
        return True

    def load_smart_folders_from_sorted(self) -> dict[str, list[tuple[str, SmartFolderConfig]]]:
        """Load smart folders from sorted/{context}/smartfolders.yaml files.

        Returns:
            Dict mapping context name to list of (sf_name, SmartFolderConfig) tuples.
            Empty dict if no smartfolders.yaml files found.
        """
        sorted_dir = self.user_root / "sorted"
        if not sorted_dir.is_dir():
            return {}

        result: dict[str, list[tuple[str, SmartFolderConfig]]] = {}
        for ctx_dir in sorted_dir.iterdir():
            if not ctx_dir.is_dir() or ctx_dir.name.startswith("."):
                continue
            sf_yaml = _find_ci(ctx_dir, "smartfolders.yaml")
            if sf_yaml is None:
                continue
            try:
                with open(sf_yaml) as f:
                    data = yaml.safe_load(f)
                if not data or not isinstance(data, dict):
                    continue
                sf_data = data.get("smart_folders", {})
                if not isinstance(sf_data, dict):
                    continue
                ctx_name = ctx_dir.name.lower()
                entries: list[tuple[str, SmartFolderConfig]] = []
                for sf_name, sf_dict in sf_data.items():
                    if not isinstance(sf_dict, dict):
                        continue
                    # Validate context field matches directory
                    sf_context = sf_dict.get("context", "").lower()
                    if sf_context and sf_context != ctx_name:
                        logger.warning(
                            "[%s] Smart folder '%s' context '%s' does not match directory '%s', skipping",
                            self.username, sf_name, sf_context, ctx_dir.name,
                        )
                        continue
                    sf_config = SmartFolderConfig.from_dict(sf_name, sf_dict, context_name=ctx_name)
                    if sf_config:
                        entries.append((sf_name, sf_config))
                if entries:
                    result[ctx_name] = entries
            except Exception as e:
                logger.warning("Failed to load smart folders from %s: %s", sf_yaml, e)

        return result

    # ------------------------------------------------------------------
    # Generated data: load / save / query
    # ------------------------------------------------------------------

    def _load_generated_file(self, context_name: str, path: Path) -> dict[str, Any]:
        """Load generated data from a generated.yaml file.

        Returns {field_name: {"candidates": [...]}} or empty dict.
        """
        if not path.exists():
            return {}
        try:
            with open(path) as f:
                data = yaml.safe_load(f)
            if not data or not isinstance(data, dict):
                return {}
            fields = data.get("fields", {})
            if not isinstance(fields, dict):
                return {}
            return fields
        except Exception as e:
            logger.warning(
                "[%s] Failed to load generated file %s: %s",
                self.username, path, e,
            )
            return {}

    def _save_generated_file(self, context_name: str) -> bool:
        """Save generated data to disk. Removes the file if empty."""
        if context_name not in self.generated_files:
            return False

        gen_path = self.generated_files[context_name]
        gen_fields = self.generated_data.get(context_name, {})

        # Build output — only include fields that have candidates
        output_fields: dict[str, Any] = {}
        for field_name, field_data in gen_fields.items():
            candidates = field_data.get("candidates", [])
            if candidates:
                output_fields[field_name] = {"candidates": candidates}

        if not output_fields:
            if gen_path.exists():
                try:
                    gen_path.unlink()
                except Exception:
                    pass
            return True

        try:
            gen_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = gen_path.with_suffix(".tmp")
            with open(tmp_path, "w") as f:
                f.write("# Auto-generated candidates and clues - do not edit manually\n")
                yaml.safe_dump(
                    {"fields": output_fields}, f,
                    default_flow_style=False, allow_unicode=True, sort_keys=False,
                )
            tmp_path.rename(gen_path)
            return True
        except Exception as e:
            logger.error(
                "[%s] Failed to save generated file %s: %s",
                self.username, gen_path, e,
            )
            return False

    def _get_all_candidates(self, context_name: str, field_name: str) -> Optional[list]:
        """Merge base + generated candidates for a field.

        Returns None if the field has no candidates concept.
        """
        # Load the raw YAML for the context to get field config
        ctx_data = self._load_context_yaml(context_name)
        if not ctx_data:
            return None
        fields = ctx_data.get("fields", {})
        field_config = fields.get(field_name, {})
        if not isinstance(field_config, dict):
            return None
        base_candidates = field_config.get("candidates")
        if base_candidates is None:
            return None

        # Deep copy base candidates
        all_candidates: list = []
        for c in base_candidates:
            if isinstance(c, dict):
                all_candidates.append(dict(c))
            else:
                all_candidates.append(c)

        # Merge generated candidates
        gen_fields = self.generated_data.get(context_name, {})
        gen_field = gen_fields.get(field_name) or {}
        gen_candidates = gen_field.get("candidates", [])

        for gen_c in gen_candidates:
            if isinstance(gen_c, str):
                # New candidate — add if not already present
                exists = any(
                    (isinstance(c, dict) and (c.get("name") == gen_c or c.get("short") == gen_c))
                    or (isinstance(c, str) and c == gen_c)
                    for c in all_candidates
                )
                if not exists:
                    all_candidates.append(gen_c)
            elif isinstance(gen_c, dict):
                gen_name = gen_c.get("name", "")
                gen_clues = gen_c.get("clues", [])
                if not gen_name:
                    continue
                # Find matching base candidate and merge clues
                found = False
                for i, c in enumerate(all_candidates):
                    if isinstance(c, dict) and c.get("name") == gen_name:
                        existing_clues = list(c.get("clues", []))
                        for clue in gen_clues:
                            if clue not in existing_clues:
                                existing_clues.append(clue)
                        all_candidates[i]["clues"] = existing_clues
                        found = True
                        break
                if not found:
                    all_candidates.append(dict(gen_c))

        return all_candidates

    def _load_context_yaml(self, context_name: str) -> Optional[dict]:
        """Load the raw YAML dict for a context from sorted/{context}/context.yaml."""
        name = context_name.lower()
        sorted_ctx = _find_ci(self.user_root / "sorted" / name, "context.yaml")
        if sorted_ctx is None:
            return None
        try:
            with open(sorted_ctx) as f:
                data = yaml.safe_load(f)
            if data and isinstance(data, dict) and data.get("name", "").lower() == name:
                return data
        except Exception:
            pass
        return None

    def is_new_item(self, context_name: str, field_name: str, value: str) -> bool:
        """Check if a value is new (not in base or generated candidates)."""
        if not value or context_name not in self.contexts:
            return False
        ctx_data = self._load_context_yaml(context_name)
        if not ctx_data:
            return False
        fields = ctx_data.get("fields", {})
        if field_name not in fields:
            return False
        field_config = fields[field_name]
        if not isinstance(field_config, dict):
            return False
        candidates = field_config.get("candidates")
        if candidates is None:
            return False
        if not field_config.get("allow_new_candidates", True):
            return False
        # Check base candidates
        for c in candidates:
            if isinstance(c, dict):
                if c.get("name") == value or c.get("short") == value:
                    return False
            elif c == value:
                return False
        # Check generated candidates
        gen_fields = self.generated_data.get(context_name, {})
        gen_field = gen_fields.get(field_name, {})
        for c in gen_field.get("candidates", []):
            if isinstance(c, str) and c == value:
                return False
            elif isinstance(c, dict):
                if c.get("name") == value or c.get("short") == value:
                    return False
        return True

    def record_new_item(self, context_name: str, field_name: str, value: str) -> bool:
        """Record a new candidate value in the generated file."""
        if not self.is_new_item(context_name, field_name, value):
            return False
        if context_name not in self.generated_data:
            self.generated_data[context_name] = {}
        if field_name not in self.generated_data[context_name]:
            self.generated_data[context_name][field_name] = {"candidates": []}
        if "candidates" not in self.generated_data[context_name][field_name]:
            self.generated_data[context_name][field_name]["candidates"] = []
        self.generated_data[context_name][field_name]["candidates"].append(value)
        if self._save_generated_file(context_name):
            logger.info(
                "[%s] Recorded new %s '%s' in context '%s'",
                self.username, field_name, value, context_name,
            )
            return True
        return False

    def record_new_clue(
        self, context_name: str, field_name: str, candidate_value: str, clue: str,
    ) -> bool:
        """Record a new clue for an existing candidate in the generated file."""
        if context_name not in self.contexts:
            return False
        ctx_data = self._load_context_yaml(context_name)
        if not ctx_data:
            return False
        fields = ctx_data.get("fields", {})
        if field_name not in fields:
            return False
        field_config = fields[field_name]
        if not isinstance(field_config, dict):
            return False
        candidates = field_config.get("candidates")
        if candidates is None:
            return False

        # Find candidate in base, check allow_new_clues
        candidate_found = False
        allows_new_clues = False
        for c in candidates:
            if isinstance(c, dict) and c.get("name") == candidate_value:
                candidate_found = True
                allows_new_clues = c.get("allow_new_clues", False)
                break
            elif c == candidate_value:
                return False  # Simple string candidate, can't add clues

        if not candidate_found or not allows_new_clues:
            return False

        # Check if clue already exists in base
        for c in candidates:
            if isinstance(c, dict) and c.get("name") == candidate_value:
                if clue in c.get("clues", []):
                    return False
                break

        # Check if clue already exists in generated data
        gen_fields = self.generated_data.get(context_name, {})
        gen_field = gen_fields.get(field_name, {})
        for c in gen_field.get("candidates", []):
            if isinstance(c, dict) and c.get("name") == candidate_value:
                if clue in c.get("clues", []):
                    return False
                break

        # Add to generated data
        if context_name not in self.generated_data:
            self.generated_data[context_name] = {}
        if field_name not in self.generated_data[context_name]:
            self.generated_data[context_name][field_name] = {"candidates": []}
        if "candidates" not in self.generated_data[context_name][field_name]:
            self.generated_data[context_name][field_name]["candidates"] = []

        gen_candidates = self.generated_data[context_name][field_name]["candidates"]
        found_gen = False
        for c in gen_candidates:
            if isinstance(c, dict) and c.get("name") == candidate_value:
                if "clues" not in c:
                    c["clues"] = []
                c["clues"].append(clue)
                found_gen = True
                break
        if not found_gen:
            gen_candidates.append({"name": candidate_value, "clues": [clue]})

        if self._save_generated_file(context_name):
            logger.info(
                "[%s] Recorded new clue for %s '%s' in context '%s': %s",
                self.username, field_name, candidate_value, context_name, clue,
            )
            return True
        return False

    def get_context(self, name: str) -> Optional[ContextConfig]:
        """Get context by name (case-insensitive)."""
        return self.contexts.get(name.lower())

    def get_context_for_api(self, name: str) -> Optional[dict[str, Any]]:
        """Load the full context YAML dict for a context by name.

        Loads from sorted/{name}/context.yaml.
        Merges generated candidates/clues into the fields before returning.
        """
        data = self._load_context_yaml(name)
        if not data:
            return None
        if "description" not in data:
            data["description"] = data.get("name", name)

        # Merge generated candidates into fields
        ctx_name = name.lower()
        if ctx_name in self.generated_data and "fields" in data:
            for field_name in data["fields"]:
                merged = self._get_all_candidates(ctx_name, field_name)
                if merged is not None:
                    data["fields"][field_name]["candidates"] = merged

        return data


# =============================================================================
# File Sorter
# =============================================================================


class FileSorter:
    """Sorts files from reviewed folder into nested folder structure."""

    def __init__(self, user_root: Path, config: UserConfig, db: Optional["DocumentDB"] = None):
        self.user_root = user_root
        self.username = get_username_from_root(user_root)
        self.config = config
        self.context_manager = SorterContextManager(user_root, self.username)
        self.reviewed_dir = user_root / config.reviewed_folder
        self.sorted_dir = user_root / config.sorted_folder
        self.processed_dir = user_root / config.processed_folder
        self.db = db

    def reload_contexts(self) -> bool:
        """Reload context configurations."""
        return self.context_manager.load()

    async def sort_file(self, file_path: Path) -> Optional[Path]:
        """
        Sort a file from the reviewed folder.

        Queries the database for metadata and context, then moves the file
        into the appropriate nested folder structure.

        Returns the final target path on success, None on failure.
        """
        username = self.username
        filename = file_path.name

        logger.info("[%s] Sorting file: %s", username, filename)

        if not self.db:
            logger.warning("[%s] No database available, cannot sort: %s", username, filename)
            return None

        # Look up metadata from DB
        record = await _lookup_metadata_from_db(self.db, file_path, username)
        if not record:
            logger.warning("[%s] File not in DB, cannot sort: %s", username, filename)
            return None

        metadata = _ensure_dict(record.get("metadata", {}))
        context_name = record.get("context_name")

        if not context_name:
            logger.warning("[%s] No context in DB record for: %s", username, filename)
            return None

        context = self.context_manager.get_context(context_name)
        if not context:
            logger.warning(
                "[%s] Unknown context '%s' for file: %s",
                username, context_name, filename
            )
            return None

        # Build folder path from context's folders configuration
        if not context.folders:
            logger.warning(
                "[%s] Context '%s' has no folders defined, cannot sort: %s",
                username, context.name, filename
            )
            return None

        folder_parts = []
        for folder_field in context.folders:
            value = metadata.get(folder_field)
            if not value:
                # Folder field is absent or null — file is unsortable
                logger.info(
                    "[%s] Folder field '%s' missing/null, moving to unsortable: %s",
                    username, folder_field, filename
                )
                return await self._move_to_unsortable(file_path, record)
            # Sanitize folder name
            sanitized = self._sanitize_folder_name(str(value))
            if not sanitized:
                logger.warning(
                    "[%s] Invalid folder value for field '%s': %s",
                    username, folder_field, value
                )
                return await self._move_to_unsortable(file_path, record)
            folder_parts.append(sanitized)

        logger.debug("[%s] Metadata fields: %s", username, metadata)

        # Create target directory
        target_dir = self.sorted_dir
        for part in folder_parts:
            target_dir = target_dir / part

        try:
            target_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logger.error("[%s] Failed to create directory %s: %s", username, target_dir, e)
            return None

        # Determine target path, handling conflicts
        target_path = target_dir / filename
        if target_path.exists():
            # Append UUID to avoid conflict
            stem = file_path.stem
            suffix = file_path.suffix
            unique_id = uuid.uuid4().hex[:8]
            new_filename = f"{stem}_{unique_id}{suffix}"
            target_path = target_dir / new_filename
            logger.info("[%s] Filename conflict, renamed to: %s", username, new_filename)

        # Move file
        try:
            file_path.rename(target_path)
            logger.info(
                "[%s] Sorted: %s -> %s",
                username, filename, target_path.relative_to(self.sorted_dir)
            )

            # Update DB: new path and status
            try:
                await self.db.update_file_path(
                    record["id"],
                    new_path=str(target_path),
                    new_status="sorted",
                )
            except Exception as e:
                logger.warning("[%s] DB update after sort failed: %s", username, e)

            return target_path
        except Exception as e:
            logger.error("[%s] Failed to move file %s: %s", username, filename, e)
            return None

    async def _move_to_unsortable(
        self, file_path: Path, record: dict[str, Any]
    ) -> Optional[Path]:
        """
        Move a file to the unsortable/ folder.

        Used when a folder field is absent or null, so the file cannot be
        placed into the folder hierarchy.

        Returns the unsortable path, or None on failure.
        """
        username = self.username
        unsortable_dir = self.user_root / "unsortable"
        try:
            unsortable_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logger.error("[%s] Failed to create unsortable dir: %s", username, e)
            return None

        target_path = unsortable_dir / file_path.name
        if target_path.exists():
            stem = file_path.stem
            suffix = file_path.suffix
            unique_id = uuid.uuid4().hex[:8]
            target_path = unsortable_dir / f"{stem}_{unique_id}{suffix}"

        try:
            file_path.rename(target_path)
            logger.info("[%s] Moved to unsortable: %s -> %s", username, file_path.name, target_path.name)
        except Exception as e:
            logger.error("[%s] Failed to move %s to unsortable: %s", username, file_path.name, e)
            return None

        # Update DB
        if self.db:
            try:
                await self.db.update_file_path(
                    record["id"],
                    new_path=str(target_path),
                    new_status="unsortable",
                )
            except Exception as e:
                logger.warning("[%s] DB update after unsortable move failed: %s", username, e)

        return target_path

    @staticmethod
    def _sanitize_folder_name(name: str) -> str:
        """Sanitize a string for use as a folder name."""
        # Replace problematic characters
        sanitized = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name)
        # Collapse multiple underscores
        sanitized = re.sub(r"_+", "_", sanitized)
        # Remove leading/trailing underscores and spaces
        sanitized = sanitized.strip("_ ")
        return sanitized


# =============================================================================
# Smart Folder Manager
# =============================================================================


class SmartFolderManager:
    """
    Manages smart folders for a user's sorted directory.
    
    Watches leaf folders (deepest level of sorting pattern) and creates
    symlinks in smart folder subdirectories for files matching conditions.
    """

    def __init__(
        self,
        user_root: Path,
        sorted_dir: Path,
        context_manager: "SorterContextManager",
        db: Optional["DocumentDB"] = None,
    ):
        self.user_root = user_root
        self.sorted_dir = sorted_dir
        self.context_manager = context_manager
        self.username = get_username_from_root(user_root)
        self.db = db

    def get_leaf_folders(self) -> list[tuple[Path, ContextConfig]]:
        """
        Find all leaf folders in the sorted directory.
        
        A leaf folder is at the depth specified by the context's folders config.
        Determines context by parsing filenames found in directories.
        
        Returns list of (leaf_folder_path, context) tuples.
        """
        leaf_folders: list[tuple[Path, ContextConfig]] = []
        seen_leaf_folders: set[str] = set()
        
        if not self.sorted_dir.exists():
            return leaf_folders
        
        # Get contexts that have smart folders
        contexts_with_smart_folders = {
            name: ctx for name, ctx in self.context_manager.contexts.items()
            if ctx.folders and ctx.smart_folders
        }
        
        if not contexts_with_smart_folders:
            return leaf_folders
        
        # Walk the sorted directory to find files and determine their contexts
        self._scan_for_leaf_folders(
            self.sorted_dir,
            contexts_with_smart_folders,
            leaf_folders,
            seen_leaf_folders,
            depth=0,
        )
        
        return leaf_folders

    def _scan_for_leaf_folders(
        self,
        current_dir: Path,
        contexts_with_smart_folders: dict[str, ContextConfig],
        result: list[tuple[Path, ContextConfig]],
        seen: set[str],
        depth: int,
    ) -> None:
        """Recursively scan for leaf folders by examining depth against context folder configs."""
        if not current_dir.is_dir():
            return

        # At the right depth, check if any context's folder depth matches
        # and the first folder component matches the context name
        try:
            relative = current_dir.relative_to(self.sorted_dir)
            first_folder = relative.parts[0] if relative.parts else None
        except ValueError:
            first_folder = None
        for context_name, context in contexts_with_smart_folders.items():
            expected_depth = len(context.folders)
            if depth == expected_depth and first_folder == context_name:
                # Check if this directory has any non-symlink files
                has_files = any(
                    item.is_file() and not item.is_symlink()
                    and not item.name.startswith(".")
                    and not item.name.startswith("~")
                    and ".syncthing." not in item.name
                    and not item.name.endswith(".tmp")
                    for item in current_dir.iterdir()
                )
                if has_files:
                    folder_key = str(current_dir)
                    if folder_key not in seen:
                        seen.add(folder_key)
                        result.append((current_dir, context))
                    return  # Found context, no need to recurse deeper

        # Recurse into subdirectories
        for child in current_dir.iterdir():
            if child.is_dir() and not child.name.startswith("."):
                # Skip smart folder subdirectories (check against all contexts)
                is_smart_folder = any(
                    child.name in ctx.smart_folders
                    for ctx in contexts_with_smart_folders.values()
                )
                if not is_smart_folder:
                    self._scan_for_leaf_folders(
                        child,
                        contexts_with_smart_folders,
                        result,
                        seen,
                        depth + 1,
                    )

    def ensure_smart_folder_dirs(self, leaf_folder: Path, context: ContextConfig) -> None:
        """Create smart folder subdirectories in a leaf folder."""
        for sf_name in context.smart_folders:
            sf_dir = leaf_folder / sf_name
            if not sf_dir.exists():
                try:
                    sf_dir.mkdir(parents=True, exist_ok=True)
                    logger.debug(
                        "[%s] Created smart folder: %s",
                        self.username, sf_dir.relative_to(self.sorted_dir)
                    )
                except Exception as e:
                    logger.error(
                        "[%s] Failed to create smart folder %s: %s",
                        self.username, sf_dir, e
                    )

    def process_file(
        self,
        file_path: Path,
        leaf_folder: Path,
        context: ContextConfig,
        fields: Optional[dict[str, Any]] = None,
    ) -> None:
        """
        Process a file in a leaf folder, creating symlinks in matching smart folders.

        Args:
            file_path: Path to the file
            leaf_folder: The leaf folder containing the file
            context: The context configuration
            fields: Metadata fields from DB. If None, file is skipped.
        """
        filename = file_path.name

        # Skip hidden files and temp files
        if filename.startswith(".") or filename.startswith("~"):
            return
        if ".syncthing." in filename or filename.endswith(".tmp"):
            return

        # Skip symlinks (don't process files in smart folders)
        if file_path.is_symlink():
            return

        # Skip directories
        if not file_path.is_file():
            return

        if fields is None:
            logger.debug(
                "[%s] No metadata for smart folders: %s",
                self.username, filename
            )
            return

        logger.debug("[%s] Smart folder check for %s, fields: %s", self.username, filename, fields)

        # Use the actual filename for symlinks (no more shortened names since
        # filenames are now arbitrary and not tied to metadata)
        symlink_name = filename

        # Check each smart folder condition
        for sf_name, sf_config in context.smart_folders.items():
            sf_dir = leaf_folder / sf_name
            symlink_path = sf_dir / symlink_name

            # Check filename regex filter first (if configured)
            if not sf_config.matches_filename(filename):
                # Filename doesn't match regex filter - remove symlink if it exists
                if symlink_path.is_symlink():
                    try:
                        symlink_path.unlink()
                        logger.info(
                            "[%s] Removed smart folder link (filename_regex no longer matches): %s",
                            self.username, symlink_path.relative_to(self.sorted_dir)
                        )
                    except Exception as e:
                        logger.error(
                            "[%s] Failed to remove symlink %s: %s",
                            self.username, symlink_path, e
                        )
                continue

            # Evaluate condition against metadata fields (cast values to str for regex matching)
            str_fields = {k: str(v) for k, v in fields.items() if v is not None}

            # If no condition is configured, filename match alone is sufficient
            condition_matches = (
                sf_config.condition is None
                or sf_config.condition.evaluate(str_fields)
            )
            if condition_matches:
                # Condition matches - create symlink if it doesn't exist
                if not symlink_path.exists() and not symlink_path.is_symlink():
                    try:
                        # Ensure smart folder directory exists
                        sf_dir.mkdir(parents=True, exist_ok=True)
                        # Create relative symlink pointing to original filename
                        relative_target = Path("..") / filename
                        symlink_path.symlink_to(relative_target)
                        logger.info(
                            "[%s] Smart folder link: %s -> %s",
                            self.username,
                            symlink_path.relative_to(self.sorted_dir),
                            filename,
                        )
                    except Exception as e:
                        logger.error(
                            "[%s] Failed to create symlink %s: %s",
                            self.username, symlink_path, e
                        )
            else:
                # Condition doesn't match - remove symlink if it exists
                if symlink_path.is_symlink():
                    try:
                        symlink_path.unlink()
                        logger.info(
                            "[%s] Removed smart folder link (condition no longer matches): %s",
                            self.username, symlink_path.relative_to(self.sorted_dir)
                        )
                    except Exception as e:
                        logger.error(
                            "[%s] Failed to remove symlink %s: %s",
                            self.username, symlink_path, e
                        )

    def cleanup_orphaned_symlinks(self, leaf_folder: Path, context: ContextConfig) -> None:
        """Remove orphaned/stale symlinks and unexpected files in smart folders.

        Removes:
        - Symlinks with broken targets (file was deleted)
        - Symlinks whose name doesn't match any real file in the leaf folder (stale from renames)
        - Non-symlink files accidentally placed in smart folder dirs (logged as warning)
        """
        # Build set of real filenames in the leaf folder once
        leaf_files: set[str] = set()
        try:
            for item in leaf_folder.iterdir():
                if item.is_file() and not item.is_symlink():
                    leaf_files.add(item.name)
        except OSError:
            return

        for sf_name in context.smart_folders:
            sf_dir = leaf_folder / sf_name
            if not sf_dir.exists():
                continue

            for item in sf_dir.iterdir():
                if item.name.startswith(".") or item.name.startswith("~"):
                    continue
                if ".syncthing." in item.name or item.name.endswith(".tmp"):
                    continue
                if item.is_symlink():
                    # Check if target exists
                    try:
                        target = item.resolve()
                        if not target.exists():
                            item.unlink()
                            logger.info(
                                "[%s] Cleaned up orphaned symlink: %s",
                                self.username, item.relative_to(self.sorted_dir)
                            )
                            continue
                    except Exception as e:
                        logger.debug(
                            "[%s] Error checking symlink %s: %s",
                            self.username, item, e
                        )
                        # Try to unlink broken symlink
                        try:
                            item.unlink()
                            logger.info(
                                "[%s] Cleaned up broken symlink: %s",
                                self.username, item.relative_to(self.sorted_dir)
                            )
                        except Exception:
                            pass
                        continue

                    # Check if symlink name matches a real file in the leaf folder
                    if item.name not in leaf_files:
                        try:
                            item.unlink()
                            logger.info(
                                "[%s] Cleaned up stale symlink (no matching file): %s",
                                self.username, item.relative_to(self.sorted_dir)
                            )
                        except Exception as e:
                            logger.debug(
                                "[%s] Error removing stale symlink %s: %s",
                                self.username, item, e
                            )
                elif not item.is_dir():
                    # Non-symlink, non-directory file in smart folder dir
                    logger.warning(
                        "[%s] Unexpected non-symlink file in smart folder: %s",
                        self.username, item.relative_to(self.sorted_dir)
                    )

    async def process_leaf_folder(self, leaf_folder: Path, context: ContextConfig) -> None:
        """Process all files in a leaf folder."""
        self.ensure_smart_folder_dirs(leaf_folder, context)
        self.cleanup_orphaned_symlinks(leaf_folder, context)

        for item in leaf_folder.iterdir():
            if item.is_file() and not item.is_symlink():
                fields = None
                if self.db:
                    record = await _lookup_metadata_from_db(self.db, item, self.username)
                    if record:
                        fields = _ensure_dict(record.get("metadata", {}))
                self.process_file(item, leaf_folder, context, fields=fields)

    def handle_file_deleted(self, file_path: Path, leaf_folder: Path, context: ContextConfig) -> None:
        """Handle deletion of a file - remove corresponding symlinks."""
        filename = file_path.name

        for sf_name in context.smart_folders:
            symlink_path = leaf_folder / sf_name / filename
            if symlink_path.is_symlink():
                try:
                    symlink_path.unlink()
                    logger.info(
                        "[%s] Removed symlink for deleted file: %s",
                        self.username, symlink_path.relative_to(self.sorted_dir)
                    )
                except Exception as e:
                    logger.error(
                        "[%s] Failed to remove symlink %s: %s",
                        self.username, symlink_path, e
                    )


class SmartFolderHandler(FileSystemEventHandler):
    """
    Watches sorted directory for file changes to update smart folder symlinks
    and handle files placed at non-leaf positions.
    """

    def __init__(
        self,
        manager: SmartFolderManager,
        context_manager: "SorterContextManager",
        loop: asyncio.AbstractEventLoop,
        sorter: Optional[FileSorter] = None,
        mrdocument_url: str = "",
        ocr_language: str = "auto",
        stt_url: str = "",
    ):
        self.manager = manager
        self.context_manager = context_manager
        self.loop = loop
        self.processing: set[str] = set()
        self.sorter = sorter
        self.mrdocument_url = mrdocument_url
        self.ocr_language = ocr_language
        self.stt_url = stt_url
        self._resorting_paths: set[str] = set()

    async def _get_context_and_leaf(self, file_path: Path) -> Optional[tuple[ContextConfig, Path, Optional[dict[str, Any]]]]:
        """
        Determine the context, leaf folder, and metadata for a file path.

        Queries the database to determine which context the file belongs to,
        then checks if the file is in a leaf folder at the correct depth.

        Returns (context, leaf_folder, metadata) or None if not in a valid leaf folder.
        """
        try:
            relative = file_path.relative_to(self.manager.sorted_dir)
        except ValueError:
            return None

        parts = relative.parts
        if len(parts) < 2:  # Need at least one folder + filename
            return None

        # Look up context from DB
        db = self.manager.db
        context_name = None
        metadata = None

        if db:
            record = await _lookup_metadata_from_db(db, file_path, self.manager.username)
            if record:
                context_name = record.get("context_name")
                metadata = _ensure_dict(record.get("metadata", {}))

        if not context_name:
            return None

        context = self.context_manager.get_context(context_name)
        if not context or not context.folders or not context.smart_folders:
            return None

        depth = len(context.folders)

        # The file should be at depth levels below sorted_dir
        if len(parts) == depth + 1:
            leaf_folder = file_path.parent
            return (context, leaf_folder, metadata)

        # If len(parts) > depth + 1, file might be in a smart folder subdir
        if len(parts) == depth + 2:
            potential_smart_folder = parts[depth]
            if potential_smart_folder in context.smart_folders:
                # File is inside a smart folder - ignore it
                return None

        return None

    def _skip_reason(self, file_path: Path) -> str | None:
        """Return None if file should be processed, or a reason string if skipped."""
        name = file_path.name
        if name.startswith(".") or name.startswith("~"):
            return "hidden file"
        if ".syncthing." in name or name.endswith(".tmp"):
            return "syncthing temp file"
        return None

    def on_created(self, event):
        if event.is_directory:
            logger.info("[sorted] FS event CREATED dir=%s -> processing new directory", Path(event.src_path).name)
            asyncio.run_coroutine_threadsafe(
                self._process_new_directory(Path(event.src_path)), self.loop
            )
            return

        file_path = Path(event.src_path)
        if str(file_path) in self._resorting_paths:
            logger.info("[sorted] FS event CREATED file=%s -> skip (resorting in progress)", file_path.name)
            return
        skip = self._skip_reason(file_path)
        if skip:
            logger.info("[sorted] FS event CREATED file=%s -> skip (%s)", file_path.name, skip)
            return
        logger.info("[sorted] FS event CREATED file=%s -> processing file change", file_path.name)
        asyncio.run_coroutine_threadsafe(
            self._process_file_change(file_path), self.loop
        )

    def on_moved(self, event):
        if event.is_directory:
            return

        src_path = Path(event.src_path)
        dest_path = Path(event.dest_path)

        # Check if both src and dest are within sorted/ (internal move)
        try:
            src_path.relative_to(self.manager.sorted_dir)
            dest_path.relative_to(self.manager.sorted_dir)
            is_internal = True
        except ValueError:
            is_internal = False

        if is_internal and str(src_path) not in self._resorting_paths:
            skip = self._skip_reason(dest_path)
            if skip:
                logger.info("[sorted] FS event MOVED (internal) %s -> %s -> skip (%s)", src_path.name, dest_path.name, skip)
                return
            logger.info("[sorted] FS event MOVED (internal) %s -> %s -> processing", src_path.name, dest_path.name)
            asyncio.run_coroutine_threadsafe(
                self._handle_internal_move(src_path, dest_path), self.loop
            )
            return

        # Not an internal move — treat as delete + create
        skip_src = self._skip_reason(src_path)
        skip_dest = self._skip_reason(dest_path)
        if skip_src:
            logger.info("[sorted] FS event MOVED (external) src=%s -> skip delete (%s)", src_path.name, skip_src)
        else:
            logger.info("[sorted] FS event MOVED (external) src=%s -> processing delete", src_path.name)
            asyncio.run_coroutine_threadsafe(
                self._process_file_delete(src_path), self.loop
            )
        if skip_dest:
            logger.info("[sorted] FS event MOVED (external) dest=%s -> skip create (%s)", dest_path.name, skip_dest)
        else:
            logger.info("[sorted] FS event MOVED (external) dest=%s -> processing create", dest_path.name)
            asyncio.run_coroutine_threadsafe(
                self._process_file_change(dest_path), self.loop
            )

    def on_deleted(self, event):
        if event.is_directory:
            return

        file_path = Path(event.src_path)
        skip = self._skip_reason(file_path)
        if skip:
            logger.info("[sorted] FS event DELETED file=%s -> skip (%s)", file_path.name, skip)
            return
        logger.info("[sorted] FS event DELETED file=%s -> processing", file_path.name)
        asyncio.run_coroutine_threadsafe(
            self._process_file_delete(file_path), self.loop
        )

    async def _process_file_change(self, file_path: Path):
        """Process a file creation or modification."""
        file_key = str(file_path)

        if file_key in self.processing:
            return

        self.processing.add(file_key)
        try:
            # Wait for file to stabilize
            await asyncio.sleep(1)

            if not file_path.exists():
                return

            result = await self._get_context_and_leaf(file_path)
            if result:
                context, leaf_folder, metadata = result
                self.manager.process_file(file_path, leaf_folder, context, fields=metadata)
            else:
                # Not a known leaf file — check further
                await self._handle_unrecognized_file(file_path)
        except Exception as e:
            logger.error(
                "[%s] Error processing file change %s: %s",
                self.manager.username, file_path.name, e,
            )
        finally:
            self.processing.discard(file_key)

    async def _process_file_delete(self, file_path: Path):
        """Process a file deletion."""
        db = self.manager.db

        # Clean up file_locations
        if db:
            try:
                await db.remove_file_location_by_path(str(file_path))
            except Exception as e:
                logger.debug("[%s] Failed to remove file location on delete: %s", self.manager.username, e)

        # For delete, we don't need metadata — just context and leaf folder
        # Since the file is deleted, DB lookup by path won't work after OutputFolderWatcher
        # clears it. We fall back to depth-based context determination.
        try:
            relative = file_path.relative_to(self.manager.sorted_dir)
        except ValueError:
            return

        parts = relative.parts
        if len(parts) < 2:
            return

        # Try each context to find one where depth and first folder match
        first_folder = parts[0]
        for context in self.context_manager.contexts.values():
            if not context.folders or not context.smart_folders:
                continue
            depth = len(context.folders)
            if len(parts) == depth + 1 and first_folder == context.name:
                leaf_folder = file_path.parent
                self.manager.handle_file_deleted(file_path, leaf_folder, context)
                return

    async def _handle_unrecognized_file(self, file_path: Path) -> None:
        """
        Handle a file that wasn't recognized as a known leaf file.

        Flow:
        1. Compute hash, look for existing DB record (copy detection)
        2. If hash matches → _handle_copy
        3. If non-leaf → _handle_unknown_file(is_leaf=False)
        4. If leaf but unknown → _handle_unknown_file(is_leaf=True)
        """
        from db import compute_sha256

        username = self.manager.username
        db = self.manager.db

        # Step 1: Copy detection via hash
        if db:
            try:
                file_hash = compute_sha256(file_path)

                # Check documents table
                record = await db.get_by_hash(file_hash, username)
                if not record:
                    matches = await db.find_matches_by_hash(file_hash, username)
                    if matches:
                        record = matches[0]

                # Also check file_locations
                if not record:
                    record = await db.find_document_by_location_hash(file_hash, username)

                if record:
                    # Hash matches an existing document — this is a copy
                    await self._handle_copy(file_path, record, file_hash)
                    return
            except Exception as e:
                logger.debug("[%s] Hash lookup failed for %s: %s", username, file_path.name, e)

        # Step 2: Non-leaf analysis
        analysis = self._analyze_non_leaf_file(file_path)
        if analysis:
            await self._handle_unknown_file(file_path, analysis, is_leaf=False)
            return

        # Step 3: Unknown leaf analysis
        analysis = self._analyze_unknown_leaf_file(file_path)
        if analysis:
            await self._handle_unknown_file(file_path, analysis, is_leaf=True)

    async def _handle_copy(
        self, file_path: Path, existing_record: dict[str, Any], file_hash: str
    ) -> None:
        """
        Handle a file that is a copy of an existing document.

        If the copy is in a consistent branch → just register the location.
        If inconsistent → update metadata to match new branch, delete old conflicting copies.
        """
        username = self.manager.username
        db = self.manager.db
        if not db:
            return

        doc_id = existing_record["id"]
        existing_metadata = _ensure_dict(existing_record.get("metadata", {}))

        logger.info(
            "[%s] Copy detected: %s (matches document %s)",
            username, file_path.name, doc_id,
        )

        # Get branch metadata from new file's location
        branch = self._extract_branch_metadata(file_path)
        if not branch:
            # Can't determine branch — just register location
            await db.add_file_location(doc_id, str(file_path), file_hash)
            return

        context: ContextConfig = branch["context"]
        new_inferred: dict[str, str] = branch["inferred_fields"]

        # Check consistency: do all inferred fields match existing metadata?
        is_consistent = all(
            existing_metadata.get(field) == value
            for field, value in new_inferred.items()
            if existing_metadata.get(field) is not None
        )

        # Register the new location
        await db.add_file_location(doc_id, str(file_path), file_hash)

        if is_consistent:
            logger.info("[%s] Copy is consistent with existing metadata, keeping all copies", username)
            return

        # Inconsistent — update metadata to match new branch
        logger.info(
            "[%s] Copy is in different branch, updating metadata: %s",
            username, new_inferred,
        )

        updated_metadata = dict(existing_metadata)
        for field, value in new_inferred.items():
            updated_metadata[field] = value
        updated_metadata = _cleanup_metadata(updated_metadata, context)

        await db.update_metadata(doc_id, updated_metadata, context_name=context.name)

        # Recompute assigned_filename
        orig_name = existing_record.get("original_filename")
        pattern = context.resolve_filename_pattern(orig_name)
        assigned_filename = _format_filename(updated_metadata, pattern, source_filename=orig_name)
        await db.update_assigned_filename(doc_id, assigned_filename)

        # Check all other locations for consistency — delete conflicting ones
        locations = await db.get_locations_for_document(doc_id)
        for loc in locations:
            loc_path = Path(loc["file_path"])
            if str(loc_path) == str(file_path):
                continue  # Skip the new copy

            loc_branch = self._extract_branch_metadata(loc_path)
            if loc_branch:
                loc_inferred = loc_branch["inferred_fields"]
                # Check if this location's branch matches the NEW metadata
                loc_consistent = all(
                    updated_metadata.get(field) == value
                    for field, value in loc_inferred.items()
                    if updated_metadata.get(field) is not None
                )
                if not loc_consistent:
                    # Move conflicting copy to void
                    logger.info(
                        "[%s] Moving conflicting copy to void: %s", username, loc_path
                    )
                    try:
                        if loc_path.exists():
                            if loc_branch["is_leaf"]:
                                for ctx in self.context_manager.contexts.values():
                                    if ctx.smart_folders:
                                        self.manager.handle_file_deleted(
                                            loc_path, loc_path.parent, ctx
                                        )
                            _move_to_void(loc_path, self.manager.user_root, username)
                    except Exception as e:
                        logger.warning("[%s] Failed to move conflicting copy %s to void: %s", username, loc_path, e)
                    await db.remove_file_location(doc_id, str(loc_path))

    async def _handle_internal_move(self, src_path: Path, dest_path: Path) -> None:
        """
        Handle a file moved within sorted/ (user moved it between branches).

        Updates file_locations, metadata, assigned_filename, and smart folder symlinks.
        """
        from db import compute_sha256

        username = self.manager.username
        db = self.manager.db
        if not db:
            return

        logger.info("[%s] Internal move: %s -> %s", username, src_path.name, dest_path.name)

        # Update file_locations path
        await db.update_file_location_path(str(src_path), str(dest_path))

        # Look up the document
        record = await db.get_document_by_location(str(dest_path))
        if not record:
            record = await _lookup_metadata_from_db(db, dest_path, username)
        if not record:
            # Unknown file — new arrival (e.g. syncthing sync) or user drop.
            # Process through the standard new-file flow.
            logger.info("[%s] No DB record for moved file, processing as new: %s", username, dest_path.name)
            await self._handle_unrecognized_file(dest_path)
            return

        doc_id = record["id"]
        metadata = dict(_ensure_dict(record.get("metadata")))

        # Update current_file_path if it pointed to the old path
        if record.get("current_file_path") == str(src_path):
            await db.update_file_path(doc_id, str(dest_path))

        # Extract new branch metadata from dest
        branch = self._extract_branch_metadata(dest_path)
        if not branch:
            return

        context: ContextConfig = branch["context"]
        new_inferred: dict[str, str] = branch["inferred_fields"]

        # Update folder-derived metadata fields (keep non-folder fields unchanged)
        for field, value in new_inferred.items():
            metadata[field] = value
        metadata = _cleanup_metadata(metadata, context)

        await db.update_metadata(doc_id, metadata, context_name=context.name)

        # Recompute assigned_filename (informational — no rename on moves)
        orig_name = record.get("original_filename")
        pattern = context.resolve_filename_pattern(orig_name)
        assigned_filename = _format_filename(metadata, pattern, source_filename=orig_name)
        await db.update_assigned_filename(doc_id, assigned_filename)

        # Clean up old symlinks for source
        src_branch = self._extract_branch_metadata(src_path)
        if src_branch and src_branch["is_leaf"]:
            for ctx in self.context_manager.contexts.values():
                if ctx.smart_folders:
                    self.manager.handle_file_deleted(src_path, src_path.parent, ctx)

        # Create new symlinks for destination
        if branch["is_leaf"]:
            dest_result = await self._get_context_and_leaf(dest_path)
            if dest_result:
                ctx, leaf_folder, meta = dest_result
                self.manager.process_file(dest_path, leaf_folder, ctx, fields=metadata)

        # Remove other copies under sorted/
        sorted_dir = self.manager.sorted_dir
        locations = await db.get_locations_for_document(doc_id)
        for loc in locations:
            loc_path = Path(loc["file_path"])
            if str(loc_path) == str(dest_path):
                continue
            try:
                loc_path.relative_to(sorted_dir)
            except ValueError:
                continue  # Not under sorted/ — leave it
            logger.info("[%s] Moving other copy to void after move: %s", username, loc_path)
            try:
                if loc_path.exists():
                    loc_branch = self._extract_branch_metadata(loc_path)
                    if loc_branch and loc_branch["is_leaf"]:
                        for ctx in self.context_manager.contexts.values():
                            if ctx.smart_folders:
                                self.manager.handle_file_deleted(loc_path, loc_path.parent, ctx)
                    _move_to_void(loc_path, self.manager.user_root, username)
            except Exception as e:
                logger.warning("[%s] Failed to move copy %s to void: %s", username, loc_path, e)
            await db.remove_file_location(doc_id, str(loc_path))

    def _extract_branch_metadata(self, file_path: Path) -> Optional[dict[str, Any]]:
        """
        Extract metadata from a file's folder position in sorted/.

        Returns dict with context, context_name, inferred_fields, is_leaf
        or None if the file can't be analyzed (not in a context folder).
        """
        try:
            relative = file_path.relative_to(self.manager.sorted_dir)
        except ValueError:
            return None

        parts = relative.parts
        if len(parts) < 2:  # Need at least one folder + filename
            return None

        folder_parts = parts[:-1]

        # First folder component should match a context name
        context_name_candidate = folder_parts[0]
        context = self.context_manager.get_context(context_name_candidate)
        if not context or not context.folders:
            return None

        expected_depth = len(context.folders)
        current_depth = len(folder_parts)

        # Map folder components to field names
        inferred_fields: dict[str, str] = {}
        for i, folder_value in enumerate(folder_parts):
            if i < len(context.folders):
                field_name = context.folders[i]
                inferred_fields[field_name] = folder_value

        is_leaf = current_depth >= expected_depth

        return {
            "context": context,
            "context_name": context.name,
            "inferred_fields": inferred_fields,
            "is_leaf": is_leaf,
        }

    def _analyze_non_leaf_file(self, file_path: Path) -> Optional[dict[str, Any]]:
        """
        Analyze a file at a non-leaf depth in sorted/.

        Returns analysis dict or None if the file is at/beyond leaf depth.
        """
        result = self._extract_branch_metadata(file_path)
        if result is None:
            return None
        if result["is_leaf"]:
            return None

        context: ContextConfig = result["context"]
        missing_fields = [
            f for f in context.folders
            if f not in result["inferred_fields"]
        ]
        result["missing_fields"] = missing_fields
        return result

    def _analyze_unknown_leaf_file(self, file_path: Path) -> Optional[dict[str, Any]]:
        """
        Analyze a file at leaf depth in sorted/ that has no DB record.

        Returns analysis dict or None if the file is not at leaf depth.
        """
        result = self._extract_branch_metadata(file_path)
        if result is None:
            return None
        if not result["is_leaf"]:
            return None
        return result

    async def _handle_unknown_file(
        self, file_path: Path, analysis: dict[str, Any], is_leaf: bool
    ) -> None:
        """
        Handle a file in sorted/ that has no DB record (or needs reprocessing).

        Works for both non-leaf files (need sorting) and leaf-depth files (already placed).
        Calls mrdocument for metadata extraction, archives original, writes processed output,
        and sorts to the correct leaf (or stays in place if already at leaf).
        """
        from db import compute_sha256, compute_sha256_bytes

        username = self.manager.username
        context: ContextConfig = analysis["context"]
        context_name: str = analysis["context_name"]
        inferred_fields: dict[str, str] = analysis["inferred_fields"]
        db = self.manager.db

        if not db:
            logger.warning("[%s] No database — cannot handle unknown file: %s", username, file_path.name)
            return

        if not self.sorter:
            logger.warning("[%s] No sorter — cannot handle unknown file: %s", username, file_path.name)
            return

        logger.info(
            "[%s] Unknown file detected (%s): %s (inferred: %s)",
            username, "leaf" if is_leaf else "non-leaf", file_path.name, inferred_fields,
        )

        # a) DB record lookup or creation
        record = await _lookup_metadata_from_db(db, file_path, username)
        from_pipeline = False
        if record:
            # Guard: if already sorted with complete folder fields → skip (loop avoidance)
            if record.get("status") == "sorted":
                existing_meta = _ensure_dict(record.get("metadata", {}))
                all_folder_fields_present = all(
                    existing_meta.get(f) for f in context.folders
                )
                if all_folder_fields_present:
                    logger.debug(
                        "[%s] Skipping already-sorted file: %s", username, file_path.name
                    )
                    # Backfill file_locations if not present
                    file_hash = compute_sha256(file_path)
                    await db.add_file_location(record["id"], str(file_path), file_hash)
                    return

            # Check if the previous output file is still physically available
            if record.get("status") in ("processed", "reviewed", "sorted"):
                previous_exists = await _check_previous_file_exists(db, record)
                if previous_exists:
                    # Previous file still exists → this is a duplicate
                    logger.info(
                        "[%s] Previous file for DB entry %s still exists — moving to duplicates: %s",
                        username, record["id"], file_path.name,
                    )
                    _move_file_to_duplicates(
                        file_path, self.sorter.user_root,
                        self.sorter.config.duplicates_folder, username,
                    )
                    return
                else:
                    # Previous file is missing → clear DB entry, process from scratch
                    logger.info(
                        "[%s] Previous file for DB entry %s is missing — clearing entry and reprocessing: %s",
                        username, record["id"], file_path.name,
                    )
                    await db.delete_document(record["id"])
                    record = None

        if record:
            doc_id = record["id"]
            metadata = _ensure_dict(record.get("metadata", {}))
            # File came through the pipeline if it has reviewed/processed status
            from_pipeline = record.get("status") in ("reviewed", "processed", "sorted")
        else:
            file_hash = compute_sha256(file_path)
            doc_id = await db.insert_document(
                username=username,
                original_filename=file_path.name,
                original_file_hash=file_hash,
                current_file_path=str(file_path),
                current_file_hash=file_hash,
                status="incoming",
            )
            metadata = {}

        # b) Merge inferred fields into existing metadata
        for field_name, field_value in inferred_fields.items():
            if not metadata.get(field_name):
                metadata[field_name] = field_value

        metadata["context"] = context_name
        metadata = _cleanup_metadata(metadata, context)

        # Update DB with merged metadata
        await db.update_status(
            doc_id, "incoming",
            metadata=metadata,
            context_name=context_name,
        )

        # c) Check completeness
        filename_pattern = context.resolve_filename_pattern(file_path.name)
        is_complete = not _has_missing_fields(metadata, context)
        all_folder_fields = all(metadata.get(f) for f in context.folders)

        if is_complete and all_folder_fields and not is_leaf and not _is_audio_file(file_path):
            # Metadata already complete — just sort to the correct leaf
            logger.info(
                "[%s] Metadata complete for non-leaf file, sorting directly: %s",
                username, file_path.name,
            )
            self._resorting_paths.add(str(file_path))
            try:
                target_path = await self.sorter.sort_file(file_path)
                if target_path:
                    file_hash = compute_sha256(target_path)
                    await db.add_file_location(doc_id, str(target_path), file_hash)
                    # Compute and store assigned_filename
                    assigned = _format_filename(metadata, filename_pattern, source_filename=file_path.name)
                    await db.update_assigned_filename(doc_id, assigned)
            finally:
                self._resorting_paths.discard(str(file_path))
            return

        # d) Need mrdocument processing (incomplete metadata, or at leaf needing extraction)
        # Audio files use the STT pipeline instead of /process
        if _is_audio_file(file_path):
            if not self.stt_url:
                logger.warning(
                    "[%s] No STT_URL configured — cannot process audio file: %s",
                    username, file_path.name,
                )
                return
            if not self.mrdocument_url:
                logger.warning(
                    "[%s] No MRDOCUMENT_URL — cannot process audio file: %s",
                    username, file_path.name,
                )
                return

            from watcher import SttConfig
            stt_config = SttConfig.load(self.sorter.user_root)
            if not stt_config:
                logger.warning(
                    "[%s] No stt.yaml found — cannot process audio file: %s",
                    username, file_path.name,
                )
                return

            # Build locked_fields from inferred fields
            locked_fields = {
                field_name: {"value": field_value, "clues": []}
                for field_name, field_value in inferred_fields.items()
            }

            # Load full context YAML for the API
            context_yaml = self.context_manager.get_context_for_api(context_name)
            if not context_yaml:
                logger.error(
                    "[%s] Could not load full context YAML for '%s'", username, context_name
                )
                return
            contexts = [context_yaml]

            keyterms = _collect_keyterms_from_context(context_yaml, inferred_fields)

            result = await _call_stt_and_process_transcript(
                file_path, self.stt_url, self.mrdocument_url, stt_config,
                contexts, transcription_keyterms=keyterms or None,
                locked_fields=locked_fields, username=username,
                user_dir=str(self.sorter.user_root),
                is_intro="intro" in file_path.name.lower(),
            )
            if not result:
                logger.error(
                    "[%s] Audio processing failed for file: %s",
                    username, file_path.name,
                )
                return

            api_metadata, text_content, corrected_json = result
            suggested_filename = api_metadata.pop("_suggested_filename", "transcript.pdf")
            api_metadata = _cleanup_metadata(api_metadata, context)

            # Update DB with metadata
            await db.update_status(
                doc_id, "processed",
                metadata=api_metadata,
                context_name=api_metadata.get("context", context_name),
                processed_at_now=True,
            )

            # Compute assigned_filename (TXT)
            assigned_filename = _format_filename(api_metadata, filename_pattern, source_filename=file_path.name)
            # Ensure .txt extension
            if assigned_filename.endswith(".pdf"):
                assigned_filename = assigned_filename[:-4] + ".txt"
            elif not assigned_filename.endswith(".txt"):
                assigned_filename = assigned_filename.rsplit(".", 1)[0] + ".txt" if "." in assigned_filename else assigned_filename + ".txt"
            await db.update_assigned_filename(doc_id, assigned_filename)

            # Determine output filename
            keep_original_name = from_pipeline or self.sorter.config.migration
            if keep_original_name:
                output_filename = file_path.stem + ".txt"
            else:
                output_filename = assigned_filename

            # Archive original audio under its original filename
            has_existing_archive = record and record.get("original_file_hash")
            if not has_existing_archive:
                archive_dir = self.sorter.user_root / "archive"
                archive_dir.mkdir(parents=True, exist_ok=True)

                archive_name = file_path.name
                archive_path = archive_dir / archive_name
                if archive_path.exists():
                    stem = file_path.stem
                    suffix = file_path.suffix
                    unique_id = uuid.uuid4().hex[:8]
                    archive_name = f"{stem}_{unique_id}{suffix}"
                    archive_path = archive_dir / archive_name

                try:
                    file_path.rename(archive_path)
                    logger.info("[%s] Archived original audio: %s -> %s", username, file_path.name, archive_name)
                    await db.update_original_filename(doc_id, archive_name)
                except Exception as e:
                    logger.error("[%s] Failed to archive audio %s: %s", username, file_path.name, e)
                    return

                # Also write corrected JSON to archive
                json_name = Path(archive_name).stem + ".json"
                json_path = archive_dir / json_name
                if json_path.exists():
                    unique_id = uuid.uuid4().hex[:8]
                    json_name = Path(archive_name).stem + f"_{unique_id}.json"
                    json_path = archive_dir / json_name
                try:
                    json_path.write_text(
                        json.dumps(corrected_json, indent=2, ensure_ascii=False),
                        encoding="utf-8",
                    )
                    logger.info("[%s] Archived corrected JSON: %s", username, json_name)
                    # Track JSON as associated file in DB
                    try:
                        json_hash = compute_sha256(json_path)
                        await db.add_file_location(doc_id, str(json_path), json_hash)
                    except Exception as e2:
                        logger.warning("[%s] DB file_location for JSON failed: %s", username, e2)
                except Exception as e:
                    logger.error("[%s] Failed to write corrected JSON %s: %s", username, json_name, e)
            else:
                # Original already archived — move duplicate to void
                if not _move_to_void(file_path, self.manager.user_root, username):
                    return

            # Write TXT output only (no PDF for transcripts)
            if is_leaf:
                # Already at correct leaf — write output in place
                text_path = file_path.parent / output_filename
                try:
                    text_path.write_text(text_content, encoding="utf-8")
                except Exception as e:
                    logger.error("[%s] Failed to write text %s: %s", username, output_filename, e)
                    return

                text_hash = compute_sha256(text_path)
                await db.update_file_path(doc_id, str(text_path), new_hash=text_hash, new_status="sorted")
                await db.add_file_location(doc_id, str(text_path), text_hash)

                # Update smart folder symlinks
                self.manager.process_file(text_path, text_path.parent, context, fields=api_metadata)

                logger.info("[%s] Processed audio at leaf: %s", username, text_path.name)
            else:
                # Write to same directory, then sort
                text_path = file_path.parent / output_filename
                try:
                    text_path.write_text(text_content, encoding="utf-8")
                except Exception as e:
                    logger.error("[%s] Failed to write text %s: %s", username, output_filename, e)
                    return

                text_hash = compute_sha256(text_path)
                await db.update_file_path(doc_id, str(text_path), new_hash=text_hash)

                # Sort TXT to leaf
                self._resorting_paths.add(str(text_path))
                try:
                    target_path = await self.sorter.sort_file(text_path)
                    if target_path:
                        final_hash = compute_sha256(target_path)
                        await db.add_file_location(doc_id, str(target_path), final_hash)
                        self.manager.process_file(target_path, target_path.parent, context, fields=api_metadata)
                finally:
                    self._resorting_paths.discard(str(text_path))

            return

        if not self.mrdocument_url:
            logger.warning(
                "[%s] No mrdocument URL — cannot process file: %s",
                username, file_path.name,
            )
            return

        # Build locked_fields from inferred fields
        locked_fields = {
            field_name: {"value": field_value, "clues": []}
            for field_name, field_value in inferred_fields.items()
        }

        # Load full context YAML for the API
        context_yaml = self.context_manager.get_context_for_api(context_name)
        if not context_yaml:
            logger.error(
                "[%s] Could not load full context YAML for '%s'", username, context_name
            )
            return
        contexts = [context_yaml]

        api_result = await _call_mrdocument(
            file_path,
            self.mrdocument_url,
            self.ocr_language,
            contexts,
            locked_fields=locked_fields,
            username=username,
        )

        if not api_result:
            logger.error(
                "[%s] mrdocument processing failed for file: %s",
                username, file_path.name,
            )
            return

        api_metadata, output_bytes, output_ext = api_result
        api_metadata = _cleanup_metadata(api_metadata, context)

        # Update DB with mrdocument metadata
        await db.update_status(
            doc_id, "processed",
            metadata=api_metadata,
            context_name=api_metadata.get("context", context_name),
            processed_at_now=True,
        )

        # Compute assigned_filename
        assigned_filename = _format_filename(api_metadata, filename_pattern, source_filename=file_path.name)
        if output_ext == ".docx" and assigned_filename.endswith(".pdf"):
            assigned_filename = assigned_filename[:-4] + ".docx"
        await db.update_assigned_filename(doc_id, assigned_filename)

        # Determine output filename: keep original or use assigned
        # From pipeline or migration mode → keep original name
        keep_original_name = from_pipeline or self.sorter.config.migration
        if keep_original_name:
            output_filename = file_path.stem + output_ext
        else:
            output_filename = assigned_filename

        # e) Archive original
        has_existing_archive = record and record.get("original_file_hash")
        if not has_existing_archive:
            archive_dir = self.sorter.user_root / "archive"
            archive_dir.mkdir(parents=True, exist_ok=True)

            archive_name = file_path.name
            archive_path = archive_dir / archive_name
            if archive_path.exists():
                stem = file_path.stem
                suffix = file_path.suffix
                unique_id = uuid.uuid4().hex[:8]
                archive_name = f"{stem}_{unique_id}{suffix}"
                archive_path = archive_dir / archive_name

            try:
                file_path.rename(archive_path)
                logger.info("[%s] Archived original: %s -> %s", username, file_path.name, archive_name)
                await db.update_original_filename(doc_id, archive_name)
            except Exception as e:
                logger.error("[%s] Failed to archive %s: %s", username, file_path.name, e)
                return
        else:
            # Original already archived — move duplicate to void
            if not _move_to_void(file_path, self.manager.user_root, username):
                return

        # f) Write processed output
        if is_leaf:
            # Already at correct leaf — write output in place
            output_path = file_path.parent / output_filename
            try:
                output_path.write_bytes(output_bytes)
            except Exception as e:
                logger.error("[%s] Failed to write processed output %s: %s", username, output_filename, e)
                return

            new_hash = compute_sha256_bytes(output_bytes)
            await db.update_file_path(doc_id, str(output_path), new_hash=new_hash, new_status="sorted")
            await db.add_file_location(doc_id, str(output_path), new_hash)

            # Update smart folder symlinks
            self.manager.process_file(output_path, output_path.parent, context, fields=api_metadata)

            logger.info("[%s] Processed leaf file in place: %s", username, output_path.name)
        else:
            # Write to the same directory where the original was, then sort
            output_path = file_path.parent / output_filename
            try:
                output_path.write_bytes(output_bytes)
            except Exception as e:
                logger.error("[%s] Failed to write processed output %s: %s", username, output_filename, e)
                return

            # Update DB with new file info
            new_hash = compute_sha256_bytes(output_bytes)
            await db.update_file_path(doc_id, str(output_path), new_hash=new_hash)

            # Sort to leaf position
            self._resorting_paths.add(str(output_path))
            try:
                target_path = await self.sorter.sort_file(output_path)
                if target_path:
                    final_hash = compute_sha256(target_path)
                    await db.add_file_location(doc_id, str(target_path), final_hash)
                    # Update smart folder symlinks
                    self.manager.process_file(target_path, target_path.parent, context, fields=api_metadata)
            finally:
                self._resorting_paths.discard(str(output_path))

    async def _process_new_directory(self, dir_path: Path):
        """Process a new directory and all files within it recursively."""
        await asyncio.sleep(1)  # Wait for directory to stabilize

        if not dir_path.exists() or not dir_path.is_dir():
            return

        try:
            relative = dir_path.relative_to(self.manager.sorted_dir)
        except ValueError:
            return

        parts = relative.parts
        if not parts:
            return

        depth = len(parts)
        username = self.manager.username

        # Check each context to see if this could be a leaf folder for it
        first_folder = parts[0]
        for context in self.context_manager.contexts.values():
            if not context.folders or not context.smart_folders:
                continue

            # Check if depth matches and first folder matches context name
            if depth == len(context.folders) and first_folder == context.name:
                logger.info(
                    "[%s] New potential leaf folder detected: %s",
                    username, relative
                )
                self.manager.ensure_smart_folder_dirs(dir_path, context)
                # Process any existing files
                await self.manager.process_leaf_folder(dir_path, context)
                break

        # Recursively process all files in the new directory tree
        file_count = 0
        for root, dirs, files in os.walk(dir_path):
            dirs[:] = [d for d in dirs if not d.startswith(".")]
            for fname in files:
                if fname.startswith(".") or fname.startswith("~"):
                    continue
                if ".syncthing." in fname or fname.endswith(".tmp"):
                    continue
                fpath = Path(root) / fname
                if fpath.is_symlink():
                    continue
                try:
                    await self._process_file_change(fpath)
                    file_count += 1
                except Exception as e:
                    logger.error(
                        "[%s] Error processing %s in new directory: %s",
                        username, fpath.name, e,
                    )
        if file_count:
            logger.info(
                "[%s] Processed %d file(s) in new directory: %s",
                username, file_count, relative,
            )


# =============================================================================
# File Watcher
# =============================================================================


class ReviewedFileHandler(FileSystemEventHandler):
    """Watches for new files in the reviewed folder."""

    def __init__(self, sorter: FileSorter, loop: asyncio.AbstractEventLoop):
        self.sorter = sorter
        self.loop = loop
        self.processing: set[str] = set()

    def _skip_reason(self, file_path: Path) -> str | None:
        """Return None if file should be processed, or a reason string if skipped."""
        if file_path.name.startswith(".") or file_path.name.startswith("~"):
            return "hidden file"
        if ".syncthing." in file_path.name or file_path.name.endswith(".tmp"):
            return "syncthing temp file"
        if not file_path.is_file():
            return "not a file"
        return None

    def on_created(self, event):
        if event.is_directory:
            return
        file_path = Path(event.src_path)
        skip = self._skip_reason(file_path)
        if skip:
            logger.info("[reviewed] FS event CREATED file=%s -> skip (%s)", file_path.name, skip)
            return
        logger.info("[reviewed] FS event CREATED file=%s -> processing (wait for stable)", file_path.name)
        asyncio.run_coroutine_threadsafe(
            self._process_after_stable(file_path), self.loop
        )

    def on_moved(self, event):
        if event.is_directory:
            return
        file_path = Path(event.dest_path)
        skip = self._skip_reason(file_path)
        if skip:
            logger.info("[reviewed] FS event MOVED file=%s -> skip (%s)", file_path.name, skip)
            return
        logger.info("[reviewed] FS event MOVED file=%s -> processing (immediate)", file_path.name)
        asyncio.run_coroutine_threadsafe(
            self._process_immediately(file_path), self.loop
        )

    async def _process_immediately(self, file_path: Path):
        """Process file immediately (already complete, e.g. atomic rename)."""
        file_key = str(file_path)

        if file_key in self.processing:
            return

        self.processing.add(file_key)
        try:
            self.sorter.reload_contexts()
            await self.sorter.sort_file(file_path)
        finally:
            self.processing.discard(file_key)

    async def _process_after_stable(self, file_path: Path):
        """Process file after its size stabilises (for non-atomic writes)."""
        file_key = str(file_path)

        if file_key in self.processing:
            return

        self.processing.add(file_key)
        try:
            prev_size = -1
            for _ in range(50):  # 50 * 200ms = 10s timeout
                if not file_path.exists():
                    logger.debug("File no longer exists: %s", file_path.name)
                    return
                cur_size = file_path.stat().st_size
                if cur_size == prev_size:
                    break
                prev_size = cur_size
                await asyncio.sleep(0.2)

            self.sorter.reload_contexts()
            await self.sorter.sort_file(file_path)
        finally:
            self.processing.discard(file_key)


# =============================================================================
# Config Change Handling
# =============================================================================


def _cleanup_empty_dirs(dirs_to_check: list[Path], stop_at: Path) -> None:
    """
    Walk up from each directory, removing empty directories until stop_at.
    """
    for dir_path in dirs_to_check:
        current = dir_path
        while current != stop_at and current.is_relative_to(stop_at):
            try:
                if current.is_dir() and not any(current.iterdir()):
                    current.rmdir()
                    logger.debug("Removed empty directory: %s", current)
                else:
                    break
            except Exception:
                break
            current = current.parent


async def _handle_config_changes(
    sorter: FileSorter,
    old_contexts: dict[str, ContextConfig],
    new_contexts: dict[str, ContextConfig],
    sorted_handlers: list["SmartFolderHandler"],
    username: str,
) -> None:
    """
    Detect and handle config changes between old and new contexts.

    - New fields added (migration=True only): backfill as null
    - Folders changed: resort all documents of that context
    """
    db = sorter.db
    if not db:
        return

    for ctx_key, new_ctx in new_contexts.items():
        old_ctx = old_contexts.get(ctx_key)
        if not old_ctx:
            continue  # New context entirely — no migration needed

        # 8a: New fields added
        if sorter.config.migration:
            new_field_names = set(new_ctx.field_names) - set(old_ctx.field_names)
            if new_field_names:
                logger.info(
                    "[%s] New fields detected for context '%s': %s (migration backfill)",
                    username, new_ctx.name, new_field_names,
                )
                documents = await db.get_documents_by_context(username, new_ctx.name)
                for doc in documents:
                    metadata = dict(_ensure_dict(doc.get("metadata", {})))
                    updated = False
                    for field_name in new_field_names:
                        if field_name not in metadata:
                            metadata[field_name] = None
                            updated = True
                    if updated:
                        metadata = _cleanup_metadata(metadata, new_ctx)
                        await db.update_metadata(doc["id"], metadata, context_name=new_ctx.name)

        # 8b: Folders changed → resort
        if old_ctx.folders != new_ctx.folders:
            logger.info(
                "[%s] Folders changed for context '%s': %s -> %s",
                username, new_ctx.name, old_ctx.folders, new_ctx.folders,
            )
            # Find the sorted handler for this user
            handler = None
            for h in sorted_handlers:
                if h.manager.user_root == sorter.user_root:
                    handler = h
                    break
            await _resort_context(sorter, new_ctx, handler, username)


async def _resort_context(
    sorter: FileSorter,
    context: ContextConfig,
    handler: Optional["SmartFolderHandler"],
    username: str,
) -> None:
    """
    Re-sort all documents of a context after folder config changes.

    For each document:
    - If all folder fields present and non-null → move to new sorted/ location
    - If any folder field null or absent → move to unsortable/
    """
    db = sorter.db
    if not db:
        return

    documents = await db.get_documents_by_context(username, context.name)
    if not documents:
        return

    logger.info(
        "[%s] Re-sorting %d document(s) for context '%s'",
        username, len(documents), context.name,
    )

    dirs_to_check: list[Path] = []

    for doc in documents:
        doc_id = doc["id"]
        metadata = _ensure_dict(doc.get("metadata", {}))

        # Find physical files
        locations = await db.get_locations_for_document(doc_id)
        file_paths: list[str] = []
        if locations:
            file_paths = [loc["file_path"] for loc in locations]
        elif doc.get("current_file_path"):
            file_paths = [doc["current_file_path"]]

        if not file_paths:
            continue

        # Determine if all folder fields are present and non-null
        all_folder_fields_ok = all(
            metadata.get(f) for f in context.folders
        ) if context.folders else False

        for fp_str in file_paths:
            fp = Path(fp_str)
            if not fp.exists():
                continue

            old_parent = fp.parent

            if all_folder_fields_ok:
                # Compute new target from new folders config
                folder_parts = []
                for folder_field in context.folders:
                    value = metadata.get(folder_field)
                    sanitized = FileSorter._sanitize_folder_name(str(value))
                    folder_parts.append(sanitized)

                target_dir = sorter.sorted_dir
                for part in folder_parts:
                    target_dir = target_dir / part
                target_dir.mkdir(parents=True, exist_ok=True)

                target_path = target_dir / fp.name
                if target_path.exists() and target_path != fp:
                    stem = fp.stem
                    suffix = fp.suffix
                    unique_id = uuid.uuid4().hex[:8]
                    target_path = target_dir / f"{stem}_{unique_id}{suffix}"

                if target_path != fp:
                    # Remove old smart folder symlinks before moving
                    if handler and context.smart_folders:
                        handler.manager.handle_file_deleted(fp, old_parent, context)

                    if handler:
                        handler._resorting_paths.add(str(fp))
                        handler._resorting_paths.add(str(target_path))

                    try:
                        fp.rename(target_path)
                        logger.info("[%s] Re-sorted: %s -> %s", username, fp.name, target_path)
                    except Exception as e:
                        logger.error("[%s] Failed to re-sort %s: %s", username, fp.name, e)
                        if handler:
                            handler._resorting_paths.discard(str(fp))
                            handler._resorting_paths.discard(str(target_path))
                        continue

                    # Update file_locations and current_file_path
                    await db.update_file_location_path(str(fp), str(target_path))
                    if doc.get("current_file_path") == str(fp):
                        await db.update_file_path(doc_id, str(target_path), new_status="sorted")

                    dirs_to_check.append(old_parent)

                    if handler:
                        handler._resorting_paths.discard(str(fp))
                        handler._resorting_paths.discard(str(target_path))
            else:
                # Missing folder field → move to unsortable
                unsortable_dir = sorter.user_root / "unsortable"
                unsortable_dir.mkdir(parents=True, exist_ok=True)

                target_path = unsortable_dir / fp.name
                if target_path.exists() and target_path != fp:
                    stem = fp.stem
                    suffix = fp.suffix
                    unique_id = uuid.uuid4().hex[:8]
                    target_path = unsortable_dir / f"{stem}_{unique_id}{suffix}"

                if target_path != fp:
                    # Remove old smart folder symlinks
                    if handler and context.smart_folders:
                        handler.manager.handle_file_deleted(fp, old_parent, context)

                    if handler:
                        handler._resorting_paths.add(str(fp))
                        handler._resorting_paths.add(str(target_path))

                    try:
                        fp.rename(target_path)
                        logger.info("[%s] Moved to unsortable: %s", username, fp.name)
                    except Exception as e:
                        logger.error("[%s] Failed to move %s to unsortable: %s", username, fp.name, e)
                        if handler:
                            handler._resorting_paths.discard(str(fp))
                            handler._resorting_paths.discard(str(target_path))
                        continue

                    await db.update_file_location_path(str(fp), str(target_path))
                    if doc.get("current_file_path") == str(fp):
                        await db.update_file_path(doc_id, str(target_path), new_status="unsortable")

                    dirs_to_check.append(old_parent)

                    if handler:
                        handler._resorting_paths.discard(str(fp))
                        handler._resorting_paths.discard(str(target_path))

    # Clean up empty directories
    if dirs_to_check:
        _cleanup_empty_dirs(dirs_to_check, sorter.sorted_dir)

    # Re-evaluate smart folders for new leaf structure
    if handler:
        try:
            await process_existing_smart_folders(handler.manager)
        except Exception as e:
            logger.error("[%s] Failed to re-evaluate smart folders after re-sort: %s", username, e)


# =============================================================================
# Config File Watcher
# =============================================================================


class ConfigFileHandler(FileSystemEventHandler):
    """
    Watches for changes to YAML configuration files and triggers reload.

    Monitors:
      - sorted/{context}/context.yaml - Context configuration
      - config.yaml - User configuration

    Uses debouncing to avoid multiple reloads for rapid changes.
    """

    def __init__(
        self,
        sorters: dict[Path, FileSorter],
        smart_folder_managers: list[SmartFolderManager],
        user_root: Path,
        loop: asyncio.AbstractEventLoop,
        sorted_handlers: Optional[list["SmartFolderHandler"]] = None,
        debounce_seconds: float = 1.0,
    ):
        self.sorters = sorters  # Map of reviewed_dir -> FileSorter
        self.smart_folder_managers = smart_folder_managers  # List of SmartFolderManagers for this user
        self.sorted_handlers = sorted_handlers or []
        self.user_root = user_root
        self.username = get_username_from_root(user_root)
        self.loop = loop
        self.debounce_seconds = debounce_seconds
        self._pending_reload: Optional[asyncio.TimerHandle] = None
        self._last_reload_time: float = 0

    def _is_config_file(self, path: Path) -> bool:
        """Check if a path is a configuration file we should watch."""
        # Must be a YAML file
        if path.suffix.lower() not in (".yaml", ".yml"):
            return False
        
        # Must be directly in the user root (not in subfolders)
        if path.parent != self.user_root:
            return False
        
        # Ignore temp files and hidden files
        if path.name.startswith(".") or path.name.startswith("~"):
            return False
        if ".syncthing." in path.name or path.name.endswith(".tmp"):
            return False
        
        return True

    def _schedule_reload(self):
        """Schedule a debounced reload."""
        # Cancel any pending reload
        if self._pending_reload is not None:
            self._pending_reload.cancel()
        
        # Schedule new reload
        self._pending_reload = self.loop.call_later(
            self.debounce_seconds,
            lambda: asyncio.run_coroutine_threadsafe(self._do_reload(), self.loop),
        )

    async def _do_reload(self):
        """Perform the actual reload."""
        self._pending_reload = None
        current_time = time.time()

        # Extra protection against rapid reloads
        if current_time - self._last_reload_time < self.debounce_seconds:
            return

        self._last_reload_time = current_time

        logger.info("[%s] Config change detected, reloading contexts...", self.username)

        # Snapshot old contexts before reload
        old_contexts: dict[str, ContextConfig] = {}
        for reviewed_dir, sorter in self.sorters.items():
            if sorter.user_root == self.user_root:
                old_contexts = dict(sorter.context_manager.contexts)
                break

        # Reload contexts for all sorters associated with this user
        reload_count = 0
        for reviewed_dir, sorter in self.sorters.items():
            if sorter.user_root == self.user_root:
                try:
                    if sorter.reload_contexts():
                        reload_count += 1
                except Exception as e:
                    logger.error("[%s] Failed to reload contexts: %s", self.username, e)

        if reload_count > 0:
            logger.info("[%s] Reloaded contexts for %d sorter(s)", self.username, reload_count)

        # Detect config changes and handle them
        for reviewed_dir, sorter in self.sorters.items():
            if sorter.user_root == self.user_root:
                new_contexts = sorter.context_manager.contexts
                try:
                    await _handle_config_changes(
                        sorter, old_contexts, new_contexts,
                        self.sorted_handlers, self.username,
                    )
                except Exception as e:
                    logger.error("[%s] Failed to handle config changes: %s", self.username, e)
                break

        # Re-evaluate smart folders with new configuration
        for manager in self.smart_folder_managers:
            if manager.user_root == self.user_root:
                try:
                    await process_existing_smart_folders(manager)
                except Exception as e:
                    logger.error("[%s] Failed to re-evaluate smart folders: %s", self.username, e)

    def on_modified(self, event):
        if event.is_directory:
            return
        file_path = Path(event.src_path)
        if self._is_config_file(file_path):
            logger.debug("[%s] Config file modified: %s", self.username, file_path.name)
            self._schedule_reload()

    def on_created(self, event):
        if event.is_directory:
            return
        file_path = Path(event.src_path)
        if self._is_config_file(file_path):
            logger.debug("[%s] Config file created: %s", self.username, file_path.name)
            self._schedule_reload()

    def on_moved(self, event):
        if event.is_directory:
            return
        dest_path = Path(event.dest_path)
        if self._is_config_file(dest_path):
            logger.debug("[%s] Config file moved to: %s", self.username, dest_path.name)
            self._schedule_reload()


# =============================================================================
# Main
# =============================================================================


def load_user_config(user_root: Path) -> UserConfig:
    """Load user configuration from config.yaml."""
    config_path = user_root / "config.yaml"
    if not config_path.exists():
        return UserConfig()
    
    try:
        with open(config_path) as f:
            data = yaml.safe_load(f)
        return UserConfig.from_dict(data)
    except Exception as e:
        logger.warning("Failed to load config from %s: %s", config_path, e)
        return UserConfig()


def _get_watch_parent_dirs(watcher_config: WatcherConfig) -> list[Path]:
    """
    Extract parent directories from watcher config patterns.

    For glob patterns like "/sync/*", returns the parent directory ("/sync/").
    These are the directories to watch with watchdog for new user folders.
    """
    parents: set[Path] = set()

    for pattern in watcher_config.watch_patterns:
        if "*" in pattern:
            parent = Path(pattern.split("*")[0])
            if parent.exists() and parent.is_dir():
                parents.add(parent)
        else:
            path = Path(pattern)
            if path.parent.exists() and path.parent.is_dir():
                parents.add(path.parent)

    if not parents:
        sync = Path("/sync")
        if sync.exists():
            parents.add(sync)

    return sorted(parents)


class NewUserFolderHandler(FileSystemEventHandler):
    """
    Watches parent directories (e.g., /sync/) for new user folders.
    Sets an asyncio.Event to wake the main loop.
    """

    def __init__(self, folder_event: asyncio.Event, loop: asyncio.AbstractEventLoop):
        self.folder_event = folder_event
        self.loop = loop

    def _signal(self):
        self.loop.call_soon_threadsafe(self.folder_event.set)

    def on_created(self, event):
        if event.is_directory:
            logger.debug("New directory detected: %s", event.src_path)
            self._signal()

    def on_moved(self, event):
        if event.is_directory:
            self._signal()


def discover_sorter_folders(watcher_config: WatcherConfig, db: Optional["DocumentDB"] = None) -> list[tuple[Path, FileSorter]]:
    """
    Discover folders to watch for sorting.

    Returns list of (reviewed_dir, sorter) tuples.
    """
    sorters = []

    watch_dirs = watcher_config.get_watch_directories()
    for watch_dir in watch_dirs:
        if not watch_dir.is_dir() or watch_dir.name.startswith("."):
            continue

        # Check for sorted/ directory with context configs
        sorted_dir = watch_dir / "sorted"
        if not sorted_dir.is_dir():
            continue

        # Load user config
        config = load_user_config(watch_dir)

        if not config.enabled:
            logger.info("User disabled in config: %s", watch_dir)
            continue

        # Create sorter
        sorter = FileSorter(watch_dir, config, db=db)
        if not sorter.reload_contexts():
            logger.debug("No valid contexts for sorting in %s", watch_dir)
            continue

        # Ensure directories exist
        reviewed_dir = watch_dir / config.reviewed_folder
        sorted_dir = watch_dir / config.sorted_folder
        reviewed_dir.mkdir(parents=True, exist_ok=True)
        sorted_dir.mkdir(parents=True, exist_ok=True)

        sorters.append((reviewed_dir, sorter))
        logger.info("Sorter watching: %s", reviewed_dir)

    return sorters


def discover_sorted_folder_watchers(
    sorter_folders: list[tuple[Path, FileSorter]],
    db: Optional["DocumentDB"] = None,
) -> list[tuple[Path, SmartFolderManager, "SorterContextManager", FileSorter]]:
    """
    Discover sorted folders to watch for smart folders and non-leaf file handling.

    Creates watchers for all users whose contexts have `folders` defined.
    Returns list of (sorted_dir, smart_folder_manager, context_manager, sorter) tuples.
    """
    result = []

    for reviewed_dir, sorter in sorter_folders:
        # Watch sorted/ for any user that has contexts with folders defined
        has_folders = any(
            ctx.folders for ctx in sorter.context_manager.contexts.values()
        )

        if not has_folders:
            continue

        manager = SmartFolderManager(
            user_root=sorter.user_root,
            sorted_dir=sorter.sorted_dir,
            context_manager=sorter.context_manager,
            db=db,
        )

        result.append((sorter.sorted_dir, manager, sorter.context_manager, sorter))

        # Log which contexts have smart folders
        for ctx in sorter.context_manager.contexts.values():
            if ctx.smart_folders:
                logger.info(
                    "[%s] Smart folders for context '%s': %s",
                    sorter.username,
                    ctx.name,
                    list(ctx.smart_folders.keys()),
                )

    return result


async def process_existing_smart_folders(
    manager: SmartFolderManager,
) -> None:
    """Process existing files in all leaf folders for smart folder symlinks."""
    leaf_folders = manager.get_leaf_folders()

    if not leaf_folders:
        return

    logger.info(
        "[%s] Processing %d existing leaf folder(s) for smart folders",
        manager.username,
        len(leaf_folders),
    )

    for leaf_folder, context in leaf_folders:
        await manager.process_leaf_folder(leaf_folder, context)


async def process_existing_unknown_files(handler: SmartFolderHandler) -> None:
    """
    Walk sorted/ tree on startup, find unknown files (non-leaf and leaf),
    process them through _handle_unknown_file(), and backfill file_locations.
    """
    from db import compute_sha256

    sorted_dir = handler.manager.sorted_dir
    username = handler.manager.username
    db = handler.manager.db

    if not sorted_dir.exists():
        return

    unknown_files: list[tuple[Path, dict[str, Any], bool]] = []  # (path, analysis, is_leaf)
    backfilled_count = 0
    reprocessed_count = 0
    relocated_count = 0
    total_files = 0

    for root, dirs, files in os.walk(sorted_dir):
        # Skip hidden dirs and smart folder subdirs
        dirs[:] = [
            d for d in dirs
            if not d.startswith(".")
            and not any(d in ctx.smart_folders for ctx in handler.context_manager.contexts.values() if ctx.smart_folders)
        ]

        for fname in files:
            if fname.startswith(".") or fname.startswith("~"):
                continue
            if ".syncthing." in fname or fname.endswith(".tmp"):
                continue
            if handler.sorter and handler.sorter.config.should_ignore_file(fname):
                continue

            fpath = Path(root) / fname
            if fpath.is_symlink():
                continue

            total_files += 1

            # Check if file has a DB record
            if db:
                record = await _lookup_metadata_from_db(db, fpath, username)
                if record:
                    # Backfill file_locations for known files
                    try:
                        file_hash = compute_sha256(fpath)
                        _, was_inserted = await db.add_file_location(record["id"], str(fpath), file_hash)
                        if was_inserted:
                            backfilled_count += 1
                    except Exception as e:
                        logger.debug("[%s] Failed to backfill location for %s: %s", username, fpath.name, e)

                    # Migration backfill + reprocessing for known files
                    ctx_name = record.get("context_name")
                    ctx = handler.context_manager.get_context(ctx_name) if ctx_name else None
                    if ctx:
                        metadata = dict(_ensure_dict(record.get("metadata")))
                        updated = False

                        # 7a0: Infer metadata from folder position
                        branch = handler._extract_branch_metadata(fpath)
                        if branch:
                            inferred = branch["inferred_fields"]
                            for field, value in inferred.items():
                                if metadata.get(field) != value:
                                    metadata[field] = value
                                    updated = True
                            if branch["context_name"] != ctx_name:
                                ctx_name = branch["context_name"]
                                ctx = branch["context"]

                        # 7a: Migration backfill — convert "not specified" → "missing"
                        if handler.sorter and handler.sorter.config.migration:
                            for field_name in ctx.field_names:
                                if field_name not in metadata:
                                    metadata[field_name] = None
                                    updated = True

                        if updated:
                            metadata = _cleanup_metadata(metadata, ctx)
                            await db.update_metadata(record["id"], metadata, context_name=ctx_name)
                            logger.info(
                                "[%s] Updated metadata for %s",
                                username, fpath.name,
                            )

                        # 7b: Reprocess files with missing fields
                        # Skip audio files (require STT pipeline) and unsupported types
                        if _has_missing_fields(metadata, ctx) and handler.mrdocument_url and _is_processable_file(fpath):
                            locked_fields = {
                                k: {"value": v, "clues": []}
                                for k, v in metadata.items()
                                if v is not None
                            }
                            context_yaml = handler.context_manager.get_context_for_api(ctx_name)
                            if context_yaml:
                                logger.info(
                                    "[%s] Reprocessing file with missing fields: %s",
                                    username, fpath,
                                )
                                api_result = await _call_mrdocument(
                                    fpath,
                                    handler.mrdocument_url,
                                    handler.ocr_language,
                                    [context_yaml],
                                    locked_fields=locked_fields,
                                    username=username,
                                )
                                if api_result:
                                    api_metadata, _, _ = api_result
                                    api_metadata = _cleanup_metadata(api_metadata, ctx)
                                    # Drop fields that are still None after reprocessing
                                    # so they become "not specified" and won't trigger
                                    # another reprocessing attempt on next startup.
                                    still_missing = [
                                        k for k in ctx.field_names
                                        if k in api_metadata and api_metadata[k] is None
                                    ]
                                    for k in still_missing:
                                        del api_metadata[k]
                                    await db.update_status(
                                        record["id"], record.get("status", "processed"),
                                        metadata=api_metadata,
                                        context_name=api_metadata.get("context", ctx_name),
                                        processed_at_now=True,
                                    )
                                    metadata = api_metadata
                                    reprocessed_count += 1
                                    if still_missing:
                                        logger.info(
                                            "[%s] Reprocessed %s (fields still unresolved: %s)",
                                            username, fpath, ", ".join(still_missing),
                                        )
                                    else:
                                        logger.info(
                                            "[%s] Reprocessed metadata for %s",
                                            username, fpath,
                                        )

                        # 7c: Relocate to correct leaf folder
                        if ctx.folders:
                            folder_parts = []
                            sortable = True
                            for folder_field in ctx.folders:
                                value = metadata.get(folder_field)
                                if not value:
                                    sortable = False
                                    break
                                sanitized = FileSorter._sanitize_folder_name(str(value))
                                if not sanitized:
                                    sortable = False
                                    break
                                folder_parts.append(sanitized)

                            if sortable:
                                expected_dir = handler.manager.sorted_dir
                                for part in folder_parts:
                                    expected_dir = expected_dir / part

                                if fpath.parent.resolve() != expected_dir.resolve():
                                    try:
                                        expected_dir.mkdir(parents=True, exist_ok=True)
                                        target_path = expected_dir / fpath.name
                                        if target_path.exists() and target_path != fpath:
                                            stem = fpath.stem
                                            suffix = fpath.suffix
                                            unique_id = uuid.uuid4().hex[:8]
                                            target_path = expected_dir / f"{stem}_{unique_id}{suffix}"
                                        fpath.rename(target_path)
                                        await db.update_file_path(record["id"], str(target_path))
                                        logger.info(
                                            "[%s] Relocated: %s -> %s",
                                            username, fpath.name,
                                            target_path.relative_to(handler.manager.sorted_dir),
                                        )
                                        relocated_count += 1
                                    except Exception as e:
                                        logger.error(
                                            "[%s] Failed to relocate %s: %s",
                                            username, fpath.name, e,
                                        )

                    continue

            # No DB record — analyze
            analysis = handler._analyze_non_leaf_file(fpath)
            if analysis:
                unknown_files.append((fpath, analysis, False))
            else:
                analysis = handler._analyze_unknown_leaf_file(fpath)
                if analysis:
                    unknown_files.append((fpath, analysis, True))

    if not unknown_files:
        logger.info(
            "[%s] Startup scan complete: %d file(s) in sorted/, %d backfilled, %d reprocessed, %d relocated, 0 unknown",
            username, total_files, backfilled_count, reprocessed_count, relocated_count,
        )
        return

    logger.info(
        "[%s] Found %d unknown file(s) in sorted/ at startup",
        username, len(unknown_files),
    )

    processed_count = 0
    error_count = 0
    for fpath, analysis, is_leaf in unknown_files:
        try:
            await handler._handle_unknown_file(fpath, analysis, is_leaf)
            processed_count += 1
        except Exception as e:
            error_count += 1
            logger.error("[%s] Failed to process unknown file %s: %s", username, fpath.name, e)

    logger.info(
        "[%s] Startup scan complete: %d file(s) in sorted/, %d backfilled, %d reprocessed, %d relocated, %d unknown (%d processed, %d errors)",
        username, total_files, backfilled_count, reprocessed_count, relocated_count,
        len(unknown_files), processed_count, error_count,
    )


async def _move_unknown_files_to_unprocessed(
    folder: Path, sorter: FileSorter, folder_label: str,
) -> int:
    """
    Scan a folder for files without DB records and move them to unprocessed/.

    Returns the number of files moved.
    """
    username = sorter.username
    if not folder.exists() or not sorter.db:
        return 0

    files = [f for f in folder.iterdir() if f.is_file()]
    files = [f for f in files if not f.name.startswith(".") and not f.name.startswith("~")]
    files = [f for f in files if ".syncthing." not in f.name and not f.name.endswith(".tmp")]
    if sorter.config:
        files = [f for f in files if not sorter.config.should_ignore_file(f.name)]

    moved = 0
    for file_path in files:
        record = await _lookup_metadata_from_db(sorter.db, file_path, username)
        if not record:
            _move_file_to_unprocessed(file_path, sorter.user_root, username)
            moved += 1

    if moved:
        logger.info(
            "[%s] Moved %d file(s) without DB records from %s/ to unprocessed/",
            username, moved, folder_label,
        )
    return moved


async def process_existing_files(sorter: FileSorter):
    """Process any existing files in the reviewed and processed folders."""
    username = sorter.username

    # Move files without DB records from reviewed/ and processed/ to unprocessed/
    await _move_unknown_files_to_unprocessed(sorter.reviewed_dir, sorter, "reviewed")
    await _move_unknown_files_to_unprocessed(sorter.processed_dir, sorter, "processed")

    # Sort remaining reviewed files (those with DB records)
    reviewed_dir = sorter.reviewed_dir
    if not reviewed_dir.exists():
        return

    files = [f for f in reviewed_dir.iterdir() if f.is_file()]
    files = [f for f in files if not f.name.startswith(".") and not f.name.startswith("~")]
    files = [f for f in files if ".syncthing." not in f.name and not f.name.endswith(".tmp")]
    if sorter.config:
        files = [f for f in files if not sorter.config.should_ignore_file(f.name)]

    if not files:
        return

    logger.info("[%s] Found %d file(s) in reviewed folder", username, len(files))

    for file_path in files:
        await sorter.sort_file(file_path)


async def main():
    """Main entry point."""
    watcher_config_path = Path(os.environ.get("WATCHER_CONFIG", "/app/watcher.yaml"))
    database_url = os.environ.get("DATABASE_URL")

    logger.info("=== File Sorter Service ===")
    logger.info("Watcher config: %s", watcher_config_path)

    # Load watcher configuration
    watcher_config = WatcherConfig.load(watcher_config_path)

    loop = asyncio.get_running_loop()

    # Initialize database connection
    db: Optional["DocumentDB"] = None
    if database_url:
        try:
            from db import DocumentDB
            db = DocumentDB(database_url)
            await db.connect()
            logger.info("Database connected: DB-based sorting enabled")
        except Exception as e:
            logger.warning("Database connection failed: %s (sorter will not function without DB)", e)
            db = None
    else:
        logger.warning("DATABASE_URL not set — sorter requires DB for metadata lookup")

    # Event-driven: use asyncio.Event to wake main loop on new folders
    folder_event = asyncio.Event()

    # Start observer for parent directory watching
    observer = Observer()

    parent_dirs = _get_watch_parent_dirs(watcher_config)
    user_folder_handler = NewUserFolderHandler(folder_event, loop)
    for parent_dir in parent_dirs:
        observer.schedule(user_folder_handler, str(parent_dir), recursive=True)
        logger.info("Watching parent dir for new users: %s", parent_dir)

    observer.start()

    # Discover folders to watch
    logger.info("Discovering folders to watch...")
    sorter_folders = discover_sorter_folders(watcher_config, db=db)

    if not sorter_folders:
        logger.warning("No folders to watch for sorting")
        logger.info("Waiting for folders to appear...")

        while not sorter_folders:
            try:
                await asyncio.wait_for(folder_event.wait(), timeout=60)
            except asyncio.TimeoutError:
                pass
            folder_event.clear()
            sorter_folders = discover_sorter_folders(watcher_config, db=db)

    logger.info("Watching %d folder(s) for sorting", len(sorter_folders))

    # Process existing files in reviewed folders
    for reviewed_dir, sorter in sorter_folders:
        await process_existing_files(sorter)

    # Read env vars for mrdocument
    mrdocument_url = os.environ.get("MRDOCUMENT_URL", "")
    ocr_language = os.environ.get("OCR_LANGUAGE", "auto")
    stt_url = os.environ.get("STT_URL", "")

    # Discover sorted folder configurations
    smart_folder_watchers = discover_sorted_folder_watchers(sorter_folders, db=db)

    # Process existing files in sorted folders for smart folder symlinks
    for sorted_dir, manager, context_manager, sorter in smart_folder_watchers:
        await process_existing_smart_folders(manager)

    # Build a map of reviewed_dir -> sorter for config handler
    sorter_map: dict[Path, FileSorter] = {reviewed_dir: sorter for reviewed_dir, sorter in sorter_folders}

    # Build a list of smart folder managers (for config reload to re-evaluate)
    smart_folder_manager_list: list[SmartFolderManager] = [
        manager for _, manager, _, _ in smart_folder_watchers
    ]

    # Create sorted folder handlers (needed for startup scan and config change handling)
    sorted_handlers: list[SmartFolderHandler] = []
    for sorted_dir, manager, context_manager, sorter in smart_folder_watchers:
        handler = SmartFolderHandler(
            manager, context_manager, loop,
            sorter=sorter, mrdocument_url=mrdocument_url, ocr_language=ocr_language,
            stt_url=stt_url,
        )
        sorted_handlers.append(handler)

    # Track user roots we've added config watchers for
    config_watched_roots: set[Path] = set()

    # Watch reviewed folders for sorting
    for reviewed_dir, sorter in sorter_folders:
        handler = ReviewedFileHandler(sorter, loop)
        observer.schedule(handler, str(reviewed_dir), recursive=False)
        logger.info("  Watching reviewed: %s", reviewed_dir)

        # Add config watcher for user root (once per user)
        user_root = sorter.user_root
        if user_root not in config_watched_roots:
            config_handler = ConfigFileHandler(
                sorter_map, smart_folder_manager_list, user_root, loop,
                sorted_handlers=sorted_handlers,
            )
            observer.schedule(config_handler, str(user_root), recursive=False)
            config_watched_roots.add(user_root)
            logger.info("  Watching config: %s/*.yaml", user_root)

    # Startup scan: process unknown files before watchers start (avoids race)
    for handler in sorted_handlers:
        await process_existing_unknown_files(handler)

    # Now schedule and start all watchers
    for (sorted_dir, _, _, _), handler in zip(smart_folder_watchers, sorted_handlers):
        observer.schedule(handler, str(sorted_dir), recursive=True)
        logger.info("  Watching sorted: %s", sorted_dir)

    logger.info("")
    logger.info("Ready to sort files")

    try:
        while True:
            # Wait for folder event or 300s timeout (reconciliation interval)
            try:
                await asyncio.wait_for(folder_event.wait(), timeout=300)
            except asyncio.TimeoutError:
                pass
            folder_event.clear()

            # Smart folder reconciliation runs on every wake (event or timeout)
            for manager in smart_folder_manager_list:
                try:
                    await process_existing_smart_folders(manager)
                except Exception as e:
                    logger.error(
                        "[%s] Smart folder reconciliation failed: %s",
                        manager.username, e
                    )

            # Check for new folders
            current_folders = discover_sorter_folders(watcher_config, db=db)
            current_reviewed_dirs = {str(d) for d, _ in current_folders}
            existing_reviewed_dirs = {str(d) for d, _ in sorter_folders}

            new_folders = [(d, s) for d, s in current_folders if str(d) not in existing_reviewed_dirs]

            if new_folders:
                logger.info("Detected %d new folder(s) to watch", len(new_folders))
                for reviewed_dir, sorter in new_folders:
                    logger.info("  Adding: %s", reviewed_dir)
                    await process_existing_files(sorter)
                    handler = ReviewedFileHandler(sorter, loop)
                    observer.schedule(handler, str(reviewed_dir), recursive=False)

                    # Update sorter map
                    sorter_map[reviewed_dir] = sorter

                # Also set up sorted folder watching for new folders
                new_smart_folder_watchers = discover_sorted_folder_watchers(new_folders, db=db)
                for sorted_dir, manager, context_manager, sorter in new_smart_folder_watchers:
                    await process_existing_smart_folders(manager)
                    handler = SmartFolderHandler(
                        manager, context_manager, loop,
                        sorter=sorter, mrdocument_url=mrdocument_url, ocr_language=ocr_language,
                        stt_url=stt_url,
                    )
                    observer.schedule(handler, str(sorted_dir), recursive=True)
                    logger.info("  Adding sorted folder watcher: %s", sorted_dir)
                    smart_folder_manager_list.append(manager)
                    sorted_handlers.append(handler)

                smart_folder_watchers.extend(new_smart_folder_watchers)

                # Add config watchers for new user roots
                for reviewed_dir, sorter in new_folders:
                    user_root = sorter.user_root
                    if user_root not in config_watched_roots:
                        config_handler = ConfigFileHandler(
                            sorter_map, smart_folder_manager_list, user_root, loop,
                            sorted_handlers=sorted_handlers,
                        )
                        observer.schedule(config_handler, str(user_root), recursive=False)
                        config_watched_roots.add(user_root)
                        logger.info("  Watching config: %s/*.yaml", user_root)

                sorter_folders = current_folders

    except KeyboardInterrupt:
        logger.info("Shutting down...")
        observer.stop()
    finally:
        observer.join()
        if db:
            await db.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
