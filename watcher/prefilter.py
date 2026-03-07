"""
Pre-filter for document watcher v2.

Runs before step 1 to move files with unsupported extensions to error/.
Prevents unsupported files from entering the pipeline and accumulating
as permanently-failing records.
"""

import logging
from pathlib import Path
from uuid import uuid4

from step3 import AUDIO_EXTENSIONS, DOCUMENT_EXTENSIONS

logger = logging.getLogger(__name__)

SUPPORTED_EXTENSIONS = DOCUMENT_EXTENSIONS | AUDIO_EXTENSIONS


EXCLUDED_DIRS = {"error", "void"}


def prefilter(root: Path) -> int:
    """Move files with unsupported extensions to error/.

    Scans all top-level directories except error/, void/, and hidden dirs.
    Uses recursive scanning to catch files in subdirectories (e.g. sorted/context/).

    Returns the number of files moved.
    """
    moved = 0
    error_dir = root / "error"

    for dirpath in root.iterdir():
        if not dirpath.is_dir() or dirpath.is_symlink():
            continue
        if dirpath.name.startswith("."):
            continue
        if dirpath.name in EXCLUDED_DIRS:
            continue
        for f in dirpath.rglob("*"):
            if not f.is_file() or f.is_symlink():
                continue
            if f.name.startswith(".") or f.name.startswith("~"):
                continue
            if f.suffix.lower() == ".tmp":
                continue
            # Skip config files in sorted/{context}/
            rel = str(f.relative_to(root))
            parts = rel.split("/")
            if (len(parts) == 3 and parts[0] == "sorted"
                    and parts[2].lower() in ("context.yaml", "smartfolders.yaml", "generated.yaml")):
                continue
            ext = f.suffix.lower()
            if ext in SUPPORTED_EXTENSIONS:
                continue
            error_dir.mkdir(parents=True, exist_ok=True)
            dest = error_dir / f.name
            if dest.exists():
                dest = error_dir / f"{dest.stem}_{uuid4().hex[:8]}{dest.suffix}"
            try:
                f.rename(dest)
                logger.info(
                    "Unsupported file moved to error: %s",
                    f.relative_to(root),
                )
                moved += 1
            except OSError as e:
                logger.error("Failed to move unsupported file %s: %s", f.name, e)

    return moved
