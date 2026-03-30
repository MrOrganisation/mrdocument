"""Read context.yaml and generated.yaml from the filesystem.

Provides tools to enumerate contexts, fields, and candidates for
a user's sorted/ directory, matching the watcher's merging logic.
"""

import logging
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


class ContextReadError(Exception):
    """Raised when a context file cannot be read or parsed."""


class ContextReader:
    """Reads context configuration from a user's sorted/ directory."""

    def __init__(self, sync_root: str) -> None:
        self._sync_root = Path(sync_root)

    def _sorted_dir(self, username: str) -> Path:
        return self._sync_root / username / "sorted"

    def list_contexts(self, username: str) -> list[dict]:
        """Enumerate all contexts available to the user.

        Returns a list of context summaries (name, description,
        filename_pattern, folders).
        """
        sorted_dir = self._sorted_dir(username)
        if not sorted_dir.is_dir():
            return []

        contexts = []
        for entry in sorted(sorted_dir.iterdir()):
            if not entry.is_dir():
                continue
            config_path = _find_ci(entry, "context.yaml")
            if config_path is None:
                continue
            try:
                data = _load_yaml(config_path)
                if not isinstance(data, dict):
                    continue
                contexts.append({
                    "name": data.get("name", entry.name),
                    "description": data.get("description", ""),
                    "filename_pattern": data.get("filename", ""),
                    "audio_filename_pattern": data.get("audio_filename", ""),
                    "folders": data.get("folders", []),
                })
            except Exception:
                logger.warning("Error loading context %s", entry.name, exc_info=True)
        return contexts

    def list_fields(self, username: str, context: str) -> list[dict]:
        """Enumerate all fields for a given context.

        Returns field metadata without full candidate lists.
        """
        data = self._load_context(username, context)
        fields_data = data.get("fields", {})
        if not isinstance(fields_data, dict):
            return []

        generated = self._load_generated(username, context)

        result = []
        for field_name, field_config in fields_data.items():
            if not isinstance(field_config, dict):
                continue

            base_candidates = field_config.get("candidates", [])
            gen_candidates = _get_generated_candidates(generated, field_name)
            merged = _merge_candidates(base_candidates, gen_candidates)

            result.append({
                "name": field_name,
                "instructions": field_config.get("instructions", ""),
                "allow_new_candidates": field_config.get("allow_new_candidates", True),
                "candidate_count": len(merged),
            })
        return result

    def list_candidates(self, username: str, context: str, field: str) -> dict:
        """Return the merged candidate list for a context field.

        Merges base candidates from context.yaml with generated
        candidates from generated.yaml, matching the watcher's logic.
        """
        data = self._load_context(username, context)
        fields_data = data.get("fields", {})
        if not isinstance(fields_data, dict):
            raise ContextReadError(f"Context '{context}' has no fields")

        field_config = fields_data.get(field)
        if field_config is None or not isinstance(field_config, dict):
            raise ContextReadError(
                f"Field '{field}' not found in context '{context}'"
            )

        base_candidates = field_config.get("candidates", [])
        generated = self._load_generated(username, context)
        gen_candidates = _get_generated_candidates(generated, field)
        merged = _merge_candidates(base_candidates, gen_candidates)

        return {
            "context": context,
            "field": field,
            "allow_new_candidates": field_config.get("allow_new_candidates", True),
            "candidates": merged,
        }

    def _load_context(self, username: str, context: str) -> dict:
        """Load and return the context.yaml for a context."""
        sorted_dir = self._sorted_dir(username)
        # Case-insensitive context directory lookup
        context_dir = _find_ci_dir(sorted_dir, context)
        if context_dir is None:
            raise ContextReadError(f"Context directory '{context}' not found")

        config_path = _find_ci(context_dir, "context.yaml")
        if config_path is None:
            raise ContextReadError(
                f"No context.yaml found in {context_dir}"
            )

        data = _load_yaml(config_path)
        if not isinstance(data, dict):
            raise ContextReadError(f"context.yaml is not a mapping in {context_dir}")
        return data

    def _load_generated(self, username: str, context: str) -> dict:
        """Load generated.yaml for a context, returning empty dict if absent."""
        sorted_dir = self._sorted_dir(username)
        context_dir = _find_ci_dir(sorted_dir, context)
        if context_dir is None:
            return {}

        gen_path = _find_ci(context_dir, "generated.yaml")
        if gen_path is None:
            return {}

        try:
            data = _load_yaml(gen_path)
            if isinstance(data, dict):
                return data
        except Exception:
            logger.warning(
                "Error loading generated.yaml for context %s", context, exc_info=True
            )
        return {}


def _find_ci(directory: Path, filename: str) -> Path | None:
    """Case-insensitive file lookup within a directory."""
    if not directory.is_dir():
        return None
    lower = filename.lower()
    for entry in directory.iterdir():
        if entry.is_file() and entry.name.lower() == lower:
            return entry
    return None


def _find_ci_dir(parent: Path, dirname: str) -> Path | None:
    """Case-insensitive directory lookup within a parent directory."""
    if not parent.is_dir():
        return None
    lower = dirname.lower()
    for entry in parent.iterdir():
        if entry.is_dir() and entry.name.lower() == lower:
            return entry
    return None


def _load_yaml(path: Path) -> Any:
    """Load a YAML file and return the parsed data."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _get_generated_candidates(generated: dict, field_name: str) -> list:
    """Extract generated candidates for a field from generated.yaml data.

    generated.yaml structure:
      fields:
        field_name:
          candidates:
            - "value"
            - {name: "value", clues: [...]}
    """
    fields = generated.get("fields", {})
    if not isinstance(fields, dict):
        return []
    field_data = fields.get(field_name, {})
    if not isinstance(field_data, dict):
        return []
    candidates = field_data.get("candidates", [])
    if not isinstance(candidates, list):
        return []
    return candidates


def _candidate_name(candidate: Any) -> str | None:
    """Extract the identifying name from a candidate (string or object)."""
    if isinstance(candidate, str):
        return candidate
    if isinstance(candidate, dict):
        return candidate.get("name")
    return None


def _candidate_short(candidate: Any) -> str | None:
    """Extract the short name from a candidate object."""
    if isinstance(candidate, dict):
        return candidate.get("short")
    return None


def _merge_candidates(base: list, generated: list) -> list:
    """Merge generated candidates into base candidates.

    Matches the watcher's get_all_candidates_json() logic:
    1. Start with all base candidates
    2. For each generated candidate:
       a. If simple string: add if not already present (by name or short)
       b. If object with name: find matching base candidate, merge clues;
          otherwise add as new
    3. Deduplicate by name and short
    """
    result = list(base)

    # Build lookup of existing names and shorts
    existing_names: set[str] = set()
    existing_shorts: set[str] = set()
    for c in result:
        name = _candidate_name(c)
        if name:
            existing_names.add(name.lower())
        short = _candidate_short(c)
        if short:
            existing_shorts.add(short.lower())

    for gen_candidate in generated:
        gen_name = _candidate_name(gen_candidate)
        gen_short = _candidate_short(gen_candidate)

        if gen_name and gen_name.lower() in existing_names:
            # Candidate exists — merge clues if the generated one has clues
            if isinstance(gen_candidate, dict) and "clues" in gen_candidate:
                _merge_clues_into(result, gen_name, gen_candidate["clues"])
            continue

        if gen_short and gen_short.lower() in existing_shorts:
            continue

        # New candidate — add it
        result.append(gen_candidate)
        if gen_name:
            existing_names.add(gen_name.lower())
        if gen_short:
            existing_shorts.add(gen_short.lower())

    return result


def _merge_clues_into(candidates: list, target_name: str, new_clues: list) -> None:
    """Merge new clues into an existing candidate's clues list."""
    for i, candidate in enumerate(candidates):
        name = _candidate_name(candidate)
        if name and name.lower() == target_name.lower():
            if isinstance(candidate, str):
                # Upgrade string to object with clues
                candidates[i] = {"name": candidate, "clues": list(new_clues)}
            elif isinstance(candidate, dict):
                existing_clues = candidate.get("clues", [])
                existing_set = {c.lower() for c in existing_clues if isinstance(c, str)}
                for clue in new_clues:
                    if isinstance(clue, str) and clue.lower() not in existing_set:
                        existing_clues.append(clue)
                        existing_set.add(clue.lower())
                candidate["clues"] = existing_clues
            return
