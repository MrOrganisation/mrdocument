"""Interface to AI models for metadata extraction using structured tool output."""

import asyncio
import logging
import re
import unicodedata
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Optional, TypedDict

import anthropic
import yaml

from mrdocument.costs import get_cost_tracker

logger = logging.getLogger(__name__)


# =============================================================================
# Type Definitions
# =============================================================================


class ItemWithShort(TypedDict, total=False):
    """
    Candidate item with display name and optional short form/clues.

    Fields:
        name: Display name shown to AI and used in metadata (required)
        short: Short form used in filenames (optional, defaults to name if missing)
        clues: List of clues to help AI understand when to use this value (optional)
        allow_new_clues: Whether AI can suggest new clues for this candidate (default: false)
        transcription_keyterms: List of key terms to improve STT accuracy when this candidate is selected (optional)
    """

    name: str
    short: str
    clues: list[str]
    allow_new_clues: bool
    transcription_keyterms: list[str]


class FieldConfig(TypedDict, total=False):
    """Configuration for a metadata field.
    
    Required:
        instructions: Semantic description of this field for the AI model
    
    Optional:
        candidates: List of allowed values, or None/missing for no candidates
            - Missing or None: No concept of candidates for this field
            - []: Empty list, new candidates will be tracked when AI invents them
            - [...]: List of candidates to choose from
        blacklist: Values to reject
        allow_new_candidates: Whether AI can invent new values (default: False)
            - If candidates is [], allow_new_candidates must be True
        include_in_context_determination: Whether to include this field's candidates
            and clues in the context determination stage (default: False)
    """

    instructions: str  # Required: semantic description of this field
    candidates: list[str | ItemWithShort] | None  # Optional: list of allowed values
    blacklist: list[str]  # Optional: values to reject
    allow_new_candidates: bool  # Optional: whether AI can invent new values (default: False)
    include_in_context_determination: bool  # Optional: include in context classification (default: False)


class ContextConfig(TypedDict, total=False):
    """Configuration for a document context.
    
    Required:
        name: Unique identifier for this context
        description: Human-readable description for AI classification
        filename: Pattern for generating filenames, e.g., "{date}_{type}_{sender}"
        fields: Dictionary of field configurations
    
    Optional:
        transcription_keyterms: List of key terms to improve STT accuracy for this context
    
    Note: The 'date' field is always available and should not be defined in fields.
    It extracts dates in YYYY-MM-DD format.
    """

    name: str
    description: str
    filename: str  # Required: filename pattern with {field} placeholders
    fields: dict[str, FieldConfig]  # Required: field configurations
    transcription_keyterms: list[str]  # Optional: keyterms for STT accuracy


# =============================================================================
# Character Replacements for Filename Sanitization
# =============================================================================

CHAR_REPLACEMENTS = {
    "ä": "ae",
    "ö": "oe",
    "ü": "ue",
    "Ä": "Ae",
    "Ö": "Oe",
    "Ü": "Ue",
    "ß": "ss",
    "æ": "ae",
    "œ": "oe",
    "ø": "o",
    "å": "a",
    "é": "e",
    "è": "e",
    "ê": "e",
    "ë": "e",
    "à": "a",
    "â": "a",
    "ù": "u",
    "û": "u",
    "ô": "o",
    "î": "i",
    "ï": "i",
    "ç": "c",
    "ñ": "n",
}


# =============================================================================
# Configuration Loading
# =============================================================================


def load_config(config_path: Optional[Path] = None) -> dict[str, Any]:
    """
    Load AI configuration from YAML file.

    Args:
        config_path: Path to config file. Defaults to config.yaml in this module's directory.

    Returns:
        Configuration dictionary
    """
    if config_path is None:
        config_path = Path(__file__).parent / "config.yaml"

    with open(config_path) as f:
        return yaml.safe_load(f)


# =============================================================================
# Document Metadata
# =============================================================================


def _resolve_filename_pattern(
    raw: Any,
    source_filename: Optional[str] = None,
) -> str:
    """Resolve a filename config (string or conditional list) to a pattern."""
    if isinstance(raw, str):
        return raw
    if not isinstance(raw, list):
        return str(raw)
    default = None
    for entry in raw:
        if not isinstance(entry, dict) or "pattern" not in entry:
            continue
        match_re = entry.get("match")
        if match_re is None:
            default = entry["pattern"]
        elif source_filename and re.search(match_re, source_filename):
            return entry["pattern"]
    return default or str(raw)


@dataclass
class DocumentMetadata:
    """Extracted document metadata with arbitrary fields.
    
    Attributes:
        fields: Dictionary of field values (field_name -> value)
        date: Extracted date (always available)
        context: Name of the determined context
        new_clues: New clues suggested by AI for candidates (field_name -> (value, clue))
    """

    fields: dict[str, Any]
    date: Optional[date] = None
    context: Optional[str] = None
    # New clues suggested by AI for candidates (field_name -> (value, clue))
    new_clues: dict[str, tuple[str, str]] = None

    def __post_init__(self):
        if self.new_clues is None:
            self.new_clues = {}

    def get_field(self, name: str) -> Optional[Any]:
        """Get a field value by name."""
        return self.fields.get(name)
    
    # Legacy accessors for backward compatibility
    @property
    def doc_type(self) -> Optional[str]:
        return self.fields.get("type")
    
    @property
    def sender(self) -> Optional[str]:
        return self.fields.get("sender")
    
    @property
    def topic(self) -> Optional[str]:
        return self.fields.get("topic")
    
    @property
    def subject(self) -> Optional[str]:
        return self.fields.get("subject")
    
    @property
    def keywords(self) -> list[str]:
        return self.fields.get("keywords", [])

    def to_filename(self, pattern: str, source_filename: Optional[str] = None) -> str:
        """
        Generate a filename from the metadata using a pattern.

        Args:
            pattern: Filename pattern with {field} placeholders, e.g., "{date}_{type}_{sender}"
            source_filename: Original filename; resolves the {source_filename} placeholder.

        Returns:
            Sanitized filename with .pdf extension

        Note: The 'date' field preserves its format (YYYY-MM-DD), while all
        other fields have '-' converted to '_' via _sanitize.

        Example:
            >>> metadata.to_filename("{date}_{type}_{sender}")
            "2024-01-15_invoice_acme_corp.pdf"
        """
        # Build replacement dict with all fields
        replacements = {}

        # Add source_filename (stem only)
        if source_filename:
            replacements["source_filename"] = Path(source_filename).stem
        else:
            replacements["source_filename"] = ""
        
        # Add context
        if self.context:
            replacements["context"] = self.context
        else:
            replacements["context"] = ""
        
        # Add date (not sanitized - preserves YYYY-MM-DD format)
        # Use 0000-00-00 as fallback when no date could be determined
        if self.date:
            replacements["date"] = self.date.strftime("%Y-%m-%d")
        else:
            replacements["date"] = "0000-00-00"
        
        # Add all other fields
        for field_name, value in self.fields.items():
            if value is not None:
                if isinstance(value, list):
                    # For list fields (like keywords), join with underscore
                    replacements[field_name] = "_".join(str(v) for v in value)
                else:
                    replacements[field_name] = str(value)
            else:
                replacements[field_name] = ""
        
        # Apply pattern
        result = pattern
        for field_name, value in replacements.items():
            placeholder = "{" + field_name + "}"
            if placeholder in result:
                if field_name == "date":
                    # Date field: preserve format (YYYY-MM-DD), no sanitization
                    sanitized = value
                else:
                    sanitized = self._sanitize(value) if value else ""
                result = result.replace(placeholder, sanitized)
        
        # Clean up empty placeholders and resulting artifacts
        # Remove unreplaced placeholders
        result = re.sub(r"\{[^}]+\}", "", result)
        # Remove multiple consecutive separators
        result = re.sub(r"[-_]{2,}", "-", result)
        # Remove leading/trailing separators
        result = result.strip("-_")
        
        if not result:
            return "document.pdf"

        return (result + ".pdf").lower()

    @staticmethod
    def _sanitize(s: str) -> str:
        """Sanitize a string for use in a filename field."""
        if not s:
            return ""
            
        # Replace known umlauts and special chars
        for char, replacement in CHAR_REPLACEMENTS.items():
            s = s.replace(char, replacement)

        # Normalize unicode and remove remaining diacritics
        s = unicodedata.normalize("NFKD", s)
        s = s.encode("ASCII", "ignore").decode("ASCII")

        # Replace whitespace sequences with single underscore
        s = re.sub(r"\s+", "_", s)

        # Replace hyphens with underscores
        s = s.replace("-", "_")

        # Replace problematic filename characters with underscores
        s = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", s)

        # Collapse multiple underscores
        s = re.sub(r"_+", "_", s)

        # Remove leading/trailing underscores
        s = s.strip("_")

        # Truncate to reasonable length
        if len(s) > 50:
            s = s[:50].rstrip("_")

        return s


# =============================================================================
# Exceptions
# =============================================================================


class AiError(Exception):
    """Error during AI processing."""

    pass


class ConfigurationError(AiError):
    """Error in configuration (e.g., allow_new_candidates=false with no candidates)."""

    pass


# =============================================================================
# AI Client
# =============================================================================


class ModelConfig:
    """Configuration for a single AI model."""
    
    def __init__(self, config: dict | str):
        """
        Initialize model config from dict or string.
        
        Args:
            config: Either a dict with name/max_tokens/extended_thinking/thinking_budget,
                    or a string (model name) for simple config.
        """
        if isinstance(config, str):
            # Simple string format - just model name with defaults
            self.name = config
            self.max_tokens = 1024
            self.extended_thinking = False
            self.thinking_budget = 10000
        else:
            # Dict format with full config
            self.name = config.get("name")
            if not self.name:
                raise ConfigurationError("Model config missing 'name' field")
            self.max_tokens = config.get("max_tokens", 1024)
            self.extended_thinking = config.get("extended_thinking", False)
            self.thinking_budget = config.get("thinking_budget", 10000)
    
    def __repr__(self) -> str:
        return f"ModelConfig(name={self.name}, max_tokens={self.max_tokens}, extended_thinking={self.extended_thinking})"


class AiClient:
    """Client for AI-based metadata extraction using Anthropic's tool_use."""

    def __init__(
        self,
        api_key: str,
        model: Optional[str] = None,
        config_path: Optional[Path] = None,
    ):
        """
        Initialize AI client.

        Args:
            api_key: Anthropic API key
            model: Model to use (overrides config file). Can be a single model or comma-separated list.
            config_path: Path to config YAML file
        """
        self.config = load_config(config_path)
        self.client = anthropic.Anthropic(api_key=api_key)
        
        # Load models as a list of ModelConfig
        if model:
            # Override from parameter (can be comma-separated) - uses defaults for other params
            self.models = [ModelConfig(m.strip()) for m in model.split(",")]
        elif "models" in self.config:
            # New config format: list of model configs
            models_config = self.config["models"]
            if not models_config:
                raise ConfigurationError("'models' list is empty in config.yaml")
            self.models = [ModelConfig(m) for m in models_config]
        elif "model" in self.config:
            # Legacy config format: single model section
            legacy = self.config["model"]
            if "name" in legacy:
                self.models = [ModelConfig({
                    "name": legacy["name"],
                    "max_tokens": legacy.get("max_tokens", 1024),
                    "extended_thinking": legacy.get("extended_thinking", False),
                    "thinking_budget": legacy.get("thinking_budget", 10000),
                })]
            elif "models" in legacy:
                # Nested models in legacy model section
                self.models = [ModelConfig(m) for m in legacy["models"]]
            else:
                raise ConfigurationError("No model configured in config.yaml")
        else:
            raise ConfigurationError("No models configured in config.yaml")
        
        if not self.models:
            raise ConfigurationError("At least one model must be configured")
            
        self.max_input_chars = self.config["extraction"]["max_input_chars"]

        # Load instruction templates
        self.extraction_system_prompt = self.config.get("extraction_system_prompt", "")
        self.base_instruction_strict = self.config.get("base_instruction_strict", "")
        self.base_instruction_flexible = self.config.get("base_instruction_flexible", "")
        self.blacklist_instruction = self.config.get("blacklist_instruction", "")
        self.new_clue_instruction = self.config.get("new_clue_instruction", "")
        
        # Field instruction wrapper (explains how to interpret field instructions)
        self.field_instruction_wrapper = self.config.get("field_instruction_wrapper", "Field purpose: {instructions}")

        # Language instructions
        self.language_instruction_template = self.config.get("language_instruction", "")
        self.default_language_instruction = self.config.get("default_language_instruction", "")

        # Context determination
        self.context_system_prompt = self.config.get("context_system_prompt", "")
        self.context_tool_description = self.config.get("context_tool_description", "")

        logger.info(
            "AiClient initialized with %d model(s): %s",
            len(self.models),
            [m.name for m in self.models],
        )

    def _get_candidate_names(self, candidates: list[str | ItemWithShort]) -> list[str]:
        """Extract display names from candidates list."""
        names = []
        for c in candidates:
            if isinstance(c, dict):
                # Use "name" if present, skip if not
                if "name" in c:
                    names.append(c["name"])
                # Dicts without "name" are invalid, skip them
            else:
                names.append(c)
        return names

    def _format_candidates_with_clues(
        self, candidates: list[str | ItemWithShort]
    ) -> str:
        """Format candidates list, including clues where available."""
        formatted = []
        for c in candidates:
            if isinstance(c, dict):
                name = c.get("name")
                if not name:
                    continue
                clues = c.get("clues", [])
                if clues:
                    clues_text = "; ".join(clues)
                    formatted.append(f"- {name}: {clues_text}")
                else:
                    formatted.append(f"- {name}")
            else:
                formatted.append(f"- {c}")
        return "\n".join(formatted)

    def _has_any_clues(self, candidates: list[str | ItemWithShort]) -> bool:
        """Check if any candidate has clues."""
        for c in candidates:
            if isinstance(c, dict) and c.get("clues"):
                return True
        return False

    def _get_candidates_allowing_new_clues(
        self, candidates: list[str | ItemWithShort]
    ) -> list[str]:
        """Get list of candidate names that allow new clues."""
        names = []
        for c in candidates:
            if isinstance(c, dict) and c.get("allow_new_clues", False):
                name = c.get("name")
                if name:
                    names.append(name)
        return names

    def _build_name_to_short_mapping(
        self, candidates: list[str | ItemWithShort]
    ) -> dict[str, str]:
        """Build mapping from display name to short form."""
        mapping = {}
        for c in candidates:
            if isinstance(c, dict):
                # Only add mapping if both "name" and "short" are present
                if "name" in c and "short" in c:
                    mapping[c["name"]] = c["short"]
                # If only "name" present (no "short"), no mapping needed
            # Simple strings don't need mapping
        return mapping

    def _build_field_description(
        self,
        field_name: str,
        field_config: FieldConfig,
    ) -> str:
        """
        Build the description for a field in the tool schema.

        Combines: field instruction + candidate handling + blacklist warning

        Note: candidates can be:
        - Missing or None: No concept of candidates for this field
        - []: Empty list, new candidates will be tracked when AI invents them
        - [...]: List of candidates to choose from
        
        Args:
            field_name: Name of the field
            field_config: Field configuration (instructions required)
            
        Returns:
            Combined description string for the AI model
            
        Raises:
            ConfigurationError: If instructions missing or invalid candidate config
        """
        parts = []

        # Get field instructions (required)
        instructions = field_config.get("instructions")
        if not instructions:
            raise ConfigurationError(
                f"Field '{field_name}' is missing required 'instructions'"
            )

        # Get candidates - missing means no candidates (same as null)
        candidates = field_config.get("candidates")
        has_candidates = candidates is not None
        if candidates is None:
            candidates = []
        
        allow_new_candidates = field_config.get("allow_new_candidates", False)
        blacklist = field_config.get("blacklist", [])

        # Validate: if candidates is empty list [], allow_new_candidates must be True
        if has_candidates and len(candidates) == 0 and not allow_new_candidates:
            raise ConfigurationError(
                f"Field '{field_name}' has candidates: [] but allow_new_candidates: false. "
                "An empty candidate list requires allow_new_candidates: true."
            )

        # Add field instructions first
        parts.append(instructions.strip())

        # Add candidate handling (only if candidates is defined, even if empty list)
        if has_candidates and candidates:
            if allow_new_candidates:
                parts.append(self.base_instruction_flexible.strip())
            else:
                parts.append(self.base_instruction_strict.strip())

            # List the candidates (with clues if any are provided)
            if self._has_any_clues(candidates):
                parts.append("Available values:")
                parts.append(self._format_candidates_with_clues(candidates))
            else:
                candidate_names = self._get_candidate_names(candidates)
                parts.append(f"Available values: {', '.join(candidate_names)}")
        elif has_candidates:
            # Empty list [] - AI can invent values that will be tracked
            parts.append("No predefined values. You may create an appropriate value.")

        # Blacklist warning
        if blacklist:
            blacklist_text = self.blacklist_instruction.format(
                blacklist=", ".join(blacklist)
            ).strip()
            parts.append(blacklist_text)

        return "\n".join(parts)

    def _build_field_schema(
        self,
        field_name: str,
        field_config: FieldConfig,
    ) -> dict[str, Any]:
        """
        Build JSON schema for a single field.

        Uses enum when allow_new_candidates=false with non-empty candidates, otherwise string.
        
        Args:
            field_name: Name of the field
            field_config: Field configuration
            
        Returns:
            JSON schema dict for this field
        """
        candidates = field_config.get("candidates")
        if candidates is None:
            candidates = []
        allow_new_candidates = field_config.get("allow_new_candidates", False)

        # _build_field_description handles validation
        description = self._build_field_description(field_name, field_config)

        if candidates and not allow_new_candidates:
            # Strict mode: use enum
            candidate_names = self._get_candidate_names(candidates)
            return {
                "type": "string",
                "enum": candidate_names,
                "description": description,
            }
        else:
            # Flexible mode: use string
            return {
                "type": "string",
                "description": description,
            }

    def _build_new_clue_schema(
        self,
        field_name: str,
        field_config: Optional[FieldConfig],
    ) -> Optional[dict[str, Any]]:
        """
        Build JSON schema for a new_clue field if any candidates allow new clues.

        Returns None if no candidates allow new clues.
        """
        if field_config is None:
            return None

        candidates = field_config.get("candidates", [])
        allowing_clues = self._get_candidates_allowing_new_clues(candidates)

        if not allowing_clues:
            return None

        description = self.new_clue_instruction.format(
            field_name=field_name,
            candidates=", ".join(allowing_clues),
        ).strip()

        return {
            "type": "string",
            "description": description,
        }

    def _build_extraction_tool(
        self,
        field_configs: dict[str, FieldConfig],
    ) -> dict[str, Any]:
        """
        Build the tool definition for metadata extraction.

        Args:
            field_configs: Configuration for each field from context's 'fields' object

        Returns:
            Tool definition dict for Anthropic API
            
        Note: The 'date' field is always included automatically.
        """
        # Date is always available
        properties = {
            "date": {
                "type": "string",
                "description": "Document date in YYYY-MM-DD format, or null if not found.",
            },
        }
        
        # Build schema for each configured field
        required_fields = []
        for field_name, field_config in field_configs.items():
            if field_name == "date":
                # Skip if someone accidentally defines date - it's handled automatically
                logger.warning("Field 'date' should not be defined in fields - it is always available automatically")
                continue
                
            properties[field_name] = self._build_field_schema(field_name, field_config)
            required_fields.append(field_name)
            
            # Add new_clue field if any candidates allow new clues
            new_clue_schema = self._build_new_clue_schema(field_name, field_config)
            if new_clue_schema:
                properties[f"{field_name}_new_clue"] = new_clue_schema

        return {
            "name": "extract_metadata",
            "description": "Extract metadata from the document.",
            "input_schema": {
                "type": "object",
                "properties": properties,
                "required": required_fields,
            },
        }

    def _build_context_description(self, ctx: ContextConfig, include_all_candidates: bool = False) -> str:
        """
        Build the description for a single context, including field candidates if flagged.

        Args:
            ctx: Context configuration
            include_all_candidates: If True, include candidate names from all fields
                as keywords (without clues or other metadata). Used for transcript
                classification where keyword presence helps context determination.

        Returns:
            Formatted context description string
        """
        parts = [f"- {ctx['name']}: {ctx['description']}"]

        fields = ctx.get("fields", {})

        if include_all_candidates:
            # Collect all candidate names across all fields as flat keywords
            keywords = []
            for field_name, field_config in fields.items():
                candidates = field_config.get("candidates")
                if not candidates:
                    continue
                for candidate in candidates:
                    if isinstance(candidate, dict):
                        name = candidate.get("name")
                        if name:
                            keywords.append(name)
                    else:
                        keywords.append(str(candidate))
            if keywords:
                parts.append(f"  Keywords: {', '.join(keywords)}")
        else:
            # Only include fields with include_in_context_determination=True
            for field_name, field_config in fields.items():
                if not field_config.get("include_in_context_determination", False):
                    continue

                candidates = field_config.get("candidates")
                if not candidates:
                    continue

                # Format candidates with clues
                field_parts = [f"  {field_name} values:"]
                for candidate in candidates:
                    if isinstance(candidate, dict):
                        name = candidate.get("name")
                        if not name:
                            continue
                        clues = candidate.get("clues", [])
                        if clues:
                            clues_text = "; ".join(clues)
                            field_parts.append(f"    - {name}: {clues_text}")
                        else:
                            field_parts.append(f"    - {name}")
                    else:
                        field_parts.append(f"    - {candidate}")

                if len(field_parts) > 1:  # Only add if we have actual candidates
                    parts.extend(field_parts)

        return "\n".join(parts)

    def _build_context_tool(self, contexts: list[ContextConfig], include_all_candidates: bool = False) -> dict[str, Any]:
        """
        Build the tool definition for context determination.

        Uses enum to force selection from available contexts.
        Includes field candidates with clues for fields marked with
        include_in_context_determination=True, or all candidate names as
        keywords when include_all_candidates is True.
        """
        context_names = [ctx["name"] for ctx in contexts]
        context_descriptions = [
            self._build_context_description(ctx, include_all_candidates=include_all_candidates) for ctx in contexts
        ]

        return {
            "name": "classify_context",
            "description": self.context_tool_description
            + "\n\nAvailable contexts:\n"
            + "\n".join(context_descriptions),
            "input_schema": {
                "type": "object",
                "properties": {
                    "context": {
                        "type": "string",
                        "enum": context_names,
                        "description": "The context this document belongs to.",
                    },
                },
                "required": ["context"],
            },
        }

    def _apply_name_to_short_mapping(
        self,
        value: Optional[str],
        field_config: Optional[FieldConfig],
    ) -> Optional[str]:
        """Apply name→short mapping if applicable."""
        if value is None or field_config is None:
            return value

        candidates = field_config.get("candidates", [])
        mapping = self._build_name_to_short_mapping(candidates)

        if value in mapping:
            return mapping[value]
        return value

    async def determine_context(
        self,
        text: str,
        contexts: list[ContextConfig],
        filename: Optional[str] = None,
        user_dir: Optional[Path] = None,
        include_all_candidates: bool = False,
    ) -> str:
        """
        Determine the context of a document using tool_use with enum.

        Args:
            text: Extracted text from the document
            contexts: List of available context configurations
            filename: Original filename (provides additional context for classification)
            user_dir: User directory for cost tracking (optional)
            include_all_candidates: If True, include all field candidate names as
                keywords in context descriptions (used for transcript classification)

        Returns:
            The name of the determined context

        Raises:
            AiError: If context determination fails with all models (API errors)
            ConfigurationError: If no contexts provided
        """
        if not text.strip():
            raise AiError("Cannot determine context from empty text")

        if not contexts:
            raise ConfigurationError("No contexts provided - at least one context is required")

        # Truncate text if too long
        if len(text) > self.max_input_chars:
            text = text[: self.max_input_chars] + "\n[... truncated ...]"

        # Build context tool with enum
        context_tool = self._build_context_tool(contexts, include_all_candidates=include_all_candidates)

        logger.debug("Determining context from %d contexts using tool_use", len(contexts))

        # Build user message with optional filename context
        if filename:
            user_content = f"Original filename: {filename}\n\nClassify this document:\n\n{text}"
        else:
            user_content = f"Classify this document:\n\n{text}"

        # Try each model in order until one succeeds
        last_error: Optional[Exception] = None
        
        for model_index, model_config in enumerate(self.models):
            is_last_model = (model_index == len(self.models) - 1)
            
            try:
                # Use per-model settings
                if model_config.extended_thinking:
                    max_tokens = model_config.max_tokens + model_config.thinking_budget
                else:
                    max_tokens = model_config.max_tokens

                create_params = {
                    "model": model_config.name,
                    "max_tokens": max_tokens,
                    "system": self.context_system_prompt,
                    "tools": [context_tool],
                    "messages": [
                        {
                            "role": "user",
                            "content": user_content,
                        }
                    ],
                }
                if model_config.extended_thinking:
                    # Cannot force tool_choice when thinking is enabled
                    create_params["tool_choice"] = {"type": "auto"}
                    create_params["thinking"] = {
                        "type": "enabled",
                        "budget_tokens": model_config.thinking_budget,
                    }
                else:
                    create_params["tool_choice"] = {"type": "tool", "name": "classify_context"}

                # Use streaming for extended thinking (run in thread to avoid blocking)
                def _make_api_call():
                    if model_config.extended_thinking:
                        with self.client.messages.stream(**create_params) as stream:
                            return stream.get_final_message()
                    else:
                        return self.client.messages.create(**create_params)
                
                message = await asyncio.to_thread(_make_api_call)

                # Track costs (count_document=True for first API call per document)
                if user_dir and hasattr(message, "usage"):
                    get_cost_tracker().record_anthropic(
                        model=model_config.name,
                        input_tokens=message.usage.input_tokens,
                        output_tokens=message.usage.output_tokens,
                        user_dir=user_dir,
                        count_document=True,
                    )

                # Extract tool result
                context_name = None
                for block in message.content:
                    if block.type == "tool_use" and block.name == "classify_context":
                        context_name = block.input.get("context")
                        break
                
                if context_name:
                    logger.info("Determined context: %s (model: %s)", context_name, model_config.name)
                    return context_name

                # Context is null - if last model, fail
                if is_last_model:
                    logger.error(
                        "Model %s returned null context, no more models to try",
                        model_config.name
                    )
                    raise AiError("All models failed to determine context")

                # Not last model - try next
                logger.warning(
                    "Model %s returned null context, trying next model (%d/%d)",
                    model_config.name, model_index + 1, len(self.models)
                )
                last_error = AiError(f"Model {model_config.name} returned null context")

            except anthropic.APIError as e:
                logger.warning(
                    "Model %s failed with API error: %s, trying next model (%d/%d)",
                    model_config.name, e, model_index + 1, len(self.models)
                )
                last_error = AiError(f"Anthropic API error with {model_config.name}: {e}")
                continue

        # All models failed with API errors
        logger.error("All models failed for context determination")
        raise last_error or AiError("All models failed")

    async def extract_metadata(
        self,
        text: str,
        contexts: list[ContextConfig],
        primary_language: Optional[str] = None,
        filename: Optional[str] = None,
        user_dir: Optional[Path] = None,
        locked_fields: Optional[dict[str, dict[str, Any]]] = None,
        is_audio: bool = False,
    ) -> tuple[DocumentMetadata, str]:
        """
        Extract metadata from document text using two-pass approach.

        1. First pass: Determine document context (skipped if only one context provided)
        2. Second pass: Extract metadata using context-specific configuration

        Args:
            text: Extracted text from the document
            contexts: List of context configurations
            primary_language: Output language for AI responses
            filename: Original filename (provides additional context for classification)
            user_dir: User directory for cost tracking (optional)
            locked_fields: Dict of field_name -> {"value": str, "clues": list[str]}
                          These fields are locked to specific values (from folder structure)

        Returns:
            Tuple of (DocumentMetadata with extracted fields, filename_pattern)

        Raises:
            AiError: If metadata extraction fails
            ConfigurationError: If configuration is invalid
        """
        # Require minimum text length for meaningful extraction
        MIN_TEXT_LENGTH = 10
        stripped_text = text.strip()
        if not stripped_text or len(stripped_text) < MIN_TEXT_LENGTH:
            raise AiError(
                f"Insufficient text for metadata extraction ({len(stripped_text)} chars, minimum {MIN_TEXT_LENGTH})"
            )

        # Truncate text if too long
        original_len = len(text)
        if len(text) > self.max_input_chars:
            text = text[: self.max_input_chars] + "\n[... truncated ...]"
            logger.debug("Text truncated from %d to %d chars", original_len, self.max_input_chars)

        # First pass: Determine context
        # Skip if only one context is provided (e.g., from forced context via folder)
        if len(contexts) == 1:
            context_name = contexts[0]["name"]
            logger.debug("Single context provided, skipping context determination: %s", context_name)
        else:
            context_name = await self.determine_context(text, contexts, filename=filename, user_dir=user_dir)

        # Find the context configuration
        context_config: Optional[ContextConfig] = None
        for ctx in contexts:
            if ctx["name"] == context_name:
                context_config = ctx
                break

        if context_config is None:
            raise AiError(f"Context '{context_name}' not found in configuration")

        # Get field configs from context
        field_configs = context_config.get("fields", {})
        
        # Get filename pattern (required)
        if is_audio:
            raw_pattern = context_config.get("audio_filename") or context_config.get("filename")
        else:
            raw_pattern = context_config.get("filename")
        if not raw_pattern:
            raise ConfigurationError(
                f"Context '{context_name}' is missing required 'filename' pattern"
            )
        filename_pattern = _resolve_filename_pattern(raw_pattern, filename)

        # Second pass: Extract metadata
        metadata = await self._extract_metadata_with_config(
            text=text,
            field_configs=field_configs,
            context_name=context_name,
            primary_language=primary_language,
            filename=filename,
            user_dir=user_dir,
            locked_fields=locked_fields,
        )
        
        return metadata, filename_pattern

    async def _extract_metadata_with_config(
        self,
        text: str,
        field_configs: dict[str, FieldConfig],
        context_name: str,
        primary_language: Optional[str] = None,
        filename: Optional[str] = None,
        user_dir: Optional[Path] = None,
        locked_fields: Optional[dict[str, dict[str, Any]]] = None,
    ) -> DocumentMetadata:
        """
        Extract metadata using specified field configurations.

        Args:
            text: Document text
            field_configs: Configuration for each field from context's 'fields'
            context_name: Name of the determined context
            primary_language: Output language
            filename: Original filename (provides additional context)
            user_dir: User directory for cost tracking (optional)
            locked_fields: Dict of field_name -> {"value": str, "clues": list[str]}
                          These fields are locked and should not be extracted by AI

        Returns:
            DocumentMetadata with extracted fields
        """
        # Filter out locked fields from extraction - they're already determined
        if locked_fields:
            unlocked_field_configs = {
                k: v for k, v in field_configs.items() 
                if k not in locked_fields
            }
            logger.debug("Locked fields: %s, extracting: %s", 
                        list(locked_fields.keys()), list(unlocked_field_configs.keys()))
        else:
            unlocked_field_configs = field_configs

        # Build extraction tool for unlocked fields only
        extraction_tool = self._build_extraction_tool(unlocked_field_configs)

        # Build system prompt with language instruction
        if primary_language:
            lang_instruction = self.language_instruction_template.format(
                language=primary_language
            )
        else:
            lang_instruction = self.default_language_instruction

        system_prompt = self.extraction_system_prompt.format(
            language_instruction=lang_instruction
        )

        # Build user message with optional filename context and locked field info
        user_parts = []
        
        if filename:
            user_parts.append(f"Original filename: {filename}")
        
        # Add locked fields information with clues
        if locked_fields:
            locked_info = ["The following fields have been pre-determined and are fixed:"]
            for field_name, field_info in locked_fields.items():
                value = field_info.get("value", "")
                clues = field_info.get("clues", [])
                locked_info.append(f"  - {field_name}: {value}")
                if clues:
                    locked_info.append(f"    Context clues: {', '.join(clues)}")
            locked_info.append("")
            locked_info.append("Use this context when extracting other fields.")
            user_parts.append("\n".join(locked_info))
        
        user_parts.append(f"Extract metadata from this document:\n\n{text}")
        user_content = "\n\n".join(user_parts)

        logger.debug("Extracting metadata using tool_use (context: %s, fields: %s, locked: %s)", 
                     context_name, list(unlocked_field_configs.keys()),
                     list(locked_fields.keys()) if locked_fields else [])

        # Try each model in order until one succeeds
        last_error: Optional[Exception] = None
        last_metadata: Optional[DocumentMetadata] = None
        
        for model_index, model_config in enumerate(self.models):
            is_last_model = (model_index == len(self.models) - 1)
            
            try:
                # Use per-model settings
                if model_config.extended_thinking:
                    max_tokens = model_config.max_tokens + model_config.thinking_budget
                else:
                    max_tokens = model_config.max_tokens

                create_params = {
                    "model": model_config.name,
                    "max_tokens": max_tokens,
                    "system": system_prompt,
                    "tools": [extraction_tool],
                    "messages": [
                        {
                            "role": "user",
                            "content": user_content,
                        }
                    ],
                }
                if model_config.extended_thinking:
                    # Cannot force tool_choice when thinking is enabled
                    create_params["tool_choice"] = {"type": "auto"}
                    create_params["thinking"] = {
                        "type": "enabled",
                        "budget_tokens": model_config.thinking_budget,
                    }
                else:
                    create_params["tool_choice"] = {"type": "tool", "name": "extract_metadata"}

                # Use streaming for extended thinking (run in thread to avoid blocking)
                def _make_api_call():
                    if model_config.extended_thinking:
                        with self.client.messages.stream(**create_params) as stream:
                            return stream.get_final_message()
                    else:
                        return self.client.messages.create(**create_params)
                
                message = await asyncio.to_thread(_make_api_call)

                # Track costs
                if user_dir and hasattr(message, "usage"):
                    get_cost_tracker().record_anthropic(
                        model=model_config.name,
                        input_tokens=message.usage.input_tokens,
                        output_tokens=message.usage.output_tokens,
                        user_dir=user_dir,
                    )

                # Extract tool result
                result = None
                for block in message.content:
                    if block.type == "tool_use" and block.name == "extract_metadata":
                        result = block.input
                        break
                
                if result is None:
                    # No tool_use result at all - try next model
                    if is_last_model and last_metadata is not None:
                        # Use last successful metadata even if date was null
                        logger.warning(
                            "Model %s returned no tool result, using previous metadata",
                            model_config.name
                        )
                        return last_metadata
                    
                    logger.warning(
                        "Model %s returned no metadata, trying next model (%d/%d)",
                        model_config.name, model_index + 1, len(self.models)
                    )
                    last_error = AiError(f"Model {model_config.name} returned no metadata from tool_use")
                    continue
                
                # Parse the result
                metadata = self._parse_tool_result(
                    result, unlocked_field_configs, locked_fields, context_name
                )
                last_metadata = metadata
                
                # Check if date is null - if so, try next model (unless last)
                if metadata.date is None:
                    if is_last_model:
                        # Last model - accept result even with null date
                        logger.warning(
                            "Model %s returned null date, accepting result (last model)",
                            model_config.name
                        )
                        return metadata
                    
                    logger.warning(
                        "Model %s returned null date, trying next model (%d/%d)",
                        model_config.name, model_index + 1, len(self.models)
                    )
                    last_error = AiError(f"Model {model_config.name} returned null date")
                    continue
                
                # Success - date is not null
                logger.debug("Metadata extracted successfully (model: %s)", model_config.name)
                return metadata

            except anthropic.APIError as e:
                logger.warning(
                    "Model %s failed with API error: %s, trying next model (%d/%d)",
                    model_config.name, e, model_index + 1, len(self.models)
                )
                last_error = AiError(f"Anthropic API error with {model_config.name}: {e}")
                continue

        # All models failed - if we have any metadata, return it
        if last_metadata is not None:
            logger.warning("All models failed to extract complete metadata, using last result")
            return last_metadata
        
        logger.error("All models failed for metadata extraction")
        raise last_error or AiError("All models failed")

    def _parse_tool_result(
        self,
        result: dict[str, Any],
        unlocked_field_configs: dict[str, FieldConfig],
        locked_fields: Optional[dict[str, dict[str, Any]]],
        context_name: str,
    ) -> DocumentMetadata:
        """Parse tool result into DocumentMetadata, applying name→short mappings.
        
        Args:
            result: Raw result dict from AI tool call
            unlocked_field_configs: Field configs for fields that were sent to AI
            locked_fields: Fields pre-determined from folder structure (not sent to AI)
            context_name: Name of the context
        """
        # Parse date (always available)
        doc_date = None
        if result.get("date"):
            try:
                doc_date = date.fromisoformat(result["date"])
            except ValueError:
                logger.warning("Invalid date format: %s", result.get("date"))

        # Process extracted fields (unlocked only - locked fields weren't sent to AI)
        fields = {}
        new_clues = {}
        
        for field_name, field_config in unlocked_field_configs.items():
            raw_value = result.get(field_name)
            
            # Apply name→short mapping if field has candidates
            mapped_value = self._apply_name_to_short_mapping(raw_value, field_config)
            fields[field_name] = mapped_value
            
            # Check for new clues (use raw value before short mapping)
            new_clue = result.get(f"{field_name}_new_clue")
            if new_clue and isinstance(new_clue, str) and new_clue.strip() and raw_value:
                candidates = field_config.get("candidates", [])
                if candidates:
                    allowing_clues = self._get_candidates_allowing_new_clues(candidates)
                    if raw_value in allowing_clues:
                        new_clues[field_name] = (raw_value, new_clue.strip())
                        logger.info(
                            "New clue suggested for %s '%s': %s",
                            field_name, raw_value, new_clue.strip()
                        )

        # Add locked fields (already determined from folder structure)
        if locked_fields:
            for field_name, field_info in locked_fields.items():
                fields[field_name] = field_info.get("value")
            logger.debug("Added locked fields to metadata: %s", list(locked_fields.keys()))

        metadata = DocumentMetadata(
            fields=fields,
            date=doc_date,
            context=context_name,
            new_clues=new_clues,
        )

        # Log extracted fields
        field_summary = ", ".join(
            f"{k}={v[:30] if isinstance(v, str) and len(v) > 30 else v}"
            for k, v in fields.items()
            if v is not None
        )
        logger.info(
            "Metadata extracted: context=%s, date=%s, %s",
            metadata.context,
            metadata.date,
            field_summary,
        )

        return metadata
