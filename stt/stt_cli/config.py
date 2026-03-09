"""Configuration management for STT CLI."""

from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

import yaml


DEFAULT_CONFIG_DIR = Path.home() / ".config" / "stt-cli"
DEFAULT_CONFIG_FILE = DEFAULT_CONFIG_DIR / "config.yaml"

# Module-level config path (can be overridden via CLI)
_config_path: Optional[Path] = None


def set_config_path(path: Optional[Path]) -> None:
    """Set custom config file path."""
    global _config_path
    _config_path = path


def get_config_path() -> Path:
    """Get current config file path."""
    return _config_path or DEFAULT_CONFIG_FILE


@dataclass
class Config:
    """Application configuration."""

    # ElevenLabs settings
    elevenlabs_model: str = "scribe_v2"  # scribe_v1, scribe_v1_experimental, or scribe_v2

    # Anthropic settings (for post-processing)
    anthropic_model: str = "claude-opus-4-20250514"
    correction_context: str = ""  # Additional context for correction (names, terms, etc.)
    extended_thinking: bool = True  # Enable extended thinking mode
    thinking_budget: int = 50000  # Token budget for extended thinking
    use_batch_api: bool = True  # Use batch API (async) vs direct API (sync)

    # Common settings
    default_language: str = "de-DE"
    enable_diarization: bool = False
    enable_word_timestamps: bool = False
    diarization_speaker_count: int = 2

    @classmethod
    def load(cls, config_path: Optional[Path] = None) -> "Config":
        """Load configuration from file."""
        path = config_path or get_config_path()
        if path.exists():
            with open(path) as f:
                data = yaml.safe_load(f) or {}
            # Filter out unknown keys (from old config)
            valid_keys = {f.name for f in cls.__dataclass_fields__.values()}
            filtered_data = {k: v for k, v in data.items() if k in valid_keys}
            return cls(**filtered_data)
        return cls()

    def save(self, config_path: Optional[Path] = None) -> None:
        """Save configuration to file."""
        path = config_path or get_config_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            yaml.dump(asdict(self), f, default_flow_style=False, allow_unicode=True)
