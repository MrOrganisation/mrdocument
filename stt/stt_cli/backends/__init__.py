"""Backend implementations for speech-to-text transcription."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from ..transcript import TranscriptResult


@dataclass
class TranscriptionJob:
    """Represents a transcription job that may be async or sync."""

    job_id: str
    backend: str
    is_complete: bool = False
    result: Optional[TranscriptResult] = None
    error: Optional[str] = None
    progress: int = 0  # 0-100


class Backend(ABC):
    """Abstract base class for transcription backends."""

    name: str = "base"

    @abstractmethod
    def transcribe(
        self,
        audio_path: Path,
        language: str = "de-DE",
        enable_diarization: bool = False,
        speaker_count: int = 2,
        enable_word_timestamps: bool = False,
        **kwargs,
    ) -> TranscriptionJob:
        """
        Start or perform transcription.

        Args:
            audio_path: Path to the audio file.
            language: Language code (e.g., 'de-DE', 'en-US').
            enable_diarization: Enable speaker diarization.
            speaker_count: Expected number of speakers.
            enable_word_timestamps: Include word-level timestamps.
            **kwargs: Backend-specific options.

        Returns:
            TranscriptionJob with job_id and status.
        """
        pass

    @abstractmethod
    def get_status(self, job_id: str, **kwargs) -> TranscriptionJob:
        """
        Get the status of a transcription job.

        Args:
            job_id: The job identifier.
            **kwargs: Backend-specific options.

        Returns:
            Updated TranscriptionJob with current status.
        """
        pass

    @abstractmethod
    def get_result(self, job_id: str, **kwargs) -> Optional[TranscriptResult]:
        """
        Get the transcript result for a completed job.

        Args:
            job_id: The job identifier.
            **kwargs: Backend-specific options.

        Returns:
            TranscriptResult if complete, None if still processing.
        """
        pass


def get_backend(name: str, **config) -> Backend:
    """
    Get a backend instance by name.

    Args:
        name: Backend name ('elevenlabs').
        **config: Backend-specific configuration.

    Returns:
        Backend instance.
    """
    if name == "elevenlabs":
        from .elevenlabs import ElevenLabsBackend
        return ElevenLabsBackend(**config)
    else:
        raise ValueError(f"Unknown backend: {name}. Available: elevenlabs")
