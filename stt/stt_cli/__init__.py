"""STT CLI - Speech-to-Text transcription tool using ElevenLabs."""

__version__ = "0.1.0"

from .transcript import TranscriptWord, TranscriptSegment, TranscriptResult
from .backends import get_backend, Backend

__all__ = [
    "TranscriptWord",
    "TranscriptSegment",
    "TranscriptResult",
    "get_backend",
    "Backend",
]
