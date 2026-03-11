"""HTTP API for STT transcription service."""

import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, File, Form, HTTPException, UploadFile

from .backends.elevenlabs import ElevenLabsBackend
from .convert import needs_conversion, is_supported, convert_to_flac
from .postprocess import transcript_to_json

# Configure logging for the package
log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
log_dir = os.environ.get("LOG_DIR")

handlers = [logging.StreamHandler()]
if log_dir:
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    log_file = Path(log_dir) / "stt.log"
    handlers.append(logging.FileHandler(log_file))

logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=handlers,
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="STT API",
    description="Speech-to-Text API using ElevenLabs",
    version="0.2.0",
)


@app.post("/transcribe")
async def transcribe(
    # Audio file
    file: UploadFile = File(..., description="Audio file to transcribe"),
    # ElevenLabs settings
    elevenlabs_model: str = Form("scribe_v2", description="ElevenLabs model"),
    keyterms: Optional[str] = Form(None, description="JSON array of key terms to help transcription accuracy"),
    # Common settings
    language: str = Form("de-DE", description="Language code"),
    enable_diarization: bool = Form(False, description="Enable speaker diarization"),
    diarization_speaker_count: int = Form(2, description="Expected speaker count"),
    enable_word_timestamps: bool = Form(False, description="Enable word timestamps"),
) -> dict:
    """
    Transcribe an audio file and return raw transcription JSON.

    Returns JSON with:
    - transcript: Full transcript with word-level data (segments with words array)

    Post-processing (correction, PDF generation) is handled by MrDocument.
    """
    # Check API keys
    if not os.environ.get("ELEVENLABS_API_KEY"):
        raise HTTPException(status_code=500, detail="ELEVENLABS_API_KEY not configured")

    # Save uploaded file to temp location
    suffix = Path(file.filename).suffix if file.filename else ".tmp"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        content = await file.read()
        tmp.write(content)
        tmp_path = Path(tmp.name)

    converted_path: Optional[Path] = None
    try:
        # Check file format
        if not is_supported(tmp_path) and not needs_conversion(tmp_path):
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported file format: {suffix}. "
                       "Supported: .flac, .wav, .mp3, .ogg, .webm, .mp4, .m4a, .mkv, .avi, .mov"
            )

        # Convert if necessary
        process_path = tmp_path

        if needs_conversion(tmp_path):
            logger.info("Converting %s to FLAC (uploaded as %s, %d bytes)...",
                        suffix, file.filename, len(content))
            converted_path = convert_to_flac(tmp_path)
            process_path = converted_path
            logger.info("Conversion complete: %s (%d bytes)",
                        converted_path.name, converted_path.stat().st_size)

        # Parse keyterms if provided
        keyterms_list: Optional[list[str]] = None
        if keyterms:
            try:
                keyterms_list = json.loads(keyterms)
                if not isinstance(keyterms_list, list) or not all(isinstance(k, str) for k in keyterms_list):
                    raise ValueError("keyterms must be a JSON array of strings")
            except json.JSONDecodeError as e:
                raise HTTPException(status_code=400, detail=f"Invalid keyterms JSON: {e}")

        # ElevenLabs transcription
        logger.info(f"Transcribing with ElevenLabs ({elevenlabs_model})...")
        try:
            backend = ElevenLabsBackend(model=elevenlabs_model)
            job = backend.transcribe(
                audio_path=process_path,
                language=language,
                enable_diarization=enable_diarization,
                speaker_count=diarization_speaker_count,
                enable_word_timestamps=enable_word_timestamps,
                keyterms=keyterms_list,
                original_filename=file.filename,
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            logger.exception("Transcription failed")
            raise HTTPException(status_code=500, detail=f"Transcription failed: {e}")

        transcript = job.result
        if not transcript or not transcript.segments:
            raise HTTPException(status_code=500, detail="No transcript segments returned")

        # Aggregate by speaker if diarization was used
        has_speakers = any(seg.speaker_tag is not None for seg in transcript.segments)
        if has_speakers and transcript.get_all_words():
            transcript = transcript.aggregate_by_speaker()

        # Convert to full JSON (with words)
        full_json = transcript_to_json(transcript, include_words=True)

        return {
            "transcript": full_json,
        }

    finally:
        # Clean up temp files
        if tmp_path.exists():
            tmp_path.unlink()
        if converted_path and converted_path.exists():
            converted_path.unlink()


@app.get("/health")
async def health_check() -> dict:
    """Health check endpoint."""
    return {
        "status": "healthy",
        "elevenlabs_key_set": bool(os.environ.get("ELEVENLABS_API_KEY")),
    }


def run_server(host: str = "0.0.0.0", port: int = 8000) -> None:
    """Run the API server."""
    import uvicorn
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    run_server()
