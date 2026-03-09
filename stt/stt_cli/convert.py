"""Audio conversion utilities using FFmpeg."""

import logging
import subprocess
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)

# Supported audio formats (no conversion needed)
SUPPORTED_FORMATS = {".flac", ".wav", ".mp3", ".ogg", ".webm"}

# Formats that need conversion (video containers with audio tracks)
CONVERTIBLE_FORMATS = {".mp4", ".m4a", ".mkv", ".avi", ".mov"}


def is_supported(file_path: Path) -> bool:
    """Check if the file format is directly supported."""
    return file_path.suffix.lower() in SUPPORTED_FORMATS


def needs_conversion(file_path: Path) -> bool:
    """Check if the file needs to be converted."""
    return file_path.suffix.lower() in CONVERTIBLE_FORMATS


def convert_to_flac(input_path: Path, output_path: Path | None = None) -> Path:
    """
    Extract audio from an audio/video file and convert to FLAC format.

    Args:
        input_path: Path to the input file.
        output_path: Optional output path. If not provided, creates a unique temp file.

    Returns:
        Path to the converted FLAC file.

    Raises:
        FileNotFoundError: If ffmpeg is not installed.
        RuntimeError: If conversion fails or output has no audio content.
    """
    if output_path is None:
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".flac")
        output_path = Path(tmp.name)
        tmp.close()

    input_size = input_path.stat().st_size
    logger.info(
        "Converting %s to FLAC (input: %.1f MB)",
        input_path.suffix, input_size / (1024 * 1024),
    )

    # First, probe the input to check for audio streams
    probe_cmd = [
        "ffprobe",
        "-v", "error",
        "-select_streams", "a",
        "-show_entries", "stream=codec_name,sample_rate,channels,duration",
        "-of", "default=noprint_wrappers=1",
        str(input_path),
    ]
    try:
        probe_result = subprocess.run(
            probe_cmd, capture_output=True, text=True, timeout=30,
        )
        probe_output = probe_result.stdout.strip()
        if probe_output:
            logger.info("Input audio streams: %s", probe_output.replace("\n", ", "))
        else:
            logger.warning("No audio streams found in %s", input_path.name)
            raise RuntimeError(
                f"No audio stream found in {input_path.suffix} file. "
                "The file may be video-only or use an unsupported audio codec."
            )
    except FileNotFoundError:
        pass  # ffprobe not available, proceed with conversion anyway
    except subprocess.TimeoutExpired:
        logger.warning("ffprobe timed out for %s", input_path.name)

    cmd = [
        "ffmpeg",
        "-i", str(input_path),
        "-vn",  # No video
        "-acodec", "flac",
        "-ar", "16000",  # 16kHz sample rate (good for speech)
        "-ac", "1",  # Mono
        "-y",  # Overwrite output
        str(output_path),
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
        )
        if result.stderr:
            # FFmpeg writes progress info to stderr even on success
            logger.debug("FFmpeg stderr: %s", result.stderr[-500:])
    except FileNotFoundError:
        raise FileNotFoundError(
            "ffmpeg not found. Please install ffmpeg: "
            "brew install ffmpeg (macOS) or apt install ffmpeg (Linux)"
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"FFmpeg conversion failed: {e.stderr}")

    # Validate the output
    if not output_path.exists():
        raise RuntimeError("FFmpeg produced no output file")

    output_size = output_path.stat().st_size
    if output_size < 1000:  # Less than 1KB is almost certainly empty/header-only
        raise RuntimeError(
            f"FFmpeg conversion produced near-empty output ({output_size} bytes). "
            "The input file may not contain a usable audio track."
        )

    # Check duration of converted audio
    try:
        duration = get_audio_duration(output_path)
        logger.info(
            "Conversion complete: %.1f MB -> %.1f MB, duration: %.1fs",
            input_size / (1024 * 1024),
            output_size / (1024 * 1024),
            duration,
        )
        if duration < 0.5:
            raise RuntimeError(
                f"Converted audio is too short ({duration:.1f}s). "
                "The input file may not contain a usable audio track."
            )
    except RuntimeError as e:
        if "too short" in str(e):
            raise
        # get_audio_duration failed but we have a non-empty file, proceed
        logger.warning("Could not verify audio duration: %s", e)

    return output_path


def get_audio_duration(file_path: Path) -> float:
    """
    Get the duration of an audio file in seconds.

    Args:
        file_path: Path to the audio file.

    Returns:
        Duration in seconds.

    Raises:
        FileNotFoundError: If ffprobe is not installed.
        RuntimeError: If duration cannot be determined.
    """
    cmd = [
        "ffprobe",
        "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        str(file_path),
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
        )
        return float(result.stdout.strip())
    except FileNotFoundError:
        raise FileNotFoundError(
            "ffprobe not found. Please install ffmpeg: "
            "brew install ffmpeg (macOS) or apt install ffmpeg (Linux)"
        )
    except (subprocess.CalledProcessError, ValueError) as e:
        raise RuntimeError(f"Could not determine audio duration: {e}")
