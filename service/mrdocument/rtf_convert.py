"""RTF to PDF conversion via unrtf (RTF → HTML) and WeasyPrint (HTML → PDF)."""

import logging
import subprocess

from .html_convert import html_to_pdf

logger = logging.getLogger(__name__)


def rtf_to_pdf(rtf_bytes: bytes, filename: str = "document.rtf") -> bytes:
    """
    Convert an RTF file to PDF.

    Uses unrtf to convert RTF → HTML, then WeasyPrint to render HTML → PDF.

    Args:
        rtf_bytes: Raw bytes of the RTF file
        filename: Original filename (for logging)

    Returns:
        PDF file as bytes

    Raises:
        ValueError: If RTF cannot be converted
    """
    logger.debug("Converting RTF to HTML: %s (%d bytes)", filename, len(rtf_bytes))

    try:
        result = subprocess.run(
            ["unrtf", "--html"],
            input=rtf_bytes,
            capture_output=True,
            timeout=30,
        )
    except FileNotFoundError:
        raise ValueError("unrtf is not installed")
    except subprocess.TimeoutExpired:
        raise ValueError(f"RTF conversion timed out for {filename}")

    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        raise ValueError(f"unrtf failed for {filename}: {stderr}")

    html_bytes = result.stdout
    if not html_bytes:
        raise ValueError(f"unrtf produced no output for {filename}")

    logger.info("Converted RTF to HTML: %s (%d -> %d bytes)", filename, len(rtf_bytes), len(html_bytes))
    return html_to_pdf(html_bytes, filename)
