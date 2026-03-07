"""Image to PDF conversion utilities."""

import io
import logging
from pathlib import Path
from typing import Optional

import img2pdf
from PIL import Image

logger = logging.getLogger(__name__)

# Supported image extensions (lowercase)
SUPPORTED_IMAGE_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".gif", ".tiff", ".tif", 
    ".bmp", ".webp", ".ppm", ".pgm", ".pbm", ".pnm"
}

# MIME types for supported images
IMAGE_MIME_TYPES = {
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".gif": "image/gif",
    ".tiff": "image/tiff",
    ".tif": "image/tiff",
    ".bmp": "image/bmp",
    ".webp": "image/webp",
    ".ppm": "image/x-portable-pixmap",
    ".pgm": "image/x-portable-graymap",
    ".pbm": "image/x-portable-bitmap",
    ".pnm": "image/x-portable-anymap",
}


def is_supported_image(filename: str) -> bool:
    """
    Check if a filename has a supported image extension.

    Args:
        filename: Filename to check

    Returns:
        True if supported image type, False otherwise
    """
    ext = Path(filename).suffix.lower()
    return ext in SUPPORTED_IMAGE_EXTENSIONS


def get_image_content_type(filename: str) -> Optional[str]:
    """
    Get the MIME content type for an image filename.

    Args:
        filename: Image filename

    Returns:
        MIME type string or None if not a supported image
    """
    ext = Path(filename).suffix.lower()
    return IMAGE_MIME_TYPES.get(ext)


def image_to_pdf(image_bytes: bytes, filename: str = "image") -> bytes:
    """
    Convert an image file to PDF.

    Uses img2pdf for lossless conversion when possible (JPEG, PNG, etc.),
    falls back to Pillow for formats not supported by img2pdf.

    Args:
        image_bytes: Raw bytes of the image file
        filename: Original filename (used to determine format)

    Returns:
        PDF file as bytes

    Raises:
        ValueError: If image cannot be converted
    """
    ext = Path(filename).suffix.lower()
    
    logger.debug(f"Converting image to PDF: {filename} ({len(image_bytes)} bytes)")

    # Try img2pdf first for lossless conversion (works well with JPEG, PNG)
    try:
        # img2pdf works directly with image bytes for supported formats
        pdf_bytes = img2pdf.convert(image_bytes)
        logger.info(f"Converted image to PDF using img2pdf: {len(image_bytes)} -> {len(pdf_bytes)} bytes")
        return pdf_bytes
    except img2pdf.ImageOpenError as e:
        logger.debug(f"img2pdf failed, falling back to Pillow: {e}")
    except Exception as e:
        logger.debug(f"img2pdf failed with unexpected error, falling back to Pillow: {e}")

    # Fallback to Pillow for unsupported formats (GIF, BMP, WebP, etc.)
    try:
        # Open image with Pillow
        img = Image.open(io.BytesIO(image_bytes))
        
        # Convert to RGB if necessary (PDF doesn't support all color modes)
        if img.mode in ("RGBA", "LA", "P"):
            # Convert palette and alpha images to RGB
            background = Image.new("RGB", img.size, (255, 255, 255))
            if img.mode == "P":
                img = img.convert("RGBA")
            if img.mode in ("RGBA", "LA"):
                background.paste(img, mask=img.split()[-1])  # Use alpha channel as mask
                img = background
            else:
                img = img.convert("RGB")
        elif img.mode != "RGB":
            img = img.convert("RGB")

        # Save as PDF
        output = io.BytesIO()
        img.save(output, format="PDF", resolution=100.0)
        pdf_bytes = output.getvalue()
        
        logger.info(f"Converted image to PDF using Pillow: {len(image_bytes)} -> {len(pdf_bytes)} bytes")
        return pdf_bytes

    except Exception as e:
        logger.error(f"Failed to convert image to PDF: {e}")
        raise ValueError(f"Failed to convert image to PDF: {e}")
