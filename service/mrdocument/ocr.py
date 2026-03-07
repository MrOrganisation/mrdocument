"""Interface to the OCRmyPDF service."""

import base64
import logging
import os
from dataclasses import dataclass

import aiohttp

logger = logging.getLogger(__name__)


@dataclass
class OcrResult:
    """Result from OCR processing."""

    pdf_bytes: bytes
    text: str
    filename: str
    signature_invalidated: bool = False


class OcrError(Exception):
    """Error during OCR processing."""

    pass


class OcrClient:
    """Client for the OCRmyPDF service."""

    def __init__(self, base_url: str):
        """
        Initialize OCR client.

        Args:
            base_url: Base URL of the OCR service (e.g., "http://ocrmypdf:5000")
        """
        self.base_url = base_url.rstrip("/")

    async def process_pdf(
        self,
        pdf_bytes: bytes,
        filename: str,
        language: str = "deu+eng",
    ) -> OcrResult:
        """
        Process a PDF file through OCR.

        Args:
            pdf_bytes: Raw PDF file bytes
            filename: Original filename of the PDF
            language: OCR language code (default: "eng")

        Returns:
            OcrResult containing OCR'd PDF bytes, extracted text, and filename

        Raises:
            OcrError: If OCR processing fails
        """
        url = f"{self.base_url}/ocr"

        # Truncate filename to avoid filesystem limits (keep under 100 chars + extension)
        if len(filename) > 104:
            name, ext = os.path.splitext(filename)
            filename = name[:100] + ext
            logger.debug("Truncated filename to: %s", filename)

        logger.debug("Sending OCR request to %s for file %s", url, filename)

        form_data = aiohttp.FormData()
        form_data.add_field(
            "file",
            pdf_bytes,
            filename=filename,
            content_type="application/pdf",
        )
        form_data.add_field("language", language)
        form_data.add_field("skip_text", "true")
        form_data.add_field("deskew", "true")
        form_data.add_field("clean", "true")
        form_data.add_field("return_text", "true")

        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=form_data) as response:
                if response.status != 200:
                    try:
                        error_data = await response.json()
                        error_msg = error_data.get("error", "Unknown error")
                        details = error_data.get("details", "")
                        logger.error("OCR service returned error: %s - %s", error_msg, details)
                        raise OcrError(f"OCR failed: {error_msg}. {details}".strip())
                    except aiohttp.ContentTypeError:
                        text = await response.text()
                        logger.error("OCR service returned status %d: %s", response.status, text)
                        raise OcrError(f"OCR failed with status {response.status}: {text}")

                data = await response.json()

                pdf_base64 = data.get("pdf")
                if not pdf_base64:
                    logger.error("OCR response missing PDF data")
                    raise OcrError("OCR response missing PDF data")

                text = data.get("text", "")
                signature_invalidated = data.get("signature_invalidated", False)
                
                logger.info(
                    "OCR completed for %s: %d bytes PDF, %d chars text, signature_invalidated=%s",
                    filename,
                    len(pdf_base64),
                    len(text),
                    signature_invalidated,
                )

                return OcrResult(
                    pdf_bytes=base64.b64decode(pdf_base64),
                    text=text,
                    filename=data.get("filename", filename),
                    signature_invalidated=signature_invalidated,
                )

    async def health_check(self) -> bool:
        """
        Check if the OCR service is healthy.

        Returns:
            True if healthy, False otherwise
        """
        url = f"{self.base_url}/health"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    healthy = response.status == 200
                    if not healthy:
                        logger.warning("OCR health check failed: status %d", response.status)
                    return healthy
        except aiohttp.ClientError as e:
            logger.warning("OCR health check failed: %s", e)
            return False
