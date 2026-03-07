"""PDF manipulation utilities for embedding metadata."""

import io
import logging
from datetime import date, datetime, timezone
from typing import Optional

import pikepdf

logger = logging.getLogger(__name__)


def embed_metadata(
    pdf_bytes: bytes,
    doc_type: Optional[str] = None,
    doc_date: Optional[date] = None,
    sender: Optional[str] = None,
    topic: Optional[str] = None,
    subject: Optional[str] = None,
    keywords: Optional[list[str]] = None,
) -> bytes:
    """
    Embed metadata into a PDF document.

    Uses both the PDF Info dictionary and XMP metadata for compatibility
    with various PDF readers.

    Args:
        pdf_bytes: Raw PDF file bytes
        doc_type: Document type/category (stored as Subject and Keywords)
        doc_date: Document date (stored as CreationDate)
        sender: Document sender/author (stored as Author)
        topic: General topic/dossier the document belongs to (stored in custom field and dc:description)
        subject: Document subject/title (stored as Title)
        keywords: List of keywords (combined with type, topic, sender in Keywords field)

    Returns:
        PDF bytes with embedded metadata
    """
    input_stream = io.BytesIO(pdf_bytes)
    output_stream = io.BytesIO()

    with pikepdf.open(input_stream) as pdf:
        # Access the document info dictionary
        # set_pikepdf_as_editor=False prevents automatic overwrites
        with pdf.open_metadata(set_pikepdf_as_editor=False) as meta:
            # Set Dublin Core metadata (XMP)
            if subject:
                meta["dc:title"] = subject
            if sender:
                meta["dc:creator"] = [sender]
            if doc_type:
                meta["dc:subject"] = [doc_type]
                meta["dc:type"] = doc_type
            if topic:
                # Use dc:description for topic/dossier information
                meta["dc:description"] = topic
            if doc_date:
                # XMP dates should be in ISO 8601 format
                meta["dc:date"] = [doc_date.isoformat()]

            # Set PDF metadata (XMP)
            # Combine doc_type, topic, sender, and keywords for searchability
            all_keywords = [k for k in [doc_type, topic, sender] if k]
            if keywords:
                all_keywords.extend(keywords)
            if all_keywords:
                meta["pdf:Keywords"] = ", ".join(all_keywords)
            if sender:
                meta["pdf:Producer"] = f"MrDocument (source: {sender})"

            # Set XMP basic metadata
            meta["xmp:CreatorTool"] = "MrDocument"
            meta["xmp:MetadataDate"] = datetime.now(timezone.utc).isoformat()
            if doc_date:
                meta["xmp:CreateDate"] = datetime.combine(
                    doc_date, datetime.min.time(), tzinfo=timezone.utc
                ).isoformat()

        # Also set the Info dictionary for legacy compatibility
        if pdf.docinfo is None:
            pdf.docinfo = pikepdf.Dictionary()

        if subject:
            pdf.docinfo["/Title"] = subject
        if sender:
            pdf.docinfo["/Author"] = sender
        if doc_type:
            pdf.docinfo["/Subject"] = doc_type
        # Combine doc_type, topic, sender, and keywords for searchability
        all_keywords = [k for k in [doc_type, topic, sender] if k]
        if keywords:
            all_keywords.extend(keywords)
        if all_keywords:
            pdf.docinfo["/Keywords"] = ", ".join(all_keywords)
        if topic:
            # Store topic in custom field (using standard /Topic key)
            pdf.docinfo["/Topic"] = topic
        if doc_date:
            # PDF date format: D:YYYYMMDDHHmmSSOHH'mm'
            pdf.docinfo["/CreationDate"] = f"D:{doc_date.strftime('%Y%m%d')}000000Z"

        pdf.docinfo["/Creator"] = "MrDocument"
        pdf.docinfo["/ModDate"] = f"D:{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}Z"

        pdf.save(output_stream)

    result = output_stream.getvalue()
    logger.debug(
        "Embedded metadata into PDF: type=%s, date=%s, sender=%s, topic=%s, subject=%s, keywords=%s (%d -> %d bytes)",
        doc_type,
        doc_date,
        sender,
        topic[:30] if topic else None,
        subject[:30] if subject else None,
        keywords,
        len(pdf_bytes),
        len(result),
    )
    return result
