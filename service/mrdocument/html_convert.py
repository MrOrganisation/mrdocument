"""HTML to PDF conversion utilities."""

import io
import logging
import re

from weasyprint import HTML

logger = logging.getLogger(__name__)

# Default CSS for rendering HTML as PDF
DEFAULT_CSS = """
body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
    font-size: 11pt;
    line-height: 1.5;
    color: #333;
    max-width: 800px;
    margin: 0 auto;
    padding: 20px;
}
img {
    max-width: 100%;
    height: auto;
}
table {
    border-collapse: collapse;
    width: 100%;
}
th, td {
    border: 1px solid #ddd;
    padding: 8px;
    text-align: left;
}
"""


def _strip_html_tags(html_content: str) -> str:
    """Strip HTML tags for fallback plain text rendering."""
    # Remove script and style elements
    clean = re.sub(r'<(script|style)[^>]*>.*?</\1>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    # Remove HTML tags
    clean = re.sub(r'<[^>]+>', ' ', clean)
    # Normalize whitespace
    clean = re.sub(r'\s+', ' ', clean)
    return clean.strip()


def html_to_pdf(html_bytes: bytes, filename: str = "document.html") -> bytes:
    """
    Convert an HTML file to PDF.

    Args:
        html_bytes: Raw bytes of the HTML file
        filename: Original filename (for logging)

    Returns:
        PDF file as bytes

    Raises:
        ValueError: If HTML cannot be converted
    """
    logger.debug(f"Converting HTML to PDF: {filename} ({len(html_bytes)} bytes)")

    # Try to decode HTML content
    html_content = None
    for encoding in ('utf-8', 'latin-1', 'cp1252'):
        try:
            html_content = html_bytes.decode(encoding)
            break
        except UnicodeDecodeError:
            continue

    if html_content is None:
        raise ValueError("Could not decode HTML content")

    # Inject default CSS if no style is present
    if '<style' not in html_content.lower() and 'style=' not in html_content.lower():
        style_tag = f"<style>{DEFAULT_CSS}</style>"
        if '<head>' in html_content.lower():
            html_content = re.sub(
                r'(<head[^>]*>)',
                f'\\1{style_tag}',
                html_content,
                flags=re.IGNORECASE
            )
        elif '<html>' in html_content.lower():
            html_content = re.sub(
                r'(<html[^>]*>)',
                f'\\1<head>{style_tag}</head>',
                html_content,
                flags=re.IGNORECASE
            )
        else:
            html_content = f"<html><head>{style_tag}</head><body>{html_content}</body></html>"

    output = io.BytesIO()
    try:
        HTML(string=html_content).write_pdf(output)
    except Exception as e:
        logger.warning(f"WeasyPrint failed on HTML content, trying fallback: {e}")
        # Fallback: strip HTML and render as plain text
        plain_text = _strip_html_tags(html_content)
        if not plain_text:
            plain_text = "(Could not render HTML content)"
        
        fallback_html = f"""
        <html>
        <head><style>{DEFAULT_CSS}</style></head>
        <body>
        <pre style="white-space: pre-wrap; word-wrap: break-word;">{plain_text}</pre>
        </body>
        </html>
        """
        output = io.BytesIO()
        HTML(string=fallback_html).write_pdf(output)

    pdf_bytes = output.getvalue()
    logger.info(f"Converted HTML to PDF: {len(html_bytes)} -> {len(pdf_bytes)} bytes")
    return pdf_bytes
