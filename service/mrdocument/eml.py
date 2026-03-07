"""EML to PDF conversion utilities."""

import email
import html
import io
import logging
from dataclasses import dataclass
from email import policy
from email.message import EmailMessage
from email.utils import parsedate_to_datetime
from typing import Optional

from weasyprint import HTML

logger = logging.getLogger(__name__)

# CSS for rendering email as PDF
EMAIL_CSS = """
body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
    font-size: 12pt;
    line-height: 1.5;
    color: #333;
    max-width: 800px;
    margin: 0 auto;
    padding: 20px;
}
.email-header {
    background: #f5f5f5;
    border: 1px solid #ddd;
    border-radius: 4px;
    padding: 15px;
    margin-bottom: 20px;
}
.email-header table {
    width: 100%;
    border-collapse: collapse;
}
.email-header td {
    padding: 4px 8px;
    vertical-align: top;
}
.email-header td:first-child {
    font-weight: bold;
    width: 80px;
    color: #666;
}
.email-subject {
    font-size: 14pt;
    font-weight: bold;
    margin-top: 10px;
    padding-top: 10px;
    border-top: 1px solid #ddd;
}
.email-body {
    padding: 10px 0;
}
.email-body pre {
    white-space: pre-wrap;
    word-wrap: break-word;
    font-family: inherit;
    margin: 0;
}
.attachments {
    background: #fff8e1;
    border: 1px solid #ffcc02;
    border-radius: 4px;
    padding: 10px 15px;
    margin-top: 20px;
}
.attachments h4 {
    margin: 0 0 10px 0;
    color: #666;
}
.attachments ul {
    margin: 0;
    padding-left: 20px;
}
.attachments li {
    margin: 4px 0;
}
.email-html-content {
    overflow: hidden;
    max-width: 100%;
}
.email-html-content img {
    max-width: 100%;
    height: auto;
}
.email-html-content table {
    max-width: 100%;
}
"""


@dataclass
class EmailAttachment:
    """Represents an email attachment."""

    filename: str
    content_type: str
    size: int


@dataclass
class ParsedEmail:
    """Parsed email data."""

    subject: Optional[str]
    sender: Optional[str]
    to: Optional[str]
    cc: Optional[str]
    date: Optional[str]
    body_html: Optional[str]
    body_text: Optional[str]
    attachments: list[EmailAttachment]


def parse_eml(eml_bytes: bytes) -> ParsedEmail:
    """
    Parse an EML file into structured data.

    Args:
        eml_bytes: Raw bytes of the .eml file

    Returns:
        ParsedEmail with extracted data
    """
    msg = email.message_from_bytes(eml_bytes, policy=policy.default)

    # Extract headers
    subject = msg.get("Subject")
    sender = msg.get("From")
    to = msg.get("To")
    cc = msg.get("Cc")
    date_header = msg.get("Date")

    # Parse date
    date_str = None
    if date_header:
        try:
            dt = parsedate_to_datetime(date_header)
            date_str = dt.strftime("%Y-%m-%d %H:%M:%S %Z")
        except (ValueError, TypeError):
            date_str = date_header

    # Extract body and attachments
    body_html = None
    body_text = None
    attachments = []

    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            content_disposition = str(part.get("Content-Disposition", ""))

            # Check if it's an attachment
            if "attachment" in content_disposition or part.get_filename():
                filename = part.get_filename() or "unnamed"
                try:
                    payload = part.get_payload(decode=True)
                    size = len(payload) if payload else 0
                except Exception:
                    size = 0
                attachments.append(
                    EmailAttachment(
                        filename=filename,
                        content_type=content_type,
                        size=size,
                    )
                )
            elif content_type == "text/html" and body_html is None:
                try:
                    body_html = part.get_content()
                except Exception:
                    pass
            elif content_type == "text/plain" and body_text is None:
                try:
                    body_text = part.get_content()
                except Exception:
                    pass
    else:
        content_type = msg.get_content_type()
        try:
            content = msg.get_content()
            if content_type == "text/html":
                body_html = content
            else:
                body_text = content
        except Exception:
            pass

    return ParsedEmail(
        subject=subject,
        sender=sender,
        to=to,
        cc=cc,
        date=date_str,
        body_html=body_html,
        body_text=body_text,
        attachments=attachments,
    )


def _escape(text: Optional[str]) -> str:
    """Escape text for HTML."""
    return html.escape(text) if text else ""


def _render_email_html(parsed: ParsedEmail) -> str:
    """
    Render parsed email as HTML document.

    Args:
        parsed: ParsedEmail data

    Returns:
        HTML string
    """
    # Build header rows
    header_rows = []
    if parsed.sender:
        header_rows.append(f"<tr><td>From:</td><td>{_escape(parsed.sender)}</td></tr>")
    if parsed.to:
        header_rows.append(f"<tr><td>To:</td><td>{_escape(parsed.to)}</td></tr>")
    if parsed.cc:
        header_rows.append(f"<tr><td>Cc:</td><td>{_escape(parsed.cc)}</td></tr>")
    if parsed.date:
        header_rows.append(f"<tr><td>Date:</td><td>{_escape(parsed.date)}</td></tr>")

    # Subject line
    subject_html = ""
    if parsed.subject:
        subject_html = f'<div class="email-subject">{_escape(parsed.subject)}</div>'

    # Body content
    if parsed.body_html:
        # Sanitize HTML body - strip potentially problematic elements
        # Wrap in a div to contain any styling issues
        body_content = f'<div class="email-html-content">{parsed.body_html}</div>'
    elif parsed.body_text:
        # Wrap plain text in pre tag
        body_content = f"<pre>{_escape(parsed.body_text)}</pre>"
    else:
        body_content = "<p><em>(No message body)</em></p>"

    # Attachments section
    attachments_html = ""
    if parsed.attachments:
        attachment_items = []
        for att in parsed.attachments:
            size_str = _format_size(att.size)
            attachment_items.append(
                f"<li>{_escape(att.filename)} ({_escape(att.content_type)}, {size_str})</li>"
            )
        attachments_html = f"""
        <div class="attachments">
            <h4>Attachments ({len(parsed.attachments)})</h4>
            <ul>
                {''.join(attachment_items)}
            </ul>
        </div>
        """

    return f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>{EMAIL_CSS}</style>
</head>
<body>
    <div class="email-header">
        <table>
            {''.join(header_rows)}
        </table>
        {subject_html}
    </div>
    <div class="email-body">
        {body_content}
    </div>
    {attachments_html}
</body>
</html>"""


def _format_size(size_bytes: int) -> str:
    """Format byte size as human-readable string."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    else:
        return f"{size_bytes / (1024 * 1024):.1f} MB"


def _strip_html_tags(html_text: str) -> str:
    """Strip HTML tags and return plain text."""
    import re
    # Remove script and style elements
    text = re.sub(r'<(script|style)[^>]*>.*?</\1>', '', html_text, flags=re.DOTALL | re.IGNORECASE)
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', ' ', text)
    # Decode HTML entities
    text = html.unescape(text)
    # Collapse whitespace
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def eml_to_pdf(eml_bytes: bytes) -> bytes:
    """
    Convert an EML file to PDF.

    Args:
        eml_bytes: Raw bytes of the .eml file

    Returns:
        PDF file as bytes
    """
    parsed = parse_eml(eml_bytes)

    logger.debug(
        "Parsed EML: subject=%s, from=%s, to=%s, attachments=%d",
        parsed.subject[:50] if parsed.subject else None,
        parsed.sender,
        parsed.to,
        len(parsed.attachments),
    )

    html_content = _render_email_html(parsed)

    # Convert to PDF with fallback for problematic HTML
    output = io.BytesIO()
    try:
        HTML(string=html_content).write_pdf(output)
    except Exception as e:
        logger.warning(
            "WeasyPrint failed on HTML content, trying fallback: %s", e
        )
        # Fallback: convert HTML body to plain text and try again
        if parsed.body_html:
            fallback_text = _strip_html_tags(parsed.body_html)
            parsed.body_html = None
            parsed.body_text = fallback_text if fallback_text else "(Could not render email body)"
        elif not parsed.body_text:
            parsed.body_text = "(Could not render email body)"
        html_content = _render_email_html(parsed)
        output = io.BytesIO()
        HTML(string=html_content).write_pdf(output)

    result = output.getvalue()
    logger.info(
        "Converted EML to PDF: %d bytes -> %d bytes",
        len(eml_bytes),
        len(result),
    )

    return result
