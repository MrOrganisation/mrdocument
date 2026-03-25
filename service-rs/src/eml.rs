use mailparse::{parse_mail, MailHeaderMap};
use tracing::{debug, info};

use crate::pdf::create_pdf_from_text;

/// Convert an EML file to PDF bytes.
pub fn eml_to_pdf(eml_bytes: &[u8]) -> Result<Vec<u8>, String> {
    let parsed = parse_mail(eml_bytes).map_err(|e| format!("Failed to parse EML: {}", e))?;

    let subject = parsed.headers.get_first_value("Subject").unwrap_or_default();
    let from = parsed.headers.get_first_value("From").unwrap_or_default();
    let to = parsed.headers.get_first_value("To").unwrap_or_default();
    let cc = parsed.headers.get_first_value("Cc").unwrap_or_default();
    let date = parsed.headers.get_first_value("Date").unwrap_or_default();

    debug!(
        "Parsed EML: subject={}, from={}, to={}",
        if subject.len() > 50 {
            crate::truncate::truncate_str(&subject, 50)
        } else {
            &subject
        },
        from,
        to
    );

    // Extract body text
    let body = extract_body_text(&parsed);

    // Build text representation
    let mut text_parts = Vec::new();
    if !from.is_empty() {
        text_parts.push(format!("From: {}", from));
    }
    if !to.is_empty() {
        text_parts.push(format!("To: {}", to));
    }
    if !cc.is_empty() {
        text_parts.push(format!("Cc: {}", cc));
    }
    if !date.is_empty() {
        text_parts.push(format!("Date: {}", date));
    }
    if !subject.is_empty() {
        text_parts.push(format!("Subject: {}", subject));
    }
    text_parts.push(String::new());
    text_parts.push(body);

    let full_text = text_parts.join("\n");
    let pdf_bytes = create_pdf_from_text(&full_text);

    info!(
        "Converted EML to PDF: {} bytes -> {} bytes",
        eml_bytes.len(),
        pdf_bytes.len()
    );

    Ok(pdf_bytes)
}

fn extract_body_text(mail: &mailparse::ParsedMail) -> String {
    // Try to get text/plain first, then fall back to text/html
    if mail.subparts.is_empty() {
        // Single part
        return mail.get_body().unwrap_or_default();
    }

    // Multipart - look for text/plain first
    for part in &mail.subparts {
        if part.ctype.mimetype == "text/plain" {
            if let Ok(body) = part.get_body() {
                if !body.trim().is_empty() {
                    return body;
                }
            }
        }
    }

    // Fall back to text/html (strip tags)
    for part in &mail.subparts {
        if part.ctype.mimetype == "text/html" {
            if let Ok(body) = part.get_body() {
                return strip_html_tags(&body);
            }
        }
    }

    // Recurse into subparts
    for part in &mail.subparts {
        let text = extract_body_text(part);
        if !text.trim().is_empty() {
            return text;
        }
    }

    String::new()
}

fn strip_html_tags(html: &str) -> String {
    let re_script =
        regex::Regex::new(r"(?is)<(script|style)[^>]*>.*?</\1>").unwrap();
    let cleaned = re_script.replace_all(html, "");
    let re_tags = regex::Regex::new(r"<[^>]+>").unwrap();
    let text = re_tags.replace_all(&cleaned, " ");
    let re_ws = regex::Regex::new(r"\s+").unwrap();
    re_ws.replace_all(&text, " ").trim().to_string()
}
