use tracing::{debug, info};

use crate::pdf::create_pdf_from_text;

/// Convert HTML bytes to PDF.
pub fn html_to_pdf(html_bytes: &[u8], filename: &str) -> Result<Vec<u8>, String> {
    debug!(
        "Converting HTML to PDF: {} ({} bytes)",
        filename,
        html_bytes.len()
    );

    // Try to decode HTML
    let html_content = decode_html(html_bytes)?;

    // Strip HTML tags to get text
    let text = strip_html_tags(&html_content);
    if text.trim().is_empty() {
        return Err("HTML file contains no extractable text".to_string());
    }

    let pdf_bytes = create_pdf_from_text(&text);

    info!(
        "Converted HTML to PDF: {} -> {} bytes",
        html_bytes.len(),
        pdf_bytes.len()
    );

    Ok(pdf_bytes)
}

fn decode_html(bytes: &[u8]) -> Result<String, String> {
    // Try UTF-8 first
    if let Ok(s) = std::str::from_utf8(bytes) {
        return Ok(s.to_string());
    }

    // Try Latin-1
    let (cow, _, had_errors) = encoding_rs::WINDOWS_1252.decode(bytes);
    if !had_errors {
        return Ok(cow.to_string());
    }

    // Force decode
    Ok(String::from_utf8_lossy(bytes).to_string())
}

fn strip_html_tags(html: &str) -> String {
    // Remove <script>...</script> and <style>...</style> blocks
    let re_script =
        regex::Regex::new(r"(?is)<script[^>]*>.*?</script>").unwrap();
    let cleaned = re_script.replace_all(html, "");
    let re_style =
        regex::Regex::new(r"(?is)<style[^>]*>.*?</style>").unwrap();
    let cleaned = re_style.replace_all(&cleaned, "");
    let re_tags = regex::Regex::new(r"<[^>]+>").unwrap();
    let text = re_tags.replace_all(&cleaned, " ");

    // Decode HTML entities
    let text = text
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&#39;", "'")
        .replace("&nbsp;", " ");

    let re_ws = regex::Regex::new(r"\s+").unwrap();
    re_ws.replace_all(&text, " ").trim().to_string()
}
