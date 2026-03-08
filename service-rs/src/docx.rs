use quick_xml::events::Event;
use quick_xml::reader::Reader;
use std::io::{Cursor, Read, Write};
use tracing::debug;
use zip::read::ZipArchive;

/// Extract plain text from a DOCX file.
pub fn extract_text_from_docx(docx_bytes: &[u8]) -> Result<String, String> {
    let cursor = Cursor::new(docx_bytes);
    let mut archive =
        ZipArchive::new(cursor).map_err(|e| format!("Invalid DOCX file: {}", e))?;

    let mut text = String::new();

    // Extract from word/document.xml
    if let Ok(mut file) = archive.by_name("word/document.xml") {
        let mut xml_content = String::new();
        file.read_to_string(&mut xml_content)
            .map_err(|e| format!("Failed to read document.xml: {}", e))?;
        text = extract_text_from_xml(&xml_content);
    }

    debug!(
        "Extracted {} chars from DOCX",
        text.len()
    );

    Ok(text)
}

fn extract_text_from_xml(xml: &str) -> String {
    let mut reader = Reader::from_str(xml);
    let mut in_text = false;
    let mut paragraphs: Vec<String> = Vec::new();
    let mut current_para = String::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(ref e)) | Ok(Event::Empty(ref e)) => {
                let local = e.local_name();
                let name = std::str::from_utf8(local.as_ref()).unwrap_or("");
                match name {
                    "t" => in_text = true,
                    "p" => {
                        if !current_para.is_empty() {
                            paragraphs.push(current_para.clone());
                            current_para.clear();
                        }
                    }
                    _ => {}
                }
            }
            Ok(Event::End(ref e)) => {
                let local = e.local_name();
                let name = std::str::from_utf8(local.as_ref()).unwrap_or("");
                if name == "t" {
                    in_text = false;
                }
            }
            Ok(Event::Text(ref e)) => {
                if in_text {
                    if let Ok(t) = e.unescape() {
                        current_para.push_str(&t);
                    }
                }
            }
            Ok(Event::Eof) => break,
            Err(_) => break,
            _ => {}
        }
    }

    if !current_para.is_empty() {
        paragraphs.push(current_para);
    }

    paragraphs
        .iter()
        .filter(|p| !p.trim().is_empty())
        .cloned()
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// Embed metadata into a DOCX file's core properties.
pub fn embed_metadata_in_docx(
    docx_bytes: &[u8],
    title: Option<&str>,
    author: Option<&str>,
    subject: Option<&str>,
    keywords: Option<&[String]>,
) -> Result<Vec<u8>, String> {
    let cursor = Cursor::new(docx_bytes);
    let mut archive =
        ZipArchive::new(cursor).map_err(|e| format!("Failed to read DOCX: {}", e))?;

    let mut output = Vec::new();
    {
        let out_cursor = Cursor::new(&mut output);
        let mut writer = zip::ZipWriter::new(out_cursor);

        // Check if core.xml exists
        let has_core = archive.by_name("docProps/core.xml").is_ok();

        for i in 0..archive.len() {
            let mut file = archive
                .by_index(i)
                .map_err(|e| format!("Failed to read entry: {}", e))?;
            let name = file.name().to_string();

            let options = zip::write::SimpleFileOptions::default()
                .compression_method(file.compression());

            let mut content = Vec::new();
            file.read_to_end(&mut content)
                .map_err(|e| format!("Failed to read entry {}: {}", name, e))?;

            if name == "docProps/core.xml" {
                // Modify core properties
                let modified = modify_core_properties(
                    &String::from_utf8_lossy(&content),
                    title,
                    author,
                    subject,
                    keywords,
                );
                writer
                    .start_file(&name, options)
                    .map_err(|e| format!("Write error: {}", e))?;
                writer
                    .write_all(modified.as_bytes())
                    .map_err(|e| format!("Write error: {}", e))?;
            } else {
                writer
                    .start_file(&name, options)
                    .map_err(|e| format!("Write error: {}", e))?;
                writer
                    .write_all(&content)
                    .map_err(|e| format!("Write error: {}", e))?;
            }
        }

        // If no core.xml existed, create one
        if !has_core {
            let core = create_core_properties(title, author, subject, keywords);
            let options = zip::write::SimpleFileOptions::default();
            writer
                .start_file("docProps/core.xml", options)
                .map_err(|e| format!("Write error: {}", e))?;
            writer
                .write_all(core.as_bytes())
                .map_err(|e| format!("Write error: {}", e))?;
        }

        writer
            .finish()
            .map_err(|e| format!("Failed to finish ZIP: {}", e))?;
    }

    debug!(
        "Embedded metadata in DOCX: title={:?}, author={:?}",
        title, author
    );

    Ok(output)
}

fn modify_core_properties(
    xml: &str,
    title: Option<&str>,
    author: Option<&str>,
    subject: Option<&str>,
    keywords: Option<&[String]>,
) -> String {
    // Simple approach: try to replace known elements, or append before closing tag
    let mut result = xml.to_string();

    if let Some(t) = title {
        if result.contains("<dc:title>") {
            let re = regex::Regex::new(r"<dc:title>.*?</dc:title>").unwrap();
            result = re.replace(&result, &format!("<dc:title>{}</dc:title>", xml_escape(t))).to_string();
        } else if let Some(pos) = result.rfind("</cp:coreProperties>") {
            result.insert_str(pos, &format!("<dc:title>{}</dc:title>", xml_escape(t)));
        }
    }

    if let Some(a) = author {
        if result.contains("<dc:creator>") {
            let re = regex::Regex::new(r"<dc:creator>.*?</dc:creator>").unwrap();
            result = re.replace(&result, &format!("<dc:creator>{}</dc:creator>", xml_escape(a))).to_string();
        } else if let Some(pos) = result.rfind("</cp:coreProperties>") {
            result.insert_str(pos, &format!("<dc:creator>{}</dc:creator>", xml_escape(a)));
        }
    }

    if let Some(s) = subject {
        if result.contains("<dc:subject>") {
            let re = regex::Regex::new(r"<dc:subject>.*?</dc:subject>").unwrap();
            result = re.replace(&result, &format!("<dc:subject>{}</dc:subject>", xml_escape(s))).to_string();
        } else if let Some(pos) = result.rfind("</cp:coreProperties>") {
            result.insert_str(pos, &format!("<dc:subject>{}</dc:subject>", xml_escape(s)));
        }
    }

    if let Some(kw) = keywords {
        let kw_str = kw.join(", ");
        if result.contains("<cp:keywords>") {
            let re = regex::Regex::new(r"<cp:keywords>.*?</cp:keywords>").unwrap();
            result = re.replace(&result, &format!("<cp:keywords>{}</cp:keywords>", xml_escape(&kw_str))).to_string();
        } else if let Some(pos) = result.rfind("</cp:coreProperties>") {
            result.insert_str(pos, &format!("<cp:keywords>{}</cp:keywords>", xml_escape(&kw_str)));
        }
    }

    result
}

fn create_core_properties(
    title: Option<&str>,
    author: Option<&str>,
    subject: Option<&str>,
    keywords: Option<&[String]>,
) -> String {
    let mut xml = String::from(
        r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/">"#,
    );

    if let Some(t) = title {
        xml.push_str(&format!("<dc:title>{}</dc:title>", xml_escape(t)));
    }
    if let Some(a) = author {
        xml.push_str(&format!("<dc:creator>{}</dc:creator>", xml_escape(a)));
    }
    if let Some(s) = subject {
        xml.push_str(&format!("<dc:subject>{}</dc:subject>", xml_escape(s)));
    }
    if let Some(kw) = keywords {
        xml.push_str(&format!(
            "<cp:keywords>{}</cp:keywords>",
            xml_escape(&kw.join(", "))
        ));
    }

    xml.push_str("</cp:coreProperties>");
    xml
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
