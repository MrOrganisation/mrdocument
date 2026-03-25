use chrono::{NaiveDate, Utc};
use lopdf::{dictionary, Document, Object, StringFormat};
use tracing::{debug, warn};

/// Embed metadata into a PDF document using the Info dictionary.
pub fn embed_metadata(
    pdf_bytes: &[u8],
    doc_type: Option<&str>,
    doc_date: Option<NaiveDate>,
    sender: Option<&str>,
    topic: Option<&str>,
    subject: Option<&str>,
    keywords: Option<&[String]>,
) -> Result<Vec<u8>, String> {
    let mut doc = Document::load_mem(pdf_bytes)
        .map_err(|e| format!("Failed to load PDF: {}", e))?;

    // Build keywords list
    let mut all_keywords: Vec<String> = Vec::new();
    if let Some(t) = doc_type {
        all_keywords.push(t.to_string());
    }
    if let Some(t) = topic {
        all_keywords.push(t.to_string());
    }
    if let Some(s) = sender {
        all_keywords.push(s.to_string());
    }
    if let Some(kw) = keywords {
        all_keywords.extend(kw.iter().cloned());
    }

    // Set Info dictionary entries
    let info_id = if let Some(info_ref) = doc.trailer.get(b"Info").ok().and_then(|v| v.as_reference().ok()) {
        info_ref
    } else {
        // Create new Info dictionary
        let dict = lopdf::Dictionary::new();
        let id = doc.add_object(Object::Dictionary(dict));
        doc.trailer.set("Info", Object::Reference(id));
        id
    };

    if let Ok(Object::Dictionary(ref mut info)) = doc.get_object_mut(info_id) {
        if let Some(s) = subject {
            info.set("Title", Object::String(s.as_bytes().to_vec(), StringFormat::Literal));
        }
        if let Some(s) = sender {
            info.set("Author", Object::String(s.as_bytes().to_vec(), StringFormat::Literal));
        }
        if let Some(t) = doc_type {
            info.set("Subject", Object::String(t.as_bytes().to_vec(), StringFormat::Literal));
        }
        if !all_keywords.is_empty() {
            let kw_str = all_keywords.join(", ");
            info.set("Keywords", Object::String(kw_str.as_bytes().to_vec(), StringFormat::Literal));
        }
        if let Some(t) = topic {
            info.set("Topic", Object::String(t.as_bytes().to_vec(), StringFormat::Literal));
        }
        if let Some(d) = doc_date {
            let date_str = format!("D:{}000000Z", d.format("%Y%m%d"));
            info.set("CreationDate", Object::String(date_str.as_bytes().to_vec(), StringFormat::Literal));
        }
        info.set("Creator", Object::String(b"MrDocument".to_vec(), StringFormat::Literal));
        let mod_date = format!("D:{}Z", Utc::now().format("%Y%m%d%H%M%S"));
        info.set("ModDate", Object::String(mod_date.as_bytes().to_vec(), StringFormat::Literal));

        if let Some(s) = sender {
            let producer = format!("MrDocument (source: {})", s);
            info.set("Producer", Object::String(producer.as_bytes().to_vec(), StringFormat::Literal));
        }
    }

    let mut output = Vec::new();
    doc.save_to(&mut output)
        .map_err(|e| format!("Failed to save PDF: {}", e))?;

    debug!(
        "Embedded metadata into PDF: type={:?}, date={:?}, sender={:?}, topic={:?}, subject={:?} ({} -> {} bytes)",
        doc_type, doc_date, sender, topic.map(|s| crate::truncate::truncate_str(s, 30)),
        subject.map(|s| crate::truncate::truncate_str(s, 30)),
        pdf_bytes.len(), output.len()
    );

    Ok(output)
}

/// Create a minimal valid PDF from text content.
pub fn create_pdf_from_text(text: &str) -> Vec<u8> {
    // Create a minimal but valid PDF
    let mut doc = Document::with_version("1.4");

    let pages_id = doc.new_object_id();
    let page_id = doc.new_object_id();
    let content_id = doc.new_object_id();
    let font_id = doc.new_object_id();

    // Font
    doc.objects.insert(font_id, Object::Dictionary(lopdf::dictionary! {
        "Type" => "Font",
        "Subtype" => "Type1",
        "BaseFont" => "Helvetica",
    }));

    // Escape text for PDF content stream
    let escaped_text = text
        .replace('\\', "\\\\")
        .replace('(', "\\(")
        .replace(')', "\\)")
        .chars()
        .filter(|c| c.is_ascii())
        .collect::<String>();

    // Build content stream with line wrapping
    let mut content = String::from("BT\n/F1 10 Tf\n72 750 Td\n12 TL\n");
    for line in escaped_text.lines().take(60) {
        let line = crate::truncate::truncate_str(line, 80);
        content.push_str(&format!("({}) '\n", line));
    }
    content.push_str("ET");

    doc.objects.insert(
        content_id,
        Object::Stream(lopdf::Stream::new(
            lopdf::dictionary! {},
            content.into_bytes(),
        )),
    );

    // Page
    doc.objects.insert(page_id, Object::Dictionary(lopdf::dictionary! {
        "Type" => "Page",
        "Parent" => Object::Reference(pages_id),
        "MediaBox" => vec![0.into(), 0.into(), 595.into(), 842.into()],
        "Contents" => Object::Reference(content_id),
        "Resources" => lopdf::dictionary! {
            "Font" => lopdf::dictionary! {
                "F1" => Object::Reference(font_id),
            },
        },
    }));

    // Pages
    doc.objects.insert(pages_id, Object::Dictionary(lopdf::dictionary! {
        "Type" => "Pages",
        "Kids" => vec![Object::Reference(page_id)],
        "Count" => 1,
    }));

    // Catalog
    let catalog_id = doc.add_object(lopdf::dictionary! {
        "Type" => "Catalog",
        "Pages" => Object::Reference(pages_id),
    });

    doc.trailer.set("Root", Object::Reference(catalog_id));

    let mut output = Vec::new();
    doc.save_to(&mut output).unwrap_or_else(|e| {
        warn!("Failed to create PDF from text: {}", e);
    });

    if output.is_empty() {
        // Fallback: return a hardcoded minimal PDF
        return MINIMAL_PDF.to_vec();
    }

    output
}

// Minimal valid PDF as fallback
const MINIMAL_PDF: &[u8] = b"%PDF-1.4
1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj
2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj
3 0 obj<</Type/Page/MediaBox[0 0 595 842]/Parent 2 0 R>>endobj
xref
0 4
0000000000 65535 f
0000000009 00000 n
0000000058 00000 n
0000000115 00000 n
trailer<</Size 4/Root 1 0 R>>
startxref
190
%%EOF";
