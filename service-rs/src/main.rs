mod ai;
mod config;
mod costs;
mod docx;
mod eml;
mod html_convert;
mod image_convert;
mod ocr;
mod pdf;
mod rtf_convert;
mod transcript;

use axum::{
    extract::{DefaultBodyLimit, Multipart, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use base64::Engine;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{error, info, warn};

use ai::{AiClient, AiError, DocumentMetadata};
use ocr::{OcrClient, OcrError};

// =============================================================================
// Application State
// =============================================================================

struct AppState {
    ocr_client: OcrClient,
    ai_client: AiClient,
    correction_model: String,
    correction_extended_thinking: bool,
    correction_thinking_budget: u32,
    correction_use_batch: bool,
}

// =============================================================================
// Field/Context parsing
// =============================================================================

fn parse_field_config(data: &Value) -> Option<Value> {
    let obj = data.as_object()?;
    if !obj.contains_key("instructions") {
        return None;
    }
    if !obj.get("instructions").map(|v| v.is_string()).unwrap_or(false) {
        return None;
    }
    Some(data.clone())
}

fn parse_contexts(data: &Value) -> Option<Vec<Value>> {
    let arr = data.as_array()?;
    if arr.is_empty() {
        return None;
    }

    let mut contexts = Vec::new();
    for item in arr {
        let obj = match item.as_object() {
            Some(o) => o,
            None => continue,
        };

        // Required: name and description
        if !obj.contains_key("name") || !obj.contains_key("description") {
            continue;
        }

        // Handle filename
        let mut ctx = item.clone();
        let filename_val = obj.get("filename");
        if let Some(Value::Array(arr)) = filename_val {
            // Conditional list - find default
            let mut default_pattern = None;
            for entry in arr {
                if let Some(eobj) = entry.as_object() {
                    if eobj.contains_key("pattern") && !eobj.contains_key("match") {
                        default_pattern = eobj.get("pattern").cloned();
                        break;
                    }
                }
            }
            if let Some(pattern) = default_pattern {
                ctx["filename"] = pattern;
            }
        }
        if ctx.get("filename").and_then(|v| v.as_str()).is_none()
            && !ctx.get("filename").map(|v| v.is_array()).unwrap_or(false)
        {
            ctx["filename"] = json!("{context}-{date}");
        }

        // Validate fields
        if !obj.contains_key("fields") || !obj.get("fields").map(|v| v.is_object()).unwrap_or(false) {
            continue;
        }

        // Validate field configs
        let fields = obj.get("fields").and_then(|v| v.as_object());
        if let Some(fields) = fields {
            let mut valid_fields = serde_json::Map::new();
            for (fname, fdata) in fields {
                if let Some(fc) = parse_field_config(fdata) {
                    valid_fields.insert(fname.clone(), fc);
                }
            }
            let mut ctx_obj = ctx.as_object().unwrap().clone();
            ctx_obj.insert("fields".to_string(), Value::Object(valid_fields));
            ctx = Value::Object(ctx_obj);
        }

        contexts.push(ctx);
    }

    if contexts.is_empty() {
        None
    } else {
        Some(contexts)
    }
}

// =============================================================================
// Handlers
// =============================================================================

async fn health(State(state): State<Arc<AppState>>) -> Json<Value> {
    let ocr_healthy = state.ocr_client.health_check().await;
    Json(json!({
        "status": if ocr_healthy { "healthy" } else { "degraded" },
        "service": "mrdocument",
        "ocr_service": if ocr_healthy { "healthy" } else { "unhealthy" },
    }))
}

async fn process_document(
    State(state): State<Arc<AppState>>,
    mut multipart: Multipart,
) -> (StatusCode, Json<Value>) {
    let mut file_bytes: Option<Vec<u8>> = None;
    let mut filename = "document.pdf".to_string();
    let mut language = "eng".to_string();
    let mut primary_language: Option<String> = None;
    let mut contexts: Option<Vec<Value>> = None;
    let mut user_dir: Option<PathBuf> = None;
    let mut locked_fields: Option<HashMap<String, Value>> = None;

    // Read multipart fields
    while let Ok(Some(field)) = multipart.next_field().await {
        let name = field.name().unwrap_or("").to_string();
        match name.as_str() {
            "file" => {
                filename = field
                    .file_name()
                    .unwrap_or("document.pdf")
                    .to_string();
                match field.bytes().await {
                    Ok(b) => file_bytes = Some(b.to_vec()),
                    Err(e) => {
                        return (
                            StatusCode::BAD_REQUEST,
                            Json(json!({"error": format!("Failed to read file: {}", e)})),
                        );
                    }
                }
            }
            "language" => {
                if let Ok(b) = field.bytes().await {
                    language = String::from_utf8_lossy(&b).to_string();
                }
            }
            "primary_language" => {
                if let Ok(b) = field.bytes().await {
                    primary_language = Some(String::from_utf8_lossy(&b).to_string());
                }
            }
            "contexts" => {
                if let Ok(b) = field.bytes().await {
                    match serde_json::from_slice::<Value>(&b) {
                        Ok(v) => contexts = parse_contexts(&v),
                        Err(e) => {
                            return (
                                StatusCode::BAD_REQUEST,
                                Json(json!({"error": format!("Invalid JSON in 'contexts' field: {}", e)})),
                            );
                        }
                    }
                }
            }
            "locked_fields" => {
                if let Ok(b) = field.bytes().await {
                    match serde_json::from_slice::<HashMap<String, Value>>(&b) {
                        Ok(v) => locked_fields = Some(v),
                        Err(e) => {
                            return (
                                StatusCode::BAD_REQUEST,
                                Json(json!({"error": format!("Invalid JSON in 'locked_fields' field: {}", e)})),
                            );
                        }
                    }
                }
            }
            "user_dir" => {
                if let Ok(b) = field.bytes().await {
                    let s = String::from_utf8_lossy(&b).to_string();
                    if !s.is_empty() {
                        user_dir = Some(PathBuf::from(s));
                    }
                }
            }
            _ => {}
        }
    }

    let file_bytes = match file_bytes {
        Some(b) => b,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "No file provided"})),
            );
        }
    };

    let contexts = match contexts {
        Some(c) => c,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "contexts field is required with at least one context"})),
            );
        }
    };

    let fl = filename.to_lowercase();
    let is_pdf = fl.ends_with(".pdf");
    let is_eml = fl.ends_with(".eml");
    let is_html = fl.ends_with(".html") || fl.ends_with(".htm");
    let is_docx = fl.ends_with(".docx");
    let is_text = fl.ends_with(".txt") || fl.ends_with(".md");
    let is_rtf = fl.ends_with(".rtf");
    let is_image = image_convert::is_supported_image(&filename);

    if !is_pdf && !is_eml && !is_html && !is_docx && !is_text && !is_rtf && !is_image {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "Only PDF, EML, HTML, DOCX, RTF, TXT, MD, and image files are supported"})),
        );
    }

    // Text files
    if is_text {
        return process_text_file(
            &state,
            &file_bytes,
            &filename,
            primary_language.as_deref(),
            &contexts,
            user_dir.as_deref(),
            locked_fields.as_ref(),
        )
        .await;
    }

    // DOCX files
    if is_docx {
        return process_docx_file(
            &state,
            &file_bytes,
            &filename,
            primary_language.as_deref(),
            &contexts,
            user_dir.as_deref(),
            locked_fields.as_ref(),
        )
        .await;
    }

    // Convert to PDF if needed
    let (pdf_bytes, fname) = if is_eml {
        info!("Converting EML to PDF: {} ({} bytes)", filename, file_bytes.len());
        match eml::eml_to_pdf(&file_bytes) {
            Ok(pdf) => {
                let new_name = filename[..filename.len() - 4].to_string() + ".pdf";
                (pdf, new_name)
            }
            Err(e) => {
                error!("EML conversion failed for {}: {}", filename, e);
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": format!("EML conversion failed: {}", e)})),
                );
            }
        }
    } else if is_html {
        info!("Converting HTML to PDF: {} ({} bytes)", filename, file_bytes.len());
        match html_convert::html_to_pdf(&file_bytes, &filename) {
            Ok(pdf) => {
                let stem = Path::new(&filename)
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("document");
                (pdf, format!("{}.pdf", stem))
            }
            Err(e) => {
                error!("HTML conversion failed for {}: {}", filename, e);
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": format!("HTML conversion failed: {}", e)})),
                );
            }
        }
    } else if is_rtf {
        info!("Converting RTF to PDF: {} ({} bytes)", filename, file_bytes.len());
        match rtf_convert::rtf_to_pdf(&file_bytes, &filename) {
            Ok(pdf) => {
                let stem = Path::new(&filename)
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("document");
                (pdf, format!("{}.pdf", stem))
            }
            Err(e) => {
                error!("RTF conversion failed for {}: {}", filename, e);
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": format!("RTF conversion failed: {}", e)})),
                );
            }
        }
    } else if is_image {
        info!("Converting image to PDF: {} ({} bytes)", filename, file_bytes.len());
        match image_convert::image_to_pdf(&file_bytes, &filename) {
            Ok(pdf) => {
                let stem = Path::new(&filename)
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("image");
                (pdf, format!("{}.pdf", stem))
            }
            Err(e) => {
                error!("Image conversion failed for {}: {}", filename, e);
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": format!("Image conversion failed: {}", e)})),
                );
            }
        }
    } else {
        (file_bytes.clone(), filename.clone())
    };

    info!(
        "Processing document: {} ({} bytes), language={}, contexts={}",
        fname,
        pdf_bytes.len(),
        language,
        contexts.len()
    );

    // Step 1: OCR
    let ocr_result = match state.ocr_client.process_pdf(&pdf_bytes, &fname, &language).await {
        Ok(r) => r,
        Err(OcrError::Failed(msg)) => {
            error!("OCR failed for {}: {}", fname, msg);
            return (StatusCode::BAD_GATEWAY, Json(json!({"error": format!("OCR failed: {}", msg)})));
        }
        Err(OcrError::Request(e)) => {
            error!("OCR request failed for {}: {}", fname, e);
            return (StatusCode::BAD_GATEWAY, Json(json!({"error": format!("OCR failed: {}", e)})));
        }
    };

    // Step 2: AI metadata extraction
    let (metadata, filename_pattern) = match state
        .ai_client
        .extract_metadata(
            &ocr_result.text,
            &contexts,
            primary_language.as_deref(),
            Some(&filename),
            user_dir.as_deref(),
            locked_fields.as_ref(),
            false,
        )
        .await
    {
        Ok(r) => r,
        Err(AiError::Configuration(msg)) => {
            return (StatusCode::BAD_REQUEST, Json(json!({"error": format!("Configuration error: {}", msg)})));
        }
        Err(e) => {
            error!("AI processing failed for {}: {}", fname, e);
            return (StatusCode::BAD_GATEWAY, Json(json!({"error": format!("AI processing failed: {}", e)})));
        }
    };

    // Step 3: Embed metadata into PDF
    let pdf_with_metadata = match pdf::embed_metadata(
        &ocr_result.pdf_bytes,
        metadata.doc_type(),
        metadata.date,
        metadata.sender(),
        metadata.topic(),
        metadata.subject(),
        Some(&metadata.keywords()),
    ) {
        Ok(pdf) => pdf,
        Err(e) => {
            warn!("Failed to embed metadata: {}, using original PDF", e);
            ocr_result.pdf_bytes
        }
    };

    // Step 4: Generate filename
    let suggested_filename = metadata.to_filename(&filename_pattern, Some(&filename));
    info!(
        "Document processed: {} -> {} (context: {:?})",
        filename, suggested_filename, metadata.context
    );

    // Build response
    let mut metadata_response = serde_json::Map::new();
    metadata_response.insert(
        "context".to_string(),
        metadata.context.clone().map(Value::String).unwrap_or(Value::Null),
    );
    metadata_response.insert(
        "date".to_string(),
        metadata.date.map(|d| Value::String(d.to_string())).unwrap_or(Value::Null),
    );
    for (k, v) in &metadata.fields {
        metadata_response.insert(k.clone(), v.clone());
    }

    let mut response = json!({
        "filename": suggested_filename,
        "pdf": base64::engine::general_purpose::STANDARD.encode(&pdf_with_metadata),
        "metadata": metadata_response,
    });

    if !metadata.new_clues.is_empty() {
        let mut clues_obj = serde_json::Map::new();
        for (field, (value, clue)) in &metadata.new_clues {
            clues_obj.insert(
                field.clone(),
                json!({"value": value, "clue": clue}),
            );
        }
        response["new_clues"] = Value::Object(clues_obj);
    }

    if ocr_result.signature_invalidated {
        response["signature_invalidated"] = json!(true);
    }

    (StatusCode::OK, Json(response))
}

async fn process_text_file(
    state: &AppState,
    file_bytes: &[u8],
    filename: &str,
    primary_language: Option<&str>,
    contexts: &[Value],
    user_dir: Option<&Path>,
    locked_fields: Option<&HashMap<String, Value>>,
) -> (StatusCode, Json<Value>) {
    // Decode text
    let text = match std::str::from_utf8(file_bytes) {
        Ok(s) => s.to_string(),
        Err(_) => {
            let (cow, _, _) = encoding_rs::WINDOWS_1252.decode(file_bytes);
            cow.to_string()
        }
    };

    if text.trim().is_empty() {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "Text file is empty"})));
    }

    info!(
        "Processing text file: {} ({} chars), contexts={}",
        filename,
        text.len(),
        contexts.len()
    );

    let (metadata, filename_pattern) = match state
        .ai_client
        .extract_metadata(&text, contexts, primary_language, Some(filename), user_dir, locked_fields, false)
        .await
    {
        Ok(r) => r,
        Err(AiError::Configuration(msg)) => {
            return (StatusCode::BAD_REQUEST, Json(json!({"error": format!("Configuration error: {}", msg)})));
        }
        Err(e) => {
            return (StatusCode::BAD_GATEWAY, Json(json!({"error": format!("AI processing failed: {}", e)})));
        }
    };

    let mut suggested_filename = metadata.to_filename(&filename_pattern, Some(filename));
    let original_ext = Path::new(filename)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("txt");
    if suggested_filename.ends_with(".pdf") {
        suggested_filename = format!("{}.{}", &suggested_filename[..suggested_filename.len() - 4], original_ext);
    }

    let mut metadata_response = serde_json::Map::new();
    metadata_response.insert("context".to_string(), metadata.context.clone().map(Value::String).unwrap_or(Value::Null));
    metadata_response.insert("date".to_string(), metadata.date.map(|d| Value::String(d.to_string())).unwrap_or(Value::Null));
    for (k, v) in &metadata.fields {
        metadata_response.insert(k.clone(), v.clone());
    }

    let mut response = json!({
        "filename": suggested_filename,
        "metadata": metadata_response,
    });

    if !metadata.new_clues.is_empty() {
        let mut clues_obj = serde_json::Map::new();
        for (field, (value, clue)) in &metadata.new_clues {
            clues_obj.insert(field.clone(), json!({"value": value, "clue": clue}));
        }
        response["new_clues"] = Value::Object(clues_obj);
    }

    (StatusCode::OK, Json(response))
}

async fn process_docx_file(
    state: &AppState,
    file_bytes: &[u8],
    filename: &str,
    primary_language: Option<&str>,
    contexts: &[Value],
    user_dir: Option<&Path>,
    locked_fields: Option<&HashMap<String, Value>>,
) -> (StatusCode, Json<Value>) {
    let text = match docx::extract_text_from_docx(file_bytes) {
        Ok(t) => t,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(json!({"error": format!("DOCX extraction failed: {}", e)})));
        }
    };

    if text.trim().is_empty() {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "DOCX file contains no extractable text"})));
    }

    let (metadata, filename_pattern) = match state
        .ai_client
        .extract_metadata(&text, contexts, primary_language, Some(filename), user_dir, locked_fields, false)
        .await
    {
        Ok(r) => r,
        Err(AiError::Configuration(msg)) => {
            return (StatusCode::BAD_REQUEST, Json(json!({"error": format!("Configuration error: {}", msg)})));
        }
        Err(e) => {
            return (StatusCode::BAD_GATEWAY, Json(json!({"error": format!("AI processing failed: {}", e)})));
        }
    };

    let docx_with_metadata = match docx::embed_metadata_in_docx(
        file_bytes,
        metadata.subject(),
        metadata.sender(),
        metadata.topic().map(|t| {
            if let Some(dt) = metadata.doc_type() {
                format!("{} - {}", dt, t)
            } else {
                t.to_string()
            }
        }).as_deref().or(metadata.doc_type()),
        Some(&metadata.keywords()),
    ) {
        Ok(d) => d,
        Err(e) => {
            warn!("Failed to embed DOCX metadata: {}", e);
            file_bytes.to_vec()
        }
    };

    let mut suggested_filename = metadata.to_filename(&filename_pattern, Some(filename));
    if suggested_filename.ends_with(".pdf") {
        suggested_filename = format!("{}.docx", &suggested_filename[..suggested_filename.len() - 4]);
    }

    let mut metadata_response = serde_json::Map::new();
    metadata_response.insert("context".to_string(), metadata.context.clone().map(Value::String).unwrap_or(Value::Null));
    metadata_response.insert("date".to_string(), metadata.date.map(|d| Value::String(d.to_string())).unwrap_or(Value::Null));
    for (k, v) in &metadata.fields {
        metadata_response.insert(k.clone(), v.clone());
    }

    let mut response = json!({
        "filename": suggested_filename,
        "docx": base64::engine::general_purpose::STANDARD.encode(&docx_with_metadata),
        "metadata": metadata_response,
    });

    if !metadata.new_clues.is_empty() {
        let mut clues_obj = serde_json::Map::new();
        for (field, (value, clue)) in &metadata.new_clues {
            clues_obj.insert(field.clone(), json!({"value": value, "clue": clue}));
        }
        response["new_clues"] = Value::Object(clues_obj);
    }

    (StatusCode::OK, Json(response))
}

async fn process_transcript_handler(
    State(state): State<Arc<AppState>>,
    Json(data): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let transcript_json = match data.get("transcript") {
        Some(t) => t.clone(),
        None => {
            return (StatusCode::BAD_REQUEST, Json(json!({"error": "transcript field is required"})));
        }
    };

    let filename = data.get("filename").and_then(|v| v.as_str()).unwrap_or("transcript").to_string();
    let primary_language = data.get("primary_language").and_then(|v| v.as_str()).map(|s| s.to_string());
    let contexts = match data.get("contexts").and_then(|v| parse_contexts(v)) {
        Some(c) => c,
        None => {
            return (StatusCode::BAD_REQUEST, Json(json!({"error": "contexts field is required with at least one context"})));
        }
    };
    let user_dir = data.get("user_dir").and_then(|v| v.as_str()).map(PathBuf::from);
    let locked_fields: Option<HashMap<String, Value>> = data
        .get("locked_fields")
        .and_then(|v| serde_json::from_value(v.clone()).ok());
    let pre_classified = data.get("pre_classified");

    info!(
        "Processing transcript: {}, segments={}, contexts={}, pre_classified={}",
        filename,
        transcript_json.get("segments").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0),
        contexts.len(),
        pre_classified.is_some()
    );

    // Step 1: Strip words
    let light_json = transcript::strip_words_from_json(&transcript_json);

    let (metadata, filename_pattern) = if let Some(pc) = pre_classified {
        // Pre-classified flow
        let pc_context = pc.get("context").and_then(|v| v.as_str()).map(|s| s.to_string());
        let pc_date = pc.get("date").and_then(|v| v.as_str()).and_then(|s| {
            chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()
        });
        let pc_fields: HashMap<String, Value> = pc
            .get("fields")
            .and_then(|v| v.as_object())
            .map(|obj| obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();

        let metadata = DocumentMetadata::new(pc_fields, pc_date, pc_context.clone());

        let pattern = contexts.iter()
            .find(|ctx| ctx.get("name").and_then(|v| v.as_str()) == pc_context.as_deref())
            .and_then(|ctx| ctx.get("audio_filename").or_else(|| ctx.get("filename")))
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| contexts.first().and_then(|c| c.get("filename").and_then(|v| v.as_str())).unwrap_or("{date}-{context}"))
            .to_string();

        (metadata, pattern)
    } else {
        // Standard flow
        let classification_text = transcript::create_text_content(&light_json);
        let transcript_instruction = "Note: This is a transcript of an audio recording. \
            With high probability, metadata such as document type, date, subject, \
            or participants has been dictated at the beginning or end of the recording. \
            Pay special attention to the first and last segments for this information.\n\n";
        let full_text = format!("{}{}", transcript_instruction, classification_text);

        match state.ai_client.extract_metadata(
            &full_text,
            &contexts,
            primary_language.as_deref(),
            Some(&filename),
            user_dir.as_deref(),
            locked_fields.as_ref(),
            true,
        ).await {
            Ok(mut r) => {
                r.0.new_clues.clear();
                r
            }
            Err(AiError::Configuration(msg)) => {
                return (StatusCode::BAD_REQUEST, Json(json!({"error": format!("Configuration error: {}", msg)})));
            }
            Err(e) => {
                return (StatusCode::BAD_GATEWAY, Json(json!({"error": format!("AI processing failed: {}", e)})));
            }
        }
    };

    // Build correction context
    let correction_context = build_correction_context(&metadata, &contexts);

    // Correct transcript
    let corrected_json = match transcript::correct_transcript_json(
        &light_json,
        None,
        &state.correction_model,
        state.correction_extended_thinking,
        state.correction_thinking_budget,
        &correction_context,
        state.correction_use_batch,
        user_dir.as_deref(),
    ).await {
        Ok(j) => j,
        Err(e) => {
            error!("Transcript correction failed: {}", e);
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": format!("Internal error: {}", e)})));
        }
    };

    let text_content = transcript::create_text_content(&corrected_json);
    let suggested_filename = metadata.to_filename(&filename_pattern, Some(&filename));

    let mut metadata_response = serde_json::Map::new();
    metadata_response.insert("context".to_string(), metadata.context.clone().map(Value::String).unwrap_or(Value::Null));
    metadata_response.insert("date".to_string(), metadata.date.map(|d| Value::String(d.to_string())).unwrap_or(Value::Null));
    for (k, v) in &metadata.fields {
        metadata_response.insert(k.clone(), v.clone());
    }

    (StatusCode::OK, Json(json!({
        "corrected_json": corrected_json,
        "text": text_content,
        "filename": suggested_filename,
        "metadata": metadata_response,
    })))
}

async fn classify_audio_handler(
    State(state): State<Arc<AppState>>,
    Json(data): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let filename = match data.get("filename").and_then(|v| v.as_str()) {
        Some(f) => f.to_string(),
        None => return (StatusCode::BAD_REQUEST, Json(json!({"error": "filename field is required"}))),
    };
    let contexts = match data.get("contexts").and_then(|v| parse_contexts(v)) {
        Some(c) => c,
        None => return (StatusCode::BAD_REQUEST, Json(json!({"error": "contexts field is required with at least one context"}))),
    };

    info!("Classifying audio file: {}, contexts={}", filename, contexts.len());

    let classification_text = format!("Audio file: {}", filename);
    let (metadata, _) = match state.ai_client.extract_metadata(
        &classification_text,
        &contexts,
        None,
        Some(&filename),
        None,
        None,
        false,
    ).await {
        Ok(r) => r,
        Err(AiError::Configuration(msg)) => {
            return (StatusCode::BAD_REQUEST, Json(json!({"error": format!("Configuration error: {}", msg)})));
        }
        Err(e) => {
            return (StatusCode::BAD_GATEWAY, Json(json!({"error": format!("AI classification failed: {}", e)})));
        }
    };

    let keyterms = collect_transcription_keyterms(&metadata, &contexts);

    let mut metadata_response = serde_json::Map::new();
    metadata_response.insert("context".to_string(), metadata.context.clone().map(Value::String).unwrap_or(Value::Null));
    metadata_response.insert("date".to_string(), metadata.date.map(|d| Value::String(d.to_string())).unwrap_or(Value::Null));
    for (k, v) in &metadata.fields {
        metadata_response.insert(k.clone(), v.clone());
    }

    (StatusCode::OK, Json(json!({
        "context": metadata.context,
        "metadata": metadata_response,
        "transcription_keyterms": keyterms,
    })))
}

async fn classify_transcript_handler(
    State(state): State<Arc<AppState>>,
    Json(data): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let transcript_json = match data.get("transcript") {
        Some(t) => t.clone(),
        None => return (StatusCode::BAD_REQUEST, Json(json!({"error": "transcript field is required"}))),
    };
    let filename = match data.get("filename").and_then(|v| v.as_str()) {
        Some(f) => f.to_string(),
        None => return (StatusCode::BAD_REQUEST, Json(json!({"error": "filename field is required"}))),
    };
    let contexts = match data.get("contexts").and_then(|v| parse_contexts(v)) {
        Some(c) => c,
        None => return (StatusCode::BAD_REQUEST, Json(json!({"error": "contexts field is required with at least one context"}))),
    };

    info!("Classifying transcript for intro file: {}, contexts={}", filename, contexts.len());

    let light_json = transcript::strip_words_from_json(&transcript_json);
    let classification_text = transcript::create_text_content(&light_json);
    let intro_instruction = "At the beginning or the end of the transcript you will find meta information \
        spoken by the person who created the recording. This may include: Date, speaker, \
        number of speakers, background, disturbances in the recording, interruptions, \
        number of related files, possible core interpretation, important open questions, etc.\n\n";
    let full_text = format!("{}{}", intro_instruction, classification_text);

    // Determine context
    let context_name = if contexts.len() == 1 {
        contexts[0].get("name").and_then(|v| v.as_str()).unwrap_or("unknown").to_string()
    } else {
        match state.ai_client.determine_context(&full_text, &contexts, Some(&filename), None, true).await {
            Ok(c) => c,
            Err(e) => {
                return (StatusCode::BAD_GATEWAY, Json(json!({"error": format!("AI classification failed: {}", e)})));
            }
        }
    };

    let context_config = match contexts.iter().find(|c| c.get("name").and_then(|v| v.as_str()) == Some(&context_name)) {
        Some(c) => c,
        None => return (StatusCode::BAD_REQUEST, Json(json!({"error": format!("Context '{}' not found", context_name)}))),
    };

    // Augmented field configs with number_of_speakers
    let mut field_configs: HashMap<String, Value> = context_config
        .get("fields")
        .and_then(|v| v.as_object())
        .map(|obj| obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default();
    field_configs.insert("number_of_speakers".to_string(), json!({
        "instructions": "The number of speakers/participants mentioned in the recording intro. Return as an integer string, e.g. '2'."
    }));

    let mut metadata = match state.ai_client.extract_metadata_with_config(
        &full_text,
        &field_configs,
        &context_name,
        None,
        Some(&filename),
        None,
        None,
    ).await {
        Ok(m) => m,
        Err(e) => {
            return (StatusCode::BAD_GATEWAY, Json(json!({"error": format!("AI classification failed: {}", e)})));
        }
    };
    metadata.new_clues.clear();

    // Extract number_of_speakers
    let number_of_speakers = metadata.fields.remove("number_of_speakers")
        .and_then(|v| v.as_str().and_then(|s| s.parse::<i64>().ok()).map(Value::from));

    let keyterms = collect_transcription_keyterms(&metadata, &contexts);

    let mut metadata_response = serde_json::Map::new();
    metadata_response.insert("context".to_string(), metadata.context.clone().map(Value::String).unwrap_or(Value::Null));
    metadata_response.insert("date".to_string(), metadata.date.map(|d| Value::String(d.to_string())).unwrap_or(Value::Null));
    for (k, v) in &metadata.fields {
        metadata_response.insert(k.clone(), v.clone());
    }

    (StatusCode::OK, Json(json!({
        "context": metadata.context,
        "metadata": metadata_response,
        "transcription_keyterms": keyterms,
        "number_of_speakers": number_of_speakers,
    })))
}

// =============================================================================
// Helper functions
// =============================================================================

fn collect_transcription_keyterms(metadata: &DocumentMetadata, contexts: &[Value]) -> Vec<String> {
    let mut keyterms = std::collections::BTreeSet::new();

    let context_config = contexts.iter().find(|c| {
        c.get("name").and_then(|v| v.as_str()) == metadata.context.as_deref()
    });

    if let Some(ctx) = context_config {
        // Context-level keyterms
        if let Some(arr) = ctx.get("transcription_keyterms").and_then(|v| v.as_array()) {
            for v in arr {
                if let Some(s) = v.as_str() {
                    keyterms.insert(s.to_string());
                }
            }
        }

        // Candidate keyterms
        if let Some(fields) = ctx.get("fields").and_then(|v| v.as_object()) {
            for (fname, fconfig) in fields {
                let value = metadata.get_field_str(fname);
                if value.is_none() {
                    continue;
                }
                let value = value.unwrap();
                if let Some(candidates) = fconfig.get("candidates").and_then(|v| v.as_array()) {
                    for c in candidates {
                        if let Some(obj) = c.as_object() {
                            let name = obj.get("name").and_then(|v| v.as_str());
                            let short = obj.get("short").and_then(|v| v.as_str());
                            if name == Some(value) || short == Some(value) {
                                if let Some(kt) = obj.get("transcription_keyterms").and_then(|v| v.as_array()) {
                                    for k in kt {
                                        if let Some(s) = k.as_str() {
                                            keyterms.insert(s.to_string());
                                        }
                                    }
                                }
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    keyterms.into_iter().collect()
}

fn build_correction_context(metadata: &DocumentMetadata, contexts: &[Value]) -> String {
    let mut parts = Vec::new();

    let context_config = contexts.iter().find(|c| {
        c.get("name").and_then(|v| v.as_str()) == metadata.context.as_deref()
    });

    if let Some(ctx) = context_config {
        if let Some(desc) = ctx.get("description").and_then(|v| v.as_str()) {
            parts.push(format!("Document context: {}", desc));
        }

        if let Some(fields) = ctx.get("fields").and_then(|v| v.as_object()) {
            for (fname, fconfig) in fields {
                let value = metadata.get_field_str(fname);
                if value.is_none() {
                    continue;
                }
                let value = value.unwrap();
                if let Some(candidates) = fconfig.get("candidates").and_then(|v| v.as_array()) {
                    for c in candidates {
                        if let Some(obj) = c.as_object() {
                            if obj.get("name").and_then(|v| v.as_str()) == Some(value) {
                                if let Some(clues) = obj.get("clues").and_then(|v| v.as_array()) {
                                    let clue_strs: Vec<&str> = clues.iter().filter_map(|v| v.as_str()).collect();
                                    if !clue_strs.is_empty() {
                                        let capitalized = fname.chars().next()
                                            .map(|c| c.to_uppercase().to_string())
                                            .unwrap_or_default() + &fname[1..];
                                        parts.push(format!("{} '{}': {}", capitalized, value, clue_strs.join("; ")));
                                    }
                                }
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    parts.join("\n")
}

// =============================================================================
// Main
// =============================================================================

#[tokio::main]
async fn main() {
    // Setup logging
    let log_level = std::env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_string());
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&log_level)),
        )
        .init();

    let ocr_url = std::env::var("OCR_URL").unwrap_or_else(|_| "http://ocrmypdf:5000".to_string());
    let anthropic_api_key = std::env::var("ANTHROPIC_API_KEY").unwrap_or_else(|_| {
        error!("ANTHROPIC_API_KEY environment variable is required");
        std::process::exit(1);
    });
    let anthropic_model = std::env::var("ANTHROPIC_MODEL").ok();
    let host = std::env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port: u16 = std::env::var("PORT")
        .unwrap_or_else(|_| "8000".to_string())
        .parse()
        .unwrap_or(8000);

    let config = config::load_config(None);

    let correction_config = &config.transcript_correction;
    let correction_model = correction_config.model.clone();
    let correction_extended_thinking = correction_config.extended_thinking;
    let correction_thinking_budget = correction_config.thinking_budget;
    let correction_use_batch = correction_config.use_batch;

    let ai_client = AiClient::new(&anthropic_api_key, anthropic_model.as_deref(), config)
        .unwrap_or_else(|e| {
            error!("Failed to initialize AI client: {}", e);
            std::process::exit(1);
        });

    info!("Starting MrDocument service v{} (commit {}) on {}:{}", env!("CARGO_PKG_VERSION"), env!("GIT_COMMIT_HASH"), host, port);
    info!(
        "Transcript correction: model={}, extended_thinking={}, budget={}, use_batch={}",
        correction_model, correction_extended_thinking, correction_thinking_budget, correction_use_batch
    );

    let state = Arc::new(AppState {
        ocr_client: OcrClient::new(&ocr_url),
        ai_client,
        correction_model,
        correction_extended_thinking,
        correction_thinking_budget,
        correction_use_batch,
    });

    // Initialize cost tracker
    costs::get_cost_tracker();

    let app = Router::new()
        .route("/health", get(health))
        .route("/process", post(process_document))
        .route("/process_transcript", post(process_transcript_handler))
        .route("/classify_audio", post(classify_audio_handler))
        .route("/classify_transcript", post(classify_transcript_handler))
        .layer(DefaultBodyLimit::max(100 * 1024 * 1024)) // 100MB
        .with_state(state);

    let addr = format!("{}:{}", host, port);
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    info!("MrDocument service listening on {}", addr);

    // Graceful shutdown
    let shutdown = async {
        tokio::signal::ctrl_c().await.ok();
        info!("Shutting down...");
        costs::shutdown_cost_tracker();
    };

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
        .unwrap();
}
