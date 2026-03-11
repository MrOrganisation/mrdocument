//! Processing service calls for document watcher v2.
//!
//! Sends documents to the mrdocument service for classification and OCR,
//! and orchestrates audio processing via STT + mrdocument service.
//! Writes results and sidecar metadata to `.output/`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context as _, Result};
use base64::Engine;
use once_cell::sync::Lazy;
use reqwest::multipart;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::config::SorterContextManager;
use crate::models::Record;

// ---------------------------------------------------------------------------
// Extension-based type detection
// ---------------------------------------------------------------------------

/// Supported audio/video file extensions.
pub const AUDIO_EXTENSIONS: &[&str] = &[
    ".flac", ".wav", ".mp3", ".ogg", ".webm", ".mp4", ".m4a", ".mkv", ".avi", ".mov",
];

/// Supported document file extensions.
pub const DOCUMENT_EXTENSIONS: &[&str] = &[
    ".pdf", ".eml", ".html", ".htm", ".docx", ".txt", ".md", ".rtf", ".jpg", ".jpeg", ".png",
    ".gif", ".tiff", ".tif", ".bmp", ".webp", ".ppm", ".pgm", ".pbm", ".pnm",
];

/// Maps file extension to MIME content type.
pub static CONTENT_TYPE_MAP: Lazy<HashMap<&'static str, &'static str>> = Lazy::new(|| {
    let mut m = HashMap::new();
    m.insert(".pdf", "application/pdf");
    m.insert(".eml", "message/rfc822");
    m.insert(".html", "text/html");
    m.insert(".htm", "text/html");
    m.insert(
        ".docx",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    );
    m.insert(".txt", "text/plain");
    m.insert(".md", "text/markdown");
    m.insert(".rtf", "application/rtf");
    m.insert(".jpg", "image/jpeg");
    m.insert(".jpeg", "image/jpeg");
    m.insert(".png", "image/png");
    m.insert(".gif", "image/gif");
    m.insert(".tiff", "image/tiff");
    m.insert(".tif", "image/tiff");
    m.insert(".bmp", "image/bmp");
    m.insert(".webp", "image/webp");
    m.insert(".ppm", "image/x-portable-pixmap");
    m.insert(".pgm", "image/x-portable-graymap");
    m.insert(".pbm", "image/x-portable-bitmap");
    m.insert(".pnm", "image/x-portable-anymap");
    m.insert(".flac", "audio/flac");
    m.insert(".wav", "audio/wav");
    m.insert(".mp3", "audio/mpeg");
    m.insert(".ogg", "audio/ogg");
    m.insert(".webm", "video/webm");
    m.insert(".mp4", "video/mp4");
    m.insert(".m4a", "audio/mp4");
    m.insert(".mkv", "video/x-matroska");
    m.insert(".avi", "video/x-msvideo");
    m.insert(".mov", "video/quicktime");
    m
});

/// Get MIME content type for a file extension.
fn get_content_type(ext: &str) -> &'static str {
    CONTENT_TYPE_MAP
        .get(ext)
        .copied()
        .unwrap_or("application/octet-stream")
}

/// Check if extension indicates an audio/video file.
fn is_audio(ext: &str) -> bool {
    AUDIO_EXTENSIONS.contains(&ext)
}

// ---------------------------------------------------------------------------
// STT Configuration
// ---------------------------------------------------------------------------

/// STT (Speech-to-Text) configuration loaded from `stt.yaml`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SttConfig {
    #[serde(default = "default_language")]
    pub language: String,
    #[serde(default = "default_elevenlabs_model")]
    pub elevenlabs_model: String,
    #[serde(default = "default_enable_diarization")]
    pub enable_diarization: bool,
    #[serde(default = "default_diarization_speaker_count")]
    pub diarization_speaker_count: u32,
}

fn default_language() -> String {
    "de-DE".to_string()
}
fn default_elevenlabs_model() -> String {
    "scribe_v2".to_string()
}
fn default_enable_diarization() -> bool {
    true
}
fn default_diarization_speaker_count() -> u32 {
    2
}

impl Default for SttConfig {
    fn default() -> Self {
        Self {
            language: default_language(),
            elevenlabs_model: default_elevenlabs_model(),
            enable_diarization: default_enable_diarization(),
            diarization_speaker_count: default_diarization_speaker_count(),
        }
    }
}

impl SttConfig {
    /// Load STT config from user folder. Returns `None` if not configured.
    ///
    /// If `stt.yaml` exists but is empty or not a mapping, returns default config.
    pub fn load(user_root: &Path) -> Option<Self> {
        let stt_path = user_root.join("stt.yaml");
        if !stt_path.exists() {
            return None;
        }
        match std::fs::read_to_string(&stt_path) {
            Ok(contents) => match serde_yaml::from_str::<serde_yaml::Value>(&contents) {
                Ok(value) => {
                    if value.is_null() || !value.is_mapping() {
                        return Some(Self::default());
                    }
                    match serde_yaml::from_value::<SttConfig>(value) {
                        Ok(config) => Some(config),
                        Err(e) => {
                            warn!(
                                "Failed to parse stt.yaml from {}: {}",
                                user_root.display(),
                                e
                            );
                            None
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to load stt.yaml from {}: {}",
                        user_root.display(),
                        e
                    );
                    None
                }
            },
            Err(e) => {
                warn!(
                    "Failed to read stt.yaml from {}: {}",
                    user_root.display(),
                    e
                );
                None
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Processor
// ---------------------------------------------------------------------------

/// Sends documents/audio to the mrdocument service for processing.
#[derive(Clone)]
pub struct Processor {
    pub root: PathBuf,
    pub name: String,
    pub service_url: String,
    pub stt_url: Option<String>,
    pub timeout: Duration,
    pub max_retries: u32,
    pub retry_delay: f64,
    pub contexts: Option<Vec<serde_json::Value>>,
    pub context_manager: Option<SorterContextManager>,
    client: reqwest::Client,
}

impl Processor {
    /// Create a new processor.
    pub fn new(
        root: PathBuf,
        name: String,
        service_url: String,
        stt_url: Option<String>,
        timeout: f64,
        contexts: Option<Vec<serde_json::Value>>,
        context_manager: Option<SorterContextManager>,
    ) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs_f64(timeout))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            root,
            name,
            service_url,
            stt_url,
            timeout: Duration::from_secs_f64(timeout),
            max_retries: 3,
            retry_delay: 2.0,
            contexts,
            context_manager,
            client,
        }
    }

    /// Process a single record by calling the appropriate service.
    ///
    /// Documents go to mrdocument `/process`.
    /// Audio files go through the STT orchestration flow.
    /// On success: writes output file and sidecar JSON to `.output/`.
    /// On error: creates a 0-byte file at `.output/{output_filename}`.
    pub async fn process_one(&self, record: &Record) -> Result<()> {
        let output_dir = self.root.join(".output");
        tokio::fs::create_dir_all(&output_dir).await?;

        let output_filename = record
            .output_filename
            .as_deref()
            .context("No output_filename set on record")?;
        let output_path = output_dir.join(output_filename);
        let sidecar_path = output_dir.join(format!("{}.meta.json", output_filename));

        let process_result: Result<()> = async {
            let source_path_entry = record
                .source_file()
                .context("No source file on record")?;
            let source_path = self.root.join(&source_path_entry.path);
            let ext = source_path
                .extension()
                .and_then(|e| e.to_str())
                .map(|e| format!(".{}", e.to_ascii_lowercase()))
                .unwrap_or_default();

            let is_doc = DOCUMENT_EXTENSIONS.contains(&ext.as_str());
            let is_aud = AUDIO_EXTENSIONS.contains(&ext.as_str());

            if !is_doc && !is_aud {
                warn!(
                    "Skipping unsupported file type {}: {}",
                    if ext.is_empty() { "(no extension)" } else { &ext },
                    source_path_entry.path
                );
                touch_file(&output_path).await?;
                return Ok(());
            }

            let file_bytes = tokio::fs::read(&source_path)
                .await
                .with_context(|| format!("Failed to read source file {}", source_path.display()))?;
            let content_type = get_content_type(&ext);
            let filename = source_path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("file")
                .to_string();

            // If context is pre-set (e.g., from sorted/), filter to that context
            let all_contexts = self.contexts.clone().unwrap_or_default();
            let contexts = if let Some(ref ctx) = record.context {
                let filtered: Vec<serde_json::Value> = all_contexts
                    .iter()
                    .filter(|c| c.get("name").and_then(|n| n.as_str()) == Some(ctx.as_str()))
                    .cloned()
                    .collect();
                if filtered.is_empty() {
                    all_contexts
                } else {
                    filtered
                }
            } else {
                all_contexts
            };

            let result: Option<serde_json::Value> = if is_aud {
                self.process_audio(&file_bytes, &filename, content_type, &contexts)
                    .await?
            } else {
                self.call_service(&file_bytes, &filename, content_type, "document", &contexts)
                    .await?
            };

            let result = match result {
                Some(r) => r,
                None => {
                    touch_file(&output_path).await?;
                    return Ok(());
                }
            };

            // Extract response
            let metadata = result.get("metadata").cloned().unwrap_or(serde_json::json!({}));
            let content_b64 = result.get("pdf").and_then(|v| v.as_str());
            let text_content = result.get("text").and_then(|v| v.as_str());

            let content_bytes = if let Some(b64) = content_b64 {
                base64::engine::general_purpose::STANDARD
                    .decode(b64)
                    .context("Failed to decode base64 PDF content")?
            } else if let Some(text) = text_content {
                text.as_bytes().to_vec()
            } else {
                file_bytes.clone()
            };

            // For audio/transcript results, ensure filename has .txt extension
            let mut suggested_filename = result
                .get("filename")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            if let Some(ref mut sf) = suggested_filename {
                if text_content.is_some() {
                    // Strip any existing extension before adding .txt
                    let stem = Path::new(sf.as_str())
                        .file_stem()
                        .and_then(|s| s.to_str())
                        .unwrap_or(sf);
                    *sf = format!("{}.txt", stem);
                }
            }

            // Atomic write: tmp file -> rename
            let tmp_path = output_path.with_extension("tmp");
            tokio::fs::write(&tmp_path, &content_bytes).await?;
            tokio::fs::rename(&tmp_path, &output_path).await?;

            // Write sidecar
            let assigned_fn = suggested_filename.or_else(|| {
                result
                    .get("filename")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            });
            let sidecar = serde_json::json!({
                "context": metadata.get("context"),
                "metadata": metadata,
                "assigned_filename": assigned_fn,
            });
            tokio::fs::write(&sidecar_path, serde_json::to_string(&sidecar)?)
                .await?;

            // new_clues handling is best-effort; skipped in cloned processor context

            info!(
                "[{}] Processed {} -> {}",
                self.name, source_path_entry.path, output_filename
            );
            Ok(())
        }
        .await;

        if let Err(e) = process_result {
            error!("[{}] Failed to process {}: {}", self.name, output_filename, e);
            // Create 0-byte file on error
            let _ = touch_file(&output_path).await;
        }

        Ok(())
    }

    // ------------------------------------------------------------------
    // Document processing
    // ------------------------------------------------------------------

    /// Call the mrdocument `/process` endpoint with retry logic.
    async fn call_service(
        &self,
        file_bytes: &[u8],
        filename: &str,
        content_type: &str,
        file_type: &str,
        contexts: &[serde_json::Value],
    ) -> Result<Option<serde_json::Value>> {
        let url = format!("{}/process", self.service_url);

        let file_bytes = file_bytes.to_vec();
        let filename_owned = filename.to_string();
        let content_type = content_type.to_string();
        let file_type = file_type.to_string();
        let contexts_json = serde_json::to_string(contexts)?;
        let root_str = self.root.to_string_lossy().to_string();
        let label_filename = filename_owned.clone();

        let make_form = move || -> Result<multipart::Form> {
            let file_part = multipart::Part::bytes(file_bytes.clone())
                .file_name(filename_owned.clone())
                .mime_str(&content_type)?;
            let form = multipart::Form::new()
                .part("file", file_part)
                .text("type", file_type.clone())
                .text("contexts", contexts_json.clone())
                .text("user_dir", root_str.clone());
            Ok(form)
        };

        self.call_with_retry(&url, make_form, self.max_retries, "process", Some(&label_filename), Some(self.timeout))
            .await
    }

    // ------------------------------------------------------------------
    // Audio processing -- STT orchestration
    // ------------------------------------------------------------------

    /// Orchestrate audio processing: classify -> STT -> process transcript.
    ///
    /// Returns the response value on success, or `None` on failure.
    async fn process_audio(
        &self,
        file_bytes: &[u8],
        filename: &str,
        content_type: &str,
        contexts: &[serde_json::Value],
    ) -> Result<Option<serde_json::Value>> {
        // Check prerequisites
        let stt_url = match &self.stt_url {
            Some(url) => url.clone(),
            None => {
                warn!("[{}] Audio file {} skipped: no STT URL configured", self.name, filename);
                return Ok(None);
            }
        };

        let stt_config = match SttConfig::load(&self.root) {
            Some(config) => config,
            None => {
                warn!("[{}] Audio file {} skipped: no stt.yaml found", self.name, filename);
                return Ok(None);
            }
        };

        // Step 1: Classify audio by filename (optional)
        let keyterms = self.classify_audio(filename, contexts).await;

        // Step 2: First STT pass (required)
        let transcript = self
            .stt_transcribe(
                &stt_url,
                file_bytes,
                filename,
                content_type,
                &stt_config,
                keyterms.as_deref(),
                None,
            )
            .await?;

        let mut transcript = match transcript {
            Some(t) => t,
            None => return Ok(None),
        };

        // Validate transcript
        let segments = transcript.get("segments").and_then(|v| v.as_array());
        if segments.is_none() || segments.unwrap().is_empty() {
            error!("[{}] Empty transcript for {}", self.name, filename);
            return Ok(None);
        }

        info!(
            "[{}] Got transcript for {}: {} segments",
            self.name,
            filename,
            segments.unwrap().len()
        );

        // Step 3: Intro two-pass (optional)
        let mut pre_classified: Option<serde_json::Value> = None;
        if filename.to_lowercase().contains("intro") {
            let (new_transcript, new_pre_classified) = self
                .intro_two_pass(
                    &stt_url,
                    file_bytes,
                    filename,
                    content_type,
                    &stt_config,
                    &transcript,
                    contexts,
                )
                .await;
            transcript = new_transcript;
            pre_classified = new_pre_classified;
        }

        // Step 4: Process transcript (required)
        self.process_transcript(&transcript, filename, pre_classified.as_ref(), contexts)
            .await
    }

    /// Classify audio by filename to get transcription keyterms.
    ///
    /// Optional -- failure returns `None` (processing continues without keyterms).
    async fn classify_audio(&self, filename: &str, contexts: &[serde_json::Value]) -> Option<Vec<String>> {
        let result: Result<Option<Vec<String>>> = async {
            let request_body = serde_json::json!({
                "filename": filename,
                "contexts": contexts,
            });

            let url = format!("{}/classify_audio", self.service_url);

            let result = self
                .call_json_with_retry(&url, &request_body, 2, "classify_audio", Some(filename), Some(Duration::from_secs(120)))
                .await?;

            match result {
                Some(r) => {
                    let context = r.get("context").and_then(|v| v.as_str());
                    let keyterms: Option<Vec<String>> = r
                        .get("transcription_keyterms")
                        .and_then(|v| v.as_array())
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                .collect()
                        });

                    if let Some(ref kt) = keyterms {
                        if !kt.is_empty() {
                            info!(
                                "[{}] Audio classification for {}: context={}, {} keyterms",
                                self.name,
                                filename,
                                context.unwrap_or("none"),
                                kt.len()
                            );
                        }
                    }

                    Ok(if keyterms.as_ref().map_or(true, |kt| kt.is_empty()) {
                        None
                    } else {
                        keyterms
                    })
                }
                None => Ok(None),
            }
        }
        .await;

        match result {
            Ok(kt) => kt,
            Err(e) => {
                warn!("[{}] Audio classification failed: {}", self.name, e);
                None
            }
        }
    }

    /// Send audio to STT service for transcription.
    ///
    /// Returns the transcript value on success, or `None` on failure.
    async fn stt_transcribe(
        &self,
        stt_url: &str,
        file_bytes: &[u8],
        filename: &str,
        content_type: &str,
        stt_config: &SttConfig,
        keyterms: Option<&[String]>,
        speaker_count: Option<u32>,
    ) -> Result<Option<serde_json::Value>> {
        let url = format!("{}/transcribe", stt_url);

        let file_bytes = file_bytes.to_vec();
        let filename_owned = filename.to_string();
        let content_type_owned = content_type.to_string();
        let language = stt_config.language.clone();
        let model = stt_config.elevenlabs_model.clone();
        let enable_diarization = stt_config.enable_diarization;
        let n_speakers = speaker_count.unwrap_or(stt_config.diarization_speaker_count);
        let keyterms_json = keyterms.map(|kt| serde_json::to_string(kt).unwrap_or_default());

        let make_form = move || -> Result<multipart::Form> {
            let file_part = multipart::Part::bytes(file_bytes.clone())
                .file_name(filename_owned.clone())
                .mime_str(&content_type_owned)?;
            let mut form = multipart::Form::new()
                .part("file", file_part)
                .text("language", language.clone())
                .text("elevenlabs_model", model.clone())
                .text(
                    "enable_diarization",
                    enable_diarization.to_string().to_lowercase(),
                )
                .text("diarization_speaker_count", n_speakers.to_string());
            if let Some(ref kt) = keyterms_json {
                form = form.text("keyterms", kt.clone());
            }
            Ok(form)
        };

        let timeout = Duration::from_secs(1800);
        let result = self
            .call_with_retry(&url, make_form, self.max_retries, "stt_transcribe", Some(filename), Some(timeout))
            .await?;

        Ok(result.and_then(|r| r.get("transcript").cloned()))
    }

    /// Handle intro file two-pass flow.
    ///
    /// Returns `(final_transcript, pre_classified)`.
    /// Falls back to original transcript on any failure.
    async fn intro_two_pass(
        &self,
        stt_url: &str,
        file_bytes: &[u8],
        filename: &str,
        content_type: &str,
        stt_config: &SttConfig,
        transcript: &serde_json::Value,
        contexts: &[serde_json::Value],
    ) -> (serde_json::Value, Option<serde_json::Value>) {
        info!(
            "[{}] Intro file detected, starting two-pass flow for {}",
            self.name, filename
        );
        let mut pre_classified: Option<serde_json::Value> = None;
        let mut final_transcript = transcript.clone();

        let result: Result<()> = async {
            // Classify transcript to get richer keyterms + speaker count
            let request_body = serde_json::json!({
                "transcript": transcript,
                "filename": filename,
                "contexts": contexts,
            });

            let url = format!("{}/classify_transcript", self.service_url);
            let ct_result = self
                .call_json_with_retry(&url, &request_body, 2, "classify_transcript", Some(filename), Some(Duration::from_secs(300)))
                .await?;

            let ct_result = match ct_result {
                Some(r) => r,
                None => {
                    warn!("[{}] Transcript classification failed, using first pass only", self.name);
                    return Ok(());
                }
            };

            let keyterms_2: Option<Vec<String>> = ct_result
                .get("transcription_keyterms")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect()
                });
            let n_speakers = ct_result
                .get("number_of_speakers")
                .and_then(|v| v.as_u64())
                .map(|n| n as u32);
            let ct_context = ct_result
                .get("context")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let ct_metadata = ct_result.get("metadata").cloned().unwrap_or(serde_json::json!({}));

            info!(
                "[{}] Transcript classification: context={}, keyterms={}, speakers={:?}",
                self.name,
                ct_context.as_deref().unwrap_or("none"),
                keyterms_2.as_ref().map_or(0, |kt| kt.len()),
                n_speakers
            );

            // Build pre_classified if classification succeeded
            if let (Some(ctx), Some(meta_obj)) =
                (&ct_context, ct_metadata.as_object())
            {
                if !meta_obj.is_empty() {
                    let mut pc_fields = serde_json::Map::new();
                    for (k, v) in meta_obj.iter() {
                        if k != "context" && k != "date" {
                            pc_fields.insert(k.clone(), v.clone());
                        }
                    }
                    pre_classified = Some(serde_json::json!({
                        "context": ctx,
                        "date": ct_metadata.get("date"),
                        "fields": pc_fields,
                    }));
                }
            }

            // Second STT pass with improved keyterms
            if let Some(ref kt2) = keyterms_2 {
                if !kt2.is_empty() {
                    info!("[{}] Running second STT pass with {} keyterms", self.name, kt2.len());
                    let speaker_count =
                        n_speakers.and_then(|n| if n > 1 { Some(n) } else { None });
                    let transcript_2 = self
                        .stt_transcribe(
                            stt_url,
                            file_bytes,
                            filename,
                            content_type,
                            stt_config,
                            Some(kt2),
                            speaker_count,
                        )
                        .await?;

                    if let Some(t2) = transcript_2 {
                        let has_segments = t2
                            .get("segments")
                            .and_then(|v| v.as_array())
                            .map_or(false, |a| !a.is_empty());
                        if has_segments {
                            info!(
                                "[{}] Second STT pass successful: {} segments",
                                self.name,
                                t2.get("segments")
                                    .and_then(|v| v.as_array())
                                    .map_or(0, |a| a.len())
                            );
                            final_transcript = t2;
                        } else {
                            warn!("[{}] Second STT pass failed, using first pass", self.name);
                        }
                    } else {
                        warn!("[{}] Second STT pass failed, using first pass", self.name);
                    }
                }
            }

            Ok(())
        }
        .await;

        if let Err(e) = result {
            warn!("[{}] Intro two-pass error: {}, using first pass", self.name, e);
        }

        (final_transcript, pre_classified)
    }

    /// Send transcript to mrdocument for processing.
    async fn process_transcript(
        &self,
        transcript: &serde_json::Value,
        filename: &str,
        pre_classified: Option<&serde_json::Value>,
        contexts: &[serde_json::Value],
    ) -> Result<Option<serde_json::Value>> {
        let mut request_body = serde_json::json!({
            "transcript": transcript,
            "filename": filename,
            "contexts": contexts,
            "user_dir": self.root.to_string_lossy(),
        });

        if let Some(pc) = pre_classified {
            request_body
                .as_object_mut()
                .unwrap()
                .insert("pre_classified".to_string(), pc.clone());
        }

        let url = format!("{}/process_transcript", self.service_url);
        let timeout = Duration::from_secs(1800);
        self.call_json_with_retry(&url, &request_body, self.max_retries, "process_transcript", Some(filename), Some(timeout))
            .await
    }

    // ------------------------------------------------------------------
    // Shared retry logic
    // ------------------------------------------------------------------

    /// Execute a multipart form POST with retry logic.
    ///
    /// Retries on 5xx and 429; fails immediately on other 4xx client errors.
    /// Uses exponential backoff between retries (capped at 30s).
    async fn call_with_retry<MF>(
        &self,
        url: &str,
        make_form: MF,
        max_retries: u32,
        label: &str,
        source: Option<&str>,
        timeout: Option<Duration>,
    ) -> Result<Option<serde_json::Value>>
    where
        MF: Fn() -> Result<multipart::Form>,
    {
        let mut delay = self.retry_delay;
        let max_delay: f64 = 30.0;
        let tag = match source {
            Some(s) => format!("{} [{}]", label, s),
            None => label.to_string(),
        };

        let request_timeout = timeout.unwrap_or(self.timeout);

        for attempt in 0..=max_retries {
            let form = make_form()?;
            let result = self
                .client
                .post(url)
                .timeout(request_timeout)
                .multipart(form)
                .send()
                .await;

            match result {
                Ok(response) => {
                    let status = response.status().as_u16();

                    if status == 200 {
                        let body: serde_json::Value = response.json().await?;
                        return Ok(Some(body));
                    }

                    if status < 500 && status != 429 {
                        error!("{} client error: HTTP {}", tag, status);
                        return Ok(None);
                    }

                    warn!(
                        "{} error (attempt {}/{}): HTTP {}",
                        tag,
                        attempt + 1,
                        max_retries + 1,
                        status
                    );
                }
                Err(e) => {
                    warn!(
                        "{} connection error (attempt {}/{}): {}",
                        tag,
                        attempt + 1,
                        max_retries + 1,
                        e
                    );
                }
            }

            if attempt < max_retries {
                tokio::time::sleep(Duration::from_secs_f64(delay)).await;
                delay = (delay * 2.0).min(max_delay);
            }
        }

        Ok(None)
    }

    /// Execute a JSON POST with retry logic.
    ///
    /// Retries on 5xx and 429; fails immediately on other 4xx client errors.
    /// Uses exponential backoff between retries (capped at 30s).
    async fn call_json_with_retry(
        &self,
        url: &str,
        body: &serde_json::Value,
        max_retries: u32,
        label: &str,
        source: Option<&str>,
        timeout: Option<Duration>,
    ) -> Result<Option<serde_json::Value>> {
        let mut delay = self.retry_delay;
        let max_delay: f64 = 30.0;
        let tag = match source {
            Some(s) => format!("{} [{}]", label, s),
            None => label.to_string(),
        };

        let request_timeout = timeout.unwrap_or(self.timeout);

        for attempt in 0..=max_retries {
            let result = self
                .client
                .post(url)
                .timeout(request_timeout)
                .json(body)
                .send()
                .await;

            match result {
                Ok(response) => {
                    let status = response.status().as_u16();

                    if status == 200 {
                        let json_body: serde_json::Value = response.json().await?;
                        return Ok(Some(json_body));
                    }

                    if status < 500 && status != 429 {
                        error!("{} client error: HTTP {}", tag, status);
                        return Ok(None);
                    }

                    warn!(
                        "{} error (attempt {}/{}): HTTP {}",
                        tag,
                        attempt + 1,
                        max_retries + 1,
                        status
                    );
                }
                Err(e) => {
                    warn!(
                        "{} connection error (attempt {}/{}): {}",
                        tag,
                        attempt + 1,
                        max_retries + 1,
                        e
                    );
                }
            }

            if attempt < max_retries {
                tokio::time::sleep(Duration::from_secs_f64(delay)).await;
                delay = (delay * 2.0).min(max_delay);
            }
        }

        Ok(None)
    }
}

/// Create a 0-byte file (touch).
async fn touch_file(path: &Path) -> Result<()> {
    tokio::fs::write(path, b"").await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_content_type_map() {
        assert_eq!(get_content_type(".pdf"), "application/pdf");
        assert_eq!(get_content_type(".jpg"), "image/jpeg");
        assert_eq!(get_content_type(".jpeg"), "image/jpeg");
        assert_eq!(get_content_type(".mp3"), "audio/mpeg");
        assert_eq!(get_content_type(".unknown"), "application/octet-stream");
        assert_eq!(get_content_type(""), "application/octet-stream");
    }

    #[test]
    fn test_is_audio() {
        assert!(is_audio(".flac"));
        assert!(is_audio(".mp3"));
        assert!(is_audio(".mp4"));
        assert!(is_audio(".m4a"));
        assert!(!is_audio(".pdf"));
        assert!(!is_audio(".txt"));
        assert!(!is_audio(""));
    }

    #[test]
    fn test_document_extensions() {
        assert!(DOCUMENT_EXTENSIONS.contains(&".pdf"));
        assert!(DOCUMENT_EXTENSIONS.contains(&".jpg"));
        assert!(DOCUMENT_EXTENSIONS.contains(&".png"));
        assert!(DOCUMENT_EXTENSIONS.contains(&".txt"));
        assert!(!DOCUMENT_EXTENSIONS.contains(&".mp3"));
        assert!(!DOCUMENT_EXTENSIONS.contains(&".flac"));
    }

    #[test]
    fn test_stt_config_default() {
        let cfg = SttConfig::default();
        assert_eq!(cfg.language, "de-DE");
        assert_eq!(cfg.elevenlabs_model, "scribe_v2");
        assert!(cfg.enable_diarization);
        assert_eq!(cfg.diarization_speaker_count, 2);
    }

    #[test]
    fn test_stt_config_load_from_yaml() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("stt.yaml"),
            "language: en-US\nenable_diarization: false\n",
        )
        .unwrap();
        let config = SttConfig::load(dir.path()).unwrap();
        assert_eq!(config.language, "en-US");
        assert!(!config.enable_diarization);
        // Fields not specified in YAML should get defaults
        assert_eq!(config.elevenlabs_model, "scribe_v2");
        assert_eq!(config.diarization_speaker_count, 2);
    }

    #[test]
    fn test_stt_config_load_missing() {
        let dir = tempfile::TempDir::new().unwrap();
        // No stt.yaml file exists
        let config = SttConfig::load(dir.path());
        assert!(config.is_none());
    }

    #[test]
    fn test_stt_config_load_empty() {
        let dir = tempfile::TempDir::new().unwrap();
        // Empty file should return default config
        std::fs::write(dir.path().join("stt.yaml"), "").unwrap();
        let config = SttConfig::load(dir.path()).unwrap();
        assert_eq!(config.language, "de-DE");
        assert_eq!(config.elevenlabs_model, "scribe_v2");
        assert!(config.enable_diarization);
        assert_eq!(config.diarization_speaker_count, 2);
    }

    #[test]
    fn test_stt_config_custom_values() {
        let dir = tempfile::TempDir::new().unwrap();
        let yaml = "\
language: fr-FR
elevenlabs_model: custom_model_v3
enable_diarization: true
diarization_speaker_count: 5
";
        std::fs::write(dir.path().join("stt.yaml"), yaml).unwrap();
        let config = SttConfig::load(dir.path()).unwrap();
        assert_eq!(config.language, "fr-FR");
        assert_eq!(config.elevenlabs_model, "custom_model_v3");
        assert!(config.enable_diarization);
        assert_eq!(config.diarization_speaker_count, 5);
    }

    #[test]
    fn test_all_audio_extensions() {
        let expected = vec![
            ".flac", ".wav", ".mp3", ".ogg", ".webm", ".mp4", ".m4a", ".mkv", ".avi", ".mov",
        ];
        for ext in &expected {
            assert!(
                AUDIO_EXTENSIONS.contains(ext),
                "AUDIO_EXTENSIONS missing {}",
                ext
            );
        }
        assert_eq!(AUDIO_EXTENSIONS.len(), expected.len());
    }

    #[test]
    fn test_all_document_extensions() {
        let expected = vec![
            ".pdf", ".eml", ".html", ".htm", ".docx", ".txt", ".md", ".rtf", ".jpg", ".jpeg",
            ".png", ".gif", ".tiff", ".tif", ".bmp", ".webp", ".ppm", ".pgm", ".pbm", ".pnm",
        ];
        for ext in &expected {
            assert!(
                DOCUMENT_EXTENSIONS.contains(ext),
                "DOCUMENT_EXTENSIONS missing {}",
                ext
            );
        }
        assert_eq!(DOCUMENT_EXTENSIONS.len(), expected.len());
    }

    #[test]
    fn test_content_type_map_completeness() {
        // Every extension in AUDIO_EXTENSIONS and DOCUMENT_EXTENSIONS
        // should have a mapping in CONTENT_TYPE_MAP.
        for ext in AUDIO_EXTENSIONS.iter().chain(DOCUMENT_EXTENSIONS.iter()) {
            assert!(
                CONTENT_TYPE_MAP.contains_key(ext),
                "CONTENT_TYPE_MAP missing mapping for {}",
                ext
            );
            // The mapped value should not be the fallback
            let ct = get_content_type(ext);
            assert_ne!(
                ct, "application/octet-stream",
                "Extension {} maps to fallback content type",
                ext
            );
        }
    }
}
