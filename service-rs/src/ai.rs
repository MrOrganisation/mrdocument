use chrono::NaiveDate;
use regex::Regex;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::Path;
use tracing::{debug, error, info, warn};
use unicode_normalization::UnicodeNormalization;

use crate::config::{AppConfig, ModelConfigYaml};
use crate::costs::get_cost_tracker;

// =============================================================================
// Type Definitions
// =============================================================================

// =============================================================================
// Character Replacements for Filename Sanitization
// =============================================================================

const CHAR_REPLACEMENTS: &[(&str, &str)] = &[
    ("ä", "ae"),
    ("ö", "oe"),
    ("ü", "ue"),
    ("Ä", "Ae"),
    ("Ö", "Oe"),
    ("Ü", "Ue"),
    ("ß", "ss"),
    ("æ", "ae"),
    ("œ", "oe"),
    ("ø", "o"),
    ("å", "a"),
    ("é", "e"),
    ("è", "e"),
    ("ê", "e"),
    ("ë", "e"),
    ("à", "a"),
    ("â", "a"),
    ("ù", "u"),
    ("û", "u"),
    ("ô", "o"),
    ("î", "i"),
    ("ï", "i"),
    ("ç", "c"),
    ("ñ", "n"),
];

// =============================================================================
// Document Metadata
// =============================================================================

#[derive(Debug, Clone)]
pub struct DocumentMetadata {
    pub fields: HashMap<String, Value>,
    pub date: Option<NaiveDate>,
    pub context: Option<String>,
    pub new_clues: HashMap<String, (String, String)>,
}

impl DocumentMetadata {
    pub fn new(fields: HashMap<String, Value>, date: Option<NaiveDate>, context: Option<String>) -> Self {
        Self {
            fields,
            date,
            context,
            new_clues: HashMap::new(),
        }
    }

    pub fn get_field_str(&self, name: &str) -> Option<&str> {
        self.fields.get(name).and_then(|v| v.as_str())
    }

    pub fn doc_type(&self) -> Option<&str> {
        self.get_field_str("type")
    }
    pub fn sender(&self) -> Option<&str> {
        self.get_field_str("sender")
    }
    pub fn topic(&self) -> Option<&str> {
        self.get_field_str("topic")
    }
    pub fn subject(&self) -> Option<&str> {
        self.get_field_str("subject")
    }
    pub fn keywords(&self) -> Vec<String> {
        match self.fields.get("keywords") {
            Some(Value::Array(arr)) => arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect(),
            _ => vec![],
        }
    }

    pub fn to_filename(&self, pattern: &str, source_filename: Option<&str>) -> String {
        let mut replacements: HashMap<String, String> = HashMap::new();

        // source_filename
        if let Some(sf) = source_filename {
            let stem = Path::new(sf).file_stem().and_then(|s| s.to_str()).unwrap_or("");
            replacements.insert("source_filename".to_string(), stem.to_string());
        } else {
            replacements.insert("source_filename".to_string(), String::new());
        }

        // context
        replacements.insert(
            "context".to_string(),
            self.context.clone().unwrap_or_default(),
        );

        // date
        if let Some(d) = self.date {
            replacements.insert("date".to_string(), d.format("%Y-%m-%d").to_string());
        } else {
            replacements.insert("date".to_string(), "0000-00-00".to_string());
        }

        // all other fields
        for (field_name, value) in &self.fields {
            let s = match value {
                Value::Array(arr) => arr
                    .iter()
                    .filter_map(|v| v.as_str())
                    .collect::<Vec<_>>()
                    .join("_"),
                Value::String(s) => s.clone(),
                Value::Null => String::new(),
                other => other.to_string(),
            };
            replacements.insert(field_name.clone(), s);
        }

        // Apply pattern
        let mut result = pattern.to_string();
        for (field_name, value) in &replacements {
            let placeholder = format!("{{{}}}", field_name);
            if result.contains(&placeholder) {
                let sanitized = if field_name == "date" {
                    value.clone()
                } else if !value.is_empty() {
                    sanitize(value)
                } else {
                    String::new()
                };
                result = result.replace(&placeholder, &sanitized);
            }
        }

        // Clean up
        let re_placeholder = Regex::new(r"\{[^}]+\}").unwrap();
        result = re_placeholder.replace_all(&result, "").to_string();
        let re_multi_sep = Regex::new(r"[-_]{2,}").unwrap();
        result = re_multi_sep.replace_all(&result, "-").to_string();
        result = result.trim_matches(|c| c == '-' || c == '_').to_string();

        if result.is_empty() {
            return "document.pdf".to_string();
        }

        format!("{}.pdf", result.to_lowercase())
    }
}

pub fn sanitize(s: &str) -> String {
    if s.is_empty() {
        return String::new();
    }

    let mut result = s.to_string();

    // Replace known umlauts and special chars
    for (from, to) in CHAR_REPLACEMENTS {
        result = result.replace(from, to);
    }

    // Normalize unicode and remove remaining diacritics
    result = result.nfkd().collect::<String>();
    result = result.chars().filter(|c| c.is_ascii()).collect();

    // Replace whitespace with underscore
    let re_ws = Regex::new(r"\s+").unwrap();
    result = re_ws.replace_all(&result, "_").to_string();

    // Replace hyphens with underscores
    result = result.replace('-', "_");

    // Replace problematic filename characters
    let re_bad = Regex::new(r#"[<>:"/\\|?*\x00-\x1f]"#).unwrap();
    result = re_bad.replace_all(&result, "_").to_string();

    // Collapse multiple underscores
    let re_multi = Regex::new(r"_+").unwrap();
    result = re_multi.replace_all(&result, "_").to_string();

    // Remove leading/trailing underscores
    result = result.trim_matches('_').to_string();

    // Truncate
    if result.len() > 50 {
        result = result[..50].trim_end_matches('_').to_string();
    }

    result
}

pub fn resolve_filename_pattern(raw: &Value, source_filename: Option<&str>) -> String {
    match raw {
        Value::String(s) => s.clone(),
        Value::Array(arr) => {
            let mut default = None;
            for entry in arr {
                if let Some(obj) = entry.as_object() {
                    if let Some(pattern) = obj.get("pattern").and_then(|v| v.as_str()) {
                        if let Some(match_re) = obj.get("match").and_then(|v| v.as_str()) {
                            if let Some(fname) = source_filename {
                                if let Ok(re) = Regex::new(match_re) {
                                    if re.is_match(fname) {
                                        return pattern.to_string();
                                    }
                                }
                            }
                        } else {
                            default = Some(pattern.to_string());
                        }
                    }
                }
            }
            default.unwrap_or_else(|| format!("{:?}", raw))
        }
        other => other.to_string(),
    }
}

// =============================================================================
// Errors
// =============================================================================

#[derive(Debug, thiserror::Error)]
pub enum AiError {
    #[error("AI processing failed: {0}")]
    Failed(String),
    #[error("Unprocessable input: {0}")]
    UnprocessableInput(String),
    #[error("Configuration error: {0}")]
    Configuration(String),
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
}

// =============================================================================
// AI Client
// =============================================================================

pub struct AiClient {
    client: reqwest::Client,
    api_key: String,
    base_url: String,
    pub config: AppConfig,
}

impl AiClient {
    pub fn new(api_key: &str, model_override: Option<&str>, config: AppConfig) -> Result<Self, AiError> {
        let base_url = std::env::var("ANTHROPIC_BASE_URL")
            .unwrap_or_else(|_| "https://api.anthropic.com".to_string());

        let mut config = config;

        // Override models if specified
        if let Some(model_str) = model_override {
            config.models = model_str
                .split(',')
                .map(|m| ModelConfigYaml {
                    name: m.trim().to_string(),
                    max_tokens: 1024,
                    extended_thinking: false,
                    thinking_budget: 10000,
                })
                .collect();
        }

        if config.models.is_empty() {
            return Err(AiError::Configuration(
                "At least one model must be configured".to_string(),
            ));
        }

        info!(
            "AiClient initialized with {} model(s): {:?}",
            config.models.len(),
            config.models.iter().map(|m| &m.name).collect::<Vec<_>>()
        );

        Ok(Self {
            client: reqwest::Client::new(),
            api_key: api_key.to_string(),
            base_url,
            config,
        })
    }

    // ---- Helper methods ----

    fn get_candidate_names(candidates: &[Value]) -> Vec<String> {
        candidates
            .iter()
            .filter_map(|c| match c {
                Value::String(s) => Some(s.clone()),
                Value::Object(obj) => obj.get("name").and_then(|v| v.as_str()).map(|s| s.to_string()),
                _ => None,
            })
            .collect()
    }

    fn format_candidates_with_clues(candidates: &[Value]) -> String {
        candidates
            .iter()
            .filter_map(|c| match c {
                Value::String(s) => Some(format!("- {}", s)),
                Value::Object(obj) => {
                    let name = obj.get("name").and_then(|v| v.as_str())?;
                    let clues = obj
                        .get("clues")
                        .and_then(|v| v.as_array())
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|v| v.as_str())
                                .collect::<Vec<_>>()
                                .join("; ")
                        });
                    if let Some(clues_text) = clues.filter(|s| !s.is_empty()) {
                        Some(format!("- {}: {}", name, clues_text))
                    } else {
                        Some(format!("- {}", name))
                    }
                }
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn has_any_clues(candidates: &[Value]) -> bool {
        candidates.iter().any(|c| {
            c.as_object()
                .and_then(|obj| obj.get("clues"))
                .and_then(|v| v.as_array())
                .map(|arr| !arr.is_empty())
                .unwrap_or(false)
        })
    }

    fn get_candidates_allowing_new_clues(candidates: &[Value]) -> Vec<String> {
        candidates
            .iter()
            .filter_map(|c| {
                let obj = c.as_object()?;
                if obj
                    .get("allow_new_clues")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    obj.get("name").and_then(|v| v.as_str()).map(|s| s.to_string())
                } else {
                    None
                }
            })
            .collect()
    }

    fn build_name_to_short_mapping(candidates: &[Value]) -> HashMap<String, String> {
        let mut mapping = HashMap::new();
        for c in candidates {
            if let Some(obj) = c.as_object() {
                if let (Some(name), Some(short)) = (
                    obj.get("name").and_then(|v| v.as_str()),
                    obj.get("short").and_then(|v| v.as_str()),
                ) {
                    mapping.insert(name.to_string(), short.to_string());
                }
            }
        }
        mapping
    }

    fn build_field_description(&self, field_name: &str, field_config: &Value) -> Result<String, AiError> {
        let obj = field_config.as_object().ok_or_else(|| {
            AiError::Configuration(format!("Field '{}' config is not an object", field_name))
        })?;

        let instructions = obj
            .get("instructions")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                AiError::Configuration(format!(
                    "Field '{}' is missing required 'instructions'",
                    field_name
                ))
            })?;

        let candidates = obj.get("candidates");
        let has_candidates = candidates.is_some() && !candidates.unwrap().is_null();
        let candidates_arr = candidates
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let allow_new = obj
            .get("allow_new_candidates")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let blacklist: Vec<String> = obj
            .get("blacklist")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        // Validate
        if has_candidates && candidates_arr.is_empty() && !allow_new {
            return Err(AiError::Configuration(format!(
                "Field '{}' has candidates: [] but allow_new_candidates: false",
                field_name
            )));
        }

        let mut parts = vec![instructions.trim().to_string()];

        if has_candidates && !candidates_arr.is_empty() {
            if allow_new {
                parts.push(self.config.base_instruction_flexible.trim().to_string());
            } else {
                parts.push(self.config.base_instruction_strict.trim().to_string());
            }

            if Self::has_any_clues(&candidates_arr) {
                parts.push("Available values:".to_string());
                parts.push(Self::format_candidates_with_clues(&candidates_arr));
            } else {
                let names = Self::get_candidate_names(&candidates_arr);
                parts.push(format!("Available values: {}", names.join(", ")));
            }
        } else if has_candidates {
            parts.push("No predefined values. You may create an appropriate value.".to_string());
        }

        if !blacklist.is_empty() {
            let bl_text = self
                .config
                .blacklist_instruction
                .replace("{blacklist}", &blacklist.join(", "));
            parts.push(bl_text.trim().to_string());
        }

        Ok(parts.join("\n"))
    }

    fn build_field_schema(&self, field_name: &str, field_config: &Value) -> Result<Value, AiError> {
        let empty_map = serde_json::Map::new();
        let obj = field_config.as_object().unwrap_or(&empty_map);
        let candidates = obj.get("candidates");
        let candidates_arr = candidates
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let allow_new = obj
            .get("allow_new_candidates")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let description = self.build_field_description(field_name, field_config)?;

        if !candidates_arr.is_empty() && !allow_new {
            let names = Self::get_candidate_names(&candidates_arr);
            Ok(json!({
                "type": "string",
                "enum": names,
                "description": description,
            }))
        } else {
            Ok(json!({
                "type": "string",
                "description": description,
            }))
        }
    }

    fn build_extraction_tool(&self, field_configs: &HashMap<String, Value>) -> Result<Value, AiError> {
        let mut properties = serde_json::Map::new();
        properties.insert(
            "date".to_string(),
            json!({
                "type": "string",
                "description": "Document date in YYYY-MM-DD format, or null if not found.",
            }),
        );

        let mut required_fields = Vec::new();

        for (field_name, field_config) in field_configs {
            if field_name == "date" {
                warn!("Field 'date' should not be defined in fields");
                continue;
            }

            properties.insert(field_name.clone(), self.build_field_schema(field_name, field_config)?);
            required_fields.push(Value::String(field_name.clone()));

            // New clue field
            let obj = field_config.as_object();
            if let Some(candidates) = obj.and_then(|o| o.get("candidates")).and_then(|v| v.as_array()) {
                let allowing = Self::get_candidates_allowing_new_clues(candidates);
                if !allowing.is_empty() {
                    let desc = self
                        .config
                        .new_clue_instruction
                        .replace("{field_name}", field_name)
                        .replace("{candidates}", &allowing.join(", "));
                    properties.insert(
                        format!("{}_new_clue", field_name),
                        json!({
                            "type": "string",
                            "description": desc.trim(),
                        }),
                    );
                }
            }
        }

        Ok(json!({
            "name": "extract_metadata",
            "description": "Extract metadata from the document.",
            "input_schema": {
                "type": "object",
                "properties": properties,
                "required": required_fields,
            },
        }))
    }

    fn build_context_description(&self, ctx: &Value, include_all_candidates: bool) -> String {
        let name = ctx.get("name").and_then(|v| v.as_str()).unwrap_or("");
        let desc = ctx.get("description").and_then(|v| v.as_str()).unwrap_or("");
        let mut parts = vec![format!("- {}: {}", name, desc)];

        if let Some(fields) = ctx.get("fields").and_then(|v| v.as_object()) {
            if include_all_candidates {
                let mut keywords = Vec::new();
                for (_fname, fconfig) in fields {
                    if let Some(candidates) = fconfig.get("candidates").and_then(|v| v.as_array()) {
                        for c in candidates {
                            match c {
                                Value::String(s) => keywords.push(s.clone()),
                                Value::Object(obj) => {
                                    if let Some(name) = obj.get("name").and_then(|v| v.as_str()) {
                                        keywords.push(name.to_string());
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
                if !keywords.is_empty() {
                    parts.push(format!("  Keywords: {}", keywords.join(", ")));
                }
            } else {
                for (fname, fconfig) in fields {
                    let include = fconfig
                        .get("include_in_context_determination")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);
                    if !include {
                        continue;
                    }
                    if let Some(candidates) = fconfig.get("candidates").and_then(|v| v.as_array()) {
                        if candidates.is_empty() {
                            continue;
                        }
                        let mut field_parts = vec![format!("  {} values:", fname)];
                        for c in candidates {
                            match c {
                                Value::String(s) => field_parts.push(format!("    - {}", s)),
                                Value::Object(obj) => {
                                    if let Some(name) = obj.get("name").and_then(|v| v.as_str()) {
                                        let clues = obj
                                            .get("clues")
                                            .and_then(|v| v.as_array())
                                            .map(|arr| {
                                                arr.iter()
                                                    .filter_map(|v| v.as_str())
                                                    .collect::<Vec<_>>()
                                                    .join("; ")
                                            });
                                        if let Some(ct) = clues.filter(|s| !s.is_empty()) {
                                            field_parts.push(format!("    - {}: {}", name, ct));
                                        } else {
                                            field_parts.push(format!("    - {}", name));
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                        if field_parts.len() > 1 {
                            parts.extend(field_parts);
                        }
                    }
                }
            }
        }

        parts.join("\n")
    }

    fn build_context_tool(&self, contexts: &[Value], include_all_candidates: bool) -> Value {
        let context_names: Vec<Value> = contexts
            .iter()
            .filter_map(|ctx| ctx.get("name").and_then(|v| v.as_str()).map(|s| Value::String(s.to_string())))
            .collect();

        let context_descriptions: Vec<String> = contexts
            .iter()
            .map(|ctx| self.build_context_description(ctx, include_all_candidates))
            .collect();

        let desc = format!(
            "{}\n\nAvailable contexts:\n{}",
            self.config.context_tool_description,
            context_descriptions.join("\n")
        );

        json!({
            "name": "classify_context",
            "description": desc,
            "input_schema": {
                "type": "object",
                "properties": {
                    "context": {
                        "type": "string",
                        "enum": context_names,
                        "description": "The context this document belongs to.",
                    },
                },
                "required": ["context"],
            },
        })
    }

    fn apply_name_to_short(value: Option<&str>, field_config: &Value) -> Option<String> {
        let val = value?;
        let candidates = field_config
            .get("candidates")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let mapping = Self::build_name_to_short_mapping(&candidates);
        Some(mapping.get(val).cloned().unwrap_or_else(|| val.to_string()))
    }

    // ---- API call methods ----

    async fn call_api(&self, model: &ModelConfigYaml, params: Value) -> Result<Value, AiError> {
        let url = format!("{}/v1/messages", self.base_url);

        let response = self
            .client
            .post(&url)
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .json(&params)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(AiError::Failed(format!(
                "API error {} from model {}: {}",
                status, model.name, body
            )));
        }

        let data: Value = response.json().await?;
        Ok(data)
    }

    async fn call_api_streaming(&self, model: &ModelConfigYaml, mut params: Value) -> Result<Value, AiError> {
        params["stream"] = json!(true);
        let url = format!("{}/v1/messages", self.base_url);

        let response = self
            .client
            .post(&url)
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .json(&params)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(AiError::Failed(format!(
                "Streaming API error {} from model {}: {}",
                status, model.name, body
            )));
        }

        // Parse SSE stream to reconstruct final message
        let body = response.text().await?;
        parse_sse_response(&body)
    }

    pub async fn determine_context(
        &self,
        text: &str,
        contexts: &[Value],
        filename: Option<&str>,
        user_dir: Option<&Path>,
        include_all_candidates: bool,
    ) -> Result<String, AiError> {
        if text.trim().is_empty() {
            return Err(AiError::Failed(
                "Cannot determine context from empty text".to_string(),
            ));
        }
        if contexts.is_empty() {
            return Err(AiError::Configuration(
                "No contexts provided".to_string(),
            ));
        }

        let max_chars = self.config.extraction.max_input_chars;
        let truncated = if text.len() > max_chars {
            format!("{}\n[... truncated ...]", &text[..max_chars])
        } else {
            text.to_string()
        };

        let context_tool = self.build_context_tool(contexts, include_all_candidates);

        let user_content = if let Some(fname) = filename {
            format!(
                "Original filename: {}\n\nClassify this document:\n\n{}",
                fname, truncated
            )
        } else {
            format!("Classify this document:\n\n{}", truncated)
        };

        let mut last_error = None;

        for (idx, model) in self.config.models.iter().enumerate() {
            let is_last = idx == self.config.models.len() - 1;

            let max_tokens = if model.extended_thinking {
                model.max_tokens + model.thinking_budget
            } else {
                model.max_tokens
            };

            let mut params = json!({
                "model": model.name,
                "max_tokens": max_tokens,
                "system": self.config.context_system_prompt,
                "tools": [context_tool],
                "messages": [{"role": "user", "content": user_content}],
            });

            if model.extended_thinking {
                params["tool_choice"] = json!({"type": "auto"});
                params["thinking"] = json!({
                    "type": "enabled",
                    "budget_tokens": model.thinking_budget,
                });
            } else {
                params["tool_choice"] = json!({"type": "tool", "name": "classify_context"});
            }

            let result = if model.extended_thinking {
                self.call_api_streaming(model, params).await
            } else {
                self.call_api(model, params).await
            };

            match result {
                Ok(message) => {
                    // Track costs
                    if let Some(ud) = user_dir {
                        if let Some(usage) = message.get("usage") {
                            let input = usage.get("input_tokens").and_then(|v| v.as_u64()).unwrap_or(0);
                            let output = usage.get("output_tokens").and_then(|v| v.as_u64()).unwrap_or(0);
                            get_cost_tracker().record_anthropic(
                                &model.name,
                                input,
                                output,
                                ud,
                                true,
                            );
                        }
                    }

                    // Extract context from tool_use
                    if let Some(content) = message.get("content").and_then(|v| v.as_array()) {
                        for block in content {
                            if block.get("type").and_then(|v| v.as_str()) == Some("tool_use")
                                && block.get("name").and_then(|v| v.as_str()) == Some("classify_context")
                            {
                                if let Some(ctx) = block
                                    .get("input")
                                    .and_then(|v| v.get("context"))
                                    .and_then(|v| v.as_str())
                                {
                                    info!("Determined context: {} (model: {})", ctx, model.name);
                                    return Ok(ctx.to_string());
                                }
                            }
                        }
                    }

                    if is_last {
                        return Err(AiError::UnprocessableInput(
                            "All models failed to determine context".to_string(),
                        ));
                    }
                    warn!(
                        "Model {} returned null context, trying next",
                        model.name
                    );
                    last_error = Some(AiError::UnprocessableInput(format!(
                        "Model {} returned null context",
                        model.name
                    )));
                }
                Err(e) => {
                    warn!("Model {} failed: {}, trying next", model.name, e);
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| AiError::Failed("All models failed".to_string())))
    }

    pub async fn extract_metadata(
        &self,
        text: &str,
        contexts: &[Value],
        primary_language: Option<&str>,
        filename: Option<&str>,
        user_dir: Option<&Path>,
        locked_fields: Option<&HashMap<String, Value>>,
        is_audio: bool,
    ) -> Result<(DocumentMetadata, String), AiError> {
        let min_len = 10;
        let stripped = text.trim();
        if stripped.is_empty() || stripped.len() < min_len {
            return Err(AiError::Failed(format!(
                "Insufficient text for metadata extraction ({} chars, minimum {})",
                stripped.len(),
                min_len
            )));
        }

        let max_chars = self.config.extraction.max_input_chars;
        let truncated = if text.len() > max_chars {
            format!("{}\n[... truncated ...]", &text[..max_chars])
        } else {
            text.to_string()
        };

        // Determine context
        let context_name = if contexts.len() == 1 {
            contexts[0]
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string()
        } else {
            self.determine_context(&truncated, contexts, filename, user_dir, false)
                .await?
        };

        // Find context config
        let context_config = contexts
            .iter()
            .find(|ctx| ctx.get("name").and_then(|v| v.as_str()) == Some(&context_name))
            .ok_or_else(|| {
                AiError::Failed(format!("Context '{}' not found", context_name))
            })?;

        // Get field configs
        let field_configs: HashMap<String, Value> = context_config
            .get("fields")
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            })
            .unwrap_or_default();

        // Get filename pattern
        let raw_pattern = if is_audio {
            context_config
                .get("audio_filename")
                .or_else(|| context_config.get("filename"))
        } else {
            context_config.get("filename")
        };
        let raw_pattern = raw_pattern.ok_or_else(|| {
            AiError::Configuration(format!(
                "Context '{}' is missing required 'filename' pattern",
                context_name
            ))
        })?;
        let filename_pattern = resolve_filename_pattern(raw_pattern, filename);

        // Extract metadata
        let metadata = self
            .extract_metadata_with_config(
                &truncated,
                &field_configs,
                &context_name,
                primary_language,
                filename,
                user_dir,
                locked_fields,
            )
            .await?;

        Ok((metadata, filename_pattern))
    }

    pub async fn extract_metadata_with_config(
        &self,
        text: &str,
        field_configs: &HashMap<String, Value>,
        context_name: &str,
        primary_language: Option<&str>,
        filename: Option<&str>,
        user_dir: Option<&Path>,
        locked_fields: Option<&HashMap<String, Value>>,
    ) -> Result<DocumentMetadata, AiError> {
        // Filter out locked fields
        let unlocked: HashMap<String, Value> = if let Some(locked) = locked_fields {
            field_configs
                .iter()
                .filter(|(k, _)| !locked.contains_key(*k))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        } else {
            field_configs.clone()
        };

        let extraction_tool = self.build_extraction_tool(&unlocked)?;

        // Build system prompt
        let lang_instruction = if let Some(lang) = primary_language {
            self.config
                .language_instruction
                .replace("{language}", lang)
        } else {
            self.config.default_language_instruction.clone()
        };
        let system_prompt = self
            .config
            .extraction_system_prompt
            .replace("{language_instruction}", &lang_instruction);

        // Build user message
        let mut user_parts = Vec::new();
        if let Some(fname) = filename {
            user_parts.push(format!("Original filename: {}", fname));
        }
        if let Some(locked) = locked_fields {
            if !locked.is_empty() {
                let mut locked_info = vec![
                    "The following fields have been pre-determined and are fixed:".to_string(),
                ];
                for (fname, finfo) in locked {
                    let value = finfo.get("value").and_then(|v| v.as_str()).unwrap_or("");
                    locked_info.push(format!("  - {}: {}", fname, value));
                    if let Some(clues) = finfo.get("clues").and_then(|v| v.as_array()) {
                        let clue_strs: Vec<&str> = clues.iter().filter_map(|v| v.as_str()).collect();
                        if !clue_strs.is_empty() {
                            locked_info.push(format!("    Context clues: {}", clue_strs.join(", ")));
                        }
                    }
                }
                locked_info.push(String::new());
                locked_info.push("Use this context when extracting other fields.".to_string());
                user_parts.push(locked_info.join("\n"));
            }
        }
        user_parts.push(format!("Extract metadata from this document:\n\n{}", text));
        let user_content = user_parts.join("\n\n");

        debug!(
            "Extracting metadata (context: {}, fields: {:?})",
            context_name,
            unlocked.keys().collect::<Vec<_>>()
        );

        let mut last_error = None;
        let mut last_metadata = None;

        for (idx, model) in self.config.models.iter().enumerate() {
            let is_last = idx == self.config.models.len() - 1;

            let max_tokens = if model.extended_thinking {
                model.max_tokens + model.thinking_budget
            } else {
                model.max_tokens
            };

            let mut params = json!({
                "model": model.name,
                "max_tokens": max_tokens,
                "system": system_prompt,
                "tools": [extraction_tool],
                "messages": [{"role": "user", "content": user_content}],
            });

            if model.extended_thinking {
                params["tool_choice"] = json!({"type": "auto"});
                params["thinking"] = json!({
                    "type": "enabled",
                    "budget_tokens": model.thinking_budget,
                });
            } else {
                params["tool_choice"] = json!({"type": "tool", "name": "extract_metadata"});
            }

            let result = if model.extended_thinking {
                self.call_api_streaming(model, params).await
            } else {
                self.call_api(model, params).await
            };

            match result {
                Ok(message) => {
                    // Track costs
                    if let Some(ud) = user_dir {
                        if let Some(usage) = message.get("usage") {
                            let input = usage.get("input_tokens").and_then(|v| v.as_u64()).unwrap_or(0);
                            let output = usage.get("output_tokens").and_then(|v| v.as_u64()).unwrap_or(0);
                            get_cost_tracker().record_anthropic(
                                &model.name,
                                input,
                                output,
                                ud,
                                false,
                            );
                        }
                    }

                    // Extract tool result
                    let mut tool_result = None;
                    if let Some(content) = message.get("content").and_then(|v| v.as_array()) {
                        for block in content {
                            if block.get("type").and_then(|v| v.as_str()) == Some("tool_use")
                                && block.get("name").and_then(|v| v.as_str()) == Some("extract_metadata")
                            {
                                tool_result = block.get("input").cloned();
                                break;
                            }
                        }
                    }

                    let Some(result) = tool_result else {
                        if is_last {
                            if let Some(meta) = last_metadata {
                                return Ok(meta);
                            }
                        }
                        warn!("Model {} returned no metadata", model.name);
                        last_error = Some(AiError::Failed(format!(
                            "Model {} returned no metadata",
                            model.name
                        )));
                        continue;
                    };

                    let metadata = self.parse_tool_result(
                        &result,
                        &unlocked,
                        locked_fields,
                        context_name,
                    );
                    last_metadata = Some(metadata.clone());

                    if metadata.date.is_none() {
                        if is_last {
                            return Ok(metadata);
                        }
                        warn!("Model {} returned null date, trying next", model.name);
                        last_error = Some(AiError::Failed(format!(
                            "Model {} returned null date",
                            model.name
                        )));
                        continue;
                    }

                    debug!("Metadata extracted successfully (model: {})", model.name);
                    return Ok(metadata);
                }
                Err(e) => {
                    warn!("Model {} failed: {}", model.name, e);
                    last_error = Some(e);
                }
            }
        }

        if let Some(meta) = last_metadata {
            warn!("All models failed to extract complete metadata, using last result");
            return Ok(meta);
        }

        Err(last_error.unwrap_or_else(|| AiError::Failed("All models failed".to_string())))
    }

    fn parse_tool_result(
        &self,
        result: &Value,
        unlocked_field_configs: &HashMap<String, Value>,
        locked_fields: Option<&HashMap<String, Value>>,
        context_name: &str,
    ) -> DocumentMetadata {
        // Parse date
        let doc_date = result
            .get("date")
            .and_then(|v| v.as_str())
            .and_then(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").ok());

        let mut fields = HashMap::new();
        let mut new_clues = HashMap::new();

        for (field_name, field_config) in unlocked_field_configs {
            let raw_value = result.get(field_name).and_then(|v| v.as_str());
            let mapped = Self::apply_name_to_short(raw_value, field_config);
            if let Some(val) = mapped {
                fields.insert(field_name.clone(), Value::String(val));
            } else {
                fields.insert(field_name.clone(), Value::Null);
            }

            // Check for new clues
            let new_clue_key = format!("{}_new_clue", field_name);
            if let Some(clue) = result.get(&new_clue_key).and_then(|v| v.as_str()) {
                if !clue.trim().is_empty() {
                    if let Some(raw_val) = raw_value {
                        let candidates = field_config
                            .get("candidates")
                            .and_then(|v| v.as_array())
                            .cloned()
                            .unwrap_or_default();
                        let allowing = Self::get_candidates_allowing_new_clues(&candidates);
                        if allowing.contains(&raw_val.to_string()) {
                            new_clues.insert(
                                field_name.clone(),
                                (raw_val.to_string(), clue.trim().to_string()),
                            );
                            info!(
                                "New clue suggested for {} '{}': {}",
                                field_name,
                                raw_val,
                                clue.trim()
                            );
                        }
                    }
                }
            }
        }

        // Add locked fields
        if let Some(locked) = locked_fields {
            for (fname, finfo) in locked {
                let value = finfo
                    .get("value")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                fields.insert(fname.clone(), Value::String(value));
            }
        }

        let field_summary: Vec<String> = fields
            .iter()
            .filter(|(_, v)| !v.is_null())
            .map(|(k, v)| {
                let s = v.as_str().unwrap_or("");
                if s.len() > 30 {
                    format!("{}={}...", k, &s[..30])
                } else {
                    format!("{}={}", k, s)
                }
            })
            .collect();
        info!(
            "Metadata extracted: context={}, date={:?}, {}",
            context_name,
            doc_date,
            field_summary.join(", ")
        );

        DocumentMetadata {
            fields,
            date: doc_date,
            context: Some(context_name.to_string()),
            new_clues,
        }
    }
}

// =============================================================================
// SSE Response Parser
// =============================================================================

fn parse_sse_response(body: &str) -> Result<Value, AiError> {
    let mut message_id = String::new();
    let mut model = String::new();
    let mut content_blocks: Vec<Value> = Vec::new();
    let mut current_block_type = String::new();
    let mut current_tool_name = String::new();
    let mut current_tool_id = String::new();
    let mut text_accum = String::new();
    let mut json_accum = String::new();
    let mut input_tokens = 0u64;
    let mut output_tokens = 0u64;
    let mut stop_reason = String::new();

    for line in body.lines() {
        if !line.starts_with("data: ") {
            continue;
        }
        let data = &line[6..];
        let event: Value = match serde_json::from_str(data) {
            Ok(v) => v,
            Err(_) => continue,
        };

        let event_type = event.get("type").and_then(|v| v.as_str()).unwrap_or("");

        match event_type {
            "message_start" => {
                if let Some(msg) = event.get("message") {
                    message_id = msg.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                    model = msg.get("model").and_then(|v| v.as_str()).unwrap_or("").to_string();
                    if let Some(usage) = msg.get("usage") {
                        input_tokens = usage.get("input_tokens").and_then(|v| v.as_u64()).unwrap_or(0);
                    }
                }
            }
            "content_block_start" => {
                if let Some(cb) = event.get("content_block") {
                    current_block_type = cb.get("type").and_then(|v| v.as_str()).unwrap_or("").to_string();
                    if current_block_type == "tool_use" {
                        current_tool_name = cb.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        current_tool_id = cb.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        json_accum.clear();
                    } else {
                        text_accum.clear();
                    }
                }
            }
            "content_block_delta" => {
                if let Some(delta) = event.get("delta") {
                    let delta_type = delta.get("type").and_then(|v| v.as_str()).unwrap_or("");
                    if delta_type == "text_delta" {
                        if let Some(t) = delta.get("text").and_then(|v| v.as_str()) {
                            text_accum.push_str(t);
                        }
                    } else if delta_type == "input_json_delta" {
                        if let Some(j) = delta.get("partial_json").and_then(|v| v.as_str()) {
                            json_accum.push_str(j);
                        }
                    }
                }
            }
            "content_block_stop" => {
                if current_block_type == "tool_use" {
                    let input: Value = serde_json::from_str(&json_accum).unwrap_or(Value::Object(serde_json::Map::new()));
                    content_blocks.push(json!({
                        "type": "tool_use",
                        "id": current_tool_id,
                        "name": current_tool_name,
                        "input": input,
                    }));
                } else if current_block_type == "text" {
                    content_blocks.push(json!({
                        "type": "text",
                        "text": text_accum,
                    }));
                }
                current_block_type.clear();
            }
            "message_delta" => {
                if let Some(delta) = event.get("delta") {
                    if let Some(sr) = delta.get("stop_reason").and_then(|v| v.as_str()) {
                        stop_reason = sr.to_string();
                    }
                }
                if let Some(usage) = event.get("usage") {
                    output_tokens = usage.get("output_tokens").and_then(|v| v.as_u64()).unwrap_or(0);
                }
            }
            _ => {}
        }
    }

    Ok(json!({
        "id": message_id,
        "type": "message",
        "role": "assistant",
        "content": content_blocks,
        "model": model,
        "stop_reason": stop_reason,
        "usage": {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_filename_m4a_source_filename_pattern() {
        let mut fields = HashMap::new();
        fields.insert("type".to_string(), Value::String("Notiz".to_string()));
        fields.insert("sender".to_string(), Value::String("Dr. Braun".to_string()));

        let meta = DocumentMetadata {
            fields,
            date: Some(NaiveDate::from_ymd_opt(2025, 8, 3).unwrap()),
            context: Some("privat".to_string()),
            new_clues: HashMap::new(),
        };

        let result = meta.to_filename(
            "{context}-{source_filename}-{date}",
            Some("privatnotiz.m4a"),
        );
        assert_eq!(result, "privat-privatnotiz-2025-08-03.pdf");
    }

    #[test]
    fn test_to_filename_source_filename_stem_only() {
        let meta = DocumentMetadata {
            fields: HashMap::new(),
            date: Some(NaiveDate::from_ymd_opt(2025, 1, 15).unwrap()),
            context: Some("work".to_string()),
            new_clues: HashMap::new(),
        };

        let result = meta.to_filename(
            "{context}-{source_filename}",
            Some("recording.wav"),
        );
        assert_eq!(result, "work-recording.pdf");
    }

    /// Regression test: the watcher used to strip the extension before
    /// sending the filename to the service.  For dotted stems like
    /// "my.recording.m4a" the watcher sent "my.recording", then
    /// `to_filename` called `file_stem("my.recording")` → "my", losing
    /// the ".recording" part.
    ///
    /// After the fix the watcher sends the full filename (with extension),
    /// so `file_stem` strips exactly once → "my.recording".
    #[test]
    fn test_to_filename_source_filename_dotted_stem() {
        let meta = DocumentMetadata {
            fields: HashMap::new(),
            date: Some(NaiveDate::from_ymd_opt(2025, 8, 3).unwrap()),
            context: Some("privat".to_string()),
            new_clues: HashMap::new(),
        };

        // After fix: watcher sends full filename with extension
        let result = meta.to_filename(
            "{context}-{source_filename}-{date}",
            Some("my.recording.m4a"),
        );
        assert_eq!(result, "privat-my.recording-2025-08-03.pdf");
    }

    #[test]
    fn test_to_filename_without_source_filename() {
        let meta = DocumentMetadata {
            fields: HashMap::new(),
            date: Some(NaiveDate::from_ymd_opt(2025, 1, 15).unwrap()),
            context: Some("work".to_string()),
            new_clues: HashMap::new(),
        };

        // source_filename placeholder resolves to empty → gets cleaned up
        let result = meta.to_filename(
            "{context}-{source_filename}-{date}",
            None,
        );
        assert_eq!(result, "work-2025-01-15.pdf");
    }

    #[test]
    fn test_ai_error_variants_display() {
        let failed = AiError::Failed("API error 500".to_string());
        assert!(failed.to_string().contains("AI processing failed"));

        let unprocessable = AiError::UnprocessableInput("cannot classify".to_string());
        assert!(unprocessable.to_string().contains("Unprocessable input"));

        let config = AiError::Configuration("missing field".to_string());
        assert!(config.to_string().contains("Configuration error"));
    }

    #[test]
    fn test_resolve_filename_pattern_conditional_array() {
        // Simulate JSON from YAML conditional filename array
        let raw = serde_json::json!([
            {
                "match": ".*\\.(mp3|m4a|mp4|mov|wav)",
                "pattern": "{context}-{source_filename}-{date}"
            },
            {
                "pattern": "{context}-{type}-{date}-{sender}"
            }
        ]);

        // Audio file should match the first rule
        let result = resolve_filename_pattern(&raw, Some("condpattern-audio.m4a"));
        assert_eq!(result, "{context}-{source_filename}-{date}",
            "Audio file should match the conditional regex");

        // PDF should fall through to default
        let result = resolve_filename_pattern(&raw, Some("document.pdf"));
        assert_eq!(result, "{context}-{type}-{date}-{sender}");

        // No filename should use default
        let result = resolve_filename_pattern(&raw, None);
        assert_eq!(result, "{context}-{type}-{date}-{sender}");
    }

    #[test]
    fn test_resolve_filename_pattern_yaml_to_json_roundtrip() {
        // Simulate the exact YAML→JSON conversion from get_context_for_api
        let yaml_str = r#"
filename:
  - match: '.*\.(mp3|m4a|mp4|mov|wav)'
    pattern: '{context}-{source_filename}-{date}'
  - pattern: '{context}-{type}-{date}-{sender}'
"#;
        let yaml_val: serde_yaml::Value = serde_yaml::from_str(yaml_str).unwrap();
        let json_str = serde_json::to_string(&yaml_val).unwrap();
        let json_val: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        let filename_field = json_val.get("filename").unwrap();

        let result = resolve_filename_pattern(filename_field, Some("test.m4a"));
        assert_eq!(result, "{context}-{source_filename}-{date}",
            "Audio file should match after YAML→JSON roundtrip. JSON: {}", json_str);
    }
}
