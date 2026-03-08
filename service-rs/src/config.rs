use serde::Deserialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing::warn;

#[derive(Debug, Clone, Deserialize)]
pub struct ModelConfigYaml {
    pub name: String,
    #[serde(default = "default_max_tokens")]
    pub max_tokens: u32,
    #[serde(default)]
    pub extended_thinking: bool,
    #[serde(default = "default_thinking_budget")]
    pub thinking_budget: u32,
}

fn default_max_tokens() -> u32 {
    1024
}
fn default_thinking_budget() -> u32 {
    10000
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExtractionConfig {
    pub max_input_chars: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TranscriptCorrectionConfig {
    #[serde(default = "default_correction_model")]
    pub model: String,
    #[serde(default = "default_true")]
    pub extended_thinking: bool,
    #[serde(default = "default_correction_budget")]
    pub thinking_budget: u32,
    #[serde(default)]
    pub use_batch: bool,
}

fn default_correction_model() -> String {
    "claude-sonnet-4-20250514".to_string()
}
fn default_true() -> bool {
    true
}
fn default_correction_budget() -> u32 {
    50000
}

impl Default for TranscriptCorrectionConfig {
    fn default() -> Self {
        Self {
            model: default_correction_model(),
            extended_thinking: true,
            thinking_budget: 50000,
            use_batch: true,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub models: Vec<ModelConfigYaml>,
    pub extraction: ExtractionConfig,
    #[serde(default)]
    pub extraction_system_prompt: String,
    #[serde(default)]
    pub base_instruction_strict: String,
    #[serde(default)]
    pub base_instruction_flexible: String,
    #[serde(default)]
    pub blacklist_instruction: String,
    #[serde(default)]
    pub new_clue_instruction: String,
    #[serde(default)]
    pub language_instruction: String,
    #[serde(default)]
    pub default_language_instruction: String,
    #[serde(default)]
    pub context_system_prompt: String,
    #[serde(default)]
    pub context_tool_description: String,
    #[serde(default)]
    pub transcript_correction: TranscriptCorrectionConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ModelPricing {
    pub input_per_1m: f64,
    pub output_per_1m: f64,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct PricingConfig {
    #[serde(default)]
    pub anthropic: HashMap<String, ModelPricing>,
}

pub fn load_config(config_path: Option<&Path>) -> AppConfig {
    let path = config_path
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| default_config_path());

    let content = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("Failed to read config from {:?}: {}", path, e));
    serde_yaml::from_str(&content)
        .unwrap_or_else(|e| panic!("Failed to parse config {:?}: {}", path, e))
}

pub fn load_pricing(pricing_path: Option<&Path>) -> PricingConfig {
    let path = pricing_path
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| default_pricing_path());

    match std::fs::read_to_string(&path) {
        Ok(content) => serde_yaml::from_str(&content).unwrap_or_else(|e| {
            warn!("Failed to parse pricing config {:?}: {}", path, e);
            PricingConfig::default()
        }),
        Err(_) => {
            warn!("Pricing config not found at {:?}, using empty pricing", path);
            PricingConfig::default()
        }
    }
}

fn default_config_path() -> PathBuf {
    // Look in same directory as executable, then fallback to current dir
    let exe_dir = std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|p| p.to_path_buf()));
    if let Some(dir) = exe_dir {
        let p = dir.join("config.yaml");
        if p.exists() {
            return p;
        }
    }
    PathBuf::from("config.yaml")
}

fn default_pricing_path() -> PathBuf {
    let exe_dir = std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|p| p.to_path_buf()));
    if let Some(dir) = exe_dir {
        let p = dir.join("pricing.yaml");
        if p.exists() {
            return p;
        }
    }
    PathBuf::from("pricing.yaml")
}
