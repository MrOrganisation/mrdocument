use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use tracing::{debug, error, info, warn};

use crate::config::{load_pricing, PricingConfig};

struct CostRecord {
    model: String,
    input_tokens: u64,
    output_tokens: u64,
    user_dir: PathBuf,
    count_document: bool,
}

pub struct CostTracker {
    filename: String,
    pricing: PricingConfig,
    queue: Mutex<Vec<CostRecord>>,
    api_log: Mutex<Option<std::fs::File>>,
}

impl CostTracker {
    pub fn new() -> Self {
        let pricing = load_pricing(None);
        let api_log_file = std::env::var("LOG_DIR")
            .ok()
            .map(|dir| {
                let path = PathBuf::from(dir).join("api_calls.log");
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)
                    .map_err(|e| warn!("Failed to open API call log {:?}: {}", path, e))
                    .ok()
            })
            .flatten();
        info!("CostTracker initialized (api_log={})", api_log_file.is_some());
        Self {
            filename: "mrdocument_costs.json".to_string(),
            pricing,
            queue: Mutex::new(Vec::new()),
            api_log: Mutex::new(api_log_file),
        }
    }

    pub fn record_anthropic(
        &self,
        model: &str,
        input_tokens: u64,
        output_tokens: u64,
        user_dir: &Path,
        count_document: bool,
    ) {
        if let Ok(mut q) = self.queue.lock() {
            q.push(CostRecord {
                model: model.to_string(),
                input_tokens,
                output_tokens,
                user_dir: user_dir.to_path_buf(),
                count_document,
            });
        }
        debug!(
            "Recorded Anthropic usage: model={}, in={}, out={}, count_doc={}",
            model, input_tokens, output_tokens, count_document
        );
        self.flush();
    }

    /// Log an API call with truncated request body to the api_calls.log file.
    pub fn log_api_call(
        &self,
        operation: &str,
        model: &str,
        filename: Option<&str>,
        request_body: &serde_json::Value,
        input_tokens: u64,
        output_tokens: u64,
        user_dir: &Path,
    ) {
        let cost = self.calculate_cost(model, input_tokens, output_tokens);

        // Truncate each string field in the request body to 10 chars
        let truncated = truncate_json_fields(request_body, 10);

        let timestamp = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ");
        let fname = filename.unwrap_or("-");
        let username = extract_username(user_dir);

        let line = format!(
            "{} | {} | {} | {} | {} | in={} out={} | cost=${:.6} | body={}\n",
            timestamp,
            username,
            operation,
            model,
            fname,
            input_tokens,
            output_tokens,
            cost,
            truncated,
        );

        if let Ok(mut guard) = self.api_log.lock() {
            if let Some(ref mut file) = *guard {
                let _ = file.write_all(line.as_bytes());
                let _ = file.flush();
            }
        }
    }

    pub fn flush(&self) {
        let records = {
            let mut q = match self.queue.lock() {
                Ok(q) => q,
                Err(_) => return,
            };
            std::mem::take(&mut *q)
        };

        if records.is_empty() {
            return;
        }

        let mut by_user: HashMap<PathBuf, Vec<&CostRecord>> = HashMap::new();
        for record in &records {
            by_user
                .entry(record.user_dir.clone())
                .or_default()
                .push(record);
        }

        let today = chrono::Local::now().format("%Y-%m-%d").to_string();
        for (user_dir, user_records) in by_user {
            if let Err(e) = self.write_user_costs(&user_dir, &today, &user_records) {
                error!("Failed to write costs for {:?}: {}", user_dir, e);
            }
        }
    }

    fn write_user_costs(
        &self,
        user_dir: &Path,
        today: &str,
        records: &[&CostRecord],
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Find username
        let mut username = None;
        let mut current = Some(user_dir);
        while let Some(path) = current {
            if let Some(parent) = path.parent() {
                if parent.file_name().map(|n| n == "sync").unwrap_or(false)
                    || parent == Path::new("/sync")
                {
                    username = path.file_name().and_then(|n| n.to_str()).map(|s| s.to_string());
                    break;
                }
            }
            current = path.parent();
        }
        let username = username.unwrap_or_else(|| {
            user_dir
                .parent()
                .and_then(|p| p.file_name())
                .and_then(|n| n.to_str())
                .unwrap_or("unknown")
                .to_string()
        });

        let costs_dir = PathBuf::from("/costs").join(&username);
        std::fs::create_dir_all(&costs_dir)?;
        let costs_path = costs_dir.join(&self.filename);

        let mut data: Value = if costs_path.exists() {
            let content = std::fs::read_to_string(&costs_path)?;
            serde_json::from_str(&content).unwrap_or(json!({}))
        } else {
            json!({})
        };

        // Ensure today's entry
        if data.get(today).is_none() {
            data[today] = json!({"anthropic": {}});
        }

        // Aggregate
        for record in records {
            let day = &mut data[today];
            if day.get("anthropic").is_none() {
                day["anthropic"] = json!({});
            }
            let model_key = &record.model;
            if day["anthropic"].get(model_key).is_none() {
                day["anthropic"][model_key] = json!({
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "cost": 0.0,
                    "documents": 0,
                });
            }
            let model_data = &mut day["anthropic"][model_key];
            let it = model_data["input_tokens"].as_u64().unwrap_or(0) + record.input_tokens;
            let ot = model_data["output_tokens"].as_u64().unwrap_or(0) + record.output_tokens;
            model_data["input_tokens"] = json!(it);
            model_data["output_tokens"] = json!(ot);
            if record.count_document {
                let docs = model_data["documents"].as_u64().unwrap_or(0) + 1;
                model_data["documents"] = json!(docs);
            }
            let cost = self.calculate_cost(&record.model, record.input_tokens, record.output_tokens);
            let prev_cost = model_data["cost"].as_f64().unwrap_or(0.0);
            model_data["cost"] = json!(((prev_cost + cost) * 1_000_000.0).round() / 1_000_000.0);
        }

        // Recalculate totals
        let mut total_anthropic: HashMap<String, Value> = HashMap::new();
        if let Some(obj) = data.as_object() {
            for (key, day_data) in obj {
                if key == "total" {
                    continue;
                }
                if let Some(anth) = day_data.get("anthropic").and_then(|v| v.as_object()) {
                    for (model_name, model_stats) in anth {
                        let entry = total_anthropic.entry(model_name.clone()).or_insert_with(|| {
                            json!({"input_tokens": 0, "output_tokens": 0, "cost": 0.0, "documents": 0})
                        });
                        let it = entry["input_tokens"].as_u64().unwrap_or(0)
                            + model_stats.get("input_tokens").and_then(|v| v.as_u64()).unwrap_or(0);
                        let ot = entry["output_tokens"].as_u64().unwrap_or(0)
                            + model_stats.get("output_tokens").and_then(|v| v.as_u64()).unwrap_or(0);
                        let cost = entry["cost"].as_f64().unwrap_or(0.0)
                            + model_stats.get("cost").and_then(|v| v.as_f64()).unwrap_or(0.0);
                        let docs = entry["documents"].as_u64().unwrap_or(0)
                            + model_stats.get("documents").and_then(|v| v.as_u64()).unwrap_or(0);
                        entry["input_tokens"] = json!(it);
                        entry["output_tokens"] = json!(ot);
                        entry["cost"] = json!((cost * 1_000_000.0).round() / 1_000_000.0);
                        entry["documents"] = json!(docs);
                        if docs > 0 {
                            entry["cost_per_document"] =
                                json!(((cost / docs as f64) * 1_000_000.0).round() / 1_000_000.0);
                        }
                    }
                }
            }
        }
        data["total"] = json!({"anthropic": total_anthropic});

        // Atomic write
        let tmp_path = costs_path.with_extension("tmp");
        std::fs::write(&tmp_path, serde_json::to_string_pretty(&data)?)?;
        std::fs::rename(&tmp_path, &costs_path)?;
        debug!("Flushed {} records to {:?}", records.len(), costs_path);

        Ok(())
    }

    fn calculate_cost(&self, model: &str, input_tokens: u64, output_tokens: u64) -> f64 {
        if let Some(pricing) = self.pricing.anthropic.get(model) {
            let input_cost = (input_tokens as f64 / 1_000_000.0) * pricing.input_per_1m;
            let output_cost = (output_tokens as f64 / 1_000_000.0) * pricing.output_per_1m;
            input_cost + output_cost
        } else {
            warn!("No pricing found for model: {}", model);
            0.0
        }
    }
}

/// Extract username from a user_dir path (looks for `/sync/{username}` pattern).
fn extract_username(user_dir: &Path) -> String {
    let mut current = Some(user_dir);
    while let Some(path) = current {
        if let Some(parent) = path.parent() {
            if parent.file_name().map(|n| n == "sync").unwrap_or(false)
                || parent == Path::new("/sync")
            {
                return path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown")
                    .to_string();
            }
        }
        current = path.parent();
    }
    user_dir
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown")
        .to_string()
}

/// Truncate string values in a JSON value to `max_len` chars for logging.
fn truncate_json_fields(value: &serde_json::Value, max_len: usize) -> String {
    match value {
        Value::Object(map) => {
            let entries: Vec<String> = map
                .iter()
                .map(|(k, v)| format!("{}: {}", k, truncate_json_fields(v, max_len)))
                .collect();
            format!("{{{}}}", entries.join(", "))
        }
        Value::Array(arr) => {
            let items: Vec<String> = arr
                .iter()
                .take(3)
                .map(|v| truncate_json_fields(v, max_len))
                .collect();
            if arr.len() > 3 {
                format!("[{}, ...+{}]", items.join(", "), arr.len() - 3)
            } else {
                format!("[{}]", items.join(", "))
            }
        }
        Value::String(s) => {
            if s.len() > max_len {
                format!("\"{}...\"", crate::truncate::truncate_str(s, max_len))
            } else {
                format!("\"{}\"", s)
            }
        }
        other => other.to_string(),
    }
}

// Global singleton
static COST_TRACKER: std::sync::OnceLock<CostTracker> = std::sync::OnceLock::new();

pub fn get_cost_tracker() -> &'static CostTracker {
    COST_TRACKER.get_or_init(CostTracker::new)
}

pub fn shutdown_cost_tracker() {
    if let Some(tracker) = COST_TRACKER.get() {
        tracker.flush();
    }
}
