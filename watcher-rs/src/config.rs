//! Configuration and sorting logic for the document watcher.
//!
//! Ported from `sorter.py` – covers filename sanitisation, context/smart-folder
//! configuration, watcher config, and the `SorterContextManager`.

use anyhow::{Context as _, Result};
use regex::Regex;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing::{debug, error, info, warn};
use unicode_normalization::UnicodeNormalization;

// ---------------------------------------------------------------------------
// Character replacements
// ---------------------------------------------------------------------------

/// Replacement pairs for common non-ASCII characters used in filenames.
pub static CHAR_REPLACEMENTS: &[(&str, &str)] = &[
    ("\u{00e4}", "ae"), // ä
    ("\u{00f6}", "oe"), // ö
    ("\u{00fc}", "ue"), // ü
    ("\u{00df}", "ss"), // ß
    ("\u{00e9}", "e"),  // é
    ("\u{00e8}", "e"),  // è
    ("\u{00ea}", "e"),  // ê
    ("\u{00eb}", "e"),  // ë
    ("\u{00e0}", "a"),  // à
    ("\u{00e2}", "a"),  // â
    ("\u{00f9}", "u"),  // ù
    ("\u{00fb}", "u"),  // û
    ("\u{00f4}", "o"),  // ô
    ("\u{00ee}", "i"),  // î
    ("\u{00ef}", "i"),  // ï
    ("\u{00e7}", "c"),  // ç
    ("\u{00f1}", "n"),  // ñ
    ("\u{00e6}", "ae"), // æ
    ("\u{0153}", "oe"), // œ
    ("\u{00f8}", "o"),  // ø
    ("\u{00e5}", "a"),  // å
    ("\u{00c4}", "Ae"), // Ä
    ("\u{00d6}", "Oe"), // Ö
    ("\u{00dc}", "Ue"), // Ü
];

// ---------------------------------------------------------------------------
// Filename helpers
// ---------------------------------------------------------------------------

/// Sanitize a string for use as a filename component.
///
/// 1. Apply character replacements (ä→ae, etc.)
/// 2. NFKD-normalise, then strip non-ASCII
/// 3. Replace whitespace / dashes with `_`
/// 4. Strip filesystem-special characters
/// 5. Collapse runs of `_`, strip leading/trailing `_`, cap at 50 chars
pub fn sanitize_filename_part(s: &str) -> String {
    let mut out = s.to_string();

    // 1. char replacements
    for &(from, to) in CHAR_REPLACEMENTS {
        out = out.replace(from, to);
    }

    // 2. NFKD + ASCII-only
    out = out.nfkd().collect::<String>();
    out.retain(|c| c.is_ascii());

    // 3. whitespace → underscore, dash → underscore
    let re_ws = Regex::new(r"\s+").unwrap();
    out = re_ws.replace_all(&out, "_").to_string();
    out = out.replace('-', "_");

    // 4. strip special chars  <>:"/\|?*  and control chars
    let re_special = Regex::new(r#"[<>:"/\\|?*\x00-\x1f]"#).unwrap();
    out = re_special.replace_all(&out, "_").to_string();

    // 5. collapse underscores
    let re_uu = Regex::new(r"_+").unwrap();
    out = re_uu.replace_all(&out, "_").to_string();
    out = out.trim_matches('_').to_string();

    // 6. cap at 50
    if out.len() > 50 {
        out.truncate(50);
        out = out.trim_end_matches('_').to_string();
    }

    out
}

/// Format a filename from metadata and a pattern string.
///
/// * `{field}` placeholders are substituted from `metadata`.
/// * The special placeholder `{source_filename}` resolves to the stem of the
///   supplied original filename.
/// * `{date}` values are **not** sanitised.
/// * The result is always lowercase and ends in `.pdf`.
/// * If substitution yields an empty string, returns `"document.pdf"`.
pub fn format_filename(
    metadata: &serde_json::Value,
    pattern: &str,
    source_filename: Option<&str>,
) -> String {
    let re_ph = Regex::new(r"\{(\w+)\}").unwrap();

    let mut result = pattern.to_string();
    let placeholders: Vec<String> = re_ph
        .captures_iter(pattern)
        .map(|c| c[1].to_string())
        .collect();

    for field_name in &placeholders {
        let placeholder = format!("{{{}}}", field_name);
        let value: Option<String> = if field_name == "source_filename" {
            source_filename.map(|sf| {
                Path::new(sf)
                    .file_stem()
                    .map(|s| s.to_string_lossy().to_string())
                    .unwrap_or_default()
            })
        } else {
            match metadata.get(field_name.as_str()) {
                Some(serde_json::Value::String(v)) => Some(v.clone()),
                Some(serde_json::Value::Array(arr)) => {
                    let parts: Vec<String> = arr
                        .iter()
                        .filter_map(|v| v.as_str().map(|s| sanitize_filename_part(s)))
                        .collect();
                    if parts.is_empty() {
                        None
                    } else {
                        Some(parts.join("_"))
                    }
                }
                Some(other) => {
                    let s = other.to_string().trim_matches('"').to_string();
                    if s.is_empty() || s == "null" {
                        None
                    } else {
                        Some(s)
                    }
                }
                None => None,
            }
        };

        if let Some(v) = value {
            let sanitized = if field_name == "date" {
                v
            } else {
                sanitize_filename_part(&v)
            };
            result = result.replace(&placeholder, &sanitized);
        } else {
            result = result.replace(&placeholder, "");
        }
    }

    // Collapse runs of -/_ to single -, strip leading/trailing -_
    let re_collapse = Regex::new(r"[-_]{2,}").unwrap();
    result = re_collapse.replace_all(&result, "-").to_string();
    result = result.trim_matches(&['-', '_'][..]).to_string();

    if result.is_empty() {
        return "document.pdf".to_string();
    }

    let result = format!("{}.pdf", result).to_lowercase();
    result
}

// ---------------------------------------------------------------------------
// SmartFolderCondition
// ---------------------------------------------------------------------------

/// A condition tree for smart-folder matching.
///
/// Either a leaf **statement** (`field` + `value` regex) or an inner
/// **operator** node (`and` / `or` / `not`).
#[derive(Debug, Clone)]
pub enum SmartFolderCondition {
    /// Leaf: case-insensitive regex full-match on a single field.
    Statement {
        field: String,
        value: String,
        compiled: Option<Regex>,
    },
    /// Inner node: logical combination of child conditions.
    Operator {
        op: String, // "and", "or", "not"
        operands: Vec<SmartFolderCondition>,
    },
}

impl SmartFolderCondition {
    /// Evaluate the condition against a set of field values.
    pub fn evaluate(&self, fields: &HashMap<String, String>) -> bool {
        match self {
            SmartFolderCondition::Statement {
                field, compiled, ..
            } => {
                let field_value = fields.get(field.as_str()).map(|s| s.as_str()).unwrap_or("");
                match compiled {
                    Some(re) => {
                        // Full-match: anchor the pattern
                        let anchored =
                            Regex::new(&format!("(?i)^(?:{})$", re.as_str())).ok();
                        match anchored {
                            Some(a) => a.is_match(field_value),
                            None => false,
                        }
                    }
                    None => false,
                }
            }
            SmartFolderCondition::Operator { op, operands } => match op.as_str() {
                "not" => {
                    if operands.len() != 1 {
                        warn!("'not' operator requires exactly one operand");
                        return false;
                    }
                    !operands[0].evaluate(fields)
                }
                "and" => {
                    if operands.is_empty() {
                        return true;
                    }
                    operands.iter().all(|op| op.evaluate(fields))
                }
                "or" => {
                    if operands.is_empty() {
                        return false;
                    }
                    operands.iter().any(|op| op.evaluate(fields))
                }
                other => {
                    warn!("Unknown operator: {}", other);
                    false
                }
            },
        }
    }

    /// Parse a condition from a YAML mapping.
    pub fn from_dict(
        data: &serde_yaml::Value,
        context_name: &str,
        sf_name: &str,
    ) -> Option<Self> {
        let map = match data.as_mapping() {
            Some(m) => m,
            None => {
                warn!(
                    "Condition must be a dict, got non-mapping (context '{}', smart folder '{}')",
                    context_name, sf_name,
                );
                return None;
            }
        };

        // Statement?
        let has_field = map.contains_key(&serde_yaml::Value::String("field".into()));
        let has_value = map.contains_key(&serde_yaml::Value::String("value".into()));
        if has_field && has_value {
            let field = yaml_str(data, "field").unwrap_or_default();
            let value = yaml_str(data, "value").unwrap_or_default();
            let compiled = match regex::RegexBuilder::new(&value)
                .case_insensitive(true)
                .build()
            {
                Ok(re) => Some(re),
                Err(e) => {
                    warn!("Invalid regex pattern '{}': {}", value, e);
                    None
                }
            };
            return Some(SmartFolderCondition::Statement {
                field,
                value,
                compiled,
            });
        }

        // Operator?
        if let Some(op_val) = yaml_str(data, "operator") {
            let operator = op_val.to_lowercase();
            if !["and", "or", "not"].contains(&operator.as_str()) {
                warn!(
                    "Unknown operator: {} (context '{}', smart folder '{}')",
                    operator, context_name, sf_name,
                );
                return None;
            }

            let operands_key = serde_yaml::Value::String("operands".into());
            let operands_raw = map.get(&operands_key);

            let operand_list: Vec<&serde_yaml::Value> = match operands_raw {
                Some(serde_yaml::Value::Sequence(seq)) => seq.iter().collect(),
                Some(val @ serde_yaml::Value::Mapping(_)) => vec![val],
                _ => Vec::new(),
            };

            let operands: Vec<SmartFolderCondition> = operand_list
                .into_iter()
                .filter_map(|v| SmartFolderCondition::from_dict(v, context_name, sf_name))
                .collect();

            return Some(SmartFolderCondition::Operator {
                op: operator,
                operands,
            });
        }

        warn!(
            "Condition must have either (field, value) or (operator, operands) \
             (context '{}', smart folder '{}')",
            context_name, sf_name,
        );
        None
    }
}

// ---------------------------------------------------------------------------
// SmartFolderConfig
// ---------------------------------------------------------------------------

/// Configuration for a single smart folder.
#[derive(Debug, Clone)]
pub struct SmartFolderConfig {
    pub name: String,
    pub condition: Option<SmartFolderCondition>,
    pub filename_regex: Option<String>,
    compiled_filename_regex: Option<Regex>,
}

impl SmartFolderConfig {
    /// Does `filename` pass the filename_regex filter?
    ///
    /// * No regex configured → always `true`.
    /// * Regex failed to compile → always `false`.
    /// * Otherwise uses **search** (not full-match).
    pub fn matches_filename(&self, filename: &str) -> bool {
        match (&self.filename_regex, &self.compiled_filename_regex) {
            (None, _) => true,
            (Some(_), None) => false, // regex failed to compile
            (Some(_), Some(re)) => re.is_match(filename),
        }
    }

    /// Parse from YAML data.
    pub fn from_dict(
        name: &str,
        data: &serde_yaml::Value,
        context_name: &str,
    ) -> Option<Self> {
        let map = data.as_mapping()?;
        if map.is_empty() {
            warn!("Smart folder config must be a dict");
            return None;
        }

        let filename_regex = yaml_str(data, "filename_regex");
        let compiled_filename_regex = filename_regex.as_ref().and_then(|pat| {
            match regex::RegexBuilder::new(pat)
                .case_insensitive(true)
                .build()
            {
                Ok(re) => Some(re),
                Err(e) => {
                    warn!(
                        "Invalid filename_regex pattern '{}' for smart folder '{}': {}",
                        pat, name, e,
                    );
                    None
                }
            }
        });

        let condition = yaml_mapping(data, "condition").and_then(|cond_val| {
            SmartFolderCondition::from_dict(cond_val, context_name, name)
        });

        // At least one of condition or filename_regex must be present.
        if condition.is_none() && filename_regex.is_none() {
            warn!(
                "Smart folder '{}' must have 'condition' and/or 'filename_regex'",
                name,
            );
            return None;
        }

        Some(SmartFolderConfig {
            name: name.to_string(),
            condition,
            filename_regex,
            compiled_filename_regex,
        })
    }
}

// ---------------------------------------------------------------------------
// FilenameRule
// ---------------------------------------------------------------------------

/// A conditional filename pattern rule.
#[derive(Debug, Clone)]
pub struct FilenameRule {
    pub pattern: String,
    /// Optional regex – `None` means this is the default/fallback rule.
    pub match_regex: Option<String>,
}

// ---------------------------------------------------------------------------
// ContextConfig
// ---------------------------------------------------------------------------

/// Context configuration for sorting.
#[derive(Debug, Clone)]
pub struct ContextConfig {
    pub name: String,
    pub filename_pattern: String,
    pub folders: Vec<String>,
    pub smart_folders: HashMap<String, SmartFolderConfig>,
    pub field_names: Vec<String>,
    pub filename_rules: Vec<FilenameRule>,
}

impl ContextConfig {
    /// Pick the first matching conditional rule, or fall back to the default
    /// filename pattern.
    pub fn resolve_filename_pattern(&self, source_filename: Option<&str>) -> String {
        if self.filename_rules.is_empty() || source_filename.is_none() {
            return self.filename_pattern.clone();
        }
        let sf = source_filename.unwrap();
        for rule in &self.filename_rules {
            if let Some(ref pat) = rule.match_regex {
                if let Ok(re) = Regex::new(pat) {
                    if re.is_match(sf) {
                        return rule.pattern.clone();
                    }
                }
            }
        }
        self.filename_pattern.clone()
    }

    /// Parse a context from YAML data.
    pub fn from_dict(data: &serde_yaml::Value) -> Option<Self> {
        let name = yaml_str(data, "name")?;
        let filename_raw = data
            .as_mapping()?
            .get(&serde_yaml::Value::String("filename".into()))?;

        let mut filename_rules: Vec<FilenameRule> = Vec::new();
        let filename_pattern: String;

        match filename_raw {
            serde_yaml::Value::String(s) => {
                filename_pattern = s.clone();
            }
            serde_yaml::Value::Sequence(seq) => {
                let mut default_pattern: Option<String> = None;
                for entry in seq {
                    if let Some(map) = entry.as_mapping() {
                        let pat_key = serde_yaml::Value::String("pattern".into());
                        if let Some(serde_yaml::Value::String(pat)) = map.get(&pat_key) {
                            let match_regex = yaml_str(entry, "match");
                            let rule = FilenameRule {
                                pattern: pat.clone(),
                                match_regex: match_regex.clone(),
                            };
                            if rule.match_regex.is_none() {
                                default_pattern = Some(pat.clone());
                            }
                            filename_rules.push(rule);
                        }
                    }
                }
                match default_pattern {
                    Some(dp) => filename_pattern = dp,
                    None => {
                        error!(
                            "Context '{}': filename is a conditional list with no default pattern \
                             (add an entry without 'match')",
                            name,
                        );
                        return None;
                    }
                }
            }
            _ => return None,
        };

        // folders
        let folders: Vec<String> = match data
            .as_mapping()
            .and_then(|m| m.get(&serde_yaml::Value::String("folders".into())))
        {
            Some(serde_yaml::Value::Sequence(seq)) => seq
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .filter(|s| !s.is_empty())
                .collect(),
            _ => Vec::new(),
        };

        // smart_folders
        let mut smart_folders: HashMap<String, SmartFolderConfig> = HashMap::new();
        if let Some(sf_map) = data
            .as_mapping()
            .and_then(|m| m.get(&serde_yaml::Value::String("smart_folders".into())))
            .and_then(|v| v.as_mapping())
        {
            for (k, v) in sf_map {
                if let Some(sf_name) = k.as_str() {
                    if let Some(sf_config) =
                        SmartFolderConfig::from_dict(sf_name, v, &name)
                    {
                        smart_folders.insert(sf_name.to_string(), sf_config);
                    }
                }
            }
        }

        // field_names: always starts with ["context", "date"], then adds explicit fields
        let mut field_names: Vec<String> = vec!["context".into(), "date".into()];
        if let Some(fields_map) = data
            .as_mapping()
            .and_then(|m| m.get(&serde_yaml::Value::String("fields".into())))
            .and_then(|v| v.as_mapping())
        {
            for k in fields_map.keys() {
                if let Some(fname) = k.as_str() {
                    let fname = fname.to_string();
                    if !field_names.contains(&fname) {
                        field_names.push(fname);
                    }
                }
            }
        }

        Some(ContextConfig {
            name,
            filename_pattern,
            folders,
            smart_folders,
            field_names,
            filename_rules,
        })
    }
}

// ---------------------------------------------------------------------------
// WatcherConfig
// ---------------------------------------------------------------------------

/// Global watcher configuration.
#[derive(Debug, Clone)]
pub struct WatcherConfig {
    pub watch_patterns: Vec<String>,
    pub debounce_seconds: f64,
    pub full_scan_seconds: f64,
}

impl Default for WatcherConfig {
    fn default() -> Self {
        Self {
            watch_patterns: vec!["/sync/*".into()],
            debounce_seconds: 15.0,
            full_scan_seconds: 300.0,
        }
    }
}

impl WatcherConfig {
    /// Load watcher configuration from a YAML file.
    pub fn load(path: &Path) -> Self {
        if !path.exists() {
            info!("No watcher config at {:?}, using defaults", path);
            return Self::default();
        }
        match std::fs::read_to_string(path) {
            Ok(contents) => match serde_yaml::from_str::<serde_yaml::Value>(&contents) {
                Ok(data) => {
                    let patterns = data
                        .as_mapping()
                        .and_then(|m| m.get(&serde_yaml::Value::String("watch_patterns".into())))
                        .and_then(|v| v.as_sequence())
                        .map(|seq| {
                            seq.iter()
                                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_else(|| vec!["/sync/*".into()]);

                    let debounce = yaml_f64(&data, "debounce_seconds").unwrap_or(15.0);
                    let full_scan = yaml_f64(&data, "full_scan_seconds").unwrap_or(300.0);

                    WatcherConfig {
                        watch_patterns: patterns,
                        debounce_seconds: debounce,
                        full_scan_seconds: full_scan,
                    }
                }
                Err(e) => {
                    warn!("Failed to parse watcher config from {:?}: {}", path, e);
                    Self::default()
                }
            },
            Err(e) => {
                warn!("Failed to load watcher config from {:?}: {}", path, e);
                Self::default()
            }
        }
    }

    /// Expand glob patterns to get concrete watch directories.
    pub fn get_watch_directories(&self) -> Vec<PathBuf> {
        let mut directories = std::collections::BTreeSet::<PathBuf>::new();
        for pattern in &self.watch_patterns {
            if pattern.contains('*') {
                if let Ok(paths) = glob::glob(pattern) {
                    for entry in paths.flatten() {
                        if entry.is_dir()
                            && !entry
                                .file_name()
                                .map(|n| n.to_string_lossy().starts_with('.'))
                                .unwrap_or(false)
                        {
                            directories.insert(entry);
                        }
                    }
                }
            } else {
                let p = PathBuf::from(pattern);
                if p.exists() && p.is_dir() {
                    directories.insert(p);
                }
            }
        }
        directories.into_iter().collect()
    }
}

// ---------------------------------------------------------------------------
// get_username_from_root
// ---------------------------------------------------------------------------

/// Derive the username (directory directly under `/sync`) from a user-root path.
///
/// For `/sync/heike/mrdocument` returns `"heike"`.
/// Falls back to the final component of `root`.
pub fn get_username_from_root(root: &Path) -> String {
    let sync_root = Path::new("/sync").canonicalize().unwrap_or_else(|_| PathBuf::from("/sync"));
    let resolved = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());

    // Walk up looking for the directory directly under sync_root
    let mut current = Some(resolved.as_path());
    while let Some(p) = current {
        if let Some(parent) = p.parent() {
            if parent == sync_root {
                return p
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_default();
            }
        }
        current = p.parent();
    }

    // Fallback
    root.file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// SorterContextManager
// ---------------------------------------------------------------------------

/// Manages context configurations loaded from `sorted/{ctx}/context.yaml` files
/// and the generated-data overlay stored in `generated.yaml`.
#[derive(Clone)]
pub struct SorterContextManager {
    pub contexts: HashMap<String, ContextConfig>,
    pub user_root: PathBuf,
    pub username: String,
    /// Per-context generated data: `{ field_name: { "candidates": [...] } }`.
    generated_data: HashMap<String, HashMap<String, serde_yaml::Value>>,
    /// Paths to generated.yaml files, keyed by context name.
    generated_files: HashMap<String, PathBuf>,
}

impl SorterContextManager {
    pub fn new(root: &Path, username: &str) -> Self {
        Self {
            contexts: HashMap::new(),
            user_root: root.to_path_buf(),
            username: username.to_string(),
            generated_data: HashMap::new(),
            generated_files: HashMap::new(),
        }
    }

    /// Load contexts from `sorted/{context}/context.yaml` files.
    /// Returns `true` if at least one context was loaded.
    pub fn load(&mut self) -> bool {
        self.load_from_sorted()
    }

    fn load_from_sorted(&mut self) -> bool {
        let sorted_dir = self.user_root.join("sorted");
        if !sorted_dir.is_dir() {
            return false;
        }

        let mut contexts: HashMap<String, ContextConfig> = HashMap::new();

        let entries = match std::fs::read_dir(&sorted_dir) {
            Ok(e) => e,
            Err(_) => return false,
        };

        for entry in entries.flatten() {
            let ctx_dir = entry.path();
            if !ctx_dir.is_dir() {
                continue;
            }
            if ctx_dir
                .file_name()
                .map(|n| n.to_string_lossy().starts_with('.'))
                .unwrap_or(true)
            {
                continue;
            }

            let ctx_yaml = match find_ci(&ctx_dir, "context.yaml") {
                Some(p) => p,
                None => continue,
            };

            match std::fs::read_to_string(&ctx_yaml) {
                Ok(contents) => {
                    let data: serde_yaml::Value = match serde_yaml::from_str(&contents) {
                        Ok(d) => d,
                        Err(_) => continue,
                    };
                    if !data.is_mapping() {
                        continue;
                    }
                    let context = match ContextConfig::from_dict(&data) {
                        Some(c) => c,
                        None => continue,
                    };
                    let dir_name = ctx_dir
                        .file_name()
                        .unwrap()
                        .to_string_lossy()
                        .to_lowercase();
                    if context.name.to_lowercase() != dir_name {
                        warn!(
                            "[{}] Context name '{}' does not match directory '{}', skipping",
                            self.username,
                            context.name,
                            dir_name,
                        );
                        continue;
                    }
                    contexts.insert(context.name.to_lowercase(), context);
                }
                Err(e) => {
                    warn!("Failed to load context from {:?}: {}", ctx_yaml, e);
                }
            }
        }

        if contexts.is_empty() {
            return false;
        }

        self.contexts = contexts;
        self.generated_data.clear();
        self.generated_files.clear();

        let ctx_names: Vec<String> = self.contexts.keys().cloned().collect();
        for ctx_name in &ctx_names {
            let ctx_dir = self.user_root.join("sorted").join(ctx_name);
            let gen_path =
                find_ci(&ctx_dir, "generated.yaml").unwrap_or_else(|| ctx_dir.join("generated.yaml"));
            self.generated_files.insert(ctx_name.clone(), gen_path.clone());
            let gen_fields = self.load_generated_file(&gen_path);
            if !gen_fields.is_empty() {
                self.generated_data.insert(ctx_name.clone(), gen_fields);
            }
        }

        info!(
            "[{}] Sorter loaded {} context(s) from sorted/: {:?}",
            self.username,
            self.contexts.len(),
            self.contexts.keys().collect::<Vec<_>>(),
        );
        true
    }

    /// Load smart folders from `sorted/{context}/smartfolders.yaml` files.
    pub fn load_smart_folders_from_sorted(
        &self,
    ) -> HashMap<String, Vec<(String, SmartFolderConfig)>> {
        let sorted_dir = self.user_root.join("sorted");
        if !sorted_dir.is_dir() {
            return HashMap::new();
        }

        let mut result: HashMap<String, Vec<(String, SmartFolderConfig)>> = HashMap::new();

        let entries = match std::fs::read_dir(&sorted_dir) {
            Ok(e) => e,
            Err(_) => return HashMap::new(),
        };

        for entry in entries.flatten() {
            let ctx_dir = entry.path();
            if !ctx_dir.is_dir() {
                continue;
            }
            if ctx_dir
                .file_name()
                .map(|n| n.to_string_lossy().starts_with('.'))
                .unwrap_or(true)
            {
                continue;
            }

            let sf_yaml = match find_ci(&ctx_dir, "smartfolders.yaml") {
                Some(p) => p,
                None => continue,
            };

            match std::fs::read_to_string(&sf_yaml) {
                Ok(contents) => {
                    let data: serde_yaml::Value = match serde_yaml::from_str(&contents) {
                        Ok(d) => d,
                        Err(_) => continue,
                    };
                    let sf_data = match data
                        .as_mapping()
                        .and_then(|m| m.get(&serde_yaml::Value::String("smart_folders".into())))
                        .and_then(|v| v.as_mapping())
                    {
                        Some(d) => d,
                        None => continue,
                    };

                    let ctx_name = ctx_dir
                        .file_name()
                        .unwrap()
                        .to_string_lossy()
                        .to_lowercase();

                    let mut entries_vec: Vec<(String, SmartFolderConfig)> = Vec::new();
                    for (k, v) in sf_data {
                        let sf_name = match k.as_str() {
                            Some(s) => s.to_string(),
                            None => continue,
                        };
                        if !v.is_mapping() {
                            continue;
                        }
                        // Validate context field matches directory
                        if let Some(sf_context) = yaml_str(v, "context") {
                            if !sf_context.is_empty()
                                && sf_context.to_lowercase() != ctx_name
                            {
                                warn!(
                                    "[{}] Smart folder '{}' context '{}' does not match directory '{}', skipping",
                                    self.username, sf_name, sf_context, ctx_name,
                                );
                                continue;
                            }
                        }
                        if let Some(sf_config) =
                            SmartFolderConfig::from_dict(&sf_name, v, &ctx_name)
                        {
                            entries_vec.push((sf_name, sf_config));
                        }
                    }
                    if !entries_vec.is_empty() {
                        result.insert(ctx_name, entries_vec);
                    }
                }
                Err(e) => {
                    warn!("Failed to load smart folders from {:?}: {}", sf_yaml, e);
                }
            }
        }

        result
    }

    /// Load the full context YAML dict for API consumption, with generated
    /// candidates merged in.
    pub fn get_context_for_api(&self, name: &str) -> Option<serde_json::Value> {
        let ctx_name = name.to_lowercase();
        let data = self.load_context_yaml(&ctx_name)?;

        // Convert serde_yaml::Value → serde_json::Value
        let json_str = serde_json::to_string(&data).ok()?;
        let mut json_val: serde_json::Value = serde_json::from_str(&json_str).ok()?;

        // Ensure description exists
        if json_val.get("description").is_none() {
            let desc = json_val
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or(name)
                .to_string();
            json_val["description"] = serde_json::Value::String(desc);
        }

        // Merge generated candidates into fields
        if let Some(gen_fields) = self.generated_data.get(&ctx_name) {
            if let Some(fields_obj) = json_val.get_mut("fields") {
                if let Some(fields_map) = fields_obj.as_object_mut() {
                    for field_name in fields_map.keys().cloned().collect::<Vec<_>>() {
                        if let Some(merged) =
                            self.get_all_candidates_json(&ctx_name, &field_name, &data, gen_fields)
                        {
                            if let Some(field_obj) = fields_map
                                .get_mut(&field_name)
                                .and_then(|v| v.as_object_mut())
                            {
                                field_obj.insert(
                                    "candidates".to_string(),
                                    merged,
                                );
                            }
                        }
                    }
                }
            }
        }

        Some(json_val)
    }

    /// Check if a value is new (not present in base or generated candidates).
    pub fn is_new_item(&self, context_name: &str, field_name: &str, value: &str) -> bool {
        if value.is_empty() || !self.contexts.contains_key(context_name) {
            return false;
        }
        let ctx_data = match self.load_context_yaml(context_name) {
            Some(d) => d,
            None => return false,
        };
        let fields = match ctx_data
            .as_mapping()
            .and_then(|m| m.get(&serde_yaml::Value::String("fields".into())))
            .and_then(|v| v.as_mapping())
        {
            Some(f) => f,
            None => return false,
        };
        let field_key = serde_yaml::Value::String(field_name.into());
        let field_config = match fields.get(&field_key).and_then(|v| v.as_mapping()) {
            Some(fc) => fc,
            None => return false,
        };

        let candidates = match field_config.get(&serde_yaml::Value::String("candidates".into())) {
            Some(c) => c,
            None => return false, // no candidates concept
        };

        // Check allow_new_candidates (default true)
        let allow = field_config
            .get(&serde_yaml::Value::String("allow_new_candidates".into()))
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        if !allow {
            return false;
        }

        // Check base candidates
        if let Some(seq) = candidates.as_sequence() {
            for c in seq {
                if let Some(s) = c.as_str() {
                    if s == value {
                        return false;
                    }
                }
                if let Some(map) = c.as_mapping() {
                    if yaml_str_from_mapping(map, "name").as_deref() == Some(value) {
                        return false;
                    }
                    if yaml_str_from_mapping(map, "short").as_deref() == Some(value) {
                        return false;
                    }
                }
            }
        }

        // Check generated candidates
        if let Some(gen_fields) = self.generated_data.get(context_name) {
            if let Some(gen_field) = gen_fields.get(field_name) {
                if let Some(gen_candidates) = gen_field
                    .as_mapping()
                    .and_then(|m| m.get(&serde_yaml::Value::String("candidates".into())))
                    .and_then(|v| v.as_sequence())
                {
                    for c in gen_candidates {
                        if let Some(s) = c.as_str() {
                            if s == value {
                                return false;
                            }
                        }
                        if let Some(map) = c.as_mapping() {
                            if yaml_str_from_mapping(map, "name").as_deref() == Some(value) {
                                return false;
                            }
                            if yaml_str_from_mapping(map, "short").as_deref() == Some(value) {
                                return false;
                            }
                        }
                    }
                }
            }
        }

        true
    }

    /// Record a new candidate value in the generated file.
    pub fn record_new_item(
        &mut self,
        context_name: &str,
        field_name: &str,
        value: &str,
    ) -> bool {
        if !self.is_new_item(context_name, field_name, value) {
            return false;
        }

        let gen = self
            .generated_data
            .entry(context_name.to_string())
            .or_default();
        let field_entry = gen.entry(field_name.to_string()).or_insert_with(|| {
            let mut map = serde_yaml::Mapping::new();
            map.insert(
                serde_yaml::Value::String("candidates".into()),
                serde_yaml::Value::Sequence(Vec::new()),
            );
            serde_yaml::Value::Mapping(map)
        });

        if let Some(candidates) = field_entry
            .as_mapping_mut()
            .and_then(|m| m.get_mut(&serde_yaml::Value::String("candidates".into())))
            .and_then(|v| v.as_sequence_mut())
        {
            candidates.push(serde_yaml::Value::String(value.to_string()));
        }

        if self.save_generated_file(context_name) {
            info!(
                "[{}] Recorded new {} '{}' in context '{}'",
                self.username, field_name, value, context_name,
            );
            return true;
        }
        false
    }

    /// Record a new clue for an existing candidate in the generated file.
    pub fn record_new_clue(
        &mut self,
        context_name: &str,
        field_name: &str,
        candidate_value: &str,
        clue: &str,
    ) -> bool {
        if !self.contexts.contains_key(context_name) {
            return false;
        }
        let ctx_data = match self.load_context_yaml(context_name) {
            Some(d) => d,
            None => return false,
        };

        let fields = match ctx_data
            .as_mapping()
            .and_then(|m| m.get(&serde_yaml::Value::String("fields".into())))
            .and_then(|v| v.as_mapping())
        {
            Some(f) => f,
            None => return false,
        };
        let field_key = serde_yaml::Value::String(field_name.into());
        let field_config = match fields.get(&field_key).and_then(|v| v.as_mapping()) {
            Some(fc) => fc,
            None => return false,
        };
        let candidates = match field_config
            .get(&serde_yaml::Value::String("candidates".into()))
            .and_then(|v| v.as_sequence())
        {
            Some(c) => c,
            None => return false,
        };

        // Find candidate in base, check allow_new_clues
        let mut candidate_found = false;
        let mut allows_new_clues = false;
        for c in candidates {
            if let Some(map) = c.as_mapping() {
                if yaml_str_from_mapping(map, "name").as_deref() == Some(candidate_value) {
                    candidate_found = true;
                    allows_new_clues = map
                        .get(&serde_yaml::Value::String("allow_new_clues".into()))
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);
                    break;
                }
            } else if c.as_str() == Some(candidate_value) {
                return false; // simple string candidate, can't add clues
            }
        }

        if !candidate_found || !allows_new_clues {
            return false;
        }

        // Check if clue already exists in base
        for c in candidates {
            if let Some(map) = c.as_mapping() {
                if yaml_str_from_mapping(map, "name").as_deref() == Some(candidate_value) {
                    if let Some(clues) = map
                        .get(&serde_yaml::Value::String("clues".into()))
                        .and_then(|v| v.as_sequence())
                    {
                        for existing in clues {
                            if existing.as_str() == Some(clue) {
                                return false;
                            }
                        }
                    }
                    break;
                }
            }
        }

        // Check if clue already exists in generated data
        if let Some(gen_fields) = self.generated_data.get(context_name) {
            if let Some(gen_field) = gen_fields.get(field_name) {
                if let Some(gen_candidates) = gen_field
                    .as_mapping()
                    .and_then(|m| m.get(&serde_yaml::Value::String("candidates".into())))
                    .and_then(|v| v.as_sequence())
                {
                    for c in gen_candidates {
                        if let Some(map) = c.as_mapping() {
                            if yaml_str_from_mapping(map, "name").as_deref()
                                == Some(candidate_value)
                            {
                                if let Some(clues) = map
                                    .get(&serde_yaml::Value::String("clues".into()))
                                    .and_then(|v| v.as_sequence())
                                {
                                    for existing in clues {
                                        if existing.as_str() == Some(clue) {
                                            return false;
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

        // Add clue to generated data
        let gen = self
            .generated_data
            .entry(context_name.to_string())
            .or_default();
        let field_entry = gen.entry(field_name.to_string()).or_insert_with(|| {
            let mut map = serde_yaml::Mapping::new();
            map.insert(
                serde_yaml::Value::String("candidates".into()),
                serde_yaml::Value::Sequence(Vec::new()),
            );
            serde_yaml::Value::Mapping(map)
        });

        if let Some(gen_candidates) = field_entry
            .as_mapping_mut()
            .and_then(|m| m.get_mut(&serde_yaml::Value::String("candidates".into())))
            .and_then(|v| v.as_sequence_mut())
        {
            // Find existing generated entry for this candidate
            let mut found_gen = false;
            for c in gen_candidates.iter_mut() {
                if let Some(map) = c.as_mapping_mut() {
                    if yaml_str_from_mapping(
                        &serde_yaml::Mapping::from_iter(map.iter().map(|(k, v)| (k.clone(), v.clone()))),
                        "name",
                    )
                    .as_deref()
                        == Some(candidate_value)
                    {
                        let clues_key = serde_yaml::Value::String("clues".into());
                        let clues = map
                            .entry(clues_key)
                            .or_insert(serde_yaml::Value::Sequence(Vec::new()));
                        if let Some(seq) = clues.as_sequence_mut() {
                            seq.push(serde_yaml::Value::String(clue.to_string()));
                        }
                        found_gen = true;
                        break;
                    }
                }
            }
            if !found_gen {
                let mut entry = serde_yaml::Mapping::new();
                entry.insert(
                    serde_yaml::Value::String("name".into()),
                    serde_yaml::Value::String(candidate_value.to_string()),
                );
                entry.insert(
                    serde_yaml::Value::String("clues".into()),
                    serde_yaml::Value::Sequence(vec![serde_yaml::Value::String(
                        clue.to_string(),
                    )]),
                );
                gen_candidates.push(serde_yaml::Value::Mapping(entry));
            }
        }

        if self.save_generated_file(context_name) {
            info!(
                "[{}] Recorded new clue for {} '{}' in context '{}': {}",
                self.username, field_name, candidate_value, context_name, clue,
            );
            return true;
        }
        false
    }

    // ----- Private helpers -----

    fn load_generated_file(
        &self,
        path: &Path,
    ) -> HashMap<String, serde_yaml::Value> {
        if !path.exists() {
            return HashMap::new();
        }
        match std::fs::read_to_string(path) {
            Ok(contents) => match serde_yaml::from_str::<serde_yaml::Value>(&contents) {
                Ok(data) => {
                    if let Some(fields) = data
                        .as_mapping()
                        .and_then(|m| m.get(&serde_yaml::Value::String("fields".into())))
                        .and_then(|v| v.as_mapping())
                    {
                        let mut result = HashMap::new();
                        for (k, v) in fields {
                            if let Some(key) = k.as_str() {
                                result.insert(key.to_string(), v.clone());
                            }
                        }
                        return result;
                    }
                    HashMap::new()
                }
                Err(_) => HashMap::new(),
            },
            Err(e) => {
                warn!("Failed to load generated file {:?}: {}", path, e);
                HashMap::new()
            }
        }
    }

    fn save_generated_file(&self, context_name: &str) -> bool {
        let gen_path = match self.generated_files.get(context_name) {
            Some(p) => p,
            None => return false,
        };
        let gen_fields = self.generated_data.get(context_name);

        // Build output — only include fields that have candidates
        let mut output_fields = serde_yaml::Mapping::new();
        if let Some(fields) = gen_fields {
            for (field_name, field_data) in fields {
                let has_candidates = field_data
                    .as_mapping()
                    .and_then(|m| m.get(&serde_yaml::Value::String("candidates".into())))
                    .and_then(|v| v.as_sequence())
                    .map(|s| !s.is_empty())
                    .unwrap_or(false);
                if has_candidates {
                    let mut entry = serde_yaml::Mapping::new();
                    if let Some(candidates) = field_data
                        .as_mapping()
                        .and_then(|m| m.get(&serde_yaml::Value::String("candidates".into())))
                    {
                        entry.insert(
                            serde_yaml::Value::String("candidates".into()),
                            candidates.clone(),
                        );
                    }
                    output_fields.insert(
                        serde_yaml::Value::String(field_name.clone()),
                        serde_yaml::Value::Mapping(entry),
                    );
                }
            }
        }

        if output_fields.is_empty() {
            if gen_path.exists() {
                let _ = std::fs::remove_file(gen_path);
            }
            return true;
        }

        let mut root = serde_yaml::Mapping::new();
        root.insert(
            serde_yaml::Value::String("fields".into()),
            serde_yaml::Value::Mapping(output_fields),
        );

        let tmp_path = gen_path.with_extension("tmp");
        if let Some(parent) = gen_path.parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                error!("Failed to create directory {:?}: {}", parent, e);
                return false;
            }
        }

        match std::fs::File::create(&tmp_path) {
            Ok(mut f) => {
                use std::io::Write;
                let _ = writeln!(
                    f,
                    "# Auto-generated candidates and clues - do not edit manually"
                );
                match serde_yaml::to_string(&serde_yaml::Value::Mapping(root)) {
                    Ok(yaml_str) => {
                        if let Err(e) = f.write_all(yaml_str.as_bytes()) {
                            error!("Failed to write generated file {:?}: {}", tmp_path, e);
                            return false;
                        }
                    }
                    Err(e) => {
                        error!("Failed to serialize generated data: {}", e);
                        return false;
                    }
                }
            }
            Err(e) => {
                error!("Failed to create temp file {:?}: {}", tmp_path, e);
                return false;
            }
        }

        if let Err(e) = std::fs::rename(&tmp_path, gen_path) {
            error!(
                "[{}] Failed to save generated file {:?}: {}",
                self.username, gen_path, e,
            );
            return false;
        }
        true
    }

    fn load_context_yaml(&self, context_name: &str) -> Option<serde_yaml::Value> {
        let name = context_name.to_lowercase();
        let ctx_dir = self.user_root.join("sorted").join(&name);
        let sorted_ctx = find_ci(&ctx_dir, "context.yaml")?;
        let contents = std::fs::read_to_string(&sorted_ctx).ok()?;
        let data: serde_yaml::Value = serde_yaml::from_str(&contents).ok()?;
        if data.is_mapping() {
            let data_name = yaml_str(&data, "name")
                .map(|n| n.to_lowercase())
                .unwrap_or_default();
            if data_name == name {
                return Some(data);
            }
        }
        None
    }

    /// Merge base + generated candidates for a field, returning a JSON value.
    fn get_all_candidates_json(
        &self,
        context_name: &str,
        field_name: &str,
        ctx_data: &serde_yaml::Value,
        gen_fields: &HashMap<String, serde_yaml::Value>,
    ) -> Option<serde_json::Value> {
        let fields = ctx_data
            .as_mapping()?
            .get(&serde_yaml::Value::String("fields".into()))?
            .as_mapping()?;
        let field_config = fields
            .get(&serde_yaml::Value::String(field_name.into()))?
            .as_mapping()?;
        let base_candidates = field_config
            .get(&serde_yaml::Value::String("candidates".into()))?
            .as_sequence()?;

        // Convert base candidates to JSON
        let base_json_str = serde_json::to_string(
            &serde_yaml::Value::Sequence(base_candidates.clone()),
        )
        .ok()?;
        let mut all: Vec<serde_json::Value> = serde_json::from_str(&base_json_str).ok()?;

        // Merge generated candidates
        if let Some(gen_field) = gen_fields.get(field_name) {
            if let Some(gen_candidates) = gen_field
                .as_mapping()
                .and_then(|m| m.get(&serde_yaml::Value::String("candidates".into())))
                .and_then(|v| v.as_sequence())
            {
                for gen_c in gen_candidates {
                    if let Some(gen_str) = gen_c.as_str() {
                        // Check if already present
                        let exists = all.iter().any(|c| {
                            if let Some(s) = c.as_str() {
                                s == gen_str
                            } else if let Some(obj) = c.as_object() {
                                obj.get("name").and_then(|v| v.as_str()) == Some(gen_str)
                                    || obj.get("short").and_then(|v| v.as_str()) == Some(gen_str)
                            } else {
                                false
                            }
                        });
                        if !exists {
                            all.push(serde_json::Value::String(gen_str.to_string()));
                        }
                    } else if let Some(gen_map) = gen_c.as_mapping() {
                        let gen_name = yaml_str_from_mapping(gen_map, "name")
                            .unwrap_or_default();
                        if gen_name.is_empty() {
                            continue;
                        }
                        let gen_clues: Vec<String> = gen_map
                            .get(&serde_yaml::Value::String("clues".into()))
                            .and_then(|v| v.as_sequence())
                            .map(|seq| {
                                seq.iter()
                                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                    .collect()
                            })
                            .unwrap_or_default();

                        // Find matching base candidate and merge clues
                        let mut found = false;
                        for c in all.iter_mut() {
                            if let Some(obj) = c.as_object_mut() {
                                if obj.get("name").and_then(|v| v.as_str()) == Some(&gen_name) {
                                    let existing_clues: Vec<String> = obj
                                        .get("clues")
                                        .and_then(|v| v.as_array())
                                        .map(|arr| {
                                            arr.iter()
                                                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                                .collect()
                                        })
                                        .unwrap_or_default();
                                    let mut merged = existing_clues;
                                    for clue in &gen_clues {
                                        if !merged.contains(clue) {
                                            merged.push(clue.clone());
                                        }
                                    }
                                    obj.insert(
                                        "clues".into(),
                                        serde_json::Value::Array(
                                            merged
                                                .into_iter()
                                                .map(serde_json::Value::String)
                                                .collect(),
                                        ),
                                    );
                                    found = true;
                                    break;
                                }
                            }
                        }
                        if !found {
                            let gen_json_str =
                                serde_json::to_string(&serde_yaml::Value::Mapping(gen_map.clone()))
                                    .unwrap_or_default();
                            if let Ok(gen_json) =
                                serde_json::from_str::<serde_json::Value>(&gen_json_str)
                            {
                                all.push(gen_json);
                            }
                        }
                    }
                }
            }
        }

        Some(serde_json::Value::Array(all))
    }
}

// ---------------------------------------------------------------------------
// YAML utility helpers
// ---------------------------------------------------------------------------

/// Case-insensitive file lookup inside a directory.
fn find_ci(directory: &Path, filename: &str) -> Option<PathBuf> {
    let target = filename.to_lowercase();
    let entries = std::fs::read_dir(directory).ok()?;
    for entry in entries.flatten() {
        if entry
            .file_name()
            .to_string_lossy()
            .to_lowercase()
            == target
        {
            return Some(entry.path());
        }
    }
    None
}

/// Extract a string value from a YAML mapping by key.
fn yaml_str(data: &serde_yaml::Value, key: &str) -> Option<String> {
    data.as_mapping()
        .and_then(|m| m.get(&serde_yaml::Value::String(key.into())))
        .and_then(|v| match v {
            serde_yaml::Value::String(s) => Some(s.clone()),
            serde_yaml::Value::Number(n) => Some(n.to_string()),
            serde_yaml::Value::Bool(b) => Some(b.to_string()),
            _ => None,
        })
}

/// Extract a string from a YAML mapping directly.
fn yaml_str_from_mapping(map: &serde_yaml::Mapping, key: &str) -> Option<String> {
    map.get(&serde_yaml::Value::String(key.into()))
        .and_then(|v| v.as_str().map(|s| s.to_string()))
}

/// Get a sub-value that is itself a mapping.
fn yaml_mapping<'a>(data: &'a serde_yaml::Value, key: &str) -> Option<&'a serde_yaml::Value> {
    data.as_mapping()
        .and_then(|m| m.get(&serde_yaml::Value::String(key.into())))
        .filter(|v| v.is_mapping())
}

/// Extract an f64 from a YAML mapping.
fn yaml_f64(data: &serde_yaml::Value, key: &str) -> Option<f64> {
    data.as_mapping()
        .and_then(|m| m.get(&serde_yaml::Value::String(key.into())))
        .and_then(|v| v.as_f64())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_filename_part_basic() {
        assert_eq!(sanitize_filename_part("hello world"), "hello_world");
        assert_eq!(sanitize_filename_part("foo-bar"), "foo_bar");
        assert_eq!(sanitize_filename_part("__a__b__"), "a_b");
    }

    #[test]
    fn test_sanitize_filename_part_umlaut() {
        assert_eq!(sanitize_filename_part("Ärzte"), "Aerzte");
        assert_eq!(sanitize_filename_part("über"), "ueber");
        assert_eq!(sanitize_filename_part("Straße"), "Strasse");
    }

    #[test]
    fn test_sanitize_filename_part_max_length() {
        let long = "a".repeat(100);
        let result = sanitize_filename_part(&long);
        assert!(result.len() <= 50);
    }

    #[test]
    fn test_format_filename_simple() {
        let meta = serde_json::json!({"context": "invoice", "date": "2024-01-15"});
        let result = format_filename(&meta, "{context}-{date}", None);
        assert_eq!(result, "invoice-2024-01-15.pdf");
    }

    #[test]
    fn test_format_filename_empty() {
        let meta = serde_json::json!({});
        let result = format_filename(&meta, "{context}-{date}", None);
        assert_eq!(result, "document.pdf");
    }

    #[test]
    fn test_format_filename_source_filename() {
        let meta = serde_json::json!({"context": "invoice"});
        let result = format_filename(&meta, "{context}-{source_filename}", Some("scan_001.pdf"));
        assert_eq!(result, "invoice-scan_001.pdf");
    }

    #[test]
    fn test_smart_folder_condition_statement() {
        let cond = SmartFolderCondition::Statement {
            field: "context".into(),
            value: "invoice".into(),
            compiled: Some(Regex::new("invoice").unwrap()),
        };
        let mut fields = HashMap::new();
        fields.insert("context".to_string(), "invoice".to_string());
        assert!(cond.evaluate(&fields));

        fields.insert("context".to_string(), "receipt".to_string());
        assert!(!cond.evaluate(&fields));
    }

    #[test]
    fn test_smart_folder_condition_and() {
        let cond = SmartFolderCondition::Operator {
            op: "and".into(),
            operands: vec![
                SmartFolderCondition::Statement {
                    field: "a".into(),
                    value: "x".into(),
                    compiled: Some(Regex::new("x").unwrap()),
                },
                SmartFolderCondition::Statement {
                    field: "b".into(),
                    value: "y".into(),
                    compiled: Some(Regex::new("y").unwrap()),
                },
            ],
        };
        let mut fields = HashMap::new();
        fields.insert("a".to_string(), "x".to_string());
        fields.insert("b".to_string(), "y".to_string());
        assert!(cond.evaluate(&fields));

        fields.insert("b".to_string(), "z".to_string());
        assert!(!cond.evaluate(&fields));
    }

    // --- Additional config.rs tests ---

    #[test]
    fn test_sanitize_filename_part_special_chars() {
        assert_eq!(sanitize_filename_part("file<>name"), "file_name");
        assert_eq!(sanitize_filename_part("a:b/c\\d"), "a_b_c_d");
        assert_eq!(sanitize_filename_part("test?file*"), "test_file");
    }

    #[test]
    fn test_sanitize_filename_part_empty() {
        assert_eq!(sanitize_filename_part(""), "");
        assert_eq!(sanitize_filename_part("___"), "");
    }

    #[test]
    fn test_sanitize_filename_part_accented_chars() {
        assert_eq!(sanitize_filename_part("café"), "cafe");
        assert_eq!(sanitize_filename_part("naïve"), "naive");
        assert_eq!(sanitize_filename_part("résumé"), "resume");
    }

    #[test]
    fn test_format_filename_with_all_fields() {
        let meta = serde_json::json!({
            "context": "arbeit",
            "date": "2025-01-15",
            "type": "Rechnung",
            "sender": "Schulze GmbH",
        });
        let result = format_filename(&meta, "{context}-{date}-{type}-{sender}", None);
        assert_eq!(result, "arbeit-2025-01-15-rechnung-schulze_gmbh.pdf");
    }

    #[test]
    fn test_format_filename_missing_field_omitted() {
        let meta = serde_json::json!({"context": "work"});
        let result = format_filename(&meta, "{context}-{date}-{type}", None);
        // Missing fields are removed, dashes collapsed
        assert_eq!(result, "work.pdf");
    }

    #[test]
    fn test_format_filename_array_field() {
        let meta = serde_json::json!({"context": "work", "keywords": ["invoice", "urgent"]});
        let result = format_filename(&meta, "{context}-{keywords}", None);
        assert_eq!(result, "work-invoice_urgent.pdf");
    }

    #[test]
    fn test_format_filename_always_lowercase() {
        let meta = serde_json::json!({"context": "Work", "type": "INVOICE"});
        let result = format_filename(&meta, "{context}-{type}", None);
        assert_eq!(result, "work-invoice.pdf");
    }

    #[test]
    fn test_format_filename_source_filename_stem_only() {
        let meta = serde_json::json!({"context": "work"});
        let result = format_filename(&meta, "{context}-{source_filename}", Some("document.pdf"));
        // source_filename uses the stem (no extension)
        assert_eq!(result, "work-document.pdf");
    }

    #[test]
    fn test_format_filename_m4a_source_filename_pattern() {
        // Simulates the privat audio_filename pattern: "{context}-{source_filename}-{date}"
        let meta = serde_json::json!({
            "context": "privat",
            "date": "2025-08-03",
            "type": "Notiz",
            "sender": "Dr. Braun",
        });
        let result = format_filename(
            &meta,
            "{context}-{source_filename}-{date}",
            Some("privatnotiz.m4a"),
        );
        // source_filename strips extension → "privatnotiz"
        assert_eq!(result, "privat-privatnotiz-2025-08-03.pdf");
    }

    /// Regression: the watcher stripped the extension before sending the
    /// filename to the service, so `source_filename` received an
    /// already-stripped stem.  For dotted filenames like "my.recording.m4a"
    /// the watcher sent "my.recording", then `format_filename` called
    /// `file_stem("my.recording")` → "my", losing ".recording".
    ///
    /// The fix: the watcher must send the full filename (with extension)
    /// so that `file_stem` strips exactly once.
    #[test]
    fn test_format_filename_source_filename_dotted_stem() {
        let meta = serde_json::json!({
            "context": "privat",
            "date": "2025-08-03",
        });
        // Full filename (with extension) → file_stem strips once → correct
        let result = format_filename(
            &meta,
            "{context}-{source_filename}-{date}",
            Some("my.recording.m4a"),
        );
        assert_eq!(result, "privat-my.recording-2025-08-03.pdf");

        // Pre-stripped stem (the bug) → file_stem strips again → wrong
        let buggy = format_filename(
            &meta,
            "{context}-{source_filename}-{date}",
            Some("my.recording"),  // already stripped by watcher
        );
        // Demonstrates the bug: "my" instead of "my.recording"
        assert_eq!(buggy, "privat-my-2025-08-03.pdf");
    }

    #[test]
    fn test_format_filename_date_not_sanitized() {
        let meta = serde_json::json!({"date": "2025-01-15"});
        let result = format_filename(&meta, "{date}", None);
        // date keeps its dashes (not sanitized to underscores)
        assert_eq!(result, "2025-01-15.pdf");
    }

    #[test]
    fn test_smart_folder_condition_or() {
        let cond = SmartFolderCondition::Operator {
            op: "or".into(),
            operands: vec![
                SmartFolderCondition::Statement {
                    field: "type".into(),
                    value: "invoice".into(),
                    compiled: Some(Regex::new("invoice").unwrap()),
                },
                SmartFolderCondition::Statement {
                    field: "type".into(),
                    value: "receipt".into(),
                    compiled: Some(Regex::new("receipt").unwrap()),
                },
            ],
        };
        let mut fields = HashMap::new();
        fields.insert("type".to_string(), "invoice".to_string());
        assert!(cond.evaluate(&fields));

        fields.insert("type".to_string(), "receipt".to_string());
        assert!(cond.evaluate(&fields));

        fields.insert("type".to_string(), "contract".to_string());
        assert!(!cond.evaluate(&fields));
    }

    #[test]
    fn test_smart_folder_condition_not() {
        let cond = SmartFolderCondition::Operator {
            op: "not".into(),
            operands: vec![SmartFolderCondition::Statement {
                field: "type".into(),
                value: "draft".into(),
                compiled: Some(Regex::new("draft").unwrap()),
            }],
        };
        let mut fields = HashMap::new();
        fields.insert("type".to_string(), "invoice".to_string());
        assert!(cond.evaluate(&fields));

        fields.insert("type".to_string(), "draft".to_string());
        assert!(!cond.evaluate(&fields));
    }

    #[test]
    fn test_smart_folder_condition_regex_case_insensitive() {
        let cond = SmartFolderCondition::Statement {
            field: "sender".into(),
            value: "schulze.*".into(),
            compiled: Some(Regex::new("schulze.*").unwrap()),
        };
        let mut fields = HashMap::new();
        fields.insert("sender".to_string(), "Schulze GmbH".to_string());
        assert!(cond.evaluate(&fields));
    }

    #[test]
    fn test_smart_folder_condition_missing_field() {
        let cond = SmartFolderCondition::Statement {
            field: "nonexistent".into(),
            value: "anything".into(),
            compiled: Some(Regex::new("anything").unwrap()),
        };
        let fields = HashMap::new();
        assert!(!cond.evaluate(&fields));
    }

    #[test]
    fn test_smart_folder_condition_empty_and() {
        let cond = SmartFolderCondition::Operator {
            op: "and".into(),
            operands: vec![],
        };
        let fields = HashMap::new();
        assert!(cond.evaluate(&fields)); // empty AND = true
    }

    #[test]
    fn test_smart_folder_condition_empty_or() {
        let cond = SmartFolderCondition::Operator {
            op: "or".into(),
            operands: vec![],
        };
        let fields = HashMap::new();
        assert!(!cond.evaluate(&fields)); // empty OR = false
    }

    #[test]
    fn test_smart_folder_condition_from_dict_statement() {
        let yaml: serde_yaml::Value = serde_yaml::from_str(
            "field: type\nvalue: invoice",
        )
        .unwrap();
        let cond = SmartFolderCondition::from_dict(&yaml, "test", "sf1").unwrap();
        let mut fields = HashMap::new();
        fields.insert("type".to_string(), "invoice".to_string());
        assert!(cond.evaluate(&fields));
    }

    #[test]
    fn test_smart_folder_condition_from_dict_operator() {
        let yaml: serde_yaml::Value = serde_yaml::from_str(
            "operator: or\noperands:\n  - field: type\n    value: invoice\n  - field: type\n    value: receipt",
        )
        .unwrap();
        let cond = SmartFolderCondition::from_dict(&yaml, "test", "sf1").unwrap();
        let mut fields = HashMap::new();
        fields.insert("type".to_string(), "receipt".to_string());
        assert!(cond.evaluate(&fields));
    }

    #[test]
    fn test_smart_folder_config_from_dict() {
        let yaml: serde_yaml::Value = serde_yaml::from_str(
            "condition:\n  field: type\n  value: invoice",
        )
        .unwrap();
        let config = SmartFolderConfig::from_dict("invoices", &yaml, "work").unwrap();
        assert_eq!(config.name, "invoices");
        assert!(config.condition.is_some());
        assert!(config.filename_regex.is_none());
    }

    #[test]
    fn test_smart_folder_config_filename_regex() {
        let yaml: serde_yaml::Value = serde_yaml::from_str(
            "filename_regex: '.*\\.pdf$'\ncondition:\n  field: type\n  value: invoice",
        )
        .unwrap();
        let config = SmartFolderConfig::from_dict("pdf_only", &yaml, "work").unwrap();
        assert!(config.matches_filename("document.pdf"));
        assert!(!config.matches_filename("document.txt"));
    }

    #[test]
    fn test_smart_folder_config_no_condition_or_regex_fails() {
        let yaml: serde_yaml::Value = serde_yaml::from_str("description: test").unwrap();
        let config = SmartFolderConfig::from_dict("empty", &yaml, "work");
        assert!(config.is_none());
    }

    #[test]
    fn test_smart_folder_config_matches_filename_no_regex() {
        let config = SmartFolderConfig {
            name: "test".into(),
            condition: None,
            filename_regex: None,
            compiled_filename_regex: None,
        };
        assert!(config.matches_filename("anything.pdf"));
        assert!(config.matches_filename("anything.txt"));
    }

    #[test]
    fn test_context_config_resolve_filename_pattern_default() {
        let ctx = ContextConfig {
            name: "work".into(),
            filename_pattern: "{context}-{date}".into(),
            folders: vec![],
            smart_folders: HashMap::new(),
            field_names: vec![],
            filename_rules: vec![],
        };
        assert_eq!(
            ctx.resolve_filename_pattern(Some("doc.pdf")),
            "{context}-{date}"
        );
    }

    #[test]
    fn test_context_config_resolve_filename_pattern_conditional() {
        let ctx = ContextConfig {
            name: "work".into(),
            filename_pattern: "{context}-{date}".into(),
            folders: vec![],
            smart_folders: HashMap::new(),
            field_names: vec![],
            filename_rules: vec![
                FilenameRule {
                    pattern: "{context}-audio-{date}".into(),
                    match_regex: Some(r"(?i)\.(mp3|flac|wav)$".into()),
                },
                FilenameRule {
                    pattern: "{context}-{date}".into(),
                    match_regex: None, // default
                },
            ],
        };
        // Audio file matches first rule
        assert_eq!(
            ctx.resolve_filename_pattern(Some("recording.mp3")),
            "{context}-audio-{date}"
        );
        // PDF doesn't match audio rule, falls through to default
        assert_eq!(
            ctx.resolve_filename_pattern(Some("document.pdf")),
            "{context}-{date}"
        );
    }

    #[test]
    fn test_context_config_resolve_no_source_filename() {
        let ctx = ContextConfig {
            name: "work".into(),
            filename_pattern: "{context}-{date}".into(),
            folders: vec![],
            smart_folders: HashMap::new(),
            field_names: vec![],
            filename_rules: vec![FilenameRule {
                pattern: "special".into(),
                match_regex: Some(".*".into()),
            }],
        };
        // No source_filename → always returns default pattern
        assert_eq!(ctx.resolve_filename_pattern(None), "{context}-{date}");
    }

    #[test]
    fn test_context_config_from_dict() {
        let yaml: serde_yaml::Value = serde_yaml::from_str(
            "name: arbeit\nfilename: \"{context}-{date}-{type}\"\nfields:\n  type: {}\n  sender: {}\nfolders:\n  - context\n  - sender",
        )
        .unwrap();
        let ctx = ContextConfig::from_dict(&yaml).unwrap();
        assert_eq!(ctx.name, "arbeit");
        assert_eq!(ctx.filename_pattern, "{context}-{date}-{type}");
        assert_eq!(ctx.folders, vec!["context", "sender"]);
        // field_names always starts with context, date
        assert!(ctx.field_names.contains(&"context".to_string()));
        assert!(ctx.field_names.contains(&"date".to_string()));
        assert!(ctx.field_names.contains(&"type".to_string()));
        assert!(ctx.field_names.contains(&"sender".to_string()));
    }

    #[test]
    fn test_context_config_conditional_filename() {
        let yaml: serde_yaml::Value = serde_yaml::from_str(
            "name: work\nfilename:\n  - pattern: \"{context}-audio-{date}\"\n    match: \"(?i)\\\\.(mp3|wav)$\"\n  - pattern: \"{context}-{date}\"",
        )
        .unwrap();
        let ctx = ContextConfig::from_dict(&yaml).unwrap();
        assert_eq!(ctx.filename_rules.len(), 2);
        assert!(ctx.filename_rules[0].match_regex.is_some());
        assert!(ctx.filename_rules[1].match_regex.is_none());
        assert_eq!(ctx.filename_pattern, "{context}-{date}"); // default
    }
}
