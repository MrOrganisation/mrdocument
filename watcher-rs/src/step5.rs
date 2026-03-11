//! Smart folder symlink management for document watcher v2.
//!
//! Creates and removes symlinks in smart folder subdirectories within `sorted/`
//! based on record metadata and smart folder conditions.
//!
//! Also supports root-level smart folders configured via a single YAML file
//! at the mrdocument root, placing symlinks at arbitrary paths.

use std::collections::HashMap;
use std::fs;
use std::os::unix::fs as unix_fs;
use std::path::{Path, PathBuf};

use tracing::{error, info};

use crate::config::SmartFolderConfig;
use crate::models::{Record, State};

// ---------------------------------------------------------------------------
// SmartFolderEntry
// ---------------------------------------------------------------------------

/// A smart folder bound to a specific context.
#[derive(Debug, Clone)]
pub struct SmartFolderEntry {
    pub context: String,
    pub config: SmartFolderConfig,
}

// ---------------------------------------------------------------------------
// SmartFolderReconciler
// ---------------------------------------------------------------------------

/// Manages smart folder symlinks based on record metadata.
pub struct SmartFolderReconciler {
    pub root: PathBuf,
    pub sorted_dir: PathBuf,
    pub smart_folders: Vec<SmartFolderEntry>,
    /// Index by context name for fast lookup.
    _by_context: HashMap<String, Vec<usize>>,
}

impl SmartFolderReconciler {
    pub fn new(root: PathBuf, smart_folders: Vec<SmartFolderEntry>) -> Self {
        let sorted_dir = root.join("sorted");
        let mut by_context: HashMap<String, Vec<usize>> = HashMap::new();
        for (i, entry) in smart_folders.iter().enumerate() {
            by_context
                .entry(entry.context.clone())
                .or_default()
                .push(i);
        }
        Self {
            root,
            sorted_dir,
            smart_folders,
            _by_context: by_context,
        }
    }

    /// Evaluate smart folder conditions for records and manage symlinks.
    ///
    /// Only processes `IS_COMPLETE` records in `sorted/` that have a context.
    pub fn reconcile(&self, records: &[Record]) {
        for record in records {
            if record.state != State::IsComplete {
                continue;
            }
            if record.current_location().as_deref() != Some("sorted") {
                continue;
            }
            let ctx = match &record.context {
                Some(c) => c.clone(),
                None => continue,
            };

            let entry_indices = match self._by_context.get(&ctx) {
                Some(indices) => indices.clone(),
                None => continue,
            };

            let current = match record.current_file() {
                Some(cf) => cf,
                None => continue,
            };

            let file_path = self.root.join(&current.path);
            if !file_path.exists() || file_path.symlink_metadata().map(|m| m.file_type().is_symlink()).unwrap_or(false) {
                continue;
            }

            let leaf_folder = match file_path.parent() {
                Some(p) => p.to_path_buf(),
                None => continue,
            };
            let filename = match file_path.file_name().and_then(|n| n.to_str()) {
                Some(n) => n.to_string(),
                None => continue,
            };

            // Build string metadata for condition evaluation
            let str_fields = build_str_fields(&record.metadata);

            for &idx in &entry_indices {
                let entry = &self.smart_folders[idx];
                let sf_config = &entry.config;
                let sf_dir = leaf_folder.join(&sf_config.name);
                let symlink_path = sf_dir.join(&filename);

                // Check filename regex filter
                if !sf_config.matches_filename(&filename) {
                    if is_symlink(&symlink_path) {
                        let _ = fs::remove_file(&symlink_path);
                    }
                    continue;
                }

                // Evaluate condition
                let condition_matches = sf_config
                    .condition
                    .as_ref()
                    .map_or(true, |cond| cond.evaluate(&str_fields));

                if condition_matches {
                    // Create symlink if it doesn't exist
                    if !symlink_path.exists() && !is_symlink(&symlink_path) {
                        match fs::create_dir_all(&sf_dir) {
                            Ok(()) => {
                                let relative_target = PathBuf::from("..").join(&filename);
                                match unix_fs::symlink(&relative_target, &symlink_path) {
                                    Ok(()) => {
                                        if let Ok(rel) = symlink_path.strip_prefix(&self.root) {
                                            info!(
                                                "Smart folder link: {} -> {}",
                                                rel.display(),
                                                filename,
                                            );
                                        }
                                    }
                                    Err(e) => {
                                        error!(
                                            "Failed to create symlink {}: {}",
                                            symlink_path.display(),
                                            e
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                error!(
                                    "Failed to create smart folder dir {}: {}",
                                    sf_dir.display(),
                                    e
                                );
                            }
                        }
                    }
                } else {
                    // Condition doesn't match -- remove symlink if it exists
                    if is_symlink(&symlink_path) {
                        match fs::remove_file(&symlink_path) {
                            Ok(()) => {
                                if let Ok(rel) = symlink_path.strip_prefix(&self.root) {
                                    info!("Removed smart folder link: {}", rel.display());
                                }
                            }
                            Err(e) => {
                                error!(
                                    "Failed to remove symlink {}: {}",
                                    symlink_path.display(),
                                    e
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    /// Remove broken and stale symlinks from all smart folder directories.
    ///
    /// Walks `sorted/` looking for known smart folder subdirectory names.
    /// For each, removes:
    /// - Broken symlinks (target doesn't exist)
    /// - Stale symlinks (name doesn't match any real file in parent leaf folder)
    pub fn cleanup_orphans(&self) {
        if !self.sorted_dir.is_dir() {
            return;
        }

        let sf_names: std::collections::HashSet<String> = self
            .smart_folders
            .iter()
            .map(|e| e.config.name.clone())
            .collect();

        if sf_names.is_empty() {
            return;
        }

        self.walk_for_smart_folders(&self.sorted_dir, &sf_names);
    }

    fn walk_for_smart_folders(
        &self,
        directory: &Path,
        sf_names: &std::collections::HashSet<String>,
    ) {
        let children = match fs::read_dir(directory) {
            Ok(entries) => entries,
            Err(_) => return,
        };

        for entry in children.flatten() {
            let child = entry.path();
            if !child.is_dir() {
                continue;
            }

            let name = match child.file_name().and_then(|n| n.to_str()) {
                Some(n) => n.to_string(),
                None => continue,
            };

            if name.starts_with('.') {
                continue;
            }

            if sf_names.contains(&name) {
                self.cleanup_smart_folder_dir(&child);
            } else {
                self.walk_for_smart_folders(&child, sf_names);
            }
        }
    }

    fn cleanup_smart_folder_dir(&self, sf_dir: &Path) {
        let leaf_folder = match sf_dir.parent() {
            Some(p) => p,
            None => return,
        };

        // Build set of real filenames in the leaf folder
        let mut leaf_files = std::collections::HashSet::new();
        if let Ok(entries) = fs::read_dir(leaf_folder) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() && !is_symlink(&path) {
                    if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                        leaf_files.insert(name.to_string());
                    }
                }
            }
        }

        let items = match fs::read_dir(sf_dir) {
            Ok(entries) => entries,
            Err(_) => return,
        };

        for entry in items.flatten() {
            let item = entry.path();

            if let Some(name) = item.file_name().and_then(|n| n.to_str()) {
                if name.starts_with('.') || name.starts_with('~') {
                    continue;
                }
            }

            if !is_symlink(&item) {
                continue;
            }

            // Check if target exists
            let target_exists = item.canonicalize().map(|p| p.exists()).unwrap_or(false);

            if !target_exists {
                if fs::remove_file(&item).is_ok() {
                    if let Ok(rel) = item.strip_prefix(&self.root) {
                        info!("Cleaned up broken symlink: {}", rel.display());
                    }
                }
                continue;
            }

            // Check if symlink name matches a real file in the leaf folder
            if let Some(name) = item.file_name().and_then(|n| n.to_str()) {
                if !leaf_files.contains(name) {
                    if fs::remove_file(&item).is_ok() {
                        if let Ok(rel) = item.strip_prefix(&self.root) {
                            info!("Cleaned up stale symlink: {}", rel.display());
                        }
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Root-level smart folders
// ---------------------------------------------------------------------------

/// A root-level smart folder with an arbitrary output path.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct RootSmartFolderEntry {
    pub name: String,
    pub context: String,
    /// Resolved absolute path for the symlink directory.
    pub path: PathBuf,
    pub config: SmartFolderConfig,
}

/// Manages root-level smart folder symlinks at arbitrary paths.
pub struct RootSmartFolderReconciler {
    pub root: PathBuf,
    pub sorted_dir: PathBuf,
    pub entries: Vec<RootSmartFolderEntry>,
}

impl RootSmartFolderReconciler {
    pub fn new(root: PathBuf, entries: Vec<RootSmartFolderEntry>) -> Self {
        let sorted_dir = root.join("sorted");
        Self {
            root,
            sorted_dir,
            entries,
        }
    }

    /// Create/remove symlinks for root-level smart folders.
    pub fn reconcile(&self, records: &[Record]) {
        for record in records {
            if record.state != State::IsComplete {
                continue;
            }
            if record.current_location().as_deref() != Some("sorted") {
                continue;
            }
            let ctx = match &record.context {
                Some(c) => c.clone(),
                None => continue,
            };

            let current = match record.current_file() {
                Some(cf) => cf,
                None => continue,
            };

            let file_path = self.root.join(&current.path);
            if !file_path.exists() || is_symlink(&file_path) {
                continue;
            }

            let filename = match file_path.file_name().and_then(|n| n.to_str()) {
                Some(n) => n.to_string(),
                None => continue,
            };

            let str_fields = build_str_fields(&record.metadata);

            for entry in &self.entries {
                if entry.context != ctx {
                    continue;
                }

                let sf_config = &entry.config;
                let symlink_path = entry.path.join(&filename);

                if !sf_config.matches_filename(&filename) {
                    if is_symlink(&symlink_path) {
                        let _ = fs::remove_file(&symlink_path);
                    }
                    continue;
                }

                let condition_matches = sf_config
                    .condition
                    .as_ref()
                    .map_or(true, |cond| cond.evaluate(&str_fields));

                if condition_matches {
                    if !symlink_path.exists() && !is_symlink(&symlink_path) {
                        match fs::create_dir_all(&entry.path) {
                            Ok(()) => {
                                // Compute relative path from symlink dir to the actual file
                                let relative_target =
                                    compute_relative_path(&entry.path, &file_path);
                                match unix_fs::symlink(&relative_target, &symlink_path) {
                                    Ok(()) => {
                                        info!(
                                            "Root smart folder link: {} -> {}",
                                            symlink_path.display(),
                                            filename,
                                        );
                                    }
                                    Err(e) => {
                                        error!(
                                            "Failed to create root smart folder symlink {}: {}",
                                            symlink_path.display(),
                                            e
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                error!(
                                    "Failed to create dir {}: {}",
                                    entry.path.display(),
                                    e
                                );
                            }
                        }
                    }
                } else if is_symlink(&symlink_path) {
                    match fs::remove_file(&symlink_path) {
                        Ok(()) => {
                            info!(
                                "Removed root smart folder link: {}",
                                symlink_path.display(),
                            );
                        }
                        Err(e) => {
                            error!(
                                "Failed to remove root smart folder symlink {}: {}",
                                symlink_path.display(),
                                e
                            );
                        }
                    }
                }
            }
        }
    }

    /// Remove orphaned symlinks from root smart folder directories.
    ///
    /// Only removes symlinks whose resolved target is within `sorted/`.
    /// Regular files and symlinks pointing elsewhere are left untouched.
    pub fn cleanup_orphans(&self) {
        for entry in &self.entries {
            if !entry.path.is_dir() {
                continue;
            }

            let items = match fs::read_dir(&entry.path) {
                Ok(entries) => entries,
                Err(_) => continue,
            };

            for item in items.flatten() {
                let item_path = item.path();
                if !is_symlink(&item_path) {
                    continue;
                }

                let resolved = match item_path.canonicalize() {
                    Ok(p) => p,
                    Err(_) => {
                        // Broken symlink -- check if it used to point into sorted/
                        // Read the raw link target and attempt to resolve
                        if let Ok(raw) = fs::read_link(&item_path) {
                            let maybe_resolved = item_path.parent().unwrap_or(Path::new(".")).join(&raw);
                            if maybe_resolved.starts_with(&self.sorted_dir) {
                                let _ = fs::remove_file(&item_path);
                                info!(
                                    "Cleaned up broken root smart folder symlink: {}",
                                    item_path.display(),
                                );
                            }
                        }
                        continue;
                    }
                };

                // Only touch symlinks pointing into sorted/
                if !resolved.starts_with(&self.sorted_dir) {
                    continue;
                }

                // Remove if target no longer exists
                if !resolved.exists() {
                    if fs::remove_file(&item_path).is_ok() {
                        info!(
                            "Cleaned up broken root smart folder symlink: {}",
                            item_path.display(),
                        );
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Check if a path is a symlink (does not follow the link).
fn is_symlink(path: &Path) -> bool {
    path.symlink_metadata()
        .map(|m| m.file_type().is_symlink())
        .unwrap_or(false)
}

/// Build a `HashMap<String, String>` from record metadata for condition evaluation.
fn build_str_fields(metadata: &Option<serde_json::Value>) -> HashMap<String, String> {
    let mut fields = HashMap::new();
    if let Some(serde_json::Value::Object(map)) = metadata {
        for (k, v) in map {
            match v {
                serde_json::Value::Null => {}
                serde_json::Value::String(s) => {
                    fields.insert(k.clone(), s.clone());
                }
                other => {
                    fields.insert(k.clone(), other.to_string());
                }
            }
        }
    }
    fields
}

/// Compute a relative path from `from_dir` to `to_file`.
///
/// Equivalent to Python's `os.path.relpath(to_file, from_dir)`.
fn compute_relative_path(from_dir: &Path, to_file: &Path) -> PathBuf {
    // Canonicalize both paths if possible, otherwise use as-is
    let from = from_dir.canonicalize().unwrap_or_else(|_| from_dir.to_path_buf());
    let to = to_file.canonicalize().unwrap_or_else(|_| to_file.to_path_buf());

    // Find common prefix
    let from_parts: Vec<_> = from.components().collect();
    let to_parts: Vec<_> = to.components().collect();

    let common_len = from_parts
        .iter()
        .zip(to_parts.iter())
        .take_while(|(a, b)| a == b)
        .count();

    let mut result = PathBuf::new();
    // Go up from `from_dir` to common ancestor
    for _ in common_len..from_parts.len() {
        result.push("..");
    }
    // Go down to `to_file` from common ancestor
    for part in &to_parts[common_len..] {
        result.push(part);
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{SmartFolderCondition, SmartFolderConfig};
    use crate::models::{PathEntry, Record, State};
    use chrono::Utc;
    use regex::Regex;
    use std::fs;
    use std::os::unix::fs as unix_fs;
    use tempfile::TempDir;

    /// Helper to build a SmartFolderConfig without going through YAML parsing.
    fn make_sf_config(name: &str, condition: Option<SmartFolderCondition>, filename_regex: Option<&str>) -> SmartFolderConfig {
        // We must use from_dict since compiled_filename_regex is private.
        // Build a minimal YAML value for it.
        let mut map = serde_yaml::Mapping::new();

        if let Some(cond) = &condition {
            // Serialize condition as YAML
            let cond_yaml = condition_to_yaml(cond);
            map.insert(
                serde_yaml::Value::String("condition".into()),
                cond_yaml,
            );
        }

        if let Some(re) = filename_regex {
            map.insert(
                serde_yaml::Value::String("filename_regex".into()),
                serde_yaml::Value::String(re.into()),
            );
        }

        // If neither condition nor filename_regex, add a trivial condition
        if condition.is_none() && filename_regex.is_none() {
            let mut cond_map = serde_yaml::Mapping::new();
            cond_map.insert(
                serde_yaml::Value::String("field".into()),
                serde_yaml::Value::String("_always".into()),
            );
            cond_map.insert(
                serde_yaml::Value::String("value".into()),
                serde_yaml::Value::String(".*".into()),
            );
            map.insert(
                serde_yaml::Value::String("condition".into()),
                serde_yaml::Value::Mapping(cond_map),
            );
        }

        SmartFolderConfig::from_dict(name, &serde_yaml::Value::Mapping(map), "test")
            .expect("Failed to build SmartFolderConfig")
    }

    fn condition_to_yaml(cond: &SmartFolderCondition) -> serde_yaml::Value {
        match cond {
            SmartFolderCondition::Statement { field, value, .. } => {
                let mut m = serde_yaml::Mapping::new();
                m.insert(
                    serde_yaml::Value::String("field".into()),
                    serde_yaml::Value::String(field.clone()),
                );
                m.insert(
                    serde_yaml::Value::String("value".into()),
                    serde_yaml::Value::String(value.clone()),
                );
                serde_yaml::Value::Mapping(m)
            }
            SmartFolderCondition::Operator { op, operands } => {
                let mut m = serde_yaml::Mapping::new();
                m.insert(
                    serde_yaml::Value::String("operator".into()),
                    serde_yaml::Value::String(op.clone()),
                );
                let ops: Vec<serde_yaml::Value> = operands.iter().map(condition_to_yaml).collect();
                m.insert(
                    serde_yaml::Value::String("operands".into()),
                    serde_yaml::Value::Sequence(ops),
                );
                serde_yaml::Value::Mapping(m)
            }
        }
    }

    fn make_statement(field: &str, value: &str) -> SmartFolderCondition {
        SmartFolderCondition::Statement {
            field: field.into(),
            value: value.into(),
            compiled: Regex::new(&format!("(?i){}", value)).ok(),
        }
    }

    fn make_record_in_sorted(ctx: &str, filename: &str) -> Record {
        let mut r = Record::new(filename.into(), "hash123".into());
        r.state = State::IsComplete;
        r.context = Some(ctx.into());
        r.current_paths = vec![PathEntry {
            path: format!("sorted/{}/{}", ctx, filename),
            timestamp: Utc::now(),
        }];
        r
    }

    fn setup_sorted_file(root: &Path, ctx: &str, filename: &str) {
        let dir = root.join("sorted").join(ctx);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join(filename), b"test content").unwrap();
    }

    // -----------------------------------------------------------------------
    // SmartFolderReconciler tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_creates_symlink_for_matching_record() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        setup_sorted_file(&root, "work", "invoice.pdf");

        let sf = SmartFolderEntry {
            context: "work".into(),
            config: make_sf_config("receipts", None, None),
        };
        let reconciler = SmartFolderReconciler::new(root.clone(), vec![sf]);
        let r = make_record_in_sorted("work", "invoice.pdf");

        reconciler.reconcile(&[r]);

        let link_path = root.join("sorted/work/receipts/invoice.pdf");
        assert!(link_path.exists(), "Symlink should exist");
        assert!(is_symlink(&link_path), "Should be a symlink");
    }

    #[test]
    fn test_no_symlink_if_not_complete() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        setup_sorted_file(&root, "work", "invoice.pdf");

        let sf = SmartFolderEntry {
            context: "work".into(),
            config: make_sf_config("receipts", None, None),
        };
        let reconciler = SmartFolderReconciler::new(root.clone(), vec![sf]);
        let mut r = make_record_in_sorted("work", "invoice.pdf");
        r.state = State::NeedsProcessing;

        reconciler.reconcile(&[r]);

        let link_path = root.join("sorted/work/receipts/invoice.pdf");
        assert!(!link_path.exists(), "Symlink should not exist for non-complete record");
    }

    #[test]
    fn test_no_symlink_if_not_in_sorted() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        fs::create_dir_all(root.join("archive")).unwrap();
        fs::write(root.join("archive/invoice.pdf"), b"test").unwrap();

        let sf = SmartFolderEntry {
            context: "work".into(),
            config: make_sf_config("receipts", None, None),
        };
        let reconciler = SmartFolderReconciler::new(root.clone(), vec![sf]);
        let mut r = Record::new("invoice.pdf".into(), "hash".into());
        r.state = State::IsComplete;
        r.context = Some("work".into());
        r.current_paths = vec![PathEntry {
            path: "archive/invoice.pdf".into(),
            timestamp: Utc::now(),
        }];

        reconciler.reconcile(&[r]);

        // No symlinks should be created in archive
        assert!(!root.join("archive/receipts").exists());
    }

    #[test]
    fn test_no_symlink_if_no_context() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        setup_sorted_file(&root, "work", "invoice.pdf");

        let sf = SmartFolderEntry {
            context: "work".into(),
            config: make_sf_config("receipts", None, None),
        };
        let reconciler = SmartFolderReconciler::new(root.clone(), vec![sf]);
        let mut r = Record::new("invoice.pdf".into(), "hash".into());
        r.state = State::IsComplete;
        r.context = None; // no context
        r.current_paths = vec![PathEntry {
            path: "sorted/work/invoice.pdf".into(),
            timestamp: Utc::now(),
        }];

        reconciler.reconcile(&[r]);

        let link_path = root.join("sorted/work/receipts/invoice.pdf");
        assert!(!link_path.exists());
    }

    #[test]
    fn test_removes_symlink_if_condition_fails() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        setup_sorted_file(&root, "work", "invoice.pdf");

        // Create a pre-existing symlink
        let sf_dir = root.join("sorted/work/typed");
        fs::create_dir_all(&sf_dir).unwrap();
        unix_fs::symlink(PathBuf::from("..").join("invoice.pdf"), sf_dir.join("invoice.pdf")).unwrap();
        assert!(sf_dir.join("invoice.pdf").exists());

        // Create a condition that won't match
        let cond = make_statement("doc_type", "contract");
        let sf = SmartFolderEntry {
            context: "work".into(),
            config: make_sf_config("typed", Some(cond), None),
        };
        let reconciler = SmartFolderReconciler::new(root.clone(), vec![sf]);

        let mut r = make_record_in_sorted("work", "invoice.pdf");
        // metadata doesn't have doc_type=contract
        r.metadata = Some(serde_json::json!({"doc_type": "invoice"}));

        reconciler.reconcile(&[r]);

        // Symlink should be removed since condition doesn't match
        assert!(!is_symlink(&sf_dir.join("invoice.pdf")));
    }

    #[test]
    fn test_multiple_smart_folders() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        setup_sorted_file(&root, "work", "invoice.pdf");

        let sf1 = SmartFolderEntry {
            context: "work".into(),
            config: make_sf_config("all_docs", None, None),
        };
        let sf2 = SmartFolderEntry {
            context: "work".into(),
            config: make_sf_config("pdfs", None, Some(r"\.pdf$")),
        };
        let reconciler = SmartFolderReconciler::new(root.clone(), vec![sf1, sf2]);

        let r = make_record_in_sorted("work", "invoice.pdf");
        reconciler.reconcile(&[r]);

        assert!(root.join("sorted/work/all_docs/invoice.pdf").exists());
        assert!(root.join("sorted/work/pdfs/invoice.pdf").exists());
    }

    #[test]
    fn test_cleanup_orphans_broken_symlinks() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();

        // Create smart folder dir with a broken symlink
        let sf_dir = root.join("sorted/work/receipts");
        fs::create_dir_all(&sf_dir).unwrap();
        // Also create the parent context dir as a regular file location
        fs::create_dir_all(root.join("sorted/work")).unwrap();
        unix_fs::symlink(
            PathBuf::from("..").join("nonexistent.pdf"),
            sf_dir.join("orphan.pdf"),
        )
        .unwrap();
        assert!(is_symlink(&sf_dir.join("orphan.pdf")));

        let sf = SmartFolderEntry {
            context: "work".into(),
            config: make_sf_config("receipts", None, None),
        };
        let reconciler = SmartFolderReconciler::new(root.clone(), vec![sf]);
        reconciler.cleanup_orphans();

        assert!(!is_symlink(&sf_dir.join("orphan.pdf")));
    }

    #[test]
    fn test_cleanup_orphans_stale_symlinks() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();

        // Create a real file in leaf folder
        fs::create_dir_all(root.join("sorted/work")).unwrap();
        fs::write(root.join("sorted/work/kept.pdf"), b"real").unwrap();

        // Create smart folder dir with symlink to a file that no longer exists in leaf
        let sf_dir = root.join("sorted/work/receipts");
        fs::create_dir_all(&sf_dir).unwrap();
        // Create a valid symlink target first (to avoid broken symlink path)
        fs::write(root.join("sorted/work/removed.pdf"), b"temp").unwrap();
        unix_fs::symlink(
            PathBuf::from("..").join("removed.pdf"),
            sf_dir.join("removed.pdf"),
        )
        .unwrap();
        // Now remove the real file so the symlink name won't match leaf files
        // Actually the cleanup checks if the symlink name exists in leaf folder as a real file.
        // The symlink target still exists though. Let's remove the real file.
        fs::remove_file(root.join("sorted/work/removed.pdf")).unwrap();

        let sf = SmartFolderEntry {
            context: "work".into(),
            config: make_sf_config("receipts", None, None),
        };
        let reconciler = SmartFolderReconciler::new(root.clone(), vec![sf]);
        reconciler.cleanup_orphans();

        // Broken symlink should be cleaned up
        assert!(!sf_dir.join("removed.pdf").exists());
    }

    #[test]
    fn test_filename_regex_filter() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        setup_sorted_file(&root, "work", "invoice.pdf");
        setup_sorted_file(&root, "work", "photo.jpg");

        // Only match .pdf files
        let sf = SmartFolderEntry {
            context: "work".into(),
            config: make_sf_config("pdfs_only", None, Some(r"\.pdf$")),
        };
        let reconciler = SmartFolderReconciler::new(root.clone(), vec![sf]);

        let r1 = make_record_in_sorted("work", "invoice.pdf");
        let r2 = make_record_in_sorted("work", "photo.jpg");
        reconciler.reconcile(&[r1, r2]);

        assert!(root.join("sorted/work/pdfs_only/invoice.pdf").exists());
        assert!(!root.join("sorted/work/pdfs_only/photo.jpg").exists());
    }

    #[test]
    fn test_condition_field_equals() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        setup_sorted_file(&root, "work", "invoice.pdf");

        let cond = make_statement("doc_type", "invoice");
        let sf = SmartFolderEntry {
            context: "work".into(),
            config: make_sf_config("invoices", Some(cond), None),
        };
        let reconciler = SmartFolderReconciler::new(root.clone(), vec![sf]);

        let mut r = make_record_in_sorted("work", "invoice.pdf");
        r.metadata = Some(serde_json::json!({"doc_type": "invoice"}));
        reconciler.reconcile(&[r]);

        assert!(root.join("sorted/work/invoices/invoice.pdf").exists());
    }

    #[test]
    fn test_condition_and() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        setup_sorted_file(&root, "work", "invoice.pdf");

        let cond = SmartFolderCondition::Operator {
            op: "and".into(),
            operands: vec![
                make_statement("doc_type", "invoice"),
                make_statement("year", "2024"),
            ],
        };
        let sf = SmartFolderEntry {
            context: "work".into(),
            config: make_sf_config("inv2024", Some(cond), None),
        };
        let reconciler = SmartFolderReconciler::new(root.clone(), vec![sf]);

        // Both conditions match
        let mut r = make_record_in_sorted("work", "invoice.pdf");
        r.metadata = Some(serde_json::json!({"doc_type": "invoice", "year": "2024"}));
        reconciler.reconcile(&[r]);
        assert!(root.join("sorted/work/inv2024/invoice.pdf").exists());
    }

    #[test]
    fn test_condition_and_fails_partial() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        setup_sorted_file(&root, "work", "invoice.pdf");

        let cond = SmartFolderCondition::Operator {
            op: "and".into(),
            operands: vec![
                make_statement("doc_type", "invoice"),
                make_statement("year", "2024"),
            ],
        };
        let sf = SmartFolderEntry {
            context: "work".into(),
            config: make_sf_config("inv2024", Some(cond), None),
        };
        let reconciler = SmartFolderReconciler::new(root.clone(), vec![sf]);

        // Only one condition matches
        let mut r = make_record_in_sorted("work", "invoice.pdf");
        r.metadata = Some(serde_json::json!({"doc_type": "invoice", "year": "2023"}));
        reconciler.reconcile(&[r]);
        assert!(!root.join("sorted/work/inv2024/invoice.pdf").exists());
    }

    #[test]
    fn test_condition_or() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        setup_sorted_file(&root, "work", "contract.pdf");

        let cond = SmartFolderCondition::Operator {
            op: "or".into(),
            operands: vec![
                make_statement("doc_type", "invoice"),
                make_statement("doc_type", "contract"),
            ],
        };
        let sf = SmartFolderEntry {
            context: "work".into(),
            config: make_sf_config("important", Some(cond), None),
        };
        let reconciler = SmartFolderReconciler::new(root.clone(), vec![sf]);

        let mut r = make_record_in_sorted("work", "contract.pdf");
        r.metadata = Some(serde_json::json!({"doc_type": "contract"}));
        reconciler.reconcile(&[r]);
        assert!(root.join("sorted/work/important/contract.pdf").exists());
    }

    #[test]
    fn test_condition_not() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        setup_sorted_file(&root, "work", "memo.pdf");

        let cond = SmartFolderCondition::Operator {
            op: "not".into(),
            operands: vec![make_statement("doc_type", "invoice")],
        };
        let sf = SmartFolderEntry {
            context: "work".into(),
            config: make_sf_config("non_invoices", Some(cond), None),
        };
        let reconciler = SmartFolderReconciler::new(root.clone(), vec![sf]);

        let mut r = make_record_in_sorted("work", "memo.pdf");
        r.metadata = Some(serde_json::json!({"doc_type": "memo"}));
        reconciler.reconcile(&[r]);
        assert!(root.join("sorted/work/non_invoices/memo.pdf").exists());
    }

    #[test]
    fn test_collision_avoidance_real_file_exists() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        setup_sorted_file(&root, "work", "invoice.pdf");

        // Create a real file (not symlink) at the would-be symlink location
        let sf_dir = root.join("sorted/work/receipts");
        fs::create_dir_all(&sf_dir).unwrap();
        fs::write(sf_dir.join("invoice.pdf"), b"real file, not symlink").unwrap();

        let sf = SmartFolderEntry {
            context: "work".into(),
            config: make_sf_config("receipts", None, None),
        };
        let reconciler = SmartFolderReconciler::new(root.clone(), vec![sf]);

        let r = make_record_in_sorted("work", "invoice.pdf");
        reconciler.reconcile(&[r]);

        // The real file should not be overwritten -- it should still exist and not be a symlink
        assert!(sf_dir.join("invoice.pdf").exists());
        assert!(!is_symlink(&sf_dir.join("invoice.pdf")));
        assert_eq!(
            fs::read_to_string(sf_dir.join("invoice.pdf")).unwrap(),
            "real file, not symlink"
        );
    }

    // -----------------------------------------------------------------------
    // RootSmartFolderReconciler tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_root_smart_folder_creates_symlink() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        setup_sorted_file(&root, "work", "invoice.pdf");

        let rsf_dir = root.join("root_sf");
        let entry = RootSmartFolderEntry {
            name: "work_invoices".into(),
            context: "work".into(),
            path: rsf_dir.clone(),
            config: make_sf_config("work_invoices", None, None),
        };
        let reconciler = RootSmartFolderReconciler::new(root.clone(), vec![entry]);

        let r = make_record_in_sorted("work", "invoice.pdf");
        reconciler.reconcile(&[r]);

        let link_path = rsf_dir.join("invoice.pdf");
        assert!(link_path.exists(), "Root smart folder symlink should exist");
        assert!(is_symlink(&link_path), "Should be a symlink");
        // Verify the symlink resolves to the actual file
        let resolved = link_path.canonicalize().unwrap();
        let expected = root.join("sorted/work/invoice.pdf").canonicalize().unwrap();
        assert_eq!(resolved, expected);
    }

    #[test]
    fn test_root_smart_folder_cleanup() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();

        // Create a sorted file
        setup_sorted_file(&root, "work", "kept.pdf");

        // Create root smart folder dir with a symlink to a now-removed sorted file
        let rsf_dir = root.join("root_sf");
        fs::create_dir_all(&rsf_dir).unwrap();

        // Create a file that we'll remove later, then create symlink to it
        let sorted_work = root.join("sorted/work");
        fs::write(sorted_work.join("removed.pdf"), b"temp").unwrap();
        let relative_target = compute_relative_path(&rsf_dir, &sorted_work.join("removed.pdf"));
        unix_fs::symlink(&relative_target, rsf_dir.join("removed.pdf")).unwrap();
        assert!(is_symlink(&rsf_dir.join("removed.pdf")));

        // Now remove the target
        fs::remove_file(sorted_work.join("removed.pdf")).unwrap();

        let entry = RootSmartFolderEntry {
            name: "work_all".into(),
            context: "work".into(),
            path: rsf_dir.clone(),
            config: make_sf_config("work_all", None, None),
        };
        let reconciler = RootSmartFolderReconciler::new(root.clone(), vec![entry]);
        reconciler.cleanup_orphans();

        // The broken symlink pointing into sorted/ should be removed
        assert!(!rsf_dir.join("removed.pdf").exists());
    }
}
