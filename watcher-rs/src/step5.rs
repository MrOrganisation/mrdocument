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

use tracing::{error, info, warn};

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
