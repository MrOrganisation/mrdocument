//! Pre-filter for document watcher.
//!
//! Runs before step 1 to move files with unsupported extensions to `error/`.
//! Prevents unsupported files from entering the pipeline and accumulating
//! as permanently-failing records.

use std::collections::HashSet;
use std::fs;
use std::path::Path;

use once_cell::sync::Lazy;
use tracing::{error, info};
use uuid::Uuid;

use crate::step3::{AUDIO_EXTENSIONS, DOCUMENT_EXTENSIONS};

/// Union of all supported file extensions (documents + audio).
pub static SUPPORTED_EXTENSIONS: Lazy<HashSet<&'static str>> = Lazy::new(|| {
    let mut set = HashSet::new();
    for ext in DOCUMENT_EXTENSIONS.iter() {
        set.insert(*ext);
    }
    for ext in AUDIO_EXTENSIONS.iter() {
        set.insert(*ext);
    }
    set
});

/// Top-level directories excluded from pre-filter scanning.
const EXCLUDED_DIRS: &[&str] = &["error", "void"];

/// Config filenames that are allowed inside `sorted/{context}/`.
const CONFIG_FILENAMES: &[&str] = &["context.yaml", "smartfolders.yaml", "generated.yaml"];

/// Move files with unsupported extensions to `error/`.
///
/// Scans all top-level directories except `error/`, `void/`, and hidden dirs.
/// Uses recursive scanning to catch files in subdirectories (e.g. `sorted/context/`).
///
/// Returns the number of files moved.
pub fn prefilter(root: &Path) -> usize {
    let mut moved: usize = 0;
    let error_dir = root.join("error");

    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(e) => {
            error!("Failed to read root directory {}: {}", root.display(), e);
            return 0;
        }
    };

    for entry in entries.flatten() {
        let dirpath = entry.path();

        // Only consider directories, skip symlinks
        if !dirpath.is_dir() || dirpath.is_symlink() {
            continue;
        }

        let dir_name = match dirpath.file_name().and_then(|n| n.to_str()) {
            Some(name) => name.to_string(),
            None => continue,
        };

        // Skip hidden dirs
        if dir_name.starts_with('.') {
            continue;
        }

        // Skip excluded dirs
        if EXCLUDED_DIRS.contains(&dir_name.as_str()) {
            continue;
        }

        // Recursively walk the directory
        for file in walk_recursive(&dirpath) {
            if !file.is_file() || file.is_symlink() {
                continue;
            }

            let filename = match file.file_name().and_then(|n| n.to_str()) {
                Some(name) => name.to_string(),
                None => continue,
            };

            // Skip hidden and temp-prefixed files
            if filename.starts_with('.') || filename.starts_with('~') {
                continue;
            }

            // Skip .tmp extension
            if let Some(ext) = file.extension().and_then(|e| e.to_str()) {
                if ext.eq_ignore_ascii_case("tmp") {
                    continue;
                }
            }

            // Skip config files in sorted/{context}/
            if let Ok(rel) = file.strip_prefix(root) {
                let rel_str = rel.to_string_lossy();
                let parts: Vec<&str> = rel_str.split('/').collect();
                if parts.len() == 3
                    && parts[0] == "sorted"
                    && CONFIG_FILENAMES
                        .iter()
                        .any(|c| c.eq_ignore_ascii_case(parts[2]))
                {
                    continue;
                }
            }

            // Check extension
            let ext_lower = file
                .extension()
                .and_then(|e| e.to_str())
                .map(|e| format!(".{}", e.to_ascii_lowercase()))
                .unwrap_or_default();

            if !ext_lower.is_empty() && SUPPORTED_EXTENSIONS.contains(ext_lower.as_str()) {
                continue;
            }
            // Files with no extension also fall through as unsupported

            // Move to error/
            if let Err(e) = fs::create_dir_all(&error_dir) {
                error!("Failed to create error directory: {}", e);
                continue;
            }

            let dest = error_dir.join(&filename);
            let dest = if dest.exists() {
                let stem = file
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("file");
                let suffix = file
                    .extension()
                    .and_then(|e| e.to_str())
                    .map(|e| format!(".{}", e))
                    .unwrap_or_default();
                let hex8 = &Uuid::new_v4().to_string().replace('-', "")[..8];
                error_dir.join(format!("{}_{}{}", stem, hex8, suffix))
            } else {
                dest
            };

            match fs::rename(&file, &dest) {
                Ok(()) => {
                    if let Ok(rel) = file.strip_prefix(root) {
                        info!("Unsupported file moved to error: {}", rel.display());
                    }
                    moved += 1;
                }
                Err(e) => {
                    error!("Failed to move unsupported file {}: {}", filename, e);
                }
            }
        }
    }

    moved
}

/// Recursively walk a directory and collect all file paths.
fn walk_recursive(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut results = Vec::new();
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() && !path.is_symlink() {
                results.extend(walk_recursive(&path));
            } else {
                results.push(path);
            }
        }
    }
    results
}
