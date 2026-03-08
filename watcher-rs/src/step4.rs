//! Filesystem reconciliation for document watcher v2.
//!
//! Performs actual file moves based on temporary fields set by [`crate::step2::reconcile`].

use std::fs;
use std::path::{Path, PathBuf};

use chrono::Utc;
use tracing::warn;
use uuid::Uuid;

use crate::models::{PathEntry, Record, State};

/// Move a file from `src` to `dest`, handling collisions and parent dirs.
///
/// Creates parent directories as needed.  If `dest` already exists, appends a
/// UUID fragment (`_<hex8>`) to the filename.  Uses [`fs::rename`] for an atomic
/// move (same filesystem).
///
/// Returns the actual destination path on success, or `None` if the source file
/// was not found.
pub fn move_file(src: &Path, dest: &Path) -> Option<PathBuf> {
    if !src.exists() {
        warn!("Source file not found, skipping: {}", src.display());
        return None;
    }

    if let Some(parent) = dest.parent() {
        if let Err(e) = fs::create_dir_all(parent) {
            warn!("Failed to create parent dirs for {}: {}", dest.display(), e);
            return None;
        }
    }

    let actual_dest = if dest.exists() {
        let stem = dest
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("file");
        let suffix = dest
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| format!(".{}", e))
            .unwrap_or_default();
        let hex8 = &Uuid::new_v4().to_string().replace('-', "")[..8];
        dest.with_file_name(format!("{}_{}{}", stem, hex8, suffix))
    } else {
        dest.to_path_buf()
    };

    match fs::rename(src, &actual_dest) {
        Ok(()) => Some(actual_dest),
        Err(e) => {
            warn!(
                "Failed to move {} -> {}: {}",
                src.display(),
                actual_dest.display(),
                e
            );
            None
        }
    }
}

/// Build a `void/` destination path with today's UTC date as subdirectory.
///
/// Layout: `{root}/void/{YYYY-MM-DD}/{location}/{location_path}/{filename}`
pub fn void_dest(root: &Path, path: &str) -> PathBuf {
    let (location, location_path, filename) = Record::decompose_path(path);
    let date_dir = Utc::now().format("%Y-%m-%d").to_string();
    if location_path.is_empty() {
        root.join("void").join(&date_dir).join(&location).join(&filename)
    } else {
        root.join("void")
            .join(&date_dir)
            .join(&location)
            .join(&location_path)
            .join(&filename)
    }
}

/// Performs filesystem moves based on [`Record`] temporary fields.
pub struct FilesystemReconciler {
    pub root: PathBuf,
}

impl FilesystemReconciler {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    /// Process all records, performing filesystem operations for each.
    pub fn reconcile(&self, records: &mut [Record]) {
        for record in records.iter_mut() {
            self.reconcile_one(record);
        }
    }

    fn reconcile_one(&self, record: &mut Record) {
        // 1. source_reference: move source to archive/error/missing
        if let Some(ref source_ref) = record.source_reference.clone() {
            let src = self.root.join(source_ref);
            let (_, _, filename) = Record::decompose_path(source_ref);
            let dest = match record.state {
                State::HasError => self.root.join("error").join(&filename),
                State::IsMissing => self.root.join("missing").join(&filename),
                _ => self.root.join("archive").join(&filename),
            };

            let actual = move_file(&src, &dest);
            if let Some(actual_path) = actual {
                // Update source_paths to reflect the new location
                let dest_path = actual_path
                    .strip_prefix(&self.root)
                    .map(|p| p.to_string_lossy().to_string())
                    .unwrap_or_else(|_| actual_path.to_string_lossy().to_string());
                let now = Utc::now();

                if let Some(idx) = record
                    .source_paths
                    .iter()
                    .position(|pe| pe.path == *source_ref)
                {
                    let old_entry = record.source_paths[idx].clone();
                    record.missing_source_paths.push(old_entry);
                    record.source_paths[idx] = PathEntry {
                        path: dest_path,
                        timestamp: now,
                    };
                }
            } else {
                // Source file is gone -- remove from source_paths
                if let Some(idx) = record
                    .source_paths
                    .iter()
                    .position(|pe| pe.path == *source_ref)
                {
                    let old_entry = record.source_paths.remove(idx);
                    record.missing_source_paths.push(old_entry);
                }
            }
        }

        // 2. duplicate_sources: each to duplicates/{location}/{location_path}/{filename}
        for dup_path in record.duplicate_sources.clone() {
            let src = self.root.join(&dup_path);
            let (location, location_path, filename) = Record::decompose_path(&dup_path);
            let dest = if location_path.is_empty() {
                self.root.join("duplicates").join(&location).join(&filename)
            } else {
                self.root
                    .join("duplicates")
                    .join(&location)
                    .join(&location_path)
                    .join(&filename)
            };
            move_file(&src, &dest);
        }

        // 3. current_reference + target_path: move current to target
        if let (Some(ref current_ref), Some(ref target)) = (
            record.current_reference.clone(),
            record.target_path.clone(),
        ) {
            let src = self.root.join(current_ref);
            let dest = self.root.join(target);
            let actual = move_file(&src, &dest);

            if let Some(actual_path) = actual {
                let actual_rel = actual_path
                    .strip_prefix(&self.root)
                    .map(|p| p.to_string_lossy().to_string())
                    .unwrap_or_else(|_| actual_path.to_string_lossy().to_string());
                let now = Utc::now();

                if let Some(idx) = record
                    .current_paths
                    .iter()
                    .position(|pe| pe.path == *current_ref)
                {
                    record.current_paths[idx] = PathEntry {
                        path: actual_rel,
                        timestamp: now,
                    };
                }

                // Clean up .meta.json sidecar if moving from .output
                let loc = Record::decompose_path(current_ref).0;
                if loc == ".output" {
                    let sidecar = src
                        .parent()
                        .map(|p| p.join(format!("{}.meta.json", src.file_name().unwrap_or_default().to_string_lossy())));
                    if let Some(sidecar_path) = sidecar {
                        if sidecar_path.exists() {
                            let _ = fs::remove_file(&sidecar_path);
                        }
                    }
                }
            } else {
                // Current file is gone
                if let Some(idx) = record
                    .current_paths
                    .iter()
                    .position(|pe| pe.path == *current_ref)
                {
                    let old_entry = record.current_paths.remove(idx);
                    record.missing_current_paths.push(old_entry);
                }
            }
        }

        // 4. deleted_paths: each to void/{date}/{location}/{filename}
        for del_path in record.deleted_paths.clone() {
            let src = self.root.join(&del_path);
            let location = Record::decompose_path(&del_path).0;
            let dest = void_dest(&self.root, &del_path);

            if move_file(&src, &dest).is_some() {
                // Clean up .meta.json sidecar if deleting from .output
                if location == ".output" {
                    let sidecar = src
                        .parent()
                        .map(|p| p.join(format!("{}.meta.json", src.file_name().unwrap_or_default().to_string_lossy())));
                    if let Some(sidecar_path) = sidecar {
                        if sidecar_path.exists() {
                            let _ = fs::remove_file(&sidecar_path);
                        }
                    }
                }
            }
        }

        // 5. needs_deletion: all source + current paths to void
        if record.state == State::NeedsDeletion {
            for pe in &record.source_paths {
                let src = self.root.join(&pe.path);
                let dest = void_dest(&self.root, &pe.path);
                move_file(&src, &dest);
            }

            for pe in &record.current_paths {
                let src = self.root.join(&pe.path);
                let dest = void_dest(&self.root, &pe.path);
                move_file(&src, &dest);
            }
        }
    }
}
