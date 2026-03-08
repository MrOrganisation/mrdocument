//! Audio link reconciler -- places symlinks to source audio files alongside transcripts.
//!
//! When an audio file is transcribed, a symlink named after the transcript is
//! placed next to it in `sorted/` or `processed/`, pointing back to the source
//! audio in `archive/`.

use std::collections::HashSet;
use std::fs;
use std::os::unix::fs as unix_fs;
use std::path::{Path, PathBuf};

use tracing::{debug, info, warn};

use crate::models::{Record, State};

/// Audio file extensions (same set as `crate::step3::AUDIO_EXTENSIONS`).
pub const AUDIO_EXTENSIONS: &[&str] = &[
    ".flac", ".wav", ".mp3", ".ogg", ".webm",
    ".mp4", ".m4a", ".mkv", ".avi", ".mov",
];

/// Maintains symlinks from transcript files to their source audio in `archive/`.
pub struct AudioLinkReconciler {
    pub root: PathBuf,
    pub archive_dir: PathBuf,
    /// Tracks expected symlink paths after reconcile, used by cleanup_orphans.
    _expected_links: HashSet<PathBuf>,
}

/// Locations where audio links are created.
const LINK_LOCATIONS: &[&str] = &["sorted", "processed"];

impl AudioLinkReconciler {
    pub fn new(root: PathBuf) -> Self {
        let archive_dir = root.join("archive");
        Self {
            root,
            archive_dir,
            _expected_links: HashSet::new(),
        }
    }

    /// Ensure audio links exist for all completed audio-origin records.
    ///
    /// For each `IS_COMPLETE` record in `sorted/` or `processed/` whose
    /// `original_filename` has an audio extension, creates a symlink named
    /// after the transcript file (same stem + audio extension) pointing to
    /// the source audio in `archive/`.
    pub fn reconcile(&mut self, records: &[Record]) {
        self._expected_links.clear();

        for record in records {
            if record.state != State::IsComplete {
                continue;
            }

            let current_loc = match record.current_location() {
                Some(loc) => loc,
                None => continue,
            };
            if !LINK_LOCATIONS.contains(&current_loc.as_str()) {
                continue;
            }

            // Check if this record originated from an audio file
            let orig_ext = Path::new(&record.original_filename)
                .extension()
                .and_then(|e| e.to_str())
                .map(|e| format!(".{}", e.to_ascii_lowercase()))
                .unwrap_or_default();

            if !AUDIO_EXTENSIONS.contains(&orig_ext.as_str()) {
                continue;
            }

            // Need source in archive
            let source_file = match record.source_file() {
                Some(sf) => sf,
                None => continue,
            };
            let source_loc = Record::decompose_path(&source_file.path).0;
            if source_loc != "archive" {
                continue;
            }

            // Current file (transcript) in sorted/ or processed/
            let current_file = match record.current_file() {
                Some(cf) => cf,
                None => continue,
            };
            let current_path = self.root.join(&current_file.path);
            if !current_path.exists() {
                continue;
            }

            // Symlink: same stem as transcript + original audio extension
            let link_name = format!(
                "{}{}",
                current_path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or(""),
                orig_ext
            );
            let link_path = current_path
                .parent()
                .unwrap_or(Path::new("."))
                .join(&link_name);

            self._expected_links.insert(link_path.clone());

            // Compute relative target from link location to archive source
            let source_abs = self.root.join(&source_file.path);
            let target = compute_relative_path(
                link_path.parent().unwrap_or(Path::new(".")),
                &source_abs,
            );
            let target_str = target.to_string_lossy().to_string();

            if is_symlink(&link_path) {
                // Check if target matches
                if let Ok(existing_target) = fs::read_link(&link_path) {
                    if existing_target.to_string_lossy() == target_str {
                        continue;
                    }
                }
                // Target changed -- recreate
                let _ = fs::remove_file(&link_path);
            } else if link_path.exists() {
                // Non-symlink file with same name, don't overwrite
                warn!("Cannot create audio link, file exists: {}", link_path.display());
                continue;
            }

            match unix_fs::symlink(&target, &link_path) {
                Ok(()) => {
                    debug!(
                        "Created audio link: {} -> {}",
                        link_path.display(),
                        target_str,
                    );
                }
                Err(e) => {
                    warn!(
                        "Failed to create audio link {}: {}",
                        link_path.display(),
                        e
                    );
                }
            }
        }
    }

    /// Remove audio link symlinks that are no longer expected.
    ///
    /// Must be called after [`reconcile`](Self::reconcile) so that the expected
    /// links set is populated.  Walks `sorted/` and `processed/` for symlinks
    /// whose target resolves to `archive/` and removes any not in the expected
    /// set.
    pub fn cleanup_orphans(&self) {
        let archive_resolved = self
            .archive_dir
            .canonicalize()
            .unwrap_or_else(|_| self.archive_dir.clone());

        for loc in LINK_LOCATIONS {
            let scan_dir = self.root.join(loc);
            if !scan_dir.exists() {
                continue;
            }

            self.walk_and_cleanup(&scan_dir, &archive_resolved);
        }
    }

    fn walk_and_cleanup(&self, dir: &Path, archive_resolved: &Path) {
        let entries = match fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(_) => return,
        };

        for entry in entries.flatten() {
            let link_path = entry.path();

            if link_path.is_dir() && !is_symlink(&link_path) {
                self.walk_and_cleanup(&link_path, archive_resolved);
                continue;
            }

            if !is_symlink(&link_path) {
                continue;
            }
            if self._expected_links.contains(&link_path) {
                continue;
            }

            // Check if this symlink points into archive/
            let raw_target = match fs::read_link(&link_path) {
                Ok(t) => t,
                Err(_) => continue,
            };
            let resolved = link_path
                .parent()
                .unwrap_or(Path::new("."))
                .join(&raw_target);
            let resolved = resolved
                .canonicalize()
                .unwrap_or(resolved);

            if !resolved.starts_with(archive_resolved) {
                continue; // Not an audio link (e.g. smart folder symlink)
            }

            // Audio link not in expected set -- remove
            if fs::remove_file(&link_path).is_ok() {
                debug!("Removed orphaned audio link: {}", link_path.display());
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

/// Compute a relative path from `from_dir` to `to_file`.
///
/// Equivalent to Python's `os.path.relpath(to_file, from_dir)`.
fn compute_relative_path(from_dir: &Path, to_file: &Path) -> PathBuf {
    let from = from_dir
        .canonicalize()
        .unwrap_or_else(|_| from_dir.to_path_buf());
    let to = to_file
        .canonicalize()
        .unwrap_or_else(|_| to_file.to_path_buf());

    let from_parts: Vec<_> = from.components().collect();
    let to_parts: Vec<_> = to.components().collect();

    let common_len = from_parts
        .iter()
        .zip(to_parts.iter())
        .take_while(|(a, b)| a == b)
        .count();

    let mut result = PathBuf::new();
    for _ in common_len..from_parts.len() {
        result.push("..");
    }
    for part in &to_parts[common_len..] {
        result.push(part);
    }

    result
}
