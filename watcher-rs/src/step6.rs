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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{PathEntry, Record, State};
    use chrono::Utc;
    use std::fs;
    use std::os::unix::fs as unix_fs;
    use tempfile::TempDir;

    /// Create the standard test setup: an audio file in archive/ and a transcript in sorted/.
    fn setup_audio_record(root: &Path, ctx: &str) -> Record {
        // Create archive source audio
        fs::create_dir_all(root.join("archive")).unwrap();
        fs::write(root.join("archive/recording.mp3"), b"audio data").unwrap();

        // Create transcript in sorted/
        let sorted_ctx = root.join("sorted").join(ctx);
        fs::create_dir_all(&sorted_ctx).unwrap();
        fs::write(sorted_ctx.join("recording.txt"), b"transcript text").unwrap();

        // Build record
        let mut r = Record::new("recording.mp3".into(), "hash123".into());
        r.state = State::IsComplete;
        r.context = Some(ctx.into());
        r.source_paths = vec![PathEntry {
            path: "archive/recording.mp3".into(),
            timestamp: Utc::now(),
        }];
        r.current_paths = vec![PathEntry {
            path: format!("sorted/{}/recording.txt", ctx),
            timestamp: Utc::now(),
        }];
        r
    }

    // -----------------------------------------------------------------------
    // Core functionality
    // -----------------------------------------------------------------------

    #[test]
    fn test_creates_audio_link() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        let r = setup_audio_record(&root, "work");

        let mut reconciler = AudioLinkReconciler::new(root.clone());
        reconciler.reconcile(&[r]);

        let link_path = root.join("sorted/work/recording.mp3");
        assert!(link_path.exists(), "Audio link should exist");
        assert!(is_symlink(&link_path), "Should be a symlink");
        // Verify the symlink resolves to the archive file
        let resolved = link_path.canonicalize().unwrap();
        let expected = root.join("archive/recording.mp3").canonicalize().unwrap();
        assert_eq!(resolved, expected);
    }

    #[test]
    fn test_no_link_if_not_audio_origin() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();

        // Create a record with a PDF original (not audio)
        fs::create_dir_all(root.join("archive")).unwrap();
        fs::write(root.join("archive/document.pdf"), b"pdf").unwrap();
        fs::create_dir_all(root.join("sorted/work")).unwrap();
        fs::write(root.join("sorted/work/document.pdf"), b"sorted pdf").unwrap();

        let mut r = Record::new("document.pdf".into(), "hash".into());
        r.state = State::IsComplete;
        r.context = Some("work".into());
        r.source_paths = vec![PathEntry {
            path: "archive/document.pdf".into(),
            timestamp: Utc::now(),
        }];
        r.current_paths = vec![PathEntry {
            path: "sorted/work/document.pdf".into(),
            timestamp: Utc::now(),
        }];

        let mut reconciler = AudioLinkReconciler::new(root.clone());
        reconciler.reconcile(&[r]);

        // No extra symlinks should exist in the sorted folder
        let entries: Vec<_> = fs::read_dir(root.join("sorted/work"))
            .unwrap()
            .flatten()
            .collect();
        assert_eq!(entries.len(), 1, "Only the original PDF should exist");
    }

    #[test]
    fn test_no_link_if_not_complete() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        let mut r = setup_audio_record(&root, "work");
        r.state = State::NeedsProcessing;

        let mut reconciler = AudioLinkReconciler::new(root.clone());
        reconciler.reconcile(&[r]);

        let link_path = root.join("sorted/work/recording.mp3");
        assert!(!link_path.exists(), "Should not create link for non-complete record");
    }

    #[test]
    fn test_no_link_if_source_not_archive() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();

        // Source in inbox instead of archive
        fs::create_dir_all(root.join("inbox")).unwrap();
        fs::write(root.join("inbox/recording.mp3"), b"audio").unwrap();
        fs::create_dir_all(root.join("sorted/work")).unwrap();
        fs::write(root.join("sorted/work/recording.txt"), b"transcript").unwrap();

        let mut r = Record::new("recording.mp3".into(), "hash".into());
        r.state = State::IsComplete;
        r.context = Some("work".into());
        r.source_paths = vec![PathEntry {
            path: "inbox/recording.mp3".into(),
            timestamp: Utc::now(),
        }];
        r.current_paths = vec![PathEntry {
            path: "sorted/work/recording.txt".into(),
            timestamp: Utc::now(),
        }];

        let mut reconciler = AudioLinkReconciler::new(root.clone());
        reconciler.reconcile(&[r]);

        let link_path = root.join("sorted/work/recording.mp3");
        assert!(!link_path.exists(), "Should not create link if source not in archive");
    }

    #[test]
    fn test_collision_avoidance() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        let r = setup_audio_record(&root, "work");

        // Place a real file where the symlink would go
        fs::write(root.join("sorted/work/recording.mp3"), b"real file").unwrap();

        let mut reconciler = AudioLinkReconciler::new(root.clone());
        reconciler.reconcile(&[r]);

        // The real file should not be overwritten
        let link_path = root.join("sorted/work/recording.mp3");
        assert!(!is_symlink(&link_path), "Should not replace a real file with symlink");
        assert_eq!(
            fs::read_to_string(&link_path).unwrap(),
            "real file",
        );
    }

    #[test]
    fn test_cleanup_orphans_removes_stale() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();

        // Create archive file and a symlink pointing to it
        fs::create_dir_all(root.join("archive")).unwrap();
        fs::write(root.join("archive/old_recording.mp3"), b"audio").unwrap();
        fs::create_dir_all(root.join("sorted/work")).unwrap();

        let link_path = root.join("sorted/work/old_recording.mp3");
        let relative_target = compute_relative_path(
            root.join("sorted/work").as_path(),
            &root.join("archive/old_recording.mp3"),
        );
        unix_fs::symlink(&relative_target, &link_path).unwrap();
        assert!(is_symlink(&link_path));

        // Reconcile with empty records -- no expected links
        let mut reconciler = AudioLinkReconciler::new(root.clone());
        reconciler.reconcile(&[]);
        reconciler.cleanup_orphans();

        // The orphaned symlink pointing into archive/ should be removed
        assert!(!link_path.exists(), "Orphaned audio link should be cleaned up");
    }

    #[test]
    fn test_idempotent() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        let r = setup_audio_record(&root, "work");

        let mut reconciler = AudioLinkReconciler::new(root.clone());

        // Run twice
        reconciler.reconcile(&[r.clone()]);
        let link_path = root.join("sorted/work/recording.mp3");
        assert!(is_symlink(&link_path));
        let target_first = fs::read_link(&link_path).unwrap();

        reconciler.reconcile(&[r]);
        assert!(is_symlink(&link_path));
        let target_second = fs::read_link(&link_path).unwrap();

        // Should be identical -- no duplication
        assert_eq!(target_first, target_second);

        // Count symlinks in sorted/work -- should be exactly one audio link + one real file
        let symlinks: Vec<_> = fs::read_dir(root.join("sorted/work"))
            .unwrap()
            .flatten()
            .filter(|e| is_symlink(&e.path()))
            .collect();
        assert_eq!(symlinks.len(), 1, "Should have exactly one audio link symlink");
    }

    #[test]
    fn test_link_in_processed_dir() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();

        // Create archive source audio
        fs::create_dir_all(root.join("archive")).unwrap();
        fs::write(root.join("archive/recording.wav"), b"audio").unwrap();

        // Create transcript in processed/ instead of sorted/
        fs::create_dir_all(root.join("processed/work")).unwrap();
        fs::write(root.join("processed/work/recording.txt"), b"transcript").unwrap();

        let mut r = Record::new("recording.wav".into(), "hash".into());
        r.state = State::IsComplete;
        r.context = Some("work".into());
        r.source_paths = vec![PathEntry {
            path: "archive/recording.wav".into(),
            timestamp: Utc::now(),
        }];
        r.current_paths = vec![PathEntry {
            path: "processed/work/recording.txt".into(),
            timestamp: Utc::now(),
        }];

        let mut reconciler = AudioLinkReconciler::new(root.clone());
        reconciler.reconcile(&[r]);

        let link_path = root.join("processed/work/recording.wav");
        assert!(link_path.exists(), "Audio link should exist in processed/");
        assert!(is_symlink(&link_path));
        let resolved = link_path.canonicalize().unwrap();
        let expected = root.join("archive/recording.wav").canonicalize().unwrap();
        assert_eq!(resolved, expected);
    }

    #[test]
    fn test_rename_updates_link() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();

        // Create archive source audio
        fs::create_dir_all(root.join("archive")).unwrap();
        fs::write(root.join("archive/recording.mp3"), b"audio").unwrap();

        // Create transcript
        fs::create_dir_all(root.join("sorted/work")).unwrap();
        fs::write(root.join("sorted/work/recording.txt"), b"transcript").unwrap();

        let mut r = Record::new("recording.mp3".into(), "hash".into());
        r.state = State::IsComplete;
        r.context = Some("work".into());
        r.source_paths = vec![PathEntry {
            path: "archive/recording.mp3".into(),
            timestamp: Utc::now(),
        }];
        r.current_paths = vec![PathEntry {
            path: "sorted/work/recording.txt".into(),
            timestamp: Utc::now(),
        }];

        let mut reconciler = AudioLinkReconciler::new(root.clone());
        reconciler.reconcile(&[r]);

        let link_path = root.join("sorted/work/recording.mp3");
        assert!(is_symlink(&link_path));

        // Now simulate a rename: transcript moved, create new file at new location
        fs::write(root.join("sorted/work/meeting_notes.txt"), b"renamed transcript").unwrap();
        // Remove old transcript
        fs::remove_file(root.join("sorted/work/recording.txt")).unwrap();

        let mut r2 = Record::new("recording.mp3".into(), "hash".into());
        r2.state = State::IsComplete;
        r2.context = Some("work".into());
        r2.source_paths = vec![PathEntry {
            path: "archive/recording.mp3".into(),
            timestamp: Utc::now(),
        }];
        r2.current_paths = vec![PathEntry {
            path: "sorted/work/meeting_notes.txt".into(),
            timestamp: Utc::now(),
        }];

        reconciler.reconcile(&[r2]);

        // New link should exist
        let new_link = root.join("sorted/work/meeting_notes.mp3");
        assert!(is_symlink(&new_link), "New audio link should exist after rename");

        // Old link is now orphaned -- cleanup should remove it
        reconciler.cleanup_orphans();
        assert!(!root.join("sorted/work/recording.mp3").exists(),
                "Old audio link should be cleaned up after rename");
    }

    #[test]
    fn test_no_link_if_no_current() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();

        // Create archive source audio but no current file
        fs::create_dir_all(root.join("archive")).unwrap();
        fs::write(root.join("archive/recording.mp3"), b"audio").unwrap();

        let mut r = Record::new("recording.mp3".into(), "hash".into());
        r.state = State::IsComplete;
        r.context = Some("work".into());
        r.source_paths = vec![PathEntry {
            path: "archive/recording.mp3".into(),
            timestamp: Utc::now(),
        }];
        // No current_paths

        let mut reconciler = AudioLinkReconciler::new(root.clone());
        reconciler.reconcile(&[r]);

        // No symlinks should be created anywhere
        assert!(!root.join("sorted").exists() || {
            let count = walkdir_symlink_count(&root.join("sorted"));
            count == 0
        });
    }

    /// Helper: count symlinks recursively in a directory.
    fn walkdir_symlink_count(dir: &Path) -> usize {
        if !dir.exists() {
            return 0;
        }
        let mut count = 0;
        for entry in fs::read_dir(dir).unwrap().flatten() {
            let p = entry.path();
            if is_symlink(&p) {
                count += 1;
            } else if p.is_dir() {
                count += walkdir_symlink_count(&p);
            }
        }
        count
    }
}
