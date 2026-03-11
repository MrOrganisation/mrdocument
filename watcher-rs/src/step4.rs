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

/// Today's UTC date as `YYYY-MM-DD` string (used as subdirectory name).
pub fn today_date_dir() -> String {
    Utc::now().format("%Y-%m-%d").to_string()
}

/// Build a `void/` destination path with today's UTC date as subdirectory.
///
/// Layout: `{root}/void/{YYYY-MM-DD}/{location}/{location_path}/{filename}`
pub fn void_dest(root: &Path, path: &str) -> PathBuf {
    let (location, location_path, filename) = Record::decompose_path(path);
    let date_dir = today_date_dir();
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
                State::HasError => self.root.join("error").join(today_date_dir()).join(&filename),
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

        // 2. duplicate_sources: each to duplicates/{date}/{location}/{location_path}/{filename}
        for dup_path in record.duplicate_sources.clone() {
            let src = self.root.join(&dup_path);
            let (location, location_path, filename) = Record::decompose_path(&dup_path);
            let date_dir = today_date_dir();
            let dest = if location_path.is_empty() {
                self.root.join("duplicates").join(&date_dir).join(&location).join(&filename)
            } else {
                self.root
                    .join("duplicates")
                    .join(&date_dir)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{PathEntry, Record, State};
    use chrono::Utc;
    use std::fs;
    use tempfile::TempDir;

    fn make_record(filename: &str, hash: &str) -> Record {
        let mut r = Record::new(filename.into(), hash.into());
        r.state = State::IsComplete;
        r
    }

    // -----------------------------------------------------------------------
    // move_file tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_move_file_basic() {
        let tmp = TempDir::new().unwrap();
        let src = tmp.path().join("source.pdf");
        let dest = tmp.path().join("dest.pdf");
        fs::write(&src, b"hello").unwrap();

        let result = move_file(&src, &dest);
        assert_eq!(result, Some(dest.clone()));
        assert!(!src.exists());
        assert!(dest.exists());
        assert_eq!(fs::read_to_string(&dest).unwrap(), "hello");
    }

    #[test]
    fn test_move_file_creates_parent_dirs() {
        let tmp = TempDir::new().unwrap();
        let src = tmp.path().join("source.pdf");
        let dest = tmp.path().join("a/b/c/dest.pdf");
        fs::write(&src, b"content").unwrap();

        let result = move_file(&src, &dest);
        assert_eq!(result, Some(dest.clone()));
        assert!(dest.exists());
        assert_eq!(fs::read_to_string(&dest).unwrap(), "content");
    }

    #[test]
    fn test_move_file_collision() {
        let tmp = TempDir::new().unwrap();
        let src = tmp.path().join("source.pdf");
        let dest = tmp.path().join("dest.pdf");
        fs::write(&src, b"new").unwrap();
        fs::write(&dest, b"existing").unwrap();

        let result = move_file(&src, &dest);
        assert!(result.is_some());
        let actual = result.unwrap();
        // Should not be the original dest (collision avoidance)
        assert_ne!(actual, dest);
        // Should be in the same directory with a UUID suffix
        assert_eq!(actual.parent(), dest.parent());
        // Original should still exist with its content
        assert_eq!(fs::read_to_string(&dest).unwrap(), "existing");
        // Moved file should have new content
        assert_eq!(fs::read_to_string(&actual).unwrap(), "new");
        // The filename should contain the original stem and extension
        let name = actual.file_name().unwrap().to_str().unwrap();
        assert!(name.starts_with("dest_"));
        assert!(name.ends_with(".pdf"));
    }

    #[test]
    fn test_move_file_source_missing() {
        let tmp = TempDir::new().unwrap();
        let src = tmp.path().join("nonexistent.pdf");
        let dest = tmp.path().join("dest.pdf");

        let result = move_file(&src, &dest);
        assert_eq!(result, None);
    }

    // -----------------------------------------------------------------------
    // void_dest tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_void_dest_simple() {
        let tmp = TempDir::new().unwrap();
        let result = void_dest(tmp.path(), "archive/file.pdf");
        let today = Utc::now().format("%Y-%m-%d").to_string();
        let expected = tmp.path().join("void").join(&today).join("archive").join("file.pdf");
        assert_eq!(result, expected);
    }

    #[test]
    fn test_void_dest_with_location_path() {
        let tmp = TempDir::new().unwrap();
        let result = void_dest(tmp.path(), "sorted/work/file.pdf");
        let today = Utc::now().format("%Y-%m-%d").to_string();
        let expected = tmp.path().join("void").join(&today).join("sorted").join("work").join("file.pdf");
        assert_eq!(result, expected);
    }

    // -----------------------------------------------------------------------
    // FilesystemReconciler: source_reference tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_source_reference_to_archive() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        fs::create_dir_all(root.join("inbox")).unwrap();
        fs::write(root.join("inbox/test.pdf"), b"data").unwrap();

        let reconciler = FilesystemReconciler::new(root.clone());
        let mut r = make_record("test.pdf", "hash1");
        r.source_reference = Some("inbox/test.pdf".into());
        r.source_paths = vec![PathEntry {
            path: "inbox/test.pdf".into(),
            timestamp: Utc::now(),
        }];

        let mut records = vec![r];
        reconciler.reconcile(&mut records);

        assert!(!root.join("inbox/test.pdf").exists());
        assert!(root.join("archive/test.pdf").exists());
        // source_paths should be updated
        assert_eq!(records[0].source_paths[0].path, "archive/test.pdf");
    }

    #[test]
    fn test_source_reference_to_error() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        fs::create_dir_all(root.join("inbox")).unwrap();
        fs::write(root.join("inbox/test.pdf"), b"data").unwrap();

        let reconciler = FilesystemReconciler::new(root.clone());
        let mut r = make_record("test.pdf", "hash1");
        r.state = State::HasError;
        r.source_reference = Some("inbox/test.pdf".into());
        r.source_paths = vec![PathEntry {
            path: "inbox/test.pdf".into(),
            timestamp: Utc::now(),
        }];

        let mut records = vec![r];
        reconciler.reconcile(&mut records);

        assert!(!root.join("inbox/test.pdf").exists());
        assert!(root.join("error").join(today_date_dir()).join("test.pdf").exists());
    }

    #[test]
    fn test_source_reference_to_missing() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        fs::create_dir_all(root.join("inbox")).unwrap();
        fs::write(root.join("inbox/test.pdf"), b"data").unwrap();

        let reconciler = FilesystemReconciler::new(root.clone());
        let mut r = make_record("test.pdf", "hash1");
        r.state = State::IsMissing;
        r.source_reference = Some("inbox/test.pdf".into());
        r.source_paths = vec![PathEntry {
            path: "inbox/test.pdf".into(),
            timestamp: Utc::now(),
        }];

        let mut records = vec![r];
        reconciler.reconcile(&mut records);

        assert!(!root.join("inbox/test.pdf").exists());
        assert!(root.join("missing/test.pdf").exists());
    }

    #[test]
    fn test_source_reference_updates_source_paths() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        fs::create_dir_all(root.join("inbox")).unwrap();
        fs::write(root.join("inbox/doc.pdf"), b"data").unwrap();

        let reconciler = FilesystemReconciler::new(root.clone());
        let mut r = make_record("doc.pdf", "hash1");
        r.source_reference = Some("inbox/doc.pdf".into());
        r.source_paths = vec![PathEntry {
            path: "inbox/doc.pdf".into(),
            timestamp: Utc::now(),
        }];

        let mut records = vec![r];
        reconciler.reconcile(&mut records);

        assert_eq!(records[0].source_paths.len(), 1);
        assert_eq!(records[0].source_paths[0].path, "archive/doc.pdf");
        // Old path should be in missing_source_paths
        assert_eq!(records[0].missing_source_paths.len(), 1);
        assert_eq!(records[0].missing_source_paths[0].path, "inbox/doc.pdf");
    }

    #[test]
    fn test_source_reference_collision() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        fs::create_dir_all(root.join("inbox")).unwrap();
        fs::create_dir_all(root.join("archive")).unwrap();
        fs::write(root.join("inbox/test.pdf"), b"new").unwrap();
        fs::write(root.join("archive/test.pdf"), b"existing").unwrap();

        let reconciler = FilesystemReconciler::new(root.clone());
        let mut r = make_record("test.pdf", "hash1");
        r.source_reference = Some("inbox/test.pdf".into());
        r.source_paths = vec![PathEntry {
            path: "inbox/test.pdf".into(),
            timestamp: Utc::now(),
        }];

        let mut records = vec![r];
        reconciler.reconcile(&mut records);

        // Original archive file should still exist
        assert!(root.join("archive/test.pdf").exists());
        // Source file should be gone from inbox
        assert!(!root.join("inbox/test.pdf").exists());
        // source_paths should have a collision-suffixed path
        let new_path = &records[0].source_paths[0].path;
        assert!(new_path.starts_with("archive/test_"));
        assert!(new_path.ends_with(".pdf"));
    }

    #[test]
    fn test_source_reference_missing_file() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        // Don't create the source file -- it's gone

        let reconciler = FilesystemReconciler::new(root.clone());
        let mut r = make_record("test.pdf", "hash1");
        r.source_reference = Some("inbox/test.pdf".into());
        r.source_paths = vec![PathEntry {
            path: "inbox/test.pdf".into(),
            timestamp: Utc::now(),
        }];

        let mut records = vec![r];
        reconciler.reconcile(&mut records);

        // source_paths should be emptied, moved to missing_source_paths
        assert!(records[0].source_paths.is_empty());
        assert_eq!(records[0].missing_source_paths.len(), 1);
        assert_eq!(records[0].missing_source_paths[0].path, "inbox/test.pdf");
    }

    // -----------------------------------------------------------------------
    // FilesystemReconciler: duplicate_sources tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_duplicate_sources_moved() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        fs::create_dir_all(root.join("inbox")).unwrap();
        fs::write(root.join("inbox/dup.pdf"), b"duplicate").unwrap();

        let reconciler = FilesystemReconciler::new(root.clone());
        let mut r = make_record("test.pdf", "hash1");
        r.duplicate_sources = vec!["inbox/dup.pdf".into()];

        let mut records = vec![r];
        reconciler.reconcile(&mut records);

        assert!(!root.join("inbox/dup.pdf").exists());
        assert!(root.join("duplicates").join(today_date_dir()).join("inbox/dup.pdf").exists());
    }

    // -----------------------------------------------------------------------
    // FilesystemReconciler: current_reference + target_path tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_current_reference_target_path_move() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        fs::create_dir_all(root.join(".output")).unwrap();
        fs::write(root.join(".output/uuid123"), b"processed").unwrap();

        let reconciler = FilesystemReconciler::new(root.clone());
        let mut r = make_record("test.pdf", "hash1");
        r.current_reference = Some(".output/uuid123".into());
        r.target_path = Some("sorted/work/test.pdf".into());
        r.current_paths = vec![PathEntry {
            path: ".output/uuid123".into(),
            timestamp: Utc::now(),
        }];

        let mut records = vec![r];
        reconciler.reconcile(&mut records);

        assert!(!root.join(".output/uuid123").exists());
        assert!(root.join("sorted/work/test.pdf").exists());
        assert_eq!(
            fs::read_to_string(root.join("sorted/work/test.pdf")).unwrap(),
            "processed"
        );
    }

    #[test]
    fn test_current_reference_updates_current_paths() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        fs::create_dir_all(root.join(".output")).unwrap();
        fs::write(root.join(".output/uuid123"), b"data").unwrap();

        let reconciler = FilesystemReconciler::new(root.clone());
        let mut r = make_record("test.pdf", "hash1");
        r.current_reference = Some(".output/uuid123".into());
        r.target_path = Some("sorted/work/result.pdf".into());
        r.current_paths = vec![PathEntry {
            path: ".output/uuid123".into(),
            timestamp: Utc::now(),
        }];

        let mut records = vec![r];
        reconciler.reconcile(&mut records);

        assert_eq!(records[0].current_paths.len(), 1);
        assert_eq!(records[0].current_paths[0].path, "sorted/work/result.pdf");
    }

    #[test]
    fn test_current_reference_collision() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        fs::create_dir_all(root.join(".output")).unwrap();
        fs::create_dir_all(root.join("sorted/work")).unwrap();
        fs::write(root.join(".output/uuid123"), b"new").unwrap();
        fs::write(root.join("sorted/work/test.pdf"), b"existing").unwrap();

        let reconciler = FilesystemReconciler::new(root.clone());
        let mut r = make_record("test.pdf", "hash1");
        r.current_reference = Some(".output/uuid123".into());
        r.target_path = Some("sorted/work/test.pdf".into());
        r.current_paths = vec![PathEntry {
            path: ".output/uuid123".into(),
            timestamp: Utc::now(),
        }];

        let mut records = vec![r];
        reconciler.reconcile(&mut records);

        // Original should still exist
        assert_eq!(
            fs::read_to_string(root.join("sorted/work/test.pdf")).unwrap(),
            "existing"
        );
        // current_paths should have collision-suffixed path
        let new_path = &records[0].current_paths[0].path;
        assert!(new_path.starts_with("sorted/work/test_"));
        assert!(new_path.ends_with(".pdf"));
    }

    #[test]
    fn test_current_reference_missing_file() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        // Don't create the current file -- it's gone

        let reconciler = FilesystemReconciler::new(root.clone());
        let mut r = make_record("test.pdf", "hash1");
        r.current_reference = Some(".output/uuid123".into());
        r.target_path = Some("sorted/work/test.pdf".into());
        r.current_paths = vec![PathEntry {
            path: ".output/uuid123".into(),
            timestamp: Utc::now(),
        }];

        let mut records = vec![r];
        reconciler.reconcile(&mut records);

        assert!(records[0].current_paths.is_empty());
        assert_eq!(records[0].missing_current_paths.len(), 1);
        assert_eq!(records[0].missing_current_paths[0].path, ".output/uuid123");
    }

    // -----------------------------------------------------------------------
    // FilesystemReconciler: deleted_paths tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_deleted_paths_to_void() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        fs::create_dir_all(root.join("sorted/work")).unwrap();
        fs::write(root.join("sorted/work/old.pdf"), b"old").unwrap();

        let reconciler = FilesystemReconciler::new(root.clone());
        let mut r = make_record("old.pdf", "hash1");
        r.deleted_paths = vec!["sorted/work/old.pdf".into()];

        let mut records = vec![r];
        reconciler.reconcile(&mut records);

        assert!(!root.join("sorted/work/old.pdf").exists());
        let today = Utc::now().format("%Y-%m-%d").to_string();
        assert!(root.join("void").join(&today).join("sorted/work/old.pdf").exists());
    }

    // -----------------------------------------------------------------------
    // FilesystemReconciler: needs_deletion tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_needs_deletion_all_to_void() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        fs::create_dir_all(root.join("archive")).unwrap();
        fs::create_dir_all(root.join("sorted/work")).unwrap();
        fs::write(root.join("archive/test.pdf"), b"source").unwrap();
        fs::write(root.join("sorted/work/test.pdf"), b"current").unwrap();

        let reconciler = FilesystemReconciler::new(root.clone());
        let mut r = make_record("test.pdf", "hash1");
        r.state = State::NeedsDeletion;
        r.source_paths = vec![PathEntry {
            path: "archive/test.pdf".into(),
            timestamp: Utc::now(),
        }];
        r.current_paths = vec![PathEntry {
            path: "sorted/work/test.pdf".into(),
            timestamp: Utc::now(),
        }];

        let mut records = vec![r];
        reconciler.reconcile(&mut records);

        assert!(!root.join("archive/test.pdf").exists());
        assert!(!root.join("sorted/work/test.pdf").exists());
        let today = Utc::now().format("%Y-%m-%d").to_string();
        assert!(root.join("void").join(&today).join("archive/test.pdf").exists());
        assert!(root.join("void").join(&today).join("sorted/work/test.pdf").exists());
    }

    // -----------------------------------------------------------------------
    // FilesystemReconciler: multiple operations in one record
    // -----------------------------------------------------------------------

    #[test]
    fn test_multiple_operations_in_one_record() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();

        // Set up source file in inbox
        fs::create_dir_all(root.join("inbox")).unwrap();
        fs::write(root.join("inbox/doc.pdf"), b"source").unwrap();

        // Set up current file in .output
        fs::create_dir_all(root.join(".output")).unwrap();
        fs::write(root.join(".output/uuid456"), b"processed").unwrap();

        // Set up duplicate in inbox
        fs::create_dir_all(root.join("inbox/sub")).unwrap();
        fs::write(root.join("inbox/sub/dup.pdf"), b"dup").unwrap();

        // Set up file to delete
        fs::create_dir_all(root.join("sorted/old")).unwrap();
        fs::write(root.join("sorted/old/stale.pdf"), b"stale").unwrap();

        let reconciler = FilesystemReconciler::new(root.clone());
        let mut r = make_record("doc.pdf", "hash1");
        r.source_reference = Some("inbox/doc.pdf".into());
        r.source_paths = vec![PathEntry {
            path: "inbox/doc.pdf".into(),
            timestamp: Utc::now(),
        }];
        r.current_reference = Some(".output/uuid456".into());
        r.target_path = Some("sorted/work/doc.pdf".into());
        r.current_paths = vec![PathEntry {
            path: ".output/uuid456".into(),
            timestamp: Utc::now(),
        }];
        r.duplicate_sources = vec!["inbox/sub/dup.pdf".into()];
        r.deleted_paths = vec!["sorted/old/stale.pdf".into()];

        let mut records = vec![r];
        reconciler.reconcile(&mut records);

        // Source moved to archive
        assert!(root.join("archive/doc.pdf").exists());
        assert!(!root.join("inbox/doc.pdf").exists());

        // Current moved to sorted
        assert!(root.join("sorted/work/doc.pdf").exists());
        assert!(!root.join(".output/uuid456").exists());

        // Duplicate moved
        assert!(root.join("duplicates").join(today_date_dir()).join("inbox/sub/dup.pdf").exists());

        // Deleted moved to void
        let today = Utc::now().format("%Y-%m-%d").to_string();
        assert!(root.join("void").join(&today).join("sorted/old/stale.pdf").exists());
    }
}
