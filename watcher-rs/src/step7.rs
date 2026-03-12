//! History folder reconciler -- maintains `history/YYYY-MM-DD/` symlinks.
//!
//! For each `IsComplete` record with a processed file in `sorted/` or
//! `processed/`, creates a symlink under `history/{date_added}/` pointing
//! to the processed file.  Symlinks are updated when files move and
//! cleaned up when records are deleted.

use std::collections::HashSet;
use std::fs;
use std::os::unix::fs as unix_fs;
use std::path::{Path, PathBuf};

use tracing::{debug, warn};

use crate::models::{Record, State};

/// Locations where processed files live (symlink targets).
const TARGET_LOCATIONS: &[&str] = &["sorted", "processed"];

/// Maintains `history/YYYY-MM-DD/` symlinks to processed files.
pub struct HistoryReconciler {
    pub root: PathBuf,
    pub history_dir: PathBuf,
    /// Tracks expected symlink paths after reconcile, used by cleanup_orphans.
    _expected_links: HashSet<PathBuf>,
}

impl HistoryReconciler {
    pub fn new(root: PathBuf) -> Self {
        let history_dir = root.join("history");
        Self {
            root,
            history_dir,
            _expected_links: HashSet::new(),
        }
    }

    /// Ensure history symlinks exist for all completed records.
    pub fn reconcile(&mut self, records: &[Record]) {
        self._expected_links.clear();

        for record in records {
            if record.state != State::IsComplete {
                continue;
            }

            // Need a date to place the symlink
            let date = match record.effective_date_added() {
                Some(d) => d,
                None => continue,
            };

            // Need a current file in sorted/ or processed/
            let current_file = match record.current_file() {
                Some(cf) => cf,
                None => continue,
            };
            let current_loc = Record::decompose_path(&current_file.path).0;
            if !TARGET_LOCATIONS.contains(&current_loc.as_str()) {
                continue;
            }

            let current_abs = self.root.join(&current_file.path);
            if !current_abs.exists() {
                continue;
            }

            // Symlink name: use the filename from the current path
            let filename = match current_abs.file_name().and_then(|f| f.to_str()) {
                Some(f) => f.to_string(),
                None => continue,
            };

            let date_dir = self.history_dir.join(date.format("%Y-%m-%d").to_string());
            let link_path = date_dir.join(&filename);

            self._expected_links.insert(link_path.clone());

            // Ensure date directory exists
            if !date_dir.exists() {
                if let Err(e) = fs::create_dir_all(&date_dir) {
                    warn!("Failed to create history dir {}: {}", date_dir.display(), e);
                    continue;
                }
            }

            // Compute relative target from link location to current file
            let target = compute_relative_path(&date_dir, &current_abs);
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
                warn!(
                    "Cannot create history link, file exists: {}",
                    link_path.display()
                );
                continue;
            }

            match unix_fs::symlink(&target, &link_path) {
                Ok(()) => {
                    debug!(
                        "Created history link: {} -> {}",
                        link_path.display(),
                        target_str,
                    );
                }
                Err(e) => {
                    warn!(
                        "Failed to create history link {}: {}",
                        link_path.display(),
                        e
                    );
                }
            }
        }
    }

    /// Remove history symlinks that are no longer expected.
    ///
    /// Must be called after [`reconcile`](Self::reconcile).
    /// Walks `history/` for symlinks and removes any not in the expected set.
    /// Also removes empty date directories.
    pub fn cleanup_orphans(&self) {
        if !self.history_dir.exists() {
            return;
        }

        let date_dirs = match fs::read_dir(&self.history_dir) {
            Ok(entries) => entries,
            Err(_) => return,
        };

        for entry in date_dirs.flatten() {
            let date_dir = entry.path();
            if !date_dir.is_dir() || is_symlink(&date_dir) {
                continue;
            }

            let links = match fs::read_dir(&date_dir) {
                Ok(entries) => entries,
                Err(_) => continue,
            };

            for link_entry in links.flatten() {
                let link_path = link_entry.path();
                if !is_symlink(&link_path) {
                    continue;
                }
                if self._expected_links.contains(&link_path) {
                    continue;
                }

                // Orphaned history symlink -- remove
                if fs::remove_file(&link_path).is_ok() {
                    debug!("Removed orphaned history link: {}", link_path.display());
                }
            }

            // Remove empty date directories
            if is_dir_empty(&date_dir) {
                if fs::remove_dir(&date_dir).is_ok() {
                    debug!("Removed empty history dir: {}", date_dir.display());
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

/// Check if a directory is empty.
fn is_dir_empty(path: &Path) -> bool {
    fs::read_dir(path)
        .map(|mut entries| entries.next().is_none())
        .unwrap_or(true)
}

/// Compute a relative path from `from_dir` to `to_file`.
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
    use chrono::{NaiveDate, Utc};
    use std::fs;
    use std::os::unix::fs as unix_fs;
    use tempfile::TempDir;

    fn setup_record(root: &Path, ctx: &str, date: NaiveDate) -> Record {
        // Create processed file in sorted/
        let sorted_ctx = root.join("sorted").join(ctx);
        fs::create_dir_all(&sorted_ctx).unwrap();
        fs::write(sorted_ctx.join("2025-01-15-Invoice.pdf"), b"pdf content").unwrap();

        let mut r = Record::new("invoice.pdf".into(), "hash123".into());
        r.state = State::IsComplete;
        r.context = Some(ctx.into());
        r.date_added = Some(date);
        r.source_paths = vec![PathEntry {
            path: "archive/invoice.pdf".into(),
            timestamp: Utc::now(),
        }];
        r.current_paths = vec![PathEntry {
            path: format!("sorted/{}/2025-01-15-Invoice.pdf", ctx),
            timestamp: Utc::now(),
        }];
        r
    }

    #[test]
    fn test_creates_history_link() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        let date = NaiveDate::from_ymd_opt(2025, 3, 15).unwrap();
        let r = setup_record(&root, "work", date);

        let mut reconciler = HistoryReconciler::new(root.clone());
        reconciler.reconcile(&[r]);

        let link_path = root.join("history/2025-03-15/2025-01-15-Invoice.pdf");
        assert!(link_path.exists(), "History link should exist");
        assert!(is_symlink(&link_path), "Should be a symlink");
        let resolved = link_path.canonicalize().unwrap();
        let expected = root
            .join("sorted/work/2025-01-15-Invoice.pdf")
            .canonicalize()
            .unwrap();
        assert_eq!(resolved, expected);
    }

    #[test]
    fn test_no_link_if_not_complete() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        let date = NaiveDate::from_ymd_opt(2025, 3, 15).unwrap();
        let mut r = setup_record(&root, "work", date);
        r.state = State::NeedsProcessing;

        let mut reconciler = HistoryReconciler::new(root.clone());
        reconciler.reconcile(&[r]);

        assert!(!root.join("history").exists() || is_dir_empty(&root.join("history")));
    }

    #[test]
    fn test_no_link_if_no_date() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        let mut r = setup_record(&root, "work", NaiveDate::from_ymd_opt(2025, 1, 1).unwrap());
        r.date_added = None;
        r.source_paths.clear(); // no fallback either

        let mut reconciler = HistoryReconciler::new(root.clone());
        reconciler.reconcile(&[r]);

        assert!(!root.join("history").exists() || is_dir_empty(&root.join("history")));
    }

    #[test]
    fn test_cleanup_orphans_removes_stale() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();

        // Create a file to point at and a symlink in history/
        fs::create_dir_all(root.join("sorted/work")).unwrap();
        fs::write(root.join("sorted/work/old.pdf"), b"pdf").unwrap();
        let date_dir = root.join("history/2025-01-01");
        fs::create_dir_all(&date_dir).unwrap();
        let link_path = date_dir.join("old.pdf");
        let target = compute_relative_path(&date_dir, &root.join("sorted/work/old.pdf"));
        unix_fs::symlink(&target, &link_path).unwrap();
        assert!(is_symlink(&link_path));

        // Reconcile with empty records -- no expected links
        let mut reconciler = HistoryReconciler::new(root.clone());
        reconciler.reconcile(&[]);
        reconciler.cleanup_orphans();

        assert!(!link_path.exists(), "Orphaned history link should be cleaned up");
        // Empty date dir should also be removed
        assert!(!date_dir.exists(), "Empty date dir should be removed");
    }

    #[test]
    fn test_idempotent() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        let date = NaiveDate::from_ymd_opt(2025, 3, 15).unwrap();
        let r = setup_record(&root, "work", date);

        let mut reconciler = HistoryReconciler::new(root.clone());

        reconciler.reconcile(&[r.clone()]);
        let link_path = root.join("history/2025-03-15/2025-01-15-Invoice.pdf");
        assert!(is_symlink(&link_path));
        let target_first = fs::read_link(&link_path).unwrap();

        reconciler.reconcile(&[r]);
        assert!(is_symlink(&link_path));
        let target_second = fs::read_link(&link_path).unwrap();

        assert_eq!(target_first, target_second);
    }

    #[test]
    fn test_link_updates_on_move() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        let date = NaiveDate::from_ymd_opt(2025, 3, 15).unwrap();
        let r = setup_record(&root, "work", date);

        let mut reconciler = HistoryReconciler::new(root.clone());
        reconciler.reconcile(&[r]);

        let link_path = root.join("history/2025-03-15/2025-01-15-Invoice.pdf");
        assert!(is_symlink(&link_path));

        // Simulate file move: create new file at new location
        fs::create_dir_all(root.join("sorted/private")).unwrap();
        fs::write(
            root.join("sorted/private/2025-01-15-Invoice.pdf"),
            b"pdf content",
        )
        .unwrap();
        fs::remove_file(root.join("sorted/work/2025-01-15-Invoice.pdf")).unwrap();

        let mut r2 = Record::new("invoice.pdf".into(), "hash123".into());
        r2.state = State::IsComplete;
        r2.context = Some("private".into());
        r2.date_added = Some(date);
        r2.source_paths = vec![PathEntry {
            path: "archive/invoice.pdf".into(),
            timestamp: Utc::now(),
        }];
        r2.current_paths = vec![PathEntry {
            path: "sorted/private/2025-01-15-Invoice.pdf".into(),
            timestamp: Utc::now(),
        }];

        reconciler.reconcile(&[r2]);

        // Link should now point to new location
        let resolved = link_path.canonicalize().unwrap();
        let expected = root
            .join("sorted/private/2025-01-15-Invoice.pdf")
            .canonicalize()
            .unwrap();
        assert_eq!(resolved, expected);
    }

    #[test]
    fn test_collision_avoidance() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        let date = NaiveDate::from_ymd_opt(2025, 3, 15).unwrap();
        let r = setup_record(&root, "work", date);

        // Place a real file where the symlink would go
        let date_dir = root.join("history/2025-03-15");
        fs::create_dir_all(&date_dir).unwrap();
        fs::write(date_dir.join("2025-01-15-Invoice.pdf"), b"real file").unwrap();

        let mut reconciler = HistoryReconciler::new(root.clone());
        reconciler.reconcile(&[r]);

        let link_path = date_dir.join("2025-01-15-Invoice.pdf");
        assert!(!is_symlink(&link_path), "Should not replace a real file");
        assert_eq!(fs::read_to_string(&link_path).unwrap(), "real file");
    }

    #[test]
    fn test_fallback_to_source_timestamp() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();

        // Create processed file
        fs::create_dir_all(root.join("sorted/work")).unwrap();
        fs::write(root.join("sorted/work/doc.pdf"), b"pdf").unwrap();

        let ts = "2025-06-15T10:00:00Z"
            .parse::<chrono::DateTime<Utc>>()
            .unwrap();
        let mut r = Record::new("doc.pdf".into(), "hash".into());
        r.state = State::IsComplete;
        r.date_added = None; // not set
        r.source_paths = vec![PathEntry {
            path: "archive/doc.pdf".into(),
            timestamp: ts,
        }];
        r.current_paths = vec![PathEntry {
            path: "sorted/work/doc.pdf".into(),
            timestamp: Utc::now(),
        }];

        let mut reconciler = HistoryReconciler::new(root.clone());
        reconciler.reconcile(&[r]);

        // Should use source timestamp date as fallback
        let link_path = root.join("history/2025-06-15/doc.pdf");
        assert!(is_symlink(&link_path), "Should use source timestamp fallback");
    }

    #[test]
    fn test_link_in_processed_dir() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        let date = NaiveDate::from_ymd_opt(2025, 3, 15).unwrap();

        fs::create_dir_all(root.join("processed")).unwrap();
        fs::write(root.join("processed/doc.pdf"), b"pdf").unwrap();

        let mut r = Record::new("doc.pdf".into(), "hash".into());
        r.state = State::IsComplete;
        r.date_added = Some(date);
        r.source_paths = vec![PathEntry {
            path: "archive/doc.pdf".into(),
            timestamp: Utc::now(),
        }];
        r.current_paths = vec![PathEntry {
            path: "processed/doc.pdf".into(),
            timestamp: Utc::now(),
        }];

        let mut reconciler = HistoryReconciler::new(root.clone());
        reconciler.reconcile(&[r]);

        let link_path = root.join("history/2025-03-15/doc.pdf");
        assert!(is_symlink(&link_path), "Should create link for processed/ files");
    }
}
