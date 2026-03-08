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

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs as unix_fs;
    use tempfile::TempDir;

    fn setup_dirs(root: &Path) {
        for d in &[
            "archive", "incoming", "error", "sorted", "processed", "reviewed", "trash",
        ] {
            fs::create_dir_all(root.join(d)).unwrap();
        }
    }

    fn write_file(path: &Path, content: &[u8]) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, content).unwrap();
    }

    #[test]
    fn test_moves_unsupported_from_incoming() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        setup_dirs(root);
        write_file(&root.join("incoming/font.ttf"), b"data");
        let moved = prefilter(root);
        assert_eq!(moved, 1);
        assert!(!root.join("incoming/font.ttf").exists());
        // File should be in error/
        assert!(root.join("error/font.ttf").exists());
    }

    #[test]
    fn test_moves_unsupported_from_archive() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        setup_dirs(root);
        write_file(&root.join("archive/spreadsheet.numbers"), b"data");
        let moved = prefilter(root);
        assert_eq!(moved, 1);
        assert!(!root.join("archive/spreadsheet.numbers").exists());
        assert!(root.join("error/spreadsheet.numbers").exists());
    }

    #[test]
    fn test_moves_unsupported_from_sorted_subdirs() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        setup_dirs(root);
        write_file(
            &root.join("sorted/context/nested.numbers"),
            b"data",
        );
        let moved = prefilter(root);
        assert_eq!(moved, 1);
        assert!(!root.join("sorted/context/nested.numbers").exists());
        assert!(root.join("error/nested.numbers").exists());
    }

    #[test]
    fn test_moves_unsupported_from_processed() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        setup_dirs(root);
        write_file(&root.join("processed/thesis.tex"), b"data");
        let moved = prefilter(root);
        assert_eq!(moved, 1);
        assert!(!root.join("processed/thesis.tex").exists());
        assert!(root.join("error/thesis.tex").exists());
    }

    #[test]
    fn test_moves_unsupported_from_reviewed() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        setup_dirs(root);
        write_file(&root.join("reviewed/budget.numbers"), b"data");
        let moved = prefilter(root);
        assert_eq!(moved, 1);
        assert!(!root.join("reviewed/budget.numbers").exists());
        assert!(root.join("error/budget.numbers").exists());
    }

    #[test]
    fn test_moves_unsupported_from_trash() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        setup_dirs(root);
        write_file(&root.join("trash/junk.ttf"), b"data");
        let moved = prefilter(root);
        assert_eq!(moved, 1);
        assert!(!root.join("trash/junk.ttf").exists());
        assert!(root.join("error/junk.ttf").exists());
    }

    #[test]
    fn test_skips_error_dir() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        setup_dirs(root);
        write_file(&root.join("error/already_here.ttf"), b"data");
        let moved = prefilter(root);
        assert_eq!(moved, 0);
        assert!(root.join("error/already_here.ttf").exists());
    }

    #[test]
    fn test_skips_void_dir() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        setup_dirs(root);
        fs::create_dir_all(root.join("void")).unwrap();
        write_file(&root.join("void/something.ttf"), b"data");
        let moved = prefilter(root);
        assert_eq!(moved, 0);
        assert!(root.join("void/something.ttf").exists());
    }

    #[test]
    fn test_skips_hidden_dirs() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        setup_dirs(root);
        write_file(&root.join(".output/something.ttf"), b"data");
        let moved = prefilter(root);
        assert_eq!(moved, 0);
        assert!(root.join(".output/something.ttf").exists());
    }

    #[test]
    fn test_keeps_supported_files() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        setup_dirs(root);
        write_file(&root.join("incoming/doc.pdf"), b"pdf");
        write_file(&root.join("incoming/photo.jpg"), b"jpg");
        write_file(&root.join("incoming/song.mp3"), b"mp3");
        let moved = prefilter(root);
        assert_eq!(moved, 0);
        assert!(root.join("incoming/doc.pdf").exists());
        assert!(root.join("incoming/photo.jpg").exists());
        assert!(root.join("incoming/song.mp3").exists());
    }

    #[test]
    fn test_handles_name_collision() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        setup_dirs(root);
        // Pre-existing file in error/ with same name
        write_file(&root.join("error/font.ttf"), b"existing");
        write_file(&root.join("incoming/font.ttf"), b"new");
        let moved = prefilter(root);
        assert_eq!(moved, 1);
        // Original error file still exists
        assert!(root.join("error/font.ttf").exists());
        // There should be a second file in error/ with a unique suffix
        let error_files: Vec<_> = fs::read_dir(root.join("error"))
            .unwrap()
            .flatten()
            .filter(|e| {
                e.file_name()
                    .to_str()
                    .map(|n| n.starts_with("font_") && n.ends_with(".ttf"))
                    .unwrap_or(false)
            })
            .collect();
        assert_eq!(error_files.len(), 1);
    }

    #[test]
    fn test_multiple_unsupported_across_dirs() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        setup_dirs(root);
        write_file(&root.join("incoming/a.ttf"), b"1");
        write_file(&root.join("archive/b.numbers"), b"2");
        write_file(&root.join("processed/c.tex"), b"3");
        write_file(&root.join("reviewed/d.exe"), b"4");
        let moved = prefilter(root);
        assert_eq!(moved, 4);
    }

    #[test]
    fn test_skips_hidden_files() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        setup_dirs(root);
        write_file(&root.join("incoming/.hidden_unsupported"), b"data");
        let moved = prefilter(root);
        assert_eq!(moved, 0);
    }

    #[test]
    fn test_skips_symlinks() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        setup_dirs(root);
        // Create a real file to point to
        write_file(&root.join("incoming/real.pdf"), b"data");
        // Create a symlink with unsupported extension
        unix_fs::symlink(
            root.join("incoming/real.pdf"),
            root.join("incoming/link.ttf"),
        )
        .unwrap();
        let moved = prefilter(root);
        assert_eq!(moved, 0);
    }

    #[test]
    fn test_empty_root() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        let moved = prefilter(root);
        assert_eq!(moved, 0);
    }

    #[test]
    fn test_creates_error_dir() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        // Only create incoming, not error
        fs::create_dir_all(root.join("incoming")).unwrap();
        write_file(&root.join("incoming/font.ttf"), b"data");
        assert!(!root.join("error").exists());
        let moved = prefilter(root);
        assert_eq!(moved, 1);
        assert!(root.join("error").exists());
        assert!(root.join("error/font.ttf").exists());
    }

    #[test]
    fn test_no_extension_moved() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        setup_dirs(root);
        write_file(&root.join("incoming/Makefile"), b"data");
        let moved = prefilter(root);
        assert_eq!(moved, 1);
        assert!(root.join("error/Makefile").exists());
    }

    #[test]
    fn test_config_files_not_moved() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        setup_dirs(root);
        write_file(
            &root.join("sorted/mycontext/context.yaml"),
            b"config",
        );
        write_file(
            &root.join("sorted/mycontext/smartfolders.yaml"),
            b"config",
        );
        let moved = prefilter(root);
        assert_eq!(moved, 0);
        assert!(root.join("sorted/mycontext/context.yaml").exists());
        assert!(root.join("sorted/mycontext/smartfolders.yaml").exists());
    }

    #[test]
    fn test_generated_yaml_not_moved() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        setup_dirs(root);
        write_file(
            &root.join("sorted/mycontext/generated.yaml"),
            b"config",
        );
        let moved = prefilter(root);
        assert_eq!(moved, 0);
        assert!(root.join("sorted/mycontext/generated.yaml").exists());
    }

    #[test]
    fn test_other_yaml_in_sorted_moved() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        setup_dirs(root);
        write_file(
            &root.join("sorted/mycontext/random.yaml"),
            b"not config",
        );
        let moved = prefilter(root);
        assert_eq!(moved, 1);
        assert!(!root.join("sorted/mycontext/random.yaml").exists());
        assert!(root.join("error/random.yaml").exists());
    }

    #[test]
    fn test_tmp_files_skipped() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        setup_dirs(root);
        write_file(&root.join("incoming/tempfile.tmp"), b"data");
        write_file(&root.join("archive/other.tmp"), b"data");
        let moved = prefilter(root);
        assert_eq!(moved, 0);
    }
}
