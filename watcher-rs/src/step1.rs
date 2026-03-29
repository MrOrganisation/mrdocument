//! Filesystem detection for document watcher.
//!
//! First cycle: full scan of watched directories to establish baseline state.
//! Subsequent cycles: inotify-driven incremental detection (only hash files
//! that actually changed on disk).

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use tracing::{error, info, trace};
use uuid::Uuid;

use crate::models::{ChangeItem, EventType, Record};
use crate::prefilter::SUPPORTED_EXTENSIONS;

/// Precomputed O(1) lookup indexes built from a DB snapshot.
struct SnapshotIndex {
    /// All `source_hash` and `hash` values across records.
    known_hashes: HashSet<String>,
    /// All `output_filename` values.
    output_filenames: HashSet<String>,
    /// All `current_paths` entries that start with `.output/`.
    output_current_paths: HashSet<String>,
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Directories scanned directly (non-recursive).
pub const DIRECT_DIRS: &[&str] = &[
    "archive",
    "incoming",
    "reclassify",
    "reviewed",
    "processed",
    "reset",
    "trash",
    ".output",
];

/// Directories scanned recursively with inotify watches.
pub const RECURSIVE_DIRS: &[&str] = &["sorted"];

/// Locations where unknown files are allowed (not stray).
const ELIGIBLE_LOCATIONS: &[&str] = &["incoming", "reclassify", "sorted"];

/// Config filenames inside `sorted/{context}/`.
const CONFIG_FILENAMES: &[&str] = &["context.yaml", "smartfolders.yaml", "generated.yaml"];

/// Syncthing temporary file patterns.
const SYNCTHING_PATTERNS: &[&str] = &[".syncthing.", "~syncthing~"];

/// Temporary file extensions to ignore.
const TEMP_EXTENSIONS: &[&str] = &[".tmp"];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Compute SHA-256 hash of a file, returned as a lowercase hex string.
pub fn compute_sha256(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 8192];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

/// Compute a content hash that ignores metadata.
///
/// - **PDF**: hash page content streams only (ignoring Info dict, XMP, etc.)
/// - **Audio** (mp3/mp4/m4a/mov/wav/ogg/flac/aac/wma/webm): hash raw audio
///   data (stripping ID3 / container metadata)
/// - **TXT / everything else**: same as normal SHA-256
///
/// Returns `None` when the content hash would be identical to the normal hash
/// (i.e. for TXT and other non-PDF/audio files), so the caller can skip
/// storing a redundant value.
pub fn compute_content_hash(path: &Path) -> Option<String> {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_lowercase())
        .unwrap_or_default();

    match ext.as_str() {
        "pdf" => compute_pdf_content_hash(path),
        "mp3" => compute_mp3_content_hash(path),
        "mp4" | "m4a" | "mov" | "webm" => compute_mp4_content_hash(path),
        _ => None, // TXT and others: content hash == normal hash
    }
}

/// Hash PDF page content streams, ignoring metadata.
fn compute_pdf_content_hash(path: &Path) -> Option<String> {
    let doc = lopdf::Document::load(path).ok()?;
    let mut hasher = Sha256::new();
    let mut has_content = false;

    // Collect and sort page IDs for deterministic ordering
    let mut page_ids: Vec<lopdf::ObjectId> = doc.page_iter().collect();
    page_ids.sort();

    for page_id in page_ids {
        if let Ok(content_data) = doc.get_page_content(page_id) {
            hasher.update(&content_data);
            has_content = true;
        }
    }

    if has_content {
        Some(format!("{:x}", hasher.finalize()))
    } else {
        None
    }
}

/// Hash MP3 audio data, stripping ID3v2 (header) and ID3v1 (tail) tags.
fn compute_mp3_content_hash(path: &Path) -> Option<String> {
    let data = fs::read(path).ok()?;
    if data.len() < 10 {
        return None;
    }

    let mut start = 0usize;
    // Skip ID3v2 header if present
    if data.len() >= 10 && &data[0..3] == b"ID3" {
        let size = ((data[6] as usize & 0x7F) << 21)
            | ((data[7] as usize & 0x7F) << 14)
            | ((data[8] as usize & 0x7F) << 7)
            | (data[9] as usize & 0x7F);
        start = 10 + size;
    }

    let mut end = data.len();
    // Skip ID3v1 tag at end if present
    if end >= 128 && &data[end - 128..end - 125] == b"TAG" {
        end -= 128;
    }

    if start >= end {
        return None;
    }

    let mut hasher = Sha256::new();
    hasher.update(&data[start..end]);
    Some(format!("{:x}", hasher.finalize()))
}

/// Hash MP4/M4A/MOV audio data (mdat atom content), ignoring metadata atoms.
fn compute_mp4_content_hash(path: &Path) -> Option<String> {
    let data = fs::read(path).ok()?;
    let mut hasher = Sha256::new();
    let mut has_mdat = false;

    // Walk top-level atoms looking for mdat
    let mut pos = 0usize;
    while pos + 8 <= data.len() {
        let size = u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
            as usize;
        let atom_type = &data[pos + 4..pos + 8];

        if size < 8 {
            break; // invalid atom
        }

        if atom_type == b"mdat" {
            let atom_end = (pos + size).min(data.len());
            if pos + 8 < atom_end {
                hasher.update(&data[pos + 8..atom_end]);
                has_mdat = true;
            }
        }

        pos += size;
    }

    if has_mdat {
        Some(format!("{:x}", hasher.finalize()))
    } else {
        None
    }
}

/// Check if a relative path is a config file in `sorted/{context}/`.
fn is_config_file(rel_path: &str) -> bool {
    let parts: Vec<&str> = rel_path.split('/').collect();
    parts.len() == 3
        && parts[0] == "sorted"
        && CONFIG_FILENAMES
            .iter()
            .any(|c| c.eq_ignore_ascii_case(parts[2]))
}

/// Check if a filename should be ignored (hidden/temp/syncthing files).
fn is_ignored(filename: &str) -> bool {
    if filename.starts_with('.') || filename.starts_with('~') {
        return true;
    }
    for pattern in SYNCTHING_PATTERNS {
        if filename.contains(pattern) {
            return true;
        }
    }
    for ext in TEMP_EXTENSIONS {
        if filename.ends_with(ext) {
            return true;
        }
    }
    false
}

/// Extract the top-level location from a relative path.
fn get_location(rel_path: &str) -> &str {
    rel_path.split('/').next().unwrap_or("")
}

/// Check if a file has a supported extension.
fn has_supported_extension(path: &Path) -> bool {
    match path.extension().and_then(|e| e.to_str()) {
        Some(ext) => {
            let ext_lower = format!(".{}", ext.to_ascii_lowercase());
            SUPPORTED_EXTENSIONS.contains(ext_lower.as_str())
        }
        None => {
            // Files with no extension: allow through (don't filter them out here).
            // The ext-check is "if ext and ext not in SUPPORTED" in Python,
            // which means no-extension files pass through.
            true
        }
    }
}

// ---------------------------------------------------------------------------
// FilesystemDetector
// ---------------------------------------------------------------------------

/// Scans watched directories and detects filesystem changes.
///
/// First call to [`detect`] does a full scan and starts an inotify observer.
/// Subsequent calls use inotify events for O(changes) instead of O(all-files).
pub struct FilesystemDetector {
    root: PathBuf,
    /// path → (hash, size, modified_secs) for cache-aware scanning.
    previous_state: HashMap<String, (String, u64, u64)>,
    watcher: Option<RecommendedWatcher>,
    changed_paths: Arc<Mutex<HashSet<String>>>,
    event_tx: Option<mpsc::Sender<()>>,
    event_rx: Option<mpsc::Receiver<()>>,
    /// Set to `true` when a config file in `sorted/` changes.
    pub config_changed: bool,
    root_sf_hash: Option<String>,
    /// Paths from `smartfolder_paths` in root `smartfolders.yaml`.
    /// Monitored for discovered `smartfolder.yaml` config changes.
    smartfolder_paths: Vec<PathBuf>,
    /// External wake-up signal (e.g. from DB LISTEN/NOTIFY).
    db_notify: Arc<tokio::sync::Notify>,
}

impl FilesystemDetector {
    /// Create a new detector for the given root directory.
    pub fn new(root: PathBuf, db_notify: Arc<tokio::sync::Notify>) -> Self {
        let (tx, rx) = mpsc::channel(256);
        Self {
            root,
            previous_state: HashMap::new(), // (hash, size, mtime_secs)
            watcher: None,
            changed_paths: Arc::new(Mutex::new(HashSet::new())),
            event_tx: Some(tx),
            event_rx: Some(rx),
            config_changed: false,
            root_sf_hash: None,
            smartfolder_paths: Vec::new(),
            db_notify,
        }
    }

    /// Start inotify watches on all watched directories.
    fn start_observer(&mut self) -> Result<()> {
        let changed = Arc::clone(&self.changed_paths);
        let root = self.root.clone();
        let tx = self.event_tx.clone().expect("event_tx not available");

        let mut watcher =
            notify::recommended_watcher(move |res: std::result::Result<Event, notify::Error>| {
                if let Ok(event) = res {
                    // Only process file-mutating events (create, modify, delete, move).
                    // The notify crate also delivers Access (open/close) and Attribute
                    // events which fire on every read_dir/stat/read_link call — these
                    // would cause a self-sustaining loop since our own reconcilers
                    // read files in watched directories every cycle.
                    match event.kind {
                        EventKind::Create(_)
                        | EventKind::Modify(_)
                        | EventKind::Remove(_)
                        | EventKind::Other => {}
                        _ => return, // skip Access, Attribute, and Any events
                    }

                    let mut any_relevant = false;
                    for path in &event.paths {
                        if let Ok(rel) = path.strip_prefix(&root) {
                            let filename = rel
                                .file_name()
                                .and_then(|n| n.to_str())
                                .unwrap_or("");
                            // Skip syncthing temp files and hidden/temp files early
                            if filename.starts_with('.')
                                || filename.starts_with('~')
                                || filename.ends_with(".tmp")
                            {
                                continue;
                            }
                            let rel_str = rel.to_string_lossy().to_string();
                            trace!("inotify: {:?} {}", event.kind, rel_str);
                            let mut set = changed.lock().unwrap();
                            set.insert(rel_str);
                            any_relevant = true;
                        }
                    }
                    // Only signal if at least one relevant path was accepted.
                    if any_relevant {
                        let _ = tx.try_send(());
                    }
                }
            })?;

        // Schedule watches on direct dirs (non-recursive).
        for dirname in DIRECT_DIRS {
            let dirpath = self.root.join(dirname);
            if dirpath.is_dir() {
                watcher.watch(&dirpath, RecursiveMode::NonRecursive)?;
            }
        }

        // Schedule watches on recursive dirs.
        for dirname in RECURSIVE_DIRS {
            let dirpath = self.root.join(dirname);
            if dirpath.is_dir() {
                watcher.watch(&dirpath, RecursiveMode::Recursive)?;
            }
        }

        self.watcher = Some(watcher);
        info!("Started filesystem observer on {}", self.root.display());
        Ok(())
    }

    /// Stop the inotify observer.
    #[allow(dead_code)]
    pub fn stop(&mut self) {
        if let Some(mut w) = self.watcher.take() {
            // Unwatch all paths; drop will also stop the watcher.
            for dirname in DIRECT_DIRS.iter().chain(RECURSIVE_DIRS.iter()) {
                let dirpath = self.root.join(dirname);
                let _ = w.unwatch(&dirpath);
            }
        }
    }

    /// Wait for a filesystem or database event, up to `timeout_secs` seconds.
    ///
    /// Returns `true` if an event arrived (filesystem change or DB
    /// LISTEN/NOTIFY), `false` if the timeout expired.
    pub async fn wait_for_event(&mut self, timeout_secs: f64) -> bool {
        let duration = tokio::time::Duration::from_secs_f64(timeout_secs);
        let db_notified = self.db_notify.notified();

        match &mut self.event_rx {
            Some(rx) if self.watcher.is_some() => {
                tokio::select! {
                    result = tokio::time::timeout(duration, rx.recv()) => {
                        matches!(result, Ok(Some(())))
                    }
                    _ = db_notified => true,
                }
            }
            _ => {
                // No filesystem watcher — still respond to DB notifications.
                matches!(
                    tokio::time::timeout(duration, db_notified).await,
                    Ok(()),
                )
            }
        }
    }

    /// Drain all pending notifications from the event channel without blocking.
    ///
    /// Called at the end of a cycle to discard notifications generated by the
    /// cycle's own filesystem operations (symlink reconciler, file moves, etc.).
    /// The paths themselves remain in `changed_paths` and will be processed
    /// (harmlessly skipped) on the next real event.
    pub fn drain_notifications(&mut self) {
        if let Some(rx) = &mut self.event_rx {
            while rx.try_recv().is_ok() {}
        }
    }

    /// Set `smartfolder_paths` for monitoring discovered smartfolder configs.
    pub fn set_smartfolder_paths(&mut self, paths: Vec<PathBuf>) {
        self.smartfolder_paths = paths;
    }

    /// Check if root `smartfolders.yaml` or any discovered `smartfolder.yaml`
    /// in `smartfolder_paths` changed and set `config_changed`.
    fn check_root_smartfolders_yaml(&mut self) {
        let sf_path = self.root.join("smartfolders.yaml");

        // Build a combined hash of root config + all discovered configs.
        let mut combined = String::new();

        if sf_path.is_file() {
            if let Ok(h) = compute_sha256(&sf_path) {
                combined.push_str(&h);
            }
        }

        // Hash all discovered smartfolder.yaml files from smartfolder_paths.
        let mut discovered: Vec<(String, String)> = Vec::new();
        for search_path in &self.smartfolder_paths {
            for config_file in crate::config::find_smartfolder_yaml_files(search_path) {
                if let Ok(h) = compute_sha256(&config_file) {
                    discovered.push((config_file.display().to_string(), h));
                }
            }
        }
        discovered.sort();
        for (path, hash) in &discovered {
            combined.push_str(path);
            combined.push_str(hash);
        }

        let current_hash = if combined.is_empty() {
            None
        } else {
            let h = Sha256::digest(combined.as_bytes());
            Some(format!("{:x}", h))
        };

        if current_hash != self.root_sf_hash {
            if self.root_sf_hash.is_some() {
                // Only flag change after first run (not on initial load).
                self.config_changed = true;
            }
            self.root_sf_hash = current_hash;
        }
    }

    /// Detect filesystem changes.
    ///
    /// First call: full scan + start inotify observer.
    /// Subsequent calls: process inotify events.  When `full_scan` is true
    /// **and** the observer is already running, a filesystem scan is performed
    /// first so that files missed by inotify (e.g. due to Docker bind-mount
    /// event delivery gaps) are still picked up.
    pub async fn detect(&mut self, db_snapshot: &[Record], full_scan: bool) -> Vec<ChangeItem> {
        self.check_root_smartfolders_yaml();
        if self.watcher.is_none() {
            self.detect_full(db_snapshot)
        } else {
            if full_scan {
                self.inject_full_scan_paths();
            }
            self.detect_incremental(db_snapshot)
        }
    }

    /// Scan all watched directories and inject paths for files not already
    /// tracked in `previous_state` into `changed_paths`, so that the next
    /// `detect_incremental` picks them up.
    fn inject_full_scan_paths(&self) {
        let mut injected = 0usize;
        let mut set = self.changed_paths.lock().unwrap();

        let should_inject = |path: &Path, rel_str: &str| -> bool {
            let filename = match path.file_name().and_then(|n| n.to_str()) {
                Some(name) => name,
                None => return false,
            };
            if is_ignored(filename) {
                return false;
            }
            if !has_supported_extension(path) {
                return false;
            }
            if is_config_file(rel_str) {
                return false;
            }
            true
        };

        for dirname in DIRECT_DIRS {
            let dirpath = self.root.join(dirname);
            if !dirpath.is_dir() {
                continue;
            }
            if let Ok(entries) = fs::read_dir(&dirpath) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if !path.is_file() || path.is_symlink() {
                        continue;
                    }
                    if let Ok(rel) = path.strip_prefix(&self.root) {
                        let rel_str = rel.to_string_lossy().to_string();
                        if should_inject(&path, &rel_str)
                            && !self.previous_state.contains_key(&rel_str)
                        {
                            set.insert(rel_str);
                            injected += 1;
                        }
                    }
                }
            }
        }

        for dirname in RECURSIVE_DIRS {
            let dirpath = self.root.join(dirname);
            if !dirpath.is_dir() {
                continue;
            }
            for file in walk_recursive(&dirpath) {
                if !file.is_file() || file.is_symlink() {
                    continue;
                }
                if let Ok(rel) = file.strip_prefix(&self.root) {
                    let rel_str = rel.to_string_lossy().to_string();
                    if should_inject(&file, &rel_str)
                        && !self.previous_state.contains_key(&rel_str)
                    {
                        set.insert(rel_str);
                        injected += 1;
                    }
                }
            }
        }

        if injected > 0 {
            info!("inject_full_scan_paths: {} new paths injected", injected);
        }
    }

    // ------------------------------------------------------------------
    // Full scan
    // ------------------------------------------------------------------

    fn scan(&self) -> HashMap<String, (String, u64)> {
        let mut result: HashMap<String, (String, u64)> = HashMap::new();

        let mut scan_file = |path: &Path, rel_str: String| {
            let meta = match path.metadata() {
                Ok(m) => m,
                Err(_) => {
                    tracing::warn!("Failed to read metadata for {}", rel_str);
                    return;
                }
            };
            let size = meta.len();
            let mtime_secs = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs())
                .unwrap_or(0);

            // Use cached hash if file metadata (size + mtime) is unchanged
            if let Some((cached_hash, cached_size, cached_mtime)) =
                self.previous_state.get(&rel_str)
            {
                if *cached_size == size && *cached_mtime == mtime_secs {
                    result.insert(rel_str, (cached_hash.clone(), size));
                    return;
                }
            }

            match compute_sha256(path) {
                Ok(hash) => {
                    result.insert(rel_str, (hash, size));
                }
                Err(_) => {
                    tracing::warn!("Failed to hash {}", path.display());
                }
            }
        };

        // Direct dirs (non-recursive)
        for dirname in DIRECT_DIRS {
            let dirpath = self.root.join(dirname);
            if !dirpath.is_dir() {
                continue;
            }
            if let Ok(entries) = fs::read_dir(&dirpath) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if !path.is_file() || path.is_symlink() {
                        continue;
                    }
                    let filename = match path.file_name().and_then(|n| n.to_str()) {
                        Some(name) => name.to_string(),
                        None => continue,
                    };
                    if is_ignored(&filename) {
                        continue;
                    }
                    if !has_supported_extension(&path) {
                        continue;
                    }
                    if let Ok(rel) = path.strip_prefix(&self.root) {
                        scan_file(&path, rel.to_string_lossy().to_string());
                    }
                }
            }
        }

        // Recursive dirs
        for dirname in RECURSIVE_DIRS {
            let dirpath = self.root.join(dirname);
            if !dirpath.is_dir() {
                continue;
            }
            for file in walk_recursive(&dirpath) {
                if !file.is_file() || file.is_symlink() {
                    continue;
                }
                let filename = match file.file_name().and_then(|n| n.to_str()) {
                    Some(name) => name.to_string(),
                    None => continue,
                };
                if is_ignored(&filename) {
                    continue;
                }
                if !has_supported_extension(&file) {
                    continue;
                }
                if let Ok(rel) = file.strip_prefix(&self.root) {
                    let rel_str = rel.to_string_lossy().to_string();
                    if is_config_file(&rel_str) {
                        continue;
                    }
                    scan_file(&file, rel_str);
                }
            }
        }

        result
    }

    fn detect_full(&mut self, db_snapshot: &[Record]) -> Vec<ChangeItem> {
        // Start observer before scan so events during scan are captured.
        if let Err(e) = self.start_observer() {
            error!("Failed to start observer: {}", e);
        }

        let t0 = std::time::Instant::now();
        let current_state = self.scan();
        let t_scan = t0.elapsed();

        let mut changes: Vec<ChangeItem> = Vec::new();

        // Build O(1) lookup indexes from snapshot.
        let index = Self::build_snapshot_index(db_snapshot);

        // Build map of paths already tracked in DB -> their record's hashes.
        let mut known_paths: HashMap<String, Option<String>> = HashMap::new();
        for record in db_snapshot {
            for pe in &record.source_paths {
                known_paths.insert(pe.path.clone(), Some(record.source_hash.clone()));
            }
            for pe in &record.current_paths {
                known_paths.insert(pe.path.clone(), record.hash.clone());
            }
        }

        let mut content_hash_count = 0u32;
        let t1 = std::time::Instant::now();

        for (rel_path, (file_hash, file_size)) in &current_state {
            let location = get_location(rel_path);

            // Stray detection: unknown file in non-eligible location
            if !Self::is_known(file_hash, rel_path, &index) {
                if !ELIGIBLE_LOCATIONS.contains(&location) {
                    if location == ".output" {
                        self.delete_stray(rel_path);
                    } else {
                        self.move_to_error(rel_path);
                    }
                    continue;
                }
            }

            // Skip files already tracked in DB with unchanged hash
            if let Some(Some(existing_hash)) = known_paths.get(rel_path) {
                if existing_hash == file_hash {
                    continue;
                }
            }

            // Only compute content hash for files that are genuinely new or changed
            let abs_path = self.root.join(rel_path);
            let content_hash = compute_content_hash(&abs_path);
            content_hash_count += 1;

            changes.push(ChangeItem {
                event_type: EventType::Addition,
                path: rel_path.clone(),
                hash: Some(file_hash.clone()),
                content_hash,
                size: Some(*file_size),
            });
        }

        let t_match = t1.elapsed();

        // DB paths that don't exist on disk (stale/deleted while watcher was down)
        let mut seen: HashSet<String> = HashSet::new();
        for record in db_snapshot {
            for pe in &record.source_paths {
                if !current_state.contains_key(&pe.path) && seen.insert(pe.path.clone()) {
                    changes.push(ChangeItem {
                        event_type: EventType::Removal,
                        path: pe.path.clone(),
                        hash: None,
                        content_hash: None,
                        size: None,
                    });
                }
            }
            for pe in &record.current_paths {
                if !current_state.contains_key(&pe.path) && seen.insert(pe.path.clone()) {
                    changes.push(ChangeItem {
                        event_type: EventType::Removal,
                        path: pe.path.clone(),
                        hash: None,
                        content_hash: None,
                        size: None,
                    });
                }
            }
        }

        info!(
            "detect_full: {} files scanned, scan={:.2}s match={:.2}s content_hashes={}",
            current_state.len(),
            t_scan.as_secs_f64(),
            t_match.as_secs_f64(),
            content_hash_count,
        );

        // Update previous state with metadata for cache-aware scanning
        self.previous_state = current_state
            .into_iter()
            .map(|(path, (hash, size))| {
                let mtime_secs = self
                    .root
                    .join(&path)
                    .metadata()
                    .ok()
                    .and_then(|m| m.modified().ok())
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                (path, (hash, size, mtime_secs))
            })
            .collect();

        changes
    }

    // ------------------------------------------------------------------
    // Incremental detection
    // ------------------------------------------------------------------

    fn detect_incremental(&mut self, db_snapshot: &[Record]) -> Vec<ChangeItem> {
        // Drain inotify events
        let changed_paths: HashSet<String> = {
            let mut set = self.changed_paths.lock().unwrap();
            std::mem::take(&mut *set)
        };

        if changed_paths.is_empty() {
            return Vec::new();
        }

        info!("detect_incremental: {} inotify events", changed_paths.len());
        if tracing::enabled!(tracing::Level::TRACE) {
            let mut sorted_paths: Vec<&String> = changed_paths.iter().collect();
            sorted_paths.sort();
            for p in &sorted_paths {
                trace!("  inotify path: {}", p);
            }
        }

        // Build O(1) lookup indexes from snapshot.
        let index = Self::build_snapshot_index(db_snapshot);

        let mut changes: Vec<ChangeItem> = Vec::new();

        for rel_path in &changed_paths {
            let filename = Path::new(rel_path)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("");

            if is_ignored(filename) {
                continue;
            }

            // Config files in sorted/ trigger a reload, not a document change
            if is_config_file(rel_path) {
                self.config_changed = true;
                continue;
            }

            // Skip unsupported file extensions
            if !has_supported_extension(Path::new(filename)) {
                continue;
            }

            let abs_path = self.root.join(rel_path);
            let was_known = self.previous_state.contains_key(rel_path);

            if abs_path.is_file() && !abs_path.is_symlink() {
                // For known files, skip if metadata (size + mtime) is unchanged.
                // This avoids expensive SHA-256 computation on spurious inotify events.
                if was_known {
                    if let Ok(meta) = abs_path.metadata() {
                        let size = meta.len();
                        let mtime_secs = meta
                            .modified()
                            .ok()
                            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                            .map(|d| d.as_secs())
                            .unwrap_or(0);
                        if let Some((_, cached_size, cached_mtime)) =
                            self.previous_state.get(rel_path)
                        {
                            if *cached_size == size && *cached_mtime == mtime_secs {
                                continue;
                            }
                        }
                    }
                }

                // File exists -- hash it
                let (file_hash, file_size) = match (
                    compute_sha256(&abs_path),
                    abs_path.metadata().map(|m| m.len()),
                ) {
                    (Ok(h), Ok(s)) => (h, s),
                    _ => continue,
                };

                if !was_known {
                    // New file -- stray detection
                    let location = get_location(rel_path);
                    if !Self::is_known(&file_hash, rel_path, &index)
                        && !ELIGIBLE_LOCATIONS.contains(&location)
                    {
                        if location == ".output" {
                            self.delete_stray(rel_path);
                        } else {
                            self.move_to_error(rel_path);
                        }
                        continue;
                    }
                }

                let content_hash = compute_content_hash(&abs_path);
                changes.push(ChangeItem {
                    event_type: EventType::Addition,
                    path: rel_path.clone(),
                    hash: Some(file_hash.clone()),
                    content_hash,
                    size: Some(file_size),
                });

                // Update state
                let mtime_secs = abs_path
                    .metadata()
                    .ok()
                    .and_then(|m| m.modified().ok())
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                self.previous_state
                    .insert(rel_path.clone(), (file_hash, file_size, mtime_secs));
            } else {
                // File gone
                if was_known {
                    changes.push(ChangeItem {
                        event_type: EventType::Removal,
                        path: rel_path.clone(),
                        hash: None,
                        content_hash: None,
                        size: None,
                    });
                    self.previous_state.remove(rel_path);
                }
            }
        }

        changes
    }

    // ------------------------------------------------------------------
    // Stray handling
    // ------------------------------------------------------------------

    /// Build O(1) lookup indexes from a snapshot for [`Self::is_known`].
    fn build_snapshot_index(snapshot: &[Record]) -> SnapshotIndex {
        let mut known_hashes: HashSet<String> = HashSet::with_capacity(snapshot.len() * 2);
        let mut output_filenames: HashSet<String> = HashSet::new();
        let mut output_current_paths: HashSet<String> = HashSet::new();

        for r in snapshot {
            if !r.source_hash.is_empty() {
                known_hashes.insert(r.source_hash.clone());
            }
            if let Some(ref h) = r.hash {
                if !h.is_empty() {
                    known_hashes.insert(h.clone());
                }
            }
            if let Some(ref ofn) = r.output_filename {
                output_filenames.insert(ofn.clone());
            }
            for pe in &r.current_paths {
                if pe.path.starts_with(".output/") {
                    output_current_paths.insert(pe.path.clone());
                }
            }
        }

        SnapshotIndex {
            known_hashes,
            output_filenames,
            output_current_paths,
        }
    }

    /// Check if a file is known, using precomputed indexes for O(1) lookups.
    fn is_known(file_hash: &str, rel_path: &str, index: &SnapshotIndex) -> bool {
        let location = get_location(rel_path);

        // .output files match by filename (or sidecar suffix)
        if location == ".output" {
            let filename = Path::new(rel_path)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("");

            // Sidecar files (.meta.json) are associated with their output file
            if filename.ends_with(".meta.json") {
                let base = &filename[..filename.len() - ".meta.json".len()];
                if index.output_filenames.contains(base) {
                    return true;
                }
                let base_path = format!(".output/{}", base);
                return index.output_current_paths.contains(&base_path);
            }

            if index.output_filenames.contains(filename) {
                return true;
            }
            return index.output_current_paths.contains(rel_path);
        }

        // Other locations: match by source_hash or hash
        index.known_hashes.contains(file_hash)
    }

    /// Delete a stray file that we know we created (e.g. `.output/`).
    fn delete_stray(&self, rel_path: &str) {
        let src = self.root.join(rel_path);
        match fs::remove_file(&src) {
            Ok(()) => info!("Deleted stray output file: {}", rel_path),
            Err(e) => error!("Failed to delete stray file {}: {}", rel_path, e),
        }
    }

    /// Move a stray file to the `error/` directory.
    fn move_to_error(&self, rel_path: &str) {
        let src = self.root.join(rel_path);
        let error_dir = self.root.join("error").join(crate::step4::today_date_dir());
        if let Err(e) = fs::create_dir_all(&error_dir) {
            error!("Failed to create error directory: {}", e);
            return;
        }

        let filename = src
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown");
        let mut dest = error_dir.join(filename);

        if dest.exists() {
            let stem = src
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("file");
            let suffix = src
                .extension()
                .and_then(|e| e.to_str())
                .map(|e| format!(".{}", e))
                .unwrap_or_default();
            let hex8 = &Uuid::new_v4().to_string().replace('-', "")[..8];
            dest = error_dir.join(format!("{}_{}{}", stem, hex8, suffix));
        }

        match fs::rename(&src, &dest) {
            Ok(()) => {
                info!(
                    "Moved stray file to error: {} -> {}",
                    rel_path,
                    dest.file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or("?")
                );
            }
            Err(_) => {
                // Fallback: copy + remove (cross-device move)
                match fs::copy(&src, &dest).and_then(|_| fs::remove_file(&src)) {
                    Ok(()) => {
                        info!(
                            "Moved stray file to error: {} -> {}",
                            rel_path,
                            dest.file_name()
                                .and_then(|n| n.to_str())
                                .unwrap_or("?")
                        );
                    }
                    Err(e) => {
                        error!("Failed to move stray file {} to error: {}", rel_path, e);
                    }
                }
            }
        }
    }
}

/// Recursively walk a directory and collect all file paths.
fn walk_recursive(dir: &Path) -> Vec<PathBuf> {
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
    use tempfile::TempDir;

    /// Create all watched directories inside the given root.
    fn setup_dirs(root: &Path) {
        for d in &[
            "archive",
            "incoming",
            "reviewed",
            "processed",
            "reset",
            "trash",
            ".output",
            "sorted",
        ] {
            fs::create_dir_all(root.join(d)).unwrap();
        }
    }

    /// Helper to create a Record with source_paths and optional hash/output_filename.
    fn make_record(
        source_hash: &str,
        source_paths: &[&str],
        current_paths: &[&str],
        hash: Option<&str>,
        output_filename: Option<&str>,
    ) -> Record {
        let ts = chrono::Utc::now();
        let mut rec = Record::new("test.pdf".into(), source_hash.to_string());
        for p in source_paths {
            rec.source_paths.push(crate::models::PathEntry {
                path: p.to_string(),
                timestamp: ts,
            });
        }
        for p in current_paths {
            rec.current_paths.push(crate::models::PathEntry {
                path: p.to_string(),
                timestamp: ts,
            });
        }
        rec.hash = hash.map(|h| h.to_string());
        rec.output_filename = output_filename.map(|o| o.to_string());
        rec
    }

    // -----------------------------------------------------------------------
    // Helper function tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_compute_sha256() {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("test.txt");
        fs::write(&file_path, b"hello world").unwrap();
        let hash = compute_sha256(&file_path).unwrap();
        // SHA-256 of "hello world"
        assert_eq!(
            hash,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
        // Verify it's a 64-char hex string
        assert_eq!(hash.len(), 64);
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_is_config_file_positive() {
        assert!(is_config_file("sorted/work/context.yaml"));
        assert!(is_config_file("sorted/work/smartfolders.yaml"));
        assert!(is_config_file("sorted/work/generated.yaml"));
    }

    #[test]
    fn test_is_config_file_negative() {
        assert!(!is_config_file("sorted/work/data.yaml"));
        assert!(!is_config_file("sorted/work/readme.md"));
    }

    #[test]
    fn test_is_config_file_wrong_depth() {
        // Only 2 parts -- missing context dir level
        assert!(!is_config_file("sorted/context.yaml"));
        // 4 parts -- too deep
        assert!(!is_config_file("sorted/work/sub/context.yaml"));
    }

    #[test]
    fn test_is_ignored_hidden() {
        assert!(is_ignored(".hidden"));
        assert!(is_ignored(".DS_Store"));
    }

    #[test]
    fn test_is_ignored_tilde() {
        assert!(is_ignored("~tempfile"));
        assert!(is_ignored("~lock.file"));
    }

    #[test]
    fn test_is_ignored_syncthing() {
        assert!(is_ignored(".syncthing.file.tmp"));
        assert!(is_ignored("~syncthing~file"));
    }

    #[test]
    fn test_is_ignored_tmp() {
        assert!(is_ignored("file.tmp"));
        assert!(is_ignored("document.tmp"));
    }

    #[test]
    fn test_is_ignored_normal() {
        assert!(!is_ignored("document.pdf"));
        assert!(!is_ignored("photo.jpg"));
        assert!(!is_ignored("report.docx"));
    }

    #[test]
    fn test_get_location() {
        assert_eq!(get_location("archive/file.pdf"), "archive");
        assert_eq!(get_location("incoming/doc.pdf"), "incoming");
        assert_eq!(get_location(".output/uuid"), ".output");
    }

    #[test]
    fn test_get_location_nested() {
        assert_eq!(get_location("sorted/work/file.pdf"), "sorted");
        assert_eq!(get_location("sorted/work/sub/file.pdf"), "sorted");
    }

    #[test]
    fn test_has_supported_extension_pdf() {
        assert!(has_supported_extension(Path::new("document.pdf")));
        assert!(has_supported_extension(Path::new("document.PDF")));
    }

    #[test]
    fn test_has_supported_extension_mp3() {
        assert!(has_supported_extension(Path::new("audio.mp3")));
        assert!(has_supported_extension(Path::new("audio.MP3")));
    }

    #[test]
    fn test_has_supported_extension_unsupported() {
        assert!(!has_supported_extension(Path::new("font.ttf")));
        assert!(!has_supported_extension(Path::new("data.csv")));
    }

    #[test]
    fn test_has_supported_extension_no_ext() {
        // Files with no extension pass through (are allowed)
        assert!(has_supported_extension(Path::new("Makefile")));
        assert!(has_supported_extension(Path::new("README")));
    }

    // -----------------------------------------------------------------------
    // FilesystemDetector::scan tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_scan_finds_files() {
        let dir = TempDir::new().unwrap();
        let root = dir.path();
        setup_dirs(root);

        // Create test files
        fs::write(root.join("incoming/doc.pdf"), b"pdf content").unwrap();
        fs::create_dir_all(root.join("sorted/work")).unwrap();
        fs::write(root.join("sorted/work/report.pdf"), b"report").unwrap();

        let detector = FilesystemDetector::new(root.to_path_buf(), Arc::new(tokio::sync::Notify::new()));
        let result = detector.scan();

        assert!(result.contains_key("incoming/doc.pdf"));
        assert!(result.contains_key("sorted/work/report.pdf"));
    }

    #[test]
    fn test_scan_ignores_hidden() {
        let dir = TempDir::new().unwrap();
        let root = dir.path();
        setup_dirs(root);

        fs::write(root.join("incoming/.hidden"), b"secret").unwrap();
        fs::write(root.join("incoming/visible.pdf"), b"visible").unwrap();

        let detector = FilesystemDetector::new(root.to_path_buf(), Arc::new(tokio::sync::Notify::new()));
        let result = detector.scan();

        assert!(!result.contains_key("incoming/.hidden"));
        assert!(result.contains_key("incoming/visible.pdf"));
    }

    #[test]
    fn test_scan_ignores_symlinks() {
        let dir = TempDir::new().unwrap();
        let root = dir.path();
        setup_dirs(root);

        let target = root.join("incoming/real.pdf");
        fs::write(&target, b"real content").unwrap();

        let link = root.join("incoming/link.pdf");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let detector = FilesystemDetector::new(root.to_path_buf(), Arc::new(tokio::sync::Notify::new()));
        let result = detector.scan();

        assert!(result.contains_key("incoming/real.pdf"));
        // Symlinks should be skipped
        #[cfg(unix)]
        assert!(!result.contains_key("incoming/link.pdf"));
    }

    #[test]
    fn test_scan_recursive_sorted() {
        let dir = TempDir::new().unwrap();
        let root = dir.path();
        setup_dirs(root);

        fs::create_dir_all(root.join("sorted/work")).unwrap();
        fs::write(root.join("sorted/work/deep.pdf"), b"deep content").unwrap();

        let detector = FilesystemDetector::new(root.to_path_buf(), Arc::new(tokio::sync::Notify::new()));
        let result = detector.scan();

        assert!(result.contains_key("sorted/work/deep.pdf"));
    }

    #[test]
    fn test_scan_skips_config_files() {
        let dir = TempDir::new().unwrap();
        let root = dir.path();
        setup_dirs(root);

        fs::create_dir_all(root.join("sorted/work")).unwrap();
        fs::write(root.join("sorted/work/context.yaml"), b"config: true").unwrap();
        fs::write(root.join("sorted/work/smartfolders.yaml"), b"sf: true").unwrap();
        fs::write(root.join("sorted/work/generated.yaml"), b"gen: true").unwrap();
        fs::write(root.join("sorted/work/document.pdf"), b"document").unwrap();

        let detector = FilesystemDetector::new(root.to_path_buf(), Arc::new(tokio::sync::Notify::new()));
        let result = detector.scan();

        assert!(!result.contains_key("sorted/work/context.yaml"));
        assert!(!result.contains_key("sorted/work/smartfolders.yaml"));
        assert!(!result.contains_key("sorted/work/generated.yaml"));
        assert!(result.contains_key("sorted/work/document.pdf"));
    }

    // -----------------------------------------------------------------------
    // detect (full) tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_detect_full_initial_additions() {
        let dir = TempDir::new().unwrap();
        let root = dir.path();
        setup_dirs(root);

        fs::write(root.join("incoming/a.pdf"), b"aaa").unwrap();
        fs::write(root.join("incoming/b.pdf"), b"bbb").unwrap();

        let mut detector = FilesystemDetector::new(root.to_path_buf(), Arc::new(tokio::sync::Notify::new()));
        let changes = detector.detect(&[], false).await;

        // Both files should be reported as additions
        let additions: Vec<_> = changes
            .iter()
            .filter(|c| c.event_type == EventType::Addition)
            .collect();
        assert_eq!(additions.len(), 2);

        let paths: Vec<&str> = additions.iter().map(|c| c.path.as_str()).collect();
        assert!(paths.contains(&"incoming/a.pdf"));
        assert!(paths.contains(&"incoming/b.pdf"));

        // Each should have a hash and size
        for a in &additions {
            assert!(a.hash.is_some());
            assert!(a.size.is_some());
        }
    }

    #[tokio::test]
    async fn test_detect_full_with_existing_records() {
        let dir = TempDir::new().unwrap();
        let root = dir.path();
        setup_dirs(root);

        let content = b"known content";
        fs::write(root.join("incoming/known.pdf"), content).unwrap();

        // Compute hash of the file
        let file_hash = compute_sha256(&root.join("incoming/known.pdf")).unwrap();

        // Create a DB record that matches this file
        let record = make_record(
            &file_hash,
            &["incoming/known.pdf"],
            &[],
            None,
            None,
        );

        let mut detector = FilesystemDetector::new(root.to_path_buf(), Arc::new(tokio::sync::Notify::new()));
        let changes = detector.detect(&[record], false).await;

        // The known file should NOT be re-reported as addition
        let additions: Vec<_> = changes
            .iter()
            .filter(|c| c.event_type == EventType::Addition)
            .collect();
        assert!(
            additions.is_empty(),
            "Known files should not be reported as additions"
        );
    }

    #[tokio::test]
    async fn test_detect_full_removal_for_missing() {
        let dir = TempDir::new().unwrap();
        let root = dir.path();
        setup_dirs(root);

        // Create a DB record referencing a file that does NOT exist on disk
        let record = make_record(
            "somehash",
            &["incoming/gone.pdf"],
            &[],
            None,
            None,
        );

        let mut detector = FilesystemDetector::new(root.to_path_buf(), Arc::new(tokio::sync::Notify::new()));
        let changes = detector.detect(&[record], false).await;

        let removals: Vec<_> = changes
            .iter()
            .filter(|c| c.event_type == EventType::Removal)
            .collect();
        assert_eq!(removals.len(), 1);
        assert_eq!(removals[0].path, "incoming/gone.pdf");
        assert!(removals[0].hash.is_none());
        assert!(removals[0].size.is_none());
    }

    // -----------------------------------------------------------------------
    // is_known tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_is_known_by_source_hash() {
        let record = make_record("abc123", &["incoming/doc.pdf"], &[], None, None);
        let index = FilesystemDetector::build_snapshot_index(&[record]);
        assert!(FilesystemDetector::is_known(
            "abc123",
            "incoming/doc.pdf",
            &index
        ));
    }

    #[test]
    fn test_is_known_by_hash() {
        let record = make_record(
            "source_hash",
            &["incoming/doc.pdf"],
            &[],
            Some("processed_hash"),
            None,
        );
        let index = FilesystemDetector::build_snapshot_index(&[record]);
        assert!(FilesystemDetector::is_known(
            "processed_hash",
            "incoming/other.pdf",
            &index
        ));
    }

    #[test]
    fn test_is_known_output_by_filename() {
        let record = make_record(
            "src",
            &["incoming/doc.pdf"],
            &[],
            None,
            Some("uuid-output"),
        );
        let index = FilesystemDetector::build_snapshot_index(&[record]);
        assert!(FilesystemDetector::is_known(
            "anyhash",
            ".output/uuid-output",
            &index
        ));
    }

    #[test]
    fn test_is_known_output_sidecar() {
        let record = make_record(
            "src",
            &["incoming/doc.pdf"],
            &[],
            None,
            Some("uuid-output"),
        );
        let index = FilesystemDetector::build_snapshot_index(&[record]);
        // Sidecar .meta.json files should be recognized as known
        assert!(FilesystemDetector::is_known(
            "anyhash",
            ".output/uuid-output.meta.json",
            &index
        ));
    }

    #[test]
    fn test_is_known_unknown() {
        let record = make_record("abc", &["incoming/doc.pdf"], &[], None, None);
        let index = FilesystemDetector::build_snapshot_index(&[record]);
        assert!(!FilesystemDetector::is_known(
            "completely_different_hash",
            "incoming/unknown.pdf",
            &index
        ));
    }

    // -----------------------------------------------------------------------
    // inject_full_scan_paths tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_detect_full_scan_injects_missed_files() {
        let dir = TempDir::new().unwrap();
        let root = dir.path();
        setup_dirs(root);

        // Place one file before the initial scan.
        fs::write(root.join("incoming/existing.pdf"), b"existing").unwrap();

        let mut detector = FilesystemDetector::new(root.to_path_buf(), Arc::new(tokio::sync::Notify::new()));

        // First detect: full scan — starts the inotify observer and populates
        // previous_state with "incoming/existing.pdf".
        let changes = detector.detect(&[], false).await;
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].path, "incoming/existing.pdf");

        // Simulate a file that appeared while inotify missed it (e.g. Docker
        // bind-mount gap).  Write directly — no inotify event is generated
        // because the watcher only watches for a short window and we write
        // immediately after draining.
        fs::write(root.join("incoming/missed.pdf"), b"missed").unwrap();

        // Incremental detect WITHOUT full_scan — the missed file should NOT
        // appear because there is no inotify event for it.
        let changes = detector.detect(&[], false).await;
        let missed: Vec<_> = changes
            .iter()
            .filter(|c| c.path == "incoming/missed.pdf")
            .collect();
        // The file may or may not show up via inotify depending on timing;
        // what we really care about is the full_scan=true path below.

        // Incremental detect WITH full_scan=true — inject_full_scan_paths
        // should inject the missed file so it gets picked up.
        let changes = detector.detect(&[], true).await;
        let additions: Vec<_> = changes
            .iter()
            .filter(|c| c.path == "incoming/missed.pdf" && c.event_type == EventType::Addition)
            .collect();
        assert!(
            !additions.is_empty() || missed.len() == 1,
            "missed.pdf should be detected via full_scan injection \
             (or was already picked up via inotify)"
        );
    }

    #[tokio::test]
    async fn test_full_scan_does_not_reinject_known_files() {
        let dir = TempDir::new().unwrap();
        let root = dir.path();
        setup_dirs(root);

        fs::write(root.join("incoming/only.pdf"), b"content").unwrap();

        let mut detector = FilesystemDetector::new(root.to_path_buf(), Arc::new(tokio::sync::Notify::new()));

        // Initial full scan — populates previous_state.
        let changes = detector.detect(&[], false).await;
        assert_eq!(changes.len(), 1);

        // Second call with full_scan=true — the already-known file should NOT
        // be injected again (it is in previous_state).
        let changes = detector.detect(&[], true).await;
        let additions: Vec<_> = changes
            .iter()
            .filter(|c| c.event_type == EventType::Addition)
            .collect();
        assert!(
            additions.is_empty(),
            "known files should not be re-reported after full_scan injection"
        );
    }

    #[tokio::test]
    async fn test_full_scan_injects_recursive_dir_files() {
        let dir = TempDir::new().unwrap();
        let root = dir.path();
        setup_dirs(root);

        let mut detector = FilesystemDetector::new(root.to_path_buf(), Arc::new(tokio::sync::Notify::new()));

        // Initial full scan with empty filesystem.
        let changes = detector.detect(&[], false).await;
        assert!(changes.is_empty());

        // Add a file deep in the recursive "sorted" tree.
        fs::create_dir_all(root.join("sorted/project")).unwrap();
        fs::write(root.join("sorted/project/deep.pdf"), b"deep").unwrap();

        // full_scan=true should inject it.
        let changes = detector.detect(&[], true).await;
        let deep: Vec<_> = changes
            .iter()
            .filter(|c| {
                c.path == "sorted/project/deep.pdf"
                    && c.event_type == EventType::Addition
            })
            .collect();
        assert!(
            !deep.is_empty(),
            "file in recursive dir should be injected by full_scan"
        );
    }

    #[test]
    fn test_dedup_pdfs_same_content_hash() {
        let test_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("tests/integration/generated");
        let a = test_dir.join("dedup_variant_a.pdf");
        let b = test_dir.join("dedup_variant_b.pdf");
        let c = test_dir.join("dedup_variant_c.pdf");

        if !a.exists() || !b.exists() || !c.exists() {
            eprintln!("Skipping: dedup test PDFs not generated yet");
            return;
        }

        let ch_a = compute_content_hash(&a).expect("content hash A");
        let ch_b = compute_content_hash(&b).expect("content hash B");
        let ch_c = compute_content_hash(&c).expect("content hash C");

        assert_eq!(ch_a, ch_b, "content hash A != B");
        assert_eq!(ch_b, ch_c, "content hash B != C");

        // File hashes must differ
        let fh_a = compute_sha256(&a).unwrap();
        let fh_b = compute_sha256(&b).unwrap();
        let fh_c = compute_sha256(&c).unwrap();
        assert_ne!(fh_a, fh_b, "file hash A == B (should differ)");
        assert_ne!(fh_b, fh_c, "file hash B == C (should differ)");
    }
}
