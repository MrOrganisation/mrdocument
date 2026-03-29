//! Orchestrator for document watcher v2.
//!
//! Ties together steps 1-6 into a single polling cycle that handles filesystem
//! detection, preprocessing, service calls, reconciliation, and filesystem moves.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use chrono::Utc;
use serde_json::Value as JsonValue;
use tokio::sync::Semaphore;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::config::{format_filename, SorterContextManager};
use crate::db::Database;
use crate::models::{PathEntry, Record, State};
use crate::prefilter::prefilter;
use crate::step1::{compute_sha256, FilesystemDetector};
use crate::step2;
use crate::step3::Processor;
use crate::step4::FilesystemReconciler;
use crate::step5::{
    RootSmartFolderEntry, RootSmartFolderReconciler, SmartFolderEntry,
    SmartFolderReconciler,
};
use crate::step6::AudioLinkReconciler;
use crate::step7::HistoryReconciler;

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Extract context field names from a [`SorterContextManager`].
///
/// Bridge between the sorter config and the v2 pipeline.
pub fn context_field_names_from_sorter(
    cm: &SorterContextManager,
) -> HashMap<String, Vec<String>> {
    cm.contexts
        .iter()
        .map(|(name, ctx)| (name.clone(), ctx.field_names.clone()))
        .collect()
}

/// Extract context folder hierarchy from a [`SorterContextManager`].
///
/// Maps context name to its folder field list (e.g. `{"arbeit": ["context", "sender"]}`).
pub fn context_folders_from_sorter(
    cm: &SorterContextManager,
) -> HashMap<String, Vec<String>> {
    cm.contexts
        .iter()
        .map(|(name, ctx)| (name.clone(), ctx.folders.clone()))
        .collect()
}

/// Get all contexts as raw JSON values for mrdocument API calls.
///
/// Loads the full YAML dict for each context (with fields, description, etc.)
/// needed by `/classify_audio` and `/classify_transcript` endpoints.
pub fn contexts_for_api_from_sorter(cm: &SorterContextManager) -> Vec<JsonValue> {
    let mut result = Vec::new();
    for name in cm.contexts.keys() {
        if let Some(ctx) = cm.get_context_for_api(name) {
            result.push(ctx);
        }
    }
    result
}

/// Build a callback that recomputes `assigned_filename` from record metadata.
///
/// Uses the context's filename pattern (including conditional rules) and the
/// record's `original_filename` for pattern matching / `{source_filename}`.
pub fn build_recompute_filename(
    cm: &SorterContextManager,
) -> Box<dyn Fn(&Record) -> Option<String> + Send + Sync> {
    let contexts = cm.contexts.clone();
    Box::new(move |record: &Record| -> Option<String> {
        let ctx_name = record.context.as_ref()?;
        let metadata = record.metadata.as_ref()?;
        let ctx = contexts.get(ctx_name)?;
        let pattern = ctx.resolve_filename_pattern(Some(record.original_filename.as_str()));

        let new_name =
            format_filename(metadata, &pattern, Some(record.original_filename.as_str()));

        // format_filename always appends .pdf -- preserve the original extension
        // when the record's assigned_filename uses a different one (e.g. .txt).
        if let Some(ref assigned) = record.assigned_filename {
            let orig_ext = Path::new(assigned)
                .extension()
                .and_then(|e| e.to_str())
                .map(|e| format!(".{}", e.to_ascii_lowercase()))
                .unwrap_or_default();
            if !orig_ext.is_empty() && orig_ext != ".pdf" {
                let stem = Path::new(&new_name)
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or(&new_name);
                return Some(format!("{}{}", stem, orig_ext));
            }
        }

        Some(new_name)
    })
}

// ---------------------------------------------------------------------------
// Record snapshot for change detection
// ---------------------------------------------------------------------------

/// Snapshot of persistent fields for change detection.
#[derive(Debug, Clone, PartialEq)]
struct RecordSnapshot {
    state: State,
    output_filename: Option<String>,
    context: Option<String>,
    assigned_filename: Option<String>,
    hash: Option<String>,
    source_paths: Vec<String>,
    current_paths: Vec<String>,
    missing_source_paths: Vec<String>,
    missing_current_paths: Vec<String>,
    metadata_repr: String,
}

fn take_snapshot(record: &Record) -> RecordSnapshot {
    RecordSnapshot {
        state: record.state,
        output_filename: record.output_filename.clone(),
        context: record.context.clone(),
        assigned_filename: record.assigned_filename.clone(),
        hash: record.hash.clone(),
        source_paths: record
            .source_paths
            .iter()
            .map(|pe| format!("{}@{}", pe.path, pe.timestamp))
            .collect(),
        current_paths: record
            .current_paths
            .iter()
            .map(|pe| format!("{}@{}", pe.path, pe.timestamp))
            .collect(),
        missing_source_paths: record
            .missing_source_paths
            .iter()
            .map(|pe| format!("{}@{}", pe.path, pe.timestamp))
            .collect(),
        missing_current_paths: record
            .missing_current_paths
            .iter()
            .map(|pe| format!("{}@{}", pe.path, pe.timestamp))
            .collect(),
        metadata_repr: record
            .metadata
            .as_ref()
            .map(|m| m.to_string())
            .unwrap_or_default(),
    }
}

// ---------------------------------------------------------------------------
// DocumentWatcherV2
// ---------------------------------------------------------------------------

/// Main orchestrator: polling cycle that ties steps 1-6 together.
#[allow(dead_code)]
pub struct DocumentWatcherV2 {
    pub root: PathBuf,
    pub name: String,
    pub db: Arc<Database>,
    pub service_url: String,
    pub context_field_names: Option<HashMap<String, Vec<String>>>,
    pub context_folders: Option<HashMap<String, Vec<String>>>,
    pub folder_field_candidates:
        Option<HashMap<String, HashMap<String, (Vec<String>, bool)>>>,
    pub detector: FilesystemDetector,
    pub processor: Processor,
    pub reconciler: FilesystemReconciler,
    pub smart_folder_reconciler: Option<SmartFolderReconciler>,
    pub root_smart_folder_reconciler: Option<RootSmartFolderReconciler>,
    pub audio_link_reconciler: Option<AudioLinkReconciler>,
    pub history_reconciler: HistoryReconciler,
    pub context_manager: Option<SorterContextManager>,

    _in_flight: HashSet<Uuid>,
    _semaphore: Arc<Semaphore>,
    _pending_full_scan: bool,
    _audio_links: bool,
    _recompute_filename:
        Option<Box<dyn Fn(&Record) -> Option<String> + Send + Sync>>,
}

impl DocumentWatcherV2 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        root: PathBuf,
        db: Arc<Database>,
        service_url: String,
        context_field_names: Option<HashMap<String, Vec<String>>>,
        context_folders: Option<HashMap<String, Vec<String>>>,
        _poll_interval: f64,
        processor_timeout: f64,
        stt_url: Option<String>,
        contexts_for_api: Option<Vec<JsonValue>>,
        smart_folders: Option<Vec<SmartFolderEntry>>,
        root_smart_folders: Option<Vec<RootSmartFolderEntry>>,
        audio_links: bool,
        max_concurrent: usize,
        name: Option<String>,
        context_manager: Option<SorterContextManager>,
        db_notify: Arc<tokio::sync::Notify>,
    ) -> Self {
        let watcher_name = name.clone().unwrap_or_else(|| {
            root.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("unknown")
                .to_string()
        });

        let detector = FilesystemDetector::new(root.clone(), db_notify);
        let processor = Processor::new(
            root.clone(),
            watcher_name.clone(),
            service_url.clone(),
            stt_url,
            processor_timeout,
            contexts_for_api,
            context_manager.clone(),
        );
        let reconciler = FilesystemReconciler::new(root.clone());

        let smart_folder_reconciler =
            smart_folders.map(|sf| SmartFolderReconciler::new(root.clone(), sf));
        let root_smart_folder_reconciler =
            root_smart_folders.map(|rsf| RootSmartFolderReconciler::new(root.clone(), rsf));
        let audio_link_reconciler = if audio_links {
            Some(AudioLinkReconciler::new(root.clone()))
        } else {
            None
        };
        let history_reconciler = HistoryReconciler::new(root.clone());

        Self {
            root,
            name: watcher_name,
            db,
            service_url,
            context_field_names,
            context_folders,
            folder_field_candidates: None,
            detector,
            processor,
            reconciler,
            smart_folder_reconciler,
            root_smart_folder_reconciler,
            audio_link_reconciler,
            history_reconciler,
            context_manager,
            _in_flight: HashSet::new(),
            _semaphore: Arc::new(Semaphore::new(max_concurrent)),
            _pending_full_scan: false,
            _audio_links: audio_links,
            _recompute_filename: None,
        }
    }

    /// Read sidecar JSON for an `.output` file.
    ///
    /// `output_path` is a relative path like `.output/uuid-123`.
    /// Returns parsed sidecar dict, or empty object if not found/parse error.
    #[allow(dead_code)]
    fn read_sidecar(&self, output_path: &str) -> JsonValue {
        Self::read_sidecar_from_root(&self.root, &self.name, output_path)
    }

    /// Static version of read_sidecar that doesn't require `&self`.
    fn read_sidecar_from_root(root: &Path, watcher_name: &str, output_path: &str) -> JsonValue {
        let sidecar_path = root.join(format!("{}.meta.json", output_path));
        match std::fs::read_to_string(&sidecar_path) {
            Ok(content) => match serde_json::from_str(&content) {
                Ok(val) => val,
                Err(e) => {
                    warn!(
                        "[{}] Failed to parse sidecar {}: {}",
                        watcher_name,
                        sidecar_path.display(),
                        e
                    );
                    JsonValue::Object(serde_json::Map::new())
                }
            },
            Err(e) => {
                warn!(
                    "[{}] Failed to read sidecar {}: {}",
                    watcher_name,
                    sidecar_path.display(),
                    e
                );
                JsonValue::Object(serde_json::Map::new())
            }
        }
    }

    /// Ingest a completed `.output` file into the record.
    ///
    /// Reads sidecar metadata, computes hash, checks for duplicates,
    /// updates `current_paths`, and clears `output_filename` so step 7
    /// won't re-launch.
    ///
    /// Static method to allow calling from spawned tasks without `&self`.
    async fn ingest_output(
        db: &Database,
        root: &Path,
        watcher_name: &str,
        record_id: Uuid,
        output_filename: &str,
    ) -> Result<()> {
        let output_path = root.join(".output").join(output_filename);
        if !output_path.exists() {
            return Ok(());
        }

        let file_size = std::fs::metadata(&output_path)
            .map(|m| m.len())
            .unwrap_or(0);
        if file_size == 0 {
            Self::clear_output_filename(db, watcher_name, record_id, output_filename, true)
                .await;
            // Clean up the 0-byte output file and sidecar so stray detection
            // doesn't try to delete them later.
            let _ = std::fs::remove_file(&output_path);
            let sidecar_path = root.join(".output").join(format!("{}.meta.json", output_filename));
            let _ = std::fs::remove_file(&sidecar_path);
            return Ok(());
        }

        let sidecar = Self::read_sidecar_from_root(
            root,
            watcher_name,
            &format!(".output/{}", output_filename),
        );
        let file_hash = compute_sha256(&output_path)?;
        let now = Utc::now();

        // Re-fetch record from DB (reconcile may have modified it)
        let snapshot = db.get_snapshot(Some(watcher_name)).await?;
        let fresh = match snapshot.iter().find(|r| r.id == record_id) {
            Some(r) => r,
            None => return Ok(()),
        };

        if fresh.output_filename.as_deref() != Some(output_filename) {
            return Ok(());
        }

        // Duplicate hash check
        let is_dup = snapshot.iter().any(|r| {
            r.id != record_id
                && (r.source_hash == file_hash
                    || r.hash.as_ref() == Some(&file_hash))
        });

        if is_dup {
            warn!(
                "[{}] Duplicate hash {} for output {}, discarding",
                watcher_name, file_hash, output_filename,
            );
            let mut fresh = fresh.clone();
            fresh
                .deleted_paths
                .push(format!(".output/{}", output_filename));
            let mut remaining = Vec::new();
            for pe in &fresh.source_paths {
                if Record::decompose_path(&pe.path).0 == "archive" {
                    fresh.duplicate_sources.push(pe.path.clone());
                } else {
                    remaining.push(pe.clone());
                }
            }
            fresh.source_paths = remaining;
            fresh.state = State::HasError;
            fresh.output_filename = None;
            db.save_record(&fresh).await?;
            return Ok(());
        }

        let mut fresh = fresh.clone();
        if fresh.context.is_none() {
            if let Some(ctx) = sidecar.get("context").and_then(|v| v.as_str()) {
                fresh.context = Some(ctx.to_string());
            }
        }
        fresh.metadata = step2::merge_metadata(
            fresh.metadata.as_ref(),
            sidecar.get("metadata").cloned(),
        );
        fresh.assigned_filename = sidecar
            .get("assigned_filename")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        fresh.hash = Some(file_hash);
        fresh.current_paths.push(PathEntry {
            path: format!(".output/{}", output_filename),
            timestamp: now,
        });
        fresh.output_filename = None;
        db.save_record(&fresh).await?;
        Ok(())
    }

    /// Clear `output_filename` on a record (e.g. after processing error).
    ///
    /// Static method to allow calling from spawned tasks without `&self`.
    async fn clear_output_filename(
        db: &Database,
        watcher_name: &str,
        record_id: Uuid,
        output_filename: &str,
        is_error: bool,
    ) {
        let result: Result<()> = async {
            let snapshot = db.get_snapshot(Some(watcher_name)).await?;
            if let Some(fresh) = snapshot.iter().find(|r| r.id == record_id) {
                if fresh.output_filename.as_deref() == Some(output_filename) {
                    let mut fresh = fresh.clone();
                    if is_error {
                        fresh.state = State::HasError;
                    }
                    fresh.output_filename = None;
                    db.save_record(&fresh).await?;
                }
            }
            Ok(())
        }
        .await;

        if let Err(e) = result {
            warn!(
                "[{}] Failed to clear output_filename for {}: {}",
                watcher_name, output_filename, e
            );
        }
    }

    /// Stop observer and wait for in-flight processing tasks.
    #[allow(dead_code)]
    pub async fn shutdown(&mut self) {
        self.detector.stop();
        // In-flight tasks will naturally complete since we hold references.
        // The semaphore ensures no new work starts.
        if !self._in_flight.is_empty() {
            info!(
                "[{}] Waiting for {} processing tasks...",
                self.name,
                self._in_flight.len()
            );
        }
    }

    /// Reload context and smart folder config from disk.
    pub fn reload_config(
        &mut self,
        load_smart_folders_fn: &dyn Fn(&SorterContextManager) -> Option<Vec<SmartFolderEntry>>,
        load_root_smart_folders_fn: &dyn Fn(&Path) -> (Option<Vec<RootSmartFolderEntry>>, Vec<std::path::PathBuf>),
    ) {
        let cm = match &mut self.context_manager {
            Some(cm) => cm,
            None => return,
        };

        cm.load();
        self.context_field_names = Some(context_field_names_from_sorter(cm));
        self.context_folders = Some(context_folders_from_sorter(cm));
        self.folder_field_candidates = Some(cm.folder_field_candidates());
        self._recompute_filename = Some(build_recompute_filename(cm));

        // Update processor contexts and context_manager
        self.processor.contexts = Some(contexts_for_api_from_sorter(cm));
        self.processor.context_manager = Some(cm.clone());

        // Reload smart folders
        let smart_folders = load_smart_folders_fn(cm);
        self.smart_folder_reconciler =
            smart_folders.map(|sf| SmartFolderReconciler::new(self.root.clone(), sf));

        // Reload root-level smart folders (including discovered smartfolder_paths)
        let (root_smart_folders, smartfolder_paths) = load_root_smart_folders_fn(&self.root);
        self.root_smart_folder_reconciler =
            root_smart_folders.map(|rsf| RootSmartFolderReconciler::new(self.root.clone(), rsf));
        self.detector.set_smartfolder_paths(smartfolder_paths);

        info!(
            "[{}] Config reloaded: {} context(s)",
            self.name,
            cm.contexts.len()
        );
    }

    /// Returns true if a pending full scan flag was set (e.g. by config change).
    pub fn pending_full_scan(&self) -> bool {
        self._pending_full_scan
    }

    /// Clear the pending full scan flag.
    pub fn clear_pending_full_scan(&mut self) {
        self._pending_full_scan = false;
    }

    /// Execute one complete pipeline cycle.
    ///
    /// # Arguments
    /// * `full_scan` - If true, run prefilter and full filesystem scan in the
    ///   detector (vs incremental inotify-only). Symlink reconciliation runs
    ///   on every cycle.
    ///
    /// # Returns
    /// `true` if any record state transitions occurred (caller should re-run
    /// the cycle to let downstream steps act on the new states).
    pub async fn run_cycle(&mut self, full_scan: bool) -> Result<bool> {
        let t0 = Instant::now();

        // 0. Move unsupported file types to error/ (full scan only)
        if full_scan {
            prefilter(&self.root);
        }
        let t_prefilter = Instant::now();

        // 1. Get current DB snapshot (filtered by username)
        let snapshot = self.db.get_snapshot(Some(self.name.as_str())).await?;
        let t_snapshot = Instant::now();

        // 2. Detect filesystem changes
        let changes = self.detector.detect(&snapshot, full_scan).await;
        let t_detect = Instant::now();

        // Check if config files changed (triggers reload + full scan)
        if self.detector.config_changed {
            self.detector.config_changed = false;
            // Note: reload_config needs external callbacks; the caller
            // (run_watcher) should handle this via the pending flag.
            self._pending_full_scan = true;
        }

        if !changes.is_empty() {
            info!(
                "[{}] Cycle: {} changes detected, {} records in DB",
                self.name,
                changes.len(),
                snapshot.len(),
            );
            for c in &changes {
                info!(
                    "[{}]   change: {} {}",
                    self.name,
                    c.event_type.as_str(),
                    c.path
                );
            }
        }

        // 3-6. Preprocess if there are changes
        let mut modified_ids: Vec<Uuid> = Vec::new();
        let mut new_records: Vec<Record> = Vec::new();
        let mut snapshot_mut = snapshot;

        if !changes.is_empty() {
            let mut created: Vec<Record> = Vec::new();
            let (m_ids, created_recs, rejected_paths) = step2::preprocess(
                &changes,
                &mut snapshot_mut,
                &mut created,
                |path| Self::read_sidecar_from_root(&self.root, &self.name, path),
                self.context_folders.as_ref(),
                self.folder_field_candidates.as_ref(),
            );
            modified_ids = m_ids;
            new_records = created_recs;

            // Move files rejected due to invalid context to error/
            for (src_rel, dest_rel) in &rejected_paths {
                let src = self.root.join(src_rel);
                let dest = self.root.join(dest_rel);
                if let Some(parent) = dest.parent() {
                    if let Err(e) = std::fs::create_dir_all(parent) {
                        error!(
                            "[{}] Failed to create error directory {}: {}",
                            self.name,
                            parent.display(),
                            e
                        );
                        continue;
                    }
                }
                match std::fs::rename(&src, &dest) {
                    Ok(()) => info!(
                        "[{}] Moved invalid-context file {} -> {}",
                        self.name, src_rel, dest_rel
                    ),
                    Err(e) => error!(
                        "[{}] Failed to move invalid-context file {} -> {}: {}",
                        self.name, src_rel, dest_rel, e
                    ),
                }
            }

            if !modified_ids.is_empty() || !new_records.is_empty() {
                info!(
                    "[{}] Preprocess: {} modified, {} new",
                    self.name,
                    modified_ids.len(),
                    new_records.len(),
                );
            }

            for record in &mut new_records {
                record.username = Some(self.name.clone());
                self.db.create_record(record).await?;
            }

            // Save modified records from snapshot
            for record in &snapshot_mut {
                if modified_ids.contains(&record.id) {
                    self.db.save_record(record).await?;
                }
            }
        }

        // 7. Launch processing as background tasks (non-blocking)
        let to_process = self
            .db
            .get_records_with_output_filename(Some(self.name.as_str()))
            .await?;
        // Prune in-flight set: records whose processing completed (output_filename
        // cleared by ingest_output) should be eligible for re-launch (reclassify).
        let active_ids: HashSet<Uuid> = to_process.iter().map(|r| r.id).collect();
        self._in_flight.retain(|id| active_ids.contains(id));
        let new_launches: Vec<Record> = to_process
            .into_iter()
            .filter(|r| !self._in_flight.contains(&r.id))
            .collect();

        if !new_launches.is_empty() {
            info!(
                "[{}] Launching {} processing tasks ({} already in-flight)",
                self.name,
                new_launches.len(),
                self._in_flight.len(),
            );
        }

        for record in new_launches {
            info!(
                "[{}]   process: {:?} state={} src={}",
                self.name,
                record.output_filename,
                record.state.as_str(),
                record
                    .source_file()
                    .map(|sf| sf.path.as_str())
                    .unwrap_or("none"),
            );
            self._in_flight.insert(record.id);

            let output_filename = record
                .output_filename
                .clone()
                .unwrap_or_default();
            let record_id = record.id;

            // Clone/Arc shared state for the spawned task
            let semaphore = self._semaphore.clone();
            let db = Arc::clone(&self.db);
            let root = self.root.clone();
            let watcher_name = self.name.clone();

            let task_processor = self.processor.clone();

            tokio::spawn(async move {
                let _permit = match semaphore.acquire().await {
                    Ok(p) => p,
                    Err(e) => {
                        error!("[{}] Semaphore error: {}", watcher_name, e);
                        return;
                    }
                };

                match task_processor.process_one(&record).await {
                    Ok(()) => {
                        // Ingest result immediately to close the re-launch gap
                        if let Err(e) = Self::ingest_output(
                            &db,
                            &root,
                            &watcher_name,
                            record_id,
                            &output_filename,
                        )
                        .await
                        {
                            error!(
                                "[{}] Ingest failed for {}: {}",
                                watcher_name, output_filename, e
                            );
                            Self::clear_output_filename(
                                &db,
                                &watcher_name,
                                record_id,
                                &output_filename,
                                false,
                            )
                            .await;
                        }
                    }
                    Err(e) => {
                        error!(
                            "[{}] Processing failed for {}: {}",
                            watcher_name, output_filename, e
                        );
                        Self::clear_output_filename(
                            &db,
                            &watcher_name,
                            record_id,
                            &output_filename,
                            true,
                        )
                        .await;
                    }
                }
            });
        }

        // 8. Reconcile all records (only save when something changed)
        let t_reconcile = Instant::now();
        let mut all_records = self.db.get_snapshot(Some(self.name.as_str())).await?;
        let mut saves = 0usize;
        let mut state_transitions = 0usize;

        for record in &mut all_records {
            let snap_before = take_snapshot(record);
            let old_state = record.state;

            let recompute_fn: Option<&dyn Fn(&Record) -> Option<String>> =
                self._recompute_filename.as_ref().map(|f| &**f as &dyn Fn(&Record) -> Option<String>);
            let result = step2::reconcile(
                record,
                self.context_field_names.as_ref(),
                self.context_folders.as_ref(),
                recompute_fn,
            );

            match result {
                None => {
                    info!("[{}] Reconcile: DELETE {}", self.name, record.id);
                    self.db.delete_record(record.id).await?;
                    saves += 1;
                }
                Some(reconciled) => {
                    let snap_after = take_snapshot(reconciled);
                    let has_temp = reconciled.target_path.is_some()
                        || reconciled.source_reference.is_some()
                        || reconciled.current_reference.is_some()
                        || !reconciled.duplicate_sources.is_empty()
                        || !reconciled.deleted_paths.is_empty();
                    let changed = has_temp || snap_after != snap_before;

                    if reconciled.state != old_state {
                        state_transitions += 1;
                        info!(
                            "[{}] Reconcile: {} {}->{}  ofn={:?} src_ref={:?}",
                            self.name,
                            reconciled.original_filename,
                            old_state.as_str(),
                            reconciled.state.as_str(),
                            reconciled.output_filename,
                            reconciled.source_reference,
                        );
                    }

                    if changed {
                        self.db.save_record(reconciled).await?;
                        saves += 1;
                    }
                }
            }
        }

        let t_fs = Instant::now();

        // 9. Filesystem reconcile (move files based on temp fields)
        let mut actionable = self
            .db
            .get_records_with_temp_fields(Some(self.name.as_str()))
            .await?;
        if !actionable.is_empty() {
            info!(
                "[{}] Filesystem reconcile: {} records",
                self.name,
                actionable.len()
            );
        }
        self.reconciler.reconcile(&mut actionable);

        // 10. Clear temp fields and finalize
        for record in &mut actionable {
            record.clear_temporary_fields();
            if record.state == State::NeedsDeletion {
                record.state = State::IsDeleted;
            }
            self.db.save_record(record).await?;
        }

        let t_symlink = Instant::now();

        // 11. Symlink reconciliation (smart folders + root smart folders + audio links + history)
        {
            let complete_snapshot = self.db.get_snapshot(Some(self.name.as_str())).await?;
            let complete_records: Vec<&Record> = complete_snapshot
                .iter()
                .filter(|r| r.state == State::IsComplete)
                .collect();
            let sorted_records: Vec<&Record> = complete_records
                .iter()
                .filter(|r| r.current_location().as_deref() == Some("sorted"))
                .copied()
                .collect();

            if let Some(ref sfr) = self.smart_folder_reconciler {
                if !sorted_records.is_empty() {
                    let records_owned: Vec<Record> =
                        sorted_records.iter().map(|r| (*r).clone()).collect();
                    sfr.reconcile(&records_owned);
                }
                sfr.cleanup_orphans();
            }

            if let Some(ref rsfr) = self.root_smart_folder_reconciler {
                if !sorted_records.is_empty() {
                    let records_owned: Vec<Record> =
                        sorted_records.iter().map(|r| (*r).clone()).collect();
                    rsfr.reconcile(&records_owned);
                }
                rsfr.cleanup_orphans();
            }

            if let Some(ref mut alr) = self.audio_link_reconciler {
                let link_location_records: Vec<Record> = complete_records
                    .iter()
                    .filter(|r| {
                        let loc = r.current_location();
                        loc.as_deref() == Some("sorted")
                            || loc.as_deref() == Some("processed")
                    })
                    .map(|r| (*r).clone())
                    .collect();
                if !link_location_records.is_empty() {
                    alr.reconcile(&link_location_records);
                }
                alr.cleanup_orphans();
            }

            // History folder: symlinks in history/YYYY-MM-DD/ for all complete records
            let history_records: Vec<Record> = complete_records
                .iter()
                .filter(|r| {
                    let loc = r.current_location();
                    loc.as_deref() == Some("sorted")
                        || loc.as_deref() == Some("processed")
                })
                .map(|r| (*r).clone())
                .collect();
            self.history_reconciler.reconcile(&history_records);
            self.history_reconciler.cleanup_orphans();
        }

        // Drain notification channel so that events generated by this cycle's
        // own filesystem operations (symlink reconcilers, file moves, etc.)
        // don't immediately wake up wait_for_event and cause empty cycling.
        self.detector.drain_notifications();

        let had_activity = state_transitions > 0
            || !actionable.is_empty()
            || !modified_ids.is_empty()
            || !new_records.is_empty();

        let elapsed = t0.elapsed();
        info!(
            "[{}] Cycle done: {:.1}s (prefilter={:.2} snapshot={:.2} detect={:.2} reconcile={:.2} fs={:.2} symlink={:.2}), \
             {} records, {} changes, {} reconcile saves, {} state transitions",
            self.name,
            elapsed.as_secs_f64(),
            (t_prefilter - t0).as_secs_f64(),
            (t_snapshot - t_prefilter).as_secs_f64(),
            (t_detect - t_snapshot).as_secs_f64(),
            (t_fs - t_reconcile).as_secs_f64(),
            (t_symlink - t_fs).as_secs_f64(),
            (Instant::now() - t_symlink).as_secs_f64(),
            all_records.len(),
            changes.len(),
            saves,
            state_transitions,
        );

        Ok(had_activity)
    }
}
