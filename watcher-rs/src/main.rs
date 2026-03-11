//! Document watcher v2 -- application entry point.
//!
//! Single-process replacement for the v1 watcher + sorter pair.
//! Uses a polling loop with [`DocumentWatcherV2`] orchestrators per user root.

mod config;
mod db;
mod models;
mod orchestrator;
mod prefilter;
mod step1;
mod step2;
mod step3;
mod step4;
mod step5;
mod step6;

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::extract::State as AxumState;
use axum::response::Json;
use axum::routing::get;
use axum::Router;
use serde_json::json;
use tokio::signal;
use tokio::time::{self, Duration};
use tracing::{error, info, warn};

use crate::config::{
    get_username_from_root, SmartFolderConfig, SorterContextManager, WatcherConfig,
};
use crate::db::Database;
use crate::orchestrator::{
    context_field_names_from_sorter, context_folders_from_sorter,
    contexts_for_api_from_sorter, DocumentWatcherV2,
};
use crate::models::Record;
use crate::step1::compute_content_hash;
use crate::step4::move_file;
use crate::step5::{RootSmartFolderEntry, SmartFolderEntry};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Required subdirectories for each user root.
const REQUIRED_DIRS: &[&str] = &[
    "archive",
    "incoming",
    "reviewed",
    "processed",
    "trash",
    ".output",
    "sorted",
    "error",
    "void",
    "missing",
    "duplicates",
];

// ---------------------------------------------------------------------------
// Health server
// ---------------------------------------------------------------------------

/// Health check handler.
async fn health_handler(
    AxumState(ready): AxumState<Arc<AtomicBool>>,
) -> (axum::http::StatusCode, Json<serde_json::Value>) {
    if ready.load(Ordering::Relaxed) {
        (
            axum::http::StatusCode::OK,
            Json(json!({
                "status": "healthy",
                "service": "watcher-v2"
            })),
        )
    } else {
        (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "status": "not_ready",
                "service": "watcher-v2"
            })),
        )
    }
}

/// Start the health check HTTP server on the given port.
async fn start_health_server(port: u16, ready: Arc<AtomicBool>) -> Result<()> {
    let app = Router::new()
        .route("/health", get(health_handler))
        .with_state(ready);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("Health server listening on port {}", port);

    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        if let Err(e) = axum::serve(listener, app).await {
            error!("Health server error: {}", e);
        }
    });

    Ok(())
}

// ---------------------------------------------------------------------------
// Directory setup
// ---------------------------------------------------------------------------

/// Create all required directories for a user root.
fn ensure_directories(user_root: &Path) {
    for d in REQUIRED_DIRS {
        let dir = user_root.join(d);
        if let Err(e) = std::fs::create_dir_all(&dir) {
            warn!(
                "Failed to create directory {}: {}",
                dir.display(),
                e
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Smart folder loading
// ---------------------------------------------------------------------------

/// Load smart folders: try `sorted/` YAML files first, fallback to embedded.
///
/// Returns a list of [`SmartFolderEntry`] or `None`.
fn load_smart_folders(context_manager: &SorterContextManager) -> Option<Vec<SmartFolderEntry>> {
    // Contexts with sorted/ smartfolders.yaml files
    let sorted_sf = context_manager.load_smart_folders_from_sorted();

    // Contexts that have embedded smart_folders in context YAML
    let mut embedded_contexts = HashSet::new();
    for (ctx_name, ctx) in &context_manager.contexts {
        if !ctx.smart_folders.is_empty() {
            embedded_contexts.insert(ctx_name.clone());
        }
    }

    let mut smart_folders = Vec::new();

    // For each context: prefer sorted/ file, fallback to embedded
    let all_contexts: HashSet<String> = sorted_sf
        .keys()
        .cloned()
        .chain(embedded_contexts.iter().cloned())
        .collect();

    for ctx_name in &all_contexts {
        if let Some(sf_list) = sorted_sf.get(ctx_name) {
            for (_sf_name, sf_config) in sf_list {
                smart_folders.push(SmartFolderEntry {
                    context: ctx_name.clone(),
                    config: sf_config.clone(),
                });
            }
        } else if embedded_contexts.contains(ctx_name) {
            if let Some(ctx) = context_manager.contexts.get(ctx_name) {
                for (_sf_name, sf_config) in &ctx.smart_folders {
                    smart_folders.push(SmartFolderEntry {
                        context: ctx_name.clone(),
                        config: sf_config.clone(),
                    });
                }
            }
        }
    }

    if smart_folders.is_empty() {
        None
    } else {
        Some(smart_folders)
    }
}

/// Load root-level smart folders from `{root}/smartfolders.yaml`.
///
/// Returns a list of [`RootSmartFolderEntry`] or `None` if file missing/empty.
fn load_root_smart_folders(root: &Path) -> Option<Vec<RootSmartFolderEntry>> {
    let config_path = root.join("smartfolders.yaml");
    if !config_path.is_file() {
        return None;
    }

    let content = match std::fs::read_to_string(&config_path) {
        Ok(c) => c,
        Err(e) => {
            warn!("Failed to read {}: {}", config_path.display(), e);
            return None;
        }
    };

    let data: serde_yaml::Value = match serde_yaml::from_str(&content) {
        Ok(d) => d,
        Err(e) => {
            warn!("Failed to parse {}: {}", config_path.display(), e);
            return None;
        }
    };

    let sf_dict = match data.get("smart_folders") {
        Some(serde_yaml::Value::Mapping(m)) => m,
        _ => return None,
    };

    let mut entries = Vec::new();

    for (key, sf_data) in sf_dict {
        let sf_name = match key.as_str() {
            Some(n) => n.to_string(),
            None => continue,
        };

        if !sf_data.is_mapping() {
            warn!("Root smart folder '{}': expected dict, skipping", sf_name);
            continue;
        }

        let context = match sf_data.get("context").and_then(|v| v.as_str()) {
            Some(c) => c.to_string(),
            None => {
                warn!(
                    "Root smart folder '{}': missing context, skipping",
                    sf_name
                );
                continue;
            }
        };

        let path_str = match sf_data.get("path").and_then(|v| v.as_str()) {
            Some(p) => p.to_string(),
            None => {
                warn!("Root smart folder '{}': missing path, skipping", sf_name);
                continue;
            }
        };

        // Resolve path: absolute stays absolute, relative resolves against root
        let path = if Path::new(&path_str).is_absolute() {
            PathBuf::from(&path_str)
        } else {
            root.join(&path_str)
        };

        // Parse condition/filename_regex via SmartFolderConfig
        let config = match SmartFolderConfig::from_dict(&sf_name, sf_data, &context) {
            Some(c) => c,
            None => continue,
        };

        entries.push(RootSmartFolderEntry {
            name: sf_name,
            context,
            path,
            config,
        });
    }

    if entries.is_empty() {
        None
    } else {
        Some(entries)
    }
}

// ---------------------------------------------------------------------------
// User setup
// ---------------------------------------------------------------------------

/// Set up a [`DocumentWatcherV2`] for a single user root.
fn setup_user(
    user_root: &Path,
    db: Arc<Database>,
    service_url: &str,
    poll_interval: f64,
    processor_timeout: f64,
    stt_url: Option<&str>,
    max_concurrent: usize,
) -> DocumentWatcherV2 {
    let username = get_username_from_root(user_root);
    ensure_directories(user_root);

    let mut context_field_names = None;
    let mut ctx_folders = None;
    let mut contexts_for_api = None;
    let mut smart_folders = None;
    let mut context_manager = SorterContextManager::new(user_root, &username);

    if context_manager.load() {
        context_field_names = Some(context_field_names_from_sorter(&context_manager));
        ctx_folders = Some(context_folders_from_sorter(&context_manager));
        contexts_for_api = Some(contexts_for_api_from_sorter(&context_manager));
        info!(
            "[{}] Loaded {} context(s)",
            username,
            context_field_names.as_ref().map(|m| m.len()).unwrap_or(0)
        );

        smart_folders = load_smart_folders(&context_manager);
        if let Some(ref sf) = smart_folders {
            info!("[{}] Loaded {} smart folder(s)", username, sf.len());
        }
    }

    let root_smart_folders = load_root_smart_folders(user_root);
    if let Some(ref rsf) = root_smart_folders {
        info!(
            "[{}] Loaded {} root smart folder(s)",
            username,
            rsf.len()
        );
    }

    DocumentWatcherV2::new(
        user_root.to_path_buf(),
        db,
        service_url.to_string(),
        context_field_names,
        ctx_folders,
        poll_interval,
        processor_timeout,
        stt_url.map(|s| s.to_string()),
        contexts_for_api,
        smart_folders,
        root_smart_folders,
        true, // audio_links
        max_concurrent,
        Some(username),
        Some(context_manager),
    )
}

// ---------------------------------------------------------------------------
// Content hash backfill
// ---------------------------------------------------------------------------

/// Backfill `source_content_hash` and `content_hash` for existing records
/// where these columns are NULL but could be computed from files on disk.
/// Runs once at startup before the first cycle.
async fn backfill_content_hashes(db: &Database, root: &Path, username: &str) -> Result<()> {
    let snapshot = db.get_snapshot(Some(username)).await?;
    let mut backfilled = 0u32;

    for record in &snapshot {
        let needs_source = record.source_content_hash.is_none();
        let needs_current = record.content_hash.is_none() && record.hash.is_some();

        if !needs_source && !needs_current {
            continue;
        }

        let mut new_source_ch: Option<String> = None;
        let mut new_content_ch: Option<String> = None;

        if needs_source {
            if let Some(sf) = record.source_file() {
                let abs_path = root.join(&sf.path);
                if abs_path.is_file() {
                    new_source_ch = compute_content_hash(&abs_path);
                }
            }
        }

        if needs_current {
            if let Some(cf) = record.current_file() {
                let abs_path = root.join(&cf.path);
                if abs_path.is_file() {
                    new_content_ch = compute_content_hash(&abs_path);
                }
            }
        }

        if new_source_ch.is_some() || new_content_ch.is_some() {
            db.update_content_hashes(
                record.id,
                new_source_ch.as_deref(),
                new_content_ch.as_deref(),
            )
            .await?;
            backfilled += 1;
        }
    }

    if backfilled > 0 {
        info!(
            "[{}] Backfilled content hashes for {} records",
            username, backfilled
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Post-backfill deduplication
// ---------------------------------------------------------------------------

/// After content-hash backfill, enforce the **content-hash uniqueness
/// invariant**: every non-NULL value in `source_content_hash` or
/// `content_hash` must be unique across *both* columns and all records.
/// Records sharing `source_hash` or `hash` are also grouped.
///
/// Records are grouped via union-find on all four hash columns.  For each
/// group the **winner** is chosen by:
///   1. Records whose processed file is under `sorted/` take priority.
///   2. Among equals, the most recently updated record wins.
///
/// Loser records have their source and processed files moved to
/// `duplicates/` and are deleted from the database.
async fn deduplicate_after_backfill(db: &Database, root: &Path, username: &str) -> Result<()> {
    use std::collections::HashMap;

    let snapshot = db.get_snapshot(Some(username)).await?;
    let n = snapshot.len();
    if n < 2 {
        return Ok(());
    }

    // --- Union-find helpers (path-compressed, inline) ----------------------
    let mut parent: Vec<usize> = (0..n).collect();

    fn uf_find(parent: &mut [usize], mut i: usize) -> usize {
        while parent[i] != i {
            parent[i] = parent[parent[i]]; // path halving
            i = parent[i];
        }
        i
    }
    fn uf_union(parent: &mut [usize], a: usize, b: usize) {
        let ra = uf_find(parent, a);
        let rb = uf_find(parent, b);
        if ra != rb {
            parent[rb] = ra;
        }
    }

    // --- Build equivalence classes from all hash columns -------------------
    let mut seen: HashMap<String, usize> = HashMap::new();

    for (i, record) in snapshot.iter().enumerate() {
        // source_hash and hash (exact-file hashes)
        if !record.source_hash.is_empty() {
            if let Some(&prev) = seen.get(&record.source_hash) {
                uf_union(&mut parent, prev, i);
            } else {
                seen.insert(record.source_hash.clone(), i);
            }
        }
        if let Some(ref h) = record.hash {
            if !h.is_empty() {
                if let Some(&prev) = seen.get(h.as_str()) {
                    uf_union(&mut parent, prev, i);
                } else {
                    seen.insert(h.clone(), i);
                }
            }
        }
        // content hashes (unique across both columns)
        for ch in [&record.source_content_hash, &record.content_hash] {
            if let Some(ref val) = ch {
                if val.is_empty() {
                    continue;
                }
                if let Some(&prev) = seen.get(val.as_str()) {
                    uf_union(&mut parent, prev, i);
                } else {
                    seen.insert(val.clone(), i);
                }
            }
        }
    }

    // --- Collect groups with > 1 member ------------------------------------
    let mut groups: HashMap<usize, Vec<usize>> = HashMap::new();
    for i in 0..n {
        let root_idx = uf_find(&mut parent, i);
        groups.entry(root_idx).or_default().push(i);
    }

    let mut deduplicated = 0u32;

    for (_root_idx, members) in &groups {
        if members.len() < 2 {
            continue;
        }

        // Pick the winner from the group
        let group_records: Vec<&Record> = members.iter().map(|&i| &snapshot[i]).collect();
        let local_winner = pick_dedup_winner(&group_records);
        let winner_idx = members[local_winner];
        let winner = &snapshot[winner_idx];
        info!(
            "[{}] Dedup group: {} records, winner: {} ({})",
            username,
            members.len(),
            winner.original_filename,
            winner.current_location().unwrap_or_else(|| "-".into()),
        );

        for &idx in members {
            if idx == winner_idx {
                continue;
            }
            let loser = &snapshot[idx];

            // Move source files to duplicates/
            for pe in &loser.source_paths {
                move_to_duplicates(root, &pe.path);
            }

            // Move processed files to duplicates/
            for pe in &loser.current_paths {
                move_to_duplicates(root, &pe.path);
            }

            info!(
                "[{}] Dedup: removing record {} ({})",
                username, loser.id, loser.original_filename,
            );
            db.delete_record(loser.id).await?;
            deduplicated += 1;
        }
    }

    if deduplicated > 0 {
        info!(
            "[{}] Deduplicated {} records by content hash",
            username, deduplicated
        );
    }
    Ok(())
}

/// Pick the winner from a group of duplicate records.
///
/// Priority:
///   1. Records with a processed file under `sorted/` beat others.
///   2. Among equals, the most recently updated record wins.
///
/// Returns the index *within `group`* of the winning record.
fn pick_dedup_winner(group: &[&Record]) -> usize {
    group
        .iter()
        .enumerate()
        .max_by(|(_, a), (_, b)| {
            let a_sorted = a.current_location().as_deref() == Some("sorted");
            let b_sorted = b.current_location().as_deref() == Some("sorted");
            a_sorted
                .cmp(&b_sorted)
                .then_with(|| a.updated_at.cmp(&b.updated_at))
        })
        .map(|(i, _)| i)
        .unwrap_or(0)
}

/// Move a single file into `duplicates/{date}/{location}/{location_path}/{filename}`.
fn move_to_duplicates(root: &Path, rel_path: &str) {
    let src = root.join(rel_path);
    let (location, location_path, filename) = Record::decompose_path(rel_path);
    let date_dir = crate::step4::today_date_dir();
    let dest = if location_path.is_empty() {
        root.join("duplicates").join(&date_dir).join(&location).join(&filename)
    } else {
        root.join("duplicates")
            .join(&date_dir)
            .join(&location)
            .join(&location_path)
            .join(&filename)
    };
    move_file(&src, &dest);
}

// ---------------------------------------------------------------------------
// Per-watcher event loop
// ---------------------------------------------------------------------------

/// Event-driven loop for a single watcher.
///
/// - Startup: always runs a full scan.
/// - Then waits for inotify events (incremental) or full_scan timer.
/// - Debounces events by waiting for quiet before running a cycle.
async fn run_watcher(
    mut watcher: DocumentWatcherV2,
    full_scan_seconds: f64,
    debounce_seconds: f64,
) {
    // Backfill content hashes for existing records before first cycle
    if let Err(e) = backfill_content_hashes(&watcher.db, &watcher.root, &watcher.name).await {
        error!(
            "[{}] Content hash backfill failed: {}",
            watcher.name, e
        );
    }

    // Deduplicate records that share the same content hash (post-migration)
    if let Err(e) = deduplicate_after_backfill(&watcher.db, &watcher.root, &watcher.name).await {
        error!(
            "[{}] Post-backfill deduplication failed: {}",
            watcher.name, e
        );
    }

    let result: Result<(), anyhow::Error> = async {
        let mut had_activity = watcher.run_cycle(true).await?;
        let mut last_full = tokio::time::Instant::now();

        loop {
            // Re-run immediately if the previous cycle had state transitions
            if had_activity {
                had_activity = watcher.run_cycle(false).await?;
                continue;
            }

            let elapsed = last_full.elapsed().as_secs_f64();
            let time_to_full = full_scan_seconds - elapsed;

            if time_to_full <= 0.0 {
                // Full scan due -- wait for quiet first
                watcher.wait_for_quiet(debounce_seconds).await;
                had_activity = watcher.run_cycle(true).await?;
                last_full = tokio::time::Instant::now();
            } else {
                // Wait for inotify event or full scan timer
                let got_event = watcher
                    .detector
                    .wait_for_event(time_to_full)
                    .await;
                if got_event {
                    watcher.wait_for_quiet(debounce_seconds).await;
                    had_activity = watcher.run_cycle(false).await?;

                    // Config change detected -> run full scan
                    if watcher.pending_full_scan() {
                        watcher.clear_pending_full_scan();
                        // Perform config reload
                        watcher.reload_config(
                            &|cm| load_smart_folders(cm),
                            &|root| load_root_smart_folders(root),
                        );
                        had_activity = watcher.run_cycle(true).await?;
                        last_full = tokio::time::Instant::now();
                    }
                } else {
                    // Timer expired, loop back to full scan branch
                    had_activity = false;
                }
            }
        }
    }
    .await;

    if let Err(e) = result {
        error!("[{}] Watcher task error: {}", watcher.name, e);
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    // Set permissive umask so directories/files created by the watcher are
    // accessible to other users sharing the same bind-mounted volume.
    #[cfg(unix)]
    unsafe {
        libc::umask(0);
    }

    // 1. Configuration from environment
    let mrdocument_url =
        std::env::var("MRDOCUMENT_URL").unwrap_or_else(|_| "http://mrdocument-service:8000".into());
    let database_url = std::env::var("DATABASE_URL")
        .context("DATABASE_URL environment variable is required")?;
    let stt_url = std::env::var("STT_URL").ok();
    let health_port: u16 = std::env::var("HEALTH_PORT")
        .unwrap_or_else(|_| "8080".into())
        .parse()
        .unwrap_or(8080);
    let watcher_config_path = PathBuf::from(
        std::env::var("WATCHER_CONFIG").unwrap_or_else(|_| "/app/watcher.yaml".into()),
    );
    let poll_interval: f64 = std::env::var("POLL_INTERVAL")
        .unwrap_or_else(|_| "5".into())
        .parse()
        .unwrap_or(5.0);
    let processor_timeout: f64 = std::env::var("PROCESSOR_TIMEOUT")
        .unwrap_or_else(|_| "900".into())
        .parse()
        .unwrap_or(900.0);
    let max_concurrent: usize = std::env::var("MAX_CONCURRENT_PROCESSING")
        .unwrap_or_else(|_| "5".into())
        .parse()
        .unwrap_or(5);

    // 2. Logging / tracing
    let log_level = std::env::var("LOG_LEVEL").unwrap_or_else(|_| "info".into());
    let filter_str = format!("{},lopdf=warn", log_level);
    let env_filter = tracing_subscriber::EnvFilter::try_new(&filter_str)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,lopdf=warn"));
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .init();

    info!("Watcher v2 starting (version {})", env!("CARGO_PKG_VERSION"));

    // 3. Database connection
    let db = Arc::new(Database::connect(&database_url).await?);

    // 4. Health server
    let ready = Arc::new(AtomicBool::new(false));
    start_health_server(health_port, ready.clone()).await?;

    // 5. Discover user roots
    let watcher_config = WatcherConfig::load(&watcher_config_path);
    let mut watch_dirs = watcher_config.get_watch_directories();

    while watch_dirs.is_empty() {
        info!("No watch folders found, waiting...");
        time::sleep(Duration::from_secs(60)).await;
        watch_dirs = watcher_config.get_watch_directories();
    }

    let dir_strs: Vec<String> = watch_dirs.iter().map(|d| d.display().to_string()).collect();
    info!(
        "Discovered {} watch directories: {:?}",
        watch_dirs.len(),
        dir_strs
    );

    // 6. Per-user orchestrator setup
    let mut watcher_handles = Vec::new();
    let mut known_dirs: HashSet<PathBuf> = HashSet::new();

    let debounce_seconds = watcher_config.debounce_seconds;
    let full_scan_seconds = watcher_config.full_scan_seconds;

    for user_root in &watch_dirs {
        let watcher = setup_user(
            user_root,
            Arc::clone(&db),
            &mrdocument_url,
            poll_interval,
            processor_timeout,
            stt_url.as_deref(),
            max_concurrent,
        );
        known_dirs.insert(user_root.clone());

        let handle = tokio::spawn(run_watcher(watcher, full_scan_seconds, debounce_seconds));
        watcher_handles.push(handle);
    }

    // 7. Directory discovery task
    let db_discovery = Arc::clone(&db);
    let mrdocument_url_discovery = mrdocument_url.clone();
    let stt_url_discovery = stt_url.clone();
    let watcher_config_discovery = watcher_config.clone();
    let known_dirs_arc = Arc::new(tokio::sync::Mutex::new(known_dirs));
    let watcher_handles_arc = Arc::new(tokio::sync::Mutex::new(watcher_handles));

    let discovery_handle = tokio::spawn({
        let known_dirs = known_dirs_arc.clone();
        let handles = watcher_handles_arc.clone();
        async move {
            loop {
                time::sleep(Duration::from_secs_f64(full_scan_seconds)).await;
                let current_dirs = watcher_config_discovery.get_watch_directories();
                let mut kd = known_dirs.lock().await;
                let mut hs = handles.lock().await;
                for new_dir in current_dirs {
                    if !kd.contains(&new_dir) {
                        info!("New user directory discovered: {}", new_dir.display());
                        let w = setup_user(
                            &new_dir,
                            Arc::clone(&db_discovery),
                            &mrdocument_url_discovery,
                            poll_interval,
                            processor_timeout,
                            stt_url_discovery.as_deref(),
                            max_concurrent,
                        );
                        kd.insert(new_dir);
                        let h = tokio::spawn(run_watcher(w, full_scan_seconds, debounce_seconds));
                        hs.push(h);
                    }
                }
            }
        }
    });

    ready.store(true, Ordering::Relaxed);
    info!(
        "Watcher v2 ready, debounce={:.1}s full_scan={:.1}s",
        debounce_seconds, full_scan_seconds,
    );

    // Wait for shutdown signal
    match signal::ctrl_c().await {
        Ok(()) => {
            info!("Shutting down...");
        }
        Err(e) => {
            error!("Failed to listen for shutdown signal: {}", e);
        }
    }

    // Graceful shutdown
    ready.store(false, Ordering::Relaxed);
    discovery_handle.abort();

    // Abort watcher tasks
    let handles = watcher_handles_arc.lock().await;
    for handle in handles.iter() {
        handle.abort();
    }

    info!("Watcher v2 stopped");
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{PathEntry, Record, State};
    use chrono::Utc;

    fn make_record_at(
        filename: &str,
        hash: &str,
        current_path: Option<&str>,
        updated: &str,
    ) -> Record {
        let mut r = Record::new(filename.into(), hash.into());
        r.state = State::IsComplete;
        r.updated_at = Some(updated.parse::<chrono::DateTime<Utc>>().unwrap());
        if let Some(cp) = current_path {
            r.current_paths.push(PathEntry {
                path: cp.into(),
                timestamp: Utc::now(),
            });
        }
        r
    }

    #[test]
    fn test_pick_dedup_winner_sorted_beats_processed() {
        let a = make_record_at("a.pdf", "h1", Some("processed/a.pdf"), "2025-01-01T00:00:00Z");
        let b = make_record_at("b.pdf", "h2", Some("sorted/arbeit/b.pdf"), "2025-01-01T00:00:00Z");
        let group: Vec<&Record> = vec![&a, &b];
        assert_eq!(pick_dedup_winner(&group), 1, "sorted/ should win");
    }

    #[test]
    fn test_pick_dedup_winner_most_recent_wins() {
        let a = make_record_at("a.pdf", "h1", Some("sorted/arbeit/a.pdf"), "2025-01-01T00:00:00Z");
        let b = make_record_at("b.pdf", "h2", Some("sorted/arbeit/b.pdf"), "2025-06-01T00:00:00Z");
        let group: Vec<&Record> = vec![&a, &b];
        assert_eq!(pick_dedup_winner(&group), 1, "newer updated_at should win");
    }

    #[test]
    fn test_pick_dedup_winner_sorted_beats_newer_processed() {
        let old_sorted =
            make_record_at("a.pdf", "h1", Some("sorted/arbeit/a.pdf"), "2025-01-01T00:00:00Z");
        let new_processed =
            make_record_at("b.pdf", "h2", Some("processed/b.pdf"), "2025-12-01T00:00:00Z");
        let group: Vec<&Record> = vec![&old_sorted, &new_processed];
        assert_eq!(
            pick_dedup_winner(&group),
            0,
            "sorted/ should beat processed/ even when older"
        );
    }

    #[test]
    fn test_pick_dedup_winner_three_records() {
        let processed =
            make_record_at("a.pdf", "h1", Some("processed/a.pdf"), "2025-06-01T00:00:00Z");
        let old_sorted =
            make_record_at("b.pdf", "h2", Some("sorted/arbeit/b.pdf"), "2025-01-01T00:00:00Z");
        let new_sorted =
            make_record_at("c.pdf", "h3", Some("sorted/privat/c.pdf"), "2025-09-01T00:00:00Z");
        let group: Vec<&Record> = vec![&processed, &old_sorted, &new_sorted];
        assert_eq!(
            pick_dedup_winner(&group),
            2,
            "newest sorted/ record should win"
        );
    }

    #[test]
    fn test_pick_dedup_winner_no_current_paths() {
        let a = make_record_at("a.pdf", "h1", None, "2025-01-01T00:00:00Z");
        let b = make_record_at("b.pdf", "h2", None, "2025-06-01T00:00:00Z");
        let group: Vec<&Record> = vec![&a, &b];
        assert_eq!(
            pick_dedup_winner(&group),
            1,
            "with no current paths, newer should win"
        );
    }
}
