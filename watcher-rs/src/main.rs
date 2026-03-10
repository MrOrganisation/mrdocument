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
use crate::step1::compute_content_hash;
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
    let env_filter = tracing_subscriber::EnvFilter::try_new(&log_level)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .init();

    info!("Watcher v2 starting");

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
