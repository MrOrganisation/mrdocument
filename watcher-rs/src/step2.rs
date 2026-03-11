//! Preprocessing and reconciliation for document watcher.
//!
//! Pure functions: no I/O, no DB, no filesystem operations.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use once_cell::sync::Lazy;
use regex::Regex;
use tracing::warn;
use uuid::Uuid;

use crate::models::{ChangeItem, EventType, PathEntry, Record, State};

// ---------------------------------------------------------------------------
// Location-aware matching table
// ---------------------------------------------------------------------------

/// Configuration for each top-level location.
///
/// Tuple: `(match_fields_in_priority_order, allows_new_records)`.
/// - `"source_hash"` -- compare `change.hash` vs `record.source_hash` -> `source_paths`
/// - `"hash"` -- compare `change.hash` vs `record.hash` -> `current_paths`
pub static LOCATION_CONFIG: Lazy<HashMap<&'static str, (&'static [&'static str], bool)>> =
    Lazy::new(|| {
        let mut m: HashMap<&str, (&[&str], bool)> = HashMap::new();
        m.insert("incoming", (&["source_hash", "hash"], true));
        m.insert("sorted", (&["hash", "source_hash"], true));
        m.insert("archive", (&["source_hash"], false));
        m.insert("missing", (&["source_hash"], false));
        m.insert("processed", (&["hash"], false));
        m.insert("reviewed", (&["hash"], false));
        m.insert("reset", (&["hash"], false));
        m.insert("trash", (&["source_hash", "hash"], false));
        m
    });

/// Valid locations for `current_paths` entries.
const VALID_CURRENT_LOCATIONS: &[&str] = &[".output", "processed", "reset", "reviewed", "sorted"];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn find_by_source_hash<'a>(records: &'a [Record], hash_value: &str) -> Option<&'a Record> {
    records.iter().find(|r| r.source_hash == hash_value)
}

#[allow(dead_code)]
fn find_by_source_hash_mut<'a>(
    records: &'a mut [Record],
    hash_value: &str,
) -> Option<&'a mut Record> {
    records.iter_mut().find(|r| r.source_hash == hash_value)
}

fn find_by_hash<'a>(records: &'a [Record], hash_value: &str) -> Option<&'a Record> {
    records
        .iter()
        .find(|r| r.hash.as_deref() == Some(hash_value))
}

#[allow(dead_code)]
fn find_by_hash_mut<'a>(records: &'a mut [Record], hash_value: &str) -> Option<&'a mut Record> {
    records
        .iter_mut()
        .find(|r| r.hash.as_deref() == Some(hash_value))
}

fn find_by_source_content_hash<'a>(records: &'a [Record], hash_value: &str) -> Option<&'a Record> {
    records
        .iter()
        .find(|r| r.source_content_hash.as_deref() == Some(hash_value))
}

fn find_by_content_hash<'a>(records: &'a [Record], hash_value: &str) -> Option<&'a Record> {
    records
        .iter()
        .find(|r| r.content_hash.as_deref() == Some(hash_value))
}

#[allow(dead_code)]
fn find_by_output_filename_mut<'a>(
    records: &'a mut [Record],
    filename: &str,
) -> Option<&'a mut Record> {
    records
        .iter_mut()
        .find(|r| r.output_filename.as_deref() == Some(filename))
}

/// Check whether a file with the given hashes would violate the uniqueness
/// invariant: every non-NULL `source_content_hash` / `content_hash` value
/// must be unique across both columns and all records.  Regular hashes
/// (`source_hash`, `hash`) are also checked for exact-file dedup.
fn is_duplicate_hash(
    records: &[Record],
    hash_value: &str,
    content_hash: Option<&str>,
    exclude_id: Uuid,
) -> bool {
    if hash_value.is_empty() {
        return false;
    }
    for r in records {
        if r.id == exclude_id {
            continue;
        }
        // Check regular hashes
        if r.source_hash == hash_value {
            return true;
        }
        if r.hash.as_deref() == Some(hash_value) {
            return true;
        }
        // Check content hashes (unique across both columns)
        if let Some(ch) = content_hash {
            if !ch.is_empty() {
                if r.source_content_hash.as_deref() == Some(ch) {
                    return true;
                }
                if r.content_hash.as_deref() == Some(ch) {
                    return true;
                }
            }
        }
    }
    false
}

/// Regex matching collision suffixes: `_<8 hex chars>` before the file extension.
static COLLISION_SUFFIX_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"_[0-9a-f]{8}(\.[^.]+)$").unwrap());

/// Check if `actual_path` is `expected_path` or a collision-suffixed variant.
pub(crate) fn is_collision_variant(actual_path: &str, expected_path: &str) -> bool {
    if actual_path == expected_path {
        return true;
    }
    let stripped = COLLISION_SUFFIX_RE.replace(actual_path, "$1");
    stripped == expected_path
}

/// Decompose a relative path into `(location, loc_path, filename)`.
fn decompose_path(path: &str) -> (String, String, String) {
    Record::decompose_path(path)
}

/// Add record index to modified list if not already there.
fn mark_modified(record_id: Uuid, modified_ids: &mut HashSet<Uuid>) {
    modified_ids.insert(record_id);
}

// ---------------------------------------------------------------------------
// compute_target_path
// ---------------------------------------------------------------------------

/// Compute expected target path in `sorted/` for a record.
///
/// Uses `context_folders` config to build subdirectory hierarchy from metadata.
/// Falls back to `sorted/{context}/{filename}` if folders not configured.
pub fn compute_target_path(
    record: &Record,
    context_folders: Option<&HashMap<String, Vec<String>>>,
) -> Option<String> {
    let context = record.context.as_ref()?;
    let assigned = record.assigned_filename.as_ref()?;

    if let Some(folders_map) = context_folders {
        if let Some(folders) = folders_map.get(context) {
            if let Some(ref metadata) = record.metadata {
                let mut parts: Vec<String> = Vec::new();
                for field in folders {
                    // Always use record.context for "context" field (authoritative)
                    if field == "context" {
                        parts.push(context.clone());
                        continue;
                    }
                    if let Some(value) = metadata.get(field) {
                        if let Some(s) = value.as_str() {
                            if !s.is_empty() {
                                parts.push(s.to_string());
                                continue;
                            }
                        } else if !value.is_null() {
                            parts.push(value.to_string());
                            continue;
                        }
                    }
                    break;
                }
                if !parts.is_empty() {
                    return Some(format!("sorted/{}/{}", parts.join("/"), assigned));
                }
            }
        }
    }

    Some(format!("sorted/{}/{}", context, assigned))
}

// ---------------------------------------------------------------------------
// preprocess
// ---------------------------------------------------------------------------

/// Process filesystem changes against existing records.
///
/// The `read_sidecar` parameter is a closure that reads sidecar JSON for
/// an `.output` path, returning a `serde_json::Value`.
///
/// Returns `(modified_records, new_records)`.
pub fn preprocess<F>(
    changes: &[ChangeItem],
    records: &mut Vec<Record>,
    new_records: &mut Vec<Record>,
    read_sidecar: F,
) -> (Vec<Uuid>, Vec<Record>)
where
    F: Fn(&str) -> serde_json::Value,
{
    let mut modified_ids: HashSet<Uuid> = HashSet::new();
    let mut created: Vec<Record> = Vec::new();
    let now = Utc::now();

    for change in changes {
        match change.event_type {
            EventType::Addition => {
                handle_addition(
                    change,
                    records,
                    new_records,
                    &mut created,
                    &read_sidecar,
                    &mut modified_ids,
                    now,
                );
            }
            EventType::Removal => {
                handle_removal(change, records, &mut modified_ids);
            }
        }
    }

    (modified_ids.into_iter().collect(), created)
}

fn handle_addition<F>(
    change: &ChangeItem,
    records: &mut Vec<Record>,
    existing_new: &mut Vec<Record>,
    created: &mut Vec<Record>,
    read_sidecar: &F,
    modified_ids: &mut HashSet<Uuid>,
    now: DateTime<Utc>,
) where
    F: Fn(&str) -> serde_json::Value,
{
    let (location, _, filename) = decompose_path(&change.path);

    // .output handling
    if location == ".output" {
        // Find index first, then check duplicate with immutable borrow
        let idx = records.iter().position(|r| r.output_filename.as_deref() == Some(&filename));
        if let Some(idx) = idx {
            let record_id = records[idx].id;
            if change.size == Some(0) {
                records[idx].state = State::HasError;
                records[idx].output_filename = None;
            } else {
                let sidecar = read_sidecar(&change.path);
                let change_hash = change.hash.as_deref().unwrap_or("");
                let change_content_hash = change.content_hash.as_deref();

                if is_duplicate_hash(records, change_hash, change_content_hash, record_id) {
                    warn!(
                        "Duplicate hash {} for output {}, discarding result",
                        change_hash, change.path
                    );
                    let record = &mut records[idx];
                    record.deleted_paths.push(change.path.clone());
                    // Move archive source to duplicates, keep non-archive sources
                    let mut remaining = Vec::new();
                    for pe in record.source_paths.drain(..) {
                        let (pe_loc, _, _) = decompose_path(&pe.path);
                        if pe_loc == "archive" {
                            record.duplicate_sources.push(pe.path.clone());
                        } else {
                            remaining.push(pe);
                        }
                    }
                    record.source_paths = remaining;
                    record.state = State::HasError;
                    record.output_filename = None;
                    mark_modified(record_id, modified_ids);
                    return;
                }

                // Preserve pre-set context (e.g., forced from sorted/ directory)
                let record = &mut records[idx];
                if record.context.is_none() {
                    record.context = sidecar
                        .get("context")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                }
                record.metadata = sidecar.get("metadata").cloned();
                record.assigned_filename = sidecar
                    .get("assigned_filename")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                record.hash = change.hash.clone();
                record.content_hash = change.content_hash.clone();
                record
                    .current_paths
                    .push(PathEntry { path: change.path.clone(), timestamp: now });
                record.output_filename = None;
            }
            mark_modified(record_id, modified_ids);
        }
        return;
    }

    // Non-.output locations
    let config = match LOCATION_CONFIG.get(location.as_str()) {
        Some(c) => *c,
        None => return,
    };

    let (match_fields, allows_new) = config;
    let change_hash = change.hash.as_deref().unwrap_or("");
    let change_content_hash = change.content_hash.as_deref();

    // Try to match against existing records
    let mut matched_id: Option<Uuid> = None;
    let mut matched_field: Option<&str> = None;

    for field in match_fields {
        if change_hash.is_empty() {
            continue;
        }
        match *field {
            "source_hash" => {
                if let Some(r) = find_by_source_hash(records, change_hash) {
                    matched_id = Some(r.id);
                    matched_field = Some(field);
                    break;
                }
                if let Some(r) = find_by_source_hash(existing_new, change_hash) {
                    matched_id = Some(r.id);
                    matched_field = Some(field);
                    break;
                }
                if let Some(r) = find_by_source_hash(created, change_hash) {
                    matched_id = Some(r.id);
                    matched_field = Some(field);
                    break;
                }
                // Also match by content hash
                if let Some(ch) = change_content_hash {
                    if !ch.is_empty() {
                        if let Some(r) = find_by_source_content_hash(records, ch) {
                            matched_id = Some(r.id);
                            matched_field = Some(field);
                            break;
                        }
                        if let Some(r) = find_by_source_content_hash(existing_new, ch) {
                            matched_id = Some(r.id);
                            matched_field = Some(field);
                            break;
                        }
                        if let Some(r) = find_by_source_content_hash(created, ch) {
                            matched_id = Some(r.id);
                            matched_field = Some(field);
                            break;
                        }
                    }
                }
            }
            "hash" => {
                if let Some(r) = find_by_hash(records, change_hash) {
                    matched_id = Some(r.id);
                    matched_field = Some(field);
                    break;
                }
                if let Some(r) = find_by_hash(existing_new, change_hash) {
                    matched_id = Some(r.id);
                    matched_field = Some(field);
                    break;
                }
                if let Some(r) = find_by_hash(created, change_hash) {
                    matched_id = Some(r.id);
                    matched_field = Some(field);
                    break;
                }
                // Also match by content hash
                if let Some(ch) = change_content_hash {
                    if !ch.is_empty() {
                        if let Some(r) = find_by_content_hash(records, ch) {
                            matched_id = Some(r.id);
                            matched_field = Some(field);
                            break;
                        }
                        if let Some(r) = find_by_content_hash(existing_new, ch) {
                            matched_id = Some(r.id);
                            matched_field = Some(field);
                            break;
                        }
                        if let Some(r) = find_by_content_hash(created, ch) {
                            matched_id = Some(r.id);
                            matched_field = Some(field);
                            break;
                        }
                    }
                }
            }
            _ => {}
        }
    }

    if let (Some(id), Some(field)) = (matched_id, matched_field) {
        // Find the record in one of the three lists and update it
        let record = records
            .iter_mut()
            .find(|r| r.id == id)
            .or_else(|| existing_new.iter_mut().find(|r| r.id == id))
            .or_else(|| created.iter_mut().find(|r| r.id == id));

        if let Some(record) = record {
            let path_list = if field == "hash" {
                &mut record.current_paths
            } else {
                &mut record.source_paths
            };

            let already_tracked = path_list.iter().any(|pe| pe.path == change.path);
            if !already_tracked {
                path_list.push(PathEntry { path: change.path.clone(), timestamp: now });
                mark_modified(record.id, modified_ids);
            }
        }
    } else if allows_new {
        let mut new_record = Record::new(filename.clone(), change_hash.to_string());
        new_record.source_content_hash = change.content_hash.clone();
        new_record.source_paths.push(PathEntry {
            path: change.path.clone(),
            timestamp: now,
        });

        // Pre-set context from directory for sorted/ files
        if location == "sorted" {
            let (_, loc_path, _) = decompose_path(&change.path);
            if !loc_path.is_empty() {
                new_record.context = loc_path.split('/').next().map(|s| s.to_string());
            }
        }

        created.push(new_record);
    }
}

fn handle_removal(change: &ChangeItem, records: &mut [Record], modified_ids: &mut HashSet<Uuid>) {
    for record in records.iter_mut() {
        // Check source_paths
        if let Some(idx) = record
            .source_paths
            .iter()
            .position(|pe| pe.path == change.path)
        {
            let pe = record.source_paths.remove(idx);
            record.missing_source_paths.push(pe);
            mark_modified(record.id, modified_ids);
            return;
        }

        // Check current_paths
        if let Some(idx) = record
            .current_paths
            .iter()
            .position(|pe| pe.path == change.path)
        {
            let pe = record.current_paths.remove(idx);
            record.missing_current_paths.push(pe);
            mark_modified(record.id, modified_ids);
            return;
        }
    }
}

// ---------------------------------------------------------------------------
// reconcile
// ---------------------------------------------------------------------------

/// Reconcile a record's state based on its paths and fields.
///
/// Returns the modified record, or `None` if the record should be deleted.
///
/// # Arguments
///
/// * `record` - The record to reconcile (modified in place).
/// * `context_field_names` - Maps context name to its `field_names` for
///   metadata completeness checks.
/// * `context_folders` - Maps context name to folder field list.
/// * `recompute_filename` - Callback that recomputes `assigned_filename` for a
///   record from its metadata and context config. Used by `reset/`.
pub fn reconcile<'a>(
    record: &'a mut Record,
    context_field_names: Option<&HashMap<String, Vec<String>>>,
    context_folders: Option<&HashMap<String, Vec<String>>>,
    recompute_filename: Option<&dyn Fn(&Record) -> Option<String>>,
) -> Option<&'a mut Record> {
    record.clear_temporary_fields();

    // ------------------------------------------------------------------
    // Phase 1: source_paths
    // ------------------------------------------------------------------
    if !record.source_paths.is_empty() {
        let sf_path = record.source_file().unwrap().path.clone();
        let (s_loc, _, sf_filename) = decompose_path(&sf_path);

        if s_loc == "trash" {
            record.state = State::NeedsDeletion;
            return Some(record);
        }

        if record.state == State::IsNew {
            record.output_filename = Some(Uuid::new_v4().to_string());
            record.state = State::NeedsProcessing;
            record.source_reference = Some(sf_path.clone());
            let now = Utc::now();
            record.source_paths.push(PathEntry {
                path: format!("archive/{}", sf_filename),
                timestamp: now,
            });
            return Some(record);
        }

        // IS_COMPLETE: new source is a duplicate -- never reprocess.
        if record.state == State::IsComplete && s_loc != "archive" && s_loc != "missing" {
            for pe in &record.source_paths {
                let (pe_loc, _, _) = decompose_path(&pe.path);
                if pe_loc != "archive" && pe_loc != "missing" {
                    record.duplicate_sources.push(pe.path.clone());
                }
            }
            record.source_paths.retain(|pe| {
                let (pe_loc, _, _) = decompose_path(&pe.path);
                pe_loc == "archive" || pe_loc == "missing"
            });
            return Some(record);
        }

        if s_loc != "archive" && s_loc != "missing" && record.state != State::HasError {
            record.source_reference = Some(sf_path.clone());
            let now = Utc::now();
            let mut new_paths: Vec<PathEntry> = Vec::new();
            for pe in &record.source_paths {
                let (pe_loc, _, _) = decompose_path(&pe.path);
                if pe_loc == "archive" || pe_loc == "missing" {
                    new_paths.push(pe.clone());
                } else if pe.path != sf_path {
                    record.duplicate_sources.push(pe.path.clone());
                }
            }
            new_paths.push(PathEntry {
                path: format!("archive/{}", sf_filename),
                timestamp: now,
            });
            record.source_paths = new_paths;
        }

        // Recovery: record with new source in processable location -> reprocess.
        let recoverable = record.state == State::IsMissing
            || (record.state == State::NeedsProcessing && record.output_filename.is_none());

        if recoverable && s_loc != "archive" && s_loc != "missing" && s_loc != "error" {
            record.output_filename = Some(Uuid::new_v4().to_string());
            record.state = State::NeedsProcessing;
            // Clear stale processing results
            record.context = None;
            record.metadata = None;
            record.assigned_filename = None;
            record.hash = None;
            record.missing_current_paths.clear();
            if !record.current_paths.is_empty() {
                for pe in &record.current_paths {
                    record.deleted_paths.push(pe.path.clone());
                }
                record.current_paths.clear();
            }
            return Some(record);
        }
    }

    // ------------------------------------------------------------------
    // Phase 2: has_error
    // ------------------------------------------------------------------
    if record.state == State::HasError {
        let s_loc = record.source_location();

        // Orphan record: source already in error/ and no current paths
        if s_loc.as_deref() == Some("error") && record.current_paths.is_empty() {
            return None;
        }

        // No source paths at all (file was deleted externally) -- clean up
        if s_loc.is_none() && record.current_paths.is_empty() {
            return None;
        }

        // Recovery: new source in a processable location -> retry
        if s_loc.as_deref() == Some("incoming") || s_loc.as_deref() == Some("sorted") {
            record.output_filename = Some(Uuid::new_v4().to_string());
            record.state = State::NeedsProcessing;
            let sf_path = record.source_file().unwrap().path.clone();
            let (_, _, sf_fn) = decompose_path(&sf_path);
            record.source_reference = Some(sf_path.clone());
            let now = Utc::now();
            // Clean up stale source_paths (old archive entries etc.)
            let mut new_paths: Vec<PathEntry> = record
                .source_paths
                .iter()
                .filter(|pe| pe.path == sf_path)
                .cloned()
                .collect();
            new_paths.push(PathEntry {
                path: format!("archive/{}", sf_fn),
                timestamp: now,
            });
            record.source_paths = new_paths;
            // Clear stale processing state
            record.context = None;
            record.metadata = None;
            record.assigned_filename = None;
            record.hash = None;
            if !record.current_paths.is_empty() {
                for pe in &record.current_paths {
                    record.deleted_paths.push(pe.path.clone());
                }
                record.current_paths.clear();
            }
            return Some(record);
        }

        if s_loc.as_deref() == Some("archive") {
            let sf_path = record.source_file().unwrap().path.clone();
            record.source_reference = Some(sf_path);
            record.duplicate_sources.clear();
        }

        if !record.current_paths.is_empty() {
            for pe in &record.current_paths {
                record.deleted_paths.push(pe.path.clone());
            }
            record.current_paths.clear();
        }

        return Some(record);
    }

    // ------------------------------------------------------------------
    // Phase 3: current_paths
    // ------------------------------------------------------------------

    // Trash detection via current_paths (processed file in trash/)
    if record
        .current_paths
        .iter()
        .any(|pe| decompose_path(&pe.path).0 == "trash")
    {
        record.state = State::NeedsDeletion;
        return Some(record);
    }

    // Early returns for special states
    if record.state == State::IsNew {
        record.output_filename = Some(Uuid::new_v4().to_string());
        record.state = State::NeedsProcessing;
        return Some(record);
    }

    if record.state == State::IsDeleted {
        return None;
    }

    if record.state == State::NeedsProcessing && record.current_paths.is_empty() {
        return Some(record);
    }

    // Missing detection
    if record.current_paths.is_empty() {
        if !record.missing_current_paths.is_empty() {
            record.state = State::IsMissing;
            // Move source from archive to missing/
            if !record.source_paths.is_empty() {
                let sf_path = record.source_file().unwrap().path.clone();
                let (sf_loc, _, _) = decompose_path(&sf_path);
                if sf_loc == "archive" {
                    record.source_reference = Some(sf_path);
                }
            }
        }
        return Some(record);
    }

    // Reappearance
    if record.state == State::IsMissing {
        record.state = State::IsComplete;
        // If source was moved to missing/, restore it to archive/
        if let Some(sf) = record.source_file() {
            let (sf_loc, _, _) = decompose_path(&sf.path);
            if sf_loc == "missing" {
                record.source_reference = Some(sf.path.clone());
            }
        }
    }

    // Invalid location cleanup
    let mut valid: Vec<PathEntry> = Vec::new();
    for pe in &record.current_paths {
        let (loc, _, _) = decompose_path(&pe.path);
        if VALID_CURRENT_LOCATIONS.contains(&loc.as_str()) {
            valid.push(pe.clone());
        } else {
            record.deleted_paths.push(pe.path.clone());
        }
    }
    record.current_paths = valid;

    if record.current_paths.is_empty() {
        return Some(record);
    }

    // Deduplicate: keep most recent
    if record.current_paths.len() > 1 {
        let mut sorted_paths = record.current_paths.clone();
        sorted_paths.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        for pe in &sorted_paths[..sorted_paths.len() - 1] {
            record.deleted_paths.push(pe.path.clone());
        }
        record.current_paths = vec![sorted_paths.last().unwrap().clone()];
    }

    // Single current path handling
    let current = record.current_paths[0].clone();
    let (c_loc, _, _) = decompose_path(&current.path);

    match c_loc.as_str() {
        ".output" => {
            if record.assigned_filename.is_some() {
                // If source was in sorted/, return to sorted/ instead of processed/
                let came_from_sorted = record
                    .source_paths
                    .iter()
                    .chain(record.missing_source_paths.iter())
                    .any(|pe| decompose_path(&pe.path).0 == "sorted");

                if came_from_sorted {
                    let target = compute_target_path(record, context_folders);
                    record.target_path = Some(target.unwrap_or_else(|| {
                        format!(
                            "processed/{}",
                            record.assigned_filename.as_deref().unwrap_or("")
                        )
                    }));
                } else {
                    record.target_path = Some(format!(
                        "processed/{}",
                        record.assigned_filename.as_deref().unwrap_or("")
                    ));
                }
                record.current_reference = Some(current.path.clone());
            }
        }

        "processed" => {
            record.state = State::IsComplete;
        }

        "reviewed" => {
            let target = compute_target_path(record, context_folders);
            if let Some(t) = target {
                record.target_path = Some(t);
                record.current_reference = Some(current.path.clone());
            }
        }

        "reset" => {
            if let Some(recompute) = recompute_filename {
                if let Some(new_name) = recompute(record) {
                    record.assigned_filename = Some(new_name);
                }
            }
            let target = compute_target_path(record, context_folders);
            if let Some(t) = target {
                record.target_path = Some(t);
                record.current_reference = Some(current.path.clone());
            }
            record.state = State::IsComplete;
        }

        "sorted" => {
            // Adopt user changes: if user renamed or moved the file, update record
            let current_filename = current.path.rsplit('/').next().unwrap_or("");
            if let Some(ref assigned) = record.assigned_filename {
                if current_filename != assigned {
                    let expected =
                        compute_target_path(record, context_folders).unwrap_or_default();
                    if !is_collision_variant(&current.path, &expected) {
                        record.assigned_filename = Some(current_filename.to_string());
                    }
                }
            }

            let (_, loc_path, _) = decompose_path(&current.path);
            if !loc_path.is_empty() {
                let dir_context = loc_path.split('/').next().unwrap_or("");
                if !dir_context.is_empty()
                    && record.context.as_deref() != Some(dir_context)
                {
                    record.context = Some(dir_context.to_string());
                }
            }

            // Metadata completeness check
            if let (Some(cfn), Some(ref ctx)) = (context_field_names, &record.context) {
                if let Some(field_names) = cfn.get(ctx.as_str()) {
                    let metadata = record.metadata.get_or_insert_with(|| {
                        serde_json::Value::Object(serde_json::Map::new())
                    });
                    if let Some(obj) = metadata.as_object_mut() {
                        for fn_name in field_names {
                            if !obj.contains_key(fn_name) {
                                obj.insert(fn_name.clone(), serde_json::Value::Null);
                            }
                        }
                    }
                }
            }

            record.state = State::IsComplete;
        }

        _ => {}
    }

    Some(record)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{ChangeItem, EventType, PathEntry, Record, State};
    use chrono::{DateTime, Duration, Utc};
    use serde_json::json;
    use std::collections::HashMap;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn _ts(offset_hours: i64) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2025-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
            + Duration::hours(offset_hours)
    }

    /// Create a Record with sensible defaults.  Caller can override fields
    /// after construction.
    fn _make_record() -> Record {
        Record::new("test.pdf".to_string(), "abc123".to_string())
    }

    fn _make_change(
        event_type: EventType,
        path: &str,
        hash: Option<&str>,
        size: Option<u64>,
    ) -> ChangeItem {
        ChangeItem {
            event_type,
            path: path.to_string(),
            hash: hash.map(|s| s.to_string()),
            content_hash: None,
            size,
        }
    }

    fn _noop_sidecar(_path: &str) -> serde_json::Value {
        json!({})
    }

    // -----------------------------------------------------------------------
    // TestPreprocessOutput (3 tests)
    // -----------------------------------------------------------------------

    #[test]
    fn test_output_matches_sidecar_ingested() {
        let mut records = vec![{
            let mut r = _make_record();
            r.output_filename = Some("uuid-123".to_string());
            r.state = State::NeedsProcessing;
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            ".output/uuid-123",
            Some("content_hash"),
            Some(1024),
        );
        let sidecar_data = json!({
            "context": "work",
            "metadata": {"date": "2025-01-01", "title": "Invoice"},
            "assigned_filename": "2025-01-Invoice.pdf",
        });

        let (modified_ids, created) =
            preprocess(&[change], &mut records, &mut new_records, |_p| {
                sidecar_data.clone()
            });

        assert_eq!(modified_ids.len(), 1);
        assert!(modified_ids.contains(&records[0].id));
        assert_eq!(records[0].context.as_deref(), Some("work"));
        assert_eq!(
            records[0].metadata,
            Some(json!({"date": "2025-01-01", "title": "Invoice"}))
        );
        assert_eq!(
            records[0].assigned_filename.as_deref(),
            Some("2025-01-Invoice.pdf")
        );
        assert_eq!(records[0].hash.as_deref(), Some("content_hash"));
        assert!(records[0].output_filename.is_none());
        assert_eq!(records[0].current_paths.len(), 1);
        assert_eq!(records[0].current_paths[0].path, ".output/uuid-123");
        assert!(created.is_empty());
    }

    #[test]
    fn test_output_zero_byte_sets_error() {
        let mut records = vec![{
            let mut r = _make_record();
            r.output_filename = Some("uuid-456".to_string());
            r.state = State::NeedsProcessing;
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            ".output/uuid-456",
            Some("empty_hash"),
            Some(0),
        );

        let (modified_ids, created) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar);

        assert_eq!(modified_ids.len(), 1);
        assert_eq!(records[0].state, State::HasError);
        assert!(records[0].output_filename.is_none());
        assert!(created.is_empty());
    }

    #[test]
    fn test_output_no_match_ignored() {
        let mut records = vec![{
            let mut r = _make_record();
            r.output_filename = Some("other-uuid".to_string());
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            ".output/unknown-uuid",
            Some("some_hash"),
            Some(100),
        );

        let (modified_ids, created) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar);

        assert!(modified_ids.is_empty());
        assert!(created.is_empty());
    }

    // -----------------------------------------------------------------------
    // TestPreprocessIncoming (3 tests)
    // -----------------------------------------------------------------------

    #[test]
    fn test_incoming_matches_source_hash() {
        let mut records = vec![{
            let mut r = _make_record();
            r.source_hash = "file_hash_abc".to_string();
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "incoming/doc.pdf",
            Some("file_hash_abc"),
            Some(500),
        );

        let (modified_ids, created) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar);

        assert_eq!(modified_ids.len(), 1);
        assert!(modified_ids.contains(&records[0].id));
        assert!(records[0]
            .source_paths
            .iter()
            .any(|pe| pe.path == "incoming/doc.pdf"));
        assert!(created.is_empty());
    }

    #[test]
    fn test_incoming_unknown_creates_new() {
        let mut records = vec![{
            let mut r = _make_record();
            r.source_hash = "existing_hash".to_string();
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "incoming/new.pdf",
            Some("new_hash"),
            Some(1024),
        );

        let (modified_ids, created) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar);

        assert!(modified_ids.is_empty());
        assert_eq!(created.len(), 1);
        assert_eq!(created[0].original_filename, "new.pdf");
        assert_eq!(created[0].source_hash, "new_hash");
        assert_eq!(created[0].source_paths[0].path, "incoming/new.pdf");
    }

    #[test]
    fn test_two_identical_sources_same_cycle() {
        let mut records: Vec<Record> = Vec::new();
        let mut new_records: Vec<Record> = Vec::new();
        let change_a = _make_change(
            EventType::Addition,
            "incoming/a.pdf",
            Some("same_hash"),
            Some(500),
        );
        let change_b = _make_change(
            EventType::Addition,
            "incoming/b.pdf",
            Some("same_hash"),
            Some(500),
        );

        let (_modified_ids, created) = preprocess(
            &[change_a, change_b],
            &mut records,
            &mut new_records,
            _noop_sidecar,
        );

        assert_eq!(
            created.len(),
            1,
            "Expected 1 record for identical sources, got {}",
            created.len()
        );
        let paths: Vec<&str> = created[0].source_paths.iter().map(|pe| pe.path.as_str()).collect();
        assert!(paths.contains(&"incoming/a.pdf"));
        assert!(paths.contains(&"incoming/b.pdf"));
    }

    // -----------------------------------------------------------------------
    // TestPreprocessSorted (3 tests)
    // -----------------------------------------------------------------------

    #[test]
    fn test_sorted_matches_hash() {
        let mut records = vec![{
            let mut r = _make_record();
            r.hash = Some("sorted_hash".to_string());
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "sorted/work/doc.pdf",
            Some("sorted_hash"),
            Some(800),
        );

        let (modified_ids, _created) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar);

        assert_eq!(modified_ids.len(), 1);
        assert!(records[0]
            .current_paths
            .iter()
            .any(|pe| pe.path == "sorted/work/doc.pdf"));
    }

    #[test]
    fn test_sorted_matches_source_hash_not_hash() {
        let mut records = vec![{
            let mut r = _make_record();
            r.source_hash = "src_hash".to_string();
            r.hash = Some("different_hash".to_string());
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "sorted/work/original.pdf",
            Some("src_hash"),
            Some(600),
        );

        let (modified_ids, _created) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar);

        assert_eq!(modified_ids.len(), 1);
        assert!(records[0]
            .source_paths
            .iter()
            .any(|pe| pe.path == "sorted/work/original.pdf"));
        assert!(!records[0]
            .current_paths
            .iter()
            .any(|pe| pe.path == "sorted/work/original.pdf"));
    }

    #[test]
    fn test_sorted_unknown_creates_new() {
        let mut records: Vec<Record> = Vec::new();
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "sorted/personal/unknown.pdf",
            Some("unknown_hash"),
            Some(300),
        );

        let (_modified_ids, created) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar);

        assert_eq!(created.len(), 1);
        assert_eq!(created[0].original_filename, "unknown.pdf");
        assert_eq!(created[0].context.as_deref(), Some("personal"));
    }

    // -----------------------------------------------------------------------
    // TestPreprocessOtherLocations (7 tests)
    // -----------------------------------------------------------------------

    #[test]
    fn test_archive_matches_source_hash() {
        let mut records = vec![{
            let mut r = _make_record();
            r.source_hash = "arch_hash".to_string();
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "archive/doc.pdf",
            Some("arch_hash"),
            Some(500),
        );

        let (modified_ids, _created) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar);

        assert_eq!(modified_ids.len(), 1);
        assert!(records[0]
            .source_paths
            .iter()
            .any(|pe| pe.path == "archive/doc.pdf"));
    }

    #[test]
    fn test_missing_matches_source_hash() {
        let mut records = vec![{
            let mut r = _make_record();
            r.source_hash = "miss_hash".to_string();
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "missing/doc.pdf",
            Some("miss_hash"),
            Some(500),
        );

        let (modified_ids, _created) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar);

        assert_eq!(modified_ids.len(), 1);
        assert!(records[0]
            .source_paths
            .iter()
            .any(|pe| pe.path == "missing/doc.pdf"));
    }

    #[test]
    fn test_missing_both_match_uses_source_hash() {
        let mut records = vec![{
            let mut r = _make_record();
            r.source_hash = "both_hash".to_string();
            r.hash = Some("both_hash".to_string());
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "missing/doc.pdf",
            Some("both_hash"),
            Some(500),
        );

        let (modified_ids, _created) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar);

        assert_eq!(modified_ids.len(), 1);
        assert!(records[0]
            .source_paths
            .iter()
            .any(|pe| pe.path == "missing/doc.pdf"));
        assert!(!records[0]
            .current_paths
            .iter()
            .any(|pe| pe.path == "missing/doc.pdf"));
    }

    #[test]
    fn test_missing_unknown_not_created() {
        let mut records: Vec<Record> = Vec::new();
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "missing/stray.pdf",
            Some("stray_hash"),
            Some(500),
        );

        let (modified_ids, created) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar);

        assert!(modified_ids.is_empty());
        assert!(created.is_empty());
    }

    #[test]
    fn test_archive_unknown_not_created() {
        let mut records: Vec<Record> = Vec::new();
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "archive/stray.pdf",
            Some("stray_hash"),
            Some(500),
        );

        let (modified_ids, created) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar);

        assert!(modified_ids.is_empty());
        assert!(created.is_empty());
    }

    #[test]
    fn test_processed_matches_hash() {
        let mut records = vec![{
            let mut r = _make_record();
            r.hash = Some("proc_hash".to_string());
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "processed/doc.pdf",
            Some("proc_hash"),
            Some(700),
        );

        let (modified_ids, _created) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar);

        assert_eq!(modified_ids.len(), 1);
        assert!(records[0]
            .current_paths
            .iter()
            .any(|pe| pe.path == "processed/doc.pdf"));
    }

    #[test]
    fn test_trash_matches_source_hash() {
        let mut records = vec![{
            let mut r = _make_record();
            r.source_hash = "trash_hash".to_string();
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "trash/doc.pdf",
            Some("trash_hash"),
            Some(500),
        );

        let (modified_ids, _created) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar);

        assert_eq!(modified_ids.len(), 1);
        assert!(records[0]
            .source_paths
            .iter()
            .any(|pe| pe.path == "trash/doc.pdf"));
    }

    // -----------------------------------------------------------------------
    // TestPreprocessRemovals (2 tests)
    // -----------------------------------------------------------------------

    #[test]
    fn test_removal_from_source_paths() {
        let mut records = vec![{
            let mut r = _make_record();
            r.source_paths.push(PathEntry {
                path: "incoming/doc.pdf".to_string(),
                timestamp: _ts(0),
            });
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(EventType::Removal, "incoming/doc.pdf", None, None);

        let (modified_ids, _created) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar);

        assert_eq!(modified_ids.len(), 1);
        assert!(!records[0]
            .source_paths
            .iter()
            .any(|pe| pe.path == "incoming/doc.pdf"));
        assert!(records[0]
            .missing_source_paths
            .iter()
            .any(|pe| pe.path == "incoming/doc.pdf"));
    }

    #[test]
    fn test_removal_from_current_paths() {
        let mut records = vec![{
            let mut r = _make_record();
            r.current_paths.push(PathEntry {
                path: "sorted/work/doc.pdf".to_string(),
                timestamp: _ts(0),
            });
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(EventType::Removal, "sorted/work/doc.pdf", None, None);

        let (modified_ids, _created) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar);

        assert_eq!(modified_ids.len(), 1);
        assert!(!records[0]
            .current_paths
            .iter()
            .any(|pe| pe.path == "sorted/work/doc.pdf"));
        assert!(records[0]
            .missing_current_paths
            .iter()
            .any(|pe| pe.path == "sorted/work/doc.pdf"));
    }

    // -----------------------------------------------------------------------
    // TestPreprocessLocationAware (2 tests)
    // -----------------------------------------------------------------------

    #[test]
    fn test_archive_both_match_uses_source_hash() {
        let mut records = vec![{
            let mut r = _make_record();
            r.source_hash = "both_hash".to_string();
            r.hash = Some("both_hash".to_string());
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "archive/doc.pdf",
            Some("both_hash"),
            Some(500),
        );

        let (modified_ids, _created) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar);

        assert_eq!(modified_ids.len(), 1);
        assert!(records[0]
            .source_paths
            .iter()
            .any(|pe| pe.path == "archive/doc.pdf"));
        assert!(!records[0]
            .current_paths
            .iter()
            .any(|pe| pe.path == "archive/doc.pdf"));
    }

    #[test]
    fn test_sorted_both_match_prefers_hash() {
        let mut records = vec![{
            let mut r = _make_record();
            r.source_hash = "both_hash".to_string();
            r.hash = Some("both_hash".to_string());
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "sorted/work/doc.pdf",
            Some("both_hash"),
            Some(500),
        );

        let (modified_ids, _created) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar);

        assert_eq!(modified_ids.len(), 1);
        assert!(records[0]
            .current_paths
            .iter()
            .any(|pe| pe.path == "sorted/work/doc.pdf"));
        assert!(!records[0]
            .source_paths
            .iter()
            .any(|pe| pe.path == "sorted/work/doc.pdf"));
    }

    // -----------------------------------------------------------------------
    // TestPreprocessIdempotency (3 tests)
    // -----------------------------------------------------------------------

    #[test]
    fn test_duplicate_source_path_not_appended() {
        let mut records = vec![{
            let mut r = _make_record();
            r.source_hash = "abc123".to_string();
            r.state = State::IsComplete;
            r.source_paths.push(PathEntry {
                path: "archive/doc.pdf".to_string(),
                timestamp: _ts(0),
            });
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "archive/doc.pdf",
            Some("abc123"),
            Some(100),
        );

        let (modified_ids, _created) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar);

        assert_eq!(
            modified_ids.len(),
            0,
            "Record should not be marked modified"
        );
        assert_eq!(
            records[0].source_paths.len(),
            1,
            "No duplicate path should be added"
        );
    }

    #[test]
    fn test_duplicate_current_path_not_appended() {
        let mut records = vec![{
            let mut r = _make_record();
            r.hash = Some("def456".to_string());
            r.state = State::IsComplete;
            r.current_paths.push(PathEntry {
                path: "sorted/work/doc.pdf".to_string(),
                timestamp: _ts(0),
            });
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "sorted/work/doc.pdf",
            Some("def456"),
            Some(100),
        );

        let (modified_ids, _created) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar);

        assert_eq!(
            modified_ids.len(),
            0,
            "Record should not be marked modified"
        );
        assert_eq!(
            records[0].current_paths.len(),
            1,
            "No duplicate path should be added"
        );
    }

    #[test]
    fn test_new_path_still_appended() {
        let mut records = vec![{
            let mut r = _make_record();
            r.source_hash = "abc123".to_string();
            r.state = State::IsComplete;
            r.source_paths.push(PathEntry {
                path: "archive/old.pdf".to_string(),
                timestamp: _ts(0),
            });
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "incoming/new-copy.pdf",
            Some("abc123"),
            Some(100),
        );

        let (modified_ids, _created) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar);

        assert_eq!(modified_ids.len(), 1);
        assert!(records[0]
            .source_paths
            .iter()
            .any(|pe| pe.path == "incoming/new-copy.pdf"));
    }

    // -----------------------------------------------------------------------
    // TestReconcileSourcePaths (9 tests)
    // -----------------------------------------------------------------------

    #[test]
    fn test_source_in_trash() {
        let mut record = _make_record();
        record.source_paths.push(PathEntry {
            path: "trash/doc.pdf".to_string(),
            timestamp: _ts(0),
        });

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.state, State::NeedsDeletion);
    }

    #[test]
    fn test_is_new_sets_processing() {
        let mut record = _make_record();
        record.state = State::IsNew;
        record.source_paths.push(PathEntry {
            path: "incoming/invoice.pdf".to_string(),
            timestamp: _ts(0),
        });

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.state, State::NeedsProcessing);
        assert!(r.output_filename.is_some());
        assert_eq!(r.source_reference.as_deref(), Some("incoming/invoice.pdf"));
        let archive_entries: Vec<_> = r
            .source_paths
            .iter()
            .filter(|pe| pe.path.starts_with("archive/"))
            .collect();
        assert_eq!(archive_entries.len(), 1);
        assert_eq!(archive_entries[0].path, "archive/invoice.pdf");
    }

    #[test]
    fn test_source_not_archive_not_lost() {
        let mut record = _make_record();
        record.state = State::NeedsProcessing;
        record.source_paths.push(PathEntry {
            path: "incoming/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.source_paths.push(PathEntry {
            path: "incoming/copy.pdf".to_string(),
            timestamp: _ts(1),
        });

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.source_reference.as_deref(), Some("incoming/copy.pdf"));
        assert!(r.duplicate_sources.contains(&"incoming/doc.pdf".to_string()));
        let archive_entries: Vec<_> = r
            .source_paths
            .iter()
            .filter(|pe| pe.path.starts_with("archive/"))
            .collect();
        assert!(!archive_entries.is_empty());
    }

    #[test]
    fn test_is_complete_new_source_is_duplicate() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.source_paths.push(PathEntry {
            path: "archive/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.source_paths.push(PathEntry {
            path: "incoming/doc.pdf".to_string(),
            timestamp: _ts(1),
        });
        record.current_paths.push(PathEntry {
            path: "sorted/work/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("doc.pdf".to_string());
        record.hash = Some("abc".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.state, State::IsComplete);
        assert!(r.duplicate_sources.contains(&"incoming/doc.pdf".to_string()));
        assert!(r.source_paths.iter().any(|pe| pe.path == "archive/doc.pdf"));
        assert!(!r
            .source_paths
            .iter()
            .any(|pe| pe.path.starts_with("incoming/")));
        assert!(r
            .current_paths
            .iter()
            .any(|pe| pe.path == "sorted/work/doc.pdf"));
        assert!(r.output_filename.is_none());
    }

    #[test]
    fn test_is_complete_new_source_in_sorted_is_duplicate() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.source_paths.push(PathEntry {
            path: "archive/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.source_paths.push(PathEntry {
            path: "sorted/work/doc.pdf".to_string(),
            timestamp: _ts(1),
        });
        record.current_paths.push(PathEntry {
            path: "processed/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("doc.pdf".to_string());
        record.hash = Some("abc".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.state, State::IsComplete);
        assert!(r
            .duplicate_sources
            .contains(&"sorted/work/doc.pdf".to_string()));
        assert!(!r.source_paths.iter().any(|pe| {
            let (loc, _, _) = Record::decompose_path(&pe.path);
            loc == "sorted"
        }));
    }

    #[test]
    fn test_is_complete_source_in_archive_no_duplicate() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.source_paths.push(PathEntry {
            path: "archive/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.current_paths.push(PathEntry {
            path: "sorted/work/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("doc.pdf".to_string());
        record.hash = Some("abc".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.state, State::IsComplete);
        assert!(r.duplicate_sources.is_empty());
    }

    #[test]
    fn test_is_missing_recovers_with_new_source() {
        let mut record = _make_record();
        record.state = State::IsMissing;
        record.source_paths.push(PathEntry {
            path: "missing/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.source_paths.push(PathEntry {
            path: "incoming/doc.pdf".to_string(),
            timestamp: _ts(1),
        });
        record.missing_current_paths.push(PathEntry {
            path: "sorted/work/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("doc.pdf".to_string());
        record.hash = Some("abc".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.state, State::NeedsProcessing);
        assert!(r.output_filename.is_some());
    }

    #[test]
    fn test_is_missing_sets_source_reference() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.source_paths.push(PathEntry {
            path: "archive/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.missing_current_paths.push(PathEntry {
            path: "sorted/work/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("doc.pdf".to_string());
        record.hash = Some("abc".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.state, State::IsMissing);
        assert_eq!(r.source_reference.as_deref(), Some("archive/doc.pdf"));
    }

    #[test]
    fn test_is_missing_no_source_reference_without_archive() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.source_paths.push(PathEntry {
            path: "missing/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.missing_current_paths.push(PathEntry {
            path: "sorted/work/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("doc.pdf".to_string());
        record.hash = Some("abc".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.state, State::IsMissing);
        assert!(r.source_reference.is_none());
    }

    // -----------------------------------------------------------------------
    // TestReconcileHasError (3 tests)
    // -----------------------------------------------------------------------

    #[test]
    fn test_error_source_in_error_no_current_deletes() {
        let mut record = _make_record();
        record.state = State::HasError;
        record.source_paths.push(PathEntry {
            path: "error/doc.pdf".to_string(),
            timestamp: _ts(0),
        });

        let result = reconcile(&mut record, None, None, None);

        assert!(result.is_none());
    }

    #[test]
    fn test_error_source_in_archive() {
        let mut record = _make_record();
        record.state = State::HasError;
        record.source_paths.push(PathEntry {
            path: "archive/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.duplicate_sources.push("incoming/extra.pdf".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.source_reference.as_deref(), Some("archive/doc.pdf"));
        assert!(r.duplicate_sources.is_empty());
    }

    #[test]
    fn test_error_with_current_paths() {
        let mut record = _make_record();
        record.state = State::HasError;
        record.source_paths.push(PathEntry {
            path: "error/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.current_paths.push(PathEntry {
            path: ".output/uuid".to_string(),
            timestamp: _ts(0),
        });

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert!(r.current_paths.is_empty());
        assert!(r.deleted_paths.contains(&".output/uuid".to_string()));
    }

    // -----------------------------------------------------------------------
    // TestReconcileCurrentPaths (15 tests)
    // -----------------------------------------------------------------------

    #[test]
    fn test_current_in_trash_needs_deletion() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.source_paths.push(PathEntry {
            path: "archive/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.current_paths.push(PathEntry {
            path: "trash/doc.pdf".to_string(),
            timestamp: _ts(1),
        });
        record.hash = Some("some_hash".to_string());
        record.context = Some("work".to_string());
        record.assigned_filename = Some("doc.pdf".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.state, State::NeedsDeletion);
    }

    #[test]
    fn test_is_new_needs_processing() {
        // No source_paths → source section skipped → reach current section
        let mut record = _make_record();
        record.state = State::IsNew;

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.state, State::NeedsProcessing);
        assert!(r.output_filename.is_some());
    }

    #[test]
    fn test_is_deleted_removed() {
        let mut record = _make_record();
        record.state = State::IsDeleted;
        record.missing_current_paths.push(PathEntry {
            path: "sorted/work/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.missing_source_paths.push(PathEntry {
            path: "archive/doc.pdf".to_string(),
            timestamp: _ts(0),
        });

        let result = reconcile(&mut record, None, None, None);

        assert!(result.is_none());
    }

    #[test]
    fn test_needs_processing_no_current() {
        let mut record = _make_record();
        record.state = State::NeedsProcessing;
        record.output_filename = Some("some-uuid".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.state, State::NeedsProcessing);
    }

    #[test]
    fn test_no_current_missing_current_is_missing() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.missing_current_paths.push(PathEntry {
            path: "sorted/work/doc.pdf".to_string(),
            timestamp: _ts(0),
        });

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.state, State::IsMissing);
    }

    #[test]
    fn test_is_missing_current_reappeared() {
        let mut record = _make_record();
        record.state = State::IsMissing;
        record.current_paths.push(PathEntry {
            path: "sorted/work/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("doc.pdf".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.state, State::IsComplete);
    }

    #[test]
    fn test_is_missing_still_empty() {
        let mut record = _make_record();
        record.state = State::IsMissing;
        record.missing_current_paths.push(PathEntry {
            path: "sorted/work/doc.pdf".to_string(),
            timestamp: _ts(0),
        });

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.state, State::IsMissing);
    }

    #[test]
    fn test_invalid_location_deleted() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.current_paths.push(PathEntry {
            path: "incoming/doc.pdf".to_string(),
            timestamp: _ts(0),
        });

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert!(r.current_paths.is_empty());
        assert!(r.deleted_paths.contains(&"incoming/doc.pdf".to_string()));
    }

    #[test]
    fn test_multiple_keep_most_recent() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.current_paths.push(PathEntry {
            path: "sorted/work/old.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.current_paths.push(PathEntry {
            path: "sorted/work/new.pdf".to_string(),
            timestamp: _ts(2),
        });
        record.current_paths.push(PathEntry {
            path: "sorted/work/mid.pdf".to_string(),
            timestamp: _ts(1),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("new.pdf".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.current_paths.len(), 1);
        assert_eq!(r.current_paths[0].path, "sorted/work/new.pdf");
        assert!(r.deleted_paths.contains(&"sorted/work/old.pdf".to_string()));
        assert!(r.deleted_paths.contains(&"sorted/work/mid.pdf".to_string()));
    }

    #[test]
    fn test_single_output_target_processed() {
        let mut record = _make_record();
        record.state = State::NeedsProcessing;
        record.source_paths.push(PathEntry {
            path: "archive/test.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.current_paths.push(PathEntry {
            path: ".output/uuid-out".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("2025-01-Invoice.pdf".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(
            r.target_path.as_deref(),
            Some("processed/2025-01-Invoice.pdf")
        );
        assert_eq!(r.current_reference.as_deref(), Some(".output/uuid-out"));
    }

    #[test]
    fn test_single_output_from_sorted_target_sorted() {
        let mut record = _make_record();
        record.state = State::NeedsProcessing;
        record.source_paths.push(PathEntry {
            path: "archive/test.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.missing_source_paths.push(PathEntry {
            path: "sorted/work/test.pdf".to_string(),
            timestamp: _ts(-1),
        });
        record.current_paths.push(PathEntry {
            path: ".output/uuid-out".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("2025-01-Invoice.pdf".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(
            r.target_path.as_deref(),
            Some("sorted/work/2025-01-Invoice.pdf")
        );
        assert_eq!(r.current_reference.as_deref(), Some(".output/uuid-out"));
    }

    #[test]
    fn test_single_processed_is_complete() {
        let mut record = _make_record();
        record.state = State::NeedsProcessing;
        record.current_paths.push(PathEntry {
            path: "processed/doc.pdf".to_string(),
            timestamp: _ts(0),
        });

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.state, State::IsComplete);
    }

    #[test]
    fn test_single_reviewed_target_sorted() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.current_paths.push(PathEntry {
            path: "reviewed/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("2025-01-Doc.pdf".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(
            r.target_path.as_deref(),
            Some("sorted/work/2025-01-Doc.pdf")
        );
        assert_eq!(r.current_reference.as_deref(), Some("reviewed/doc.pdf"));
    }

    #[test]
    fn test_single_sorted_matches_complete() {
        let mut record = _make_record();
        record.state = State::NeedsProcessing;
        record.current_paths.push(PathEntry {
            path: "sorted/work/2025-01-Invoice.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("2025-01-Invoice.pdf".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.state, State::IsComplete);
    }

    #[test]
    fn test_single_sorted_doesnt_match() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.current_paths.push(PathEntry {
            path: "sorted/work/wrong_name.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("2025-01-Invoice.pdf".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.assigned_filename.as_deref(), Some("wrong_name.pdf"));
        assert_eq!(r.state, State::IsComplete);
        assert!(r.target_path.is_none());
    }

    // -----------------------------------------------------------------------
    // TestComputeTargetPath (5 tests)
    // -----------------------------------------------------------------------

    #[test]
    fn test_compute_target_path_with_context_and_filename() {
        let mut record = _make_record();
        record.context = Some("work".to_string());
        record.assigned_filename = Some("2025-01-Invoice.pdf".to_string());

        let result = compute_target_path(&record, None);

        assert_eq!(result.as_deref(), Some("sorted/work/2025-01-Invoice.pdf"));
    }

    #[test]
    fn test_compute_target_path_no_context_returns_none() {
        let mut record = _make_record();
        record.context = None;
        record.assigned_filename = Some("doc.pdf".to_string());

        let result = compute_target_path(&record, None);

        assert!(result.is_none());
    }

    #[test]
    fn test_compute_target_path_no_filename_returns_none() {
        let mut record = _make_record();
        record.context = Some("work".to_string());
        record.assigned_filename = None;

        let result = compute_target_path(&record, None);

        assert!(result.is_none());
    }

    #[test]
    fn test_compute_target_path_with_context_folders() {
        let mut record = _make_record();
        record.context = Some("arbeit".to_string());
        record.assigned_filename = Some("arbeit-rechnung-2025.pdf".to_string());
        record.metadata = Some(json!({
            "context": "arbeit",
            "sender": "Schulze GmbH",
            "type": "Rechnung",
        }));
        let folders: HashMap<String, Vec<String>> =
            [("arbeit".to_string(), vec!["context".to_string(), "sender".to_string()])]
                .into_iter()
                .collect();

        let result = compute_target_path(&record, Some(&folders));

        assert_eq!(
            result.as_deref(),
            Some("sorted/arbeit/Schulze GmbH/arbeit-rechnung-2025.pdf")
        );
    }

    #[test]
    fn test_compute_target_path_with_context_folders_missing_field() {
        let mut record = _make_record();
        record.context = Some("arbeit".to_string());
        record.assigned_filename = Some("arbeit-rechnung-2025.pdf".to_string());
        record.metadata = Some(json!({"context": "arbeit"}));
        let folders: HashMap<String, Vec<String>> =
            [("arbeit".to_string(), vec!["context".to_string(), "sender".to_string()])]
                .into_iter()
                .collect();

        let result = compute_target_path(&record, Some(&folders));

        // "sender" is missing, so only "context" folder part is used
        assert_eq!(
            result.as_deref(),
            Some("sorted/arbeit/arbeit-rechnung-2025.pdf")
        );
    }

    // -----------------------------------------------------------------------
    // TestIsCollisionVariant (9 tests)
    // -----------------------------------------------------------------------

    #[test]
    fn test_collision_exact_match() {
        assert!(is_collision_variant(
            "sorted/work/invoice.pdf",
            "sorted/work/invoice.pdf",
        ));
    }

    #[test]
    fn test_collision_suffix_matches() {
        assert!(is_collision_variant(
            "sorted/work/invoice_e3ca2c9b.pdf",
            "sorted/work/invoice.pdf",
        ));
    }

    #[test]
    fn test_collision_different_suffix_still_matches() {
        assert!(is_collision_variant(
            "sorted/work/invoice_abcd1234.pdf",
            "sorted/work/invoice.pdf",
        ));
    }

    #[test]
    fn test_collision_different_filename_no_match() {
        assert!(!is_collision_variant(
            "sorted/work/receipt_e3ca2c9b.pdf",
            "sorted/work/invoice.pdf",
        ));
    }

    #[test]
    fn test_collision_different_directory_no_match() {
        assert!(!is_collision_variant(
            "sorted/personal/invoice_e3ca2c9b.pdf",
            "sorted/work/invoice.pdf",
        ));
    }

    #[test]
    fn test_collision_non_hex_suffix_no_match() {
        assert!(!is_collision_variant(
            "sorted/work/invoice_zzzzzzzz.pdf",
            "sorted/work/invoice.pdf",
        ));
    }

    #[test]
    fn test_collision_wrong_length_suffix_no_match() {
        assert!(!is_collision_variant(
            "sorted/work/invoice_abcd12.pdf",
            "sorted/work/invoice.pdf",
        ));
    }

    #[test]
    fn test_collision_deep_path_with_collision() {
        assert!(is_collision_variant(
            "sorted/belege/nain_trading/79901/belege-2025-naturescene_231x173-972_e3ca2c9b.pdf",
            "sorted/belege/nain_trading/79901/belege-2025-naturescene_231x173-972.pdf",
        ));
    }

    #[test]
    fn test_collision_txt_extension() {
        assert!(is_collision_variant(
            "sorted/work/transcript_aabbccdd.txt",
            "sorted/work/transcript.txt",
        ));
    }

    // -----------------------------------------------------------------------
    // TestReconcileSortedCollisionVariant (6 tests)
    // -----------------------------------------------------------------------

    #[test]
    fn test_collision_variant_is_complete() {
        let mut record = _make_record();
        record.state = State::NeedsProcessing;
        record.current_paths.push(PathEntry {
            path: "sorted/work/2025-01-Invoice_e3ca2c9b.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("2025-01-Invoice.pdf".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.state, State::IsComplete);
        assert!(r.target_path.is_none());
        assert!(r.current_reference.is_none());
    }

    #[test]
    fn test_collision_variant_with_context_folders() {
        let mut record = _make_record();
        record.state = State::NeedsProcessing;
        record.current_paths.push(PathEntry {
            path: "sorted/arbeit/Schulze GmbH/rechnung_abcd1234.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("arbeit".to_string());
        record.assigned_filename = Some("rechnung.pdf".to_string());
        record.metadata = Some(json!({"context": "arbeit", "sender": "Schulze GmbH"}));
        let folders: HashMap<String, Vec<String>> =
            [("arbeit".to_string(), vec!["context".to_string(), "sender".to_string()])]
                .into_iter()
                .collect();

        let result = reconcile(&mut record, None, Some(&folders), None);

        let r = result.unwrap();
        assert_eq!(r.state, State::IsComplete);
        assert!(r.target_path.is_none());
    }

    #[test]
    fn test_user_rename_adopts_new_filename() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.current_paths.push(PathEntry {
            path: "sorted/work/my-custom-name.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("2025-01-Invoice.pdf".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.assigned_filename.as_deref(), Some("my-custom-name.pdf"));
        assert_eq!(r.state, State::IsComplete);
        assert!(r.target_path.is_none());
    }

    #[test]
    fn test_user_rename_context_change() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.current_paths.push(PathEntry {
            path: "sorted/personal/my-custom-name.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("my-custom-name.pdf".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.context.as_deref(), Some("personal"));
        assert_eq!(r.assigned_filename.as_deref(), Some("my-custom-name.pdf"));
        assert_eq!(r.state, State::IsComplete);
        assert!(r.target_path.is_none());
    }

    #[test]
    fn test_user_rename_and_context_change() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.current_paths.push(PathEntry {
            path: "sorted/personal/renamed.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("2025-01-Invoice.pdf".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.assigned_filename.as_deref(), Some("renamed.pdf"));
        assert_eq!(r.context.as_deref(), Some("personal"));
        assert_eq!(r.state, State::IsComplete);
        assert!(r.target_path.is_none());
    }

    #[test]
    fn test_exact_match_still_works() {
        let mut record = _make_record();
        record.state = State::NeedsProcessing;
        record.current_paths.push(PathEntry {
            path: "sorted/work/2025-01-Invoice.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("2025-01-Invoice.pdf".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.state, State::IsComplete);
        assert!(r.target_path.is_none());
    }

    // -----------------------------------------------------------------------
    // TestPreprocessOutputDuplicateHash (7 tests)
    // -----------------------------------------------------------------------

    #[test]
    fn test_output_hash_matches_other_source_hash() {
        let mut r1 = _make_record();
        r1.source_hash = "XHASH".to_string();
        r1.state = State::IsComplete;

        let mut r2 = _make_record();
        r2.source_hash = "other".to_string();
        r2.output_filename = Some("uuid-r2".to_string());
        r2.state = State::NeedsProcessing;
        r2.source_paths.push(PathEntry {
            path: "archive/r2.pdf".to_string(),
            timestamp: _ts(0),
        });

        let _r1_id = r1.id;
        let r2_id = r2.id;
        let mut records = vec![r1, r2];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            ".output/uuid-r2",
            Some("XHASH"),
            Some(1024),
        );

        let (modified_ids, _created) = preprocess(&[change], &mut records, &mut new_records, |_p| {
            json!({"context": "work", "metadata": {}, "assigned_filename": "out.pdf"})
        });

        assert_eq!(modified_ids.len(), 1);
        assert!(modified_ids.contains(&r2_id));
        // r2 is the second record
        assert_eq!(records[1].state, State::HasError);
        assert!(records[1].output_filename.is_none());
        assert!(records[1]
            .deleted_paths
            .contains(&".output/uuid-r2".to_string()));
        assert!(records[1]
            .duplicate_sources
            .contains(&"archive/r2.pdf".to_string()));
        assert!(records[1].source_paths.is_empty());
        assert!(records[1].current_paths.is_empty());
        // r1 unchanged
        assert_eq!(records[0].state, State::IsComplete);
    }

    #[test]
    fn test_output_hash_matches_other_hash() {
        let mut r1 = _make_record();
        r1.source_hash = "src1".to_string();
        r1.hash = Some("XHASH".to_string());
        r1.state = State::IsComplete;

        let mut r2 = _make_record();
        r2.source_hash = "src2".to_string();
        r2.output_filename = Some("uuid-r2".to_string());
        r2.state = State::NeedsProcessing;
        r2.source_paths.push(PathEntry {
            path: "archive/r2.pdf".to_string(),
            timestamp: _ts(0),
        });

        let mut records = vec![r1, r2];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            ".output/uuid-r2",
            Some("XHASH"),
            Some(1024),
        );

        let (_modified_ids, _created) =
            preprocess(&[change], &mut records, &mut new_records, |_p| {
                json!({"context": "work", "metadata": {}, "assigned_filename": "out.pdf"})
            });

        assert_eq!(records[1].state, State::HasError);
        assert!(records[1]
            .deleted_paths
            .contains(&".output/uuid-r2".to_string()));
        assert!(records[1]
            .duplicate_sources
            .contains(&"archive/r2.pdf".to_string()));
    }

    #[test]
    fn test_output_hash_matches_own_source_hash_only() {
        let mut r = _make_record();
        r.source_hash = "XHASH".to_string();
        r.output_filename = Some("uuid-r".to_string());
        r.state = State::NeedsProcessing;

        let mut records = vec![r];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            ".output/uuid-r",
            Some("XHASH"),
            Some(1024),
        );

        let (_modified_ids, _created) =
            preprocess(&[change], &mut records, &mut new_records, |_p| {
                json!({"context": "work", "metadata": {"date": "2025"}, "assigned_filename": "out.pdf"})
            });

        assert_eq!(records[0].hash.as_deref(), Some("XHASH"));
        assert_eq!(records[0].context.as_deref(), Some("work"));
        assert_eq!(records[0].current_paths.len(), 1);
        assert!(records[0].deleted_paths.is_empty());
    }

    #[test]
    fn test_output_hash_no_match() {
        let mut r = _make_record();
        r.source_hash = "src".to_string();
        r.output_filename = Some("uuid-r".to_string());
        r.state = State::NeedsProcessing;

        let mut records = vec![r];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            ".output/uuid-r",
            Some("NEWHASH"),
            Some(1024),
        );

        let (_modified_ids, _created) =
            preprocess(&[change], &mut records, &mut new_records, |_p| {
                json!({"context": "work", "metadata": {}, "assigned_filename": "out.pdf"})
            });

        assert_eq!(records[0].hash.as_deref(), Some("NEWHASH"));
        assert_eq!(records[0].context.as_deref(), Some("work"));
        assert_eq!(records[0].current_paths.len(), 1);
        assert!(records[0].deleted_paths.is_empty());
    }

    #[test]
    fn test_output_duplicate_keeps_non_archive_sources() {
        let mut r1 = _make_record();
        r1.source_hash = "XHASH".to_string();
        r1.state = State::IsComplete;

        let mut r2 = _make_record();
        r2.source_hash = "src2".to_string();
        r2.output_filename = Some("uuid-r2".to_string());
        r2.state = State::NeedsProcessing;
        r2.source_paths.push(PathEntry {
            path: "archive/r2.pdf".to_string(),
            timestamp: _ts(0),
        });
        r2.source_paths.push(PathEntry {
            path: "missing/r2.pdf".to_string(),
            timestamp: _ts(1),
        });

        let mut records = vec![r1, r2];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            ".output/uuid-r2",
            Some("XHASH"),
            Some(1024),
        );

        let (_modified_ids, _created) =
            preprocess(&[change], &mut records, &mut new_records, |_p| {
                json!({"context": "work", "metadata": {}, "assigned_filename": "out.pdf"})
            });

        assert_eq!(records[1].state, State::HasError);
        assert!(records[1]
            .duplicate_sources
            .contains(&"archive/r2.pdf".to_string()));
        // missing/r2.pdf stays in source_paths
        assert_eq!(records[1].source_paths.len(), 1);
        assert_eq!(records[1].source_paths[0].path, "missing/r2.pdf");
    }

    #[test]
    fn test_two_identical_outputs_same_cycle() {
        let mut r1 = _make_record();
        r1.source_hash = "src1".to_string();
        r1.output_filename = Some("uuid-r1".to_string());
        r1.state = State::NeedsProcessing;
        r1.source_paths.push(PathEntry {
            path: "archive/r1.pdf".to_string(),
            timestamp: _ts(0),
        });

        let mut r2 = _make_record();
        r2.source_hash = "src2".to_string();
        r2.output_filename = Some("uuid-r2".to_string());
        r2.state = State::NeedsProcessing;
        r2.source_paths.push(PathEntry {
            path: "archive/r2.pdf".to_string(),
            timestamp: _ts(0),
        });

        let mut records = vec![r1, r2];
        let mut new_records: Vec<Record> = Vec::new();
        let c1 = _make_change(
            EventType::Addition,
            ".output/uuid-r1",
            Some("IDENTICAL"),
            Some(1024),
        );
        let c2 = _make_change(
            EventType::Addition,
            ".output/uuid-r2",
            Some("IDENTICAL"),
            Some(1024),
        );

        let (_modified_ids, _created) =
            preprocess(&[c1, c2], &mut records, &mut new_records, |_p| {
                json!({"context": "work", "metadata": {}, "assigned_filename": "out.pdf"})
            });

        // r1: ingested normally (first in the batch)
        assert_eq!(records[0].hash.as_deref(), Some("IDENTICAL"));
        assert_eq!(records[0].context.as_deref(), Some("work"));
        assert_eq!(records[0].current_paths.len(), 1);
        assert!(records[0].deleted_paths.is_empty());

        // r2: detected as duplicate (r1.hash was already set)
        assert_eq!(records[1].state, State::HasError);
        assert!(records[1].output_filename.is_none());
        assert!(records[1]
            .deleted_paths
            .contains(&".output/uuid-r2".to_string()));
        assert!(records[1]
            .duplicate_sources
            .contains(&"archive/r2.pdf".to_string()));
        assert!(records[1].source_paths.is_empty());
    }

    // -----------------------------------------------------------------------
    // TestPreprocessIncomingMatchesHash (3 tests)
    // -----------------------------------------------------------------------

    #[test]
    fn test_incoming_matches_record_hash() {
        let mut records = vec![{
            let mut r = _make_record();
            r.source_hash = "SOURCE".to_string();
            r.hash = Some("PROCESSED".to_string());
            r.state = State::IsComplete;
            r.current_paths.push(PathEntry {
                path: "sorted/work/doc.pdf".to_string(),
                timestamp: _ts(0),
            });
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "incoming/copy.pdf",
            Some("PROCESSED"),
            Some(500),
        );

        let (modified_ids, created) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar);

        assert_eq!(modified_ids.len(), 1);
        assert!(modified_ids.contains(&records[0].id));
        // incoming matches source_hash first, but here source_hash="SOURCE" != "PROCESSED"
        // so it matches hash="PROCESSED" -> current_paths
        assert!(records[0]
            .current_paths
            .iter()
            .any(|pe| pe.path == "incoming/copy.pdf"));
        assert!(created.is_empty());
    }

    #[test]
    fn test_incoming_matches_source_hash_preferred() {
        let mut records = vec![{
            let mut r = _make_record();
            r.source_hash = "HASH1".to_string();
            r.hash = Some("HASH1".to_string());
            r.state = State::IsComplete;
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "incoming/doc.txt",
            Some("HASH1"),
            Some(500),
        );

        let (modified_ids, _created) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar);

        assert_eq!(modified_ids.len(), 1);
        // Matched by source_hash (first in list for incoming) → source_paths
        assert!(records[0]
            .source_paths
            .iter()
            .any(|pe| pe.path == "incoming/doc.txt"));
    }

    #[test]
    fn test_incoming_hash_match_cleaned_by_reconcile() {
        let mut record = _make_record();
        record.source_hash = "SOURCE".to_string();
        record.hash = Some("PROCESSED".to_string());
        record.state = State::IsComplete;
        record.current_paths.push(PathEntry {
            path: "sorted/work/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.current_paths.push(PathEntry {
            path: "incoming/copy.pdf".to_string(),
            timestamp: _ts(1),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("doc.pdf".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        // incoming/ is not a valid current location → moved to deleted_paths
        assert!(r.deleted_paths.contains(&"incoming/copy.pdf".to_string()));
        assert!(!r
            .current_paths
            .iter()
            .any(|pe| pe.path == "incoming/copy.pdf"));
        assert_eq!(r.state, State::IsComplete);
    }

    // -----------------------------------------------------------------------
    // TestPreprocessReset (3 tests)
    // -----------------------------------------------------------------------

    #[test]
    fn test_reset_matches_hash() {
        let mut records = vec![{
            let mut r = _make_record();
            r.hash = Some("reset_hash".to_string());
            r.state = State::IsComplete;
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "reset/doc.pdf",
            Some("reset_hash"),
            Some(700),
        );

        let (modified_ids, _created) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar);

        assert_eq!(modified_ids.len(), 1);
        assert!(records[0]
            .current_paths
            .iter()
            .any(|pe| pe.path == "reset/doc.pdf"));
    }

    #[test]
    fn test_reset_unknown_not_created() {
        let mut records: Vec<Record> = Vec::new();
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "reset/stray.pdf",
            Some("unknown_hash"),
            Some(500),
        );

        let (modified_ids, created) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar);

        assert!(modified_ids.is_empty());
        assert!(created.is_empty());
    }

    #[test]
    fn test_reset_does_not_match_source_hash() {
        let mut records = vec![{
            let mut r = _make_record();
            r.source_hash = "src_hash".to_string();
            r.hash = None;
            r
        }];
        let mut new_records: Vec<Record> = Vec::new();
        let change = _make_change(
            EventType::Addition,
            "reset/doc.pdf",
            Some("src_hash"),
            Some(500),
        );

        let (modified_ids, _created) =
            preprocess(&[change], &mut records, &mut new_records, _noop_sidecar);

        // source_hash match is not configured for reset/
        assert!(modified_ids.is_empty());
    }

    // -----------------------------------------------------------------------
    // TestReconcileReset (8 tests)
    // -----------------------------------------------------------------------

    #[test]
    fn test_reset_moves_to_sorted() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.current_paths.push(PathEntry {
            path: "reset/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.metadata = Some(json!({
            "context": "work",
            "date": "2025-01-15",
            "type": "Invoice",
        }));
        record.assigned_filename = Some("old-name.pdf".to_string());
        record.original_filename = "original.pdf".to_string();

        let recompute = |_r: &Record| -> Option<String> {
            Some("invoice-2025-01-15.pdf".to_string())
        };

        let result = reconcile(&mut record, None, None, Some(&recompute));

        let r = result.unwrap();
        assert_eq!(
            r.assigned_filename.as_deref(),
            Some("invoice-2025-01-15.pdf")
        );
        assert_eq!(
            r.target_path.as_deref(),
            Some("sorted/work/invoice-2025-01-15.pdf")
        );
        assert_eq!(r.current_reference.as_deref(), Some("reset/doc.pdf"));
        assert_eq!(r.state, State::IsComplete);
    }

    #[test]
    fn test_reset_uses_context_folders() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.current_paths.push(PathEntry {
            path: "reset/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.metadata = Some(json!({
            "context": "work",
            "date": "2025",
            "sender": "Acme",
        }));
        record.assigned_filename = Some("old.pdf".to_string());

        let recompute = |_r: &Record| -> Option<String> {
            Some("invoice-2025.pdf".to_string())
        };
        let folders: HashMap<String, Vec<String>> = [(
            "work".to_string(),
            vec!["context".to_string(), "sender".to_string()],
        )]
        .into_iter()
        .collect();

        let result = reconcile(&mut record, None, Some(&folders), Some(&recompute));

        let r = result.unwrap();
        assert_eq!(r.assigned_filename.as_deref(), Some("invoice-2025.pdf"));
        assert_eq!(
            r.target_path.as_deref(),
            Some("sorted/work/Acme/invoice-2025.pdf")
        );
    }

    #[test]
    fn test_reset_without_recompute_callback() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.current_paths.push(PathEntry {
            path: "reset/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("existing-name.pdf".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        assert_eq!(r.assigned_filename.as_deref(), Some("existing-name.pdf"));
        assert_eq!(
            r.target_path.as_deref(),
            Some("sorted/work/existing-name.pdf")
        );
        assert_eq!(r.current_reference.as_deref(), Some("reset/doc.pdf"));
    }

    #[test]
    fn test_reset_recompute_returns_none() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.current_paths.push(PathEntry {
            path: "reset/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("keep-this.pdf".to_string());

        let recompute = |_r: &Record| -> Option<String> { None };

        let result = reconcile(&mut record, None, None, Some(&recompute));

        let r = result.unwrap();
        assert_eq!(r.assigned_filename.as_deref(), Some("keep-this.pdf"));
        assert_eq!(
            r.target_path.as_deref(),
            Some("sorted/work/keep-this.pdf")
        );
    }

    #[test]
    fn test_reset_no_context() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.current_paths.push(PathEntry {
            path: "reset/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = None;
        record.assigned_filename = Some("doc.pdf".to_string());

        let recompute = |_r: &Record| -> Option<String> { Some("new.pdf".to_string()) };

        let result = reconcile(&mut record, None, None, Some(&recompute));

        let r = result.unwrap();
        assert_eq!(r.assigned_filename.as_deref(), Some("new.pdf"));
        assert!(r.target_path.is_none());
        assert_eq!(r.state, State::IsComplete);
    }

    #[test]
    fn test_reset_no_assigned_filename() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.current_paths.push(PathEntry {
            path: "reset/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = None;

        let recompute = |_r: &Record| -> Option<String> { None };

        let result = reconcile(&mut record, None, None, Some(&recompute));

        let r = result.unwrap();
        assert!(r.target_path.is_none());
    }

    #[test]
    fn test_reset_deduplicates_current_paths() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.current_paths.push(PathEntry {
            path: "processed/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.current_paths.push(PathEntry {
            path: "reset/doc.pdf".to_string(),
            timestamp: _ts(1),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("doc.pdf".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        // Dedup keeps most recent (reset/), deletes older (processed/)
        assert_eq!(r.current_paths.len(), 1);
        assert_eq!(r.current_paths[0].path, "reset/doc.pdf");
        assert!(r.deleted_paths.contains(&"processed/doc.pdf".to_string()));
    }

    #[test]
    fn test_reset_not_cleaned_as_invalid_location() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.current_paths.push(PathEntry {
            path: "reset/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.assigned_filename = Some("doc.pdf".to_string());

        let result = reconcile(&mut record, None, None, None);

        let r = result.unwrap();
        // Should NOT be in deleted_paths
        assert!(!r.deleted_paths.contains(&"reset/doc.pdf".to_string()));
        assert_eq!(r.current_reference.as_deref(), Some("reset/doc.pdf"));
    }

    // -----------------------------------------------------------------------
    // TestReconcileContextFieldNames (1 test)
    // -----------------------------------------------------------------------

    #[test]
    fn test_sorted_with_context_field_names_missing_fields_added() {
        let mut record = _make_record();
        record.state = State::IsComplete;
        record.current_paths.push(PathEntry {
            path: "sorted/work/doc.pdf".to_string(),
            timestamp: _ts(0),
        });
        record.context = Some("work".to_string());
        record.metadata = Some(json!({"context": "work", "date": "2025-01-01"}));
        record.assigned_filename = Some("doc.pdf".to_string());

        let cfn: HashMap<String, Vec<String>> = [(
            "work".to_string(),
            vec![
                "context".to_string(),
                "date".to_string(),
                "category".to_string(),
            ],
        )]
        .into_iter()
        .collect();

        let result = reconcile(&mut record, Some(&cfn), None, None);

        let r = result.unwrap();
        let meta = r.metadata.as_ref().unwrap();
        assert!(meta.get("category").is_some());
        assert!(meta.get("category").unwrap().is_null());
        assert_eq!(meta.get("context").unwrap().as_str(), Some("work"));
        assert_eq!(meta.get("date").unwrap().as_str(), Some("2025-01-01"));
    }
}
